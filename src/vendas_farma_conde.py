import os
print("Rodando em:", os.getcwd())

import re
import ssl
import smtplib
import logging
import traceback
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from pandas.tseries.offsets import BDay
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders


# =========================================================
# CONFIGURAÇÕES GERAIS
# =========================================================

BASE_URL = "https://senffnet.vtexcommercestable.com.br"

VTEX_APP_KEY = os.getenv("VTEX_APP_KEY")
VTEX_APP_TOKEN = os.getenv("VTEX_APP_TOKEN")

SMTP_SERVER = "smtp.skymail.net.br"
SMTP_PORT = 465
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = SMTP_USER

BASE_OUTPUT_DIR = "output/bruto"
CIRC_OUTPUT_DIR = "circularizacao"
CONFIG_SELLERS_FILE = "config/email_farmaconde.xlsx"

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "vtex_farma_conde.log")

HTTP_TIMEOUT = 30
DEFAULT_MAX_WORKERS = min(32, (os.cpu_count() or 4) * 4)
TZ_BR = timezone(timedelta(hours=-3))


# =========================================================
# VALIDAÇÕES
# =========================================================

if not all([VTEX_APP_KEY, VTEX_APP_TOKEN, SMTP_USER, SMTP_PASSWORD]):
    raise RuntimeError("❌ Variáveis de ambiente obrigatórias não definidas")


# =========================================================
# PASTAS
# =========================================================

os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
os.makedirs(CIRC_OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# =========================================================
# LOGS
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def log(msg: str):
    logging.info(msg)
    print(msg)


# =========================================================
# HELPERS
# =========================================================

def vtex_headers():
    return {
        "Content-Type": "application/json",
        "X-VTEX-API-AppKey": VTEX_APP_KEY,
        "X-VTEX-API-AppToken": VTEX_APP_TOKEN
    }


@lru_cache(maxsize=10000)
def formatar_data_curta(iso_str):
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(TZ_BR)
        return dt.strftime("%d/%m/%Y")
    except:
        return iso_str


def janela_ontem_utc():
    ontem = datetime.now(TZ_BR).date() - timedelta(days=1)

    ini = datetime(ontem.year, ontem.month, ontem.day, 0, 0, 0, tzinfo=TZ_BR)
    fim = datetime(ontem.year, ontem.month, ontem.day, 23, 59, 59, 999000, tzinfo=TZ_BR)

    fmt = lambda d: d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return (
        fmt(ini),
        fmt(fim),
        ontem.strftime("%Y-%m-%d"),
        ontem.strftime("%d/%m/%Y"),
        ontem.strftime("%d-%m-%y")
    )


def carregar_sellers():
    df = pd.read_excel(CONFIG_SELLERS_FILE)
    return [{
        "id": str(r["sellerId"]).strip(),
        "display": str(r["sellerName"]).strip(),
        "emailTo": [e.strip() for e in str(r.get("emailTo","")).split(";") if e.strip()],
        "emailCc": [e.strip() for e in str(r.get("emailCc","")).split(";") if e.strip()],
    } for _, r in df.iterrows() if str(r.get("ativo","")).lower() == "sim"]


# =========================================================
# VTEX
# =========================================================

def listar_resumo(start_utc, end_utc, seller_name):
    orders, page = [], 1
    s = requests.Session()
    s.headers.update(vtex_headers())

    while True:
        r = s.get(
            f"{BASE_URL}/api/oms/pvt/orders",
            params={
                "page": page,
                "per_page": 100,
                "f_status": "invoiced",
                "f_sellerNames": seller_name,
                "f_invoicedDate": f"invoicedDate:[{start_utc} TO {end_utc}]"
            },
            timeout=HTTP_TIMEOUT
        )

        if r.status_code != 200:
            break

        lista = r.json().get("list", [])
        if not lista:
            break

        orders.extend(lista)
        page += 1

        if len(lista) < 100:
            break

    return orders


def detalhe(order_id):
    try:
        r = requests.get(
            f"{BASE_URL}/api/oms/pvt/orders/{order_id}",
            headers=vtex_headers(),
            timeout=HTTP_TIMEOUT
        )
        return r.json() if r.status_code == 200 else None
    except:
        return None


# =========================================================
# PROCESSAMENTO
# =========================================================

def get_total(totals, code):
    return next((t.get("value",0)/100 for t in totals if t.get("id")==code), 0.0)


def gerar_linhas(order, seller):
    if seller["id"] not in [s.get("id") for s in order.get("sellers", [])]:
        return []

    totals = order.get("totals", [])
    itens = get_total(totals,"Items")
    frete = get_total(totals,"Shipping")

    linhas = []
    for tx in order.get("paymentData",{}).get("transactions",[]):
        for p in tx.get("payments",[]):
            linhas.append({
                "Faturado em": formatar_data_curta(order.get("invoicedDate")),
                "Pedido": order.get("orderId"),
                "Seller": seller["display"],
                "Total_itens": itens,
                "Frete": frete,
                "Valor_total": itens+frete,
                "Parcelas": p.get("installments")
            })
    return linhas


# =========================================================
# CIRCULARIZAÇÃO
# =========================================================

def circularizar(path, sufixo):
    df = pd.read_excel(path).drop_duplicates()
    df["Faturado em"] = pd.to_datetime(df["Faturado em"], dayfirst=True)

    for i in range(1,13):
        df[f"Parcela {i}"] = None

    for i,r in df.iterrows():
        if not r["Parcelas"]: continue
        for p in range(int(min(r["Parcelas"],12))):
            d = (r["Faturado em"] + pd.DateOffset(months=p+1)).replace(day=15)
            if d.weekday()>=5: d+=BDay(1)
            df.at[i,f"Parcela {p+1}"] = d.strftime("%d/%m/%Y")

    out = os.path.join(CIRC_OUTPUT_DIR,f"Farma-Conde_{sufixo}.xlsx")
    df.to_excel(out,index=False)
    return out


# =========================================================
# EMAIL
# =========================================================

def enviar_email(path, seller, data_brt):
    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(seller["emailTo"])
    msg["Cc"] = ", ".join(seller["emailCc"])
    msg["Subject"] = f"Farma Conde – Circularização – {data_brt}"

    msg.attach(MIMEText("Segue relatório de circularização.","plain"))

    with open(path,"rb") as f:
        part = MIMEBase("application","octet-stream")
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition",f'attachment; filename="{os.path.basename(path)}"')
        msg.attach(part)

    with smtplib.SMTP_SSL(SMTP_SERVER,SMTP_PORT) as s:
        s.login(SMTP_USER,SMTP_PASSWORD)
        s.sendmail(EMAIL_FROM, seller["emailTo"]+seller["emailCc"], msg.as_string())


# =========================================================
# MAIN
# =========================================================

def main():
    start_utc,end_utc,data_iso,data_brt,suf = janela_ontem_utc()
    seller = carregar_sellers()[0]

    resumo = listar_resumo(start_utc,end_utc,seller["display"])
    detalhes = {}

    with ThreadPoolExecutor(DEFAULT_MAX_WORKERS) as ex:
        fut = {ex.submit(detalhe,o["orderId"]):o["orderId"] for o in resumo}
        for f in as_completed(fut):
            if f.result(): detalhes[fut[f]]=f.result()

    linhas=[]
    for o in resumo:
        if o["orderId"] in detalhes:
            linhas.extend(gerar_linhas(detalhes[o["orderId"]],seller))

    bruto = os.path.join(BASE_OUTPUT_DIR,f"vendas_{data_iso}.xlsx")
    pd.DataFrame(linhas).drop_duplicates().to_excel(bruto,index=False)

    final = circularizar(bruto,suf)
    enviar_email(final,seller,data_brt)

    log("✅ Processo finalizado")


if __name__ == "__main__":
    main()
