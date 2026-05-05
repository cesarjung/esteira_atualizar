# importador_carteira.py — CORRIGIDO (dtype + números J/K)

import os, re, json, time, random, unicodedata, pathlib
from datetime import datetime
from typing import List, Any

import pandas as pd
import gspread
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1, a1_to_rowcol
from google.oauth2.service_account import Credentials as SACreds


ORIGEM_ID   = '1lUNIeWCddfmvJEjWJpQMtuR4oRuMsI3VImDY0xBp3Bs'
DESTINO_ID  = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_ORIGEM  = 'Carteira'
ABA_DESTINO = 'Carteira'

COLS_ORIGEM = 'A,Z,B,C,D,E,U,T,N,AA,AB,CN,CQ,CR,CS,BQ,CE,V'.split(',')
DATE_LETTERS = 'CN,CQ,CR,CS,BQ,CE'.split(',')


def now():
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')


def log(msg):
    print(f"[{now()}] {msg}", flush=True)


def a1index(L):
    return a1_to_rowcol(f"{L}1")[1]


def col_letter(n):
    return re.sub(r'\d', '', rowcol_to_a1(1, n))


def limpar_numero_brasil(v):
    if v is None:
        return ""

    s = str(v).strip().lstrip("'").strip()

    if s == "":
        return ""

    s = (
        s.replace("R$", "")
         .replace(" ", "")
         .replace("\u00A0", "")
         .replace(".", "")
         .replace(",", ".")
    )

    s = re.sub(r"[^0-9.-]", "", s)

    if s in ("", "-", ".", "-."):
        return ""

    try:
        return float(s)
    except:
        return ""


def parse_dates(series_like: pd.Series) -> pd.Series:
    s = pd.to_datetime(series_like, dayfirst=True, errors='coerce')
    return s.dt.strftime('%d/%m/%Y').where(s.notna(), "")


def make_creds():
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        return SACreds.from_service_account_info(json.loads(env_json))

    return SACreds.from_service_account_file("credenciais.json")


def abrir_planilhas():
    gc = gspread.authorize(make_creds())

    b_src = gc.open_by_key(ORIGEM_ID)
    b_dst = gc.open_by_key(DESTINO_ID)

    return (
        b_src.worksheet(ABA_ORIGEM),
        b_dst.worksheet(ABA_DESTINO)
    )


# =========================
# FUNÇÃO CORRIGIDA
# =========================
def ler_origem_para_df(w_src):

    lastL = col_letter(max(a1index(c) for c in COLS_ORIGEM))
    rng = f"A5:{lastL}"

    log(f"📥 Lendo {rng}")
    dat = w_src.get(rng)

    if not dat:
        return pd.DataFrame()

    hdr, rows = dat[0], dat[1:]

    idx = [a1index(c) - 1 for c in COLS_ORIGEM]

    tbl = [
        [r[i] if i < len(r) else "" for i in idx]
        for r in rows if r and str(r[0]).strip()
    ]

    df = pd.DataFrame(
        tbl,
        columns=[hdr[i] if i < len(hdr) else f"COL_{COLS_ORIGEM[j]}"
                 for j, i in enumerate(idx)]
    )

    # 🔥 CORREÇÃO PRINCIPAL (resolve teu erro)
    df = df.astype(object)

    log(f"📊 {len(df)} linhas carregadas")

    pos = {l: i for i, l in enumerate(COLS_ORIGEM)}

    # datas
    for l in DATE_LETTERS:
        p = pos.get(l)
        if p is not None:
            df.iloc[:, p] = parse_dates(df.iloc[:, p])

    # 🔥 números J e K
    for pos_num in [9, 10]:
        if pos_num < len(df.columns):
            log(f"🔢 Convertendo coluna {col_letter(pos_num+1)} para número")
            df.iloc[:, pos_num] = df.iloc[:, pos_num].apply(limpar_numero_brasil)

    return df


# =========================
# MAIN
# =========================
def main():
    log("🚀 Iniciando importador_carteira")

    w_src, w_dst = abrir_planilhas()

    df = ler_origem_para_df(w_src)

    if df.empty:
        log("⚠️ Sem dados")
        return

    values = df.values.tolist()

    log(f"📤 Enviando {len(values)} linhas")

    w_dst.clear()

    w_dst.update("A1", [list(df.columns)])
    w_dst.update("A2", values, value_input_option='USER_ENTERED')

    log("✅ Finalizado com sucesso")


if __name__ == "__main__":
    main()
