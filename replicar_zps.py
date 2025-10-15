# replicar_zps.py — resiliente, rápido e com números contáveis; formatações opcionais; sem pular destino
from datetime import datetime
import os, json, pathlib
import re
import time
import sys
import random

import gspread
from google.oauth2.service_account import Credentials as SACreds
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1

# =========================
# CONFIGURAÇÃO
# =========================
ID_ORIGEM   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM  = "zps"

PLANILHAS_DESTINO = [
    "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c",
    "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M",
    "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c",
    "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw",
]

# Opções (OFF por padrão p/ performance)
APLICAR_FORMATO_DATAS    = False   # aplica dd/MM/yyyy nas colunas de data
APLICAR_FORMATO_NUMEROS  = False   # aplica #,##0.00 nas colunas numéricas
CARIMBAR                  = True    # grava timestamp em uma célula
CARIMBAR_CEL              = "R1"    # alvo desejado; se não existir, cai no último col da aba
HARD_CLEAR_BEFORE_WRITE   = False   # se True, limpa A:última_col antes de escrever (1 chamada extra)

# Colunas do seu caso: C(2), F(5), G(6) numéricas; A(0), N(13) datas (0-based)
COLS_NUM_IDX  = {2, 5, 6}
COLS_DATE_IDX = {0, 13}

# =========================
# TUNING / RETRIES
# =========================
SCOPES               = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
TRANSIENT_CODES      = {429, 500, 502, 503, 504}
MAX_RETRIES          = 6
BASE_SLEEP           = 1.0
PAUSE_BETWEEN_WRITES = 0.10
PAUSE_BETWEEN_DESTS  = 0.6
EXTRA_TAIL_ROWS      = 200

DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5   # 5,10,20,40,80 s

# =========================
# CREDENCIAIS FLEXÍVEIS
# =========================
def make_creds():
    env = os.environ.get("GOOGLE_CREDENTIALS")
    if env:
        return SACreds.from_service_account_info(json.loads(env), scopes=SCOPES)
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    return SACreds.from_service_account_file(pathlib.Path("credenciais.json"), scopes=SCOPES)

# =========================
# RETRY HELPERS
# =========================
def _status_code(e: APIError):
    m = re.search(r"\[(\d{3})\]", str(e))
    try:
        return int(m.group(1)) if m else None
    except Exception:
        return None

def _with_retry(fn, *args, desc=None, **kwargs):
    for tent in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = _status_code(e)
            if code not in TRANSIENT_CODES or tent >= MAX_RETRIES:
                print(f"❌ {desc or fn.__name__} falhou: {e}")
                raise
            slp = min(60.0, BASE_SLEEP * (2 ** (tent - 1)) + random.uniform(0, 0.75))
            print(f"⚠️  {desc or fn.__name__}: HTTP {code} — retry {tent}/{MAX_RETRIES-1} em {slp:.1f}s")
            time.sleep(slp)

# =========================
# A1 / COL HELPERS
# =========================
def get_last_col_letter(n_cols: int) -> str:
    a1 = rowcol_to_a1(1, n_cols)  # ex.: 'K1'
    return re.sub(r"\d+", "", a1) # 'K'

def a1_parse(cell: str):
    m = re.match(r"^([A-Za-z]+)(\d+)$", cell.strip())
    if not m:
        return "A", 1
    return m.group(1).upper(), int(m.group(2))

def col_letter_to_index(letter: str) -> int:
    letter = letter.upper()
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx  # 1-based

def col_index_to_letter(index: int) -> str:
    res = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        res = chr(rem + ord('A')) + res
    return res

# =========================
# TRATAMENTO DOS DADOS
# =========================
def limpar_num(texto: str):
    """Converte strings em float (remove R$, pontos de milhar, trata vírgula como decimal, etc.)."""
    if texto is None:
        return ""
    s = str(texto).strip()
    if not s:
        return ""
    if s.startswith("'"):
        s = s[1:]
    s = s.replace("R$", "").strip()
    s = re.sub(r"[^0-9,\.\-+eE]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""

def normaliza_data(v: str) -> str:
    """Tenta dd/MM/yyyy a partir de (dd/mm/aaaa, dd/mm/aa, yyyy-mm-dd, dd-mm-aaaa)."""
    if v is None:
        return ""
    s = str(v).strip().replace("’", "").replace("‘", "").replace("'", "")
    if not s:
        return ""
    s = re.sub(r"[^0-9/\-: ]", "", s)
    dt = None
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s.split(" ")[0], fmt)
            break
        except Exception:
            continue
    return dt.strftime("%d/%m/%Y") if dt else s

# =========================
# GRID / WRITE HELPERS
# =========================
def ensure_grid(ws, min_rows: int, min_cols: int):
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
    except Exception:
        cur_rows = cur_cols = 0
    if cur_rows < min_rows or cur_cols < min_cols:
        _with_retry(ws.resize, rows=max(cur_rows, min_rows), cols=max(cur_cols, min_cols), desc="resize")

def values_clear(ws, a1_range, tag="values_clear"):
    _with_retry(ws.spreadsheet.values_clear, a1_range, desc=tag)
    time.sleep(PAUSE_BETWEEN_WRITES)

def safe_update(ws, a1_range, values, user_entered=True, tag="update"):
    opt = "USER_ENTERED" if user_entered else "RAW"
    _with_retry(ws.update, range_name=a1_range, values=values, value_input_option=opt, desc=tag)
    time.sleep(PAUSE_BETWEEN_WRITES)

# =========================
# LEITURA DA ORIGEM
# =========================
def ler_origem(gc):
    print(f"📥 Lendo dados de {ID_ORIGEM}/{ABA_ORIGEM} …")
    ws_origem = _with_retry(gc.open_by_key, ID_ORIGEM, desc="open_by_key origem").worksheet(ABA_ORIGEM)
    valores  = _with_retry(ws_origem.get_all_values, desc="get_all_values zps") or []
    if not valores:
        print("⚠️ Aba 'zps' vazia.")
        sys.exit(0)

    cabecalho    = valores[0]
    linhas_raw   = valores[1:]
    num_colunas  = len(cabecalho)

    def tratar_linha(row):
        out = []
        for i in range(num_colunas):
            v = row[i] if i < len(row) and row[i] is not None else ""
            if i in COLS_NUM_IDX:
                out.append(limpar_num(v))     # float → número contável
            elif i in COLS_DATE_IDX:
                out.append(normaliza_data(v)) # string dd/mm/aaaa
            else:
                s = str(v)
                out.append(s[1:] if s.startswith("'") else s)
        return out

    linhas = [tratar_linha(r) for r in linhas_raw if any((c or "").strip() for c in r[:num_colunas])]
    all_vals = [cabecalho] + linhas
    print(f"✅ {len(linhas)} linhas preparadas.\n")
    return all_vals, num_colunas

# =========================
# ESCRITA / FORMATAÇÃO / CARIMBO
# =========================
def escrever_tudo(ws_dest, all_vals, num_colunas):
    nlin = len(all_vals)
    last_col_letter = get_last_col_letter(num_colunas)

    # grade razoável (com “rabo extra” para limpeza estável)
    ensure_grid(ws_dest, min_rows=nlin + EXTRA_TAIL_ROWS, min_cols=num_colunas)

    # hard clear opcional
    if HARD_CLEAR_BEFORE_WRITE:
        values_clear(ws_dest, f"'{ws_dest.title}'!A:{last_col_letter}", tag="values_clear A:última")

    # escrita única
    rng = f"A1:{last_col_letter}{nlin}"
    safe_update(ws_dest, rng, all_vals, user_entered=True, tag=f"update {rng}")

    # limpa “rabo” abaixo dos dados atuais
    end_clear = max(ws_dest.row_count, nlin + EXTRA_TAIL_ROWS)
    if end_clear > (nlin + 1):
        tail_rng = f"'{ws_dest.title}'!A{nlin+1}:{last_col_letter}{end_clear}"
        values_clear(ws_dest, tail_rng, tag="values_clear rabo")

def formatar_colunas(ws_dest, total_linhas, num_colunas):
    if not (APLICAR_FORMATO_DATAS or APLICAR_FORMATO_NUMEROS) or total_linhas == 0:
        return
    end_row = total_linhas + 1  # exclusivo (inclui cabeçalho)
    reqs = []
    sheet_id = ws_dest.id

    if APLICAR_FORMATO_DATAS:
        for idx in sorted(COLS_DATE_IDX):
            if idx < num_colunas:
                reqs.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": end_row,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })

    if APLICAR_FORMATO_NUMEROS:
        for idx in sorted(COLS_NUM_IDX):
            if idx < num_colunas:
                reqs.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": end_row,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })

    if reqs:
        _with_retry(ws_dest.spreadsheet.batch_update, {"requests": reqs}, desc="batch_update format")

def carimbar(ws_dest, num_colunas, nlin):
    if not CARIMBAR:
        return
    try:
        desired_letters, desired_row = a1_parse(CARIMBAR_CEL)
        desired_col_1b = col_letter_to_index(desired_letters)
    except Exception:
        desired_col_1b, desired_row = 1, 1

    try:
        max_cols = ws_dest.col_count
        max_rows = ws_dest.row_count
    except Exception:
        max_cols = num_colunas
        max_rows = max(nlin, 1)

    safe_col_1b = min(desired_col_1b, max_cols if max_cols else num_colunas)
    safe_row    = min(max(desired_row, 1), max_rows if max_rows else 1)
    safe_cell   = f"{col_index_to_letter(safe_col_1b)}{safe_row}"

    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    try:
        _with_retry(ws_dest.update, values=[[f"Atualizado em: {ts}"]],
                    range_name=safe_cell, value_input_option="RAW", desc=f"carimbar {safe_cell}")
    except Exception as e:
        print(f"⚠️ Carimbo ignorado: {e}")

# =========================
# DESTINO
# =========================
def replicar_para(gc, planilha_id: str, all_vals, num_colunas):
    print(f"➡️ Atualizando {planilha_id}/{ABA_ORIGEM} …")
    sh = _with_retry(gc.open_by_key, planilha_id, desc=f"open_by_key destino {planilha_id}")
    try:
        ws_dest = _with_retry(sh.worksheet, ABA_ORIGEM, desc=f"worksheet {ABA_ORIGEM} destino")
    except WorksheetNotFound:
        ws_dest = _with_retry(
            sh.add_worksheet,
            title=ABA_ORIGEM,
            rows=max(len(all_vals) + EXTRA_TAIL_ROWS, 1000),
            cols=max(num_colunas + 1, 26),
            desc=f"add_worksheet {ABA_ORIGEM} destino"
        )

    escrever_tudo(ws_dest, all_vals, num_colunas)
    formatar_colunas(ws_dest, len(all_vals) - 1, num_colunas)
    carimbar(ws_dest, num_colunas, len(all_vals))
    print(f"✅ Replicado {len(all_vals) - 1} linhas para {planilha_id}.")
    time.sleep(PAUSE_BETWEEN_DESTS)

def tentar_destino_ate_dar_certo(gc, planilha_id: str, all_vals, num_colunas):
    for tentativa in range(1, DESTINO_MAX_TENTATIVAS + 1):
        try:
            if tentativa > 1:
                atraso = DESTINO_BACKOFF_BASE_S * (2 ** (tentativa - 2))  # 5,10,20,40,80...
                print(f"🔁 Tentativa {tentativa}/{DESTINO_MAX_TENTATIVAS} para {planilha_id} — aguardando {atraso}s")
                time.sleep(atraso)
            replicar_para(gc, planilha_id, all_vals, num_colunas)
            return
        except Exception as e:
            print(f"❌ Falha na tentativa {tentativa} para {planilha_id}: {e}")
            if tentativa == DESTINO_MAX_TENTATIVAS:
                print(f"⛔️ Não foi possível atualizar {planilha_id} após {DESTINO_MAX_TENTATIVAS} tentativas. Abortando.")
                sys.exit(1)

# =========================
# EXECUÇÃO
# =========================
def main():
    gc = gspread.authorize(make_creds())
    all_vals, num_colunas = ler_origem(gc)
    print(f"📦 Pronto para replicar: {len(all_vals) - 1} linhas (A:{get_last_col_letter(num_colunas)}).")
    for pid in PLANILHAS_DESTINO:
        tentar_destino_ate_dar_certo(gc, pid, all_vals, num_colunas)
    print("🏁 Replicação de ZPS finalizada.")

if __name__ == "__main__":
    main()
