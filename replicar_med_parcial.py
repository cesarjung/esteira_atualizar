# replicar_med_parcial.py — resiliente, sem pular destino; cria aba se faltar; retries/backoff; carimbo seguro

from datetime import datetime
import os, json, pathlib
import re
import time
import sys
import random
from typing import List

import gspread
from google.oauth2.service_account import Credentials as SACreds
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1

# ========== CONFIG ==========
ID_MASTER   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"  # fonte onde "MED PARCIAL" está atualizada
ABA         = "MED PARCIAL"

PLANILHAS_DESTINO = [
    "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c",
    "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M",
    "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c",
    "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw",
]

# faixa fixa: A..Q (17 colunas)
N_COLS       = 17
RANGE_ORIGEM = "A1:Q"

# Opções (OFF por padrão p/ performance/quota)
APLICAR_FORMATO_DATAS    = False   # ligue se quiser dd/MM/yyyy nas colunas de data (cosmético)
APLICAR_FORMATO_NUMEROS  = False   # ligue se quiser #,##0.00 nas colunas numéricas (cosmético)
CARIMBAR                 = True
CARIMBAR_CEL             = "R1"    # se não existir, cai na última coluna existente (linha 1)
HARD_CLEAR_BEFORE_WRITE  = False   # limpa A:Q antes de escrever (1 chamada extra)

# Colunas a tratar (0-based). Seu script tratava F (5) e formatava G (6) e K (10).
COLS_NUM_IDX  = {5, 6, 10}   # F, G, K como números
COLS_DATE_IDX = set()        # adicione índices de datas se precisar

# Tuning / retries
SCOPES                 = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
TRANSIENT_CODES        = {429, 500, 502, 503, 504}
MAX_RETRIES            = 6
BASE_SLEEP             = 1.0
PAUSE_BETWEEN_WRITES   = 0.10
PAUSE_BETWEEN_DESTS    = 0.6
EXTRA_TAIL_ROWS        = 200
DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5   # 5,10,20,40,80 s

# ========== CREDENCIAIS FLEXÍVEIS ==========
def make_creds():
    env = os.environ.get("GOOGLE_CREDENTIALS")
    if env:
        return SACreds.from_service_account_info(json.loads(env), scopes=SCOPES)
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    return SACreds.from_service_account_file(pathlib.Path("credenciais.json"), scopes=SCOPES)

# ========== RETRY / UTILS ==========
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

def get_last_col_letter(n_cols: int) -> str:
    a1 = rowcol_to_a1(1, n_cols)  # ex.: 'Q1'
    return re.sub(r"\d+", "", a1) # 'Q'

def a1_parse(cell: str):
    m = re.match(r"^([A-Za-z]+)(\d+)$", cell.strip())
    if not m:
        return "A", 1
    return m.group(1).upper(), int(m.group(2))

def col_letter_to_index_1b(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx

def col_index_to_letter_1b(index: int) -> str:
    res = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        res = chr(rem + ord('A')) + res
    return res

def a1(col_1b: int, row_1b: int) -> str:
    return f"{col_index_to_letter_1b(col_1b)}{row_1b}"

def agora() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

# ========== TRATAMENTO ==========
def limpar_valor(valor):
    """Converte strings em float (R$, pontos de milhar, vírgula decimal). Se não der, retorna ''."""
    if valor is None:
        return ""
    s = str(valor).strip()
    if not s:
        return ""
    if s.startswith("'"):
        s = s[1:]
    s = re.sub(r"[^\d,.\-+eE]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""

def tratar_linha(row: List[str], n_cols: int):
    # garante exatamente n_cols células + remove apóstrofo inicial
    r = [(c if c is not None else "") for c in row[:n_cols]] + [""] * max(0, n_cols - len(row))
    for i in range(n_cols):
        if isinstance(r[i], str) and r[i].startswith("'"):
            r[i] = r[i][1:]
    # força números nas colunas configuradas
    for idx in COLS_NUM_IDX:
        if idx < n_cols:
            r[idx] = limpar_valor(r[idx])
    # (se houver datas, normalize aqui e/ou use APLICAR_FORMATO_DATAS para batch_update)
    return r

# ========== LEITURA MASTER ==========
def ler_master():
    creds = make_creds()
    gc = gspread.authorize(creds)
    print(f"📥 Lendo {ID_MASTER}/{ABA} ({RANGE_ORIGEM})…")
    ws_src = _with_retry(gc.open_by_key, ID_MASTER, desc="open_by_key master").worksheet(ABA)
    vals = _with_retry(ws_src.get, RANGE_ORIGEM, desc="get A1:Q") or []
    if not vals:
        print("⚠️ Nada a replicar (faixa vazia).")
        sys.exit(0)
    header = (vals[0] + [""] * N_COLS)[:N_COLS]
    rows_raw = vals[1:]
    rows = []
    for r in rows_raw:
        if not any((c or "").strip() for c in r[:N_COLS]):  # ignora totalmente vazias
            continue
        rows.append(tratar_linha(r, N_COLS))
    all_vals = [header] + rows
    print(f"✅ {len(rows)} linhas preparadas.\n")
    return gc, all_vals

# ========== ESCRITA / FORMATAÇÃO ==========
def ensure_grid(ws, min_rows: int, min_cols: int):
    rows = max(ws.row_count, min_rows)
    cols = max(ws.col_count, min_cols)
    if rows != ws.row_count or cols != ws.col_count:
        print(f"🧩 resize {ws.title}: {ws.row_count}x{ws.col_count} → {rows}x{cols}")
        _with_retry(ws.resize, rows=rows, cols=cols, desc=f"resize {ws.title}")

def limpar_corpo(ws, last_col_letter: str, nlin: int):
    # limpa A2:Q{end_clear} (ou onde for o last_col_letter)
    end_clear = max(ws.row_count, nlin + EXTRA_TAIL_ROWS)
    rng = f"'{ws.title}'!A2:{last_col_letter}{end_clear}"
    _with_retry(ws.spreadsheet.values_clear, rng, desc=f"values_clear {rng}")
    time.sleep(PAUSE_BETWEEN_WRITES)

def limpar_rabo(ws, last_col_letter: str, nlin: int):
    end_clear = max(ws.row_count, nlin + EXTRA_TAIL_ROWS)
    if end_clear > (nlin + 1):
        tail = f"'{ws.title}'!A{nlin+1}:{last_col_letter}{end_clear}"
        _with_retry(ws.spreadsheet.values_clear, tail, desc=f"values_clear {tail}")
        time.sleep(PAUSE_BETWEEN_WRITES)

def escrever(ws, last_col_letter: str, all_vals: List[List[str]]):
    nlin = len(all_vals)
    rng  = f"A1:{last_col_letter}{nlin}"
    ensure_grid(ws, min_rows=nlin + EXTRA_TAIL_ROWS, min_cols=N_COLS)
    if HARD_CLEAR_BEFORE_WRITE:
        _with_retry(ws.spreadsheet.values_clear, f"'{ws.title}'!A:{last_col_letter}", desc=f"values_clear A:{last_col_letter}")
        time.sleep(PAUSE_BETWEEN_WRITES)
    _with_retry(ws.update, range_name=rng, values=all_vals, value_input_option="USER_ENTERED", desc=f"update {rng}")
    time.sleep(PAUSE_BETWEEN_WRITES)

def formatar(ws, last_col_letter: str, n_rows_data: int):
    if not (APLICAR_FORMATO_DATAS or APLICAR_FORMATO_NUMEROS) or n_rows_data == 0:
        return
    end_row = n_rows_data + 1  # exclusivo (cabeçalho + n_rows)
    reqs = []
    sid = ws.id

    if APLICAR_FORMATO_NUMEROS:
        for idx in sorted(COLS_NUM_IDX):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row,
                                  "startColumnIndex": idx, "endColumnIndex": idx + 1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    if APLICAR_FORMATO_DATAS and COLS_DATE_IDX:
        for idx in sorted(COLS_DATE_IDX):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row,
                                  "startColumnIndex": idx, "endColumnIndex": idx + 1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    if reqs:
        _with_retry(ws.spreadsheet.batch_update, {"requests": reqs}, desc="batch_update format")

def carimbar(ws):
    if not CARIMBAR:
        return
    desired_letter, desired_row = a1_parse(CARIMBAR_CEL)
    desired_col_1b = col_letter_to_index_1b(desired_letter)
    max_cols = getattr(ws, "col_count", N_COLS)
    max_rows = getattr(ws, "row_count", 1)
    safe_col_1b = min(desired_col_1b, max_cols if max_cols else N_COLS)
    safe_row = min(max(desired_row, 1), max_rows if max_rows else 1)
    cell = a1(safe_col_1b, safe_row)
    ts = agora()
    _with_retry(ws.update, range_name=cell, values=[[f"Atualizado em: {ts}"]], value_input_option="RAW", desc=f"carimbar {cell}")

# ========== DESTINO ==========
def replicar_para(gc, planilha_id: str, all_vals: List[List[str]]):
    print(f"➡️ Atualizando {planilha_id}/{ABA} …")
    last_col_letter = get_last_col_letter(N_COLS)
    try:
        sh = _with_retry(gc.open_by_key, planilha_id, desc=f"open_by_key destino {planilha_id}")
        try:
            ws = _with_retry(sh.worksheet, ABA, desc=f"worksheet {ABA} destino")
        except WorksheetNotFound:
            ws = _with_retry(sh.add_worksheet, title=ABA,
                             rows=max(len(all_vals) + EXTRA_TAIL_ROWS, 1000),
                             cols=max(N_COLS + 1, 26),
                             desc=f"add_worksheet {ABA} destino")
        # garantir grade + limpeza + escrita
        limpar_corpo(ws, last_col_letter, len(all_vals))
        escrever(ws, last_col_letter, all_vals)
        limpar_rabo(ws, last_col_letter, len(all_vals))
        # formatação opcional
        formatar(ws, last_col_letter, len(all_vals) - 1)
        # carimbo
        carimbar(ws)
        print(f"✅ Replicado {len(all_vals) - 1} linhas para {planilha_id}.")
    finally:
        time.sleep(PAUSE_BETWEEN_DESTS)

def tentar_destino_ate_dar_certo(gc, planilha_id: str, all_vals: List[List[str]]):
    for tentativa in range(1, DESTINO_MAX_TENTATIVAS + 1):
        try:
            if tentativa > 1:
                atraso = DESTINO_BACKOFF_BASE_S * (2 ** (tentativa - 2))  # 5,10,20,40,80...
                print(f"🔁 Tentativa {tentativa}/{DESTINO_MAX_TENTATIVAS} para {planilha_id} — aguardando {atraso}s")
                time.sleep(atraso)
            replicar_para(gc, planilha_id, all_vals)
            return  # sucesso
        except Exception as e:
            print(f"❌ Falha na tentativa {tentativa} para {planilha_id}: {e}")
            if tentativa == DESTINO_MAX_TENTATIVAS:
                print(f"⛔️ Não foi possível atualizar {planilha_id} após {DESTINO_MAX_TENTATIVAS} tentativas. Abortando.")
                sys.exit(1)

# ========== EXECUÇÃO ==========
def main():
    gc, all_vals = ler_master()
    print(f"📦 Pronto para replicar: {len(all_vals) - 1} linhas (A:Q).")
    for pid in PLANILHAS_DESTINO:
        tentar_destino_ate_dar_certo(gc, pid, all_vals)
    print("🏁 Processo concluído.")

if __name__ == "__main__":
    main()
