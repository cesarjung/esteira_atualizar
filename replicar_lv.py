# replicar_lv.py — resiliente, rápido e com números contáveis; sem pular destino

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

# ========= CONFIG =========
ID_FONTE     = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'   # planilha principal (LV CICLO pronta)
ABA_FONTE    = 'LV CICLO'
RANGE_FONTE  = 'A1:Y'   # não traz Z (reservada p/ status/timestamp)

DESTINOS = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]
ABA_DESTINO  = 'LV CICLO'

# Colunas (0-based) — do seu original:
NUM_COLS = [5, 10, 19, 21, 22]  # F, K, T, V, W
DATE_COL = 7                    # H

# Faixa fixa
N_COLS = 25  # A..Y
LAST_COL_LETTER = re.sub(r'\d+', '', rowcol_to_a1(1, N_COLS))  # 'Y'

# Opções (desligadas por padrão p/ quota/velocidade)
APLICAR_FORMATO_NUMEROS = False
APLICAR_FORMATO_DATAS   = False
CARIMBAR                = True
CARIMBAR_CEL            = 'Z1'     # se não existir, usa última coluna existente da linha 1
HARD_CLEAR_BEFORE_WRITE = False    # limpar A:Y antes de escrever (1 chamada extra)

# Retries / tuning
TRANSIENT_CODES         = {429, 500, 502, 503, 504}
MAX_RETRIES             = 6
BASE_SLEEP              = 1.0
PAUSE_BETWEEN_DESTS     = 0.6
PAUSE_BETWEEN_WRITES    = 0.10
EXTRA_TAIL_ROWS         = 200      # limpeza extra do rabo
DESTINO_MAX_TENTATIVAS  = 5
DESTINO_BACKOFF_BASE_S  = 5        # 5,10,20,40,80s

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# ========= CREDENCIAIS FLEXÍVEIS =========
def make_creds():
    env = os.environ.get('GOOGLE_CREDENTIALS')
    if env:
        return SACreds.from_service_account_info(json.loads(env), scopes=SCOPES)
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    return SACreds.from_service_account_file(pathlib.Path('credenciais.json'), scopes=SCOPES)

# ========= RETRY/UTILS =========
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

def col_letter_to_index_1b(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx  # 1-based

def col_index_to_letter_1b(index: int) -> str:
    res = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        res = chr(rem + ord('A')) + res
    return res

def a1(col_1b: int, row_1b: int) -> str:
    return f"{col_index_to_letter_1b(col_1b)}{row_1b}"

def agora() -> str:
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')

# ========= TRATAMENTO =========
def limpar_num(txt: str):
    if txt is None:
        return ""
    s = str(txt).strip()
    if not s:
        return ""
    if s.startswith("'"):
        s = s[1:]
    s = s.replace('’', '').replace('‘', '').replace("'", "")
    s = re.sub(r"[^\d,.\-+eE]", "", s)
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except Exception:
        return ""

def tratar_linha(row: List[str], n_cols: int):
    # garante n_cols e remove apóstrofo inicial
    r = [(c if c is not None else "") for c in row[:n_cols]] + [""] * max(0, n_cols - len(row))
    for i in range(n_cols):
        if isinstance(r[i], str) and r[i].startswith("'"):
            r[i] = r[i][1:]
    # números
    for c in NUM_COLS:
        if c < n_cols:
            r[c] = limpar_num(r[c])
    # data H (7) -> string dd/MM/yyyy p/ USER_ENTERED interpretar
    if DATE_COL < n_cols:
        s = str(r[DATE_COL]).strip()
        if s:
            s_norm = re.sub(r"[^0-9/\-:]", "", s)
            dt = None
            for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
                try:
                    dt = datetime.strptime(s_norm.split(' ')[0], fmt)
                    break
                except Exception:
                    continue
            if dt:
                r[DATE_COL] = dt.strftime("%d/%m/%Y")
    return r

# ========= GRADE/LIMPEZA/ESCRITA =========
def ensure_grid(ws, min_rows: int, min_cols: int):
    rows = max(ws.row_count, min_rows)
    cols = max(ws.col_count, min_cols)
    if rows != ws.row_count or cols != ws.col_count:
        print(f"🧩 resize {ws.title}: {ws.row_count}x{ws.col_count} → {rows}x{cols}")
        _with_retry(ws.resize, rows=rows, cols=cols, desc=f"resize {ws.title}")

def limpar_corpo(ws, nlin: int):
    # limpa A2:Y{end_clear}
    end_clear = max(ws.row_count, nlin + EXTRA_TAIL_ROWS)
    rng = f"'{ws.title}'!A2:{a1(col_letter_to_index_1b(LAST_COL_LETTER), end_clear)}"
    _with_retry(ws.spreadsheet.values_clear, rng, desc=f"values_clear {rng}")
    time.sleep(PAUSE_BETWEEN_WRITES)

def limpar_rabo(ws, nlin: int):
    # limpa abaixo do último registro (A{nlin+1}:Y{end_clear})
    end_clear = max(ws.row_count, nlin + EXTRA_TAIL_ROWS)
    if end_clear > (nlin + 1):
        tail = f"'{ws.title}'!A{nlin+1}:{a1(col_letter_to_index_1b(LAST_COL_LETTER), end_clear)}"
        _with_retry(ws.spreadsheet.values_clear, tail, desc=f"values_clear {tail}")
        time.sleep(PAUSE_BETWEEN_WRITES)

def escrever(ws, all_vals: List[List[str]]):
    nlin = len(all_vals)
    rng  = f"A1:{LAST_COL_LETTER}{nlin}"
    ensure_grid(ws, min_rows=nlin, min_cols=N_COLS)
    _with_retry(ws.update, range_name=rng, values=all_vals, value_input_option="USER_ENTERED", desc=f"update {rng}")
    time.sleep(PAUSE_BETWEEN_WRITES)

def carimbar(ws):
    if not CARIMBAR:
        return
    # Z1 se possível; senão usa última coluna
    desired_letter = re.match(r'^([A-Za-z]+)', CARIMBAR_CEL).group(1) if CARIMBAR_CEL else 'Z'
    desired_row = int(re.search(r'\d+$', CARIMBAR_CEL).group(0)) if CARIMBAR_CEL and re.search(r'\d+$', CARIMBAR_CEL) else 1
    desired_col_1b = col_letter_to_index_1b(desired_letter)
    max_cols = getattr(ws, "col_count", N_COLS)
    max_rows = getattr(ws, "row_count", 1)
    safe_col_1b = min(desired_col_1b, max_cols if max_cols else N_COLS)
    safe_row = min(max(desired_row, 1), max_rows if max_rows else 1)
    cell = a1(safe_col_1b, safe_row)
    ts = agora()
    _with_retry(ws.update, range_name=cell, values=[[f'Atualizado em: {ts}']], value_input_option='RAW', desc=f"carimbar {cell}")

# ========= MAIN =========
def main():
    creds = make_creds()
    gc = gspread.authorize(creds)

    # --- Ler fonte ---
    print(f"📥 Lendo {ID_FONTE}/{ABA_FONTE} ({RANGE_FONTE})…")
    sh_src = _with_retry(gc.open_by_key, ID_FONTE, desc="open_by_key fonte")
    ws_src = _with_retry(sh_src.worksheet, ABA_FONTE, desc="worksheet fonte")
    vals = _with_retry(ws_src.get, RANGE_FONTE, desc=f"get {RANGE_FONTE}") or []
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
    nlin = len(all_vals)
    print(f"✅ {len(rows)} linhas preparadas.\n")

    # --- Replicar para cada destino ---
    for i, pid in enumerate(DESTINOS, start=1):
        print(f"➡️ [{i}/{len(DESTINOS)}] Atualizando {pid}/{ABA_DESTINO} …")
        for tentativa in range(1, DESTINO_MAX_TENTATIVAS + 1):
            try:
                sh = _with_retry(gc.open_by_key, pid, desc=f"open_by_key destino {pid}")
                try:
                    ws = _with_retry(sh.worksheet, ABA_DESTINO, desc=f"worksheet {ABA_DESTINO} destino")
                except WorksheetNotFound:
                    ws = _with_retry(sh.add_worksheet, title=ABA_DESTINO,
                                     rows=max(nlin, 1000),
                                     cols=max(26, N_COLS),
                                     desc=f"add_worksheet {ABA_DESTINO} destino")

                ensure_grid(ws, min_rows=nlin + EXTRA_TAIL_ROWS, min_cols=max(26, N_COLS))

                if HARD_CLEAR_BEFORE_WRITE:
                    _with_retry(ws.spreadsheet.values_clear, f"'{ws.title}'!A:{LAST_COL_LETTER}", desc="values_clear A:Y")

                limpar_corpo(ws, nlin)
                escrever(ws, all_vals)
                limpar_rabo(ws, nlin)
                # formatação pesada desligada por padrão (ligue se necessário)
                # carimbo
                carimbar(ws)

                print(f"✅ Replicado {len(rows)} linhas para {pid}.")
                time.sleep(PAUSE_BETWEEN_DESTS)
                break
            except Exception as e:
                print(f"❌ Tentativa {tentativa}/{DESTINO_MAX_TENTATIVAS} falhou para {pid}: {e}")
                if tentativa == DESTINO_MAX_TENTATIVAS:
                    print(f"⛔️ Não foi possível atualizar {pid} após {DESTINO_MAX_TENTATIVAS} tentativas. Abortando.")
                    sys.exit(1)
                atraso = DESTINO_BACKOFF_BASE_S * (2 ** (tentativa - 1))
                print(f"⏳ Repetindo em {atraso}s…")
                time.sleep(atraso)

    print("🏁 Replicação de LV CICLO finalizada.")

if __name__ == "__main__":
    main()
