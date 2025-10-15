# replicar_operacao.py — resiliente, rápido e com números contáveis; formatações opcionais; sem pular destino

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

# ========== CONFIG ==========
ID_PRINCIPAL  = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'  # planilha principal
ABA_FONTE     = 'OPERACAO'
ABA_DESTINO   = 'OPERACAO'
DESTINOS = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]

# Faixa fixa deste relatório
N_COLS       = 13  # A..M
RANGE_ORIGEM = 'A1:M'

# Opções (OFF por padrão p/ performance/quota)
APLICAR_FORMATO_DATAS    = False    # E (coluna 5, índice 4)
APLICAR_FORMATO_NUMEROS  = False    # D (coluna 4, índice 3)
CARIMBAR                 = True
CARIMBAR_CEL             = 'N1'     # se não existir, cai na última coluna da linha 1
HARD_CLEAR_BEFORE_WRITE  = False    # limpa A:M antes de escrever (1 chamada extra)

# Colunas por tipo (0-based)
COL_DATA_IDX = {4}  # E
COL_NUM_IDX  = {3}  # D

# Tuning / retries
SCOPES                 = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
TRANSIENT_CODES        = {429, 500, 502, 503, 504}
MAX_RETRIES            = 6
BASE_SLEEP             = 1.0
PAUSE_BETWEEN_WRITES   = 0.10
PAUSE_BETWEEN_DESTS    = 0.6
EXTRA_TAIL_ROWS        = 200
DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5  # 5,10,20,40,80 s

# ========== CREDENCIAIS FLEXÍVEIS ==========
def make_creds():
    env = os.environ.get('GOOGLE_CREDENTIALS')
    if env:
        return SACreds.from_service_account_info(json.loads(env), scopes=SCOPES)
    env_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    return SACreds.from_service_account_file(pathlib.Path('credenciais.json'), scopes=SCOPES)

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
    a1 = rowcol_to_a1(1, n_cols)  # ex.: 'M1'
    return re.sub(r'\d+', '', a1) # 'M'

def a1_parse(cell: str):
    m = re.match(r'^([A-Za-z]+)(\d+)$', cell.strip())
    if not m:
        return 'A', 1
    return m.group(1).upper(), int(m.group(2))

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

# ========== TRATAMENTO ==========
def limpar_num(txt: str):
    """Converte strings numéricas (R$, pontos de milhar, vírgula decimal) em float; vazio -> ''."""
    if txt is None:
        return ""
    s = str(txt).strip()
    if not s:
        return ""
    if s.startswith("'"):
        s = s[1:]
    s = s.replace("R$", "").strip()
    s = re.sub(r"[^0-9,\.\-+eE]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""

def tratar_linha(row, n_cols):
    # garante exatamente n_cols células, remove apóstrofo de textos
    r = [(c if c is not None else "") for c in row[:n_cols]] + [""] * max(0, n_cols - len(row))
    # remove apóstrofo inicial
    for i in range(n_cols):
        if isinstance(r[i], str) and r[i].startswith("'"):
            r[i] = r[i][1:]
    # D (3) número
    if 3 < n_cols:
        r[3] = limpar_num(r[3])
    # E (4) data -> string 'dd/MM/yyyy' (Sheets interpreta com USER_ENTERED)
    if 4 < n_cols:
        s = str(r[4]).strip()
        if s:
            s_norm = re.sub(r"[^0-9/\-]", "", s)
            dt = None
            for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
                try:
                    from datetime import datetime as _dt
                    dt = _dt.strptime(s_norm, fmt)
                    break
                except Exception:
                    continue
            r[4] = dt.strftime("%d/%m/%Y") if dt else s
    return r

# ========== LEITURA (MASTER) ==========
def ler_fonte(gc):
    print(f"📥 Lendo {ID_PRINCIPAL}/{ABA_FONTE} ({RANGE_ORIGEM})…")
    ws_src = _with_retry(gc.open_by_key, ID_PRINCIPAL, desc="open_by_key master").worksheet(ABA_FONTE)
    vals = _with_retry(ws_src.get, RANGE_ORIGEM, desc='get A1:M') or []
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
    return all_vals

# ========== ESCRITA / FORMATAÇÃO ==========
def ensure_grid(ws, min_rows: int, min_cols: int):
    """Garante linhas/colunas suficientes para evitar Range exceeds grid limits."""
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
    except Exception:
        cur_rows = cur_cols = 0
    need_resize = (cur_rows < min_rows) or (cur_cols < min_cols)
    if need_resize:
        _with_retry(ws.resize, rows=max(cur_rows, min_rows), cols=max(cur_cols, min_cols), desc='resize')

def limpar_corpo(ws, last_col_letter: str, nlin: int):
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

def escrever(ws, last_col_letter: str, all_vals):
    nlin = len(all_vals)
    rng  = f"A1:{last_col_letter}{nlin}"
    ensure_grid(ws, min_rows=nlin + EXTRA_TAIL_ROWS, min_cols=N_COLS)

    if HARD_CLEAR_BEFORE_WRITE:
        _with_retry(ws.spreadsheet.values_clear, f"'{ws.title}'!A:{last_col_letter}", desc='values_clear A:M')
        time.sleep(PAUSE_BETWEEN_WRITES)

    _with_retry(ws.update, values=all_vals, range_name=rng,
                value_input_option="USER_ENTERED", desc='update A1:M')
    time.sleep(PAUSE_BETWEEN_WRITES)

def formatar(ws, n_rows_data: int):
    """Formatação opcional via batch_update. OFF por padrão."""
    if not (APLICAR_FORMATO_DATAS or APLICAR_FORMATO_NUMEROS) or n_rows_data == 0:
        return
    end_row = n_rows_data + 1  # exclusivo
    reqs = []
    sheet_id = ws.id
    # datas
    if APLICAR_FORMATO_DATAS:
        for idx in sorted(COL_DATA_IDX):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                                  "startColumnIndex": idx, "endColumnIndex": idx + 1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    # números
    if APLICAR_FORMATO_NUMEROS:
        for idx in sorted(COL_NUM_IDX):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                                  "startColumnIndex": idx, "endColumnIndex": idx + 1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    if reqs:
        _with_retry(ws.spreadsheet.batch_update, {"requests": reqs},
                    desc='batch_update format')

def carimbar(ws):
    if not CARIMBAR:
        return
    desired_letter, desired_row = a1_parse(CARIMBAR_CEL)
    desired_col_1b = col_letter_to_index_1b(desired_letter)

    # usa última coluna se CARIMBAR_CEL exceder grade
    try:
        max_cols = ws.col_count
        max_rows = ws.row_count
    except Exception:
        max_cols = N_COLS
        max_rows = 1

    safe_col_1b = min(desired_col_1b, max_cols if max_cols else N_COLS)
    safe_row = min(max(desired_row, 1), max_rows if max_rows else 1)
    safe_cell = f"{col_index_to_letter_1b(safe_col_1b)}{safe_row}"

    ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    _with_retry(ws.update, values=[[f'Atualizado em: {ts}']],
                range_name=safe_cell, value_input_option="RAW",
                desc=f'carimbar {safe_cell}')

# ========== DESTINO ==========
def replicar_para(gc, dest_id: str, all_vals):
    print(f"➡️ Atualizando {dest_id}/{ABA_DESTINO} …")
    last_col_letter = get_last_col_letter(N_COLS)
    sh = _with_retry(gc.open_by_key, dest_id, desc=f"open_by_key destino {dest_id}")
    try:
        ws = _with_retry(sh.worksheet, ABA_DESTINO, desc=f"worksheet {ABA_DESTINO} destino")
    except WorksheetNotFound:
        ws = _with_retry(sh.add_worksheet, title=ABA_DESTINO,
                         rows=max(len(all_vals) + EXTRA_TAIL_ROWS, 1000),
                         cols=max(N_COLS + 1, 26),
                         desc=f"add_worksheet {ABA_DESTINO} destino")

    # limpeza + escrita + rabo
    limpar_corpo(ws, last_col_letter, len(all_vals))
    escrever(ws, last_col_letter, all_vals)
    limpar_rabo(ws, last_col_letter, len(all_vals))

    # formatação opcional
    formatar(ws, len(all_vals) - 1)
    # carimbo
    carimbar(ws)
    print(f"✅ Replicado {len(all_vals) - 1} linhas para {dest_id}.")
    time.sleep(PAUSE_BETWEEN_DESTS)

def tentar_destino_ate_dar_certo(gc, planilha_id: str, all_vals):
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
    creds = make_creds()
    gc = gspread.authorize(creds)
    all_vals = ler_fonte(gc)
    print(f"📦 Pronto para replicar: {len(all_vals) - 1} linhas (A:M).")
    for dest in DESTINOS:
        tentar_destino_ate_dar_certo(gc, dest, all_vals)
    print("🏁 Processo concluído.")

if __name__ == '__main__':
    main()
