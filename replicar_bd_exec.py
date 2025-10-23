# replicar_bd_exec.py — A(origem)->A(dest), B(origem)->B(dest); limpa A2:B; limpa rabo; retries; sem pular destino
from datetime import datetime
import os
import re
import time
import sys
import json
import random
import pathlib
from typing import Optional, List

# ====== FUSO (opcional; não altera a lógica) ======
os.environ.setdefault("TZ", "America/Sao_Paulo")
try:
    import time as _t; _t.tzset()
except Exception:
    pass

import gspread
from google.oauth2.service_account import Credentials as SACreds
from gspread.exceptions import APIError, WorksheetNotFound

# ========= CONFIG =========
ID_ORIGEM    = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA          = 'BD_EXEC'

DESTINOS = [
    "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c",
    "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M",
    "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c",
    "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw",
]

START_COL = 'A'
END_COL   = 'B'
START_ROW = 2
CARIMBAR_CEL = 'E1'   # exige >= 5 colunas

APAGAR_ANTES_A_B        = True   # apagão em A2:B antes de colar
APLICAR_FORMATO_DATA_B  = False  # se True, força B como DATE dd/mm/yyyy
CARIMBAR                = True

# ========= TUNING (retries/backoff/pausas) =========
TRANSIENT_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 6
BASE_SLEEP  = 1.0  # s — exponencial + jitter
PAUSE_BETWEEN_WRITES = 0.12  # s
PAUSE_BETWEEN_DESTS  = 0.6   # s

DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5  # 5,10,20,40,80

# ========= CREDENCIAIS =========
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_PATH_FALLBACK = "credenciais.json"

def make_creds():
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        try:
            return SACreds.from_service_account_info(json.loads(env_json), scopes=SCOPES)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS inválido: {e}")

    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)

    script_dir = pathlib.Path(__file__).resolve().parent
    for p in (script_dir / CREDENTIALS_PATH_FALLBACK, pathlib.Path.cwd() / CREDENTIALS_PATH_FALLBACK):
        if p.is_file():
            return SACreds.from_service_account_file(str(p), scopes=SCOPES)

    raise FileNotFoundError("Credenciais não encontradas (GOOGLE_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS ou credenciais.json).")

creds = make_creds()
gc = gspread.authorize(creds)

# ========= UTILS =========
def _status_code(e: APIError):
    m = re.search(r"\[(\d+)\]", str(e))
    try:
        return int(m.group(1)) if m else None
    except Exception:
        return None

def _with_retry(fn, *args, desc=None, **kwargs):
    """Retry com backoff exponencial + jitter para erros transitórios."""
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

def _col_letter_to_index_1b(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx

def _ensure_grid(ws, min_rows: int, min_cols_letter: str):
    """Garante linhas/colunas mínimas (ex.: E1 precisa de col >= 5)."""
    min_cols = _col_letter_to_index_1b(min_cols_letter)
    cur_rows = getattr(ws, "row_count", 0)
    cur_cols = getattr(ws, "col_count", 0)
    if cur_rows < min_rows or cur_cols < min_cols:
        new_rows = max(cur_rows, min_rows)
        new_cols = max(cur_cols, min_cols)
        print(f"🧩 resize → {ws.title}: {cur_rows}x{cur_cols} -> {new_rows}x{new_cols}")
        _with_retry(ws.resize, rows=new_rows, cols=new_cols, desc=f"resize {ws.title}")

def _values_clear(ws, a1, desc="values_clear"):
    # usa endpoint do Spreadsheet (limpa sem alterar formatação)
    _with_retry(ws.spreadsheet.values_clear, a1, desc=desc)
    time.sleep(PAUSE_BETWEEN_WRITES)

def _safe_update(ws, a1, values, value_input_option="USER_ENTERED", desc="update"):
    _with_retry(ws.update, range_name=a1, values=values, value_input_option=value_input_option, desc=desc)
    time.sleep(PAUSE_BETWEEN_WRITES)

# ========= TRATAMENTO =========
def limpar_num(txt):
    if txt is None:
        return ""
    s = str(txt).strip()
    if not s:
        return ""
    if s.startswith("'"):
        s = s[1:]
    s = s.replace('R$', '').replace(' ', '')
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '.')
    s = re.sub(r'[^0-9.\-+eE]', '', s)
    try:
        return float(s) if s != "" else ""
    except Exception:
        return ""

def normaliza_data_ddmmyyyy(txt):
    if txt is None:
        return ""
    s = str(txt).strip().replace('’', '').replace('‘', '').replace("'", "")
    if not s:
        return ""
    s = re.sub(r'[^0-9/\-: ]', '', s)
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s.split(' ')[0], fmt)
            return dt.strftime("%d/%m/%Y")
        except Exception:
            continue
    return s  # USER_ENTERED tenta interpretar

def tratar_par_ab(a_raw, b_raw):
    """A(origem)→A(dest) texto; B(origem)→B(dest) data dd/mm/aaaa ou número limpo."""
    a_val = str(a_raw)[1:] if isinstance(a_raw, str) and str(a_raw).startswith("'") else a_raw
    b_fmt = normaliza_data_ddmmyyyy(b_raw)
    if b_fmt and re.match(r'^\d{2}/\d{2}/\d{4}$', b_fmt):
        b_val = b_fmt
    else:
        b_val = limpar_num(b_raw)
    return [a_val if a_val is not None else "", b_val if b_val is not None else ""]

# ========= LER FONTE via Values API =========
print(f"📥 Lendo {ID_ORIGEM}/{ABA} (A2:B) via Values API…")
book_src = _with_retry(gc.open_by_key, ID_ORIGEM, desc="open_by_key origem")
resp = _with_retry(book_src.values_get, f"{ABA}!A2:B", desc="values_get A2:B")
vals = resp.get("values", []) if isinstance(resp, dict) else (resp or [])

linhas: List[List[str]] = []
for r in vals:
    a = r[0] if len(r) > 0 else ""
    b = r[1] if len(r) > 1 else ""
    if not (str(a).strip() or str(b).strip()):
        continue
    linhas.append(tratar_par_ab(a, b))

nlin = len(linhas)
print(f"✅ {nlin} linhas preparadas.\n")

if nlin == 0:
    print("⚠️ Nada a replicar (A2:B está vazio).")
    sys.exit(0)

# ========= ESCRITA =========
def escrever_tudo(ws):
    first_row = START_ROW
    last_row  = START_ROW + nlin - 1
    rng = f"{START_COL}{first_row}:{END_COL}{last_row}"

    # Garante grade p/ dados (A..B) e também p/ carimbar E1
    _ensure_grid(ws, min_rows=max(last_row, 2), min_cols_letter='E')

    # 🔒 apagão antes (A2:B) — faixa explícita (evita “infinito”)
    if APAGAR_ANTES_A_B:
        end_clear = max(ws.row_count, last_row + 200)  # limpa um pouco além do necessário
        _values_clear(ws, f"'{ws.title}'!A2:B{end_clear}", desc='values_clear A2:B{end}')

    # escrita única
    _safe_update(ws, rng, linhas, value_input_option="USER_ENTERED", desc=f"update {rng}")

    # limpa rabo (A{last_row+1}:B{end_clear})
    end_tail = max(ws.row_count, last_row + 200)
    if end_tail >= last_row + 1:
        tail_rng = f"'{ws.title}'!A{last_row+1}:B{end_tail}"
        _values_clear(ws, tail_rng, desc='values_clear rabo A:B')

def formatar(ws):
    if not APLICAR_FORMATO_DATA_B or nlin == 0:
        return
    first_row = START_ROW
    last_row  = START_ROW + nlin - 1
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": first_row - 1,
                "endRowIndex": last_row,
                "startColumnIndex": _col_letter_to_index_1b('B') - 1,
                "endColumnIndex": _col_letter_to_index_1b('C') - 1
            },
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
            "fields": "userEnteredFormat.numberFormat"
        }
    }
    try:
        _with_retry(ws.spreadsheet.batch_update, {"requests": [req]}, desc='format B as DATE')
    except APIError as e:
        # soft-fail
        print(f"⚠️  Formatação ignorada (API instável): {e}")

def carimbar(ws):
    if not CARIMBAR:
        return
    ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    # Garante que E1 exista
    _ensure_grid(ws, min_rows=2, min_cols_letter='E')
    try:
        _safe_update(ws, CARIMBAR_CEL, [[f'Atualizado em: {ts}']], value_input_option='RAW', desc='carimbar E1')
    except APIError as e:
        print(f"⚠️  Carimbo ignorado: {e}")

def replicar_para(dest_id: str):
    print(f"➡️ Atualizando {dest_id}/{ABA} …")
    book = _with_retry(gc.open_by_key, dest_id, desc=f"open_by_key destino {dest_id}")
    try:
        ws = _with_retry(book.worksheet, ABA, desc=f"worksheet {ABA} destino")
    except WorksheetNotFound:
        ws = _with_retry(book.add_worksheet, title=ABA,
                         rows=max(START_ROW + nlin + 200, 1000), cols=10,
                         desc=f"add_worksheet {ABA} destino")

    # status
    try:
        _ensure_grid(ws, min_rows=2, min_cols_letter='E')
        _safe_update(ws, CARIMBAR_CEL, [['Atualizando...']], value_input_option='RAW', desc='status E1')
    except APIError as e:
        print(f"⚠️  Não foi possível marcar status em E1: {e}")

    escrever_tudo(ws)
    formatar(ws)
    carimbar(ws)
    print(f"✅ Replicado {nlin} linhas para {dest_id}.")
    time.sleep(PAUSE_BETWEEN_DESTS)  # alivia write/min entre planilhas

def tentar_destino_ate_dar_certo(planilha_id: str):
    for tentativa in range(1, DESTINO_MAX_TENTATIVAS + 1):
        try:
            if tentativa > 1:
                atraso = DESTINO_BACKOFF_BASE_S * (2 ** (tentativa - 2))  # 5,10,20,40,80...
                print(f"🔁 Tentativa {tentativa}/{DESTINO_MAX_TENTATIVAS} para {planilha_id} — aguardando {atraso}s")
                time.sleep(atraso)
            replicar_para(planilha_id)
            return
        except Exception as e:
            print(f"❌ Falha na tentativa {tentativa} para {planilha_id}: {e}")
            if tentativa == DESTINO_MAX_TENTATIVAS:
                print(f"⛔️ Não foi possível atualizar {planilha_id} após {DESTINO_MAX_TENTATIVAS} tentativas. Abortando.")
                sys.exit(1)

# ========= EXECUÇÃO =========
print(f"📦 Pronto para replicar: {nlin} linhas (A:B).")
for pid in DESTINOS:
    tentar_destino_ate_dar_certo(pid)
print("🏁 Replicação de BD_EXEC (A:B) finalizada.")
