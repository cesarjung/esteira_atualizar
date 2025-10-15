# replicar_cart_plan.py — replica BD_EXEC!F:J (UNIDADE, FIM PREVISTO, STATUS EXECUCAO, PROJETO, AL)
# - Lê:   ID_ORIGEM/ABA :: F2:J  (Values API)
# - Dest: DESTINOS       :: escreve F2:J (limpa antes + limpa rabo)
# - Resiliente: retries 429/500/502/503/504, backoff exponencial + jitter
# - Garante grade até J e E1 (status)
# - Pausas leves para respeitar write/min

from datetime import datetime
import os, re, time, sys, json, random, pathlib
from typing import List, Optional

import gspread
from google.oauth2.service_account import Credentials as SACreds
from gspread.exceptions import APIError, WorksheetNotFound

# ========= CONFIG =========
ID_ORIGEM      = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA            = 'BD_EXEC'

DESTINOS = [
    "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c",
    "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M",
    "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c",
    "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw",
]

SRC_RANGE      = 'F2:J'   # 5 colunas
DST_START_COL  = 'F'
DST_END_COL    = 'J'
DST_START_ROW  = 2
CARIMBAR_CEL   = 'E1'

APAGAR_ANTES_FJ        = True
APLICAR_FORMATO_DATA_G = False  # se True, força G como DATE dd/MM/yyyy
CARIMBAR               = True

# ========= TUNING =========
TRANSIENT_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES     = 6
BASE_SLEEP      = 1.0
PAUSE_BETWEEN_WRITES = 0.12
PAUSE_BETWEEN_DESTS  = 0.6

DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5  # 5,10,20,40,80

# ========= CREDENCIAIS FLEXÍVEIS =========
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

# ========= RETRY / UTILS =========
def _status_code(e: APIError) -> Optional[int]:
    m = re.search(r"\[(\d+)\]", str(e))
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

def _col_letter_to_index_1b(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx

def _ensure_grid(ws, min_rows: int, min_cols_letter: str):
    min_cols = _col_letter_to_index_1b(min_cols_letter)
    cur_rows = getattr(ws, "row_count", 0)
    cur_cols = getattr(ws, "col_count", 0)
    if cur_rows < min_rows or cur_cols < min_cols:
        new_rows = max(cur_rows, min_rows)
        new_cols = max(cur_cols, min_cols)
        print(f"🧩 resize → {ws.title}: {cur_rows}x{cur_cols} -> {new_rows}x{new_cols}")
        _with_retry(ws.resize, rows=new_rows, cols=new_cols, desc=f"resize {ws.title}")

def _values_get(book, a1):
    resp = _with_retry(book.values_get, a1, desc=f"values_get {a1}")
    return resp.get("values", []) if isinstance(resp, dict) else (resp or [])

def _values_clear(ws, a1, desc="values_clear"):
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
            from datetime import datetime as _dt
            dt = _dt.strptime(s.split(' ')[0], fmt)
            return dt.strftime("%d/%m/%Y")
        except Exception:
            continue
    return s  # USER_ENTERED tenta interpretar

def tratar_row_fghij(r5):
    """
    Entrada: lista de 5 valores (F,G,H,I,J).
    - Remove apóstrofo inicial em F,H,I,J.
    - G: tenta data (dd/mm/aaaa); senão número limpo.
    """
    r = (r5 + [""] * 5)[:5]
    for idx in (0, 2, 3, 4):  # F,H,I,J
        if isinstance(r[idx], str) and r[idx].startswith("'"):
            r[idx] = r[idx][1:]
    g_raw = r[1]
    g_fmt = normaliza_data_ddmmyyyy(g_raw)
    r[1] = g_fmt if g_fmt and re.match(r'^\d{2}/\d{2}/\d{4}$', g_fmt) else limpar_num(g_raw)
    return r

# ========= LER FONTE (Values API) =========
print(f"📥 Lendo {ID_ORIGEM}/{ABA} ({SRC_RANGE}) via Values API…")
book_src = _with_retry(gc.open_by_key, ID_ORIGEM, desc="open_by_key origem")
vals = _values_get(book_src, f"{ABA}!{SRC_RANGE}")  # lista de linhas

linhas: List[List[str]] = []
for r in vals:
    r5 = (r + [""] * 5)[:5]
    if not any((str(c or "").strip() for c in r5)):
        continue
    linhas.append(tratar_row_fghij(r5))

nlin = len(linhas)
print(f"✅ {nlin} linhas preparadas.\n")

if nlin == 0:
    print("⚠️ Nada a replicar (F2:J está vazio).")
    sys.exit(0)

# ========= ESCRITA =========
def escrever_tudo(ws):
    first_row = DST_START_ROW
    last_row  = DST_START_ROW + nlin - 1
    rng = f"{DST_START_COL}{first_row}:{DST_END_COL}{last_row}"

    # Garante grade p/ dados (até J) e p/ status (E1)
    _ensure_grid(ws, min_rows=max(last_row, 2), min_cols_letter='J')
    _ensure_grid(ws, min_rows=2, min_cols_letter='E')

    # apagão antes F:J com faixa explícita (evita “infinito”)
    if APAGAR_ANTES_FJ:
        end_clear = max(ws.row_count, last_row + 200)
        _values_clear(ws, f"'{ws.title}'!{DST_START_COL}{DST_START_ROW}:{DST_END_COL}{end_clear}",
                      desc='values_clear F2:J{end}')

    # escrita única
    _safe_update(ws, rng, linhas, value_input_option="USER_ENTERED", desc=f"update {rng}")

    # limpa rabo (linhas abaixo do último)
    end_tail = max(ws.row_count, last_row + 200)
    if end_tail >= last_row + 1:
        tail_rng = f"'{ws.title}'!{DST_START_COL}{last_row+1}:{DST_END_COL}{end_tail}"
        _values_clear(ws, tail_rng, desc='values_clear rabo F:J')

def formatar(ws):
    if not APLICAR_FORMATO_DATA_G or nlin == 0:
        return
    first_row = DST_START_ROW
    last_row  = DST_START_ROW + nlin - 1
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": first_row - 1,
                "endRowIndex": last_row,
                "startColumnIndex": _col_letter_to_index_1b('G') - 1,
                "endColumnIndex": _col_letter_to_index_1b('H') - 1
            },
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
            "fields": "userEnteredFormat.numberFormat"
        }
    }
    try:
        _with_retry(ws.spreadsheet.batch_update, {"requests": [req]}, desc='format G as DATE')
    except APIError as e:
        print(f"⚠️  Formatação ignorada (API instável): {e}")

def carimbar(ws):
    if not CARIMBAR:
        return
    ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
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
                         rows=max(DST_START_ROW + nlin + 200, 1000), cols=20,
                         desc=f"add_worksheet {ABA} destino")

    # status inicial
    try:
        _ensure_grid(ws, min_rows=2, min_cols_letter='E')
        _safe_update(ws, CARIMBAR_CEL, [['Atualizando...']], value_input_option='RAW', desc='status E1')
    except APIError as e:
        print(f"⚠️  Não foi possível marcar status em E1: {e}")

    escrever_tudo(ws)
    formatar(ws)
    carimbar(ws)
    print(f"✅ Replicado {nlin} linhas para {dest_id}.")
    time.sleep(PAUSE_BETWEEN_DESTS)

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
print(f"📦 Pronto para replicar: {nlin} linhas (F:J).")
for pid in DESTINOS:
    tentar_destino_ate_dar_certo(pid)
print("🏁 Replicação de BD_EXEC (F:J) finalizada.")
