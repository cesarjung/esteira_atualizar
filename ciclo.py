# ciclo.py — v2 (no-hardclear, no pre-clear) — 2025-10-14 23:05 BRT
# 1 read (origem), 1 write (destino), 1 clear do "rabo". Sem batch_clear em colunas antes.
from datetime import datetime
import os, time, re, random, json, pathlib
from typing import Optional

import gspread
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials as SACreds

__VERSION__ = "ciclo.py v2 no-hardclear"

print(f">>> {__VERSION__} — caminho: {__file__}", flush=True)

# ========= FUSO (opcional; não altera a lógica) =========
os.environ.setdefault("TZ", "America/Sao_Paulo")
try:
    import time as _t; _t.tzset()
except Exception:
    pass

# =========================
# CONFIG
# =========================
FORCAR_FORMATACAO = os.environ.get("FORCAR_FORMATACAO", "0") == "1"

ID_ORIGEM   = '19xV_P6KIoZB9U03yMcdRb2oF_Q7gVdaukjAvE4xOvl8'
ID_DESTINO  = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_ORIGEM  = 'OBRAS GERAL'
ABA_DESTINO = 'CICLO'
INTERVALO_ORIGEM = 'A1:T'  # 20 colunas

# Destino: colar a partir de D (4) até W (4+20-1)
DEST_START_LET = 'D'
SRC_WIDTH = 20
DEST_START_NUM = 4
DEST_END_NUM   = DEST_START_NUM + SRC_WIDTH - 1
def _num_to_col(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r)+s
    return s
DEST_END_LET = _num_to_col(DEST_END_NUM)

# Credenciais
CREDENTIALS_PATH = "credenciais.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Retry
MAX_RETRIES     = 6
BASE_SLEEP      = 1.0
RETRYABLE_CODES = {429, 500, 502, 503, 504}

# =========================
# UTILS
# =========================
def agora_str(): return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
def _status_from_apierror(e: APIError) -> Optional[int]:
    m = re.search(r"\[(\d+)\]", str(e)); return int(m.group(1)) if m else None

def gs_retry(fn, *args, desc="", max_tries=MAX_RETRIES, base=BASE_SLEEP, **kw):
    tent=0
    while True:
        try:
            return fn(*args, **kw)
        except APIError as e:
            tent+=1; code=_status_from_apierror(e)
            if tent>=max_tries or (code is not None and code not in RETRYABLE_CODES):
                print(f"❌ {desc or fn.__name__}: {e}", flush=True); raise
            slp=min(30.0, base*(2**(tent-1))+random.uniform(0,0.6))
            print(f"[retry] ⚠️ {desc or fn.__name__}: {e} — retry {tent}/{max_tries-1} em {slp:.1f}s", flush=True)
            time.sleep(slp)

def make_creds():
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        return SACreds.from_service_account_info(json.loads(env_json), scopes=SCOPES)
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    script_dir = pathlib.Path(__file__).resolve().parent
    for p in (script_dir / CREDENTIALS_PATH, pathlib.Path.cwd() / CREDENTIALS_PATH):
        if p.is_file():
            return SACreds.from_service_account_file(str(p), scopes=SCOPES)
    raise FileNotFoundError("Credenciais não encontradas (GOOGLE_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS ou credenciais.json).")

# =========================
# AUTH / OPEN
# =========================
creds = make_creds()
gc = gspread.authorize(creds)
b_src = gs_retry(gc.open_by_key, ID_ORIGEM,  desc="open origem")
b_dst = gs_retry(gc.open_by_key, ID_DESTINO, desc="open destino")
w_src = gs_retry(b_src.worksheet,  ABA_ORIGEM,  desc="ws origem")
w_dst = gs_retry(b_dst.worksheet, ABA_DESTINO, desc="ws destino")

# (opcional) se houver filtro básico que atrapalhe, remove gentilmente
try: w_dst.clear_basic_filter()
except Exception: pass

# =========================
# LEITURA ORIGEM
# =========================
dados = gs_retry(w_src.get, INTERVALO_ORIGEM, desc=f"get {ABA_ORIGEM}!{INTERVALO_ORIGEM}")
if not dados:
    # nada para colar; só limpa “rabo” e timestamp
    total = w_dst.row_count or 2
    if total > 1:
        sobra = f"{DEST_START_LET}2:{DEST_END_LET}{total}"
        gs_retry(w_dst.batch_clear, [sobra], desc=f"clear vazio {sobra}")
    gs_retry(w_dst.update, range_name='Z1', values=[[f'Atualizado em {agora_str()}']], desc="stamp vazio")
    print("✅ CICLO sem dados — rabo limpo + timestamp.", flush=True)
    raise SystemExit(0)

hdr, linhas = dados[0], dados[1:]

# =========================
# NORMALIZAÇÕES (mesma lógica)
# =========================
def normalizar_data(txt):
    if not txt: return ""
    s = str(txt).strip().lstrip("'").strip()
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
    if m: return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    if re.match(r'^\d{2}/\d{2}/\d{4}$', s): return s
    m = re.match(r'^(\d{2})/(\d{2})/(\d{2})$', s)
    if m: return f"{m.group(1)}/{m.group(2)}/20{m.group(3)}"
    return s

for r in linhas:
    for idx in (10, 11, 15):  # números
        if idx < len(r):
            bruto = str(r[idx]).replace("R$", "").replace(".", "").replace(",", ".")
            bruto = re.sub(r"[^\d.\-]", "", bruto)
            try: r[idx] = float(bruto) if bruto not in ("", ".", "-") else ""
            except Exception: r[idx] = ""
    for idx in (9, 12, 14):   # datas
        if idx < len(r): r[idx] = normalizar_data(r[idx])

# =========================
# ESCRITA DESTINO (sem pré-clear, sem hard_clear)
# =========================
gs_retry(w_dst.update, range_name='Z1', values=[['Atualizando']], desc="status Z1")

# 1 única colagem em D1:W{n} (USER_ENTERED)
dest_first = f"{DEST_START_LET}1"
gs_retry(
    b_dst.values_update,
    f"{ABA_DESTINO}!{dest_first}",
    params={'valueInputOption': 'USER_ENTERED'},
    body={'values': [hdr] + linhas},
    desc="values_update COLAGEM"
)

# pós-clear: apaga só o que SOBRA abaixo do novo fim
lin_fim = len(linhas) + 1
total = w_dst.row_count or (lin_fim + 5000)
if total > lin_fim + 1:
    sobra = f"{DEST_START_LET}{lin_fim+1}:{DEST_END_LET}{total}"
    gs_retry(w_dst.batch_clear, [sobra], desc=f"post clear {sobra}")

# formatação opcional (igual à sua)
if FORCAR_FORMATACAO:
    try:
        n = len(linhas)
        if n > 0:
            sid = w_dst._properties['sheetId']
            end = n + 1
            reqs = {
                "requests": [
                    {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end, "startColumnIndex": 13, "endColumnIndex": 14},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end, "startColumnIndex": 14, "endColumnIndex": 15},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end, "startColumnIndex": 18, "endColumnIndex": 19},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end, "startColumnIndex": 12, "endColumnIndex": 13},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end, "startColumnIndex": 15, "endColumnIndex": 16},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end, "startColumnIndex": 17, "endColumnIndex": 18},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                ]
            }
            gs_retry(w_dst.spreadsheet.batch_update, reqs, desc="format opcional")
    except APIError as e:
        print(f"[AVISO] Formatação opcional falhou (segue): {e}", flush=True)

gs_retry(w_dst.update, range_name='Z1', values=[[f'Atualizado em {agora_str()}']], desc="final Z1")
print("✅ CICLO atualizado — 1 read, 1 write, 1 clear(rabo). Sem hard_clear.", flush=True)
