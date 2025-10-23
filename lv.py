# lv.py — resiliente (credenciais flexíveis), clear com range qualificado e formatação opcional

import os
import re
import time
import random
import pandas as pd
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials as SACreds
from gspread.exceptions import APIError, WorksheetNotFound

# ====== FLAG: formatação opcional (desligada por padrão) ======
FORCAR_FORMATACAO = os.environ.get("FORCAR_FORMATACAO", "0") == "1"

# ====== CONFIGURAÇÕES ======
ID_ORIGEM     = '19xV_P6KIoZB9U03yMcdRb2oF_Q7gVdaukjAvE4xOvl8'
ABA_ORIGEM    = 'LV GERAL'
RANGE_ORIGEM  = 'A:Y'   # inclui cabeçalho

ID_DESTINO    = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_DESTINO   = 'LV CICLO'
CAM_CRED      = 'credenciais.json'  # fallback local

CHUNK_ROWS    = int(os.environ.get("CHUNK_ROWS", "2000"))
MAX_RETRIES   = 6
BASE_SLEEP    = 1.1
RETRYABLE     = {429, 500, 502, 503, 504}

# ====== LOG ======
def now_str():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def log(msg):
    print(f"[{now_str()}] {msg}", flush=True)

def _status_from_apierror(e: APIError):
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

# ====== CREDENCIAIS FLEXÍVEIS ======
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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
    # fallback local
    if os.path.isfile(CAM_CRED):
        return SACreds.from_service_account_file(CAM_CRED, scopes=SCOPES)
    # tenta ao lado do script/CWD
    import pathlib
    script_dir = pathlib.Path(__file__).resolve().parent
    for p in (script_dir / CAM_CRED, pathlib.Path.cwd() / CAM_CRED):
        if p.is_file():
            return SACreds.from_service_account_file(str(p), scopes=SCOPES)
    raise FileNotFoundError("Credenciais não encontradas (GOOGLE_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS ou credenciais.json).")

# ====== RETRY COM BACKOFF + JITTER ======
def with_retry(fn, *args, max_retries=MAX_RETRIES, base_sleep=BASE_SLEEP, desc="", **kwargs):
    tent = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            tent += 1
            code = _status_from_apierror(e)
            if tent >= max_retries or (code is not None and code not in RETRYABLE):
                log(f"❌ Falhou: {desc or fn.__name__} | {e}")
                raise
            sleep_s = min(60.0, (base_sleep * (2 ** (tent - 1))) + random.uniform(0, 0.75))
            log(f"⚠️  {e} — retry {tent}/{max_retries-1} em {sleep_s:.1f}s — {desc or fn.__name__}")
            time.sleep(sleep_s)

def ensure_size(ws, rows, cols):
    rows = max(rows, 2)
    cols = max(cols, 26)  # garante coluna Z
    if ws.row_count < rows or ws.col_count < cols:
        log(f"🧩 Ajustando grade: linhas {ws.row_count}->{rows}, colunas {ws.col_count}->{cols}")
        with_retry(ws.resize, rows, cols, desc="resize destino")

def remove_basic_filter_safe(ws):
    try:
        ws.clear_basic_filter()
    except Exception:
        pass

def safe_clear(ws, a1):
    # Usa Values API com o título da aba entre aspas — evita 404 em nomes com espaço
    log(f"🧹 Limpando intervalo {a1}")
    remove_basic_filter_safe(ws)
    rng = f"'{ws.title}'!{a1}"
    with_retry(ws.spreadsheet.values_clear, rng, desc=f"values_clear {rng}")

def safe_update(ws, a1, values):
    log(f"✍️  Update em {a1} ({len(values)} linhas)")
    with_retry(ws.update, range_name=a1, values=values, value_input_option='USER_ENTERED',
               desc=f"update {a1}")

def chunked_update(ws, values, start_row=1, start_col='A', end_col='Y'):
    n = len(values)
    i = 0
    bloco = 0
    t0 = time.time()
    while i < n:
        part = values[i:i+CHUNK_ROWS]
        a1 = f"{start_col}{start_row + i}:{end_col}{start_row + i + len(part) - 1}"
        bloco += 1
        log(f"🚚 Enviando bloco {bloco} — linhas {start_row+i}..{start_row+i+len(part)-1} de {n}")
        safe_update(ws, a1, part)
        i += len(part)
    log(f"✅ Upload concluído em {time.time() - t0:.1f}s ({n} linhas)")

# ====== INÍCIO ======
import json  # após defs, pois make_creds usa json

log("🟢 INÍCIO LV CICLO")
t0_total = time.time()

# Autenticação
log("🔐 Autenticando…")
cred = make_creds()
gc = gspread.authorize(cred)

# Abertura
log("📂 Abrindo planilha origem/destino…")
book_src = with_retry(gc.open_by_key, ID_ORIGEM, desc="open_by_key origem")
book_dst = with_retry(gc.open_by_key, ID_DESTINO, desc="open_by_key destino")

try:
    ws_src = with_retry(book_src.worksheet, ABA_ORIGEM, desc="abrir ws origem")
except WorksheetNotFound:
    log("❌ Aba de origem não encontrada."); raise

try:
    ws_dst = with_retry(book_dst.worksheet, ABA_DESTINO, desc="abrir ws destino")
except WorksheetNotFound:
    log("🆕 Criando aba destino…")
    ws_dst = with_retry(book_dst.add_worksheet, title=ABA_DESTINO, rows=1000, cols=26,
                        desc="criar worksheet destino")

# Garante tamanho antes do primeiro status
ensure_size(ws_dst, ws_dst.row_count, 26)

# TIMESTAMP INICIAL
log("🏷️  Marcando status inicial em Z1…")
safe_update(ws_dst, 'Z1', [['Atualizando...']])

# LEITURA
log(f"📥 Lendo dados da origem ({ABA_ORIGEM}!{RANGE_ORIGEM})…")
dados = with_retry(ws_src.get, RANGE_ORIGEM, desc=f"get {ABA_ORIGEM}!{RANGE_ORIGEM}")
df = pd.DataFrame(dados)
log(f"🔎 Linhas lidas (inclui cabeçalho): {len(df)}")

# Garante 25 colunas (A:Y)
if df.shape[1] < 25:
    add = 25 - df.shape[1]
    log(f"➕ Normalizando colunas: adicionando {add} colunas vazias até Y")
    for _ in range(add):
        df[df.shape[1]] = ""

# TRATAMENTOS
log("🧽 Tratando colunas numéricas e data…")
num_cols = [5, 10, 19, 21, 22]  # F, K, T, V, W (0-based)
date_col = 7                     # H (0-based)

# Números (linhas a partir da 2)
for c in num_cols:
    if c < df.shape[1]:
        s = (df.iloc[1:, c].astype(str)
             .str.replace("’", "", regex=False)
             .str.replace("‘", "", regex=False)
             .str.replace("'", "", regex=False)
             .str.replace(r"[^\d,.\-]", "", regex=True)
             .str.replace(".", "", regex=False)
             .str.replace(",", ".", regex=False))
        df.iloc[1:, c] = pd.to_numeric(s, errors='coerce')

# Datas
if date_col < df.shape[1]:
    serie = (df.iloc[1:, date_col].astype(str)
             .str.replace("’", "", regex=False)
             .str.replace("‘", "", regex=False)
             .str.replace("'", "", regex=False)
             .str.replace(r"[^\d/:\-]", "", regex=True))
    dt = pd.to_datetime(serie, dayfirst=True, errors='coerce')
    df.iloc[1:, date_col] = dt.dt.strftime('%d/%m/%Y')

# Troca NaN/NaT por vazio
df = df.where(pd.notnull(df), "")

# PREPARA ESCRITA
n_rows, _ = df.shape
log(f"📏 Tamanho a escrever: {n_rows} linhas × 25 colunas (A:Y)")
ensure_size(ws_dst, n_rows, 26)  # mantém Z disponível

# Limpa A:Y (preserva Z1) — agora com range qualificado
safe_clear(ws_dst, "A:Y")

# Converte DF e envia em blocos
values = df.values.tolist()
chunked_update(ws_dst, values, start_row=1, start_col='A', end_col='Y')

# FORMATAÇÃO OPCIONAL
if FORCAR_FORMATACAO and n_rows > 1:
    try:
        log("🎨 Aplicando formatação opcional…")
        sheet_id = ws_dst._properties['sheetId']
        start_row_idx = 1       # linha 2 (dados)
        end_row_idx = n_rows    # até última linha

        def repeat_num(col_idx):
            return {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row_idx,
                        "endRowIndex": end_row_idx,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            }

        def repeat_date(col_idx):
            return {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row_idx,
                        "endRowIndex": end_row_idx,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            }

        reqs = {
            "requests": [
                repeat_num(5),   # F
                repeat_num(10),  # K
                repeat_num(19),  # T
                repeat_num(21),  # V
                repeat_num(22),  # W
                repeat_date(7),  # H
            ]
        }
        with_retry(ws_dst.spreadsheet.batch_update, reqs, desc="batch_update formato")
        log("✅ Formatação aplicada.")
    except APIError as e:
        log(f"⚠️  Falha na formatação opcional (seguindo mesmo assim): {e}")

# TIMESTAMP FINAL
log("🏁 Gravando timestamp final em Z1…")
safe_update(ws_dst, 'Z1', [[f'Atualizado em {now_str()}']])

log(f"🎉 LV CICLO concluído em {time.time() - t0_total:.1f}s  (formatação opcional: {'ON' if FORCAR_FORMATACAO else 'OFF'})")
