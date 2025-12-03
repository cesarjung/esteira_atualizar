# zps_importador_v2.py — robusto (Drive + Sheets), com expansão automática da grade
import io
import os
import time
import math
import random
import json
import pathlib
from datetime import datetime
from typing import Optional

import pandas as pd

# ====== FUSO (opcional; não altera a lógica) ======
os.environ.setdefault("TZ", "America/Sao_Paulo")
try:
    import time as _t; _t.tzset()
except Exception:
    pass

# ====== checagem amigável de dependências do Google API ======
try:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    from googleapiclient.errors import HttpError
except ModuleNotFoundError:
    py = r"C:\Users\Sirtec\AppData\Local\Programs\Python\Python313\python.exe"
    print(
        "\n[ERRO] O pacote 'googleapiclient' não está instalado neste Python.\n"
        "Instale com os comandos abaixo (no MESMO Python que você usa para rodar):\n\n"
        f'  "{py}" -m pip install --upgrade pip setuptools wheel\n'
        f'  "{py}" -m pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib openpyxl pandas\n'
    )
    raise

from google.oauth2.service_account import Credentials

# ========= CONFIG =========
CREDENTIALS_PATH_FALLBACK = "credenciais.json"  # usado se não houver envs
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]

FOLDER_ORIGEM_ID = "177E69Fo-sgAU9vvPf4LdB6M9l9wRfPhc"  # Pasta do BANCO.xlsx
SPREADSHEET_ID   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_DESTINO      = "zps"
EMPRESAS         = ["SINO ELETRICIDADE LTDA", "SIRTEC SISTEMAS ELÉTRICOS LTDA."]

# Tuning
BLOCK_ROWS  = 2000           # linhas por bloco de envio
BATCH_GROUP = 8              # quantos ranges acumulamos antes de dar flush
MAX_RETRIES = 6
BASE_SLEEP  = 1.0
TRANSIENT_CODES = {429, 500, 502, 503, 504}

# ========= LOG =========
def now_hms() -> str:
    return datetime.now().strftime("%H:%M:%S")

def log(msg: str):
    print(f"[{now_hms()}] {msg}", flush=True)

# ========= AUTH =========
def make_creds() -> Credentials:
    """
    Ordem:
      1) GOOGLE_CREDENTIALS (JSON inline)
      2) GOOGLE_APPLICATION_CREDENTIALS (path p/ .json)
      3) credenciais.json (ao lado do script ou no CWD)
    """
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        return Credentials.from_service_account_info(json.loads(env_json), scopes=SCOPES)

    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return Credentials.from_service_account_file(env_path, scopes=SCOPES)

    script_dir = pathlib.Path(__file__).resolve().parent
    for p in (script_dir / CREDENTIALS_PATH_FALLBACK, pathlib.Path.cwd() / CREDENTIALS_PATH_FALLBACK):
        if p.is_file():
            return Credentials.from_service_account_file(str(p), scopes=SCOPES)

    raise FileNotFoundError(
        "Credenciais não encontradas. Use GOOGLE_CREDENTIALS (JSON inline), "
        "GOOGLE_APPLICATION_CREDENTIALS (caminho do .json) ou credenciais.json."
    )

# ========= RETRY =========
def _status_http_error(e: HttpError) -> Optional[int]:
    return getattr(getattr(e, "resp", None), "status", None)

def with_retry(callable_factory, desc: str):
    """
    callable_factory: função SEM argumentos que retorna o request/operation a executar (já com .execute() quando for o caso)
    Ex.: with_retry(lambda: drive.files().list(...).execute(), "files.list")
    """
    attempt = 0
    while True:
        try:
            return callable_factory()
        except HttpError as e:
            status = _status_http_error(e)
            attempt += 1
            if status in TRANSIENT_CODES and attempt < MAX_RETRIES:
                sleep_s = min(60, BASE_SLEEP * (2 ** (attempt - 1)) + random.uniform(0, 0.75))
                log(f"⚠️  HTTP {status} em {desc} — retry {attempt}/{MAX_RETRIES-1} em {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            log(f"❌ Falhou {desc}: HTTP {status} — {e}")
            raise
        except (TimeoutError, ConnectionError) as e:
            attempt += 1
            if attempt < MAX_RETRIES:
                sleep_s = min(60, BASE_SLEEP * (2 ** (attempt - 1)) + random.uniform(0, 0.75))
                log(f"⚠️  {type(e).__name__} em {desc} — retry {attempt}/{MAX_RETRIES-1} em {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            log(f"❌ Falhou {desc}: {type(e).__name__} — {e}")
            raise

# ========= INÍCIO =========
t0_total = time.time()
log("🔐 Autenticando Drive/Sheets…")
creds = make_creds()
drive = build("drive", "v3", credentials=creds)
sheets = build("sheets", "v4", credentials=creds)

# ========= BUSCA DO ARQUIVO =========
log("📥 Procurando BANCO.xlsx mais recente…")
resp = with_retry(
    lambda: drive.files().list(
        q=f"name = 'BANCO.xlsx' and trashed = false and '{FOLDER_ORIGEM_ID}' in parents",
        spaces="drive",
        corpora="allDrives",
        fields="files(id, name, modifiedTime, size)",
        orderBy="modifiedTime desc",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        pageSize=1,
    ).execute(),
    "files.list(BANCO.xlsx)"
)
files = resp.get("files", [])
if not files:
    log("❌ Arquivo BANCO.xlsx não encontrado. Limpando aba e saindo.")
    with_retry(lambda: sheets.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=ABA_DESTINO
    ).execute(), "values.clear(vazio)")
    with_retry(lambda: sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=f"{ABA_DESTINO}!K1",
        valueInputOption="USER_ENTERED",
        body={"values": [[f"Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"]]}
    ).execute(), "values.update(K1 vazio)")
    raise SystemExit(0)

file = files[0]
file_id = file["id"]
size_bytes = int(file.get("size", 0) or 0)
log(f"📄 Arquivo: {file['name']}  ID: {file_id}  Modificado: {file['modifiedTime']}  Tamanho: {size_bytes/1_048_576:.2f} MB")

# ========= DOWNLOAD =========
log("⬇️  Baixando arquivo do Drive…")
buf = io.BytesIO()
request = drive.files().get_media(fileId=file_id)
downloader = MediaIoBaseDownload(buf, request, chunksize=4 * 1024 * 1024)

done = False
last_pct = -1
t0_dl = time.time()
while not done:
    try:
        status, done = downloader.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            if pct != last_pct:
                if size_bytes:
                    got = int(status.progress() * size_bytes)
                    log(f"   ↳ Progresso: {pct:3d}% ({got/1_048_576:.2f} MB de {size_bytes/1_048_576:.2f} MB)")
                else:
                    log(f"   ↳ Progresso: {pct:3d}%")
                last_pct = pct
    except HttpError as e:
        code = _status_http_error(e)
        if code in TRANSIENT_CODES:
            sleep_s = min(60, BASE_SLEEP + random.uniform(0, 0.75))
            log(f"⚠️  HTTP {code} durante download. Pausando {sleep_s:.1f}s e retomando…")
            time.sleep(sleep_s)
            continue
        raise

buf.seek(0)
log(f"✅ Download concluído em {time.time() - t0_dl:.1f}s")

# ========= LEITURA DO EXCEL =========
log("📊 Lendo planilha Excel em memória…")
t0_read = time.time()
# requer 'openpyxl'
df = pd.read_excel(buf, engine="openpyxl")
colunas_originais = df.columns
log(f"🧮 Linhas totais no arquivo: {len(df)} (leitura em {time.time() - t0_read:.1f}s)")

# ========= FILTROS =========
log("🚫 Removendo linhas com coluna X iniciando por 'TRANSP'…")
col_x_upper = df.iloc[:, 23].astype(str).str.strip().str.upper()
mask_transp = col_x_upper.str.startswith("TRANSP")
df_sem_transp = df[~mask_transp].copy()
log(f"   ↳ Removidas: {mask_transp.sum()} | Restantes: {len(df_sem_transp)}")

log("🔎 Filtrando por empresas na coluna J…")
df_filtrado = df_sem_transp[df_sem_transp.iloc[:, 9].astype(str).isin(EMPRESAS)].copy()
if df_filtrado.empty:
    log("⚠️  Nenhuma linha válida após filtros. Limpando aba e saindo.")
    with_retry(lambda: sheets.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=ABA_DESTINO
    ).execute(), "values.clear(vazia)")
    with_retry(lambda: sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=f"{ABA_DESTINO}!K1",
        valueInputOption="USER_ENTERED",
        body={"values": [[f"Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"]]}
    ).execute(), "values.update(K1 vazio)")
    raise SystemExit(0)

log(f"   ↳ Linhas após filtro de empresas: {len(df_filtrado)}")

# ========= TRATAMENTO / SELEÇÃO =========
log("🛠️ Preparando colunas de saída…")
col_E  = df_filtrado.iloc[:, 4]
col_N  = df_filtrado.iloc[:, 13].astype(str)
col_Bd = col_N.str[:9]

df_final = pd.DataFrame({
    colunas_originais[4]:  col_E,                 # coluna E original
    "B":                    col_Bd,               # 9 primeiros caracteres da N
    colunas_originais[23]: df_filtrado.iloc[:, 23],
    colunas_originais[24]: df_filtrado.iloc[:, 24],
    colunas_originais[25]: df_filtrado.iloc[:, 25],
    colunas_originais[26]: df_filtrado.iloc[:, 26],
    colunas_originais[27]: df_filtrado.iloc[:, 27],
})

df_final["H"] = df_final["B"].astype(str).str[0]
df_final["I"] = df_final["B"].astype(str).str[-7:]

# ========= AUX: INFO DA ABA / EXPANSÃO DE GRADE =========
def get_sheet_grid(spreadsheet_id: str, title: str):
    """
    Retorna (sheet_id, rowCount, columnCount) da aba com esse título.
    """
    info = with_retry(
        lambda: sheets.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))",
        ).execute(),
        "spreadsheets.get(gridProperties)"
    )
    for sh in info.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == title:
            gp = props.get("gridProperties", {}) or {}
            return (
                props.get("sheetId"),
                gp.get("rowCount", 0),
                gp.get("columnCount", 0),
            )
    return None, 0, 0

# ========= LIMPEZA DA ABA =========
log("🧽 Limpando conteúdo da aba (zps)…")
with_retry(
    lambda: sheets.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=ABA_DESTINO
    ).execute(),
    "values.clear(zps)"
)

# ========= UPLOAD =========
log("📤 Enviando dados para a aba (em blocos agregados)…")
valores = [df_final.columns.tolist()] + df_final.values.tolist()
if not valores:
    log("⛔ Nada para enviar.")
else:
    # ====== GARANTE GRADE SUFICIENTE NA ABA ======
    sheet_id, row_count, col_count = get_sheet_grid(SPREADSHEET_ID, ABA_DESTINO)

    linhas_necessarias = len(valores)          # cabeçalho + dados
    colunas_necessarias = len(df_final.columns)

    if sheet_id and (row_count < linhas_necessarias or col_count < colunas_necessarias):
        novo_row_count = max(row_count, linhas_necessarias + 1000)  # folga de 1000 linhas
        novo_col_count = max(col_count, colunas_necessarias)

        log(
            f"📏 Expandindo grade da aba '{ABA_DESTINO}' "
            f"de {row_count}×{col_count} para {novo_row_count}×{novo_col_count}…"
        )

        body = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "rowCount": novo_row_count,
                                "columnCount": novo_col_count,
                            },
                        },
                        "fields": "gridProperties.rowCount,gridProperties.columnCount",
                    }
                }
            ]
        }

        with_retry(
            lambda: sheets.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body=body
            ).execute(),
            "batchUpdate(expandGrid)"
        )

    # ====== CABEÇALHO ======
    with_retry(
        lambda: sheets.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{ABA_DESTINO}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [valores[0]]},
        ).execute(),
        "values.update(cabecalho)"
    )

    # ====== BLOCO A BLOCO ======
    t0_up = time.time()
    i, bloco = 1, 0
    pending_ranges = []

    def flush_batch():
        if not pending_ranges:
            return
        body = {"valueInputOption": "USER_ENTERED", "data": pending_ranges}
        with_retry(
            lambda: sheets.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body=body
            ).execute(),
            f"values.batchUpdate({len(pending_ranges)} ranges)"
        )
        pending_ranges.clear()

    total_rows = len(valores) - 1
    while i < len(valores):
        parte = valores[i:i + BLOCK_ROWS]
        start_row = i + 1
        end_row = i + len(parte)
        bloco += 1
        log(f"   ↳ Bloco {bloco}: linhas {start_row}..{end_row} ({len(parte)} linhas)")
        pending_ranges.append({
            "range": f"{ABA_DESTINO}!A{start_row}",
            "majorDimension": "ROWS",
            "values": parte
        })
        i += len(parte)
        if len(pending_ranges) >= BATCH_GROUP:
            flush_batch()
            time.sleep(0.3)

    flush_batch()
    log(f"✅ Upload concluído em {time.time() - t0_up:.1f}s ({total_rows} linhas)")

# ========= TIMESTAMP =========
agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
with_retry(
    lambda: sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{ABA_DESTINO}!K1",
        valueInputOption="USER_ENTERED",
        body={"values": [[f"Atualizado em {agora}"]]},
    ).execute(),
    "values.update(K1 timestamp)"
)

log(f"🎉 Finalizado com sucesso. Linhas enviadas: {len(df_final)}  (tempo total {time.time() - t0_total:.1f}s)")
