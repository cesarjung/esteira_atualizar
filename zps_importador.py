import io
import os
import time
import math
import random
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# ========= CONFIG =========
SERVICE_ACCOUNT_FILE = 'credenciais.json'
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
FOLDER_ORIGEM_ID = '177E69Fo-sgAU9vvPf4LdB6M9l9wRfPhc'  # Pasta do BANCO.xlsx
SPREADSHEET_ID = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_DESTINO = 'zps'
EMPRESAS = ['SINO ELETRICIDADE LTDA', 'SIRTEC SISTEMAS ELÉTRICOS LTDA.']

# Tuning
BLOCK_ROWS = 2000     # linhas por bloco no upload
MAX_RETRIES = 6       # tentativas para chamadas HTTP
BASE_SLEEP = 1.0      # backoff base

# ========= LOG =========
def now(): return datetime.now().strftime("%H:%M:%S")
def log(msg): print(f"[{now()}] {msg}", flush=True)

# ========= RETRY =========
TRANSIENT_CODES = {429, 500, 503}

def with_retry(callable_fn, *args, desc="", **kwargs):
    attempt = 0
    while True:
        try:
            return callable_fn(*args, **kwargs)
        except HttpError as e:
            status = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
            attempt += 1
            if status in TRANSIENT_CODES and attempt < MAX_RETRIES:
                sleep_s = min(60, BASE_SLEEP * (2 ** (attempt - 1)) + random.uniform(0, 0.75))
                log(f"⚠️  HTTP {status} em {desc or callable_fn.__name__}. Retry {attempt}/{MAX_RETRIES-1} em {sleep_s:.1f}s…")
                time.sleep(sleep_s)
                continue
            log(f"❌ Falhou: {desc or callable_fn.__name__} | HTTP {status} | {e}")
            raise

# ========= AUTH =========
t0_total = time.time()
log("🔐 Autenticando serviços Drive/Sheets…")
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# ========= BUSCA DO ARQUIVO =========
log("📥 Procurando BANCO.xlsx mais recente…")
resp = with_retry(
    drive_service.files().list,
    q=f"name = 'BANCO.xlsx' and trashed = false and '{FOLDER_ORIGEM_ID}' in parents",
    spaces='drive',
    corpora='allDrives',
    fields='files(id, name, modifiedTime, size)',
    orderBy='modifiedTime desc',
    supportsAllDrives=True,
    includeItemsFromAllDrives=True,
    pageSize=1,  # pega só o mais recente
    desc="files.list(BANCO.xlsx)"
).execute()

arquivos = resp.get('files', [])
if not arquivos:
    log("❌ Arquivo BANCO.xlsx não encontrado.")
    raise SystemExit(0)

file = arquivos[0]
file_id = file['id']
tamanho = int(file.get('size', 0)) if file.get('size') else None
log(f"📄 Arquivo: {file['name']}  ID: {file_id}  Modificado: {file['modifiedTime']}  Tamanho: {f'{tamanho/1_048_576:.2f} MB' if tamanho else 'N/D'}")

# ========= DOWNLOAD =========
log("⬇️  Baixando arquivo do Drive…")
file_stream = io.BytesIO()
request = drive_service.files().get_media(fileId=file_id)
downloader = MediaIoBaseDownload(file_stream, request, chunksize=4 * 1024 * 1024)  # 4 MB

done = False
last_pct = -1
t0_dl = time.time()
while not done:
    try:
        status, done = downloader.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            if pct != last_pct:
                if tamanho:
                    bytes_done = int(status.progress() * tamanho)
                    log(f"   ↳ Progresso: {pct:3d}% ({bytes_done/1_048_576:.2f} MB de {tamanho/1_048_576:.2f} MB)")
                else:
                    log(f"   ↳ Progresso: {pct:3d}%")
                last_pct = pct
    except HttpError as e:
        code = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
        if code in TRANSIENT_CODES:
            sleep_s = min(60, BASE_SLEEP + random.uniform(0, 0.75))
            log(f"⚠️  HTTP {code} durante download. Pausando {sleep_s:.1f}s e retomando…")
            time.sleep(sleep_s)
            continue
        raise

file_stream.seek(0)
log(f"✅ Download concluído em {time.time() - t0_dl:.1f}s")

# ========= LEITURA DO EXCEL =========
log("📊 Lendo planilha Excel em memória…")
t0_read = time.time()
df = pd.read_excel(file_stream, engine='openpyxl')  # mantém compatibilidade atual
colunas_originais = df.columns
log(f"🧮 Linhas totais no arquivo: {len(df)} (leitura em {time.time() - t0_read:.1f}s)")

# ========= FILTROS =========
# Remover linhas em que a coluna X (índice 23) começa com 'TRANSP'
log("🚫 Filtrando linhas com X iniciando por 'TRANSP'…")
col_x_upper = df.iloc[:, 23].astype(str).str.strip().str.upper()
mask_transp = col_x_upper.str.startswith('TRANSP')
removidas_transp = int(mask_transp.sum())
df_sem_transp = df[~mask_transp].copy()
log(f"   ↳ Removidas: {removidas_transp} | Restantes: {len(df_sem_transp)}")

# Filtrar empresas na coluna J (índice 9)
log("🔎 Filtrando por empresas na coluna J…")
df_filtrado = df_sem_transp[df_sem_transp.iloc[:, 9].astype(str).isin(EMPRESAS)].copy()
if df_filtrado.empty:
    log("⚠️  Nenhuma linha válida após filtros. Limpando aba e saindo.")
    with_retry(
        sheets_service.spreadsheets().values().clear,
        spreadsheetId=SPREADSHEET_ID, range=ABA_DESTINO,
        desc="values.clear(vazia)"
    ).execute()
    with_retry(
        sheets_service.spreadsheets().values().update,
        spreadsheetId=SPREADSHEET_ID, range=f"{ABA_DESTINO}!K1",
        valueInputOption="USER_ENTERED",
        body={"values": [[f"Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"]]},
        desc="values.update(K1 vazio)"
    ).execute()
    raise SystemExit(0)
log(f"   ↳ Linhas após filtro de empresas: {len(df_filtrado)}")

# ========= TRATAMENTO / SELEÇÃO =========
log("🛠️ Preparando colunas de saída…")
col_E  = df_filtrado.iloc[:, 4]                  # E
col_N  = df_filtrado.iloc[:, 13].astype(str)     # N
col_Bd = col_N.str[:9]                           # B derivada de N
col_X  = df_filtrado.iloc[:, 23]                 # X
col_Y  = df_filtrado.iloc[:, 24]                 # Y
col_Z  = df_filtrado.iloc[:, 25]                 # Z
col_AA = df_filtrado.iloc[:, 26]                 # AA
col_AB = df_filtrado.iloc[:, 27]                 # AB

df_final = pd.DataFrame({
    colunas_originais[4]: col_E,
    'B': col_Bd,
    colunas_originais[23]: col_X,
    colunas_originais[24]: col_Y,
    colunas_originais[25]: col_Z,
    colunas_originais[26]: col_AA,
    colunas_originais[27]: col_AB,
})

# Colunas derivadas H e I a partir de B
df_final['H'] = df_final['B'].astype(str).str[0]
df_final['I'] = df_final['B'].astype(str).str[-7:]

# ========= LIMPEZA NA ABA =========
log("🧽 Limpando conteúdo da aba (zps)…")
with_retry(
    sheets_service.spreadsheets().values().clear,
    spreadsheetId=SPREADSHEET_ID,
    range=ABA_DESTINO,
    desc="values.clear(zps)"
).execute()

# ========= UPLOAD (em blocos) =========
log("📤 Enviando dados para a aba (em blocos)…")
valores = [df_final.columns.tolist()] + df_final.values.tolist()
total = len(valores)
if total == 0:
    log("⛔ Nada para enviar.")
else:
    # Cabeçalho
    with_retry(
        sheets_service.spreadsheets().values().update,
        spreadsheetId=SPREADSHEET_ID,
        range=f"{ABA_DESTINO}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [valores[0]]},
        desc="values.update(cabecalho)"
    ).execute()

    # Dados em blocos
    t0_up = time.time()
    i = 1
    bloco = 0
    while i < total:
        parte = valores[i:i+BLOCK_ROWS]
        start_row = i + 1   # +1 porque a primeira linha é o cabeçalho em A1
        end_row   = i + len(parte)
        range_a1 = f"{ABA_DESTINO}!A{start_row}"
        bloco += 1
        log(f"   ↳ Bloco {bloco}: linhas {start_row}..{end_row} ({len(parte)} linhas)")

        with_retry(
            sheets_service.spreadsheets().values().update,
            spreadsheetId=SPREADSHEET_ID,
            range=range_a1,
            valueInputOption="USER_ENTERED",
            body={"values": parte},
            desc=f"values.update({range_a1})"
        ).execute()
        i += len(parte)

    log(f"✅ Upload concluído em {time.time() - t0_up:.1f}s ({total-1} linhas de dados)")

# ========= TIMESTAMP =========
agora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
with_retry(
    sheets_service.spreadsheets().values().update,
    spreadsheetId=SPREADSHEET_ID,
    range=f"{ABA_DESTINO}!K1",
    valueInputOption="USER_ENTERED",
    body={"values": [[f"Atualizado em {agora}"]]},
    desc="values.update(K1 timestamp)"
).execute()

log(f"🎉 Finalizado com sucesso. Linhas enviadas: {len(df_final)}  (tempo total {time.time() - t0_total:.1f}s)")
