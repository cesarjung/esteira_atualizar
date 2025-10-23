import os
import re
import time
import random
import pandas as pd
import gspread
from datetime import datetime, date
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1

# ====== FUSO (opcional; não altera a lógica) ======
os.environ.setdefault("TZ", "America/Sao_Paulo")
try:
    import time as _t; _t.tzset()
except Exception:
    pass

# ================== FLAGS ==================
FORCAR_FORMATACAO = os.environ.get("FORCAR_FORMATACAO", "0") == "1"

# ================== CONFIG =================
ID_ORIGEM    = '18-AoLupeaUIOdkW89o6SLK6Z9d8X0dKXgdjft_daMBk'
ID_DESTINO   = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_ORIGEM   = 'Quadro Geral'
RANGE_ORIGEM = 'B17:M'     # 12 colunas (B..M)
ABA_DESTINO  = 'OPERACAO'
CAM_CRED     = 'credenciais.json'

CHUNK_ROWS      = int(os.environ.get("CHUNK_ROWS", "2000"))
MAX_RETRIES     = 6
BASE_SLEEP      = 1.0
TRANSIENT_CODES = {429, 500, 502, 503, 504}

# ================== LOG ====================
def now(): return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
def log(msg): print(f"[{now()}] {msg}", flush=True)

# ===== RETRY com backoff + jitter ==========
def _status_code(e: APIError):
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

def with_retries(fn, *args, retries=MAX_RETRIES, base_sleep=BASE_SLEEP, desc="", **kwargs):
    tent = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            tent += 1
            code = _status_code(e)
            if tent >= retries or code not in TRANSIENT_CODES:
                log(f"❌ Falhou: {desc or fn.__name__} | {e}")
                raise
            slp = min(60, base_sleep * (2 ** (tent - 1)) + random.uniform(0, 0.75))
            log(f"⚠️  HTTP {code} — retry {tent}/{retries-1} em {slp:.1f}s — passo: {desc or fn.__name__}")
            time.sleep(slp)

# ============ Helpers de planilha ==========
def ensure_capacity(ws, min_rows, min_cols):
    rows = max(ws.row_count, min_rows)
    cols = max(ws.col_count, min_cols)
    if rows != ws.row_count or cols != ws.col_count:
        log(f"🧩 Redimensionando para {rows} linhas × {cols} colunas…")
        with_retries(ws.resize, rows=rows, cols=cols, desc="resize destino")

def safe_clear(ws, a1):
    log(f"🧹 Limpando intervalo {a1}")
    with_retries(ws.batch_clear, [a1], desc=f"batch_clear {a1}")

def safe_update(ws, a1, values):
    log(f"✍️  Update em {a1} ({len(values)} linhas)")
    with_retries(ws.update, range_name=a1, values=values, value_input_option='USER_ENTERED',
                 desc=f"update {a1}")

def update_in_blocks(ws, start_row, start_col, values, block_rows=CHUNK_ROWS):
    total = len(values)
    if total == 0:
        return
    cols = len(values[0])
    i = 0
    bloco = 0
    t0 = time.time()
    while i < total:
        part = values[i:i+block_rows]
        end_row = start_row + len(part) - 1
        end_col = start_col + cols - 1
        rng = f"{rowcol_to_a1(start_row, start_col)}:{rowcol_to_a1(end_row, end_col)}"
        bloco += 1
        log(f"🚚 Enviando bloco {bloco} — {rng} ({len(part)} linhas)")
        with_retries(ws.update, values=part, range_name=rng, value_input_option='USER_ENTERED',
                     desc=f"update {rng}")
        i += len(part)
        start_row = end_row + 1
        time.sleep(0.15)  # suaviza write/min por usuário
    log(f"✅ Upload concluído em {time.time() - t0:.1f}s ({total} linhas)")

# ===== Normalização para API =====
def normalize_cell(v):
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return v.strftime("%Y-%m-%d")  # ISO (USER_ENTERED interpreta)
    return v

def to_matrix(df: pd.DataFrame):
    if df.empty:
        return []
    return [[normalize_cell(c) for c in row] for row in df.values.tolist()]

# ================== INÍCIO =================
t0 = time.time()
log("🚀 Iniciando OPERACAO")

# ---- Autenticação
log("🔐 Autenticando…")
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
cred = Credentials.from_service_account_file(CAM_CRED, scopes=scopes)
gc = gspread.authorize(cred)

# ---- Abertura
log("📂 Abrindo planilhas…")
plan_origem  = with_retries(gc.open_by_key, ID_ORIGEM,  desc="open_by_key origem")
plan_destino = with_retries(gc.open_by_key, ID_DESTINO, desc="open_by_key destino")
aba_origem   = with_retries(plan_origem.worksheet,  ABA_ORIGEM,  desc="worksheet origem")
try:
    aba_destino = with_retries(plan_destino.worksheet, ABA_DESTINO, desc="worksheet destino")
except WorksheetNotFound:
    log("🆕 Criando aba destino…")
    aba_destino = with_retries(plan_destino.add_worksheet, title=ABA_DESTINO, rows=1000, cols=14,
                               desc="add_worksheet destino")

# Garante capacidade para escrever N1 antes do status
ensure_capacity(aba_destino, min_rows=2, min_cols=14)

# ---- Status inicial
log("🏷️  Marcando status inicial em N1…")
safe_update(aba_destino, 'N1', [['Atualizando...']])

# ---- Leitura
log(f"📥 Lendo origem ({ABA_ORIGEM}!{RANGE_ORIGEM})…")
dados = with_retries(aba_origem.get, RANGE_ORIGEM, desc="get origem")
log(f"🔎 Linhas lidas (inclui cabeçalho da origem na 1ª linha): {len(dados)}")

if not dados:
    log("ℹ️  Origem vazia. Limpando A2:M e finalizando.")
    safe_clear(aba_destino, "A2:M")
    safe_update(aba_destino, 'N1', [[f'Atualizado em: {now()}']])
    raise SystemExit(0)

# ---- Tratamento: D número, E data (pula cabeçalho)
log("🧽 Tratando colunas (D valor, E data) — ignorando cabeçalho…")
for i in range(1, len(dados)):  # começa em 1 para não mexer no cabeçalho
    linha = dados[i]

    # D (índice 3 em B..M) -> número
    if len(linha) > 3:
        bruto = str(linha[3]).replace("’", "").replace("‘", "").replace("'", "")
        bruto = re.sub(r'[^\d.,-]', '', bruto)
        if ',' in bruto and '.' in bruto:
            bruto = bruto.replace('.', '').replace(',', '.')
        elif ',' in bruto:
            bruto = bruto.replace(',', '.')
        try:
            linha[3] = float(bruto) if bruto not in ("", ".", "-") else ""
        except Exception:
            linha[3] = ""

    # E (índice 4) -> data ISO (USER_ENTERED interpreta)
    if len(linha) > 4:
        valor = str(linha[4]).strip().replace("’", "").replace("‘", "").replace("'", "")
        valor = re.sub(r"[^\d/:-]", "", valor)
        linha[4] = ""
        for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
            try:
                d = datetime.strptime(valor, fmt)
                linha[4] = d.strftime("%Y-%m-%d")
                break
            except Exception:
                pass

# ---- DataFrame e normalização
log("🧱 Convertendo para DataFrame e normalizando…")
df = pd.DataFrame(dados)  # inclui cabeçalho como 1ª linha
values = to_matrix(df)
qtd_linhas = len(values)
qtd_colunas = len(values[0]) if values else 0
end_a1 = rowcol_to_a1(1, qtd_colunas).rstrip('1') if qtd_colunas else 'A'
log(f"📏 Tamanho a escrever: {qtd_linhas} linhas × {qtd_colunas} colunas (vai para A..{end_a1})")

# ---- Capacidade + Limpeza total A2:M
ensure_capacity(aba_destino, min_rows=qtd_linhas + 2, min_cols=max(14, qtd_colunas))
safe_clear(aba_destino, "A2:M")  # limpa A..M a partir da linha 2 (preserva N1)

# ---- Escrita (A2 em diante), USER_ENTERED
if qtd_linhas > 0:
    log("🚚 Escrevendo dados em blocos…")
    update_in_blocks(aba_destino, start_row=2, start_col=1, values=values, block_rows=CHUNK_ROWS)
else:
    log("⛔ Nada a escrever.")

# ---- Formatação opcional (fail-soft): D número, E data
if FORCAR_FORMATACAO and qtd_linhas > 1:
    try:
        log("🎨 Aplicando formatação opcional…")
        sheet_id = aba_destino._properties['sheetId']
        start_row_idx = 1              # linha 2 (0-based)
        end_row_idx   = 1 + qtd_linhas # exclusivo

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
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            }

        # Destino começa em A (0). D=3, E=4
        reqs = {"requests": [repeat_num(3), repeat_date(4)]}
        with_retries(aba_destino.spreadsheet.batch_update, reqs, desc="batch_update formato")
        log("✅ Formatação aplicada.")
    except APIError as e:
        log(f"⚠️  Falha na formatação opcional (seguindo mesmo assim): {e}")

# ---- Timestamp final
log("🏁 Gravando timestamp final em N1…")
safe_update(aba_destino, 'N1', [[f'Atualizado em: {now()}']])

log(f"🎉 OPERACAO concluído em {time.time() - t0:.1f}s (formatação opcional: {'ON' if FORCAR_FORMATACAO else 'OFF'})")
