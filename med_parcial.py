import os
import re
import time
import random
import pandas as pd
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ================== FLAGS ==================
FORCAR_FORMATACAO = os.environ.get("FORCAR_FORMATACAO", "0") == "1"

# ================== CONFIG =================
ID_PLANILHA_ORIGEM  = "19xV_P6KIoZB9U03yMcdRb2oF_Q7gVdaukjAvE4xOvl8"
ID_PLANILHA_DESTINO = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM          = "MED PARCIAIS GERAL"
ABA_DESTINO         = "MED PARCIAL"
CAMINHO_CREDENCIAIS = "credenciais.json"

CHUNK_ROWS  = 2000
MAX_RETRIES = 6

# ================== LOG ====================
def now(): return datetime.now().strftime("%H:%M:%S")
def log(msg): print(f"[{now()}] {msg}", flush=True)

# ===== RETRY com backoff + jitter ==========
def with_retry(func, *args, retries=MAX_RETRIES, base=1.0, desc="", **kwargs):
    tent = 0
    while True:
        try:
            return func(*args, **kwargs)
        except APIError as e:
            tent += 1
            if tent >= retries:
                log(f"❌ Falhou: {desc or func.__name__} | {e}")
                raise
            sleep_s = (base * (2 ** (tent - 1))) + random.uniform(0, 0.75)
            log(f"⚠️  {e}. Retentativa {tent}/{retries-1} em {sleep_s:.1f}s — passo: {desc or func.__name__}")
            time.sleep(min(60, sleep_s))

# ============ Helpers de planilha ==========
def ensure_size(ws, rows, cols):
    rows = max(rows, 2)      # pelo menos 2 linhas
    cols = max(cols, 18)     # R1 exige ao menos 18 colunas (A..R)
    if ws.row_count < rows or ws.col_count < cols:
        log(f"🧩 Ajustando grade: linhas {ws.row_count}->{rows}, colunas {ws.col_count}->{cols}")
        with_retry(ws.resize, rows, cols, desc="resize destino")

def safe_clear(ws, a1):
    log(f"🧹 Limpando intervalo {a1}")
    with_retry(ws.batch_clear, [a1], desc=f"batch_clear {a1}")

def safe_update(ws, a1, values):
    log(f"✍️  Update em {a1} ({len(values)} linhas)")
    with_retry(ws.update, range_name=a1, values=values, value_input_option="USER_ENTERED",
               desc=f"update {a1}")

def chunked_update(ws, values, start_row=1, start_col='A', end_col='P'):
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

# ================== INÍCIO =================
inicio = time.time()
log("🚀 Iniciando MED PARCIAL")

# ---- Autenticação
log("🔐 Autenticando no Google…")
escopos = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credenciais = Credentials.from_service_account_file(CAMINHO_CREDENCIAIS, scopes=escopos)
gc = gspread.authorize(credenciais)

# ---- Abertura
log(f"📂 Abrindo origem {ID_PLANILHA_ORIGEM} / '{ABA_ORIGEM}' e destino {ID_PLANILHA_DESTINO} / '{ABA_DESTINO}'…")
planilha_origem  = with_retry(gc.open_by_key, ID_PLANILHA_ORIGEM, desc="open_by_key origem")
planilha_destino = with_retry(gc.open_by_key, ID_PLANILHA_DESTINO, desc="open_by_key destino")

aba_origem  = with_retry(planilha_origem.worksheet,  ABA_ORIGEM,  desc="worksheet origem")
try:
    aba_destino = with_retry(planilha_destino.worksheet, ABA_DESTINO, desc="worksheet destino")
    ensure_size(aba_destino, aba_destino.row_count, 18)   # Garante R1
except gspread.WorksheetNotFound:
    log("🆕 Criando aba destino…")
    aba_destino = with_retry(planilha_destino.add_worksheet, title=ABA_DESTINO, rows=1000, cols=18,
                             desc="criar worksheet destino")

# ---- Status inicial
log("🏷️  Status inicial em R1…")
safe_update(aba_destino, "R1", [["Atualizando..."]])

# ---- Leitura
log("📥 Lendo dados da origem (A1:P)…")
dados_origem = with_retry(aba_origem.get, "A1:P", desc="get origem")
if not dados_origem:
    log("❌ Sem dados na origem. Abortando.")
    safe_update(aba_destino, "R1", [["Sem dados na origem"]])
    raise SystemExit(0)

cabecalho = dados_origem[0]
dados     = dados_origem[1:]
log(f"🔎 Linhas carregadas (sem cabeçalho): {len(dados)}")

# ---- Tratamento numérico (F e J na origem)
log("🧽 Limpando valores numéricos (F,J)…")
def limpar_valor(valor):
    if isinstance(valor, str):
        valor = re.sub(r"[^\d,.\-]", "", valor)
        valor = valor.replace(".", "").replace(",", ".")
    try:
        return float(valor)
    except Exception:
        return "" if (isinstance(valor, float) and pd.isna(valor)) else valor

for linha in dados:
    if len(linha) >= 6:
        linha[5] = limpar_valor(linha[5])   # F (origem)
    if len(linha) >= 10:
        linha[9] = limpar_valor(linha[9])   # J (origem)

# ---- Coluna A: PROJETO CORRIGIDO (9 primeiros de B)
log("🧮 Montando A: PROJETO CORRIGIDO…")
projetos_corrigidos = [["PROJETO CORRIGIDO"]]
projetos_corrigidos += [[(linha[1] or "")[:9]] if len(linha) > 1 else [""] for linha in dados]

limite_linhas = len(dados) + 1
log(f"📏 Tamanho a escrever: {limite_linhas} linhas × 16 colunas (B:P) + A")
ensure_size(aba_destino, limite_linhas, 18)

# === Limpeza completa (A:P) para não sobrar resíduos de execuções anteriores ===
safe_clear(aba_destino, "A:P")

# ---- Escreve A1:A{n}
log(f"📤 Colando A1:A{limite_linhas}…")
chunked_update(aba_destino, projetos_corrigidos, start_row=1, start_col="A", end_col="A")

# ---- Preparar dados B..P (remove col A da origem)
dados_completo = [linha[1:] for linha in [cabecalho] + dados]

# ---- Colar B1:P{n}
intervalo_destino = f"B1:P{limite_linhas}"
log(f"📤 Colando {intervalo_destino} (USER_ENTERED)…")
chunked_update(aba_destino, dados_completo, start_row=1, start_col="B", end_col="P")

# ---- Formatação opcional (fail-soft)
if FORCAR_FORMATACAO and limite_linhas > 1:
    try:
        log("🎨 Aplicando formatação opcional…")
        sheet_id = aba_destino._properties['sheetId']
        start_row_idx = 1          # a partir da linha 2
        end_row_idx   = limite_linhas

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

        # Destino: B..P  -> índices: B=1 ... P=15
        reqs = {
            "requests": [
                repeat_num(6),   # G (valor a receber)
                repeat_num(10),  # K (valor faturado)
                repeat_date(7),  # H (data)
                repeat_date(9),  # J (data)
            ]
        }
        with_retry(aba_destino.spreadsheet.batch_update, reqs, desc="batch_update formato")
        log("✅ Formatação aplicada.")
    except APIError as e:
        log(f"⚠️  Falha na formatação opcional (seguindo mesmo assim): {e}")

# ---- Timestamp final
agora_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
log(f"🕒 Gravando timestamp final em R1…")
safe_update(aba_destino, "R1", [[f"Atualizado em: {agora_str}"]])

log(f"🏁 Concluído em {time.time() - inicio:.1f}s — MED PARCIAL OK (formatação opcional: {'ON' if FORCAR_FORMATACAO else 'OFF'})")
