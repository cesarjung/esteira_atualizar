import os
import re
import time
import random
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ========= FLAGS =========
FORCAR_FORMATACAO = os.environ.get("FORCAR_FORMATACAO", "0") == "1"
CHUNK_ROWS        = int(os.environ.get("CHUNK_ROWS", "3000"))
MAX_RETRIES       = 6

# ========= LOG =========
def now(): return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
def log(msg): print(f"[{now()}] {msg}", flush=True)

# ========= RETRY =========
def with_retry(fn, *args, desc="", base_sleep=1.0, max_retries=MAX_RETRIES, **kwargs):
    tent = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            tent += 1
            if tent >= max_retries:
                log(f"❌ Falhou: {desc or fn.__name__} | {e}")
                raise
            slp = min(60, base_sleep * (2 ** (tent - 1)) + random.uniform(0, 0.75))
            log(f"⚠️  {e} — retry {tent}/{max_retries-1} em {slp:.1f}s ({desc or fn.__name__})")
            time.sleep(slp)

# ========= HELPERS =========
def ensure_size(ws, min_rows, min_cols):
    rows = max(ws.row_count, min_rows)
    cols = max(ws.col_count, min_cols)
    if rows != ws.row_count or cols != ws.col_count:
        log(f"🧩 Redimensionando destino para {rows} linhas × {cols} colunas…")
        with_retry(ws.resize, rows, cols, desc="resize destino")

def safe_clear(ws, ranges):
    if isinstance(ranges, str):
        ranges = [ranges]
    log(f"🧹 Limpando: {', '.join(ranges)}")
    with_retry(ws.batch_clear, ranges, desc=f"batch_clear {ranges}")

def safe_update(ws, a1, values):
    log(f"✍️  Update {a1} ({len(values)} linhas)")
    with_retry(ws.update, range_name=a1, values=values, value_input_option="USER_ENTERED", desc=f"update {a1}")

def chunked_update(ws, start_row, start_col_letter, end_col_letter, values):
    # values: lista de linhas (mesmo nº de colunas)
    n = len(values)
    if n == 0:
        return
    i, bloco = 0, 0
    while i < n:
        parte = values[i:i+CHUNK_ROWS]
        a1 = f"{start_col_letter}{start_row + i}:{end_col_letter}{start_row + i + len(parte) - 1}"
        bloco += 1
        log(f"🚚 Bloco {bloco}: {a1} ({len(parte)} linhas)")
        safe_update(ws, a1, parte)
        i += len(parte)

def parse_data_br(txt):
    """Retorna 'dd/mm/yyyy' ou ''."""
    if not txt:
        return ""
    s = str(txt).strip().replace("’","").replace("‘","").replace("'","")
    # mantém apenas dígitos e separadores comuns
    s = re.sub(r"[^0-9/:\-\s]", "", s)
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s, fmt)
            return d.strftime("%d/%m/%Y")
        except Exception:
            pass
    return ""  # se não reconheceu, deixa vazio (evita lixo numérico)

# ========= CONFIG =========
log("🟢 INÍCIO importação BD_EXEC (Carteira_Planejador → F/G/H/I/K)")
log("🔐 Autenticando…")
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds  = Credentials.from_service_account_file('credenciais.json', scopes=scopes)
gc     = gspread.authorize(creds)

PLANILHA_DESTINO_ID = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_DESTINO         = "BD_EXEC"

ORIGENS = [
    "1OTHF2ytEOjGgfE49paARXkz9GjaklOQC_UhiXwUjC2E",
    "1XmpY8mqkRou-CRY68j1ljHH8W8zcROy7wnwMMSfbV7o",
    "1sGHf-zWXoxjnO20QBw2KWX39BSCzT8rzHdEz1hL7jyU",
    "1FO5tyhXygbbzSmmTGdnm45j4DD_rRFQgEheN8T8Wy70",
    "1rj2V7CxbZwkan63eCeLkH9G00Gi041IZNC6vwEgq6yI",
    "1NV0oObhLHAqnSpJKmeBBHQQxcxwlRh14TKQwO561GEw",
    "1rzT8o6XZi4v8j7CYLky3BD3sT5IPjv1PRb45ipBfbw4",
    "1oS619l3x_D1mXkvDpw8vs91G6ipZmsK83JqEIwPj7Uk",
    "1dNwj8qWTl1k92PxI9iXwaNZYITnxuKP-kOF1QnZK3Iw",
    "1gN2tR_LCuRnVCQ9tm2UURnVuMlJPVNEjvmo02TwFQCI"
]

# ========= ABERTURA DESTINO =========
log("📂 Abrindo destino…")
book_dst = with_retry(gc.open_by_key, PLANILHA_DESTINO_ID, desc="open_by_key destino")
ws_dst   = with_retry(book_dst.worksheet, ABA_DESTINO, desc="worksheet destino")
ensure_size(ws_dst, min_rows=2, min_cols=11)  # até K

# Status
safe_update(ws_dst, "E1", [["Atualizando"]])

# Cabeçalhos (uma vez só)
headers_FI = [["UNIDADE", "FIM PREVISTO", "STATUS EXECUCAO", "PROJETO"]]
header_K   = [["DATA BI"]]
safe_update(ws_dst, "F1:I1", headers_FI)
safe_update(ws_dst, "K1",   header_K)

# ========= COLETA DE DADOS =========
todos_FI = []  # linhas com 4 colunas → F..I
todas_K  = []  # 1 coluna → K
total_linhas = 0
t0 = time.time()

for idx, origem_id in enumerate(ORIGENS, 1):
    try:
        log(f"📥 [{idx}/{len(ORIGENS)}] Lendo origem {origem_id} :: 'Carteira_Planejador'…")
        book_src = with_retry(gc.open_by_key, origem_id, desc=f"open_by_key origem {idx}")
        ws_src   = with_retry(book_src.worksheet, "Carteira_Planejador", desc=f"worksheet origem {idx}")

        # Pega de A6:BI (até a coluna 61), todas as linhas
        valores = with_retry(ws_src.get, "A6:BI", desc=f"get A6:BI origem {idx}")
        log(f"   ↳ Linhas lidas: {len(valores)}")

        # Extrai M(13)->idx12, O(15)->14, P(16)->15, Q(17)->16, BI(61)->60
        for row in valores:
            m  = row[12] if len(row) > 12 else ""  # UNIDADE
            o  = row[14] if len(row) > 14 else ""  # FIM PREVISTO (data)
            p  = row[15] if len(row) > 15 else ""  # STATUS EXECUCAO
            q  = row[16] if len(row) > 16 else ""  # PROJETO
            bi = row[60] if len(row) > 60 else ""  # DATA BI (data)

            o_fmt  = parse_data_br(o)
            bi_fmt = parse_data_br(bi)

            todos_FI.append([m, o_fmt, p, q])
            todas_K.append([bi_fmt])

        total_linhas += len(valores)
        log(f"   ✅ Acumulado: {total_linhas} linhas")
    except Exception as e:
        log(f"⚠️  Falha ao processar origem {origem_id}: {e} — continuando…")
        continue

log(f"🧮 Total consolidado: {len(todos_FI)} linhas úteis")

# ========= LIMPEZA DESTINO =========
safe_clear(ws_dst, ["F2:I", "K2:K"])

# ========= UPLOAD (EM BLOCOS) =========
if todos_FI:
    chunked_update(ws_dst, start_row=2, start_col_letter="F", end_col_letter="I", values=todos_FI)
    chunked_update(ws_dst, start_row=2, start_col_letter="K", end_col_letter="K", values=todas_K)
else:
    log("⛔ Nada para escrever.")

# ========= FORMATAÇÃO OPCIONAL =========
if FORCAR_FORMATACAO and len(todos_FI) > 0:
    try:
        log("🎨 Formatação opcional em G e K…")
        sheet_id = ws_dst._properties['sheetId']
        # dados começam na linha 2 → startRowIndex=1; fim exclusivo:
        end_row_idx = 1 + len(todos_FI)

        reqs = {
            "requests": [
                # G (col 6 zero-based) e K (col 10 zero-based)
                {"repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row_idx,
                              "startColumnIndex": 6, "endColumnIndex": 7},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }},
                {"repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row_idx,
                              "startColumnIndex": 10, "endColumnIndex": 11},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }},
            ]
        }
        with_retry(ws_dst.spreadsheet.batch_update, reqs, desc="batch_update formatação")
        log("✅ Formatação aplicada.")
    except APIError as e:
        log(f"⚠️  Falha na formatação opcional (seguindo): {e}")
else:
    log("⏭️ Formatação opcional desativada.")

# ========= TIMESTAMP =========
agora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
safe_update(ws_dst, "E1", [[f"Atualizado em: {agora}"]])

log(f"🏁 FINALIZADO. Linhas processadas: {total_linhas} | tempo total {time.time():.1f}s")
