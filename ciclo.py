# ciclo.py
from datetime import datetime
import os
import time
import re
import random

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# =========================
# FLAGS / CONFIG
# =========================
FORCAR_FORMATACAO = os.environ.get("FORCAR_FORMATACAO", "0") == "1"

ID_ORIGEM = '19xV_P6KIoZB9U03yMcdRb2oF_Q7gVdaukjAvE4xOvl8'
ID_DESTINO = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_ORIGEM = 'OBRAS GERAL'
ABA_DESTINO = 'CICLO'
INTERVALO_ORIGEM = 'A1:T'
CAMINHO_CREDENCIAIS = 'credenciais.json'

# limites de chunk
CHUNK_CLEAR_COLS = 4         # limpar valores em blocos de 4 colunas (batch_clear)
CHUNK_HARD_ROWS  = 5000      # hard-clear por grupos de linhas
MAX_RETRIES      = 6
BASE_SLEEP       = 1.1
RETRYABLE_CODES  = {429, 500, 502, 503, 504}

# =========================
# Helpers p/ colunas
# =========================
def col_to_num(col: str) -> int:
    n = 0
    for c in col:
        n = n * 26 + (ord(c.upper()) - 64)
    return n

def num_to_col(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

DEST_START_LET = 'D'
DEST_START_NUM = col_to_num(DEST_START_LET)

m = re.search(r':([A-Z]+)$', INTERVALO_ORIGEM)
SRC_END_LET = m.group(1) if m else 'T'
SRC_WIDTH = col_to_num(SRC_END_LET) - col_to_num('A') + 1

DEST_END_NUM = DEST_START_NUM + SRC_WIDTH - 1
DEST_END_LET = num_to_col(DEST_END_NUM)
CLEAR_RANGE = f'{DEST_START_LET}:{DEST_END_LET}'  # ex.: D:W

# =========================
# AUTENTICAÇÃO
# =========================
escopos = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
credenciais = Credentials.from_service_account_file(CAMINHO_CREDENCIAIS, scopes=escopos)
cliente = gspread.authorize(credenciais)

# =========================
# UTIL
# =========================
def _status_from_apierror(e: APIError):
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

def gs_retry(fn, *args, max_tries=MAX_RETRIES, base_sleep=BASE_SLEEP, desc="", **kwargs):
    tentativa = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            tentativa += 1
            code = _status_from_apierror(e)
            if tentativa >= max_tries or (code is not None and code not in RETRYABLE_CODES):
                raise
            slp = min(60.0, (base_sleep * (2 ** (tentativa - 1))) + random.uniform(0, 0.75))
            if desc:
                print(f"[retry] ⚠️ {desc}: {e} — retry {tentativa}/{max_tries-1} em {slp:.1f}s", flush=True)
            time.sleep(slp)

def agora_str():
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')

# =========================
# Limpeza estável (valores)
# =========================
def remove_basic_filter_safe(wks):
    try:
        wks.clear_basic_filter()
    except Exception:
        pass

def chunked_ranges_by_cols(start_col, end_col, start_row, end_row, chunk_cols=CHUNK_CLEAR_COLS):
    c = start_col
    while c <= end_col:
        c_end = min(end_col, c + chunk_cols - 1)
        col_a = num_to_col(c)
        col_b = num_to_col(c_end)
        rng = f"{col_a}{start_row}:{col_b}{end_row}"
        yield rng
        c = c_end + 1

def clear_values_chunked(wks, start_col, end_col, start_row=2, end_row=None, chunk_cols=CHUNK_CLEAR_COLS):
    """
    Limpa apenas VALORES em blocos de colunas, usando batch_clear([range]).
    Equivale à 1ª camada da sua lógica.
    """
    if end_row is None:
        end_row = max(wks.row_count, 1000)
    remove_basic_filter_safe(wks)
    for rng in chunked_ranges_by_cols(start_col, end_col, start_row, end_row, chunk_cols):
        gs_retry(wks.batch_clear, [rng], desc=f"batch_clear {rng}")

# =========================
# Hard clear adaptativo (userEnteredValue)
# =========================
def a1_to_idx(a1):
    import re as _re
    def col_to_n(col_letters):
        col = 0
        for ch in col_letters:
            col = col * 26 + (ord(ch.upper()) - ord('A') + 1)
        return col
    m = _re.match(r"([A-Z]+)(\d+):([A-Z]+)(\d+)", a1)
    if not m:
        raise ValueError(f"Range A1 inválido: {a1}")
    c1 = col_to_n(m.group(1))
    r1 = int(m.group(2))
    c2 = col_to_n(m.group(3))
    r2 = int(m.group(4))
    return (r1 - 1, c1 - 1, r2, c2)  # zero-based / end exclusive

def _sheet_reopen_if_404(wks):
    """Reabre a worksheet em caso de 404/cache, reduz picos de 5xx na sequência."""
    try:
        bk = wks.spreadsheet
        title = wks.title
        return gs_retry(bk.worksheet, title, desc=f"reopen {title}")
    except Exception:
        return wks  # se falhar, usa o original mesmo

def hard_clear_columns_adapt(wks, start_col, end_col, start_row=2, end_row=None,
                             chunk_rows=CHUNK_HARD_ROWS):
    """
    2ª camada da sua lógica (NÃO removida):
      - Gera requests updateCells(userEnteredValue) por blocos de linhas × faixa de colunas.
      - Reabre a worksheet em 404, e dá micro-pausa entre requests para aliviar 503.
    """
    if end_row is None:
        end_row = max(wks.row_count, 1000)

    sheet_id = wks.id
    c0 = start_col - 1
    c1 = end_col

    total_rows = end_row - start_row + 1
    grupos = max(1, (total_rows + chunk_rows - 1) // chunk_rows)
    print(f"[hard_clear] total_rows={total_rows} chunk_rows={chunk_rows} grupos_iniciais={grupos}", flush=True)

    r_start = start_row
    while r_start <= end_row:
        r_end = min(end_row, r_start + chunk_rows - 1)
        r0 = r_start - 1
        r1 = r_end      # end exclusive

        req = {
            "requests": [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": r0,
                            "endRowIndex": r1,
                            "startColumnIndex": c0,
                            "endColumnIndex": c1
                        },
                        "fields": "userEnteredValue"
                    }
                }
            ],
            "includeSpreadsheetInResponse": False,
            "responseIncludeGridData": False,
        }

        # retry com reabertura defensiva
        tent = 0
        while True:
            try:
                gs_retry(wks.spreadsheet.batch_update, req, desc=f"hard_clear {r_start}:{r_end}")
                break
            except APIError as e:
                tent += 1
                code = _status_from_apierror(e)
                if code == 404 and tent < MAX_RETRIES:
                    wks = _sheet_reopen_if_404(wks)
                    time.sleep(0.6)
                    continue
                if tent >= MAX_RETRIES or (code is not None and code not in RETRYABLE_CODES):
                    raise
                slp = min(60.0, (BASE_SLEEP * (2 ** (tent - 1))) + random.uniform(0, 0.75))
                print(f"[hard_clear] ⚠️ {e} — retry {tent}/{MAX_RETRIES-1} em {slp:.1f}s (linhas {r_start}-{r_end})", flush=True)
                time.sleep(slp)

        # micro pausa entre blocos do hard-clear
        time.sleep(0.25)
        r_start = r_end + 1

# =========================
# ABERTURA DAS PLANILHAS
# =========================
planilha_origem  = gs_retry(cliente.open_by_key, ID_ORIGEM, desc="open origem")
planilha_destino = gs_retry(cliente.open_by_key, ID_DESTINO, desc="open destino")
aba_origem       = gs_retry(planilha_origem.worksheet, ABA_ORIGEM, desc="ws origem")
aba_destino      = gs_retry(planilha_destino.worksheet, ABA_DESTINO, desc="ws destino")

# =========================
# LER E PROCESSAR DADOS
# =========================
dados = gs_retry(aba_origem.get, INTERVALO_ORIGEM, desc=f"get {ABA_ORIGEM}!{INTERVALO_ORIGEM}")
if not dados:
    # Sem dados: mantém sua limpeza em camadas + timestamp
    gs_retry(aba_destino.batch_clear, [CLEAR_RANGE], desc=f"clear {CLEAR_RANGE}")
    hard_clear_columns_adapt(
        aba_destino,
        DEST_START_NUM,
        DEST_END_NUM,
        start_row=2,
        end_row=None,
        chunk_rows=CHUNK_HARD_ROWS
    )
    gs_retry(aba_destino.update, range_name='Z1', values=[[f'Atualizado em {agora_str()}']], desc="stamp Z1")
    raise SystemExit(0)

cabecalho = dados[0]
dados = dados[1:]

def normalizar_data(txt):
    if not txt:
        return ""
    s = str(txt).strip().lstrip("'").strip()
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    if re.match(r'^\d{2}/\d{2}/\d{4}$', s):
        return s
    m = re.match(r'^(\d{2})/(\d{2})/(\d{2})$', s)
    if m:
        return f"{m.group(1)}/{m.group(2)}/20{m.group(3)}"
    return s

# valores: K(10), L(11), P(15) | datas: J(9), M(12), O(14)
for linha in dados:
    for idx in (10, 11, 15):
        if idx < len(linha):
            bruto = str(linha[idx]).replace("R$", "").replace(".", "").replace(",", ".")
            bruto = re.sub(r"[^\d.\-]", "", bruto)
            try:
                linha[idx] = float(bruto) if bruto not in ("", ".", "-") else ""
            except Exception:
                linha[idx] = ""
    for idx in (9, 12, 14):
        if idx < len(linha):
            linha[idx] = normalizar_data(linha[idx])

# =========================
# ATUALIZAÇÃO NA DESTINO
# =========================

# 1) LIMPEZA EM CAMADAS (mantida)
# 1.1) batch_clear do intervalo D:W (valores)
end_row_est = max(aba_destino.row_count, len(dados) + 200)  # evita limpar além do necessário
clear_values_chunked(
    aba_destino,
    DEST_START_NUM,
    DEST_END_NUM,
    start_row=2,
    end_row=end_row_est,
    chunk_cols=CHUNK_CLEAR_COLS
)

# 1.2) hard clear (updateCells userEnteredValue) em blocos de linhas
hard_clear_columns_adapt(
    aba_destino,
    DEST_START_NUM,
    DEST_END_NUM,
    start_row=2,
    end_row=end_row_est,
    chunk_rows=CHUNK_HARD_ROWS
)

# 2) Status inicial
gs_retry(aba_destino.update, range_name='Z1', values=[['Atualizando']], desc="status Z1")

# 3) Colagem (mantém USER_ENTERED)
gs_retry(
    planilha_destino.values_update,
    f"{ABA_DESTINO}!{DEST_START_LET}1",
    params={'valueInputOption': 'USER_ENTERED'},
    body={'values': [cabecalho] + dados},
    desc="values_update COLAGEM"
)

# 4) PÓS-CLEAR (garantir que nada ficou abaixo do novo fim)
lin_fim = len(dados) + 1  # 1 = cabeçalho
total_rows = aba_destino.row_count
if total_rows > lin_fim + 1:
    faixa_sobra = f"{DEST_START_LET}{lin_fim+1}:{DEST_END_LET}{total_rows}"
    gs_retry(aba_destino.batch_clear, [faixa_sobra], desc=f"post clear {faixa_sobra}")

# 5) Formatação opcional (mantida)
if FORCAR_FORMATACAO:
    try:
        num_linhas = len(dados)
        if num_linhas > 0:
            sheet_id = aba_destino._properties['sheetId']
            lin_fim = num_linhas + 1
            reqs = {
                "requests": [
                    # N (idx 13) NUMBER
                    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": lin_fim, "startColumnIndex": 13, "endColumnIndex": 14},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    # O (idx 14) NUMBER
                    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": lin_fim, "startColumnIndex": 14, "endColumnIndex": 15},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    # S (idx 18) NUMBER
                    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": lin_fim, "startColumnIndex": 18, "endColumnIndex": 19},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    # M (idx 12) DATE
                    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": lin_fim, "startColumnIndex": 12, "endColumnIndex": 13},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    # P (idx 15) DATE
                    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": lin_fim, "startColumnIndex": 15, "endColumnIndex": 16},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                    # R (idx 17) DATE
                    {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": lin_fim, "startColumnIndex": 17, "endColumnIndex": 18},
                                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}},
                                    "fields": "userEnteredFormat.numberFormat"}},
                ]
            }
            gs_retry(aba_destino.spreadsheet.batch_update, reqs, max_tries=MAX_RETRIES, desc="format opcional")
    except APIError as e:
        print(f"[AVISO] Falha na formatação opcional (continua mesmo assim): {e}")

# 6) FINALIZAR
gs_retry(aba_destino.update, range_name='Z1', values=[[f'Atualizado em {agora_str()}']], desc="final Z1")
print(f"✅ CICLO atualizado (limpeza {CLEAR_RANGE} + hard clear (adaptativo) + pós-clear).", flush=True)
