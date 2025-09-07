# ciclo.py
from datetime import datetime
import os
import time
import re
import random
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

FORCAR_FORMATACAO = os.environ.get("FORCAR_FORMATACAO", "0") == "1"

# === CONFIGURAÇÕES ===
ID_ORIGEM = '19xV_P6KIoZB9U03yMcdRb2oF_Q7gVdaukjAvE4xOvl8'
ID_DESTINO = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_ORIGEM = 'OBRAS GERAL'
ABA_DESTINO = 'CICLO'
INTERVALO_ORIGEM = 'A1:T'
CAMINHO_CREDENCIAIS = 'credenciais.json'

# --- helpers p/ colunas ---
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

# === AUTENTICAÇÃO ===
escopos = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credenciais = Credentials.from_service_account_file(CAMINHO_CREDENCIAIS, scopes=escopos)
cliente = gspread.authorize(credenciais)

# ---------- util ----------
def gs_retry(fn, *args, max_tries=6, base_sleep=1.0, **kwargs):
    tentativa = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except APIError:
            tentativa += 1
            if tentativa >= max_tries:
                raise
            slp = (base_sleep * (2 ** (tentativa - 1))) + random.uniform(0, 0.75)
            time.sleep(slp)

def agora_str():
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')

def hard_clear_columns(aba, start_col_1based: int, end_col_1based: int):
    """
    Limpa de forma 'forçada' os valores (userEnteredValue) em TODAS as linhas
    entre as colunas start..end (inclusive). Não mexe em formatos.
    """
    sheet_id = aba._properties['sheetId']
    req = {
        "requests": [{
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,  # da primeira linha
                    "startColumnIndex": start_col_1based - 1,  # 0-based
                    "endColumnIndex": end_col_1based          # exclusivo
                    # endRowIndex omitido = vai até o final da grade
                },
                "fields": "userEnteredValue"
            }
        }]
    }
    gs_retry(aba.spreadsheet.batch_update, req)

# === ABERTURA DAS PLANILHAS ===
planilha_origem  = gs_retry(cliente.open_by_key, ID_ORIGEM)
planilha_destino = gs_retry(cliente.open_by_key, ID_DESTINO)
aba_origem       = gs_retry(planilha_origem.worksheet, ABA_ORIGEM)
aba_destino      = gs_retry(planilha_destino.worksheet, ABA_DESTINO)

# === LER E PROCESSAR DADOS ===
dados = gs_retry(aba_origem.get, INTERVALO_ORIGEM)
if not dados:
    # Sem dados: limpa duro e carimba timestamp
    gs_retry(aba_destino.batch_clear, [CLEAR_RANGE])              # 1) tentativa padrão
    hard_clear_columns(aba_destino, DEST_START_NUM, DEST_END_NUM) # 2) hard clear
    gs_retry(aba_destino.update, range_name='Z1', values=[[f'Atualizado em {agora_str()}']])
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

# === ATUALIZAÇÃO NA PLANILHA DESTINO ===

# 1) LIMPEZA EM CAMADAS
gs_retry(aba_destino.batch_clear, [CLEAR_RANGE])                      # camada 1
hard_clear_columns(aba_destino, DEST_START_NUM, DEST_END_NUM)         # camada 2 (hard)

# 2) Status
gs_retry(aba_destino.update, range_name='Z1', values=[['Atualizando']])

# 3) Colagem
gs_retry(
    planilha_destino.values_update,
    f"{ABA_DESTINO}!{DEST_START_LET}1",
    params={'valueInputOption': 'USER_ENTERED'},
    body={'values': [cabecalho] + dados}
)

# 4) PÓS-CLEAR (garantir que nada ficou abaixo do novo fim)
lin_fim = len(dados) + 1  # 1 = cabeçalho
total_rows = aba_destino.row_count
if total_rows > lin_fim + 1:
    faixa_sobra = f"{DEST_START_LET}{lin_fim+1}:{DEST_END_LET}{total_rows}"
    gs_retry(aba_destino.batch_clear, [faixa_sobra])

# --- Formatação opcional ---
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
            gs_retry(aba_destino.spreadsheet.batch_update, reqs, max_tries=6)
    except APIError as e:
        print(f"[AVISO] Falha na formatação opcional (continua mesmo assim): {e}")

# === FINALIZAR ===
gs_retry(aba_destino.update, range_name='Z1', values=[[f'Atualizado em {agora_str()}']])
print(f"✅ CICLO atualizado (limpeza {CLEAR_RANGE} + hard clear + pós-clear).")
