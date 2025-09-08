# replicar_bd_exec_fghij.py — rápido, resiliente e com limpeza correta de F:J
from datetime import datetime
import re
import time
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

# ========= CONFIG =========
CAMINHO_CRED   = 'credenciais.json'
ID_ORIGEM      = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA            = 'BD_EXEC'

DESTINOS = [
    "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c",
    "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M",
    "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c",
    "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw",
]

# Faixa de origem/destino
SRC_RANGE = 'F2:J'   # F,G,H,I,J  (antes: F2:I)
DST_START_COL = 'F'
DST_END_COL   = 'J'  # (antes: I)
DST_START_ROW = 2
CARIMBAR_CEL  = 'E1'

# Opções
APAGAR_ANTES_FI        = True   # apagão em F2:J antes de escrever
APLICAR_FORMATO_DATA_G = False  # se True, força G como DATE dd/mm/yyyy
CARIMBAR               = True

# Retries e backoff
RETRY_CRIT = (1, 3, 7, 15)
RETRY_SOFT = (1,)
DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5  # 5,10,20,40,80

# ========= AUTH =========
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(CAMINHO_CRED, scopes=SCOPES)
gc = gspread.authorize(creds)

# ========= UTILS =========
def _is_transient(e: Exception) -> bool:
    s = str(e)
    return any(t in s for t in ('[500]', '[503]', 'backendError', 'rateLimitExceeded',
                                'Internal error', 'service is currently unavailable'))

def _retry(delays, fn, *args, swallow_final=False, op_name=None, **kwargs):
    total = len(delays)
    for i, d in enumerate(delays, start=1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if not _is_transient(e):
                if swallow_final:
                    print(f"⚠️ Operação ignorada ({op_name or 'op'}): {e}")
                    return None
                raise
            tag = f" ({op_name})" if op_name else ""
            print(f"⚠️ Falha transitória da API{tag}: {e} — tentativa {i}/{total}; aguardando {d}s")
            if i == total:
                if swallow_final:
                    print(f"⚠️ API instável — operação ignorada após {total} tentativas{tag}.")
                    return None
                raise
            time.sleep(d)

def _col_letter_to_index_1b(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx

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
    # vírgula decimal; remove milhar
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
            dt = datetime.strptime(s.split(' ')[0], fmt)
            return dt.strftime("%d/%m/%Y")
        except Exception:
            continue
    return s  # USER_ENTERED tenta interpretar

def tratar_row_fghij(r5):
    """F,H,I,J removem apóstrofo inicial; G vira data dd/mm/aaaa se possível, senão número."""
    r = (r5 + [""] * 5)[:5]
    # tira apóstrofo de F,H,I,J
    for idx in (0, 2, 3, 4):
        if isinstance(r[idx], str) and r[idx].startswith("'"):
            r[idx] = r[idx][1:]
    # G preferencialmente data
    g_raw = r[1]
    g_fmt = normaliza_data_ddmmyyyy(g_raw)
    r[1] = g_fmt if g_fmt and re.match(r'^\d{2}/\d{2}/\d{4}$', g_fmt) else limpar_num(g_raw)
    return r

# ========= LER FONTE =========
print(f"📥 Lendo {ID_ORIGEM}/{ABA} ({SRC_RANGE})…")
ws_src = gc.open_by_key(ID_ORIGEM).worksheet(ABA)
vals = _retry(RETRY_CRIT, ws_src.get, SRC_RANGE, op_name='get F2:J') or []

linhas = []
for r in vals:
    r5 = (r + [""] * 5)[:5]
    if not any((c or "").strip() for c in r5):
        continue
    linhas.append(tratar_row_fghij(r5))

nlin = len(linhas)
print(f"✅ {nlin} linhas preparadas.\n")

if nlin == 0:
    print("⚠️ Nada a replicar (F2:J está vazio).")
    sys.exit(0)

# ========= ESCRITA =========
def ensure_grid(ws, min_rows: int, min_cols_letter: str):
    min_cols = _col_letter_to_index_1b(min_cols_letter)
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
    except Exception:
        cur_rows = cur_cols = 0
    if cur_rows < min_rows or cur_cols < min_cols:
        _retry(RETRY_CRIT, ws.resize, rows=max(cur_rows, min_rows), cols=max(cur_cols, min_cols), op_name='resize')

def escrever_tudo(ws):
    first_row = DST_START_ROW
    last_row  = DST_START_ROW + nlin - 1
    rng = f"{DST_START_COL}{first_row}:{DST_END_COL}{last_row}"

    ensure_grid(ws, min_rows=max(last_row, 2), min_cols_letter=DST_END_COL)

    # apagão antes (F2:J)
    if APAGAR_ANTES_FI:
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, f"'{ws.title}'!{DST_START_COL}{DST_START_ROW}:{DST_END_COL}", op_name='values_clear F2:J')

    # escrita única
    _retry(RETRY_CRIT, ws.update, values=linhas, range_name=rng,
           value_input_option="USER_ENTERED", op_name='update F2:J')

    # limpa rabo (linhas abaixo)
    tail_rng = f"'{ws.title}'!{DST_START_COL}{last_row+1}:{DST_END_COL}"
    _retry(RETRY_CRIT, ws.spreadsheet.values_clear, tail_rng, op_name='values_clear rabo F:J')

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
    _retry(RETRY_SOFT, ws.spreadsheet.batch_update, {"requests": [req]},
           swallow_final=True, op_name='format G as DATE')

def carimbar(ws):
    if not CARIMBAR:
        return
    ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    _retry(RETRY_SOFT, ws.update,
           values=[[f'Atualizado em: {ts}']],
           range_name=CARIMBAR_CEL,
           value_input_option='RAW',
           swallow_final=True,
           op_name='carimbar E1')

def replicar_para(dest_id: str):
    print(f"➡️ Atualizando {dest_id}/{ABA} …")
    book = gc.open_by_key(dest_id)
    try:
        ws = book.worksheet(ABA)
    except WorksheetNotFound:
        ws = book.add_worksheet(title=ABA, rows=max(DST_START_ROW + nlin + 100, 1000), cols=20)

    # status inicial
    _retry(RETRY_SOFT, ws.update, values=[['Atualizando...']], range_name=CARIMBAR_CEL,
           value_input_option='RAW', swallow_final=True, op_name='status E1')

    escrever_tudo(ws)
    formatar(ws)
    carimbar(ws)
    print(f"✅ Replicado {nlin} linhas para {dest_id}.")

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
