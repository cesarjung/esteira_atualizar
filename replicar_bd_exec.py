# replicar_bd_exec_ab.py — A(origem)→A(dest), B(origem)→B(dest); apaga A2:B; limpa rabo; retries; sem pular destino
from datetime import datetime
import re
import time
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

# ========= CONFIG =========
CAMINHO_CRED = 'credenciais.json'
ID_ORIGEM    = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA          = 'BD_EXEC'

DESTINOS = [
    "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c",
    "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M",
    "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c",
    "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw",
]

# Destino
START_COL = 'A'
END_COL   = 'B'
START_ROW = 2
CARIMBAR_CEL = 'E1'

# Opções
APAGAR_ANTES_A_B        = True   # apagão em A2:B antes de colar
APLICAR_FORMATO_DATA_B  = False  # se True, força B como DATE dd/mm/yyyy
CARIMBAR                = True

# Retries / backoff
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

def tratar_par_ab(a_raw, b_raw):
    """A(origem)→A(dest) texto; B(origem)→B(dest) data dd/mm/aaaa ou número limpo."""
    # Coluna A: string sem apóstrofo inicial
    a_val = str(a_raw)[1:] if isinstance(a_raw, str) and str(a_raw).startswith("'") else a_raw
    # Coluna B: tenta data; se não der, usa número limpo
    b_fmt = normaliza_data_ddmmyyyy(b_raw)
    if b_fmt and re.match(r'^\d{2}/\d{2}/\d{4}$', b_fmt):
        b_val = b_fmt
    else:
        b_val = limpar_num(b_raw)
    return [a_val if a_val is not None else "", b_val if b_val is not None else ""]

# ========= LER FONTE =========
print(f"📥 Lendo {ID_ORIGEM}/{ABA} (A2:B)…")
ws_src = gc.open_by_key(ID_ORIGEM).worksheet(ABA)
vals = _retry(RETRY_CRIT, ws_src.get, 'A2:B', op_name='get A2:B') or []

linhas = []
for r in vals:
    a = r[0] if len(r) > 0 else ""
    b = r[1] if len(r) > 1 else ""
    if not (str(a).strip() or str(b).strip()):
        continue
    linhas.append(tratar_par_ab(a, b))

nlin = len(linhas)
print(f"✅ {nlin} linhas preparadas.\n")

if nlin == 0:
    print("⚠️ Nada a replicar (A2:B está vazio).")
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
    first_row = START_ROW
    last_row  = START_ROW + nlin - 1
    rng = f"{START_COL}{first_row}:{END_COL}{last_row}"

    ensure_grid(ws, min_rows=max(last_row, 2), min_cols_letter=END_COL)

    # 🔒 apagão antes (A2:B) — preserva cabeçalhos A1/B1
    if APAGAR_ANTES_A_B:
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, f"'{ws.title}'!A2:B", op_name='values_clear A2:B')

    # escrita única
    _retry(RETRY_CRIT, ws.update, values=linhas, range_name=rng,
           value_input_option="USER_ENTERED", op_name='update A2:B')

    # limpa rabo (A{last_row+1}:B)
    tail_rng = f"'{ws.title}'!A{last_row+1}:B"
    _retry(RETRY_CRIT, ws.spreadsheet.values_clear, tail_rng, op_name='values_clear rabo A:B')

def formatar(ws):
    if not APLICAR_FORMATO_DATA_B or nlin == 0:
        return
    first_row = START_ROW
    last_row  = START_ROW + nlin - 1
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": first_row - 1,
                "endRowIndex": last_row,
                "startColumnIndex": _col_letter_to_index_1b('B') - 1,
                "endColumnIndex": _col_letter_to_index_1b('C') - 1
            },
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
            "fields": "userEnteredFormat.numberFormat"
        }
    }
    _retry(RETRY_SOFT, ws.spreadsheet.batch_update, {"requests": [req]},
           swallow_final=True, op_name='format B as DATE')

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
        ws = book.add_worksheet(title=ABA, rows=max(START_ROW + nlin + 100, 1000), cols=10)

    # status
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
print(f"📦 Pronto para replicar: {nlin} linhas (A:B).")
for pid in DESTINOS:
    tentar_destino_ate_dar_certo(pid)
print("🏁 Replicação de BD_EXEC (A:B) finalizada.")
