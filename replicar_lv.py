# replicar_lv.py — resiliente, rápido e com números contáveis; formatações opcionais; sem pular destino
from datetime import datetime
import re
import time
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1

# ========= CONFIG =========
ID_FONTE     = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'   # planilha principal (LV CICLO pronta)
ABA_FONTE    = 'LV CICLO'
RANGE_FONTE  = 'A1:Y'   # não traz Z (reservada p/ status/timestamp)

DESTINOS = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]
ABA_DESTINO  = 'LV CICLO'
CAM_CRED     = 'credenciais.json'

# Colunas (0-based) — do seu original:
NUM_COLS = [5, 10, 19, 21, 22]  # F, K, T, V, W
DATE_COL = 7                    # H

# Faixa fixa
N_COLS = 25  # A..Y
LAST_COL_LETTER = re.sub(r'\d+', '', rowcol_to_a1(1, N_COLS))  # 'Y'

# Opções (desligadas por padrão p/ performance)
APLICAR_FORMATO_NUMEROS = False
APLICAR_FORMATO_DATAS   = False
CARIMBAR                 = True
CARIMBAR_CEL             = 'Z1'     # se não existir, usa última coluna existente da linha 1
HARD_CLEAR_BEFORE_WRITE  = False    # limpar A:Y antes de escrever (1 chamada extra)

# Retries de chamadas individuais
RETRY_CRIT = (1, 3, 7, 15)
RETRY_SOFT = (1,)

# Tentativas externas por DESTINO (não pular nunca)
DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5  # 5,10,20,40,80s

# ========= AUTH =========
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(CAM_CRED, scopes=SCOPES)
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

def col_letter_to_index_1b(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx  # 1-based

def col_index_to_letter_1b(index: int) -> str:
    res = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        res = chr(rem + ord('A')) + res
    return res

def a1_parse(cell: str):
    m = re.match(r'^([A-Za-z]+)(\d+)$', cell.strip())
    if not m:
        return 'A', 1
    return m.group(1).upper(), int(m.group(2))

# ========= TRATAMENTO =========
def limpar_num(txt: str):
    if txt is None:
        return ""
    s = str(txt).strip()
    if not s:
        return ""
    if s.startswith("'"):
        s = s[1:]
    s = s.replace('’', '').replace('‘', '').replace("'", "")
    s = re.sub(r"[^\d,.\-+eE]", "", s)
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except Exception:
        return ""

def tratar_linha(row, n_cols):
    # garante n_cols e remove apóstrofo inicial
    r = [(c if c is not None else "") for c in row[:n_cols]] + [""] * max(0, n_cols - len(row))
    for i in range(n_cols):
        if isinstance(r[i], str) and r[i].startswith("'"):
            r[i] = r[i][1:]
    # números
    for c in NUM_COLS:
        if c < n_cols:
            r[c] = limpar_num(r[c])
    # data H (7) -> string dd/MM/yyyy p/ USER_ENTERED interpretar
    if DATE_COL < n_cols:
        s = str(r[DATE_COL]).strip()
        if s:
            s_norm = re.sub(r"[^0-9/\-:]", "", s)
            dt = None
            for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
                try:
                    dt = datetime.strptime(s_norm.split(' ')[0], fmt)
                    break
                except Exception:
                    continue
            if dt:
                r[DATE_COL] = dt.strftime("%d/%m/%Y")
    return r

# ========= LER FONTE =========
print(f"📥 Lendo {ID_FONTE}/{ABA_FONTE} ({RANGE_FONTE})…")
ws_src = gc.open_by_key(ID_FONTE).worksheet(ABA_FONTE)
vals = _retry(RETRY_CRIT, ws_src.get, RANGE_FONTE, op_name='get fonte') or []
if not vals:
    print("⚠️ Nada a replicar (faixa vazia).")
    sys.exit(0)

header = (vals[0] + [""] * N_COLS)[:N_COLS]
rows_raw = vals[1:]
rows = []
for r in rows_raw:
    if not any((c or "").strip() for c in r[:N_COLS]):  # ignora totalmente vazias
        continue
    rows.append(tratar_linha(r, N_COLS))

all_vals = [header] + rows
nlin = len(all_vals)
print(f"✅ {len(rows)} linhas preparadas.\n")

# ========= ESCRITA / FORMATAÇÃO =========
def ensure_grid(ws, min_rows: int, min_cols: int):
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
    except Exception:
        cur_rows = cur_cols = 0
    if cur_rows < min_rows or cur_cols < min_cols:
        _retry(RETRY_CRIT, ws.resize, rows=max(cur_rows, min_rows), cols=max(cur_cols, min_cols), op_name='resize')

def escrever_tudo(ws):
    rng = f"A1:{LAST_COL_LETTER}{nlin}"
    ensure_grid(ws, min_rows=nlin, min_cols=N_COLS)

    if HARD_CLEAR_BEFORE_WRITE:
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, f"'{ws.title}'!A:{LAST_COL_LETTER}", op_name='values_clear A:Y')

    _retry(RETRY_CRIT, ws.update,
           values=all_vals, range_name=rng,
           value_input_option="USER_ENTERED", op_name='update A1:Y')

    # limpa “rabo” (linhas abaixo do dataset)
    try:
        max_rows = ws.row_count
    except Exception:
        max_rows = nlin
    if max_rows > nlin:
        tail_rng = f"'{ws.title}'!A{nlin+1}:{LAST_COL_LETTER}"
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, tail_rng, op_name='values_clear rabo')

def formatar(ws):
    # só aplica se ligado e houver linhas
    if not (APLICAR_FORMATO_NUMEROS or APLICAR_FORMATO_DATAS) or len(rows) == 0:
        return
    end_row = len(rows) + 1  # exclusivo
    reqs = []
    sid = ws.id

    if APLICAR_FORMATO_NUMEROS:
        for idx in sorted(NUM_COLS):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row,
                                  "startColumnIndex": idx, "endColumnIndex": idx + 1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    if APLICAR_FORMATO_DATAS and DATE_COL < N_COLS:
        reqs.append({
            "repeatCell": {
                "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row,
                          "startColumnIndex": DATE_COL, "endColumnIndex": DATE_COL + 1},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        })
    if reqs:
        _retry(RETRY_SOFT, ws.spreadsheet.batch_update, {"requests": reqs},
               swallow_final=True, op_name='batch_update format')

def carimbar(ws):
    if not CARIMBAR:
        return
    # célula segura: se Z1 não existir, usa última coluna disponível na linha 1
    desired_letter, desired_row = a1_parse(CARIMBAR_CEL)
    desired_col_1b = col_letter_to_index_1b(desired_letter)
    try:
        max_cols = ws.col_count
        max_rows = ws.row_count
    except Exception:
        max_cols = N_COLS
        max_rows = max(nlin, 1)
    safe_col_1b = min(desired_col_1b, max_cols if max_cols else N_COLS)
    safe_row = min(max(desired_row, 1), max_rows if max_rows else 1)
    safe_cell = f"{col_index_to_letter_1b(safe_col_1b)}{safe_row}"
    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    _retry(RETRY_SOFT, ws.update,
           values=[[f'Atualizado em: {ts}']],
           range_name=safe_cell,
           value_input_option='RAW',
           swallow_final=True,
           op_name=f'carimbar {safe_cell}')

def replicar_para(dest_id: str):
    print(f"➡️ Atualizando {dest_id}/{ABA_DESTINO} …")
    book = gc.open_by_key(dest_id)
    try:
        ws = book.worksheet(ABA_DESTINO)
    except WorksheetNotFound:
        # cria com cols >= 26 para comportar a coluna Z do carimbo
        ws = book.add_worksheet(title=ABA_DESTINO, rows=max(nlin, 1000), cols=max(26, N_COLS))
    escrever_tudo(ws)   # crítico
    formatar(ws)        # opcional
    carimbar(ws)        # opcional (nunca derruba destino)
    print(f"✅ Replicado {len(rows)} linhas para {dest_id}.")

def tentar_destino_ate_dar_certo(planilha_id: str):
    for tentativa in range(1, DESTINO_MAX_TENTATIVAS + 1):
        try:
            if tentativa > 1:
                atraso = DESTINO_BACKOFF_BASE_S * (2 ** (tentativa - 2))  # 5,10,20,40,80...
                print(f"🔁 Tentativa {tentativa}/{DESTINO_MAX_TENTATIVAS} para {planilha_id} — aguardando {atraso}s")
                time.sleep(atraso)
            replicar_para(planilha_id)
            return  # sucesso
        except Exception as e:
            print(f"❌ Falha na tentativa {tentativa} para {planilha_id}: {e}")
            if tentativa == DESTINO_MAX_TENTATIVAS:
                print(f"⛔️ Não foi possível atualizar {planilha_id} após {DESTINO_MAX_TENTATIVAS} tentativas. Abortando.")
                sys.exit(1)

# ========= EXECUÇÃO =========
print(f"📦 Pronto para replicar: {len(rows)} linhas (A:Y).")
for pid in DESTINOS:
    tentar_destino_ate_dar_certo(pid)
print("🏁 Replicação de LV CICLO finalizada.")
