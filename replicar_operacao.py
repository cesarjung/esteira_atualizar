# replicar_operacao.py — resiliente, rápido e com números contáveis; formatações opcionais
from datetime import datetime
import re
import time
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

# ========== CONFIG ==========
ID_PRINCIPAL  = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'  # planilha principal
ABA_FONTE     = 'OPERACAO'
ABA_DESTINO   = 'OPERACAO'
DESTINOS = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]
CAMINHO_CREDENCIAIS = 'credenciais.json'

# Faixa fixa deste relatório
N_COLS = 13  # A..M
RANGE_ORIGEM = 'A1:M'

# Opções (OFF por padrão p/ performance)
APLICAR_FORMATO_DATAS   = False    # E (coluna 5, índice 4)
APLICAR_FORMATO_NUMEROS = False    # D (coluna 4, índice 3)
CARIMBAR                 = True
CARIMBAR_CEL             = 'N1'    # se não existir, cai na última coluna disponível (linha 1)
PULAR_DESTINO_SE_FALHAR  = True
HARD_CLEAR_BEFORE_WRITE  = False   # limpa A:M antes de escrever (1 chamada extra)

# Colunas por tipo (0-based)
COL_DATA_IDX = {4}  # E
COL_NUM_IDX  = {3}  # D

# Retries
RETRY_CRIT = (1, 3, 7, 15)  # operações críticas
RETRY_SOFT = (1,)           # cosméticos

# ========== AUTH ==========
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(CAMINHO_CREDENCIAIS, scopes=SCOPES)
gc = gspread.authorize(creds)

# ========== UTILS ==========
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
            # não transitório: só engole se permitido
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

def get_last_col_letter(n_cols: int) -> str:
    a1 = rowcol_to_a1(1, n_cols)  # ex.: 'M1'
    return re.sub(r'\d+', '', a1) # 'M'

def a1_parse(cell: str):
    m = re.match(r'^([A-Za-z]+)(\d+)$', cell.strip())
    if not m:
        return 'A', 1
    return m.group(1).upper(), int(m.group(2))

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

# ========== TRATATIVAS ==========
_re_num = re.compile(r"^\s*'?[-+]?[\d.,]+(?:e[-+]?\d+)?\s*$", re.IGNORECASE)

def limpar_num(txt: str):
    """Converte strings de número (R$, pontos de milhar, vírgula decimal) em float; vazio em ''."""
    if txt is None: return ""
    s = str(txt).strip()
    if not s: return ""
    if s.startswith("'"): s = s[1:]
    s = s.replace("R$", "").strip()
    s = re.sub(r"[^0-9,\.\-+eE]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""

def tratar_linha(row, n_cols):
    # garante exatamente n_cols células, remove apóstrofo de textos
    r = [(c if c is not None else "") for c in row[:n_cols]] + [""] * max(0, n_cols - len(row))
    # D (3) número; E (4) data -> manter como string 'dd/MM/yyyy' (Sheets interpreta com USER_ENTERED)
    # Remove apóstrofo inicial de todas as células texto
    for i in range(n_cols):
        if isinstance(r[i], str) and r[i].startswith("'"):
            r[i] = r[i][1:]
    # D
    if 3 < n_cols:
        r[3] = limpar_num(r[3])
    # E
    if 4 < n_cols:
        s = str(r[4]).strip()
        if s:
            s_norm = re.sub(r"[^0-9/\-]", "", s)
            dt = None
            for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
                try:
                    from datetime import datetime as _dt
                    dt = _dt.strptime(s_norm, fmt)
                    break
                except Exception:
                    continue
            r[4] = dt.strftime("%d/%m/%Y") if dt else s  # USER_ENTERED deve interpretar
    return r

# ========== LEITURA ==========
print(f"📥 Lendo {ID_PRINCIPAL}/{ABA_FONTE} ({RANGE_ORIGEM})…")
ws_src = gc.open_by_key(ID_PRINCIPAL).worksheet(ABA_FONTE)
vals = _retry(RETRY_CRIT, ws_src.get, RANGE_ORIGEM, op_name='get origem') or []
if not vals:
    print("⚠️ Nada a replicar (faixa vazia).")
    exit(0)

header = (vals[0] + [""] * N_COLS)[:N_COLS]
rows_raw = vals[1:]
rows = []
for r in rows_raw:
    # descarta linhas totalmente vazias
    if not any((c or "").strip() for c in r[:N_COLS]): 
        continue
    rows.append(tratar_linha(r, N_COLS))

all_vals = [header] + rows
nlin = len(all_vals)
last_col_letter = get_last_col_letter(N_COLS)

# ========== ESCRITA / FORMATAÇÃO ==========
def ensure_grid(ws, min_rows: int, min_cols: int):
    """Garante linhas/colunas suficientes para evitar Range exceeds grid limits."""
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
    except Exception:
        cur_rows = cur_cols = 0
    need_resize = (cur_rows < min_rows) or (cur_cols < min_cols)
    if need_resize:
        _retry(RETRY_CRIT, ws.resize, rows=max(cur_rows, min_rows), cols=max(cur_cols, min_cols), op_name='resize')

def escrever_tudo(ws):
    rng = f"A1:{last_col_letter}{nlin}"

    # garante grade
    ensure_grid(ws, min_rows=nlin, min_cols=N_COLS)

    # hard clear opcional
    if HARD_CLEAR_BEFORE_WRITE:
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, f"'{ws.title}'!A:{last_col_letter}", op_name='values_clear A:M')

    # única escrita (USER_ENTERED => números sem apostrófo)
    _retry(RETRY_CRIT, ws.update, values=all_vals, range_name=rng,
           value_input_option="USER_ENTERED", op_name='update A1:M')

    # limpa o “rabo” (A{n+1}:M)
    try:
        max_rows = ws.row_count
    except Exception:
        max_rows = nlin
    if max_rows > nlin:
        tail_rng = f"'{ws.title}'!A{nlin+1}:{last_col_letter}"
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, tail_rng, op_name='values_clear rabo')

def formatar(ws):
    """Formatação opcional via batch_update. OFF por padrão."""
    if not (APLICAR_FORMATO_DATAS or APLICAR_FORMATO_NUMEROS) or len(rows) == 0:
        return
    end_row = len(rows) + 1  # exclusivo
    reqs = []
    sheet_id = ws.id
    # datas
    if APLICAR_FORMATO_DATAS:
        for idx in sorted(COL_DATA_IDX):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": end_row,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    # números
    if APLICAR_FORMATO_NUMEROS:
        for idx in sorted(COL_NUM_IDX):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": end_row,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    if reqs:
        _retry(RETRY_SOFT, ws.spreadsheet.batch_update, {"requests": reqs},
               swallow_final=True, op_name='batch_update format')

def carimbar(ws):
    if not CARIMBAR: 
        return
    desired_letter, desired_row = a1_parse(CARIMBAR_CEL)
    desired_col_1b = col_letter_to_index_1b(desired_letter)

    # usa última coluna se CARIMBAR_CEL exceder grade
    try:
        max_cols = ws.col_count
        max_rows = ws.row_count
    except Exception:
        max_cols = N_COLS
        max_rows = max(nlin, 1)

    safe_col_1b = min(desired_col_1b, max_cols if max_cols else N_COLS)
    safe_row = min(max(desired_row, 1), max_rows if max_rows else 1)
    safe_cell = f"{col_index_to_letter_1b(safe_col_1b)}{safe_row}"

    ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    _retry(RETRY_SOFT, ws.update,
           values=[[f'Atualizado em: {ts}']],
           range_name=safe_cell,
           value_input_option="RAW",
           swallow_final=True,
           op_name=f'carimbar {safe_cell}')

def replicar_para(dest_id: str):
    try:
        print(f"➡️ Atualizando {dest_id}/{ABA_DESTINO} …")
        ws = gc.open_by_key(dest_id).worksheet(ABA_DESTINO)
        escrever_tudo(ws)   # crítico (com retries)
        formatar(ws)        # opcional (retry curto + swallow)
        carimbar(ws)        # opcional (nunca derruba destino)
        print(f"✅ Replicado {len(rows)} linhas para {dest_id}.")
    except Exception as e:
        msg = f"⛔️ Erro ao atualizar {dest_id}: {e}"
        if PULAR_DESTINO_SE_FALHAR:
            print(msg + " — pulando destino.")
        else:
            raise

# ========== EXECUÇÃO ==========
print(f"📦 Pronto para replicar: {len(rows)} linhas (A:M).")
for dest in DESTINOS:
    replicar_para(dest)
print("🏁 Processo concluído.")
