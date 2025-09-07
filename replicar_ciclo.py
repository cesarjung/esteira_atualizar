# replicar_ciclo.py — resiliente, rápido e com números contáveis; formatações opcionais; sem pular destino
from datetime import datetime
import re
import time
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1

# ========= CONFIG =========
ID_PRINCIPAL   = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'   # planilha onde está a aba CICLO atualizada
ABA_CICLO      = 'CICLO'
RANGE_ORIGEM   = 'D1:T'   # NÃO inclui Z
CAMINHO_CRED   = 'credenciais.json'

PLANILHAS_DESTINO = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]

# Colunas relativas ao intervalo D:T (0-based relativas a D)
# J(10)->6, K(11)->7, L(12)->8, M(13)->9, O(15)->11, P(16)->12
IDX_DATAS_REL   = [6, 9, 11]   # J, M, O (relativo a D)
IDX_NUM_REL     = [7, 8, 12]   # K, L, P (relativo a D)

# Faixa fixa D..T
START_COL_LETTER = 'D'
END_COL_LETTER   = 'T'
N_COLS           = 17  # D..T

# Opções (desligadas por padrão p/ performance)
APLICAR_FORMATO_NUMEROS = False
APLICAR_FORMATO_DATAS   = False
CARIMBAR                 = True
CARIMBAR_CEL             = 'Z1'     # se não existir, usa última coluna existente da linha 1
HARD_CLEAR_BEFORE_WRITE  = False    # limpar D:T antes de escrever (1 chamada extra)

# Retries
RETRY_CRIT = (1, 3, 7, 15)
RETRY_SOFT = (1,)

# Tentativas externas por DESTINO (não pular nunca)
DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5  # 5,10,20,40,80s

# ========= AUTH =========
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(CAMINHO_CRED, scopes=SCOPES)
cli = gspread.authorize(creds)

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
def limpar_num(v: str):
    if v is None:
        return ''
    s = str(v).strip()
    if s == '':
        return ''
    if s.startswith("'"):
        s = s[1:]
    s = s.replace('R$', '').replace(' ', '')
    # se tem vírgula e ponto, assume ponto milhar/ vírgula decimal
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '.')
    s = re.sub(r'[^0-9.\-+eE]', '', s)
    try:
        return float(s) if s != '' else ''
    except:
        return ''

def normaliza_data(v: str):
    if v is None:
        return ''
    s = str(v).strip().replace('’', '').replace('‘', '').replace("'", "")
    if s == '':
        return ''
    s = re.sub(r'[^0-9/\-: ]', '', s)
    dt = None
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s.split(' ')[0], fmt)
            break
        except:
            continue
    return dt.strftime("%d/%m/%Y") if dt else s

def tratar_linha(row):
    # garante N_COLS e remove apóstrofo inicial
    r = [(c if c is not None else "") for c in row[:N_COLS]] + [""] * max(0, N_COLS - len(row))
    for i in range(N_COLS):
        if isinstance(r[i], str) and r[i].startswith("'"):
            r[i] = r[i][1:]

    # Datas relativas a D
    for c in IDX_DATAS_REL:
        if c < N_COLS:
            r[c] = normaliza_data(r[c])

    # Números relativos a D
    for c in IDX_NUM_REL:
        if c < N_COLS:
            r[c] = limpar_num(r[c])

    return r

# ========= LER DA PLANILHA PRINCIPAL =========
print(f"📥 Lendo {ID_PRINCIPAL}/{ABA_CICLO} ({RANGE_ORIGEM})…")
ws_src = cli.open_by_key(ID_PRINCIPAL).worksheet(ABA_CICLO)
vals = _retry(RETRY_CRIT, ws_src.get, RANGE_ORIGEM, op_name='get fonte') or []
if not vals:
    print("⚠️ Nada a replicar (faixa vazia).")
    sys.exit(0)

cabec = (vals[0] + [""] * N_COLS)[:N_COLS]
linhas_raw = vals[1:]
linhas = []
for r in linhas_raw:
    if not any((c or "").strip() for c in r[:N_COLS]):  # ignora totalmente vazias
        continue
    linhas.append(tratar_linha(r))

all_vals = [cabec] + linhas
nlin = len(all_vals)
print(f"✅ {len(linhas)} linhas preparadas.\n")

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
    rng = f"{START_COL_LETTER}1:{END_COL_LETTER}{nlin}"
    ensure_grid(ws, min_rows=nlin, min_cols=col_letter_to_index_1b(END_COL_LETTER))

    if HARD_CLEAR_BEFORE_WRITE:
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear,
               f"'{ws.title}'!{START_COL_LETTER}:{END_COL_LETTER}", op_name='values_clear D:T')

    _retry(RETRY_CRIT, ws.update,
           values=all_vals, range_name=rng,
           value_input_option="USER_ENTERED", op_name='update D1:T')

    # limpa “rabo” (linhas abaixo do dataset) apenas em D:T
    try:
        max_rows = ws.row_count
    except Exception:
        max_rows = nlin
    if max_rows > nlin:
        tail_rng = f"'{ws.title}'!{START_COL_LETTER}{nlin+1}:{END_COL_LETTER}"
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, tail_rng, op_name='values_clear rabo D:T')

def formatar(ws):
    # só aplica se ligado e houver linhas
    if not (APLICAR_FORMATO_NUMEROS or APLICAR_FORMATO_DATAS) or len(linhas) == 0:
        return
    end_row = len(linhas) + 1  # exclusivo
    reqs = []
    sid = ws.id

    # índices absolutos (0-based) das colunas em relação a A
    start_abs = col_letter_to_index_1b(START_COL_LETTER) - 1  # D -> 3

    if APLICAR_FORMATO_NUMEROS:
        for rel in sorted(IDX_NUM_REL):
            abs_idx = start_abs + rel
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row,
                              "startColumnIndex": abs_idx, "endColumnIndex": abs_idx + 1},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

    if APLICAR_FORMATO_DATAS:
        for rel in sorted(IDX_DATAS_REL):
            abs_idx = start_abs + rel
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row,
                              "startColumnIndex": abs_idx, "endColumnIndex": abs_idx + 1},
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
        max_cols = col_letter_to_index_1b(END_COL_LETTER)
        max_rows = max(nlin, 1)
    safe_col_1b = min(desired_col_1b, max_cols if max_cols else col_letter_to_index_1b(END_COL_LETTER))
    safe_row = min(max(desired_row, 1), max_rows if max_rows else 1)
    safe_cell = f"{col_index_to_letter_1b(safe_col_1b)}{safe_row}"
    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    _retry(RETRY_SOFT, ws.update,
           values=[[f'Atualizado em: {ts}']],
           range_name=safe_cell,
           value_input_option='RAW',
           swallow_final=True,
           op_name=f'carimbar {safe_cell}')

def replicar_para(pid: str):
    print(f"➡️ Atualizando {pid}/{ABA_CICLO} …")
    book = cli.open_by_key(pid)
    try:
        ws = book.worksheet(ABA_CICLO)
    except WorksheetNotFound:
        ws = book.add_worksheet(title=ABA_CICLO, rows=max(nlin, 1000), cols=max(26, col_letter_to_index_1b(END_COL_LETTER)))

    escrever_tudo(ws)   # crítico
    formatar(ws)        # opcional
    carimbar(ws)        # opcional (nunca derruba destino)
    print(f"✅ Replicado {len(linhas)} linhas para {pid}.")

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
print(f"📦 Pronto para replicar: {len(linhas)} linhas ({START_COL_LETTER}:{END_COL_LETTER}).")
for pid in PLANILHAS_DESTINO:
    tentar_destino_ate_dar_certo(pid)
print("🏁 Replicação de CICLO (D:T) finalizada.")
