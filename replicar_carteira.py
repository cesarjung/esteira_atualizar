# replicar_carteira.py — resiliente, sem pular destino; números contáveis e formatações opcionais
from datetime import datetime
import re
import time
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

try:
    from gspread_formatting import format_cell_range, CellFormat, NumberFormat
except Exception:
    format_cell_range = None
    CellFormat = None
    NumberFormat = None

# === CONFIGURAÇÕES ===
ID_MASTER = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA = 'Carteira'
DESTINOS = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]

CAMINHO_CREDENCIAIS = r'C:\Users\Sirtec\Desktop\Importador Carteira\credenciais.json'
ESCOPOS = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# === OPÇÕES ===
APLICAR_FORMATO_DATAS   = False      # formata A como dd/MM/yyyy (cosmético)
APLICAR_FORMATO_NUMEROS = False      # formata colunas numéricas (cosmético)
COLUNAS_NUMERICAS       = ()         # ex.: ('G','K','L','Y')
CARIMBAR_T2             = True       # escreve "Atualizado em: ..." em T2 (cosmético)
HARD_CLEAR_BEFORE_WRITE = False      # faz apagão em A:S antes de escrever (preserva T)

# === RETRIES ===
RETRY_CRIT = (1, 3, 7, 15)  # operações críticas (até 4 tentativas)
RETRY_SOFT = (1,)           # cosméticos (retry mínimo)

# Tentativas externas por destino (sem pular)
DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5  # 5,10,20,40,80

ERROS_PADRAO = {'#N/A', '#DIV/0!', '#REF!', '#VALUE!', '#NAME?', '#NUM!', '#NULL!'}

# === AUTENTICAÇÃO ===
creds = Credentials.from_service_account_file(CAMINHO_CREDENCIAIS, scopes=ESCOPOS)
gc = gspread.authorize(creds)

# === UTILS ===
def _is_transient(err: Exception) -> bool:
    s = str(err)
    return any(t in s for t in ('[500]', '[503]', 'backendError', 'rateLimitExceeded',
                                'Internal error', 'service is currently unavailable'))

def _retry(delays, fn, *args, swallow_final=False, op_name=None, **kwargs):
    total = len(delays)
    for i, d in enumerate(delays, start=1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if not _is_transient(e):
                raise
            tag = f" ({op_name})" if op_name else ""
            print(f"⚠️ Falha transitória da API{tag} ({e}). Tentativa {i}/{total}. Aguardando {d}s…", flush=True)
            if i == total:
                if swallow_final:
                    print(f"⚠️ API instável — operação ignorada após {total} tentativas{tag}.", flush=True)
                    return None
                raise
            time.sleep(d)

def _col_letter_to_index(letter: str) -> int:
    return ord(letter.strip().upper()) - ord('A')

NUM_COL_INDEXES = { _col_letter_to_index(c) for c in COLUNAS_NUMERICAS }

def normalizar_data(valor: str) -> str:
    s = (valor or '').strip()
    if not s:
        return s
    try:
        if len(s) >= 10 and s[4] == '-' and s[7] == '-':  # YYYY-MM-DD
            return datetime.strptime(s[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
    except Exception:
        pass
    for fmt in ('%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y'):
        try:
            return datetime.strptime(s.split(' ')[0], fmt).strftime('%d/%m/%Y')
        except Exception:
            continue
    return s

_re_num = re.compile(r"^\s*'?[-+]?[\d.,]+\s*$")

def to_number_if_applicable(text: str):
    if text is None:
        return text
    s = str(text).strip()
    if s.startswith("'"):
        s = s[1:]  # remove apóstrofo inicial
    if not _re_num.match(s):
        return s
    s2 = re.sub(r"[^0-9,.\-]", "", s)
    if ',' in s2 and '.' in s2:
        s2 = s2.replace('.', '').replace(',', '.')
    elif ',' in s2 and '.' not in s2:
        s2 = s2.replace(',', '.')
    try:
        return float(s2)
    except Exception:
        return s

def ensure_grid(ws, min_rows: int, min_cols: int):
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
    except Exception:
        cur_rows = cur_cols = 0
    if cur_rows < min_rows or cur_cols < min_cols:
        _retry(RETRY_CRIT, ws.resize, rows=max(cur_rows, min_rows), cols=max(cur_cols, min_cols), op_name='resize')

def ler_master_A_S():
    ws = gc.open_by_key(ID_MASTER).worksheet(ABA)
    vals = _retry(RETRY_CRIT, ws.get, 'A1:S', op_name='ler_master')
    if not vals:
        return [], []
    headers = (vals[0] + ['']*19)[:19]
    linhas = []
    for r in vals[1:]:
        row = (r + ['']*19)[:19]
        if row[0].strip():
            row = [("" if c in ERROS_PADRAO else c) for c in row]
            row[0] = normalizar_data(row[0])  # data em A
            linhas.append(row)
    return headers, linhas

def preparar_valores(headers, linhas):
    all_rows = [headers] + linhas
    out = []
    for r_idx, row in enumerate(all_rows):
        new_row = []
        for c_idx, val in enumerate(row):
            val = "" if val is None else val
            if r_idx > 0 and c_idx in NUM_COL_INDEXES:
                new_row.append(to_number_if_applicable(val))
            else:
                s = str(val)
                new_row.append(s[1:] if s.startswith("'") else s)
        out.append(new_row)
    return out

def escrever_tudo(ws, all_vals):
    nlin = len(all_vals)
    ensure_grid(ws, min_rows=nlin, min_cols=19)  # A..S = 19 colunas
    if HARD_CLEAR_BEFORE_WRITE:
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, f"'{ws.title}'!A:S", op_name='values_clear A:S')
    rng = f"A1:S{nlin}"
    _retry(
        RETRY_CRIT, ws.update,
        values=all_vals,
        range_name=rng,
        value_input_option="USER_ENTERED",
        op_name='update A1:S'
    )
    # limpa rabo antigo em A:S (sem tocar T)
    try:
        max_rows = ws.row_count
    except Exception:
        max_rows = nlin
    if max_rows > nlin:
        rng_tail = f"'{ws.title}'!A{nlin+1}:S"
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, rng_tail, op_name='values_clear rabo')

def formatar_datas(ws, nlin):
    if not (APLICAR_FORMATO_DATAS and format_cell_range and CellFormat and NumberFormat):
        return
    try:
        fmt = CellFormat(numberFormat=NumberFormat(type='DATE', pattern='dd/MM/yyyy'))
        _retry(RETRY_SOFT, format_cell_range, ws, f"A2:A{nlin}", fmt, swallow_final=True, op_name='format A')
    except Exception as e:
        print(f"⚠️ Formatação de datas ignorada: {e}")

def formatar_numeros(ws, nlin):
    if not (APLICAR_FORMATO_NUMEROS and format_cell_range and CellFormat and NumberFormat and NUM_COL_INDEXES):
        return
    try:
        fmt = CellFormat(numberFormat=NumberFormat(type='NUMBER', pattern="#,##0.00"))
        for c_idx in sorted(NUM_COL_INDEXES):
            col_letter = chr(ord('A') + c_idx)
            _retry(RETRY_SOFT, format_cell_range, ws, f"{col_letter}2:{col_letter}{nlin}", fmt, swallow_final=True, op_name=f'format {col_letter}')
    except Exception as e:
        print(f"⚠️ Formatação numérica ignorada: {e}")

def carimbar(ws):
    if not CARIMBAR_T2:
        return
    ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    _retry(
        RETRY_SOFT, ws.update,
        values=[[f'Atualizado em: {ts}']],
        range_name='T2',
        value_input_option='RAW',
        swallow_final=True,
        op_name='carimbar T2'
    )

def atualizar_destino(planilha_id, headers, linhas):
    print(f"➡️ Atualizando planilha {planilha_id} / aba {ABA} ...", flush=True)
    book = gc.open_by_key(planilha_id)
    try:
        ws = book.worksheet(ABA)
    except WorksheetNotFound:
        # cria com ao menos 26 colunas para preservar T (carimbo)
        ws = book.add_worksheet(title=ABA, rows=max(len(linhas) + 5, 1000), cols=26)
    all_vals = preparar_valores(headers, linhas)
    escrever_tudo(ws, all_vals)                 # CRÍTICO (com retries)
    nlin = len(all_vals)
    formatar_datas(ws, nlin)                    # OPCIONAL (retry curto, engole erro)
    formatar_numeros(ws, nlin)                  # OPCIONAL (retry curto, engole erro)
    carimbar(ws)                                # OPCIONAL (retry curto, engole erro)
    print(f"✅ {len(linhas)} linhas copiadas para {planilha_id}", flush=True)

def tentar_destino_ate_dar_certo(planilha_id: str, headers, linhas):
    for tentativa in range(1, DESTINO_MAX_TENTATIVAS + 1):
        try:
            if tentativa > 1:
                atraso = DESTINO_BACKOFF_BASE_S * (2 ** (tentativa - 2))  # 5,10,20,40,80...
                print(f"🔁 Tentativa {tentativa}/{DESTINO_MAX_TENTATIVAS} para {planilha_id} — aguardando {atraso}s")
                time.sleep(atraso)
            atualizar_destino(planilha_id, headers, linhas)
            return  # sucesso
        except Exception as e:
            print(f"❌ Falha na tentativa {tentativa} para {planilha_id}: {e}")
            if tentativa == DESTINO_MAX_TENTATIVAS:
                print(f"⛔️ Não foi possível atualizar {planilha_id} após {DESTINO_MAX_TENTATIVAS} tentativas. Abortando.")
                sys.exit(1)

# === EXECUÇÃO ===
if __name__ == '__main__':
    cab, dados = ler_master_A_S()
    if not cab:
        print("❌ Nada para replicar na aba Carteira do master.")
        sys.exit(0)
    print(f"📦 Pronto para replicar: {len(dados)} linhas (A:S).")
    for pid in DESTINOS:
        tentar_destino_ate_dar_certo(pid, cab, dados)
    print("🏁 Réplica finalizada para todas as planilhas.")
