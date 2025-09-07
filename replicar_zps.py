# replicar_zps.py — resiliente, rápido e com números contáveis; formatações opcionais
from datetime import datetime
import re
import time
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

# =========================
# CONFIGURAÇÃO
# =========================
ID_ORIGEM = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM = "zps"
CAMINHO_CREDENCIAIS = r"credenciais.json"

PLANILHAS_DESTINO = [
    "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c",
    "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M",
    "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c",
    "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw",
]

# Opções (OFF por padrão p/ performance)
APLICAR_FORMATO_DATAS   = False   # aplica dd/MM/yyyy nas colunas de data
APLICAR_FORMATO_NUMEROS = False   # aplica #,##0.00 nas colunas numéricas
CARIMBAR                 = True    # grava timestamp em uma célula
CARIMBAR_CEL             = "R1"    # alvo desejado; se não existir, cai no último col da aba
PULAR_DESTINO_SE_FALHAR  = True    # não derruba tudo se 1 destino insistir em falhar
HARD_CLEAR_BEFORE_WRITE  = False   # se True, limpa A:última_col antes de escrever (1 chamada extra)

# Colunas do seu caso: C(2), F(5), G(6) numéricas; A(0), N(13) datas
COLS_NUM_IDX  = {2, 5, 6}
COLS_DATE_IDX = {0, 13}

# Retries
RETRY_CRIT = (1, 3, 7, 15)  # operações críticas
RETRY_SOFT = (1,)           # cosméticos

# =========================
# AUTENTICAÇÃO
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(CAMINHO_CREDENCIAIS, scopes=SCOPES)
gc = gspread.authorize(creds)

# =========================
# UTILS
# =========================
def _is_transient(err: Exception) -> bool:
    s = str(err)
    return any(t in s for t in ("[500]", "[503]", "backendError", "rateLimitExceeded",
                                "Internal error", "service is currently unavailable"))

def _retry(delays, fn, *args, swallow_final=False, op_name=None, **kwargs):
    total = len(delays)
    for i, d in enumerate(delays, start=1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if not _is_transient(e):
                # Erros não transitórios só são engolidos se swallow_final=True
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

_re_num = re.compile(r"^\s*'?[-+]?[\d.,]+(?:e[-+]?\d+)?\s*$", re.IGNORECASE)

def limpar_num(texto: str):
    """Converte strings em float (remove R$, pontos de milhar, trata vírgula como decimal, etc.)."""
    if texto is None:
        return ""
    s = str(texto).strip()
    if not s:
        return ""
    if s.startswith("'"):
        s = s[1:]
    s = s.replace("R$", "").strip()
    s = re.sub(r"[^0-9,\.\-+eE]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""

def get_last_col_letter(n_cols: int) -> str:
    a1 = rowcol_to_a1(1, n_cols)  # ex.: 'K1'
    return re.sub(r"\d+", "", a1) # 'K'

def a1_parse(cell: str):
    m = re.match(r"^([A-Za-z]+)(\d+)$", cell.strip())
    if not m:
        return "A", 1
    return m.group(1).upper(), int(m.group(2))

def col_letter_to_index(letter: str) -> int:
    letter = letter.upper()
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx  # 1-based

def col_index_to_letter(index: int) -> str:
    # 1-based → letters
    res = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        res = chr(rem + ord('A')) + res
    return res

# =========================
# LEITURA DA ORIGEM
# =========================
print("📥 Lendo dados da aba 'zps'…")
ws_origem = gc.open_by_key(ID_ORIGEM).worksheet(ABA_ORIGEM)
valores = _retry(RETRY_CRIT, ws_origem.get_all_values, op_name="get_all_values")
if not valores:
    raise RuntimeError("A aba 'zps' está vazia.")

cabecalho = valores[0]
linhas_raw = valores[1:]
num_colunas = len(cabecalho)
total_linhas = len(linhas_raw)
print(f"✅ {total_linhas} linhas carregadas.\n")

# =========================
# TRATAMENTO DOS DADOS
# =========================
def tratar_linha(row):
    out = []
    for i in range(num_colunas):
        v = row[i].strip() if i < len(row) and row[i] is not None else ""
        if i in COLS_NUM_IDX:
            out.append(limpar_num(v))  # float → garante número contável
        else:
            out.append(v[1:] if isinstance(v, str) and v.startswith("'") else v)
    return out

linhas = [tratar_linha(r) for r in linhas_raw]
all_vals = [cabecalho] + linhas
nlin = len(all_vals)

# =========================
# FUNÇÕES DE ESCRITA/FORMATAÇÃO
# =========================
def escrever_tudo(ws_dest):
    last_col_letter = get_last_col_letter(num_colunas)
    rng = f"A1:{last_col_letter}{nlin}"

    if HARD_CLEAR_BEFORE_WRITE:
        _retry(RETRY_CRIT, ws_dest.spreadsheet.values_clear,
               f"'{ws_dest.title}'!A:{last_col_letter}", op_name="values_clear A:última")

    _retry(RETRY_CRIT, ws_dest.update,
           values=all_vals, range_name=rng,
           value_input_option="USER_ENTERED", op_name="update A1:última")

    # limpa “rabo” (linhas abaixo do dataset atual)
    try:
        max_rows = ws_dest.row_count
    except Exception:
        max_rows = nlin
    if max_rows > nlin:
        tail_rng = f"'{ws_dest.title}'!A{nlin+1}:{last_col_letter}"
        _retry(RETRY_CRIT, ws_dest.spreadsheet.values_clear, tail_rng, op_name="values_clear rabo")

def formatar_colunas(ws_dest):
    if not (APLICAR_FORMATO_DATAS or APLICAR_FORMATO_NUMEROS) or total_linhas == 0:
        return
    end_row = total_linhas + 1  # exclusivo
    reqs = []
    sheet_id = ws_dest.id

    if APLICAR_FORMATO_DATAS:
        for idx in sorted(COLS_DATE_IDX):
            if idx < num_colunas:
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

    if APLICAR_FORMATO_NUMEROS:
        for idx in sorted(COLS_NUM_IDX):
            if idx < num_colunas:
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
        _retry(RETRY_SOFT, ws_dest.spreadsheet.batch_update, {"requests": reqs},
               swallow_final=True, op_name="batch_update format")

def carimbar(ws_dest):
    if not CARIMBAR:
        return
    # Calcula posição segura p/ o carimbo:
    # - se CARIMBAR_CEL exceder as colunas/linhas da aba, usa última coluna disponível na linha 1
    try:
        desired_col_letters, desired_row = a1_parse(CARIMBAR_CEL)
        desired_col_idx_1b = col_letter_to_index(desired_col_letters)  # 1-based
    except Exception:
        desired_col_idx_1b, desired_row = 1, 1  # fallback A1

    try:
        max_cols = ws_dest.col_count
        max_rows = ws_dest.row_count
    except Exception:
        max_cols = num_colunas
        max_rows = max(nlin, 1)

    safe_col_1b = min(desired_col_idx_1b, max_cols if max_cols else 1)
    safe_row    = min(max(desired_row, 1), max_rows if max_rows else 1)
    safe_cell   = f"{col_index_to_letter(safe_col_1b)}{safe_row}"

    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    # Engole qualquer erro (inclusive 400) — carimbo nunca bloqueia destino
    try:
        _retry(RETRY_SOFT, ws_dest.update,
               values=[[f"Atualizado em: {ts}"]],
               range_name=safe_cell,
               value_input_option="RAW",
               swallow_final=True,
               op_name=f"carimbar {safe_cell}")
    except Exception as e:
        print(f"⚠️ Carimbo ignorado: {e}")

def replicar_para(planilha_id: str):
    try:
        print(f"➡️ Atualizando aba '{ABA_ORIGEM}' na planilha {planilha_id} …")
        ws_dest = gc.open_by_key(planilha_id).worksheet(ABA_ORIGEM)
        escrever_tudo(ws_dest)       # CRÍTICO (com retries)
        formatar_colunas(ws_dest)    # OPCIONAL (retry curto + swallow)
        carimbar(ws_dest)            # OPCIONAL (nunca falha o destino)
        print(f"✅ {total_linhas} linhas coladas e finalizadas.\n")
    except Exception as e:
        msg = f"⛔️ Erro ao atualizar {planilha_id}: {e}"
        if PULAR_DESTINO_SE_FALHAR:
            print(msg + " — pulando destino.\n")
        else:
            raise

# =========================
# EXECUÇÃO
# =========================
for pid in PLANILHAS_DESTINO:
    replicar_para(pid)
