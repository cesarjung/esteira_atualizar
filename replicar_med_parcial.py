# replicar_med_parcial.py — sem pular destinos; tenta várias vezes e falha o script se não preencher
from datetime import datetime
import re
import time
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

# ========== CONFIG ==========
ID_PLANILHA_DESTINO_ORIGINAL = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"  # fonte onde "MED PARCIAL" já está
ABA_ORIGEM = "MED PARCIAL"
PLANILHAS_DESTINO = [
    "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c",
    "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M",
    "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c",
    "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw",
]
CAMINHO_CREDENCIAIS = "credenciais.json"

# faixa fixa: A..Q (17 colunas)
N_COLS = 17
RANGE_ORIGEM = "A1:Q"

# Opções (OFF por padrão p/ performance)
APLICAR_FORMATO_DATAS   = False      # ligue se quiser dd/MM/yyyy nas colunas de data
APLICAR_FORMATO_NUMEROS = False      # ligue se quiser #,##0.00 nas colunas numéricas
CARIMBAR                 = True
CARIMBAR_CEL             = "R1"      # se não existir, cai na última coluna disponível (linha 1)
HARD_CLEAR_BEFORE_WRITE  = False     # limpa A:Q antes de escrever (1 chamada extra)

# Colunas a tratar (0-based). Seu script tratava F (5) e formatava G (6) e K (10).
COLS_NUM_IDX  = {5, 6, 10}   # F, G, K como números
COLS_DATE_IDX = set()        # adicione índices se tiver colunas de datas

# Retries (operações individuais)
RETRY_CRIT = (1, 3, 7, 15)   # operações críticas
RETRY_SOFT = (1,)            # cosméticos

# Tentativas externas por DESTINO (não pular nunca)
DESTINO_MAX_TENTATIVAS = 5
DESTINO_BACKOFF_BASE_S = 5   # 5s, 10s, 20s, 40s, 80s…

# ========== AUTH ==========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(CAMINHO_CREDENCIAIS, scopes=SCOPES)
gc = gspread.authorize(creds)

# ========== UTILS ==========
def _is_transient(e: Exception) -> bool:
    s = str(e)
    return any(t in s for t in ("[500]", "[503]", "backendError", "rateLimitExceeded",
                                "Internal error", "service is currently unavailable"))

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

def get_last_col_letter(n_cols: int) -> str:
    a1 = rowcol_to_a1(1, n_cols)  # ex.: 'Q1'
    return re.sub(r"\d+", "", a1) # 'Q'

def a1_parse(cell: str):
    m = re.match(r"^([A-Za-z]+)(\d+)$", cell.strip())
    if not m:
        return "A", 1
    return m.group(1).upper(), int(m.group(2))

def col_letter_to_index_1b(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx

def col_index_to_letter_1b(index: int) -> str:
    res = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        res = chr(rem + ord('A')) + res
    return res

# ========== TRATAMENTO ==========
def limpar_valor(valor):
    """Converte strings em float (R$, pontos de milhar, vírgula decimal). Se não der, retorna ''."""
    if valor is None:
        return ""
    s = str(valor).strip()
    if not s:
        return ""
    if s.startswith("'"):
        s = s[1:]
    s = re.sub(r"[^\d,.\-+eE]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""

def tratar_linha(row, n_cols):
    # garante exatamente n_cols células
    r = [(c if c is not None else "") for c in row[:n_cols]] + [""] * max(0, n_cols - len(row))
    # remove apóstrofo inicial de todos os textos
    for i in range(n_cols):
        if isinstance(r[i], str) and r[i].startswith("'"):
            r[i] = r[i][1:]
    # força números nas colunas configuradas
    for idx in COLS_NUM_IDX:
        if idx < n_cols:
            r[idx] = limpar_valor(r[idx])
    return r

# ========== LEITURA ==========
print(f"📥 Lendo {ID_PLANILHA_DESTINO_ORIGINAL}/{ABA_ORIGEM} ({RANGE_ORIGEM})…")
ws_src = gc.open_by_key(ID_PLANILHA_DESTINO_ORIGINAL).worksheet(ABA_ORIGEM)
vals = _retry(RETRY_CRIT, ws_src.get, RANGE_ORIGEM, op_name="get origem") or []
if not vals:
    print("⚠️ Nada a replicar (faixa vazia).")
    sys.exit(0)

header = (vals[0] + [""] * N_COLS)[:N_COLS]
rows_raw = vals[1:]
rows = []
for r in rows_raw:
    if not any((c or "").strip() for c in r[:N_COLS]):  # ignora linhas totalmente vazias
        continue
    rows.append(tratar_linha(r, N_COLS))

all_vals = [header] + rows
nlin = len(all_vals)
last_col_letter = get_last_col_letter(N_COLS)
print(f"✅ {len(rows)} linhas preparadas.\n")

# ========== ESCRITA / FORMATAÇÃO ==========
def ensure_grid(ws, min_rows: int, min_cols: int):
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
    except Exception:
        cur_rows = cur_cols = 0
    if cur_rows < min_rows or cur_cols < min_cols:
        _retry(RETRY_CRIT, ws.resize, rows=max(cur_rows, min_rows), cols=max(cur_cols, min_cols), op_name="resize")

def escrever_tudo(ws):
    rng = f"A1:{last_col_letter}{nlin}"
    ensure_grid(ws, min_rows=nlin, min_cols=N_COLS)

    if HARD_CLEAR_BEFORE_WRITE:
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, f"'{ws.title}'!A:{last_col_letter}", op_name="values_clear A:Q")

    _retry(RETRY_CRIT, ws.update,
           values=all_vals, range_name=rng,
           value_input_option="USER_ENTERED", op_name="update A1:Q")

    # limpa “rabo” (linhas abaixo do dataset)
    try:
        max_rows = ws.row_count
    except Exception:
        max_rows = nlin
    if max_rows > nlin:
        tail_rng = f"'{ws.title}'!A{nlin+1}:{last_col_letter}"
        _retry(RETRY_CRIT, ws.spreadsheet.values_clear, tail_rng, op_name="values_clear rabo")

def formatar(ws):
    if not (APLICAR_FORMATO_DATAS or APLICAR_FORMATO_NUMEROS) or len(rows) == 0:
        return
    end_row = len(rows) + 1  # exclusivo
    reqs = []
    sid = ws.id

    if APLICAR_FORMATO_NUMEROS:
        for idx in sorted(COLS_NUM_IDX):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row,
                                  "startColumnIndex": idx, "endColumnIndex": idx + 1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    if APLICAR_FORMATO_DATAS and COLS_DATE_IDX:
        for idx in sorted(COLS_DATE_IDX):
            if idx < N_COLS:
                reqs.append({
                    "repeatCell": {
                        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row,
                                  "startColumnIndex": idx, "endColumnIndex": idx + 1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
    if reqs:
        _retry(RETRY_SOFT, ws.spreadsheet.batch_update, {"requests": reqs},
               swallow_final=True, op_name="batch_update format")

def carimbar(ws):
    if not CARIMBAR:
        return
    # célula segura (se R1 não existir, usa última coluna da linha 1)
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
           values=[[f"Atualizado em: {ts}"]],
           range_name=safe_cell,
           value_input_option="RAW",
           swallow_final=True,
           op_name=f"carimbar {safe_cell}")

def replicar_para(planilha_id: str):
    print(f"➡️ Atualizando {planilha_id}/{ABA_ORIGEM} …")
    ws = gc.open_by_key(planilha_id).worksheet(ABA_ORIGEM)
    escrever_tudo(ws)   # crítico
    formatar(ws)        # opcional
    carimbar(ws)        # opcional e seguro
    print(f"✅ Replicado {len(rows)} linhas para {planilha_id}.")

def tentar_destino_ate_dar_certo(planilha_id: str):
    for tentativa in range(1, DESTINO_MAX_TENTATIVAS + 1):
        try:
            if tentativa > 1:
                atraso = DESTINO_BACKOFF_BASE_S * (2 ** (tentativa - 2))  # 5,10,20,40,...
                print(f"🔁 Tentativa {tentativa}/{DESTINO_MAX_TENTATIVAS} para {planilha_id} — aguardando {atraso}s")
                time.sleep(atraso)
            replicar_para(planilha_id)
            return  # sucesso, sai
        except Exception as e:
            print(f"❌ Falha na tentativa {tentativa} para {planilha_id}: {e}")
            if tentativa == DESTINO_MAX_TENTATIVAS:
                # Não pode deixar planilha sem preencher: aborta o script com RC!=0
                print(f"⛔️ Não foi possível atualizar {planilha_id} após {DESTINO_MAX_TENTATIVAS} tentativas. Abortando.")
                sys.exit(1)

# ========== EXECUÇÃO ==========
print(f"📦 Pronto para replicar: {len(rows)} linhas (A:Q).")
for pid in PLANILHAS_DESTINO:
    tentar_destino_ate_dar_certo(pid)
print("🏁 Processo concluído.")
