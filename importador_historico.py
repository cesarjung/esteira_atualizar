# importador_historico_rapido.py — BD_Carteira -> Historico na MESMA planilha
from datetime import datetime, timedelta
import gspread
import re, time
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError, WorksheetNotFound

# ========= CONFIG =========
ID_PLANILHA  = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM   = "BD_Carteira"
ABA_DESTINO  = "Historico"
CAM_CRED     = "credenciais.json"

FORMULA_AE = '=ARRAYFORMULA(SE(B3:B=""; ""; SE((AD3:AD="-") + ÉERROS(PROCH(AD3:AD; Esteira!$B$1:$K$1; 1; 0)); 0; 1)))'

RETRY_CRIT = (1, 3, 7, 15)
BASE_SERIAL = datetime(1899, 12, 30)

# ========= AUTH =========
scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds  = ServiceAccountCredentials.from_json_keyfile_name(CAM_CRED, scopes)
gc     = gspread.authorize(creds)

# ========= UTILS =========
def log(step, msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {step} {msg}", flush=True)

def _is_transient(e: Exception) -> bool:
    s = str(e)
    return any(t in s for t in ('[500]', '[503]', 'backendError', 'Internal error', 'service is currently unavailable', 'rateLimitExceeded'))

def _retry(delays, fn, *args, op_name=None, **kwargs):
    total = len(delays)
    for i, d in enumerate(delays, start=1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if not _is_transient(e):
                raise
            tag = f" ({op_name})" if op_name else ""
            log("RETRY", f"falha transitória{tag}: {e} — tentativa {i}/{total}; aguardando {d}s")
            if i == total:
                raise
            time.sleep(d)

def col_letter_to_index_0b(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1

# ========= TRATAMENTO =========
def to_serial_ddmmyyyy(val: str):
    v = (val or "").strip()
    if v.startswith("'"):
        v = v[1:]
    try:
        d = datetime.strptime(v, "%d/%m/%Y")
        return (d - BASE_SERIAL).days
    except:
        # aceita serial informado
        try:
            return int(float(v))
        except:
            return ""

def to_float_brl(val: str):
    s = (val or "").strip()
    if s.startswith("'"):
        s = s[1:]
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s) if s not in ("", "-", ".", "-.", ".-") else ""
    except:
        return ""

# Trata A..AK (37 colunas) da ORIGEM
# NÃO converter AC(origem) (i=28) — vira AD(destino) como texto
# Converte datas N,O (13,14) e números L,Y,AE,AF,AH,AI (11,24,30,31,33,34)
def tratar_bloco_AK(linha):
    out = []
    for i in range(37):
        val = linha[i].strip() if i < len(linha) and linha[i] is not None else ""
        if i in (13, 14):  # N, O
            out.append(to_serial_ddmmyyyy(val))
        elif i in (11, 24, 30, 31, 33, 34):  # L, Y, AE, AF, AH, AI
            out.append(to_float_brl(val))
        else:
            out.append(val[1:] if isinstance(val, str) and val.startswith("'") else val)
    return out

def parse_hist_date(a_str):
    s = (a_str or "").strip()
    try:
        return datetime.strptime(s, "%d/%m/%Y")
    except:
        try:
            x = int(float(s))
            return BASE_SERIAL + timedelta(days=x)
        except:
            return None

# ========= EXECUÇÃO =========
t0 = time.perf_counter()
hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
serial_hoje = (hoje - BASE_SERIAL).days
limite_data = hoje - timedelta(days=7)

log("INÍCIO", f"Janela: {limite_data.strftime('%d/%m/%Y')} .. {hoje.strftime('%d/%m/%Y')}")

book   = gc.open_by_key(ID_PLANILHA)
ws_src = book.worksheet(ABA_ORIGEM)
ws_dst = book.worksheet(ABA_DESTINO)

# 1) Ler ORIGEM (A4:AK) e filtrar linhas com A preenchido
log("ORIGEM", "Lendo A4:AK…")
orig_vals    = _retry(RETRY_CRIT, ws_src.get, 'A4:AK', op_name='get origem') or []
orig_validas = [l for l in orig_vals if l and (l[0] or "").strip() != ""]
log("ORIGEM", f"Linhas válidas: {len(orig_validas):,}")

# 2) Localizar bloco contíguo da última semana no HISTÓRICO lendo só A3:A
log("HIST", "Lendo A3:A para localizar bloco da última semana…")
colA = _retry(RETRY_CRIT, ws_dst.get, 'A3:A', op_name='get A3:A') or []
start_idx = end_idx = None
for i in range(len(colA)-1, -1, -1):
    d = parse_hist_date(colA[i][0] if colA[i] else "")
    if d and (limite_data <= d < hoje):
        end_idx = i if end_idx is None else end_idx
        start_idx = i
    elif end_idx is not None:
        break
bloco_len = (end_idx - start_idx + 1) if start_idx is not None else 0
if bloco_len:
    log("HIST", f"Bloco encontrado: linhas {3+start_idx}..{3+start_idx+bloco_len-1} ({bloco_len:,})")
else:
    log("HIST", "Sem bloco contíguo da última semana (seguirá só com novas).")

# 3) Tratar novas linhas (A..AK -> tipos corretos)
log("TRATAR", "Convertendo datas/números das novas linhas…")
tratadas = [tratar_bloco_AK(l) for l in orig_validas]

# 4) Montar payload:
#    A (datas), B..AD (29 colunas: A..AC -> B..AD), AF..AL (7 colunas: AE..AK -> AF..AL), AE (fórmula)
colA_total = []
if bloco_len > 0:
    colA_total.extend([[colA[start_idx + i][0]] for i in range(bloco_len)])
for _ in tratadas:
    colA_total.append([serial_hoje])

left_total = []
if bloco_len > 0:
    left_total = _retry(RETRY_CRIT, ws_dst.get,
                        f'B{3+start_idx}:AD{3+start_idx+bloco_len-1}', op_name='get B..AD bloco') or []
    left_total = [(r + [""]*29)[:29] for r in left_total]
left_total.extend([row[:29] for row in tratadas])

right_total = []
if bloco_len > 0:
    right_total = _retry(RETRY_CRIT, ws_dst.get,
                         f'AF{3+start_idx}:AL{3+start_idx+bloco_len-1}', op_name='get AF..AL bloco') or []
    right_total = [(r + [""]*7)[:7] for r in right_total]
right_total.extend([row[30:] for row in tratadas])

total_linhas = len(colA_total)
ultima_linha = 2 + total_linhas  # A3..A{ultima_linha}

# === AJUSTE: garantir tamanho da aba e limpar rabo com segurança ===
# Linhas necessárias até a última linha que vamos escrever
rows_needed = ultima_linha
# Garantir que exista a coluna AL
cols_needed = max(ws_dst.col_count, col_letter_to_index_0b('AL') + 1)

if ws_dst.row_count < rows_needed:
    _retry(RETRY_CRIT, ws_dst.resize, rows_needed, ws_dst.col_count, op_name='resize rows')

if ws_dst.col_count < cols_needed:
    _retry(RETRY_CRIT, ws_dst.resize, max(ws_dst.row_count, rows_needed), cols_needed, op_name='resize cols')

log("WRITE", f"Escrevendo {total_linhas:,} linhas (A + B..AD + AE fórmula + AF..AL)…")

# Limpeza do "rabo" A{ultima_linha+1}:AL — apenas se existir
tail_start = ultima_linha + 1
# Após resize, considere no mínimo rows_needed
max_row = max(ws_dst.row_count, rows_needed)
if tail_start <= max_row:
    _retry(RETRY_CRIT, ws_dst.spreadsheet.values_clear,
           f"'{ws_dst.title}'!A{tail_start}:AL", op_name='clear tail')

# limpar AE para a ARRAYFORMULA expandir
_retry(RETRY_CRIT, ws_dst.spreadsheet.values_clear, f"'{ws_dst.title}'!AE3:AE", op_name='clear AE')

# 5) Gravação
payload = []
# timestamp em A1 (opcional)
payload.append({"range": f"{ws_dst.title}!A1", "values": [[f"Atualizado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}"]]})
if total_linhas > 0:
    payload.append({"range": f"{ws_dst.title}!A3:A{ultima_linha}", "values": colA_total})
    payload.append({"range": f"{ws_dst.title}!B3",                 "values": left_total})
    payload.append({"range": f"{ws_dst.title}!AF3",                "values": right_total})
    payload.append({"range": f"{ws_dst.title}!AE3",                "values": [[FORMULA_AE]]})
else:
    payload.append({"range": f"{ws_dst.title}!AE3",                "values": [[FORMULA_AE]]})

_retry(RETRY_CRIT, ws_dst.spreadsheet.values_batch_update,
       body={"valueInputOption": "USER_ENTERED", "data": payload},
       op_name='values_batch_update')

log("FIM", f"✅ Histórico atualizado ({len(tratadas):,} novas linhas).")
log("DURAÇÃO", f"{time.perf_counter() - t0:.2f}s")
