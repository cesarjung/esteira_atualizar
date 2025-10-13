import time, random, re, unicodedata
from datetime import datetime

import os, json, pathlib
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1, a1_to_rowcol

# ---------- CONFIG ----------
ORIGEM_ID     = '1lUNIeWCddfmvJEjWJpQMtuR4oRuMsI3VImDY0xBp3Bs'
DESTINO_ID    = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_ORIGEM    = 'Carteira'
ABA_DESTINO   = 'Carteira'
CRED_JSON     = 'credenciais.json'   # fallback local

# Colunas da ORIGEM na ordem desejada
COLS_ORIGEM   = ['A','Z','B','C','D','E','U','T','N','AA','AB','CN','CQ','CR','CS','BQ','CE','V']
# Colunas de data (da ORIGEM) que viram L..Q no DESTINO
DATE_LETTERS  = ['CN','CQ','CR','CS','BQ','CE']  # 6 datas → L..Q

CHUNK_ROWS    = 2000
MAX_RETRIES   = 6
FORCAR_DESTAQ = False  # destaque amarelo nas inserções

# Mapeamento de Unidades (usado em CICLO.D→R e LV.A→R)
MAP_UNIDADE = {
    'CONQUISTA': 'VITORIA DA CONQUISTA',
    'ITAPETINGA': 'ITAPETINGA',
    'JEQUIE': 'JEQUIE',
    'GUANAMBI': 'GUANAMBI',
    'BARREIRAS': 'BARREIRAS',
    'LAPA': 'BOM JESUS DA LAPA',
    'IRECE': 'IRECE',
    'IBOTIRAMA': 'IBOTIRAMA',
    'BRUMADO': 'BRUMADO',
    'LIVRAMENTO': 'LIVRAMENTO',
}

# ---------- LOG & RETRY ----------
def now(): return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
def log(msg): print(f"[{now()}] {msg}", flush=True)

RETRYABLE_CODES = {429, 500, 502, 503, 504}

def _status_code_from_apierror(e: APIError):
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

def _sleep_backoff(attempt, base=1.0):
    time.sleep(min(60.0, base * (2 ** (attempt - 1)) + random.uniform(0, 0.8)))

def with_retry(fn, *a, desc="", base=1, maxr=MAX_RETRIES, **k):
    r = 0
    while True:
        try:
            return fn(*a, **k)
        except APIError as e:
            r += 1
            code = _status_code_from_apierror(e)
            if r >= maxr or (code is not None and code not in RETRYABLE_CODES):
                log(f"❌ {desc or fn.__name__}: {e}")
                raise
            if code == 429:
                wait = min(60.0, 5.0 * r + random.uniform(0, 2.0))
                log(f"⚠️  {e} — retry {r}/{maxr-1} em {wait:.1f}s ({desc or fn.__name__})")
                time.sleep(wait)
            else:
                s = min(60, base * 2 ** (r - 1) + random.uniform(0, .75))
                log(f"⚠️  {e} — retry {r}/{maxr-1} em {s:.1f}s ({desc or fn.__name__})")
                time.sleep(s)

# ---------- HELPERS ----------
def col_letter(n): return re.sub(r'\d','',rowcol_to_a1(1,n))
def a1index(L):    return a1_to_rowcol(f"{L}1")[1]

def ensure(ws, r, c):
    if ws.row_count < r or ws.col_count < c:
        log(f"🧩 resize → {r}x{c}")
        with_retry(ws.resize, r, c, desc="resize")

def norm_acento_up(s: str) -> str:
    if s is None: return ''
    s = str(s).strip().upper()
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def normalize(v):
    if v is None: return ""
    try:
        if pd.isna(v): return ""
    except: pass
    if isinstance(v,(pd.Timestamp,datetime)): return v.strftime("%d/%m/%Y")
    return v

def df2values(df): return [[normalize(c) for c in row] for row in df.values.tolist()]

def parse_dates(sr):
    s = pd.to_datetime(sr, dayfirst=True, errors='coerce')
    m = s.isna()
    if m.any():
        n = pd.to_numeric(sr, errors='coerce')
        s = s.where(~m, pd.to_datetime(n, unit='D', origin='1899-12-30', errors='coerce'))
    return s.dt.strftime('%d/%m/%Y').where(s.notna(), "")

# ---------- Leituras RESILIENTES ----------
def get_with_retry(ws, a1_range: str, max_tries=MAX_RETRIES, base_sleep=1.1):
    for attempt in range(1, max_tries + 1):
        try:
            return ws.get(a1_range)
        except APIError as e:
            code = _status_code_from_apierror(e)
            if code not in RETRYABLE_CODES or attempt >= max_tries:
                raise
            if code == 429:
                wait = min(60.0, 5.0 * attempt + random.uniform(0, 2.0))
                log(f"[get_with_retry] 429 em {a1_range} — aguardando {wait:.1f}s…")
                time.sleep(wait)
            else:
                log(f"[get_with_retry] {code} em {a1_range} — retry {attempt}/{max_tries-1}")
                _sleep_backoff(attempt, base_sleep)
    return []

def batch_get_with_retry(ws, ranges, max_tries=MAX_RETRIES, base_sleep=1.1, desc="batch_get"):
    for attempt in range(1, max_tries + 1):
        try:
            return ws.batch_get(ranges)
        except APIError as e:
            code = _status_code_from_apierror(e)
            if code not in RETRYABLE_CODES or attempt >= max_tries:
                log(f"❌ {desc}: {e}")
                raise
            if code == 429:
                wait = min(60.0, 5.0 * attempt + random.uniform(0, 2.0))
                log(f"[{desc}] 429 — aguardando {wait:.1f}s…")
                time.sleep(wait)
            else:
                log(f"[{desc}] {code} — retry {attempt}/{max_tries-1}")
                _sleep_backoff(attempt, base_sleep)

# ---- leitura delimitada de UMA coluna (fallback) ----
def load_col_bounded(ws, L, chunk_rows=10000):
    end_row = max(ws.row_count or 0, 2)
    if end_row < 2:
        return []
    a1 = f"{L}2:{L}{end_row}"
    try:
        raw = get_with_retry(ws, a1)
        return [(r[0].strip() if r and r[0] else "") for r in raw]
    except APIError as e:
        log(f"[load_col_bounded] downgrade para chunks ({L}) por erro: {e}")
        out = []
        start = 2
        while start <= end_row:
            stop = min(end_row, start + chunk_rows - 1)
            part = get_with_retry(ws, f"{L}{start}:{L}{stop}")
            out.extend([(r[0].strip() if r and r[0] else "") for r in part])
            start = stop + 1
        return out

# ---- leitura delimitada de VÁRIAS colunas, com batch e fallback em chunks ----
def load_cols_bounded(ws, letters, chunk_rows=5000):
    """
    Retorna dict { 'E': [...], 'F': [...], ... } lendo de 2..row_count.
    Tenta uma única chamada batch_get para TODAS as colunas.
    Se der erro, faz fallback por CHUNKS mas ainda batendo em batch (todas as letras de uma vez).
    """
    letters = list(letters)
    end_row = max(ws.row_count or 0, 2)
    out = {L: [] for L in letters}
    if end_row < 2:
        return out

    # tentativa 1: uma chamada batch para tudo
    try:
        ranges = [f"{L}2:{L}{end_row}" for L in letters]
        res = batch_get_with_retry(ws, ranges, desc=f"batch_get {ws.title} {','.join(letters)}")
        for L, r in zip(letters, res):
            out[L] = [(row[0].strip() if row and row[0] else "") for row in r]
        return out
    except APIError as e:
        log(f"[load_cols_bounded] fallback em chunks por erro: {e}")

    # fallback por chunks (mas ainda em batch por chunk)
    start = 2
    while start <= end_row:
        stop = min(end_row, start + chunk_rows - 1)
        ranges = [f"{L}{start}:{L}{stop}" for L in letters]
        res = batch_get_with_retry(ws, ranges, desc=f"batch_get {ws.title} {start}-{stop}")
        for L, r in zip(letters, res):
            out[L].extend([(row[0].strip() if row and row[0] else "") for row in r])
        start = stop + 1
    return out

def highlight(ws, start, count, end_col="Q"):
    if not FORCAR_DESTAQ or count <= 0: return
    try:
        from gspread_formatting import format_cell_range,CellFormat,Color
        rng=f"A{start}:{end_col}{start+count-1}"
        yellow=CellFormat(backgroundColor=Color(1,1,0.6))
        with_retry(format_cell_range, ws, rng, yellow, desc=f"highlight {rng}")
        log("🎨 Inserções destacadas em amarelo.")
    except Exception as e:
        log(f"⚠️  Falhou ao colorir: {e}")

# ---------- AUTH ----------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def make_creds():
    env = os.environ.get("GOOGLE_CREDENTIALS")
    if env:
        return Credentials.from_service_account_info(json.loads(env), scopes=SCOPES)
    return Credentials.from_service_account_file(pathlib.Path(CRED_JSON), scopes=SCOPES)

log("🔐 Autenticando…")
gc = gspread.authorize(make_creds())

log("📂 Abrindo planilhas…")
b_src = with_retry(gc.open_by_key, ORIGEM_ID,  desc="open origem")
b_dst = with_retry(gc.open_by_key, DESTINO_ID, desc="open destino")
w_src = with_retry(b_src.worksheet, ABA_ORIGEM,  desc="ws origem")
w_dst = with_retry(b_dst.worksheet, ABA_DESTINO, desc="ws destino")

ensure(w_dst, 2, 20)  # por causa do status em T2

# ---------- LEITURA ORIGEM ----------
lastL = col_letter(max(a1index(c) for c in COLS_ORIGEM))
rng   = f"A5:{lastL}"
log(f"🧭 Lendo cabeçalho (linha 5) e dados… ({rng})")
dat   = with_retry(w_src.get, rng, desc=f"get {rng}")
hdr, rows = dat[0], dat[1:]

idx      = [a1index(c)-1 for c in COLS_ORIGEM]
tbl      = [[r[i] if i<len(r) else "" for i in idx] for r in rows if r and r[0].strip()]
df       = pd.DataFrame(tbl, columns=[hdr[i] if i<len(hdr) else "" for i in idx])
log(f"🧱 Origem: {len(df)} linhas × {len(df.columns)} colunas")

# datas L..Q
pos = {l:i for i,l in enumerate(COLS_ORIGEM)}
for l in DATE_LETTERS:
    p = pos[l]
    if p < len(df.columns):
        df.iloc[:,p] = parse_dates(df.iloc[:,p])

# num AC (se existir)
if "AC" in df.columns:
    df["AC"] = pd.to_numeric(df["AC"].astype(str)
                             .str.replace("R$","",regex=False)
                             .str.replace(".","",regex=False)
                             .str.replace(",",".",regex=False),
                             errors='coerce')

# ---------- ESCRITA PRINCIPAL ----------
vals  = df2values(df)
rows0 = len(vals)
cols0 = len(df.columns)

ensure(w_dst, rows0+1, max(20, cols0))
endL  = col_letter(max(1, cols0))
with_retry(w_dst.batch_clear, [f"A2:{endL}"], desc="clear dados")
with_retry(w_dst.update,
           range_name=f"A1:{rowcol_to_a1(1,cols0)}",
           values=[list(df.columns)],
           value_input_option='USER_ENTERED')
with_retry(w_dst.update, range_name="T2",
           values=[[f"Atualizando... {now()}"]],
           value_input_option='USER_ENTERED')

if vals:
    log(f"🚚 Escrevendo {rows0} linhas em blocos de {CHUNK_ROWS}…")
    i=0
    while i<rows0:
        part=vals[i:i+CHUNK_ROWS]
        a1=f"A{2+i}:{endL}{1+i+len(part)}"
        with_retry(w_dst.update, range_name=a1, values=part, value_input_option='USER_ENTERED')
        i+=len(part)
log("✅ Escrita de Carteira concluída.")

# ---------- CICLO (E→A F→B C→H L→K D→R) ----------
log("🔗 CICLO (E→A, F→B, C→H, L→K, D→R)")
w_ciclo = with_retry(b_dst.worksheet, "CICLO", desc="ws CICLO")

# Lê TODAS as colunas de uma vez (com fallback em chunks)
ciclo_cols = load_cols_bounded(w_ciclo, ["E","F","C","L","D"])
E  = ciclo_cols["E"]   # → A (ID/chave)
F  = ciclo_cols["F"]   # → B
C  = ciclo_cols["C"]   # → H
L_ = ciclo_cols["L"]   # → K
D  = ciclo_cols["D"]   # → R (unidade mapeada)

# IDs já existentes (delimitado até o fim atual da planilha de destino)
exist_A = load_col_bounded(w_dst, "A")
exist = set(v for v in exist_A if v)

larg  = max(cols0, 18)  # garante até R (col 18 -> idx 17)
novas = []
N = max(len(E), len(F), len(C), len(L_), len(D))
for i in range(N):
    key = E[i] if i < len(E) else ""
    if not key or key in exist:
        continue

    uni_raw = D[i] if i < len(D) else ""
    uni_map = MAP_UNIDADE.get(norm_acento_up(uni_raw), uni_raw.strip())

    linha = [''] * larg
    linha[0]  = key                             # A ← E
    if larg >= 2:  linha[1]  = F[i] if i<len(F)  else ""  # B ← F
    if larg >= 8:  linha[7]  = C[i] if i<len(C)  else ""  # H ← C
    if larg >= 11: linha[10] = L_[i] if i<len(L_) else "" # K ← L
    if larg >= 18: linha[17] = uni_map                        # R ← D
    novas.append(linha)

if novas:
    start = rows0 + 2
    with_retry(w_dst.append_rows, novas, value_input_option='USER_ENTERED', desc="append CICLO")
    highlight(w_dst, start, len(novas), end_col="Q")
    rows0 += len(novas)
    log(f"✅ {len(novas)} linhas da CICLO inseridas.")
else:
    log("ℹ️  Nenhum novo ID da CICLO a inserir.")

# ---------- LV CICLO (B→A, C→B, 'SOMENTE LV'→H, Unidade→R) ----------
log("🔗 LV CICLO (B→A, C→B, 'SOMENTE LV'→H, Unidade→R)")
w_lv = with_retry(b_dst.worksheet, "LV CICLO", desc="ws LV")

# Também em batch (A,B,C)
lv_cols = load_cols_bounded(w_lv, ["A","B","C"])
A_uni = lv_cols["A"]   # Unidade (bruta)
B_id  = lv_cols["B"]   # → A
C_prj = lv_cols["C"]   # → B

# Reaproveita o conjunto existente lido antes
exist = set(exist)

novas_lv=[]; cont={}
N = max(len(A_uni), len(B_id), len(C_prj))
for i in range(N):
    vid = B_id[i] if i < len(B_id) else ""
    if not vid or vid in exist:
        continue

    uni_raw = A_uni[i] if i < len(A_uni) else ""
    uni_map = MAP_UNIDADE.get(norm_acento_up(uni_raw), uni_raw.strip())

    linha = [''] * larg
    linha[0]  = vid                            # A ← B
    if larg >= 2:  linha[1]  = C_prj[i] if i<len(C_prj) else ""  # B ← C
    if larg >= 8:  linha[7]  = "SOMENTE LV"                       # H
    if larg >= 18: linha[17] = uni_map                            # R ← Unidade
    novas_lv.append(linha)
    cont[uni_map] = cont.get(uni_map, 0) + 1

if novas_lv:
    start = rows0 + 2
    with_retry(w_dst.append_rows, novas_lv, value_input_option='USER_ENTERED', desc="append LV")
    highlight(w_dst, start, len(novas_lv), end_col="Q")
    rows0 += len(novas_lv)
    resumo = ", ".join(f"{u}: {q}" for u,q in sorted(cont.items()))
    if resumo: log(f"📌 Unidades atribuídas (R): {resumo}")
    log(f"✅ {len(novas_lv)} linhas da LV CICLO inseridas.")
else:
    log("ℹ️  Nenhum novo ID da LV CICLO a inserir.")

# ---------- TIMESTAMP ----------
with_retry(w_dst.update, range_name="T2",
           values=[[f"Atualizado em: {now()}"]], value_input_option='USER_ENTERED')
log(f"🎉 Fim — {rows0} linhas totais inseridas/atualizadas.")
