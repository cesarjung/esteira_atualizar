# importador_carteira.py
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

CHUNK_ROWS_READ  = 1000   # leitura em blocos (Sheets → Python)
CHUNK_ROWS_WRITE = 2000   # escrita em blocos (Python → Sheets)
MAX_RETRIES      = 6
FORCAR_DESTAQ    = False  # destaque amarelo nas inserções
PAUSE_BETWEEN_READS = 0.6 # micro pausa entre chunks de leitura
MAX_BLANK_CHUNKS   = 2    # encerra após N chunks seguidos totalmente vazios

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
    import re
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

def with_retry(fn,*a,desc="",base=1,maxr=MAX_RETRIES,**k):
    r=0
    while True:
        try:
            return fn(*a,**k)
        except APIError as e:
            r+=1
            code=_status_code_from_apierror(e)
            if r>=maxr or (code is not None and code not in RETRYABLE_CODES):
                log(f"❌ {desc or fn.__name__}: {e}")
                raise
            # backoff com jitter
            s=min(60,base*2**(r-1)+random.uniform(0,.75))
            log(f"⚠️  {e} — retry {r}/{maxr-1} em {s:.1f}s ({desc or fn.__name__})")
            time.sleep(s)

# ---------- HELPERS ----------
def col_letter(n): return re.sub(r'\d','',rowcol_to_a1(1,n))
def a1index(L):    return a1_to_rowcol(f"{L}1")[1]

def ensure(ws,r,c):
    if ws.row_count<r or ws.col_count<c:
        log(f"🧩 resize → {r}x{c}")
        with_retry(ws.resize,r,c,desc="resize")

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
    # 1) tenta dd/mm/yyyy; 2) tenta serial Excel
    s=pd.to_datetime(sr,dayfirst=True,errors='coerce')
    m=s.isna()
    if m.any():
        n=pd.to_numeric(sr,errors='coerce')
        s=s.where(~m,pd.to_datetime(n,unit='D',origin='1899-12-30',errors='coerce'))
    return s.dt.strftime('%d/%m/%Y').where(s.notna(),"")

def highlight(ws,start,count,end_col="Q"):
    if not FORCAR_DESTAQ or count<=0: return
    try:
        from gspread_formatting import format_cell_range,CellFormat,Color
        rng=f"A{start}:{end_col}{start+count-1}"
        yellow=CellFormat(backgroundColor=Color(1,1,0.6))
        with_retry(format_cell_range,ws,rng,yellow,desc=f"highlight {rng}")
        log("🎨 Inserções destacadas em amarelo.")
    except Exception as e:
        log(f"⚠️  Falhou ao colorir: {e}")

# ---------- AUTH (Secret ou arquivo local) ----------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def make_creds():
    env = os.environ.get("GOOGLE_CREDENTIALS")
    if env:
        return Credentials.from_service_account_info(json.loads(env), scopes=SCOPES)
    return Credentials.from_service_account_file(pathlib.Path(CRED_JSON), scopes=SCOPES)

# ---------- Conectividade ----------
log("🔐 Autenticando…")
gc = gspread.authorize(make_creds())

log("📂 Abrindo planilhas…")
b_src = with_retry(gc.open_by_key, ORIGEM_ID,  desc="open origem")
b_dst = with_retry(gc.open_by_key, DESTINO_ID, desc="open destino")
w_src = with_retry(b_src.worksheet, ABA_ORIGEM,  desc="ws origem")
w_dst = with_retry(b_dst.worksheet, ABA_DESTINO, desc="ws destino")

ensure(w_dst,2,20)  # por causa do status em T2

# ---------- LEITURA ORIGEM ----------
lastL = col_letter(max(a1index(c) for c in COLS_ORIGEM))
rng   = f"A5:{lastL}"
log(f"🧭 Lendo cabeçalho (linha 5) e dados… ({rng})")
dat   = with_retry(w_src.get, rng, desc=f"get {rng}")
hdr, rows = dat[0], dat[1:]

idx      = [a1index(c)-1 for c in COLS_ORIGEM]
tbl      = [[r[i] if i<len(r) else "" for i in idx] for r in rows if r and str(r[0]).strip()]
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
    log(f"🚚 Escrevendo {rows0} linhas em blocos de {CHUNK_ROWS_WRITE}…")
    i=0
    while i<rows0:
        part=vals[i:i+CHUNK_ROWS_WRITE]
        a1=f"A{2+i}:{endL}{1+i+len(part)}"
        with_retry(w_dst.update, range_name=a1, values=part, value_input_option='USER_ENTERED')
        i+=len(part)
log("✅ Escrita de Carteira concluída.")

# ---------- Helpers de leitura robusta (Sheets→Python) ----------
def _reopen_ws(book, title):
    return with_retry(book.worksheet, title, desc=f"reopen {title}")

def batch_get_columns_chunked(book, ws, title, letters, start_row=2,
                              chunk_rows=CHUNK_ROWS_READ,
                              pause=PAUSE_BETWEEN_READS,
                              max_blank_chunks=MAX_BLANK_CHUNKS,
                              tag_log="batch_get"):
    """
    Lê várias colunas (letters) em blocos de linhas via batch_get.
    Para cada chunk (p.ex. 2..1001, 1002..2001, ...), faz UMA chamada batch_get
    com ranges [E2:E1001, F2:F1001, ...]. Se um chunk vier totalmente vazio em
    TODAS as colunas, conta blank_chunk; ao atingir max_blank_chunks, encerra.
    Reabre worksheet se a API responder 404, e re-tenta 5xx/429.
    Retorna dict {letter: [valores]} com mesmo comprimento (preenche "" se faltar).
    """
    results = {L: [] for L in letters}
    row = start_row
    blank_chunks = 0

    while True:
        end_row = row + chunk_rows - 1
        ranges = [f"{L}{row}:{L}{end_row}" for L in letters]

        # retry próprio com reabertura
        retries = 0
        while True:
            try:
                data = ws.batch_get(ranges)
                break
            except APIError as e:
                code = _status_code_from_apierror(e)
                retries += 1
                if code == 404 and retries < MAX_RETRIES:
                    print(f"[{tag_log}] ⚠️  404 em {title} — reabrindo worksheet e tentando de novo…", flush=True)
                    ws = _reopen_ws(book, title)
                    time.sleep(min(2.0, 0.5 + 0.2 * retries))
                    continue
                if retries >= MAX_RETRIES or code not in RETRYABLE_CODES:
                    raise
                print(f"[{tag_log}] ⚠️  {e} — retry {retries}/{MAX_RETRIES-1}", flush=True)
                time.sleep(min(60, 1.1*(2**(retries-1)) + random.uniform(0,0.75)))

        # data é uma lista na mesma ordem de 'ranges'
        non_empty_in_chunk = 0
        # normaliza para mesmo número de linhas por coluna
        max_len = 0
        cleaned_cols = []
        for idx, L in enumerate(letters):
            col_data = data[idx] if idx < len(data) else []
            # col_data = [[val], [val], ...]
            flat = [(r[0].strip() if r and len(r)>0 else "") for r in col_data]
            cleaned_cols.append(flat)
            if any(bool(x) for x in flat):
                non_empty_in_chunk += 1
            if len(flat) > max_len:
                max_len = len(flat)

        # pad a mesma altura
        for col_list in cleaned_cols:
            if len(col_list) < max_len:
                col_list.extend([""]*(max_len - len(col_list)))

        # append no dict final
        for L, flat in zip(letters, cleaned_cols):
            results[L].extend(flat)

        # heurística de parada
        if non_empty_in_chunk == 0:
            blank_chunks += 1
        else:
            blank_chunks = 0

        if blank_chunks >= max_blank_chunks:
            break

        row = end_row + 1
        time.sleep(pause)

    # strip final (remove cauda vazia sincronizada entre colunas)
    # encontra o último índice em que pelo menos UMA coluna tem conteúdo
    last_idx = -1
    for i in range(len(next(iter(results.values())))):
        if any((results[L][i] or "") for L in letters):
            last_idx = i
    if last_idx >= 0:
        for L in letters:
            results[L] = results[L][:last_idx+1]
    else:
        for L in letters:
            results[L] = []

    return results

def read_column_chunked(book, ws, title, letter, start_row=2):
    data = batch_get_columns_chunked(book, ws, title, [letter], start_row=start_row,
                                     chunk_rows=CHUNK_ROWS_READ, pause=PAUSE_BETWEEN_READS,
                                     max_blank_chunks=MAX_BLANK_CHUNKS, tag_log=f"get {title}!{letter}")
    return data[letter]

# ---------- Ler existentes (Carteira!A) uma vez ----------
exist_ids = set()
colA_vals = read_column_chunked(b_dst, w_dst, ABA_DESTINO, "A", start_row=2)
for v in colA_vals:
    v = (v or "").strip()
    if v:
        exist_ids.add(v)

larg  = max(cols0, 18)  # garante até R (col 18 -> idx 17)

# ---------- CICLO (E→A F→B C→H L→K D→R) ----------
log("🔗 CICLO (E→A, F→B, C→H, L→K, D→R)")
w_ciclo = with_retry(b_dst.worksheet, "CICLO", desc="ws CICLO")

cols_ciclo = ['E','F','C','L','D']
cdata = batch_get_columns_chunked(b_dst, w_ciclo, "CICLO", cols_ciclo,
                                  start_row=2, chunk_rows=CHUNK_ROWS_READ,
                                  pause=PAUSE_BETWEEN_READS, max_blank_chunks=MAX_BLANK_CHUNKS,
                                  tag_log="batch_get CICLO E,F,C,L,D")
E = cdata['E']; F = cdata['F']; C = cdata['C']; L_ = cdata['L']; D = cdata['D']

novas = []
N = max(len(E), len(F), len(C), len(L_), len(D))
for i in range(N):
    key = (E[i] if i < len(E) else "").strip()
    if not key or key in exist_ids:
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
    start = len(colA_vals) + 2  # posição inicial antes de append
    with_retry(w_dst.append_rows, novas, value_input_option='USER_ENTERED', desc="append CICLO")
    highlight(w_dst, start, len(novas), end_col="Q")
    log(f"✅ {len(novas)} linhas da CICLO inseridas.")
    # atualiza exist_ids e colA_vals localmente para uso pela LV
    for ln in novas:
        vid = (ln[0] or "").strip()
        if vid:
            exist_ids.add(vid)
            colA_vals.append(vid)
else:
    log("ℹ️  Nenhum novo ID da CICLO a inserir.")

# ---------- LV CICLO (B→A, C→B, 'SOMENTE LV'→H, Unidade→R) ----------
log("🔗 LV CICLO (B→A, C→B, 'SOMENTE LV'→H, Unidade→R)")
w_lv = with_retry(b_dst.worksheet, "LV CICLO", desc="ws LV")

cols_lv = ['A','B','C']  # Unidade, ID, Projeto
lvdata = batch_get_columns_chunked(b_dst, w_lv, "LV CICLO", cols_lv,
                                   start_row=2, chunk_rows=CHUNK_ROWS_READ,
                                   pause=PAUSE_BETWEEN_READS, max_blank_chunks=MAX_BLANK_CHUNKS,
                                   tag_log="batch_get LV CICLO A,B,C")
A_uni = lvdata['A']; B_id = lvdata['B']; C_prj = lvdata['C']

novas_lv=[]; cont={}
N = max(len(A_uni), len(B_id), len(C_prj))
for i in range(N):
    vid = (B_id[i] if i < len(B_id) else "").strip()
    if not vid or vid in exist_ids:
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
    start = len(colA_vals) + 2  # posição atual
    with_retry(w_dst.append_rows, novas_lv, value_input_option='USER_ENTERED', desc="append LV")
    highlight(w_dst, start, len(novas_lv), end_col="Q")
    resumo = ", ".join(f"{u}: {q}" for u,q in sorted(cont.items()))
    if resumo: log(f"📌 Unidades atribuídas (R): {resumo}")
    log(f"✅ {len(novas_lv)} linhas da LV CICLO inseridas.")
else:
    log("ℹ️  Nenhum novo ID da LV CICLO a inserir.")

# ---------- TIMESTAMP ----------
with_retry(w_dst.update, range_name="T2",
           values=[[f"Atualizado em: {now()}"]], value_input_option='USER_ENTERED')
total_final = len(colA_vals) + len(novas) + len(novas_lv)  # apenas para log
log(f"🎉 Fim — linhas totais (estimado) na Carteira após inserções: ~{total_final}.")
