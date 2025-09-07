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
# Fallback local (se rodar na sua máquina). No Actions vem do Secret.
CRED_JSON     = 'credenciais.json'

# Colunas da ORIGEM na ordem desejada
COLS_ORIGEM   = ['A','Z','B','C','D','E','U','T','N','AA','AB','CN','CQ','CR','CS','BQ','CE','V']
# Colunas de data (da ORIGEM) que viram L..Q no DESTINO
DATE_LETTERS  = ['CN','CQ','CR','CS','BQ','CE']  # 6 datas → L..Q

CHUNK_ROWS    = 2000
MAX_RETRIES   = 6
FORCAR_DESTAQ = True  # destaque amarelo nas inserções

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

def with_retry(fn,*a,desc="",base=1,maxr=MAX_RETRIES,**k):
    r=0
    while True:
        try: return fn(*a,**k)
        except APIError as e:
            r+=1
            if r>=maxr: log(f"❌ {desc or fn.__name__}: {e}"); raise
            s=min(60,base*2**(r-1)+random.uniform(0,.75))
            log(f"⚠️  {e} — retry {r}/{maxr-1} em {s:.1f}s ({desc or fn.__name__})")
            time.sleep(s)

# ---------- HELPERS ----------
def col_letter(n): return re.sub(r'\d','',rowcol_to_a1(1,n))     # 1-based -> 'A','AA',...
def a1index(L):    return a1_to_rowcol(f"{L}1")[1]                # 1-based
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
    s=pd.to_datetime(sr,dayfirst=True,errors='coerce')
    m=s.isna()
    if m.any():
        n=pd.to_numeric(sr,errors='coerce')
        s=s.where(~m,pd.to_datetime(n,unit='D',origin='1899-12-30',errors='coerce'))
    return s.dt.strftime('%d/%m/%Y').where(s.notna(),"")

def load_col(ws,L):
    raw=with_retry(ws.get,f"{L}2:{L}",desc=f"get {ws.title}!{L}2:{L}")
    return [(r[0].strip() if r and r[0] else "") for r in raw]

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

# ---------- AUTH (portável: Secret ou arquivo local) ----------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def make_creds():
    env = os.environ.get("GOOGLE_CREDENTIALS")
    if env:
        # Secret no Actions (JSON em string)
        info = json.loads(env)
        return Credentials.from_service_account_info(info, scopes=SCOPES)
    # Fallback local
    cred_path = pathlib.Path(CRED_JSON)
    return Credentials.from_service_account_file(cred_path, scopes=SCOPES)

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

idx      = [a1index(c)-1 for c in COLS_ORIGEM]  # 0-based
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
E  = load_col(w_ciclo, "E")   # → A (ID/chave)
F  = load_col(w_ciclo, "F")   # → B
C  = load_col(w_ciclo, "C")   # → H
L_ = load_col(w_ciclo, "L")   # → K   (CORRETO)
D  = load_col(w_ciclo, "D")   # → R (unidade mapeada)

# somente itens que NÃO vieram na etapa anterior (IDs já presentes em A)
exist = set(r[0].strip() for r in with_retry(w_dst.get, "A2:A", desc="get A2:A") if r and r[0].strip())

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
    if larg >= 11: linha[10] = L_[i] if i<len(L_) else "" # K ← L   (CORRETO)
    if larg >= 18: linha[17] = uni_map                      # R ← D (mapeado)
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
A_uni = load_col(w_lv, "A")   # Unidade (bruta)
B_id  = load_col(w_lv, "B")   # → A
C_prj = load_col(w_lv, "C")   # → B

# IDs já existentes (inclui o que veio da etapa principal + CICLO)
exist = set(r[0].strip() for r in with_retry(w_dst.get, "A2:A", desc="get A2:A again") if r and r[0].strip())

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
