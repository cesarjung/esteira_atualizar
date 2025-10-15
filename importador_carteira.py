# importador_carteira.py — Carteira limpa + CICLO/LV (CSV com retry + fallback Sheets) — FIX sem apóstrofo
# -*- coding: utf-8 -*-

import os, re, json, time, random, unicodedata, pathlib, io
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1, a1_to_rowcol
from google.oauth2.service_account import Credentials as SACreds
from google.auth.transport.requests import Request as GARequest

# ───────── CONFIG ─────────
ORIGEM_ID   = '1lUNIeWCddfmvJEjWJpQMtuR4oRuMsI3VImDY0xBp3Bs'
DESTINO_ID  = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_ORIGEM  = 'Carteira'
ABA_DESTINO = 'Carteira'
CRED_JSON   = 'credenciais.json'  # pode ser env GOOGLE_CREDENTIALS (JSON) ou GOOGLE_APPLICATION_CREDENTIALS (caminho)

COLS_ORIGEM  = ['A','Z','B','C','D','E','U','T','N','AA','AB','CN','CQ','CR','CS','BQ','CE','V']
DATE_LETTERS = ['CN','CQ','CR','CS','BQ','CE']

CHUNK_ROWS_WRITE = 2000
MAX_RETRIES      = 5
RETRYABLE_CODES  = {429, 500, 502, 503, 504}
FORCAR_DESTAQ    = False

# leitura Sheets (fallback) em micro-ranges tolerantes
BATCH_ROWS_PER_RANGE  = 120
RANGES_PER_BATCH_CALL = 25
FETCH_ROWS_STEP       = 120

# Colunas que o cliente quer SEM apóstrofo (números e datas), pela letra da aba de destino
NUM_COLS_FIX  = ['J', 'K']         # moeda/número
DATE_COLS_FIX = ['L', 'M', 'P', 'Q']  # datas dd/MM/yyyy

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

# ───────── LOG / RETRY ─────────
def now(): return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
def log(msg): print(f"[{now()}] {msg}", flush=True)
def _status_code_from_apierror(e: APIError):
    m = re.search(r"\[(\d+)\]", str(e)); return int(m.group(1)) if m else None

def with_retry(fn, *a, desc="", base=0.6, maxr=MAX_RETRIES, **k):
    r=0
    while True:
        try:
            return fn(*a, **k)
        except APIError as e:
            r+=1; code=_status_code_from_apierror(e)
            if r>=maxr or (code is not None and code not in RETRYABLE_CODES):
                log(f"❌ {desc or fn.__name__}: {e}"); raise
            s=min(20, base*(2**(r-1)) + random.uniform(0,0.5))
            log(f"⚠️  {e} — retry {r}/{maxr-1} em {s:.1f}s ({desc or fn.__name__})")
            time.sleep(s)

# ───────── HELPERS ─────────
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

def normalize_cell(v):
    try:
        if pd.isna(v): return ""
    except: pass
    if isinstance(v,(pd.Timestamp,datetime)): return v.strftime("%d/%m/%Y")
    return v if v is not None else ""

def df2values(df): return [[normalize_cell(c) for c in row] for row in df.values.tolist()]

def parse_dates(series_like: pd.Series) -> pd.Series:
    s = pd.to_datetime(series_like, format="%d/%m/%Y", errors='coerce')
    m = s.isna()
    if m.any():
        s2 = pd.to_datetime(series_like, dayfirst=True, errors='coerce')
        s = s.where(~m, s2); m = s.isna()
    if m.any():
        n = pd.to_numeric(series_like, errors='coerce')
        s = s.where(~m, pd.to_datetime(n, unit='D', origin='1899-12-30', errors='coerce'))
    return s.dt.strftime('%d/%m/%Y').where(s.notna(), "")

def parse_brl_to_float(x):
    if x is None: return ""
    s = str(x).strip()
    if not s: return ""
    # remove R$, espaços, NBSP:
    s = s.replace("R$","").replace("\u00a0","").replace(" ","")
    # resolve milhar/decimal
    if "," in s and "." in s:
        s = s.replace(".","").replace(",",".")
    elif "," in s:
        s = s.replace(",",".")
    # mantém apenas [-+0-9.eE]
    s = re.sub(r"[^0-9.\-+eE]","",s)
    if s in ("", ".", "-", "+"): return ""
    try:
        return float(s)
    except Exception:
        return ""

def parse_date_cell(x):
    """Retorna dd/MM/yyyy (string) ou ''."""
    if x is None: return ""
    s = str(x).strip().replace("’","").replace("‘","").replace("'","")
    s = re.sub(r"[^0-9/\-: ]","",s)
    for fmt in ("%d/%m/%Y","%d/%m/%y","%Y-%m-%d","%d-%m-%Y"):
        try:
            return datetime.strptime(s.split(" ")[0], fmt).strftime("%d/%m/%Y")
        except Exception:
            continue
    # também tenta serial Excel
    try:
        f = float(s.replace(",","."))
        base = datetime(1899,12,30)
        if f>0:
            return (base + pd.to_timedelta(f, unit="D")).strftime("%d/%m/%Y")
    except Exception:
        pass
    return s if s else ""

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

# ───────── AUTH / OPEN ─────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

def make_creds():
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        info = json.loads(env_json)
        return SACreds.from_service_account_info(info, scopes=SCOPES)
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    script_dir = pathlib.Path(__file__).resolve().parent
    candidates = [script_dir / CRED_JSON, pathlib.Path.cwd() / CRED_JSON]
    for p in candidates:
        if p.is_file():
            return SACreds.from_service_account_file(p, scopes=SCOPES)
    hint = (
        "Não encontrei credenciais.\n"
        "Use:\n"
        "  a) set GOOGLE_CREDENTIALS com o JSON completo\n"
        "  b) set GOOGLE_APPLICATION_CREDENTIALS com o caminho do .json\n"
        f"  c) coloque '{CRED_JSON}' ao lado do script ({script_dir}) ou no diretório atual ({pathlib.Path.cwd()})\n"
    )
    raise FileNotFoundError(hint)

def abrir_planilhas():
    log("🔐 Autenticando…")
    creds = make_creds()
    gc = gspread.authorize(creds)
    log("📂 Abrindo planilhas…")
    b_src = with_retry(gc.open_by_key, ORIGEM_ID,  desc="open origem")
    b_dst = with_retry(gc.open_by_key, DESTINO_ID, desc="open destino")
    w_src = with_retry(b_src.worksheet, ABA_ORIGEM,  desc="ws origem")
    w_dst = with_retry(b_dst.worksheet, ABA_DESTINO, desc="ws destino")
    return creds, gc, b_src, b_dst, w_src, w_dst

# ───────── DRIVE CSV (com Retry/streaming/timeout) ─────────
def _access_token(creds: SACreds) -> str:
    if not creds.valid:
        creds.refresh(GARequest())
    return creds.token

def _requests_session_with_retry(total=6, backoff=0.6) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=total, read=total, connect=total,
        backoff_factor=backoff,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=10)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def export_sheet_to_df_csv(creds: SACreds, spreadsheet_id: str, gid: int) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
    headers = {"Authorization": f"Bearer {_access_token(creds)}"}
    sess = _requests_session_with_retry()
    with sess.get(url, headers=headers, stream=True, timeout=(10, 240)) as resp:
        if resp.status_code != 200:
            raise RuntimeError(f"Export falhou ({resp.status_code})")
        buf = io.StringIO()
        for chunk in resp.iter_content(chunk_size=1 << 16, decode_unicode=True):
            if chunk: buf.write(chunk)
        buf.seek(0)
        return pd.read_csv(buf, dtype=str).fillna("")

def letter_to_index(letter: str) -> int:
    n=0
    for c in letter:
        n = n*26 + (ord(c.upper()) - ord('A') + 1)
    return n-1

# ───────── FALLBACK SHEETS (micro-ranges tolerantes) ─────────
def ler_coluna_ids_batch_tolerante(ws, col_letter: str, start_row: int = 2) -> Tuple[List[str], List[int]]:
    values, rows_abs = [], []
    max_rows = ws.row_count or (start_row + 20000)
    row = start_row
    while row <= max_rows:
        ranges, local_rows = [], []
        for _ in range(RANGES_PER_BATCH_CALL):
            if row > max_rows: break
            end = min(row + BATCH_ROWS_PER_RANGE - 1, max_rows)
            ranges.append(f"{col_letter}{row}:{col_letter}{end}")
            local_rows.append((row,end))
            row = end + 1
        if not ranges: break
        try:
            data_blocks = ws.batch_get(ranges)
        except Exception as e:
            log(f"⚠️  Ignorando lote ({ws.title}!{col_letter}): {e}")
            continue
        all_empty = True
        for (start,end), block in zip(local_rows, data_blocks):
            flat = [(r[0].strip() if (r and len(r)>0 and isinstance(r[0],str)) else (r[0] if r else "")) for r in (block or [])]
            expected = end - start + 1
            if len(flat) < expected: flat += [""]*(expected - len(flat))
            if any(flat): all_empty = False
            values.extend(flat); rows_abs.extend(range(start, end+1))
        if all_empty: break
    return values, rows_abs

def batch_get_rows_tolerante(ws, row_indices: List[int], first_col: str, last_col: str) -> List[List[str]]:
    if not row_indices: return []
    ranges = [f"{first_col}{r}:{last_col}{r}" for r in row_indices]
    out=[]; step=FETCH_ROWS_STEP
    for i in range(0,len(ranges),step):
        chunk = ranges[i:i+step]
        try:
            blocks = ws.batch_get(chunk)
            for b in blocks: out.append(b[0] if b else [])
        except Exception as e:
            log(f"⚠️  Ignorando bloco {i//step+1}: {e}")
            out.extend([[] for _ in chunk])
    return out

# ───────── CAPTURA CICLO / LV (CSV com fallback) ─────────
def capturar_ciclo(creds: SACreds, b_dst) -> List[tuple]:
    try:
        ws = b_dst.worksheet("CICLO")
    except Exception:
        log("ℹ️  Aba 'CICLO' não encontrada — pulando."); return []
    # CSV
    try:
        df = export_sheet_to_df_csv(creds, DESTINO_ID, ws.id)
        if df.empty: return []
        idxC, idxD, idxE, idxF, idxL = map(letter_to_index, ['C','D','E','F','L'])
        num_cols = df.shape[1]
        idxs = [(i if i < num_cols else None) for i in [idxC,idxD,idxE,idxF,idxL]]
        out=[]
        for _, row in df.iterrows():
            valC = row.iat[idxs[0]] if idxs[0] is not None else ""
            valD = row.iat[idxs[1]] if idxs[1] is not None else ""
            valE = row.iat[idxs[2]] if idxs[2] is not None else ""
            valF = row.iat[idxs[3]] if idxs[3] is not None else ""
            valL = row.iat[idxs[4]] if idxs[4] is not None else ""
            vid  = str(valE).strip()
            if not vid: continue
            uni = MAP_UNIDADE.get(norm_acento_up(str(valD)), str(valD).strip())
            out.append(("CICLO", vid, str(valF), str(valC), str(valL), uni))
        if out: log(f"✅ CICLO via CSV: {len(out)} linhas")
        return out
    except Exception as e:
        log(f"⚠️  CSV CICLO falhou — fallback Sheets: {e}")

    # Fallback Sheets
    try:
        ids, rows_abs = ler_coluna_ids_batch_tolerante(ws, 'E', start_row=2)
        linhas_ids = [(r, (v or "").strip()) for v, r in zip(ids, rows_abs) if (v or "").strip()]
        if not linhas_ids: return []
        fetched = batch_get_rows_tolerante(ws, [r for (r,_) in linhas_ids], 'C', 'L')
        idxC, idxD, idxE, idxF = 0,1,2,3
        idxL = a1index('L') - a1index('C')
        out=[]
        for (r_abs, id_val), rvals in zip(linhas_ids, fetched):
            vid = ((rvals[idxE] if idxE < len(rvals) else "") or id_val).strip()
            if not vid: continue
            valF = rvals[idxF] if idxF < len(rvals) else ""
            valC = rvals[idxC] if idxC < len(rvals) else ""
            valL = rvals[idxL] if idxL < len(rvals) else ""
            rawD = rvals[idxD] if idxD < len(rvals) else ""
            uni = MAP_UNIDADE.get(norm_acento_up(rawD), (rawD or "").strip())
            out.append(("CICLO", vid, valF, valC, valL, uni))
        log(f"✅ CICLO via Sheets(fallback): {len(out)} linhas")
        return out
    except Exception as e:
        log(f"❌ CICLO falhou em ambos caminhos: {e}")
        return []

def capturar_lv(creds: SACreds, b_dst) -> List[tuple]:
    try:
        ws = b_dst.worksheet("LV CICLO")
    except Exception:
        log("ℹ️  Aba 'LV CICLO' não encontrada — pulando."); return []
    # CSV
    try:
        df = export_sheet_to_df_csv(creds, DESTINO_ID, ws.id)
        if df.empty: return []
        idxA, idxB, idxC = map(letter_to_index, ['A','B','C'])
        num_cols = df.shape[1]
        idxs = [(i if i < num_cols else None) for i in [idxA,idxB,idxC]]
        out=[]
        for _, row in df.iterrows():
            uni_raw = row.iat[idxs[0]] if idxs[0] is not None else ""
            vid     = row.iat[idxs[1]] if idxs[1] is not None else ""
            proj    = row.iat[idxs[2]] if idxs[2] is not None else ""
            vid = str(vid).strip()
            if not vid: continue
            uni = MAP_UNIDADE.get(norm_acento_up(str(uni_raw)), str(uni_raw).strip())
            out.append(("LV", vid, str(proj), uni))
        if out: log(f"✅ LV via CSV: {len(out)} linhas")
        return out
    except Exception as e:
        log(f"⚠️  CSV LV falhou — fallback Sheets: {e}")

    # Fallback Sheets
    try:
        ids, rows_abs = ler_coluna_ids_batch_tolerante(ws, 'B', start_row=2)
        linhas_ids = [(r, (v or "").strip()) for v, r in zip(ids, rows_abs) if (v or "").strip()]
        if not linhas_ids: return []
        fetched = batch_get_rows_tolerante(ws, [r for (r,_) in linhas_ids], 'A', 'C')
        out=[]
        for (r_abs, id_val), rvals in zip(linhas_ids, fetched):
            uni_raw = rvals[0] if len(rvals)>0 else ""
            vid     = (rvals[1] if len(rvals)>1 else id_val).strip()
            proj    = rvals[2] if len(rvals)>2 else ""
            if not vid: continue
            uni = MAP_UNIDADE.get(norm_acento_up(uni_raw), (uni_raw or "").strip())
            out.append(("LV", vid, proj, uni))
        log(f"✅ LV via Sheets(fallback): {len(out)} linhas")
        return out
    except Exception as e:
        log(f"❌ LV falhou em ambos caminhos: {e}")
        return []

# ───────── ORIGEM → DF ─────────
def ler_origem_para_df(w_src) -> pd.DataFrame:
    lastL = col_letter(max(a1index(c) for c in COLS_ORIGEM))
    rng   = f"A5:{lastL}"
    log(f"🧭 Lendo cabeçalho (linha 5) e dados… ({rng})")
    dat   = with_retry(w_src.get, rng, desc=f"get {rng}")
    if not dat: return pd.DataFrame()
    hdr, rows = dat[0], dat[1:]
    idx = [a1index(c)-1 for c in COLS_ORIGEM]
    tbl = [[r[i] if i<len(r) else "" for i in idx] for r in rows if r and str(r[0]).strip()]
    df  = pd.DataFrame(tbl, columns=[hdr[i] if i<len(hdr) else f"COL_{COLS_ORIGEM[j]}" for j,i in enumerate(idx)])
    log(f"🧱 Origem: {len(df)} linhas × {len(df.columns)} colunas")
    pos = {l:i for i,l in enumerate(COLS_ORIGEM)}
    for l in DATE_LETTERS:
        p = pos.get(l)
        if p is not None and p < len(df.columns): df.iloc[:,p] = parse_dates(df.iloc[:,p])
    # (mantemos os demais textos como vieram; faremos fix-up por letra na planilha destino)
    return df

# ───────── WRITE Carteira ─────────
def escrever_df_na_destino(w_dst, df: pd.DataFrame) -> int:
    rows0 = len(df); cols0 = len(df.columns)
    ensure(w_dst, rows0 + 2, max(20, cols0))
    endL  = col_letter(max(1, cols0))
    with_retry(w_dst.batch_clear, [f"A2:{endL}"], desc="clear dados")
    if cols0 > 0:
        with_retry(w_dst.update, range_name=f"A1:{rowcol_to_a1(1, cols0)}", values=[list(df.columns)], value_input_option='RAW')
    # status
    with_retry(w_dst.update, range_name="T2", values=[[f"Atualizando... {now()}"]], value_input_option='RAW')
    if rows0 > 0 and cols0 > 0:
        vals = df2values(df)
        log(f"🚚 Escrevendo {rows0} linhas em blocos de {CHUNK_ROWS_WRITE} (USER_ENTERED)…")
        i=0
        while i<rows0:
            part=vals[i:i+CHUNK_ROWS_WRITE]
            a1=f"A{2+i}:{endL}{1+i+len(part)}"
            # USER_ENTERED para permitir o Sheets interpretar números/datas quando possível
            with_retry(w_dst.update, range_name=a1, values=part, value_input_option='USER_ENTERED')
            i+=len(part)
    log("✅ Escrita de Carteira concluída.")
    return 2 + rows0

# ───────── INSERIR linhas (CICLO/LV) ─────────
def inserir_linhas(w_dst, rows: List[List[Any]], start_row: int) -> int:
    if not rows: return start_row
    last_col_idx = 1
    for r in rows:
        for j,v in enumerate(r, start=1):
            if v not in ("", None, []): last_col_idx = max(last_col_idx, j)
    endL = col_letter(last_col_idx)
    a1 = f"A{start_row}:{endL}{start_row+len(rows)-1}"
    # USER_ENTERED para evitar apóstrofo também nas inserções
    with_retry(w_dst.update, range_name=a1, values=rows, value_input_option='USER_ENTERED')
    if FORCAR_DESTAQ: highlight(w_dst, start_row, len(rows), endL)
    return start_row + len(rows)

# ───────── FIX-UP pós-escrita (remove ' em J,K,L,M,P,Q) ─────────
def fixup_num_and_dates(w_dst, total_rows_est: int):
    if total_rows_est <= 0:
        return
    # Garante a grade pelo menos até a última linha estimada
    ensure(w_dst, total_rows_est + 2, max(a1index('Q'), w_dst.col_count or 20))

    def get_col(col_letter):
        rng = f"{col_letter}2:{col_letter}{total_rows_est+1}"
        vals = with_retry(w_dst.get, rng, desc=f"get {rng}") or []
        # achata
        flat = [ (row[0] if row and len(row)>0 else "") for row in vals ]
        # se vier curto, completa
        if len(flat) < (total_rows_est):
            flat += [""]*((total_rows_est) - len(flat))
        return flat

    def put_col(col_letter, data):
        # regrava em blocos com USER_ENTERED
        step = 2000
        for i in range(0, len(data), step):
            parte = [[v] for v in data[i:i+step]]
            a1 = f"{col_letter}{2+i}:{col_letter}{1+i+len(parte)}"
            with_retry(w_dst.update, range_name=a1, values=parte, value_input_option='USER_ENTERED')

    # NUMÉRICOS
    for c in NUM_COLS_FIX:
        raw = get_col(c)
        conv = [parse_brl_to_float(x) for x in raw]
        put_col(c, conv)

    # DATAS
    for c in DATE_COLS_FIX:
        raw = get_col(c)
        conv = [parse_date_cell(x) for x in raw]
        put_col(c, conv)

    log("🧼 Fix-up concluído (J,K numéricos; L,M,P,Q datas).")

# ───────── MAIN ─────────
def main():
    log("▶️  importador_carteira.py — iniciando")
    creds, gc, b_src, b_dst, w_src, w_dst = abrir_planilhas()

    # 0) CAPTURAR CICLO / LV (CSV com retry; fallback Sheets tolerante)
    dados_ciclo = capturar_ciclo(creds, b_dst)   # ("CICLO", vid, F, C, L, uni)
    dados_lv    = capturar_lv(creds, b_dst)      # ("LV", vid, proj, uni)

    # 1) ORIGEM → DF
    df = ler_origem_para_df(w_src)

    # 2) Limpa e reescreve Carteira
    next_row = escrever_df_na_destino(w_dst, df)

    # 3) IDs da ORIGEM
    exist_ids = set()
    if not df.empty:
        exist_ids = set(x for x in df.iloc[:,0].astype(str).str.strip().tolist() if x)

    # 4) Monta linhas de CICLO/LV (sem novas leituras)
    larg_min = max(len(df.columns) if not df.empty else 0, a1index('R'))
    linhas: List[List[Any]] = []

    # CICLO: E→A, F→B, C→H, L→K, D→R
    for _, vid, valF, valC, valL, uni in dados_ciclo:
        if not vid or vid in exist_ids: continue
        ln = [''] * max(larg_min, a1index('R'))
        ln[a1index('A')-1] = vid
        ln[a1index('B')-1] = valF
        ln[a1index('H')-1] = valC
        # K (moeda): já mando “limpo” para Sheets interpretar
        ln[a1index('K')-1] = parse_brl_to_float(valL)
        ln[a1index('R')-1] = uni
        linhas.append(ln)
        exist_ids.add(vid)

    # LV: B→A, C→B, 'SOMENTE LV'→H, Unidade→R
    for _, vid, proj, uni in dados_lv:
        if not vid or vid in exist_ids: continue
        ln = [''] * max(larg_min, a1index('R'))
        ln[a1index('A')-1] = vid
        ln[a1index('B')-1] = proj
        ln[a1index('H')-1] = "SOMENTE LV"
        ln[a1index('R')-1] = uni
        linhas.append(ln)
        exist_ids.add(vid)

    # 5) Inserir tudo (USER_ENTERED)
    if linhas:
        log(f"🔗 Inserindo {len(linhas)} linhas de CICLO/LV…")
        next_row = inserir_linhas(w_dst, linhas, next_row)
        log(f"✅ {len(linhas)} linhas inseridas (CICLO/LV).")
    else:
        log("ℹ️  Sem linhas adicionais de CICLO/LV para inserir.")

    # 6) FIX-UP das colunas problemáticas (remove ' e deixa contável)
    total_estimado = max(next_row - 2, 0)
    if total_estimado > 0:
        fixup_num_and_dates(w_dst, total_estimado)

    # 7) Status final
    with_retry(w_dst.update, range_name="T2",
               values=[[f"Concluído em {now()}"]], value_input_option='RAW')

    log(f"🎉 Fim — linhas totais (estimado) na Carteira após inserções: ~{total_estimado}.")

if __name__ == "__main__":
    main()
