# replicar_ciclo.py — resiliente e rápido; limpa só o necessário; sem pular destino

from datetime import datetime
import os, json, pathlib
import re
import time
import sys
import random
from typing import List, Tuple

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials as SACreds

# ========= CONFIG =========
ID_MASTER        = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'   # planilha onde está a aba CICLO atualizada
ABA_CICLO       = 'CICLO'
RANGE_ORIGEM    = 'D1:T'   # NÃO inclui Z
DESTINOS        = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]

# Faixa fixa D..T
START_COL_LETTER = 'D'
END_COL_LETTER   = 'T'
N_COLS           = 17  # D..T (inclusive)

# Colunas relativas ao intervalo D:T (0-based relativas a D)
# J(10)->6, K(11)->7, L(12)->8, M(13)->9, O(15)->11, P(16)->12
IDX_DATAS_REL   = [6, 9, 11]   # J, M, O (relativo a D)
IDX_NUM_REL     = [7, 8, 12]   # K, L, P (relativo a D)

# Opções
APLICAR_FORMATO_NUMEROS = False   # desligado para poupar quota
APLICAR_FORMATO_DATAS   = False
CARIMBAR                = True
CARIMBAR_CEL            = 'Z1'     # se não existir, usa última coluna existente da linha 1

# Tuning / retries
TRANSIENT_CODES         = {429, 500, 502, 503, 504}
MAX_RETRIES             = 6
BASE_SLEEP              = 1.0
PAUSE_BETWEEN_DESTS     = 0.6
PAUSE_BETWEEN_WRITES    = 0.10
EXTRA_TAIL_ROWS         = 200      # limpeza extra do rabo
DESTINO_MAX_TENTATIVAS  = 5
DESTINO_BACKOFF_BASE_S  = 5        # 5,10,20,40,80

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# ========= CREDENCIAIS FLEXÍVEIS =========
def make_creds():
    env = os.environ.get('GOOGLE_CREDENTIALS')
    if env:
        return SACreds.from_service_account_info(json.loads(env), scopes=SCOPES)
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    script_dir = pathlib.Path(__file__).resolve().parent
    for p in (script_dir / 'credenciais.json', pathlib.Path.cwd() / 'credenciais.json'):
        if p.is_file():
            return SACreds.from_service_account_file(p, scopes=SCOPES)
    raise FileNotFoundError("Credenciais não encontradas (GOOGLE_CREDENTIALS, GOOGLE_APPLICATION_CREDENTIALS ou credenciais.json).")

# ========= RETRY/UTILS =========
def _status_code(e: APIError):
    m = re.search(r"\[(\d{3})\]", str(e))
    try:
        return int(m.group(1)) if m else None
    except Exception:
        return None

def _with_retry(fn, *args, desc=None, **kwargs):
    for tent in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = _status_code(e)
            if code not in TRANSIENT_CODES or tent >= MAX_RETRIES:
                print(f"❌ {desc or fn.__name__} falhou: {e}")
                raise
            slp = min(60.0, BASE_SLEEP * (2 ** (tent - 1)) + random.uniform(0, 0.75))
            print(f"⚠️  {desc or fn.__name__}: HTTP {code} — retry {tent}/{MAX_RETRIES-1} em {slp:.1f}s")
            time.sleep(slp)

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

def a1(col_1b: int, row_1b: int) -> str:
    return f"{col_index_to_letter_1b(col_1b)}{row_1b}"

def agora() -> str:
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')

# ========= TRATAMENTO =========
def limpar_num(v):
    if v is None:
        return ''
    s = str(v).strip()
    if s == '':
        return ''
    if s.startswith("'"):
        s = s[1:]
    s = s.replace('R$', '').replace(' ', '')
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '.')
    s = re.sub(r'[^0-9.\-+eE]', '', s)
    try:
        return float(s) if s != '' else ''
    except Exception:
        return ''

def normaliza_data(v):
    if v is None:
        return ''
    s = str(v).strip().replace('’', '').replace('‘', '').replace("'", "")
    if s == '':
        return ''
    s = re.sub(r'[^0-9/\-: ]', '', s)
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s.split(' ')[0], fmt)
            return dt.strftime("%d/%m/%Y")
        except Exception:
            continue
    return s

def tratar_linha(row: List) -> List:
    r = [(c if c is not None else "") for c in row[:N_COLS]] + [""] * max(0, N_COLS - len(row))
    for i in range(N_COLS):
        if isinstance(r[i], str) and r[i].startswith("'"):
            r[i] = r[i][1:]
    for c in IDX_DATAS_REL:
        if c < N_COLS:
            r[c] = normaliza_data(r[c])
    for c in IDX_NUM_REL:
        if c < N_COLS:
            r[c] = limpar_num(r[c])
    return r

# ========= GRADE/ESCRITA =========
def ensure_grid(ws, min_rows: int, min_cols: int):
    rows = max(ws.row_count, min_rows)
    cols = max(ws.col_count, min_cols)
    if rows != ws.row_count or cols != ws.col_count:
        print(f"🧩 resize {ws.title}: {ws.row_count}x{ws.col_count} → {rows}x{cols}")
        _with_retry(ws.resize, rows=rows, cols=cols, desc=f"resize {ws.title}")

def limpar_corpo(ws, nlin: int):
    # limpa D2:T{end_clear}
    end_clear = max(ws.row_count, nlin + EXTRA_TAIL_ROWS)
    rng = f"'{ws.title}'!{START_COL_LETTER}2:{a1(col_letter_to_index_1b(END_COL_LETTER), end_clear)}"
    _with_retry(ws.spreadsheet.values_clear, rng, desc=f"values_clear {rng}")
    time.sleep(PAUSE_BETWEEN_WRITES)

def limpar_rabo(ws, nlin: int):
    # limpa abaixo do último registro (D{nlin+1}:T{end_clear})
    end_clear = max(ws.row_count, nlin + EXTRA_TAIL_ROWS)
    if end_clear > (nlin + 1):
        tail = f"'{ws.title}'!{START_COL_LETTER}{nlin+1}:{a1(col_letter_to_index_1b(END_COL_LETTER), end_clear)}"
        _with_retry(ws.spreadsheet.values_clear, tail, desc=f"values_clear {tail}")
        time.sleep(PAUSE_BETWEEN_WRITES)

def escrever(ws, all_vals: List[List]):
    nlin = len(all_vals)
    rng  = f"{START_COL_LETTER}1:{END_COL_LETTER}{nlin}"
    ensure_grid(ws, min_rows=nlin, min_cols=col_letter_to_index_1b(END_COL_LETTER))
    # escreve cabeçalho + dados numa chamada
    _with_retry(ws.update, range_name=rng, values=all_vals, value_input_option="USER_ENTERED", desc=f"update {rng}")
    time.sleep(PAUSE_BETWEEN_WRITES)

def formatar(ws, nlin: int):
    if not (APLICAR_FORMATO_NUMEROS or APLICAR_FORMATO_DATAS) or nlin <= 1:
        return
    reqs = []
    sid = ws.id
    start_abs = col_letter_to_index_1b(START_COL_LETTER) - 1
    end_row_excl = nlin  # 1..(nlin-1) = dados

    if APLICAR_FORMATO_NUMEROS:
        for rel in sorted(IDX_NUM_REL):
            abs_idx = start_abs + rel
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row_excl,
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
                    "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row_excl,
                              "startColumnIndex": abs_idx, "endColumnIndex": abs_idx + 1},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

    if reqs:
        try:
            _with_retry(ws.spreadsheet.batch_update, {"requests": reqs}, desc="batch_update format")
        except APIError as e:
            print(f"⚠️  Formatação ignorada (API instável): {e}")

def carimbar(ws):
    if not CARIMBAR:
        return
    # Z1 se possível; senão usa última coluna
    desired_letter = re.match(r'^([A-Za-z]+)', CARIMBAR_CEL).group(1) if CARIMBAR_CEL else 'Z'
    desired_row = int(re.search(r'(\d+)$', CARIMBAR_CEL).group(1)) if CARIMBAR_CEL and re.search(r'\d+$', CARIMBAR_CEL) else 1
    desired_col_1b = col_letter_to_index_1b(desired_letter)
    max_cols = getattr(ws, "col_count", col_letter_to_index_1b(END_COL_LETTER))
    max_rows = getattr(ws, "row_count", 1)
    safe_col_1b = min(desired_col_1b, max_cols if max_cols else col_letter_to_index_1b(END_COL_LETTER))
    safe_row = min(max(desired_row, 1), max_rows if max_rows else 1)
    cell = a1(safe_col_1b, safe_row)
    ts = agora()
    _with_retry(ws.update, range_name=cell, values=[[f'Atualizado em: {ts}']], value_input_option='RAW', desc=f"carimbar {cell}")

# ========= MAIN =========
def main():
    creds = make_creds()
    gc = gspread.authorize(creds)

    # --- Ler master ---
    print(f"📥 Lendo {ID_MASTER}/{ABA_CICLO} ({RANGE_ORIGEM})…")
    sh_src = _with_retry(gc.open_by_key, ID_MASTER, desc="open_by_key master")
    ws_src = _with_retry(sh_src.worksheet, ABA_CICLO, desc="worksheet master")
    vals = _with_retry(ws_src.get, RANGE_ORIGEM, desc=f"get {RANGE_ORIGEM}") or []
    if not vals:
        print("⚠️ Nada a replicar (faixa vazia).")
        sys.exit(0)

    cabec = (vals[0] + [""] * N_COLS)[:N_COLS]
    linhas = []
    for r in vals[1:]:
        if not any((str(c or "").strip() for c in r[:N_COLS])):  # ignora linhas totalmente vazias
            continue
        linhas.append(tratar_linha(r))
    all_vals = [cabec] + linhas
    nlin = len(all_vals)  # inclui cabeçalho
    print(f"✅ {len(linhas)} linhas preparadas.\n")

    # --- Replicar para cada destino ---
    for i, pid in enumerate(DESTINOS, start=1):
        print(f"➡️ [{i}/{len(DESTINOS)}] Atualizando {pid}/{ABA_CICLO} …")
        for tentativa in range(1, DESTINO_MAX_TENTATIVAS + 1):
            try:
                sh = _with_retry(gc.open_by_key, pid, desc=f"open_by_key destino {pid}")
                try:
                    ws = _with_retry(sh.worksheet, ABA_CICLO, desc=f"worksheet {ABA_CICLO} destino")
                except WorksheetNotFound:
                    ws = _with_retry(sh.add_worksheet, title=ABA_CICLO,
                                     rows=max(nlin, 1000),
                                     cols=max(26, col_letter_to_index_1b(END_COL_LETTER)),
                                     desc=f"add_worksheet {ABA_CICLO} destino")

                ensure_grid(ws, min_rows=nlin + EXTRA_TAIL_ROWS, min_cols=col_letter_to_index_1b(END_COL_LETTER))

                # limpeza direcionada
                limpar_corpo(ws, nlin)

                # escrita única
                escrever(ws, all_vals)

                # formatação opcional
                formatar(ws, nlin)

                # limpa rabo (abaixo do fim)
                limpar_rabo(ws, nlin)

                # carimbo
                carimbar(ws)

                print(f"✅ Replicado {len(linhas)} linhas para {pid}.")
                time.sleep(PAUSE_BETWEEN_DESTS)
                break
            except Exception as e:
                print(f"❌ Tentativa {tentativa}/{DESTINO_MAX_TENTATIVAS} falhou para {pid}: {e}")
                if tentativa == DESTINO_MAX_TENTATIVAS:
                    print(f"⛔️ Não foi possível atualizar {pid} após {DESTINO_MAX_TENTATIVAS} tentativas. Abortando.")
                    sys.exit(1)
                atraso = DESTINO_BACKOFF_BASE_S * (2 ** (tentativa - 1))
                print(f"⏳ Repetindo em {atraso}s…")
                time.sleep(atraso)

    print("🏁 Replicação de CICLO (D:T) finalizada.")

if __name__ == "__main__":
    main()
