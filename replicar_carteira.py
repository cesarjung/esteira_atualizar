# replicar_carteira.py — resiliente (menos 503), credenciais flexíveis, uma sessão, limpeza limitada e carimbo T2

from datetime import datetime
import re
import time
import sys
import os, json, pathlib
import random
from typing import List, Tuple

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials as SACreds

try:
    from gspread_formatting import format_cell_range, CellFormat, NumberFormat
except Exception:
    format_cell_range = None
    CellFormat = None
    NumberFormat = None

# ========= CONFIG =========
ID_MASTER = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA       = 'Carteira'
DESTINOS  = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# ========= FLAGS / TUNING =========
APLICAR_FORMATACAO_NUMERICA = False   # desligado para poupar quota
CHUNK_ROWS                  = int(os.environ.get("CHUNK_ROWS", "4000"))
MAX_RETRIES                 = 6
BASE_SLEEP                  = 1.0     # base para backoff exponencial
PAUSE_BETWEEN_WRITES        = 0.12    # pequenas pausas aliviam write/min
COLS_MIN                    = 20      # garante até T (A..T) p/ carimbo T2
EXTRA_TAIL_ROWS             = 200     # limpeza do “rabo” além do fim

TRANSIENT = {429, 500, 502, 503, 504}

# ========= CREDENCIAIS FLEXÍVEIS =========
def make_creds():
    env = os.environ.get('GOOGLE_CREDENTIALS')
    if env:
        return SACreds.from_service_account_info(json.loads(env), scopes=SCOPES)
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    return SACreds.from_service_account_file(pathlib.Path('credenciais.json'), scopes=SCOPES)

# ========= RETRY / UTILS =========
def agora():
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')

def _status_code(e: APIError):
    m = re.search(r"\[(\d{3})\]", str(e))
    try:
        return int(m.group(1)) if m else None
    except Exception:
        return None

def with_retry(fn, *args, desc="", **kwargs):
    for tent in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = _status_code(e)
            if code not in TRANSIENT or tent >= MAX_RETRIES:
                print(f"❌ {desc or fn.__name__} falhou: {e}")
                raise
            slp = min(60, BASE_SLEEP * (2 ** (tent - 1)) + random.uniform(0, 0.75))
            print(f"⚠️  {desc or fn.__name__}: HTTP {code} — retry {tent}/{MAX_RETRIES-1} em {slp:.1f}s")
            time.sleep(slp)

def a1(col_1b: int, row_1b: int) -> str:
    """Converte (col, row) 1-based para A1."""
    letras = ""
    c = col_1b
    while c > 0:
        c, rem = divmod(c - 1, 26)
        letras = chr(65 + rem) + letras
    return f"{letras}{row_1b}"

def ensure_grid(ws, min_rows, min_cols):
    rows = max(ws.row_count, min_rows)
    cols = max(ws.col_count, min_cols)
    if rows != ws.row_count or cols != ws.col_count:
        print(f"🧩 resize {ws.title}: {ws.row_count}x{ws.col_count} → {rows}x{cols}")
        with_retry(ws.resize, rows=rows, cols=cols, desc=f"resize {ws.title}")

def values_clear(ws, a1_range, tag="values_clear"):
    with_retry(ws.spreadsheet.values_clear, a1_range, desc=tag)
    time.sleep(PAUSE_BETWEEN_WRITES)

def safe_update(ws, a1_range, values, user_entered=True, tag="update"):
    opt = "USER_ENTERED" if user_entered else "RAW"
    with_retry(ws.update, range_name=a1_range, values=values, value_input_option=opt, desc=tag)
    time.sleep(PAUSE_BETWEEN_WRITES)

# ========= CONVERSÕES NUMÉRICAS (opcional) =========
def converter_numeros(dados: List[List], colunas_numericas: List[int]) -> List[List]:
    """Converte strings para float nas colunas 1-based indicadas."""
    def conv(v):
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        s = s.replace("R$", "").replace("\u00a0", "").replace(" ", "")
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return v
    out = []
    for row in dados:
        new = list(row)
        for c in colunas_numericas:
            idx = c - 1
            if 0 <= idx < len(new):
                new[idx] = conv(new[idx])
        out.append(new)
    return out

def aplicar_formatacao(ws, colunas_numericas: List[int]):
    """Aplica NumberFormat padrão decimal nas colunas (1-based). Fail-soft."""
    if not APLICAR_FORMATACAO_NUMERICA or not (format_cell_range and NumberFormat and CellFormat):
        return
    try:
        for col in colunas_numericas:
            rng = f"{a1(col, 2)}:{a1(col, ws.row_count)}"
            fmt = CellFormat(numberFormat=NumberFormat(type='NUMBER', pattern='0.00'))
            format_cell_range(ws, rng, fmt)
    except Exception as e:
        print(f"⚠️  Formatação numérica ignorada: {e}")

# ========= I/O PRINCIPAL =========
def ler_master_A_S(gc: gspread.Client) -> Tuple[List[str], List[List]]:
    """Lê master (Carteira) A1:S (cabeçalho + dados)."""
    print(f"📖 Abrindo master {ID_MASTER}/{ABA} …")
    sh = with_retry(gc.open_by_key, ID_MASTER, desc="open_by_key master")
    ws = with_retry(sh.worksheet, ABA, desc="worksheet master")
    valores = with_retry(ws.get, "A1:S", desc="get A1:S") or []
    if not valores:
        return [], []
    cabecalho = valores[0]
    dados = valores[1:]
    print(f"✅ Master lido: {len(dados)} linhas.")
    return cabecalho, dados

def limpar_e_escrever_destino(gc: gspread.Client, planilha_id: str, cabecalho: List[str], dados: List[List]):
    """Limpa e escreve A:S no destino, garante grade até T2 p/ carimbo, limpa rabo."""
    print(f"📦 Abrindo destino {planilha_id} …")
    sh = with_retry(gc.open_by_key, planilha_id, desc=f"open_by_key {planilha_id}")
    try:
        ws = with_retry(sh.worksheet, ABA, desc=f"worksheet {ABA}")
    except WorksheetNotFound:
        ws = with_retry(
            sh.add_worksheet,
            title=ABA,
            rows=max(len(dados) + 2, 1000),
            cols=max(COLS_MIN, 60),
            desc=f"add_worksheet {ABA}"
        )

    # Garante dimensões: dados + T (col 20) para timestamp
    min_rows = max(2 + len(dados), 2)
    ensure_grid(ws, min_rows=min_rows, min_cols=COLS_MIN)  # >= 20 colunas (A..T)

    # Status inicial (T2)
    try:
        safe_update(ws, 'T2', [['Atualizando...']], user_entered=False, tag='status T2')
    except Exception as e:
        print(f"⚠️  Não foi possível marcar status em T2: {e}")

    # Limpeza A2:S{end_clear} (FAIXA LIMITADA — evita “coluna inteira”)
    end_clear = max(ws.row_count, len(dados) + 2 + EXTRA_TAIL_ROWS)
    rng_clear = f"'{ws.title}'!A2:{a1(19, end_clear)}"  # 19 = S
    print(f"🧽 Limpando dados antigos ({rng_clear})…")
    values_clear(ws, rng_clear, tag=f'values_clear {rng_clear}')

    # Cabeçalho
    print("📝 Escrevendo cabeçalho (A1:S1)…")
    safe_update(ws, 'A1:S1', [cabecalho], user_entered=True, tag='update header A1:S1')

    # Conversão numérica (ajuste se necessário)
    colunas_numericas = [12, 13, 14, 15, 16, 17]  # L..Q (1-based)
    dados_fmt = converter_numeros(dados, colunas_numericas) if APLICAR_FORMATACAO_NUMERICA else dados

    # Escrita em blocos
    print(f"🚚 Escrevendo {len(dados_fmt)} linhas em blocos de {CHUNK_ROWS}…")
    i = 0
    while i < len(dados_fmt):
        parte = dados_fmt[i:i + CHUNK_ROWS]
        start = 2 + i
        end   = 1 + i + len(parte)  # 2..(1+len) cobre 'len(parte)' linhas
        rng   = f"A{start}:{a1(19, end)}"  # S = 19
        safe_update(ws, rng, parte, user_entered=True, tag=f'update {rng}')
        i += len(parte)

    # Formatação numérica opcional
    aplicar_formatacao(ws, colunas_numericas)

    # Limpa rabo abaixo do último dado (A{end+1}:S{end_clear})
    last_row = len(dados_fmt) + 1  # 1=cabeçalho
    if end_clear >= last_row + 1:
        tail_rng = f"'{ws.title}'!A{last_row+1}:{a1(19, end_clear)}"
        values_clear(ws, tail_rng, tag=f'values_clear tail {tail_rng}')

    # Timestamp final em T2
    try:
        safe_update(ws, 'T2', [[f"Replicado em: {agora()}"]], user_entered=True, tag='timestamp T2')
    except Exception as e:
        print(f"⚠️  Falha ao gravar timestamp em T2: {e}")

    print(f"✅ Finalizado destino {planilha_id}")

def tentar_destino_ate_dar_certo(gc: gspread.Client, planilha_id: str, cabecalho: List[str], dados: List[List]):
    """Retries de alto nível por destino (sem pular)."""
    for tentativa in range(1, 6):
        try:
            if tentativa > 1:
                atraso = min(60, BASE_SLEEP * (2 ** (tentativa - 1)) + 0.3 * tentativa)
                print(f"🔁 Tentativa {tentativa}/5 para {planilha_id} — aguardando {atraso:.1f}s…")
                time.sleep(atraso)
            limpar_e_escrever_destino(gc, planilha_id, cabecalho, dados)
            return
        except APIError as e:
            print(f"⚠️  Destino {planilha_id} – APIError: {e}")
        except Exception as e:
            print(f"⚠️  Destino {planilha_id} – erro: {e}")
    print(f"⛔️ Não foi possível atualizar {planilha_id} após 5 tentativas. Abortando.")
    sys.exit(1)

# ========= MAIN =========
if __name__ == '__main__':
    creds = make_creds()
    gc = gspread.authorize(creds)

    cab, dados = ler_master_A_S(gc)
    if not cab:
        print("❌ Nada para replicar na aba Carteira do master.")
        sys.exit(0)

    print(f"📦 Pronto para replicar: {len(dados)} linhas (A:S).")
    for pid in DESTINOS:
        tentar_destino_ate_dar_certo(gc, pid, cab, dados)
        time.sleep(0.6)  # pequena pausa entre destinos
    print("🏁 Réplica finalizada para todas as planilhas.")
