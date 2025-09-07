# replicador_historico.py — rápido, sem formatação, AB/AC numéricos, escrita em lote por destino
from datetime import datetime
import re
import time
import sys
import unicodedata
import gspread
from gspread.exceptions import APIError

# === CONFIG ===
ID_ORIGEM       = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_HISTORICO   = "Historico"
CAMINHO_CRED    = "credenciais.json"  # fallback local

# === DESTINOS ===
PID_IRECE        = "1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c"
PID_BAR_IBO      = "1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M"
PID_BRU_GUA_LIV_LAPA = "10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c"
PID_VC_JEQ_ITA   = "1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw"

# Mapeamento: planilha → conjunto de unidades (coluna AD) permitidas
def _norm(s: str) -> str:
    s = (s or "").strip().upper()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s

MAPEAMENTO_DESTINOS = {
    PID_IRECE: { _norm("IRECE") },
    PID_BAR_IBO: { _norm("BARREIRAS"), _norm("IBOTIRAMA") },
    PID_BRU_GUA_LIV_LAPA: {
        _norm("BRUMADO"),
        _norm("GUANAMBI"),
        _norm("LIVRAMENTO"),
        _norm("BOM JESUS DA LAPA"),
    },
    PID_VC_JEQ_ITA: {
        _norm("VITORIA DA CONQUISTA"),
        _norm("JEQUIE"),
        _norm("ITAPETINGA"),
    },
}

PLANILHAS_DESTINO = list(MAPEAMENTO_DESTINOS.keys())

# Fórmula fixa em AE3 (mantida)
FORMULA_AE = '=ARRAYFORMULA(SE(B3:B=""; ""; SE((AD3:AD="-") + ÉERROS(PROCH(AD3:AD; Esteira!$B$1:$K$1; 1; 0)); 0; 1)))'

# Retries
RETRY_CRIT = (1, 3, 7, 15)    # backoff para operações críticas
MAX_TENTATIVAS_DEST = 5
DEST_BACKOFF_BASE_S = 5        # 5,10,20,40,80s

# === AUTENTICAÇÃO (Secret ou arquivo local) ===
import os, json, pathlib
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def make_creds():
    env = os.environ.get("GOOGLE_CREDENTIALS")
    if env:
        return Credentials.from_service_account_info(json.loads(env), scopes=SCOPES)
    return Credentials.from_service_account_file(pathlib.Path(CAMINHO_CRED), scopes=SCOPES)

gc = gspread.authorize(make_creds())

# === UTILS ===
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
            print(f"⚠️ Falha transitória{tag}: {e} — tentativa {i}/{total}; aguardando {d}s", flush=True)
            if i == total:
                raise
            time.sleep(d)

def _col_index_to_letter_1b(index: int) -> str:
    # 1->A, 2->B, ... 27->AA
    res = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        res = chr(rem + ord('A')) + res
    return res

def _clean_number_brl(val: str):
    """Converte '1.234,56' -> 1234.56; vazio/ruído -> ''."""
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

def tratar_linha_AB_AC(row, ncols):
    """Mantém linha com ncols colunas; força AB (idx 27) e AC (idx 28) numéricos; restante intacto."""
    r = (row + [""] * ncols)[:ncols]
    if ncols > 27:
        r[27] = _clean_number_brl(r[27])
    if ncols > 28:
        r[28] = _clean_number_brl(r[28])
    return r

def _garantir_grid(ws, linhas_dados, cab2):
    """
    Garante que a planilha tenha linhas/colunas suficientes para escrever:
      - Dados a partir de A3 (nlin = len(linhas_dados) => última linha = 2 + nlin)
      - Colunas pelo menos até len(cab2) e até AE (coluna 31) para a fórmula
    """
    nlin = len(linhas_dados)
    ncols_cab = len(cab2) if cab2 else (len(linhas_dados[0]) if nlin else 1)

    rows_needed = 2 + nlin                # A3..A{2+nlin}
    cols_needed = max(ncols_cab, 31)      # garantir AE

    if ws.row_count < rows_needed:
        _retry(RETRY_CRIT, ws.resize, rows_needed, ws.col_count, op_name='resize rows')
    current_rows = max(ws.row_count, rows_needed)

    if ws.col_count < cols_needed:
        _retry(RETRY_CRIT, ws.resize, current_rows, cols_needed, op_name='resize cols')

def escrever_destino(ws, cab1, cab2, linhas_tratadas):
    """
    Escreve:
      - A1: cabeçalho 1
      - A2: cabeçalho 2
      - A3: dados (todas as colunas do cabeçalho 2)
      - AE3: fórmula ARRAYFORMULA (após limpar AE)
      - Limpa rabo abaixo do último dado (A..lastcol) somente se existir
    """
    nlin = len(linhas_tratadas)
    ncols = len(cab2) if cab2 else (len(linhas_tratadas[0]) if nlin else 1)
    last_col_letter = _col_index_to_letter_1b(ncols)

    # 0) Garante grid suficiente ANTES de qualquer clear/write
    _garantir_grid(ws, linhas_tratadas, cab2)

    # 1) Limpa rabo antigo abaixo do novo final (A..last_col) — só se existir
    if nlin > 0:
        ultima = 2 + nlin
        tail_start = ultima + 1
        if tail_start <= ws.row_count:
            _retry(RETRY_CRIT, ws.spreadsheet.values_clear,
                   f"'{ws.title}'!A{tail_start}:{last_col_letter}", op_name='clear tail')

    # 2) Limpa AE (para a ARRAYFORMULA expandir)
    _retry(RETRY_CRIT, ws.spreadsheet.values_clear, f"'{ws.title}'!AE3:AE", op_name='clear AE')

    # 3) Payload único
    payload = []
    if cab1:
        payload.append({"range": f"{ws.title}!A1", "values": [cab1]})
    if cab2:
        payload.append({"range": f"{ws.title}!A2", "values": [cab2]})
    if nlin > 0:
        payload.append({"range": f"{ws.title}!A3", "values": linhas_tratadas})
        payload.append({"range": f"{ws.title}!AE3", "values": [[FORMULA_AE]]})
    else:
        payload.append({"range": f"{ws.title}!AE3", "values": [[FORMULA_AE]]})

    _retry(
        RETRY_CRIT,
        ws.spreadsheet.values_batch_update,
        body={"valueInputOption": "USER_ENTERED", "data": payload},
        op_name='values_batch_update'
    )

def replicar_para(planilha_id, cab1, cab2, linhas):
    print(f"\n📁 Atualizando planilha destino: {planilha_id}", flush=True)
    book = gc.open_by_key(planilha_id)
    ws = book.worksheet(ABA_HISTORICO)

    # Ajusta linhas: mantém largura do cabeçalho 2; trata AB/AC
    ncols = len(cab2) if cab2 else (len(linhas[0]) if linhas else 0)
    linhas_tratadas = [tratar_linha_AB_AC(l, ncols) for l in linhas]

    escrever_destino(ws, cab1, cab2, linhas_tratadas)
    print(f"✅ Finalizado: {len(linhas_tratadas)} linhas coladas.", flush=True)

def tentar_ate_dar_certo(planilha_id, cab1, cab2, linhas):
    for tentativa in range(1, MAX_TENTATIVAS_DEST + 1):
        try:
            if tentativa > 1:
                atraso = DEST_BACKOFF_BASE_S * (2 ** (tentativa - 2))  # 5,10,20,40,80
                print(f"🔁 Tentativa {tentativa}/{MAX_TENTATIVAS_DEST} — aguardando {atraso}s", flush=True)
                time.sleep(atraso)
            replicar_para(planilha_id, cab1, cab2, linhas)
            return
        except Exception as e:
            print(f"❌ Erro ao atualizar {planilha_id}: {e}", flush=True)
            if tentativa == MAX_TENTATIVAS_DEST:
                print("⛔️ Abortando: não foi possível atualizar todos os destinos.", flush=True)
                sys.exit(1)

# === LEITURA DA PLANILHA ORIGINAL ===
print("📥 Lendo dados da aba 'Historico' da planilha principal...")
orig = gc.open_by_key(ID_ORIGEM).worksheet(ABA_HISTORICO)
dados = _retry(RETRY_CRIT, orig.get_all_values, op_name='get_all_values') or []
cabecalho_1 = dados[0] if len(dados) > 0 else []
cabecalho_2 = dados[1] if len(dados) > 1 else []
linhas_dados = dados[2:] if len(dados) > 2 else []
print(f"✅ {len(linhas_dados)} linhas carregadas com sucesso.\n")

# Índice zero-based da coluna AD (A=0 ... Z=25, AA=26, AB=27, AC=28, AD=29)
IDX_AD = 29

# === EXECUTA PARA TODOS OS DESTINOS COM FILTRO PRÉVIO POR AD ===
for pid in PLANILHAS_DESTINO:
    permitidos = MAPEAMENTO_DESTINOS.get(pid, set())
    # filtra somente linhas com AD presente e dentro do conjunto permitido
    filtradas = [
        l for l in linhas_dados
        if len(l) > IDX_AD and _norm(l[IDX_AD]) in permitidos
    ]
    print(f"🧮 Destino {pid}: {len(filtradas)} linhas após filtro AD ∈ {sorted(list(permitidos))}", flush=True)
    tentar_ate_dar_certo(pid, cabecalho_1, cabecalho_2, filtradas)
