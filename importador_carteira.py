# importador_carteira.py — resiliente, com N tentativas antes de seguir (soft-fail)

import os
import re
import sys
import json
import time
import random
import pathlib
from datetime import datetime
from typing import List, Optional

import pandas as pd
import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials as SACreds

# ================== CONFIG ==================
SPREADSHEET_ID_MASTER   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"

ABA_CARTEIRA_DESTINO    = "Carteira"           # destino
RANGE_ORIGEM_PRINCIPAL  = "A5:CS"              # base principal (já existente)

USAR_CICLO_COMPLEMENTAR = True
ABA_CICLO               = "CICLO"
RANGE_CICLO             = "D1:T"               # cabeçalho + dados

USAR_LV_COMPLEMENTAR    = True
ABA_LV_CICLO            = "LV CICLO"
RANGE_LV                = "A1:Y"               # cabeçalho + dados

# escrita / limpeza
BLOCK_ROWS              = int(os.environ.get("CHUNK_ROWS", "2000"))
PAUSE_BETWEEN_WRITES    = 0.10
EXTRA_TAIL_ROWS         = 200

# retry (API)
TRANSIENT_CODES         = {429, 500, 502, 503, 504}
MAX_RETRIES_API         = 6
BASE_SLEEP_API          = 1.0

# retry (resolução de abas / leituras)
RESOLVE_ATTEMPTS        = 5     # tentativas para achar a aba
READ_ATTEMPTS           = 5     # tentativas para ler o intervalo
BASE_SLEEP_META         = 1.2   # base de backoff para metadata/leituras

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ================== LOG/RETRY ==================
def now_hms() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def log(msg: str):
    print(f"[{now_hms()}] {msg}", flush=True)

def _status(e: APIError) -> Optional[int]:
    m = re.search(r"\[(\d{3})\]", str(e))
    return int(m.group(1)) if m else None

def with_retry(fn, *args, desc="", **kwargs):
    """Retry para chamadas gspread (update/clear/etc.)."""
    for tent in range(1, MAX_RETRIES_API + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = _status(e)
            if code not in TRANSIENT_CODES or tent >= MAX_RETRIES_API:
                log(f"❌ {desc or fn.__name__} falhou: {e}")
                raise
            slp = min(60.0, BASE_SLEEP_API * (2 ** (tent - 1)) + random.uniform(0, 0.75))
            log(f"⚠️  {desc or fn.__name__}: HTTP {code} — retry {tent}/{MAX_RETRIES_API-1} em {slp:.1f}s")
            time.sleep(slp)

# ================== CREDS ==================
def make_creds():
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        try:
            return SACreds.from_service_account_info(json.loads(env_json), scopes=SCOPES)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS inválido: {e}")
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    script_dir = pathlib.Path(__file__).resolve().parent
    for p in (script_dir / "credenciais.json", pathlib.Path.cwd() / "credenciais.json"):
        if p.is_file():
            return SACreds.from_service_account_file(str(p), scopes=SCOPES)
    raise FileNotFoundError(
        "Credenciais não encontradas. Use GOOGLE_CREDENTIALS (JSON inline) "
        "ou GOOGLE_APPLICATION_CREDENTIALS (caminho do .json) ou credenciais.json."
    )

# ================== SHEETS HELPERS ==================
def ensure_grid(ws, min_rows: int, min_cols: int):
    rows = max(ws.row_count, min_rows)
    cols = max(ws.col_count, min_cols)
    if rows != ws.row_count or cols != ws.col_count:
        log(f"🧩 resize {ws.title}: {ws.row_count}x{ws.col_count} → {rows}x{cols}")
        with_retry(ws.resize, rows=rows, cols=cols, desc=f"resize {ws.title}")

def col_letter(col_1b: int) -> str:
    res = ""
    c = col_1b
    while c > 0:
        c, rem = divmod(c - 1, 26)
        res = chr(65 + rem) + res
    return res

def clear_range(ws, a1: str):
    with_retry(ws.spreadsheet.values_clear, f"'{ws.title}'!{a1}", desc=f"values_clear {ws.title}!{a1}")
    time.sleep(PAUSE_BETWEEN_WRITES)

def update_range(ws, a1: str, values: List[List], user_entered=True, tag="update"):
    opt = "USER_ENTERED" if user_entered else "RAW"
    with_retry(ws.update, range_name=a1, values=values, value_input_option=opt, desc=tag)
    time.sleep(PAUSE_BETWEEN_WRITES)

def chunked_write(ws, start_row: int, start_col_1b: int, values: List[List]):
    total = len(values)
    if total == 0:
        return
    cols = len(values[0])
    i = 0
    bloco = 0
    while i < total:
        part = values[i:i + BLOCK_ROWS]
        end_row = start_row + len(part) - 1
        end_col = start_col_1b + cols - 1
        a1 = f"{col_letter(start_col_1b)}{start_row}:{col_letter(end_col)}{end_row}"
        bloco += 1
        log(f"🚚 Escrevendo bloco {bloco} — {a1} ({len(part)} linhas)")
        update_range(ws, a1, part, user_entered=True, tag=f"update {a1}")
        i += len(part)
        start_row = end_row + 1

def to_matrix(df: pd.DataFrame) -> List[List]:
    return [] if df.empty else df.values.tolist()

# -------- busca de aba robusta (case-insensitive; ignora espaços) --------
def _norm_title(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip().lower())

def resolve_worksheet_with_retries(sh, desired_title: str) -> Optional[gspread.Worksheet]:
    """Tenta achar a aba por diversas vezes (para contornar propagação/caches)."""
    want = _norm_title(desired_title)
    for tent in range(1, RESOLVE_ATTEMPTS + 1):
        try:
            # tentativa direta
            return with_retry(sh.worksheet, desired_title, desc=f"worksheet {desired_title}")
        except WorksheetNotFound:
            pass
        try:
            meta = with_retry(sh.fetch_sheet_metadata, desc="fetch_sheet_metadata")
            for s in meta.get("sheets", []):
                title = s.get("properties", {}).get("title", "")
                if _norm_title(title) == want:
                    return with_retry(sh.worksheet, title, desc=f"worksheet {title} (equivalente)")
        except APIError as e:
            # se der APIError transitório, tratamos abaixo via pausa e próximo loop
            code = _status(e)
            if code not in TRANSIENT_CODES:
                log(f"⚠️  Metadata falhou (não transitório): {e}")
        # backoff antes da próxima tentativa
        slp = min(30.0, BASE_SLEEP_META * (2 ** (tent - 1)) + random.uniform(0, 0.5))
        log(f"🔎 '{desired_title}' não encontrada (tentativa {tent}/{RESOLVE_ATTEMPTS}) — tentando de novo em {slp:.1f}s")
        time.sleep(slp)
    return None

def read_values_with_retries(ws, a1: str) -> pd.DataFrame:
    """Lê um intervalo tentando algumas vezes antes de desistir."""
    for tent in range(1, READ_ATTEMPTS + 1):
        try:
            raw = with_retry(ws.get, a1, desc=f"get {ws.title}!{a1}") or []
            return pd.DataFrame(raw) if raw else pd.DataFrame([])
        except APIError as e:
            code = _status(e)
            if code not in TRANSIENT_CODES:
                log(f"❌ Leitura {ws.title}!{a1} falhou (não transitório): {e}")
                break
            slp = min(45.0, BASE_SLEEP_META * (2 ** (tent - 1)) + random.uniform(0, 0.5))
            log(f"⚠️  Leitura {ws.title}!{a1}: HTTP {code} — retry {tent}/{READ_ATTEMPTS-1} em {slp:.1f}s")
            time.sleep(slp)
    return pd.DataFrame([])

# ================== COMPLEMENTOS ==================
def try_load_ciclo(sh) -> pd.DataFrame:
    if not USAR_CICLO_COMPLEMENTAR:
        return pd.DataFrame([])
    ws = resolve_worksheet_with_retries(sh, ABA_CICLO)
    if ws is None:
        log("ℹ️  Aba 'CICLO' indisponível após várias tentativas — seguindo sem complemento.")
        return pd.DataFrame([])
    df = read_values_with_retries(ws, RANGE_CICLO)
    log(f"   ↳ CICLO: {len(df)} linhas × {df.shape[1]} colunas")
    return df

def try_load_lv(sh) -> pd.DataFrame:
    if not USAR_LV_COMPLEMENTAR:
        return pd.DataFrame([])
    ws = resolve_worksheet_with_retries(sh, ABA_LV_CICLO)
    if ws is None:
        log("ℹ️  Aba 'LV CICLO' indisponível após várias tentativas — seguindo sem complemento.")
        return pd.DataFrame([])
    df = read_values_with_retries(ws, RANGE_LV)
    log(f"   ↳ LV CICLO: {len(df)} linhas × {df.shape[1]} colunas")
    return df

# ================== MAIN ==================
def main():
    log("▶️  importador_carteira.py — iniciando")
    log("🔐 Autenticando…")
    creds = make_creds()
    gc = gspread.authorize(creds)

    log("📂 Abrindo planilha master…")
    sh = with_retry(gc.open_by_key, SPREADSHEET_ID_MASTER, desc="open_by_key master")

    # destino (Carteira)
    ws_dest = resolve_worksheet_with_retries(sh, ABA_CARTEIRA_DESTINO)
    if ws_dest is None:
        try:
            ws_dest = with_retry(sh.add_worksheet, title=ABA_CARTEIRA_DESTINO, rows=2000, cols=100,
                                 desc="add_worksheet Carteira")
        except Exception as e:
            log(f"⚠️  Não consegui abrir/criar 'Carteira'. Encerrando sem derrubar o pipeline. Detalhe: {e}")
            return 0

    # base principal (lida da própria Carteira A5:CS)
    log("🧭 Lendo base principal (Carteira!A5:CS)…")
    df_principal = read_values_with_retries(ws_dest, RANGE_ORIGEM_PRINCIPAL)
    log(f"🧱 Base principal: {len(df_principal)} linhas × {df_principal.shape[1]} colunas")

    # complementos — com várias tentativas antes de desistir
    df_ciclo = try_load_ciclo(sh)
    df_lv    = try_load_lv(sh)

    # ======= PREPARO PARA ESCRITA =======
    values = to_matrix(df_principal)
    total_rows = len(values)

    # garante grade e limpa área
    min_rows = max(2 + total_rows, 2)
    ensure_grid(ws_dest, min_rows=min_rows + EXTRA_TAIL_ROWS, min_cols=100)
    end_clear = max(ws_dest.row_count, 5 + total_rows + EXTRA_TAIL_ROWS)
    rng_clear = f"A5:CS{end_clear}"
    log(f"🧽 Limpando {rng_clear}…")
    try:
        clear_range(ws_dest, rng_clear)
    except Exception as e:
        log(f"⚠️  Falha ao limpar {rng_clear}: {e}")

    # Escrita em blocos (se houver algo a escrever)
    if total_rows > 0:
        log(f"🚚 Escrevendo {total_rows} linhas (USER_ENTERED)…")
        try:
            chunked_write(ws_dest, start_row=5, start_col_1b=1, values=values)  # A=1
            log("✅ Escrita de Carteira concluída.")
        except Exception as e:
            log(f"⚠️  Falha na escrita da base principal: {e}")
    else:
        log("ℹ️  Base principal vazia — nada a escrever.")

    # Integrações futuras: aqui você pode aplicar merge/append usando df_ciclo / df_lv.
    if df_ciclo.empty and df_lv.empty:
        log("ℹ️  Sem linhas adicionais de CICLO/LV para inserir (após múltiplas tentativas).")
    else:
        if not df_ciclo.empty:
            log(f"ℹ️  (info) CICLO carregado com {len(df_ciclo)} linhas — integrar conforme regra desejada.")
        if not df_lv.empty:
            log(f"ℹ️  (info) LV CICLO carregado com {len(df_lv)} linhas — integrar conforme regra desejada.")

    log("🎉 Fim do importador (com tentativas e soft-fail).")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except Exception as e:
        log(f"⚠️  Erro não tratado: {e} — encerrando sem abortar.")
        sys.exit(0)
