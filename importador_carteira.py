# importador_carteira.py — robusto (não pula etapa), com re-open e re-resolve em 404/503

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

ABA_CARTEIRA_DESTINO    = "Carteira"     # destino e também origem da base principal
RANGE_ORIGEM_PRINCIPAL  = "A5:CS"

USAR_CICLO_COMPLEMENTAR = True
ABA_CICLO               = "CICLO"
RANGE_CICLO             = "D1:T"

USAR_LV_COMPLEMENTAR    = True
ABA_LV_CICLO            = "LV CICLO"
RANGE_LV                = "A1:Y"

# escrita / limpeza
BLOCK_ROWS              = int(os.environ.get("CHUNK_ROWS", "2000"))
PAUSE_BETWEEN_WRITES    = 0.08
EXTRA_TAIL_ROWS         = 200

# retry / backoff
TRANSIENT_CODES         = {429, 500, 502, 503, 504}
MAX_RETRIES_API         = 8        # por operação baixa-nível (get/update/clear)
BASE_SLEEP_API          = 0.8

# tentativas “macro” (garantia de etapa): quantas vezes reabrimos/voltamos tudo antes de desistir
MAX_PASSOS_HARD         = 18       # ~18 ciclos, com backoff curto, ~5–10 min de insistência total
BASE_SLEEP_HARD         = 0.9

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
    """Retry curto para chamadas gspread (update/get/clear/resize...)."""
    for tent in range(1, MAX_RETRIES_API + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = _status(e)
            if code not in TRANSIENT_CODES or tent >= MAX_RETRIES_API:
                log(f"❌ {desc or fn.__name__} falhou: {e}")
                raise
            slp = min(20.0, BASE_SLEEP_API * (1.6 ** (tent - 1)) + random.uniform(0, 0.5))
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

def _norm_title(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip().lower())

def resolve_worksheet(sh, desired_title: str) -> Optional[gspread.Worksheet]:
    """Resolve direto ou por metadata, sem logs ruidosos."""
    try:
        return with_retry(sh.worksheet, desired_title, desc=f"worksheet {desired_title}")
    except WorksheetNotFound:
        meta = with_retry(sh.fetch_sheet_metadata, desc="fetch_sheet_metadata")
        want = _norm_title(desired_title)
        for s in meta.get("sheets", []):
            title = s.get("properties", {}).get("title", "")
            if _norm_title(title) == want:
                return with_retry(sh.worksheet, title, desc=f"worksheet {title} (equivalente)")
        return None

def read_values_df(ws, a1: str) -> pd.DataFrame:
    """Leitura usando values_get (ligeiramente mais estável) e fallback p/ ws.get."""
    try:
        resp = with_retry(ws.spreadsheet.values_get, f"'{ws.title}'!{a1}",
                          params={"majorDimension": "ROWS"}, desc=f"values_get {ws.title}!{a1}")
        values = resp.get("values", []) or []
        return pd.DataFrame(values) if values else pd.DataFrame([])
    except APIError as e:
        code = _status(e)
        if code in TRANSIENT_CODES:
            # fallback para ws.get também com retry
            raw = with_retry(ws.get, a1, desc=f"get {ws.title}!{a1}") or []
            return pd.DataFrame(raw) if raw else pd.DataFrame([])
        raise

# ================== “HARD LOOP” ==================
def hard_load_everything(gc) -> tuple[gspread.Worksheet, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Garante ler Carteira + (CICLO/LV se habilitados).
    Reabre a planilha/resolve abas quando der 404/503.
    Só retorna quando tudo estiver carregado (ou explode após MAX_PASSOS_HARD).
    """
    for passo in range(1, MAX_PASSOS_HARD + 1):
        log(f"🔁 Passo {passo}/{MAX_PASSOS_HARD} — abrindo planilha e resolvendo abas…")
        sh = with_retry(gc.open_by_key, SPREADSHEET_ID_MASTER, desc="open_by_key master")

        ws_dest = resolve_worksheet(sh, ABA_CARTEIRA_DESTINO)
        if ws_dest is None:
            try:
                ws_dest = with_retry(sh.add_worksheet, title=ABA_CARTEIRA_DESTINO, rows=2000, cols=100,
                                     desc="add_worksheet Carteira")
            except Exception as e:
                log(f"⚠️  Não consegui abrir/criar '{ABA_CARTEIRA_DESTINO}': {e}")

        ok_carteira = False
        ok_ciclo    = not USAR_CICLO_COMPLEMENTAR
        ok_lv       = not USAR_LV_COMPLEMENTAR

        df_principal = pd.DataFrame([])
        df_ciclo     = pd.DataFrame([])
        df_lv        = pd.DataFrame([])

        try:
            if ws_dest is not None:
                df_principal = read_values_df(ws_dest, RANGE_ORIGEM_PRINCIPAL)
                ok_carteira = not df_principal.empty
                if not ok_carteira:
                    log("⚠️  Carteira vazia ou leitura falhou — tentando novamente.")
            else:
                log("⚠️  Worksheet 'Carteira' indisponível.")
        except APIError as e:
            log(f"⚠️  Falha lendo Carteira: {e}")

        # CICLO
        if USAR_CICLO_COMPLEMENTAR:
            try:
                ws_ciclo = resolve_worksheet(sh, ABA_CICLO)
                if ws_ciclo is None:
                    log("⚠️  Aba 'CICLO' não encontrada.")
                else:
                    df_ciclo = read_values_df(ws_ciclo, RANGE_CICLO)
                    ok_ciclo = not df_ciclo.empty
                    if not ok_ciclo:
                        log("⚠️  'CICLO' vazio ou leitura falhou — tentando novamente.")
            except APIError as e:
                log(f"⚠️  Falha lendo 'CICLO': {e}")

        # LV
        if USAR_LV_COMPLEMENTAR:
            try:
                ws_lv = resolve_worksheet(sh, ABA_LV_CICLO)
                if ws_lv is None:
                    log("⚠️  Aba 'LV CICLO' não encontrada.")
                else:
                    df_lv = read_values_df(ws_lv, RANGE_LV)
                    ok_lv = not df_lv.empty
                    if not ok_lv:
                        log("⚠️  'LV CICLO' vazio ou leitura falhou — tentando novamente.")
            except APIError as e:
                log(f"⚠️  Falha lendo 'LV CICLO': {e}")

        if ok_carteira and ok_ciclo and ok_lv:
            # Sucesso: todos os necessários carregados
            log(f"🧱 Base principal: {len(df_principal)} linhas × {df_principal.shape[1]} colunas")
            if USAR_CICLO_COMPLEMENTAR:
                log(f"   ↳ CICLO: {len(df_ciclo)} linhas × {df_ciclo.shape[1]} colunas")
            if USAR_LV_COMPLEMENTAR:
                log(f"   ↳ LV CICLO: {len(df_lv)} linhas × {df_lv.shape[1]} colunas")
            return ws_dest, df_principal, df_ciclo, df_lv

        # espera breve e tenta tudo de novo (re-open/re-resolve)
        slp = min(15.0, BASE_SLEEP_HARD * (1.5 ** (passo - 1)) + random.uniform(0, 0.6))
        log(f"⏳ Ainda não consegui todos: Carteira={ok_carteira} CICLO={ok_ciclo} LV={ok_lv} — retry hard em {slp:.1f}s")
        time.sleep(slp)

    raise RuntimeError("Falha ao carregar Carteira/CICLO/LV após tentativas máximas.")

# ================== MAIN ==================
def main():
    log("▶️  importador_carteira.py — iniciando")
    log("🔐 Autenticando…")
    creds = make_creds()
    gc = gspread.authorize(creds)

    # hard loop: não segue até tudo estar realmente carregado
    ws_dest, df_principal, df_ciclo, df_lv = hard_load_everything(gc)

    # ======= PREPARO PARA ESCRITA =======
    values = to_matrix(df_principal)
    total_rows = len(values)

    # garante grade e limpa área somente se houver algo para escrever
    if total_rows == 0:
        # Por segurança, não limpa e não escreve vazio
        log("❌ Inesperado: df_principal vazio após hard load. Abortando com erro.")
        return 2

    min_rows = max(2 + total_rows, 2)
    ensure_grid(ws_dest, min_rows=min_rows + EXTRA_TAIL_ROWS, min_cols=100)
    end_clear = max(ws_dest.row_count, 5 + total_rows + EXTRA_TAIL_ROWS)
    rng_clear = f"A5:CS{end_clear}"
    log(f"🧽 Limpando {rng_clear}…")
    clear_range(ws_dest, rng_clear)

    # Escrita em blocos
    log(f"🚚 Escrevendo {total_rows} linhas (USER_ENTERED)…")
    chunked_write(ws_dest, start_row=5, start_col_1b=1, values=values)
    log("✅ Escrita de Carteira concluída.")

    # Integrações futuras com CICLO/LV (se/quando você quiser consolidar):
    if USAR_CICLO_COMPLEMENTAR:
        log(f"ℹ️  CICLO carregado ({len(df_ciclo)} linhas) — integrar conforme tua regra, se aplicável.")
    if USAR_LV_COMPLEMENTAR:
        log(f"ℹ️  LV CICLO carregado ({len(df_lv)} linhas) — integrar conforme tua regra, se aplicável.")

    log("🎉 Fim do importador — etapa garantida (sem pular) e com exit code 0.")
    return 0

if __name__ == "__main__":
    rc = 0
    try:
        rc = int(main() or 0)
    except Exception as e:
        log(f"💥 Erro fatal: {e}")
        rc = 2
    sys.exit(rc)
