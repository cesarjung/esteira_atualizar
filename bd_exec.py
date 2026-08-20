# -*- coding: utf-8 -*-
import os
import re
import time
import random
import gspread
from datetime import datetime
from pathlib import Path
from google.oauth2.service_account import Credentials as SACreds
from gspread.exceptions import APIError

# ================== FLAGS / TUNING ==================
FORCAR_FORMATACAO = os.environ.get("FORCAR_FORMATACAO", "0") == "1"  # aplica formato na coluna B
CHUNK_ROWS        = int(os.environ.get("CHUNK_ROWS", "5000"))        # linhas por bloco no upload
MAX_RETRIES       = 6
BASE_SLEEP        = 1.0
TRANSIENT_CODES   = {404, 408, 409, 429, 500, 502, 503, 504}

# ================== CONFIG ==================
URL_ORIGEM          = 'https://docs.google.com/spreadsheets/d/189JPWONK4hSpziocviwSQOtj59rWl9tbhkVvrxb6Lds'
NOME_ABA_ORIGEM     = 'BD_Serv_Esteira'
INTERVALO_ORIGEM    = 'A2:B'

ID_PLANILHA_DESTINO = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
NOME_ABA_DESTINO    = 'BD_EXEC'
CREDENTIALS_PATH    = 'credenciais.json'  # fallback local

# Fuso horário opcional (para timestamps coerentes)
os.environ.setdefault("TZ", "America/Sao_Paulo")
try:
    import time as _t; _t.tzset()
except Exception:
    pass

# ================== LOG ==================
def now(): return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
def log(msg): print(f"[{now()}] {msg}", flush=True)

# ================== RETRY ==================
def _status_code(e: APIError):
    # gspread expõe o código no texto: "APIError: [429]: message..."
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

def with_retry(fn, *args, desc="", base_sleep=BASE_SLEEP, max_retries=MAX_RETRIES, **kwargs):
    tent = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            tent += 1
            code = _status_code(e)
            if tent >= max_retries or (code is not None and code not in TRANSIENT_CODES):
                log(f"❌ Falhou: {desc or fn.__name__} | {e}")
                raise
            slp = min(60, base_sleep * (2 ** (tent - 1)) + random.uniform(0, 0.75))
            log(f"⚠️  HTTP {code} — retry {tent}/{max_retries-1} em {slp:.1f}s ({desc or fn.__name__})")
            time.sleep(slp)

# ================== AUTH ROBUSTA (para GitHub Actions) ==================
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

def make_creds():
    """
    Ordem:
      1) GOOGLE_CREDENTIALS  -> conteúdo JSON inline (service account)
      2) GOOGLE_APPLICATION_CREDENTIALS -> caminho do .json
      3) credenciais.json no diretório do script ou no diretório atual
    """
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        log("[auth] usando GOOGLE_CREDENTIALS (inline JSON)")
        try:
            import json as _json
            info = _json.loads(env_json)
            return SACreds.from_service_account_info(info, scopes=SCOPES)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS inválido: {e}")

    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and Path(env_path).is_file():
        log(f"[auth] usando GOOGLE_APPLICATION_CREDENTIALS → {env_path}")
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)

    # Fallback: arquivo local
    script_dir = Path(__file__).resolve().parent
    candidates = [script_dir / CREDENTIALS_PATH, Path.cwd() / CREDENTIALS_PATH]
    for p in candidates:
        if p.is_file():
            log(f"[auth] usando arquivo local → {p}")
            return SACreds.from_service_account_file(str(p), scopes=SCOPES)

    raise FileNotFoundError(
        "Não encontrei credenciais do Google.\n"
        "Defina GOOGLE_CREDENTIALS (JSON inline) OU GOOGLE_APPLICATION_CREDENTIALS (caminho) "
        f"OU coloque '{CREDENTIALS_PATH}' ao lado do script."
    )

# ================== HELPERS ==================
def ensure_size(ws, min_rows, min_cols):
    rows = max(ws.row_count, min_rows)
    cols = max(ws.col_count, min_cols)
    if rows != ws.row_count or cols != ws.col_count:
        log(f"🧩 Redimensionando destino para {rows} linhas × {cols} colunas…")
        with_retry(ws.resize, rows, cols, desc="resize destino")

def safe_clear(ws, ranges):
    if isinstance(ranges, str):
        ranges = [ranges]
    log(f"🧹 Limpando: {', '.join(ranges)}")
    with_retry(ws.batch_clear, ranges, desc=f"batch_clear {ranges}")

def safe_update(ws, a1, values):
    log(f"✍️  Update {a1} ({len(values)} linhas)")
    with_retry(ws.update, range_name=a1, values=values, value_input_option='USER_ENTERED',
               desc=f"update {a1}")

def chunked_update(ws, start_row, start_col_letter, end_col_letter, values):
    n = len(values)
    if n == 0:
        return
    i, bloco = 0, 0
    t0 = time.time()
    while i < n:
        parte = values[i:i+CHUNK_ROWS]
        a1 = f"{start_col_letter}{start_row + i}:{end_col_letter}{start_row + i + len(parte) - 1}"
        bloco += 1
        log(f"🚚 Bloco {bloco}: {a1} ({len(parte)} linhas)")
        safe_update(ws, a1, parte)
        i += len(parte)
        time.sleep(0.2)  # micro pausa para evitar rajadas
    log(f"✅ Upload concluído em {time.time() - t0:.1f}s ({n} linhas)")

def parse_valor(s):
    """Converte strings tipo 'R$ 1.234,56' em float 1234.56; vazio se não parseável."""
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    # Remove qualquer coisa que não seja dígito, separador decimal ou sinal
    s = re.sub(r'[^\d,.\-()]', '', s)
    # Trata parênteses como negativo: (123,45) -> -123,45
    neg = s.startswith('(') and s.endswith(')')
    s = s.strip('()')
    # Milhares: remove pontos quando há vírgula decimal
    if ',' in s and '.' in s:
        s = s.replace('.', '')
    # vírgula decimal -> ponto
    s = s.replace(',', '.')
    try:
        val = float(s)
        return -val if neg else val
    except Exception:
        return ""  # mantém célula vazia

# ================== INÍCIO ==================
def main():
    log("🟢 INÍCIO: copiar A,B (Código/Valor) → BD_EXEC!A,B + status em E2")

    # ---- Autenticação
    log("🔐 Autenticando…")
    creds = make_creds()
    gc = gspread.authorize(creds)

    # ---- Abrir origem/destino
    log("📂 Abrindo origem por URL…")
    planilha_origem = with_retry(gc.open_by_url, URL_ORIGEM, desc="open_by_url origem")
    aba_origem      = with_retry(planilha_origem.worksheet, NOME_ABA_ORIGEM, desc="worksheet origem")

    log("📂 Abrindo destino por ID…")
    planilha_destino = with_retry(gc.open_by_key, ID_PLANILHA_DESTINO, desc="open_by_key destino")
    try:
        aba_destino = with_retry(planilha_destino.worksheet, NOME_ABA_DESTINO, desc="worksheet destino")
    except gspread.WorksheetNotFound:
        log("🆕 Criando aba destino…")
        aba_destino = with_retry(planilha_destino.add_worksheet, title=NOME_ABA_DESTINO, rows=1000, cols=5,
                                 desc="add_worksheet destino")

    # Garante pelo menos colunas até E (status) e B (dados)
    ensure_size(aba_destino, min_rows=2, min_cols=5)

    # ---- Status inicial
    safe_update(aba_destino, "E2", [["Atualizando"]])

    # ---- Leitura
    log(f"📥 Lendo origem: {NOME_ABA_ORIGEM}!{INTERVALO_ORIGEM} …")
    dados = with_retry(aba_origem.get, INTERVALO_ORIGEM, desc="get origem")
    log(f"🔎 Linhas lidas: {len(dados)}")

    # ---- Tratamento/filtragem
    log("🧽 Tratando e filtrando linhas…")
    dados_filtrados = []
    for linha in dados:
        codigo = str(linha[0]).strip() if len(linha) > 0 else ""
        if not codigo:
            continue
        bruto = str(linha[1]).strip() if len(linha) > 1 else ""
        valor = parse_valor(bruto) if bruto else ""
        dados_filtrados.append([codigo, valor])

    log(f"✅ Linhas válidas para envio: {len(dados_filtrados)}")

    # ---- Limpeza (todas as linhas de A2:B) e cabeçalhos
    safe_clear(aba_destino, "A2:B")  # limpa TODAS as linhas de A..B a partir da linha 2
    safe_update(aba_destino, "A1:B1", [["Código", "Valor"]])

    # ---- Upload em blocos
    if dados_filtrados:
        chunked_update(aba_destino, start_row=2, start_col_letter="A", end_col_letter="B", values=dados_filtrados)
    else:
        log("⛔ Nada para escrever.")

    # ---- Formatação opcional (coluna B como número)
    if FORCAR_FORMATACAO and len(dados_filtrados) > 0:
        try:
            log("🎨 Aplicando formatação opcional em B (número)…")
            sheet_id = aba_destino._properties['sheetId']
            end_row_idx = 1 + len(dados_filtrados)  # dados começam na linha 2 (idx 1)

            reqs = {
                "requests": [
                    {"repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": end_row_idx,
                            "startColumnIndex": 1,   # B = 1 (0-based)
                            "endColumnIndex": 2
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }}
                ]
            }
            with_retry(aba_destino.spreadsheet.batch_update, reqs, desc="batch_update formato B")
            log("✅ Formatação aplicada.")
        except APIError as e:
            log(f"⚠️  Falha na formatação opcional (seguindo): {e}")
    else:
        log("⏭️ Formatação opcional desativada ou sem dados.")

    # ---- Status final
    safe_update(aba_destino, "E2", [[f"Atualizado em: {now()}"]])

    log("🏁 FINALIZADO.")

if __name__ == "__main__":
    main()
