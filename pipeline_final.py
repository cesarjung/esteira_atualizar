# run_pipeline.py
import subprocess
import sys
import time
import re
import random
from datetime import datetime
from pathlib import Path

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# =======================
# CONFIGURAÇÕES
# =======================
CREDENTIALS_PATH = "credenciais.json"
SPREADSHEET_ID = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
BD_CONFIG_SHEET = "BD_Config"

# Tentativas máximas por passo dentro do bloco antes de desistir
MAX_ATTEMPTS_PER_STEP = 3

# Leitura segura: ranges por chamada e tentativas
BATCH_GET_MAX_PER_CALL = 40
MAX_API_RETRIES = 6
BASE_SLEEP = 1.1
RETRYABLE_CODES = {429, 500, 502, 503, 504}

BLOCK1 = [
    ("ciclo.py", 2),
    ("lv.py", 3),
    ("med_parcial.py", 4),
    ("operacao.py", 5),
]
BLOCK2 = [
    ("zps_importador.py", 6),
    ("cart_plan.py", 7),
    ("bd_exec.py", 8),
    ("importador_carteira.py", 9),
]

# =======================
# UTILITÁRIOS
# =======================
def fmt_now():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def banner(msg: str):
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80 + "\n", flush=True)

def _status_code_from_apierror(e: APIError) -> int | None:
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

def _sleep_backoff(attempt: int, base: float = BASE_SLEEP):
    s = min(60.0, base * (2 ** (attempt - 1)) + random.uniform(0, 0.75))
    time.sleep(s)

def retry_update(ws, range_name: str, values, value_input_option: str = "USER_ENTERED", desc="update"):
    attempt = 0
    while True:
        try:
            return ws.update(range_name=range_name, values=values, value_input_option=value_input_option)
        except APIError as e:
            attempt += 1
            code = _status_code_from_apierror(e)
            if attempt >= MAX_API_RETRIES or code not in RETRYABLE_CODES:
                raise
            print(f"[retry_update] ⚠️ {desc} {range_name}: {e} — retry {attempt}/{MAX_API_RETRIES-1}", flush=True)
            _sleep_backoff(attempt)

def batch_get_safe(ws, ranges, max_per_call=BATCH_GET_MAX_PER_CALL, max_retries=MAX_API_RETRIES, base_sleep=BASE_SLEEP, desc="batch_get"):
    if not ranges:
        return []
    results = []
    for i in range(0, len(ranges), max_per_call):
        chunk = ranges[i:i + max_per_call]
        attempt = 0
        while True:
            try:
                results.extend(ws.batch_get(chunk))
                break
            except APIError as e:
                attempt += 1
                code = _status_code_from_apierror(e)
                if attempt >= max_retries or code not in RETRYABLE_CODES:
                    raise
                print(f"[batch_get_safe] ⚠️ {desc}: {e} — retry {attempt}/{max_retries-1} (chunk={len(chunk)})", flush=True)
                _sleep_backoff(attempt, base_sleep)
    return results

def get_ws():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(BD_CONFIG_SHEET)

def set_start(ws, row: int):
    retry_update(ws, range_name=f"D{row}", values=[["Atualizando"]], desc="set_start D")
    retry_update(ws, range_name=f"E{row}", values=[[""]], desc="set_start E")

def set_ok(ws, row: int):
    retry_update(ws, range_name=f"D{row}", values=[[fmt_now()]], desc="set_ok D")
    retry_update(ws, range_name=f"E{row}", values=[["OK"]], desc="set_ok E")

def set_fail(ws, row: int):
    retry_update(ws, range_name=f"D{row}", values=[[fmt_now()]], desc="set_fail D")
    retry_update(ws, range_name=f"E{row}", values=[["Falhou"]], desc="set_fail E")

def is_ok_value(v: str) -> bool:
    return (v or "").strip().upper() == "OK"

def get_status_map(ws, rows):
    rngs = [f"E{r}" for r in rows]
    vals = batch_get_safe(ws, rngs, desc="get_status_map")
    out = {}
    for r, cell in zip(rows, vals):
        value = cell[0][0] if cell and cell[0] else ""
        out[r] = value
    return out

def run_script(script_path: Path, idx: int, total: int, attempt: int) -> int:
    start = time.perf_counter()
    print(f"▶️  {fmt_now()}  ({idx}/{total}) {script_path.name}  [tentativa {attempt}] — iniciando", flush=True)
    try:
        result = subprocess.run([sys.executable, "-u", str(script_path)], cwd=str(script_path.parent), check=False)
        elapsed = time.perf_counter() - start
        if result.returncode == 0:
            print(f"✅ {fmt_now()}  ({idx}/{total}) {script_path.name} — concluído em {elapsed:.1f}s", flush=True)
        else:
            print(f"❌ {fmt_now()}  ({idx}/{total}) {script_path.name} — falhou em {elapsed:.1f}s  RC={result.returncode}", flush=True)
        return result.returncode
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"❌ {fmt_now()}  ({idx}/{total}) {script_path.name} — ERRO: {e} (em {elapsed:.1f}s)", flush=True)
        return 1

def run_step(ws, base_dir: Path, script: str, row: int, idx: int, total: int, attempt: int):
    set_start(ws, row)
    rc = run_script(base_dir / script, idx, total, attempt)
    if rc == 0:
        set_ok(ws, row)
    else:
        set_fail(ws, row)
    return rc == 0

def ensure_block(ws, base_dir: Path, steps, idx_offset: int = 0) -> int:
    """
    Executa todos os passos do bloco ao menos uma vez, depois reexecuta
    apenas os que não estiverem OK (E{row} != "OK"), até MAX_ATTEMPTS_PER_STEP.
    Retorna total de passos efetivamente executados (somando tentativas).
    """
    total_planned = len(steps)
    executed = 0
    attempts = {row: 0 for _, row in steps}

    # Primeira passada
    for i, (script, row) in enumerate(steps, start=1):
        attempts[row] += 1
        executed += 1
        run_step(ws, base_dir, script, row, idx_offset + i, idx_offset + total_planned, attempts[row])

    # Reexecuta somente pendentes com leitura robusta de status
    while True:
        status = get_status_map(ws, [row for _, row in steps])
        pending = [(s, r) for s, r in steps if not is_ok_value(status.get(r, ""))]

        if not pending:
            break

        for script, row in pending:
            if attempts[row] >= MAX_ATTEMPTS_PER_STEP:
                print(f"⚠️  Máximo de tentativas atingido para {script} (linha E{row} ainda != OK).", flush=True)
                continue
            attempts[row] += 1
            executed += 1
            run_step(ws, base_dir, script, row, idx_offset + 1, idx_offset + total_planned, attempts[row])

        status = get_status_map(ws, [row for _, row in steps])
        if all(is_ok_value(status.get(r, "")) for _, r in steps):
            break

        if all(attempts[r] >= MAX_ATTEMPTS_PER_STEP for _, r in steps):
            break

    return executed

def main():
    base_dir = Path(__file__).parent.resolve()
    ws = get_ws()

    banner("PIPELINE – Execução com controle de status na aba BD_Config")
    overall_start = time.perf_counter()

    # =======================
    # BLOCO 1
    # =======================
    banner("BLOCO 1: ciclo → lv → med_parcial → operacao")
    exec_block1 = ensure_block(ws, base_dir, BLOCK1, idx_offset=0)

    status_b1 = get_status_map(ws, [row for _, row in BLOCK1])
    if not all(is_ok_value(status_b1.get(r, "")) for _, r in BLOCK1):
        print("❌ Nem todos os passos do BLOCO 1 ficaram OK. Interrompendo o pipeline.", flush=True)
        total_time = time.perf_counter() - overall_start
        banner(f"FIM DO PIPELINE – Tempo total: {total_time:.1f}s")
        sys.exit(1)

    # =======================
    # BLOCO 2
    # =======================
    banner("BLOCO 2: zps_importador → cart_plan → bd_exec → importador_carteira")
    exec_block2 = ensure_block(ws, base_dir, BLOCK2, idx_offset=len(BLOCK1))

    status_b2 = get_status_map(ws, [row for _, row in BLOCK2])
    if not all(is_ok_value(status_b2.get(r, "")) for _, r in BLOCK2):
        print("❌ Nem todos os passos do BLOCO 2 ficaram OK. Interrompendo o pipeline.", flush=True)
        total_time = time.perf_counter() - overall_start
        banner(f"FIM DO PIPELINE – Tempo total: {total_time:.1f}s")
        sys.exit(1)

    retry_update(ws, range_name="F1", values=[[fmt_now()]], desc="carimbo F1")
    print("✅ Todos os códigos ficaram OK. Timestamp gravado em BD_Config!F1.", flush=True)

    total_time = time.perf_counter() - overall_start
    banner(f"FIM DO PIPELINE – Tempo total: {total_time:.1f}s")
    print(f"Resumo: passos executados (inclui re-tentativas) – Bloco 1: {exec_block1}, Bloco 2: {exec_block2}", flush=True)

if __name__ == "__main__":
    main()
