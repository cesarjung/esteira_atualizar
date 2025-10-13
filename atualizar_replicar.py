# ---- usar credenciais do GitHub Actions (Secret GOOGLE_CREDENTIALS) ----
import os, pathlib
CREDS_ENV = os.environ.get("GOOGLE_CREDENTIALS")
CREDS_PATH = pathlib.Path("credenciais.json")
if CREDS_ENV and not CREDS_PATH.exists():
    CREDS_PATH.write_text(CREDS_ENV, encoding="utf-8")
# -----------------------------------------------------------------------
# run_atualizar_com_replicas.py
# Orquestrador único: Atualiza bancos (BLOCO 1 e BLOCO 2) e, se OK, executa as réplicas (replicar_*).
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
# CONFIGURAÇÕES GERAIS
# =======================
CREDENTIALS_PATH = "credenciais.json"
SPREADSHEET_ID = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
BD_CONFIG_SHEET = "BD_Config"

# Tentativas e limites para leituras/atualizações na planilha de controle
MAX_ATTEMPTS_PER_STEP = 3
BATCH_GET_MAX_PER_CALL = 40
MAX_API_RETRIES = 6
BASE_SLEEP = 1.1
RETRYABLE_CODES = {429, 500, 502, 503, 504}

# Passos de atualização (iguais ao seu run_pipeline.py)
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
# CONFIG DAS RÉPLICAS
# =======================
STOP_ON_FAILURE = True     # Se False, continua mesmo se algum replicar_* falhar
SKIP_MISSING = False       # Se True, pula scripts de réplica ausentes (apenas avisa)

# Tentativas por script replicar_* (quando retorna RC != 0)
MAX_ATTEMPTS_PER_SCRIPT = 3
# Backoff base (segundos). Será multiplicado exponencialmente e com leve jitter.
BACKOFF_BASE_SECONDS = 5

SCRIPTS_REPLICA = [
    "replicar_carteira.py",
    "replicar_bd_exec.py",
    "replicar_cart_plan.py",
    "replicar_ciclo.py",
    "replicar_lv.py",
    "replicar_med_parcial.py",
    "replicar_operacao.py",
    "replicar_zps.py",
]

# =======================
# UTILITÁRIOS COMUNS
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

def _retry_5xx(fn, *args, max_tries=MAX_API_RETRIES, base=BASE_SLEEP, **kwargs):
    """Retry helper para 429/5xx com backoff e jitter."""
    last = None
    for attempt in range(1, max_tries + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            last = e
            code = _status_code_from_apierror(e)
            if code in RETRYABLE_CODES:
                _sleep_backoff(attempt, base)
                continue
            raise
    raise last

def retry_update(ws, range_name: str, values, value_input_option: str = "USER_ENTERED", desc="update"):
    """
    Update com retry. Para 429, usa backoff adicional.
    """
    attempt = 0
    while True:
        try:
            return ws.update(range_name=range_name, values=values, value_input_option=value_input_option)
        except APIError as e:
            attempt += 1
            code = _status_code_from_apierror(e)
            if attempt >= MAX_API_RETRIES or code not in RETRYABLE_CODES:
                raise
            # 429 precisa de respiro maior (evita 'per minute per user')
            if code == 429:
                wait = min(60.0, 5.0 * attempt + random.uniform(0, 2.0))
                print(f"[retry_update] ⏳ {desc} {range_name}: 429 rate limit — aguardando {wait:.1f}s (tentativa {attempt})", flush=True)
                time.sleep(wait)
            else:
                print(f"[retry_update] ⚠️ {desc} {range_name}: {e} — retry {attempt}/{MAX_API_RETRIES-1}", flush=True)
                _sleep_backoff(attempt)

def batch_get_safe(ws, ranges, max_per_call=BATCH_GET_MAX_PER_CALL, max_retries=MAX_API_RETRIES, base_sleep=BASE_SLEEP, desc="batch_get"):
    """
    Mantém a MESMA lógica: ler uma lista de ranges e devolver como ws.batch_get.
    Estratégia robusta contra 503/500/429:
      1) Tenta em chunks (≤ max_per_call, começando conservador).
      2) Se 5xx/429, rebaixa para chunk=1.
      3) Se ainda falhar, fallback para ws.get(a1) serial, preservando a ordem.
    """
    if not ranges:
        return []

    results = []
    n = len(ranges)
    i = 0
    cur_chunk = min(max_per_call, 8)

    while i < n:
        grp = ranges[i:i + cur_chunk]
        try:
            res = _retry_5xx(ws.batch_get, grp, max_tries=max_retries, base=base_sleep)
            results.extend(res)
            i += cur_chunk
        except APIError as e:
            code = _status_code_from_apierror(e)
            if code in RETRYABLE_CODES:
                if cur_chunk > 1:
                    print(f"[batch_get_safe] ⚠️ {desc}: {e} — rebaixando para chunk=1 (era {cur_chunk})", flush=True)
                    cur_chunk = 1
                    continue
                # chunk=1 e ainda falhou → fallback serial
                a1 = grp[0]
                try:
                    val = _retry_5xx(ws.get, a1, max_tries=max_retries, base=base_sleep)
                    results.append(val if isinstance(val, list) else [])
                    i += 1
                except APIError as e2:
                    print(f"[batch_get_safe] ⚠️ fallback ws.get falhou em {a1}: {e2}", flush=True)
                    results.append([])
                    i += 1
            else:
                raise
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

# ---- Escrita de status (D/E) em chamada ÚNICA ----
def set_start(ws, row: int):
    # D{row}="Atualizando", E{row}=""
    retry_update(ws, range_name=f"D{row}:E{row}", values=[["Atualizando", ""]], desc="set_start D:E")

def set_ok(ws, row: int):
    # D{row}=timestamp, E{row}="OK"
    retry_update(ws, range_name=f"D{row}:E{row}", values=[[fmt_now(), "OK"]], desc="set_ok D:E")

def set_fail(ws, row: int):
    # D{row}=timestamp, E{row}="Falhou"
    retry_update(ws, range_name=f"D{row}:E{row}", values=[[fmt_now(), "Falhou"]], desc="set_fail D:E")

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

# =======================
# EXECUÇÃO DOS PASSOS (ATUALIZAÇÃO)
# =======================
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

        # FIX: releitura correta
        status = get_status_map(ws, [r for _, r in steps])
        if all(is_ok_value(status.get(r, "")) for _, r in steps):
            break

        if all(attempts[r] >= MAX_ATTEMPTS_PER_STEP for _, r in steps):
            break

    return executed

# =======================
# EXECUÇÃO DAS RÉPLICAS
# =======================
def _sleep_with_backoff(attempt_idx: int):
    # Exponencial: base * (2^(attempt-1)), com jitter ±20%
    base = BACKOFF_BASE_SECONDS * (2 ** (attempt_idx - 1))
    jitter = base * random.uniform(-0.2, 0.2)
    wait_s = max(1.0, base + jitter)
    print(f"⏳ Repetindo em {wait_s:.1f}s...\n", flush=True)
    time.sleep(wait_s)

def run_script_once(script_path: Path, idx: int, total: int) -> int:
    start = time.perf_counter()
    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"▶️  {ts}  ({idx}/{total}) Iniciando: {script_path.name}", flush=True)
    try:
        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            cwd=str(script_path.parent),
            check=False
        )
        elapsed = time.perf_counter() - start
        if result.returncode == 0:
            print(f"✅ {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}  ({idx}/{total}) Concluído: {script_path.name}  (em {elapsed:.1f}s)", flush=True)
        else:
            print(f"❌ {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}  ({idx}/{total}) Falhou:    {script_path.name}  (em {elapsed:.1f}s)  RC={result.returncode}", flush=True)
        return result.returncode
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"❌ {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}  ({idx}/{total}) ERRO ao executar {script_path.name}: {e}  (em {elapsed:.1f}s)", flush=True)
        return 1

def run_script_with_retries(script_path: Path, idx: int, total: int) -> int:
    for attempt in range(1, MAX_ATTEMPTS_PER_SCRIPT + 1):
        if attempt > 1:
            print(f"🔁 Tentativa {attempt}/{MAX_ATTEMPTS_PER_SCRIPT} para {script_path.name}", flush=True)
        rc = run_script_once(script_path, idx, total)
        if rc == 0:
            return 0
        if attempt < MAX_ATTEMPTS_PER_SCRIPT:
            _sleep_with_backoff(attempt)
    return rc  # falha definitiva

def run_replicas(base_dir: Path) -> bool:
    banner("RÉPLICAS: execução sequencial dos scripts replicar_*")

    # Checagem de existência
    missing = [s for s in SCRIPTS_REPLICA if not (base_dir / s).exists()]
    if missing and not SKIP_MISSING:
        print("Arquivos de réplica não encontrados:")
        for m in missing:
            print(f" - {m}")
        print("\nColoque todos os arquivos na mesma pasta deste orquestrador ou habilite SKIP_MISSING=True.")
        return False
    elif missing and SKIP_MISSING:
        print("Aviso: os seguintes scripts de réplica estão ausentes e serão pulados:")
        for m in missing:
            print(f" - {m}")

    run_list = [s for s in SCRIPTS_REPLICA if (base_dir / s).exists()] if SKIP_MISSING else SCRIPTS_REPLICA
    total = len(run_list)

    failures = []
    overall_start = time.perf_counter()
    for i, script in enumerate(run_list, start=1):
        rc = run_script_with_retries(base_dir / script, i, total)
        if rc != 0:
            failures.append(script)
            if STOP_ON_FAILURE:
                print("\nInterrompendo réplicas por falha. (Defina STOP_ON_FAILURE=False para continuar apesar dos erros.)")
                break

    total_time = time.perf_counter() - overall_start
    banner(f"FIM DAS RÉPLICAS – Tempo total: {total_time:.1f}s")
    if failures:
        print("Resumo (réplicas): houve falha em:")
        for f in failures:
            print(f" - {f}")
        return False
    else:
        print("Resumo (réplicas): todos os passos concluídos com sucesso. ✅")
        return True

# =======================
# MAIN
# =======================
def main():
    base_dir = Path(__file__).parent.resolve()
    ws = get_ws()

    banner("PIPELINE – Atualização com controle de status na aba BD_Config")
    overall_start = time.perf_counter()

    # -------- BLOCO 1 --------
    banner("BLOCO 1: ciclo → lv → med_parcial → operacao")
    exec_block1 = ensure_block(ws, base_dir, BLOCK1, idx_offset=0)
    status_b1 = get_status_map(ws, [row for _, row in BLOCK1])
    if not all(is_ok_value(status_b1.get(r, "")) for _, r in BLOCK1):
        print("❌ Nem todos os passos do BLOCO 1 ficaram OK. Interrompendo.", flush=True)
        total_time = time.perf_counter() - overall_start
        banner(f"FIM (INTERROMPIDO) – Tempo total: {total_time:.1f}s")
        sys.exit(1)

    # -------- BLOCO 2 --------
    banner("BLOCO 2: zps_importador → cart_plan → bd_exec → importador_carteira")
    exec_block2 = ensure_block(ws, base_dir, BLOCK2, idx_offset=len(BLOCK1))
    status_b2 = get_status_map(ws, [row for _, row in BLOCK2])
    if not all(is_ok_value(status_b2.get(r, "")) for _, r in BLOCK2):
        print("❌ Nem todos os passos do BLOCO 2 ficaram OK. Interrompendo.", flush=True)
        total_time = time.perf_counter() - overall_start
        banner(f"FIM (INTERROMPIDO) – Tempo total: {total_time:.1f}s")
        sys.exit(1)

    # Timestamp padrão do seu pipeline após atualizações (mantido)
    retry_update(ws, range_name="F1", values=[[fmt_now()]], desc="carimbo F1 (após BLOCO 2)")
    print("✅ Atualizações OK. Timestamp gravado em BD_Config!F1.", flush=True)

    # -------- RÉPLICAS --------
    ok_replicas = run_replicas(base_dir)

    total_time = time.perf_counter() - overall_start
    banner(f"FIM DO ORQUESTRADOR – Tempo total: {total_time:.1f}s")
    if not ok_replicas:
        print("Resumo geral: Atualizações OK, mas houve falhas em réplicas.", flush=True)
        sys.exit(1)
    else:
        print("Resumo geral: Atualizações OK e réplicas OK. ✅", flush=True)

if __name__ == "__main__":
    main()
