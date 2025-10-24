# === GitHub Actions-friendly Google credentials helper ===
import os, json, pathlib
from google.oauth2.service_account import Credentials as SACreds
Credentials = SACreds  # retrocompat

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

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
        "Credenciais não encontradas. Defina GOOGLE_CREDENTIALS com o JSON "
        "ou GOOGLE_APPLICATION_CREDENTIALS com o caminho do .json, "
        "ou mantenha 'credenciais.json' local."
    )
# === end helper ===

# atualizar_replicar.py — Orquestrador com controle BD_Config e réplicas

import os, re, json, time, random, pathlib
from datetime import datetime
from pathlib import Path
import subprocess
import sys
from typing import Dict, List, Tuple, Optional

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials as SACreds

# =======================
# CONFIGURAÇÕES GERAIS
# =======================
CREDENTIALS_PATH = "credenciais.json"  # usado apenas como fallback local
SPREADSHEET_ID = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
BD_CONFIG_SHEET = "BD_Config"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MAX_ATTEMPTS_PER_STEP = 3
BATCH_GET_MAX_PER_CALL = 40
MAX_API_RETRIES = 6
BASE_SLEEP = 1.1
RETRYABLE_CODES = {429, 500, 502, 503, 504}

BLOCK1 = [
    ("ciclo.py",        2),
    ("lv.py",           3),
    ("med_parcial.py",  4),
    ("operacao.py",     5),
]
BLOCK2 = [
    ("zps_importador.py", 6),
    ("cart_plan.py",      7),
    ("bd_exec.py",        8),
    ("importador_carteira.py", 9),
]

# =======================
# CONFIG DAS RÉPLICAS
# =======================
STOP_ON_FAILURE = True
SKIP_MISSING = False
MAX_ATTEMPTS_PER_SCRIPT = 3
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
# UTILITÁRIOS
# =======================
_STATUS_CACHE: Dict[int, str] = {}

def fmt_now() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def banner(msg: str):
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80 + "\n", flush=True)

def _status_code_from_apierror(e: APIError) -> Optional[int]:
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

def _sleep_backoff(attempt: int, base: float = BASE_SLEEP):
    s = min(60.0, base * (2 ** (attempt - 1)) + random.uniform(0, 0.75))
    time.sleep(s)

# ========= CREDENCIAIS =========
def make_creds_orchestrator():
    # Reusa o helper acima
    return make_creds()

def get_ws():
    creds = make_creds_orchestrator()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(BD_CONFIG_SHEET)

# -------- writes compactadas (D & E) com fallback qualificado --------
def _update_DE_row(ws, row: int, d_val: str, e_val: str):
    attempt = 0
    while True:
        try:
            # Caminho “normal”: worksheet.update no range relativo
            return ws.update(
                range_name=f"D{row}:E{row}",
                values=[[d_val, e_val]],
                value_input_option="USER_ENTERED",
            )
        except APIError as e:
            attempt += 1
            code = _status_code_from_apierror(e)

            # Fallback específico para 404: usa values_update com o nome da aba
            if code == 404:
                try:
                    rng = f"'{ws.title}'!D{row}:E{row}"
                    return ws.spreadsheet.values_update(
                        rng,
                        params={"valueInputOption": "USER_ENTERED"},
                        body={"values": [[d_val, e_val]]},
                    )
                except APIError as e2:
                    code2 = _status_code_from_apierror(e2)
                    if attempt >= MAX_API_RETRIES or (code2 is not None and code2 not in RETRYABLE_CODES):
                        raise
                    _sleep_backoff(attempt)
                    continue

            if attempt >= MAX_API_RETRIES or (code is not None and code not in RETRYABLE_CODES):
                raise

            if code == 429:
                wait = min(60.0, 5.0 * attempt + random.uniform(0, 2.0))
                print(f"[update_DE] ⚠️  429 — aguardando {wait:.1f}s (linha {row})", flush=True)
                time.sleep(wait)
            else:
                print(f"[update_DE] ⚠️  {e} — retry {attempt}/{MAX_API_RETRIES-1}", flush=True)
                _sleep_backoff(attempt)

def set_start(ws, row: int):
    _STATUS_CACHE[row] = "Atualizando"
    _update_DE_row(ws, row, "Atualizando", "")

def set_ok(ws, row: int):
    _STATUS_CACHE[row] = "OK"
    _update_DE_row(ws, row, fmt_now(), "OK")

def set_fail(ws, row: int):
    _STATUS_CACHE[row] = "Falhou"
    _update_DE_row(ws, row, fmt_now(), "Falhou")

# -------- leitura resiliente por Values API --------
def _values_get_resilient(spreadsheet, a1_range: str, desc: str, max_retries: int = MAX_API_RETRIES):
    attempt = 0
    while True:
        try:
            resp = spreadsheet.values_get(a1_range)  # dict
            return resp.get("values", []) or []
        except APIError as e:
            attempt += 1
            code = _status_code_from_apierror(e)
            if attempt >= max_retries or (code is not None and code not in RETRYABLE_CODES):
                raise
            print(f"[{desc}] ⚠️  {e} — retry {attempt}/{max_retries-1}", flush=True)
            _sleep_backoff(attempt)

def get_status_map(ws, rows: List[int]) -> Dict[int, str]:
    if not rows:
        return {}

    out: Dict[int, str] = {}
    missing: List[int] = []
    for r in rows:
        if r in _STATUS_CACHE:
            out[r] = _STATUS_CACHE[r]
        else:
            missing.append(r)

    if not missing:
        return out

    lo, hi = min(missing), max(missing)
    a1 = f"{BD_CONFIG_SHEET}!E{lo}:E{hi}"
    data = _values_get_resilient(ws.spreadsheet, a1, desc="get_status_map(values_get)")

    for i, rr in enumerate(range(lo, hi + 1)):
        v = ""
        if i < len(data) and data[i] and len(data[i]) > 0:
            v = (data[i][0] or "").strip()
        out.setdefault(rr, v)
        _STATUS_CACHE.setdefault(rr, v)

    return {r: out.get(r, "") for r in rows}

# =======================
# EXECUÇÃO DOS PASSOS
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

def ensure_block(ws, base_dir: Path, steps: List[Tuple[str, int]], idx_offset: int = 0) -> int:
    total_planned = len(steps)
    executed = 0
    attempts = {row: 0 for _, row in steps}

    # Primeira passada
    for i, (script, row) in enumerate(steps, start=1):
        attempts[row] += 1
        executed += 1
        run_step(ws, base_dir, script, row, idx_offset + i, idx_offset + total_planned, attempts[row])

    time.sleep(0.7)

    # Reexecuta apenas pendentes
    while True:
        status = get_status_map(ws, [row for _, row in steps])
        pending = [(s, r) for s, r in steps if (status.get(r, "").strip().upper() != "OK")]

        if not pending:
            break

        time.sleep(0.6)

        for script, row in pending:
            if attempts[row] >= MAX_ATTEMPTS_PER_STEP:
                print(f"⚠️  Máximo de tentativas atingido para {script} (linha E{row} ainda != OK).", flush=True)
                continue
            attempts[row] += 1
            executed += 1
            # nota: usa índice do bloco novamente para logs (estético)
            run_step(ws, base_dir, script, row, idx_offset + 1, idx_offset + total_planned, attempts[row])

        status = get_status_map(ws, [row for _, row in steps])
        if all((status.get(r, "").strip().upper() == "OK") for _, r in steps):
            break
        if all(attempts[r] >= MAX_ATTEMPTS_PER_STEP for _, r in steps):
            break

    return executed

# =======================
# EXECUÇÃO DAS RÉPLICAS
# =======================
def _sleep_with_backoff(attempt_idx: int):
    base = BACKOFF_BASE_SECONDS * (2 ** (attempt_idx - 1))
    jitter = base * random.uniform(-0.2, 0.2)
    wait_s = max(1.0, base + jitter)
    print(f"⏳ Repetindo em {wait_s:.1f}s...\n", flush=True)
    time.sleep(wait_s)

def run_script_once(script_path: Path, idx: int, total: int) -> int:
    start = time.perf_counter()
    ts = fmt_now()
    print(f"▶️  {ts}  ({idx}/{total}) Iniciando: {script_path.name}", flush=True)
    try:
        result = subprocess.run([sys.executable, "-u", str(script_path)], cwd=str(script_path.parent), check=False)
        elapsed = time.perf_counter() - start
        if result.returncode == 0:
            print(f"✅ {fmt_now()}  ({idx}/{total}) Concluído: {script_path.name}  (em {elapsed:.1f}s)", flush=True)
        else:
            print(f"❌ {fmt_now()}  ({idx}/{total}) Falhou:    {script_path.name}  (em {elapsed:.1f}s)  RC={result.returncode}", flush=True)
        return result.returncode
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"❌ {fmt_now()}  ({idx}/{total}) ERRO ao executar {script_path.name}: {e}  (em {elapsed:.1f}s)", flush=True)
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
    return rc

def run_replicas(base_dir: Path) -> bool:
    banner("RÉPLICAS: execução sequencial dos scripts replicar_*")

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

    # BLOCO 1
    banner("BLOCO 1: ciclo → lv → med_parcial → operacao")
    _ = ensure_block(ws, base_dir, BLOCK1, idx_offset=0)
    status_b1 = get_status_map(ws, [row for _, row in BLOCK1])
    if not all((status_b1.get(r, "").strip().upper() == "OK") for _, r in BLOCK1):
        print("❌ Nem todos os passos do BLOCO 1 ficaram OK. Interrompendo.", flush=True)
        total_time = time.perf_counter() - overall_start
        banner(f"FIM (INTERROMPIDO) – Tempo total: {total_time:.1f}s")
        sys.exit(1)

    # BLOCO 2
    banner("BLOCO 2: zps_importador → cart_plan → bd_exec → importador_carteira")
    _ = ensure_block(ws, base_dir, BLOCK2, idx_offset=len(BLOCK1))
    status_b2 = get_status_map(ws, [row for _, row in BLOCK2])
    if not all((status_b2.get(r, "").strip().upper() == "OK") for _, r in BLOCK2):
        print("❌ Nem todos os passos do BLOCO 2 ficaram OK. Interrompendo.", flush=True)
        total_time = time.perf_counter() - overall_start
        banner(f"FIM (INTERROMPIDO) – Tempo total: {total_time:.1f}s")
        sys.exit(1)

    # Timestamp padrão do pipeline após atualizações
    _update_DE_row(ws, 1, fmt_now(), "OK (BLOCOS 1+2)")
    print("✅ Atualizações OK. Timestamp gravado em BD_Config!D1:E1.", flush=True)

    # RÉPLICAS
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
