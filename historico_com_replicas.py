# run_historico_com_replicas.py
# Executa o importador do Histórico e, se OK, executa o replicador do Histórico.

import os
import subprocess
import sys
import time
import random
from datetime import datetime
from pathlib import Path

# ===== CONFIG =====
# Se False, continua mesmo se o importador falhar (não recomendado)
STOP_ON_FAILURE = True

# Tentativas por script quando retorna RC != 0
MAX_ATTEMPTS = 3

# Backoff base (segundos) — cresce exponencialmente a cada tentativa, com jitter ±20%
BACKOFF_BASE_SECONDS = 5

# Deadline interno: encerra com resumo antes do timeout-minutes do runner matar o
# job. Sem isto, uma degradacao da planilha origem levava o run ate o teto e o
# job morria sem dizer o que ficou de fora.
DEADLINE_MIN = float(os.getenv("PIPELINE_DEADLINE_MIN", "0") or 0)
_T0 = time.monotonic()


def deadline_exceeded() -> bool:
    return DEADLINE_MIN > 0 and (time.monotonic() - _T0) > DEADLINE_MIN * 60.0

# Scripts em ORDEM
SCRIPT_IMPORTADOR = "importador_historico.py"
SCRIPT_REPLICADOR = "replicador_historico.py"

# ===== UTILS =====
def ts():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def banner(msg: str):
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80 + "\n", flush=True)

def _sleep_with_backoff(attempt_idx: int):
    base = BACKOFF_BASE_SECONDS * (2 ** (attempt_idx - 1))
    jitter = base * random.uniform(-0.2, 0.2)
    wait_s = max(1.0, base + jitter)
    print(f"⏳ Repetindo em {wait_s:.1f}s...\n", flush=True)
    time.sleep(wait_s)

def run_script_once(script_path: Path, idx: int, total: int) -> int:
    start = time.perf_counter()
    print(f"▶️  {ts()}  ({idx}/{total}) Iniciando: {script_path.name}", flush=True)
    try:
        result = subprocess.run([sys.executable, "-u", str(script_path)],
                                cwd=str(script_path.parent), check=False)
        elapsed = time.perf_counter() - start
        if result.returncode == 0:
            print(f"✅ {ts()}  ({idx}/{total}) Concluído: {script_path.name}  (em {elapsed:.1f}s)", flush=True)
        else:
            print(f"❌ {ts()}  ({idx}/{total}) Falhou:    {script_path.name}  (em {elapsed:.1f}s)  RC={result.returncode}", flush=True)
        return result.returncode
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"❌ {ts()}  ({idx}/{total}) ERRO ao executar {script_path.name}: {e}  (em {elapsed:.1f}s)", flush=True)
        return 1

def run_with_retries(script_path: Path, idx: int, total: int) -> int:
    rc = 1
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if attempt > 1 and deadline_exceeded():
            print(f"⏰ Deadline de {DEADLINE_MIN:.0f} min atingido — sem mais tentativas "
                  f"para {script_path.name}", flush=True)
            return rc
        if attempt > 1:
            print(f"🔁 Tentativa {attempt}/{MAX_ATTEMPTS} para {script_path.name}", flush=True)
        rc = run_script_once(script_path, idx, total)
        if rc == 0:
            break
        if attempt < MAX_ATTEMPTS:
            _sleep_with_backoff(attempt)
    return rc

# ===== MAIN =====
def main():
    base_dir = Path(__file__).parent.resolve()

    # Checagem de existência
    missing = [s for s in (SCRIPT_IMPORTADOR, SCRIPT_REPLICADOR) if not (base_dir / s).exists()]
    if missing:
        banner("Arquivos não encontrados")
        for m in missing:
            print(f" - {m}")
        print("\nColoque este orquestrador na mesma pasta dos scripts necessários.")
        sys.exit(1)

    banner("HISTÓRICO → RÉPLICAS (orquestrador)")

    if DEADLINE_MIN > 0:
        print(f"⏱️  Deadline interno: {DEADLINE_MIN:.0f} min (PIPELINE_DEADLINE_MIN)", flush=True)

    overall_start = time.perf_counter()

    # 1) Importador
    rc_import = run_with_retries(base_dir / SCRIPT_IMPORTADOR, idx=1, total=2)
    if rc_import != 0 and STOP_ON_FAILURE:
        total_time = time.perf_counter() - overall_start
        banner(f"FIM (INTERROMPIDO) – Importador falhou – Tempo total: {total_time:.1f}s")
        sys.exit(1)

    # 2) Replicador (só roda se importador OK ou se STOP_ON_FAILURE=False)
    if deadline_exceeded():
        total_time = time.perf_counter() - overall_start
        banner(f"FIM (INTERROMPIDO) – Deadline de {DEADLINE_MIN:.0f} min atingido "
               f"apos o importador – Tempo total: {total_time:.1f}s")
        print(f"Resumo: importador RC={rc_import}; {SCRIPT_REPLICADOR} NAO executado "
              f"por deadline. ❌", flush=True)
        sys.exit(1)

    rc_repl = run_with_retries(base_dir / SCRIPT_REPLICADOR, idx=2, total=2)

    total_time = time.perf_counter() - overall_start
    banner(f"FIM – Tempo total: {total_time:.1f}s")

    if rc_import == 0 and rc_repl == 0:
        print("Resumo: Histórico importado e replicado com sucesso. ✅")
        sys.exit(0)
    elif rc_import != 0:
        print("Resumo: Importador falhou. ❌")
        sys.exit(1)
    else:
        print("Resumo: Importador OK, mas replicador falhou. ❌")
        sys.exit(1)

if __name__ == "__main__":
    main()

