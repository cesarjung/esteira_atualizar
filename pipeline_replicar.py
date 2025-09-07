# run_replicar_pipeline.py
import subprocess
import sys
import time
import random
from datetime import datetime
from pathlib import Path

# === CONFIGURAÇÃO ===
STOP_ON_FAILURE = True    # Se False, continua mesmo se algum script falhar
SKIP_MISSING    = False   # Se True, pula scripts ausentes (apenas avisa)

# Tentativas por script quando ele retorna RC != 0 (falha)
MAX_ATTEMPTS_PER_SCRIPT = 3
# Backoff base (segundos). Será multiplicado exponencialmente e com leve jitter.
BACKOFF_BASE_SECONDS = 5

SCRIPTS = [
    "replicar_carteira.py",
    "replicar_bd_exec.py",
    "replicar_cart_plan.py",
    "replicar_ciclo.py",
    "replicar_lv.py",
    "replicar_med_parcial.py",
    "replicar_operacao.py",
    "replicar_zps.py",
]

def banner(msg: str):
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80 + "\n")

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

def main():
    base_dir = Path(__file__).parent.resolve()
    banner("PIPELINE – Execução sequencial dos scripts replicar_*")

    # Checagem de existência
    missing = [s for s in SCRIPTS if not (base_dir / s).exists()]
    if missing and not SKIP_MISSING:
        print("Arquivos não encontrados:")
        for m in missing:
            print(f" - {m}")
        print("\nColoque todos os arquivos na mesma pasta deste pipeline ou habilite SKIP_MISSING=True.")
        sys.exit(1)
    elif missing and SKIP_MISSING:
        print("Aviso: os seguintes scripts estão ausentes e serão pulados:")
        for m in missing:
            print(f" - {m}")

    run_list = [s for s in SCRIPTS if (base_dir / s).exists()] if SKIP_MISSING else SCRIPTS
    total = len(run_list)

    overall_start = time.perf_counter()
    failures = []

    for i, script in enumerate(run_list, start=1):
        rc = run_script_with_retries(base_dir / script, i, total)
        if rc != 0:
            failures.append(script)
            if STOP_ON_FAILURE:
                print("\nInterrompendo pipeline por falha. (Altere STOP_ON_FAILURE=False para continuar apesar dos erros.)")
                break

    total_time = time.perf_counter() - overall_start
    banner(f"FIM DO PIPELINE – Tempo total: {total_time:.1f}s")
    if failures:
        print("Resumo: houve falha em:")
        for f in failures:
            print(f" - {f}")
        sys.exit(1)
    else:
        print("Resumo: todos os passos concluídos com sucesso. ✅")

if __name__ == "__main__":
    main()
