# cache_origem.py — leitura UNICA da origem para as 8 replicas.
#
# A origem (Esteira CCM/BA - Geral, 5,5 MB) responde ~186s com 503 a cada
# requisicao sob carga, independente do tamanho do range: CICLO (428 linhas)
# custava o mesmo que BD_EXEC (18k). Com 8 replicas lendo separado, cada uma
# pagava 3-5 desses 503 (~10 min so de leitura) e o deadline de 90 min estourava
# antes de med_parcial/operacao/zps.
#
# O orquestrador faz UM values:batchGet com os 8 ranges logo apos o BLOCO 2,
# grava cada um em <ORIGEM_CACHE_DIR>/<chave>.json e as replicas leem dali.
# Sem a variavel (script-avulso.yml, execucao local) ou sem o arquivo, carregar()
# devolve None e a replica le a origem como sempre leu.
import json
import os
from pathlib import Path

ENV_DIR = "ORIGEM_CACHE_DIR"

# chave -> range na origem. Todas as replicas leem com o render padrao
# (FORMATTED_VALUE), entao um unico batchGet serve a todas.
RANGES = {
    "carteira":    "'Carteira'!A1:S",
    "bd_exec_ab":  "'BD_EXEC'!A2:B",
    "bd_exec_fj":  "'BD_EXEC'!F2:J",
    "ciclo":       "'CICLO'!D1:T",
    "lv":          "'LV CICLO'!A1:Y",
    "med_parcial": "'MED PARCIAL'!A1:Q",
    "operacao":    "'OPERACAO'!A1:M",
    "zps":         "'zps'",
}

def gravar(cache_dir, value_ranges):
    """Grava um JSON por chave, na ordem de RANGES (a mesma do batchGet)."""
    d = Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    for chave, vr in zip(RANGES, value_ranges):
        vals = (vr or {}).get("values", []) or []
        (d / f"{chave}.json").write_text(json.dumps(vals, ensure_ascii=False), encoding="utf-8")

def carregar(chave, retangular=False):
    """Linhas do cache, ou None para cair na leitura direta da origem.

    retangular=True imita get_all_values() (preenche as linhas com "" ate a
    largura maxima); ws.get()/values_get nao preenchem, e o cache vem cru.
    """
    d = os.environ.get(ENV_DIR)
    if not d:
        return None
    p = Path(d) / f"{chave}.json"
    if not p.is_file():
        return None
    try:
        vals = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"⚠️  Cache da origem ilegível ({chave}): {e} — lendo da origem.", flush=True)
        return None
    if retangular and vals:
        w = max(len(r) for r in vals)
        vals = [r + [""] * (w - len(r)) for r in vals]
    print(f"📦 Origem via leitura em lote (cache '{chave}'): {len(vals)} linhas.", flush=True)
    return vals
