# congelar_bd_carteira.py — aplica as formulas da BD_Carteira, espera calcular
# e cola tudo como VALORES.
#
# Por que: a BD_Carteira e 100% formula (ninguem digita nela). Z e AK tinham
# formula POR LINHA ate a linha 20000 (~360 MB de texto de formula), AM varre o
# Historico (~100k linhas) e X a zps (104k) para cada projeto. Com as formulas
# vivas, cada escrita do BLOCO 1/2 em Carteira/CICLO/zps/BD_EXEC/... disparava
# o recalculo da aba inteira, e qualquer leitura de VALOR da planilha (replicas,
# bd_esteira_plan, historico) ficava 185s esperando e voltava 503. Ler a mesma
# aba como FORMULA levava 0,4s: o gargalo e o recalculo, nao o tamanho.
#
# Modelo das formulas: aba BD_Formulas, linha 1 = cabecalho na mesma ordem da
# BD_Carteira, linha 2 = formula de cada coluna como TEXTO (gravada RAW, para
# nao calcular ali). Todas vao para a linha 4; a coluna A (=UNIQUE(Carteira!A2:A),
# a coluna vital) tambem, e A3 fica com o cabecalho. Coluna cujo modelo nao usa intervalo aberto
# (A4:A) e formula POR LINHA (hoje Z e AK) e e copiada para baixo so ate o
# ultimo projeto, nao ate a linha 20000.
#
# Se a linha 2 da BD_Formulas estiver vazia e a BD_Carteira ainda tiver
# formulas, o script semeia o modelo a partir dela antes de seguir.
#
# Colunas VIVAS (COLS_VIVAS): continuam formula depois do congelamento porque
# leem parametros digitados na aba Esteira (datas, checkboxes de unidade,
# periodo de conclusoes) direto ou via Z. Elas so dependem da Esteira e de
# colunas da propria BD_Carteira, entao escritas em Carteira/CICLO/zps/... nao
# as recalculam. Achadas pelo cabecalho, nao pela letra.
#
# Seguranca: nada e limpo antes das formulas entrarem (a mesma escrita troca
# valores velhos por formulas). Se a espera pelo calculo estourar, as formulas
# ficam no lugar — a aba continua certa, so pesada como antes — e sai com 1.

import os, re, sys, time, json, hashlib, random
from datetime import datetime

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

try:
    from requests.exceptions import ConnectionError as _ReqConnError
    from requests.exceptions import Timeout as _ReqTimeout
    RETRYABLE_EXC = (APIError, _ReqTimeout, _ReqConnError)
except Exception:  # requests ausente: mantem o comportamento antigo
    RETRYABLE_EXC = (APIError,)

# ========= CONFIG =========
SPREADSHEET_ID = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ALVO       = "BD_Carteira"
ABA_MODELO     = "BD_Formulas"
ABA_CARTEIRA   = "Carteira"
ULTIMA_COL     = "AN"
LINHA_CAB      = 3    # cabecalho da BD_Carteira
LINHA_DADOS    = 4    # ancora das formulas A..AN
CAM_CRED       = "credenciais.json"   # fallback local

# Cabecalhos das colunas que NAO sao congeladas (hoje Z, AA, AB, AD, AK).
# Diretas na Esteira: Status, Filtro Unidade, Status Producao x Ciclo;
# via Status: Grupo, Valor Considerado.
COLS_VIVAS = ("Status", "Grupo", "Valor Considerado", "Filtro Unidade",
              "Status Produção x Ciclo")

# Teto para esperar o calculo. A planilha sob carga responde 503 em ~186s por
# leitura, entao cada volta do laco pode custar alguns minutos.
# Run 41113076236: 5 leituras com 503 antes da primeira completa (~17 min).
ESPERA_MAX_MIN = float(os.getenv("CONGELAR_ESPERA_MAX_MIN", "45"))
ESPERA_INTERVALO_S = 20
MAX_API_RETRIES = 6
RETRYABLE_CODES = {404, 408, 409, 429, 500, 502, 503, 504}

# Celulas que indicam calculo ainda em andamento.
MARCAS_CARREGANDO = ("Loading...", "Carregando...")
# Intervalo aberto a partir da linha 4 (A4:A, $A4:$A): formula de coluna inteira.
RE_INTERVALO_ABERTO = re.compile(r"\$?[A-Z]{1,3}\$?4:\$?[A-Z]{1,3}(?![\$\d])")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def make_creds():
    env = os.environ.get("GOOGLE_CREDENTIALS")
    if env:
        return Credentials.from_service_account_info(json.loads(env), scopes=SCOPES)
    return Credentials.from_service_account_file(CAM_CRED, scopes=SCOPES)

# ========= UTILS =========
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def _status_code(e):
    m = re.search(r"\[(\d+)\]", str(e))
    return int(m.group(1)) if m else None

def com_retry(desc, fn, tentativas=MAX_API_RETRIES):
    for i in range(1, tentativas + 1):
        try:
            return fn()
        except RETRYABLE_EXC as e:
            code = _status_code(e)
            if code is not None and code not in RETRYABLE_CODES:
                raise
            if i == tentativas:
                raise RuntimeError(f"{desc} — falhou após {tentativas} tentativas: {e}")
            espera = min(60.0, 2.0 * (2 ** (i - 1))) + random.uniform(0, 1.5)
            log(f"⚠️ {desc} — tentativa {i}/{tentativas} falhou ({type(e).__name__} {code or ''}) | aguardando {espera:.1f}s")
            time.sleep(espera)

def col_letra(n):  # 1-based
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def col_num(letra):
    n = 0
    for ch in letra:
        n = n * 26 + (ord(ch) - 64)
    return n

def norm(s):
    return re.sub(r"\s+", " ", str(s or "")).strip().casefold()

def valores(resp):
    return [vr.get("values", []) for vr in resp.get("valueRanges", [])]

def linha(vals, i, largura):
    r = vals[i] if i < len(vals) else []
    return [(r[j] if j < len(r) else "") for j in range(largura)]

# ========= ETAPAS =========
def ler_modelo(sh, largura):
    """Le cabecalhos e o modelo; semeia BD_Formulas!A2 se estiver vazia."""
    resp = com_retry("ler modelo e cabecalhos", lambda: sh.values_batch_get(
        [f"'{ABA_MODELO}'!A1:{ULTIMA_COL}2",
         f"'{ABA_ALVO}'!A2:{ULTIMA_COL}{LINHA_CAB}",
         f"'{ABA_ALVO}'!A{LINHA_DADOS}:{ULTIMA_COL}{LINHA_DADOS}"],
        params={"valueRenderOption": "FORMULA"}))
    mod, cab_alvo, ancora = valores(resp)
    cab_mod  = linha(mod, 0, largura)
    formulas = linha(mod, 1, largura)
    cab2     = linha(cab_alvo, 0, largura)
    cab3     = linha(cab_alvo, 1, largura)

    # Cabecalho do modelo tem de bater coluna a coluna com o da BD_Carteira
    # (linha 3, ou linha 2 onde ela tiver titulo).
    erradas = []
    for j in range(largura):
        aceitos = {norm(cab3[j]), norm(cab2[j])} - {""}
        if aceitos and norm(cab_mod[j]) not in aceitos:
            erradas.append(f"{col_letra(j + 1)} ('{cab_mod[j]}' ≠ '{cab3[j]}')")
    if erradas:
        raise RuntimeError(f"Cabeçalho da {ABA_MODELO} não bate com a {ABA_ALVO}: " + "; ".join(erradas))

    if not any(str(f).strip() for f in formulas):
        atual = linha(ancora, 0, largura)
        if not str(atual[0]).startswith("="):
            raise RuntimeError(f"{ABA_MODELO}!A2 vazia e a {ABA_ALVO} não tem fórmulas para semear o modelo.")
        log(f"🌱 {ABA_MODELO} linha 2 vazia — semeando com as fórmulas atuais da {ABA_ALVO}.")
        com_retry("semear modelo", lambda: sh.values_update(
            f"'{ABA_MODELO}'!A2:{ULTIMA_COL}2",
            params={"valueInputOption": "RAW"},
            body={"values": [atual]}))
        formulas = atual

    formulas = [str(f).strip() for f in formulas]
    vazias = [col_letra(j + 1) for j, f in enumerate(formulas) if not f.startswith("=")]
    if formulas[0] == "" or not formulas[0].startswith("="):
        raise RuntimeError(f"{ABA_MODELO}!A2 (coluna vital A) não tem fórmula.")
    if vazias:
        log(f"ℹ️ Colunas sem fórmula no modelo (ficam vazias): {', '.join(vazias)}")
    return formulas, cab_mod

def achar_vivas(cab_mod):
    """Indices (0-based) das COLS_VIVAS pelo cabecalho do modelo."""
    pos = {norm(c): j for j, c in enumerate(cab_mod) if j > 0}
    faltando = [c for c in COLS_VIVAS if norm(c) not in pos]
    if faltando:
        raise RuntimeError(f"Colunas vivas não achadas no cabeçalho da {ABA_MODELO}: {', '.join(faltando)}")
    return sorted(pos[norm(c)] for c in COLS_VIVAS)

def faixas_congelar(largura, vivas):
    """Faixas contiguas [ini, fim) de colunas a congelar (todas menos as vivas)."""
    faixas, j = [], 0
    while j < largura:
        if j in vivas:
            j += 1
            continue
        k = j
        while k < largura and k not in vivas:
            k += 1
        faixas.append((j, k))
        j = k
    return faixas

def contar_projetos(sh):
    """Projetos unicos da Carteira = linhas que o UNIQUE de A4 vai gerar.
    Lido como FORMULA: a Carteira nao tem formula, entao vem o mesmo valor, sem
    esperar o recalculo da planilha (FORMATTED_VALUE levou 11 min de 503)."""
    resp = com_retry("ler Carteira!A", lambda: sh.values_batch_get(
        [f"'{ABA_CARTEIRA}'!A1:A"], params={"valueRenderOption": "FORMULA"}))
    col = [str(r[0]).strip() if r else "" for r in valores(resp)[0]]
    unicos = {v for v in col[1:] if v}  # col[0] = cabecalho (fora do A2:A)
    return len(unicos)

def colunas_por_linha(formulas):
    return [j for j in range(1, len(formulas))
            if formulas[j].startswith("=") and not RE_INTERVALO_ABERTO.search(formulas[j])]

def copiar_por_linha(sh, ws, por_linha, ultima):
    """Formulas por linha: linha 4 copiada ate `ultima`, rabo abaixo limpo."""
    reqs = []
    for j in por_linha:
        if ultima > LINHA_DADOS:
            reqs.append({"copyPaste": {
                "source": {"sheetId": ws.id, "startRowIndex": LINHA_DADOS - 1, "endRowIndex": LINHA_DADOS,
                           "startColumnIndex": j, "endColumnIndex": j + 1},
                "destination": {"sheetId": ws.id, "startRowIndex": LINHA_DADOS, "endRowIndex": ultima,
                                "startColumnIndex": j, "endColumnIndex": j + 1},
                "pasteType": "PASTE_FORMULA"}})
        if ws.row_count > ultima:
            reqs.append({"updateCells": {
                "range": {"sheetId": ws.id, "startRowIndex": ultima, "endRowIndex": ws.row_count,
                          "startColumnIndex": j, "endColumnIndex": j + 1},
                "fields": "userEnteredValue"}})
    if reqs:
        com_retry("copiar fórmulas por linha", lambda: sh.batch_update({"requests": reqs}))
        log(f"✍️ Fórmulas por linha ({', '.join(col_letra(j + 1) for j in por_linha)}) "
            f"copiadas até a linha {ultima}.")

def aplicar_formulas(sh, ws, formulas, por_linha, ultima):
    """UMA escrita troca os valores congelados pelas formulas (nada e limpo
    antes); depois copia as formulas por linha ate `ultima`.
    Nao inverter: copiando Z/AK antes (e92ad7e), as 13 mil formulas por linha
    ja vivas recalculavam a cada coluna de array e o calculo foi de ~8 min
    para ~20-45+ min (runs 41747169101 e 41802367571). Nesta ordem a copia
    leva alguns 503, mas a aba fecha o calculo em segundos depois dela."""
    largura = len(formulas)
    grade = ws.row_count
    # Faixas contiguas de colunas de array: linha 4 = formula, linhas 5..grade
    # = "" para o ARRAYFORMULA ter onde expandir.
    data = []
    j = 0
    while j < largura:
        if j in por_linha:
            data.append({"range": f"'{ABA_ALVO}'!{col_letra(j + 1)}{LINHA_DADOS}",
                         "values": [[formulas[j]]]})
            j += 1
            continue
        k = j
        while k + 1 < largura and (k + 1) not in por_linha:
            k += 1
        bloco = [formulas[j:k + 1]] + [[""] * (k - j + 1) for _ in range(grade - LINHA_DADOS)]
        data.append({"range": f"'{ABA_ALVO}'!{col_letra(j + 1)}{LINHA_DADOS}:{col_letra(k + 1)}{grade}",
                     "values": bloco})
        j = k + 1
    com_retry("gravar fórmulas", lambda: sh.values_batch_update(
        {"valueInputOption": "USER_ENTERED", "data": data}))
    log("✍️ Fórmulas gravadas (linha 4).")
    copiar_por_linha(sh, ws, por_linha, ultima)

def _hash(vals):
    return hashlib.sha256(json.dumps(vals, ensure_ascii=False).encode()).hexdigest()

def esperar_calculo(sh, ws, largura, por_linha, vivas, ultima, esperado):
    """Le os valores ate a primeira leitura completa sem celula carregando.
    Uma basta: com a aba toda viva, ler A3:AN leva ~170-185s e o Google corta
    em ~185s com 503, entao duas leituras seguidas quase nunca passam (runs
    41638130300 e 41651180624: so a volta 6 leu, as outras 13 deram 503).
    O Google so devolve valores com o calculo terminado. A extensao real e a
    do UNIQUE em A: se passar de `ultima`, estende as formulas por linha e
    recomeca. Devolve (ultima, hash das colunas congeladas)."""
    limite = time.monotonic() + ESPERA_MAX_MIN * 60
    volta = 0
    while time.monotonic() < limite:
        volta += 1
        rng = f"'{ABA_ALVO}'!A{LINHA_CAB}:{col_letra(largura)}{ws.row_count}"
        t = time.monotonic()
        try:
            vals = valores(sh.values_batch_get([rng]))[0]
        except RETRYABLE_EXC as e:
            log(f"⏳ volta {volta}: leitura falhou em {time.monotonic() - t:.0f}s "
                f"({type(e).__name__} {_status_code(e) or ''}) — planilha ainda calculando.")
            time.sleep(ESPERA_INTERVALO_S)
            continue
        # vals[i] = linha LINHA_CAB + i
        ult_a = max((LINHA_CAB + i for i, r in enumerate(vals) if r and str(r[0]).strip()), default=LINHA_CAB)
        n_a = sum(1 for r in vals[1:] if r and str(r[0]).strip())
        carregando = sum(1 for r in vals for c in r if str(c).strip() in MARCAS_CARREGANDO)
        log(f"⏳ volta {volta}: leitura em {time.monotonic() - t:.0f}s | projetos em A {n_a} "
            f"(Carteira {esperado}) | última linha {ult_a} | carregando {carregando}")
        if ult_a > ultima:
            ultima = ult_a
            log(f"↕️ UNIQUE foi além do previsto — estendendo fórmulas por linha até {ultima}.")
            copiar_por_linha(sh, ws, por_linha, ultima)
        elif n_a > 0 and carregando == 0:
            ancora = linha(vals, LINHA_DADOS - LINHA_CAB, largura)
            quebradas = [col_letra(j + 1) for j, c in enumerate(ancora) if str(c).startswith("#REF")]
            if quebradas:
                raise RuntimeError(f"#REF! na linha {LINHA_DADOS} (expansão bloqueada?): {', '.join(quebradas)}")
            if n_a != esperado:
                log(f"⚠️ A tem {n_a} projetos e a Carteira {esperado} únicos — "
                    f"diferença de grafia (maiúsculas/espaços); segue com o que o UNIQUE gerou.")
            return ultima, _hash(_so_congeladas(vals, largura, vivas, ultima))
        time.sleep(ESPERA_INTERVALO_S)
    raise RuntimeError(f"Cálculo não estabilizou em {ESPERA_MAX_MIN:.0f} min — fórmulas mantidas na {ABA_ALVO}.")

def colar_valores(sh, ws, faixas, ultima):
    reqs = []
    for ini, fim in faixas:
        rng = {"sheetId": ws.id, "startRowIndex": LINHA_CAB - 1, "endRowIndex": ultima,
               "startColumnIndex": ini, "endColumnIndex": fim}
        reqs.append({"copyPaste": {"source": rng, "destination": rng, "pasteType": "PASTE_VALUES"}})
    com_retry("colar como valores", lambda: sh.batch_update({"requests": reqs}))

def _so_congeladas(vals, largura, vivas, ultima):
    return [[c for j, c in enumerate(linha(vals, i, largura)) if j not in vivas]
            for i in range(ultima - LINHA_CAB + 1)]

def conferir(sh, largura, vivas, ultima, hash_calc):
    """Colunas congeladas: sem formula e com os valores calculados.
    Colunas vivas: formula ainda na linha 4."""
    rng = f"'{ABA_ALVO}'!A{LINHA_CAB}:{col_letra(largura)}{ultima}"
    resp = com_retry("conferir fórmulas", lambda: sh.values_batch_get(
        [rng], params={"valueRenderOption": "FORMULA"}))
    form = valores(resp)[0]
    restantes = sum(1 for r in _so_congeladas(form, largura, vivas, ultima) for c in r if str(c).startswith("="))
    if restantes:
        raise RuntimeError(f"Ainda há {restantes} fórmulas nas colunas congeladas após colar valores.")
    ancora = linha(form, LINHA_DADOS - LINHA_CAB, largura)
    sem = [col_letra(j + 1) for j in vivas if not str(ancora[j]).startswith("=")]
    if sem:
        raise RuntimeError(f"Colunas vivas perderam a fórmula na linha {LINHA_DADOS}: {', '.join(sem)}")
    # Diferenca aqui = alguma fonte mudou entre a leitura e a colagem; o
    # copyPaste colou o valor mais novo, entao so avisa.
    vals = com_retry("conferir valores", lambda: valores(sh.values_batch_get([rng]))[0])
    if _hash(_so_congeladas(vals, largura, vivas, ultima)) != hash_calc:
        log("⚠️ Valores colados diferem da leitura de estabilidade — alguma fonte mudou no meio; ficou o valor mais novo.")

# ========= EXECUÇÃO =========
def main():
    t0 = time.perf_counter()
    gc = gspread.authorize(make_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(ABA_ALVO)
    largura = col_num(ULTIMA_COL)

    formulas, cab_mod = ler_modelo(sh, largura)
    vivas = achar_vivas(cab_mod)
    faixas = faixas_congelar(largura, vivas)
    log(f"🌿 Colunas vivas (seguem fórmula): {', '.join(col_letra(j + 1) for j in vivas)}.")
    n = contar_projetos(sh)
    ultima = LINHA_DADOS - 1 + n
    por_linha = colunas_por_linha(formulas)
    log(f"📋 Carteira: {n} projetos únicos → BD_Carteira até a linha {ultima} (grade {ws.row_count}).")
    if n == 0:
        raise RuntimeError("Carteira sem projetos — não vou congelar uma BD_Carteira vazia.")

    aplicar_formulas(sh, ws, formulas, por_linha, ultima)
    t_calc = time.perf_counter()
    ultima, h = esperar_calculo(sh, ws, largura, por_linha, vivas, ultima, n)
    log(f"✅ Cálculo estável em {time.perf_counter() - t_calc:.0f}s.")

    colar_valores(sh, ws, faixas, ultima)
    conferir(sh, largura, vivas, ultima, h)
    log(f"🧊 {ABA_ALVO} congelada em valores (A{LINHA_CAB}:{ULTIMA_COL}{ultima}, "
        f"menos {', '.join(col_letra(j + 1) for j in vivas)}). "
        f"Duração total {time.perf_counter() - t0:.0f}s.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"❌ {e}")
        sys.exit(1)
