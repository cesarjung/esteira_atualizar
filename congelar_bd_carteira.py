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
# nao calcular ali). B..AN vao para a linha 4; A3 fica com o cabecalho.
# Coluna A (a vital) NAO e formula: o script grava como valor a lista de
# projetos unicos da Carteira!A2:A, na ordem de aparicao (= UNIQUE). Com o
# UNIQUE vivo as 39 colunas recalculavam sobre uma lista ainda sendo gerada e
# o calculo passava de 45 min; com A parada ele fecha em minutos. O modelo de
# A em BD_Formulas!A2 fica so de referencia. Coluna cujo modelo nao usa intervalo aberto
# (A4:A) e formula POR LINHA (hoje Z e AK) e e copiada para baixo so ate o
# ultimo projeto, nao ate a linha 20000.
#
# Se a linha 2 da BD_Formulas estiver vazia e a BD_Carteira ainda tiver
# formulas, o script semeia o modelo a partir dela antes de seguir.
#
# Colunas VIVAS (COLS_VIVAS): continuam formula depois do congelamento porque
# leem parametros digitados na aba Esteira (datas, checkboxes de unidade,
# periodo de conclusoes) direto ou via Z. Achadas pelo cabecalho, nao pela letra.
#
# Ordem (por blocos):
#   0. limpa as vivas (linha 4 ate o fim): Z/AK dependem de quase tudo e,
#      presentes durante o calculo, recalculavam 13 mil linhas a cada coluna
#      que fechava;
#   1. coluna A como valor;
#   2. camadas das colunas congeladas, montadas pelas dependencias entre
#      colunas da propria aba: camada 1 so usa A (+ outras abas), camada 2
#      usa a camada 1, ... Cada camada: grava formulas (linha 4 = formula,
#      abaixo = vazio), espera calcular, cola como valor;
#   3. recoloca as vivas; elas calculam uma vez sobre valores parados, sem
#      espera.
#
# Seguranca: se algo falhar depois de limpar as vivas, elas sao recolocadas
# antes de sair. Camada que estourar a espera fica como formula (certa, so
# pesada) e o script sai com 1.

import os, re, sys, time, json, random
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
    vazias = [col_letra(j + 1) for j, f in enumerate(formulas) if j > 0 and not f.startswith("=")]
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

def ler_projetos(sh):
    """Projetos unicos da Carteira!A2:A na ordem de aparicao (= UNIQUE): o
    conteudo da coluna A da BD_Carteira. Lido como FORMULA: a Carteira nao tem
    formula, entao vem o mesmo valor, sem esperar o recalculo da planilha
    (FORMATTED_VALUE levou 11 min de 503)."""
    resp = com_retry("ler Carteira!A", lambda: sh.values_batch_get(
        [f"'{ABA_CARTEIRA}'!A2:A"], params={"valueRenderOption": "FORMULA"}))
    col = [r[0] if r else "" for r in valores(resp)[0]]
    return list(dict.fromkeys(v for v in col if str(v).strip()))

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

def gravar_projetos(sh, ws, projetos):
    """Coluna A = lista de projetos como valor (RAW), resto da grade limpo."""
    grade = ws.row_count
    vals = [[p] for p in projetos] + [[""] for _ in range(grade - LINHA_DADOS + 1 - len(projetos))]
    com_retry("gravar projetos em A", lambda: sh.values_update(
        f"'{ABA_ALVO}'!A{LINHA_DADOS}:A{grade}",
        params={"valueInputOption": "RAW"}, body={"values": vals}))
    log(f"✍️ Coluna A: {len(projetos)} projetos gravados como valor.")

RE_OUTRA_ABA = re.compile(r"('[^']+'|[A-Za-z_][A-Za-z0-9_]*)![$A-Z0-9:]+")
RE_REF_CEL   = re.compile(r"(?<![A-Za-z0-9_$])\$?([A-Z]{1,3})\$?\d+")
RE_REF_FAIXA = re.compile(r"(?<![A-Za-z0-9_$])\$?([A-Z]{1,3})\$?\d*:\$?([A-Z]{1,3})(?![A-Za-z(])")

def deps_internas(formula, largura):
    """Colunas da propria aba (indices 0-based) citadas na formula."""
    t = re.sub(r'"[^"]*"', "", formula)          # strings
    t = RE_OUTRA_ABA.sub(" ", t)                  # refs de outras abas
    letras = set(RE_REF_CEL.findall(t))
    for a, b in RE_REF_FAIXA.findall(t):
        letras.update((a, b))
    return {col_num(l) - 1 for l in letras if col_num(l) <= largura}

def montar_camadas(formulas, vivas):
    """Camadas das colunas congeladas: cada uma so depende de A, de outras
    abas e de camadas anteriores. Congelada que dependa de viva = erro."""
    largura = len(formulas)
    resto = [j for j in range(1, largura) if j not in vivas and formulas[j].startswith("=")]
    deps = {j: deps_internas(formulas[j], largura) - {j} for j in resto}
    erradas = [col_letra(j + 1) for j in resto if deps[j] & set(vivas)]
    if erradas:
        raise RuntimeError(f"Colunas congeladas que dependem de coluna viva: {', '.join(erradas)}")
    prontas, camadas = {0}, []
    while resto:
        camada = [j for j in resto if deps[j] <= prontas]
        if not camada:
            raise RuntimeError("Dependência circular entre colunas: "
                               + ", ".join(col_letra(j + 1) for j in resto))
        camadas.append(camada)
        prontas.update(camada)
        resto = [j for j in resto if j not in camada]
    return camadas

def faixas(cols):
    """Colunas (0-based) agrupadas em faixas contiguas [ini, fim)."""
    out = []
    for j in sorted(cols):
        if out and out[-1][1] == j:
            out[-1][1] = j + 1
        else:
            out.append([j, j + 1])
    return [tuple(f) for f in out]

def nomes(cols):
    return ", ".join(col_letra(a + 1) if b - a == 1 else f"{col_letra(a + 1)}-{col_letra(b)}"
                     for a, b in faixas(cols))

def limpar_colunas(sh, ws, cols):
    """Apaga o conteudo das colunas da linha 4 ate o fim da grade."""
    reqs = [{"updateCells": {
        "range": {"sheetId": ws.id, "startRowIndex": LINHA_DADOS - 1, "endRowIndex": ws.row_count,
                  "startColumnIndex": a, "endColumnIndex": b},
        "fields": "userEnteredValue"}} for a, b in faixas(cols)]
    com_retry("limpar colunas", lambda: sh.batch_update({"requests": reqs}))

def gravar_formulas(sh, ws, formulas, cols, por_linha, ultima):
    """Linha 4 = formula; abaixo = vazio (o ARRAYFORMULA expande ali). Coluna
    por linha: linha 4 copiada ate `ultima`."""
    grade = ws.row_count
    data = []
    for a, b in faixas(cols):
        data.append({"range": f"'{ABA_ALVO}'!{col_letra(a + 1)}{LINHA_DADOS}:{col_letra(b)}{grade}",
                     "values": [formulas[a:b]] + [[""] * (b - a) for _ in range(grade - LINHA_DADOS)]})
    com_retry("gravar fórmulas", lambda: sh.values_batch_update(
        {"valueInputOption": "USER_ENTERED", "data": data}))
    copiar_por_linha(sh, ws, [j for j in cols if j in por_linha], ultima)

def esperar_calculo(sh, cols, ultima):
    """Le as colunas ate a primeira leitura completa sem celula carregando
    (o Google so devolve valores com o calculo terminado; com a aba pesada a
    leitura volta 503 em ~185s)."""
    rngs = [f"'{ABA_ALVO}'!{col_letra(a + 1)}{LINHA_DADOS}:{col_letra(b)}{ultima}" for a, b in faixas(cols)]
    limite = time.monotonic() + ESPERA_MAX_MIN * 60
    volta = 0
    while time.monotonic() < limite:
        volta += 1
        t = time.monotonic()
        try:
            blocos = valores(sh.values_batch_get(rngs))
        except RETRYABLE_EXC as e:
            log(f"   ⏳ volta {volta}: leitura falhou em {time.monotonic() - t:.0f}s "
                f"({type(e).__name__} {_status_code(e) or ''}) — planilha ainda calculando.")
            time.sleep(ESPERA_INTERVALO_S)
            continue
        carregando = sum(1 for bl in blocos for r in bl for c in r if str(c).strip() in MARCAS_CARREGANDO)
        log(f"   ⏳ volta {volta}: leitura em {time.monotonic() - t:.0f}s | carregando {carregando}")
        if carregando == 0:
            quebradas = [col_letra(a + 1 + k) for (a, b), bl in zip(faixas(cols), blocos)
                         for k, c in enumerate(bl[0] if bl else []) if str(c).startswith("#REF")]
            if quebradas:
                raise RuntimeError(f"#REF! na linha {LINHA_DADOS} (expansão bloqueada?): {', '.join(quebradas)}")
            return
        time.sleep(ESPERA_INTERVALO_S)
    raise RuntimeError(f"Cálculo não estabilizou em {ESPERA_MAX_MIN:.0f} min — fórmulas mantidas na {ABA_ALVO}.")

def colar_valores(sh, ws, cols, ultima):
    reqs = []
    for a, b in faixas(cols):
        rng = {"sheetId": ws.id, "startRowIndex": LINHA_DADOS - 1, "endRowIndex": ultima,
               "startColumnIndex": a, "endColumnIndex": b}
        reqs.append({"copyPaste": {"source": rng, "destination": rng, "pasteType": "PASTE_VALUES"}})
    com_retry("colar como valores", lambda: sh.batch_update({"requests": reqs}))

def conferir_coluna_a(sh, projetos, ultima):
    resp = com_retry("conferir coluna A", lambda: sh.values_batch_get(
        [f"'{ABA_ALVO}'!A{LINHA_DADOS}:A{ultima}"], params={"valueRenderOption": "FORMULA"}))
    col_a = [r[0] if r else "" for r in valores(resp)[0]]
    col_a += [""] * (len(projetos) - len(col_a))
    difs = [i for i, (a, p) in enumerate(zip(col_a, projetos)) if str(a) != str(p)]
    if difs:
        i = difs[0]
        raise RuntimeError(f"Coluna A não bate com a Carteira (linha {LINHA_DADOS + i}: '{col_a[i]}' ≠ '{projetos[i]}').")

def conferir(sh, cols, ultima, esperado_formula):
    """Colunas `cols` com (True) ou sem (False) formula da linha 4 a `ultima`."""
    rngs = [f"'{ABA_ALVO}'!{col_letra(a + 1)}{LINHA_DADOS}:{col_letra(b)}{ultima}" for a, b in faixas(cols)]
    blocos = com_retry("conferir", lambda: valores(sh.values_batch_get(
        rngs, params={"valueRenderOption": "FORMULA"})))
    if esperado_formula:
        sem = [col_letra(a + 1 + k) for (a, b), bl in zip(faixas(cols), blocos)
               for k in range(b - a) if not str(linha(bl, 0, b - a)[k]).startswith("=")]
        if sem:
            raise RuntimeError(f"Colunas sem fórmula na linha {LINHA_DADOS}: {', '.join(sem)}")
    else:
        restantes = sum(1 for bl in blocos for r in bl for c in r if str(c).startswith("="))
        if restantes:
            raise RuntimeError(f"Ainda há {restantes} fórmulas em {nomes(cols)} após colar valores.")

def recolocar_vivas(sh, ws, formulas, vivas, por_linha, ultima):
    gravar_formulas(sh, ws, formulas, vivas, por_linha, ultima)
    conferir(sh, vivas, LINHA_DADOS, True)
    log(f"🌿 Colunas vivas recolocadas: {nomes(vivas)}.")

# ========= EXECUÇÃO =========
def main():
    t0 = time.perf_counter()
    gc = gspread.authorize(make_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(ABA_ALVO)
    largura = col_num(ULTIMA_COL)

    formulas, cab_mod = ler_modelo(sh, largura)
    vivas = achar_vivas(cab_mod)
    camadas = montar_camadas(formulas, vivas)
    por_linha = colunas_por_linha(formulas)
    log(f"🌿 Colunas vivas: {nomes(vivas)}.")
    for i, c in enumerate(camadas, 1):
        log(f"🧱 Camada {i} ({len(c)} colunas): {nomes(c)}.")
    projetos = ler_projetos(sh)
    n = len(projetos)
    ultima = LINHA_DADOS - 1 + n
    log(f"📋 Carteira: {n} projetos únicos → BD_Carteira até a linha {ultima} (grade {ws.row_count}).")
    if n == 0:
        raise RuntimeError("Carteira sem projetos — não vou congelar uma BD_Carteira vazia.")

    limpar_colunas(sh, ws, vivas)
    log(f"🧽 Colunas vivas limpas: {nomes(vivas)}.")
    try:
        gravar_projetos(sh, ws, projetos)
        conferir_coluna_a(sh, projetos, ultima)
        for i, cols in enumerate(camadas, 1):
            t = time.perf_counter()
            gravar_formulas(sh, ws, formulas, cols, por_linha, ultima)
            log(f"✍️ Camada {i}: fórmulas gravadas ({nomes(cols)}).")
            esperar_calculo(sh, cols, ultima)
            colar_valores(sh, ws, cols, ultima)
            conferir(sh, cols, ultima, False)
            log(f"🧊 Camada {i} congelada em {time.perf_counter() - t:.0f}s.")
    finally:
        recolocar_vivas(sh, ws, formulas, vivas, por_linha, ultima)
    log(f"✅ {ABA_ALVO} congelada (A{LINHA_DADOS}:{ULTIMA_COL}{ultima}, menos {nomes(vivas)}). "
        f"Duração total {time.perf_counter() - t0:.0f}s.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"❌ {e}")
        sys.exit(1)
