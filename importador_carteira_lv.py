import time
from datetime import datetime
import unicodedata

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

# === CONFIGURAÇÕES ===
ORIGEM_ID = '1lUNIeWCddfmvJEjWJpQMtuR4oRuMsI3VImDY0xBp3Bs'
DESTINO_ID = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA_ORIGEM = 'Carteira'
ABA_DESTINO = 'Carteira'
CAMINHO_CREDENCIAIS = r'C:\Users\Sirtec\Desktop\Importador Carteira\credenciais.json'

# Colunas da origem (letras) na ordem desejada
COLUNAS_ORIGEM = ['A', 'Z', 'B', 'C', 'D', 'E', 'U', 'T', 'N', 'AA', 'AB', 'CN', 'CQ', 'CR', 'CS', 'BQ', 'CE', 'V']

# Tamanho do bloco (linhas por chunk)
CHUNK_ROWS = 2000

def log(msg):
    print(f"[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] {msg}", flush=True)

def retry(callable_, *args, **kwargs):
    """Retry exponencial para erros transitórios (429/500/503)."""
    for tent in range(6):  # 0..5 -> ~1s,2s,4s,8s,16s,32s
        try:
            return callable_(*args, **kwargs)
        except APIError as e:
            msg = str(e)
            if any(code in msg for code in ('429', '500', '503')):
                espera = min(60, 2 ** tent)
                log(f'⚠️  API {msg.strip()} — tent={tent+1} | aguardando {espera}s...')
                time.sleep(espera)
                continue
            raise

def norm_sem_acentos_up(s: str) -> str:
    if s is None:
        return ''
    s = str(s).strip().upper()
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

# === AUTENTICAÇÃO ===
log('🔐 Autenticando...')
escopos = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
credenciais = Credentials.from_service_account_file(CAMINHO_CREDENCIAIS, scopes=escopos)
cliente = gspread.authorize(credenciais)

# === ABRIR PLANILHAS ===
log('📂 Abrindo planilhas...')
planilha_origem = retry(cliente.open_by_key, ORIGEM_ID)
planilha_destino = retry(cliente.open_by_key, DESTINO_ID)

aba_origem = retry(planilha_origem.worksheet, ABA_ORIGEM)
aba_destino = retry(planilha_destino.worksheet, ABA_DESTINO)

# === IDENTIFICAR ÍNDICES DAS COLUNAS ===
log('🧭 Mapeando colunas e lendo cabeçalhos...')
cabecalhos_completos = retry(aba_origem.row_values, 5)  # linha 5
col_indices = []
for letra in COLUNAS_ORIGEM:
    idx = gspread.utils.a1_to_rowcol(letra + '1')[1] - 1
    col_indices.append(idx)

# === OBTER DADOS A PARTIR DA LINHA 5 ===
log('⬇️  Lendo dados da origem...')
dados_completos = retry(aba_origem.get_all_values)
dados = dados_completos[4:]  # linha 5 em diante

# === FILTRAR APENAS AS COLUNAS DESEJADAS ===
log('🔎 Filtrando colunas selecionadas...')
dados_filtrados = []
for linha in dados:
    if len(linha) > 0 and str(linha[0]).strip():  # verifica se coluna A está preenchida
        nova_linha = []
        for idx in col_indices:
            valor = linha[idx] if idx < len(linha) else ''
            nova_linha.append(valor)
        dados_filtrados.append(nova_linha)

# === MONTAR DATAFRAME COM CABEÇALHOS ===
cabecalhos_selecionados = [cabecalhos_completos[i] if i < len(cabecalhos_completos) else '' for i in col_indices]
df = pd.DataFrame(dados_filtrados, columns=cabecalhos_selecionados)
log(f'🧱 DataFrame montado: {len(df)} linhas x {len(df.columns)} colunas.')

# === AJUSTES ESPECÍFICOS (mantendo sua lógica atual) ===
# 1) Datas na primeira coluna selecionada
if len(cabecalhos_selecionados) > 0:
    col_data = cabecalhos_selecionados[0]
    if col_data in df.columns:
        try:
            datas_convertidas = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce')
            df[col_data] = datas_convertidas.fillna(df[col_data])
            log('📅 Conversão de datas aplicada na primeira coluna selecionada (quando possível).')
        except Exception as e:
            log(f"⚠️  Erro ao converter datas: {e}")

# 2) Conversão numérica da coluna 'AC' (se existir)
if "AC" in df.columns:
    try:
        df["AC"] = (
            df["AC"].astype(str)
            .str.replace("R$", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["AC"] = pd.to_numeric(df["AC"], errors='coerce')
        log("🔢 Conversão numérica aplicada na coluna 'AC'.")
    except Exception as e:
        log(f"⚠️  Erro ao converter coluna AC para número: {e}")

# === LIMPAR ABA DESTINO E SINALIZAR STATUS ===
log('🧹 Limpando aba destino...')
retry(aba_destino.clear)

log('⏳ Escrevendo status de execução em T2...')
agora_ini = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
retry(aba_destino.update, range_name='T2', values=[[f'Atualizando... {agora_ini}']])

# === PRÉ-REDIMENSIONAR ABA DESTINO (evita resize interno do set_with_dataframe) ===
rows_needed = len(df) + 1  # + cabeçalho
cols_needed = len(df.columns)
if (aba_destino.row_count < rows_needed) or (aba_destino.col_count < cols_needed):
    log(f'📐 Redimensionando aba destino para {rows_needed} linhas x {cols_needed} colunas...')
    retry(aba_destino.resize, rows_needed, cols_needed)
else:
    log('📐 Redimensionamento não necessário.')

# === ESCREVER CABEÇALHO ===
log('🧾 Escrevendo cabeçalho...')
last_cell_header = rowcol_to_a1(1, cols_needed)  # ex.: R1 (se houver 18 colunas)
header_range = f'A1:{last_cell_header}'
retry(aba_destino.update, range_name=header_range, values=[list(df.columns)])

# === ESCREVER DADOS EM BLOCOS ===
total = len(df)
log(f'🚚 Iniciando escrita em blocos de {CHUNK_ROWS} linhas (total: {total})...')
inicio = 0
bloco = 1
while inicio < total:
    fim = min(inicio + CHUNK_ROWS, total)
    chunk_df = df.iloc[inicio:fim]

    log(f'   ▶️  Bloco {bloco}: linhas {inicio+1}–{fim}...')
    # Escreve bloco começando na linha 2 (após o cabeçalho)
    retry(
        set_with_dataframe,
        aba_destino,
        chunk_df,
        row=2 + inicio,
        col=1,
        include_index=False,
        include_column_header=False,
        resize=False
    )
    log(f'   ✅ Bloco {bloco} concluído ({fim}/{total}).')
    inicio = fim
    bloco += 1

log('✅ Escrita em blocos finalizada.')

# === PÓS-IMPORTAÇÃO: INSERIR LINHAS DA ABA CICLO QUE NÃO ESTÃO NA CARTEIRA ===
log('🔗 Verificando itens da aba CICLO que não estão na CARTEIRA...')
aba_ciclo = retry(planilha_destino.worksheet, 'CICLO')
dados_ciclo = retry(aba_ciclo.get_all_values)

coluna_E = [linha[4].strip() for linha in dados_ciclo[1:] if len(linha) > 4]  # E (ID)
coluna_C = [linha[2].strip() if len(linha) > 2 else '' for linha in dados_ciclo[1:]]  # C
coluna_F = [linha[5].strip() if len(linha) > 5 else '' for linha in dados_ciclo[1:]]  # F

dados_atualizados = retry(aba_destino.get_all_values)
coluna_A_atual = set([linha[0].strip() for linha in dados_atualizados[1:] if len(linha) > 0])

linhas_a_inserir = []
for i, valor in enumerate(coluna_E):
    if valor and valor not in coluna_A_atual:
        nova_linha = [''] * cols_needed   # usa a mesma largura da CARTEIRA (A..)
        nova_linha[0] = valor            # Coluna A ← E da CICLO
        nova_linha[1] = coluna_F[i]      # Coluna B ← F da CICLO
        nova_linha[7] = coluna_C[i]      # Coluna H ← C da CICLO
        linhas_a_inserir.append(nova_linha)

if linhas_a_inserir:
    log(f'➕ Inserindo {len(linhas_a_inserir)} novas linhas vindas da aba CICLO...')
    retry(aba_destino.append_rows, linhas_a_inserir)
    log('✅ Linhas da CICLO inseridas.')
else:
    log("ℹ️  Nenhuma nova linha da aba CICLO a inserir (todas já estavam presentes).")

# === EXTRA: INSERIR LINHAS DA ABA LV CICLO QUE NÃO ESTÃO NA CARTEIRA + UNIDADE EM R ===
log('🔗 (EXTRA) Verificando itens da aba LV CICLO que não estão na CARTEIRA...')
aba_lv = retry(planilha_destino.worksheet, 'LV CICLO')
dados_lv = retry(aba_lv.get_all_values)

# Mapeamento de unidades (chave normalizada sem acento/maiúscula -> valor final)
map_unidade = {
    'CONQUISTA': 'VITORIA DA CONQUISTA',
    'ITAPETINGA': 'ITAPETINGA',
    'JEQUIE': 'JEQUIE',          # JEQUIÉ -> JEQUIE
    'GUANAMBI': 'GUANAMBI',
    'BARREIRAS': 'BARREIRAS',
    'LAPA': 'BOM JESUS DA LAPA',
    'IRECE': 'IRECE',            # IRECÊ -> IRECE
    'IBOTIRAMA': 'IBOTIRAMA',
    'BRUMADO': 'BRUMADO',
    'LIVRAMENTO': 'LIVRAMENTO',
}

# Monta trincas (A=Unidade, B=ID, C=Projeto) da LV CICLO, ignorando cabeçalho
lv_trincas = []
for row in dados_lv[1:]:
    if len(row) > 1:
        unidade_raw = row[0].strip() if len(row) > 0 else ''
        b_val = row[1].strip()
        c_val = row[2].strip() if len(row) > 2 else ''
        lv_trincas.append((unidade_raw, b_val, c_val))

# Recarrega CARTEIRA após inclusão da CICLO
dados_carteira = retry(aba_destino.get_all_values)
existentes_carteira = set(
    (linha[0].strip() if len(linha) > 0 else '')
    for linha in dados_carteira[1:]
)

linhas_lv_a_inserir = []
vistos = set()  # evita duplicados da própria LV
contagem_por_unidade = {}

for unidade_raw, b_val, c_val in lv_trincas:
    if b_val and (b_val not in existentes_carteira) and (b_val not in vistos):
        # Determina a unidade (coluna R) conforme regras
        chave = norm_sem_acentos_up(unidade_raw)
        unidade_final = map_unidade.get(chave, unidade_raw.strip())  # fallback: o que veio da LV

        nova = [''] * cols_needed        # largura igual ao cabeçalho atual (A..R)
        nova[0] = b_val                  # Coluna A ← B da LV CICLO
        nova[1] = c_val                  # Coluna B ← C da LV CICLO
        nova[7] = 'SOMENTE LV'           # Coluna H ← texto fixo
        if cols_needed >= 18:
            nova[17] = unidade_final     # Coluna R ← Unidade mapeada

        linhas_lv_a_inserir.append(nova)
        vistos.add(b_val)
        contagem_por_unidade[unidade_final] = contagem_por_unidade.get(unidade_final, 0) + 1

if linhas_lv_a_inserir:
    log(f'➕ (EXTRA) Inserindo {len(linhas_lv_a_inserir)} novas linhas vindas da aba LV CICLO (com Unidade em R)...')
    retry(aba_destino.append_rows, linhas_lv_a_inserir)
    # Resumo da etapa no CMD
    if contagem_por_unidade:
        resumo = ', '.join([f"{u}: {q}" for u, q in sorted(contagem_por_unidade.items())])
        log(f'📌 (EXTRA) Unidades atribuídas (coluna R): {resumo}')
    log('✅ (EXTRA) Linhas da LV CICLO inseridas com coluna R preenchida.')
else:
    log('ℹ️  (EXTRA) Nenhuma linha adicional da LV CICLO a inserir.')

# === ESTAMPAR DATA E HORA NA CÉLULA T2 ===
try:
    agora_fim = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    retry(aba_destino.update, range_name="T2", values=[[f"Atualizado em: {agora_fim}"]])
    log(f"🕒 Data e hora registradas em T2: {agora_fim}")
except Exception as e:
    log(f"⚠️  Erro ao registrar data e hora em T2: {e}")

log(f'🎉 Finalizado! {len(df)} linhas processadas.')
