# replicar_carteira.py — resiliente, sem pular destino; números contáveis e formatações opcionais
from datetime import datetime
import re
import time
import sys
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

try:
    from gspread_formatting import format_cell_range, CellFormat, NumberFormat
except Exception:
    format_cell_range = None
    CellFormat = None
    NumberFormat = None

# === CONFIGURAÇÕES ===
ID_MASTER = '1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM'
ABA = 'Carteira'
DESTINOS = [
    '1zIfub-pAVtZGSjYT1Qa7HzjAof56VExU7U5WwLE382c',
    '1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M',
    '10Y7VKFsn-UKgMqpM63LiUD2N9_XmfSr29CuK3mq84_c',
    '1B-d3mYf7WwiAnkUTV0419f91OzPF8rcpimgtFNfQ3Mw',
]

ESCOPOS = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# === AUTENTICAÇÃO (portável) ===
import os, json, pathlib

def make_creds():
    env = os.environ.get('GOOGLE_CREDENTIALS')
    if env:
        return Credentials.from_service_account_info(json.loads(env), scopes=ESCOPOS)
    return Credentials.from_service_account_file(pathlib.Path('credenciais.json'), scopes=ESCOPOS)

# === OPÇÕES ===
APLICAR_FORMATACAO_NUMERICA = True     # aplicar NumberFormat em colunas numéricas
TENTATIVAS_MAX = 3                     # tentativas para cada script
ATRASO_BASE = 5.0                      # atraso base entre tentativas

# === AJUDA ===
def a1(col, row):
    """Converte (col, row) 1-based para A1."""
    letras = ""
    while col > 0:
        col, rem = divmod(col - 1, 26)
        letras = chr(65 + rem) + letras
    return f"{letras}{row}"

def agora():
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')

def ler_master_A_S():
    """Lê do master (aba Carteira) o intervalo A1:S (cabeçalho + dados)."""
    # === AUTENTICAÇÃO ===
    creds = make_creds()
    gc = gspread.authorize(creds)

    print(f"📖 Abrindo master {ID_MASTER} / aba {ABA} …")
    sh = gc.open_by_key(ID_MASTER)
    ws = sh.worksheet(ABA)

    # lê cabeçalho + dados
    intervalo = "A1:S"
    valores = ws.get(intervalo)
    if not valores:
        return [], []

    cabecalho = valores[0]
    dados = valores[1:]
    print(f"✅ Master lido: {len(dados)} linhas.")
    return cabecalho, dados

def converter_numeros(dados, colunas_numericas):
    """Converte strings para números (float) nas colunas indicadas (1-based)."""
    def conv(v):
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        # remove símbolos comuns
        s = s.replace("R$", "").replace(".", "").replace(" ", "").replace("\u00a0", "")
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return v  # mantém original

    out = []
    for row in dados:
        new = list(row)
        for c in colunas_numericas:
            idx = c - 1
            if 0 <= idx < len(new):
                new[idx] = conv(new[idx])
        out.append(new)
    return out

def aplicar_formatacao(ws, colunas_numericas):
    """Aplica NumberFormat padrão moeda/numero nas colunas passadas (1-based)."""
    if not APLICAR_FORMATACAO_NUMERICA or not format_cell_range or not NumberFormat or not CellFormat:
        return
    try:
        for col in colunas_numericas:
            rng = f"{a1(col, 2)}:{a1(col, ws.row_count)}"
            fmt = CellFormat(numberFormat=NumberFormat(type='NUMBER', pattern='0.00'))
            format_cell_range(ws, rng, fmt)
    except Exception as e:
        print(f"⚠️  Falhou formatacao numerica: {e}")

def limpar_e_escrever_destino(planilha_id, cabecalho, dados):
    """Limpa e escreve A:S no destino, preservando cabeçalho."""
    creds = make_creds()
    gc = gspread.authorize(creds)
    print(f"📦 Abrindo destino {planilha_id} …")
    sh = gc.open_by_key(planilha_id)
    try:
        ws = sh.worksheet(ABA)
    except WorksheetNotFound:
        # cria se não existir
        ws = sh.add_worksheet(title=ABA, rows=2000, cols=60)

    # garante dimensões mínimas
    if ws.row_count < len(dados) + 2 or ws.col_count < 19:
        alvo_linhas = max(ws.row_count, len(dados) + 2)
        alvo_cols = max(ws.col_count, 19)
        print(f"🧩 resize {ws.title} → {alvo_linhas} x {alvo_cols}")
        ws.resize(alvo_linhas, alvo_cols)

    # limpa A2:S
    print("🧽 Limpando dados antigos (A2:S)…")
    ws.batch_clear(['A2:S'])

    # escreve cabeçalho (A1:S1)
    print("📝 Escrevendo cabeçalho…")
    ws.update('A1:S1', [cabecalho], value_input_option='USER_ENTERED')

    # conversão de colunas possivelmente numéricas
    colunas_numericas = [12, 13, 14, 15, 16, 17]  # exemplo, ajuste se necessário
    dados_fmt = converter_numeros(dados, colunas_numericas)

    # escreve dados em blocos
    print(f"🚚 Escrevendo {len(dados_fmt)} linhas em blocos…")
    CHUNK = 1000
    ini = 0
    while ini < len(dados_fmt):
        parte = dados_fmt[ini:ini+CHUNK]
        rng = f"A{2+ini}:{a1(19, 1+ini+len(parte))}"
        ws.update(rng, parte, value_input_option='USER_ENTERED')
        ini += len(parte)

    # formatação numérica (opcional)
    aplicar_formatacao(ws, colunas_numericas)

    # timestamp
    try:
        ws.update('T2', [[f"Replicado em: {agora()}"]], value_input_option='USER_ENTERED')
    except Exception:
        pass

    print(f"✅ Finalizado destino {planilha_id}")

def tentar_destino_ate_dar_certo(planilha_id, cabecalho, dados):
    """Tenta replicar um destino com retries, sem pular em caso de erro transitório."""
    for tentativa in range(1, 6):
        try:
            limpar_e_escrever_destino(planilha_id, cabecalho, dados)
            return
        except APIError as e:
            # erros comuns de cota/transiente
            print(f"⚠️  Destino {planilha_id} – APIError: {e}")
        except Exception as e:
            print(f"⚠️  Destino {planilha_id} – erro: {e}")

        atraso = min(60, ATRASO_BASE * tentativa + 0.5 * tentativa * tentativa)
        print(f"⏳ Tentativa {tentativa}/5 falhou; aguardando {atraso:.1f}s e tentando novamente…")
        time.sleep(atraso)

        if tentativa == 5:
            print(f"⛔️ Não foi possível atualizar {planilha_id} após 5 tentativas. Abortando.")
            sys.exit(1)

# === EXECUÇÃO ===
if __name__ == '__main__':
    cab, dados = ler_master_A_S()
    if not cab:
        print("❌ Nada para replicar na aba Carteira do master.")
        sys.exit(0)
    print(f"📦 Pronto para replicar: {len(dados)} linhas (A:S).")
    for pid in DESTINOS:
        tentar_destino_ate_dar_certo(pid, cab, dados)
    print("🏁 Réplica finalizada para todas as planilhas.")
