# diag_workbook.py — inspeciona uso de células (grade) por aba num workbook
# Uso: python diag_workbook.py <SPREADSHEET_ID> [<SPREADSHEET_ID> ...]
import os, sys, json, pathlib
import gspread
from google.oauth2.service_account import Credentials as SACreds

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
LIMITE = 10_000_000

def make_creds():
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        return SACreds.from_service_account_info(json.loads(env_json), scopes=SCOPES)
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.isfile(env_path):
        return SACreds.from_service_account_file(env_path, scopes=SCOPES)
    p = pathlib.Path("credenciais.json")
    if p.is_file():
        return SACreds.from_service_account_file(str(p), scopes=SCOPES)
    raise FileNotFoundError("Credenciais não encontradas.")

def main():
    ids = sys.argv[1:] or ["1NL6fGUhJyde7_ttTkWRVxg78mAOw8Z5W-LBesK_If_M"]
    gc = gspread.authorize(make_creds())
    for sid in ids:
        print(f"\n===== WORKBOOK {sid} =====")
        sh = gc.open_by_key(sid)
        print(f"Título: {sh.title}")
        total = 0
        linhas = []
        for ws in sh.worksheets():
            r, c = ws.row_count, ws.col_count
            cells = r * c
            total += cells
            linhas.append((cells, ws.title, r, c))
        linhas.sort(reverse=True)
        print(f"{'CÉLULAS':>12}  {'LINHAS':>8} x {'COLS':>5}  ABA")
        for cells, title, r, c in linhas:
            print(f"{cells:>12,}  {r:>8,} x {c:>5,}  {title}")
        pct = 100.0 * total / LIMITE
        print(f"----- TOTAL: {total:,} células  ({pct:.1f}% do limite de {LIMITE:,}) -----")
        print(f"----- FOLGA: {LIMITE - total:,} células -----")

if __name__ == "__main__":
    main()
