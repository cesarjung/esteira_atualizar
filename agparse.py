import argparse
import ast
import os
import sys
import pathlib
from typing import Set, Dict, Tuple

# Python 3.10+ tem sys.stdlib_module_names
STDLIB = set(getattr(sys, "stdlib_module_names", ())) | {
    # fallback/extra comuns
    "abc","argparse","array","asyncio","base64","binascii","bisect","builtins","calendar","cmath",
    "collections","concurrent","contextlib","copy","csv","ctypes","dataclasses","datetime","decimal",
    "enum","errno","faulthandler","fnmatch","fractions","functools","gc","getopt","getpass","gettext",
    "glob","gzip","hashlib","heapq","hmac","html","http","imaplib","importlib","inspect","io","ipaddress",
    "itertools","json","logging","math","mimetypes","multiprocessing","numbers","operator","os","pathlib",
    "pickle","pkgutil","platform","plistlib","pprint","queue","random","re","sched","secrets","select",
    "shlex","signal","site","smtplib","socket","sqlite3","ssl","statistics","string","stringprep","struct",
    "subprocess","sys","tempfile","textwrap","threading","time","timeit","tkinter","tokenize","traceback",
    "types","typing","unicodedata","unittest","urllib","uuid","warnings","weakref","xml","zipfile","zoneinfo",
}

# Pastas a ignorar na varredura
IGNORE_DIRS = {".git", ".hg", ".svn", "__pycache__", ".venv", "venv", "env", ".mypy_cache", ".pytest_cache",
               ".idea", ".vscode", "build", "dist", ".eggs", ".tox"}

# Regras de mapeamento: módulo importado -> pacote PyPI
EXACT_MAP = {
    "gspread": "gspread",
    "gspread_formatting": "gspread-formatting",
    "oauth2client": "oauth2client",
    "httplib2": "httplib2",
    "requests": "requests",
    "pandas": "pandas",
    "numpy": "numpy",
    "openpyxl": "openpyxl",
    "dateutil": "python-dateutil",
    "PIL": "Pillow",
    "yaml": "PyYAML",
    "cv2": "opencv-python",
    "sklearn": "scikit-learn",
    "dotenv": "python-dotenv",
    "bs4": "beautifulsoup4",
    "selenium": "selenium",
    "folium": "folium",
    "streamlit": "streamlit",
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
}

# Regras por prefixo (capturam imports do tipo "from google.oauth2 import ...")
PREFIX_MAP = [
    ("google.oauth2", "google-auth"),
    ("googleapiclient", "google-api-python-client"),
    ("google.auth", "google-auth"),
    ("google.auth.transport", "google-auth"),
    ("google.cloud", "google-cloud-core"),  # ajuste conforme seu uso (storage, bigquery, etc.)
]

def is_local_module(mod: str, root: pathlib.Path) -> bool:
    """
    Considera "local" se existir arquivo/pasta Python no repo:
    - <root>/<mod>.py  OU
    - <root>/<mod>/__init__.py
    """
    if not mod or "." in mod:
        mod = (mod or "").split(".")[0]
    return (root / f"{mod}.py").exists() or (root / mod / "__init__.py").exists()

def discover_imports(root: pathlib.Path) -> Tuple[Set[str], Set[str]]:
    """
    Varre .py e retorna dois conjuntos:
    - bases: nomes-base (antes do primeiro ponto)
    - fulls: nomes completos (ex.: 'google.oauth2', 'googleapiclient.discovery')
    """
    bases, fulls = set(), set()
    for p in root.rglob("*.py"):
        # pular pastas ignoradas
        rel = p.relative_to(root)
        if any(part in IGNORE_DIRS for part in rel.parts):
            continue
        try:
            src = p.read_text(encoding="utf-8")
            tree = ast.parse(src)
        except Exception:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for n in node.names:
                    name = n.name.strip()
                    if name:
                        bases.add(name.split(".")[0])
                        fulls.add(name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    name = node.module.strip()
                    bases.add(name.split(".")[0])
                    fulls.add(name)
    return bases, fulls

def map_to_packages(bases: Set[str], fulls: Set[str], root: pathlib.Path) -> Set[str]:
    pkgs = set()

    # 1) Prefix rules
    for pref, pkg in PREFIX_MAP:
        if any(f.startswith(pref) for f in fulls):
            pkgs.add(pkg)

    # 2) Exact maps (base)
    for b in bases:
        if b in STDLIB:
            continue
        if is_local_module(b, root):
            continue
        if b in EXACT_MAP:
            pkgs.add(EXACT_MAP[b])
            continue
        # Heurística: se parece ser pacote externo, inclui o próprio nome
        # (ex.: "gspread" já pego acima; para outros como "tabulate" etc.)
        if b not in {"google"}:  # evita adicionar "google" genérico
            pkgs.add(b)

    # 3) Casos Google específicos (se importou google sem capturar nada acima)
    if any(f.startswith("google.oauth2") for f in fulls):
        pkgs.add("google-auth")
    if any(f.startswith("googleapiclient") for f in fulls):
        pkgs.add("google-api-python-client")

    return pkgs

def apply_pin(packages: Set[str]) -> Dict[str, str]:
    """
    Tenta pegar versão instalada via importlib.metadata. Se não achar, deixa sem pin.
    """
    out = {}
    try:
        from importlib.metadata import version, PackageNotFoundError
    except Exception:
        # Python <3.8: pode cair aqui, mas no seu caso é 3.11+, então ok
        version = None
        PackageNotFoundError = Exception  # fallback
    for pkg in sorted(packages):
        if version:
            try:
                ver = version(pkg)
                out[pkg] = ver
            except PackageNotFoundError:
                out[pkg] = ""  # não instalado -> sem versão
        else:
            out[pkg] = ""
    return out

def main():
    ap = argparse.ArgumentParser(description="Gera requirements.txt varrendo imports dos .py")
    ap.add_argument("--path", default=".", help="Pasta raiz a varrer (default: .)")
    ap.add_argument("--output", default="requirements.txt", help="Arquivo de saída (default: requirements.txt)")
    ap.add_argument("--pin", action="store_true", help="Fixar versões instaladas (se disponíveis)")
    args = ap.parse_args()

    root = pathlib.Path(args.path).resolve()
    bases, fulls = discover_imports(root)
    packages = map_to_packages(bases, fulls, root)

    # Limpeza: alguns pacotes redundantes que às vezes escapam
    # (remova daqui se você realmente usar)
    redundant = {"pip", "setuptools", "wheel"}
    packages = {p for p in packages if p not in redundant}

    if args.pin:
        pinned = apply_pin(packages)
        lines = [f"{pkg}=={ver}" if ver else pkg for pkg, ver in sorted(pinned.items())]
    else:
        lines = sorted(packages)

    out = "\n".join(lines) + "\n"
    pathlib.Path(args.output).write_text(out, encoding="utf-8")
    print(f"[OK] Gerado {args.output} com {len(lines)} pacotes.")
    print(out)

if __name__ == "__main__":
    main()
