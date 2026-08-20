"""Hardening de rede aplicado a TODO processo Python do repo.

O Python importa `sitecustomize` automaticamente no startup se ele estiver no
sys.path. O workflow define `PYTHONPATH: .`, entao este arquivo vale tanto para
o orquestrador quanto para cada script disparado via subprocess — sem precisar
editar os 25 scripts.

Problema resolvido: gspread e google-api-python-client nao passam `timeout` ao
requests. Sem timeout, uma chamada degradada do Sheets fica pendurada ~190s
antes de devolver 404/500/503. Com MAX_RETRIES=6 isso da ~1140s por script, e
3 tentativas por replica estouram o timeout-minutes do job (evidencia: falhas
com "em 1143.9s" nos runs cancelados em 2h00).
"""

import os

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))   # read timeout, segundos
CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))

try:
    import requests.sessions as _sessions

    _orig_request = _sessions.Session.request

    if not getattr(_orig_request, "_timeout_patched", False):

        def _request(self, method, url, **kwargs):
            # So preenche quando o chamador nao definiu nada; scripts que ja
            # passam timeout explicito (ex.: importador_carteira.py) seguem iguais.
            if kwargs.get("timeout") is None:
                kwargs["timeout"] = (CONNECT_TIMEOUT, HTTP_TIMEOUT)
            return _orig_request(self, method, url, **kwargs)

        _request._timeout_patched = True
        _sessions.Session.request = _request
except Exception as e:  # nunca quebrar o startup do interpretador
    print(f"[sitecustomize] aviso: patch de timeout nao aplicado: {e}")

try:
    import socket

    socket.setdefaulttimeout(HTTP_TIMEOUT + 30)
except Exception:
    pass
