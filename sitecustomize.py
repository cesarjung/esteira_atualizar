"""Hardening de rede aplicado a TODO processo Python do repo.

O Python importa `sitecustomize` automaticamente no startup se ele estiver no
sys.path. O workflow define `PYTHONPATH: .`, entao isto vale tanto para o
orquestrador quanto para cada script disparado via subprocess — sem editar os
25 scripts.

Problema: gspread e google-api-python-client nao passam `timeout` ao requests.
Sem timeout, uma chamada pode ficar pendurada indefinidamente.

Sobre o valor (300s): a planilha origem "Esteira CCM/BA - Geral" (1gDktQhF...,
~5,5 MB) e editada ao vivo no expediente. Enquanto isso a Sheets API responde
em ~190s e devolve 500/503/409/404 mesmo em leitura. Esses 500/503 os wrappers
`except APIError` do repo JA tratam com backoff, e o script conclui.

O timeout PRECISA ficar acima desses ~190s. Com 180s a chamada era abortada
antes da resposta do servidor e virava requests.exceptions.ReadTimeout — que
nao e APIError e portanto escapa de todos os 37 wrappers de retry do repo,
matando o script na hora. Medido: ciclo.py falhou 3x em 363s no run
32396124086 (timeout 180) contra sucesso em 1042s no run 32376629883 (sem
timeout algum). Isto e teto contra pendura infinita, nao acelerador.

PENDENTE: os wrappers so pegam APIError. Enquanto isso nao mudar, qualquer
ReadTimeout/ConnectionError continua sendo fatal sem retry — o valor alto do
timeout so torna isso raro, nao impossivel.

A alavanca real e reduzir o numero de chamadas e rodar fora do expediente.

IMPORTANTE: como este modulo roda no startup de todo interpretador, qualquer
excecao aqui quebra TODOS os scripts. Nada neste arquivo pode levantar — daí o
parse defensivo de env e o try/except em volta de tudo.
"""

import os

_DEFAULT_READ = 300.0
_DEFAULT_CONNECT = 15.0


def _num(env_name, default):
    """float(env) tolerante: vazio, lixo ou ausente caem no default."""
    raw = (os.environ.get(env_name) or "").strip()
    if not raw:
        return default
    try:
        v = float(raw)
    except (TypeError, ValueError):
        print(f"[sitecustomize] {env_name}={raw!r} invalido; usando {default}")
        return default
    return v if v > 0 else default


HTTP_TIMEOUT = _num("HTTP_TIMEOUT", _DEFAULT_READ)
CONNECT_TIMEOUT = _num("HTTP_CONNECT_TIMEOUT", _DEFAULT_CONNECT)

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

    socket.setdefaulttimeout(HTTP_TIMEOUT + 60)
except Exception:
    pass
