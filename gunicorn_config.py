# Gunicorn config for SNIN MCP Gateway — production
# gunicorn -c gunicorn_config.py gateway:application

import multiprocessing
import os

# ─── Worker Pool ───────────────────────────────────────────────
# 4 workers × threads — конкурентность без GIL-блокировок на I/O
workers = 4
worker_class = "gthread"
threads = 4
max_requests = 10000          # авто-рециклинг при утечках
max_requests_jitter = 1000    # случайный разброс, чтобы не все разом
timeout = 30                  # таймаут на запрос
graceful_timeout = 10         # на доработку перед убийством

# ─── Networking ────────────────────────────────────────────────
bind = "0.0.0.0:9951"
backlog = 2048                # очередь соединений (SOMAXCONN)
keepalive = 5                 # keep-alive на клиентских соединениях

# ─── Memory Safety ─────────────────────────────────────────────
# Мягкий лимит: worker пересоздаётся при превышении памяти
max_requests = 10000

# ─── Logging ───────────────────────────────────────────────────
accesslog = os.path.join(os.path.dirname(__file__), "logs", "gunicorn_access.log")
errorlog = os.path.join(os.path.dirname(__file__), "logs", "gunicorn_error.log")
loglevel = "info"
access_log_format = '%(h)s %(l)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)sus'

# ─── Process Naming ────────────────────────────────────────────
proc_name = "snin-mcp-gateway"

# ─── Preload ───────────────────────────────────────────────────
# Не прелоадим — каждый worker получает свой KNOWN_AGENTS
preload_app = False
