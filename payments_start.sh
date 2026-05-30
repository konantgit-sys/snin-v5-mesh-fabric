#!/bin/bash
# V4 — Запуск платежей (cheque_book + verifier)
cd /home/agent/data/sites/relay-mesh

# cheque_book :9916
python3 -u cheque_book.py >> logs/cheque_book.log 2>&1 &

# verifier :9915 (test mode — Solana RPC недоступен)
python3 -u verifier.py --port 9915 >> logs/verifier.log 2>&1 &
