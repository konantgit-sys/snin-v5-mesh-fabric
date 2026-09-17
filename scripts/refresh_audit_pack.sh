#!/bin/bash
# Пересборка аудит-пака для sentinel-dash.v2.site/audit/
# Пакет — снимок: пересобирается по расписанию и по требованию.
#
# Почему через временную папку: пак раздаётся наружу, и его в этот же момент
# может проверять человек в браузере. Если писать файлы поверх рабочих, читатель
# поймает половину старого и половину нового пакета — и получит ложное «НЕ СОВПАЛО».
# Поэтому собираем рядом, проверяем там же и подменяем папку целиком (rename атомарен).
set -e
export LC_ALL=C

OUT=/home/agent/data/sites/sentinel-dash/audit/pack
TMP="${OUT}.new.$$"
OLD="${OUT}.old"
LOG=/home/agent/data/backups/monitor/audit_pack.log
mkdir -p "$(dirname "$LOG")"

cd /home/agent/data/sites/relay-mesh

cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

rm -rf "$TMP"
python3 -m proof_mesh.audit_pack build --out "$TMP" --events 400
python3 -m proof_mesh.audit_pack verify "$TMP"
# приёмка фазы 4: пакет без подписей свидетелей обязан НЕ пройти проверку
python3 -m proof_mesh.audit_pack verify-no-witness "$TMP" >/dev/null

rm -rf "$OLD"
if [ -d "$OUT" ]; then mv "$OUT" "$OLD"; fi
mv "$TMP" "$OUT"
rm -rf "$OLD"

echo "готово: $(date -u +%FT%TZ) пак заменён целиком"
