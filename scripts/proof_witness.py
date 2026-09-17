#!/usr/bin/env python3
"""
Прогон свидетелей чекпоинта по расписанию (фаза 4).

Два независимых свидетеля подписывают свежий чекпоинт:
  w1 — читает реестр (быстрый путь);
  w2 — читает сертификат с ВНЕШНИХ релеев (независимый путь: видит то, что
       реально опубликовано наружу, а не то, что лежит у нас в базе).

Проверка требует минимум 2 подписи и минимум 1 независимый путь чтения —
поэтому пропуск любого из двух свидетелей виден как отказ проверки.

Пишет одну строку JSON в stdout (её и складывает cron в witness.log).
Код возврата: 0 — чекпоинт подтверждён, 1 — нет.
"""

import json
import sys
import time

sys.path.insert(0, "/home/agent/data/sites/relay-mesh")

from proof_mesh import witness  # noqa: E402

PLAN = (("w1", "local"), ("w2", "relay"))


def main() -> int:
    results = []
    # оба свидетеля — на ОДИН чекпоинт: берём сертификат не младше 90 с,
    # иначе релеи его ещё не разнесли и второй свидетель подпишет другую высоту
    target = witness.newest_settled(90) or witness.checkpoint_local()
    if not target:
        print(json.dumps({"ts": int(time.time()), "error": "чекпоинта нет"}, ensure_ascii=False))
        return 1
    results = [{"target": target["height"], "origin": target.get("origin")}]
    for name, source in PLAN:
        try:
            att = witness.attest(name, source, height=target["height"])
            results.append({"witness": name, "source": att["source"],
                            "height": att["height"], "ok": True})
        except SystemExit as e:
            results.append({"witness": name, "source": source, "ok": False, "error": str(e)})
        except Exception as e:
            results.append({"witness": name, "source": source, "ok": False,
                            "error": f"{type(e).__name__}: {e}"[:200]})
    st = witness.checkpoint_status(target["height"])
    health = witness.witness_health()
    print(json.dumps({
        "ts": int(time.time()),
        "attest": results,
        "checkpoint": {k: st.get(k) for k in
                       ("height", "witnesses", "required", "relay_sourced", "ok", "reason")},
        "health": health,
    }, ensure_ascii=False))
    return 0 if health.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
