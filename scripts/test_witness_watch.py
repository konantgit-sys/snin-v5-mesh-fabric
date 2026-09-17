"""
Тест сигнала сторожа по свидетелям (фаза 4): «witness офлайн → алерт».

Живой контур не трогаем: Telegram перехватывается в список, файлы состояния и
лога подменяются на временные, а здоровье свидетелей подаётся напрямую —
проверяется именно логика алерта (когда шумит, когда молчит, когда повторяет).

Запуск: python3 /home/agent/data/scripts/test_witness_watch.py
"""

import importlib.util
import json
import tempfile
import time

TMP = tempfile.mkdtemp(prefix="witness_watch_")
spec = importlib.util.spec_from_file_location(
    "cw", "/home/agent/data/scripts/sentinel_chain_watch.py")
cw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cw)

cw.STATE_FILE = TMP + "/state.json"
cw.LOG_FILE = TMP + "/log.txt"
SENT: list[str] = []
cw.send_tg = lambda text: (SENT.append(text), True)[1]

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  {'✅' if cond else '❌'} {name}{(' — ' + detail) if detail else ''}")


BAD = {"ok": False, "reason": "свидетелей 1, нужно 2", "height": 20281, "witnesses": 1,
       "required": 2, "signers": ["w1"], "relay_sourced": 0}
GOOD = {"ok": True, "height": 20282, "witnesses": 2, "signers": ["w1", "w2"], "relay_sourced": 1}

print("── свидетель упал ──")
st: dict = {}
cw.witness_report(st, BAD)
check("алерт отправлен один раз", len(SENT) == 1, SENT[0] if SENT else "нет")
check("в алерте названа причина", "не подтверждён свидетелями" in SENT[0] and "нужно 2" in SENT[0])

print("\n── второй прогон без изменений: тишина ──")
cw.witness_report(st, BAD)
check("повторов нет (дедуп)", len(SENT) == 1, f"алертов {len(SENT)}")
check("в логе помечено дедупом", "дедуп" in open(cw.LOG_FILE).read())

print("\n── восстановление ──")
cw.witness_report(st, GOOD)
check("сообщение о восстановлении", len(SENT) == 2 and "снова подтверждён" in SENT[1], SENT[-1])
check("состояние сброшено", st.get("witness_since") is None)

print("\n── проверка недоступна (сеть/таймаут): шума нет, счётчик растёт ──")
cw.witness_report(st, {"ok": None, "error": "TimeoutExpired"})
check("алерта нет", len(SENT) == 2, f"алертов {len(SENT)}")
check("счётчик сбоев посчитан", st.get("witness_fails") == 1, str(st.get("witness_fails")))

print("\n── сбой длится: повторный алерт через час ──")
st2 = {"witness_since": time.time() - 4000, "witness_alert": time.time() - 4000}
cw.witness_report(st2, BAD)
check("повторный алерт ушёл", len(SENT) == 3 and "уже" in SENT[2], SENT[-1])

print("\n── реальный вызов проверки свидетелей ──")
real = cw._witness_health_raw()
check("живая проверка отвечает", isinstance(real, dict) and real.get("ok") is not None,
      json.dumps(real, ensure_ascii=False))
check("чекпоинт сейчас подтверждён", real.get("ok") is True, json.dumps(real, ensure_ascii=False))

print(f"\n═══ ИТОГ: ✅ {len(PASS)} / ❌ {len(FAIL)} ═══")
if FAIL:
    print("провалено: " + ", ".join(FAIL))
raise SystemExit(1 if FAIL else 0)
