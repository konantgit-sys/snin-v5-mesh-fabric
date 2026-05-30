"""
mesh_message_handler.py — модуль обработки входящих mesh-сообщений для агентов.

Каждый агент использует этот модуль чтобы:
  1. Распарсить входящее mesh-сообщение
  2. Понять: от кого, кому, какой тип, какой sequence
  3. Проверить: адресовано ли ему
  4. Залогировать структурированно
  5. Сформировать осмысленный ответ
"""

import json
import time
import os
from datetime import datetime
from collections import deque

# Типы mesh-сообщений
MESSAGE_TYPES = {
    "greeting":    {"expected": ["greeting"],    "response_type": "confirmation"},
    "confirmation": {"expected": ["confirmation"], "response_type": "data_share"},
    "data_share":  {"expected": ["data_share"],  "response_type": "ack"},
    "ack":         {"expected": ["ack"],          "response_type": "loop_complete"},
    "loop_complete": {"expected": ["loop_complete"], "response_type": None},
    "exchange_v2bot": {"expected": ["exchange_v2bot"], "response_type": None},
}

class MeshMessageHandler:
    """Обработчик mesh-сообщений для агента."""

    def __init__(self, agent_name: str, pubkey: str, log_dir: str = None):
        self.agent_name = agent_name
        self.pubkey = pubkey
        self.log_dir = log_dir or "/home/agent/data/sites/relay-mesh/logs"
        
        # Журнал входящих сообщений (in-memory, последние 50)
        self.inbox = deque(maxlen=50)
        
        # Журнал исходящих
        self.outbox = deque(maxlen=50)
        
        # Счётчики по отправителям
        self.by_sender = {}
        
        # Последний sequence (чтобы не дублировать)
        self.last_seq = 0
        
        self._ensure_log()

    def _ensure_log(self):
        """Создаёт лог-файл если нет."""
        self.log_path = os.path.join(self.log_dir, f"mesh_{self.agent_name}.log")
        # Просто убеждаемся что директория есть

    def _log(self, direction: str, data: dict):
        """Пишет в файловый лог."""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{direction}] {json.dumps(data, ensure_ascii=False)}"
        try:
            with open(self.log_path, "a") as f:
                f.write(line + "\n")
        except:
            pass

    def parse(self, raw_event: dict) -> dict:
        """
        Разобрать входящее событие.
        Возвращает структурированный dict с полями:
          - ok: bool
          - from_name: str | None
          - to_name: str | None
          - msg_type: str | None
          - sequence: int
          - is_for_me: bool
          - text: str | None
          - error: str | None
        """
        result = {
            "ok": False,
            "from_name": None,
            "to_name": None,
            "msg_type": "unknown",
            "sequence": 0,
            "is_for_me": False,
            "text": None,
            "error": None,
        }

        try:
            # content может быть строкой JSON или dict
            content = raw_event.get("content", {})
            if isinstance(content, str):
                content = json.loads(content)

            result["from_name"] = content.get("from", content.get("from_name", "?"))
            result["to_name"] = content.get("to", content.get("to_name", "?"))
            result["text"] = content.get("text", "")
            result["sequence"] = content.get("sequence", 0)
            result["msg_type"] = content.get("type", content.get("subject", "unknown"))
            result["kind"] = raw_event.get("kind", "?")

            # Проверка: мне ли?
            target = result["to_name"]
            if target and (target == self.agent_name or target == "broadcast" or target == "cryter_feed"):
                result["is_for_me"] = True

            result["ok"] = True

            # Сохраняем во входящие
            self.inbox.append(result)
            self.last_seq = max(self.last_seq, result["sequence"])

            # Счётчик по отправителю
            sender = result["from_name"]
            if sender:
                self.by_sender[sender] = self.by_sender.get(sender, 0) + 1

            # Лог
            self._log("IN", {
                "from": result["from_name"],
                "to": result["to_name"],
                "type": result["msg_type"],
                "seq": result["sequence"],
                "for_me": result["is_for_me"],
                "text": result["text"][:60] if result["text"] else "",
            })

        except (json.JSONDecodeError, TypeError, AttributeError) as e:
            result["error"] = f"parse_error: {e}"
            self._log("IN_ERROR", {"error": str(e), "raw": str(raw_event)[:200]})

        return result

    def route(self, parsed: dict) -> dict | None:
        """
        Определить: нужно ли отвечать и что ответить.
        Возвращает ответное сообщение или None.
        """
        if not parsed["ok"] or not parsed["is_for_me"]:
            return None

        msg_type = parsed["msg_type"]
        seq = parsed["sequence"]
        from_name = parsed["from_name"]
        text = parsed["text"] or ""

        # Определяем тип ответа
        type_info = MESSAGE_TYPES.get(msg_type, {"response_type": None})
        response_type = type_info.get("response_type")

        if response_type is None:
            return None  # Не требует ответа

        # Формируем ответ в зависимости от типа
        responses = {
            "confirmation": f"✅ {self.agent_name} получил сообщение от {from_name} (seq={seq}). Связь подтверждена.",
            "data_share": f"📊 {self.agent_name}: данные приняты. Сохраняю в буфер. seq={seq} от {from_name}.",
            "ack": f"🔄 {self.agent_name}: acknowledgement. seq={seq} — loop продолжается.",
            "loop_complete": f"🏁 {self.agent_name}: loop замкнут. seq={seq}.",
        }

        response_text = responses.get(response_type, f"📨 {self.agent_name}: принято от {from_name} (seq={seq})")

        # Не отвечаем если последний ответ уже был на этот seq
        # (защита от циклов)
        for prev in list(self.outbox)[-3:]:
            if prev.get("in_reply_to_seq") == seq and prev.get("to") == from_name:
                return None  # Уже отвечали

        answer = {
            "type": response_type,
            "from": self.agent_name,
            "to": from_name,
            "text": response_text,
            "sequence": seq + 1,
            "in_reply_to_seq": seq,
            "in_reply_to_from": from_name,
            "timestamp": int(time.time()),
            "meta": {
                "channel": "mesh",
                "priority": "high",
                "pipeline": True,
                "via": self.agent_name,
                "handler_version": "1.0"
            }
        }

        self.outbox.append(answer)
        self._log("OUT", {
            "to": from_name,
            "type": response_type,
            "seq": seq + 1,
            "in_reply": seq,
            "text": response_text[:60],
        })

        return answer

    def status(self) -> dict:
        """Текущее состояние обработчика."""
        return {
            "agent": self.agent_name,
            "pubkey": self.pubkey[:16],
            "inbox_count": len(self.inbox),
            "outbox_count": len(self.outbox),
            "by_sender": dict(self.by_sender),
            "last_seq": self.last_seq,
        }

    def recent_inbox(self, n: int = 5) -> list:
        """Последние n входящих."""
        return list(self.inbox)[-n:]

    def recent_outbox(self, n: int = 5) -> list:
        """Последние n исходящих."""
        return list(self.outbox)[-n:]
