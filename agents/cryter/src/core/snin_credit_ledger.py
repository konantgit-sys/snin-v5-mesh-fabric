"""
SNIN Credit Ledger — единый реестр внутренней экономики (#4)

Объединяет разрозненные куски:
  - chrono.db/transfers (27 записей, agent-to-agent)
  - accounting.db/payments (13 тестовых, balances ПУСТЫЕ)
  - post_bonus (72 награды cryter по 10 SNIN)
  - agent_faucet (выдача SNIN)

Создаёт ЕДИНЫЙ баланс для каждого агента.
Агент может:
  1. Заработать (earn) — за посты, верификацию, relay-услуги
  2. Потратить (spend) — запрос услуг у другого агента
  3. Перевести (transfer) — прямой kind:8015 перевод
  4. Запросить из faucet — до 100 SNIN/час

Экономика замкнутая: earn → balance → spend → earn другого агента.
"""

import json
import time
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("snin_credits")

# ─── Paths ───
CHRONO_DB = Path.home() / "data" / "sites" / "chrono" / "chrono.db"
ACCOUNTING_DB = Path.home() / "data" / "sites" / "relay-mesh" / "accounting.db"
LEDGER_DB = Path.home() / "data" / "sites" / "relay-mesh" / "snin_credits.db"

# ─── Earning rates (SNIN per action) ───
EARN_POST = 10            # за один пост в nostr
EARN_COMMENT = 2          # за комментарий
EARN_VERIFICATION = 5     # верификация ZK-доказательства
EARN_RELAY_HOUR = 3       # час работы relay-сервиса
EARN_ATTESTATION = 8      # выдача VC-аттестации
EARN_TASK_COMPLETION = 15 # выполнение задачи другого агента

# ─── Limits ───
FAUCET_MAX_PER_HOUR = 100    # максимум из крана в час
FAUCET_COOLDOWN = 3600       # 1 час между запросами
MIN_BALANCE_FOR_SPEND = 0    # нельзя уйти в минус

logger = logging.getLogger("snin_credits")


class CreditLedger:
    """Единый реестр балансов агентов SNIN."""

    def __init__(self, db_path=LEDGER_DB, skip_migration=False):
        self.db_path = db_path
        self._init_db()
        if not skip_migration:
            self._migrate_from_chrono()

    def _init_db(self):
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS balances (
                    agent_id TEXT PRIMARY KEY,
                    balance REAL NOT NULL DEFAULT 0,
                    earned_total REAL DEFAULT 0,
                    spent_total REAL DEFAULT 0,
                    last_faucet_at REAL,
                    faucet_claimed_total REAL DEFAULT 0,
                    last_updated REAL
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tx_id TEXT UNIQUE,
                    from_agent TEXT,
                    to_agent TEXT,
                    amount REAL NOT NULL,
                    tx_type TEXT NOT NULL,
                    reason TEXT,
                    kind INTEGER DEFAULT 8015,
                    event_id TEXT,
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_tx_from ON transactions(from_agent);
                CREATE INDEX IF NOT EXISTS idx_tx_to ON transactions(to_agent);
                CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(tx_type);
                CREATE INDEX IF NOT EXISTS idx_tx_created ON transactions(created_at);
            """)
            conn.commit()

    def _migrate_from_chrono(self):
        """One-time: перенести данные из chrono.db и accounting.db в единый реестр."""
        migrated = False

        # 1. Перенос transfers из chrono.db
        try:
            chrono_conn = sqlite3.connect(f"file:{CHRONO_DB}?mode=ro", uri=True)
            rows = chrono_conn.execute(
                "SELECT from_agent, to_agent, amount, ts, reason FROM transfers"
            ).fetchall()
            chrono_conn.close()

            for row in rows:
                from_agent, to_agent, amount, ts, reason = row
                tx_id = f"migrate:chrono:{ts}:{from_agent}:{to_agent}"
                self._record_tx(tx_id, from_agent, to_agent, amount,
                              "transfer", reason or "", ts, skip_balance=False)
            if rows:
                logger.info(f"Migrated {len(rows)} transfers from chrono.db")
                migrated = True
        except Exception as e:
            logger.debug(f"Chrono migration skipped: {e}")

        # 2. Перенос post_bonus
        try:
            chrono_conn = sqlite3.connect(f"file:{CHRONO_DB}?mode=ro", uri=True)
            rows = chrono_conn.execute(
                "SELECT event_id, agent_name, amount, created_at FROM post_bonus"
            ).fetchall()
            chrono_conn.close()

            for row in rows:
                event_id, agent_name, amount, ts = row
                tx_id = f"migrate:post_bonus:{event_id[:16]}"
                self._record_tx(tx_id, "system", agent_name, amount,
                              "earn_post", f"Post bonus: {event_id[:24]}...",
                              ts, skip_balance=False)
            if rows:
                logger.info(f"Migrated {len(rows)} post_bonus from chrono.db")
                migrated = True
        except Exception as e:
            logger.debug(f"Post bonus migration skipped: {e}")

        # 3. Перенос payments из accounting.db
        try:
            acc_conn = sqlite3.connect(f"file:{ACCOUNTING_DB}?mode=ro", uri=True)
            rows = acc_conn.execute(
                "SELECT event_id, pubkey, recipient, amount, method, status, created_at "
                "FROM payments WHERE status='verified'"
            ).fetchall()
            acc_conn.close()

            for row in rows:
                event_id, pubkey, recipient, amount, method, status, ts = row
                tx_id = f"migrate:payment:{event_id}"
                self._record_tx(tx_id, pubkey, recipient, amount,
                              "payment_" + method, f"Payment via {method}",
                              ts, skip_balance=False)
            if rows:
                logger.info(f"Migrated {len(rows)} verified payments from accounting.db")
                migrated = True
        except Exception as e:
            logger.debug(f"Accounting migration skipped: {e}")

        if migrated:
            self._recalculate_all_balances()
            logger.info("Balances recalculated after migration")

    # ═══ Core operations ═══

    def _record_tx(self, tx_id, from_agent, to_agent, amount, tx_type,
                   reason="", created_at=None, skip_balance=False):
        """Записать транзакцию. Возвращает True если новая."""
        ts = created_at or time.time()
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.execute(
                    "INSERT INTO transactions(tx_id, from_agent, to_agent, amount, "
                    "tx_type, reason, created_at) VALUES (?,?,?,?,?,?,?)",
                    (tx_id, from_agent, to_agent, amount, tx_type, reason, ts)
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False  # уже существует

    def _ensure_balance_row(self, agent_id):
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO balances(agent_id) VALUES (?)",
                (agent_id,)
            )
            conn.commit()

    def get_balance(self, agent_id: str) -> float:
        """Получить текущий баланс агента."""
        with sqlite3.connect(str(self.db_path)) as conn:
            row = conn.execute(
                "SELECT balance FROM balances WHERE agent_id=?", (agent_id,)
            ).fetchone()
            return row[0] if row else 0.0

    def get_full_stats(self, agent_id: str) -> dict:
        """Полная статистика: баланс, заработано, потрачено, faucet."""
        with sqlite3.connect(str(self.db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM balances WHERE agent_id=?", (agent_id,)
            ).fetchone()
            if not row:
                return {"agent_id": agent_id, "balance": 0.0, "earned_total": 0,
                        "spent_total": 0, "faucet_claimed": 0}
            return {
                "agent_id": row[0],
                "balance": row[1],
                "earned_total": row[2],
                "spent_total": row[3],
                "last_faucet_at": row[4],
                "faucet_claimed_total": row[5],
                "last_updated": row[6],
            }

    # ═══ Earn ═══

    def earn(self, agent_id: str, amount: float, tx_type: str,
             reason: str = "", event_id: str = "") -> bool:
        """Агент зарабатывает SNIN."""
        self._ensure_balance_row(agent_id)

        tx_id = f"earn:{agent_id}:{tx_type}:{int(time.time())}:{event_id[:16] if event_id else 'none'}"

        if not self._record_tx(tx_id, "system", agent_id, amount, tx_type, reason):
            return False

        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "UPDATE balances SET balance=balance+?, earned_total=earned_total+?, "
                "last_updated=? WHERE agent_id=?",
                (amount, amount, time.time(), agent_id)
            )
            conn.commit()

        logger.info(f"💰 {agent_id[:16]}... earned +{amount} SNIN ({tx_type}): {reason}")
        return True

    # ═══ Spend ═══

    def spend(self, from_agent: str, to_agent: str, amount: float,
              reason: str = "", event_id: str = "") -> tuple[bool, str]:
        """
        Агент тратит SNIN на услугу другого агента.
        Returns: (success, error_message)
        """
        if amount <= 0:
            return False, "Amount must be positive"

        balance = self.get_balance(from_agent)
        if balance < amount:
            return False, f"Insufficient balance: {balance} < {amount}"
        if balance - amount < MIN_BALANCE_FOR_SPEND:
            return False, f"Would go below minimum balance ({MIN_BALANCE_FOR_SPEND})"

        tx_id = f"spend:{from_agent}:{to_agent}:{int(time.time())}:{event_id[:16] if event_id else 'none'}"

        if not self._record_tx(tx_id, from_agent, to_agent, amount,
                               "spend", reason):
            return False, "Transaction already exists"

        self._ensure_balance_row(to_agent)

        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "UPDATE balances SET balance=balance-?, spent_total=spent_total+?, "
                "last_updated=? WHERE agent_id=?",
                (amount, amount, time.time(), from_agent)
            )
            conn.execute(
                "UPDATE balances SET balance=balance+?, earned_total=earned_total+?, "
                "last_updated=? WHERE agent_id=?",
                (amount, amount, time.time(), to_agent)
            )
            conn.commit()

        logger.info(f"💸 {from_agent[:16]}... → {to_agent[:16]}... {amount} SNIN: {reason}")
        return True, "ok"

    # ═══ Faucet ═══

    def faucet_claim(self, agent_id: str, amount: float = 50) -> tuple[bool, str]:
        """
        Агент запрашивает SNIN из крана.
        Лимит: 100 SNIN/час.
        """
        self._ensure_balance_row(agent_id)

        stats = self.get_full_stats(agent_id)
        last_claim = stats.get("last_faucet_at")

        # Проверка cooldown
        if last_claim and (time.time() - last_claim) < FAUCET_COOLDOWN:
            remaining = FAUCET_COOLDOWN - (time.time() - last_claim)
            minutes = int(remaining / 60)
            return False, f"Cooldown: wait {minutes} min"

        # Проверка лимита
        if amount > FAUCET_MAX_PER_HOUR:
            amount = FAUCET_MAX_PER_HOUR

        tx_id = f"faucet:{agent_id}:{int(time.time())}"

        if not self._record_tx(tx_id, "faucet", agent_id, amount,
                               "faucet", "Faucet claim"):
            return False, "Already claimed this tick"

        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "UPDATE balances SET balance=balance+?, "
                "faucet_claimed_total=faucet_claimed_total+?, "
                "last_faucet_at=?, last_updated=? WHERE agent_id=?",
                (amount, amount, time.time(), time.time(), agent_id)
            )
            conn.commit()

        logger.info(f"🚰 {agent_id[:16]}... faucet +{amount} SNIN")
        return True, "ok"

    # ═══ Transfer (agent-to-agent kind:8015) ═══

    def transfer(self, from_agent: str, to_agent: str, amount: float,
                 reason: str = "") -> tuple[bool, str]:
        """Прямой перевод между агентами (kind:8015)."""
        return self.spend(from_agent, to_agent, amount,
                         f"Transfer: {reason}")

    # ═══ Reporting ═══

    def _recalculate_all_balances(self):
        """Пересчитать все балансы из истории транзакций."""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("UPDATE balances SET balance=0, earned_total=0, spent_total=0")
            conn.commit()

            # Recalculate from all transactions
            rows = conn.execute(
                "SELECT from_agent, to_agent, amount, tx_type FROM transactions"
            ).fetchall()

            updates: dict[str, dict] = {}
            for from_agent, to_agent, amount, tx_type in rows:
                if to_agent not in updates:
                    updates[to_agent] = {"balance": 0, "earned": 0, "spent": 0}
                if from_agent not in updates:
                    updates[from_agent] = {"balance": 0, "earned": 0, "spent": 0}

                if tx_type.startswith("earn") or tx_type == "faucet":
                    updates[to_agent]["balance"] += amount
                    updates[to_agent]["earned"] += amount
                elif tx_type == "spend" or tx_type == "transfer":
                    updates[from_agent]["balance"] -= amount
                    updates[from_agent]["spent"] += amount
                    updates[to_agent]["balance"] += amount
                    updates[to_agent]["earned"] += amount

            for agent_id, data in updates.items():
                conn.execute(
                    "INSERT OR REPLACE INTO balances(agent_id, balance, earned_total, "
                    "spent_total, last_updated) VALUES (?,?,?,?,?)",
                    (agent_id, data["balance"], data["earned"],
                     data["spent"], time.time())
                )
            conn.commit()

    def get_economy_report(self) -> dict:
        """Полный отчёт по экономике."""
        with sqlite3.connect(str(self.db_path)) as conn:
            total_balance = conn.execute(
                "SELECT COALESCE(SUM(balance),0) FROM balances"
            ).fetchone()[0]
            total_earned = conn.execute(
                "SELECT COALESCE(SUM(earned_total),0) FROM balances"
            ).fetchone()[0]
            total_spent = conn.execute(
                "SELECT COALESCE(SUM(spent_total),0) FROM balances"
            ).fetchone()[0]
            agent_count = conn.execute(
                "SELECT COUNT(*) FROM balances WHERE balance > 0"
            ).fetchone()[0]
            all_agents = conn.execute(
                "SELECT agent_id, balance, earned_total, spent_total FROM balances "
                "ORDER BY balance DESC"
            ).fetchall()

            # Recent transactions
            recent = conn.execute(
                "SELECT * FROM transactions ORDER BY created_at DESC LIMIT 20"
            ).fetchall()

            # Breakdown by type
            type_stats = conn.execute(
                "SELECT tx_type, COUNT(*), COALESCE(SUM(amount),0) "
                "FROM transactions GROUP BY tx_type"
            ).fetchall()

        return {
            "total_supply": round(total_balance, 1),
            "total_earned": round(total_earned, 1),
            "total_spent": round(total_spent, 1),
            "agents_with_balance": agent_count,
            "top_agents": [
                {"agent": r[0][:16], "balance": r[1], "earned": r[2], "spent": r[3]}
                for r in all_agents[:10]
            ],
            "recent_transactions": [
                {"tx_id": r[1][:32], "from": r[2][:16] if r[2] else "system",
                 "to": r[3][:16], "amount": r[4], "type": r[5], "reason": r[6][:60]}
                for r in recent[:10]
            ],
            "by_type": [
                {"type": r[0], "count": r[1], "volume": round(r[2], 1)}
                for r in type_stats
            ],
            "rates": {
                "post": EARN_POST,
                "comment": EARN_COMMENT,
                "verification": EARN_VERIFICATION,
                "relay_hour": EARN_RELAY_HOUR,
                "attestation": EARN_ATTESTATION,
                "task": EARN_TASK_COMPLETION,
                "faucet_max_per_hour": FAUCET_MAX_PER_HOUR,
            }
        }

    def get_transaction_history(self, agent_id: str, limit=20) -> list:
        """История транзакций конкретного агента."""
        with sqlite3.connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT * FROM transactions WHERE from_agent=? OR to_agent=? "
                "ORDER BY created_at DESC LIMIT ?",
                (agent_id, agent_id, limit)
            ).fetchall()
            return [
                {
                    "tx_id": r[1][:32],
                    "from": r[2][:16] if r[2] else "system",
                    "to": r[3][:16] if r[3] else "system",
                    "amount": r[4],
                    "type": r[5],
                    "reason": r[6][:80] if r[6] else "",
                    "time": datetime.fromtimestamp(r[8]).isoformat() if r[8] else "",
                }
                for r in rows
            ]
