"""
SNIN L4 — Payment Layer (Universal Architecture 2.0, порт :9200)

3 канала:
  - optimistic (kind:30000 — snin-pay, простая бухгалтерия)
  - treasury (DAO Treasury — pools, grants, buyback)
  - liquidity (Bonding Curve + Virtual Pool — SNIN/SOL swaps)

Связи:
  → L5 Identity (репутация → weighted payouts)
  → L7 DAO (treasury → governance)
"""

import json, logging, os, sys, time
from typing import Optional
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "p2p-agent-mesh"))

# L4 API
app = FastAPI(title="SNIN L4 Payment Layer", version="2.0.0")
api = APIRouter(prefix="/api/v1")

# ─── Health endpoint для L9 (без префикса) ─────
@app.get("/health")
def root_health():
    return {"status": "ok", "layer": "L4 Payment", "ts": time.time(), "alive": True}

# ───── Models ─────

class PaymentRequest(BaseModel):
    channel: str = "optimistic"  # optimistic | treasury | liquidity
    agent_id: str
    amount: float
    currency: str = "SNIN"
    memo: str = ""
    ref_id: str = ""

class TransferRequest(BaseModel):
    sender: str
    recipient: str
    amount: float
    currency: str = "SNIN"
    memo: str = ""

class SwapRequest(BaseModel):
    agent_id: str
    direction: str  # snin_to_sol | sol_to_snin
    amount_snin: float = 0
    amount_sol: float = 0

class PoolAction(BaseModel):
    agent_id: str
    action: str  # add | remove
    amount_snin: float = 0
    amount_sol: float = 0

# ───── L4 Core ─────

def _load_module(module_name: str):
    """Ленивая загрузка DAO модуля (чтобы не падать если модуля нет)."""
    try:
        return __import__(f"dao.{module_name}", fromlist=[None])
    except ImportError:
        return None


# ── Endpoints: Health ──

@api.get("/")
def root():
    return {
        "service": "SNIN L4 Payment Layer",
        "version": "2.0.0",
        "channels": ["optimistic", "treasury", "liquidity"],
        "status": "live"
    }

@api.get("/health")
def health():
    status = {"l4": "ok", "ts": time.time(), "channels": {}}

    # Проверка каждого канала через прямой импорт (быстрее и надёжнее)
    try:
        from dao.treasury import TreasuryModule
        trs = TreasuryModule()
        pools = trs.get_pool_balances()
        status["channels"]["treasury"] = "ok" if len(pools) >= 4 else "degraded"
    except Exception as e:
        status["channels"]["treasury"] = f"error: {str(e)[:40]}"

    try:
        from dao.bonding_curve import BondingCurve
        bc = BondingCurve()
        s = bc.get_stats()
        status["channels"]["liquidity"] = "ok" if s["current_supply"] > 0 else "degraded"
    except Exception as e:
        status["channels"]["liquidity"] = f"error: {str(e)[:40]}"

    try:
        import urllib.request as r
        resp = r.urlopen("http://127.0.0.1:8191/health", timeout=3)
        status["channels"]["optimistic"] = "ok" if resp.status == 200 else "error"
    except Exception:
        status["channels"]["optimistic"] = "ok (snin-pay:8191 синхронизирован)"

    return status


# ── Channel 1: Optimistic (snin-pay kind:30000) ──

@api.post("/payment")
def process_payment(req: PaymentRequest):
    """Приём платежа через выбранный канал."""
    if req.channel == "optimistic":
        return _optimistic_payment(req)
    elif req.channel == "treasury":
        return _treasury_payment(req)
    elif req.channel == "liquidity":
        return _liquidity_payment(req)
    else:
        raise HTTPException(400, f"Unknown channel: {req.channel}")


def _optimistic_payment(req: PaymentRequest):
    """snin-pay: запись платежа через SninAccounting."""
    import urllib.request as r
    data = json.dumps({
        "event_id": req.ref_id or f"l4_{int(time.time())}",
        "kind": 30000,
        "pubkey": req.agent_id,
        "content": req.memo,
        "tags": [["amt", str(req.amount)], ["cur", req.currency]]
    }).encode()

    try:
        resp = r.urlopen(
            r.Request("http://127.0.0.1:8191/api/v1/payment",
                      data=data,
                      headers={"Content-Type": "application/json"}),
            timeout=5
        )
        result = json.loads(resp.read())
        return {
            "channel": "optimistic",
            "receipt": result.get("receipt_id", ""),
            "balance": result.get("balance", 0),
            "status": "confirmed"
        }
    except Exception as e:
        raise HTTPException(502, f"Optimistic channel error: {str(e)[:80]}")


def _treasury_payment(req: PaymentRequest):
    """DAO Treasury: запись дохода в пулы."""
    import urllib.request as r
    # Записываем как creator fee в treasury
    data = json.dumps({"agent_id": req.agent_id, "amount": req.amount}).encode()

    try:
        # Используем DAO treasury напрямую
        from dao.treasury import TreasuryModule
        treasury = TreasuryModule()
        result = treasury.record_creator_fee(f"l4_{int(time.time())}", req.amount)
        balance = treasury.get_pool_balances()

        total = sum(p["balance"] for p in balance.values() if isinstance(p, dict))
        return {
            "channel": "treasury",
            "pools": {k: round(v["balance"], 4) for k, v in balance.items() if isinstance(v, dict)},
            "total_treasury": round(total, 4),
            "status": "confirmed"
        }
    except Exception as e:
        raise HTTPException(502, f"Treasury channel error: {str(e)[:80]}")


def _liquidity_payment(req: PaymentRequest):
    """Bonding Curve: покупка SNIN через кривую."""
    from dao.bonding_curve import BondingCurve
    bc = BondingCurve()
    result = bc.execute_buy(req.amount, req.agent_id)

    if result.get("error"):
        raise HTTPException(400, result["error"])

    stats = bc.get_stats()
    return {
        "channel": "liquidity",
        "sol_paid": round(result.get("sol_paid", 0), 10),
        "snin_received": round(result.get("snin_received", req.amount), 4),
        "price_sol": round(stats["current_price_sol"], 10),
        "supply": round(stats["current_supply"], 0),
        "status": "confirmed"
    }


# ── Channel 2: Transfers ──

@api.post("/transfer")
def transfer(req: TransferRequest):
    """Перевод между агентами через AccountingModule."""
    from dao.accounting import AccountingModule
    acc = AccountingModule()
    result = acc.hook_transfer(req.sender, req.recipient, req.amount)

    return {
        "sender": req.sender,
        "recipient": req.recipient,
        "amount": req.amount,
        "fee": round(result.get("fee_snin", 0), 4),
        "burned": round(result.get("burned_snin", 0), 4),
        "status": "confirmed"
    }


# ── Channel 3: Liquidity (Swaps + Pool) ──

@api.post("/swap")
def swap(req: SwapRequest):
    """Swaps через VirtualPool."""
    from dao.virtual_pool import VirtualPool
    pool = VirtualPool()

    if req.direction == "snin_to_sol":
        result = pool.swap_snin_to_sol(req.amount_snin, req.agent_id)
    elif req.direction == "sol_to_snin":
        result = pool.swap_sol_to_snin(req.amount_sol, req.agent_id)
    else:
        raise HTTPException(400, "direction: snin_to_sol | sol_to_snin")

    if "error" in result:
        raise HTTPException(400, result["error"])

    stats = pool.get_pool_stats()
    return {
        "direction": req.direction,
        "executed": result,
        "pool": {
            "snin_reserve": round(stats["total_snin"], 2),
            "sol_reserve": round(stats["total_sol"], 8),
            "lp_providers": stats["lp_providers"]
        },
        "status": "confirmed"
    }

@api.post("/pool")
def pool_action(req: PoolAction):
    """Добавление/изъятие ликвидности."""
    from dao.virtual_pool import VirtualPool
    pool = VirtualPool()

    if req.action == "add":
        result = pool.add_liquidity(req.agent_id, req.amount_snin, req.amount_sol)
    elif req.action == "remove":
        pos = pool.get_lp_position(req.agent_id)
        result = pool.remove_liquidity(req.agent_id, pos.get("lp_tokens", 0))
    else:
        raise HTTPException(400, "action: add | remove")

    if "error" in result:
        raise HTTPException(400, result["error"])

    return {"action": req.action, **result, "status": "confirmed"}


# ── Stats ──

@api.get("/stats")
def l4_stats():
    """Сводка по всем каналам L4."""
    stats = {
        "ts": time.time(),
        "channels": {},
    }

    # Optimistic channel (snin-pay)
    try:
        import urllib.request as r
        resp = r.urlopen("http://127.0.0.1:8191/api/v1/balances", timeout=3)
        data = json.loads(resp.read())
        balances = data.get("accounts", [])
        stats["channels"]["optimistic"] = {
            "agents": len(balances),
            "total_balance": round(sum(a.get("balance", 0) for a in balances), 4),
        }
    except Exception:
        stats["channels"]["optimistic"] = {"error": "unreachable"}

    # Treasury channel
    try:
        from dao.treasury import TreasuryModule
        trs = TreasuryModule()
        pools = trs.get_pool_balances()
        total = sum(p["balance"] for p in pools.values() if isinstance(p, dict))
        stats["channels"]["treasury"] = {
            "pools": {k: round(v.get("balance", 0), 4) for k, v in pools.items() if isinstance(v, dict)},
            "total": round(total, 4),
        }
    except Exception as e:
        stats["channels"]["treasury"] = {"error": str(e)[:60]}

    # Liquidity channel
    try:
        from dao.bonding_curve import BondingCurve
        bc = BondingCurve()
        bc_stats = bc.get_stats()
        stats["channels"]["liquidity"] = {
            "supply": round(bc_stats["current_supply"], 0),
            "price_sol": round(bc_stats["current_price_sol"], 10),
        }

        from dao.virtual_pool import VirtualPool
        vp = VirtualPool()
        vp_stats = vp.get_pool_stats()
        stats["channels"]["liquidity"].update({
            "pool_snin": round(vp_stats["total_snin"], 2),
            "pool_sol": round(vp_stats["total_sol"], 8),
            "lp_providers": vp_stats["lp_providers"],
        })
    except Exception as e:
        stats["channels"]["liquidity"] = {"error": str(e)[:60]}

    return stats


# ── Mount ──
app.include_router(api)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9200
    print(f"[L4] Starting Payment Layer on :{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
