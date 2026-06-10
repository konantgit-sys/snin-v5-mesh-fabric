"""
router_api.py — Entry point + Health API + Status printer для Smart Router
Фаза 2: Выделено из smart_router.py для декомпозиции.
"""

import asyncio
import signal
import sys
import time

from smart_router import SmartRouter, _GLOBAL_ROUTER, HEALTH_PORT, BP_MAX_CONCURRENT
from router_policy import aredis
from gossip_stream import GossipStream
from cpu_worker import shutdown_pools


async def print_status(router: SmartRouter):
    """Печать статуса раз в 30 секунд."""
    start = time.time()
    while True:
        await asyncio.sleep(30)
        elapsed = int(time.time() - start)
        r = await aredis()
        ch_summary = {}
        if r:
            for ch in ("direct", "mesh", "gossip", "nostr"):
                keys = await r.keys(f"route:stats:*:{ch}")
                ts, tf, tl = 0, 0, []
                for k in keys:
                    h = await r.hgetall(k)
                    ts += int(h.get("sent", 0))
                    tf += int(h.get("failed", 0))
                    avg = h.get("avg_latency")
                    if avg and avg != "?":
                        tl.append(float(avg))
                al = round(sum(tl) / len(tl), 1) if tl else 0
                ch_summary[ch] = {"sent": ts, "failed": tf, "avg_ms": al}

        print(f"\n[Router] {'='*55}")
        print(f"[Router] Uptime: {elapsed}s | Msgs: recv={router.stats['received']} "
              f"fwd={router.stats['forwarded']} fail={router.stats['failed']}")
        print(f"[Router] Concurrent: {router._concurrent}/{BP_MAX_CONCURRENT} | "
              f"BP: rejected={router.stats['backpressure_rejected']} warn={router.stats['backpressure_warning']}")
        print(f"[Router] CB: reroute={router.stats['cb_reroute']} blocked={router.stats['cb_blocked']} | "
              f"timeouts: mesh={router.stats['timeout:mesh']} gossip={router.stats['timeout:gossip']} "
              f"nostr={router.stats['timeout:nostr']}")
        blocked_channels = router._cb.get_blocked()
        if blocked_channels:
            print(f"[Router] 🔴 Blocked channels: {', '.join(blocked_channels)} (30s)")
        print(f"[Router] Channels: mesh={router.stats['chan_ok:mesh']} "
              f"gossip={router.stats['chan_ok:gossip']} "
              f"nostr={router.stats['chan_ok:nostr']} "
              f"direct={router.stats['chan_ok:direct']} "
              f"faf={router.stats['fire_forget_sent']}")
        print(f"[Router] RateLimiter: allowed={router._rate_limiter.stats['allowed']} "
              f"denied={router._rate_limiter.stats['denied']} "
              f"buckets={len(router._rate_limiter._buckets)}")
        print(f"[Router] Fallbacks: {router.stats['fallback_to_mesh']} | "
              f"congestion_reroute: {router.stats['congestion_reroute']} | "
              f"congestion_slow: {router.stats['congestion_slow']}")
        # Phase 4: Graph stats
        if router.graph:
            gs = router.graph.get_stats()
            print(f"[Router] 📊 Graph: nodes={gs['total_nodes']} edges={gs['total_edges']} "
                  f"online={gs['nodes_online']} avg_w={gs['avg_weight']:.2f} | "
                  f"paths={router.stats.get('graph_paths_found',0)} "
                  f"fallback={router.stats.get('graph_fallbacks',0)} "
                  f"direct={router.stats.get('graph_routed_direct',0)} "
                  f"mesh={router.stats.get('graph_routed_mesh',0)}")
        print(f"[Router] Channel health (last cycle):")
        for ch, h in router._channel_health.items():
            total = h["ok"] + h["fail"]
            if total:
                bar = "█" * max(1, int(h["avg_ms"] / 10)) if h["avg_ms"] else "?"
                print(f"  {ch:8s} ok={h['ok']} fail={h['fail']} avg={h['avg_ms']:.0f}ms {bar}")
            else:
                print(f"  {ch:8s} — no data")
        print(f"[Router] Redis delivery stats:")
        for ch, s in ch_summary.items():
            bar = "█" * max(1, int(s["avg_ms"] / 10)) if s["avg_ms"] else "?"
            print(f"  {ch:8s} → sent={s['sent']} fail={s['failed']} avg={s['avg_ms']}ms {bar}")
        print(f"[Router] {'='*55}\n")


async def health_server():
    """HTTP health endpoint для L2 Transport — на HEALTH_PORT (9933)"""
    server = await asyncio.start_server(
        lambda r, w: None,
        '127.0.0.1', HEALTH_PORT
    )
    async with server:
        print(f"[Router] ✅ Health HTTP endpoint on :{HEALTH_PORT}")
        async for client_reader, client_writer in server.accept():
            try:
                request = await asyncio.wait_for(client_reader.read(1024), timeout=2)
                if request:
                    response = (
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: application/json\r\n"
                        "Connection: close\r\n"
                        "Content-Length: 0\r\n\r\n"
                    ).encode()
                    client_writer.write(response)
                    await client_writer.drain()
            except:
                pass
            finally:
                try:
                    client_writer.close()
                except:
                    pass


async def run_router():
    """Запустить Smart Router: инициализация + event loop + health."""
    sys.stdout.flush()
    print(f"[Router] Initializing SmartRouter...")
    sys.stdout.flush()
    router = SmartRouter()
    print(f"[Router] ✅ SmartRouter created, channels: nostr={len([w for w in router._nostr_writers if w])}/5")
    sys.stdout.flush()

    # Запуск GossipStream V8 (data channel)
    try:
        print(f"[Router] Starting GossipStream V8...")
        sys.stdout.flush()
        gs = GossipStream(pubkey="sr_gossip_v8")
        await gs.start_server_async()
        router._gossip_stream = gs
        print(f"[Router] ✅ GossipStream V8 on :{gs.listen_port}")
        sys.stdout.flush()
    except Exception as e:
        print(f"[Router] ⚠️ GossipStream V8: {e}")
        sys.stdout.flush()

    print(f"[Router] Starting main loops...")
    sys.stdout.flush()
    try:
        await asyncio.gather(
            router.run(),
            print_status(router),
            router._dht_scan_loop(),
            router._reorder_timeout_loop(),
            router._reorder_cleanup_loop(),
            router._dedup_cleanup_loop(),
            router._priority_aging_loop(),
            *[router._priority_worker(i) for i in range(router._pq_workers)]
        )
    except Exception as e:
        import traceback
        print(f"[Router] 💀 Main loop crashed: {e}")
        print(traceback.format_exc())
        sys.stdout.flush()
        raise


def cleanup():
    print(f"[Router] 🧹 Cleaning up process pools...")
    shutdown_pools()
    print(f"[Router] ✅ Pools cleaned")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: (cleanup(), sys.exit(0)))
    print(f"[Router] Smart Router v2 — multi-channel, policy + self-learning")
    try:
        asyncio.run(run_router())
    except KeyboardInterrupt:
        print(f"[Router] Shutdown")
        cleanup()
    except asyncio.CancelledError:
        print(f"[Router] Cancelled")
    except Exception as e:
        import traceback
        print(f"[Router] 💀 FATAL CRASH: {e}")
        print(traceback.format_exc())
        sys.exit(1)
