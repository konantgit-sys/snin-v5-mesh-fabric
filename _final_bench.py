#!/usr/bin/env python3
import asyncio, time, sys, orjson

sys.path.insert(0, '.')
ESC = chr(27)

REAL = orjson.dumps({'kind':39002,'pubkey':'a'*64,'to':'b'*64,'meta':{'channel':'mesh','priority':'normal'},'payload':{'text':'x'*200}})+b'\n'

async def run():
    N=8; DURATION=30
    start=time.monotonic()
    conns=[(await asyncio.wait_for(asyncio.open_connection('127.0.0.1',9932),3))[1] for _ in range(N)]
    totals=[0]*N; errs=0

    async def sustained(i,w):
        nonlocal errs
        while True:
            el=time.monotonic()-start
            if el>=DURATION: break
            batch=REAL*200
            try:
                w.write(batch)
                await w.drain()
                totals[i]+=200
                await asyncio.sleep(0.0015)
            except:
                errs+=1
                break

    tasks=[asyncio.create_task(sustained(i,w)) for i,w in enumerate(conns)]
    await asyncio.gather(*tasks)
    el=time.monotonic()-start
    total=sum(totals); cps=total/el

    # BURST 2s
    conns2=[(await asyncio.wait_for(asyncio.open_connection('127.0.0.1',9932),3))[1] for _ in range(N)]
    bstart=time.monotonic()
    btotals=[0]*N
    async def burst(i,w):
        for _ in range(5000):
            w.write(REAL); btotals[i]+=1
        await w.drain()
    btasks=[asyncio.create_task(burst(i,w)) for i,w in enumerate(conns2)]
    await asyncio.gather(*btasks)
    bel=time.monotonic()-bstart
    btotal=sum(btotals); bcps=btotal/bel if bel>0 else 0

    # Вывод
    sep = ESC + "[1m" + ESC + "[96m" + "="*55 + ESC + "[0m"
    bold = ESC + "[1m"
    green = ESC + "[92m"
    yel = ESC + "[93m"
    reset = ESC + "[0m"

    print(sep)
    print(bold + "  SNIN MESH — ФИНАЛЬНЫЕ ТОРГОВЫЕ БЕНЧМАРКИ" + reset)
    print(ESC + "[96m  События: ~400 байт (реальный Nostr kind:39002)" + reset)
    print(ESC + "[96m  Архитектура: Phase 6.4 — Batch Gossip + In-memory cache" + reset)
    print(sep)
    print()
    print(bold + "SUSTAINED 30s:" + reset)
    print("  Всего:      {:,} событий".format(total))
    print("  Throughput: " + bold + green + "{:>8,.0f} msg/s".format(cps) + reset)
    print("  Ошибок:     {}".format(errs))
    print()
    print(bold + "BURST 2s (write без await):" + reset)
    print("  Всего:      {:,} событий".format(btotal))
    print("  Пик:        " + bold + yel + "{:>8,.0f} msg/s".format(bcps) + reset)
    print()
    print(bold + "СРАВНЕНИЕ СО СЦЕНАРИЕМ \"1 msg/s/агент\":" + reset)
    print("  {:>6} | {:>16} | {:>10} | {:>12}".format("Агентов","msg/день/агент","vs 1 msg/s","разрыв"))
    print("  " + "-"*52)
    for agents in [10,100,500,1000,5000,10000]:
        per_agent_day = 86400 * cps / agents
        gap = agents / cps
        ratio = per_agent_day / 86400  # vs 1 msg/s/agent
        if per_agent_day > 1e6:
            s = "{:.1f} млн".format(per_agent_day/1e6)
        elif per_agent_day > 1e3:
            s = "{:,.0f} тыс".format(per_agent_day/1e3)
        else:
            s = "{:,.0f}".format(per_agent_day)
        print("  {:>6} | {:>16} | {:>10.1f}x | {:>10.1f}ms".format(agents, s, ratio, gap*1000))
    print()
    print(bold + "ИТОГО:" + reset)
    print("  Сеть из 1,000 агентов: каждый может отправлять ~{:,.0f} msg/s".format(cps/1000))
    print("  Это в {:.0f} раз(-а) больше, чем 1 сообщение в секунду".format(cps/1000/1))
    print("  Фактический разрыв между сообщениями: {:.1f}ms".format(1000/cps*1000))
    print(sep)

asyncio.run(run())
