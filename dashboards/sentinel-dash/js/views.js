/* views.js — рендер всех секций дашборда */
let hist = null, snapData = null, curPeriod = "d7", journalFilter = null;

/* ── визуализатор звеньев (клик → модалка) ── */
function buildViz(blocks) {
  const el = $("chainViz");
  if (!blocks || !blocks.length) { el.innerHTML = '<div style="color:var(--muted);font-size:.72rem">пусто</div>'; return; }
  el.innerHTML = "";
  blocks.forEach((b, i) => {
    const live = i === blocks.length - 1;
    const wrap = document.createElement("div");
    wrap.style.opacity = "0"; wrap.style.transform = "translateY(10px)";
    wrap.style.transition = `opacity 400ms ${ease} ${i * 90}ms, transform 400ms ${ease} ${i * 90}ms`;
    wrap.innerHTML = `
      <div class="vblock${live ? " vblock--live" : ""}" data-id="${b.id}">
        <span class="vh">#${fmtN(b.id)}</span>
        <div><div class="va">${esc(b.agent)} · ${esc(b.action)}</div><div class="vt">${fmtClock(b.ts)}</div></div>
      </div>`;
    el.appendChild(wrap);
    wrap.querySelector(".vblock").addEventListener("click", () => openBlock(b));
    requestAnimationFrame(() => { wrap.style.opacity = "1"; wrap.style.transform = "translateY(0)"; });
    if (i < blocks.length - 1) {
      const link = document.createElement("div");
      link.className = "vlink";
      link.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 5v14M5 12l7 7 7-7"/></svg><span class="mono">${esc(b.hash.slice(0, 14))}… → ${esc(blocks[i + 1].hash.slice(0, 14))}…</span>`;
      el.appendChild(link);
    }
  });
}

/* ── модалка деталей блока ── */
function openBlock(b) {
  $("modalTitle").textContent = "блок #" + fmtN(b.id);
  $("modalBody").innerHTML = `
    <div class="kv"><div class="kvk">время</div><div class="kvv">${fmtDT(b.ts)} (${fmtTs(b.ts)})</div></div>
    <div class="kv"><div class="kvk">агент / экземпляр</div><div class="kvv">${esc(b.agent)}${b.inst ? " · " + esc(b.inst) : ""}</div></div>
    <div class="kv"><div class="kvk">действие</div><div class="kvv">${esc(b.action)}${b.evc ? " · evidence " + esc(b.evc) : ""}</div></div>
    <div class="kv"><div class="kvk">block_hash</div><div class="kvv">${esc(b.hash)}</div></div>
    <div class="kv"><div class="kvk">prev_hash</div><div class="kvv">${esc(b.prev || "—")}</div></div>
    <div class="kv"><div class="kvk">payload_hash</div><div class="kvv">${esc(b.ph || "—")}</div></div>
    <div class="kv"><div class="kvk">подпись</div><div class="kvv">${esc(b.sig || "—")}</div></div>`;
  $("modal").classList.add("open");
}
document.addEventListener("click", e => {
  if (e.target.closest("#modalClose") || e.target.id === "modal") $("modal").classList.remove("open");
  if (e.key === "Escape") $("modal").classList.remove("open");
});
document.addEventListener("keydown", e => { if (e.key === "Escape") $("modal").classList.remove("open"); });

/* ── рост с переключателем периодов ── */
function buildGrowth(certs) {
  const series = { d1: certs.d1 || [], d7: certs.d7 || [], d30: certs.d30 || [] };
  const pts = (series[curPeriod] || []).map(c => ({ x: c.ts, y: c.h, r: c.r }));
  if (pts.length < 2) { $("growthChart").innerHTML = '<div style="color:var(--muted);padding:10px">история недоступна</div>'; return; }
  const first = pts[0].y, last = pts[pts.length - 1].y;
  const spanH = (pts[pts.length - 1].x - pts[0].x) / 3600;
  $("growHint").textContent = `${fmtN(first)} → ${fmtN(last)} · +${fmtN(last - first)}`;
  $("growRange").innerHTML = `<span><i style="background:var(--accent2)"></i>${fmtTs(pts[0].x)} — ${fmtTs(pts[pts.length - 1].x)} (${spanH >= 48 ? Math.round(spanH / 24) + " дн" : spanH.toFixed(0) + " ч"})</span>`;
  $("growthChart").innerHTML = "";
  $("growthChart").appendChild(sparkline(pts, 820, 230, { title: "высота", extra: p => "root " + p.r + "…" }));
}
document.addEventListener("click", e => {
  const btn = e.target.closest("#periodSeg button"); if (!btn) return;
  document.querySelectorAll("#periodSeg button").forEach(b => b.classList.remove("on"));
  btn.classList.add("on"); curPeriod = btn.dataset.p;
  if (hist) buildGrowth(hist.chain.certs);
});

function buildRate(hourly) {
  if (!hourly || hourly.length < 4) { $("rateChart").innerHTML = '<div style="color:var(--muted);padding:10px">нет данных</div>'; return; }
  $("rateHint").textContent = `${fmtN(hourly.reduce((s, p) => s + p.n, 0))} событий за 48ч`;
  $("rateChart").innerHTML = "";
  $("rateChart").appendChild(barChart(hourly, 820, 160, { axis: "h", unit: "соб/час" }));
}

function buildDaily(daily) {
  if (!daily || daily.length < 2) { $("dailyChart").innerHTML = '<div style="color:var(--muted);padding:10px">нет данных</div>'; return; }
  const total = daily.reduce((s, p) => s + p.n, 0);
  $("dailyHint").textContent = `${fmtN(total)} событий · пик ${fmtN(Math.max(...daily.map(d => d.n)))}/день`;
  $("dailyChart").innerHTML = "";
  $("dailyChart").appendChild(barChart(daily, 820, 160, { axis: "d" }));
}

function buildZaps(zaps, lastTs) {
  const el = $("zapsChart");
  if (!zaps || !zaps.length) { el.innerHTML = '<div style="color:var(--muted);padding:10px">нет данных</div>'; return; }
  const pts = zaps.map(z => ({ t: z.t, n: z.n, sats: z.sats }));
  let hint = `${fmtN(pts.reduce((s, p) => s + p.n, 0))} zap-ов · ${fmtN(pts.reduce((s, p) => s + p.sats, 0))} sat`;
  if (lastTs) {
    const age = Date.now() / 1000 - lastTs;
    if (age < 0) age = 0;
    const alive = age < 3600;
    hint += ` · <span style="color:${alive ? "var(--ok)" : "var(--warn)"}">последний ${fmtAgo(age)}</span>`;
  }
  $("zapsHint").innerHTML = hint;
  el.innerHTML = "";
  el.appendChild(barChart(pts, 820, 160, { axis: "d", mode: "zap", unit: "zap-ов", sub: p => "сумма " + fmtN(p.sats) + " sat" }));
}

/* ── таймлайн непрерывности ── */
function buildTimeline(chain) {
  const W = 30 * 86400;
  const end = chain.last_event_ts || Math.floor(Date.now() / 1000);
  const start = end - W;
  const gaps = (chain.gaps_30d || []).filter(g => g.to >= start && g.from <= end);
  const tl = $("tlTrack");
  tl.querySelectorAll(".tl-seg").forEach(el => el.remove());
  const pct = ts => Math.min(100, Math.max(0, ((ts - start) / W) * 100));
  let cursor = start;
  gaps.forEach(g => {
    if (g.from > cursor) {
      const s = document.createElement("div");
      s.className = "tl-seg ok"; s.style.left = pct(cursor) + "%"; s.style.width = Math.max(0.3, pct(g.from) - pct(cursor)) + "%";
      tl.appendChild(s);
    }
    const gs = document.createElement("div");
    gs.className = "tl-seg gap";
    gs.style.left = pct(g.from) + "%"; gs.style.width = Math.max(0.4, pct(g.to) - pct(g.from)) + "%";
    gs.addEventListener("mouseenter", ev => ttShow(ev.clientX, ev.clientY - 40,
      `<div class="tt-h">разрыв ${fmtDur(g.hours)}</div><div class="tt-r">${fmtDT(g.from)} — ${fmtDT(g.to)}</div>`));
    gs.addEventListener("mouseleave", ttHide);
    tl.appendChild(gs);
    cursor = g.to;
  });
  if (cursor < end) {
    const s = document.createElement("div");
    s.className = "tl-seg ok"; s.style.left = pct(cursor) + "%"; s.style.width = Math.max(0.3, pct(end) - pct(cursor)) + "%";
    tl.appendChild(s);
  }
  const now = $("tlNow"); now.style.display = "block"; now.style.left = "100%";
  $("tlScale").innerHTML = `<span>${fmtDay(start)}</span><span>${fmtDay(start + 10 * 86400)}</span><span>${fmtDay(start + 20 * 86400)}</span><span>${fmtDay(end)}</span>`;
  const totGap = gaps.reduce((s, g) => s + g.hours, 0);
  $("tlHint").textContent = gaps.length ? `${gaps.length} разрыв · ${fmtDur(totGap)} простоя` : "идеально — 0 разрывов";
  $("tlLegend").innerHTML = `<span><i style="background:var(--ok)"></i>запись идёт</span>` +
    (gaps.length ? `<span><i style="background:repeating-linear-gradient(45deg,#f87171,#f87171 4px,#fbbf24 4px,#fbbf24 8px)"></i>простой</span>` : "");
}
/* ── журнал с фильтром по агенту ── */
function journalAgents(j) { return [...new Set(j.map(e => e.agent))].sort(); }
let liveSeen = new Set();
let liveActive = false;
function journalRowHtml(e, isNew) {
  const row = document.createElement("div");
  row.className = "log-row" + (isNew ? " log-row--new" : "");
  row.innerHTML = `<span class="log-t">${fmtClock(e.ts)}</span><span class="log-agent" data-a="${esc(e.agent)}">${esc(e.agent)}</span><span class="log-a">${esc(e.action || "event")}</span><span class="log-h">${esc(e.hash || "")}</span>`;
  return row;
}
function renderJournalRows(src) {
  const el = $("journal");
  if (!src.length) { el.innerHTML = '<div style="color:var(--muted)">пусто</div>'; return; }
  el.innerHTML = "";
  // свежие сверху
  [...src].reverse().forEach(e => el.appendChild(journalRowHtml(e, false)));
}
function buildJournal(j) {
  const src = journalFilter ? j.filter(e => e.agent === journalFilter) : j;
  liveSeen = new Set(j.map(e => e.id));
  renderJournalRows(src);
}
function buildLive(live) {
  if (!live || !live.events || !live.events.length) return;
  const rows = live.events;
  const firstLive = !liveActive;
  liveActive = true;
  // чипы пересобрать, если набор агентов изменился
  const agSet = [...new Set(rows.map(e => e.agent))].sort().join("|");
  if (agSet !== window.__liveAgents) { window.__liveAgents = agSet; buildJournalChips(rows); }
  const src = journalFilter ? rows.filter(e => e.agent === journalFilter) : rows;
  const maxId = live.last_event ? live.last_event.id : Math.max(...src.map(e => e.id), 0);
  // первый live-рендер = инициализация: ничего не подсвечивать
  const prevSeen = liveSeen;
  liveSeen = new Set(rows.map(e => e.id));
  if (liveSeen.size > 400) liveSeen = new Set(rows.map(e => e.id));
  const isNew = firstLive ? () => false : e => !prevSeen.has(e.id);
  const el = $("journal");
  el.innerHTML = "";
  [...src].reverse().forEach(e => el.appendChild(journalRowHtml(e, isNew(e))));
  countUp($("kpiHeight"), live.height ?? 0, 500);
  const sec = Math.max(0, Math.round(Date.now() / 1000 - (live.generated_at || 0)));
  window.__liveTs = live.generated_at || 0;
  window.__lastEvTs = live.last_event ? live.last_event.ts : 0;
  updateLivePill();
  $("liveAge").textContent = fmtAgo(sec);
  countUp($("kpiEvents"), live.last_event ? live.last_event.id : maxId, 500);
  $("liveCnt").textContent = fmtN(live.last_event ? live.last_event.id : maxId) + " · +" + fmtN(rows.filter(isNew).length);
}
function buildJournalChips(j) {
  const el = $("journalChips");
  const agents = journalAgents(j);
  let html = `<span class="chip${journalFilter ? "" : " on"}" data-a="">все</span>`;
  agents.forEach(a => { html += `<span class="chip${journalFilter === a ? " on" : ""}" data-a="${esc(a)}">${esc(a)}</span>`; });
  el.innerHTML = html;
  el.querySelectorAll(".chip").forEach(c => c.addEventListener("click", () => {
    journalFilter = c.dataset.a || null;
    buildJournalChips(j); buildJournal(j);
  }));
}
document.addEventListener("click", e => {
  const ag = e.target.closest(".log-agent"); if (!ag || !hist) return;
  journalFilter = ag.dataset.a || null;
  buildJournalChips(hist.journal); buildJournal(hist.journal);
  $("tabBar").querySelector('[data-tab="journal"]').click();
});

function buildCerts(list) {
  const el = $("certs");
  if (!list || !list.length) { el.innerHTML = '<div style="color:var(--muted)">пусто</div>'; return; }
  el.innerHTML = "";
  [...list].reverse().forEach(c => {
    const row = document.createElement("div");
    row.className = "cert-row";
    row.innerHTML = `<span class="cert-h">#<i>${fmtN(c.h)}</i></span><span class="cert-r">${esc(c.r)}…</span><span class="cert-t">${fmtClock(c.ts)}</span>`;
    el.appendChild(row);
  });
}

/* ── агенты ── */
function buildAgents(agents) {
  const el = $("agentCards");
  if (!agents || !agents.length) { el.innerHTML = '<div style="color:var(--muted)">пусто</div>'; return; }
  const acts = hist?.actions_24h || {};
  const last = agents.filter(a => a.e24 > 0).length;
  $("agentsHint").textContent = `${agents.length} агента · активны за 24ч: ${last}`;
  el.innerHTML = "";
  agents.forEach((a, i) => {
    const card = document.createElement("div");
    card.className = "agent-card";
    card.style.opacity = "0"; card.style.transform = "translateX(14px)";
    card.style.transition = `opacity 420ms ${ease} ${i * 90}ms, transform 420ms ${ease} ${i * 90}ms`;
    const actChips = (acts[a.agent_id] || []).map(x => `<span class="chip">${esc(x.action)} <b>${x.n}</b></span>`).join("");
    card.innerHTML = `
      <div class="agent-top">
        <span class="agent-name">${esc(a.agent_id)}</span>
        <span class="agent-pct">${a.pct}%</span>
        <span class="agent-meta"><b>${fmtN(a.events)}</b> всего · <b>${fmtN(a.e24)}</b> за 24ч</span>
      </div>
      <div class="agent-bar"><i></i></div>
      <div class="agent-acts">${actChips}</div>`;
    el.appendChild(card);
    requestAnimationFrame(() => requestAnimationFrame(() => {
      card.style.opacity = "1"; card.style.transform = "translateX(0)";
      card.querySelector(".agent-bar i").style.width = Math.max(4, a.pct) + "%";
    }));
  });
}

/* ── релеи ── */
function buildRelays(relays) {
  const el = $("relayGrid");
  if (!relays || !relays.length) { el.innerHTML = '<div style="color:var(--muted)">нет данных</div>'; return; }
  const ok = relays.filter(r => r.status === "ok").length;
  const dead = relays.filter(r => r.status === "dead").length;
  $("relaysHint").textContent = `${relays.length} релеев · живых ${ok} · мёртвых ${dead}`;
  el.innerHTML = "";
  relays.forEach(r => {
    const card = document.createElement("div");
    card.className = "relay-card";
    card.title = r.full + (r.tier && r.tier !== "unknown" ? " · tier: " + r.tier : "");
    const lat = r.status === "ok" ? r.lat + "мс" : "—";
    card.innerHTML = `
      <div class="relay-top"><span class="rp ${r.status}"></span><span class="relay-host">${esc(r.u)}</span></div>
      <div class="relay-meta"><span>score <b>${r.score}</b></span><span class="relay-lat">${lat}</span></div>`;
    el.appendChild(card);
  });
}

/* ── экономика ── */
function buildEconomy(econ) {
  const zap = (econ.payments || []).find(p => p.kind === 9735);
  const inv = (econ.payments || []).find(p => p.kind === 9734);
  const rows = [
    ["Кошельки в реестре", fmtN(econ.wallets ?? 0), "", "var(--accent)"],
    ["Zap-ы · kind 9735", zap ? fmtN(zap.count) : "0", zap ? fmtSats(zap.total_msat) : "", "var(--accent2)"],
    ["Счета · kind 9734", inv ? fmtN(inv.count) : "0", inv ? fmtSats(inv.total_msat) : "", "var(--violet)"],
  ];
  $("economy").innerHTML = rows.map(([k, v, s, col]) => `
    <div class="econ-row"><span class="k"><svg viewBox="0 0 24 24" fill="none" stroke="${col}" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v20M17 6H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>${k}</span><span class="v">${v}${s ? "<small>" + s + "</small>" : ""}</span></div>`).join("");
}
function buildPayTop(top) {
  const el = $("payTop");
  if (!top || !top.length) { el.innerHTML = '<div style="color:var(--muted)">пусто</div>'; return; }
  const maxS = Math.max(...top.map(t => t.sats));
  el.innerHTML = "";
  top.forEach(t => {
    const row = document.createElement("div");
    row.className = "pay-row";
    row.style.opacity = "0";
    row.style.transition = `opacity 420ms ${ease}`;
    row.innerHTML = `
      <span class="pay-who">${esc(t.who)}</span><span class="pay-n">×${t.n}</span><span class="pay-s">${fmtSats(t.sats * 1000)}</span>
      <span class="pay-bar-wrap"><i style="--w:${Math.round(100 * t.sats / maxS)}%"></i></span>`;
    el.appendChild(row);
    requestAnimationFrame(() => { row.style.opacity = "1"; row.querySelector("i").style.width = row.style.getPropertyValue("--w"); });
  });
}

function whoName(p) {
  if (!p) return "";
  if (p.startsWith("8ae7965a")) return "Cryter";
  if (p.startsWith("8d468694")) return "Remora";
  if (p.startsWith("39c15ed9")) return "v2bot";
  return p.slice(0, 8) + "…";
}

/* ── запы НАМ (входящие на наши ключи) ── */
function buildOurZaps(oz) {
  const el = $("ourZaps");
  const hint = $("ourZapsHint");
  const total = oz?.total || { count: 0, sats_all: 0, sats_30d: 0 };
  const list = oz?.list || [];
  hint.textContent = total.count
    ? `${total.count} zap · ${fmtSats(total.sats_all * 1000)} всего · ${fmtSats(total.sats_30d * 1000)} за 30д`
    : "входящие · пока 0";
  if (!total.count) {
    el.innerHTML = `<div style="display:flex;align-items:center;gap:10px;color:var(--muted);padding:6px 0">
      <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2 4.5 13.5H11L9.5 22 19 10h-6.5L13 2z"/></svg>
      <span>Пока 0 — приёмник настроен (brashfoster340@walletofsatoshi.com). Первый zap появится здесь.</span></div>`;
    return;
  }
  el.innerHTML = "";
  list.forEach((z, i) => {
    const row = document.createElement("div");
    row.className = "pay-row";
    row.style.opacity = "0";
    row.style.transition = `opacity 380ms ${ease} ${Math.min(i * 60, 400)}ms`;
    row.innerHTML = `
      <span class="pay-who">${esc(z.sender)}<small style="opacity:.55;margin-left:4px">→ ${whoName(z.recv)}</small></span>
      <span class="pay-n">${fmtTs(z.ts)}</span>
      <span class="pay-s" style="color:var(--ok)">+${z.sats} sat</span>
      <span class="pay-bar-wrap"><i style="--w:100%;background:linear-gradient(90deg,var(--ok),transparent)"></i></span>`;
    el.appendChild(row);
    requestAnimationFrame(() => { row.style.opacity = "1"; });
  });
}

/* ── supervisor ── */
function buildSupervisor(sup) {
  const el = $("supervisor");
  if (!sup) { el.innerHTML = '<div style="color:var(--muted)">недоступен</div>'; return; }
  const pct = sup.total ? Math.round(100 * sup.alive / sup.total) : 0;
  el.innerHTML = `
    <div class="sup-row"><span class="k">процессы</span><span class="v"><span style="color:var(--ok)">${sup.alive}</span>/${sup.total} живых</span></div>
    <div class="sup-row"><span class="k">мёртвых</span><span class="v" style="color:${sup.dead ? "var(--danger)" : "var(--ok)"}">${sup.dead}</span></div>
    <div class="sup-row"><span class="k">авторестартов</span><span class="v">${sup.restarts}</span></div>
    <div class="sup-bar"><i style="width:${pct}%"></i></div>`;
}

/* ── сводка цепочки (вкладка Журнал) ── */
function buildSuperChain(chainSnap) {
  const el = $("superChain");
  if (!chainSnap) { el.innerHTML = '<div style="color:var(--muted)">нет данных</div>'; return; }
  const root = chainSnap.last_root || "—";
  el.innerHTML = `
    <div class="kv"><div class="kvk">высота</div><div class="kvv">#${fmtN(chainSnap.height ?? 0)}</div></div>
    <div class="kv"><div class="kvk">событий всего</div><div class="kvv">${fmtN(chainSnap.events_total ?? 0)}</div></div>
    <div class="kv"><div class="kvk">последний корень (kind 30000)</div><div class="kvv">${esc(root.slice(0, 40))}…</div></div>`;
}

function buildGaps(gaps) {
  const el = $("gapList");
  if (!gaps || !gaps.length) { el.innerHTML = '<div style="color:var(--muted);padding:6px 2px">за 30 дней простоев не было — цепочка писалась непрерывно</div>'; return; }
  el.innerHTML = "";
  [...gaps].reverse().forEach(g => {
    const row = document.createElement("div");
    row.className = "cert-row";
    row.style.gridTemplateColumns = "1fr auto";
    row.innerHTML = `<span class="cert-r">${fmtDT(g.from)} — ${fmtDT(g.to)}</span><span class="cert-t" style="color:var(--danger);font-weight:700">${fmtDur(g.hours)}</span>`;
    el.appendChild(row);
  });
}

/* ── пилюля живости: точный возраст события, 3 состояния ── */
function updateLivePill() {
  const pill = $("livePill"); if (!pill) return;
  const txt = $("liveText");
  const ts = window.__lastEvTs || (hist && hist.chain && hist.chain.last_event_ts) || 0;
  if (!ts) { pill.classList.remove("warn", "stop"); txt.textContent = "проверка…"; return; }
  const sec = Math.max(0, Math.round(Date.now() / 1000 - ts));
  pill.classList.remove("warn", "stop");
  if (sec <= 150) txt.textContent = "пишется · " + fmtAgo(sec);
  else if (sec <= 720) { pill.classList.add("warn"); txt.textContent = "тихо · " + fmtAgo(sec); }
  else { pill.classList.add("stop"); txt.textContent = "СТОИТ · " + Math.round(sec / 60) + " мин"; }
}

/* ── мини-тренды в KPI-карточках ── */
function buildMiniSparks(certs, hourly) {
  const elH = $("sparkHeight"), elR = $("sparkRate");
  if (elH && certs) { const d7 = (certs.d7 || []).map(c => c.h);
    if (d7.length > 1) { elH.innerHTML = ""; elH.appendChild(miniSpark(d7)); } }
  if (elR && hourly) { const hv = (hourly || []).map(p => p.n);
    if (hv.length > 4) { elR.innerHTML = ""; elR.appendChild(miniSpark(hv, { color: "#38bdf8" })); } }
}

/* ── снапшот + история (общий рендер) ── */
function applySnap(data) {
  snapData = data;
  if (!liveActive) countUp($("kpiHeight"), data.chain?.height ?? 0);
  if (!liveActive) countUp($("kpiEvents"), data.chain?.events_total ?? 0);
  const cert = data.last_cert;
  if (cert) { $("kpiCert").textContent = "#" + fmtN(cert.height); $("kpiCertTime").textContent = fmtTs(cert.ts); }
  $("lastRoot").textContent = "root " + (data.chain?.last_root || "").slice(0, 26) + "…";
  $("genTime").textContent = fmtTs(data.generated_at);
  buildEconomy(data.economy || {});
  buildSuperChain(data.chain || {});
}
function applyHist(h) {
  hist = h;
  const c = h.chain;
  $("kpiUptime").textContent = fmtDur(c.uptime_hours);
  countUp($("kpiRate"), c.avg_events_per_hour_24h ?? 0);
  // сегодня vs вчера из hourly_48h
  const nowH = Math.floor(Date.now() / 3600000) * 3600;
  const hour = 3600;
  const today = (c.hourly_48h || []).filter(p => p.t >= nowH - hour * 23).reduce((s, p) => s + p.n, 0);
  const yest = (c.hourly_48h || []).filter(p => p.t >= nowH - hour * 47 && p.t < nowH - hour * 23).reduce((s, p) => s + p.n, 0);
  const dlt = yest ? Math.round(100 * (today - yest) / yest) : 0;
  $("kpiToday").textContent = fmtN(today);
  $("kpiYesterday").textContent = fmtN(yest);
  const dEl = $("kpiDelta");
  dEl.textContent = (dlt >= 0 ? "+" : "") + dlt + "%";
  dEl.style.color = dlt >= 0 ? "var(--ok)" : "var(--danger)";
  $("kpiCertInt").textContent = c.avg_cert_interval_min ?? "—";
  $("kpiDays").textContent = Math.round((Date.now() / 1000 - h.first_ts) / 86400) + " дн";
  const certs = h.certs_last || [];
  if (certs.length > 1) $("kpiGrow").textContent = fmtN(certs[certs.length - 1].h - certs[0].h);
  window.__lastEvTs = c.last_event_ts || 0;
  updateLivePill();
  buildMiniSparks(c.certs, c.hourly_48h);
  buildViz(h.blocks);
  buildGrowth(c.certs);
  buildRate(c.hourly_48h);
  buildDaily(c.daily_30d);
  buildZaps(h.zaps_daily || [], h.econ_last_ts || 0);
  buildTimeline(c);
  if (!liveActive) {
    buildJournal(h.journal);
    buildJournalChips(h.journal);
  }
  buildCerts(h.certs_last);
  buildAgents(h.agents);
  buildRelays(h.relays || []);
  buildPayTop(h.pay_top || []);
  buildOurZaps({ list: h.our_zaps || [], total: h.our_zaps_total });
  buildGaps(c.gaps_30d);
  buildSupervisor(h.supervisor);
}
