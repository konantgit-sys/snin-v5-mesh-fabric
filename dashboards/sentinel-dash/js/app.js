/* app.js — ядро: вкладки, цикл обновления, экспорт, частицы */

/* ── вкладки (hash-навигация) ── */
function switchTab(name, push = true) {
  const tab = document.querySelector(`#tabBar [data-tab="${name}"]`);
  if (!tab) return;
  document.querySelectorAll("#tabBar .tab").forEach(b => b.classList.toggle("on", b === tab));
  document.querySelectorAll(".tabpage").forEach(p => p.classList.toggle("on", p.id === "tab-" + name));
  if (push && location.hash !== "#" + name) history.replaceState(null, "", "#" + name);
}
document.querySelectorAll("#tabBar .tab").forEach(t => {
  t.addEventListener("click", () => switchTab(t.dataset.tab));
});
window.addEventListener("hashchange", () => {
  const h = location.hash.slice(1);
  if (["overview", "chain", "agents", "economy", "journal"].includes(h)) switchTab(h, false);
});
{ const h = location.hash.slice(1); if (["chain", "agents", "economy", "journal"].includes(h)) switchTab(h, false); }

/* ── цикл обновления ── */
let tick = 30;
async function load() {
  try {
    const [r1, r2] = await Promise.all([
      fetch("snapshot.json", { cache: "no-store" }),
      fetch("history.json", { cache: "no-store" }),
    ]);
    if (r1.ok) applySnap(await r1.json());
    if (r2.ok) applyHist(await r2.json());
    $("refreshTxt").textContent = "30с";
  } catch { $("liveText").textContent = "данные недоступны"; }
}
$("btnRefresh").addEventListener("click", () => { $("refreshTxt").textContent = "…"; load(); });
setInterval(() => { tick--; if (tick <= 0) { tick = 30; load(); } $("refreshTxt").textContent = tick + "с"; }, 1000);

/* ── live-цикл: живой поток событий (15с из live.json) ── */
let liveTick = 15;
async function loadLive() {
  try {
    const r = await fetch("live.json", { cache: "no-store" });
    if (r.ok) buildLive(await r.json());
  } catch {}
}
$("btnLivePause")?.addEventListener("click", toggleLivePause);
let livePaused = false;
function toggleLivePause() {
  livePaused = !livePaused;
  $("btnLivePause").classList.toggle("on", livePaused);
  if (!livePaused) { liveTick = 1; }
}
setInterval(() => {
  updateLivePill();
  if (livePaused) return;
  liveTick--; if (liveTick <= 0) { liveTick = 15; loadLive(); }
  // возраст live.json каждую секунду
  const last = window.__liveTs;
  if (last) { const sec = Math.max(0, Math.round(Date.now() / 1000 - last)); $("liveAge").textContent = fmtAgo(sec); }
}, 1000);

/* ── клик по KPI-карточке → вкладка ── */
document.addEventListener("click", e => {
  const card = e.target.closest(".card-link[data-goto]");
  if (!card) return;
  const name = card.dataset.goto;
  const tab = document.querySelector(`#tabBar [data-tab="${name}"]`);
  if (tab) switchTab(name);
});

/* ── экспорт history.json ── */
$("btnExport").addEventListener("click", async () => {
  try {
    const r = await fetch("history.json", { cache: "no-store" });
    const txt = await r.text();
    const blob = new Blob([txt], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "snin_history_" + new Date().toISOString().slice(0, 10) + ".json";
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 4000);
  } catch {}
});

/* ── частицы ── */
function particles() {
  if (matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const cv = $("particles"), cx = cv.getContext("2d");
  let W, H, pts = [];
  const resize = () => { W = cv.width = innerWidth; H = cv.height = innerHeight;
    pts = Array.from({ length: Math.min(46, innerWidth / 34) }, () => ({
      x: Math.random() * W, y: Math.random() * H,
      vx: (Math.random() - .5) * .22, vy: (Math.random() - .5) * .22, r: Math.random() * 1.4 + .5 }));
  };
  resize(); addEventListener("resize", resize);
  const tick = () => {
    cx.clearRect(0, 0, W, H);
    for (const p of pts) {
      p.x += p.vx; p.y += p.vy;
      if (p.x < 0 || p.x > W) p.vx *= -1; if (p.y < 0 || p.y > H) p.vy *= -1;
      cx.beginPath(); cx.arc(p.x, p.y, p.r, 0, 7);
      cx.fillStyle = "rgba(94,234,212,.32)"; cx.fill();
    }
    for (let i = 0; i < pts.length; i++) for (let j = i + 1; j < pts.length; j++) {
      const dx = pts[i].x - pts[j].x, dy = pts[i].y - pts[j].y, d = dx * dx + dy * dy;
      if (d < 14400) { cx.beginPath(); cx.moveTo(pts[i].x, pts[i].y); cx.lineTo(pts[j].x, pts[j].y);
        cx.strokeStyle = `rgba(94,234,212,${(1 - d / 14400) * .14})`; cx.lineWidth = 1; cx.stroke(); }
    }
    requestAnimationFrame(tick);
  };
  tick();
}

/* ── reveal + init ── */
const obs = new IntersectionObserver(es => { es.forEach(e => { if (e.isIntersecting) { e.target.classList.add("visible"); obs.unobserve(e.target); } }); }, { threshold: 0.06 });
document.querySelectorAll(".reveal").forEach(el => obs.observe(el));

particles();
load();
loadLive();  // живой журнал сразу, не ждать первый 15с-тик
