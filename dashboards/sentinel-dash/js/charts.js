/* charts.js — утилиты + SVG-движок (независимый слой) */
const ease = "cubic-bezier(0.23,1,0.32,1)";
const $ = id => document.getElementById(id);
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const fmtN = n => (n ?? 0).toLocaleString("ru-RU");
const fmtSats = msat => { const s = (msat ?? 0) / 1000;
  return s >= 1e6 ? (s/1e6).toFixed(2) + "M sat" : s >= 1e3 ? (s/1e3).toFixed(1) + "k sat" : Math.round(s) + " sat"; };
const fmtTs = ts => new Date(ts * 1000).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
const fmtDT = ts => new Date(ts * 1000).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" });
const fmtClock = ts => new Date(ts * 1000).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" });
const fmtDay = ts => new Date(ts * 1000).toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" });
const fmtDur = h => { if (h >= 24) { const d = Math.floor(h / 24), hh = Math.round(h % 24); return d + "д " + hh + "ч"; } return h.toFixed(1) + " ч"; };
const ageMin = ts => ts ? Math.max(0, Math.round(Date.now()/1000 - ts) / 60) : null;
const fmtAgo = sec => sec < 5 ? "только что" : sec < 60 ? sec + "с назад" : Math.round(sec / 60) + "м назад";
const NS = "http://www.w3.org/2000/svg";
const svgEl = (tag, attrs) => { const el = document.createElementNS(NS, tag); for (const k in attrs) el.setAttribute(k, attrs[k]); return el; };

/* ── тултип (singleton) ── */
const tt = document.createElement("div");
tt.className = "tooltip"; document.body.appendChild(tt);
const ttShow = (x, y, html) => { tt.innerHTML = html; tt.classList.add("show");
  tt.style.left = Math.min(x + 14, innerWidth - tt.offsetWidth - 10) + "px";
  tt.style.top = (y + 14) + "px"; };
const ttHide = () => tt.classList.remove("show");

/* ── count-up для KPI ── */
function countUp(el, target, dur = 900) {
  if (!el || el.dataset.v === String(target)) return;
  el.dataset.v = String(target);
  const t0 = performance.now(), from = parseFloat((el.textContent || "0").replace(/\s/g, "") || 0);
  const step = now => { const p = Math.min(1, (now - t0) / dur), e = 1 - Math.pow(1 - p, 3);
    el.textContent = fmtN(Math.round(from + (target - from) * e)); if (p < 1) requestAnimationFrame(step); };
  requestAnimationFrame(step);
  // фолбэк: RAF заморожен в фоновой вкладке — форсируем финальное значение
  setTimeout(() => {
    if (el.dataset.v === String(target) && el.textContent.replace(/\s/g, "") !== fmtN(target).replace(/\s/g, ""))
      el.textContent = fmtN(target);
  }, dur + 200);
}

/* ── sparkline (линия + площадь) с hover ── */
function sparkline(points, W, H, opts = {}) {
  const svg = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, class: "chart", preserveAspectRatio: "none" });
  const defs = svgEl("defs", {}); svg.appendChild(defs);
  const grad = svgEl("linearGradient", { id: "gradArea", x1: "0", y1: "0", x2: "0", y2: "1" });
  grad.appendChild(svgEl("stop", { offset: "0%", "stop-color": opts.color || "#5eead4", "stop-opacity": "0.28" }));
  grad.appendChild(svgEl("stop", { offset: "100%", "stop-color": opts.color || "#5eead4", "stop-opacity": "0" }));
  defs.appendChild(grad);
  for (let g = 1; g < 4; g++) svg.appendChild(svgEl("line", { x1: "0", y1: (H / 4) * g, x2: W, y2: (H / 4) * g, class: "chart-grid-line" }));
  const xs = points.map(p => p.x), ys = points.map(p => p.y);
  const x0 = Math.min(...xs), x1 = Math.max(...xs), y0 = Math.min(...ys), y1 = Math.max(...ys);
  const pad = 8;
  const px = x => x0 === x1 ? W / 2 : pad + (x - x0) / (x1 - x0) * (W - pad * 2);
  const py = y => y0 === y1 ? H - pad : H - pad - (y - y0) / (y1 - y0) * (H - pad * 2);
  const linePts = points.map((p, i) => `${i ? "L" : "M"}${px(p.x).toFixed(1)},${py(p.y).toFixed(1)}`).join(" ");
  svg.appendChild(svgEl("path", { d: linePts + ` L${px(x1).toFixed(1)},${H} L${px(x0).toFixed(1)},${H} Z`, class: "chart-area" }));
  svg.appendChild(svgEl("path", { d: linePts, class: "chart-line" }));
  points.forEach((p, i) => { if (i === 0 || i === points.length - 1)
    svg.appendChild(svgEl("circle", { cx: px(p.x), cy: py(p.y), r: i === points.length - 1 ? 4 : 2.5,
      class: i === points.length - 1 ? "chart-dot--live" : "chart-dot" })); });
  let hLine = null, hDot = null;
  svg.addEventListener("mousemove", ev => {
    const r = svg.getBoundingClientRect();
    const vx = ((ev.clientX - r.left) / r.width) * W;
    let best = 0, bd = Infinity;
    for (let i = 0; i < points.length; i++) { const d = Math.abs(px(points[i].x) - vx); if (d < bd) { bd = d; best = i; } }
    const p = points[best];
    if (!hLine) { hLine = svgEl("line", { class: "hover-line" }); hDot = svgEl("circle", { r: 5, class: "hover-dot" }); svg.appendChild(hLine); svg.appendChild(hDot); }
    hLine.setAttribute("x1", px(p.x)); hLine.setAttribute("x2", px(p.x)); hLine.setAttribute("y1", 0); hLine.setAttribute("y2", H);
    hDot.setAttribute("cx", px(p.x)); hDot.setAttribute("cy", py(p.y));
    const extra = opts.extra ? "<div class='tt-r'>" + esc(opts.extra(p)) + "</div>" : "";
    ttShow(ev.clientX, ev.clientY - 40, `<div class="tt-h">${opts.title || "высота"} #${fmtN(p.y)}</div><div class="tt-r">${fmtDT(p.x)}</div>${extra}`);
  });
  svg.addEventListener("mouseleave", () => { if (hLine) { hLine.remove(); hDot.remove(); hLine = hDot = null; } ttHide(); });
  return svg;
}

/* ── miniSpark: тонкий тренд без осей для KPI-карточек ── */
let _miniId = 0;
function miniSpark(vals, opts = {}) {
  const W = 220, H = 26, pad = 3;
  const svg = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, "aria-hidden": "true" });
  if (vals.length < 2) return svg;
  const max = Math.max(...vals), min = Math.min(...vals);
  const py = v => max === min ? H / 2 : pad + (1 - (v - min) / (max - min)) * (H - pad * 2);
  const step = (W - pad * 2) / (vals.length - 1);
  const d = vals.map((v, i) => `${i ? "L" : "M"}${(pad + i * step).toFixed(1)},${py(v).toFixed(1)}`).join(" ");
  const gid = "gMini" + (++_miniId);
  const defs = svgEl("defs", {}); svg.appendChild(defs);
  const grad = svgEl("linearGradient", { id: gid, x1: "0", y1: "0", x2: "0", y2: "1" });
  const col = opts.color || "#5eead4";
  grad.appendChild(svgEl("stop", { offset: "0%", "stop-color": col, "stop-opacity": "0.34" }));
  grad.appendChild(svgEl("stop", { offset: "100%", "stop-color": col, "stop-opacity": "0" }));
  defs.appendChild(grad);
  svg.appendChild(svgEl("path", { d: d + ` L${(pad + (vals.length - 1) * step).toFixed(1)},${H} L${pad},${H} Z`, fill: "url(#" + gid + ")" }));
  svg.appendChild(svgEl("path", { d, fill: "none", stroke: col, "stroke-width": 1.8, "stroke-linecap": "round", "stroke-linejoin": "round" }));
  const last = vals[vals.length - 1];
  svg.appendChild(svgEl("circle", { cx: pad + (vals.length - 1) * step, cy: py(last), r: 2.4, fill: col }));
  return svg;
}

/* ── barChart (бары) с hover; mode: 'sky'|'zap' ── */
function barChart(points, W, H, opts = {}) {
  const max = Math.max(1, ...points.map(p => p.n));
  const n = points.length, bw = W / n;
  const zap = opts.mode === "zap";
  const svg = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, class: "chart", preserveAspectRatio: "none" });
  const defs = svgEl("defs", {}); svg.appendChild(defs);
  const gid = zap ? "gradZap" : "gradBar";
  const grad = svgEl("linearGradient", { id: gid, x1: "0", y1: "0", x2: "0", y2: "1" });
  if (zap) {
    grad.appendChild(svgEl("stop", { offset: "0%", "stop-color": "#fbbf24", "stop-opacity": "0.95" }));
    grad.appendChild(svgEl("stop", { offset: "100%", "stop-color": "#d97706", "stop-opacity": "0.4" }));
  } else {
    grad.appendChild(svgEl("stop", { offset: "0%", "stop-color": "#38bdf8", "stop-opacity": "0.95" }));
    grad.appendChild(svgEl("stop", { offset: "100%", "stop-color": "#0ea5e9", "stop-opacity": "0.35" }));
  }
  defs.appendChild(grad);
  const nowSlot = opts.axis === "h" ? Math.floor(Date.now() / 3600000) * 3600 : null;
  points.forEach((p, i) => {
    const h = Math.max(1.2, (p.n / max) * (H - 26));
    const cls = (zap ? "chart-bar--zap " : "") + ((nowSlot && p.t === nowSlot) ? "chart-bar--now" : "");
    svg.appendChild(svgEl("rect", { x: i * bw + bw * 0.18, y: H - 10 - h, width: Math.max(1.6, bw * 0.64), height: h, rx: 1.5, class: "chart-bar " + cls }));
    if (opts.axis === "h") { if (i % 12 === 0 || i === n - 1) { const tx = svgEl("text", { x: i * bw, y: H - 2, class: "chart-axis" }); tx.textContent = fmtClock(p.t); svg.appendChild(tx); } }
    else { if (i % 5 === 0 || i === n - 1) { const tx = svgEl("text", { x: i * bw + 2, y: H - 2, class: "chart-axis" }); tx.textContent = fmtDay(p.t); svg.appendChild(tx); } }
  });
  svg.addEventListener("mousemove", ev => {
    const r = svg.getBoundingClientRect();
    const idx = Math.min(n - 1, Math.max(0, Math.floor(((ev.clientX - r.left) / r.width) * n)));
    const p = points[idx];
    const rx = (idx * bw) / W * r.width + r.left;
    svg.querySelectorAll(".chart-bar").forEach((b, i) => b.style.filter = i === idx ? "brightness(1.5)" : "");
    const label = opts.axis === "h" ? fmtDT(p.t) : fmtDay(p.t);
    const line = opts.sub ? `<div class="tt-r">${opts.sub(p)}</div>` : "";
    ttShow(rx, ev.clientY - 44, `<div class="tt-h">${label}</div><div class="tt-r">${fmtN(p.n)} ${opts.unit || "событий"}</div>${line}`);
  });
  svg.addEventListener("mouseleave", () => { svg.querySelectorAll(".chart-bar").forEach(b => b.style.filter = ""); ttHide(); });
  return svg;
}
