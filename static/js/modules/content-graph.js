// ═══════════════════════════════════════════
// V15 — Content Graph / Knowledge Mapper
// Topic clusters, trends, author-topic affinity
// ═══════════════════════════════════════════

let contentState = {
  nodes: [], edges: [],
  offsetX: 0, offsetY: 0,
  scale: 1.0,
  dragging: false, dragNode: null,
  panning: false, panStartX: 0, panStartY: 0,
  hoverNode: null,
  selectedNode: null,
  viewMode: 'graph', // graph | trends | authors
  metrics: null,
  trends: [],
  authors: [],
  nodePositions: {}
};

function initContentTab() {
  const canvas = document.getElementById('contentCanvas');
  if (!canvas) return;

  const container = canvas.parentElement;
  canvas.width = container.clientWidth || 800;
  canvas.height = (container.clientHeight || 500) - 60;

  contentState.offsetX = canvas.width / 2;
  contentState.offsetY = canvas.height / 2;
  contentState.scale = 1.0;

  canvas.onmousedown = contentMouseDown;
  canvas.onmousemove = contentMouseMove;
  canvas.onmouseup = contentMouseUp;
  canvas.onmouseleave = contentMouseUp;
  canvas.onwheel = contentWheel;
  canvas.ondblclick = contentFitGraph;
  canvas.ontouchstart = contentTouchStart;
  canvas.ontouchmove = contentTouchMove;
  canvas.ontouchend = contentMouseUp;

  loadContentGraph();
}

async function loadContentGraph() {
  const statusEl = document.getElementById('contentStatus');
  if (statusEl) statusEl.textContent = 'Loading topics...';

  try {
    const [topicsRes, trendsRes] = await Promise.all([
      fetch('/api/content/topics?limit=60&min_count=3'),
      fetch('/api/content/trends?limit=20')
    ]);

    const topics = await topicsRes.json();
    const trends = await trendsRes.json();

    contentState.nodes = topics.nodes || [];
    contentState.edges = topics.edges || [];
    contentState.metrics = topics.metrics || {};
    contentState.trends = trends.trending || [];

    // Initialize positions in a circle
    contentState.nodePositions = {};
    const cx = 600, cy = 400, r = Math.min(cx, cy) * 0.7;
    contentState.nodes.forEach((n, i) => {
      const angle = (2 * Math.PI * i) / Math.max(contentState.nodes.length, 1);
      contentState.nodePositions[n.id] = {
        x: cx + r * Math.cos(angle) + (Math.random() - 0.5) * 40,
        y: cy + r * Math.sin(angle) + (Math.random() - 0.5) * 40
      };
    });

    if (statusEl) {
      statusEl.textContent = `${contentState.metrics.node_count} topics · ${contentState.metrics.edge_count} links · ${contentState.metrics.components} clusters`;
    }

    contentFitGraph();
    startContentSimulation();
    renderContentGraph();
  } catch (e) {
    if (statusEl) statusEl.textContent = 'Error loading topics';
    console.error('Content graph error:', e);
  }
}

function startContentSimulation() {
  let iter = 0;
  const maxIter = 250;

  function step() {
    if (iter >= maxIter) { renderContentGraph(); return; }

    const nodes = contentState.nodes;
    const edges = contentState.edges;
    const pos = contentState.nodePositions;
    const kRepel = 4000, kAttr = 0.008, damping = 0.82, minDist = 25;

    const forces = {};
    nodes.forEach(n => { forces[n.id] = { fx: 0, fy: 0 }; });

    // Repulsion
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const a = nodes[i].id, b = nodes[j].id;
        const dx = (pos[a]?.x || 0) - (pos[b]?.x || 0);
        const dy = (pos[a]?.y || 0) - (pos[b]?.y || 0);
        const dist = Math.max(Math.abs(dx) + Math.abs(dy), minDist);
        const f = kRepel / (dist * dist);
        forces[a].fx += f * dx / dist;
        forces[a].fy += f * dy / dist;
        forces[b].fx -= f * dx / dist;
        forces[b].fy -= f * dy / dist;
      }
    }

    // Attraction (weighted by co-occurrence count)
    edges.forEach(e => {
      const a = e.source, b = e.target;
      const dx = (pos[b]?.x || 0) - (pos[a]?.x || 0);
      const dy = (pos[b]?.y || 0) - (pos[a]?.y || 0);
      const dist = Math.max(Math.abs(dx) + Math.abs(dy), minDist);
      const w = (e.weight || 1);
      forces[a].fx += kAttr * dx * w;
      forces[a].fy += kAttr * dy * w;
      forces[b].fx -= kAttr * dx * w;
      forces[b].fy -= kAttr * dy * w;
    });

    // Center gravity
    nodes.forEach(n => {
      forces[n.id].fx += (600 - (pos[n.id]?.x || 0)) * 0.003;
      forces[n.id].fy += (400 - (pos[n.id]?.y || 0)) * 0.003;
    });

    const maxF = 40;
    nodes.forEach(n => {
      if (!pos[n.id]) pos[n.id] = { x: 600, y: 400 };
      pos[n.id].x += Math.max(-maxF, Math.min(maxF, forces[n.id].fx)) * damping;
      pos[n.id].y += Math.max(-maxF, Math.min(maxF, forces[n.id].fy)) * damping;
    });

    iter++;
    if (iter % 5 === 0) renderContentGraph();
    requestAnimationFrame(step);
  }

  step();
}

function renderContentGraph() {
  const canvas = document.getElementById('contentCanvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0a0a14';
  ctx.fillRect(0, 0, w, h);

  function tx(x) { return (x + contentState.offsetX) * contentState.scale; }
  function ty(y) { return (y + contentState.offsetY) * contentState.scale; }

  const pos = contentState.nodePositions;
  const maxCount = Math.max(1, ...contentState.nodes.map(n => n.count || 0));

  // Edges
  contentState.edges.forEach(e => {
    const sx = pos[e.source]?.x, sy = pos[e.source]?.y;
    const tx2 = pos[e.target]?.x, ty2 = pos[e.target]?.y;
    if (sx == null || tx2 == null) return;

    const alpha = Math.min(0.6, 0.1 + (e.weight || 1) * 0.05);
    ctx.strokeStyle = `rgba(100,200,180,${alpha})`;
    ctx.lineWidth = Math.min(3, 0.5 + (e.weight || 1) * 0.3);
    ctx.beginPath();
    ctx.moveTo(tx(sx), ty(sy));
    ctx.lineTo(tx(tx2), ty(ty2));
    ctx.stroke();
  });

  // Trending set for highlight
  const trending = new Set((contentState.trends || []).slice(0, 10).map(t => t.tag));

  // Nodes
  contentState.nodes.forEach(n => {
    const px = pos[n.id]?.x, py = pos[n.id]?.y;
    if (px == null) return;
    const x = tx(px), y = ty(py);
    const isHover = contentState.hoverNode === n.id;
    const isSelected = contentState.selectedNode === n.id;
    const isTrending = trending.has(n.id);

    // Size by count
    const baseR = 6 + (n.count / maxCount) * 22;
    const r = baseR * contentState.scale;

    // Color: trending = warm, normal = cool
    const hue = isTrending ? 30 : 170 + (n.degree || 0) * 5;
    const grad = ctx.createRadialGradient(x, y, r * 0.1, x, y, r);
    grad.addColorStop(0, `hsla(${hue}, 80%, 65%, ${isHover ? 1 : 0.9})`);
    grad.addColorStop(1, `hsla(${hue}, 60%, 25%, ${isHover ? 0.8 : 0.5})`);

    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.fillStyle = grad;
    ctx.fill();

    if (isHover || isSelected) {
      ctx.beginPath();
      ctx.arc(x, y, r + 3, 0, Math.PI * 2);
      ctx.strokeStyle = isSelected ? 'rgba(240,165,0,0.9)' : 'rgba(255,255,255,0.5)';
      ctx.lineWidth = 2;
      ctx.stroke();
    }

    // Trending indicator
    if (isTrending && contentState.scale > 0.4) {
      ctx.beginPath();
      ctx.arc(x, y - r - 4, 3, 0, Math.PI * 2);
      ctx.fillStyle = '#f0a500';
      ctx.fill();
    }

    // Label
    if (contentState.scale > 0.45 || isHover) {
      const fs = Math.max(8, Math.min(13, 10 * contentState.scale));
      ctx.font = `${fs}px 'Inter', sans-serif`;
      ctx.fillStyle = 'rgba(255,255,255,0.85)';
      ctx.textAlign = 'center';
      ctx.fillText('#' + n.id.substring(0, 16), x, y + r + fs + 4);
    }
  });

  // Legend
  ctx.font = '11px Inter, sans-serif';
  ctx.textAlign = 'left';
  ctx.fillStyle = '#f0a500';
  ctx.beginPath(); ctx.arc(16, h - 20, 3, 0, Math.PI * 2); ctx.fill();
  ctx.fillStyle = 'rgba(255,255,255,0.6)';
  ctx.fillText('Trending topic', 26, h - 16);
}

// ─── Interactions ───
function contentMouseDown(e) {
  const canvas = document.getElementById('contentCanvas');
  const rect = canvas.getBoundingClientRect();
  const mx = (e.clientX - rect.left) / contentState.scale - contentState.offsetX;
  const my = (e.clientY - rect.top) / contentState.scale - contentState.offsetY;

  let hit = null;
  for (const n of contentState.nodes) {
    const px = contentState.nodePositions[n.id]?.x;
    const py = contentState.nodePositions[n.id]?.y;
    if (px == null) continue;
    if (Math.hypot(mx - px, my - py) < 30) { hit = n.id; break; }
  }

  if (hit) {
    contentState.dragNode = hit;
    contentState.selectedNode = hit;
    contentState.dragging = true;
    loadTopicDetail(hit);
  } else {
    contentState.selectedNode = null;
    hideContentDetail();
    contentState.panning = true;
    contentState.panStartX = e.clientX;
    contentState.panStartY = e.clientY;
  }
  renderContentGraph();
}

function contentMouseMove(e) {
  const canvas = document.getElementById('contentCanvas');
  const rect = canvas.getBoundingClientRect();
  const mx = (e.clientX - rect.left) / contentState.scale - contentState.offsetX;
  const my = (e.clientY - rect.top) / contentState.scale - contentState.offsetY;

  if (contentState.dragging && contentState.dragNode) {
    const pos = contentState.nodePositions;
    pos[contentState.dragNode].x += mx - (pos[contentState.dragNode].x || 0) * 0.1;
    renderContentGraph();
    return;
  }
  if (contentState.panning) {
    contentState.offsetX += (e.clientX - contentState.panStartX) / contentState.scale;
    contentState.offsetY += (e.clientY - contentState.panStartY) / contentState.scale;
    contentState.panStartX = e.clientX;
    contentState.panStartY = e.clientY;
    renderContentGraph();
    return;
  }

  let hit = null;
  for (const n of contentState.nodes) {
    const px = contentState.nodePositions[n.id]?.x;
    const py = contentState.nodePositions[n.id]?.y;
    if (px == null) continue;
    if (Math.hypot(mx - px, my - py) < 30) { hit = n.id; break; }
  }
  if (hit !== contentState.hoverNode) {
    contentState.hoverNode = hit;
    canvas.style.cursor = hit ? 'pointer' : 'grab';
    renderContentGraph();
  }
}

function contentMouseUp() {
  contentState.dragging = false;
  contentState.dragNode = null;
  contentState.panning = false;
}

function contentWheel(e) {
  e.preventDefault();
  const zoom = e.deltaY < 0 ? 1.1 : 0.9;
  const newScale = Math.max(0.2, Math.min(3, contentState.scale * zoom));
  const canvas = document.getElementById('contentCanvas');
  const rect = canvas.getBoundingClientRect();
  contentState.offsetX = (e.clientX - rect.left) / newScale - ((e.clientX - rect.left) / contentState.scale - contentState.offsetX);
  contentState.offsetY = (e.clientY - rect.top) / newScale - ((e.clientY - rect.top) / contentState.scale - contentState.offsetY);
  contentState.scale = newScale;
  renderContentGraph();
}

function contentFitGraph() {
  const canvas = document.getElementById('contentCanvas');
  if (!canvas || contentState.nodes.length === 0) return;

  const pos = contentState.nodePositions;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  contentState.nodes.forEach(n => {
    const p = pos[n.id];
    if (!p) return;
    if (p.x < minX) minX = p.x;
    if (p.y < minY) minY = p.y;
    if (p.x > maxX) maxX = p.x;
    if (p.y > maxY) maxY = p.y;
  });
  if (!isFinite(minX)) return;

  const gW = maxX - minX + 120, gH = maxY - minY + 120;
  contentState.scale = Math.min((canvas.width - 40) / gW, (canvas.height - 60) / gH, 2.0);
  contentState.offsetX = canvas.width / 2 / contentState.scale - (minX + maxX) / 2;
  contentState.offsetY = canvas.height / 2 / contentState.scale - (minY + maxY) / 2;
}

async function loadTopicDetail(tag) {
  try {
    const resp = await fetch('/api/content/topic/' + tag);
    const data = await resp.json();

    const panel = document.getElementById('contentDetail');
    if (!panel) return;
    panel.style.display = 'block';

    const authors = (data.top_authors || []).map(a => `<span style="color:var(--text-dim)">${a.name}</span> <span style="color:var(--accent)">${a.posts}</span>`).join(' · ') || 'none';
    const related = (data.related_topics || []).slice(0, 8).map(t => `#${t.tag}(${t.count})`).join(', ') || 'none';

    panel.innerHTML =
      '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">' +
      '<strong style="font-size:15px;color:var(--accent)">#' + escapeHtml(tag) + '</strong>' +
      '<button onclick="hideContentDetail()" class="btn-secondary" style="font-size:11px;padding:2px 8px">✕</button>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--text-dim);margin-bottom:6px">' + (data.total_posts || 0) + ' posts</div>' +
      '<div style="font-size:11px;line-height:1.5"><span style="color:var(--text-dim)">Authors: </span>' + authors + '</div>' +
      '<div style="font-size:11px;line-height:1.5;margin-top:3px"><span style="color:var(--text-dim)">Related: </span>' + related + '</div>';
  } catch (e) {}
}

function hideContentDetail() {
  const panel = document.getElementById('contentDetail');
  if (panel) panel.style.display = 'none';
}

function contentTouchStart(e) { if (e.touches.length === 1) contentMouseDown(e.touches[0]); }
function contentTouchMove(e) { if (e.touches.length === 1) contentMouseMove(e.touches[0]); }

window.ContentGraph = {
  init: initContentTab,
  render: renderContentGraph,
  fit: contentFitGraph
};
