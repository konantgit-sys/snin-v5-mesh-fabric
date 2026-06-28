// ═══════════════════════════════════════════════
// SNIN Client v6.0 — Phase 2 (Profiles, Threads, Search, Reactions)
// ═══════════════════════════════════════════════

const API = '/api';
const WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws';

let signerPubkey = null;
let state = {
  tab: 'feed', aiOnly: false,
  stats: null, ws: null, wsConnected: false,
  eventCount: 0, authorCount: 0
};

// ─── SVG Snippets ───
const SVG = {
  events: '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="3" width="20" height="18" rx="3"/><path d="M6 8h4M6 12h6M6 16h8"/></svg>',
  authors: '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="8" r="4"/><path d="M4 20c0-4 3.6-7 8-7s8 3 8 7"/></svg>',
  online: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="3" fill="currentColor"/><circle cx="12" cy="12" r="8" opacity="0.4"/></svg>',
  robot: '<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><rect x="3" y="5" width="18" height="14" rx="3"/><circle cx="8" cy="10" r="2" fill="white"/><circle cx="16" cy="10" r="2" fill="white"/><path d="M9 17h6M12 5V2M6 2h12" stroke="white" stroke-width="2" stroke-linecap="round"/></svg>',
  searchIcon: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4" stroke-linecap="round"/></svg>',
  heart: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 21l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.18L12 21z"/></svg>',
  replyIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M21 11.5a8.38 8.38 0 01-.9 3.8 8.5 8.5 0 01-7.6 4.7 8.38 8.38 0 01-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 01-.9-3.8 8.5 8.5 0 014.7-7.6 8.38 8.38 0 013.8-.9h.5a8.48 8.48 0 018 8v.5z"/></svg>',
  threadIcon: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/></svg>',
  zapIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" opacity="0.5"><path d="M13 2L3 14h7l-2 8 10-12h-7l2-8z"/></svg>',
  empty: '<svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1"><rect x="3" y="3" width="18" height="18" rx="3"/><path d="M9 9h6M9 13h4"/></svg>',
  warning: '<svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2"><circle cx="12" cy="12" r="10"/><path d="M12 8v4M12 16h.01"/></svg>',
  globe: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10M12 2a15.3 15.3 0 00-4 10 15.3 15.3 0 004 10"/></svg>',
  send: '<svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M2 21L23 12 2 3v7l15 2-15 2v7z"/></svg>',
  lock: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>',
};

// ─── Init ───
document.addEventListener('DOMContentLoaded', () => {
  injectAuroraOrb();
  injectGrid();
  setupNav();
  setupToggle();
  setupComposer();
  addRippleToButtons();
  initCanvasBG();
  loadStats();
  loadFeed();
  checkSigner();
  connectWS();
  setInterval(loadStats, 30000);
});

// ─── Aurora enhancements ───
function injectAuroraOrb() {
  const orb = document.createElement('div');
  orb.className = 'aurora-orb-3-injected';
  document.body.appendChild(orb);
}

function injectGrid() {
  const grid = document.createElement('div');
  grid.className = 'grid-overlay';
  document.body.appendChild(grid);
}

// ─── Canvas Particle Network ───
function initCanvasBG() {
  const canvas = document.getElementById('bgCanvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  let particles = [], w, h;

  function resize() {
    w = canvas.width = window.innerWidth;
    h = canvas.height = window.innerHeight;
  }
  resize();
  window.addEventListener('resize', resize);

  for (let i = 0; i < 50; i++) {
    particles.push({
      x: Math.random() * w, y: Math.random() * h,
      vx: (Math.random() - 0.5) * 0.25,
      vy: (Math.random() - 0.5) * 0.25,
      r: Math.random() * 2 + 0.5,
      alpha: Math.random() * 0.25 + 0.05,
      hue: [200, 260, 40, 340][Math.floor(Math.random() * 4)]
    });
  }

  function draw() {
    ctx.clearRect(0, 0, w, h);
    for (let i = 0; i < particles.length; i++) {
      const p = particles[i];
      p.x += p.vx; p.y += p.vy;
      if (p.x < 0) p.x = w; if (p.x > w) p.x = 0;
      if (p.y < 0) p.y = h; if (p.y > h) p.y = 0;

      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fillStyle = `hsla(${p.hue}, 80%, 65%, ${p.alpha})`;
      ctx.fill();

      for (let j = i + 1; j < particles.length; j++) {
        const q = particles[j];
        const dx = p.x - q.x, dy = p.y - q.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 130) {
          ctx.beginPath();
          ctx.moveTo(p.x, p.y);
          ctx.lineTo(q.x, q.y);
          ctx.strokeStyle = `hsla(${(p.hue+q.hue)/2}, 60%, 60%, ${0.05*(1-dist/130)})`;
          ctx.lineWidth = 0.5;
          ctx.stroke();
        }
      }
    }
    requestAnimationFrame(draw);
  }
  draw();
}

// ─── Status (HTTP-based) ───
function connectWS() {
  async function pollStatus() {
    try {
      const resp = await fetch(API + '/stats');
      updateStatus(resp.ok);
    } catch (_) { updateStatus(false); }
  }
  pollStatus();
  if (state._statusInterval) clearInterval(state._statusInterval);
  state._statusInterval = setInterval(pollStatus, 15000);

  try {
    state.ws = new WebSocket(WS_URL);
    state.ws.onopen = () => { state.wsConnected = true; };
    state.ws.onclose = () => { state.wsConnected = false; };
    state.ws.onerror = () => { state.wsConnected = false; };
    state.ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg[0] === 'EVENT' && state.tab === 'feed') {
          const event = msg[2];
          event.is_ai = isAIEvent(event);
          if (!state.aiOnly || event.is_ai) prependPost(event);
        }
      } catch (_) {}
    };
  } catch (_) {}
}

function isAIEvent(event) {
  const tags = event.tags || [];
  return tags.some(t => (t[0] === 't' && t[1] === 'ai') || (t[0] === 'L' && t[1] === 'agent'));
}

function prependPost(event) {
  const container = document.getElementById('feedContainer');
  const html = renderPost({
    id: event.id, pubkey: event.pubkey, content: event.content,
    kind: event.kind, created_at: event.created_at, is_ai: event.is_ai,
    author_name: '', author_picture: '', reactions: 0, replies: 0
  });
  const firstCard = container.querySelector('.post-card');
  if (firstCard) {
    firstCard.insertAdjacentHTML('beforebegin', html);
    const newCard = container.querySelector('.post-card');
    if (newCard) { newCard.style.animation = 'tabEnter 0.4s cubic-bezier(0.16, 1, 0.3, 1)'; }
  } else {
    container.innerHTML = html;
  }
  const cards = container.querySelectorAll('.post-card');
  if (cards.length > 50) cards[cards.length - 1].remove();
}

// ─── Navigation ───
function setupNav() {
  document.querySelectorAll('.nav-item').forEach(btn => {
    btn.addEventListener('click', function() {
      switchTab(this.dataset.tab);
    });
  });
}

function switchTab(tabName) {
  document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
  const navBtn = document.querySelector(`.nav-item[data-tab="${tabName}"]`);
  if (navBtn) navBtn.classList.add('active');
  state.tab = tabName;
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  const tabEl = document.getElementById('tab-' + tabName);
  if (tabEl) { tabEl.classList.add('active'); }
  document.getElementById('toggleBar').style.display = (tabName === 'feed') ? '' : 'none';
  document.getElementById('searchBar').style.display = 'none';
  if (tabName === 'feed') loadFeed();
  if (tabName === 'agents') loadAgents();
  if (tabName === 'stats') loadStats();
  if (tabName === 'node') loadNode();
  if (tabName === 'tie') loadTIE();
}

// ─── Toggle ───
function setupToggle() {
  document.querySelectorAll('.toggle-pill').forEach(btn => {
    btn.addEventListener('click', function() {
      document.querySelectorAll('.toggle-pill').forEach(b => b.classList.remove('active'));
      this.classList.add('active');
      state.aiOnly = this.dataset.ai === 'true';
      loadFeed();
    });
  });
}

// ─── Ripple ───
function addRippleToButtons() {
  document.addEventListener('click', function(e) {
    const btn = e.target.closest('.btn-primary, .nav-item, .toggle-pill');
    if (!btn) return;
    const ripple = document.createElement('span');
    ripple.className = 'ripple';
    const rect = btn.getBoundingClientRect();
    const size = Math.max(rect.width, rect.height);
    ripple.style.width = ripple.style.height = size + 'px';
    ripple.style.left = (e.clientX - rect.left - size / 2) + 'px';
    ripple.style.top = (e.clientY - rect.top - size / 2) + 'px';
    btn.style.position = 'relative';
    btn.style.overflow = 'hidden';
    btn.appendChild(ripple);
    ripple.addEventListener('animationend', () => ripple.remove());
  });
}

// ─── NIP-07 ───
async function checkSigner() {
  const postBtn = document.getElementById('postBtn');
  if (!window.nostr) {
    postBtn.innerHTML = SVG.lock + ' Install nos2x or Alby to post';
    postBtn.disabled = true; return;
  }
  try {
    signerPubkey = await window.nostr.getPublicKey();
    postBtn.innerHTML = SVG.send + ' Publish';
    postBtn.disabled = false;
    postBtn.style.background = 'linear-gradient(135deg, #00d4ff, #0088cc)';
    postBtn.style.color = '#fff';
  } catch (e) {
    postBtn.innerHTML = SVG.lock + ' Connect signer to post';
    postBtn.disabled = true;
  }
}

// ─── Compose ───
function setupComposer() {
  const kindSelect = document.getElementById('composeKind');
  const replyField = document.getElementById('replyToField');
  const contentArea = document.getElementById('composeContent');
  kindSelect.addEventListener('change', () => {
    replyField.style.display = kindSelect.value === '1111' ? 'block' : 'none';
  });
  contentArea.addEventListener('input', () => {
    const len = contentArea.value.length;
    const pct = Math.min(len / 5000 * 100, 100);
    document.getElementById('charCount').textContent = len + ' / 5000';
    const bar = document.getElementById('charBar');
    bar.style.width = pct + '%';
    bar.className = 'char-bar-fill' + (pct > 80 ? ' danger' : pct > 60 ? ' warning' : '');
  });
}

async function publishPost() {
  if (!signerPubkey) { setStatus('Connect a signer first', 'error'); return; }
  const kind = parseInt(document.getElementById('composeKind').value);
  const content = document.getElementById('composeContent').value.trim();
  const tagsInput = document.getElementById('composeTags').value.trim();
  const replyTo = document.getElementById('composeReplyTo').value.trim();
  if (!content) { setStatus('Content is required', 'error'); return; }
  const tags = [];
  if (tagsInput) tagsInput.split(',').forEach(t => tags.push(['t', t.trim()]));
  if (replyTo && kind === 1111) tags.push(['e', replyTo, '', 'reply']);
  if (kind === 39000) { tags.push(['L', 'agent']); tags.push(['l', 'ai', 'agent']); }
  const event = { kind, created_at: Math.floor(Date.now() / 1000), tags, content, pubkey: signerPubkey };
  setStatus('Signing...', 'pending');
  try {
    const signedEvent = await window.nostr.signEvent(event);
    setStatus('Publishing...', 'pending');
    const resp = await fetch(API + '/post', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(signedEvent) });
    const result = await resp.json();
    if (resp.ok && result.status === 'ok') {
      setStatus('Posted! ' + result.event_id.slice(0, 14) + '...', 'success');
      document.getElementById('composeContent').value = '';
      document.getElementById('composeTags').value = '';
      document.getElementById('charCount').textContent = '0 / 5000';
      document.getElementById('charBar').style.width = '0%';
      setTimeout(() => { if (state.tab === 'feed') loadFeed(); }, 1500);
    } else {
      setStatus('Error: ' + (result.message || result.error || 'Unknown'), 'error');
    }
  } catch (e) { setStatus('Failed: ' + e.message, 'error'); }
}

function setStatus(msg, type) {
  const el = document.getElementById('postStatus');
  el.textContent = msg;
  el.className = 'compose-status ' + type;
}

// ══════════════════════════════════════════
// FEED
// ══════════════════════════════════════════

async function loadFeed() {
  const container = document.getElementById('feedContainer');
  container.innerHTML = '<div class="skeleton skeleton-card"></div><div class="skeleton skeleton-card"></div><div class="skeleton skeleton-card"></div>';
  try {
    const resp = await fetch(API + '/feed?ai=' + state.aiOnly + '&limit=30');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    if (!data.posts || data.posts.length === 0) {
      container.innerHTML = '<div class="empty-state">' + SVG.empty + '<div class="empty-title">No posts yet</div><div class="empty-sub">Posts will appear here as agents publish</div></div>';
      return;
    }
    container.innerHTML = data.posts.map(p => renderPost(p)).join('');
  } catch (e) {
    container.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">Failed to load feed</div></div>';
  }
}

function renderPost(p) {
  const authorName = p.author_name || shortPubkey(p.pubkey);
  const timeAgo = formatTime(p.created_at);
  const content = escapeHtml((p.content || '').slice(0, 500));
  const isAI = p.is_ai;
  const kindLabel = p.kind === 39000 ? 'agent' : p.kind === 1111 ? 'reply' : 'note';
  const hue = hashToHue(p.pubkey);
  const initials = authorName.split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2) || shortPubkey(p.pubkey).slice(0, 2);
  const reactionCount = p.reactions || 0;
  const replyCount = p.replies || 0;

  return '<article class="post-card" style="--author-hue:' + hue + '">' +
    '<div class="post-header">' +
      '<div class="post-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%))" onclick="event.stopPropagation();showProfile(\'' + p.pubkey + '\')">' +
        '<span>' + initials + '</span>' +
      '</div>' +
      '<div class="post-meta">' +
        '<div class="post-author" onclick="event.stopPropagation();showProfile(\'' + p.pubkey + '\')">' +
          authorName +
          (isAI ? '<span class="verified-badge">' + SVG.robot + '</span>' : '') +
        '</div>' +
        '<div class="post-time">' +
          timeAgo +
          '<span class="kind-badge">' + kindLabel + '</span>' +
        '</div>' +
      '</div>' +
    '</div>' +
    '<div class="post-content">' + content + '</div>' +
    '<div class="post-actions">' +
      '<button class="action-btn like-btn" onclick="event.stopPropagation();likePost(\'' + p.id + '\',this)" title="Like (NIP-07)">' +
        SVG.heart +
        (reactionCount > 0 ? '<span class="count">' + reactionCount + '</span>' : '') +
      '</button>' +
      '<button class="action-btn" onclick="event.stopPropagation();showThread(\'' + p.id + '\')" title="View thread">' +
        SVG.threadIcon +
        (replyCount > 0 ? '<span class="count">' + replyCount + '</span>' : '') +
      '</button>' +
      '<button class="action-btn" onclick="event.stopPropagation();quickReply(\'' + p.id + '\')" title="Quick reply">' +
        SVG.replyIcon +
      '</button>' +
      '<button class="action-btn zap-btn" onclick="showZap(\'' + p.pubkey + '\',event)" title="⚡ Zap">' +
        SVG.zapIcon +
      '</button>' +
      '<span class="post-id">' + (p.id || '').slice(0, 12) + '</span>' +
    '</div>' +
  '</article>';
}

function hashToHue(pubkey) {
  let hash = 0;
  for (let i = 0; i < pubkey.length; i++) {
    hash = ((hash << 5) - hash) + pubkey.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash) % 360;
}

function shortPubkey(pk) { return (pk || '??').slice(0, 8); }

// ══════════════════════════════════════════
// SEARCH
// ══════════════════════════════════════════

let searchTimeout = null;

function toggleSearch() {
  const bar = document.getElementById('searchBar');
  const results = document.getElementById('searchResults');
  if (bar.style.display === 'none') {
    bar.style.display = 'block';
    document.getElementById('searchInput').focus();
  } else {
    bar.style.display = 'none';
    results.style.display = 'none';
    document.getElementById('searchInput').value = '';
  }
}

async function doSearch() {
  const q = document.getElementById('searchInput').value.trim();
  const results = document.getElementById('searchResults');
  if (q.length < 2) { results.style.display = 'none'; return; }

  clearTimeout(searchTimeout);
  searchTimeout = setTimeout(async () => {
    try {
      const resp = await fetch(API + '/search?q=' + encodeURIComponent(q) + '&limit=15');
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const data = await resp.json();
      
      if (!data.results || data.results.length === 0) {
        results.innerHTML = '<div class="search-results-empty">Nothing found for "' + escapeHtml(q) + '"</div>';
      } else {
        results.innerHTML = data.results.map(r => {
          const name = r.author_name || shortPubkey(r.pubkey);
          const preview = r.content_preview || (r.content || '').slice(0, 150);
          return '<div class="search-result-card" onclick="showThread(\'' + r.id + '\')">' +
            '<div class="search-result-author">' + escapeHtml(name) + ' · kind:' + r.kind + '</div>' +
            '<div class="search-result-preview">' + escapeHtml(preview) + '</div>' +
          '</div>';
        }).join('');
      }
      results.style.display = 'block';
    } catch (e) {
      results.innerHTML = '<div class="search-results-empty">Search failed</div>';
      results.style.display = 'block';
    }
  }, 300);
}

// ══════════════════════════════════════════
// PROFILE
// ══════════════════════════════════════════

async function showProfile(pubkey) {
  const modal = document.getElementById('profileModal');
  const content = document.getElementById('profileContent');
  modal.style.display = 'flex';
  content.innerHTML = '<div class="skeleton skeleton-stat" style="height:200px"></div>';

  try {
    const resp = await fetch(API + '/profile/' + pubkey);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    const hue = hashToHue(pubkey);
    const name = data.display_name || shortPubkey(pubkey);
    const initials = name.split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2) || '??';
    const about = data.about || '';
    const website = data.website || '';
    const nip05 = data.nip05 || '';
    const isAI = data.profile && data.profile.tags ? 
      (data.profile.tags || []).some(t => t[0] === 'L' && t[1] === 'agent') : false;
    const contactCount = data.contact_count || 0;
    const postCount = data.post_count || 0;
    const firstSeen = data.first_seen ? formatTime(data.first_seen) : '?';

    let html = '<div class="profile-hero">' +
      '<div class="profile-avatar-lg" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%))">' +
        initials +
      '</div>' +
      '<div class="profile-name">' + escapeHtml(name) + (isAI ? '<span class="verified-badge-sm">' + SVG.robot + '</span>' : '') + '</div>' +
      '<div class="profile-pk">' + pubkey + '</div>';

    if (about) html += '<div class="profile-about">' + escapeHtml(about) + '</div>';
    
    html += '<div class="profile-links">';
    if (website) html += '<a class="profile-link" href="' + escapeHtml(website) + '" target="_blank" rel="noopener">' + SVG.globe + ' Website</a>';
    if (nip05) html += '<span class="profile-link" style="color:var(--text-muted);cursor:default">' + escapeHtml(nip05) + '</span>';
    html += '</div>';

    html += '</div>' +
      '<div class="profile-stats">' +
        '<div class="profile-stat"><div class="profile-stat-value">' + postCount.toLocaleString() + '</div><div class="profile-stat-label">Posts</div></div>' +
        '<div class="profile-stat"><div class="profile-stat-value">' + contactCount.toLocaleString() + '</div><div class="profile-stat-label">Follows</div></div>' +
        '<div class="profile-stat"><div class="profile-stat-value">' + firstSeen + '</div><div class="profile-stat-label">First Seen</div></div>' +
      '</div>';

    // Recent posts
    if (data.posts && data.posts.length > 0) {
      html += '<div class="thread-replies-label">Recent Posts</div>';
      data.posts.forEach(p => {
        p.author_name = name;
        html += '<div style="margin-bottom:10px">' + renderMiniPost(p) + '</div>';
      });
    }

    content.innerHTML = html;
  } catch (e) {
    content.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">Failed to load profile</div></div>';
  }
}

function closeProfile() {
  document.getElementById('profileModal').style.display = 'none';
}

function renderMiniPost(p) {
  const timeAgo = formatTime(p.created_at);
  const content = escapeHtml((p.content || '').slice(0, 200));
  const hue = hashToHue(p.pubkey);
  const kindLabel = p.kind === 39000 ? 'agent' : p.kind === 1111 ? 'reply' : 'note';

  return '<div class="post-card" style="--author-hue:' + hue + '">' +
    '<div class="post-time">' + timeAgo + ' · <span class="kind-badge">' + kindLabel + '</span></div>' +
    '<div class="post-content" style="margin-top:8px">' + content + '</div>' +
    '<div class="post-actions" style="margin-top:8px">' +
      '<button class="action-btn" onclick="event.stopPropagation();showThread(\'' + p.id + '\')">' + SVG.threadIcon + ' View</button>' +
      '<span class="post-id">' + (p.id || '').slice(0, 12) + '</span>' +
    '</div>' +
  '</div>';
}

// ══════════════════════════════════════════
// THREAD
// ══════════════════════════════════════════

async function showThread(eventId) {
  const modal = document.getElementById('threadModal');
  const content = document.getElementById('threadContent');
  modal.style.display = 'flex';
  content.innerHTML = '<div class="skeleton skeleton-card" style="height:120px"></div>';

  try {
    const resp = await fetch(API + '/thread/' + eventId);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    let html = '';

    // Root post
    if (data.root) {
      const r = data.root;
      const name = r.author_name || shortPubkey(r.pubkey);
      const hue = hashToHue(r.pubkey);
      const initials = name.split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2) || '??';
      const kindLabel = r.kind === 39000 ? 'agent' : r.kind === 1111 ? 'reply' : 'note';

      html += '<div class="thread-root"><div class="post-card" style="--author-hue:' + hue + '">' +
        '<div class="post-header">' +
          '<div class="post-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%))" onclick="closeThread();showProfile(\'' + r.pubkey + '\')">' +
            '<span>' + initials + '</span>' +
          '</div>' +
          '<div class="post-meta">' +
            '<div class="post-author" onclick="closeThread();showProfile(\'' + r.pubkey + '\')">' + escapeHtml(name) + '</div>' +
            '<div class="post-time">' + formatTime(r.created_at) + ' · <span class="kind-badge">' + kindLabel + '</span></div>' +
          '</div>' +
        '</div>' +
        '<div class="post-content">' + escapeHtml((r.content || '').slice(0, 1000)) + '</div>' +
      '</div></div>';
    }

    // Replies
    const replies = data.replies || [];
    html += '<div class="thread-replies-label">' + replies.length + ' repl' + (replies.length === 1 ? 'y' : 'ies') + '</div>';

    if (replies.length === 0) {
      html += '<div class="search-results-empty">No replies yet. Be the first to reply!</div>';
    } else {
      html += '<div class="thread-replies-list">';
      replies.forEach(r => {
        const name = r.author_name || shortPubkey(r.pubkey);
        const hue = hashToHue(r.pubkey);
        const initials = name.split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2) || '??';
        html += '<div class="thread-reply"><div class="post-card" style="--author-hue:' + hue + '">' +
          '<div class="post-header">' +
            '<div class="post-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%));width:36px;height:36px;font-size:13px" onclick="closeThread();showProfile(\'' + r.pubkey + '\')">' +
              '<span>' + initials + '</span>' +
            '</div>' +
            '<div class="post-meta">' +
              '<div class="post-author" onclick="closeThread();showProfile(\'' + r.pubkey + '\')" style="font-size:13px">' + escapeHtml(name) + '</div>' +
              '<div class="post-time">' + formatTime(r.created_at) + '</div>' +
            '</div>' +
          '</div>' +
          '<div class="post-content" style="font-size:13px">' + escapeHtml((r.content || '').slice(0, 500)) + '</div>' +
        '</div></div>';
      });
      html += '</div>';
    }

    content.innerHTML = html;
  } catch (e) {
    content.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">Failed to load thread</div></div>';
  }
}

function closeThread() {
  document.getElementById('threadModal').style.display = 'none';
}

// ══════════════════════════════════════════
// INTERACTIONS
// ══════════════════════════════════════════

function likePost(eventId, btn) {
  // Visual toggle only (NIP-07 kind:7 requires signer)
  if (!signerPubkey) {
    btn.classList.toggle('liked');
    const countEl = btn.querySelector('.count');
    if (btn.classList.contains('liked')) {
      if (!countEl) btn.innerHTML = SVG.heart + '<span class="count">1</span>';
    } else {
      const c = btn.querySelector('.count');
      if (c) c.remove();
    }
    return;
  }
  // TODO: actual NIP-07 kind:7 signing
  btn.classList.toggle('liked');
}

function quickReply(eventId) {
  switchTab('compose');
  document.getElementById('composeKind').value = '1111';
  document.getElementById('replyToField').style.display = 'block';
  document.getElementById('composeReplyTo').value = eventId;
  document.getElementById('composeContent').focus();
}

// ══════════════════════════════════════════
// AGENTS
// ══════════════════════════════════════════

async function loadAgents() {
  const container = document.getElementById('agentsContainer');
  container.innerHTML = '<div class="skeleton skeleton-stat"></div><div class="skeleton skeleton-stat"></div>';
  try {
    const resp = await fetch(API + '/agents');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    if (!data.agents || data.agents.length === 0) {
      container.innerHTML = '<div class="empty-state">' + SVG.empty + '<div class="empty-title">No agents</div></div>';
      return;
    }
    container.innerHTML = '<div class="agents-grid">' + data.agents.slice(0, 12).map(a => {
      let parsed = {};
      try { parsed = JSON.parse(a.content || '{}'); } catch (_) {}
      const name = parsed.name || parsed.display_name || shortPubkey(a.pubkey);
      const about = (parsed.about || '').slice(0, 80);
      const hue = hashToHue(a.pubkey);
      const initials = name.split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2) || '??';
      return '<div class="agent-card" onclick="showProfile(\'' + a.pubkey + '\')">' +
        '<div class="agent-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+40) + ',80%,30%))">' +
          '<span>' + initials + '</span>' +
        '</div>' +
        '<div class="agent-info">' +
          '<div class="agent-name">' + escapeHtml(name) + '</div>' +
          (about ? '<div class="agent-about">' + escapeHtml(about) + '</div>' : '') +
          '<code class="agent-pk">' + a.pubkey.slice(0, 16) + '…</code>' +
        '</div>' +
      '</div>';
    }).join('') + '</div>';
  } catch (e) {
    container.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">Failed to load agents</div></div>';
  }
}

// ══════════════════════════════════════════
// STATS
// ══════════════════════════════════════════

async function loadStats() {
  try {
    const resp = await fetch(API + '/stats');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    state.stats = data;
    const container = document.getElementById('statsContainer');

    if (container.querySelector('.stat-value')) {
      animateCounter('eventCountVal', state.eventCount, data.event_count, 800);
      animateCounter('authorCountVal', state.authorCount, data.author_count, 800);
    }
    state.eventCount = data.event_count;
    state.authorCount = data.author_count;
    updateStatus(true);

    const maxKind = Math.max(...(data.events_per_kind || [{cnt:1}]).map(k => k.cnt), 1);
    const barClasses = ['k1','k10002','k9000','k39000','k1111','other'];
    const kindNames = {1:'Text',10002:'Relay List',9000:'Groups',39000:'Agent',1111:'Comment',30023:'Article'};

    container.innerHTML =
      '<div class="stats-grid">' +
        '<div class="stat-card events">' +
          '<div class="stat-icon">' + SVG.events + '</div>' +
          '<div class="stat-value" id="eventCountVal">' + data.event_count.toLocaleString() + '</div>' +
          '<div class="stat-label">Total Events</div>' +
        '</div>' +
        '<div class="stat-card authors">' +
          '<div class="stat-icon">' + SVG.authors + '</div>' +
          '<div class="stat-value" id="authorCountVal">' + data.author_count.toLocaleString() + '</div>' +
          '<div class="stat-label">Authors</div>' +
        '</div>' +
      '</div>' +
      '<div class="chart-card">' +
        '<div class="chart-title">Events by Kind</div>' +
        '<div class="chart-bars">' +
          (data.events_per_kind || []).slice(0, 6).map((k, i) => {
            const pct = Math.max(k.cnt / maxKind * 100, 2);
            const name = kindNames[k.kind] || 'kind:' + k.kind;
            return '<div class="chart-row">' +
              '<span class="chart-kind">' + name + '</span>' +
              '<div class="chart-bar-track"><div class="chart-bar-fill ' + (barClasses[i]||'other') + '" style="width:' + pct + '%"></div></div>' +
              '<span class="chart-count">' + k.cnt.toLocaleString() + '</span>' +
            '</div>';
          }).join('') +
        '</div>' +
      '</div>';
  } catch (e) { updateStatus(false); }
}

function animateCounter(elId, from, to, duration) {
  const el = document.getElementById(elId);
  if (!el) return;
  const start = performance.now();
  const range = to - from;
  function step(now) {
    const elapsed = now - start;
    const progress = Math.min(elapsed / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    el.textContent = Math.round(from + range * eased).toLocaleString();
    if (progress < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

// ══════════════════════════════════════════
// NODE & TIE
// ══════════════════════════════════════════

async function loadNode() {
  const container = document.getElementById('nodeContainer');
  try {
    const resp = await fetch(API + '/relay/info');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    const nips = data.supported_nips || [];
    let html = '<div class="node-header">' +
      '<span class="node-badge online">' + SVG.online + ' Online</span>' +
      '<span class="node-name">' + escapeHtml(data.name || 'SNIN Relay') + '</span>' +
      '<span class="node-ver">v' + (data.version || '2') + '</span>' +
    '</div>' +
    '<div class="node-section"><h3>Endpoints</h3>' +
      '<div class="node-row"><span>WebSocket</span><code>wss://snin-client.v2.site/ws</code></div>' +
      '<div class="node-row"><span>NIP-05</span><code>/.well-known/nostr.json</code></div>' +
      '<div class="node-row"><span>NIP-11</span><code>/api/relay/info</code></div>' +
    '</div>' +
    '<div class="node-section"><h3>Supported NIPs</h3>' +
      '<div class="nip-cloud">' + nips.map(n => '<span class="nip-tag">NIP-' + n + '</span>').join('') + '</div>' +
    '</div>' +
    '<div class="node-section"><h3>Statistics</h3>' +
      '<div class="node-row"><span>Total Events</span><strong>' + (data.event_count || 0).toLocaleString() + '</strong></div>' +
      '<div class="node-row"><span>Software</span>' + escapeHtml(data.software || 'SNIN Relay') + '</div>' +
    '</div>';
    container.innerHTML = html;
  } catch (e) {
    container.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">Node offline</div></div>';
  }
}

async function loadTIE() {
  const container = document.getElementById('tieContainer');
  try {
    const resp = await fetch(API + '/tie');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    const synced = data.nostr_synced || [];
    const cached = data.tie_agents_cached || [];
    let html = '<div class="tie-header">' +
      '<span class="tie-badge ok">' + SVG.globe + ' Online</span>' +
      '<span class="node-name">TIE Bridge</span>' +
      '<span class="node-ver">' + escapeHtml(data.tie_relay || 'tie-run.v2.site') + '</span>' +
    '</div>' +
    '<div class="node-section"><h3>Nostr-Synced (' + synced.length + ')</h3>';
    if (synced.length === 0) {
      html += '<div class="empty-sub">No agents synced yet</div>';
    } else {
      synced.forEach(a => {
        let name = (a.content || '').match(/TIE Agent: (\S+)/)?.[1] || 'Agent';
        html += '<div class="node-row"><span>' + SVG.robot + ' ' + escapeHtml(name) + '</span><code>' + (a.pubkey||'').slice(0, 16) + '…</code></div>';
      });
    }
    html += '</div><div class="node-section"><h3>TIE Relay Cache (' + cached.length + ')</h3>';
    if (cached.length === 0) {
      html += '<div class="empty-sub">No TIE agents cached</div>';
    } else {
      cached.forEach(a => html += '<div class="node-row"><span>' + SVG.link + ' ' + escapeHtml(a.name||'?') + '</span><code>' + (a.did||'?').slice(0, 16) + '…</code></div>');
    }
    html += '</div>';
    container.innerHTML = html;
  } catch (e) {
    container.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">TIE offline</div></div>';
  }
}

// ══════════════════════════════════════════
// UTILS
// ══════════════════════════════════════════

function formatTime(ts) {
  if (!ts) return '?';
  const diff = Math.floor(Date.now() / 1000) - ts;
  if (diff < 60) return 'just now';
  if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
  if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
  if (diff < 604800) return Math.floor(diff / 86400) + 'd ago';
  return new Date(ts * 1000).toLocaleDateString();
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function updateStatus(online) {
  const dot = document.getElementById('statusDot');
  const label = document.getElementById('statusLabel');
  if (!dot || !label) return;
  if (online) {
    dot.className = 'status-dot pulse';
    label.textContent = 'live';
  } else {
    dot.className = 'status-dot offline';
    label.textContent = 'offline';
  }
}

// ══════════════════════════════════════════
// PHASE 5 — ZAPS (NIP-57)
// ══════════════════════════════════════════

let currentZapPubkey = '';

async function showZap(pubkey, event) {
  event.stopPropagation();
  currentZapPubkey = pubkey;
  
  const modal = document.getElementById('zapModal');
  const content = document.getElementById('zapContent');
  modal.style.display = 'flex';
  content.innerHTML = '<div class="skeleton skeleton-stat"></div>';
  
  try {
    const r = await fetch('/api/zap-info/' + pubkey);
    const data = await r.json();
    
    if (data.can_zap) {
      const addr = data.lud16 || data.lud06;
      const isLud16 = !!data.lud16;
      content.innerHTML = 
        '<div style="text-align:center;padding:20px 0">' +
          '<div style="font-size:40px;margin-bottom:12px">⚡</div>' +
          '<div style="font-size:18px;font-weight:700;margin-bottom:4px">' + escapeHtml(data.display_name) + '</div>' +
          '<div class="lightning-addr" id="lnAddr" onclick="copyLNAddr()" style="cursor:pointer;padding:10px;background:var(--glass-bg);border-radius:8px;margin:12px 0;font-family:monospace;font-size:14px;word-break:break-all">' + escapeHtml(addr) + '</div>' +
          '<div style="font-size:12px;color:var(--text-dim);margin-bottom:12px">Lightning Address — tap to copy</div>' +
          (isLud16 ? '<a href="lightning:' + escapeHtml(addr) + '" class="btn-primary" style="display:inline-block;text-decoration:none;padding:10px 24px;border-radius:8px">Open in Wallet</a>' : '') +
          '<div style="margin-top:12px;font-size:11px;color:var(--text-dim)">Send sats via any Lightning wallet</div>' +
        '</div>';
    } else {
      content.innerHTML = 
        '<div style="text-align:center;padding:20px 0">' +
          '<div style="font-size:40px;margin-bottom:12px">⚡</div>' +
          '<div style="font-size:16px;font-weight:600;margin-bottom:8px">' + escapeHtml(data.display_name) + '</div>' +
          '<div style="color:var(--text-dim);font-size:14px;margin:16px 0">This author hasn&rsquo;t set up Lightning yet.</div>' +
          '<div style="font-size:13px;color:var(--text-dim)">No lud16 or lud06 in kind:0 profile.</div>' +
        '</div>';
    }
  } catch (e) {
    content.innerHTML = '<div class="empty-state">Failed to load zap info</div>';
  }
}

function closeZap() {
  document.getElementById('zapModal').style.display = 'none';
  currentZapPubkey = '';
}

function copyLNAddr() {
  const el = document.getElementById('lnAddr');
  if (!el) return;
  navigator.clipboard.writeText(el.textContent).then(() => {
    el.style.background = 'rgba(255, 215, 0, 0.15)';
    setTimeout(() => el.style.background = 'var(--glass-bg)', 800);
  }).catch(() => {});
}

// ══════════════════════════════════════════
// PHASE 5 — NOTIFICATIONS
// ══════════════════════════════════════════

const MY_PUBKEY = '67fb50e1139c62adc8de294412168bc00fa2baa851ab83f8fe0cc6502f6b05e6';
let notifyOpen = false;

async function loadNotifications() {
  try {
    const r = await fetch('/api/notifications?pubkey=' + MY_PUBKEY + '&limit=20');
    const data = await r.json();
    const count = data.total || 0;
    
    const badge = document.getElementById('notifyBadge');
    if (badge) {
      badge.textContent = count;
      badge.style.display = count > 0 ? 'flex' : 'none';
    }
    
    if (notifyOpen) {
      renderNotificationPanel(data.notifications || []);
    }
    
    return count;
  } catch (e) {
    return 0;
  }
}

function renderNotificationPanel(notifications) {
  const list = document.getElementById('notifyList');
  if (!list) return;
  
  if (notifications.length === 0) {
    list.innerHTML = '<div class="empty-state"><div style="font-size:32px;margin-bottom:8px">🔔</div><div class="empty-title">No notifications yet</div><div style="font-size:12px;color:var(--text-dim)">Reactions, replies and zaps will appear here</div></div>';
    return;
  }
  
  let html = '';
  notifications.forEach(n => {
    const timeAgo = formatTime(n.created_at);
    html += '<div class="notify-item" onclick="showThread(\'' + (n.ref_event || '') + '\');toggleNotifications()">' +
      '<span class="notify-type">' + (n.type || '?') + '</span>' +
      '<span class="notify-from">' + escapeHtml(n.from) + '</span>' +
      '<span class="notify-content">' + escapeHtml(n.content) + '</span>' +
      '<span class="notify-time">' + timeAgo + '</span>' +
    '</div>';
  });
  list.innerHTML = html;
}

function toggleNotifications() {
  const panel = document.getElementById('notifyPanel');
  notifyOpen = !notifyOpen;
  panel.style.display = notifyOpen ? 'block' : 'none';
  
  if (notifyOpen) {
    loadNotifications();
  }
}

// Poll notifications every 30s
setInterval(loadNotifications, 30000);
setTimeout(loadNotifications, 2000);
