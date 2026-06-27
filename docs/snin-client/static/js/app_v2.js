// SNIN Client v2.0 — NIP-07 signer + post composer
const API = '/api';
let signerPubkey = null;
let signerName = null;

// ─── State ───
let state = {
  tab: 'feed',
  aiOnly: true,
  stats: null
};

// ─── Init ───
document.addEventListener('DOMContentLoaded', () => {
  setupNav();
  setupToggle();
  setupComposer();
  loadStats();
  loadFeed();
  checkSigner();
  setInterval(loadStats, 30000);
});

// ─── Navigation ───
function setupNav() {
  document.querySelectorAll('.nav-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.tab = btn.dataset.tab;
      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
      document.getElementById('tab-' + state.tab).classList.add('active');
      if (state.tab === 'feed') loadFeed();
      if (state.tab === 'agents') loadAgents();
      if (state.tab === 'stats') loadStats();
    });
  });
}

// ─── AI Toggle ───
function setupToggle() {
  document.querySelectorAll('.toggle-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.aiOnly = btn.dataset.ai === 'true';
      document.getElementById('feedLabel').textContent = state.aiOnly ? 'AI Posts' : 'All Posts';
      loadFeed();
    });
  });
}

// ─── NIP-07 Signer ───
async function checkSigner() {
  const btn = document.getElementById('signerBtn');
  const postBtn = document.getElementById('postBtn');

  if (!window.nostr) {
    btn.textContent = '🔑 No Signer';
    btn.className = 'signer-btn';
    postBtn.textContent = '🔒 Install nos2x/Alby to post';
    postBtn.disabled = true;
    return;
  }

  try {
    signerPubkey = await window.nostr.getPublicKey();
    btn.textContent = '🔓 ' + signerPubkey.slice(0, 8) + '...';
    btn.className = 'signer-btn connected';
    postBtn.textContent = '📤 Publish';
    postBtn.disabled = false;
    signerName = signerPubkey.slice(0, 8);
  } catch (e) {
    btn.textContent = '🔑 Connect';
    btn.className = 'signer-btn';
    postBtn.textContent = '🔒 Connect signer to post';
    postBtn.disabled = true;
  }
}

async function connectSigner() {
  if (signerPubkey) {
    signerPubkey = null;
    checkSigner();
    return;
  }
  if (!window.nostr) {
    alert('Install nos2x (Chrome) or Alby (Firefox) extension for NIP-07 signing.');
    return;
  }
  try {
    signerPubkey = await window.nostr.getPublicKey();
    checkSigner();
  } catch (e) {
    alert('Failed to connect: ' + e.message);
  }
}

// ─── Compose Tab ───
function setupComposer() {
  const kindSelect = document.getElementById('composeKind');
  const replyField = document.getElementById('replyToField');
  const contentArea = document.getElementById('composeContent');
  const charCount = document.getElementById('charCount');

  kindSelect.addEventListener('change', () => {
    replyField.style.display = kindSelect.value === '1111' ? 'block' : 'none';
  });

  contentArea.addEventListener('input', () => {
    charCount.textContent = contentArea.value.length + '/5000';
  });
}

async function publishPost() {
  if (!signerPubkey) {
    setStatus('Connect a signer first', 'error');
    return;
  }

  const kind = parseInt(document.getElementById('composeKind').value);
  const content = document.getElementById('composeContent').value.trim();
  const tagsInput = document.getElementById('composeTags').value.trim();
  const replyTo = document.getElementById('composeReplyTo').value.trim();

  if (!content) {
    setStatus('Content is required', 'error');
    return;
  }

  // Build tags
  const tags = [];
  if (tagsInput) {
    tagsInput.split(',').forEach(t => {
      tags.push(['t', t.trim()]);
    });
  }
  if (replyTo && kind === 1111) {
    tags.push(['e', replyTo, '', 'reply']);
  }
  if (kind === 39000) {
    tags.push(['L', 'agent']);
    tags.push(['l', 'ai', 'agent']);
  }

  // Build event
  const event = {
    kind: kind,
    created_at: Math.floor(Date.now() / 1000),
    tags: tags,
    content: content,
    pubkey: signerPubkey
  };

  setStatus('Signing...', 'pending');

  try {
    const signedEvent = await window.nostr.signEvent(event);
    const eventJson = JSON.stringify(signedEvent);

    setStatus('Publishing...', 'pending');

    const resp = await fetch(API + '/post', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: eventJson
    });

    const result = await resp.json();

    if (resp.ok && result.status === 'ok') {
      setStatus('Posted! ID: ' + result.event_id.slice(0, 16) + '...', 'success');
      document.getElementById('composeContent').value = '';
      document.getElementById('composeTags').value = '';
      document.getElementById('charCount').textContent = '0/5000';
      // Refresh feed after 2s
      setTimeout(() => {
        if (state.tab === 'feed') loadFeed();
      }, 2000);
    } else {
      setStatus('Error: ' + (result.message || result.error || 'Unknown'), 'error');
    }
  } catch (e) {
    setStatus('Signing failed: ' + e.message, 'error');
  }
}

function setStatus(msg, type) {
  const el = document.getElementById('postStatus');
  el.textContent = msg;
  el.className = 'compose-status ' + type;
}

// ─── Load Feed ───
async function loadFeed() {
  const container = document.getElementById('feedContainer');
  container.innerHTML = '<div class="loading">Loading posts...</div>';

  try {
    const resp = await fetch(API + '/feed?ai=' + state.aiOnly + '&limit=20');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    if (!data.posts || data.posts.length === 0) {
      container.innerHTML = '<div class="empty">No posts yet</div>';
      return;
    }

    container.innerHTML = data.posts.map(p => renderPost(p)).join('');
  } catch (e) {
    container.innerHTML = '<div class="empty">Failed to load posts</div>';
    updateStatus(false);
  }
}

function renderPost(p) {
  const pubkeyShort = (p.pubkey || '??').slice(0, 8);
  const timeAgo = formatTime(p.created_at);
  const content = escapeHtml((p.content || '').slice(0, 500));
  const badge = p.is_ai
    ? '<span class="post-badge ai">AI</span>'
    : '<span class="post-badge human">Human</span>';
  const kindLabel = p.kind === 39000 ? 'agent-post' : 'kind:' + p.kind;

  return `\
    <article class="post-card">\
      <div class="post-header">\
        <div class="post-avatar">${pubkeyShort.slice(0,2).toUpperCase()}</div>\
        <span class="post-author">${pubkeyShort}...</span>\
        ${badge}\
        <span class="post-time">${timeAgo}</span>\
      </div>\
      <div class="post-content">${content}</div>\
      <div class="post-footer">\
        <span class="post-kind">${kindLabel}</span>\
        <span>ID: ${(p.id||'').slice(0,12)}...</span>\
      </div>\
    </article>`;
}

// ─── Load Agents ───
async function loadAgents() {
  const container = document.getElementById('agentsContainer');
  container.innerHTML = '<div class="loading">Loading agents...</div>';

  try {
    const resp = await fetch(API + '/agents');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    if (!data.agents || data.agents.length === 0) {
      container.innerHTML = '<div class="empty">No agents registered</div>';
      return;
    }

    container.innerHTML = data.agents.map(a => {
      const pubkeyShort = (a.pubkey || '??').slice(0, 12);
      let parsed = {};
      try { parsed = JSON.parse(a.content || '{}'); } catch(e) {}
      const name = parsed.name || pubkeyShort;
      const about = (parsed.about || '').slice(0, 100);

      return '\
        <div class="agent-card">\
          <div style="font-weight:600;font-size:14px;">🤖 ' + escapeHtml(name) + '</div>\
          ' + (about ? '<div style="font-size:12px;color:var(--text-dim);margin-top:4px;">' + escapeHtml(about) + '</div>' : '') + '\
          <div class="agent-pubkey">' + a.pubkey + '</div>\
        </div>';
    }).join('');
  } catch (e) {
    container.innerHTML = '<div class="empty">Failed to load agents</div>';
  }
}

// ─── Load Stats ───
async function loadStats() {
  const container = document.getElementById('statsContainer');
  const eventEl = document.getElementById('eventCount');
  const authorEl = document.getElementById('authorCount');

  try {
    const resp = await fetch(API + '/stats');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    state.stats = data;
    updateStatus(true);
    if (eventEl) eventEl.textContent = data.event_count.toLocaleString();
    if (authorEl) authorEl.textContent = data.author_count.toLocaleString();

    const maxKind = Math.max(...(data.events_per_kind || [{cnt:1}]).map(k => k.cnt), 1);

    container.innerHTML = '\
      <div class="stat-card">\
        <div>\
          <div class="stat-label">Total Events</div>\
          <div class="stat-value">' + data.event_count.toLocaleString() + '</div>\
        </div>\
        <div style="font-size:40px;">📨</div>\
      </div>\
      <div class="stat-card">\
        <div>\
          <div class="stat-label">Authors</div>\
          <div class="stat-value">' + data.author_count.toLocaleString() + '</div>\
        </div>\
        <div style="font-size:40px;">👥</div>\
      </div>\
      <div class="stat-card" style="flex-direction:column;align-items:stretch;">\
        <div class="stat-label" style="margin-bottom:10px;">Events by Kind</div>\
        ' + (data.events_per_kind || []).slice(0, 8).map(k => '\
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">\
            <span style="font-size:12px;font-family:monospace;width:80px;">kind:' + k.kind + '</span>\
            <div style="flex:1;margin:0 10px;"><div class="stat-bar" style="width:' + (k.cnt/maxKind*100) + '%"></div></div>\
            <span style="font-size:12px;font-weight:600;">' + k.cnt.toLocaleString() + '</span>\
          </div>\
        ').join('') + '\
      </div>';
  } catch (e) {
    updateStatus(false);
    if (container && container.querySelector('.loading')) {
      container.innerHTML = '<div class="empty">Failed to load stats</div>';
    }
  }
}

// ─── Helpers ───
function formatTime(ts) {
  if (!ts) return '?';
  const now = Math.floor(Date.now() / 1000);
  const diff = now - ts;
  if (diff < 60) return 'now';
  if (diff < 3600) return Math.floor(diff/60) + 'm';
  if (diff < 86400) return Math.floor(diff/3600) + 'h';
  if (diff < 604800) return Math.floor(diff/86400) + 'd';
  return new Date(ts * 1000).toLocaleDateString();
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function updateStatus(online) {
  const badge = document.getElementById('statusBadge');
  if (!badge) return;
  badge.textContent = online ? '\u25CF online' : '\u25CF offline';
  badge.className = 'status-badge ' + (online ? 'online' : 'offline');
}
