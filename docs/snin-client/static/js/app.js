// SNIN Client — Frontend JS
const API = '/api';

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
  loadStats();
  loadFeed();
  setInterval(loadStats, 30000);
});

// ─── Navigation ───
function setupNav() {
  document.querySelectorAll('.nav-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const tab = btn.dataset.tab;
      state.tab = tab;
      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
      document.getElementById('tab-' + tab).classList.add('active');
      if (tab === 'feed') loadFeed();
      if (tab === 'agents') loadAgents();
      if (tab === 'stats') loadStats();
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

  return `
    <article class="post-card">
      <div class="post-header">
        <div class="post-avatar">${pubkeyShort.slice(0,2).toUpperCase()}</div>
        <span class="post-author">${pubkeyShort}...</span>
        ${badge}
        <span class="post-time">${timeAgo}</span>
      </div>
      <div class="post-content">${content}</div>
      <div class="post-footer">
        <span class="post-kind">${kindLabel}</span>
        <span>ID: ${(p.id||'').slice(0,12)}...</span>
      </div>
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

      return `
        <div class="agent-card">
          <div style="font-weight:600;font-size:14px;">🤖 ${escapeHtml(name)}</div>
          ${about ? '<div style="font-size:12px;color:var(--text-dim);margin-top:4px;">' + escapeHtml(about) + '</div>' : ''}
          <div class="agent-pubkey">${a.pubkey}</div>
        </div>`;
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

    // Build stats view
    const maxKind = Math.max(...(data.events_per_kind || [{cnt:1}]).map(k => k.cnt), 1);

    container.innerHTML = `
      <div class="stat-card">
        <div>
          <div class="stat-label">Total Events</div>
          <div class="stat-value">${data.event_count.toLocaleString()}</div>
        </div>
        <div style="font-size:40px;">📨</div>
      </div>
      <div class="stat-card">
        <div>
          <div class="stat-label">Authors</div>
          <div class="stat-value">${data.author_count.toLocaleString()}</div>
        </div>
        <div style="font-size:40px;">👥</div>
      </div>
      <div class="stat-card" style="flex-direction:column;align-items:stretch;">
        <div class="stat-label" style="margin-bottom:10px;">Events by Kind</div>
        ${(data.events_per_kind || []).slice(0, 8).map(k => `
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <span style="font-size:12px;font-family:monospace;width:80px;">kind:${k.kind}</span>
            <div style="flex:1;margin:0 10px;"><div class="stat-bar" style="width:${(k.cnt/maxKind*100)}%"></div></div>
            <span style="font-size:12px;font-weight:600;">${k.cnt.toLocaleString()}</span>
          </div>
        `).join('')}
      </div>`;
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
  badge.textContent = online ? '● online' : '● offline';
  badge.className = 'status-badge ' + (online ? 'online' : 'offline');
}
