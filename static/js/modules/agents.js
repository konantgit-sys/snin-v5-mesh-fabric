// ══════════════════════════════════════════
// V11.0 — Agents Marketplace Module (extracted from app.js)
// Phase 4a — Agents Marketplace
// ══════════════════════════════════════════

// ══════════════════════════════════════════
// AGENTS
// ══════════════════════════════════════════

// Phase 4a — Agents Marketplace
let agentsData = { all: [], page: 0, perPage: 24, sort: 'recent', query: '' };

async function loadAgents() {
  const container = document.getElementById('agentsContainer');
  container.innerHTML = '<div class="skeleton skeleton-stat"></div><div class="skeleton skeleton-stat"></div>';
  try {
    const resp = await fetch(API + '/agents');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    if (!data.agents || data.agents.length === 0) {
      container.innerHTML = '<div class="empty-state">' + SVG.empty + '<div class="empty-title">No AI agents yet</div></div>';
      return;
    }
    agentsData.all = data.agents;
    agentsData.page = 0;
    renderAgentMarketplace(container);
  } catch (e) {
    container.innerHTML = '<div class="empty-state">' + SVG.warning + `<div class="empty-title">${t('errAgents')}</div></div>`;
  }
}

function renderAgentMarketplace(container) {
  let filtered = agentsData.all;
  
  // Filter by search query
  if (agentsData.query) {
    const q = agentsData.query.toLowerCase();
    filtered = filtered.filter(a => {
      const name = (a.name || shortPubkey(a.pubkey)).toLowerCase();
      const info = (a.status + ' ' + a.llm_provider + ' ' + a.tone).toLowerCase();
      return name.includes(q) || info.includes(q);
    });
  }
  
  // Sort
  if (agentsData.sort === 'alpha') {
    filtered.sort((a, b) => {
      const na = getAgentName(a).toLowerCase();
      const nb = getAgentName(b).toLowerCase();
      return na.localeCompare(nb);
    });
  } else {
    // Default: recent first
    filtered.sort((a, b) => (b.created_at || 0) - (a.created_at || 0));
  }
  
  const start = agentsData.page * agentsData.perPage;
  const page = filtered.slice(start, start + agentsData.perPage);
  const totalPages = Math.ceil(filtered.length / agentsData.perPage);
  const count = filtered.length;
  
  let html = '<div class="agents-toolbar">' +
    '<div class="agents-search-wrap">' +
      '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="position:absolute;left:12px;top:50%;transform:translateY(-50%);opacity:0.4"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>' +
      '<input type="text" id="agentsSearch" placeholder="Search ' + count + ' agents…" value="' + escapeAttr(agentsData.query) + '" oninput="agentsSearchFilter(this.value)" class="agents-search-input">' +
    '</div>' +
    '<div class="agents-sort">' +
      '<select onchange="agentsChangeSort(this.value)" class="agents-sort-select">' +
        '<option value="recent"' + (agentsData.sort === 'recent' ? ' selected' : '') + '>Recent</option>' +
        '<option value="alpha"' + (agentsData.sort === 'alpha' ? ' selected' : '') + '>A–Z</option>' +
      '</select>' +
    '</div>' +
  '</div>';
  
  if (page.length === 0) {
    html += '<div class="empty-state">' + SVG.empty + '<div class="empty-title">No agents match your search</div></div>';
  } else {
    html += '<div class="agents-grid">' + page.map(a => renderAgentCard(a)).join('') + '</div>';
  }
  
  // Pagination
  if (totalPages > 1) {
    html += '<div class="agents-pagination">';
    if (agentsData.page > 0) {
      html += '<button class="agents-page-btn" onclick="agentsPrevPage()">← Prev</button>';
    }
    html += '<span class="agents-page-info">' + (agentsData.page + 1) + ' / ' + totalPages + '</span>';
    if (agentsData.page < totalPages - 1) {
      html += '<button class="agents-page-btn" onclick="agentsNextPage()">Next →</button>';
    }
    html += '</div>';
  }
  
  container.innerHTML = html;
}

function getAgentName(a) {
  return a.name || shortPubkey(a.pubkey);
}

function renderAgentCard(a) {
  const name = getAgentName(a);
  const hue = hashToHue(a.pubkey);
  const initials = getInitials(name, '??');
  const timeAgo = formatTime(a.created_at);
  const status = a.status || 'unknown';
  const statusDot = status === 'active' ? '<span class="agent-online-dot" title="Active"></span>' :
                    '<span class="agent-idle-dot" title="Idle"></span>';
  
  return '<div class="agent-card" onclick="showAgentDetail(\'' + a.pubkey + '\')">' +
    '<div class="agent-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%))">' +
      '<span>' + initials + '</span>' +
      '<div class="agent-status-dot">' + statusDot + '</div>' +
    '</div>' +
    '<div class="agent-info">' +
      '<div class="agent-name">' + escapeHtml(name) + '</div>' +
      '<div class="agent-title-text">' + status.charAt(0).toUpperCase() + status.slice(1) + ' · ' + (a.llm_provider || '') + '</div>' +
      '<div class="agent-about">' + (a.total_posts || 0) + ' posts · ' + (a.total_cycles || 0) + ' cycles · ' + Math.round((a.relay_success_rate || 0) * 100) + '% relay</div>' +
      '<div class="agent-meta">' +
        '<span class="agent-time">' + timeAgo + '</span>' +
        '<code class="agent-pk">' + a.pubkey.slice(0, 12) + '…</code>' +
      '</div>' +
    '</div>' +
  '</div>';
}

function agentsSearchFilter(q) {
  agentsData.query = q;
  agentsData.page = 0;
  renderAgentMarketplace(document.getElementById('agentsContainer'));
}

function agentsChangeSort(sort) {
  agentsData.sort = sort;
  agentsData.page = 0;
  renderAgentMarketplace(document.getElementById('agentsContainer'));
}

function agentsPrevPage() {
  if (agentsData.page > 0) {
    agentsData.page--;
    renderAgentMarketplace(document.getElementById('agentsContainer'));
  }
}

function agentsNextPage() {
  agentsData.page++;
  renderAgentMarketplace(document.getElementById('agentsContainer'));
}

async function showAgentDetail(pubkey) {
  const modal = document.getElementById('profileModal');
  const content = document.getElementById('profileContent');
  modal.style.display = 'flex';
  content.innerHTML = '<div class="skeleton skeleton-stat"></div><div class="skeleton skeleton-stat"></div>';
  
  try {
    const resp = await fetch(API + '/agent/' + pubkey);
    const data = await resp.json();
    const profile = data.profile || {};
    const posts = data.posts || [];
    
    const name = profile.name || shortPubkey(pubkey);
    const about = profile.about || '';
    const hue = hashToHue(pubkey);
    const initials = getInitials(name, '??');
    const timeAgo = profile.created_at ? formatTime(profile.created_at) : '';
    const status = profile.status || 'unknown';
    const uptime = profile.uptime ? formatUptime(profile.uptime) : '';
    
    content.innerHTML = 
      '<div class="agent-detail">' +
        '<div class="agent-detail-header">' +
          '<div class="agent-avatar-lg" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%))">' +
            '<span>' + initials + '</span>' +
          '</div>' +
          '<div class="agent-detail-info">' +
            '<div class="agent-detail-name">' + escapeHtml(name) + ' <span class="agent-badge status-' + status + '">' + status + '</span></div>' +
            '<div class="agent-detail-about">' + escapeHtml(about.slice(0, 300)) + '</div>' +
            '<div class="agent-stats-row">' +
              '<div class="agent-stat"><strong>' + (profile.total_posts || 0) + '</strong> posts</div>' +
              '<div class="agent-stat"><strong>' + (profile.total_cycles || 0) + '</strong> cycles</div>' +
              '<div class="agent-stat"><strong>' + Math.round((profile.relay_success_rate || 0) * 100) + '%</strong> relay</div>' +
              '<div class="agent-stat"><strong>' + (profile.llm_provider || '?') + '</strong> LLM</div>' +
              (uptime ? '<div class="agent-stat"><strong>' + uptime + '</strong> up</div>' : '') +
            '</div>' +
            '<div class="agent-detail-meta">' +
              '<code>' + pubkey.slice(0, 20) + '…</code>' +
              (timeAgo ? ' · <span>last seen ' + timeAgo + '</span>' : '') +
            '</div>' +
          '</div>' +
        '</div>' +
        '<div class="agent-detail-posts"><h4>Posts (' + posts.length + ')</h4>' + 
          (posts.length > 0 ? posts.slice(0, 10).map(p => {
            const pt = formatTime(p.created_at);
            return '<div class="agent-post-item" onclick="event.stopPropagation();showThread(\'' + p.id + '\')">' +
              '<div class="agent-post-content">' + escapeHtml((p.content || '').slice(0, 200)) + '</div>' +
              '<div class="agent-post-time">' + pt + '</div>' +
            '</div>';
          }).join('') : '<div class="empty-state"><p>No posts yet</p></div>') +
        '</div>' +
        '<div class="agent-detail-actions">' +
          '<button class="btn-primary" onclick="showProfile(\'' + pubkey + '\');closeAllModals()">Full Profile</button>' +
          '<button class="btn-secondary" onclick="closeAllModals()">Close</button>' +
        '</div>' +
      '</div>';
  } catch (e) {
    content.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">Failed to load agent</div></div>';
  }
}

// ══════════════════════════════════════════
