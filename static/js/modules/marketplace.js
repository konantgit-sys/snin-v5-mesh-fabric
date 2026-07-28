// ═══════════════════════════════════════════
// V16 — Agent Marketplace / Directory
// Agent listing + Marketplace framework (kind:30002-30004)
// ═══════════════════════════════════════════

let mktState = { view: 'agents', agentPk: null };

async function mktApiGet(path) {
    const resp = await fetch(path);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp.json();
}

function mktEscapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function mktFormatTime(ts) {
    if (!ts) return '';
    const dt = new Date(ts * 1000);
    const now = new Date();
    const diffMs = now - dt;
    const diffH = Math.floor(diffMs / 3600000);
    if (diffH < 1) return Math.floor(diffMs / 60000) + 'm ago';
    if (diffH < 24) return diffH + 'h ago';
    return Math.floor(diffH / 24) + 'd ago';
}

function mktTrustColor(score) {
    if (!score && score !== 0) return '#888';
    if (score >= 0.8) return '#22c55e';
    if (score >= 0.5) return '#a3e635';
    if (score >= 0.2) return '#facc15';
    return '#ef4444';
}

function initMarketplace() {
    mktState.view = 'agents';
    mktState.agentPk = null;
    loadMarketplaceAgents();
}

async function loadMarketplaceAgents() {
    const container = document.getElementById('marketplaceContainer');
    container.innerHTML = '<div style="padding:40px;text-align:center;color:var(--text-dim)">Loading agent directory...</div>';

    try {
        const data = await mktApiGet('/api/marketplace/agents?limit=30');
        renderMktAgents(data);
    } catch (err) {
        container.innerHTML = '<div style="padding:40px;text-align:center;color:var(--text-dim)">Failed to load agents</div>';
    }
}

function renderMktAgents(data) {
    const container = document.getElementById('marketplaceContainer');
    const agents = data.agents || [];

    if (agents.length === 0) {
        container.innerHTML = `<div style="padding:60px 20px;text-align:center">
            <div style="font-size:40px;margin-bottom:12px">👻</div>
            <h3 style="margin:0 0 8px">No Agents Found</h3>
            <p style="color:var(--text-dim);margin:0">This relay has no agents yet.</p>
        </div>`;
        return;
    }

    const verified = agents.filter(a => a.is_verified).length;
    const withTrust = agents.filter(a => a.trust_score !== null).length;
    const listings = data.marketplace_listings || 0;

    let html = `<div class="mkt-header">
        <div class="mkt-stats">
            <div class="mkt-stat"><b>${agents.length}</b><small>Agents</small></div>
            <div class="mkt-stat"><b>${verified}</b><small>Verified</small></div>
            <div class="mkt-stat"><b>${withTrust}</b><small>Trusted</small></div>
            <div class="mkt-stat${listings === 0 ? ' mkt-stat-empty' : ''}"><b>${listings}</b><small>Listings</small></div>
        </div>
        ${listings === 0 ? '<div class="mkt-banner">🏪 Marketplace is empty — agents publish kind:30002 to list services</div>' : ''}
    </div><div class="mkt-grid">`;

    for (const a of agents) {
        const trustBadge = a.trust_score !== null
            ? `<span class="mkt-trust-dot" style="background:${mktTrustColor(a.trust_score)}" title="Trust: ${a.trust_score}"></span>`
            : '';
        const verifiedBadge = a.is_verified
            ? '<span class="mkt-verified" title="NIP-05 Verified">✓</span>'
            : '';

        const topTags = (a.top_tags || []).slice(0, 3)
            .map(t => `<span class="mkt-tag">#${mktEscapeHtml(t.tag)}</span>`).join('');

        html += `<div class="mkt-card" onclick="showMktAgent('${a.pubkey}')">
            <div class="mkt-card-top">
                <div class="mkt-avatar">${(a.name || '?')[0].toUpperCase()}</div>
                <div class="mkt-card-info">
                    <div class="mkt-card-name">${trustBadge}${verifiedBadge} ${mktEscapeHtml(a.name)}</div>
                    ${a.nip05 ? `<div class="mkt-nip05">${mktEscapeHtml(a.nip05)}</div>` : ''}
                    <div class="mkt-card-meta">
                        <span>${a.post_count} posts</span>
                        ${a.last_post_at ? '<span>· ' + mktFormatTime(a.last_post_at) + '</span>' : ''}
                    </div>
                </div>
            </div>
            ${a.about ? `<div class="mkt-card-about">${mktEscapeHtml(a.about)}</div>` : ''}
            <div class="mkt-card-tags">${topTags}</div>
        </div>`;
    }

    html += '</div>';
    container.innerHTML = html;
}

async function showMktAgent(pubkey) {
    const container = document.getElementById('marketplaceContainer');
    container.innerHTML = '<div style="padding:40px;text-align:center;color:var(--text-dim)">Loading...</div>';
    mktState.view = 'detail';
    mktState.agentPk = pubkey;

    try {
        const data = await mktApiGet('/api/marketplace/agent/' + pubkey);
        renderMktDetail(data.agent);
    } catch (err) {
        container.innerHTML = '<div style="padding:40px;text-align:center;color:var(--text-dim)">Failed to load agent</div>';
    }
}

function renderMktDetail(agent) {
    const container = document.getElementById('marketplaceContainer');
    if (!agent) {
        container.innerHTML = '<div style="padding:40px;text-align:center;color:var(--text-dim)">Agent not found</div>';
        return;
    }

    const trust = agent.trust || {};
    const trustHtml = trust.score !== undefined
        ? `<div class="mkt-detail-trust" style="border-left:3px solid ${mktTrustColor(trust.score)}">
            <span class="mkt-trust-level">${trust.level || 'Unknown'}</span>
            <span class="mkt-trust-score">${trust.score}</span>
            <span class="mkt-trust-trusters">${trust.truster_count || 0} trusters</span>
        </div>`
        : '';

    const topTags = (agent.top_tags || [])
        .map(t => `<span class="mkt-detail-tag"><span>#${mktEscapeHtml(t.tag)}</span><small>×${t.count}</small></span>`)
        .join('');

    const posts = (agent.recent_posts || [])
        .map(p => `<div class="mkt-detail-post">
            <div class="mkt-detail-post-time">${mktFormatTime(p.created_at)}</div>
            <div class="mkt-detail-post-content">${mktEscapeHtml(p.content)}</div>
        </div>`)
        .join('');

    const services = (agent.services || [])
        .map(s => `<div class="mkt-detail-service">
            <div class="mkt-svc-name">⚡ ${mktEscapeHtml(s.name)}</div>
            <div class="mkt-svc-desc">${mktEscapeHtml(s.description)}</div>
            <div class="mkt-svc-price">${s.price_sats} sats${s.price_description ? ' — ' + mktEscapeHtml(s.price_description) : ''}</div>
        </div>`)
        .join('');

    let html = `<div class="mkt-detail">
        <button class="mkt-back-btn" onclick="initMarketplace()">← Back to Directory</button>

        <div class="mkt-detail-header">
            <div class="mkt-detail-avatar">${(agent.name || '?')[0].toUpperCase()}</div>
            <div class="mkt-detail-info">
                <h2>${mktEscapeHtml(agent.name)}</h2>
                ${agent.nip05 ? `<div class="mkt-detail-nip05">${mktEscapeHtml(agent.nip05)}</div>` : ''}
                ${agent.website ? `<a class="mkt-detail-web" href="${mktEscapeHtml(agent.website)}" target="_blank" rel="noopener">${mktEscapeHtml(agent.website)}</a>` : ''}
                <div class="mkt-detail-stats">
                    <span>${agent.post_count} posts</span>
                    <span>· ${agent.follower_count || 0} followers</span>
                    <span>· ${agent.following_count || 0} following</span>
                </div>
            </div>
        </div>

        ${trustHtml}

        ${agent.about ? `<div class="mkt-detail-about">${mktEscapeHtml(agent.about)}</div>` : ''}

        ${topTags ? `<div class="mkt-detail-section">
            <h3>Top Topics</h3>
            <div class="mkt-detail-tags">${topTags}</div>
        </div>` : ''}

        ${services.length > 0 ? `<div class="mkt-detail-section">
            <h3>⚡ Service Listings</h3>
            <div class="mkt-detail-services">${services}</div>
        </div>` : `<div class="mkt-detail-section" style="color:var(--text-dim);font-style:italic">
            🏪 No service listings yet. Agents publish kind:30002 events.
        </div>`}

        ${posts ? `<div class="mkt-detail-section">
            <h3>Recent Posts</h3>
            <div class="mkt-detail-posts">${posts}</div>
        </div>` : ''}
    </div>`;

    container.innerHTML = html;
}

window.Marketplace = {
    init: initMarketplace
};
