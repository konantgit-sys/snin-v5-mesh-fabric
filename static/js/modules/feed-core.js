// SNIN Client — Feed Core Module
// WebSocket relay, navigation, discover feed, lang switching
// Extracted from app.js (2026-07-20, Phase 2.1)

// ─── WebSocket to Relay (Nostr protocol) ───
let wsReconnectAttempts = 0;
const MAX_RECONNECT_DELAY = 30000;
const WS_HEARTBEAT_INTERVAL = 30000;    // Send ping every 30s
const WS_HEARTBEAT_TIMEOUT = 10000;     // Wait 10s for pong, else reconnect
let wsHeartbeatTimer = null;
let wsHeartbeatTimeoutTimer = null;
let outgoingQueue = [];                  // Events queued while disconnected
const MAX_QUEUE_SIZE = 50;

// ─── Unified send: WS if connected, queue if offline ───
function wsSend(msg) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.send(msg);
    return true;
  }
  // Queue for later delivery
  if (outgoingQueue.length < MAX_QUEUE_SIZE) {
    outgoingQueue.push(msg);
  }
  // Trigger reconnect if not already connecting
  if (!state.ws || (state.ws.readyState !== WebSocket.CONNECTING && state.ws.readyState !== WebSocket.OPEN)) {
    scheduleReconnect();
  }
  return false;
}

// ─── Heartbeat: ping relay, force reconnect if silent ───
function startHeartbeat() {
  stopHeartbeat();
  wsHeartbeatTimer = setInterval(() => {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      // Use a lightweight Nostr REQ as keep-alive ping
      state.ws.send(JSON.stringify(["REQ", "hb", {"kinds":[0],"limit":1}]));
      // Expect any response within timeout; if none, connection is dead
      wsHeartbeatTimeoutTimer = setTimeout(() => {
        console.log('[WS] Heartbeat timeout — force reconnect');
        if (state.ws) { state.ws.close(); state.ws = null; }
        state.wsConnected = false;
        scheduleReconnect();
      }, WS_HEARTBEAT_TIMEOUT);
    }
  }, WS_HEARTBEAT_INTERVAL);
}

function stopHeartbeat() {
  if (wsHeartbeatTimer) { clearInterval(wsHeartbeatTimer); wsHeartbeatTimer = null; }
  if (wsHeartbeatTimeoutTimer) { clearTimeout(wsHeartbeatTimeoutTimer); wsHeartbeatTimeoutTimer = null; }
}

// ─── Flush outgoing queue after reconnect ───
function flushOutgoingQueue() {
  if (outgoingQueue.length === 0) return;
  const batch = outgoingQueue.splice(0, outgoingQueue.length);
  for (const msg of batch) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(msg);
    }
  }
  if (batch.length > 0) {
    console.log('[WS] Flushed ' + batch.length + ' queued events');
  }
}

function connectWS() {
  // Update status from HTTP too (fallback)
  async function pollStatus() {
    try {
      const resp = await fetch(API + '/stats');
      if (!state.wsConnected) updateStatus(resp.ok);
    } catch (_) { if (!state.wsConnected) updateStatus(false); }
  }
  pollStatus();
  if (state._statusInterval) clearInterval(state._statusInterval);
  state._statusInterval = setInterval(pollStatus, 15000);

  try {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) return;

    state.ws = new WebSocket(WS_URL);
    
    state.ws.onopen = () => {
      state.wsConnected = true;
      wsReconnectAttempts = 0;
      stopRestFallback();
      updateStatus(true);
      if (state._disconnectTimer) { clearTimeout(state._disconnectTimer); state._disconnectTimer = null; }
      
      // Send REQ subscription — get recent notes + live updates
      const sub = ["REQ", state.subId, {"kinds": [0,1,1111,39000,30023], "limit": 50}];
      state.ws.send(JSON.stringify(sub));
      // Only flash "синхр…" on first connect, not reconnect
      if (wsReconnectAttempts === 0) updateRelayIndicator('connected');
      
      // Start heartbeat + flush queued events
      startHeartbeat();
      flushOutgoingQueue();
    };

    state.ws.onclose = () => {
      state.wsConnected = false;
      stopHeartbeat();
      // Don't flash "reconnecting" instantly — wait 3s first
      if (!state._disconnectTimer) {
        state._disconnectTimer = setTimeout(() => {
          updateRelayIndicator('disconnected');
        }, 3000);
      }
      // V9.13.0 Phase 6b — If feed has no posts after WS disconnect, force REST reload
      setTimeout(function() {
        var fc = document.getElementById('feedContainer');
        if (fc && state.tab === 'feed' && !fc.querySelector('.post-card, .note-card, .feed-card')) {
          loadFeed();
        }
      }, 5000);
      scheduleReconnect();
      // Start REST fallback after 30s disconnected
      setTimeout(startRestFallback, 30000);
    };

    state.ws.onerror = () => {
      state.wsConnected = false;
      updateRelayIndicator('error');
    };

    state.ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        
        if (msg[0] === 'EVENT') {
          const event = msg[2];
          const subMatch = msg[1];
          event.is_ai = isAIEvent(event);
          
          if (state.tab === 'feed' && (!state.aiOnly || event.is_ai)) {
            prependPost(event);
            state.newEventsCount++;
            updateNewEventsBadge();
          }
        } else if (msg[0] === 'EOSE') {
          // End of stored events — live mode active
          updateRelayIndicator('live');
        } else if (msg[0] === 'NOTICE') {
          console.log('Relay notice:', msg[1]);
        } else if (msg[0] === 'OK') {
          // Event accepted by relay
          setStatus(t('postedToRelay'), 'success');
        }
      } catch (_) {}
    };
  } catch (_) {}
}

function scheduleReconnect() {
  if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
  const delay = Math.min(1000 * Math.pow(2, wsReconnectAttempts), MAX_RECONNECT_DELAY);
  wsReconnectAttempts++;
  state.reconnectTimer = setTimeout(connectWS, delay);
}



// ─── REST Fallback Polling ───
let restPollTimer = null;

function startRestFallback() {
  if (restPollTimer) return;
  updateRelayIndicator('rest');
  restPollTimer = setInterval(async () => {
    try {
      const resp = await fetch(API + '/posts?limit=5');
      const data = await resp.json();
      if (data.events && data.events.length > 0) {
        const container = document.getElementById('feedContainer');
        if (container) {
          // Only insert if no identical events exist (avoid duplicates with WS reconnect)
          for (const event of data.events) {
            const existing = container.querySelector(`[data-event-id="${event.id}"]`);
            if (!existing && (!state.aiOnly || event.is_ai)) {
              prependPost(event);
            }
          }
        }
      }
    } catch (_) {}
  }, 60000); // Poll every 60s when WS is down
}

function stopRestFallback() {
  if (restPollTimer) {
    clearInterval(restPollTimer);
    restPollTimer = null;
  }
}

// Patch connectWS to start/stop REST fallback

function updateRelayIndicator(status) {
  updateStatus(status === 'connected' || status === 'live' || status === 'rest');
  const dot = document.getElementById('relayDot');
  const label = document.getElementById('relayLabel');
  // REST fallback indicator
  if (status === 'rest') {
    if (dot) dot.style.background = '#f59e0b';
    if (label) label.textContent = 'REST polling';
    return;
  }
  if (!dot || !label) return;
  const states = {
    'connected': { cls: 'status-dot pulse', text: t('statusSyncing') },
    'live': { cls: 'status-dot live', text: t('statusOnline') },
    'disconnected': { cls: 'status-dot offline', text: t('statusReconnecting') },
    'error': { cls: 'status-dot offline', text: t('statusError') }
  };
  const s = states[status] || states['disconnected'];
  dot.className = s.cls;
  label.textContent = s.text;
}

function updateNewEventsBadge() {
  const badge = document.getElementById('newEventsBadge');
  if (!badge) return;
  if (state.newEventsCount > 0 && state.tab !== 'feed') {
    badge.textContent = state.newEventsCount > 99 ? '99+' : state.newEventsCount;
    badge.style.display = 'flex';
  } else {
    badge.style.display = 'none';
  }
}

function resetNewEventsBadge() {
  state.newEventsCount = 0;
  updateNewEventsBadge();
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
    /* V8.41: stagger CSS handles entrance animation */
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
  }
);
}

// V12.3 — Animated nav indicator pill
function moveNavIndicator(activeBtn) {
  var indicator = document.getElementById('navIndicator');
  var navScroll = document.getElementById('navScroll');
  if (!indicator || !navScroll) return;
  if (!activeBtn) { indicator.classList.remove('visible'); return; }
  
  var btnRect = activeBtn.getBoundingClientRect();
  var navRect = navScroll.getBoundingClientRect();
  var left = btnRect.left - navRect.left + navScroll.scrollLeft;
  indicator.style.left = left + 'px';
  indicator.style.width = btnRect.width + 'px';
  indicator.classList.add('visible');
}
// Init indicator on page load
window.addEventListener('load', function() {
  setTimeout(function() {
    var activeBtn = document.querySelector('.nav-item.active');
    if (activeBtn) moveNavIndicator(activeBtn);
  }, 200);
});
// Update on resize
window.addEventListener('resize', function() {
  var activeBtn = document.querySelector('.nav-item.active');
  if (activeBtn) moveNavIndicator(activeBtn);
});

function switchTab(tabName) {
  // Clear relay auto-refresh if leaving relay tab
  if (relayInterval && tabName !== 'relays') { clearInterval(relayInterval); relayInterval = null; }
  // V12.1 — Smooth tab-out with design-rules easing
  const oldTab = document.querySelector('.tab-content.active');
  if (oldTab) {
    oldTab.style.transition = 'transform 180ms var(--ease-out), opacity 150ms var(--ease-out)';
    oldTab.style.transform = 'translateY(-6px)';
    oldTab.style.opacity = '0';
  }
  
  document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
  const navBtn = document.querySelector(`.nav-item[data-tab="${tabName}"]`);
  if (navBtn) navBtn.classList.add('active');
  state.tab = tabName;
  
  // V12.3 — Move nav indicator pill
  moveNavIndicator(navBtn);
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  const tabEl = document.getElementById('tab-' + tabName);
  if (tabEl) { tabEl.classList.add('active'); }
  document.getElementById('toggleBar').style.display = (tabName === 'feed') ? '' : 'none';
  document.getElementById('searchBar').style.display = 'none';
  
  // V12.1 — Spring-in with proper easing
  setTimeout(function() {
    if (tabEl) {
      tabEl.style.transform = 'translateY(8px)';
      tabEl.style.opacity = '0';
      requestAnimationFrame(function() {
        tabEl.style.transition = 'transform 250ms var(--ease-out), opacity 180ms var(--ease-out)';
        tabEl.style.transform = 'translateY(0)';
        tabEl.style.opacity = '1';
      });
    }
  }, 50);
  
  if (tabName === 'feed') { loadFeed(); resetNewEventsBadge(); }
  if (tabName === 'search') initSearchTab();
  if (tabName === 'discover') loadDiscover();
  if (tabName === 'notifications') loadNotificationsTab();
  if (tabName === 'messages') loadDMChats();
  if (tabName === 'bookmarks') renderBookmarksFeed();
  if (tabName === 'account') loadAccount();
  if (tabName === 'agents') loadAgents();
  if (tabName === 'stats') loadStats();
  if (tabName === 'node') loadNode();
  if (tabName === 'tie') loadTIE();
  if (tabName === 'compose') { checkComposeAuth(); }
  if (tabName === 'dao') loadDao();
  if (tabName === 'settings') loadSettings();
  if (tabName === 'lists') loadLists();
  if (tabName === 'communities') loadCommunities();
  if (tabName === 'badges') loadBadges();
  if (tabName === 'graph') initGraphTab();
  if (tabName === 'topics') initContentTab();
  if (tabName === 'highlights') loadHighlights();
  if (tabName === 'analytics') loadAnalytics();
  if (tabName === 'relays') { loadRelays(); relayInterval = setInterval(loadRelays, 30000); }
  if (tabName === 'wallet') loadWallet();
  if (tabName === 'apps') loadApps();
  if (tabName === 'calendar') loadCalendar();
  if (tabName === 'trust') loadTrust();
  if (tabName === 'zk') loadZK();
}

// V8.41 — Stagger activation helper
function activateStagger(container) {
  if (!container || container.classList.contains('stagger-active')) return;
  // Re-observe dynamic content
  if (window.DesignForge && window.DesignForge.initStagger) {
    window.DesignForge.initStagger({ selector: '.stagger-fast, .stagger-container' });
  }
  setTimeout(() => { container.classList.add('stagger-active'); }, 10);
}

// ─── Discover Feed (V7.11) ───

async function loadDiscover() {
  const feed = document.getElementById('discoverFeed');
  if (!feed) return;
  
  feed.innerHTML = '<div class="skeleton skeleton-card"></div><div class="skeleton skeleton-card"></div><div class="skeleton skeleton-card"></div>';
  
  try {
    const r = await fetch('/api/discover?limit=30');
    const data = await r.json();
    const events = data.events || [];
    
    if (events.length === 0) {
      feed.innerHTML = '<div class="empty-state"><div class="icon-md">🌍</div><div class="empty-title">' + t('feedEmpty') + '</div><div class="text-xs-dim">' + t('feedEmptyGlobal') + '</div></div>';
      return;
    }
    
    let html = '<div class="feed-list">';
    events.forEach(ev => {
      const content = escapeHtml(ev.content || '');
      const authorName = escapeHtml(ev.author || 'unknown');
      const shortId = (ev.id || '').slice(0, 12);
      const timeAgo = getTimeAgo(ev.created_at);
      const authorPic = ev.author_picture || '';
      const hue = hashToHue(ev.pubkey);
      const initials = getInitials(authorName, ((ev.pubkey || '??')||'').slice(0,2));
      
      // Avatar HTML
      let avatarHTML;
      if (authorPic) {
        avatarHTML = '<div class="feed-avatar avatar-img" style="width:32px;height:32px;border-radius:50%;overflow:hidden;flex-shrink:0" onclick="event.stopPropagation();showProfile(\'' + ev.pubkey + '\')">' +
          '<img src="' + escapeHtml(authorPic) + '" alt="" loading="lazy" style="width:100%;height:100%;object-fit:cover" onerror="this.parentElement.classList.add(\'avatar-broken\')">' +
        '</div>';
      } else {
        avatarHTML = '<div class="feed-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%));color:#fff;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;flex-shrink:0" onclick="event.stopPropagation();showProfile(\'' + ev.pubkey + '\')">' + initials + '</div>';
      }
      
      html += '<div class="feed-card reveal stagger-item" onclick="showThread(\'' + ev.id + '\')">' +
        '<div class="feed-card-header">' +
          avatarHTML +
          '<div class="feed-author-info">' +
            '<div class="feed-author-name" onclick="event.stopPropagation();showProfile(\'' + ev.pubkey + '\')">' + authorName + '</div>' +
            '<div class="feed-author-id">' + shortId + ' · ' + timeAgo + '</div>' +
          '</div>' +
          '<button class="btn-follow-mini" onclick="event.stopPropagation();toggleFollowFeed(\'' + ev.pubkey + '\',\'' + escapeHtml(authorName) + '\',this)" title="' + t('follow') + '">+</button>' +
        '</div>' +
        '<div class="feed-card-body">' + content + '</div>' +
      '</div>';
    });
    html += '</div>';
    feed.innerHTML = html;
    activateStagger(feed);
    // V8.37: Batch verify NIP-05 for all authors
    const pubkeys = [...new Set(events.map(ev => ev.pubkey))];
    batchVerifyNip05(pubkeys);
  } catch (e) {
    feed.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">' + t('feedError') + '</div></div>';
  }
}

// V8.34 — Language Feed
let currentLangFeed = 'all';

// V8.37 — NIP-05 verification cache: pubkey → true/false
let nip05Verified = {};

async function switchLangFeed(lang) {
  currentLangFeed = lang;
  
  // Update toggle buttons
  document.querySelectorAll('#langToggleBar .lang-chip').forEach(b => {
    b.classList.toggle('active', b.dataset.lang === lang);
  });
  
  const feed = document.getElementById('discoverFeed');
  feed.innerHTML = '<div class="skeleton skeleton-card"></div><div class="skeleton skeleton-card"></div><div class="skeleton skeleton-card"></div>';
  
  if (lang === 'all') {
    loadDiscover();
    return;
  }
  
  // V8.36: Trending
  if (lang === 'trending') {
    loadTrending();
    return;
  }
  
  try {
    const r = await fetch('/api/feed/lang?lang=' + lang + '&limit=30');
    const data = await r.json();
    const events = data.posts || [];
    
    if (events.length === 0) {
      feed.innerHTML = '<div class="empty-state">' + SVG.globe + '' +
        '<div class="empty-title">No ' + lang.toUpperCase() + ' posts</div>' +
        '<div class="text-xs-dim">No posts detected in this language</div></div>';
      return;
    }
    
    let html = '<div class="feed-list">';
    events.forEach(ev => {
      const content = escapeHtml(ev.content || '');
      const authorName = escapeHtml(ev.author_name || 'unknown');
      const shortId = (ev.id || '').slice(0, 12);
      const timeAgo = getTimeAgo(ev.created_at);
      const hue = hashToHue(ev.pubkey);
      const initials = getInitials(authorName, ((ev.pubkey || '??')||'').slice(0,2));
      
      html += '<div class="feed-card reveal stagger-item" onclick="showThread(\'' + ev.id + '\')">' +
        '<div class="feed-card-header">' +
          '<div class="feed-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%));color:#fff;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;flex-shrink:0" onclick="event.stopPropagation();showProfile(\'' + ev.pubkey + '\')">' + initials + '</div>' +
          '<div class="feed-author-info">' +
            '<div class="feed-author-name" onclick="event.stopPropagation();showProfile(\'' + ev.pubkey + '\')">' + authorName + '</div>' +
            '<div class="feed-author-id">' + shortId + ' · ' + timeAgo + '</div>' +
          '</div>' +
        '</div>' +
        '<div class="feed-card-body">' + content + '</div>' +
      '</div>';
    });
    html += '</div>';
    feed.innerHTML = html;
    activateStagger(feed);
    // V8.37: Batch verify NIP-05
    const pubkeys = [...new Set(events.map(ev => ev.pubkey))];
    batchVerifyNip05(pubkeys);
  } catch (e) {
    feed.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">' + t('feedError') + '</div></div>';
  }
}

// ─── Toggle ───
