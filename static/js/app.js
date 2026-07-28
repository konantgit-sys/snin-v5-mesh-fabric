// ═══════════════════════════════════════════════
// SNIN Client v6.0 — Phase 2 (Profiles, Threads, Search, Reactions)
// ═══════════════════════════════════════════════

// V8.67 — I18N guard: if i18n.js failed, provide fallback
if (typeof I18N === 'undefined') {
  console.warn('[SNIN] I18N not loaded, using fallback (EN only)');
  window.I18N = {
    lang: 'en',
    t: function(k,f) { return f || k; },
    setLang: function(){},
    updateDOM: function(){}
  };
  window.t = function(k,f) { return f || k; };
}

const API = '/api';
const WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws';

// Shared state moved to modules/state.js

// ═══ V8.59 B1 — Multi-Account Nostr ═══

function addAccount(pubkey, nsecHex, name, picture) {
  const accounts = getAccounts();
  // Check if already exists
  const existing = accounts.find(a => a.pubkey === pubkey);
  if (existing) {
    // Update existing
    existing.nsec_hex = nsecHex;
    existing.name = name || existing.name;
    existing.picture = picture || existing.picture;
    existing.last_active = Date.now();
    saveAccounts(accounts);
    return existing.id;
  }
  const id = 'acct_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
  const account = {
    id: id,
    pubkey: pubkey,
    nsec_hex: nsecHex,
    name: name || shortPubkey(pubkey),
    picture: picture || '',
    added_at: Date.now(),
    last_active: Date.now()
  };
  accounts.push(account);
  saveAccounts(accounts);
  return id;
}

function removeAccount(id) {
  let accounts = getAccounts();
  const removed = accounts.find(a => a.id === id);
  if (!removed) return false;
  accounts = accounts.filter(a => a.id !== id);
  saveAccounts(accounts);
  // If removed active account, switch to another
  if (getActiveAccountId() === id) {
    if (accounts.length > 0) {
      switchToAccount(accounts[0].id);
    } else {
      // No accounts left — logout
      localStorage.removeItem(ACTIVE_ACCOUNT_KEY);
      sessionStorage.removeItem('snin_nsec');
      sessionStorage.removeItem('snin_pubkey');
      signerPubkey = null;
      signerNsec = null;
    }
  }
  return true;
}

function switchToAccount(id) {
  const account = getAccountById(id);
  if (!account) return false;
  
  // Update last_active
  const accounts = getAccounts();
  const a = accounts.find(x => x.id === id);
  if (a) { a.last_active = Date.now(); saveAccounts(accounts); }
  
  setActiveAccountId(id);
  sessionStorage.setItem('snin_nsec', account.nsec_hex);
  sessionStorage.setItem('snin_pubkey', account.pubkey);
  signerPubkey = account.pubkey;
  signerNsec = account.nsec_hex;
  
  // Refresh UI
  updateAccountIndicator();
  if (state.tab === 'account') loadAccount();
  if (state.tab === 'feed') loadFeed();
  if (state.tab === 'bookmarks') renderBookmarksFeed();
  
  showToast(t('tabSwitched') + account.name, 'info');
  return true;
}

function renameAccount(id, newName) {
  const accounts = getAccounts();
  const a = accounts.find(x => x.id === id);
  if (!a) return false;
  a.name = newName;
  saveAccounts(accounts);
  updateAccountIndicator();
  return true;
}

// Migrate old single-account to multi-account system
// Account indicator in header
function updateAccountIndicator() {
  const indicator = document.getElementById('accountIndicator');
  if (!indicator) return;
  
  const account = getActiveAccount();
  if (account) {
    const initials = getInitials(account.name, shortPubkey(account.pubkey));
    indicator.innerHTML = 
      '<div class="acct-indicator-avatar" style="background:linear-gradient(135deg,hsl(' + hashToHue(account.pubkey) + ',70%,40%),hsl(' + (hashToHue(account.pubkey)+40) + ',60%,30%));width:24px;height:24px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#fff;cursor:pointer" onclick="showAccountSwitcher()" title="' + account.name + '">' +
        initials +
      '</div>' +
      '<span style="font-size:11px;color:var(--text-dim);max-width:60px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;cursor:pointer" onclick="showAccountSwitcher()">' + escapeHtml(account.name) + '</span>';
    indicator.style.display = 'flex';
  } else {
    indicator.innerHTML = '';
    indicator.style.display = 'none';
  }
}

// Account switcher dropdown
function showAccountSwitcher() {
  const existing = document.getElementById('acctSwitcherOverlay');
  if (existing) { existing.remove(); return; }
  
  const accounts = getAccounts();
  const activeId = getActiveAccountId();
  
  let listHtml = '';
  accounts.forEach((a, i) => {
    const initials = getInitials(a.name, shortPubkey(a.pubkey));
    const isActive = a.id === activeId;
    listHtml += '<div class="acct-switcher-item' + (isActive ? ' active' : '') + '" onclick="switchToAccount(\'' + a.id + '\');document.getElementById(\'acctSwitcherOverlay\').remove()" style="padding:10px 14px;cursor:pointer;display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--border)">' +
      '<div class="avatar-circle-sm" style="background:linear-gradient(135deg,hsl(' + hashToHue(a.pubkey) + ',70%,50%),hsl(' + (hashToHue(a.pubkey)+40) + ',60%,35%));display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:#fff;flex-shrink:0">' + initials + '</div>' +
      '<div style="flex:1;min-width:0">' +
        '<div style="font-size:13px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + escapeHtml(a.name) + (isActive ? ' ✓' : '') + '</div>' +
        '<div style="font-size:10px;color:var(--text-dim);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + shortPubkey(a.pubkey) + '</div>' +
      '</div>' +
      '<button onclick="event.stopPropagation();removeAccount(\'' + a.id + '\');document.getElementById(\'acctSwitcherOverlay\').remove();showAccountSwitcher()" style="background:none;border:none;color:#ff6b6b;cursor:pointer;font-size:18px;padding:4px 8px" title="Remove account">×</button>' +
    '</div>';
  });
  
  listHtml += '<div class="acct-switcher-item" onclick="switchTab(\'account\');document.getElementById(\'acctSwitcherOverlay\').remove();setTimeout(function(){document.getElementById(\'nsecInput\').focus()},300)" style="padding:10px 14px;cursor:pointer;text-align:center;color:var(--accent);font-weight:600;font-size:13px">+ Add Account</div>';
  
  const overlay = document.createElement('div');
  overlay.id = 'acctSwitcherOverlay';
  overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;z-index:9999;background:rgba(0,0,0,0.6);backdrop-filter:blur(4px);display:flex;align-items:flex-start;justify-content:flex-end;padding:52px 12px 0';
  overlay.onclick = function(e) { if (e.target === overlay) overlay.remove(); };
  
  overlay.innerHTML = '<div class="acct-switcher-dropdown glass-panel" style="width:260px;max-height:400px;overflow-y:auto;border-radius:14px;border:1px solid var(--border);background:var(--glass-bg);backdrop-filter:blur(20px)" onclick="event.stopPropagation()">' +
    '<div style="padding:8px 14px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid var(--border)">Accounts (' + accounts.length + ')</div>' +
    listHtml +
    '</div>';
  
  document.body.appendChild(overlay);
}

// Init: migrate old accounts and set up indicator
document.addEventListener('DOMContentLoaded', function() {
  migrateAccounts();
  // Restore active account
  const active = getActiveAccount();
  if (active) {
    signerPubkey = active.pubkey;
    signerNsec = active.nsec_hex;
    sessionStorage.setItem('snin_nsec', active.nsec_hex);
    sessionStorage.setItem('snin_pubkey', active.pubkey);
  }
});

// ═══ END B1 Multi-Account ═══

// ─── Init ───
// Mobile detection + body class
(function() {
  const isMobile = window.innerWidth < 640;
  if (isMobile) document.documentElement.classList.add('is-mobile');
  // Update on resize
  window.addEventListener('resize', function() {
    document.documentElement.classList.toggle('is-mobile', window.innerWidth < 640);
  });
})();
// feedMode moved to modules/state.js

document.addEventListener('DOMContentLoaded', () => {
  // V9.13.0 — Wrap every call in try-catch so one failure doesn't block the rest
  function safe(fn, name) {
    try { fn(); } catch(e) { console.error('[SNIN] Init error in ' + name + ':', e.message); }
  }
  safe(() => I18N.updateDOM(), 'I18N.updateDOM');
  safe(() => initTheme(), 'initTheme');
  
  // Mobile detection — skip heavy canvas animations
  const isMobile = window.innerWidth < 640 || ('ontouchstart' in window && window.innerWidth < 1024);
  if (!isMobile) {
    safe(() => injectAuroraOrb(), 'injectAuroraOrb');
    safe(() => injectGrid(), 'injectGrid');
    safe(() => initCanvasBG(), 'initCanvasBG');
    safe(() => injectStarField(), 'injectStarField');
    safe(() => injectCyberEffects(), 'injectCyberEffects');
  }
  
  safe(() => setupNav(), 'setupNav');
  safe(() => setupToggle(), 'setupToggle');
  safe(() => loadHashtagSubs(), 'loadHashtagSubs');
  safe(() => updateHashtagTabVisibility(), 'updateHashtagTabVisibility');
  safe(() => setupComposer(), 'setupComposer');
  safe(() => addRippleToButtons(), 'addRippleToButtons');
  safe(() => setupClickBurst(), 'setupClickBurst');
  safe(() => setupMagneticTilt(), 'setupMagneticTilt');
  safe(() => setupProfileClickFallback(), 'setupProfileClickFallback');
  safe(() => loadStats(), 'loadStats');
  safe(() => loadFeed(), 'loadFeed');
  // V9.13.0 — Multi-stage fallback: 1s, 3s, 5s
  [1000, 3000, 5000].forEach(function(ms) {
    setTimeout(function() {
      var fc = document.getElementById('feedContainer');
      if (fc && fc.querySelector('.skeleton-card') && !fc.querySelector('.post-card, .note-card, .feed-card')) {
        console.warn('[SNIN] Feed stuck after ' + ms + 'ms — retrying REST load');
        loadFeed();
      }
    }, ms);
  });
  safe(() => checkSigner(), 'checkSigner');
  safe(() => connectWS(), 'connectWS');
  safe(() => initPullToRefresh(), 'initPullToRefresh');
  
  // V8.60 B2 — Design Forge integration
  if (window.DesignForge) {
    window.DesignForge.initScrollReveal({ threshold: 0.05, rootMargin: '0px 0px -20px 0px' });
    console.log('[DF] ScrollReveal active');
  }
  
  setInterval(loadStats, 30000);
  // Start notification polling — badge + panel
  loadNotifications();
  setInterval(loadNotifications, 30000);
});

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
    // Only set overflow:hidden on non-nav items (breaks flex layout)
    if (!btn.classList.contains('nav-item')) {
      btn.style.overflow = 'hidden';
    }
    btn.appendChild(ripple);
    ripple.addEventListener('animationend', () => ripple.remove());
  });
}


// ─── NIP-07 ───
async function checkSigner() {
  const postBtn = document.getElementById('postBtn');
  
  // First check: restore nsec from sessionStorage
  const savedNsec = sessionStorage.getItem('snin_nsec');
  const savedPubkey = sessionStorage.getItem('snin_pubkey');
  if (savedNsec && savedPubkey) {
    signerNsec = savedNsec;
    signerPubkey = savedPubkey;
    
    // V8.59 B1 — Ensure account is in multi-account list + update indicator
    const accounts = getAccounts();
    const existing = accounts.find(a => a.pubkey === savedPubkey);
    if (existing) {
      setActiveAccountId(existing.id);
      updateAccountIndicator();
    } else {
      const id = addAccount(savedPubkey, savedNsec, '', '');
      setActiveAccountId(id);
      updateAccountIndicator();
    }
    
    document.getElementById('nsecLoginPanel').style.display = 'none';
    document.getElementById('loggedInProfile').style.display = 'block';
    // Show pubkey immediately
    document.getElementById('profileNpub').textContent = signerPubkey.slice(0, 16) + '…';
    document.getElementById('profileName').textContent = 'Loading…';
    const hue = hashToHue(signerPubkey);
    document.getElementById('profileAvatar').innerHTML = '<div class="avatar-circle-md" style="background:linear-gradient(135deg,hsl('+hue+',80%,50%),hsl('+(hue+35)+',80%,30%));display:flex;align-items:center;justify-content:center;font-size:24px;color:#fff;font-weight:700">?</div>';
    document.getElementById('profileBio').textContent = 'Loading…';
    document.getElementById('profilePostsCount').textContent = '-';
    document.getElementById('profileFollowing').textContent = '-';
    document.getElementById('profileFollowers').textContent = '-';
    if (postBtn) {
      postBtn.innerHTML = SVG.send + ' Post';
      postBtn.disabled = false;
      postBtn.style.background = 'linear-gradient(135deg, var(--accent), #b08060)';
      postBtn.style.color = '#fff';
    }
    loadMyProfile();
    // Check if onboarding needed (first-time user)
    setTimeout(checkOnboarding, 1500);
    return;
  }
  
  // Second check: NIP-07 extension — show button
  const nip07Section = document.getElementById('nip07LoginSection');
  if (window.nostr) {
    if (nip07Section) nip07Section.style.display = 'block';
    // Auto-login only if user hasn't explicitly logged out
    if (!sessionStorage.getItem('snin_logged_out_ext')) {
      try { await loginWithExtension(); return; } catch(e) {}
    }
  } else {
    if (nip07Section) nip07Section.style.display = 'none';
  }
  
  if (postBtn) {
    postBtn.innerHTML = SVG.lock + ' Sign in to post';
    postBtn.disabled = true;
  }
}

// NIP-07 extension login — explicit call
async function loginWithExtension() {
  const postBtn = document.getElementById('postBtn');
  if (!window.nostr) {
    alert('No NIP-07 extension detected. Install Alby, nos2x, or Flamingo.');
    return;
  }
  try {
    signerPubkey = await window.nostr.getPublicKey();
    signerNsec = null;
    sessionStorage.removeItem('snin_logged_out_ext');
    sessionStorage.setItem('snin_pubkey', signerPubkey);
    
    document.getElementById('nsecLoginPanel').style.display = 'none';
    document.getElementById('loggedInProfile').style.display = 'block';
    document.getElementById('profileNpub').textContent = signerPubkey.slice(0, 16) + '…';
    document.getElementById('profileName').textContent = 'Loading…';
    const hue = hashToHue(signerPubkey);
    document.getElementById('profileAvatar').innerHTML = '<div class="avatar-circle-md" style="background:linear-gradient(135deg,hsl('+hue+',80%,50%),hsl('+(hue+35)+',80%,30%));display:flex;align-items:center;justify-content:center;font-size:24px;color:#fff;font-weight:700">?</div>';
    document.getElementById('profileBio').textContent = 'Loading…';
    document.getElementById('profilePostsCount').textContent = '-';
    document.getElementById('profileFollowing').textContent = '-';
    document.getElementById('profileFollowers').textContent = '-';
    if (postBtn) {
      postBtn.innerHTML = SVG.send + ' Post';
      postBtn.disabled = false;
      postBtn.style.background = 'linear-gradient(135deg, var(--accent), #b08060)';
      postBtn.style.color = '#fff';
    }
    loadMyProfile();
    setTimeout(checkOnboarding, 1500);
  } catch(e) {
    throw e;
  }
}




const _origShowTab = showTab;
showTab = function(tabId) {
  _origShowTab(tabId);
  if (tabId === 'calendar' && signerPubkey) loadCalendar();
  if (tabId === 'apps') loadApps();
};

async function loadApps() {
  const grid = document.getElementById('appsGrid');
  grid.innerHTML = '<p class="empty-dim">Loading apps...</p>';
  
  try {
    const resp = await fetch(API + '/apps');
    const data = await resp.json();
    
    if (!data.apps || data.apps.length === 0) {
      grid.innerHTML = '<p class="empty-dim">No apps discovered yet. NIP-89 events from other apps will appear here.</p>';
      return;
    }
    
    grid.innerHTML = data.apps.filter(a => a.name).slice(0, 20).map(app => {
      const kinds = app.handles.slice(0, 5).map(k => '<span style="display:inline-block;padding:2px 8px;border-radius:10px;background:rgba(var(--accent-rgb),0.1);color:var(--accent);font-size:11px;margin:2px">kind:' + k + '</span>').join('');
      const shortPub = app.pubkey.slice(0, 8) + '…';
      
      return '<div class="app-card" style="padding:14px;border-radius:10px;background:var(--glass-bg);border:1px solid var(--border)">' +
        '<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">' +
          (app.icon ? '<img src="' + escapeAttr(app.icon) + '" width="36" height="36" style="border-radius:8px" onerror="this.style.display=\'none\'">' : '') +
          '<div>' +
            '<div style="font-weight:700;font-size:14px">' + escapeHtml(app.name) + '</div>' +
            '<div style="font-size:11px;color:var(--text-dim);font-family:monospace">' + shortPub + '</div>' +
          '</div>' +
        '</div>' +
        (app.description ? '<p style="font-size:12px;color:var(--text-dim);margin:0 0 8px">' + escapeHtml(app.description.slice(0, 100)) + '</p>' : '') +
        '<div style="font-size:11px">' + kinds + '</div>' +
        (app.url ? '<a href="' + escapeAttr(app.url) + '" target="_blank" rel="noopener" style="display:inline-block;margin-top:6px;font-size:12px;color:var(--accent)">Visit →</a>' : '') +
      '</div>';
    }).join('');
  } catch (e) {
    grid.innerHTML = '<p class="text-dim">' + t('errApps') + e.message + '</p>';
  }
}


// ══════════════════════════════════════════
// PHASE 7 — WALLET UI (NIP-47 NWC)
// ══════════════════════════════════════════

// Hook into showTab
const _origShowTab2 = showTab;
showTab = function(tabId) {
  _origShowTab2(tabId);
  if (tabId === 'apps') loadApps();
  if (tabId === 'wallet' && signerPubkey) loadWallet();
  if (tabId === 'marketplace' && window.Marketplace) window.Marketplace.init();
};

