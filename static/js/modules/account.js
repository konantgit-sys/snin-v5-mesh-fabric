// ══════════════════════════════════════════
// V11.0 — Account Module — Profile editing, NIP-05, settings, themes (extracted from app.js)
// ══════════════════════════════════════════

// ══════════════════════════════════════════
// V7.6 — ACCOUNT / PROFILE EDITING
// ══════════════════════════════════════════

let editMode = false;

async function loadAccount(targetPubkey) {
  const container = document.getElementById('accountContainer');
  const postFeed = document.getElementById('profilePostsFeed');
  const loginPanel = document.getElementById('nsecLoginPanel');
  const profilePanel = document.getElementById('loggedInProfile');
  
  // Determine pubkey: explicit param > URL ?p= > signer
  const urlParams = new URLSearchParams(window.location.search);
  const pubkey = targetPubkey || urlParams.get('p') || signerPubkey;
  
  if (!pubkey) {
    // Show nsec login, hide everything else
    if (loginPanel) loginPanel.style.display = 'block';
    if (profilePanel) profilePanel.style.display = 'none';
    if (container) container.innerHTML = '';
    if (postFeed) postFeed.innerHTML = '';
    return;
  }
  
  // If viewing own profile with nsec — show compact loggedInProfile
  if (pubkey === signerPubkey && signerNsec) {
    if (loginPanel) loginPanel.style.display = 'none';
    if (profilePanel) profilePanel.style.display = 'block';
    loadMyProfile();
    if (container) container.innerHTML = '';
    if (postFeed) {
      postFeed.innerHTML = '<div class="skeleton skeleton-card"></div>';
      loadAccountPosts(pubkey, postFeed);
    }
    return;
  }
  
  // Viewing OTHER profile — hide auth panels, show profile in accountContainer
  if (loginPanel) loginPanel.style.display = 'none';
  if (profilePanel) profilePanel.style.display = 'none';
  
  container.innerHTML = '<div class="skeleton skeleton-card" style="height:360px"></div>';
  
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 5000);
    const resp = await fetch(API + '/profile/' + pubkey, {signal: ctrl.signal});
    clearTimeout(timer);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    
    const hue = hashToHue(pubkey);
    const name = data.display_name || '';
    const about = data.about || '';
    const picture = data.picture || '';
    const website = data.website || '';
    const nip05 = data.nip05 || '';
    const lud16 = data.lud16 || '';
    const banner = data.banner || '';
    const initials = getInitials(name, shortPubkey(pubkey));
    const postCount = data.post_count || 0;
    const followingCount = data.contact_count || 0;
    const followerCount = data.follower_count || 0;
    
    // ─── Profile Card ───
    let html = '<div class="profile-card-v8 glass-panel">';
    
    // Banner — use uploaded banner if available, else gradient
    if (banner) {
      html += '<div class="profile-banner" style="background-image:url(\'' + escapeHtml(banner) + '\');background-size:cover;background-position:center;height:140px"></div>';
    } else {
      html += '<div class="profile-banner" style="background:linear-gradient(135deg,hsl(' + hue + ',70%,25%),hsl(' + (hue+40) + ',60%,15%),hsl(' + hue + ',80%,40%))"></div>';
    }
    
    // Avatar with image
    html += '<div class="profile-avatar-section">';
    html += '<div class="profile-avatar-ring" style="background:conic-gradient(hsl(' + hue + ',80%,55%),hsl(' + (hue+60) + ',80%,50%),hsl(' + hue + ',90%,40%),hsl(' + (hue+30) + ',80%,55%))">';
    if (picture) {
      html += '<img src="' + escapeHtml(picture) + '" class="profile-avatar-img" alt="" onerror="this.style.display=\'none\'">';
    }
    html += '<div class="profile-avatar-fallback" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%))">' + initials + '</div>';
    html += '</div>';
    html += '</div>';
    
    // Name + pubkey + bio
    html += '<div class="profile-info">';
    html += '<div class="profile-name plasma-text">' + (name ? escapeHtml(name) : shortPubkey(pubkey)) + '</div>';
    if (nip05) html += '<div class="profile-nip05"><svg width="14" height="14" viewBox="0 0 24 24" style="vertical-align:middle;margin-right:4px"><circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" stroke-width="2"/><path d="M9 12l2 2 4-4" fill="none" stroke="currentColor" stroke-width="2.5"/></svg>' + escapeHtml(nip05) + '</div>';
    html += '<div class="profile-pk">' + pubkey.slice(0, 16) + '…' + pubkey.slice(-8) + '</div>';
    if (about) html += '<div class="profile-bio">' + escapeHtml(about).replace(/\n/g, '<br>') + '</div>';
    
    // Links
    html += '<div class="profile-links">';
    if (website) html += '<a href="' + escapeHtml(website) + '" target="_blank" class="profile-link-btn"><svg width="14" height="14" viewBox="0 0 24 24" style="margin-right:4px"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" fill="none" stroke="currentColor" stroke-width="2"/><polyline points="15 3 21 3 21 9" fill="none" stroke="currentColor" stroke-width="2"/><line x1="10" y1="14" x2="21" y2="3" fill="none" stroke="currentColor" stroke-width="2"/></svg>Website</a>';
    if (lud16) html += '<span class="profile-link-btn zap" title="Lightning Address: ' + escapeHtml(lud16) + '"><svg width="14" height="14" viewBox="0 0 24 24" style="margin-right:4px;color:#f5a623"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" fill="currentColor"/></svg>' + escapeHtml(lud16) + '</span>';
    html += '<button class="profile-link-btn share" onclick="shareProfile()" title="Share profile"><svg width="14" height="14" viewBox="0 0 24 24" style="margin-right:4px"><circle cx="18" cy="5" r="3" fill="none" stroke="currentColor" stroke-width="2"/><circle cx="6" cy="12" r="3" fill="none" stroke="currentColor" stroke-width="2"/><circle cx="18" cy="19" r="3" fill="none" stroke="currentColor" stroke-width="2"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49" fill="none" stroke="currentColor" stroke-width="2"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49" fill="none" stroke="currentColor" stroke-width="2"/></svg>Share</button>';
    html += '</div>';
    html += '</div>';
    
    // Stats row
    html += '<div class="profile-stats">';
    html += '<div class="profile-stat"><div class="stat-num">' + postCount + '</div><div class="stat-label">Posts</div></div>';
    html += '<div class="profile-stat"><div class="stat-num">' + followingCount + '</div><div class="stat-label">Following</div></div>';
    html += '<div class="profile-stat"><div class="stat-num">' + followerCount + '</div><div class="stat-label">Followers</div></div>';
    html += '</div>';
    
    // Edit button — only for own profile
    if (pubkey === signerPubkey) {
      html += '<div class="profile-actions"><button class="btn-edit-profile-v8" onclick="toggleEditProfileV8()">' + t('editProfile') + '</button></div>';
    }
    html += '</div>'; // end profile-card-v8
    
    // ─── D1 NIP-05 Registration (own profile only) ───
    if (pubkey === signerPubkey) {
      html += '<div class="nip05-section glass-panel" style="margin-top:12px;padding:14px 16px">';
      html += '<div style="font-weight:600;font-size:13px;margin-bottom:6px"><i class="icon icon-lock icon-sm"></i> NIP-05 Verified Identity</div>';
      html += '<div style="font-size:12px;color:var(--text-dim);margin-bottom:8px">Register your <code>@snin-client.v2.site</code> handle</div>';
      html += '<div id="nip05MyList" style="margin-bottom:8px"><span class="skeleton" style="display:inline-block;width:120px;height:14px"></span></div>';
      html += '<div style="display:flex;gap:8px;flex-wrap:wrap">';
      html += '<input id="nip05RegInput" placeholder="your-name" style="flex:1;min-width:120px;padding:6px 10px;border-radius:8px;border:1px solid var(--border-color);background:var(--card-bg);color:var(--text);font-size:12px" maxlength="30" pattern="[a-z0-9._-]+">';
      html += '<button class="btn-primary" onclick="registerNip05()" style="font-size:11px;white-space:nowrap">Register</button>';
      html += '</div>';
      html += '<div id="nip05Status" class="mt6-xs"></div>';
      html += '</div>';
      // Load my NIP-05 names
      setTimeout(loadMyNip05, 300);
    }
    
    // Edit form (hidden) — only for own profile
    if (pubkey === signerPubkey) {
    html += '<div id="profileEditV8" style="display:none;margin:16px">';
    html += '<div class="edit-card">';
    html += '<div class="edit-field"><label>' + t('nameLabel') + '</label><input id="editNameV8" value="' + escapeHtml(name) + '" placeholder="' + t('nameLabel') + '" class="edit-input"></div>';
    html += '<div class="edit-field"><label>' + t('avatarLabel') + '</label><input id="editPictureV8" value="' + escapeHtml(picture) + '" placeholder="https://…" class="edit-input"></div>';
    html += '<div class="edit-field"><label>Banner</label><div style="display:flex;gap:8px"><input id="editBannerV8" value="' + escapeHtml(banner || '') + '" placeholder="https://… or upload" class="edit-input" style="flex:1"><button class="btn-secondary" onclick="uploadBanner()" type="button" style="font-size:11px;white-space:nowrap">Upload</button></div></div>';
    html += '<div class="edit-field"><label>' + t('bioLabel') + '</label><textarea id="editAboutV8" placeholder="' + t('bioLabel') + '…" class="edit-textarea" rows="3">' + escapeHtml(about) + '</textarea></div>';
    html += '<div class="edit-field"><label>' + t('websiteLabel') + '</label><input id="editWebsiteV8" value="' + escapeHtml(website) + '" placeholder="https://…" class="edit-input"></div>';
    html += '<div class="edit-field"><label>' + t('nip05Label') + '</label><input id="editNip05V8" value="' + escapeHtml(nip05) + '" placeholder="name@domain.com" class="edit-input"></div>';
    html += '<div class="edit-field"><label>' + t('lightningLabel') + '</label><input id="editLud16V8" value="' + escapeHtml(lud16) + '" placeholder="name@getalby.com" class="edit-input"></div>';
    html += '<div class="edit-actions"><button class="btn-save-profile" onclick="saveProfileV8()">' + t('save') + '</button><button class="btn-cancel-edit" onclick="toggleEditProfileV8()">' + t('cancel') + '</button></div>';
    html += '</div></div>';
    }
    
    container.innerHTML = html;
    
    // Recent posts
    if (postFeed && data.posts && data.posts.length > 0) {
      let pHtml = '<h3 style="padding:0 16px 8px;font-size:14px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">' + t('yourPosts') + '</h3>';
      data.posts.forEach(p => {
        const content = escapeHtml((p.content || '').slice(0, 200));
        const time = getTimeAgo(p.created_at);
        pHtml += '<div class="feed-card reveal stagger-item" onclick="showThread(\'' + p.id + '\')"><div class="feed-card-body">' + content + '</div><div class="feed-author-id" style="margin-top:4px">' + time + '</div></div>';
      });
      postFeed.innerHTML = pHtml;
    } else if (postFeed) {
      postFeed.innerHTML = '';
    }
    
  } catch (e) {
    container.innerHTML = '<div class="empty-state">' + SVG.warning + '<div class="empty-title">' + t('failedToLoadProfile') + '</div></div>';
    if (postFeed) postFeed.innerHTML = '';
  }
}

async function loadAccountPosts(pubkey, feedEl) {
  try {
    const resp = await fetch(API + '/search?author=' + pubkey + '&limit=20');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    const posts = data.results || [];
    if (posts.length === 0) {
      feedEl.innerHTML = '<div class="empty-state">' + SVG.post + `<div class="empty-title">${t('feedEmpty')}</div></div>`;
      return;
    }
    feedEl.innerHTML = posts.map(p => renderPost(p)).join('');
  } catch (e) {
    feedEl.innerHTML = '<div class="empty-state">' + SVG.warning + `<div class="empty-title">${t('errPosts')}</div></div>`;
  }
}

// ─── D1 NIP-05 Registration ───
async function loadMyNip05() {
  if (!signerPubkey) return;
  const el = document.getElementById('nip05MyList');
  if (!el) return;
  try {
    const resp = await fetch(API + '/nip05/my?pubkey=' + signerPubkey);
    const data = await resp.json();
    if (data.names && data.names.length > 0) {
      el.innerHTML = data.names.map(function(n) {
        return '<span style="display:inline-block;background:var(--accent);color:#08080f;padding:2px 8px;border-radius:6px;font-size:11px;font-weight:600;margin-right:4px;margin-bottom:4px">✅ ' + escapeHtml(n.nip05) + ' <span onclick="unregisterNip05(\'' + escapeHtml(n.name) + '\')" style="cursor:pointer;margin-left:4px;opacity:0.7" title="Remove">×</span></span>';
      }).join('');
    } else {
      el.innerHTML = '<span style="color:var(--text-dim);font-size:11px">No handles registered yet</span>';
    }
  } catch(e) {
    el.innerHTML = '';
  }
}

async function registerNip05() {
  if (!signerPubkey || !signerNsec) {
    showToast(t('signInFirst'), 'error');
    return;
  }
  const inp = document.getElementById('nip05RegInput');
  const status = document.getElementById('nip05Status');
  const name = (inp.value || '').toLowerCase().trim();
  
  if (!name || name.length < 3) {
    if (status) status.innerHTML = '<span style="color:var(--danger)">Name must be at least 3 characters</span>';
    return;
  }
  if (!/^[a-z0-9][a-z0-9._-]*[a-z0-9]$/.test(name)) {
    if (status) status.innerHTML = '<span style="color:var(--danger)">Only lowercase letters, digits, dots, hyphens, underscores</span>';
    return;
  }
  
  if (status) status.innerHTML = '<span style="color:var(--accent)">Signing...</span>';
  
  try {
    // Create a kind:1 event to prove ownership
    var now = Math.floor(Date.now() / 1000);
    var event = {
      pubkey: signerPubkey,
      created_at: now,
      kind: 1,
      tags: [],
      content: 'Register NIP-05: ' + name + '@snin-client.v2.site'
    };
    
    // Get event id
    var eventStr = JSON.stringify([0, event.pubkey, event.created_at, event.kind, event.tags, event.content]);
    event.id = await sha256(eventStr);
    
    // Sign
    var sk = NostrTools ? NostrTools.hexToBytes(signerNsec) : null;
    if (!sk) throw new Error('signing not available');
    var sigBytes = await NostrTools.schnorr.sign(event.id, sk);
    event.sig = NostrTools.bytesToHex(sigBytes);
    
    // Register
    var resp = await fetch(API + '/nip05/register', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pubkey: signerPubkey, name: name, event: event })
    });
    var data = await resp.json();
    
    if (resp.ok) {
      if (status) status.innerHTML = '<span style="color:var(--success)">Registered: ' + escapeHtml(data.nip05) + '</span>';
      inp.value = '';
      loadMyNip05();
    } else {
      if (status) status.innerHTML = '<span style="color:var(--danger)">' + escapeHtml(data.error || t('profileSaveError')) + '</span>';
    }
  } catch(e) {
    if (status) status.innerHTML = '<span style="color:var(--danger)">Error: ' + escapeHtml(e.message) + '</span>';
  }
}

async function unregisterNip05(name) {
  if (!signerPubkey || !signerNsec) return;
  if (!confirm('Remove NIP-05 handle: ' + name + '@snin-client.v2.site ?')) return;
  
  try {
    var now = Math.floor(Date.now() / 1000);
    var event = {
      pubkey: signerPubkey,
      created_at: now,
      kind: 1,
      tags: [],
      content: 'Unregister NIP-05: ' + name + '@snin-client.v2.site'
    };
    var eventStr = JSON.stringify([0, event.pubkey, event.created_at, event.kind, event.tags, event.content]);
    event.id = await sha256(eventStr);
    var sk = NostrTools.hexToBytes(signerNsec);
    var sigBytes = await NostrTools.schnorr.sign(event.id, sk);
    event.sig = NostrTools.bytesToHex(sigBytes);
    
    var resp = await fetch(API + '/nip05/unregister', {
      method: 'DELETE',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pubkey: signerPubkey, name: name, event: event })
    });
    if (resp.ok) loadMyNip05();
  } catch(e) {}
}

async function sha256(str) {
  var buf = new TextEncoder().encode(str);
  var hash = await crypto.subtle.digest('SHA-256', buf);
  return Array.from(new Uint8Array(hash)).map(function(b) { return b.toString(16).padStart(2, '0'); }).join('');
}

// ─── Share profile (V8) ───
function shareProfile() {
  if (!signerPubkey) return;
  const url = 'https://snin-client.v2.site/?p=' + signerPubkey;
  const text = t('checkProfile') + url;
  if (navigator.share) {
    navigator.share({ title: 'SNIN Profile', text: text, url: url }).catch(() => {});
  } else {
    navigator.clipboard.writeText(url).then(() => {
      showToast(t('profileLinkCopied'));
    }).catch(() => {});
  }
}

// ─── Toggle edit profile (V8) ───
function toggleEditProfileV8() {
  const form = document.getElementById('profileEditV8');
  if (!form) return;
  form.style.display = form.style.display === 'none' ? 'block' : 'none';
}

// ─── Save profile (V8) ───
async function saveProfileV8() {
  if (!signerPubkey || !window.nostr) {
    showToast(t('signerRequired'), 'error');
    return;
  }
  const name = document.getElementById('editNameV8')?.value || '';
  const picture = document.getElementById('editPictureV8')?.value || '';
  const about = document.getElementById('editAboutV8')?.value || '';
  const website = document.getElementById('editWebsiteV8')?.value || '';
  const nip05 = document.getElementById('editNip05V8')?.value || '';
  const lud16 = document.getElementById('editLud16V8')?.value || '';
  const banner = document.getElementById('editBannerV8')?.value || '';
  
  const profile = { name, display_name: name, picture, about, website, nip05, lud16, banner };
  const event = {
    kind: 0,
    created_at: Math.floor(Date.now() / 1000),
    tags: [],
    content: JSON.stringify(profile),
    pubkey: signerPubkey
  };
  
  try {
    const signed = await signNostrEvent(event);
    const resp = await fetch(API + '/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(signed)
    });
    if (resp.ok) {
      showToast(t('profileUpdated'));
      toggleEditProfileV8();
      setTimeout(loadAccount, 1500);
    } else {
      showToast(t('postFailed'), 'error');
    }
  } catch (e) {
    showToast(t('nip05SignError') + e.message, 'error');
  }
}

function uploadBanner() {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = 'image/*';
  input.onchange = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    showToast(t('profileBannerLoading'));
    const reader = new FileReader();
    reader.onload = async (ev) => {
      try {
        const resp = await fetch(API + '/profile/banner', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({data: ev.target.result, pubkey: signerPubkey})
        });
        const data = await resp.json();
        if (data.ok) {
          document.getElementById('editBannerV8').value = data.url;
          showToast(t('profileBannerUploaded'));
        } else {
          showToast(t('dmUploadError'));
        }
      } catch (err) {
        showToast(t('dmUploadError'));
      }
    };
    reader.readAsDataURL(file);
  };
  input.click();
}

function toggleEditProfile() {
  editMode = !editMode;
  document.getElementById('profileDisplay').style.display = editMode ? 'none' : '';
  document.getElementById('profileEdit').style.display = editMode ? '' : 'none';
  document.getElementById('btnEditProfile').textContent = editMode ? t('editing') : t('editProfile');
}

async function saveProfile() {
  if (!signerPubkey || !window.nostr) {
    setStatus(t('signerRequired'), 'error');
    return;
  }
  
  const name = document.getElementById('editName').value.trim();
  const about = document.getElementById('editAbout').value.trim();
  const picture = document.getElementById('editPicture').value.trim();
  const website = document.getElementById('editWebsite').value.trim();
  const nip05 = document.getElementById('editNip05').value.trim();
  const lud16 = document.getElementById('editLud16').value.trim();
  
  const profile = { name, about, picture, website, nip05, lud16 };
  // Remove empty fields
  Object.keys(profile).forEach(k => { if (!profile[k]) delete profile[k]; });
  
  const event = {
    kind: 0,
    created_at: Math.floor(Date.now() / 1000),
    tags: [],
    content: JSON.stringify(profile),
    pubkey: signerPubkey
  };
  
  setStatus(t('signingProfile'), 'pending');
  try {
    const signedEvent = await signNostrEvent(event);
    setStatus(t('publishing'), 'pending');
    
    const resp = await fetch(API + '/post', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(signedEvent)
    });
    
    // Also send via WS
    wsSend(JSON.stringify(["EVENT", signedEvent]));
    
    const result = await resp.json();
    if (resp.ok && result.status === 'ok') {
      setStatus(t('profileSaved'), 'success');
      editMode = false;
      setTimeout(loadAccount, 1500);
    } else {
      setStatus('Error: ' + (result.message || 'Unknown'), 'error');
    }
  } catch (e) {
    setStatus('Failed: ' + e.message, 'error');
  }
}

// ─── Settings Tab (V8.19) ───

function loadSettings() {
  const langSel = document.getElementById('settingsLang');
  if (langSel) langSel.value = I18N.lang;

  const themeSel = document.getElementById('settingsTheme');
  if (themeSel) themeSel.value = document.documentElement.classList.contains('light') ? 'light' : 'dark';

  // Restore toggles from localStorage
  const toggles = ['autoplayMedia', 'collapseLong', 'notifyLikes', 'notifyReplies', 'notifyReposts', 'notifyZaps'];
  toggles.forEach(key => {
    const el = document.getElementById('settings' + key.charAt(0).toUpperCase() + key.slice(1));
    if (el) el.checked = localStorage.getItem('snin_' + key) !== '0';
  });

  loadSettingsRelays();
  renderMutedList();
}

function toggleSetting(key) {
  const el = document.getElementById('settings' + key.charAt(0).toUpperCase() + key.slice(1));
  if (!el) return;
  const val = el.checked ? '1' : '0';
  localStorage.setItem('snin_' + key, val);
}

function setTheme(theme) {
  if (theme === 'light') {
    document.documentElement.classList.add('light');
    localStorage.setItem('snin-theme', 'light');
  } else {
    document.documentElement.classList.remove('light');
    localStorage.setItem('snin-theme', 'dark');
  }
  updateThemeIcon(theme === 'light');
}

function loadSettingsRelays() {
  const list = document.getElementById('settingsRelayList');
  if (!list) return;

  const relays = state.relays || [];
  if (relays.length === 0) {
    list.innerHTML = '<div class="settings-empty-relays">' + t('noRelays') + '</div>';
    return;
  }

  let html = '';
  relays.forEach((r, i) => {
    const url = typeof r === 'string' ? r : (r.url || '');
    const host = url.replace(/^wss?:\/\//, '').replace(/\/$/, '');
    const isConnected = state.wsConnected;
    const color = isConnected ? 'var(--accent)' : 'var(--text-dim)';
    html += '<div class="settings-relay-row">' +
      '<div class="relay-dot" style="background:' + color + '"></div>' +
      '<span class="relay-url" title="' + url + '">' + host + '</span>' +
      '<button class="relay-remove-btn" title="' + t('relayRemove') + '" onclick="event.stopPropagation();removeRelay(' + i + ')">' +
        SVG.closeIcon +
      '</button>' +
    '</div>';
  });
  list.innerHTML = html;
}


function addRelayToSettings() {
  const input = document.getElementById('newRelayUrlSettings');
  const url = (input?.value || '').trim();
  if (!url || !url.startsWith('ws')) {
    showToast(t('relayEnterUrl'));
    return;
  }
  if (!state.relays) state.relays = [];
  // Dedup
  if (state.relays.includes(url)) {
    showToast(t('relayDuplicate'));
    return;
  }
  state.relays.push(url);
  localStorage.setItem('snin_relays', JSON.stringify(state.relays));
  input.value = '';
  loadSettingsRelays();
  showToast(t('relayAdded'));
}

// ══════════════════════════════════════════
