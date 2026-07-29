// ══════════════════════════════════════════
// V11.0 — Follow/Unfollow Module (extracted from app.js)
// FOLLOW / UNFOLLOW (kind:3 via NIP-07)
// ══════════════════════════════════════════

// INTERACTIONS
// FOLLOW / UNFOLLOW (kind:3 via NIP-07)
// ══════════════════════════════════════════

let followState = {}; // { pubkey: true/false } — local cache
let myContactEvent = null; // last known kind:3 event

async function toggleFollow(pubkey) {
  if (!signerPubkey) {
    showToast(t('followNeedSignIn'));
    return;
  }
  
  // Don't follow yourself
  if (pubkey === signerPubkey) return;
  
  const isFollowing = followState[pubkey];
  const btn = document.getElementById('followBtn-' + pubkey);
  if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; }
  
  try {
    // Build or update kind:3 event
    let tags = myContactEvent ? [...myContactEvent.tags] : [];
    tags = tags.filter(t => t[0] === 'p' && t[1] !== pubkey); // remove if exists
    tags = tags.filter(t => t[0] !== 'p' || t[1]); // clean
    
    if (!isFollowing) {
      // Add follow
      tags.push(['p', pubkey]);
    }
    // If unfollowing — pubkey already filtered out above
    
    // Add relay hints for all contacts
    tags = tags.map(t => {
      if (t[0] === 'p' && t.length === 2) {
        return ['p', t[1], 'wss://relay.damus.io', 'wss://nos.lol'];
      }
      return t;
    });
    
    const contactEvent = {
      kind: 3,
      created_at: Math.floor(Date.now() / 1000),
      tags: tags,
      content: '',
      pubkey: signerPubkey
    };
    
    const signed = await signNostrEvent(contactEvent);
    
    const resp = await fetch(API + '/post', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(signed)
    });
    
    wsSend(JSON.stringify(["EVENT", signed]));
    
    const result = await resp.json();
    if (resp.ok && result.status === 'ok') {
      followState[pubkey] = !isFollowing;
      myContactEvent = { tags: tags, created_at: contactEvent.created_at };
      // Save to localStorage
      try {
        const cache = JSON.parse(localStorage.getItem('snin_follows') || '{}');
        cache[pubkey] = !isFollowing;
        localStorage.setItem('snin_follows', JSON.stringify(cache));
        localStorage.setItem('snin_contact_event', JSON.stringify(myContactEvent));
      } catch (_) {}
      
      updateFollowButton(pubkey);
      showToast(isFollowing ? t('unfollowed') : t('following'));
    } else {
      showToast(t('postExpandError') + (result.message || t('statusUnknown')));
    }
  } catch (e) {
    showToast(t('followError') + e.message);
  }
  if (btn) { btn.disabled = false; btn.style.opacity = ''; }
}

// Mini follow for feed/discover cards
async function toggleFollowFeed(pubkey, name, el) {
  if (!signerPubkey) {
    showToast(t('subscribeNeedSignIn'));
    return;
  }
  if (pubkey === signerPubkey) return;
  
  await toggleFollow(pubkey);
  // Update mini button
  if (el && followState[pubkey]) {
    el.textContent = '✓';
    el.classList.add('following');
  } else if (el) {
    el.textContent = '+';
    el.classList.remove('following');
  }
}

function updateFollowButton(pubkey) {
  const btn = document.getElementById('followBtn-' + pubkey);
  if (!btn) return;
  const isFollowing = followState[pubkey];
  btn.textContent = isFollowing ? t('following') : t('follow');
  btn.className = 'follow-btn' + (isFollowing ? ' following' : '');
}

function loadFollowCache() {
  try {
    const cache = JSON.parse(localStorage.getItem('snin_follows') || '{}');
    followState = cache;
    myContactEvent = JSON.parse(localStorage.getItem('snin_contact_event') || 'null');
  } catch (_) {}
}
loadFollowCache();


// ══════════════════════════════════════════
