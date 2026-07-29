// ══════════════════════════════════════════
// V11.0 — Interactions Module — Like, Repost, Quote (extracted from app.js)
// ══════════════════════════════════════════

// ══════════════════════════════════════════

let likedPosts = {}; // Track which posts user has liked

async function likePost(eventId, authorPubkey, btn) {
  if (!signerPubkey) {
    showToast(t('postLoginToLike'));
    return;
  }
  
  // Toggle like
  const wasLiked = likedPosts[eventId];
  
  if (wasLiked) {
    likedPosts[eventId] = false;
    btn.classList.remove('liked');
    const c = btn.querySelector('.count');
    if (c) {
      const n = parseInt(c.textContent) - 1;
      if (n <= 0) c.remove();
      else c.textContent = n;
    }
    return;
  }
  
  // Create kind:7 reaction event
  const reactionEvent = {
    kind: 7,
    created_at: Math.floor(Date.now() / 1000),
    tags: [
      ['e', eventId],
      ['p', authorPubkey]
    ],
    content: '+',
    pubkey: signerPubkey
  };
  
  btn.style.opacity = '0.5';
  try {
    const signedEvent = await signNostrEvent(reactionEvent);
    
    // Publish via REST
    const resp = await fetch(API + '/post', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(signedEvent)
    });
    
    // Also publish via WS
    wsSend(JSON.stringify(["EVENT", signedEvent]));
    
    const result = await resp.json();
    if (resp.ok && result.status === 'ok') {
      likedPosts[eventId] = true;
      btn.classList.add('liked');
      const countEl = btn.querySelector('.count');
      if (countEl) {
        countEl.textContent = parseInt(countEl.textContent) + 1;
      } else {
        btn.innerHTML = SVG.heart + '<span class="count">1</span>';
      }
    }
  } catch (e) {
    console.error(t('likeFailed')+': ', e);
    // Fallback: visual only
    likedPosts[eventId] = true;
    btn.classList.add('liked');
    const countEl = btn.querySelector('.count');
    if (!countEl) btn.innerHTML = SVG.heart + '<span class="count">1</span>';
  }
  btn.style.opacity = '';
}

async function repostPost(eventId, pubkey, btn) {
  if (!signerPubkey) {
    showToast(t('postLoginToRepost'));
    return;
  }
  if (btn && btn.classList.contains('reposted')) return;
  
  const repostEvent = {
    kind: 6,
    created_at: Math.floor(Date.now() / 1000),
    tags: [['e', eventId], ['p', pubkey]],
    content: '',
    pubkey: signerPubkey
  };
  
  if (btn) btn.style.opacity = '0.5';
  try {
    const signedEvent = await signNostrEvent(repostEvent);
    const resp = await fetch(API + '/post', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(signedEvent)
    });
    wsSend(JSON.stringify(["EVENT", signedEvent]));
    const result = await resp.json();
    if (resp.ok && result.status === 'ok') {
      if (btn) {
        btn.classList.add('reposted');
        const countEl = btn.querySelector('.count');
        if (countEl) { countEl.textContent = parseInt(countEl.textContent) + 1; }
        else { btn.innerHTML = SVG.repostIcon + '<span class="count">1</span>'; }
      }
      showToast(t('reposted'));
    } else {
      showToast(t('postRepostFailed') + (result.message || t('statusUnknown')));
    }
  } catch (e) {
    if (btn) {
      btn.classList.add('reposted');
      const countEl = btn.querySelector('.count');
      if (!countEl) btn.innerHTML = SVG.repostIcon + '<span class="count">1</span>';
    }
    showToast(t('postRepostError') + e.message);
  }
  if (btn) btn.style.opacity = '';
}

async function quotePost(eventId) {
  if (!signerPubkey) {
    showToast(t('quoteNeedSignIn'));
    return;
  }
  
  // Show quick quote modal
  var quoteText = prompt(t('quoteCommentPrompt'));
  if (quoteText === null) return; // cancelled
  
  var content = (quoteText || '').trim();
  
  var quoteEvent = {
    kind: 1,
    created_at: Math.floor(Date.now() / 1000),
    tags: [
      ['q', eventId]
    ],
    content: content,
    pubkey: signerPubkey
  };
  
  try {
    var signedEvent = await signNostrEvent(quoteEvent);
    
    var resp = await fetch(API + '/quote', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        quoted_event_id: eventId,
        pubkey: signerPubkey,
        content: content,
        event: signedEvent
      })
    });
    
    // Broadcast via WebSocket
    wsSend(JSON.stringify(["EVENT", signedEvent]));
    
    var result = await resp.json();
    if (resp.ok && result.success) {
      showToast(t('quotePublished'));
      setTimeout(function() { loadFeed(); }, 1500);
    } else {
      showToast(t('error') + ': ' + (result.error || t('unknownError')));
    }
  } catch (e) {
    showToast(t('quoteError') + ': ' + e.message);
  }
}

// ══════════════════════════════════════════
