// ══════════════════════════════════════════
// V11.0 — DM Module — NIP-04/17/44 Encrypted Messages (extracted from app.js)
// ══════════════════════════════════════════


let dmPeer = null; // currently chatting with this pubkey
let dmDecrypted = {}; // { eventId: plaintext } cache

function loadDMChats() {
  if (!signerPubkey) {
    document.getElementById('dmConversationList').innerHTML = 
      '<div class="empty-state"><p>Sign in with nsec to use DMs</p></div>';
    return;
  }
  
  fetch(API + '/dm/list?pubkey=' + signerPubkey)
    .then(r => r.json())
    .then(data => {
      const list = document.getElementById('dmConversationList');
      if (!data.conversations || data.conversations.length === 0) {
        list.innerHTML = '<div class="empty-state"><p>No conversations yet.<br>Enter a pubkey above to start one.</p></div>';
        return;
      }
      list.innerHTML = data.conversations.map(c => {
        const name = c.display_name || c.peer.slice(0, 8) + '...';
        const lastTime = formatTime(c.last_at);
        const isActive = dmPeer === c.peer;
        return `<div class="dm-conv-item${isActive ? ' active' : ''}" onclick="openDMChat('${c.peer}')">
          <div class="dm-conv-avatar" style="background:linear-gradient(135deg,hsl(${(parseInt(c.peer.slice(0,2),16)||0)%360},60%,45%),hsl(${(parseInt(c.peer.slice(2,4),16)||0)%360},60%,30%))">${(name[0] || '?').toUpperCase()}</div>
          <div class="dm-conv-info">
            <div class="dm-conv-name">${escapeHtml(name)}</div>
            <div class="dm-conv-meta">${c.msg_count} msg · ${lastTime}</div>
          </div>
        </div>`;
      }).join('');
    })
    .catch(e => {
      document.getElementById('dmConversationList').innerHTML = 
        `<div class="empty-state"><p>${t('errConversations')}</p></div>`;
    });
}

function openDMWith(pubkey) {
  if (!pubkey || pubkey.length < 30) {
    showToast(t('dmNeedPubkey'));
    return;
  }
  openDMChat(pubkey.trim());
}

function openDMChat(pubkey) {
  dmPeer = pubkey;
  // Update sidebar active
  document.querySelectorAll('.dm-conv-item').forEach(el => el.classList.remove('active'));
  const activeEl = document.querySelector(`.dm-conv-item[onclick*="${pubkey}"]`);
  if (activeEl) activeEl.classList.add('active');
  
  // Show chat view
  document.getElementById('dmChat').querySelector('.dm-chat-placeholder').classList.add('hidden');
  document.getElementById('dmChatHeader').classList.remove('hidden');
  document.getElementById('dmCompose').classList.remove('hidden');
  document.getElementById('dmMessages').innerHTML = '';
  
  // Resolve name
  fetch(API + '/profile/' + pubkey)
    .then(r => r.json())
    .then(p => {
      document.getElementById('dmChatPeer').textContent = (p.display_name || pubkey.slice(0, 12) + '...');
    }).catch(() => {});
  
  loadDMMessages();
}

function closeDMChat() {
  dmPeer = null;
  document.getElementById('dmChat').querySelector('.dm-chat-placeholder').classList.remove('hidden');
  document.getElementById('dmChatHeader').classList.add('hidden');
  document.getElementById('dmCompose').classList.add('hidden');
  document.getElementById('dmMessages').innerHTML = '';
}

async function loadDMMessages() {
  if (!dmPeer || !signerPubkey) return;
  const container = document.getElementById('dmMessages');
  container.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text-muted)"><div class="spinner"></div></div>';
  
  try {
    const resp = await fetch(API + '/dm/conversation?pubkey_a=' + signerPubkey + '&pubkey_b=' + dmPeer + '&limit=100');
    const data = await resp.json();
    
    if (!data.messages || data.messages.length === 0) {
      container.innerHTML = '<div class="empty-state"><p>No messages yet. Say hello!</p></div>';
      return;
    }
    
    // Check for NIP-04 decrypt support
    const hasNip04 = window.nostr && window.nostr.nip04 && window.nostr.nip04.decrypt;
    const hasNip44 = window.nostr && window.nostr.nip44 && window.nostr.nip44.decrypt;
    
    let html = '';
    for (const msg of data.messages) {
      const isOut = msg.direction === 'out';
      const timeStr = formatTime(msg.created_at);
      let content = msg.content || '';
      
      // Try decrypt
      if (dmDecrypted[msg.id]) {
        content = dmDecrypted[msg.id];
      } else if (content.startsWith('{') || content.startsWith('[')) {
        content = '[JSON envelope — tap to decrypt]';
      }
      
      html += `<div class="dm-bubble ${isOut ? 'dm-out' : 'dm-in'}">
        <div class="dm-bubble-header">${escapeHtml(msg.author_name)} · ${timeStr}</div>
        <div class="dm-bubble-content" id="dm-msg-${msg.id}" 
             onclick="decryptDM('${msg.id}', '${isOut ? signerPubkey : dmPeer}', '${isOut ? dmPeer : signerPubkey}')">
          ${renderDMContent(content, msg.id)}
        </div>
      </div>`;
    }
    
    container.innerHTML = html;
    
    // Auto-decrypt: NIP-07 first, then nsec via NostrTools
    const canDecrypt = hasNip04 || hasNip44 || signerNsec;
    if (canDecrypt) {
      for (const msg of data.messages) {
        try {
          const theirPubkey = msg.direction === 'out' ? dmPeer : msg.pubkey;
          let plaintext;
          if (hasNip04) {
            plaintext = await window.nostr.nip04.decrypt(theirPubkey, msg.content);
          } else if (signerNsec) {
            plaintext = await NostrTools.nip04.decrypt(theirPubkey, signerNsec, msg.content);
          }
          if (plaintext) {
            dmDecrypted[msg.id] = plaintext;
            const el = document.getElementById('dm-msg-' + msg.id);
            if (el) el.textContent = plaintext;
          }
        } catch (e) {
          // Decrypt failed — leave as is
        }
      }
    }
    
    // Scroll to bottom
    container.scrollTop = container.scrollHeight;
  } catch (e) {
    container.innerHTML = `<div class="empty-state"><p>${t('errMessages')}</p></div>`;
  }
}

async function decryptDM(eventId, senderPubkey, recipientPubkey) {
  if (dmDecrypted[eventId]) {
    document.getElementById('dm-msg-' + eventId).innerHTML = renderDMContent(dmDecrypted[eventId], eventId);
    return;
  }
  
  // Try NIP-07 first, then nsec via NostrTools
  if (window.nostr && window.nostr.nip04) {
    try {
      const otherPubkey = dmPeer;
      const plaintext = await window.nostr.nip04.decrypt(otherPubkey, 
        document.getElementById('dm-msg-' + eventId).textContent);
      dmDecrypted[eventId] = plaintext;
      document.getElementById('dm-msg-' + eventId).innerHTML = renderDMContent(plaintext, eventId);
      return;
    } catch (e) { /* fall through to nsec */ }
  }
  
  // nsec-based decrypt via NostrTools
  if (signerNsec) {
    try {
      const theirPubkey = (signerPubkey === recipientPubkey) ? senderPubkey : recipientPubkey;
      const ciphertext = document.getElementById('dm-msg-' + eventId).textContent;
      const plaintext = await NostrTools.nip04.decrypt(theirPubkey, signerNsec, ciphertext);
      dmDecrypted[eventId] = plaintext;
      document.getElementById('dm-msg-' + eventId).innerHTML = renderDMContent(plaintext, eventId);
      return;
    } catch (e) {
      showToast(t('dmDecryptFailed') + e.message);
      return;
    }
  }
  
  showToast(t('needNip04Signer'));
}

function renderDMContent(content, msgId) {
  // Render links and images inline
  if (!content) return '';
  let html = escapeHtml(content);

  // Make URLs clickable
  html = html.replace(/(https?:\/\/[^\s<]+)/g, '<a href="$1" target="_blank" class="dm-link">$1</a>');

  // Render image URLs inline
  html = html.replace(/(https?:\/\/[^\s<]+\.(jpg|jpeg|png|gif|webp|svg))(\b|$)/gi,
    '<div class="dm-image-wrapper"><img src="$1" class="dm-inline-image" loading="lazy" onclick="viewDMImage(\'$1\')" onerror="this.style.display=\'none\'"></div>');

  return html;
}

function viewDMImage(url) {
  const overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.85);z-index:10000;display:flex;align-items:center;justify-content:center;cursor:pointer';
  overlay.innerHTML = '<img src="' + url + '" style="max-width:90vw;max-height:90vh;border-radius:8px" onclick="event.stopPropagation()"><span style="position:absolute;top:20px;right:20px;color:#fff;font-size:24px;cursor:pointer">×</span>';
  overlay.onclick = () => overlay.remove();
  document.body.appendChild(overlay);
}

function attachDMFile() {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = 'image/*';
  input.onchange = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    showToast(t('statusLoading'));
    const reader = new FileReader();
    reader.onload = async (ev) => {
      try {
        const resp = await fetch(API + '/dm/upload', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({data: ev.target.result, name: file.name, type: file.type})
        });
        const data = await resp.json();
        if (data.ok) {
          const inputEl = document.getElementById('dmInput');
          if (inputEl) inputEl.value += '\n' + data.url;
          showToast(t('dmImageAttached'));
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

async function openDMGallery() {
  if (!dmPeer || !signerPubkey) {
    showToast(t('dmOpenChatFirst'));
    return;
  }

  try {
    const resp = await fetch(API + '/dm/gallery?pubkey_a=' + signerPubkey + '&pubkey_b=' + dmPeer);
    const data = await resp.json();
    const images = data.images || [];

    if (images.length === 0) {
      showToast(t('dmNoImages'));
      return;
    }

    let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px"><span style="font-weight:600">Gallery (' + images.length + ')</span><span style="cursor:pointer;font-size:20px" onclick="this.closest(\'modal-overlay\').remove()">×</span></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;max-height:70vh;overflow-y:auto">';
    images.forEach(function(img) {
      const url = img.url || img;
      html += '<img src="' + escapeHtml(url) + '" style="width:100%;aspect-ratio:1;object-fit:cover;border-radius:6px;cursor:pointer" onclick="viewDMImage(\'' + url.replace(/'/g, "\\'") + '\')" onerror="this.style.display=\'none\'">';
    });
    html += '</div>';

    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.85);z-index:10000;display:flex;align-items:center;justify-content:center';
    overlay.innerHTML = '<div style="background:var(--bg-darker);border-radius:12px;padding:16px;max-width:500px;width:90%;max-height:85vh;overflow:auto">' + html + '</div>';
    overlay.onclick = function(e) { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  } catch (e) {
    showToast(t('dmGalleryError'));
  }
}


async function sendDM() {
  if (!dmPeer || !signerPubkey || (!window.nostr && !signerNsec)) {
    showToast(t('connectSignerDMs'));
    return;
  }
  
  const input = document.getElementById('dmInput');
  const text = input.value.trim();
  if (!text) return;
  
  const sendBtn = document.querySelector('.dm-compose .btn-primary');
  sendBtn.disabled = true;
  sendBtn.textContent = '...';
  input.disabled = true;
  
  try {
    // Encrypt: NIP-07 first, then NostrTools with nsec
    let encrypted;
    if (window.nostr && window.nostr.nip44 && window.nostr.nip44.encrypt) {
      encrypted = await window.nostr.nip44.encrypt(dmPeer, text);
    } else if (window.nostr && window.nostr.nip04 && window.nostr.nip04.encrypt) {
      encrypted = await window.nostr.nip04.encrypt(dmPeer, text);
    } else if (signerNsec) {
      encrypted = await NostrTools.nip04.encrypt(signerNsec, dmPeer, text);
    } else {
      throw new Error(t('nip04Unavailable'));
    }
    
    const event = {
      kind: 4,
      created_at: Math.floor(Date.now() / 1000),
      tags: [['p', dmPeer]],
      content: encrypted,
      pubkey: signerPubkey
    };
    
    const signed = await signNostrEvent(event);
    
    const resp = await fetch(API + '/post', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(signed)
    });
    
    wsSend(JSON.stringify(["EVENT", signed]));
    
    const result = await resp.json();
    if (resp.ok && result.status === 'ok') {
      input.value = '';
      // Reload with new message
      await new Promise(r => setTimeout(r, 300));
      await loadDMMessages();
      loadDMChats(); // Update sidebar
    } else {
      showToast(t('sendFailed')+': ' + (result.message || 'Unknown'));
    }
  } catch (e) {
    showToast(t('dmError')+': ' + (e.message || t('profileSaveError')));
  }
  
  sendBtn.disabled = false;
  sendBtn.textContent = t('dmSent');
  input.disabled = false;
  input.focus();
}

function quickReply(eventId) {
  switchTab('compose');
  document.getElementById('composeKind').value = '1111';
  document.getElementById('replyToField').style.display = 'block';
  document.getElementById('composeReplyTo').value = eventId;
  document.getElementById('composeContent').focus();
}

