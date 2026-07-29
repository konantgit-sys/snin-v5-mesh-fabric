// SNIN Client — Composer Module
// Post composer, media attach, publish flow
// Extracted from app.js (2026-07-20, Phase 2.1)

// ─── Compose ───
function setupComposer() {
  const kindSelect = document.getElementById('composeKind');
  const replyField = document.getElementById('replyToField');
  const contentArea = document.getElementById('composeContent');
  kindSelect.addEventListener('change', () => {
    replyField.style.display = kindSelect.value === '1111' ? 'block' : 'none';
  });
  contentArea.addEventListener('input', () => {
    const len = contentArea.value.length;
    const pct = Math.min(len / 5000 * 100, 100);
    document.getElementById('charCount').textContent = len + ' / 5000';
    const bar = document.getElementById('charBar');
    bar.style.width = pct + '%';
    bar.className = 'char-bar-fill' + (pct > 80 ? ' danger' : pct > 60 ? ' warning' : '');
  });
}

// ─── V8.9 Compose Auth Check ───
function checkComposeAuth() {
  const prompt = document.getElementById('composeLoginPrompt');
  const panel = document.getElementById('composePanel');
  if (!prompt || !panel) return;
  if (signerPubkey) {
    prompt.classList.remove('active');
    panel.classList.remove('logged-out');
  } else {
    prompt.classList.add('active');
    panel.classList.add('logged-out');
  }
}

async function publishPost() {
  if (!signerPubkey) { setStatus(t('signerRequired'), 'error'); return; }
  const kind = parseInt(document.getElementById('composeKind').value);
  
  // V8.35: Poll creation
  if (kind === 6969) { await createPoll(); return; }
  
  const content = document.getElementById('composeContent').value.trim();
  const tagsInput = document.getElementById('composeTags').value.trim();
  const replyTo = document.getElementById('composeReplyTo').value.trim();
  
  // Collect uploaded media URLs
  const mediaUrls = [];
  document.querySelectorAll('#mediaPreviewGrid .media-preview-item').forEach(el => {
    mediaUrls.push(el.dataset.url);
  });
  
  // Build content with media
  let finalContent = content;
  if (mediaUrls.length > 0) {
    finalContent += '\n\n' + mediaUrls.map(u => u).join('\n');
  }
  
  if (!content && mediaUrls.length === 0) { setStatus(t('postSubmit'), 'error'); return; }
  const tags = [];
  if (tagsInput) tagsInput.split(',').forEach(t => tags.push(['t', t.trim()]));
  if (replyTo && kind === 1111) tags.push(['e', replyTo, '', 'reply']);
  if (kind === 39000) { tags.push(['L', 'agent']); tags.push(['l', 'ai', 'agent']); }
  
  // Add imeta tags for NIP-96 compatibility
  for (const url of mediaUrls) {
    tags.push(['imeta', 'url ' + url]);
  }
  
  const event = { kind, created_at: Math.floor(Date.now() / 1000), tags, content: finalContent, pubkey: signerPubkey };
  setStatus(t('signing'), 'pending');
  try {
    // Try NIP-07 first, fallback to nsec
    let signedEvent;
    if (signerNsec) {
      signedEvent = signEventNsec(event);
    } else if (window.nostr) {
      signedEvent = await signNostrEvent(event);
    } else {
      throw new Error(t('authNoSigner'));
    }
    setStatus(t('publishing'), 'pending');
    
    // Send via REST
    const resp = await fetch(API + '/post', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(signedEvent) });
    const result = await resp.json();
    
    // Also send via WebSocket (Nostr protocol)
    wsSend(JSON.stringify(["EVENT", signedEvent]));
    
    if (resp.ok && result.status === 'ok') {
      setStatus('Posted! ' + result.event_id.slice(0, 14) + '...', 'success');
      document.getElementById('composeContent').value = '';
      document.getElementById('composeTags').value = '';
      document.getElementById('mediaPreviewGrid').innerHTML = '';
      document.getElementById('charCount').textContent = '0 / 5000';
      document.getElementById('charBar').style.width = '0%';
      setTimeout(() => { if (state.tab === 'feed') loadFeed(); }, 1500);
    } else {
      setStatus('Error: ' + (result.message || result.error || 'Unknown'), 'error');
    }
  } catch (e) { setStatus('Failed: ' + e.message, 'error'); }
}

// ═══ Media Upload ═══
let uploadingMedia = false;

async function handleMediaAttach(event) {
  const files = event.target.files;
  if (!files || files.length === 0) return;
  
  for (const file of files) {
    if (uploadingMedia) return;
    uploadingMedia = true;
    
    const grid = document.getElementById('mediaPreviewGrid');
    
    // Show uploading placeholder
    const uploadId = 'upload-' + Date.now();
    grid.innerHTML += `<div class="media-preview-item uploading" id="${uploadId}">
      <div class="media-upload-spinner">⬆️</div>
      <div class="media-caption">Uploading...</div>
    </div>`;
    
    try {
      const formData = new FormData();
      formData.append('file', file);
      
      const resp = await fetch(API + '/upload', { method: 'POST', body: formData });
      const result = await resp.json();
      
      document.getElementById(uploadId)?.remove();
      
      if (resp.ok && result.status === 'ok') {
        const isVideo = file.type.startsWith('video/');
        grid.innerHTML += `<div class="media-preview-item" data-url="${result.url}">
          <div class="media-preview-wrapper">
            ${isVideo 
              ? `<video src="${result.url}" controls muted preload="metadata"></video>` 
              : `<img src="${result.url}" alt="uploaded">`}
          </div>
          <button class="media-remove-btn" onclick="this.parentElement.remove()">${SVG.closeIcon}</button>
        </div>`;
      } else {
        showToast(t('uploadFailed')+': ' + (result.message || 'Unknown'));
      }
    } catch (e) {
      document.getElementById(uploadId)?.remove();
      showToast(t('uploadError')+': ' + e.message);
    }
    
    uploadingMedia = false;
  }
  
  // Reset input
  event.target.value = '';
}

function setStatus(msg, type) {
  const el = document.getElementById('postStatus');
  el.textContent = msg;
  el.className = 'compose-status ' + type;
}
