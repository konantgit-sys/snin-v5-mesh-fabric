// ══════════════════════════════════════════
// V11.0 — Thread Module (extracted from app.js)
// ══════════════════════════════════════════

let currentThreadId = null;
let currentThreadRootPubkey = null;

async function showThread(eventId) {
  const modal = document.getElementById('threadModal');
  const content = document.getElementById('threadContent');
  const replyBox = document.getElementById('threadReplyBox');
  modal.style.display = 'flex';
  content.innerHTML = '<div class="skeleton skeleton-card" style="height:120px"></div>';
  if (replyBox) replyBox.style.display = 'none';
  currentThreadId = eventId;

  try {
    const resp = await fetch(API + '/thread/' + eventId);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    let html = '';

    // Root post — full display with actions
    if (data.root) {
      const r = data.root;
      currentThreadRootPubkey = r.pubkey;
      const name = r.author_name || shortPubkey(r.pubkey);
      const hue = hashToHue(r.pubkey);
      const initials = getInitials(name, '??');
      const postId = r.id || eventId;
      const reactionCount = r.reactions || 0;
      const replyCount = data.replies ? data.replies.length : 0;

      html += '<div class="thread-root"><div class="thread-card-root">' +
        '<div class="thread-author-row">' +
          '<div class="post-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%));width:44px;height:44px;font-size:17px" onclick="closeThread();showProfile(\'' + r.pubkey + '\')">' +
            '<span>' + initials + '</span>' +
          '</div>' +
          '<div class="thread-author-info">' +
            '<div class="thread-author-name" onclick="closeThread();showProfile(\'' + r.pubkey + '\')">' + escapeHtml(name) + '</div>' +
            '<div class="thread-author-npub">' + shortPubkey(r.pubkey) + '</div>' +
          '</div>' +
        '</div>' +
        '<div class="thread-content">' + parseHashtags(escapeHtml((r.content || ''))) + '</div>' +
        '<div class="thread-time-row">' +
          '<span class="thread-time">' + formatTime(r.created_at) + '</span>' +
        '</div>' +
        '<div class="thread-actions-row">' +
          '<button class="thread-action-btn like-btn" onclick="event.stopPropagation();likePost(\'' + postId + '\',\'' + r.pubkey + '\',this)" title="Like">' +
            SVG.heart + (reactionCount > 0 ? '<span class="count">' + reactionCount + '</span>' : '') +
          '</button>' +
          '<button class="thread-action-btn" onclick="event.stopPropagation();quickReplyInThread(\'' + postId + '\')" title="Reply">' +
            SVG.replyIcon + (replyCount > 0 ? '<span class="count">' + replyCount + '</span>' : '') +
          '</button>' +
          '<button class="thread-action-btn repost-btn" onclick="event.stopPropagation();repostPost(\'' + postId + '\',\'' + r.pubkey + '\',this)" title="Repost">' +
            SVG.repostIcon + ((r.reposts || 0) > 0 ? '<span class="count">' + (r.reposts || 0) + '</span>' : '') +
          '</button>' +
          '<button class="thread-action-btn zap-btn" title="Zap" onclick="showZap(\'' + r.pubkey + '\',event)" title="Zap">' +
            SVG.zapIcon +
          '</button>' +
        '</div>' +
      '</div></div>';
    }

    // Replies
    const replies = data.replies || [];
    html += '<div class="thread-section-label">' +
      '<span class="thread-section-line"></span>' +
      '<span class="thread-section-text">' + replies.length + ' repl' + (replies.length === 1 ? 'y' : 'ies') + '</span>' +
    '</div>';

    if (replies.length === 0) {
      html += '<div class="thread-empty">' +
        '' + SVG.threadIcon + '' +
        '<div>No replies yet.</div>' +
        '<div style="font-size:12px;color:var(--text-dim);margin-top:4px">Be the first to reply</div>' +
      '</div>';
    } else {
      html += '<div class="thread-replies-list">';
      replies.forEach((r, i) => {
        const name = r.author_name || shortPubkey(r.pubkey);
        const hue = hashToHue(r.pubkey);
        const initials = getInitials(name, '??');
        const rid = r.id || '';
        const rawContent = r.content || '';
        const isLong = rawContent.length > 500;
        const isLast = i === replies.length - 1;
        html += '<div class="thread-reply' + (isLast ? ' thread-reply-last' : '') + '">' +
          '<div class="thread-reply-line"></div>' +
          '<div class="thread-reply-card">' +
            '<div class="thread-reply-header">' +
              '<div class="post-avatar" style="background:linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%));width:32px;height:32px;font-size:12px" onclick="closeThread();showProfile(\'' + r.pubkey + '\')">' +
                '<span>' + initials + '</span>' +
              '</div>' +
              '<div class="thread-reply-meta">' +
                '<span class="thread-reply-author" onclick="closeThread();showProfile(\'' + r.pubkey + '\')">' + escapeHtml(name) + '</span>' +
                '<span class="thread-reply-time">' + formatTime(r.created_at) + '</span>' +
              '</div>' +
            '</div>' +
            '<div class="thread-reply-content">' +
              escapeHtml(rawContent.slice(0, 500)) +
              (isLong ? '<span class="post-truncated">...</span><span class="post-rest" id="rest-' + rid + '" style="display:none">' + escapeHtml(rawContent.slice(500)) + '</span><br><button class="btn-read-more" onclick="event.stopPropagation();expandPost(\'' + rid + '\')">Read more ↓</button>' : '') +
            '</div>' +
            '<div class="thread-reply-actions">' +
              '<button class="thread-action-btn-sm like-btn" onclick="event.stopPropagation();likePost(\'' + rid + '\',\'' + r.pubkey + '\',this)">' + SVG.heart + '</button>' +
              '<button class="thread-action-btn-sm" onclick="event.stopPropagation();quickReplyInThread(\'' + rid + '\')">' + SVG.replyIcon + '</button>' +
              '<button class="thread-action-btn-sm repost-btn" onclick="event.stopPropagation();repostPost(\'' + rid + '\',\'' + r.pubkey + '\')">' + SVG.repostIcon + '</button>' +
            '</div>' +
          '</div>' +
        '</div>';
      });
      html += '</div>';
    }

    content.innerHTML = html;

    // Show reply box if logged in
    if (signerPubkey && replyBox) {
      replyBox.style.display = 'flex';
      const avatar = document.getElementById('threadReplyAvatar');
      if (avatar) {
        const hue = hashToHue(signerPubkey);
        const myName = (signerPubkey || '').slice(0, 2).toUpperCase();
        avatar.style.background = 'linear-gradient(135deg,hsl(' + hue + ',80%,50%),hsl(' + (hue+35) + ',80%,30%))';
        avatar.textContent = myName;
      }
      document.getElementById('threadReplyInput').value = '';
    }

  } catch (e) {
    content.innerHTML = '<div class="empty-state">' + SVG.warning + `<div class="empty-title">${t('errThread')}</div></div>`;
  }
}

function quickReplyInThread(eventId) {
  const input = document.getElementById('threadReplyInput');
  if (input) {
    input.placeholder = 'Replying to ' + eventId.slice(0, 8) + '…';
    input.focus();
  }
}

async function submitThreadReply() {
  if (!signerPubkey) { showToast(t('postLoginToReply')); return; }
  const input = document.getElementById('threadReplyInput');
  const content = input.value.trim();
  if (!content || !currentThreadId) return;

  const event = {
    kind: 1111,
    created_at: Math.floor(Date.now() / 1000),
    tags: [
      ['e', currentThreadId],
      ['p', currentThreadRootPubkey || '']
    ],
    content: content,
    pubkey: signerPubkey
  };

  try {
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
      showToast(t('postReplySent'));
      // Reload thread
      showThread(currentThreadId);
    } else {
      showToast(t('postExpandError') + (result.message || t('statusUnknown')));
    }
  } catch (e) {
    showToast(t('postReplyError'));
  }
}

function closeThread() {
  document.getElementById('threadModal').style.display = 'none';
  currentThreadId = null;
}
