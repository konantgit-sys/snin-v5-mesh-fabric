// ══════════════════════════════════════════
// V11.0 — Utils Module (extracted from app.js)
// ══════════════════════════════════════════

// UTILS

function formatTime(ts) {
  if (!ts) return '?';
  const diff = Math.floor(Date.now() / 1000) - ts;
  if (diff < 60) return t('justNow');
  if (diff < 3600) return Math.floor(diff / 60) + t('minutesAgo');
  if (diff < 86400) return Math.floor(diff / 3600) + t('hoursAgo');
  if (diff < 604800) return Math.floor(diff / 86400) + t('daysAgo');
  return new Date(ts * 1000).toLocaleDateString();
}

function formatUptime(seconds) {
  if (!seconds) return '';
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (d > 0) return d + 'd ' + h + 'h';
  if (h > 0) return h + 'h ' + m + 'm';
  return m + 'm';
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function escapeAttr(str) {
  return String(str || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function getTimeAgo(ts) {
  const now = Math.floor(Date.now() / 1000);
  const diff = now - ts;
  if (diff < 60) return 'now';
  if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
  if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
  return Math.floor(diff / 86400) + 'd ago';
}

function updateStatus(online) {
  const dot = document.getElementById('relayDot');
  const label = document.getElementById('relayLabel');
  if (!dot || !label) return;
  if (online) {
    dot.className = 'status-dot live';
    label.textContent = t('statusOnline');
  } else {
    dot.className = 'status-dot offline';
    label.textContent = t('statusOffline');
  }
}
