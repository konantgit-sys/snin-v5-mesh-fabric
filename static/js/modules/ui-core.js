// SNIN Client — UI Core Module
// SVG snippets, toast system, theme toggle, language switcher, modals
// Extracted from app.js (2026-07-20, Phase 2.1)

// ─── SVG Snippets ───
const SVG = {
  events: '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="3" width="20" height="18" rx="3"/><path d="M6 8h4M6 12h6M6 16h8"/></svg>',
  authors: '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="8" r="4"/><path d="M4 20c0-4 3.6-7 8-7s8 3 8 7"/></svg>',
  online: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="3" fill="currentColor"/><circle cx="12" cy="12" r="8" opacity="0.4"/></svg>',
  robot: '<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><rect x="3" y="5" width="18" height="14" rx="3"/><circle cx="8" cy="10" r="2" fill="white"/><circle cx="16" cy="10" r="2" fill="white"/><path d="M9 17h6M12 5V2M6 2h12" stroke="white" stroke-width="2" stroke-linecap="round"/></svg>',
  searchIcon: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4" stroke-linecap="round"/></svg>',
  heart: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 21l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.18L12 21z"/></svg>',
  replyIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M21 11.5a8.38 8.38 0 01-.9 3.8 8.5 8.5 0 01-7.6 4.7 8.38 8.38 0 01-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 01-.9-3.8 8.5 8.5 0 014.7-7.6 8.38 8.38 0 013.8-.9h.5a8.48 8.48 0 018 8v.5z"/></svg>',
  threadIcon: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/></svg>',
  zapIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" opacity="0.5"><path d="M13 2L3 14h7l-2 8 10-12h-7l2-8z"/></svg>',
  repostIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" opacity="0.5"><path d="M17 1l4 4-4 4"/><path d="M3 11V9a4 4 0 014-4h14"/><path d="M7 23l-4-4 4-4"/><path d="M21 13v2a4 4 0 01-4 4H3"/></svg>',
  empty: '<svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1"><rect x="3" y="3" width="18" height="18" rx="3"/><path d="M9 9h6M9 13h4"/></svg>',
  lockIcon: '<svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="8" y="11" width="8" height="7" rx="1"/><path d="M12 14v2"/><path d="M10 11V7a2 2 0 012-2h0a2 2 0 012 2v4"/></svg>',
  warning: '<svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2"><circle cx="12" cy="12" r="10"/><path d="M12 8v4M12 16h.01"/></svg>',
  globe: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10M12 2a15.3 15.3 0 00-4 10 15.3 15.3 0 004 10"/></svg>',
  send: '<svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M2 21L23 12 2 3v7l15 2-15 2v7z"/></svg>',
  lock: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>',
  closeIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 6L6 18M6 6l12 12" stroke-linecap="round"/></svg>',
  post: '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><path d="M14 2v6h6M16 13H8M16 17H8M10 9H8"/></svg>',
  node: '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="3"/><path d="M12 2v4M12 18v4M2 12h4M18 12h4M5.64 5.64l2.83 2.83M15.54 15.54l2.83 2.83M18.36 5.64l-2.83 2.83M8.46 15.54l-2.83 2.83"/></svg>',
  bookmarkIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 012-2h10a2 2 0 012 2z"/></svg>',
  muteIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><line x1="23" y1="9" x2="17" y2="15"/><line x1="17" y1="9" x2="23" y2="15"/></svg>',
  flameIcon: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2c-1.5 3-4 5-4 8a4 4 0 008 0c0-3-2.5-5-4-8z" fill="currentColor" opacity="0.3"/><path d="M12 22c-3.3 0-6-2.7-6-6 0-2 1-3.5 2.5-5"/><path d="M12 22c3.3 0 6-2.7 6-6 0-2-1-3.5-2.5-5"/></svg>',
  refreshIcon: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>',
  calendarIcon: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>',
  quoteIcon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M3 21c3 0 7-1 7-8V5c0-1.25-.756-2.017-2-2H4c-1.25 0-2 .75-2 1.972V11c0 1.25.75 2 2 2 1 0 1 0 1 1v1c0 1-1 2-2 2s-1 .008-1 1.031V20c0 1 0 1 1 1z"/><path d="M15 21c3 0 7-1 7-8V5c0-1.25-.757-2.017-2-2h-4c-1.25 0-2 .75-2 1.972V11c0 1.25.75 2 2 2h.75c0 2.25.25 4-2.75 4v3c0 1 0 1 1 1z"/></svg>',
  bell: '<svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1"><path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 01-3.46 0"/></svg>',
};

// ─── V7 B2 — Toast System ───
function initToastContainer() {
  if (!document.querySelector('.toast-container')) {
    const c = document.createElement('div');
    c.className = 'toast-container';
    document.body.appendChild(c);
  }
}

function closeAllModals() {
  // V8.67 — Close all open modal overlays
  var modals = document.querySelectorAll('#profileModal, #zapModal, #onboardModal, #threadModal, #articleModal');
  modals.forEach(function(m) { m.style.display = 'none'; });
  // Also close the lightbox if open
  var lb = document.getElementById('sninLightbox');
  if (lb) { lb.classList.remove('active'); document.body.style.overflow = ''; }
  // Close acct switcher
  var as = document.getElementById('acctSwitcherOverlay');
  if (as) as.remove();
}

function showTab(tabId) {
  // V8.67 — Switch tabs within a modal (e.g. profile following/followers)
  var panel = document.getElementById(tabId);
  if (!panel) return;
  // Hide all sibling tab panels with fade
  var parent = panel.parentElement;
  if (parent) {
    var panels = Array.from(parent.querySelectorAll('.tab-panel, [id^="tab-"]'));
    panels.forEach(function(p) {
      if (p === panel) return;
      if (p.style.display !== 'none') {
        p.style.opacity = '0';
        p.style.transform = 'translateY(8px)';
        setTimeout(function() { p.style.display = 'none'; }, 180);
      }
    });
  }
  // Show target with fade-in
  panel.style.display = 'block';
  panel.style.opacity = '0';
  panel.style.transform = 'translateY(8px)';
  panel.style.transition = 'opacity 180ms var(--ease-out), transform 180ms var(--ease-out)';
  requestAnimationFrame(function() {
    panel.style.opacity = '1';
    panel.style.transform = 'translateY(0)';
  });
  // Update active state on tab buttons
  var container = parent ? parent.parentElement : null;
  if (container) {
    Array.from(container.querySelectorAll('.stat-item, .tab-item')).forEach(function(b) {
      b.classList.remove('active');
    });
    var activeBtn = container.querySelector('[onclick*="' + tabId + '"]');
    if (activeBtn) activeBtn.classList.add('active');
  }
}

function showToast(msg, type) {
  type = type || 'info';
  initToastContainer();
  const container = document.querySelector('.toast-container');
  const toast = document.createElement('div');
  toast.className = 'toast toast-' + type;
  toast.textContent = msg;
  
  // V8.60 B2 — DF spring entry animation
  toast.style.transform = 'translateY(20px) scale(0.95)';
  toast.style.opacity = '0';
  container.appendChild(toast);
  
  // Force reflow then animate
  requestAnimationFrame(function() {
    requestAnimationFrame(function() {
      toast.style.transition = 'transform 0.4s cubic-bezier(0.34, 1.56, 0.64, 1), opacity 0.3s ease';
      toast.style.transform = 'translateY(0) scale(1)';
      toast.style.opacity = '1';
    });
  });
  
  // V8.60 B2 — Swipe to dismiss on mobile
  setupToastDrag(toast);
  
  setTimeout(function() {
    toast.style.transition = 'transform 0.3s ease, opacity 0.3s ease';
    toast.style.transform = 'translateY(-10px) scale(0.95)';
    toast.style.opacity = '0';
    setTimeout(function() { toast.remove(); }, 300);
  }, 3500);
}

// V8.60 B2 — Drag-to-dismiss for toasts
function setupToastDrag(toast) {
  let startY = 0, currentY = 0, dragging = false;
  
  toast.addEventListener('pointerdown', function(e) {
    startY = e.clientY;
    dragging = true;
    toast.style.transition = 'none';
    toast.setPointerCapture(e.pointerId);
  });
  
  toast.addEventListener('pointermove', function(e) {
    if (!dragging) return;
    currentY = e.clientY - startY;
    if (currentY < -5) { // Only allow pull-up
      toast.style.transform = 'translateY(' + currentY + 'px)';
      toast.style.opacity = Math.max(0, 1 - Math.abs(currentY) / 100);
    }
  });
  
  toast.addEventListener('pointerup', function(e) {
    if (!dragging) return;
    dragging = false;
    toast.style.transition = 'transform 0.3s cubic-bezier(0.34, 1.56, 0.64, 1), opacity 0.3s ease';
    if (currentY < -40) {
      // Dismiss
      toast.style.transform = 'translateY(-60px) scale(0.9)';
      toast.style.opacity = '0';
      setTimeout(function() { toast.remove(); }, 300);
    } else {
      // Snap back
      toast.style.transform = 'translateY(0) scale(1)';
      toast.style.opacity = '1';
    }
  });
}

// ═══ V7 C1 — Theme toggle ═══
function initTheme() {
  const saved = localStorage.getItem('snin-theme');
  if (saved === 'light' || (!saved && window.matchMedia('(prefers-color-scheme: light)').matches)) {
    document.documentElement.classList.add('light');
    updateThemeIcon(true);
  }
}

function toggleTheme() {
  const isLight = document.documentElement.classList.toggle('light');
  localStorage.setItem('snin-theme', isLight ? 'light' : 'dark');
  updateThemeIcon(isLight);
}

function updateThemeIcon(isLight) {
  const icon = document.getElementById('themeIcon');
  if (icon) {
    icon.innerHTML = isLight
      ? '<use href="#icon-moon"/>'
      : '<use href="#icon-sun"/>';
  }
}

function toggleLang() {
  const newLang = I18N.lang === 'ru' ? 'en' : 'ru';
  I18N.setLang(newLang);
  document.getElementById('langToggle').textContent = newLang === 'ru' ? 'RU' : 'EN';
  // Reload dynamic content to apply translations
  if (state.tab === 'feed') loadFeed();
  if (state.tab === 'discover') loadDiscover();
  if (state.tab === 'account') loadAccount();
  if (state.tab === 'messages') loadDMChats();
  if (state.tab === 'node') loadNode();
}

// Set initial lang button state
document.addEventListener('DOMContentLoaded', function() {
  // Register with original DOMContentLoaded — handled by I18N.updateDOM call
});

// ═══ V11.0 — Global error handler + ApiClient toast integration ═══
document.addEventListener('error', function(e) {
  console.error('[ErrorHandler]', e.error ? e.error.message : e.message);
  showToast(t('errorGeneral') || 'Something went wrong', 'error');
});

// Monkey-patch ApiClient to auto-toast on errors (unless { silent: true })
if (typeof ApiClient !== 'undefined') {
  const _get = ApiClient.get;
  const _post = ApiClient.post;
  ApiClient.get = async function(path, opts) {
    const result = await _get.call(ApiClient, path, opts);
    if (!result.ok && !(opts && opts.silent)) {
      showToast(getErrorMessage(result), 'error');
    }
    return result;
  };
  ApiClient.post = async function(path, body, opts) {
    const result = await _post.call(ApiClient, path, body, opts);
    if (!result.ok && !(opts && opts.silent)) {
      showToast(getErrorMessage(result), 'error');
    }
    return result;
  };
}

function getErrorMessage(result) {
  if (result.status === 0) return t('errorNetwork') || 'Network error — check connection';
  if (result.status === 429) return t('errorRateLimit') || 'Too many requests, wait a moment';
  if (result.status >= 500) return t('errorServer') || 'Server error, retrying...';
  if (result.status === 404) return t('errorNotFound') || 'Not found';
  return t('errorGeneral') || 'Something went wrong';
}

