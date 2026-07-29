// SNIN Client — Service Worker (PWA + Push)
// V9.0 — offline-first with asset pre-caching
// Auto-generated; bump CACHE_VERSION to invalidate

const CACHE_VERSION = 'snin-v9.0';
const STATIC_ASSETS = [
  '/',
  '/static/manifest.json',
  '/static/icon-192.png',
  '/static/icon-512.png',
  '/static/css/style.css',
  '/static/js/nostr-tools.min.js',
  '/static/js/i18n.js',
  '/static/js/animation-runtime.js',
  '/static/js/app-utils.js',
  '/static/js/dist/bundle.min.js',
  'https://fonts.googleapis.com/css2?family=Urbanist:ital,wght@0,300;0,400;0,500;0,600;0,700;0,800;0,900;1,400;1,600&display=swap'
];

// ─── Install: pre-cache all static assets ───
self.addEventListener('install', (e) => {
  e.waitUntil(
    caches.open(CACHE_VERSION).then(cache => {
      return Promise.allSettled(
        STATIC_ASSETS.map(url => cache.add(url).catch(err => {
          console.warn('[SW] Pre-cache failed for:', url, err.message);
        }))
      );
    })
  );
  self.skipWaiting();
});

// ─── Activate: purge old caches ───
self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== CACHE_VERSION).map(k => {
        console.log('[SW] Deleting old cache:', k);
        return caches.delete(k);
      })
    ))
  );
  self.clients.claim();
});

// ─── Fetch: cache-first for static, network-first for API ───
self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);

  // API calls — network first, no caching
  if (url.pathname.startsWith('/api/') || url.pathname === '/ws') {
    return; // let browser handle normally (no cache)
  }

  // Static assets — cache first, network fallback
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;

      return fetch(e.request).then(response => {
        // Cache successful responses for same-origin assets
        if (response.ok && (url.origin === location.origin ||
            url.hostname === 'cdn.jsdelivr.net' ||
            url.hostname === 'fonts.googleapis.com' ||
            url.hostname === 'fonts.gstatic.com')) {
          const clone = response.clone();
          caches.open(CACHE_VERSION).then(cache => cache.put(e.request, clone));
        }
        return response;
      }).catch(() => {
        // Offline fallback for navigation requests
        if (e.request.mode === 'navigate') {
          return caches.match('/');
        }
        // For other resources, just fail gracefully
        return new Response('', { status: 503 });
      });
    })
  );
});

// ─── Push notifications ───
self.addEventListener('push', (e) => {
  const data = e.data?.json() || { title: 'SNIN', body: 'New activity' };
  const opts = {
    body: data.body || '',
    icon: '/static/icon-192.png',
    badge: '/static/icon-192.png',
    tag: data.tag || 'snin-notify',
    data: { url: data.url || '/' },
    vibrate: [200, 100, 200],
    requireInteraction: false
  };
  e.waitUntil(self.registration.showNotification(data.title, opts));
});

// ─── Notification click ───
self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  e.waitUntil(
    clients.matchAll({ type: 'window' }).then(clients => {
      if (clients.length > 0) {
        clients[0].focus();
        if (e.notification.data?.url) {
          clients[0].navigate(e.notification.data.url);
        }
      } else {
        clients.openWindow(e.notification.data?.url || '/');
      }
    })
  );
});
