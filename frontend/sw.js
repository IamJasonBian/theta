// theta — minimal service worker
//
// Strategy:
//   - Precache the app shell on install (index + icon + manifest)
//   - For navigations / shell assets: cache-first, fall back to network
//   - For /ledger/* API calls and /openapi.json: network-only (no caching —
//     we never want a stale PUT result or a cached empty ledger)
//
// !!! CONVENTION !!!
// Any commit that modifies frontend/index.html, frontend/manifest.json,
// or frontend/icon.svg MUST bump CACHE_VERSION. Otherwise every phone
// that installed a previous version of the SW will keep serving stale
// HTML cache-first, ignoring every Render redeploy. This has bitten us
// once already.
//
// Bump history:
//   v1 — initial ship (commit d143a10)
//   v2 — pick up id-handling + default fallback + DQ work (current)

const CACHE_VERSION = 'theta-shell-v2';
const SHELL_ASSETS = [
  '/',
  '/index.html',
  '/manifest.json',
  '/icon.svg',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION).then((cache) => cache.addAll(SHELL_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_VERSION).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // API + OpenAPI + docs → network-only, never cache
  if (url.pathname.startsWith('/ledger') ||
      url.pathname === '/openapi.json' ||
      url.pathname === '/docs' ||
      url.pathname === '/healthz') {
    return;  // let the browser handle it normally
  }

  // Shell assets → cache-first
  event.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;
      return fetch(req).then((resp) => {
        // Only cache successful same-origin responses
        if (resp.ok && url.origin === self.location.origin) {
          const copy = resp.clone();
          caches.open(CACHE_VERSION).then((c) => c.put(req, copy));
        }
        return resp;
      }).catch(() => caches.match('/'));
    })
  );
});
