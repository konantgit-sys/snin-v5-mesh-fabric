// SNIN Client — Module concatenation builder (V2 — all modules)
const fs = require('fs');
const path = require('path');

const JS_DIR = 'static/js';
const MODULES_DIR = `${JS_DIR}/modules`;
const DIST_DIR = `${JS_DIR}/dist`;

// Dependency-ordered module load (foundation → auth → UI core → features)
const MODULE_ORDER = [
  // Layer 1: Foundation (no deps on other SNIN modules)
  'crypto', 'state', 'utils', 'api-client',
  
  // Layer 2: Auth & Identity
  'accounts', 'auth', 'key-export',
  
  // Layer 3: UI Core
  'ui-core', 'effects',
  
  // Layer 4: Core Features (used by most tabs)
  'feed-core', 'feed', 'composer', 'profile', 'thread', 'article',
  'search', 'hashtags', 'highlights',
  'notifications', 'dm',
  
  // Layer 5: Extended Features
  'zap', 'wallet', 'nwc',
  'marketplace', 'agents', 'trust', 'badges', 'zk',
  'communities', 'lists', 'bookmarks', 'polls', 'mute', 'follow',
  'relays', 'nip05', 'node-tie',
  'calendar', 'onboarding', 'lightbox',
  
  // Layer 6: Advanced
  'graph-explorer', 'content-graph', 'dao', 'analytics',
  'interactions', 'account'
];

fs.mkdirSync(DIST_DIR, { recursive: true });

// Build header
let bundle = '// SNIN Client bundle — ' + new Date().toISOString() + '\n';
bundle += '// Modules: ' + MODULE_ORDER.join(', ') + '\n';
bundle += '(function(){\n"use strict";\n';

// Concatenate all modules
let loaded = 0, missing = 0;
for (const mod of MODULE_ORDER) {
  const filePath = `${MODULES_DIR}/${mod}.js`;
  if (fs.existsSync(filePath)) {
    bundle += `\n// === ${mod}.js ===\n`;
    bundle += fs.readFileSync(filePath, 'utf8');
    bundle += '\n';
    loaded++;
  } else {
    console.warn(`⚠️  Missing module: ${mod}.js`);
    missing++;
  }
}

// Add app.js (main entry point)
bundle += '\n// === app.js (main) ===\n';
bundle += fs.readFileSync(`${JS_DIR}/app.js`, 'utf8');
bundle += '\n})();';

// Write bundle
const outPath = `${DIST_DIR}/bundle.js`;
fs.writeFileSync(outPath, bundle);
const size = fs.statSync(outPath).size;
console.log(`✅ bundle.js: ${(size/1024).toFixed(1)} KB (${loaded} modules + app.js${missing ? ', ' + missing + ' missing' : ''})`);

// Minified version
const minified = bundle
  .replace(/\/\/.*$/gm, '')          // Single-line comments
  .replace(/^\s*\/\*[\s\S]*?\*\//gm, '') // Multi-line comments
  .replace(/\n\s*\n/g, '\n')         // Collapse blank lines
  .replace(/[ \t]+$/gm, '');         // Trailing whitespace

fs.writeFileSync(`${DIST_DIR}/bundle.min.js`, minified);
const minSize = fs.statSync(`${DIST_DIR}/bundle.min.js`).size;
console.log(`✅ bundle.min.js: ${(minSize/1024).toFixed(1)} KB`);
