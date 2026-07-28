// SNIN Client — Visual Effects Module
// Canvas particle network, aurora orbs, star field, grid overlay, cyber effects
// Extracted from app.js (2026-07-20, Phase 2.1)

function injectAuroraOrb() {
  const orb = document.createElement('div');
  orb.className = 'aurora-orb-3-injected';
  document.body.appendChild(orb);
}

// V8.44 — CSS star field (50 twinkling dots)
function injectStarField() {
  const container = document.createElement('div');
  container.className = 'star-field';
  // 120 stars (V8.50 — up from 50, with neon color variants)
  const hues = [195, 195, 280, 320, 170, 40, 195, 280, 320, 195]; // weighted to cyan
  for (let i = 0; i < 120; i++) {
    const dot = document.createElement('div');
    dot.className = 'star-dot';
    dot.style.left = Math.random() * 100 + '%';
    dot.style.top = Math.random() * 100 + '%';
    dot.style.setProperty('--twinkle-dur', (2 + Math.random() * 6) + 's');
    dot.style.setProperty('--twinkle-delay', Math.random() * 7 + 's');
    const size = 1.5 + Math.random() * 2.5;
    dot.style.width = dot.style.height = size + 'px';
    const hue = hues[i % hues.length];
    dot.style.background = `hsl(${hue}, 90%, 70%)`;
    dot.style.boxShadow = `0 0 ${size * 2}px hsl(${hue}, 100%, 60%)`;
    container.appendChild(dot);
  }
  document.body.appendChild(container);
}

function injectGrid() {
  const grid = document.createElement('div');
  grid.className = 'grid-overlay';
  document.body.appendChild(grid);
}

// V8.50 — Cyber corners & data streams
function injectCyberEffects() {
  for (let i = 0; i < 4; i++) {
    const corner = document.createElement('div');
    corner.className = 'cyber-corner';
    document.body.appendChild(corner);
  }
  const streamL = document.createElement('div');
  streamL.className = 'data-stream-left';
  document.body.appendChild(streamL);
  const streamR = document.createElement('div');
  streamR.className = 'data-stream-right';
  document.body.appendChild(streamR);
}

// ─── Canvas Particle Network V8.44 (enhanced) ───
function initCanvasBG() {
  // Skip canvas on mobile — too heavy
  if (window.innerWidth < 640) return;
  const canvas = document.getElementById('bgCanvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  let particles = [], nodes = [], w, h, mouseX = -999, mouseY = -999;

  function resize() {
    w = canvas.width = window.innerWidth;
    h = canvas.height = window.innerHeight;
  }
  resize();
  window.addEventListener('resize', resize);

  // Track mouse/touch for interaction
  document.addEventListener('mousemove', e => { mouseX = e.clientX; mouseY = e.clientY; });
  document.addEventListener('mouseleave', () => { mouseX = -999; mouseY = -999; });
  document.addEventListener('touchmove', e => {
    if (e.touches.length) { mouseX = e.touches[0].clientX; mouseY = e.touches[0].clientY; }
  });
  document.addEventListener('touchend', () => { mouseX = -999; mouseY = -999; });

  // 200 particles (V8.50 — doubled from 100, brighter)
  for (let i = 0; i < 200; i++) {
    particles.push({
      x: Math.random() * w, y: Math.random() * h,
      vx: (Math.random() - 0.5) * 0.4,
      vy: (Math.random() - 0.5) * 0.4,
      r: Math.random() * 2.8 + 0.6,
      alpha: Math.random() * 0.4 + 0.08,
      hue: [195, 280, 320, 170, 210][Math.floor(Math.random() * 5)],
      twinkle: Math.random() * Math.PI * 2,
      twinkleSpeed: 0.02 + Math.random() * 0.05
    });
  }

  // 8 pulsating nodes (brighter anchors)
  for (let i = 0; i < 8; i++) {
    nodes.push({
      x: Math.random() * w, y: Math.random() * h,
      r: 3 + Math.random() * 5,
      hue: [195, 280, 320][i % 3],
      phase: Math.random() * Math.PI * 2,
      speed: 0.015 + Math.random() * 0.03
    });
  }

  let frame = 0;
  function draw() {
    ctx.clearRect(0, 0, w, h);
    frame++;

    // Draw pulsating nodes first (behind particles)
    for (const n of nodes) {
      const pulse = 0.6 + 0.4 * Math.sin(frame * n.speed + n.phase);
      const r = n.r * pulse;
      ctx.beginPath();
      ctx.arc(n.x, n.y, r * 2.5, 0, Math.PI * 2);
      ctx.fillStyle = `hsla(${n.hue}, 100%, 70%, ${0.08 * pulse})`;
      ctx.fill();
      ctx.beginPath();
      ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
      ctx.fillStyle = `hsla(${n.hue}, 100%, 80%, ${0.3 * pulse})`;
      ctx.fill();
    }

    // Draw particles
    for (const p of particles) {
      p.x += p.vx; p.y += p.vy;
      if (p.x < 0) p.x = w; if (p.x > w) p.x = 0;
      if (p.y < 0) p.y = h; if (p.y > h) p.y = 0;

      const twinkleAlpha = p.alpha * (0.4 + 0.6 * Math.sin(frame * p.twinkleSpeed + p.twinkle));

      // Mouse attraction
      if (mouseX > 0 && mouseY > 0) {
        const dx = mouseX - p.x, dy = mouseY - p.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 250) {
          p.vx += dx / dist * 0.03;
          p.vy += dy / dist * 0.03;
        }
      }

      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fillStyle = `hsla(${p.hue}, 100%, 75%, ${twinkleAlpha})`;
      ctx.fill();

      // Glow halo
      if (twinkleAlpha > 0.15) {
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r * 4, 0, Math.PI * 2);
        ctx.fillStyle = `hsla(${p.hue}, 100%, 70%, ${twinkleAlpha * 0.12})`;
        ctx.fill();
      }

      // Connect nearby
      for (let j = particles.indexOf(p) + 1; j < particles.length; j++) {
        const q = particles[j];
        const dx = p.x - q.x, dy = p.y - q.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 180) {
          ctx.beginPath();
          ctx.moveTo(p.x, p.y);
          ctx.lineTo(q.x, q.y);
          const lineAlpha = 0.1 * (1 - dist / 180);
          ctx.strokeStyle = `hsla(${(p.hue+q.hue)/2}, 80%, 65%, ${lineAlpha})`;
          ctx.lineWidth = 0.8;
          ctx.stroke();
        }
      }
    }
    requestAnimationFrame(draw);
  }
  draw();
}

// ══════════════════════════════════════════
// V11.0 — Scroll Reveal Observer (extracted from app.js)
// ══════════════════════════════════════════
// ══════════════════════════════════════════
// ══════════════════════════════════════════
// V8.0 — Scroll Reveal Observer + Ripple
// ══════════════════════════════════════════
(function() {
  // Scroll reveal
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(e => { if (e.isIntersecting) e.target.classList.add('visible'); });
  }, { threshold: 0.1 });
  
  // Observe all .reveal elements after each tab switch
  const origSwitchTab = switchTab;
  switchTab = function(name, ...args) {
    origSwitchTab(name, ...args);
    setTimeout(() => {
      document.querySelectorAll('.reveal').forEach(el => observer.observe(el));
    }, 100);
  };
  
  // Observe initially
  setTimeout(() => {
    document.querySelectorAll('.reveal').forEach(el => observer.observe(el));
  }, 200);
})();

// V8.28 — LONG-FORM ARTICLE EDITOR (kind:30023)
// ══════════════════════════════════════════
// V11.0 — Stagger, Click Burst, Magnetic Tilt, Profile Fallback (extracted from app.js)
// ══════════════════════════════════════════

// V8.52 — Apply stagger indices to all cards after render
function applyCardStagger(container) {
  if (!container) return;
  const cards = container.querySelectorAll('.post-card');
  cards.forEach((card, i) => {
    card.style.setProperty('--stagger-index', i);
  });
}

// V8.52 — Click burst particles
function setupClickBurst() {
  document.addEventListener('click', function(e) {
    const container = document.createElement('div');
    container.className = 'click-burst';
    container.style.left = e.clientX + 'px';
    container.style.top = e.clientY + 'px';
    
    const hue = [195, 280, 320, 170][Math.floor(Math.random() * 4)];
    for (let i = 0; i < 12; i++) {
      const particle = document.createElement('div');
      particle.className = 'click-particle';
      const angle = (i / 12) * Math.PI * 2;
      const distance = 20 + Math.random() * 40;
      particle.style.setProperty('--bx', Math.cos(angle) * distance + 'px');
      particle.style.setProperty('--by', Math.sin(angle) * distance + 'px');
      particle.style.background = `hsl(${hue + Math.random() * 30}, 100%, 65%)`;
      particle.style.boxShadow = `0 0 6px hsl(${hue}, 100%, 60%)`;
      container.appendChild(particle);
    }
    
    document.body.appendChild(container);
    setTimeout(() => container.remove(), 900);
  });
}

// V8.58 — Magnetic card tilt (3D perspective on hover) — MOBILE OPTIMIZED
let _tiltRAF = null;
// ─── Profile Click Fallback (V8.98) ───
// Delegated click handler catches any avatar/author click even if inline onclick fails
function setupProfileClickFallback() {
  document.addEventListener('click', function(e) {
    // Check if click was on an avatar or author name
    var target = e.target;
    // Walk up to find data-pubkey or onclick="showProfile
    for (var i = 0; i < 5 && target; i++) {
      var pk = target.getAttribute('data-pubkey');
      var onclick = target.getAttribute('onclick');
      if (pk && pk.length === 64) {
        // Found a pubkey — trigger showProfile if not already handled
        if (!onclick || !onclick.includes('showProfile')) {
          e.preventDefault();
          e.stopPropagation();
          showProfile(pk);
        }
        return;
      }
      if (onclick && onclick.includes('showProfile')) {
        return; // Already handled by inline onclick
      }
      target = target.parentElement;
    }
  }, {capture: false, passive: false});
}

let _tiltEnabled = true;

function setupMagneticTilt() {
  // Disable tilt on mobile — touch tilt is janky and kills performance
  const isMobile = window.innerWidth < 640;
  if (isMobile) { _tiltEnabled = false; return; }
  
  document.addEventListener('mousemove', function(e) {
    if (!_tiltEnabled) return;
    if (_tiltRAF) return; // throttle to one per frame
    
    _tiltRAF = requestAnimationFrame(() => {
      _tiltRAF = null;
      const cards = document.querySelectorAll('.post-card');
      cards.forEach(card => {
        const rect = card.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const centerX = rect.width / 2;
        const centerY = rect.height / 2;
        const maxTilt = 5;
        
        if (x > -50 && x < rect.width + 50 && y > -50 && y < rect.height + 50) {
          const tiltX = ((y - centerY) / centerY) * maxTilt;
          const tiltY = ((x - centerX) / centerX) * -maxTilt;
          card.style.transform = `perspective(800px) rotateX(${tiltX}deg) rotateY(${tiltY}deg) scale(1.02)`;
          card.style.transition = 'transform 0.15s ease-out';
        } else {
          card.style.transform = '';
          card.style.transition = 'transform 0.5s ease';
        }
      });
    });
  });
}

// ══════════════════════════════════════════
// V12.3 — Scroll progress bar
// ══════════════════════════════════════════
(function() {
  var bar = document.getElementById('scrollProgress');
  if (!bar) return;
  window.addEventListener('scroll', function() {
    var scrollTop = window.scrollY || document.documentElement.scrollTop;
    var docHeight = document.documentElement.scrollHeight - window.innerHeight;
    var progress = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;
    bar.style.width = Math.min(progress, 100) + '%';
  }, {passive: true});
})();

// ══════════════════════════════════════════
