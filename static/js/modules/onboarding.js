// ══════════════════════════════════════════
// V11.0 — Onboarding Module — Multi-step wizard (extracted from app.js)
// Step 1: Key Setup → Step 2: Profile → Step 3: Follow → Done!
// ══════════════════════════════════════════

// ══════════════════════════════════════════
// ONBOARDING — Multi-step Wizard (V8.85)
// Step 1: Key Setup → Step 2: Profile → Step 3: Follow → Done!
// ══════════════════════════════════════════

// Well-known Nostr profiles for onboarding
const ONBOARD_SUGGESTIONS = [
  {pubkey:'82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2', npub:'npub1sg6plzptd64u62a878hep2kev88swjh3tw00gjsfl8f237lmu63q0uf63m', intent:t('onboardAuthorLabel')},
  {pubkey:'e88a691e98d9987c964521dff60025f60700378a4879180dcbbb4a5027850411', npub:'npub180cvv07tjdrrgpa0j7j7tmnyl2yr6yr7l8j4s3evf6u64th6gkwsyjh6w6', intent:'Jack — creator of Twitter'},
  {pubkey:'32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245', npub:'npub1xtscya34g58tk0z605fvr788k263gsu6cy9x0mhnm87echrgufzsevkk5s', intent:'jb55 — Damus creator'},
  {pubkey:'97c70a44366a6535c145b333f973ea86dfdc2d7a99da618c40c64705ad98e322', npub:'npub1v0lxxxxutpvrelsksy8cdhgfux9l6a42hsj2qzquu2zk7vc9qnkszrqj49', intent:'hodlbod — Coracle creator'},
  {pubkey:'3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d', npub:'npub1800xltyrfzxd7tq9z5tm3xmftvqv7fzm3qyfuyqnrm6kq3xrxfrsk5qwp4', intent:'fiatjaf — Nostr protocol'},
  {pubkey:'63fe6318dc58583cfe16810f86dd09e18bfd76aabc24a0081ce2856f330504ed', npub:'npub1k9fmst4hv6klyqf9thy0kx0h9v30yu2d4mquyqrg3yp4v3mjc0gs6m7s8l', intent:'Karnage — Nostr dev'},
  {pubkey:'7bdef7be22dd8e59f4600e044aa53a1cf975a978dc56d1b87fe184e95c8f14b2', npub:'npub1wf4pufsucer5va8g9p0rj5dnhvfeh6d8w0g6eayaep5dhps6t9ws3gwe66', intent:'PABLOF7z — nostr_net'},
  {pubkey:'460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c', npub:'npub1v5ufyg4ls62d34gazm2audqjn0qfn2nkxtl57t2e49me27qm3r2q63fn6q', intent:'Vitor Pamplona — Amethyst'},
  {pubkey:'fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52', npub:'npub1ulw25ga7qlqkucv7nlwdyn0k47j77n8tfx6pp9qfttu2zs8saqyq63zyj0', intent:'Preston — The Bitcoin Conference'},
  {pubkey:'1bc70a0148b3f316da03fe3b89e23c71c4b9ec8df1d2f7e78426a7df5c8e1c3d', npub:'npub10qr2f0ty7vfjhsh29x7luh6p9x3j7vkndx20zwdxrpq9d9s9ukrq2c6z95', intent:'JeffG — Zap.store'},
];

let onboardCurrentStep = 1;

function checkOnboarding() {
  // Don't show if already completed
  if (localStorage.getItem('snin_onboarded')) return;
  
  // Don't show if user already has accounts AND follows > 0
  if (signerPubkey) {
    const followingEl = document.getElementById('profileFollowing');
    if (followingEl) {
      const contactCount = parseInt(followingEl.textContent) || 0;
      if (contactCount > 0) {
        localStorage.setItem('snin_onboarded', '1');
        return;
      }
    }
  }
  
  // Show onboarding — determine which step to start
  if (!signerPubkey) {
    onboardCurrentStep = 1;
  } else {
    onboardCurrentStep = 2; // already has key, go to profile
  }
  showOnboarding();
}

function showOnboarding() {
  const modal = document.getElementById('onboardModal');
  if (!modal) return;
  modal.style.display = 'flex';
  
  // Hide all steps
  document.querySelectorAll('.onboard-step').forEach(s => s.style.display = 'none');
  document.getElementById('onboardFooter').style.display = 'none';
  document.getElementById('onboardSkipBtn').style.display = '';
  
  // Show current step
  const stepEl = document.getElementById('onboardStep' + onboardCurrentStep);
  if (stepEl) stepEl.style.display = 'block';
  
  // Update step dots
  const dots = document.querySelectorAll('.onboard-dot');
  dots.forEach(d => {
    const s = parseInt(d.dataset.step);
    d.className = 'onboard-dot';
    if (s < onboardCurrentStep) d.classList.add('done');
    if (s === onboardCurrentStep) d.classList.add('active');
  });
  
  // Update title per step
  const title = document.getElementById('onboardTitle');
  const titles = {1: '🔑 Create Your Identity', 2: '👤 Set Up Profile', 3: '🌟 Follow Interesting People'};
  if (title) title.textContent = titles[onboardCurrentStep] || '⚡ Welcome to SNIN';
  
  // Step-specific setup
  if (onboardCurrentStep === 3) {
    renderOnboardSuggestions();
    document.getElementById('onboardFooter').style.display = 'block';
  }
}

function renderOnboardSuggestions() {
  const container = document.getElementById('onboardSuggestions');
  if (!container) return;
  
  let html = '<div style="font-size:13px;color:var(--text-dim);margin-bottom:12px;text-align:center">Follow interesting accounts to fill your feed:</div>';
  
  for (const s of ONBOARD_SUGGESTIONS) {
    html += 
      '<label class="onboard-item" style="display:flex;align-items:center;gap:10px;padding:10px 12px;margin:4px 0;border-radius:10px;background:var(--glass-bg);cursor:pointer">' +
        '<input type="checkbox" class="onboard-check" data-pubkey="' + s.pubkey + '" style="width:18px;height:18px;accent-color:var(--accent)">' +
        '<div style="width:36px;height:36px;border-radius:50%;background:linear-gradient(135deg,hsl(' + hashToHue(s.pubkey) + ',70%,50%),hsl(' + (hashToHue(s.pubkey)+30) + ',70%,30%));flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:14px;color:#fff;font-weight:700">' + s.intent.charAt(0) + '</div>' +
        '<div style="flex:1;min-width:0">' +
          '<div style="font-size:14px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">@' + s.intent.split('—')[0].trim() + '</div>' +
          '<div class="text-2xs-dim">' + s.intent.split('—')[1].trim() + '</div>' +
        '</div>' +
      '</label>';
  }
  
  html += '<div style="text-align:center;margin-top:12px;font-size:12px;color:var(--text-dim)">' + t('onboardingPickAccounts') + '</div>';
  container.innerHTML = html;
  
  // Pre-select first 5
  const checks = container.querySelectorAll('.onboard-check');
  checks.forEach((c, i) => { if (i < 5) c.checked = true; });
}

function goOnboardStep(step) {
  onboardCurrentStep = step;
  showOnboarding();
}

// ─── Step 1: Create Key ───
async function onboardCreateKey() {
  const status = document.getElementById('onboardStep1Status');
  status.style.display = 'block';
  status.textContent = t('onboardGenerateKey');
  status.style.color = 'var(--text-dim)';
  
  try {
    // Use NostrTools to generate key
    const sk = window.NostrTools.generateSecretKey();
    const pk = window.NostrTools.getPublicKey(sk);
    const nsec = window.NostrTools.nip19.nsecEncode(sk);
    
    // Store in sessionStorage temporarily for onboarding
    sessionStorage.setItem('snin_onboard_nsec', sk);
    
    // Derive npub
    const npub = window.NostrTools.nip19.npubEncode(pk);
    
    status.textContent = t('onboardKeyCreated');
    status.style.color = '#4caf50';
    
    // Auto-login with this key
    signerNsec = sk;
    signerPubkey = pk;
    document.getElementById('nsecInput').value = nsec;
    
    // Add to accounts + full login flow
    const accountId = addAccount(pk, sk, '', '');
    setActiveAccountId(accountId);
    updateAccountIndicator();
    document.getElementById('nsecLoginPanel').style.display = 'none';
    document.getElementById('loggedInProfile').style.display = 'block';
    sessionStorage.setItem('snin_pubkey', pk);
    document.getElementById('profileNpub').textContent = pk.slice(0, 16) + '\u2026';
    document.getElementById('profileName').textContent = 'Loading…';
    const h = hashToHue(pk);
    document.getElementById('profileAvatar').innerHTML = '<div style=\"width:100%;height:100%;border-radius:50%;background:linear-gradient(135deg,hsl('+h+',80%,50%),hsl('+(h+35)+',80%,30%));display:flex;align-items:center;justify-content:center;font-size:28px;color:#fff;font-weight:700\">?</div>';
    document.getElementById('profileBio').textContent = t('profileLoading');
    document.getElementById('profilePostsCount').textContent = '-';
    document.getElementById('profileFollowing').textContent = '-';
    document.getElementById('profileFollowers').textContent = '-';
    const postBtn = document.getElementById('postBtn');
    if (postBtn) { postBtn.disabled = false; postBtn.style.background = 'linear-gradient(135deg, var(--accent), #b08060)'; postBtn.style.color = '#fff'; }
    checkComposeAuth();
    
    // Move to step 2
    setTimeout(() => goOnboardStep(2), 600);
  } catch (e) {
    status.textContent = 'Error: ' + e.message;
    status.style.color = '#ff6b6b';
  }
}

// ─── Step 1: Import Key ───
function onboardImportKey() {
  document.getElementById('onboardImportPanel').style.display = 'block';
}

async function onboardDoImport() {
  const input = document.getElementById('onboardNsecInput').value.trim();
  const errorEl = document.getElementById('onboardImportError');
  errorEl.style.display = 'none';
  
  if (!input) {
    errorEl.textContent = t('profileKeyInsert');
    errorEl.style.display = 'block';
    return;
  }
  
  try {
    let sk, pk;
    
    if (input.startsWith('nsec')) {
      const { type, data } = window.NostrTools.nip19.decode(input);
      if (type !== 'nsec') throw new Error('Not an nsec key');
      sk = data;
      pk = window.NostrTools.getPublicKey(sk);
    } else if (/^[0-9a-fA-F]{64}$/.test(input)) {
      sk = input;
      pk = window.NostrTools.getPublicKey(sk);
    } else {
      throw new Error('Unknown key format. Use nsec1… or 64 hex chars');
    }
    
    // Store
    sessionStorage.setItem('snin_onboard_nsec', sk);
    signerNsec = sk;
    signerPubkey = pk;
    
    // Add to accounts + full login flow
    const accountId = addAccount(pk, sk, '', '');
    setActiveAccountId(accountId);
    updateAccountIndicator();
    document.getElementById('nsecLoginPanel').style.display = 'none';
    document.getElementById('loggedInProfile').style.display = 'block';
    sessionStorage.setItem('snin_pubkey', pk);
    
    // Move to step 2
    goOnboardStep(2);
  } catch (e) {
    errorEl.textContent = e.message;
    errorEl.style.display = 'block';
  }
}

// ─── Step 2: Save Profile ───
async function onboardSaveProfile() {
  const status = document.getElementById('onboardStep2Status');
  const name = document.getElementById('onboardName').value.trim();
  const about = document.getElementById('onboardAbout').value.trim();
  const picture = document.getElementById('onboardPicture').value.trim();
  
  if (!name) {
    status.style.display = 'block';
    status.textContent = t('profileNeedName');
    status.style.color = '#ff6b6b';
    return;
  }
  
  status.style.display = 'block';
  status.textContent = t('profilePublishing');
  status.style.color = 'var(--text-dim)';
  
  try {
    // Publish kind:0 profile
    const profile = { name: name };
    if (about) profile.about = about;
    if (picture) profile.picture = picture;
    
    const event = {
      kind: 0,
      pubkey: signerPubkey,
      created_at: Math.floor(Date.now() / 1000),
      tags: [],
      content: JSON.stringify(profile)
    };
    
    // Sign
    const signed = window.NostrTools.finalizeEvent(event, signerNsec);
    
    // Publish to relays
    // Publish via existing WebSocket relay proxy
    wsSend(JSON.stringify(['EVENT', signed]));
    
    // Update profile display
    document.getElementById('profileName').textContent = name;
    document.getElementById('profileNpub').textContent = signerPubkey.slice(0, 16) + '…';
    
    status.textContent = t('profileSaved');
    status.style.color = '#4caf50';
    
    // Move to step 3
    setTimeout(() => goOnboardStep(3), 600);
  } catch (e) {
    status.textContent = 'Error: ' + e.message;
    status.style.color = '#ff6b6b';
  }
}

// ─── Step 3: Bulk Follow ───
async function bulkFollowOnboard() {
  const checks = document.querySelectorAll('.onboard-check:checked');
  if (checks.length === 0) { finishOnboarding(); return; }
  
  const btn = document.getElementById('onboardFollowBtn');
  btn.textContent = 'Following…';
  btn.disabled = true;
  
  let count = 0;
  for (const cb of checks) {
    try {
      await followPubkey(cb.dataset.pubkey, false);
      count++;
      btn.textContent = 'Following… (' + count + '/' + checks.length + ')';
    } catch (e) {
      // skip failed
    }
  }
  
  finishOnboarding();
}

function finishOnboarding() {
  // Mark complete
  localStorage.setItem('snin_onboarded', '1');
  
  // Show celebration
  const modal = document.getElementById('onboardModal');
  const sheet = document.getElementById('onboardSheet');
  
  // Replace content with done screen
  const steps = document.getElementById('onboardSteps');
  if (steps) steps.style.display = 'none';
  
  document.querySelectorAll('.onboard-step').forEach(s => s.style.display = 'none');
  document.getElementById('onboardFooter').style.display = 'none';
  document.getElementById('onboardSkipBtn').style.display = 'none';
  
  document.getElementById('onboardTitle').textContent = '🎉 You\'re All Set!';
  
  // Add celebration content
  let doneDiv = document.getElementById('onboardDone');
  if (!doneDiv) {
    doneDiv = document.createElement('div');
    doneDiv.id = 'onboardDone';
    doneDiv.className = 'modal-body';
    doneDiv.style.textAlign = 'center';
    sheet.appendChild(doneDiv);
  }
  doneDiv.style.display = 'block';
  doneDiv.innerHTML = 
    '<div style="font-size:48px;margin:20px 0 12px">⚡</div>' +
    '<div style="font-size:18px;font-weight:700;margin-bottom:6px">Welcome to SNIN Network!</div>' +
    '<div style="font-size:13px;color:var(--text-dim);margin-bottom:20px">Your sovereign Nostr identity is ready.<br>Explore, connect, and build.</div>' +
    '<button class="btn-primary" onclick="closeOnboard()" style="padding:12px 32px;font-size:15px">Go to Feed →</button>';
}

function closeOnboard() {
  document.getElementById('onboardModal').style.display = 'none';
  localStorage.setItem('snin_onboarded', '1');
  // Cleanup temporary onboard data
  sessionStorage.removeItem('snin_onboard_nsec');
  // Refresh UI
  loadFeed();
  loadMyProfile();
}

// Helper: follow a pubkey via API
async function followPubkey(targetPubkey, updateUI = true) {
  if (!signerNsec || !signerPubkey) return;
  
  const hexKey = window.NostrTools.hexToBytes(signerNsec.slice(4));
  const tags = [['p', targetPubkey]];
  
  const event = {
    kind: 3,
    created_at: Math.floor(Date.now() / 1000),
    tags: tags,
    content: '',
    pubkey: signerPubkey
  };
  
  event.id = window.NostrTools.getEventHash(event);
  event.sig = window.NostrTools.getSignature(event, hexKey);
  
  // Publish to relay via API
  await fetch(API + '/event', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(event)
  });
  
  if (updateUI) {
    followState[targetPubkey] = true;
    updateFollowBtn(targetPubkey);
  }
}

// ══════════════════════════════════════════
