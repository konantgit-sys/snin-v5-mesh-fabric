// Проверка аудит-пака в браузере: sha256 через WebCrypto, подписи BIP340 — локальная библиотека.
// Ничего не отправляем на сервер: все вычисления идят здесь, в этой вкладке.
// Формулы должны совпадать с питоном байт-в-байт:
//   content_hash = sha256(payload)
//   unsigned     = sha256(prev_hash + "|" + content_hash + "|" + ts)
//   block_hash   = sha256(unsigned + "|" + signature)
//   подпись      = BIP340-schnorr(unsigned) ключом подписанта
//   корень       = block_hash верхнего блока, подпись корня — того же блока

import { schnorr } from './vendor/curves-schnorr.esm.js';

const $ = (id) => document.getElementById(id);
// Токен запуска: если во время проверки нажали другую кнопку, старый прогон
// обязан замолчать, а новый — начаться. Иначе клик «проглатывается» и на экране
// остаётся чужой вердикт.
let runToken = 0;
const enc = new TextEncoder();
const hexToBytes = (h) => Uint8Array.from(h.match(/../g).map((b) => parseInt(b, 16)));
const bytesToHex = (b) => Array.from(new Uint8Array(b)).map((x) => x.toString(16).padStart(2, '0')).join('');

async function sha256Hex(str) {
  const digest = await crypto.subtle.digest('SHA-256', enc.encode(str));
  return bytesToHex(digest);
}

async function fileSha256Hex(url) {
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`не удалось прочитать ${url}: HTTP ${res.status}`);
  return bytesToHex(await crypto.subtle.digest('SHA-256', await res.arrayBuffer()));
}

async function loadJson(url) {
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`не удалось прочитать ${url}: HTTP ${res.status}`);
  return res.json();
}

async function loadJsonl(url) {
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`не удалось прочитать ${url}: HTTP ${res.status}`);
  const text = await res.text();
  return text.trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function showVerdict() {
  const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  $('status').scrollIntoView({ behavior: reduce ? 'auto' : 'smooth', block: 'center' });
}

function setStatus(kind, title, note) {
  const box = $('status');
  box.className = `status status--${kind}`;
  $('status-title').textContent = title;
  $('status-note').textContent = note;
}

function setProgress(done, total) {
  const bar = $('progress-bar');
  const ratio = total ? Math.min(1, done / total) : 0;
  bar.style.transform = `scaleX(${ratio.toFixed(4)})`;
}

function metric(id, text) { $(id).textContent = text; }

function renderErrors(list) {
  const card = $('errors-card');
  const ul = $('errors');
  ul.innerHTML = '';
  if (!list.length) { card.hidden = true; return; }
  card.hidden = false;
  list.slice(0, 40).forEach((msg) => {
    const li = document.createElement('li');
    li.textContent = msg;
    ul.appendChild(li);
  });
  if (list.length > 40) {
    const li = document.createElement('li');
    li.textContent = `…и ещё ${list.length - 40} расхождений`;
    ul.appendChild(li);
  }
}

function renderActivity(events, live) {
  const byAgentAction = new Map();
  const bySigner = new Map();
  const byHour = new Array(24).fill(0);
  for (const e of events) {
    const key = `${e.agent_id}\u0000${e.action}`;
    byAgentAction.set(key, (byAgentAction.get(key) || 0) + 1);
    bySigner.set(e.signer_pub, (bySigner.get(e.signer_pub) || 0) + 1);
    byHour[new Date(e.ts * 1000).getUTCHours()] += 1;
  }

  const tbodyA = $('tbl-agents').querySelector('tbody');
  tbodyA.innerHTML = '';
  [...byAgentAction.entries()]
    .sort((a, b) => b[1] - a[1]).slice(0, 10)
    .forEach(([key, n]) => {
      const [agent, action] = key.split('\u0000');
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${agent || '—'}</td><td>${action || '—'}</td><td class="num">${n.toLocaleString('ru-RU')}</td>`;
      tbodyA.appendChild(tr);
    });

  const tbodyS = $('tbl-signers').querySelector('tbody');
  tbodyS.innerHTML = '';
  [...bySigner.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).forEach(([pub, n]) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td><code>${(pub || '').slice(0, 16)}…</code></td><td class="num">${n.toLocaleString('ru-RU')}</td>`;
    tbodyS.appendChild(tr);
  });

  const hoursBox = $('hours');
  hoursBox.innerHTML = '';
  const max = Math.max(1, ...byHour);
  byHour.forEach((n, h) => {
    const col = document.createElement('div');
    col.className = 'hours__col';
    col.title = `${String(h).padStart(2, '0')}:00 UTC — ${n}`;
    const bar = document.createElement('div');
    bar.className = 'hours__bar';
    bar.style.height = `${Math.max(2, (n / max) * 78)}px`;
    col.appendChild(bar);
    if (h % 6 === 0) {
      const lab = document.createElement('div');
      lab.className = 'hours__label';
      lab.textContent = String(h).padStart(2, '0');
      col.appendChild(lab);
    }
    hoursBox.appendChild(col);
  });
  requestAnimationFrame(() => {
    hoursBox.querySelectorAll('.hours__bar').forEach((b) => {
      b.style.transform = `scaleY(${Math.max(0.02, parseFloat(b.style.height) / 78)})`;
    });
  });

  const recent = $('recent');
  recent.innerHTML = '';
  [...events].sort((a, b) => b.id - a.id).slice(0, 12).forEach((e) => {
    let detail = '';
    try {
      const p = JSON.parse(e.payload || '{}');
      detail = p.summary || p.tool || p.note || '';
    } catch (_) { detail = ''; }
    const li = document.createElement('li');
    const when = new Date(e.ts * 1000).toISOString().slice(5, 16).replace('T', ' ');
    li.innerHTML = `<span class="tag">${e.agent_id || '—'}</span>`
      + `<span class="when">${when} UTC</span>`
      + `<span>#${e.id} ${e.action}${detail ? ' — ' + detail.slice(0, 110) : ''}</span>`;
    recent.appendChild(li);
  });

  $('activity-card').hidden = false;
  $('metrics').hidden = false;
  metric('m-signers', String(bySigner.size));
  if (live) $('activity-card').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function renderLimits(manifest) {
  const box = $('limits');
  box.innerHTML = '';
  const limits = manifest.limits || {};
  const items = [
    `В пакете ${Number(limits.событий_в_пакете || 0).toLocaleString('ru-RU')} блоков — это непрерывный хвост цепочки, а не весь журнал.`,
    'Проверка связей доказывает, что записи не переписаны после публикации корня. Она не доказывает, что в цепочку записано всё, что агент делал.',
    'Payload включён целиком: без него нельзя пересчитать хеш. Маскирование персональных данных до записи — отдельная задача.',
    'Подписантов несколько: архитектурный ключ и ключи самих агентов. Один подписант означал бы самоаттестацию — здесь её нет.',
  ];
  items.forEach((t) => {
    const li = document.createElement('li');
    li.textContent = t;
    box.appendChild(li);
  });
  $('limits-card').hidden = false;
}

async function check(packDir, label) {
  if (!self.crypto || !crypto.subtle) {
    setStatus('fail', 'Проверка не выполнена', 'Браузер не дал WebCrypto: нужен https и современный браузер.');
    return;
  }
  const myToken = ++runToken;
  const isCurrent = () => myToken === runToken;
  $('run').setAttribute('aria-busy', 'true');
  $('run-tampered').setAttribute('aria-busy', 'true');
  $('errors-card').hidden = true;
  $('manifest-card').hidden = true;
  setStatus('run', `Проверяю: ${label}`, 'Хеши считает ваш браузер…');
  $('progress').hidden = false;
  setProgress(0, 1);

  try {
    const manifest = await loadJson(`${packDir}/manifest.json`);
    const errors = [];

    // 1. целостность файлов
    let filesOk = 0;
    const fileNames = Object.keys(manifest.files || {});
    for (let i = 0; i < fileNames.length; i++) {
      if (!isCurrent()) return;
      const name = fileNames[i];
      const got = await fileSha256Hex(`${packDir}/${name}`);
      if (got === manifest.files[name].sha256) filesOk += 1;
      else errors.push(`файл ${name}: sha256 не совпал с манифестом`);
      setProgress(i + 1, fileNames.length + 1);
    }

    // 2. блоки
    const events = await loadJsonl(`${packDir}/events.jsonl`);
    let hashOk = 0, sigOk = 0, linksOk = 0;
    let prevExpected = null;
    for (let i = 0; i < events.length; i++) {
      if (!isCurrent()) return;
      const e = events[i];
      const ch = await sha256Hex(e.payload);
      const un = await sha256Hex(`${e.prev_hash}|${ch}|${e.ts}`);
      const bh = await sha256Hex(`${un}|${e.signature}`);
      if (ch === e.payload_hash && bh === e.block_hash) hashOk += 1;
      else errors.push(`блок ${e.id}: хеш не совпал (payload_hash ${ch === e.payload_hash ? 'ок' : 'НЕТ'}, block_hash ${bh === e.block_hash ? 'ок' : 'НЕТ'})`);
      let sig = false;
      try { sig = schnorr.verify(hexToBytes(e.signature), hexToBytes(un), hexToBytes(e.signer_pub)); } catch (_) { sig = false; }
      if (sig) sigOk += 1; else errors.push(`блок ${e.id}: подпись BIP340 недействительна`);
      if (prevExpected === null || e.prev_hash === prevExpected) linksOk += 1;
      else errors.push(`блок ${e.id}: разрыв связи с предыдущим блоком`);
      prevExpected = e.block_hash;
      if (i % 20 === 0) setProgress(events.length + i, events.length * 3);
    }

    // 3. корни реестра
    const roots = await loadJsonl(`${packDir}/roots.jsonl`);
    const byHeight = new Map(events.map((e) => [e.id, e]));
    let rootsOk = 0;
    for (const r of roots) {
      if (!isCurrent()) return;
      const e = byHeight.get(r.height);
      if (!e) continue;
      const un = await sha256Hex(`${e.prev_hash}|${await sha256Hex(e.payload)}|${e.ts}`);
      let sig = false;
      try { sig = schnorr.verify(hexToBytes(r.sig), hexToBytes(un), hexToBytes(r.pubkey)); } catch (_) { sig = false; }
      if (r.root === e.block_hash && sig) rootsOk += 1;
      else errors.push(`корень высоты ${r.height}: не соответствует блоку или подпись корня недействительна`);
    }

    metric('m-files', `${filesOk} / ${fileNames.length}`);
    metric('m-events', `${hashOk.toLocaleString('ru-RU')} / ${events.length.toLocaleString('ru-RU')}`);
    metric('m-signs', `${sigOk.toLocaleString('ru-RU')} / ${events.length.toLocaleString('ru-RU')}`);
    metric('m-links', `${linksOk.toLocaleString('ru-RU')} / ${events.length.toLocaleString('ru-RU')}`);
    metric('m-roots', `${rootsOk} / ${roots.length}`);
    renderErrors(errors);
    renderLimits(manifest);
    renderActivity(events, false);

    $('manifest-json').textContent = JSON.stringify(manifest, null, 2);
    $('foot-meta').textContent = `${manifest.pack_version} · собран ${manifest.generated_at_utc} · высоты `
      + `${manifest.chain.высоты[0]}–${manifest.chain.высоты[1]} · файлов проверено ${filesOk}/${fileNames.length}`;

    setProgress(1, 1);
    if (errors.length === 0) {
      setStatus('ok', `ПРОВЕРЕНО · ${label}`, `Сошлось: ${events.length.toLocaleString('ru-RU')} блоков (хеши, подписи, связи) и ${rootsOk} корней. `
        + 'Расхождений нет — выгрузка не переписана после публикации корней.');
    } else {
      setStatus('fail', `НЕ СОВПАЛО · ${label}`, `Найдено расхождений: ${errors.length}. Первое: ${errors[0]}`
        + ' Подробности — в разделе «Расхождения».');
    }
    showVerdict();
  } catch (err) {
    if (!isCurrent()) return;
    setStatus('fail', `Проверка не выполнена · ${label}`, `${err.message} — это не «подделка», а сбой чтения. Повторите или скажите нам.`);
    showVerdict();
  } finally {
    if (isCurrent()) {
      $('run').removeAttribute('aria-busy');
      $('run-tampered').removeAttribute('aria-busy');
    }
  }
}

$('run').addEventListener('click', () => check('./pack', 'рабочий пакет'));
$('run-tampered').addEventListener('click', () => check('./pack/tampered', 'подменённый пакет'));
$('toggle-manifest').addEventListener('click', async () => {
  const card = $('manifest-card');
  card.hidden = !card.hidden;
  if (!card.hidden && !$('manifest-json').textContent.trim()) {
    try { $('manifest-json').textContent = JSON.stringify(await loadJson('./pack/manifest.json'), null, 2); }
    catch (e) { $('manifest-json').textContent = `не удалось прочитать манифест: ${e.message}`; }
  }
});
