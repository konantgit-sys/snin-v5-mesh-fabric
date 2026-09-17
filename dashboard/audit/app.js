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

// Канонический JSON: ключи по алфавиту, без пробелов. Ровно так же это тело
// сериализуется на сервере, когда считается payload_hash — иначе проверка
// в браузере не сошлась бы на честной аттестации.
function canonicalJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  return `{${Object.keys(value).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(value[k])}`).join(',')}}`;
}

function renderWitnesses(rows, rejected, manifest, isNegative) {
  const tbody = $('tbl-witnesses').querySelector('tbody');
  tbody.innerHTML = '';
  rows.slice(0, 30).forEach((r) => {
    const tr = document.createElement('tr');
    const tag = document.createElement('td');
    tag.textContent = r.tag;
    const src = document.createElement('td');
    src.textContent = String(r.source).slice(0, 44);
    const h = document.createElement('td');
    h.className = 'num';
    h.textContent = r.height === null || r.height === undefined ? '—' : String(r.height);
    const verdict = document.createElement('td');
    const span = document.createElement('span');
    span.className = `wt__state wt__state--${r.state === 'ok' ? 'ok' : 'fail'}`;
    span.textContent = r.state === 'ok' ? 'подпись верна' : 'не зачтена';
    span.title = r.why || '';
    verdict.appendChild(span);
    tr.append(tag, src, h, verdict);
    tbody.appendChild(tr);
  });

  const box = $('witness-rejected');
  box.innerHTML = '';
  if (rejected.length) {
    rejected.slice(0, 10).forEach((t) => {
      const li = document.createElement('li');
      li.textContent = t;
      box.appendChild(li);
    });
    box.hidden = false;
  } else box.hidden = true;

  const wman = manifest.witnesses || {};
  const need = Number(wman.required || 2);
  const needRelay = Number(wman.required_relay || 1);
  const ok = rows.filter((r) => r.state === 'ok').length;
  const relay = rows.filter((r) => r.state === 'ok' && String(r.source).startsWith('relay')).length;
  $('witness-note').textContent = (isNegative ? 'Негативный тест: файл подписей свидетелей подменён на пустой. ' : '')
    + `Чекпоинт пака — высота ${(manifest.witnesses || {}).чекпоинт ? (manifest.witnesses.чекпоинт.height ?? '—') : '—'}. `
    + `Зачтено подписей: ${ok} из ${need}, с независимым путём чтения — ${relay} из ${needRelay}. `
    + 'Нужны разные ключи: один ключ — один голос, и подпись, сделанная ключом самого чекпоинта, не считается. '
    + 'Payload_hash пересчитан из тела аттестации, подпись BIP340 проверена по нему и публичному ключу свидетеля — здесь, в браузере.';
  $('witness-card').hidden = false;
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
    'Чекпоинт подтверждают свидетели: нужны две подписи разных ключей, и хотя бы одна — с чекпоинтом, прочитанным с внешних релеев. Пока оба ключа наши, независимость техническая (разные ключи и разные пути чтения), а не организационная.',
    'Проверка свидетелей — не про то, что работа была правильной. Она про то, что чекпоинт был именно таким и не был переписан задним числом.',
  ];
  items.forEach((t) => {
    const li = document.createElement('li');
    li.textContent = t;
    box.appendChild(li);
  });
  $('limits-card').hidden = false;
}

async function check(packDir, label, opts = {}) {
  if (!self.crypto || !crypto.subtle) {
    setStatus('fail', 'Проверка не выполнена', 'Браузер не дал WebCrypto: нужен https и современный браузер.');
    return;
  }
  const myToken = ++runToken;
  const isCurrent = () => myToken === runToken;
  const witOverride = opts.witnessOverride || null;
  ['run', 'run-tampered', 'run-no-witness'].forEach((id) => $(id).setAttribute('aria-busy', 'true'));
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
      const swapped = name === 'witnesses.jsonl' && witOverride;
      const got = await fileSha256Hex(swapped ? witOverride.path : `${packDir}/${name}`);
      const declared = swapped ? witOverride.sha : manifest.files[name].sha256;
      if (got === declared) filesOk += 1;
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

    // 4. свидетели чекпоинта: без двух подписей (одна с релеев) проверка не проходит
    const wman = manifest.witnesses || {};
    const wNeed = Number(wman.required || 2);
    const wNeedRelay = Number(wman.required_relay || 1);
    const cpHeight = (wman.чекпоинт || {}).height;
    const cpBlock = byHeight.get(cpHeight);
    const cpSigner = cpBlock ? cpBlock.signer_pub : '';
    let atts = [];
    try { atts = await loadJsonl(witOverride ? witOverride.path : `${packDir}/witnesses.jsonl`); }
    catch (_) { atts = []; }
    // В файле — история подписей по разным высотам. Считаем только те, что
    // относятся к чекпоинту этого пака: иначе голоса одного свидетеля по
    // разным высотам гасят друг друга как «дубли».
    const attsInFile = atts.length;
    atts = atts.filter((a) => cpHeight !== undefined && cpHeight !== null
      && Number(a.height) === Number(cpHeight));
    const seenW = new Set();
    const wRows = [];
    const wRejected = [];
    let wRelay = 0;
    for (const a of atts) {
      if (!isCurrent()) return;
      const tag = a.witness_id || String(a.witness_pub || '?').slice(0, 12);
      const body = a.payload || {};
      const ph = await sha256Hex(canonicalJson(body));
      let state = 'ok';
      let why = 'подпись верна';
      if (ph !== a.payload_hash) { state = 'fail'; why = 'payload_hash не совпал с телом'; }
      else if (body.witness_pub !== a.witness_pub || body.root !== a.root
               || Number(body.height) !== Number(a.height)) { state = 'fail'; why = 'поля не совпали с телом'; }
      else {
        let sig = false;
        try { sig = schnorr.verify(hexToBytes(a.sig), hexToBytes(ph), hexToBytes(a.witness_pub)); } catch (_) { sig = false; }
        if (!sig) { state = 'fail'; why = 'подпись BIP340 недействительна'; }
        else if (cpSigner && a.witness_pub === cpSigner) { state = 'fail'; why = 'самоаттестация: тот же ключ, что подписал чекпоинт'; }
        else if (seenW.has(a.witness_pub)) { state = 'fail'; why = 'дубль ключа: один ключ — один голос'; }
        else if (cpBlock && Number(a.height) === cpBlock.id && a.root !== cpBlock.block_hash) {
          state = 'fail'; why = `корень не совпал с блоком высоты ${a.height}`;
        }
      }
      if (state === 'ok') {
        seenW.add(a.witness_pub);
        if (String(a.source || '').startsWith('relay')) wRelay += 1;
      } else wRejected.push(`${tag}: ${why}`);
      wRows.push({ tag, source: a.source || '—', height: a.height, state, why });
      setProgress(events.length * 3 + wRows.length, events.length * 3 + atts.length + 1);
    }
    if (seenW.size < wNeed) {
      errors.push(`чекпоинт не подтверждён свидетелями: ${seenW.size} из ${wNeed}`
        + (wRejected.length ? ` (отклонено: ${wRejected.join('; ')})` : ''));
    } else if (wRelay < wNeedRelay) {
      errors.push(`нет независимого пути чтения у свидетелей: ${wRelay} из ${wNeedRelay}`);
    }

    metric('m-files', `${filesOk} / ${fileNames.length}`);
    metric('m-events', `${hashOk.toLocaleString('ru-RU')} / ${events.length.toLocaleString('ru-RU')}`);
    metric('m-signs', `${sigOk.toLocaleString('ru-RU')} / ${events.length.toLocaleString('ru-RU')}`);
    metric('m-links', `${linksOk.toLocaleString('ru-RU')} / ${events.length.toLocaleString('ru-RU')}`);
    metric('m-roots', `${rootsOk} / ${roots.length}`);
    metric('m-witnesses', `${seenW.size} / ${wNeed}`);
    renderErrors(errors);
    renderWitnesses(wRows, wRejected, manifest, Boolean(witOverride));
    renderLimits(manifest);
    renderActivity(events, false);

    $('manifest-json').textContent = JSON.stringify(manifest, null, 2);
    $('foot-meta').textContent = `${manifest.pack_version} · собран ${manifest.generated_at_utc} · высоты `
      + `${manifest.chain.высоты[0]}–${manifest.chain.высоты[1]} · файлов проверено ${filesOk}/${fileNames.length}`;

    setProgress(1, 1);
    if (errors.length === 0) {
      setStatus('ok', `ПРОВЕРЕНО · ${label}`, `Сошлось: ${events.length.toLocaleString('ru-RU')} блоков (хеши, подписи, связи), ${rootsOk} корней `
        + `и подписи свидетелей чекпоинта — ${seenW.size} из ${wNeed}, независимых путей чтения ${wRelay}. `
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
      ['run', 'run-tampered', 'run-no-witness'].forEach((id) => $(id).removeAttribute('aria-busy'));
    }
  }
}

$('run').addEventListener('click', () => check('./pack', 'рабочий пакет'));
$('run-tampered').addEventListener('click', () => check('./pack/tampered', 'подменённый пакет'));
$('run-no-witness').addEventListener('click', async () => {
  try {
    const man = await loadJson('./pack/manifest.json');
    const fx = (man.witnesses || {}).негативный_тест || {};
    const dir = String(fx.dir || 'no-witness');
    const file = String(fx.файл || 'witnesses.jsonl');
    check('./pack', 'пакет без свидетелей', {
      witnessOverride: { path: `./pack/${dir}/${file}`, sha: fx.sha256 || '' },
    });
  } catch (e) {
    setStatus('fail', 'Проверка не выполнена · пакет без свидетелей',
      `${e.message} — это сбой чтения, а не вердикт. Повторите.`);
    showVerdict();
  }
});
$('toggle-manifest').addEventListener('click', async () => {
  const card = $('manifest-card');
  card.hidden = !card.hidden;
  if (!card.hidden && !$('manifest-json').textContent.trim()) {
    try { $('manifest-json').textContent = JSON.stringify(await loadJson('./pack/manifest.json'), null, 2); }
    catch (e) { $('manifest-json').textContent = `не удалось прочитать манифест: ${e.message}`; }
  }
});
