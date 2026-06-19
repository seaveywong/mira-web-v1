/**
 * Mira 本地 API 执行器 - Content Script
 * 注入到 Facebook 页面，自动提取 Access Token（EAA Token）
 * 参考 FB Token Extractor 插件的提取逻辑
 */

// ========== 从 localStorage 提取 EAA Token ==========

function extractFromLocalStorage() {
  const result = { eaa_token: null, eaa_key: null };
  try {
    // 先检查已知的 key
    const knownKeys = [
      'EAA_token',
      'fb_access_token',
      'access_token',
      'fb_messenger_access_token',
    ];
    for (const key of knownKeys) {
      const val = localStorage.getItem(key);
      if (val && val.match(/^EAA[BC]/)) {
        result.eaa_token = val;
        result.eaa_key = key;
        return result;
      }
    }
    // 遍历所有 key 查找 EAA token
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      try {
        const val = localStorage.getItem(key);
        if (val && typeof val === 'string' && val.match(/^EAA[BC]/)) {
          result.eaa_token = val;
          result.eaa_key = key;
          break;
        }
      } catch (e) { /* 某些 key 可能无法访问 */ }
    }
  } catch (e) {
    result._error = e.message;
  }
  return result;
}

// ========== 从 sessionStorage 提取 ==========

function extractFromSessionStorage() {
  const result = { eaa_token: null, eaa_key: null };
  try {
    for (let i = 0; i < sessionStorage.length; i++) {
      const key = sessionStorage.key(i);
      try {
        const val = sessionStorage.getItem(key);
        if (val && typeof val === 'string' && val.match(/^EAA[BC]/)) {
          result.eaa_token = val;
          result.eaa_key = key;
          break;
        }
      } catch (e) { /* ignore */ }
    }
  } catch (e) {
    result._error = e.message;
  }
  return result;
}

// ========== 从页面 JS 全局变量提取 ==========

function extractFromPageJS() {
  const result = {};

  // 1. EAA Token 从 window 变量
  try {
    const windowTokenKeys = [
      '___fbAccessToken',
      '__fbAccessToken',
      'fbAccessToken',
      '_fbToken',
      '__fb_token',
    ];
    for (const key of windowTokenKeys) {
      try {
        const val = window[key];
        if (val && typeof val === 'string' && val.match(/^EAA[BC]/)) {
          result.eaa_token = val;
          result.eaa_source = `window.${key}`;
          break;
        }
      } catch (e) { /* ignore */ }
    }
  } catch (e) { /* ignore */ }

  // 2. fb_dtsg (CSRF token) — 多源提取，不同 FB 页面变量名不同
  try {
    // 方法1: 全局 JS 变量
    for (const key of ['DTSG_INIT_VAL', 'DTSGInitialData', 'DTSG_DATA', 'dtsg_init_val', 'fb_dtsg', 'fbDtsg']) {
      try {
        const val = window[key];
        if (val) {
          if (typeof val === 'string') { result.fb_dtsg = val; break; }
          if (val.token) { result.fb_dtsg = String(val.token); break; }
          if (val.dtsg) { result.fb_dtsg = String(val.dtsg); break; }
        }
      } catch (e) { /* continue */ }
    }

    // 方法2: meta / input 标签
    if (!result.fb_dtsg) {
      const meta = document.querySelector('meta[name="fb_dtsg"], meta[property="fb:fb_dtsg"]');
      if (meta) result.fb_dtsg = meta.content || meta.getAttribute('value');
    }
    if (!result.fb_dtsg) {
      const input = document.querySelector('input[name="fb_dtsg"]');
      if (input) result.fb_dtsg = input.value;
    }

    // 方法3: 内联 script JSON 正则（兜底）
    if (!result.fb_dtsg) {
      const scripts = document.querySelectorAll('script[type="application/json"], script[data-bootloader-hash]');
      for (const s of scripts) {
        const m = s.textContent?.match(/"?DTSGInitialData"?[:\s]+({[^}]+})/);
        if (m) {
          try {
            const parsed = JSON.parse(m[1]);
            if (parsed.token) { result.fb_dtsg = parsed.token; break; }
          } catch (e) {}
        }
        // 直接搜 token 字段
        const tm = s.textContent?.match(/"token"\s*:\s*"([^"]+)"/);
        if (tm) { result.fb_dtsg = tm[1]; break; }
      }
    }
  } catch (e) { /* ignore */ }

  // 3. 从页面内嵌 script 标签搜索 EAA token
  if (!result.eaa_token) {
    try {
      const scripts = document.querySelectorAll('script');
      for (const script of scripts) {
        if (script.textContent && script.textContent.includes('EAA')) {
          const match = script.textContent.match(/EAA[BC]\w{50,}/);
          if (match) {
            result.eaa_token = match[0];
            result.eaa_source = 'inline_script';
            break;
          }
        }
      }
    } catch (e) { /* ignore */ }
  }

  return result;
}

// ========== 综合提取 ==========

function extractAllTokens() {
  const lsResult = extractFromLocalStorage();
  const ssResult = extractFromSessionStorage();
  const pageResult = extractFromPageJS();

  // 优先级：localStorage > sessionStorage > 页面 JS
  const eaa_token = lsResult.eaa_token || ssResult.eaa_token || pageResult.eaa_token || null;
  let eaa_source = '';
  if (lsResult.eaa_token) eaa_source = `localStorage:${lsResult.eaa_key}`;
  else if (ssResult.eaa_token) eaa_source = `sessionStorage:${ssResult.eaa_key}`;
  else if (pageResult.eaa_token) eaa_source = pageResult.eaa_source || 'page_js';

  return {
    eaa_token,
    eaa_source,
    fb_dtsg: pageResult.fb_dtsg || null,
    _details: {
      localStorage: lsResult,
      sessionStorage: ssResult,
      page: pageResult,
    },
  };
}

function parseFbJsonText(text) {
  const raw = String(text || '').trim();
  const candidates = [
    raw,
    raw.replace(/^for\s*\(\s*;\s*;\s*\)\s*;\s*/, '').trim(),
    raw.replace(/^while\s*\(\s*1\s*\)\s*;\s*/, '').trim(),
  ];
  for (const candidate of candidates) {
    if (!candidate) continue;
    try {
      return JSON.parse(candidate);
    } catch (e) {
      const firstBrace = candidate.indexOf('{');
      const lastBrace = candidate.lastIndexOf('}');
      if (firstBrace >= 0 && lastBrace > firstBrace) {
        try {
          return JSON.parse(candidate.slice(firstBrace, lastBrace + 1));
        } catch (e2) {}
      }
    }
  }
  throw new Error('response_not_json');
}

function currentFbUserId() {
  try {
    const val = window.CurrentUserInitialData || window.__USER || null;
    if (val && val.USER_ID) return String(val.USER_ID);
    if (val && val.id) return String(val.id);
  } catch (e) {}
  try {
    const sample = collectPageTextSample();
    const m = sample.match(/"USER_ID"\s*:\s*"(\d+)"/) || sample.match(/"actorID"\s*:\s*"(\d+)"/);
    if (m) return m[1];
  } catch (e) {}
  return '';
}

function graphErrorMessage(data, fallback) {
  if (!data || typeof data !== 'object') return fallback || 'unknown_error';
  const err = data.error || data.errors;
  if (Array.isArray(err) && err.length) return err.map(x => x.message || x.description || JSON.stringify(x)).join('; ');
  if (err && typeof err === 'object') return err.message || err.error_user_msg || err.description || JSON.stringify(err);
  if (typeof err === 'string') return err;
  return data.error_user_msg || data.error_msg || fallback || '';
}

async function adsManagerGraph(path, params = {}, options = {}) {
  const cleanPath = String(path || '').replace(/^\/+/, '');
  const baseUrl = cleanPath.startsWith('http')
    ? new URL(cleanPath)
    : new URL(`https://adsmanager-graph.facebook.com/v16.0/${cleanPath}`);
  const pageTokens = extractFromPageJS();
  const fbDtsg = pageTokens.fb_dtsg || '';
  const userId = currentFbUserId();
  const method = String(options.method || 'GET').toUpperCase();
  const common = {
    suppress_http_code: '1',
    locale: 'en_US',
    access_token: 'fbspider',
  };
  if (fbDtsg) common.fb_dtsg = fbDtsg;
  if (userId) common.__user = userId;

  const request = async (mode) => {
    const url = new URL(baseUrl.toString());
    const body = new URLSearchParams();
    const merged = { ...common, ...(params || {}) };
    if (method !== 'GET') merged.method = method;
    Object.entries(merged).forEach(([k, v]) => {
      if (v === undefined || v === null || v === '') return;
      if (mode === 'post') body.set(k, String(v));
      else url.searchParams.set(k, String(v));
    });
    const init = {
      method: mode === 'post' ? 'POST' : 'GET',
      credentials: 'include',
      headers: {
        'Accept': 'application/json, text/plain, */*',
        'Referer': location.href,
      },
    };
    if (mode === 'post') {
      init.headers['Content-Type'] = 'application/x-www-form-urlencoded';
      init.body = body.toString();
    }
    const res = await fetch(url.toString(), init);
    const text = await res.text();
    const data = parseFbJsonText(text);
    const msg = graphErrorMessage(data, '');
    if (!res.ok || msg) {
      const err = new Error(msg || `HTTP ${res.status}`);
      err.data = data;
      err.status = res.status;
      throw err;
    }
    return data;
  };

  if (method !== 'GET') {
    try { return await request('post'); } catch (e) {
      return await request('get');
    }
  }
  return await request('get');
}

function walkObject(value, visit, depth = 0, seen = new Set()) {
  if (!value || depth > 9) return;
  if (typeof value !== 'object') return;
  if (seen.has(value)) return;
  seen.add(value);
  visit(value);
  if (Array.isArray(value)) {
    value.forEach(v => walkObject(v, visit, depth + 1, seen));
    return;
  }
  Object.values(value).forEach(v => walkObject(v, visit, depth + 1, seen));
}

function normalizeGraphBusiness(item) {
  if (!item || typeof item !== 'object') return null;
  const id = String(item.id || item.business_id || item.businessID || '').replace(/\D/g, '');
  if (!/^\d{6,}$/.test(id)) return null;
  return {
    id,
    name: betterLabel('', item.name || item.business_name || item.label, `BM ${id}`),
    source: 'adsmanager_graph',
  };
}

function collectBusinessesFromGraph(data) {
  const byId = new Map();
  walkObject(data, (item) => {
    if (!item || typeof item !== 'object') return;
    const looksLikeBusiness =
      item.owned_ad_accounts || item.client_ad_accounts || item.owned_pixels ||
      item.business_users || item.partner_relationships ||
      item.verification_status || item.permitted_roles || item.can_use_extended_credit;
    if (!looksLikeBusiness) return;
    const b = normalizeGraphBusiness(item);
    if (!b) return;
    const old = byId.get(b.id) || { id: b.id, name: `BM ${b.id}`, source: '' };
    old.name = betterLabel(old.name, b.name, `BM ${b.id}`);
    old.source = old.source ? `${old.source},${b.source}` : b.source;
    byId.set(b.id, old);
  });
  return [...byId.values()];
}

function collectAdAccountsFromGraph(data, fallbackBusinessId = '', fallbackBusinessName = '') {
  const byId = new Map();
  const push = (item) => {
    if (!item || typeof item !== 'object') return;
    const id = String(item.account_id || item.ad_account_id || item.id || '').replace(/^act_/, '').replace(/\D/g, '');
    if (!/^\d{6,}$/.test(id)) return;
    const business = item.business || item.owner_business || item.viewable_business || item.business_manager || null;
    const businessId = String((business && business.id) || item.business_id || fallbackBusinessId || '').replace(/\D/g, '');
    const current = byId.get(id) || {
      account_id: id,
      act_id: `act_${id}`,
      id: `act_${id}`,
      name: `act_${id}`,
      account_status: item.account_status || 1,
      write_status: 'ok',
      source: 'adsmanager_graph',
      business_id: businessId,
      business_name: fallbackBusinessName || '',
    };
    current.name = betterLabel(current.name, item.name || item.account_name, `act_${id}`);
    current.currency = item.currency || current.currency || '';
    current.timezone = item.timezone_name || current.timezone || '';
    current.account_status = item.account_status || current.account_status || 1;
    current.business_id = businessId || current.business_id || '';
    current.business_name = betterLabel(current.business_name, business && business.name || fallbackBusinessName, current.business_id ? `BM ${current.business_id}` : '');
    byId.set(id, current);
  };
  if (Array.isArray(data?.data)) data.data.forEach(push);
  walkObject(data, (item) => {
    if (item && typeof item === 'object' && (item.account_id || item.ad_account_id || String(item.id || '').startsWith('act_'))) push(item);
  });
  return [...byId.values()];
}

function collectPixelsFromGraph(data, fallbackBusinessId = '') {
  const byId = new Map();
  const push = (item) => {
    if (!item || typeof item !== 'object') return;
    const id = String(item.id || item.pixel_id || item.dataset_id || '').replace(/\D/g, '');
    if (!/^\d{6,}$/.test(id)) return;
    const current = byId.get(id) || { id, pixel_id: id, name: `Pixel ${id}`, status: '', business_id: fallbackBusinessId || '', source: 'adsmanager_graph' };
    current.name = betterLabel(current.name, item.name || item.pixel_name || item.label, `Pixel ${id}`);
    current.status = item.status || item.event_source_status || current.status || '';
    current.code = item.code || current.code || '';
    byId.set(id, current);
  };
  walkObject(data, (item) => {
    if (!item || typeof item !== 'object') return;
    const owned = item.owned_pixels && (Array.isArray(item.owned_pixels.data) ? item.owned_pixels.data : item.owned_pixels);
    const client = item.client_pixels && (Array.isArray(item.client_pixels.data) ? item.client_pixels.data : item.client_pixels);
    if (Array.isArray(owned)) owned.forEach(push);
    if (Array.isArray(client)) client.forEach(push);
    if (item.pixel_id || item.dataset_id || item.event_source_id) {
      push(item);
    }
  });
  return [...byId.values()];
}

async function runBusinessOperation(operation, payload = {}) {
  const businessId = String(payload.business_id || payload.bm_id || '').replace(/\D/g, '');
  if (operation === 'list_businesses') {
    const data = await adsManagerGraph('me/businesses', {
      fields: 'id,name,verification_status,permitted_roles,business_users,owned_pixels.limit(1){id,name,status}',
      limit: 700,
      summary: 1,
    });
    return { businesses: collectBusinessesFromGraph(data), raw: data };
  }

  if (operation === 'list_ad_accounts') {
    const businesses = [];
    const accounts = [];
    if (businessId) {
      const data = await adsManagerGraph(businessId, {
        fields: 'id,name,owned_ad_accounts.limit(5000){id,account_id,name,account_status,currency,timezone_name,business{id,name},owner_business{id,name}},client_ad_accounts.limit(5000){id,account_id,name,account_status,currency,timezone_name,business{id,name},owner_business{id,name}}',
      });
      businesses.push(...collectBusinessesFromGraph(data));
      accounts.push(...collectAdAccountsFromGraph(data, businessId, businesses[0]?.name || ''));
    }
    const all = await adsManagerGraph('me/adaccounts', {
      fields: 'id,account_id,name,account_status,currency,timezone_name,business{id,name},owner_business{id,name},agencies,users{role,id}',
      limit: 5000,
      summary: 1,
    });
    businesses.push(...collectBusinessesFromGraph(all));
    accounts.push(...collectAdAccountsFromGraph(all, '', ''));
    const filtered = businessId
      ? accounts.filter(a => String(a.business_id || '') === businessId)
      : accounts;
    return { businesses, accounts: filtered, assets: { businesses, ad_accounts: filtered, pixels: [] } };
  }

  if (operation === 'list_pixels') {
    if (!businessId) throw new Error('missing_business_id');
    const pixels = [];
    const calls = [
      () => adsManagerGraph(businessId, { fields: 'id,name,owned_pixels.limit(500){id,name,status,code},client_pixels.limit(500){id,name,status,code}' }),
      () => adsManagerGraph('me/businesses', { fields: 'id,name,owned_pixels.limit(500){id,name,status,code}', limit: 700, summary: 1 }),
    ];
    const errors = [];
    for (const call of calls) {
      try {
        const data = await call();
        pixels.push(...collectPixelsFromGraph(data, businessId));
      } catch (e) {
        errors.push(e.message || String(e));
      }
    }
    const byId = new Map();
    pixels.forEach(px => { if (!byId.has(px.id)) byId.set(px.id, px); });
    return { pixels: [...byId.values()], errors };
  }

  if (operation === 'share_pixel') {
    const pixelId = String(payload.pixel_id || '').replace(/\D/g, '');
    const partnerBusinessId = String(payload.partner_business_id || payload.partner_bm_id || '').replace(/\D/g, '');
    if (!pixelId || !partnerBusinessId) throw new Error('missing_pixel_or_partner_business');
    const attempts = [
      ['shared_agencies', { business: partnerBusinessId }],
      ['shared_accounts', { business: partnerBusinessId, account_id: payload.account_id || '' }],
    ];
    const errors = [];
    for (const [edge, params] of attempts) {
      try {
        const data = await adsManagerGraph(`${pixelId}/${edge}`, params, { method: 'POST' });
        return { ok: true, edge, data };
      } catch (e) {
        errors.push(`${edge}: ${e.message || e}`);
      }
    }
    throw new Error(errors.join('；') || 'share_pixel_failed');
  }

  if (operation === 'invite_user') {
    if (!businessId) throw new Error('missing_business_id');
    const email = String(payload.email || '').trim();
    const role = String(payload.role || 'EMPLOYEE').trim() || 'EMPLOYEE';
    if (!email || !email.includes('@')) throw new Error('invalid_email');
    const attempts = [
      [`${businessId}/business_users`, { email, role }],
      [`${businessId}/users`, { email, role }],
    ];
    const errors = [];
    for (const [path, params] of attempts) {
      try {
        const data = await adsManagerGraph(path, params, { method: 'POST' });
        return { ok: true, path, data };
      } catch (e) {
        errors.push(`${path}: ${e.message || e}`);
      }
    }
    throw new Error(errors.join('；') || 'invite_user_failed');
  }

  if (operation === 'adsmanager_graph_request') {
    const path = String(payload.path || '').replace(/^\/+/, '');
    if (!path) throw new Error('missing_path');
    const method = String(payload.method || 'GET').toUpperCase();
    const params = payload.params || payload.body || {};
    return await adsManagerGraph(path, params, { method });
  }

  throw new Error('unknown_business_operation');
}

function collectPageTextSample() {
  const chunks = [];
  try { chunks.push(location.href); } catch (e) {}
  try { chunks.push(document.title || ''); } catch (e) {}
  try {
    const scripts = Array.from(document.querySelectorAll('script'))
      .slice(0, 80)
      .map(s => s.textContent || '')
      .filter(Boolean);
    chunks.push(...scripts);
  } catch (e) {}
  try {
    chunks.push((document.body && document.body.innerText || '').slice(0, 50000));
  } catch (e) {}
  return chunks.join('\n');
}

function cleanLabelText(value) {
  return String(value || '')
    .replace(/\\u0025/g, '%')
    .replace(/\\u0026/g, '&')
    .replace(/\\\//g, '/')
    .replace(/&amp;/g, '&')
    .replace(/\s+/g, ' ')
    .trim();
}

function isGenericLabel(value) {
  const s = cleanLabelText(value).toLowerCase();
  if (!s) return true;
  if (/^\d{6,}$/.test(s)) return true;
  return [
    'meta business suite',
    'facebook',
    'settings',
    'business settings',
    'business info',
    'billing',
    'billing & payments',
    'billing and payments',
    'payments',
    'payment settings',
    'business support home',
    'security center',
    'notifications',
    'requests',
    'partners',
    'users',
    'accounts',
    'ad accounts',
    'people',
    'pages',
    'select assets',
    'assets assigned',
  ].some(x => s === x || s.includes(x));
}

function betterLabel(current, next, fallback) {
  const c = cleanLabelText(current);
  const n = cleanLabelText(next);
  if (!isGenericLabel(n)) return n.slice(0, 90);
  if (!isGenericLabel(c)) return c.slice(0, 90);
  return cleanLabelText(fallback || c || n).slice(0, 90);
}

function pageBusinessName() {
  try {
    const lines = (document.body?.innerText || '')
      .split(/\n+/)
      .map(cleanLabelText)
      .filter(Boolean);
    for (let i = 0; i < lines.length; i++) {
      if (/^business portfolio$/i.test(lines[i]) && i > 0 && !isGenericLabel(lines[i - 1])) {
        return lines[i - 1];
      }
    }
  } catch (e) {}
  return '';
}

function nearbyNameForId(id, fallback) {
  id = String(id || '').replace(/^act_/, '').replace(/\D/g, '');
  if (!id) return cleanLabelText(fallback || '');
  try {
    const lines = (document.body?.innerText || '')
      .split(/\n+/)
      .map(cleanLabelText)
      .filter(Boolean);
    const bad = /^(active|disabled|closed|details|full access|partial access|owned by|assigned|people|assets|done|cancel|select|search|ad account|ad accounts|business portfolio)$/i;
    for (let i = 0; i < lines.length; i++) {
      if (!lines[i].includes(id) && !lines[i].includes(`act_${id}`)) continue;
      for (let j = Math.max(0, i - 4); j <= Math.min(lines.length - 1, i + 4); j++) {
        const candidate = lines[j];
        if (!candidate || candidate.includes(id) || bad.test(candidate) || isGenericLabel(candidate)) continue;
        if (candidate.length > 2 && candidate.length <= 90) return candidate;
      }
    }
  } catch (e) {}
  return cleanLabelText(fallback || '');
}

function scanBusinessAccountsFromPage() {
  const businesses = [];
  const byId = new Map();
  const pageBizName = pageBusinessName();
  const push = (id, name, source) => {
    id = String(id || '').replace(/\D/g, '');
    if (!/^\d{6,}$/.test(id)) return;
    const current = byId.get(id) || {
      id,
      name: `BM ${id}`,
      source: source || 'page_scan',
    };
    current.name = betterLabel(current.name, name || pageBizName, `BM ${id}`);
    if (source && !String(current.source || '').includes(source)) {
      current.source = current.source ? `${current.source},${source}` : source;
    }
    byId.set(id, current);
  };

  try {
    const url = new URL(location.href);
    for (const key of ['business_id', 'businessID', 'selected_business_id', 'asset_owner_business_id']) {
      const v = url.searchParams.get(key);
      if (v) push(v, pageBizName, `url:${key}`);
    }
  } catch (e) {}

  try {
    document.querySelectorAll('a[href]').forEach(a => {
      const href = a.getAttribute('href') || '';
      const text = cleanLabelText(a.innerText || a.textContent || '');
      for (const m of href.matchAll(/(?:business_id|businessID|selected_business_id)[=/](\d{6,})/g)) {
        push(m[1], text, 'link');
      }
    });
  } catch (e) {}

  const sample = collectPageTextSample();
  const businessPairs = [
    /"name"\s*:\s*"([^"]{2,90})"[^{}]{0,500}?"(?:business_id|businessID|id)"\s*:\s*"?(\d{6,})"?/g,
    /"(?:business_id|businessID|id)"\s*:\s*"?(\d{6,})"?[^{}]{0,500}?"name"\s*:\s*"([^"]{2,90})"/g,
  ];
  for (const re of businessPairs) {
    for (const m of sample.matchAll(re)) {
      if (/^"name"/.test(re.source)) push(m[2], m[1], 'json_pair');
      else push(m[1], m[2], 'json_pair');
    }
  }
  const patterns = [
    /"business(?:_id|ID|Id)"\s*:\s*"?(\d{6,})"?/g,
    /"business"\s*:\s*\{[^{}]{0,500}?"id"\s*:\s*"(\d{6,})"[^{}]{0,500}?"name"\s*:\s*"([^"]+)"/g,
    /"id"\s*:\s*"(\d{6,})"[^{}]{0,220}?"name"\s*:\s*"([^"]+)"[^{}]{0,220}?"(?:Business|business)"/g,
    /business_id[=:"']+(\d{6,})/g,
  ];
  for (const re of patterns) {
    for (const m of sample.matchAll(re)) push(m[1], m[2] || pageBizName, 'text');
  }
  return { businesses: [...byId.values()] };
}

function scanAdAccountsFromPage() {
  const accounts = [];
  const byId = new Map();
  let pageBusinessId = '';
  let pageBusinessLabel = pageBusinessName();
  try {
    const url = new URL(location.href);
    pageBusinessId = (url.searchParams.get('business_id') || url.searchParams.get('businessID') || '').replace(/\D/g, '');
  } catch (e) {}
  const push = (id, name, source) => {
    id = String(id || '').replace(/^act_/, '').replace(/\D/g, '');
    if (!/^\d{6,}$/.test(id)) return;
    const current = byId.get(id) || {
      account_id: id,
      id: `act_${id}`,
      name: `act_${id}`,
      account_status: 1,
      write_status: 'ok',
      source: source || 'page_scan',
      business_id: pageBusinessId || '',
      business_name: pageBusinessLabel || '',
    };
    current.name = betterLabel(current.name, nearbyNameForId(id, name), `act_${id}`);
    if (pageBusinessId && !current.business_id) current.business_id = pageBusinessId;
    if (pageBusinessLabel && !current.business_name) current.business_name = pageBusinessLabel;
    if (source && !String(current.source || '').includes(source)) {
      current.source = current.source ? `${current.source},${source}` : source;
    }
    byId.set(id, current);
  };

  try {
    const url = new URL(location.href);
    for (const key of ['act', 'act_id', 'ad_account_id', 'account_id']) {
      const v = url.searchParams.get(key);
      if (v) push(v, nearbyNameForId(v, document.title), `url:${key}`);
    }
  } catch (e) {}

  const sample = collectPageTextSample();
  const accountPairs = [
    /"name"\s*:\s*"([^"]{2,90})"[^{}]{0,700}?"(?:account_id|ad_account_id|adAccountID|id)"\s*:\s*"?(?:act_)?(\d{6,})"?/g,
    /"(?:account_id|ad_account_id|adAccountID|id)"\s*:\s*"?(?:act_)?(\d{6,})"?[^{}]{0,700}?"name"\s*:\s*"([^"]{2,90})"/g,
  ];
  for (const re of accountPairs) {
    for (const m of sample.matchAll(re)) {
      if (/^"name"/.test(re.source)) push(m[2], m[1], 'json_pair');
      else push(m[1], m[2], 'json_pair');
    }
  }
  const patterns = [
    /act[_=](\d{6,})/g,
    /"act_(\d{6,})"/g,
    /"(?:ad_account_id|adAccountID|adAccountId|account_id)"\s*:\s*"?(?:act_)?(\d{6,})"?/g,
    /(?:ad_account_id|adAccountID|account_id)[=:"']+(?:act_)?(\d{6,})/g,
  ];
  for (const re of patterns) {
    for (const m of sample.matchAll(re)) push(m[1], nearbyNameForId(m[1], document.title), 'text');
  }
  return { accounts: [...byId.values()] };
}

// ========== 消息监听 ==========

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'extractFacebookToken') {
    const tokens = extractAllTokens();
    sendResponse(tokens);
    return true;
  }

  if (message.action === 'scanBusinessAccounts') {
    sendResponse({ ok: true, data: scanBusinessAccountsFromPage() });
    return true;
  }

  if (message.action === 'scanAdAccounts') {
    sendResponse({ ok: true, data: scanAdAccountsFromPage() });
    return true;
  }

  if (message.action === 'fbBusinessOperation') {
    (async () => {
      try {
        const data = await runBusinessOperation(message.operation, message.payload || {});
        sendResponse({ ok: true, data });
      } catch (err) {
        sendResponse({ ok: false, error: err.message || String(err), data: err.data || null });
      }
    })();
    return true;
  }

  // — FB 内部 API 调用（Cookie 鉴权，用于 BM 操作）—
  if (message.action === 'fbInternalApi') {
    (async () => {
      try {
        const pageTokens = extractFromPageJS();
        const fbDtsg = pageTokens.fb_dtsg || '';

        // 依次尝试多个可能的内部端点
        const endpoints = message.endpoints || [];

        for (const ep of endpoints) {
          try {
            const url = new URL(ep.url, location.origin);
            const opts = {
              method: ep.method || 'GET',
              headers: { 'Referer': location.href },
              credentials: 'include',
            };

            if (ep.body) {
              const params = new URLSearchParams();
              if (fbDtsg) params.set('fb_dtsg', fbDtsg);
              Object.entries(ep.body).forEach(([k, v]) => params.set(k, String(v)));
              params.set('__a', '1');
              opts.headers['Content-Type'] = 'application/x-www-form-urlencoded';
              opts.body = params.toString();
            } else if (ep.method !== 'POST') {
              url.searchParams.set('__a', '1');
              if (fbDtsg) url.searchParams.set('fb_dtsg', fbDtsg);
            }

            const res = await fetch(url.toString(), opts);
            const text = await res.text();
            let data;
            try { data = parseFbJsonText(text); } catch (e) {
              // HTML 返回 — 不是 JSON 端点，试下一个
              continue;
            }

            // 成功返回 JSON
            if (data && !data.error) {
              sendResponse({ ok: true, status: res.status, data });
              return;
            }
          } catch (e) { continue; }
        }

        sendResponse({ ok: false, error: '所有内部端点均未返回有效数据', status: 0, data: null });
      } catch (err) {
        sendResponse({ ok: false, error: '内部API: ' + (err.message || err), status: 0, data: null });
      }
    })();
    return true;
  }

  // — FB 内部 GraphQL 调用（Cookie 鉴权，用于 BM 操作）—
  if (message.action === 'fbGraphqlCall') {
    (async () => {
      try {
        const pageTokens = extractFromPageJS();
        const fbDtsg = pageTokens.fb_dtsg || '';
        if (!fbDtsg) { sendResponse({ ok: false, error: '未找到 fb_dtsg，请刷新 FB 页面' }); return; }

        const body = new URLSearchParams();
        body.set('fb_dtsg', fbDtsg);
        if (message.variables) body.set('variables', JSON.stringify(message.variables));
        if (message.doc_id) body.set('doc_id', String(message.doc_id));
        if (message.query_params) {
          Object.entries(message.query_params).forEach(([k, v]) => body.set(k, String(v)));
        }

        const res = await fetch('https://business.facebook.com/api/graphql/', {
          method: 'POST',
          credentials: 'include',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': location.href,
          },
          body: body.toString(),
        });
        const text = await res.text();
        let data;
        try { data = parseFbJsonText(text); } catch (e) { data = text; }

        sendResponse({ ok: res.ok && !(data && data.errors), status: res.status, data });
      } catch (err) {
        sendResponse({ ok: false, error: '网络异常: ' + (err.message || err), status: 0, data: null });
      }
    })();
    return true;
  }

  // — 公开 Graph API 调用（Token 鉴权，用于普通读/写操作）—
  if (message.action === 'graphApiCall') {
    (async () => {
      try {
        const { method, path, params, token } = message;
        const cleanPath = String(path || '').replace(/^\/+/, '');
        // 用 webRequest 捕获到的 API 版本，而非硬编码
        const apiVer = message.apiVersion || 'v22.0';
        const url = new URL(`https://graph.facebook.com/${apiVer}/${cleanPath}`);

        if (params) {
          Object.entries(params).forEach(([k, v]) => {
            if (v !== undefined && v !== null && v !== '') {
              url.searchParams.set(k, String(v));
            }
          });
        }
        url.searchParams.set('access_token', token);

        // 带上 fb_dtsg 等 FB 反滥用参数（页面内提取）
        const pageTokens = extractFromPageJS();
        if (pageTokens.fb_dtsg) url.searchParams.set('fb_dtsg', pageTokens.fb_dtsg);

        const init = {
          method: (method || 'GET').toUpperCase(),
          credentials: 'include',
          headers: { 'Referer': location.href },
        };
        const res = await fetch(url.toString(), init);
        const text = await res.text();
        let data;
        try { data = JSON.parse(text); } catch (e) { data = text; }

        sendResponse({
          ok: res.ok && !(data && data.error),
          status: res.status,
          data
        });
      } catch (err) {
        sendResponse({ ok: false, error: '网络异常: ' + (err.message || err), status: 0, data: null });
      }
    })();
    return true;
  }
});

// ========== 页面加载后自动尝试提取并缓存（带延迟重试） ==========

(function autoCacheOnLoad() {
  // 第一次尝试
  tryCacheToken();

  // SPA 页面（如 adsmanager）可能需要更长时间初始化
  // 在 3 秒和 8 秒后重试
  setTimeout(tryCacheToken, 3000);
  setTimeout(tryCacheToken, 8000);

  function tryCacheToken() {
    try {
      const extracted = extractAllTokens();
      if (extracted.eaa_token) {
        chrome.runtime.sendMessage({
          type: 'facebookTokenCache',
          payload: {
            eaa_token: extracted.eaa_token,
            eaa_source: extracted.eaa_source,
            fb_dtsg: extracted.fb_dtsg,
            url: location.href,
            timestamp: new Date().toISOString(),
          },
        }).catch(() => { /* background 可能未就绪 */ });
      }
    } catch (e) { /* ignore */ }
  }
})();
