/**
 * Mira 本地执行器 v2.0 — Service Worker
 *
 * 核心: 插件本地执行 Graph API，Mira 只下发任务和接收结果。
 * 不上传 Cookie/Token 明文给 Mira。
 */

// ========== 常量 ==========

const MIRA_BASE = "https://shouhu.asia";
const HEARTBEAT_ALARM = "mira_heartbeat";
const POLL_ALARM = "mira_poll";
const TOKEN_ALARM = "mira_token_refresh";
const GRAPH_BASE = "https://graph.facebook.com/v22.0";
const VERSION = "2.4.0";
const ASSET_DISCOVERY_FRESH_MS = 6 * 60 * 60 * 1000;
const ASSET_DISCOVERY_MIN_INTERVAL_MS = 20 * 60 * 1000;

const FB_COOKIE_NAMES = ['c_user', 'xs', 'fr', 'datr', 'sb'];

// Graph API path 白名单 — 只允许操作广告相关端点
const PATH_WHITELIST = [
  /^me\/adaccounts/,
  /^act_\d+/,
  /^\d{5,}/,
  /^me(\/.*)?$/,
];

// 域名白名单 — 只允许请求 Mira + Meta
const ALLOWED_DOMAINS = [
  'shouhu.asia',
  'graph.facebook.com',
  'facebook.com',
  'fbcdn.net',
  'facebookcorewwwi.onion',
];

function isAllowedUrl(url) {
  try {
    const host = new URL(url).hostname;
    return ALLOWED_DOMAINS.some(d => host === d || host.endsWith('.' + d));
  } catch (e) { return false; }
}

// 安全 fetch — 禁止非白名单域名
async function safeFetch(url, init) {
  if (!isAllowedUrl(url)) {
    console.error('[mira] 拦截非法请求:', url);
    throw new Error('blocked_domain');
  }
  return fetch(url, init);
}

const runningTasks = new Map();      // task_id → promise
const accountLocks = new Map();      // act_xxx → true (并发=1)
const seenIdempotency = new Set();   // idempotency_key (成功过的)
let lastProbeAt = 0;
let lastProbeSummary = null;
let lastTokenExtractAt = 0;
let cachedExtractedToken = null;
let assetDiscoveryPromise = null;

// ========== 工具 ==========

function randomBetween(min, max) { return min + Math.random() * (max - min); }
function randomId(p) {
  const b = new Uint8Array(12);
  crypto.getRandomValues(b);
  return `${p}_${Array.from(b).map(x => x.toString(16).padStart(2, '0')).join('')}`;
}
function maskToken(t) {
  const s = String(t || '').trim();
  if (!s) return '';
  if (s.length <= 12) return '****';
  return s.slice(0, 6) + '****' + s.slice(-4);
}
function normalizeActId(v) {
  const raw = String(v || '').trim();
  if (!raw) return '';
  return raw.startsWith('act_') ? raw : `act_${raw}`;
}
function cleanNumericId(v) {
  return String(v || '').replace(/^act_/, '').replace(/\D/g, '');
}
function cleanLabelText(v) {
  return String(v || '').replace(/\s+/g, ' ').trim();
}
function isGenericBusinessLabel(v) {
  const s = cleanLabelText(v).toLowerCase();
  if (!s || /^\d{6,}$/.test(s)) return true;
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
function isGenericPageTitle(v) {
  const s = cleanLabelText(v).toLowerCase();
  if (!s) return true;
  if (isGenericBusinessLabel(s)) return true;
  return [
    'ads manager',
    'business manager',
    'meta business settings',
    'facebook ads',
  ].some(x => s === x || s.includes(x));
}
function bestBusinessLabel(id, currentName, nextName) {
  const fallback = `BM ${id}`;
  const current = cleanLabelText(currentName);
  const next = cleanLabelText(nextName);
  if (!isGenericBusinessLabel(next)) return next.slice(0, 90);
  if (!isGenericBusinessLabel(current)) return current.slice(0, 90);
  return fallback;
}
function pushUniqueBusiness(map, item, source, accountId) {
  if (!item || typeof item !== 'object') return null;
  const id = cleanNumericId(item.id || item.business_id || item.businessID || item.businessId);
  if (!/^\d{6,}$/.test(id)) return null;
  const current = map.get(id) || {
    id,
    name: '',
    source: source || '',
    account_ids: [],
    account_count: 0,
  };
  const name = String(item.name || item.business_name || item.businessName || item.label || '').trim();
  current.name = bestBusinessLabel(id, current.name, name);
  if (source && !String(current.source || '').includes(source)) {
    current.source = current.source ? `${current.source},${source}` : source;
  }
  if (accountId && !current.account_ids.includes(accountId)) {
    current.account_ids.push(accountId);
    current.account_count = current.account_ids.length;
  }
  if (!current.name || isGenericBusinessLabel(current.name)) current.name = `BM ${id}`;
  map.set(id, current);
  return current;
}
function businessListFromMap(map) {
  return [...map.values()]
    .map(b => ({
      ...b,
      account_count: (b.account_ids || []).length,
    }))
    .sort((a, b) => (b.account_count || 0) - (a.account_count || 0) || String(a.name).localeCompare(String(b.name)));
}
function mergeAccountsById(items) {
  const byId = new Map();
  for (const item of items || []) {
    if (!item || typeof item !== 'object') continue;
    const id = cleanNumericId(item.account_id || item.act_id || item.id);
    if (!/^\d{6,}$/.test(id)) continue;
    const current = byId.get(id) || { account_id: id, act_id: `act_${id}` };
    for (const [k, v] of Object.entries(item)) {
      if (v !== undefined && v !== null && v !== '') current[k] = v;
    }
    current.account_id = id;
    current.act_id = `act_${id}`;
    byId.set(id, current);
  }
  return [...byId.values()];
}
function mergeBusinessesById(items) {
  const byId = new Map();
  for (const item of items || []) {
    pushUniqueBusiness(byId, item, item?.source || 'merge');
    const id = cleanNumericId(item?.id || item?.business_id);
    const current = byId.get(id);
    if (!current) continue;
    for (const raw of (item.account_ids || [])) {
      const actId = normalizeActId(raw);
      if (actId && !current.account_ids.includes(actId)) current.account_ids.push(actId);
    }
    current.account_count = Math.max(Number(current.account_count || 0), current.account_ids.length, Number(item.account_count || 0));
  }
  return businessListFromMap(byId);
}
function mergeAssetSummary(base, extra) {
  const accounts = mergeAccountsById([...(base.accounts || []), ...(base.assets?.ad_accounts || []), ...(extra.accounts || []), ...(extra.assets?.ad_accounts || [])]);
  const businesses = mergeBusinessesById([...(base.businesses || []), ...(base.assets?.businesses || []), ...(extra.businesses || []), ...(extra.assets?.businesses || [])]);
  const bizMap = new Map();
  businesses.forEach(b => bizMap.set(String(b.id), { ...b, account_ids: [...(b.account_ids || [])] }));
  for (const account of accounts) {
    if (account.business_id) {
      const b = bizMap.get(String(account.business_id)) || {
        id: String(account.business_id),
        name: bestBusinessLabel(account.business_id, '', account.business_name),
        source: 'account_link',
        account_ids: [],
        account_count: 0,
      };
      b.name = bestBusinessLabel(account.business_id, b.name, account.business_name);
      if (!b.account_ids.includes(account.act_id)) b.account_ids.push(account.act_id);
      b.account_count = Math.max(Number(b.account_count || 0), b.account_ids.length);
      bizMap.set(String(account.business_id), b);
    }
  }
  const mergedBusinesses = businessListFromMap(bizMap);
  return {
    accounts,
    businesses: mergedBusinesses,
    assets: { ad_accounts: accounts, businesses: mergedBusinesses, pixels: [] },
  };
}
function isWritablePath(path) {
  return PATH_WHITELIST.some(r => r.test(String(path || '').replace(/^\/+/, '')));
}

// ========== install_id（永久 UUID）==========

async function ensureInstallId() {
  const d = await chrome.storage.local.get(['install_id']);
  if (d.install_id) return d.install_id;
  const id = randomId('chrome');
  await chrome.storage.local.set({ install_id: id });
  return id;
}

// ========== HMAC 签名 ==========

async function hmacSign(secret, payload) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await crypto.subtle.sign('HMAC', key, enc.encode(payload));
  return Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function miraApi(path, body = {}, method = 'POST') {
  const s = await getSettings();
  const ts = Date.now().toString();
  const payload = JSON.stringify(body || {});
  const headers = {
    'Content-Type': 'application/json',
    'X-Mira-Node-Id': s.nodeId || '',
    'X-Mira-Timestamp': ts,
  };
  if (s.nodeSecret) {
    headers['X-Mira-Signature'] = await hmacSign(s.nodeSecret, `${path}:${ts}:${payload}`);
  }
  const res = await safeFetch(MIRA_BASE + path, { method, headers, body: payload });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.message || `HTTP ${res.status}`);
  return data;
}

// ========== Settings ==========

async function getSettings() {
  const d = await chrome.storage.local.get([
    'nodeId', 'nodeSecret', 'teamId', 'operatorId', 'operatorName',
    'accessToken', 'expiresInMinutes', 'tokenExpiresAt',
    'executorStatus', 'miraStatus', 'lastHeartbeat'
  ]);
  return {
    nodeId: d.nodeId || '',
    nodeSecret: d.nodeSecret || '',
    teamId: d.teamId || null,
    operatorId: d.operatorId || null,
    operatorName: d.operatorName || '',
    accessToken: d.accessToken || '',
    expiresInMinutes: d.expiresInMinutes || '',
    tokenExpiresAt: d.tokenExpiresAt || '',
    executorStatus: d.executorStatus || { bound: false, fbStatus: 'no_tab', accounts: [] },
    miraStatus: d.miraStatus || null,
    lastHeartbeat: d.lastHeartbeat || null,
  };
}

// ========== Token 权限探测 ==========

let lastPermissionProbe = { token: '', time: 0 };

async function verifyTokenPermissions(token) {
  // 去重：同一个 Token 30 分钟内不重复探测
  const now = Date.now();
  if (token === lastPermissionProbe.token && (now - lastPermissionProbe.time) < 30 * 60 * 1000) {
    return null;
  }
  lastPermissionProbe = { token, time: now };

  try {
    const me = await graphGetRaw(token, 'me', { fields: 'id,name' });
    const perms = await graphGetRaw(token, 'me/permissions', {});
    const granted = [];
    const declined = [];
    (perms.data || []).forEach(p => {
      const name = String(p.permission || '').trim();
      if (!name) return;
      if (String(p.status || '').toLowerCase() === 'granted') granted.push(name);
      else declined.push(name);
    });
    await chrome.storage.local.set({
      tokenStatus: {
        valid: true,
        tokenMask: maskToken(token),
        fbName: me.name || '',
        fbId: me.id || '',
        permissions: granted.sort(),
        permissionsDeclined: declined.sort(),
        hasAdsMgmt: granted.includes('ads_management'),
        hasBizMgmt: granted.includes('business_management'),
        error: null,
        updatedAt: new Date().toISOString(),
      }
    });
    return { granted, declined };
  } catch (err) {
    const msg = err.message || String(err);
    // Token 格式错误 — 清除，避免后续复用
    if (msg.includes('Malformed') || msg.includes('invalid')) {
      console.debug('[mira] Token 无效，清除');
      await chrome.storage.local.set({ accessToken: '', expiresInMinutes: '', tokenExpiresAt: '' });
      cachedExtractedToken = null;
      lastProbeAt = 0;
    }
    await chrome.storage.local.set({
      tokenStatus: {
        valid: false,
        tokenMask: maskToken(token),
        error: msg,
        updatedAt: new Date().toISOString(),
      }
    });
    return null;
  }
}

// ========== Facebook Token 提取 ==========

async function getFacebookCookies() {
  const r = {};
  for (const name of FB_COOKIE_NAMES) {
    try {
      const c = await chrome.cookies.get({ url: 'https://www.facebook.com', name });
      if (c) r[name] = c.value;
    } catch (e) { /* ignore */ }
  }
  return r;
}

async function extractFromTab(tabId) {
  try {
    return await chrome.tabs.sendMessage(tabId, { action: 'extractFacebookToken' });
  } catch (e) {
    try { await chrome.scripting.executeScript({ target: { tabId }, files: ['content-script.js'] }); } catch (e2) {}
    await new Promise(r => setTimeout(r, 500));
    return await chrome.tabs.sendMessage(tabId, { action: 'extractFacebookToken' });
  }
}

async function extractFacebookTokenFromTabs() {
  const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' });
  for (const tab of tabs) {
    try {
      const pt = await extractFromTab(tab.id);
      if (pt && pt.eaa_token) {
        const cookies = await getFacebookCookies();
        const r = { eaa_token: pt.eaa_token, eaa_source: pt.eaa_source || tab.url,
          fb_dtsg: pt.fb_dtsg || null, c_user: cookies.c_user || null, xs: cookies.xs || null,
          cookies, extracted_at: new Date().toISOString(), from_tab: tab.url };
        lastTokenExtractAt = Date.now();
        cachedExtractedToken = r;
        return r;
      }
    } catch (e) {}
  }
  if (tabs.length === 0) throw new Error('need_fb_tab');
  throw new Error('未能提取到 Token，请刷新 Facebook 页面');
}

// 刷新已有 FB 标签页触发 webRequest 被动捕获 Token
// 比开新标签页安全得多：刷新跟用户按 F5 没区别
async function refreshExistingFbTab() {
  const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' });
  if (tabs.length === 0) return null;

  // 优先 adsmanager > business > 任意 FB 页面
  const preferred = tabs.find(t => t.url && t.url.includes('adsmanager')) ||
                    tabs.find(t => t.url && t.url.includes('business')) ||
                    tabs[0];

  console.debug('[mira] 刷新已有 FB 标签页以获取新 Token');
  await chrome.tabs.reload(preferred.id);

  // 等页面加载 + FB JS 发出 Graph API 请求（webRequest 自动捕获）
  await new Promise(r => setTimeout(r, randomBetween(8000, 15000)));

  // 检查 webRequest 是否已捕获到新 Token
  const s = await getSettings();
  if (s.accessToken && s.tokenExpiresAt) {
    const remaining = new Date(s.tokenExpiresAt).getTime() - Date.now();
    if (remaining > 10 * 60 * 1000) {
      // Token 已更新（webRequest 捕获到了新的）
      return { eaa_token: s.accessToken, eaa_source: 'webRequest via tab refresh', refreshed: true };
    }
  }

  // webRequest 没捕获到，用 content-script 直接提取
  try {
    const pt = await extractFromTab(preferred.id);
    if (pt && pt.eaa_token) {
      chrome.storage.local.set({
        accessToken: pt.eaa_token,
        expiresInMinutes: '55',
        tokenExpiresAt: new Date(Date.now() + 55 * 60000).toISOString()
      }).catch(() => {});
      return { eaa_token: pt.eaa_token, eaa_source: 'content-script after refresh', refreshed: true };
    }
  } catch (e) { /* ignore */ }

  return null;
}

async function silentExtractAndSave() {
  // 第一步：已有标签页直接提取
  try {
    const ex = await extractFacebookTokenFromTabs();
    if (ex && ex.eaa_token) {
      const exp = new Date(Date.now() + 55 * 60000).toISOString();
      await chrome.storage.local.set({ accessToken: ex.eaa_token, expiresInMinutes: '55', tokenExpiresAt: exp });
      verifyTokenPermissions(ex.eaa_token).catch(() => {});
      lastProbeAt = 0;
      lastProbeSummary = null;
      return ex;
    }
  } catch (e) { /* 继续下一步 */ }

  // 第二步：Token 过期或拿不到 → 刷新已有 FB 标签页触发 webRequest
  const s = await getSettings();
  if (!s.accessToken || (s.tokenExpiresAt && new Date(s.tokenExpiresAt).getTime() - Date.now() < 15 * 60 * 1000)) {
    const refreshed = await refreshExistingFbTab();
    if (refreshed && refreshed.eaa_token) {
      lastProbeAt = 0;
      lastProbeSummary = null;
      return refreshed;
    }
  }

  return null;
}

// ========== 账户摘要 ==========

async function withCachedAssets(summary, options = {}) {
  if (options.skipCachedAssets) return summary;
  try {
    const d = await chrome.storage.local.get(['discoveredAssets', 'discoveredAssetsFbUserId']);
    const cached = d.discoveredAssets || null;
    if (!cached) return summary;
    const currentFb = summary?.fb_user?.id || '';
    const cachedFb = d.discoveredAssetsFbUserId || cached?.fb_user?.id || '';
    if (currentFb && cachedFb && currentFb !== cachedFb) return summary;
    const merged = mergeAssetSummary(summary || {}, cached);
    return {
      ...(summary || {}),
      accounts: merged.accounts,
      businesses: merged.businesses,
      assets: merged.assets,
      cached_assets_at: cached.discovered_at || '',
    };
  } catch (e) {
    return summary;
  }
}

async function buildAccountSummary(options = {}) {
  const s = await getSettings();
  const token = String(s.accessToken || '').trim();
  const businessesById = new Map();
  if (!token) {
    const tabAccounts = await discoverAccountsFromOpenTabs();
    const tabBusinesses = await scanBusinessAccountsFromOpenTabs().catch(() => ({ businesses: [] }));
    (tabBusinesses.businesses || []).forEach(b => pushUniqueBusiness(businessesById, b, b.source || 'page_scan'));
    return await withCachedAssets({
      fb_user: null,
      accounts: tabAccounts,
      businesses: businessListFromMap(businessesById),
      assets: { ad_accounts: tabAccounts, businesses: businessListFromMap(businessesById), pixels: [] },
      fb_status: tabAccounts.length ? 'ok' : 'no_token'
    }, options);
  }

  let fbUser = null;
  const accounts = [];

  try {
    const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' });
    if (tabs.length === 0) return await withCachedAssets({ fb_user: null, accounts: [], businesses: [], assets: { ad_accounts: [], businesses: [], pixels: [] }, fb_status: 'need_fb_tab' }, options);

    // me
    try {
      const me = await graphGetRaw(token, 'me', { fields: 'id,name' });
      fbUser = { id: me.id || '', name: me.name || '' };
    } catch (e) {}

    // adaccounts
    try {
      const actIds = [];
      let path = 'me/adaccounts';
      let params = {
        fields: [
          'id',
          'account_id',
          'name',
          'account_status',
          'currency',
          'timezone_name',
          'timezone_offset_hours_utc',
          'business{id,name}',
          'viewable_business{id,name}',
          'business_country_code',
          'spend_cap',
          'amount_spent'
        ].join(','),
        limit: 200
      };
      for (let i = 0; i < 10; i++) {
        const data = await graphGetRaw(token, path, params);
        for (const item of (data.data || [])) {
          const rawAccountId = item.account_id || item.id;
          const actId = normalizeActId(rawAccountId);
          if (actId) {
            actIds.push(actId);
            const business = item.business || item.viewable_business || null;
            if (business) pushUniqueBusiness(businessesById, business, item.business ? 'adaccount.business' : 'adaccount.viewable_business', actId);
            accounts.push({
              act_id: actId,
              account_id: cleanNumericId(rawAccountId),
              name: item.name || '',
              currency: item.currency || '',
              timezone: item.timezone_name || '',
              timezone_offset_hours_utc: item.timezone_offset_hours_utc,
              account_status: item.account_status,
              business_id: business ? String(business.id || '') : '',
              business_name: business ? bestBusinessLabel(business.id || '', '', business.name || '') : '',
              spend_cap: item.spend_cap,
              amount_spent: item.amount_spent,
              business_country_code: item.business_country_code || '',
              write_status: item.account_status === 1 ? 'ok' : 'error'
            });
          }
        }
        const next = data.paging && data.paging.next;
        if (!next) break;
        const u = new URL(next);
        path = u.pathname.replace(/^\/v[0-9.]+\//, '');
        params = Object.fromEntries(u.searchParams.entries());
        delete params.access_token;
      }
    } catch (e) {}

    if (!accounts.length) {
      accounts.push(...await discoverAccountsFromOpenTabs());
    }
    const tabBusinesses = await scanBusinessAccountsFromOpenTabs().catch(() => ({ businesses: [] }));
    (tabBusinesses.businesses || []).forEach(b => pushUniqueBusiness(businessesById, b, b.source || 'page_scan'));

    const businesses = businessListFromMap(businessesById);
    return await withCachedAssets({
      fb_user: fbUser,
      accounts,
      businesses,
      assets: { ad_accounts: accounts, businesses, pixels: [] },
      fb_status: 'ok'
    }, options);
  } catch (e) {
    const tabAccounts = await discoverAccountsFromOpenTabs();
    const tabBusinesses = await scanBusinessAccountsFromOpenTabs().catch(() => ({ businesses: [] }));
    (tabBusinesses.businesses || []).forEach(b => pushUniqueBusiness(businessesById, b, b.source || 'page_scan'));
    const businesses = businessListFromMap(businessesById);
    return await withCachedAssets({
      fb_user: fbUser,
      accounts: accounts.length ? accounts : tabAccounts,
      businesses,
      assets: { ad_accounts: accounts.length ? accounts : tabAccounts, businesses, pixels: [] },
      fb_status: tabAccounts.length ? 'ok' : 'error',
      last_error: e.message
    }, options);
  }
}

async function discoverAccountsFromOpenTabs() {
  const tabs = sortFacebookTabs(await chrome.tabs.query({ url: '*://*.facebook.com/*' }));
  const byId = new Map();
  const putAccount = (id, patch = {}) => {
    id = cleanNumericId(id);
    if (!/^\d{6,}$/.test(id)) return;
    const current = byId.get(id) || {
      account_id: id,
      act_id: `act_${id}`,
      id: `act_${id}`,
      name: `act_${id}`,
      currency: '',
      timezone: '',
      account_status: 1,
      write_status: 'ok',
      source: 'open_tab',
    };
    Object.entries(patch || {}).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== '') current[k] = v;
    });
    current.account_id = id;
    current.act_id = `act_${id}`;
    current.id = `act_${id}`;
    if (!current.name || String(current.name).includes('Meta Business Suite') || String(current.name).includes('Facebook')) current.name = `act_${id}`;
    byId.set(id, current);
  };
  for (const tab of tabs) {
    const url = String(tab.url || '');
    const title = String(tab.title || '');
    const ids = new Set();
    try {
      const u = new URL(url);
      for (const key of ['act', 'act_id', 'ad_account_id', 'account_id']) {
        const v = u.searchParams.get(key);
        if (v && /^\d{6,}$/.test(v)) ids.add(v);
        if (v && /^act_\d+$/.test(v)) ids.add(v.replace(/^act_/, ''));
      }
    } catch (e) {}
    for (const match of url.matchAll(/act[_=](\d{6,})/g)) ids.add(match[1]);
    for (const match of url.matchAll(/act_(\d{6,})/g)) ids.add(match[1]);
    try {
      const res = await chrome.tabs.sendMessage(tab.id, { action: 'scanAdAccounts' });
      if (res && res.ok && res.data && Array.isArray(res.data.accounts)) {
        for (const item of res.data.accounts) {
          const id = String(item.account_id || item.id || '').replace(/^act_/, '');
          if (/^\d{6,}$/.test(id)) {
            ids.add(id);
            putAccount(id, {
              ...item,
              name: item.name || (!isGenericPageTitle(title) ? title.slice(0, 80) : ''),
              source: item.source || 'page_scan',
            });
          }
        }
      }
    } catch (e) {}
    for (const id of ids) {
      putAccount(id, {
        name: !isGenericPageTitle(title) ? title.slice(0, 80) : '',
        source: 'open_tab',
      });
    }
  }
  return [...byId.values()];
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function ensureContentScript(tabId) {
  try {
    await chrome.tabs.sendMessage(tabId, { action: 'scanAdAccounts' });
    return;
  } catch (e) {
    try { await chrome.scripting.executeScript({ target: { tabId }, files: ['content-script.js'] }); } catch (e2) {}
    await sleep(400);
  }
}

async function waitForTabComplete(tabId, timeoutMs = 18000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const tab = await chrome.tabs.get(tabId);
      if (tab.status === 'complete') return tab;
    } catch (e) {
      break;
    }
    await sleep(500);
  }
  try { return await chrome.tabs.get(tabId); } catch (e) { return null; }
}

async function scanAssetsFromTab(tabId, source) {
  await ensureContentScript(tabId);
  const businesses = [];
  const accounts = [];
  let pageBusinessId = '';
  try {
    const tab = await chrome.tabs.get(tabId);
    const u = new URL(tab.url || '');
    pageBusinessId = cleanNumericId(u.searchParams.get('business_id') || u.searchParams.get('businessID') || '');
  } catch (e) {}
  try {
    const biz = await chrome.tabs.sendMessage(tabId, { action: 'scanBusinessAccounts' });
    if (biz && biz.ok && biz.data && Array.isArray(biz.data.businesses)) {
      businesses.push(...biz.data.businesses.map(b => ({ ...b, source: b.source || source })));
    }
  } catch (e) {}
  try {
    const acc = await chrome.tabs.sendMessage(tabId, { action: 'scanAdAccounts' });
    if (acc && acc.ok && acc.data && Array.isArray(acc.data.accounts)) {
      const pageBusiness = businesses.find(b => String(b.id || '') === String(pageBusinessId || '')) || null;
      accounts.push(...acc.data.accounts.map(a => ({
        ...a,
        business_id: a.business_id || pageBusinessId || '',
        business_name: bestBusinessLabel(a.business_id || pageBusinessId || '', '', a.business_name || pageBusiness?.name || ''),
        source: a.source || source,
      })));
    }
  } catch (e) {}
  try {
    if (pageBusinessId) {
      const local = await chrome.tabs.sendMessage(tabId, {
        action: 'fbBusinessOperation',
        operation: 'list_ad_accounts',
        payload: { business_id: pageBusinessId },
      });
      if (local && local.ok && local.data) {
        if (Array.isArray(local.data.businesses)) businesses.push(...local.data.businesses.map(b => ({ ...b, source: b.source || source })));
        if (Array.isArray(local.data.accounts)) {
          accounts.push(...local.data.accounts.map(a => ({
            ...a,
            business_id: a.business_id || pageBusinessId || '',
            business_name: bestBusinessLabel(a.business_id || pageBusinessId || '', '', a.business_name || ''),
            source: a.source || 'adsmanager_graph',
          })));
        }
      }
    }
  } catch (e) {}
  return { businesses, accounts, assets: { businesses, ad_accounts: accounts, pixels: [] } };
}

async function openAndScanBusinessUrl(url, reuseTabId, source) {
  let tab = null;
  let created = false;
  if (reuseTabId) {
    tab = await chrome.tabs.update(reuseTabId, { url, active: false });
  } else {
    tab = await chrome.tabs.create({ url, active: false });
    created = true;
  }
  await waitForTabComplete(tab.id, 22000);
  await sleep(3500);
  const data = await scanAssetsFromTab(tab.id, source);
  return { tabId: tab.id, created, data };
}

function businessOperationUrl(operation, payload = {}) {
  const bmId = cleanNumericId(payload.business_id || payload.bm_id || '');
  if (operation === 'list_businesses' || !bmId) return 'https://business.facebook.com/business/manage/select/';
  if (operation === 'list_pixels' || operation === 'share_pixel') {
    return `https://business.facebook.com/latest/settings/data_sources/pixels?business_id=${bmId}`;
  }
  if (operation === 'invite_user') {
    return `https://business.facebook.com/latest/settings/business_users?business_id=${bmId}`;
  }
  return `https://business.facebook.com/latest/settings/ad_accounts?business_id=${bmId}`;
}

async function findBusinessTab(bmId) {
  const tabs = sortFacebookTabs(await chrome.tabs.query({ url: '*://*.facebook.com/*' }));
  if (!tabs.length) return null;
  if (bmId) {
    const exact = tabs.find(t => String(t.url || '').includes(`business_id=${bmId}`));
    if (exact) return exact;
  }
  return tabs.find(t => String(t.url || '').includes('business.facebook.com')) || tabs[0];
}

async function runBusinessOperationInTab(operation, payload = {}) {
  const bmId = cleanNumericId(payload.business_id || payload.bm_id || '');
  let tab = await findBusinessTab(bmId);
  let created = false;
  const targetUrl = businessOperationUrl(operation, payload);

  if (!tab || (bmId && !String(tab.url || '').includes(`business_id=${bmId}`))) {
    tab = await chrome.tabs.create({ url: targetUrl, active: false });
    created = true;
  } else if (targetUrl && !String(tab.url || '').includes('business_id=') && bmId) {
    tab = await chrome.tabs.update(tab.id, { url: targetUrl, active: false });
  }

  await waitForTabComplete(tab.id, 24000);
  await sleep(operation === 'list_businesses' ? 1800 : 3200);
  await ensureContentScript(tab.id);

  try {
    const res = await chrome.tabs.sendMessage(tab.id, {
      action: 'fbBusinessOperation',
      operation,
      payload,
    });
    if (!res || !res.ok) throw new Error(res?.error || 'business_operation_failed');
    return res.data;
  } finally {
    if (created) {
      chrome.tabs.remove(tab.id).catch(() => {});
    }
  }
}

async function discoverBusinessAssetsViaPages() {
  const current = await buildAccountSummary({ skipCachedAssets: true }).catch(e => ({
    fb_user: null,
    accounts: [],
    businesses: [],
    assets: { ad_accounts: [], businesses: [], pixels: [] },
    fb_status: 'error',
    last_error: e.message || String(e),
  }));
  let merged = mergeAssetSummary(current, {});
  const discoveryUrls = [
    'https://business.facebook.com/business/manage/select/',
    'https://business.facebook.com/select/',
    'https://business.facebook.com/latest/home',
    'https://business.facebook.com/latest/settings/business_info',
    'https://business.facebook.com/settings/',
  ];
  let discoveryTabId = null;
  const errors = [];
  try {
    for (const url of discoveryUrls) {
      try {
        const scanned = await openAndScanBusinessUrl(url, discoveryTabId, 'business_discovery');
        discoveryTabId = scanned.tabId;
        merged = mergeAssetSummary(merged, scanned.data || {});
        if ((merged.businesses || []).length >= 2) break;
      } catch (e) {
        errors.push(`${url}: ${e.message || e}`);
      }
    }

    const bmIds = (merged.businesses || []).map(b => b.id).filter(Boolean).slice(0, 12);
    for (const bmId of bmIds) {
      try {
        const local = await runBusinessOperationInTab('list_ad_accounts', { business_id: bmId });
        merged = mergeAssetSummary(merged, local || {});
      } catch (e) {
        errors.push(`local:list_ad_accounts:${bmId}: ${e.message || e}`);
      }
      const urls = [
        `https://business.facebook.com/latest/settings/ad_accounts?business_id=${bmId}`,
        `https://business.facebook.com/latest/settings/business_users?business_id=${bmId}`,
        `https://business.facebook.com/latest/settings/pages?business_id=${bmId}`,
      ];
      for (const url of urls) {
        try {
          const scanned = await openAndScanBusinessUrl(url, discoveryTabId, `bm_${bmId}`);
          discoveryTabId = scanned.tabId;
          merged = mergeAssetSummary(merged, scanned.data || {});
        } catch (e) {
          errors.push(`${url}: ${e.message || e}`);
        }
      }
    }
  } finally {
    if (discoveryTabId) {
      chrome.tabs.remove(discoveryTabId).catch(() => {});
    }
  }

  const fbUserId = current.fb_user?.id || '';
  const discovered = {
    ...merged,
    fb_user: current.fb_user || null,
    fb_status: (merged.accounts.length || merged.businesses.length) ? 'ok' : current.fb_status,
    discovered_at: new Date().toISOString(),
    discovery_errors: errors.slice(-10),
  };
  await chrome.storage.local.set({ discoveredAssets: discovered, discoveredAssetsFbUserId: fbUserId });
  return discovered;
}

function parseTimeMs(value) {
  const t = value ? new Date(value).getTime() : 0;
  return Number.isFinite(t) ? t : 0;
}

async function maybeAutoDiscoverAssets(reason, summary = null) {
  if (assetDiscoveryPromise) return assetDiscoveryPromise;
  const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' }).catch(() => []);
  if (!tabs.length) return null;

  const d = await chrome.storage.local.get(['discoveredAssets', 'assetDiscoveryLastAutoAt']);
  const cached = d.discoveredAssets || null;
  const cachedAt = parseTimeMs(cached?.discovered_at);
  const lastAutoAt = parseTimeMs(d.assetDiscoveryLastAutoAt);
  const now = Date.now();
  const summaryBizCount = (summary?.businesses || summary?.assets?.businesses || []).length;
  const cachedBizCount = (cached?.businesses || cached?.assets?.businesses || []).length;
  const bestBizCount = Math.max(summaryBizCount, cachedBizCount);
  const cacheFresh = cachedAt && now - cachedAt < ASSET_DISCOVERY_FRESH_MS;
  const recentlyAuto = lastAutoAt && now - lastAutoAt < ASSET_DISCOVERY_MIN_INTERVAL_MS;

  if (cacheFresh && bestBizCount > 1) return null;
  if (recentlyAuto && bestBizCount > 0) return null;

  await chrome.storage.local.set({ assetDiscoveryLastAutoAt: new Date().toISOString(), assetDiscoveryLastReason: reason || 'auto' });
  assetDiscoveryPromise = discoverBusinessAssetsViaPages()
    .then(async (result) => {
      await chrome.storage.local.set({
        miraStatus: {
          ok: true,
          updatedAt: new Date().toLocaleString(),
          accountCount: result.accounts.length,
          fbStatus: result.fb_status,
          accounts: result.accounts,
          businesses: result.businesses || [],
          assets: result.assets || {},
          fbUser: result.fb_user,
          last_error: result.last_error || null,
          cached_assets_at: result.discovered_at || '',
        }
      });
      return result;
    })
    .catch(async (e) => {
      await chrome.storage.local.set({ assetDiscoveryLastError: e.message || String(e) });
      return null;
    })
    .finally(() => {
      assetDiscoveryPromise = null;
    });
  return assetDiscoveryPromise;
}

async function scanBusinessAccountsFromOpenTabs() {
  const tabs = sortFacebookTabs(await chrome.tabs.query({ url: '*://*.facebook.com/*' }));
  const byId = new Map();
  for (const tab of tabs) {
    try {
      const res = await chrome.tabs.sendMessage(tab.id, { action: 'scanBusinessAccounts' });
      if (!res || !res.ok || !res.data || !Array.isArray(res.data.businesses)) continue;
      for (const item of res.data.businesses) {
        const id = String(item.id || '').trim();
        if (!/^\d{6,}$/.test(id) || byId.has(id)) continue;
        byId.set(id, {
          id,
          name: bestBusinessLabel(id, '', item.name),
          source: item.source || 'page_scan',
        });
      }
    } catch (e) {}
  }
  return { businesses: [...byId.values()] };
}

// ========== Graph API（通过 content-script）==========

function sortFacebookTabs(tabs) {
  return [...(tabs || [])].sort((a, b) => {
    const score = (tab) => {
      const u = String(tab?.url || '');
      return (
        (u.includes('business.facebook.com') ? 100 : 0) +
        (u.includes('adsmanager') ? 50 : 0) +
        (u.includes('business') ? 20 : 0)
      );
    };
    return score(b) - score(a);
  });
}

async function graphGetRaw(token, path, params = {}) {
  const d = await chrome.storage.local.get(['apiVersion']);
  const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' });
  if (tabs.length > 0) {
    for (const tab of sortFacebookTabs(tabs)) {
      try {
        const res = await chrome.tabs.sendMessage(tab.id, {
          action: 'graphApiCall', method: 'GET', path, params, token,
          apiVersion: d.apiVersion || 'v22.0',
        });
        if (res && res.ok && res.data && !res.data.error) return res.data;
        if (res && res.data && res.data.error) throw new Error(res.data.error.message || 'Graph API error');
      } catch (e) {
        if (e.message && e.message.includes('Graph')) throw e;
        continue;
      }
    }
  }
  // fallback 直达
  const cp = String(path).replace(/^\/+/, '');
  const url = new URL(`${GRAPH_BASE}/${cp}`);
  Object.entries(params || {}).forEach(([k, v]) => { if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, v); });
  url.searchParams.set('access_token', token);
  const res = await safeFetch(url.toString());
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) throw new Error(data.error?.message || `HTTP ${res.status}`);
  return data;
}

async function graphRequest(method, path, params = {}) {
  let localError = null;
  try {
    return await runBusinessOperationInTab('adsmanager_graph_request', {
      method,
      path,
      params,
    });
  } catch (e) {
    localError = e;
  }

  const s = await getSettings();
  const token = String(s.accessToken || '').trim();
  if (!token) throw localError || new Error('session_invalid');

  // 获取 webRequest 捕获到的 API 版本
  const d = await chrome.storage.local.get(['apiVersion']);

  const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' });
  if (tabs.length > 0) {
    for (const tab of sortFacebookTabs(tabs)) {
      try {
        const res = await chrome.tabs.sendMessage(tab.id, {
          action: 'graphApiCall', method, path, params, token,
          apiVersion: d.apiVersion || 'v22.0',
        });
        if (res && res.ok && res.data && !res.data.error) return res.data;
        if (res && res.data && res.data.error) throw res.data.error;
      } catch (e) {
        if (e.code || e.error_subcode) throw e;
        continue;
      }
    }
  }

  const cp = String(path).replace(/^\/+/, '');
  const url = new URL(`${GRAPH_BASE}/${cp}`);
  const isGetOrDel = method === 'GET' || method === 'DELETE';
  const body = new URLSearchParams();
  Object.entries(params || {}).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') {
      if (isGetOrDel) url.searchParams.set(k, String(v));
      else body.set(k, String(v));
    }
  });
  url.searchParams.set('access_token', token);
  const init = { method };
  if (!isGetOrDel) { init.headers = { 'Content-Type': 'application/x-www-form-urlencoded' }; init.body = body.toString(); }
  const res = await safeFetch(url.toString(), init);
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) {
    const e = data.error || {};
    throw { code: e.code || 0, error_subcode: e.error_subcode || e.subcode || 0, message: e.message || `HTTP ${res.status}` };
  }
  return data;
}

// ========== Mira API: 绑定 ==========

async function bindExecutor(bindCode, browserLabel) {
  const installId = await ensureInstallId();
  const data = await miraApi('/api/local-executors/bind', {
    code: bindCode,
    browser_name: browserLabel || `Chrome-${installId.slice(-6).toUpperCase()}`,
    install_id: installId,
    extension_version: VERSION,
    capabilities: ['graph_api', 'ads_read', 'ads_management', 'discover_accounts', 'page_read'],
  });

  await chrome.storage.local.set({
    browserName: browserLabel || `Chrome-${installId.slice(-6).toUpperCase()}`,
    nodeId: data.node_id || '',
    nodeSecret: data.node_secret || '',
    teamId: data.team_id || null,
    operatorId: data.operator_id || null,
    operatorName: data.operator_name || '',
    pollIntervalSec: data.poll_interval_sec || 10,
    executorStatus: {
      bound: true,
      teamId: data.team_id,
      operatorId: data.operator_id,
      operatorName: data.operator_name || '',
    },
  });
  await ensureAlarms();

  // 绑定后自动尝试捕获 Token：有 FB 标签页就刷新触发 webRequest
  const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' });
  if (tabs.length > 0) {
    // 有 FB 页面 → 刷新触发被动捕获
    const preferred = tabs.find(t => t.url && t.url.includes('adsmanager')) ||
                      tabs.find(t => t.url && t.url.includes('business')) ||
                      tabs[0];
    chrome.tabs.reload(preferred.id);
    setTimeout(() => maybeAutoDiscoverAssets('bind').then(() => heartbeat().catch(() => {})).catch(() => {}), randomBetween(10000, 18000));
  }

  return data;
}

// ========== Mira API: 心跳 ==========

async function heartbeat() {
  const s = await getSettings();
  if (!s.nodeId) return { ok: false, reason: 'not_bound' };

  const summary = await buildAccountSummary();

  const installId = await ensureInstallId();
  const body = {
    node_id: s.nodeId,
    node_secret: s.nodeSecret,
    browser_name: (await chrome.storage.local.get(['browserName'])).browserName || `Chrome-${installId.slice(-6).toUpperCase()}`,
    capabilities: ['graph_api', 'ads_read', 'ads_management', 'discover_accounts', 'page_read'],
    status: summary.fb_status === 'need_fb_tab' ? 'need_fb_tab' : 'online',
    fb_user: summary.fb_user || { id: '', name: '' },
    accounts: summary.accounts || [],
    businesses: summary.businesses || [],
    assets: summary.assets || {},
    token_summary: {
      present: summary.fb_status !== 'need_fb_tab',
      fb_user_id: (summary.fb_user || {}).id || '',
      fb_user_name: (summary.fb_user || {}).name || '',
      accounts: summary.accounts || [],
      account_ids: (summary.accounts || []).map(a => a.act_id || a.id || a.account_id).filter(Boolean),
      businesses: summary.businesses || [],
      assets: summary.assets || {},
      has_ads_management: true,
      has_ads_read: true,
      capabilities: ['graph_api', 'ads_read', 'ads_management', 'discover_accounts', 'page_read'],
      last_error: summary.last_error || '',
    },
    queue: {
      running: runningTasks.size,
      waiting: 0,
    },
  };

  const data = await miraApi('/api/local-executors/heartbeat', body);
  maybeAutoDiscoverAssets('heartbeat', summary)
    .then((result) => { if (result) heartbeat().catch(() => {}); })
    .catch(() => {});
  const status = {
    ok: true,
    updatedAt: new Date().toLocaleString(),
    accountCount: summary.accounts.length,
    fbStatus: summary.fb_status,
    accounts: summary.accounts,
    businesses: summary.businesses || [],
    assets: summary.assets || {},
    fbUser: summary.fb_user,
    last_error: summary.last_error || null,
  };
  await chrome.storage.local.set({ miraStatus: status, lastHeartbeat: new Date().toISOString() });
  return status;
}

// ========== Mira API: 拉取任务 ==========

async function pollTasks() {
  const s = await getSettings();
  if (!s.nodeId) return [];

  try {
    const data = await miraApi('/api/local-executor/poll', {
      node_id: s.nodeId,
      node_secret: s.nodeSecret,
      capacity: 1,
    });
    if (Array.isArray(data.tasks) && data.tasks.length) return data.tasks;
    if (data.task) return [data.task];
    return [];
  } catch (e) {
    return [];
  }
}

// ========== Mira API: 回传结果 ==========

async function reportTaskResult(taskId, status, data, error, durationMs) {
  const s = await getSettings();
  const normalizedStatus = status === 'error' ? 'failed' : status;
  let errorText = '';
  if (typeof error === 'string') {
    errorText = error;
  } else if (error && typeof error === 'object') {
    const parts = [];
    if (error.message) parts.push(error.message);
    if (error.code) parts.push(`code=${error.code}`);
    if (error.subcode || error.error_subcode) parts.push(`subcode=${error.subcode || error.error_subcode}`);
    errorText = parts.join(' | ') || JSON.stringify(error);
  }
  const body = {
    node_id: s.nodeId,
    node_secret: s.nodeSecret,
    status: normalizedStatus,
    result: data || {},
    error: errorText,
    duration_ms: durationMs,
  };
  return miraApi(`/api/local-executor/tasks/${encodeURIComponent(taskId)}/update`, body);
}

// ========== 任务执行 ==========

function categorizeError(err) {
  const msg = String(err.message || err || '');
  const code = err.code || 0;
  const subcode = err.error_subcode || 0;
  if (code === 190) return 'session_invalid';
  if (code === 10 || code === 200 || msg.includes('permission')) return 'permission_denied';
  if (code === 4 || code === 17 || msg.includes('rate')) return 'rate_limited';
  if (msg.includes('need_fb_tab')) return 'need_fb_tab';
  if (msg.includes('writable') || msg.includes('account')) return 'account_not_writable';
  if (code || subcode) return 'fb_api_error';
  return 'unknown_error';
}

async function runGraphTask(task) {
  const { operation, method, path, params, body } = task;

  // discover_accounts — 特殊任务，返回账户列表
  if (operation === 'discover_accounts' || task.task_type === 'discover_accounts') {
    const summary = await buildAccountSummary();
    return {
      data: (summary.accounts || []).map(a => ({
        id: a.act_id || ('act_' + a.account_id),
        name: a.name,
        currency: a.currency,
        timezone_name: a.timezone,
        business_id: a.business_id || '',
        business_name: a.business_name || '',
        write_status: a.write_status === 'ok' ? 'writable' : 'read_only',
      })),
      businesses: summary.businesses || [],
      assets: summary.assets || {},
    };
  }

  if (path && !isWritablePath(path)) throw new Error('path_not_whitelisted');

  switch (operation) {
    case 'graph_get':
      return await graphRequest('GET', path, params || {});
    case 'graph_post':
      return await graphRequest('POST', path, body || params || {});
    case 'graph_delete':
      return await graphRequest('DELETE', path, params || {});
    case 'graph_account_probe':
      return await graphRequest('GET', path, params || { fields: 'id,name,account_status,currency,timezone_name' });
    default:
      return await graphRequest(method || 'POST', path, body || params || {});
  }
}

async function executeTask(task) {
  const taskId = task && (task.task_id || task.id || '');
  if (!task || !taskId) return;
  if (runningTasks.has(taskId)) return;

  // 幂等检查
  if (task.idempotency_key && seenIdempotency.has(task.idempotency_key)) {
    console.debug('[mira] 跳过已执行过的幂等任务:', task.idempotency_key);
    return;
  }

  // 账户队列锁：每 act_xxx 同一时间只执行一个
  const actId = task.account_id || '';
  if (actId && accountLocks.get(actId)) return;
  if (actId) accountLocks.set(actId, true);

  runningTasks.set(taskId, true);
  const start = Date.now();

  try {
    const result = await runGraphTask(task);
    const dur = Date.now() - start;
    await reportTaskResult(taskId, 'success', result, null, dur);

    // 标记幂等
    if (task.idempotency_key) seenIdempotency.add(task.idempotency_key);

  } catch (err) {
    const dur = Date.now() - start;
    await reportTaskResult(taskId, 'failed', {}, {
      code: err.code || 0,
      subcode: err.error_subcode || 0,
      message: err.message || String(err),
    }, dur);
  } finally {
    runningTasks.delete(taskId);
    if (actId) accountLocks.delete(actId);
  }
}

// ========== 告警 ==========

async function ensureAlarms() {
  const s = await getSettings();
  if (!s.nodeId) return; // 未绑定不启动
  const pollSec = 10;

  await chrome.alarms.clear(HEARTBEAT_ALARM);
  await chrome.alarms.create(HEARTBEAT_ALARM, { periodInMinutes: randomBetween(0.4, 0.9) }); // ~25-55s

  await chrome.alarms.clear(POLL_ALARM);
  await chrome.alarms.create(POLL_ALARM, { periodInMinutes: pollSec / 60 });

  await chrome.alarms.clear(TOKEN_ALARM);
  await chrome.alarms.create(TOKEN_ALARM, { delayInMinutes: randomBetween(50, 70) });
}

async function scheduleTokenRefresh() {
  await chrome.alarms.clear(TOKEN_ALARM);
  await chrome.alarms.create(TOKEN_ALARM, { delayInMinutes: randomBetween(50, 70) });
}

// ========== 初始化 ==========

async function initialSetup() {
  await new Promise(r => setTimeout(r, randomBetween(5000, 15000)));
  const s = await getSettings();
  if (s.nodeId && s.accessToken) {
    // 已有绑定和 Token，做个心跳
    heartbeat().catch(() => {});
  }
}

chrome.runtime.onInstalled.addListener(() => {
  ensureAlarms().catch(() => {});
  setTimeout(() => initialSetup().catch(() => {}), 5000);
});

initialSetup().catch(() => {});

// ========== 告警处理 ==========

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === HEARTBEAT_ALARM) {
    heartbeat().catch(() => {});
    return;
  }
  if (alarm.name === POLL_ALARM) {
    pollTasks().then(tasks => {
      for (const t of tasks) executeTask(t).catch(() => {});
    }).catch(() => {});
    return;
  }
  if (alarm.name === TOKEN_ALARM) {
    silentExtractAndSave().then(async (result) => {
      if (result) await heartbeat().catch(() => {});
      await scheduleTokenRefresh();
    }).catch(() => {});
    return;
  }
});

// ========== 消息处理 ==========

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  (async () => {
    // — 设置 —
    if (msg.type === 'getSettings') {
      sendResponse({ ok: true, data: await getSettings() });
      return;
    }
    if (msg.type === 'getFullStatus') {
      const s = await getSettings();
      const cookies = await getFacebookCookies();
      const d = await chrome.storage.local.get(['tokenStatus']);
      sendResponse({ ok: true, data: {
        ...s,
        tokenStatus: d.tokenStatus || null,
        cookies: { c_user: cookies.c_user || null },
        runningTasks: runningTasks.size,
        version: VERSION,
      }});
      return;
    }
    if (msg.type === 'saveSettings') {
      await chrome.storage.local.set({
        accessToken: msg.accessToken || '',
        expiresInMinutes: msg.expiresInMinutes || '',
        tokenExpiresAt: msg.tokenExpiresAt || '',
      });
      sendResponse({ ok: true, data: await getSettings() });
      return;
    }

    // — 绑定 —
    if (msg.type === 'bind') {
      try {
        const data = await bindExecutor(msg.bindCode, msg.browserLabel);
        sendResponse({ ok: true, data });
      } catch (e) {
        sendResponse({ ok: false, error: e.message });
      }
      return;
    }

    // — 解绑 —
    if (msg.type === 'unbind') {
      await chrome.storage.local.remove(['nodeId', 'nodeSecret', 'teamId', 'operatorId', 'operatorName']);
      await chrome.storage.local.set({ executorStatus: { bound: false } });
      sendResponse({ ok: true, data: { unbound: true } });
      return;
    }

    // — 心跳 —
    if (msg.type === 'heartbeat') {
      try {
        sendResponse({ ok: true, data: await heartbeat() });
      } catch (e) {
        sendResponse({ ok: false, error: e.message });
      }
      return;
    }

    // — 刷新账户 —
    if (msg.type === 'refreshAccounts') {
      try {
        silentExtractAndSave().catch(() => {});
        const summary = await buildAccountSummary();
        // 持久化到本地，popup 刷新不丢失
        await chrome.storage.local.set({
          miraStatus: {
            ok: true,
            updatedAt: new Date().toLocaleString(),
            accountCount: summary.accounts.length,
            fbStatus: summary.fb_status,
            accounts: summary.accounts,
            businesses: summary.businesses || [],
            assets: summary.assets || {},
            fbUser: summary.fb_user,
            last_error: summary.last_error || null,
          }
        });
        maybeAutoDiscoverAssets('refresh_accounts', summary)
          .then((result) => { if (result) heartbeat().catch(() => {}); })
          .catch(() => {});
        sendResponse({ ok: true, data: summary });
      } catch (e) {
        sendResponse({ ok: false, error: e.message });
      }
      return;
    }

    if (msg.type === 'getBusinessAssets') {
      try {
        const summary = await buildAccountSummary();
        sendResponse({ ok: true, data: {
          fb_user: summary.fb_user || null,
          accounts: summary.accounts || [],
          businesses: summary.businesses || [],
          assets: summary.assets || {},
          fb_status: summary.fb_status || '',
          last_error: summary.last_error || '',
        }});
      } catch (e) {
        sendResponse({ ok: false, error: e.message || String(e) });
      }
      return;
    }

    if (msg.type === 'discoverBusinessAssets') {
      try {
        const summary = await discoverBusinessAssetsViaPages();
        await chrome.storage.local.set({
          miraStatus: {
            ok: true,
            updatedAt: new Date().toLocaleString(),
            accountCount: summary.accounts.length,
            fbStatus: summary.fb_status,
            accounts: summary.accounts,
            businesses: summary.businesses || [],
            assets: summary.assets || {},
            fbUser: summary.fb_user,
            last_error: summary.last_error || null,
            cached_assets_at: summary.discovered_at || '',
          }
        });
        heartbeat().catch(() => {});
        sendResponse({ ok: true, data: summary });
      } catch (e) {
        sendResponse({ ok: false, error: e.message || String(e) });
      }
      return;
    }

    // — 手动提取 Token —
    if (msg.type === 'silentRefresh') {
      const ex = await silentExtractAndSave();
      if (ex) heartbeat().catch(() => {});
      sendResponse({ ok: true, data: { success: !!ex, tokenMask: ex ? maskToken(ex.eaa_token) : null } });
      return;
    }

    // — 清除 Token —
    if (msg.type === 'clearToken') {
      await chrome.storage.local.set({ accessToken: '', expiresInMinutes: '', tokenExpiresAt: '' });
      sendResponse({ ok: true, data: { cleared: true } });
      return;
    }

    // — Content-script 自动缓存 —
    if (msg.type === 'facebookTokenCache') {
      const p = msg.payload || {};
      if (p.eaa_token && (!cachedExtractedToken || p.eaa_token !== cachedExtractedToken.eaa_token)) {
        cachedExtractedToken = p;
        lastTokenExtractAt = Date.now();
        const d = await chrome.storage.local.get(['accessToken']);
        if (p.eaa_token !== d.accessToken) {
          const exp = new Date(Date.now() + 55 * 60000).toISOString();
          await chrome.storage.local.set({ accessToken: p.eaa_token, expiresInMinutes: '55', tokenExpiresAt: exp });
          lastProbeAt = 0;
          lastProbeSummary = null;
          heartbeat().catch(() => {});
        }
      }
      sendResponse({ ok: true });
      return;
    }

    // — FB 内部 REST API 调用（Cookie 鉴权，自动尝试多个端点）—
    if (msg.type === 'fbInternalApi') {
      try {
        const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' });
        if (tabs.length === 0) throw new Error('需要打开 Facebook 页面');
        const [tab] = sortFacebookTabs(tabs);
        const res = await chrome.tabs.sendMessage(tab.id, {
          action: 'fbInternalApi',
          endpoints: msg.endpoints || [],
        });
        if (res && res.ok) { sendResponse({ ok: true, data: res.data }); }
        else { sendResponse({ ok: false, error: res?.error || '内部 API 调用失败' }); }
      } catch (e) {
        sendResponse({ ok: false, error: e.message || String(e) });
      }
      return;
    }

    // — FB 内部 GraphQL 调用（BM 列表等，Cookie 鉴权）—
    if (msg.type === 'fbGraphqlCall') {
      try {
        const tabs = await chrome.tabs.query({ url: '*://*.facebook.com/*' });
        if (tabs.length === 0) throw new Error('需要打开 Facebook 页面');
        const [tab] = sortFacebookTabs(tabs);
        const res = await chrome.tabs.sendMessage(tab.id, {
          action: 'fbGraphqlCall',
          variables: msg.variables || {},
          doc_id: msg.doc_id || null,
          query_params: msg.query_params || {},
        });
        if (res && res.ok) { sendResponse({ ok: true, data: res.data }); }
        else { sendResponse({ ok: false, error: res?.error || 'GraphQL error' }); }
      } catch (e) {
        sendResponse({ ok: false, error: e.message || String(e) });
      }
      return;
    }

    if (msg.type === 'fbBusinessOperation') {
      try {
        const data = await runBusinessOperationInTab(msg.operation, msg.payload || {});
        sendResponse({ ok: true, data });
      } catch (e) {
        sendResponse({ ok: false, error: e.message || String(e) });
      }
      return;
    }

    if (msg.type === 'scanBusinessAccounts') {
      try {
        sendResponse({ ok: true, data: await scanBusinessAccountsFromOpenTabs() });
      } catch (e) {
        sendResponse({ ok: false, error: e.message || String(e) });
      }
      return;
    }

    // — 通用 Graph API 调用（像素分享/BM邀请用）—
    if (msg.type === 'graphApiCall') {
      try {
        const data = await graphRequest(msg.method || 'GET', msg.path, msg.params || {});
        sendResponse({ ok: true, data });
      } catch (e) {
        const code = e.code ? `[code=${e.code}]` : '';
        const sub = e.error_subcode ? `[sub=${e.error_subcode}]` : '';
        sendResponse({ ok: false, error: `${code}${sub} ${e.message || String(e)}`.trim() });
      }
      return;
    }

    // — 复制诊断信息 —
    if (msg.type === 'getDiagnostics') {
      const s = await getSettings();
      const cookies = await getFacebookCookies();
      sendResponse({ ok: true, data: {
        version: VERSION,
        installId: await ensureInstallId(),
        executorId: s.nodeId,
        bound: !!s.nodeId,
        teamId: s.teamId,
        operatorId: s.operatorId,
        operatorName: s.operatorName,
        fbUser: cookies.c_user ? { id: cookies.c_user } : null,
        hasToken: !!s.accessToken,
        tokenMask: maskToken(s.accessToken),
        tokenExpiresAt: s.tokenExpiresAt,
        runningTasks: runningTasks.size,
      }});
      return;
    }

    sendResponse({ ok: false, error: 'unknown message' });
  })().catch(err => sendResponse({ ok: false, error: err.message }));
  return true;
});

// ========== webRequest 被动 Token 捕获 ==========
// 零检测风险：Facebook 自己的请求经过浏览器网络层，
// 插件只读 URL 中的 access_token 参数，不发额外请求。

chrome.webRequest.onBeforeRequest.addListener(
  (details) => {
    try {
      const url = new URL(details.url);
      const token = url.searchParams.get('access_token');
      // EAAB/EAAC 才是标准 Graph API Token；EAAG 等是前端内部 Token 不可用于 API
      if (token && /^EAA[BC]/.test(token) && token !== cachedExtractedToken?.eaa_token) {
        const apiVersion = (url.pathname.match(/\/v(\d+\.?\d?)\//) || [])[1] || '';

        // 静默存储 — 不写日志，不给 popup 发通知
        chrome.storage.local.set({
          accessToken: token,
          apiVersion: apiVersion || 'v22.0',
          expiresInMinutes: '55',
          tokenExpiresAt: new Date(Date.now() + 55 * 60000).toISOString(),
        }).catch(() => {});

        cachedExtractedToken = {
          eaa_token: token,
          eaa_source: 'webRequest:' + url.hostname,
          extracted_at: new Date().toISOString(),
        };
        lastTokenExtractAt = Date.now();
        lastProbeAt = 0;
        lastProbeSummary = null;

        // 探测权限
        verifyTokenPermissions(token).catch(() => {});
        // Token 变化后尽快心跳一次
        setTimeout(() => heartbeat().catch(() => {}), randomBetween(2000, 8000));
      }
    } catch (e) { /* 静默 */ }
  },
  { urls: ['*://*.facebook.com/*', '*://graph.facebook.com/*'] }
);
