/**
 * Mira 本地执行器 v2.0 — Popup
 * 标签: 状态 | 像素分享 | BM邀请
 */
function $(id) { return document.getElementById(id); }

function send(msg) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage(msg, (res) => {
      if (chrome.runtime.lastError) { reject(new Error(chrome.runtime.lastError.message)); return; }
      if (!res || !res.ok) { reject(new Error(res?.error || '操作失败')); return; }
      resolve(res.data);
    });
  });
}

function cleanLabelText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function isGenericBusinessLabel(value) {
  const s = cleanLabelText(value).toLowerCase();
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

function businessDisplayName(item) {
  const id = String(item?.id || item?.business_id || '').replace(/\D/g, '');
  const raw = cleanLabelText(item?.name || item?.business_name || item?.businessName || item?.label || '');
  return !isGenericBusinessLabel(raw) ? raw : (id ? `BM ${id}` : 'BM');
}

function graphApi(method, path, params) {
  return send({ type: 'graphApiCall', method, path, params: params || {} });
}

function fbGraphql(variables, docId, queryParams) {
  return send({ type: 'fbGraphqlCall', variables, doc_id: docId, query_params: queryParams });
}

function fbInternalApi(endpoints) {
  return send({ type: 'fbInternalApi', endpoints });
}

function fbBusinessOperation(operation, payload) {
  return send({ type: 'fbBusinessOperation', operation, payload: payload || {} });
}

function scanBusinessAccounts() {
  return send({ type: 'scanBusinessAccounts' });
}

function getBusinessAssets() {
  return send({ type: 'getBusinessAssets' });
}

function normalizeBusinessList(raw) {
  const out = [];
  const seen = new Set();
  const push = (item, source = '') => {
    if (!item || typeof item !== 'object') return;
    const id = String(item.id || item.business_id || item.businessID || item.businessId || '').trim();
    if (!/^\d{6,}$/.test(id) || seen.has(id)) return;
    const name = businessDisplayName({ ...item, id });
    seen.add(id);
    out.push({ id, name, source });
  };
  const walk = (value, depth = 0, keyHint = '') => {
    if (!value || depth > 8) return;
    if (Array.isArray(value)) {
      value.forEach(v => walk(v, depth + 1, keyHint));
      return;
    }
    if (typeof value !== 'object') return;
    if (
      value.id && value.name &&
      (/business/i.test(keyHint) || value.business_id || value.businessID || value.__typename === 'Business')
    ) {
      push(value, keyHint || 'object');
    }
    if (value.node && typeof value.node === 'object') walk(value.node, depth + 1, keyHint || 'node');
    Object.entries(value).forEach(([k, v]) => {
      if (/business|biz|bm/i.test(k)) walk(v, depth + 1, k);
      else if (depth < 3) walk(v, depth + 1, k);
    });
  };
  if (Array.isArray(raw)) raw.forEach(v => push(v, 'array'));
  walk(raw);
  return out;
}

async function loadBusinessList() {
  const attempts = [];
  try {
    const summary = await getBusinessAssets();
    const list = normalizeBusinessList(summary?.businesses || summary?.assets?.businesses || []);
    if (list.length) return list;
    attempts.push('本地资产树返回空');
  } catch (e) {
    attempts.push(`本地资产树: ${e.message || e}`);
  }

  try {
    const data = await fbBusinessOperation('list_businesses', {});
    const list = normalizeBusinessList(data?.businesses || data);
    if (list.length) return list;
    attempts.push('本地 BM 接口返回空');
  } catch (e) {
    attempts.push(`本地 BM 接口: ${e.message || e}`);
  }

  try {
    const data = await graphApi('GET', 'me/businesses', { fields: 'id,name', limit: 200 });
    const list = normalizeBusinessList(data?.data || data);
    if (list.length) return list;
    attempts.push('Graph API 返回空');
  } catch (e) {
    attempts.push(`Graph API: ${e.message || e}`);
  }

  try {
    const data = await fbInternalApi([
      { url: 'https://business.facebook.com/business/manage/select/', method: 'GET' },
      { url: 'https://business.facebook.com/ajax/business/manage/select/', method: 'GET' },
      { url: 'https://business.facebook.com/latest/home', method: 'GET' },
      { url: 'https://business.facebook.com/settings/', method: 'GET' },
    ]);
    const list = normalizeBusinessList(data?.businesses || data);
    if (list.length) return list;
    attempts.push('内部端点返回空');
  } catch (e) {
    attempts.push(`内部端点: ${e.message || e}`);
  }

  try {
    const data = await scanBusinessAccounts();
    const list = normalizeBusinessList(data?.businesses || data);
    if (list.length) return list;
    attempts.push('页面扫描返回空');
  } catch (e) {
    attempts.push(`页面扫描: ${e.message || e}`);
  }

  throw new Error(attempts.join('；'));
}

function elapsed(ts) {
  if (!ts) return '--';
  const s = Math.round((Date.now() - new Date(ts).getTime()) / 1000);
  if (s < 60) return `${s}秒前`;
  if (s < 3600) return `${Math.round(s / 60)}分钟前`;
  return `${Math.round(s / 3600)}小时前`;
}

function remaining(ts) {
  if (!ts) return '--';
  const s = Math.round((new Date(ts).getTime() - Date.now()) / 1000);
  return s <= 0 ? '已过期' : s < 3600 ? `~${Math.round(s / 60)}分钟` : `~${Math.round(s / 3600)}小时`;
}

function setMsg(el, text, cls) {
  el.innerHTML = text;
  el.className = 'msg ' + (cls || 'info');
  el.style.display = text ? 'block' : 'none';
}

let statusData = null;
let allAccounts = [];
let acctsExpanded = false;

// ========== Tab 切换 ==========

function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelector(`.tab[data-tab="${name}"]`)?.classList.add('active');
  const panel = document.getElementById(`panel-${name}`);
  if (panel) panel.classList.add('active');
  if (name === 'status') loadAll().catch(() => {});
}

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => switchTab(tab.dataset.tab));
});

// ========== 状态面板渲染 ==========

function renderBind(s) {
  const el = $('bindContent');
  const bound = !!(s && s.nodeId);
  $('btnUnbind').style.display = bound ? '' : 'none';
  if (!bound) {
    el.innerHTML = '<div class="kv"><span class="k">状态</span><span class="v red">未绑定</span></div><div style="font-size:9px;color:#86868b">点击"绑定"，输入 Mira 后台生成的绑定码</div>';
    return;
  }
  let h = '<div class="kv"><span class="k">状态</span><span class="v green">✅ 已绑定</span></div>';
  if (s.operatorName) h += `<div class="kv"><span class="k">运营</span><span class="v">${s.operatorName}</span></div>`;
  if (s.teamId) h += `<div class="kv"><span class="k">团队 ID</span><span class="v blue">${s.teamId}</span></div>`;
  h += `<div class="kv"><span class="k">ID</span><span class="v gray" style="font-size:8px">${s.nodeId}</span></div>`;
  el.innerHTML = h;
}

function renderFb(s) {
  const el = $('fbContent');
  if (!s) { el.innerHTML = '加载中...'; return; }
  const fbOk = !!(s.cookies?.c_user);
  const hasToken = !!(s.accessToken);
  const ts = s.tokenStatus || {};
  let h = '';
  h += `<div class="kv"><span class="k">FB</span><span class="v ${fbOk ? 'green' : 'amber'}">${fbOk ? '✅ 已登录' : '⚠️ 未打开'}</span></div>`;
  if (ts.fbName) h += `<div class="kv"><span class="k">Meta</span><span class="v">${ts.fbName}</span></div>`;
  else if (fbOk) h += `<div class="kv"><span class="k">用户 ID</span><span class="v">${s.cookies.c_user}</span></div>`;
  if (hasToken) {
    const mask = s.accessToken.length > 12 ? s.accessToken.slice(0,6)+'****'+s.accessToken.slice(-4) : '****';
    h += `<div class="kv"><span class="k">Token</span><span class="v blue">${mask}</span></div>`;
    if (s.tokenExpiresAt) h += `<div class="kv"><span class="k">过期</span><span class="v ${remaining(s.tokenExpiresAt)==='已过期'?'red':'green'}">${remaining(s.tokenExpiresAt)}</span></div>`;
    // 关键权限
    if (ts.hasAdsMgmt) h += `<span class="badge g">ads_management</span> `;
    if (ts.hasBizMgmt) h += `<span class="badge b">business_management</span> `;
    if (!ts.hasAdsMgmt && !ts.hasBizMgmt && ts.permissions) {
      h += `<span class="badge y">权限探测中...</span>`;
    }
  } else {
    h += '<div class="kv"><span class="k">本地会话</span><span class="v blue">浏览器执行</span></div>';
    h += '<div style="font-size:8px;color:#86868b">Token 可选；账户/像素/邀请优先使用当前 Chrome 登录会话。</div>';
  }
  if (ts.error) h += `<div class="kv"><span class="k">错误</span><span class="v red" style="font-size:8px">${ts.error}</span></div>`;
  el.innerHTML = h;
}

function renderAccounts(summary) {
  const list = $('acctList');
  const count = $('acctCount');
  const toggle = $('acctToggle');
  const accounts = summary?.accounts || [];
  allAccounts = accounts;
  count.textContent = accounts.length ? `(${accounts.length}个)` : '';
  if (!accounts.length) {
    list.innerHTML = '<div style="font-size:9px;color:#86868b">暂无账户，点"刷新账户"会用当前 Chrome 登录会话读取。</div>';
    toggle.style.display = 'none';
    return;
  }
  toggle.style.display = accounts.length > 5 ? '' : 'none';
  const show = acctsExpanded ? accounts : accounts.slice(0, 5);
  list.innerHTML = show.map(a => {
    const statusOk = a.write_status === 'ok' || a.write_status === 'writable';
    const bm = a.business_name || a.business_id || '';
    return `<div class="acct-item"><span>${a.name || a.account_id}${bm ? `<br><span style="color:#86868b;font-size:8px">${bm}</span>` : ''}</span><span class="${statusOk?'green':'red'}">${statusOk?'可写':'异常'}</span></div>`;
  }).join('');
  toggle.textContent = acctsExpanded ? '▼ 收起' : `▶ 展开 (${accounts.length})`;
}

function renderBusinesses(summary) {
  const list = $('bizList');
  const count = $('bizCount');
  if (!list || !count) return;
  const businesses = summary?.businesses || summary?.assets?.businesses || [];
  const accounts = summary?.accounts || [];
  count.textContent = businesses.length ? `(${businesses.length}个)` : '';
  if (!businesses.length) {
    const fromAccounts = accounts.filter(a => a.business_id || a.business_name).length;
    list.innerHTML = fromAccounts
      ? '<div style="font-size:9px;color:#86868b">账户有 BM 字段，但暂无完整 BM 列表，点"刷新账户"重试。</div>'
      : '<div style="font-size:9px;color:#86868b">暂无 BM。可打开 business.facebook.com 后点"刷新账户"。</div>';
    return;
  }
  list.innerHTML = businesses.slice(0, 20).map(b => {
    const ids = (b.account_ids || []).slice(0, 3).join(' ');
    const more = (b.account_count || 0) > 3 ? ` 等 ${b.account_count} 个账户` : `${b.account_count || 0} 个账户`;
    return `<div class="biz-item"><div class="biz-title"><span>${businessDisplayName(b)}</span><span>${more}</span></div><div class="biz-sub">${b.id}${ids ? ' · ' + ids : ''}</div></div>`;
  }).join('');
}

function renderQueue(s) {
  const r = s?.runningTasks || 0;
  $('queueContent').textContent = `执行中: ${r}`;
}

function renderError() {
  const el = $('errorMsg');
  const ms = statusData?.miraStatus;
  if (!statusData) return;
  if (!statusData.nodeId) {
    el.style.display = 'block'; el.className = 'msg info';
    el.textContent = '💡 请先在 Mira 后台生成绑定码，点击"绑定"激活执行器';
    return;
  }
  if (!statusData.accessToken && !statusData.cookies?.c_user) {
    el.style.display = 'block'; el.className = 'msg info';
    el.textContent = '请先在 Chrome 中登录 facebook.com 或 business.facebook.com';
    return;
  }
  if (ms?.fbStatus === 'need_fb_tab') {
    el.style.display = 'block'; el.className = 'msg info';
    el.textContent = '请打开 Facebook / Business 页面，本地执行器会使用当前浏览器会话工作';
    return;
  }
  el.style.display = 'none';
}

async function loadAll() {
  try {
    statusData = await send({ type: 'getFullStatus' });
    renderBind(statusData);
    renderFb(statusData);
    renderAccounts(statusData.miraStatus || {});
    renderBusinesses(statusData.miraStatus || {});
    renderQueue(statusData);
    renderError();
    const bound = !!(statusData.nodeId);
    const online = bound && !!(statusData.accessToken);
    $('sysDot').className = 'dot ' + (online ? 'on' : bound ? 'warn' : 'off');
    $('verLabel').textContent = statusData.version || 'v2.0';
  } catch (e) {
    const msg = e.message || '';
    if (msg.includes('Receiving end') || msg.includes('Extension context')) {
      if ($('bindContent').textContent === '加载中...') {
        $('bindContent').innerHTML = '<span style="color:#86868b">⏳ 启动中，稍后自动刷新...</span>';
      }
    }
  }
}

// ========== 按钮：状态面板 ==========

$('btnBind').addEventListener('click', () => $('bindModal').classList.add('show'));
$('btnBindCancel').addEventListener('click', () => { $('bindModal').classList.remove('show'); $('bindErr').style.display = 'none'; });

$('btnBindConfirm').addEventListener('click', async () => {
  const code = $('bindCode').value.trim();
  const label = $('browserLabel').value.trim();
  const errEl = $('bindErr');
  if (!code) { errEl.textContent = '请输入绑定码'; errEl.style.display = 'block'; return; }
  const btn = $('btnBindConfirm');
  btn.disabled = true; btn.textContent = '绑定中...'; errEl.style.display = 'none';
  try {
    const data = await send({ type: 'bind', bindCode: code, browserLabel: label });
    $('bindModal').classList.remove('show');
    $('bindCode').value = '';
    // 立即刷新界面
    await loadAll();
    // 再确认一次
    if (!statusData?.nodeId) {
      // 可能 storage 还没写进去，延迟再刷新
      await new Promise(r => setTimeout(r, 500));
      await loadAll();
    }
  } catch (e) {
    errEl.textContent = '绑定失败: ' + (e.message || String(e));
    errEl.style.display = 'block';
  }
  btn.disabled = false; btn.textContent = '确认绑定';
});

$('btnUnbind').addEventListener('click', async () => {
  if (!confirm('解绑后需重新在 Mira 生成绑定码才能再激活。确认解绑？')) return;
  await send({ type: 'unbind' });
  statusData = null;
  await loadAll();
});

$('btnRefreshAcct').addEventListener('click', async () => {
  const btn = $('btnRefreshAcct');
  btn.disabled = true; btn.textContent = '⏳...';
  try {
    const data = await send({ type: 'refreshAccounts' });
    if (statusData) {
      if (!statusData.miraStatus) statusData.miraStatus = {};
      statusData.miraStatus.accounts = data.accounts || [];
      statusData.miraStatus.businesses = data.businesses || [];
      statusData.miraStatus.assets = data.assets || {};
      statusData.miraStatus.fbStatus = data.fb_status;
    }
    renderAccounts(data);
    renderBusinesses(data);
    renderError();
  } catch (e) {
    $('acctList').innerHTML = `<span style="color:#991b1b;font-size:9px">❌ ${e.message}</span>`;
  }
  btn.disabled = false; btn.textContent = '🔄 刷新账户';
});

$('btnDiscoverAssets').addEventListener('click', async () => {
  const btn = $('btnDiscoverAssets');
  btn.disabled = true; btn.textContent = '⏳ 发现中...';
  try {
    const data = await send({ type: 'discoverBusinessAssets' });
    if (statusData) {
      if (!statusData.miraStatus) statusData.miraStatus = {};
      statusData.miraStatus.accounts = data.accounts || [];
      statusData.miraStatus.businesses = data.businesses || [];
      statusData.miraStatus.assets = data.assets || {};
      statusData.miraStatus.fbStatus = data.fb_status;
      statusData.miraStatus.cached_assets_at = data.discovered_at || '';
    }
    renderAccounts(data);
    renderBusinesses(data);
    renderError();
    btn.textContent = `✅ BM ${data.businesses?.length || 0} / 账户 ${data.accounts?.length || 0}`;
  } catch (e) {
    btn.textContent = '❌ 失败';
    $('bizList').innerHTML = `<span style="color:#991b1b;font-size:9px">深度发现失败：${e.message}</span>`;
  }
  setTimeout(() => { btn.textContent = '🏢 深度发现资产'; btn.disabled = false; }, 2500);
});

$('btnHeartbeat').addEventListener('click', async () => {
  const btn = $('btnHeartbeat');
  btn.disabled = true; btn.textContent = '⏳...';
  try {
    await send({ type: 'heartbeat' });
    btn.textContent = '✅ OK';
    await loadAll();
  } catch (e) {
    btn.textContent = '❌ 失败';
  }
  setTimeout(() => { btn.textContent = '📡 测试连接'; btn.disabled = false; }, 1500);
});

$('btnDiag').addEventListener('click', async () => {
  try {
    const data = await send({ type: 'getDiagnostics' });
    await navigator.clipboard.writeText(JSON.stringify(data, null, 2));
    const btn = $('btnDiag');
    btn.textContent = '✅ 已复制';
    setTimeout(() => { btn.textContent = '📋 诊断'; }, 1500);
  } catch (e) { alert('失败: ' + (e.message || e)); }
});

$('btnExtractToken').addEventListener('click', async () => {
  const btn = $('btnExtractToken');
  btn.disabled = true; btn.textContent = '⏳ 提取中...';
  try {
    const data = await send({ type: 'silentRefresh' });
    btn.textContent = data.success ? '✅ 成功' : '❌ 未找到';
    await loadAll();
  } catch (e) {
    btn.textContent = '❌ ' + (e.message||'').slice(0,8);
  }
  setTimeout(() => { btn.textContent = '🔑 手动提取Token'; btn.disabled = false; }, 2000);
});

$('acctToggle').addEventListener('click', () => {
  acctsExpanded = !acctsExpanded;
  renderAccounts({ accounts: allAccounts });
});

// ========== 像素分享面板 ==========

const pxState = { bmList: [], selBmId: null, pixelList: [], selPixelId: null };

function readManualId(inputId, resultEl, label) {
  const id = ($(inputId).value || '').trim().replace(/\D/g, '');
  if (!id) {
    setMsg(resultEl, `请输入有效的 ${label}`, 'err');
    return null;
  }
  return id;
}

function selectPixelBm(id, name) {
  pxState.selBmId = id;
  pxState.selPixelId = null;
  $('bmList').innerHTML = `<div class="select-item sel" data-id="${id}">${name || '手动 BM'} <span class="id">${id}</span></div>`;
  $('pxStep2').style.display = 'block';
  $('pxStep3').style.display = 'none';
  $('pixelList').innerHTML = '<span style="color:#86868b;font-size:9px">可加载像素列表，也可以手动输入 Pixel ID。</span>';
  $('pixelResult').innerHTML = '';
}

function selectPixel(id, name) {
  pxState.selPixelId = id;
  $('pixelList').innerHTML = `<div class="select-item sel" data-id="${id}">${name || '手动 Pixel'} <span class="id">${id}</span></div>`;
  $('pxStep3').style.display = 'block';
  $('pixelResult').innerHTML = '';
}

function selectInviteBm(id, name) {
  invState.selBmId = id;
  $('inviteBmList').innerHTML = `<div class="select-item sel" data-id="${id}">${name || '手动 BM'} <span class="id">${id}</span></div>`;
  $('inviteResult').innerHTML = '';
}

async function loadBMs() {
  const el = $('bmList');
  el.innerHTML = '<span style="color:#86868b;font-size:9px">⏳ 加载中...</span>';
  try {
    const businesses = await loadBusinessList();
    pxState.bmList = businesses;
    if (!pxState.bmList.length) { el.innerHTML = '<span style="color:#86868b;font-size:9px">未找到 BM，可在下方手动输入 BM ID。</span>'; return; }
    el.innerHTML = pxState.bmList.map((bm, i) =>
      `<div class="select-item ${pxState.selBmId===bm.id?'sel':''}" data-id="${bm.id}">${i+1}. ${businessDisplayName(bm)} <span class="id">${bm.id}</span></div>`
    ).join('');
    el.querySelectorAll('.select-item').forEach(item => {
      item.addEventListener('click', () => {
        pxState.selBmId = item.dataset.id;
        pxState.selPixelId = null;
        el.querySelectorAll('.select-item').forEach(i => i.classList.remove('sel'));
        item.classList.add('sel');
        $('pxStep2').style.display = 'block';
        $('pxStep3').style.display = 'none';
        $('pixelList').innerHTML = '<span style="color:#86868b;font-size:9px">点击"加载像素列表"</span>';
        $('pixelResult').innerHTML = '';
      });
    });
  } catch (e) { el.innerHTML = `<span style="color:#991b1b;font-size:9px">自动读取 BM 失败：${e.message}<br>可在下方手动输入 BM ID 继续。</span>`; }
}

async function loadPixels() {
  if (!pxState.selBmId) return;
  const el = $('pixelList');
  el.innerHTML = '<span style="color:#86868b;font-size:9px">⏳ 加载中...</span>';
  try {
    const data = await fbBusinessOperation('list_pixels', { business_id: pxState.selBmId });
    pxState.pixelList = data.pixels || data.data || [];
    if (!pxState.pixelList.length) { el.innerHTML = '<span style="color:#86868b;font-size:9px">该 BM 下没有读取到像素，可在下方手动输入 Pixel ID。</span>'; return; }
    el.innerHTML = pxState.pixelList.map((px, i) =>
      `<div class="select-item ${pxState.selPixelId===px.id?'sel':''}" data-id="${px.id}">${i+1}. ${px.name} <span class="id">${px.id}</span></div>`
    ).join('');
    el.querySelectorAll('.select-item').forEach(item => {
      item.addEventListener('click', () => {
        pxState.selPixelId = item.dataset.id;
        el.querySelectorAll('.select-item').forEach(i => i.classList.remove('sel'));
        item.classList.add('sel');
        $('pxStep3').style.display = 'block';
        $('pixelResult').innerHTML = '';
      });
    });
  } catch (e) { el.innerHTML = `<span style="color:#991b1b;font-size:9px">自动读取像素失败：${e.message}<br>可在下方手动输入 Pixel ID 继续。</span>`; }
}

async function sharePixel() {
  const partnerBmId = $('partnerBmId').value.trim();
  if (!partnerBmId) { setMsg($('pixelResult'), '请输入合作伙伴 BM ID', 'err'); return; }
  if (!pxState.selPixelId) { setMsg($('pixelResult'), '请先选择像素', 'err'); return; }
  setMsg($('pixelResult'), '⏳ 分享中...', 'info');
  try {
    await fbBusinessOperation('share_pixel', {
      business_id: pxState.selBmId,
      pixel_id: pxState.selPixelId,
      partner_business_id: partnerBmId,
    });
    setMsg($('pixelResult'), '🎉 像素分享成功！', 'ok');
  } catch (e) {
    let hint = e.message || String(e);
    if (hint.includes('permission')) hint += ' — 请确认当前浏览器登录账号有该 BM / Pixel 管理权限';
    else if (hint.includes('already')) hint += ' — 该像素可能已分享给此 BM';
    setMsg($('pixelResult'), `❌ ${hint}`, 'err');
  }
}

$('btnLoadBMs').addEventListener('click', loadBMs);
$('btnLoadPixels').addEventListener('click', loadPixels);
$('btnSharePixel').addEventListener('click', sharePixel);
$('btnUseManualBm').addEventListener('click', () => {
  const id = readManualId('manualBmId', $('pixelResult'), 'BM ID');
  if (id) selectPixelBm(id, '手动 BM');
});
$('btnUseManualPixel').addEventListener('click', () => {
  const id = readManualId('manualPixelId', $('pixelResult'), 'Pixel ID');
  if (id) selectPixel(id, '手动 Pixel');
});

// ========== BM 邀请面板 ==========

const invState = { bmList: [], selBmId: null };

async function loadBMsForInvite() {
  const el = $('inviteBmList');
  el.innerHTML = '<span style="color:#86868b;font-size:9px">⏳ 加载中...</span>';
  try {
    const businesses = await loadBusinessList();
    invState.bmList = businesses;
    if (!invState.bmList.length) { el.innerHTML = '<span style="color:#86868b;font-size:9px">未找到 BM，可在下方手动输入 BM ID。</span>'; return; }
    el.innerHTML = invState.bmList.map((bm, i) =>
      `<div class="select-item ${invState.selBmId===bm.id?'sel':''}" data-id="${bm.id}">${i+1}. ${businessDisplayName(bm)} <span class="id">${bm.id}</span></div>`
    ).join('');
    el.querySelectorAll('.select-item').forEach(item => {
      item.addEventListener('click', () => {
        invState.selBmId = item.dataset.id;
        el.querySelectorAll('.select-item').forEach(i => i.classList.remove('sel'));
        item.classList.add('sel');
        $('inviteResult').innerHTML = '';
      });
    });
  } catch (e) { el.innerHTML = `<span style="color:#991b1b;font-size:9px">自动读取 BM 失败：${e.message}<br>可在下方手动输入 BM ID 继续。</span>`; }
}

async function inviteUser() {
  if (!invState.selBmId) { setMsg($('inviteResult'), '请先选择一个 BM', 'err'); return; }
  const email = $('inviteEmail').value.trim();
  if (!email || !email.includes('@')) { setMsg($('inviteResult'), '请输入有效邮箱', 'err'); return; }
  const role = $('inviteRole').value;
  setMsg($('inviteResult'), '⏳ 发送中...', 'info');
  try {
    await fbBusinessOperation('invite_user', { business_id: invState.selBmId, email, role });
    setMsg($('inviteResult'), `🎉 邀请已发送！${email} → ${role}`, 'ok');
    $('inviteEmail').value = '';
  } catch (e) {
    let hint = e.message || String(e);
    if (hint.includes('already') || hint.includes('invited')) hint += ' — 该用户可能已被邀请';
    else if (hint.includes('permission')) hint += ' — 请确认当前浏览器登录账号有该 BM 管理权限';
    setMsg($('inviteResult'), `❌ ${hint}`, 'err');
  }
}

$('btnLoadBMsForInvite').addEventListener('click', loadBMsForInvite);
$('btnInviteUser').addEventListener('click', inviteUser);
$('btnUseInviteManualBm').addEventListener('click', () => {
  const id = readManualId('inviteManualBmId', $('inviteResult'), 'BM ID');
  if (id) selectInviteBm(id, '手动 BM');
});

// ========== 启动 ==========

async function autoRefresh() {
  const activeTab = document.querySelector('.tab.active');
  if (activeTab && activeTab.dataset.tab === 'status') {
    await loadAll().catch(() => {});
  }
  setTimeout(autoRefresh, 5000);
}

loadAll().catch(() => {});
autoRefresh();
