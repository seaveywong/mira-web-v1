const DEFAULT_SERVER_URL = "https://shouhu.asia";
const HEARTBEAT_ALARM = "mira_local_api_executor_heartbeat";
const EXECUTOR_ALARM = "mira_local_api_executor_poll";
const GRAPH_BASE = "https://graph.facebook.com/v25.0";
const runningTasks = new Set();
let lastProbeAt = 0;
let lastProbeSummary = null;

function normalizeServerUrl(value) {
  const raw = String(value || DEFAULT_SERVER_URL).trim().replace(/\/+$/, "");
  return raw || DEFAULT_SERVER_URL;
}

function randomId(prefix) {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  return `${prefix}_${Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("")}`;
}

function maskToken(token) {
  const raw = String(token || "").trim();
  if (!raw) return "";
  if (raw.length <= 12) return "****";
  return `${raw.slice(0, 6)}****${raw.slice(-4)}`;
}

async function ensureInstallId() {
  const data = await chrome.storage.local.get(["installId"]);
  if (data.installId) return data.installId;
  const installId = randomId("chrome");
  await chrome.storage.local.set({ installId });
  return installId;
}

async function detectChromeName() {
  const installId = await ensureInstallId();
  return `Chrome-${installId.slice(-6).toUpperCase()}`;
}

async function getSettings() {
  const data = await chrome.storage.local.get([
    "serverUrl",
    "nodeName",
    "nodeId",
    "nodeSecret",
    "accessToken",
    "expiresInMinutes",
    "tokenExpiresAt",
    "lastStatus"
  ]);
  return {
    serverUrl: normalizeServerUrl(data.serverUrl),
    nodeName: data.nodeName || await detectChromeName(),
    nodeId: data.nodeId || "",
    nodeSecret: data.nodeSecret || "",
    accessToken: data.accessToken || "",
    expiresInMinutes: data.expiresInMinutes || "",
    tokenExpiresAt: data.tokenExpiresAt || "",
    lastStatus: data.lastStatus || null
  };
}

async function postJson(serverUrl, path, body) {
  const res = await fetch(normalizeServerUrl(serverUrl) + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {})
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.detail || data.message || `HTTP ${res.status}`);
  }
  return data;
}

async function graphRequest(method, path, params = {}) {
  const s = await getSettings();
  const token = String(s.accessToken || "").trim();
  if (!token) throw new Error("本地执行器未配置官方 Graph API Token");
  const cleanPath = String(path || "").replace(/^\/+/, "");
  const url = new URL(`${GRAPH_BASE}/${cleanPath}`);
  const body = new URLSearchParams();
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      if (method === "GET") url.searchParams.set(key, String(value));
      else body.set(key, String(value));
    }
  });
  if (method === "GET") url.searchParams.set("access_token", token);
  else body.set("access_token", token);
  const res = await fetch(url.toString(), {
    method,
    headers: method === "GET" ? undefined : { "Content-Type": "application/x-www-form-urlencoded" },
    body: method === "GET" ? undefined : body.toString()
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) {
    const err = data.error || {};
    const code = err.code ? `code=${err.code}` : "";
    const sub = err.error_subcode || err.subcode ? `subcode=${err.error_subcode || err.subcode}` : "";
    const detail = [err.message || `HTTP ${res.status}`, code, sub].filter(Boolean).join(" | ");
    throw new Error(detail);
  }
  return data;
}

async function graphGet(path, params = {}) {
  return graphRequest("GET", path, params);
}

async function graphPost(path, params = {}) {
  return graphRequest("POST", path, params);
}

function normalizeActId(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return raw.startsWith("act_") ? raw : `act_${raw}`;
}

async function fetchAllAdAccounts() {
  const out = [];
  let path = "me/adaccounts";
  let params = { fields: "id,name,account_status", limit: 200 };
  for (let i = 0; i < 20; i += 1) {
    const data = await graphGet(path, params);
    (data.data || []).forEach((item) => {
      const actId = normalizeActId(item.id);
      if (actId) out.push(actId);
    });
    const next = data.paging && data.paging.next;
    if (!next) break;
    const nextUrl = new URL(next);
    path = nextUrl.pathname.replace(/^\/v[0-9.]+\//, "");
    params = Object.fromEntries(nextUrl.searchParams.entries());
    delete params.access_token;
  }
  return Array.from(new Set(out));
}

async function buildTokenSummary(force = false) {
  const s = await getSettings();
  const token = String(s.accessToken || "").trim();
  if (!token) {
    lastProbeSummary = null;
    return { present: false };
  }
  if (!force && lastProbeSummary && Date.now() - lastProbeAt < 5 * 60 * 1000) {
    return lastProbeSummary;
  }
  const summary = {
    present: true,
    token_mask: maskToken(token),
    token_expires_at: s.tokenExpiresAt || "",
    fb_user_id: "",
    fb_user_name: "",
    permissions: { granted: [], declined: [] },
    account_ids: [],
    has_ads_management: false,
    has_ads_read: false,
    last_error: ""
  };
  try {
    const me = await graphGet("me", { fields: "id,name" });
    summary.fb_user_id = String(me.id || "");
    summary.fb_user_name = String(me.name || "");
    try {
      const perms = await graphGet("me/permissions", {});
      const granted = [];
      const declined = [];
      (perms.data || []).forEach((item) => {
        const name = String(item.permission || "").trim();
        if (!name) return;
        if (String(item.status || "").toLowerCase() === "granted") granted.push(name);
        else declined.push(name);
      });
      summary.permissions = {
        granted: Array.from(new Set(granted)).sort(),
        declined: Array.from(new Set(declined)).sort()
      };
      summary.has_ads_management = summary.permissions.granted.includes("ads_management");
      summary.has_ads_read = summary.permissions.granted.includes("ads_read") || summary.has_ads_management;
    } catch (err) {
      summary.last_error = `权限读取失败：${err.message || err}`;
    }
    try {
      summary.account_ids = await fetchAllAdAccounts();
    } catch (err) {
      summary.last_error = summary.last_error || `账户读取失败：${err.message || err}`;
    }
  } catch (err) {
    summary.last_error = err.message || String(err);
  }
  lastProbeAt = Date.now();
  lastProbeSummary = summary;
  return summary;
}

async function updateExecutorTask(task, status, progress, result = {}, error = "") {
  const s = await getSettings();
  if (!task || !task.id || !s.nodeId || !s.nodeSecret) return null;
  return postJson(s.serverUrl, `/api/local-executor/tasks/${encodeURIComponent(task.id)}/update`, {
    node_id: s.nodeId,
    node_secret: s.nodeSecret,
    status,
    progress,
    result,
    error,
    screenshot_data_url: ""
  });
}

async function executeAccountProbe(task) {
  const params = task.params || {};
  const actId = normalizeActId(params.act_id || task.account_id);
  if (!actId) throw new Error("任务缺少广告账户 ID");
  await updateExecutorTask(task, "running", "正在用本地 Graph API 读取账户");
  const account = await graphGet(actId, {
    fields: params.fields || "id,name,account_status,currency,timezone_name,amount_spent,spend_cap,balance"
  });
  const summary = await buildTokenSummary(true);
  await updateExecutorTask(task, "success", "账户 API 自检完成", {
    account,
    token_user: {
      id: summary.fb_user_id,
      name: summary.fb_user_name,
      has_ads_management: summary.has_ads_management,
      has_ads_read: summary.has_ads_read
    }
  });
}

async function executeUpdateStatus(task) {
  const params = task.params || {};
  const objectId = String(params.object_id || "").trim();
  const status = String(params.status || "PAUSED").trim().toUpperCase();
  const level = String(params.level || "").trim().toLowerCase();
  if (!objectId) throw new Error("任务缺少广告/广告组/系列 ID");
  if (!["ACTIVE", "PAUSED"].includes(status)) throw new Error("状态只支持 ACTIVE 或 PAUSED");
  await updateExecutorTask(task, "running", `正在将 ${level || "object"} ${objectId} 更新为 ${status}`);
  const result = await graphPost(objectId, { status });
  await updateExecutorTask(task, "success", `状态已更新为 ${status}`, {
    object_id: objectId,
    status,
    level,
    graph_result: result
  });
}

async function executeTask(task) {
  if (!task || !task.id || runningTasks.has(task.id)) return;
  runningTasks.add(task.id);
  try {
    const s = await getSettings();
    if (!String(s.accessToken || "").trim()) {
      await updateExecutorTask(task, "need_user", "请先在插件里配置官方 Graph API Token", {}, "本地执行器没有可用 API Token");
      return;
    }
    if (task.task_type === "graph_account_probe") {
      await executeAccountProbe(task);
      return;
    }
    if (task.task_type === "graph_update_status") {
      await executeUpdateStatus(task);
      return;
    }
    await updateExecutorTask(task, "failed", "插件不支持此任务类型", {}, `Unsupported task type: ${task.task_type}`);
  } catch (err) {
    await updateExecutorTask(task, "failed", "本地 API 执行失败", {}, err.message || String(err));
  } finally {
    runningTasks.delete(task.id);
  }
}

async function pollExecutorTask() {
  const s = await getSettings();
  if (!s.nodeId || !s.nodeSecret || runningTasks.size > 0) return null;
  const data = await postJson(s.serverUrl, "/api/local-executor/poll", {
    node_id: s.nodeId,
    node_secret: s.nodeSecret
  });
  if (data && data.task) {
    executeTask(data.task).catch(() => {});
  }
  return data;
}

async function registerNode(payload) {
  const serverUrl = normalizeServerUrl(payload.serverUrl);
  const nodeName = payload.nodeName || await detectChromeName();
  const data = await postJson(serverUrl, "/api/local-tokens/register", {
    code: payload.code,
    node_name: nodeName,
    browser: "Chrome",
    user_agent: navigator.userAgent
  });
  await chrome.storage.local.set({
    serverUrl,
    nodeName: payload.nodeName || data.node_name || nodeName,
    nodeId: data.node_id,
    nodeSecret: data.node_secret,
    lastStatus: {
      ok: true,
      message: "执行器绑定成功",
      updatedAt: new Date().toLocaleString()
    }
  });
  await ensureAlarm();
  return data;
}

async function heartbeat(forceProbe = false) {
  const s = await getSettings();
  if (!s.nodeId || !s.nodeSecret) {
    return { ok: false, message: "插件尚未绑定 Mira" };
  }
  const tokenSummary = await buildTokenSummary(forceProbe);
  const data = await postJson(s.serverUrl, "/api/local-tokens/heartbeat", {
    node_id: s.nodeId,
    node_secret: s.nodeSecret,
    access_token: "",
    expires_at: s.tokenExpiresAt || "",
    expires_in_minutes: null,
    token_summary: tokenSummary,
    node_name: s.nodeName || await detectChromeName(),
    browser: "Chrome",
    user_agent: navigator.userAgent
  });
  const status = {
    ok: true,
    message: data.node && data.node.status ? `心跳成功：${data.node.status}` : "心跳成功",
    updatedAt: new Date().toLocaleString(),
    node: data.node || null
  };
  await chrome.storage.local.set({ lastStatus: status });
  return status;
}

async function ensureAlarm() {
  await chrome.alarms.clear(HEARTBEAT_ALARM);
  await chrome.alarms.create(HEARTBEAT_ALARM, { periodInMinutes: 0.5 });
  await chrome.alarms.clear(EXECUTOR_ALARM);
  await chrome.alarms.create(EXECUTOR_ALARM, { periodInMinutes: 0.5 });
}

chrome.runtime.onInstalled.addListener(() => {
  ensureAlarm().catch(() => {});
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === EXECUTOR_ALARM) {
    pollExecutorTask().catch(async (err) => {
      await chrome.storage.local.set({
        lastStatus: {
          ok: false,
          message: err.message || String(err),
          updatedAt: new Date().toLocaleString()
        }
      });
    });
    return;
  }
  if (alarm.name !== HEARTBEAT_ALARM) return;
  heartbeat(false).catch(async (err) => {
    await chrome.storage.local.set({
      lastStatus: {
        ok: false,
        message: err.message || String(err),
        updatedAt: new Date().toLocaleString()
      }
    });
  });
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    if (message.type === "getSettings") {
      sendResponse({ ok: true, data: await getSettings() });
      return;
    }
    if (message.type === "saveSettings") {
      const old = await getSettings();
      const nextToken = message.accessToken || "";
      const nextMinutes = message.expiresInMinutes || "";
      let tokenExpiresAt = old.tokenExpiresAt || "";
      if (nextToken !== old.accessToken || nextMinutes !== old.expiresInMinutes) {
        const parsedMinutes = parseInt(nextMinutes || "0", 10);
        tokenExpiresAt = nextToken && Number.isFinite(parsedMinutes) && parsedMinutes > 0
          ? new Date(Date.now() + parsedMinutes * 60000).toISOString()
          : "";
        lastProbeAt = 0;
        lastProbeSummary = null;
      }
      await chrome.storage.local.set({
        serverUrl: normalizeServerUrl(message.serverUrl),
        nodeName: message.nodeName || await detectChromeName(),
        accessToken: nextToken,
        expiresInMinutes: nextMinutes,
        tokenExpiresAt
      });
      await ensureAlarm();
      sendResponse({ ok: true, data: await getSettings() });
      return;
    }
    if (message.type === "register") {
      sendResponse({ ok: true, data: await registerNode(message) });
      return;
    }
    if (message.type === "heartbeat") {
      sendResponse({ ok: true, data: await heartbeat(true) });
      return;
    }
    if (message.type === "pollTask") {
      sendResponse({ ok: true, data: await pollExecutorTask() });
      return;
    }
    if (message.type === "clearToken") {
      lastProbeAt = 0;
      lastProbeSummary = null;
      await chrome.storage.local.set({ accessToken: "", expiresInMinutes: "", tokenExpiresAt: "" });
      sendResponse({ ok: true, data: await heartbeat(true).catch((err) => ({ ok: false, message: err.message })) });
      return;
    }
    sendResponse({ ok: false, error: "unknown message" });
  })().catch((err) => {
    sendResponse({ ok: false, error: err.message || String(err) });
  });
  return true;
});
