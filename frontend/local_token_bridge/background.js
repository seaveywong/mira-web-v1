const DEFAULT_SERVER_URL = "http://43.129.230.237:8000";
const HEARTBEAT_ALARM = "mira_local_token_heartbeat";

function normalizeServerUrl(value) {
  const raw = String(value || DEFAULT_SERVER_URL).trim().replace(/\/+$/, "");
  return raw || DEFAULT_SERVER_URL;
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
    nodeName: data.nodeName || "Chrome 本地 Token",
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

async function registerNode(payload) {
  const serverUrl = normalizeServerUrl(payload.serverUrl);
  const data = await postJson(serverUrl, "/api/local-tokens/register", {
    code: payload.code,
    node_name: payload.nodeName || "Chrome 本地 Token",
    browser: "Chrome",
    user_agent: navigator.userAgent
  });
  await chrome.storage.local.set({
    serverUrl,
    nodeName: payload.nodeName || data.node_name || "Chrome 本地 Token",
    nodeId: data.node_id,
    nodeSecret: data.node_secret,
    lastStatus: {
      ok: true,
      message: "插件绑定成功",
      updatedAt: new Date().toLocaleString()
    }
  });
  await ensureAlarm();
  return data;
}

async function heartbeat() {
  const s = await getSettings();
  if (!s.nodeId || !s.nodeSecret) {
    return { ok: false, message: "插件尚未绑定 Mira" };
  }
  const data = await postJson(s.serverUrl, "/api/local-tokens/heartbeat", {
    node_id: s.nodeId,
    node_secret: s.nodeSecret,
    access_token: s.accessToken || "",
    expires_at: s.tokenExpiresAt || "",
    expires_in_minutes: null,
    node_name: s.nodeName || "Chrome 本地 Token",
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
}

chrome.runtime.onInstalled.addListener(() => {
  ensureAlarm().catch(() => {});
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name !== HEARTBEAT_ALARM) return;
  heartbeat().catch(async (err) => {
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
      }
      await chrome.storage.local.set({
        serverUrl: normalizeServerUrl(message.serverUrl),
        nodeName: message.nodeName || "Chrome 本地 Token",
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
      sendResponse({ ok: true, data: await heartbeat() });
      return;
    }
    if (message.type === "clearToken") {
      await chrome.storage.local.set({ accessToken: "", expiresInMinutes: "", tokenExpiresAt: "" });
      sendResponse({ ok: true, data: await heartbeat().catch((err) => ({ ok: false, message: err.message })) });
      return;
    }
    sendResponse({ ok: false, error: "unknown message" });
  })().catch((err) => {
    sendResponse({ ok: false, error: err.message || String(err) });
  });
  return true;
});
