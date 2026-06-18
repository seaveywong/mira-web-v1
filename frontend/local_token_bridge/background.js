const DEFAULT_SERVER_URL = "https://shouhu.asia";
const DEFAULT_META_SCOPES = "ads_management,ads_read,pages_show_list,pages_manage_ads,business_management";
const HEARTBEAT_ALARM = "mira_local_token_heartbeat";
const EXECUTOR_ALARM = "mira_local_executor_poll";
const REFRESH_SKEW_MS = 5 * 60 * 1000;
const runningTasks = new Set();

function normalizeServerUrl(value) {
  const raw = String(value || DEFAULT_SERVER_URL).trim().replace(/\/+$/, "");
  return raw || DEFAULT_SERVER_URL;
}

function randomId(prefix) {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  return `${prefix}_${Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("")}`;
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
  try {
    const info = await chrome.identity.getProfileUserInfo({ accountStatus: "ANY" });
    if (info && info.email) return `Chrome - ${info.email}`;
  } catch (err) {
    // Chrome may return empty profile info when the user is not signed in.
  }
  return `Chrome - ${installId.slice(-6).toUpperCase()}`;
}

function getRedirectUri() {
  return chrome.identity.getRedirectURL("meta");
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
    "metaAppId",
    "metaScopes",
    "autoRefreshEnabled",
    "lastStatus"
  ]);
  const detectedName = data.nodeName || await detectChromeName();
  return {
    serverUrl: normalizeServerUrl(data.serverUrl),
    nodeName: detectedName,
    nodeId: data.nodeId || "",
    nodeSecret: data.nodeSecret || "",
    accessToken: data.accessToken || "",
    expiresInMinutes: data.expiresInMinutes || "",
    tokenExpiresAt: data.tokenExpiresAt || "",
    metaAppId: data.metaAppId || "",
    metaScopes: data.metaScopes || DEFAULT_META_SCOPES,
    autoRefreshEnabled: data.autoRefreshEnabled !== false,
    redirectUri: getRedirectUri(),
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

async function updateExecutorTask(task, status, progress, result = {}, error = "", screenshotDataUrl = "") {
  const s = await getSettings();
  if (!task || !task.id || !s.nodeId || !s.nodeSecret) return null;
  return postJson(s.serverUrl, `/api/local-executor/tasks/${encodeURIComponent(task.id)}/update`, {
    node_id: s.nodeId,
    node_secret: s.nodeSecret,
    status,
    progress,
    result,
    error,
    screenshot_data_url: screenshotDataUrl || ""
  });
}

function waitForTabLoad(tabId, timeoutMs = 25000) {
  return new Promise((resolve) => {
    let done = false;
    const timer = setTimeout(() => {
      if (done) return;
      done = true;
      chrome.tabs.onUpdated.removeListener(listener);
      resolve(false);
    }, timeoutMs);
    function listener(updatedTabId, changeInfo) {
      if (updatedTabId !== tabId) return;
      if (changeInfo.status === "complete") {
        if (done) return;
        done = true;
        clearTimeout(timer);
        chrome.tabs.onUpdated.removeListener(listener);
        resolve(true);
      }
    }
    chrome.tabs.onUpdated.addListener(listener);
  });
}

async function captureTabScreenshot(tab) {
  try {
    return await chrome.tabs.captureVisibleTab(tab.windowId, { format: "jpeg", quality: 55 });
  } catch (err) {
    return "";
  }
}

async function analyzeAdsManagerTab(tabId) {
  try {
    const injected = await chrome.scripting.executeScript({
      target: { tabId },
      func: () => {
        const text = (document.body && document.body.innerText || "").slice(0, 5000);
        const low = text.toLowerCase();
        const href = location.href;
        let state = "opened";
        let reason = "Ads Manager 页面已打开";
        if (/login|登录|log in|忘记密码|password/.test(low) || /\/login/.test(href)) {
          state = "need_login";
          reason = "当前浏览器需要登录 Facebook";
        } else if (/checkpoint|安全验证|验证身份|confirm your identity/.test(low) || /checkpoint/.test(href)) {
          state = "need_user";
          reason = "Facebook 要求人工验证";
        } else if (/permission|权限不足|没有权限|not have permission|access denied/.test(low)) {
          state = "no_permission";
          reason = "当前登录账号可能没有此广告账户权限";
        } else if (/ads manager|广告管理工具|广告系列|campaigns/i.test(text)) {
          state = "ready";
          reason = "Ads Manager 页面可访问";
        }
        return {
          href,
          title: document.title || "",
          state,
          reason,
          sample: text.slice(0, 800)
        };
      }
    });
    return injected && injected[0] && injected[0].result ? injected[0].result : { state: "opened", reason: "页面已打开" };
  } catch (err) {
    return { state: "opened", reason: "页面已打开，但内容检测失败", error: err.message || String(err) };
  }
}

async function executeOpenAdsManager(task) {
  const params = task.params || {};
  const targetUrl = params.target_url;
  if (!targetUrl) throw new Error("任务缺少 target_url");
  await updateExecutorTask(task, "running", "正在打开 Ads Manager");
  const tab = await chrome.tabs.create({ url: targetUrl, active: true });
  await waitForTabLoad(tab.id);
  const analysis = await analyzeAdsManagerTab(tab.id);
  const screenshot = await captureTabScreenshot(tab);
  const finalStatus = ["ready", "opened"].includes(analysis.state) ? "success" : "need_user";
  await updateExecutorTask(
    task,
    finalStatus,
    analysis.reason || "页面状态已回传",
    { ...analysis, tab_id: tab.id, task_type: task.task_type },
    finalStatus === "success" ? "" : (analysis.reason || "需要人工处理"),
    screenshot
  );
}

async function executeTask(task) {
  if (!task || !task.id || runningTasks.has(task.id)) return;
  runningTasks.add(task.id);
  try {
    if (task.task_type === "open_ads_manager") {
      await executeOpenAdsManager(task);
      return;
    }
    await updateExecutorTask(task, "failed", "插件暂不支持此任务类型", {}, `Unsupported task type: ${task.task_type}`);
  } catch (err) {
    await updateExecutorTask(task, "failed", "本地执行失败", {}, err.message || String(err));
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
      message: "插件绑定成功",
      updatedAt: new Date().toLocaleString()
    }
  });
  await ensureAlarm();
  return data;
}

function parseAuthResult(redirectedUrl) {
  const url = new URL(redirectedUrl);
  const hash = new URLSearchParams((url.hash || "").replace(/^#/, ""));
  const query = url.searchParams;
  const params = hash.has("access_token") || hash.has("error")
    ? hash
    : query;
  const error = params.get("error") || params.get("error_message") || params.get("error_description");
  if (error) throw new Error(error);
  const accessToken = params.get("access_token");
  if (!accessToken) throw new Error("授权完成但未返回 access_token，请检查 App 回调地址和权限配置");
  const expiresIn = parseInt(params.get("expires_in") || "0", 10);
  const tokenExpiresAt = Number.isFinite(expiresIn) && expiresIn > 0
    ? new Date(Date.now() + expiresIn * 1000).toISOString()
    : "";
  return {
    accessToken,
    expiresInMinutes: expiresIn > 0 ? String(Math.max(1, Math.floor(expiresIn / 60))) : "",
    tokenExpiresAt
  };
}

async function authorizeMeta(interactive = true) {
  const s = await getSettings();
  if (!s.nodeId || !s.nodeSecret) {
    throw new Error("请先用 Mira 绑定码绑定插件，再执行 Meta 授权");
  }
  if (!s.metaAppId) {
    throw new Error("请先填写 Meta App ID");
  }
  const state = randomId("mira_state");
  const redirectUri = getRedirectUri();
  const authUrl = new URL("https://www.facebook.com/v22.0/dialog/oauth");
  authUrl.searchParams.set("client_id", s.metaAppId);
  authUrl.searchParams.set("redirect_uri", redirectUri);
  authUrl.searchParams.set("response_type", "token");
  authUrl.searchParams.set("scope", s.metaScopes || DEFAULT_META_SCOPES);
  authUrl.searchParams.set("state", state);
  authUrl.searchParams.set("auth_type", "rerequest");

  const redirectedUrl = await chrome.identity.launchWebAuthFlow({
    url: authUrl.toString(),
    interactive
  });
  if (!redirectedUrl) throw new Error("授权窗口未返回结果");
  const resultUrl = new URL(redirectedUrl);
  const resultHash = new URLSearchParams((resultUrl.hash || "").replace(/^#/, ""));
  const resultState = resultHash.get("state") || resultUrl.searchParams.get("state");
  if (resultState && resultState !== state) {
    throw new Error("授权 state 校验失败，请重新授权");
  }
  const token = parseAuthResult(redirectedUrl);
  await chrome.storage.local.set({
    accessToken: token.accessToken,
    expiresInMinutes: token.expiresInMinutes,
    tokenExpiresAt: token.tokenExpiresAt,
    autoRefreshEnabled: s.autoRefreshEnabled !== false,
    lastStatus: {
      ok: true,
      message: "Meta 授权成功，正在上报 Mira",
      updatedAt: new Date().toLocaleString()
    }
  });
  return heartbeat();
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

async function maybeAutoRefresh() {
  const s = await getSettings();
  if (!s.autoRefreshEnabled || !s.metaAppId || !s.accessToken || !s.tokenExpiresAt) return null;
  const expiresAt = Date.parse(s.tokenExpiresAt);
  if (!Number.isFinite(expiresAt) || expiresAt - Date.now() > REFRESH_SKEW_MS) return null;
  try {
    return await authorizeMeta(false);
  } catch (err) {
    await chrome.storage.local.set({
      lastStatus: {
        ok: false,
        message: `Token 即将过期，自动续取失败：${err.message || err}`,
        updatedAt: new Date().toLocaleString()
      }
    });
    return null;
  }
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
  (async () => {
    await maybeAutoRefresh();
    await heartbeat();
  })().catch(async (err) => {
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
        nodeName: message.nodeName || await detectChromeName(),
        accessToken: nextToken,
        expiresInMinutes: nextMinutes,
        tokenExpiresAt,
        metaAppId: message.metaAppId || "",
        metaScopes: message.metaScopes || DEFAULT_META_SCOPES,
        autoRefreshEnabled: message.autoRefreshEnabled !== false
      });
      await ensureAlarm();
      sendResponse({ ok: true, data: await getSettings() });
      return;
    }
    if (message.type === "register") {
      sendResponse({ ok: true, data: await registerNode(message) });
      return;
    }
    if (message.type === "authorizeMeta") {
      const settings = await getSettings();
      await chrome.storage.local.set({
        serverUrl: normalizeServerUrl(message.serverUrl || settings.serverUrl),
        nodeName: message.nodeName || settings.nodeName,
        metaAppId: message.metaAppId || settings.metaAppId,
        metaScopes: message.metaScopes || settings.metaScopes || DEFAULT_META_SCOPES,
        autoRefreshEnabled: message.autoRefreshEnabled !== false
      });
      sendResponse({ ok: true, data: await authorizeMeta(true) });
      return;
    }
    if (message.type === "heartbeat") {
      sendResponse({ ok: true, data: await heartbeat() });
      return;
    }
    if (message.type === "pollTask") {
      sendResponse({ ok: true, data: await pollExecutorTask() });
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
