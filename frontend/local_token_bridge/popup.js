function $(id) {
  return document.getElementById(id);
}

function send(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage(message, (res) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      if (!res || !res.ok) {
        reject(new Error((res && res.error) || "操作失败"));
        return;
      }
      resolve(res.data);
    });
  });
}

function setStatus(message, ok) {
  const el = $("status");
  el.className = "status " + (ok === true ? "ok" : ok === false ? "bad" : "");
  el.innerHTML = message;
}

function readForm() {
  return {
    serverUrl: $("serverUrl").value.trim(),
    nodeName: $("nodeName").value.trim(),
    code: $("bindCode").value.trim(),
    accessToken: $("accessToken").value.trim(),
    expiresInMinutes: $("expiresInMinutes").value.trim(),
    metaAppId: $("metaAppId").value.trim(),
    metaScopes: $("metaScopes").value.trim(),
    autoRefreshEnabled: $("autoRefreshEnabled").checked
  };
}

async function saveSettings() {
  const form = readForm();
  await send({ type: "saveSettings", ...form });
}

function renderNodeStatus(s) {
  if (s.nodeId) {
    const last = s.lastStatus || {};
    let html = `已绑定节点：<b>${s.nodeName || s.nodeId}</b><br>`;
    html += `最近状态：${last.message || "等待心跳"}<br>`;
    if (last.updatedAt) html += `更新时间：${last.updatedAt}<br>`;
    if (s.tokenExpiresAt) html += `Token 到期：${new Date(s.tokenExpiresAt).toLocaleString()}<br>`;
    if (last.node) {
      html += `<span class="pill">${last.node.account_count || 0} 个账户可用</span>`;
    }
    setStatus(html, last.ok !== false);
    return;
  }
  setStatus("尚未绑定 Mira。请先在 Mira 生成绑定码，然后点击「绑定插件」。", null);
}

async function load() {
  try {
    const s = await send({ type: "getSettings" });
    $("serverUrl").value = s.serverUrl || "";
    $("nodeName").value = s.nodeName || "";
    $("accessToken").value = s.accessToken || "";
    $("expiresInMinutes").value = s.expiresInMinutes || "";
    $("metaAppId").value = s.metaAppId || "";
    $("metaScopes").value = s.metaScopes || "";
    $("autoRefreshEnabled").checked = s.autoRefreshEnabled !== false;
    $("redirectUri").value = s.redirectUri || "";
    renderNodeStatus(s);
  } catch (err) {
    setStatus(err.message || String(err), false);
  }
}

async function withButton(btn, text, fn) {
  const old = btn.textContent;
  btn.disabled = true;
  btn.textContent = text;
  try {
    await fn();
  } catch (err) {
    setStatus(err.message || String(err), false);
  } finally {
    btn.disabled = false;
    btn.textContent = old;
  }
}

$("saveBtn").addEventListener("click", () => withButton($("saveBtn"), "保存中...", async () => {
  await saveSettings();
  setStatus("配置已保存。", true);
  await load();
}));

$("registerBtn").addEventListener("click", () => withButton($("registerBtn"), "绑定中...", async () => {
  const form = readForm();
  if (!form.serverUrl || !form.code) throw new Error("请填写 Mira 地址和绑定码");
  const data = await send({ type: "register", ...form });
  setStatus(`绑定成功：${data.node_name || data.node_id}`, true);
  await saveSettings();
  await load();
}));

$("authorizeBtn").addEventListener("click", () => withButton($("authorizeBtn"), "授权中...", async () => {
  const form = readForm();
  if (!form.metaAppId) throw new Error("请先填写 Meta App ID");
  await saveSettings();
  const data = await send({ type: "authorizeMeta", ...form });
  const node = data.node || data;
  let html = `授权并上报成功：${node.status || "online"}<br>`;
  html += `Meta 用户：${node.fb_user_name || "--"}<br>`;
  html += `可操作账户：${node.account_count || 0} 个<br>`;
  if (node.last_error) html += `说明：${node.last_error}<br>`;
  setStatus(html, node.status === "online");
  await load();
}));

$("heartbeatBtn").addEventListener("click", () => withButton($("heartbeatBtn"), "上报中...", async () => {
  await saveSettings();
  const data = await send({ type: "heartbeat" });
  const node = data.node || data;
  let html = `上报成功：${node.status || "online"}<br>`;
  html += `Meta 用户：${node.fb_user_name || "--"}<br>`;
  html += `可操作账户：${node.account_count || 0} 个<br>`;
  if (node.last_error) html += `说明：${node.last_error}<br>`;
  setStatus(html, node.status === "online");
}));

$("clearTokenBtn").addEventListener("click", () => withButton($("clearTokenBtn"), "清除中...", async () => {
  $("accessToken").value = "";
  $("expiresInMinutes").value = "";
  await send({ type: "clearToken" });
  setStatus("本地 Token 已清除。", true);
  await load();
}));

$("copyRedirectBtn").addEventListener("click", async () => {
  const text = $("redirectUri").value;
  if (!text) return;
  await navigator.clipboard.writeText(text);
  setStatus("插件回调地址已复制。", true);
});

load();
