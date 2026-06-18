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
    metaAppId: "",
    metaScopes: "",
    autoRefreshEnabled: false
  };
}

async function saveSettings() {
  const form = readForm();
  await send({ type: "saveSettings", ...form });
}

function renderNodeStatus(s) {
  if (s.nodeId) {
    const last = s.lastStatus || {};
    let html = `已激活浏览器：<b>${s.nodeName || s.nodeId}</b><br>`;
    html += `最近状态：${last.message || "等待心跳 / 等待任务"}<br>`;
    if (last.updatedAt) html += `更新时间：${last.updatedAt}<br>`;
    if (s.tokenExpiresAt) html += `备用 Token 到期：${new Date(s.tokenExpiresAt).toLocaleString()}<br>`;
    if (last.node) {
      html += `<span class="pill">${last.node.account_count || 0} 个账户可用</span>`;
    }
    setStatus(html, last.ok !== false);
    return;
  }
  setStatus("尚未激活 Mira。本浏览器必须先使用一次性绑定码激活，才会接收任务。", null);
}

async function load() {
  try {
    const s = await send({ type: "getSettings" });
    $("serverUrl").value = s.serverUrl || "";
    $("nodeName").value = s.nodeName || "";
    $("accessToken").value = s.accessToken || "";
    $("expiresInMinutes").value = s.expiresInMinutes || "";
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

$("registerBtn").addEventListener("click", () => withButton($("registerBtn"), "激活中...", async () => {
  const form = readForm();
  if (!form.serverUrl || !form.code) throw new Error("请填写 Mira 地址和绑定码");
  const data = await send({ type: "register", ...form });
  setStatus(`激活成功：${data.node_name || data.node_id}`, true);
  await saveSettings();
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

$("pollTaskBtn").addEventListener("click", () => withButton($("pollTaskBtn"), "拉取中...", async () => {
  await saveSettings();
  const data = await send({ type: "pollTask" });
  if (data && data.task) {
    setStatus(`已领取任务：${data.task.task_type}<br>任务 ID：${data.task.id}`, true);
  } else {
    setStatus("暂无待执行任务。", true);
  }
}));

$("clearTokenBtn").addEventListener("click", () => withButton($("clearTokenBtn"), "清除中...", async () => {
  $("accessToken").value = "";
  $("expiresInMinutes").value = "";
  await send({ type: "clearToken" });
  setStatus("备用 Token 已清除。", true);
  await load();
}));

load();
