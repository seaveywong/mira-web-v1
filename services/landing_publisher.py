import base64
import hashlib
import json
import mimetypes
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import requests


CF_API_BASE = "https://api.cloudflare.com/client/v4"
BASE_DIR = Path(os.environ.get("MIRA_BASE_DIR", "/opt/mira"))
DEFAULT_TEMPLATE_DIR = BASE_DIR / "landing_templates" / "rh_fp_advanced"
IGNORE_STATIC_NAMES = {"_headers", "_redirects", "_routes.json"}


class CloudflareError(RuntimeError):
    pass


def _headers(api_token: str) -> dict:
    return {"Authorization": f"Bearer {api_token}"}


def cf_request(api_token: str, method: str, path: str, **kwargs) -> dict:
    url = f"{CF_API_BASE}{path}"
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_headers(api_token))
    try:
        resp = requests.request(method, url, headers=headers, timeout=45, **kwargs)
        data = resp.json()
    except requests.exceptions.RequestException as exc:
        raise CloudflareError(f"Cloudflare network error: {exc}") from exc
    except ValueError as exc:
        raise CloudflareError(f"Cloudflare returned non-json response: HTTP {resp.status_code}") from exc
    if resp.status_code >= 400 or data.get("success") is False:
        errors = data.get("errors") if isinstance(data, dict) else None
        msg = "; ".join([str(e.get("message") or e) for e in errors or [] if e]) or f"HTTP {resp.status_code}"
        lower = msg.lower()
        if "invalid api token" in lower:
            msg = "Invalid API token. Pages publishing requires a Cloudflare Account API Token; R2/S3 Access Key, Secret Key, or S3 endpoint cannot be used."
        elif "permission" in lower or "not authorized" in lower:
            msg = msg + ". Check that the token includes Account Settings Read and Cloudflare Pages Edit permissions."
        raise CloudflareError(msg)
    return data.get("result", data)


def verify_token_and_accounts(api_token: str, account_id: str | None = None) -> dict:
    account_id = (account_id or "").strip()
    if account_id:
        account: dict[str, Any] = {"id": account_id, "name": account_id}
        try:
            info = cf_request(api_token, "GET", f"/accounts/{account_id}")
            if isinstance(info, dict):
                account["name"] = info.get("name") or account_id
        except CloudflareError:
            pass
        # Account API Tokens may not be allowed to call /user/tokens/verify or /accounts.
        # Pages access is the real requirement for this module, so verify it directly.
        list_pages_projects(api_token, account_id)
        return {"token": {"status": "active", "account_scoped": True}, "accounts": [account]}

    token_info = cf_request(api_token, "GET", "/user/tokens/verify")
    accounts = cf_request(api_token, "GET", "/accounts")
    if isinstance(accounts, dict) and "result" in accounts:
        accounts = accounts["result"]
    return {"token": token_info, "accounts": accounts if isinstance(accounts, list) else []}


def list_pages_projects(api_token: str, account_id: str) -> list:
    result = cf_request(api_token, "GET", f"/accounts/{account_id}/pages/projects")
    return result if isinstance(result, list) else result.get("result", [])


def add_pages_custom_domain(api_token: str, account_id: str, project_name: str, domain: str) -> dict:
    domain = normalize_custom_domain(domain)
    if not domain:
        return {}
    project_name = sanitize_project_name(project_name)
    payload = {"name": domain}
    try:
        return cf_request(
            api_token,
            "POST",
            f"/accounts/{account_id}/pages/projects/{project_name}/domains",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
    except CloudflareError as exc:
        msg = str(exc).lower()
        if "already" in msg or "exists" in msg or "duplicate" in msg:
            return {"name": domain, "status": "already_exists"}
        raise


def list_pages_custom_domains(api_token: str, account_id: str, project_name: str) -> list[dict[str, Any]]:
    project_name = sanitize_project_name(project_name)
    result = cf_request(api_token, "GET", f"/accounts/{account_id}/pages/projects/{project_name}/domains")
    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]
    if isinstance(result, dict):
        items = result.get("result") or result.get("domains") or []
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
    return []


def get_pages_custom_domain_status(api_token: str, account_id: str, project_name: str, domain: str) -> dict:
    domain = normalize_custom_domain(domain)
    if not domain:
        return {}
    domains = list_pages_custom_domains(api_token, account_id, project_name)
    for item in domains:
        name = str(item.get("name") or item.get("domain") or "").strip().lower()
        if name == domain:
            return item
    return {"name": domain, "status": "not_found"}


def ensure_project(api_token: str, account_id: str, project_name: str) -> dict:
    project_name = sanitize_project_name(project_name)
    try:
        return cf_request(api_token, "GET", f"/accounts/{account_id}/pages/projects/{project_name}")
    except CloudflareError:
        pass
    payload = {"name": project_name, "production_branch": "main"}
    try:
        return cf_request(
            api_token,
            "POST",
            f"/accounts/{account_id}/pages/projects",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
    except CloudflareError:
        return cf_request(api_token, "GET", f"/accounts/{account_id}/pages/projects/{project_name}")


def sanitize_project_name(value: str) -> str:
    cleaned = (value or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9-]+", "-", cleaned)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    if not cleaned:
        cleaned = f"mira-landing-{int(time.time())}"
    if len(cleaned) > 58:
        cleaned = cleaned[:58].strip("-")
    return cleaned


def normalize_custom_domain(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    raw = re.sub(r"^https?://", "", raw, flags=re.I)
    raw = raw.split("/")[0].split("?")[0].split("#")[0].strip().strip(".").lower()
    if not raw:
        return ""
    if len(raw) > 253 or "." not in raw:
        raise ValueError("Custom domain must be a full host name, for example go.example.com")
    label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if not re.fullmatch(rf"{label}(?:\.{label})+", raw):
        raise ValueError("Custom domain contains unsupported characters")
    return raw


def prepare_template(
    template_dir: str | os.PathLike,
    pixel_id: str,
    target_urls: list[str],
    rotation_mode: str = "sequential",
    link_kind: str = "landing",
    worker_enabled: bool = False,
    tracking_enabled: bool = True,
    protection_enabled: bool = False,
    protection_rules: dict[str, Any] | None = None,
    page_id: int | None = None,
    ingest_secret: str = "",
    ingest_url: str = "",
) -> str:
    src = Path(template_dir)
    if not src.exists() or not src.is_dir():
        raise ValueError("Template directory does not exist")
    urls = [u.strip() for u in target_urls or [] if u and u.strip()]
    if not urls:
        raise ValueError("At least one target URL is required")
    work = Path(tempfile.mkdtemp(prefix="mira_landing_"))
    shutil.copytree(src, work, dirs_exist_ok=True)
    landing = work / "landing.html"
    redirect_only = (link_kind or "landing").strip().lower() == "form"
    if not redirect_only and not landing.exists():
        raise ValueError("Template missing landing.html")

    if worker_enabled:
        primary = "/__mira/redirect"
    else:
        primary = urls[0]
    if redirect_only:
        html = _form_redirect_html(primary)
    else:
        html = landing.read_text(encoding="utf-8", errors="ignore")
        html = re.sub(
            r'var\s+RH_PIXEL_ID\s*=\s*"[^"]*"\s*;',
            f'var RH_PIXEL_ID = {json.dumps(pixel_id or "")};',
            html,
            count=1,
        )
        html = re.sub(
            r'var\s+RH_TARGET_URL\s*=\s*"[^"]*"\s*;',
            f'var RH_TARGET_URL = {json.dumps(primary)};',
            html,
            count=1,
        )
    if worker_enabled:
        html = _inject_client_tracker(html, page_id or 0)
        _write_worker(
            work / "_worker.js",
            {
                "page_id": page_id,
                "link_kind": (link_kind or "landing").strip().lower(),
                "secret": ingest_secret,
                "ingest_url": ingest_url,
                "target_urls": urls,
                "rotation_mode": rotation_mode,
                "tracking_enabled": bool(tracking_enabled),
                "protection_enabled": bool(protection_enabled),
                "protection_rules": protection_rules or {},
            },
        )
    else:
        worker_file = work / "_worker.js"
        if worker_file.exists():
            worker_file.unlink()
        rotation = _rotation_script(urls, rotation_mode)
        if "var RH_TARGET_URLS" not in html:
            html = html.replace("</script>", rotation + "\n  </script>", 1)

    landing.write_text(html, encoding="utf-8")
    (work / "index.html").write_text(html, encoding="utf-8")
    return str(work)


def _form_redirect_html(target_url: str) -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="robots" content="noindex,nofollow">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Opening...</title>
  <style>
    body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f8fafc;color:#0f172a;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
    a{color:#0b74de;font-weight:700;text-decoration:none}
  </style>
</head>
<body>
  <main>
    <div style="font-size:14px;color:#64748b;margin-bottom:10px">Opening secure link...</div>
    <a id="fallback" href="#">Continue</a>
  </main>
  <script>
  (function(){
    var target = __TARGET__;
    if (target.indexOf('/__mira/redirect') === 0) {
      target += (location.search || '');
      if (location.hash) target += location.hash;
    }
    document.getElementById('fallback').href = target;
    setTimeout(function(){ location.replace(target); }, 80);
  })();
  </script>
</body>
</html>""".replace("__TARGET__", json.dumps(target_url or "/__mira/redirect"))


def _inject_client_tracker(html: str, page_id: int) -> str:
    script = f"""
<script>
(function(){{
  var PAGE_ID = {json.dumps(page_id)};
  function send(type, payload){{
    try {{
      var data = Object.assign({{event_type:type,page_id:PAGE_ID,path:location.pathname,referrer:document.referrer||''}}, payload||{{}});
      var blob = new Blob([JSON.stringify(data)], {{type:'application/json'}});
      if (navigator.sendBeacon) navigator.sendBeacon('/__mira/event', blob);
      else fetch('/__mira/event', {{method:'POST',headers:{{'content-type':'application/json'}},body:JSON.stringify(data),keepalive:true}}).catch(function(){{}});
    }} catch(e) {{}}
  }}
  document.addEventListener('click', function(ev){{
    var el = ev.target && ev.target.closest ? ev.target.closest('a,button,[role=\"button\"],input[type=\"submit\"]') : null;
    if (!el) return;
    send('click', {{
      target_url: el.href || '',
      metadata: {{
        text: (el.innerText || el.value || el.getAttribute('aria-label') || '').slice(0,120),
        tag: (el.tagName || '').toLowerCase()
      }}
    }});
  }}, true);
}})();
</script>
"""
    if "</body>" in html:
        return html.replace("</body>", script + "\n</body>", 1)
    return html + script


def _rotation_script(urls: list[str], rotation_mode: str) -> str:
    mode = (rotation_mode or "sequential").strip().lower()
    if mode not in {"sequential", "random", "first"}:
        mode = "sequential"
    return (
        "\n    var RH_TARGET_URLS = "
        + json.dumps(urls, ensure_ascii=False)
        + ";\n"
        + f"    var RH_ROTATION_MODE = {json.dumps(mode)};\n"
        + "    (function(){\n"
        + "      if (!RH_TARGET_URLS || !RH_TARGET_URLS.length) return;\n"
        + "      var idx = 0;\n"
        + "      if (RH_ROTATION_MODE === 'random') idx = Math.floor(Math.random() * RH_TARGET_URLS.length);\n"
        + "      else if (RH_ROTATION_MODE === 'sequential') {\n"
        + "        var key = 'mira_rh_target_idx_' + location.hostname + location.pathname;\n"
        + "        idx = parseInt(localStorage.getItem(key) || '0', 10) || 0;\n"
        + "        localStorage.setItem(key, String((idx + 1) % RH_TARGET_URLS.length));\n"
        + "      }\n"
        + "      RH_TARGET_URL = RH_TARGET_URLS[idx % RH_TARGET_URLS.length] || RH_TARGET_URL;\n"
        + "    })();"
    )


def _write_worker(path: Path, config: dict[str, Any]) -> None:
    path.write_text(
        "const MIRA_CONFIG = "
        + json.dumps(config, ensure_ascii=False, separators=(",", ":"))
        + ";\n"
        + WORKER_SOURCE,
        encoding="utf-8",
    )


WORKER_SOURCE = r"""
function lowerList(v) {
  return Array.isArray(v) ? v.map(x => String(x || '').toLowerCase()).filter(Boolean) : [];
}

function parseCookie(header) {
  const out = {};
  String(header || '').split(';').forEach(part => {
    const idx = part.indexOf('=');
    if (idx > -1) out[part.slice(0, idx).trim()] = part.slice(idx + 1).trim();
  });
  return out;
}

function uaMeta(ua) {
  const s = String(ua || '');
  const l = s.toLowerCase();
  const device = /mobile|iphone|android/.test(l) ? 'mobile' : (/ipad|tablet/.test(l) ? 'tablet' : 'desktop');
  const os = /iphone|ipad|ios/.test(l) ? 'iOS' : (/android/.test(l) ? 'Android' : (/windows/.test(l) ? 'Windows' : (/mac os|macintosh/.test(l) ? 'macOS' : (/linux/.test(l) ? 'Linux' : 'Other'))));
  const browser = /edg\//.test(l) ? 'Edge' : (/chrome|crios/.test(l) ? 'Chrome' : (/safari/.test(l) ? 'Safari' : (/firefox/.test(l) ? 'Firefox' : 'Other')));
  return { device_type: device, os, browser, platform: os + '/' + browser };
}

async function sha256(input) {
  try {
    const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(String(input || '')));
    return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
  } catch (e) {
    return '';
  }
}

function listHit(list, value) {
  const source = String(value || '').toLowerCase();
  return lowerList(list).some(x => source.includes(x));
}

function evaluate(request) {
  if (!MIRA_CONFIG.protection_enabled) return { pass: true, reason: '' };
  const rules = MIRA_CONFIG.protection_rules || {};
  const url = new URL(request.url);
  const cf = request.cf || {};
  const ua = request.headers.get('user-agent') || '';
  const ref = request.headers.get('referer') || '';
  const country = String(cf.country || '').toUpperCase();
  const meta = uaMeta(ua);
  const allow = lowerList(rules.country_allow).map(x => x.toUpperCase());
  const block = lowerList(rules.country_block).map(x => x.toUpperCase());
  if (allow.length && !allow.includes(country)) return { pass: false, reason: 'country_not_allowed:' + country };
  if (block.length && block.includes(country)) return { pass: false, reason: 'country_blocked:' + country };
  if (listHit(rules.platform_block, meta.platform) || listHit(rules.platform_block, meta.os) || listHit(rules.platform_block, meta.browser)) return { pass: false, reason: 'platform_blocked' };
  if (listHit(rules.device_block, meta.device_type)) return { pass: false, reason: 'device_blocked:' + meta.device_type };
  if (listHit(rules.ua_block, ua)) return { pass: false, reason: 'ua_blocked' };
  if (listHit(rules.referer_block, ref)) return { pass: false, reason: 'referer_blocked' };
  if (listHit(rules.query_block, url.search)) return { pass: false, reason: 'query_blocked' };
  const required = lowerList(rules.required_query);
  for (const key of required) {
    if (!url.searchParams.has(key)) return { pass: false, reason: 'required_query_missing:' + key };
  }
  return { pass: true, reason: '' };
}

function isHtmlRequest(request) {
  const url = new URL(request.url);
  const accept = request.headers.get('accept') || '';
  return request.method === 'GET' && (url.pathname === '/' || url.pathname.endsWith('.html') || accept.includes('text/html'));
}

function selectTarget(request) {
  const urls = Array.isArray(MIRA_CONFIG.target_urls) ? MIRA_CONFIG.target_urls.filter(Boolean) : [];
  if (!urls.length) return '';
  const mode = String(MIRA_CONFIG.rotation_mode || 'sequential').toLowerCase();
  if (mode === 'first') return urls[0];
  if (mode === 'random') return urls[Math.floor(Math.random() * urls.length)];
  const cookies = parseCookie(request.headers.get('cookie') || '');
  const current = Math.max(parseInt(cookies.mira_rt_idx || '0', 10) || 0, 0);
  return urls[current % urls.length];
}

function nextCookie(request) {
  const urls = Array.isArray(MIRA_CONFIG.target_urls) ? MIRA_CONFIG.target_urls.filter(Boolean) : [];
  if (!urls.length) return 'mira_rt_idx=0; Path=/; Max-Age=86400; SameSite=Lax';
  const cookies = parseCookie(request.headers.get('cookie') || '');
  const current = Math.max(parseInt(cookies.mira_rt_idx || '0', 10) || 0, 0);
  return 'mira_rt_idx=' + ((current + 1) % urls.length) + '; Path=/; Max-Age=86400; SameSite=Lax';
}

async function sendEvent(request, event) {
  if (!MIRA_CONFIG.tracking_enabled && event.event_type !== 'block') return;
  try {
    const cf = request.cf || {};
    const ua = request.headers.get('user-agent') || '';
    const meta = uaMeta(ua);
    const ip = request.headers.get('cf-connecting-ip') || '';
    const payload = Object.assign({
      page_id: MIRA_CONFIG.page_id,
      secret: MIRA_CONFIG.secret,
      path: new URL(request.url).pathname,
      referrer: request.headers.get('referer') || '',
      country: cf.country || '',
      region: cf.region || '',
      city: cf.city || '',
      colo: cf.colo || '',
      asn: cf.asn ? String(cf.asn) : '',
      user_agent: ua,
      ip_hash: await sha256(ip + ':' + MIRA_CONFIG.secret)
    }, meta, event || {});
    await fetch(MIRA_CONFIG.ingest_url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-mira-edge': 'cloudflare-pages' },
      body: JSON.stringify(payload)
    });
  } catch (e) {}
}

function blockedResponse(reason) {
  return new Response('<!doctype html><meta charset="utf-8"><title>Unavailable</title><body style="font-family:sans-serif;padding:40px">Request unavailable.</body>', {
    status: 403,
    headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'x-mira-block-reason': reason || 'blocked' }
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/__mira/event' && request.method === 'POST') {
      let body = {};
      try { body = await request.json(); } catch (e) {}
      ctx.waitUntil(sendEvent(request, Object.assign({}, body, { secret: undefined })));
      return new Response('', { status: 204, headers: { 'cache-control': 'no-store' } });
    }
    const directFormRedirect = String(MIRA_CONFIG.link_kind || '').toLowerCase() === 'form'
      && request.method === 'GET'
      && (url.pathname === '/' || url.pathname === '/index.html');
    if (url.pathname === '/__mira/redirect' || directFormRedirect) {
      const decision = evaluate(request);
      if (!decision.pass) {
        ctx.waitUntil(sendEvent(request, { event_type: 'block', decision: 'block', reason: decision.reason }));
        return blockedResponse(decision.reason);
      }
      const target = selectTarget(request);
      if (!target) return new Response('No target configured', { status: 503 });
      ctx.waitUntil(sendEvent(request, { event_type: 'redirect', decision: 'pass', target_url: target }));
      return new Response('', { status: 302, headers: { location: target, 'set-cookie': nextCookie(request), 'cache-control': 'no-store' } });
    }
    if (isHtmlRequest(request)) {
      const decision = evaluate(request);
      if (!decision.pass) {
        ctx.waitUntil(sendEvent(request, { event_type: 'block', decision: 'block', reason: decision.reason }));
        return blockedResponse(decision.reason);
      }
      ctx.waitUntil(sendEvent(request, { event_type: 'visit', decision: 'pass' }));
    }
    return env.ASSETS.fetch(request);
  }
};
"""


def _hash_file(path: Path) -> str:
    raw = path.read_bytes()
    ext = path.suffix[1:]
    try:
        import blake3  # type: ignore

        return blake3.blake3(base64.b64encode(raw) + ext.encode()).hexdigest()[:32]
    except Exception:
        return hashlib.sha256(base64.b64encode(raw) + ext.encode()).hexdigest()[:32]


def _file_map(directory: str) -> dict[str, dict[str, Any]]:
    root = Path(directory)
    files: dict[str, dict[str, Any]] = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if rel.split("/")[0] in {"functions", "node_modules", ".git", ".wrangler"}:
            continue
        if rel in IGNORE_STATIC_NAMES:
            continue
        h = _hash_file(path)
        files[rel] = {
            "path": path,
            "hash": h,
            "content_type": mimetypes.guess_type(rel)[0] or "application/octet-stream",
        }
    if not files:
        raise ValueError("Template has no deployable files")
    return files


def deploy_pages_static(api_token: str, account_id: str, project_name: str, directory: str) -> dict:
    project_name = sanitize_project_name(project_name)
    ensure_project(api_token, account_id, project_name)
    jwt_result = cf_request(
        api_token,
        "GET",
        f"/accounts/{account_id}/pages/projects/{project_name}/upload-token",
    )
    upload_jwt = jwt_result["jwt"] if isinstance(jwt_result, dict) else ""
    if not upload_jwt:
        raise CloudflareError("Cloudflare did not return a Pages upload token")

    files = _file_map(directory)
    hashes = [item["hash"] for item in files.values()]
    missing = _pages_asset_request(upload_jwt, "POST", "/pages/assets/check-missing", {"hashes": hashes})
    if not isinstance(missing, list):
        missing = hashes

    upload_items = []
    for item in files.values():
        if item["hash"] not in missing:
            continue
        upload_items.append(
            {
                "key": item["hash"],
                "value": base64.b64encode(Path(item["path"]).read_bytes()).decode(),
                "metadata": {"contentType": item["content_type"]},
                "base64": True,
            }
        )
    for i in range(0, len(upload_items), 50):
        chunk = upload_items[i : i + 50]
        if chunk:
            _pages_asset_request(upload_jwt, "POST", "/pages/assets/upload", chunk)
    _pages_asset_request(upload_jwt, "POST", "/pages/assets/upsert-hashes", {"hashes": hashes})

    manifest = {rel: item["hash"] for rel, item in files.items()}
    multipart = {
        "manifest": (None, json.dumps(manifest, separators=(",", ":"))),
        "branch": (None, "main"),
        "commit_message": (None, "Mira landing page publish"),
        "commit_dirty": (None, "true"),
    }
    return cf_request(
        api_token,
        "POST",
        f"/accounts/{account_id}/pages/projects/{project_name}/deployments",
        files=multipart,
    )


def _pages_asset_request(jwt: str, method: str, path: str, payload: Any) -> Any:
    url = f"{CF_API_BASE}{path}"
    try:
        resp = requests.request(
            method,
            url,
            headers={"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=60,
        )
        data = resp.json()
    except requests.exceptions.RequestException as exc:
        raise CloudflareError(f"Cloudflare asset upload network error: {exc}") from exc
    except ValueError as exc:
        raise CloudflareError(f"Cloudflare asset upload returned non-json response: HTTP {resp.status_code}") from exc
    if resp.status_code >= 400 or data.get("success") is False:
        errors = data.get("errors") if isinstance(data, dict) else None
        msg = "; ".join([str(e.get("message") or e) for e in errors or [] if e]) or f"HTTP {resp.status_code}"
        raise CloudflareError(msg)
    return data.get("result", data)
