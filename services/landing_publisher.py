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
from urllib.parse import urlparse

import requests


CF_API_BASE = "https://api.cloudflare.com/client/v4"
BASE_DIR = Path(os.environ.get("MIRA_BASE_DIR", "/opt/mira"))
DEFAULT_TEMPLATE_DIR = BASE_DIR / "landing_templates" / "rh_fp_advanced"
IGNORE_STATIC_NAMES = {"_headers", "_redirects", "_routes.json", "_worker.js"}


class CloudflareError(RuntimeError):
    pass


def _headers(api_token: str) -> dict:
    return {"Authorization": f"Bearer {api_token}"}


def _replace_js_var(html: str, names: list[str], value: str, canonical: str | None = None) -> tuple[str, int]:
    """Replace the first matching JS string variable assignment.

    New templates should use LP_* variables. Legacy names are accepted on input,
    but the emitted public HTML is normalized to the canonical LP_* name.
    """
    canonical = canonical or (names[0] if names else "")
    total = 0
    for name in names:
        pattern = rf'var\s+{re.escape(name)}\s*=\s*"[^"]*"\s*;'
        html, count = re.subn(pattern, f"var {canonical} = {json.dumps(value or '')};", html, count=1)
        total += count
        if count:
            break
    return html, total


def _normalize_template_markers(html: str) -> str:
    """Remove legacy project/internal markers from public HTML before publish."""
    replacements = {
        "MIRA_PIXEL_ID": "LP_PIXEL_ID",
        "RH_PIXEL_ID": "LP_PIXEL_ID",
        "MIRA_TARGET_URLS": "LP_TARGET_URLS",
        "RH_TARGET_URLS": "LP_TARGET_URLS",
        "MIRA_TARGET_URL": "LP_TARGET_URL",
        "RH_TARGET_URL": "LP_TARGET_URL",
        "MIRA_ROTATION_MODE": "LP_ROTATION_MODE",
        "RH_ROTATION_MODE": "LP_ROTATION_MODE",
        "__mira": "__edge",
        "x-mira": "x-edge",
        "mira_ad_slug": "sid",
        "mira_ad_id": "aid",
        "mira_rt_idx": "lp_rt_idx",
        "Mira landing page publish": "landing page publish",
    }
    for src, dst in replacements.items():
        html = html.replace(src, dst)
    html = re.sub(
        r"<script[^>]+static\.cloudflareinsights\.com[^>]*>.*?</script>",
        "",
        html,
        flags=re.I | re.S,
    )
    return html


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


def list_account_zones(api_token: str, account_id: str) -> list[dict[str, Any]]:
    result = cf_request(api_token, "GET", "/zones", params={"account.id": account_id, "per_page": 100})
    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]
    if isinstance(result, dict):
        items = result.get("result") or []
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
    return []


def _host_from_url(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw if re.match(r"^https?://", raw, flags=re.I) else f"https://{raw}")
    return (parsed.hostname or "").strip().lower().strip(".")


def pages_cname_target(project_name: str, pages_url: str | None = None) -> str:
    host = _host_from_url(pages_url)
    if host and host.endswith(".pages.dev"):
        parts = host.split(".")
        if len(parts) == 3:
            return host
    return f"{sanitize_project_name(project_name)}.pages.dev"


def find_zone_for_domain(api_token: str, account_id: str, domain: str) -> dict[str, Any]:
    domain = normalize_custom_domain(domain)
    matches: list[dict[str, Any]] = []
    for zone in list_account_zones(api_token, account_id):
        zone_name = str(zone.get("name") or "").strip().lower().strip(".")
        if zone_name and (domain == zone_name or domain.endswith("." + zone_name)):
            matches.append(zone)
    if not matches:
        raise CloudflareError(
            f"No Cloudflare zone matched {domain}. The API token needs Zone Read for the root domain account."
        )
    matches.sort(key=lambda z: len(str(z.get("name") or "")), reverse=True)
    return matches[0]


def list_dns_records(api_token: str, zone_id: str, name: str, record_type: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {"name": normalize_custom_domain(name), "per_page": 100}
    if record_type:
        params["type"] = record_type.upper()
    result = cf_request(api_token, "GET", f"/zones/{zone_id}/dns_records", params=params)
    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]
    if isinstance(result, dict):
        items = result.get("result") or []
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
    return []


def ensure_pages_cname_dns_record(
    api_token: str,
    account_id: str,
    project_name: str,
    domain: str,
    pages_url: str | None = None,
    *,
    proxied: bool = True,
) -> dict[str, Any]:
    domain = normalize_custom_domain(domain)
    target = pages_cname_target(project_name, pages_url)
    zone = find_zone_for_domain(api_token, account_id, domain)
    zone_id = str(zone.get("id") or "").strip()
    zone_name = str(zone.get("name") or "").strip()
    if not zone_id:
        raise CloudflareError(f"Cloudflare zone for {domain} has no zone id")

    records = list_dns_records(api_token, zone_id, domain)
    cname_records = [r for r in records if str(r.get("type") or "").upper() == "CNAME"]
    conflicts = [r for r in records if str(r.get("type") or "").upper() != "CNAME"]
    if conflicts and not cname_records:
        types = ", ".join(sorted({str(r.get("type") or "UNKNOWN").upper() for r in conflicts}))
        raise CloudflareError(
            f"DNS record {domain} already exists as {types}. Remove it or choose a subdomain before CNAME automation can continue."
        )

    payload = {"type": "CNAME", "name": domain, "content": target, "ttl": 1, "proxied": bool(proxied)}
    if cname_records:
        record = cname_records[0]
        record_id = str(record.get("id") or "")
        current_content = str(record.get("content") or "").strip().lower().strip(".")
        current_proxied = bool(record.get("proxied"))
        if current_content == target and current_proxied == bool(proxied):
            return {
                "success": True,
                "action": "unchanged",
                "domain": domain,
                "target": target,
                "zone_id": zone_id,
                "zone_name": zone_name,
                "record_id": record_id,
                "proxied": current_proxied,
            }
        updated = cf_request(
            api_token,
            "PATCH",
            f"/zones/{zone_id}/dns_records/{record_id}",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        return {
            "success": True,
            "action": "updated",
            "domain": domain,
            "target": target,
            "zone_id": zone_id,
            "zone_name": zone_name,
            "record_id": str(updated.get("id") or record_id) if isinstance(updated, dict) else record_id,
            "proxied": bool((updated if isinstance(updated, dict) else {}).get("proxied", proxied)),
        }

    created = cf_request(
        api_token,
        "POST",
        f"/zones/{zone_id}/dns_records",
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    return {
        "success": True,
        "action": "created",
        "domain": domain,
        "target": target,
        "zone_id": zone_id,
        "zone_name": zone_name,
        "record_id": str(created.get("id") or "") if isinstance(created, dict) else "",
        "proxied": bool((created if isinstance(created, dict) else {}).get("proxied", proxied)),
    }


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


def delete_pages_project(api_token: str, account_id: str, project_name: str) -> dict:
    project_name = sanitize_project_name(project_name)
    if not project_name:
        raise CloudflareError("Pages project name is required")
    try:
        result = cf_request(
            api_token,
            "DELETE",
            f"/accounts/{account_id}/pages/projects/{project_name}",
        )
        return result if isinstance(result, dict) else {"result": result, "status": "deleted"}
    except CloudflareError as exc:
        lower = str(exc).lower()
        if "not found" in lower or "does not exist" in lower or "could not find" in lower:
            return {"status": "not_found", "name": project_name}
        raise


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


def get_pages_project(api_token: str, account_id: str, project_name: str) -> dict:
    project_name = sanitize_project_name(project_name)
    result = cf_request(api_token, "GET", f"/accounts/{account_id}/pages/projects/{project_name}")
    return result if isinstance(result, dict) else {}


def get_pages_deployment(api_token: str, account_id: str, project_name: str, deployment_id: str) -> dict:
    project_name = sanitize_project_name(project_name)
    deployment_id = (deployment_id or "").strip()
    if not deployment_id:
        return {}
    result = cf_request(
        api_token,
        "GET",
        f"/accounts/{account_id}/pages/projects/{project_name}/deployments/{deployment_id}",
    )
    return result if isinstance(result, dict) else {}


def _deployment_state(deployment: dict) -> tuple[str, str]:
    if not isinstance(deployment, dict):
        return "", ""
    latest = deployment.get("latest_stage") if isinstance(deployment.get("latest_stage"), dict) else {}
    latest_name = str(latest.get("name") or "").lower()
    latest_status = str(latest.get("status") or "").lower()
    for stage in deployment.get("stages") or []:
        if not isinstance(stage, dict):
            continue
        name = str(stage.get("name") or "").lower()
        status = str(stage.get("status") or "").lower()
        if name == "deploy":
            return name, status
    return latest_name, latest_status


def wait_pages_deployment(
    api_token: str,
    account_id: str,
    project_name: str,
    deployment_id: str,
    *,
    timeout_seconds: int = 120,
    interval_seconds: float = 2.0,
) -> dict:
    deadline = time.time() + max(10, timeout_seconds)
    last = {}
    while time.time() < deadline:
        last = get_pages_deployment(api_token, account_id, project_name, deployment_id)
        stage_name, stage_status = _deployment_state(last)
        if stage_status in {"success", "succeeded", "complete", "completed"} and stage_name not in {
            "queued",
            "initialize",
            "initializing",
            "clone_repo",
            "build",
            "upload",
        }:
            return last
        if stage_status in {"failure", "failed", "canceled", "cancelled", "error"}:
            raise CloudflareError(f"Pages deployment failed at stage {stage_name or 'unknown'}: {stage_status}")
        time.sleep(interval_seconds)
    stage_name, stage_status = _deployment_state(last)
    raise CloudflareError(
        f"Pages deployment did not finish within {timeout_seconds}s"
        + (f" (stage {stage_name or 'unknown'}: {stage_status or 'unknown'})" if last else "")
    )


def stable_pages_url(project_name: str, deployment: dict | None = None, project: dict | None = None) -> str:
    project_name = sanitize_project_name(project_name)
    expected_host = f"{project_name}.pages.dev"
    candidates: list[str] = []

    def _host_from(raw: str) -> str:
        url = raw if raw.startswith(("http://", "https://")) else f"https://{raw}"
        return re.sub(r"^https?://", "", url, flags=re.I).split("/", 1)[0].lower()

    def _is_stable_pages_host(host: str) -> bool:
        # Deployment preview hosts look like <deployment>.<project>.pages.dev.
        # Stable Pages hosts have exactly <project>.pages.dev.
        return bool(host.endswith(".pages.dev") and len(host.split(".")) == 3)

    for source in (project or {}, deployment or {}):
        if not isinstance(source, dict):
            continue
        for key in ("subdomain", "canonical_deployment", "url"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())
        aliases = source.get("aliases")
        if isinstance(aliases, list):
            candidates.extend(str(v).strip() for v in aliases if str(v or "").strip())
    for raw in candidates:
        host = _host_from(raw)
        if host == expected_host:
            return f"https://{host}"
    for raw in candidates:
        host = _host_from(raw)
        if _is_stable_pages_host(host):
            return f"https://{host}"
    return f"https://{expected_host}"


def sanitize_project_name(value: str) -> str:
    cleaned = (value or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9-]+", "-", cleaned)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    if not cleaned:
        cleaned = f"site-landing-{int(time.time())}"
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
    route_url: str = "",
    config_url: str = "",
) -> str:
    src = Path(template_dir)
    if not src.exists() or not src.is_dir():
        raise ValueError("Template directory does not exist")
    urls = [u.strip() for u in target_urls or [] if u and u.strip()]
    if not urls:
        raise ValueError("At least one target URL is required")
    work = Path(tempfile.mkdtemp(prefix="landing_page_"))
    redirect_only = (link_kind or "landing").strip().lower() == "form"
    if not redirect_only:
        shutil.copytree(src, work, dirs_exist_ok=True)
    landing = work / "landing.html"
    if not redirect_only and not landing.exists():
        raise ValueError("Template missing landing.html")

    if worker_enabled:
        primary = "/__edge/redirect"
    else:
        primary = urls[0]
    if redirect_only:
        html = _form_redirect_html(primary)
    else:
        html = landing.read_text(encoding="utf-8", errors="ignore")
        html = _normalize_template_markers(html)
        html, _ = _replace_js_var(html, ["LP_PIXEL_ID"], pixel_id or "", "LP_PIXEL_ID")
        html, _ = _replace_js_var(html, ["LP_TARGET_URL"], primary, "LP_TARGET_URL")
    if worker_enabled:
        html = _inject_client_tracker(html, page_id or 0)
        (work / "_routes.json").write_text(
            json.dumps({"version": 1, "include": ["/*"], "exclude": []}, separators=(",", ":")),
            encoding="utf-8",
        )
        _write_worker(
            work / "_worker.js",
            {
                "page_id": page_id,
                "link_kind": (link_kind or "landing").strip().lower(),
                "secret": ingest_secret,
                "ingest_url": ingest_url,
                "route_url": route_url,
                "config_url": config_url,
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
        if "var LP_TARGET_URLS" not in html:
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
    if (target.indexOf('/__edge/redirect') === 0) {
      target += (location.search || '');
      if (location.hash) target += location.hash;
    }
    document.getElementById('fallback').href = target;
    setTimeout(function(){ location.replace(target); }, 80);
  })();
  </script>
</body>
</html>""".replace("__TARGET__", json.dumps(target_url or "/__edge/redirect"))


def _inject_client_tracker(html: str, page_id: int) -> str:
    script = f"""
<script>
(function(){{
  var PAGE_ID = {json.dumps(page_id)};
  function adSlug(){{
    var m = String(location.pathname || '').match(/^\\/a\\/([A-Za-z0-9_-]{{4,64}})\\/?$/);
    return m ? m[1] : '';
  }}
  function adId(){{
    try {{
      var qs = new URLSearchParams(location.search || '');
      var raw = qs.get('ad') || qs.get('ad_id') || qs.get('aid') || '';
      var m = String(raw || '').match(/\\d{{6,}}/g);
      return m && m.length ? m.sort(function(a,b){{return b.length-a.length;}})[0] : '';
    }} catch(e) {{
      return '';
    }}
  }}
  function withAdContext(url){{
    var slug = adSlug();
    var ad = adId();
    if ((!slug && !ad) || !url) return url || '';
    try {{
      var u = new URL(url, location.origin);
      if (u.pathname !== '/__edge/redirect') return url;
      if (slug && !u.searchParams.get('sid')) u.searchParams.set('sid', slug);
      if (ad && !u.searchParams.get('aid')) u.searchParams.set('aid', ad);
      return u.pathname + u.search + u.hash;
    }} catch(e) {{
      return url;
    }}
  }}
  function preserveAdContext(){{
    try {{
      if (typeof window.LP_TARGET_URL === 'string') window.LP_TARGET_URL = withAdContext(window.LP_TARGET_URL);
      document.querySelectorAll('a[href]').forEach(function(a){{
        var next = withAdContext(a.getAttribute('href') || '');
        if (next) a.setAttribute('href', next);
      }});
    }} catch(e) {{}}
  }}
  function send(type, payload){{
    try {{
      var base = {{event_type:type,page_id:PAGE_ID,path:location.pathname,referrer:document.referrer||'',metadata:{{ad_slug:adSlug(),ad_id:adId()}}}};
      var data = Object.assign(base, payload||{{}});
      data.metadata = Object.assign(base.metadata || {{}}, (payload && payload.metadata) || {{}});
      var blob = new Blob([JSON.stringify(data)], {{type:'application/json'}});
      if (navigator.sendBeacon) navigator.sendBeacon('/__edge/event', blob);
      else fetch('/__edge/event', {{method:'POST',headers:{{'content-type':'application/json'}},body:JSON.stringify(data),keepalive:true}}).catch(function(){{}});
    }} catch(e) {{}}
  }}
  document.addEventListener('click', function(ev){{
    var el = ev.target && ev.target.closest ? ev.target.closest('a,button,[role=\"button\"],input[type=\"submit\"]') : null;
    if (!el) return;
    if (el.getAttribute && el.hasAttribute('href')) {{
      var nextHref = withAdContext(el.getAttribute('href') || '');
      if (nextHref) el.setAttribute('href', nextHref);
    }}
    send('click', {{
      target_url: el.href || '',
      metadata: {{
        text: (el.innerText || el.value || el.getAttribute('aria-label') || '').slice(0,120),
        tag: (el.tagName || '').toLowerCase()
      }}
    }});
  }}, true);
  document.addEventListener('submit', function(ev){{
    var form = ev.target || null;
    send('submit', {{
      target_url: form && form.action ? form.action : '',
      metadata: {{
        form_id: form && form.id ? String(form.id).slice(0,120) : '',
        form_name: form && form.name ? String(form.name).slice(0,120) : ''
      }}
    }});
  }}, true);
  preserveAdContext();
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', preserveAdContext);
  else setTimeout(preserveAdContext, 0);
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
        "\n    var LP_TARGET_URLS = "
        + json.dumps(urls, ensure_ascii=False)
        + ";\n"
        + f"    var LP_ROTATION_MODE = {json.dumps(mode)};\n"
        + "    (function(){\n"
        + "      if (!LP_TARGET_URLS || !LP_TARGET_URLS.length) return;\n"
        + "      var idx = 0;\n"
        + "      if (LP_ROTATION_MODE === 'random') idx = Math.floor(Math.random() * LP_TARGET_URLS.length);\n"
        + "      else if (LP_ROTATION_MODE === 'sequential') {\n"
        + "        var key = 'lp_target_idx_' + location.hostname + location.pathname;\n"
        + "        idx = parseInt(localStorage.getItem(key) || '0', 10) || 0;\n"
        + "        localStorage.setItem(key, String((idx + 1) % LP_TARGET_URLS.length));\n"
        + "      }\n"
        + "      LP_TARGET_URL = LP_TARGET_URLS[idx % LP_TARGET_URLS.length] || LP_TARGET_URL;\n"
        + "    })();"
    )


def _write_worker(path: Path, config: dict[str, Any]) -> None:
    path.write_text(
        "const EDGE_CONFIG = "
        + json.dumps(config, ensure_ascii=False, separators=(",", ":"))
        + ";\n"
        + WORKER_SOURCE,
        encoding="utf-8",
    )


WORKER_SOURCE = r"""
let EDGE_RUNTIME_CONFIG = null;
let EDGE_RUNTIME_CONFIG_EXPIRES = 0;

function lowerList(v) {
  return Array.isArray(v) ? v.map(x => String(x || '').toLowerCase()).filter(Boolean) : [];
}

function mergeConfig(base, live) {
  const out = Object.assign({}, base || {});
  if (!live || typeof live !== 'object') return out;
  [
    'link_kind',
    'target_urls',
    'rotation_mode',
    'tracking_enabled',
    'protection_enabled',
    'protection_rules',
    'config_updated_at',
    'config_version'
  ].forEach(k => {
    if (Object.prototype.hasOwnProperty.call(live, k)) out[k] = live[k];
  });
  return out;
}

async function runtimeConfig() {
  const now = Date.now();
  if (EDGE_RUNTIME_CONFIG && now < EDGE_RUNTIME_CONFIG_EXPIRES) return EDGE_RUNTIME_CONFIG;
  if (!EDGE_CONFIG.config_url) {
    EDGE_RUNTIME_CONFIG = EDGE_CONFIG;
    EDGE_RUNTIME_CONFIG_EXPIRES = now + 60000;
    return EDGE_RUNTIME_CONFIG;
  }
  try {
    const resp = await fetch(EDGE_CONFIG.config_url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-edge-runtime': 'site-worker' },
      body: JSON.stringify({ page_id: EDGE_CONFIG.page_id, secret: EDGE_CONFIG.secret })
    });
    if (resp.ok) {
      const data = await resp.json();
      const live = data && data.config ? data.config : data;
      EDGE_RUNTIME_CONFIG = mergeConfig(EDGE_CONFIG, live);
      const ttl = Math.max(5, Math.min(parseInt((data && data.cache_seconds) || '30', 10) || 30, 300));
      EDGE_RUNTIME_CONFIG_EXPIRES = now + ttl * 1000;
      return EDGE_RUNTIME_CONFIG;
    }
  } catch (e) {}
  EDGE_RUNTIME_CONFIG = EDGE_CONFIG;
  EDGE_RUNTIME_CONFIG_EXPIRES = now + 15000;
  return EDGE_RUNTIME_CONFIG;
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

function sourcePlatform(request) {
  const url = new URL(request.url);
  const ua = String(request.headers.get('user-agent') || '').toLowerCase();
  const ref = String(request.headers.get('referer') || '').toLowerCase();
  const qs = url.searchParams;
  const utm = String(qs.get('utm_source') || qs.get('source') || qs.get('src') || '').toLowerCase();
  const source = [utm, ref, ua].join(' ');
  if (qs.has('fbclid') || /\bfacebook\b|fb\.com|m\.facebook\.com|l\.facebook\.com|lm\.facebook\.com|fb_iab|fban|fbav/.test(source)) return 'facebook';
  if (qs.has('igshid') || /\binstagram\b|instagram\.com|ig_iab/.test(source)) return 'instagram';
  if (qs.has('ttclid') || /\btiktok\b|tiktok\.com|musical_ly/.test(source)) return 'tiktok';
  if (qs.has('gclid') || /\bgoogle\b|google\./.test(source)) return 'google';
  if (qs.has('msclkid') || /\bbing\b|bing\./.test(source)) return 'bing';
  if (/\bwhatsapp\b|wa\.me|api\.whatsapp\.com/.test(source)) return 'whatsapp';
  if (/\btelegram\b|t\.me/.test(source)) return 'telegram';
  return 'unknown';
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

function evaluate(request, cfg) {
  cfg = cfg || EDGE_CONFIG;
  if (!cfg.protection_enabled) return { pass: true, reason: '' };
  const rules = cfg.protection_rules || {};
  const url = new URL(request.url);
  const cf = request.cf || {};
  const ua = request.headers.get('user-agent') || '';
  const ref = request.headers.get('referer') || '';
  const country = String(cf.country || '').toUpperCase();
  const meta = uaMeta(ua);
  const source = sourcePlatform(request);
  const allow = lowerList(rules.country_allow).map(x => x.toUpperCase());
  const block = lowerList(rules.country_block).map(x => x.toUpperCase());
  const sourceAllow = lowerList(rules.source_allow);
  if (allow.length && !allow.includes(country)) return { pass: false, reason: 'country_not_allowed:' + country };
  if (block.length && block.includes(country)) return { pass: false, reason: 'country_blocked:' + country };
  if (sourceAllow.length && !sourceAllow.includes(source)) return { pass: false, reason: 'source_not_allowed:' + source };
  if (lowerList(rules.source_block).includes(source)) return { pass: false, reason: 'source_blocked:' + source };
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
  const method = request.method === 'GET' || request.method === 'HEAD';
  return method && (url.pathname === '/' || url.pathname.endsWith('.html') || accept.includes('text/html'));
}

function isReadRequest(request) {
  return request.method === 'GET' || request.method === 'HEAD';
}

function adSlugFromPath(pathname) {
  const m = String(pathname || '').match(/^\/a\/([A-Za-z0-9_-]{4,64})\/?$/);
  return m ? m[1] : '';
}

function adSlugFromRequest(request) {
  try {
    const url = new URL(request.url);
    const fromPath = adSlugFromPath(url.pathname);
    if (fromPath) return fromPath;
    const fromQuery = url.searchParams.get('sid') || url.searchParams.get('ad_slug') || '';
    if (/^[A-Za-z0-9_-]{4,64}$/.test(fromQuery)) return fromQuery;
    const ref = request.headers.get('referer') || request.headers.get('referrer') || '';
    if (ref) {
      const refUrl = new URL(ref);
      const fromRef = adSlugFromPath(refUrl.pathname);
      if (fromRef) return fromRef;
    }
  } catch (e) {}
  return '';
}

function adIdFromRequest(request) {
  function clean(raw) {
    const m = String(raw || '').match(/\d{6,}/g);
    return m && m.length ? m.sort((a, b) => b.length - a.length)[0] : '';
  }
  try {
    const url = new URL(request.url);
    const direct = clean(url.searchParams.get('ad') || url.searchParams.get('ad_id') || url.searchParams.get('aid') || url.searchParams.get('fb_ad_id') || '');
    if (direct) return direct;
    const ref = request.headers.get('referer') || request.headers.get('referrer') || '';
    if (ref) {
      const refUrl = new URL(ref);
      return clean(refUrl.searchParams.get('ad') || refUrl.searchParams.get('ad_id') || refUrl.searchParams.get('aid') || refUrl.searchParams.get('fb_ad_id') || '');
    }
  } catch (e) {}
  return '';
}

async function nextTargetFromServer(request, cfg) {
  cfg = cfg || EDGE_CONFIG;
  if (!cfg.route_url) return '';
  try {
    const url = new URL(request.url);
    const adSlug = adSlugFromRequest(request);
    const adId = adIdFromRequest(request);
    const resp = await fetch(cfg.route_url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-edge-runtime': 'site-worker' },
      body: JSON.stringify({
        page_id: cfg.page_id,
        secret: cfg.secret,
        path: url.pathname,
        referrer: request.headers.get('referer') || '',
        metadata: {
          host: url.hostname,
          search: String(url.search || '').slice(0, 500),
          ad_slug: adSlug,
          ad_id: adId
        }
      })
    });
    if (!resp.ok) return '';
    const data = await resp.json();
    return data && data.target_url ? String(data.target_url) : '';
  } catch (e) {
    return '';
  }
}

async function selectTarget(request, cfg) {
  cfg = cfg || EDGE_CONFIG;
  const urls = Array.isArray(cfg.target_urls) ? cfg.target_urls.filter(Boolean) : [];
  const serverTarget = await nextTargetFromServer(request, cfg);
  if (serverTarget) return serverTarget;
  if (!urls.length) return '';
  const mode = String(cfg.rotation_mode || 'sequential').toLowerCase();
  if (mode === 'first') return urls[0];
  if (mode === 'random') return urls[Math.floor(Math.random() * urls.length)];
  const cookies = parseCookie(request.headers.get('cookie') || '');
  const current = Math.max(parseInt(cookies.lp_rt_idx || '0', 10) || 0, 0);
  return urls[current % urls.length];
}

function nextCookie(request, cfg) {
  cfg = cfg || EDGE_CONFIG;
  const urls = Array.isArray(cfg.target_urls) ? cfg.target_urls.filter(Boolean) : [];
  if (!urls.length) return 'lp_rt_idx=0; Path=/; Max-Age=86400; SameSite=Lax';
  const cookies = parseCookie(request.headers.get('cookie') || '');
  const current = Math.max(parseInt(cookies.lp_rt_idx || '0', 10) || 0, 0);
  return 'lp_rt_idx=' + ((current + 1) % urls.length) + '; Path=/; Max-Age=86400; SameSite=Lax';
}

async function sendEvent(request, event, cfg) {
  cfg = cfg || EDGE_CONFIG;
  if (!cfg.tracking_enabled && event.event_type !== 'block') return;
  try {
    const cf = request.cf || {};
    const ua = request.headers.get('user-agent') || '';
    const meta = uaMeta(ua);
    const ip = request.headers.get('cf-connecting-ip') || '';
    const sourceMeta = { source_platform: sourcePlatform(request), ad_slug: adSlugFromRequest(request), ad_id: adIdFromRequest(request) };
    const payload = Object.assign({
      page_id: cfg.page_id,
      secret: cfg.secret,
      path: new URL(request.url).pathname,
      referrer: request.headers.get('referer') || '',
      country: cf.country || '',
      region: cf.region || '',
      city: cf.city || '',
      colo: cf.colo || '',
      asn: cf.asn ? String(cf.asn) : '',
      user_agent: ua,
      ip_hash: await sha256(ip + ':' + cfg.secret)
    }, meta, event || {});
    payload.secret = cfg.secret;
    payload.metadata = Object.assign(sourceMeta, (event && event.metadata) || {}, payload.metadata || {});
    await fetch(cfg.ingest_url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-edge-runtime': 'site-worker' },
      body: JSON.stringify(payload)
    });
  } catch (e) {}
}

function blockedResponse(reason) {
  return new Response('', {
    status: 302,
    headers: { location: 'https://www.facebook.com/', 'cache-control': 'no-store', 'x-edge-block-reason': reason || 'blocked' }
  });
}

function notFoundResponse() {
  return new Response('', {
    status: 404,
    headers: { 'cache-control': 'no-store' }
  });
}

function isStaticAssetPath(pathname) {
  return /\.(?:js|mjs|css|map|json|txt|xml|ico|png|jpe?g|gif|webp|avif|svg|mp4|webm|woff2?|ttf|otf)$/i.test(String(pathname || ''));
}

function isHtmlFallbackResponse(response) {
  const ct = response && response.headers ? (response.headers.get('content-type') || '') : '';
  return /text\/html/i.test(ct);
}

export default {
  async fetch(request, env, ctx) {
    const cfg = await runtimeConfig();
    const url = new URL(request.url);
    const adSlug = adSlugFromRequest(request);
    const adId = adIdFromRequest(request);
    if (url.pathname === '/_worker.js' || url.pathname === '/_routes.json') {
      return notFoundResponse();
    }
    if (url.pathname === '/__mira' || url.pathname.startsWith('/__mira/')) {
      return notFoundResponse();
    }
    if (url.pathname === '/__edge-legacy' || url.pathname.startsWith('/__edge-legacy/')) {
      return blockedResponse('legacy_route');
    }
    if (url.pathname === '/__edge/event' && request.method === 'POST') {
      let body = {};
      try { body = await request.json(); } catch (e) {}
      ctx.waitUntil(sendEvent(request, Object.assign({}, body, { secret: undefined }), cfg));
      return new Response('', { status: 204, headers: { 'cache-control': 'no-store' } });
    }
    const directFormRedirect = String(cfg.link_kind || '').toLowerCase() === 'form'
      && isReadRequest(request)
      && (url.pathname === '/' || url.pathname === '/index.html' || !!adSlug || (url.pathname === '/a' && !!adId));
    if (url.pathname === '/__edge/redirect' || directFormRedirect) {
      const decision = evaluate(request, cfg);
      if (!decision.pass) {
        ctx.waitUntil(sendEvent(request, { event_type: 'block', decision: 'block', reason: decision.reason, metadata: { ad_slug: adSlug, ad_id: adId } }, cfg));
        return blockedResponse(decision.reason);
      }
      const target = await selectTarget(request, cfg);
      if (!target) return new Response('No target configured', { status: 503 });
      ctx.waitUntil(sendEvent(request, { event_type: 'redirect', decision: 'pass', target_url: target, metadata: { ad_slug: adSlug, ad_id: adId } }, cfg));
      return new Response('', { status: 302, headers: { location: target, 'set-cookie': nextCookie(request, cfg), 'cache-control': 'no-store' } });
    }
    if (url.pathname === '/__edge' || url.pathname.startsWith('/__edge/')) {
      return notFoundResponse();
    }
    if ((adSlug || (url.pathname === '/a' && adId)) && isReadRequest(request)) {
      const decision = evaluate(request, cfg);
      if (!decision.pass) {
        ctx.waitUntil(sendEvent(request, { event_type: 'block', decision: 'block', reason: decision.reason, metadata: { ad_slug: adSlug, ad_id: adId } }, cfg));
        return blockedResponse(decision.reason);
      }
      ctx.waitUntil(sendEvent(request, { event_type: 'visit', decision: 'pass', metadata: { ad_slug: adSlug, ad_id: adId } }, cfg));
      const assetUrl = new URL(request.url);
      assetUrl.pathname = '/';
      return env.ASSETS.fetch(new Request(assetUrl.toString(), request));
    }
    if (isHtmlRequest(request)) {
      const decision = evaluate(request, cfg);
      if (!decision.pass) {
        ctx.waitUntil(sendEvent(request, { event_type: 'block', decision: 'block', reason: decision.reason }, cfg));
        return blockedResponse(decision.reason);
      }
      ctx.waitUntil(sendEvent(request, { event_type: 'visit', decision: 'pass' }, cfg));
    }
    const assetResponse = await env.ASSETS.fetch(request);
    if (isStaticAssetPath(url.pathname) && (assetResponse.status === 404 || isHtmlFallbackResponse(assetResponse))) {
      return notFoundResponse();
    }
    return assetResponse;
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


def _worker_bundle_bytes(worker_path: Path) -> bytes:
    boundary = "----edge-worker-bundle-" + hashlib.sha256(str(time.time()).encode()).hexdigest()[:24]
    metadata = json.dumps({"main_module": worker_path.name}, separators=(",", ":"))
    worker_source = worker_path.read_text(encoding="utf-8")
    parts = [
        (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="metadata"\r\n\r\n'
            f"{metadata}\r\n"
        ),
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{worker_path.name}"; filename="{worker_path.name}"\r\n'
            "Content-Type: application/javascript+module\r\n\r\n"
            f"{worker_source}\r\n"
        ),
        f"--{boundary}--\r\n",
    ]
    return "".join(parts).encode("utf-8")


def deploy_pages_static(api_token: str, account_id: str, project_name: str, directory: str) -> dict:
    project_name = sanitize_project_name(project_name)
    project = ensure_project(api_token, account_id, project_name)
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

    manifest = {f"/{rel.lstrip('/')}": item["hash"] for rel, item in files.items()}
    multipart = {
        "manifest": (None, json.dumps(manifest, separators=(",", ":"))),
        "branch": (None, "main"),
        "commit_message": (None, "landing page publish"),
        "commit_dirty": (None, "true"),
    }
    worker_file = Path(directory) / "_worker.js"
    if worker_file.exists():
        multipart["_worker.bundle"] = (
            "_worker.bundle",
            _worker_bundle_bytes(worker_file),
            "application/octet-stream",
        )
    routes_file = Path(directory) / "_routes.json"
    if routes_file.exists():
        multipart["_routes.json"] = (
            "_routes.json",
            routes_file.read_text(encoding="utf-8"),
            "application/json",
        )
    deployment = cf_request(
        api_token,
        "POST",
        f"/accounts/{account_id}/pages/projects/{project_name}/deployments",
        files=multipart,
    )
    deployment_id = str((deployment or {}).get("id") or "").strip()
    if deployment_id:
        deployment = wait_pages_deployment(api_token, account_id, project_name, deployment_id)
    try:
        project = get_pages_project(api_token, account_id, project_name)
    except CloudflareError:
        pass
    if isinstance(deployment, dict):
        deployment["deployment_url"] = deployment.get("url") or ""
        deployment["stable_url"] = stable_pages_url(project_name, deployment=deployment, project=project)
    return deployment


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
