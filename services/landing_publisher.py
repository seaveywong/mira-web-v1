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
IGNORE_STATIC_NAMES = {"_worker.js", "_headers", "_redirects", "_routes.json"}


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
        msg = "; ".join(
            [str(e.get("message") or e) for e in errors or [] if e]
        ) or f"HTTP {resp.status_code}"
        raise CloudflareError(msg)
    return data.get("result", data)


def verify_token_and_accounts(api_token: str) -> dict:
    token_info = cf_request(api_token, "GET", "/user/tokens/verify")
    accounts = cf_request(api_token, "GET", "/accounts")
    if isinstance(accounts, dict) and "result" in accounts:
        accounts = accounts["result"]
    return {"token": token_info, "accounts": accounts if isinstance(accounts, list) else []}


def list_pages_projects(api_token: str, account_id: str) -> list:
    result = cf_request(api_token, "GET", f"/accounts/{account_id}/pages/projects")
    return result if isinstance(result, list) else result.get("result", [])


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


def prepare_template(
    template_dir: str | os.PathLike,
    pixel_id: str,
    target_urls: list[str],
    rotation_mode: str = "sequential",
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
    if not landing.exists():
        raise ValueError("Template missing landing.html")
    html = landing.read_text(encoding="utf-8", errors="ignore")
    html = re.sub(
        r'var\s+RH_PIXEL_ID\s*=\s*"[^"]*"\s*;',
        f'var RH_PIXEL_ID = {json.dumps(pixel_id or "")};',
        html,
        count=1,
    )
    primary = urls[0]
    html = re.sub(
        r'var\s+RH_TARGET_URL\s*=\s*"[^"]*"\s*;',
        f'var RH_TARGET_URL = {json.dumps(primary)};',
        html,
        count=1,
    )
    rotation = _rotation_script(urls, rotation_mode)
    if "var RH_TARGET_URLS" not in html:
        html = html.replace("</script>", rotation + "\n  </script>", 1)
    landing.write_text(html, encoding="utf-8")
    # First release is static-only. Make root path serve the real landing page.
    (work / "index.html").write_text(html, encoding="utf-8")
    return str(work)


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


def _hash_file(path: Path) -> str:
    # Wrangler uses blake3(base64(file)+extension). A stable 32-char key is enough
    # for the Pages asset API as long as manifest and uploaded asset keys match.
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
    deploy_result = cf_request(
        api_token,
        "POST",
        f"/accounts/{account_id}/pages/projects/{project_name}/deployments",
        files=multipart,
    )
    return deploy_result


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
