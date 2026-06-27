import json
from typing import Any, Optional
from urllib.parse import urlparse


def clean_act_id(value: Any) -> str:
    raw = str(value or "").strip()
    if raw.startswith("act_"):
        raw = raw[4:]
    return "".join(ch for ch in raw if ch.isdigit())


def _json_loads(raw: Any, default: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw or "")
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Consolidated landing-page URL / domain-status helpers.
#
# Historically this logic was duplicated in three places with subtle drift
# (the #7 子码 bug root cause: one copy referenced a phantom column):
#   - services/launch_engine.py  _landing_link_base / _landing_domain_status_usable
#   - api/assets.py              _launch_landing_link_base / _launch_domain_status_usable
#   - services/landing_link_resolver.py (this file) _domain_status_usable / published_page_url
#
# These are the single source of truth now; the other modules delegate here.
# ---------------------------------------------------------------------------


def landing_link_base(value: Any) -> str:
    """Normalise a landing URL to its comparable base (scheme://host).

    Strips trailing ``/a/<slug>`` short-link paths and any trailing slash so
    that a page's ``pages_url``/``custom_domain`` and an inbound ad URL compare
    equal. Returns "" for short-link paths (``/a/...``) which cannot be a page
    base.
    """
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
        if not parsed.netloc:
            return raw.rstrip("/").lower()
        path = (parsed.path or "").rstrip("/")
        if path.startswith("/a/"):
            return ""
        if path not in ("", "/"):
            return raw.rstrip("/").lower()
        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
    except Exception:
        return raw.rstrip("/").lower()


def domain_status_usable(
    raw_response: Any,
    last_error: str = "",
    *,
    accept_true_status: bool = True,
) -> bool:
    """Decide whether a landing page's custom_domain is usable for serving.

    Mirrors the union of the historic checks: a runtime-usable flag wins
    immediately; otherwise the recorded ``last_error`` can short-circuit to
    False, and finally the structured ``domain_status`` payload is inspected
    for an active/verified-style status token.

    ``accept_true_status`` preserves a real behavioural difference between
    call-sites and MUST be passed explicitly where the historic default was
    False: ``launch_engine`` historically did NOT treat the literal token
    ``"true"`` as a usable status, while ``assets`` and this resolver did.
    Routing both through this one function with the flag keeps each
    call-site's exact behaviour while removing the drift that caused #7.
    """
    try:
        raw = raw_response if isinstance(raw_response, dict) else json.loads(raw_response or "{}")
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        raw = {}
    if raw.get("custom_domain_runtime_usable"):
        return True
    err = str(last_error or "").strip().lower()
    if any(token in err for token in ("authentication", "permission", "not authorized", "forbidden", "failed")):
        return False
    domain_status = raw.get("domain_status") or raw.get("custom_domain_result")
    tokens = {"active", "success", "verified", "ready", "ok"}
    if accept_true_status:
        tokens = tokens | {"true"}
    values: list[str] = []
    if isinstance(domain_status, dict):
        for key in ("status", "verified", "validation_status", "verification_status", "ssl_status"):
            value = domain_status.get(key)
            if value is not None:
                values.append(str(value).strip().lower())
    elif domain_status is not None:
        values.append(str(domain_status).strip().lower())
    return any(value in tokens for value in values)


def resolve_public_url(row: Any, *, accept_true_status: bool = True) -> str:
    """Return the usable public URL for a landing-page row, or "".

    Custom-domain pages resolve to ``https://<custom_domain>`` only when the
    domain is verified/usable; otherwise a pages.dev-style ``pages_url``
    fallback is used. Empty when neither is available.
    """
    custom_domain = str(_row_get(row, "custom_domain", "") or "").strip().rstrip("/")
    pages_url = str(_row_get(row, "pages_url", "") or "").strip().rstrip("/")
    raw_response = _row_get(row, "raw_response", "")
    last_error = str(_row_get(row, "last_error", "") or "")
    if custom_domain and domain_status_usable(raw_response, last_error, accept_true_status=accept_true_status):
        return f"https://{custom_domain}"
    if not custom_domain and pages_url:
        return pages_url
    return ""


# Backwards-compatible internal alias kept for any in-module caller; new code
# should call ``domain_status_usable`` / ``resolve_public_url`` directly.
def _domain_status_usable(status: Any, last_error: str = "") -> bool:
    err = str(last_error or "").strip().lower()
    if any(token in err for token in ("authentication", "permission", "not authorized", "forbidden", "failed")):
        return False
    values: list[str] = []
    if isinstance(status, dict):
        for key in ("status", "verified", "validation_status", "verification_status", "ssl_status"):
            value = status.get(key)
            if value is not None:
                values.append(str(value).strip().lower())
    elif status is not None:
        values.append(str(status).strip().lower())
    return any(value in {"active", "success", "verified", "ready", "ok", "true"} for value in values)


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return default


def published_page_url(row: Any) -> str:
    return resolve_public_url(row, accept_true_status=True)


def find_bound_landing_link(conn, act_id: str, target: str = "landing") -> str:
    clean = clean_act_id(act_id)
    if not clean:
        return ""
    target = "form" if str(target or "").strip().lower() == "form" else "landing"
    bind_targets = ("form", "both") if target == "form" else ("landing", "both")
    try:
        rows = conn.execute(
            """SELECT id, title, link_kind, pages_url, custom_domain, raw_response,
                      last_error, bound_act_ids, bind_target
               FROM landing_pages
               WHERE COALESCE(status,'')='published'
                 AND COALESCE(bound_act_ids,'') LIKE ?
                 AND COALESCE(bind_target,'none') IN (?,?)
               ORDER BY updated_at DESC, id DESC
               LIMIT 100""",
            (f"%{clean}%", bind_targets[0], bind_targets[1]),
        ).fetchall()
    except Exception:
        return ""
    for row in rows:
        ids = [clean_act_id(item) for item in (_json_loads(_row_get(row, "bound_act_ids", ""), []) or [])]
        if clean not in ids:
            continue
        link_kind = str(_row_get(row, "link_kind", "landing") or "landing").strip().lower()
        if target == "landing" and link_kind == "form":
            continue
        url = published_page_url(row)
        if url:
            return url
    return ""


def resolve_account_landing_link(conn, act_id: str, account: Optional[dict] = None, default: str = "") -> str:
    account_url = str((account or {}).get("landing_url") or "").strip()
    if account_url:
        return account_url
    bound_url = find_bound_landing_link(conn, act_id, "landing")
    if bound_url:
        return bound_url
    return str(default or "").strip()


def resolve_account_form_link(
    conn,
    act_id: str,
    account: Optional[dict] = None,
    landing_fallback: str = "",
    default: str = "",
) -> str:
    form_url = str((account or {}).get("form_link") or "").strip()
    if form_url:
        return form_url
    bound_url = find_bound_landing_link(conn, act_id, "form")
    if bound_url:
        return bound_url
    return str(landing_fallback or default or "").strip()
