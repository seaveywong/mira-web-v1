import json
from typing import Any, Optional


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
    custom_domain = str(_row_get(row, "custom_domain", "") or "").strip().rstrip("/")
    pages_url = str(_row_get(row, "pages_url", "") or "").strip().rstrip("/")
    raw_response = _json_loads(_row_get(row, "raw_response", ""), {})
    last_error = str(_row_get(row, "last_error", "") or "")
    runtime_usable = bool(isinstance(raw_response, dict) and raw_response.get("custom_domain_runtime_usable"))
    domain_status = None
    if isinstance(raw_response, dict):
        domain_status = raw_response.get("domain_status") or raw_response.get("custom_domain_result")
    if custom_domain and (runtime_usable or _domain_status_usable(domain_status, last_error)):
        return f"https://{custom_domain}"
    if not custom_domain and pages_url:
        return pages_url
    return ""


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
