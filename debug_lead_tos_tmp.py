import json
import requests

from api.assets import _fetch_page_access_context, _probe_lead_form_permission
from services.token_manager import ACTION_READ, get_exec_token

FB_BASE = "https://graph.facebook.com/v21.0"
act_id = "act_1142151587366081"
page_id = "105596689265722"
token = get_exec_token(act_id, ACTION_READ)
page_ctx = _fetch_page_access_context(requests, FB_BASE, token, page_id)

print("PAGE_CTX:", json.dumps(page_ctx, ensure_ascii=False))

for label, probe_token in [
    ("user_token", token),
    ("page_token", page_ctx.get("page_token") or ""),
]:
    if not probe_token:
        print(label, "missing")
        continue
    resp = requests.get(
        f"{FB_BASE}/{page_id}/leadgen_tos",
        params={"access_token": probe_token},
        timeout=20,
    )
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text[:500]}
    print(label, "leadgen_tos:", json.dumps(data, ensure_ascii=False))

    resp2 = requests.get(
        f"{FB_BASE}/{page_id}/leadgen_forms",
        params={"access_token": probe_token, "limit": 1},
        timeout=20,
    )
    try:
        data2 = resp2.json()
    except Exception:
        data2 = {"raw_text": resp2.text[:500]}
    print(label, "leadgen_forms:", json.dumps(data2, ensure_ascii=False))

print("lead_form_permission_probe:", json.dumps(_probe_lead_form_permission(requests, FB_BASE, token, page_id), ensure_ascii=False))
