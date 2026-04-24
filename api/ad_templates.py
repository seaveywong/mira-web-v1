"""
Mira — 广告模板库 API
支持消息模板（欢迎消息 + 快捷回复）和表单模板（Lead Form）的 CRUD 管理。
铺广告时可下拉选用，系统自动在目标主页上创建并绑定。
"""
import json
import logging
import requests
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from core.auth import get_current_user
from core.database import get_conn, decrypt_token

logger = logging.getLogger("mira.ad_templates")
router = APIRouter()

FB_API = "https://graph.facebook.com/v25.0"
LEAD_FORM_FIELD_TYPES = {
    "CUSTOM", "CITY", "COMPANY_NAME", "COUNTRY", "DOB", "EMAIL", "GENDER",
    "FIRST_NAME", "FULL_NAME", "JOB_TITLE", "LAST_NAME", "MARITIAL_STATUS",
    "WHATSAPP_NUMBER", "EDUCATION_LEVEL", "WEBSITE", "PHONE", "PHONE_OTP",
    "POST_CODE", "PROVINCE", "RELATIONSHIP_STATUS", "STATE", "STREET_ADDRESS",
    "ZIP", "WORK_EMAIL", "MILITARY_STATUS", "WORK_PHONE_NUMBER", "SLIDER",
    "STORE_LOOKUP", "STORE_LOOKUP_WITH_TYPEAHEAD", "DATE_TIME", "ID_CPF",
    "ID_AR_DNI", "ID_CL_RUT", "ID_CO_CC", "ID_EC_CI", "ID_PE_DNI", "ID_MX_RFC",
    "JOIN_CODE", "USER_PROVIDED_PHONE_NUMBER", "FACEBOOK_LEAD_ID", "EMAIL_ALIAS",
    "MESSENGER", "VIN", "LICENSE_PLATE", "THREAD_LINK", "ADDRESS_LINE_TWO",
}
LEAD_FORM_FIELD_ALIASES = {
    "PREDEFINED": "",
    "PHONE_NUMBER": "PHONE",
}
LEAD_FORM_BLOCKED_HOSTS = {
    "bit.ly",
    "buff.ly",
    "cutt.ly",
    "goo.gl",
    "is.gd",
    "lnkd.in",
    "ow.ly",
    "rb.gy",
    "rebrand.ly",
    "shorturl.at",
    "t.co",
    "tiny.one",
    "tinyurl.com",
}


class LeadFormCreateError(Exception):
    """Raised when Lead Form creation fails with a user-facing reason."""


# ─────────────────────────────────────────────────────────────────────────────
# 数据库初始化（在 database.py init_db 之外，首次调用时自动建表）
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_tables():
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS msg_templates (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL,
        greeting    TEXT NOT NULL DEFAULT '你好！感谢你的关注，有什么可以帮到你？',
        buttons     TEXT NOT NULL DEFAULT '[]',
        destination TEXT NOT NULL DEFAULT 'MESSENGER',
        note        TEXT,
        created_at  TEXT DEFAULT (datetime('now')),
        updated_at  TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS lead_form_templates (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        name            TEXT NOT NULL,
        headline        TEXT NOT NULL DEFAULT '立即了解详情',
        description     TEXT,
        questions       TEXT NOT NULL DEFAULT '[]',
        privacy_url     TEXT NOT NULL DEFAULT 'https://www.facebook.com',
        privacy_text    TEXT NOT NULL DEFAULT '隐私政策',
        thank_you_title TEXT DEFAULT '感谢您的提交！',
        thank_you_body  TEXT DEFAULT '我们会尽快与您联系。',
        locale          TEXT DEFAULT 'zh_CN',
        note            TEXT,
        created_at      TEXT DEFAULT (datetime('now')),
        updated_at      TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS page_lead_forms (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        page_id         TEXT NOT NULL,
        template_id     INTEGER NOT NULL,
        fb_form_id      TEXT NOT NULL,
        created_at      TEXT DEFAULT (datetime('now')),
        UNIQUE(page_id, template_id)
    );
    """)
    conn.commit()
    conn.close()


_ensure_tables()


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _extract_page_token_from_user_token(page_id: str, user_token: str) -> Optional[str]:
    if not page_id or not user_token:
        return None
    try:
        resp = requests.get(
            f"{FB_API}/me/accounts",
            params={"access_token": user_token, "fields": "id,name,access_token,tasks", "limit": 200},
            timeout=10,
        )
        data = resp.json()
        if "error" in data:
            return None
        for page in data.get("data", []) or []:
            if str(page.get("id")) == str(page_id) and page.get("access_token"):
                return str(page["access_token"])
    except Exception:
        return None
    return None


def _get_page_token(page_id: str, preferred_token: str = "") -> Optional[str]:
    """
    Find a usable Page Access Token for the target page.
    Prefer the token already chosen for the current launch, then fall back to
    other active tokens in the system.
    """
    preferred_token = (preferred_token or "").strip()
    if preferred_token:
        page_token = _extract_page_token_from_user_token(page_id, preferred_token)
        if page_token:
            return page_token

    conn = get_conn()
    tokens = conn.execute(
        "SELECT id, access_token_enc FROM fb_tokens WHERE status='active'"
    ).fetchall()
    conn.close()

    for t in tokens:
        try:
            raw = decrypt_token(t["access_token_enc"])
        except Exception:
            continue
        if preferred_token and raw == preferred_token:
            continue
        page_token = _extract_page_token_from_user_token(page_id, raw)
        if page_token:
            return page_token
    return None


def _get_setting_value(key: str, default: str = "") -> str:
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return ((row["value"] if row and row["value"] is not None else default) or "").strip()


def _normalize_custom_question_key(raw_key: str, index: int) -> str:
    cleaned = []
    for ch in (raw_key or "").strip().lower():
        if ch.isascii() and ch.isalnum():
            cleaned.append(ch)
        else:
            cleaned.append("_")
    normalized = "".join(cleaned).strip("_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized or f"custom_q_{index + 1}"


def _normalize_lead_form_questions(raw_questions: list) -> list:
    normalized = []
    for idx, item in enumerate(raw_questions or []):
        if not isinstance(item, dict):
            continue
        raw_type = str(item.get("type") or "").strip().upper()
        raw_key = str(item.get("key") or "").strip()
        label = str(item.get("label") or item.get("text") or "").strip()

        if raw_type == "PREDEFINED":
            raw_type = raw_key.upper()
        elif not raw_type and raw_key:
            raw_type = raw_key.upper()

        field_type = LEAD_FORM_FIELD_ALIASES.get(raw_type, raw_type)

        if field_type in LEAD_FORM_FIELD_TYPES and field_type != "CUSTOM":
            normalized.append({"type": field_type})
            continue

        if field_type == "CUSTOM" or label:
            custom_key = raw_key
            if not custom_key or custom_key.upper() in LEAD_FORM_FIELD_TYPES or custom_key.upper() == "PREDEFINED":
                custom_key = label
            normalized.append({
                "type": "CUSTOM",
                "key": _normalize_custom_question_key(custom_key, idx),
                "label": label or f"问题 {idx + 1}",
            })

    return normalized


def _get_follow_up_action_url(page_id: str, preferred_url: str = "") -> str:
    candidates = [
        (preferred_url or "").strip(),
        _get_setting_value("default_landing_url", ""),
    ]
    for candidate in candidates:
        if _is_safe_lead_form_url(candidate):
            return candidate
    return f"https://www.facebook.com/{page_id}"


def _is_safe_lead_form_url(candidate: str, *, allow_facebook: bool = False) -> bool:
    candidate = (candidate or "").strip()
    if not (candidate.startswith("http://") or candidate.startswith("https://")):
        return False
    try:
        host = (urlparse(candidate).hostname or "").lower().strip(".")
    except Exception:
        return False
    if not host:
        return False
    for blocked in LEAD_FORM_BLOCKED_HOSTS:
        if host == blocked or host.endswith("." + blocked):
            return False
    if not allow_facebook and (
        host == "facebook.com"
        or host.endswith(".facebook.com")
        or host == "fb.com"
        or host.endswith(".fb.com")
    ):
        return False
    return True


def _get_privacy_policy_url(preferred_url: str = "") -> str:
    candidates = [
        (preferred_url or "").strip(),
        _get_setting_value("lead_form_privacy_url", ""),
    ]
    for candidate in candidates:
        if _is_safe_lead_form_url(candidate):
            return candidate
    return "https://policies.google.com/privacy"


def _format_fb_error(result: dict) -> str:
    err = result.get("error") or {}
    user_msg = str(err.get("error_user_msg") or "").strip()
    message = str(err.get("message") or "").strip()
    code = err.get("code")
    subcode = err.get("error_subcode")

    message_lower = f"{user_msg} {message}".lower()
    if "requires pages_manage_ads permission" in message_lower:
        return "当前主页缺少 pages_manage_ads 权限，请重新授权该主页对应的管理号，或更换一个有主页广告管理权限的 Token"
    if "page access token" in message_lower:
        return "当前主页必须使用 Page Access Token 才能读取或创建 Lead Form，请确认当前操作号已在该主页的 /me/accounts 中可见"
    if "lead generation terms" in message_lower:
        return "当前主页尚未接受 Facebook Lead Generation Terms，请先访问 https://www.facebook.com/ads/leadgen/tos 完成确认"
    if code == 368 or subcode == 1346003 or "reported as abusive" in message_lower:
        return "Facebook 判定当前自动建表单使用的文案或链接存在风险（常见于短链或敏感内容），已拒绝创建。系统会回退到更安全的默认表单/链接，或请直接选择一个现成表单模板"

    parts = []
    if user_msg:
        parts.append(user_msg)
    if message and message != user_msg:
        parts.append(message)
    if code:
        code_text = f"code={code}"
        if subcode:
            code_text += f", subcode={subcode}"
        parts.append(code_text)

    return " | ".join(parts) or "Facebook API 返回未知错误"


def _post_lead_form(
    page_id: str,
    *,
    form_name: str,
    questions: list,
    privacy_url: str,
    privacy_text: str,
    follow_up_url: str,
    locale: str,
    preferred_token: str = "",
) -> str:
    normalized_questions = _normalize_lead_form_questions(questions)
    if not normalized_questions:
        raise LeadFormCreateError("表单字段格式无效，请重新编辑后再试")

    page_token = _get_page_token(page_id, preferred_token)
    if not page_token:
        raise LeadFormCreateError("未找到当前主页的 Page Access Token，请确认当前操作号已绑定该主页并具备 ADVERTISE / MANAGE 权限")

    resolved_privacy_url = _get_privacy_policy_url(privacy_url)
    payload = {
        "name": form_name,
        "questions": json.dumps(normalized_questions, ensure_ascii=False),
        "privacy_policy": json.dumps({
            "url": resolved_privacy_url,
            "link_text": privacy_text or "隐私政策",
        }, ensure_ascii=False),
        "locale": locale or "zh_CN",
        "access_token": page_token,
    }
    resolved_follow_up_url = _get_follow_up_action_url(page_id, follow_up_url)
    if resolved_follow_up_url:
        payload["follow_up_action_url"] = resolved_follow_up_url

    resp = requests.post(
        f"{FB_API}/{page_id}/leadgen_forms",
        data=payload,
        timeout=15,
    )
    try:
        result = resp.json()
    except Exception:
        result = {"error": {"message": resp.text or f"HTTP {resp.status_code}"}}

    if resp.ok and "id" in result:
        return str(result["id"])
    raise LeadFormCreateError(_format_fb_error(result))


def create_custom_lead_form_for_page(
    page_id: str,
    form_title: str,
    questions: list,
    *,
    token: str = "",
    privacy_url: str = "",
    privacy_text: str = "Privacy Policy",
    follow_up_url: str = "",
    locale: str = "en_US",
) -> str:
    resolved_privacy_url = _get_privacy_policy_url(privacy_url)
    form_name = f"[AI] {(form_title or 'Lead Form')[:60]} {datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    return _post_lead_form(
        page_id,
        form_name=form_name,
        questions=questions,
        privacy_url=resolved_privacy_url,
        privacy_text=privacy_text or "Privacy Policy",
        follow_up_url=follow_up_url,
        locale=locale or "en_US",
        preferred_token=token,
    )


def _build_welcome_message(greeting: str, buttons: list, destination: str) -> dict:
    """
    构建 FB page_welcome_message 格式（放在 link_data/video_data 内部）。
    使用 VISUAL_EDITOR + ice_breakers 格式（FB Chat Builder 标准格式）。
    destination: MESSENGER | WHATSAPP
    """
    ice_breakers = []
    for btn in buttons[:4]:  # FB 最多 4 个 ice_breaker
        title = btn.get("text", "了解详情")[:80]  # 标题最多 80 字符
        response = btn.get("response", btn.get("payload", title))[:300]  # 回复最多 300 字符
        ice_breakers.append({"title": title, "response": response})

    welcome = {
        "type": "VISUAL_EDITOR",
        "version": 2,
        "landing_screen_type": "welcome_message",
        "media_type": "text",
        "text_format": {
            "customer_action_type": "ice_breakers",
            "message": {
                "ice_breakers": ice_breakers,
                "quick_replies": [],
                "text": greeting[:300]  # 欢迎语最多 300 字符
            }
        },
        "user_edit": False,
        "surface": "visual_editor_new"
    }
    return welcome

class MsgTemplateBody(BaseModel):
    name: str
    greeting: str = "你好！感谢你的关注，有什么可以帮到你？"
    buttons: list = [
        {"text": "了解产品详情", "payload": "了解产品详情"},
        {"text": "如何购买", "payload": "如何购买"},
        {"text": "有优惠吗", "payload": "有优惠吗"}
    ]
    destination: str = "MESSENGER"  # MESSENGER | WHATSAPP
    note: Optional[str] = None


@router.get("/message")
def list_msg_templates(user=Depends(get_current_user)):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM msg_templates ORDER BY updated_at DESC"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["buttons"] = json.loads(d["buttons"])
        except Exception:
            d["buttons"] = []
        result.append(d)
    return result


@router.post("/message")
def create_msg_template(body: MsgTemplateBody, user=Depends(get_current_user)):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO msg_templates (name, greeting, buttons, destination, note, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?)""",
        (body.name, body.greeting, json.dumps(body.buttons, ensure_ascii=False),
         body.destination, body.note, now, now)
    )
    conn.commit()
    tid = cur.lastrowid
    conn.close()
    return {"id": tid, "message": "消息模板创建成功"}


@router.get("/message/{tid}")
def get_msg_template(tid: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = conn.execute("SELECT * FROM msg_templates WHERE id=?", (tid,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "模板不存在")
    d = dict(row)
    try:
        d["buttons"] = json.loads(d["buttons"])
    except Exception:
        d["buttons"] = []
    return d


@router.put("/message/{tid}")
def update_msg_template(tid: int, body: MsgTemplateBody, user=Depends(get_current_user)):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    conn.execute(
        """UPDATE msg_templates SET name=?, greeting=?, buttons=?, destination=?, note=?, updated_at=?
           WHERE id=?""",
        (body.name, body.greeting, json.dumps(body.buttons, ensure_ascii=False),
         body.destination, body.note, now, tid)
    )
    conn.commit()
    conn.close()
    return {"message": "消息模板已更新"}


@router.delete("/message/{tid}")
def delete_msg_template(tid: int, user=Depends(get_current_user)):
    conn = get_conn()
    conn.execute("DELETE FROM msg_templates WHERE id=?", (tid,))
    conn.commit()
    conn.close()
    return {"message": "消息模板已删除"}


# ─────────────────────────────────────────────────────────────────────────────
# 表单模板 CRUD
# ─────────────────────────────────────────────────────────────────────────────

class LeadFormTemplateBody(BaseModel):
    name: str
    headline: str = "立即了解详情"
    description: Optional[str] = None
    questions: list = [
        {"type": "FULL_NAME", "key": "FULL_NAME", "label": "姓名"},
        {"type": "EMAIL", "key": "EMAIL", "label": "邮箱"},
        {"type": "PHONE", "key": "PHONE", "label": "电话"}
    ]
    privacy_url: str = "https://www.facebook.com"
    privacy_text: str = "隐私政策"
    thank_you_title: str = "感谢您的提交！"
    thank_you_body: str = "我们会尽快与您联系。"
    locale: str = "zh_CN"
    note: Optional[str] = None


@router.get("/lead-form")
def list_lead_form_templates(user=Depends(get_current_user)):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM lead_form_templates ORDER BY updated_at DESC"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["questions"] = _normalize_lead_form_questions(json.loads(d["questions"]))
        except Exception:
            d["questions"] = []
        result.append(d)
    return result


@router.post("/lead-form")
def create_lead_form_template(body: LeadFormTemplateBody, user=Depends(get_current_user)):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    normalized_questions = _normalize_lead_form_questions(body.questions)
    if not normalized_questions:
        raise HTTPException(400, "请至少保留一个有效的表单字段")
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO lead_form_templates
           (name, headline, description, questions, privacy_url, privacy_text,
            thank_you_title, thank_you_body, locale, note, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (body.name, body.headline, body.description,
         json.dumps(normalized_questions, ensure_ascii=False),
         body.privacy_url, body.privacy_text,
         body.thank_you_title, body.thank_you_body,
         body.locale, body.note, now, now)
    )
    conn.commit()
    tid = cur.lastrowid
    conn.close()
    return {"id": tid, "message": "表单模板创建成功"}


@router.get("/lead-form/{tid}")
def get_lead_form_template(tid: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = conn.execute("SELECT * FROM lead_form_templates WHERE id=?", (tid,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "模板不存在")
    d = dict(row)
    try:
        d["questions"] = _normalize_lead_form_questions(json.loads(d["questions"]))
    except Exception:
        d["questions"] = []
    return d


@router.put("/lead-form/{tid}")
def update_lead_form_template(tid: int, body: LeadFormTemplateBody, user=Depends(get_current_user)):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    normalized_questions = _normalize_lead_form_questions(body.questions)
    if not normalized_questions:
        raise HTTPException(400, "请至少保留一个有效的表单字段")
    conn = get_conn()
    conn.execute(
        """UPDATE lead_form_templates
           SET name=?, headline=?, description=?, questions=?, privacy_url=?, privacy_text=?,
               thank_you_title=?, thank_you_body=?, locale=?, note=?, updated_at=?
           WHERE id=?""",
        (body.name, body.headline, body.description,
         json.dumps(normalized_questions, ensure_ascii=False),
         body.privacy_url, body.privacy_text,
         body.thank_you_title, body.thank_you_body,
         body.locale, body.note, now, tid)
    )
    conn.commit()
    conn.close()
    return {"message": "表单模板已更新"}


@router.delete("/lead-form/{tid}")
def delete_lead_form_template(tid: int, user=Depends(get_current_user)):
    conn = get_conn()
    conn.execute("DELETE FROM lead_form_templates WHERE id=?", (tid,))
    conn.execute("DELETE FROM page_lead_forms WHERE template_id=?", (tid,))
    conn.commit()
    conn.close()
    return {"message": "表单模板已删除"}


# ─────────────────────────────────────────────────────────────────────────────
# 在主页上创建 Lead Form（铺广告时调用，带缓存避免重复创建）
# ─────────────────────────────────────────────────────────────────────────────

def create_lead_form_for_page(
    page_id: str,
    template_id: int,
    token: str = "",
    follow_up_url: str = "",
) -> Optional[str]:
    """
    根据模板在指定主页上创建 Lead Form，返回 fb_form_id。
    - 若已创建过（page_lead_forms 缓存），直接返回缓存的 fb_form_id
    - token: 可选，传入操作号 token 作为备用（优先使用 Page Token）
    - follow_up_url: 可选，提交后跳转链接；若未传则回退到全局落地页或主页链接
    """
    conn = get_conn()

    # 检查缓存
    cached = conn.execute(
        "SELECT fb_form_id FROM page_lead_forms WHERE page_id=? AND template_id=?",
        (page_id, template_id)
    ).fetchone()
    if cached:
        conn.close()
        logger.info(f"[LeadForm] 命中缓存: page={page_id} template={template_id} form={cached['fb_form_id']}")
        return cached["fb_form_id"]

    # 读取模板
    tpl = conn.execute(
        "SELECT * FROM lead_form_templates WHERE id=?", (template_id,)
    ).fetchone()
    conn.close()

    if not tpl:
        logger.error(f"[LeadForm] 模板不存在: template_id={template_id}")
        raise LeadFormCreateError("表单模板不存在，请刷新后重试")

    try:
        questions = json.loads(tpl["questions"])
    except Exception:
        questions = []
    form_name = f"[AutoPilot] {tpl['name']} {datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    fb_form_id = _post_lead_form(
        page_id,
        form_name=form_name,
        questions=questions,
        privacy_url=tpl["privacy_url"],
        privacy_text=tpl["privacy_text"] or "隐私政策",
        follow_up_url=follow_up_url,
        locale=tpl["locale"] or "zh_CN",
        preferred_token=token,
    )
    conn2 = get_conn()
    conn2.execute(
        "INSERT OR REPLACE INTO page_lead_forms (page_id, template_id, fb_form_id, created_at) VALUES (?,?,?,?)",
        (page_id, template_id, fb_form_id, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn2.commit()
    conn2.close()
    logger.info(f"[LeadForm] 创建成功: page={page_id} form={fb_form_id}")
    return fb_form_id


# ─────────────────────────────────────────────────────────────────────────────
# 获取消息模板的 FB 格式（供 autopilot_engine 调用）
# ─────────────────────────────────────────────────────────────────────────────

def get_msg_template_fb_format(template_id: int) -> Optional[dict]:
    """
    根据模板 ID 返回 FB messenger_welcome_message 格式的 dict。
    供 autopilot_engine._create_ad() 直接使用。
    """
    conn = get_conn()
    tpl = conn.execute(
        "SELECT * FROM msg_templates WHERE id=?", (template_id,)
    ).fetchone()
    conn.close()

    if not tpl:
        return None

    try:
        buttons = json.loads(tpl["buttons"])
    except Exception:
        buttons = []

    return _build_welcome_message(tpl["greeting"], buttons, tpl["destination"])


# ─────────────────────────────────────────────────────────────────────────────
# 预览接口（前端展示用）
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/message/{tid}/preview")
def preview_msg_template(tid: int, user=Depends(get_current_user)):
    """返回该消息模板的 FB 格式预览"""
    fb_fmt = get_msg_template_fb_format(tid)
    if not fb_fmt:
        raise HTTPException(404, "模板不存在")
    return {"fb_format": fb_fmt}


@router.post("/lead-form/{tid}/create-on-page")
def create_form_on_page(tid: int, page_id: str, user=Depends(get_current_user)):
    """
    手动触发：在指定主页上创建 Lead Form。
    返回 fb_form_id 或错误信息。
    """
    try:
        fb_form_id = create_lead_form_for_page(page_id, tid)
    except LeadFormCreateError as exc:
        raise HTTPException(400, str(exc))
    return {"fb_form_id": fb_form_id, "message": "Lead Form 创建成功"}
