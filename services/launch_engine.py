"""
Mira v3.0 全自动铺广告执行引擎 (AutoPilot Engine)
────────────────────────────────────────────────
职责：
  1. 从 auto_campaigns 表读取 pending 任务
  2. 根据素材的 AI 文案 + 受众兴趣词，生成"笛卡尔积"广告矩阵
  3. 使用 TokenManager 获取操作号 Token
  4. 调用 Facebook Graph API 批量创建：
       Campaign → AdSet（每组受众一个）→ Ad（每组文案一条）
  5. 将创建结果写回 auto_campaign_ads 表
  6. 更新 auto_campaigns 状态

设计原则：
  - 非侵入式：不修改任何现有巡检逻辑，广告创建后由现有 guard_engine 接管
  - 操作号隔离：全程使用 ACTION_CREATE Token，操作号全灭则拒绝执行
  - 幂等安全：每次执行前检查 fb_campaign_id 是否已存在，避免重复建营
  - 失败隔离：单个 AdSet/Ad 创建失败不影响其他组继续执行
"""

import base64
import json
import logging
import mimetypes
import re
import secrets
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlparse

from core.database import get_conn
from services.execution_safety import note_write_failure, wait_for_write_slot
from services.token_manager import (
    ACTION_CREATE,
    cooldown_token_by_plain,
    get_exec_token_candidates,
    get_matrix_id_for_account,
    suspend_token_by_plain,
    wait_for_token_slot_by_plain,
)
from services.landing_link_resolver import resolve_account_form_link, resolve_account_landing_link
from services.local_executor import run_local_graph_task

logger = logging.getLogger("mira.autopilot")

FB_API_BASE = "https://graph.facebook.com/v25.0"
FB_API_VERSION = "v25.0"
ACCOUNT_STATUS_LABELS = {
    1: "active",
    2: "disabled_or_reclaimed",
    3: "payment_failed",
    7: "policy_restricted",
    9: "closed",
    100: "pending_review",
    101: "under_review",
}
LANGUAGE_LABELS = {
    "en": "English",
    "es": "Spanish",
    "pt": "Portuguese",
    "fr": "French",
    "de": "German",
    "ar": "Arabic",
    "ja": "Japanese",
    "ko": "Korean",
    "id": "Indonesian",
    "th": "Thai",
    "vi": "Vietnamese",
    "tr": "Turkish",
    "zh": "Simplified Chinese",
    "zh-tw": "Traditional Chinese",
}
COUNTRY_LANGUAGE_MAP = {
    "US": "en", "GB": "en", "CA": "en", "AU": "en", "NZ": "en", "IE": "en", "IN": "en",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es", "PE": "es", "CL": "es", "VE": "es",
    "BR": "pt", "PT": "pt",
    "FR": "fr", "BE": "fr",
    "DE": "de", "AT": "de",
    "AE": "ar", "SA": "ar", "EG": "ar", "KW": "ar", "QA": "ar",
    "JP": "ja", "KR": "ko",
    "ID": "id", "MY": "id",
    "TH": "th", "VN": "vi", "TR": "tr",
    "CN": "zh", "SG": "zh", "TW": "zh-tw", "HK": "zh-tw",
}
TRANSIENT_FB_ERROR_CODES = {1, 2, 4, 17, 32, 341, 613}
RATE_LIMIT_FB_ERROR_CODES = {4, 17, 32, 341, 613}
PHONE_FIRST_COUNTRIES = {
    "AE", "AR", "BR", "CL", "CO", "EG", "ID", "IN", "KW", "MX", "MY",
    "PE", "PH", "QA", "SA", "TH", "TR", "VN",
}
MESSAGE_OBJECTIVES = {"OUTCOME_MESSAGES", "OUTCOME_MESSAGING", "MESSAGES"}
MESSAGE_GOALS = {
    "conversations",
    "messaging_purchase_conversion",
    "messaging_appointment_conversion",
    "messaging_leads",
}


def _tw_page_not_blocked_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"COALESCE({prefix}page_is_published, 1) != 0 "
        f"AND COALESCE({prefix}page_can_advertise, 1) != 0 "
        f"AND COALESCE({prefix}page_status, 'ok') NOT IN ('restricted', 'unpublished')"
    )


def _tw_page_block_reason(row) -> str:
    if not row:
        return ""
    reasons = []
    if row["page_is_published"] == 0:
        reasons.append("主页未发布")
    if row["page_can_advertise"] == 0:
        reasons.append("不可投放")
    status = str(row["page_status"] or "").strip().lower()
    if status in {"restricted", "unpublished"}:
        reasons.append(str(row["page_status_hint"] or "").strip() or f"状态={status}")
    return " / ".join(dict.fromkeys([r for r in reasons if r]))
LANDING_REQUIRED_OBJECTIVES = {"OUTCOME_TRAFFIC", "OUTCOME_SALES", "OUTCOME_ENGAGEMENT", "OUTCOME_LEADS"}
REGULATED_IDENTITY_COUNTRIES = {"TW", "HK", "SG"}

# 语言代码 → Facebook Locale ID (用于 AdSet 语言定向)
LANGUAGE_TO_FB_LOCALE = {
    "en": [6],       # en_US
    "es": [12],      # es_ES
    "pt": [14],      # pt_BR
    "fr": [7],       # fr_FR
    "de": [4],       # de_DE
    "ar": [3],       # ar_AR
    "zh": [5],       # zh_CN
    "zh-tw": [16],   # zh_TW
    "zh-hk": [18],   # zh_HK
    "ja": [8],       # ja_JP
    "ko": [9],       # ko_KR
    "id": [19],      # id_ID
    "th": [21],      # th_TH
    "vi": [22],      # vi_VN
    "tr": [20],      # tr_TR
}
REGIONAL_REGULATION_CONFIG = {
    "TW": ("TAIWAN_UNIVERSAL", "taiwan_universal_beneficiary", "taiwan_universal_payer"),
    "HK": ("TAIWAN_UNIVERSAL", "taiwan_universal_beneficiary", "taiwan_universal_payer"),
    "SG": ("SINGAPORE_UNIVERSAL", "singapore_universal_beneficiary", "singapore_universal_payer"),
}
TAIWAN_UNIVERSAL_COUNTRIES = {"TW", "HK"}
USD_VALUE_BY_CURRENCY = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067,
    "CNY": 0.138, "HKD": 0.128, "TWD": 0.031, "SGD": 0.74,
    "AUD": 0.65, "CAD": 0.74, "BRL": 0.20, "MXN": 0.058,
    "CLP": 0.0011, "COP": 0.00025, "PEN": 0.27, "ARS": 0.001,
    "THB": 0.028, "VND": 0.000040, "IDR": 0.000063, "PHP": 0.017,
    "MYR": 0.21, "INR": 0.012, "TRY": 0.031, "ZAR": 0.053,
    "BDT": 0.0091, "PKR": 0.0036, "LKR": 0.0031, "NPR": 0.0075,
    "KRW": 0.00072, "CHF": 1.12, "NZD": 0.60, "SEK": 0.096,
    "NOK": 0.093, "DKK": 0.145, "PLN": 0.25, "CZK": 0.044,
    "HUF": 0.0028, "RON": 0.22, "BGN": 0.55, "AED": 0.272,
    "SAR": 0.267, "QAR": 0.275, "KWD": 3.26, "BHD": 2.65,
    "OMR": 2.60, "JOD": 1.41, "EGP": 0.021, "MAD": 0.099,
    "TND": 0.32, "GHS": 0.067, "NGN": 0.00065, "KES": 0.0077,
    "UAH": 0.027, "KZT": 0.0022, "GEL": 0.37,
}
NO_DECIMAL_CURRENCIES = {"JPY", "KRW", "IDR", "VND", "CLP", "COP", "HUF", "PYG", "UGX", "TZS"}


def _usd_to_account_currency(amount_usd: float, currency: str) -> float:
    currency = str(currency or "USD").upper()
    amount = float(amount_usd or 0)
    if currency == "USD":
        return amount
    rate = float(USD_VALUE_BY_CURRENCY.get(currency, 1.0) or 1.0)
    if rate <= 0:
        return amount
    return round(amount / rate, 2)


def _fb_money_units(amount: float, currency: str) -> int:
    currency = str(currency or "USD").upper()
    if currency in NO_DECIMAL_CURRENCIES:
        return max(1, int(round(float(amount or 0))))
    return max(1, int(round(float(amount or 0) * 100)))


def _normalize_verified_identity_value(value) -> str:
    cleaned = str(value or "").strip()
    if not cleaned or cleaned.lower() in {"none", "null", "undefined"}:
        return ""
    match = re.search(
        r"(?:編號|编号|編碼|编码|認證編號|认证编号|Verified\s*ID|Identity\s*ID|ID)\s*[：:\s]*([0-9]{10,20})",
        cleaned,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1)
    if re.fullmatch(r"\d{10,20}", cleaned):
        return cleaned
    digit_runs = re.findall(r"(?<!\d)([0-9]{10,20})(?!\d)", cleaned)
    return digit_runs[0] if len(digit_runs) == 1 else ""


def _normalize_campaign_objective(value: str = "") -> str:
    objective = str(value or "OUTCOME_SALES").strip().upper()
    if objective in MESSAGE_OBJECTIVES:
        return "OUTCOME_MESSAGES"
    return objective or "OUTCOME_SALES"


def _normalize_campaign_goal_fields(objective: str = "", conversion_goal: str = "") -> tuple[str, str]:
    objective_norm = _normalize_campaign_objective(objective)
    goal_norm = str(conversion_goal or "").strip().lower()
    if objective_norm == "OUTCOME_MESSAGES" and not goal_norm:
        goal_norm = "conversations"
    if objective_norm == "OUTCOME_LEADS" and not goal_norm:
        goal_norm = "lead_generation"
    return objective_norm, goal_norm


def _get_campaign_goal_meta(objective: str = "", conversion_goal: str = "") -> dict:
    objective_norm, goal_norm = _normalize_campaign_goal_fields(objective, conversion_goal)
    is_message = objective_norm in MESSAGE_OBJECTIVES or goal_norm in MESSAGE_GOALS
    is_lead = goal_norm == "lead_generation"
    landing_required = (
        objective_norm in LANDING_REQUIRED_OBJECTIVES
        and not is_message
        and not is_lead
        and goal_norm != "page_likes"
    )
    return {
        "objective": objective_norm,
        "goal": goal_norm,
        "is_message": is_message,
        "is_lead": is_lead,
        "landing_required": landing_required,
    }


class AutoPilotEngine:
    """全自动铺广告执行引擎"""

    def __init__(self):
        self.conn = None
        self._runtime_lead_form_cache = {}
        self._runtime_lead_form_error_cache = {}
        self._active_exec_candidate = None
        self._active_act_id = ""

    def _normalize_language_code(self, value: str = "") -> str:
        lang = str(value or "").strip().lower().replace("_", "-")
        if lang in ("zh-cn", "cn", "zh-hans"):
            return "zh"
        if lang in ("zh-tw", "zh-hk", "tw", "hk", "zh-hant"):
            return "zh-tw"
        if "-" in lang:
            lang = lang.split("-", 1)[0]
        return lang or "en"

    def _parse_country_codes(self, raw_value) -> list[str]:
        if raw_value is None:
            return []
        if isinstance(raw_value, list):
            values = raw_value
        else:
            raw_text = str(raw_value or "").strip()
            if not raw_text:
                return []
            try:
                parsed = json.loads(raw_text)
                values = parsed if isinstance(parsed, list) else [raw_text]
            except Exception:
                values = raw_text.split(",")
        return [str(v).strip().upper() for v in values if str(v).strip()]

    def _resolve_language_context(
        self,
        ad_language: str = "",
        target_countries=None,
        asset_info: Optional[dict] = None,
    ) -> dict:
        countries = self._parse_country_codes(target_countries)
        if not countries and asset_info:
            countries = self._parse_country_codes(asset_info.get("target_countries"))

        lang = self._normalize_language_code(ad_language)
        if lang in ("", "auto"):
            lang = ""
        if not lang:
            for country in countries:
                mapped = COUNTRY_LANGUAGE_MAP.get(country)
                if mapped:
                    lang = mapped
                    break
        lang = self._normalize_language_code(lang or "en")

        locale_map = {
            "en": "en_GB" if countries[:1] == ["GB"] else "en_US",
            "es": "es_ES",
            "pt": "pt_BR" if countries[:1] == ["BR"] else "pt_PT",
            "fr": "fr_FR",
            "de": "de_DE",
            "ar": "ar_AR",
            "ja": "ja_JP",
            "ko": "ko_KR",
            "id": "id_ID",
            "th": "th_TH",
            "vi": "vi_VN",
            "tr": "tr_TR",
            "zh": "zh_CN",
            "zh-tw": "zh_TW",
        }
        return {
            "language": lang,
            "label": LANGUAGE_LABELS.get(lang, lang or "English"),
            "locale": locale_map.get(lang, "en_US"),
            "countries": countries,
        }

    def _contains_cjk(self, text: str) -> bool:
        return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))

    def _localized_lead_form_fallback(self, ctx: dict) -> dict:
        lang = ctx["language"]
        countries = ctx.get("countries") or []
        contact_field = "PHONE" if any(country in PHONE_FIRST_COUNTRIES for country in countries) else "EMAIL"
        text_map = {
            "en": ("Get More Information", "What would you like help with first?", "Privacy Policy"),
            "es": ("Obtén más información", "¿Qué te gustaría saber primero?", "Política de privacidad"),
            "pt": ("Receba mais informações", "Com o que você quer ajuda primeiro?", "Política de Privacidade"),
            "fr": ("Obtenir plus d'informations", "Sur quoi souhaitez-vous être aidé en premier ?", "Politique de confidentialité"),
            "de": ("Mehr Informationen erhalten", "Wobei möchten Sie zuerst Unterstützung?", "Datenschutzrichtlinie"),
            "ar": ("احصل على مزيد من المعلومات", "ما أول شيء ترغب في معرفته؟", "سياسة الخصوصية"),
            "ja": ("詳しい情報を受け取る", "まず知りたいことは何ですか？", "プライバシーポリシー"),
            "ko": ("자세한 정보 받기", "먼저 알고 싶은 내용이 무엇인가요?", "개인정보 처리방침"),
            "id": ("Dapatkan Info Lebih Lanjut", "Apa yang paling ingin Anda ketahui terlebih dahulu?", "Kebijakan Privasi"),
            "th": ("รับข้อมูลเพิ่มเติม", "คุณอยากทราบเรื่องใดก่อนมากที่สุด?", "นโยบายความเป็นส่วนตัว"),
            "vi": ("Nhận thêm thông tin", "Bạn muốn được hỗ trợ điều gì trước tiên?", "Chính sách quyền riêng tư"),
            "tr": ("Daha Fazla Bilgi Al", "Önce hangi konuda destek almak istersiniz?", "Gizlilik Politikası"),
            "zh": ("获取更多信息", "你最想先了解什么？", "隐私政策"),
            "zh-tw": ("取得更多資訊", "你最想先了解什麼？", "隱私權政策"),
        }
        form_title, qualifying_question, privacy_text = text_map.get(lang, text_map["en"])
        _option_fb = {
            "en": ("Yes, I'm interested", "Tell me more"),
            "es": ("S\u00ed, me interesa", "Cu\u00e9ntame m\u00e1s"),
            "zh": ("\u662f\u7684\uff0c\u6211\u611f\u5174\u8da3", "\u544a\u8bc9\u6211\u66f4\u591a"),
            "zh-tw": ("\u662f\u7684\uff0c\u6211\u611f\u8208\u8da3", "\u544a\u8a34\u6211\u66f4\u591a"),
            "ja": ("\u306f\u3044\u3001\u8208\u5473\u304c\u3042\u308a\u307e\u3059", "\u3082\u3063\u3068\u8a73\u3057\u304f"),
            "ko": ("\ub124, \uad00\uc2ec\uc788\uc2b5\ub2c8\ub2e4", "\ub354 \uc54c\ub824\uc8fc\uc138\uc694"),
            "th": ("\u0e43\u0e0a\u0e48 \u0e09\u0e31\u0e19\u0e2a\u0e19\u0e43\u0e08", "\u0e1a\u0e2d\u0e01\u0e40\u0e1e\u0e34\u0e48\u0e21\u0e40\u0e15\u0e34\u0e21"),
            "vi": ("C\u00f3, t\u00f4i quan t\u00e2m", "Cho t\u00f4i bi\u1ebft th\u00eam"),
        }
        _options = _option_fb.get(lang, _option_fb["en"])
        _thank_map = {
            "en": ("You're all set!", "Add our contact below to receive your free report now."),
            "zh": ("\u5df2\u63d0\u4ea4\u6210\u529f\uff01", "\u6dfb\u52a0\u4e0b\u65b9\u8054\u7cfb\u65b9\u5f0f\uff0c\u7acb\u5373\u83b7\u53d6\u514d\u8d39\u62a5\u544a\u3002"),
            "zh-tw": ("\u5df2\u63d0\u4ea4\u6210\u529f\uff01", "\u65b0\u589e\u4e0b\u65b9\u806f\u7d61\u65b9\u5f0f\uff0c\u7acb\u5373\u53d6\u5f97\u514d\u8cbb\u5831\u544a\u3002"),
            "ja": ("\u53d7\u4ed8\u5b8c\u4e86\uff01", "\u4e0b\u8a18\u306e\u9023\u7d61\u5148\u3092\u8ffd\u52a0\u3057\u3066\u3001\u7121\u6599\u30ec\u30dd\u30fc\u30c8\u3092\u53d7\u3051\u53d6\u3063\u3066\u304f\u3060\u3055\u3044\u3002"),
            "ko": ("\uc81c\ucd9c \uc644\ub8cc!", "\uc544\ub798 \uc5f0\ub77d\ucc98\ub97c \ucd94\uac00\ud558\uc5ec \ubb34\ub8cc \ub9ac\ud3ec\ud2b8\ub97c \ubc1b\uc73c\uc138\uc694."),
            "th": ("\u0e2a\u0e48\u0e07\u0e41\u0e25\u0e49\u0e27!", "\u0e40\u0e1e\u0e34\u0e48\u0e21\u0e02\u0e49\u0e2d\u0e21\u0e39\u0e25\u0e15\u0e34\u0e14\u0e15\u0e48\u0e2d\u0e14\u0e49\u0e32\u0e19\u0e25\u0e48\u0e32\u0e07\u0e40\u0e1e\u0e37\u0e48\u0e2d\u0e23\u0e31\u0e1a\u0e23\u0e32\u0e22\u0e07\u0e32\u0e19\u0e1f\u0e23\u0e35\u0e17\u0e31\u0e19\u0e17\u0e35"),
            "vi": ("\u0110\u00e3 g\u1eedi!", "Th\u00eam li\u00ean h\u1ec7 b\u00ean d\u01b0\u1edbi \u0111\u1ec3 nh\u1eadn b\u00e1o c\u00e1o mi\u1ec5n ph\u00ed ngay."),
        }
        _thank = _thank_map.get(lang, _thank_map["en"])
        _btn_map = {
            "en": "Submit", "es": "Enviar", "zh": "\u63d0\u4ea4", "zh-tw": "\u63d0\u4ea4",
            "ja": "\u9001\u4fe1", "ko": "\uc81c\ucd9c", "th": "\u0e2a\u0e48\u0e07",
            "vi": "G\u1eedi", "id": "Kirim", "ar": "\u0625\u0631\u0633\u0627\u0644",
            "fr": "Envoyer", "de": "Absenden", "pt": "Enviar", "tr": "G\u00f6nder",
        }
        _btn = _btn_map.get(lang, _btn_map["en"])
        return {
            "form_title": form_title,
            "qualifying_question": qualifying_question,
            "privacy_text": privacy_text,
            "contact_field": contact_field,
            "option_a": _options[0],
            "option_b": _options[1],
            "thank_you_title": _thank[0],
            "thank_you_body": _thank[1],
            "button_text": _btn,
        }

    def _default_msg_template(self, ctx: dict, headline: str) -> dict:
        lang = ctx["language"]
        fallback_map = {
            "en": ("Thanks for reaching out. What would you like to know first?", [("More details", "I’d like more details"), ("Pricing", "Tell me about pricing"), ("How it works", "How does it work?")]),
            "es": ("Gracias por escribirnos. ¿Qué te gustaría saber primero?", [("Más detalles", "Quiero más detalles"), ("Precios", "Cuéntame sobre el precio"), ("Cómo funciona", "¿Cómo funciona?")]),
            "pt": ("Obrigado pelo contato. O que você gostaria de saber primeiro?", [("Mais detalhes", "Quero mais detalhes"), ("Preço", "Fale sobre o preço"), ("Como funciona", "Como funciona?")]),
            "zh": ("感谢留言，你最想先了解什么？", [("了解详情", "我想先了解详情"), ("价格信息", "我想了解价格信息"), ("如何开始", "我想知道如何开始")]),
            "zh-tw": ("感謝留言，你最想先了解什麼？", [("了解詳情", "我想先了解詳情"), ("價格資訊", "我想了解價格資訊"), ("如何開始", "我想知道如何開始")]),
        }
        welcome_text, pairs = fallback_map.get(lang, fallback_map["en"])
        if headline:
            welcome_text = f"{headline[:80]} · {welcome_text}"
        return {
            "welcome_text": welcome_text[:280],
            "ice_breakers": [
                {"title": title[:80], "response": response[:280]}
                for title, response in pairs
            ],
        }

    def _is_local_candidate(self, candidate: dict | None = None) -> bool:
        candidate = candidate if candidate is not None else self._active_exec_candidate
        return bool(candidate and (candidate.get("local_executor") or candidate.get("source") == "local_token"))

    def _candidate_key(self, candidate: dict) -> str:
        if self._is_local_candidate(candidate):
            return f"local:{candidate.get('node_id') or candidate.get('token_id') or candidate.get('label')}"
        plain = str(candidate.get("token_plain") or candidate.get("token") or "").strip()
        return plain or str(candidate.get("token_id") or candidate.get("label") or id(candidate))

    def _candidate_token(self, candidate: dict | None, fallback: str = "") -> str:
        if not candidate:
            return fallback or ""
        if self._is_local_candidate(candidate):
            return fallback or ""
        return str(candidate.get("token_plain") or candidate.get("token") or fallback or "").strip()

    def _local_task_timeout(self, default_seconds: int = 90) -> int:
        try:
            return max(10, min(int(default_seconds), 360))
        except Exception:
            return default_seconds

    def _run_local_graph_task(self, task_type: str, path: str, params: dict | None = None, timeout_seconds: int = 90) -> dict:
        candidate = self._active_exec_candidate
        if not self._is_local_candidate(candidate):
            raise Exception("current execution candidate is not local")
        act_id = self._active_act_id or ""
        payload = dict(params or {})
        payload["path"] = str(path or "").strip().lstrip("/")
        payload.setdefault("_timeout_sec", self._local_task_timeout(timeout_seconds))
        return run_local_graph_task(
            candidate,
            task_type,
            act_id,
            payload,
            timeout_seconds=self._local_task_timeout(timeout_seconds),
            created_by_name="launch_engine",
        )

    def _should_try_next_token(self, err_msg: str) -> bool:
        lower = str(err_msg or "").lower()
        if self._is_app_development_mode_error(err_msg):
            return True
        if any(
            token in lower
            for token in (
                "token", "session", "oauth", "permission", "access", "auth",
                "invalid", "expired", "rate limit", "request limit",
                "temporarily unavailable", "retry your request later",
                "unexpected error", "code=1", "code=2", "code=4",
                "code=17", "code=32", "code=341", "code=613",
                "cooldown", "cooling",
            )
        ):
            return True
        return False

    def _is_app_development_mode_error(self, err_msg: str) -> bool:
        raw = str(err_msg or "")
        lower = raw.lower()
        return (
            "1885183" in raw
            or ("ads creative post" in lower and "development mode" in lower)
            or ("meta app" in lower and "development mode" in lower)
        )

    def _format_app_development_mode_error(self, err_msg: str) -> str:
        return (
            "Meta App 仍处于 Development mode，Facebook 不允许用该 App 创建可投放广告。"
            "这不是受众、预算或落地页问题；必须在 Meta for Developers 将当前 OAuth App 切到 Live/Public，"
            "或在 Mira 重新配置一个已 Live 的 Meta App 并重新授权操作号后再投放。"
            f"原始错误：{err_msg}"
        )

    def _delete_fb_campaign_best_effort(self, fb_campaign_id: str, token: str, candidate: dict | None = None) -> bool:
        fb_campaign_id = str(fb_campaign_id or "").strip()
        if not fb_campaign_id:
            return False
        prev_candidate = self._active_exec_candidate
        try:
            if self._is_local_candidate(candidate):
                self._active_exec_candidate = candidate
                data = self._run_local_graph_task(
                    "graph_delete",
                    fb_campaign_id,
                    {"_timeout_sec": 30},
                    timeout_seconds=30,
                )
                ok = "error" not in data and data.get("success") is not False
                if not ok:
                    logger.warning("[AutoPilot] 删除 FB Campaign 失败: %s", json.dumps(data, ensure_ascii=False)[:300])
                return bool(ok)
            import requests as _req
            resp = _req.delete(
                f"{FB_API_BASE}/{fb_campaign_id}",
                params={"access_token": token},
                timeout=10,
            )
            if not resp.ok:
                logger.warning("[AutoPilot] 删除 FB Campaign 失败: %s", resp.text[:300])
            return bool(resp.ok)
        except Exception as exc:
            logger.warning("[AutoPilot] 删除 FB Campaign 异常: %s", exc)
            return False
        finally:
            self._active_exec_candidate = prev_candidate

    def _run_with_token_fallback(self, token_candidates: list[dict], preferred_token: str, op_name: str, fn):
        if not token_candidates:
            raise Exception("当前账户没有可用的操作号 Token")

        preferred_token = (preferred_token or "").strip()
        ordered = []
        seen = set()
        if preferred_token:
            for candidate in token_candidates:
                plain = str(candidate.get("token_plain") or candidate.get("token") or "").strip()
                key = self._candidate_key(candidate)
                if plain and plain == preferred_token and key not in seen:
                    ordered.append(candidate)
                    seen.add(key)
        for candidate in token_candidates:
            key = self._candidate_key(candidate)
            if key and key not in seen:
                ordered.append(candidate)
                seen.add(key)

        last_error = None
        for idx, candidate in enumerate(ordered):
            token_plain = self._candidate_token(candidate, preferred_token)
            label = candidate.get("label") or candidate.get("alias") or f"token_{idx + 1}"
            prev_candidate = self._active_exec_candidate
            try:
                self._active_exec_candidate = candidate
                result = fn(token_plain, candidate)
                return result, candidate
            except Exception as exc:
                last_error = exc
                err_msg = str(exc)
                logger.warning(f"[AutoPilot] {op_name} 通过 {label} 失败: {err_msg}")
                if idx >= len(ordered) - 1 or not self._should_try_next_token(err_msg):
                    raise
                continue
            finally:
                self._active_exec_candidate = prev_candidate

        raise last_error or Exception(f"{op_name} 失败")

    def _probe_page_ad_permission(self, page_id: str, token: str, candidate: dict | None = None) -> tuple[bool, str, str]:
        """Return whether this CREATE token can advertise with the selected Page."""
        page_id = str(page_id or "").strip()
        if not page_id:
            return False, "未选择主页", ""
        if self._is_local_candidate(candidate):
            prev_candidate = self._active_exec_candidate
            try:
                self._active_exec_candidate = candidate
                data = self._run_local_graph_task(
                    "graph_get",
                    "me/accounts",
                    {"params": {"fields": "id,name,tasks,is_published", "limit": 200}, "_timeout_sec": 45},
                    timeout_seconds=45,
                )
                for page in data.get("data", []) or []:
                    if str(page.get("id")) != page_id:
                        continue
                    page_name = page.get("name") or page_id
                    if page.get("is_published") is False:
                        return False, f"Page {page_name} is unpublished", page_name
                    tasks = page.get("tasks") or []
                    if "ADVERTISE" not in tasks:
                        return False, f"Page {page_name} lacks ADVERTISE permission", page_name
                    return True, "", page_name
                return False, f"Local executor cannot see page {page_id}", ""
            except Exception as exc:
                return False, f"Page permission probe failed: {exc}", ""
            finally:
                self._active_exec_candidate = prev_candidate
        if not token:
            return False, "Token 为空", ""

        url = f"{FB_API_BASE}/me/accounts"
        params = {
            "access_token": token,
            "fields": "id,name,tasks,is_published",
            "limit": 200,
        }
        seen_next = set()
        try:
            for _ in range(20):
                resp = requests.get(url, params=params, timeout=15)
                data = resp.json()
                if "error" in data:
                    err = data.get("error") or {}
                    return False, err.get("message") or "主页权限读取失败", ""
                for page in data.get("data", []) or []:
                    if str(page.get("id")) != page_id:
                        continue
                    page_name = page.get("name") or page_id
                    if page.get("is_published") is False:
                        return False, f"主页 {page_name} 未发布", page_name
                    tasks = page.get("tasks") or []
                    if "ADVERTISE" not in tasks:
                        return False, f"主页 {page_name} 缺少 ADVERTISE 权限", page_name
                    return True, "", page_name
                next_url = data.get("paging", {}).get("next")
                if not next_url or next_url in seen_next:
                    break
                seen_next.add(next_url)
                url = next_url
                params = {}
        except Exception as exc:
            return False, f"主页权限探测失败: {exc}", ""

        return False, f"当前操作号看不到主页 {page_id}", ""

    def _filter_create_tokens_for_page(self, page_id: str, token_candidates: list[dict]) -> list[dict]:
        if not page_id:
            return token_candidates

        allowed = []
        failures = []
        for idx, candidate in enumerate(token_candidates or []):
            token_plain = candidate.get("token_plain") or candidate.get("token")
            label = candidate.get("label") or candidate.get("alias") or f"操作号{idx + 1}"
            ok, reason, page_name = self._probe_page_ad_permission(page_id, token_plain, candidate=candidate)
            if ok:
                item = dict(candidate)
                item["page_name"] = page_name
                allowed.append(item)
            else:
                failures.append(f"{label}: {reason}")

        if allowed:
            if failures:
                logger.warning(
                    "[AutoPilot] 主页 %s 过滤掉 %s 个无权限 CREATE Token: %s",
                    page_id,
                    len(failures),
                    "；".join(failures[:4]),
                )
            return allowed

        detail = "；".join(failures[:5]) if failures else "没有可用操作号"
        raise Exception(
            f"当前自动铺广告操作号没有主页 {page_id} 的投放权限。"
            "请在 BM 中把该主页的广告权限分配给 System User 或 Meta 官方授权操作号，"
            "或换一个操作号可投放的主页。检测结果："
            f"{detail}"
        )

    def _get_setting(self, key: str, default: str = "") -> str:
        try:
            conn = get_conn()
            row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            conn.close()
            return row["value"] if row else default
        except Exception:
            return default

    def _setting_enabled(self, key: str, default: str = "1") -> bool:
        return str(self._get_setting(key, default)).strip().lower() in ("1", "true", "yes", "on")

    def _campaign_bool(self, campaign: dict, key: str, default: bool) -> bool:
        if not campaign or key not in campaign or campaign.get(key) is None:
            return bool(default)
        raw = campaign.get(key)
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return int(raw) != 0
        return str(raw).strip().lower() in ("1", "true", "yes", "on")

    def _launch_ad_variant_count(self, headlines: list, bodies: list, budget_usd: float, one_ad_per_adset: bool) -> int:
        count = min(len(headlines), len(bodies), 3)
        if count <= 0:
            return 0
        if not one_ad_per_adset:
            return count
        try:
            min_budget_usd = float(self._get_setting("autopilot_min_adset_budget_usd", "3") or 3)
        except Exception:
            min_budget_usd = 3.0
        if min_budget_usd > 0 and budget_usd > 0:
            affordable_count = int(float(budget_usd) // min_budget_usd)
            if affordable_count > 0:
                count = min(count, affordable_count)
            else:
                count = 1
        return max(1, count)

    def _split_adset_budget(self, daily_budget: float, ad_count: int, one_ad_per_adset: bool) -> float:
        if one_ad_per_adset and ad_count > 1:
            return round(float(daily_budget) / float(ad_count), 2)
        return float(daily_budget)

    def _load_account(self, act_id: str) -> Optional[dict]:
        conn = get_conn()
        row = conn.execute(
            """
            SELECT act_id, name, enabled, account_status, page_id, pixel_id, currency,
                   beneficiary, payer, tw_advertiser_id, form_link, landing_url
            FROM accounts
            WHERE act_id=?
            """,
            (act_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def _get_account_launch_block_reason(self, account: Optional[dict]) -> Optional[str]:
        if not account:
            return "Account missing or not imported; AutoPilot launch blocked"

        enabled = account.get("enabled")
        if enabled is not None:
            try:
                if int(enabled) != 1:
                    return "Account disabled in Mira; AutoPilot launch blocked"
            except (TypeError, ValueError):
                if str(enabled).strip().lower() not in ("1", "true", "yes", "on"):
                    return "Account enabled flag invalid; AutoPilot launch blocked"

        raw_status = account.get("account_status")
        if raw_status in (None, ""):
            return None

        try:
            account_status = int(raw_status)
        except (TypeError, ValueError):
            return f"Account status invalid ({raw_status}); AutoPilot launch blocked"

        if account_status == 1:
            return None

        status_label = ACCOUNT_STATUS_LABELS.get(account_status, f"status_{account_status}")
        return f"Account status is {status_label}; AutoPilot launch blocked"

    def _format_fb_error(self, err: dict) -> str:
        if not isinstance(err, dict):
            return f"FB API Error: {err}"

        code = err.get("code")
        subcode = err.get("error_subcode")
        message = str(err.get("message") or "Unknown Facebook API error").strip()
        user_title = str(err.get("error_user_title") or "").strip()
        user_msg = str(err.get("error_user_msg") or "").strip()

        if subcode == 1815089:
            return (
                "当前主页尚未接受 Facebook Lead Generation Terms。"
                "请先以主页身份访问 https://www.facebook.com/ads/leadgen/tos 完成确认后再发布 Lead 广告。"
            )

        parts = []
        if user_title:
            parts.append(user_title)
        if user_msg and user_msg not in parts:
            parts.append(user_msg)
        if message and message not in parts:
            parts.append(message)
        if code not in (None, ""):
            parts.append(f"code={code}")
        if subcode not in (None, ""):
            parts.append(f"subcode={subcode}")
        if not parts:
            parts.append("Unknown Facebook API error")
        return "FB API Error: " + " | ".join(parts)

    def _fb_get(self, path: str, token: str, params: dict = None) -> dict:
        """GET 请求到 FB Graph API"""
        if self._is_local_candidate():
            data = self._run_local_graph_task(
                "graph_get",
                path,
                {"params": dict(params or {}), "_timeout_sec": 90},
                timeout_seconds=90,
            )
            if "error" in data:
                raise Exception(self._format_fb_error(data["error"]))
            return data
        p = dict(params or {})
        p["access_token"] = token
        resp = requests.get(f"{FB_API_BASE}/{path}", params=p, timeout=30)
        data = resp.json()
        if "error" in data:
            raise Exception(self._format_fb_error(data["error"]))
        return data

    def _fb_post(self, path: str, token: str, payload: dict) -> dict:
        """POST 请求到 FB Graph API"""
        if self._is_local_candidate():
            data = self._run_local_graph_task(
                "graph_post",
                path,
                {"data": dict(payload or {}), "_timeout_sec": 120},
                timeout_seconds=120,
            )
            if "error" in data:
                raise Exception(self._format_fb_error(data["error"]))
            return data
        base_payload = dict(payload or {})
        debug_payload = dict(base_payload)

        for attempt in range(1, 4):
            req_payload = dict(base_payload)
            req_payload["access_token"] = token
            try:
                wait_seconds = wait_for_write_slot(token, operation=f"launch:{path}")
                if wait_seconds > 0.2:
                    logger.info(
                        f"[AutoPilot] token request slot delayed {wait_seconds:.2f}s "
                        f"(path={path}) to smooth same-token bursts"
                    )
                resp = requests.post(f"{FB_API_BASE}/{path}", json=req_payload, timeout=30)
                data = resp.json()
            except Exception as req_err:
                req_msg = str(req_err).lower()
                if "cooldown" in req_msg or "cooling" in req_msg:
                    raise
                if attempt < 3:
                    delay = 1.5 * attempt
                    logger.warning(
                        f"[AutoPilot] FB POST 网络异常，{delay:.1f}s 后重试 "
                        f"(attempt={attempt}/3, path={path}): {req_err}"
                    )
                    time.sleep(delay)
                    continue
                raise

            if "error" not in data:
                return data

            err = data["error"] or {}
            logger.error(f"[AutoPilot] FB API Error on {path}: {err} | payload={debug_payload}")
            note_write_failure(token, data, operation=f"launch:{path}")

            # 自动检测并暂停需要认证的 Token（error_subcode=2859002）
            if err.get("error_subcode") == 2859002 or "certification" in str(err.get("error_user_title", "")).lower():
                logger.warning(f"[AutoPilot] 检测到 Token 需要 Facebook 非歧视政策认证，自动暂停该 Token")
                try:
                    suspend_token_by_plain(token, reason="certification_required")
                except Exception as se:
                    logger.error(f"[AutoPilot] 自动暂停 Token 失败: {se}")

            err_code = err.get("code")
            message_lower = f"{err.get('message', '')} {err.get('error_user_msg', '')}".lower()
            is_transient = (
                err_code in TRANSIENT_FB_ERROR_CODES
                or "retry your request later" in message_lower
                or "temporarily unavailable" in message_lower
                or "unexpected error" in message_lower
                or "request limit" in message_lower
                or "rate limit" in message_lower
            )

            if is_transient and attempt < 3:
                cooldown_seconds = 120.0 if err_code in RATE_LIMIT_FB_ERROR_CODES else 15.0
                try:
                    cooldown_token_by_plain(
                        token,
                        cooldown_seconds,
                        reason=f"fb_post_error:{path}",
                        error_code=err_code,
                    )
                except Exception as cooldown_err:
                    logger.warning(f"[AutoPilot] Token 冷却记录失败: {cooldown_err}")
                delay = min(8.0, 1.5 * attempt + 1.0)
                logger.warning(
                    f"[AutoPilot] FB API 瞬时错误，{delay:.1f}s 后重试 "
                    f"(attempt={attempt}/3, path={path}, code={err_code})"
                )
                time.sleep(delay)
                continue

            raise Exception(self._format_fb_error(err))

        raise Exception(f"FB API Error: POST {path} failed after retries")

    def _create_lead_form_for_page(
        self,
        page_id: str,
        form_title: str,
        questions: list,
        *,
        token: str = "",
        privacy_url: str = "",
        privacy_text: str = "Privacy Policy",
        follow_up_url: str = "",
        locale: str = "en_US",
        context_card: dict = None,
        thank_you_title: str = "",
        thank_you_body: str = "",
        button_text: str = "",
    ) -> str:
        if not self._is_local_candidate():
            from api.ad_templates import create_custom_lead_form_for_page
            return create_custom_lead_form_for_page(
                page_id,
                form_title,
                questions,
                token=token,
                privacy_url=privacy_url,
                privacy_text=privacy_text,
                follow_up_url=follow_up_url,
                locale=locale,
                context_card=context_card,
                thank_you_title=thank_you_title,
                thank_you_body=thank_you_body,
                button_text=button_text,
            )

        from api.ad_templates import (
            _get_follow_up_action_url,
            _get_privacy_policy_url,
            _normalize_lead_form_questions,
        )
        normalized_questions = _normalize_lead_form_questions(questions)
        if not normalized_questions:
            raise Exception("Lead Form questions are invalid")
        form_name = f"[AI] {(form_title or 'Lead Form')[:60]} {datetime.now().strftime('%Y%m%d-%H%M%S')}"
        payload = {
            "name": form_name,
            "questions": json.dumps(normalized_questions, ensure_ascii=False),
            "privacy_policy": json.dumps({
                "url": _get_privacy_policy_url(privacy_url),
                "link_text": privacy_text or "Privacy Policy",
            }, ensure_ascii=False),
            "locale": locale or "en_US",
            "flexible_delivery": "ON_DELIVERY",
        }
        follow_url = _get_follow_up_action_url(page_id, follow_up_url)
        if follow_url:
            payload["follow_up_action_url"] = follow_url
        if thank_you_title:
            ty = {"title": thank_you_title, "button_type": "NONE"}
            if thank_you_body:
                ty["body"] = thank_you_body
            if button_text:
                ty["button_text"] = button_text
            if follow_up_url:
                ty["website_url"] = follow_up_url
                ty["button_type"] = "VIEW_WEBSITE"
            payload["thank_you_page"] = json.dumps(ty, ensure_ascii=False)
        if context_card:
            ctx = dict(context_card or {})
            ctx.setdefault("style", "LIST_STYLE")
            content = {}
            if ctx.get("button_text"):
                content["button_text"] = ctx.pop("button_text")
            ctx.pop("button_type", None)
            ctx.pop("body", None)
            ctx.pop("subtitle", None)
            ctx.pop("description", None)
            if content:
                ctx["content"] = content
            payload["context_card"] = json.dumps(ctx, ensure_ascii=False)
        data = self._run_local_graph_task(
            "graph_post",
            f"{page_id}/leadgen_forms",
            {
                "data": payload,
                "local_auth": {"mode": "page_token", "page_id": str(page_id or "")},
                "_timeout_sec": 120,
            },
            timeout_seconds=120,
        )
        if "error" in data:
            raise Exception(self._format_fb_error(data["error"]))
        form_id = data.get("id")
        if not form_id:
            raise Exception(f"Lead Form create returned no id: {data}")
        return str(form_id)

    # ── 主入口 ────────────────────────────────────────────────────────────────

    def run_campaign(self, campaign_id: int):
        """
        执行单个自动铺广告任务。
        由 assets.py 的 _trigger_autopilot 在后台线程中调用。
        """
        conn = get_conn()
        campaign = conn.execute(
            "SELECT * FROM auto_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
        conn.close()
        campaign = dict(campaign) if campaign else None

        if not campaign:
            logger.error(f"[AutoPilot] campaign_id={campaign_id} 不存在")
            return

        if campaign["status"] not in ("pending", "error"):
            logger.info(f"[AutoPilot] campaign_id={campaign_id} 状态为 {campaign['status']}，跳过")
            return

        stale_empty_fb_campaign_id = ""
        existing_fb_campaign_id = str(campaign.get("fb_campaign_id") or "").strip()
        if existing_fb_campaign_id:
            conn = get_conn()
            try:
                row = conn.execute(
                    """SELECT
                           COUNT(*) AS total_rows,
                           SUM(CASE WHEN fb_ad_id IS NOT NULL AND TRIM(fb_ad_id)!=''
                                     AND COALESCE(status,'done')='done' THEN 1 ELSE 0 END) AS done_ads
                       FROM auto_campaign_ads WHERE campaign_id=?""",
                    (campaign_id,),
                ).fetchone()
                done_ads = int((row["done_ads"] if row else 0) or 0)
                total_rows = int((row["total_rows"] if row else 0) or 0)
            finally:
                conn.close()
            if done_ads > 0:
                logger.info(
                    f"[AutoPilot] campaign_id={campaign_id} 已有 fb_campaign_id={existing_fb_campaign_id} "
                    f"且已有成功广告 {done_ads} 条，跳过重复创建"
                )
                if campaign["status"] == "pending":
                    self._update_campaign_status(campaign_id, "done")
                return
            if campaign.get("status") == "error" and total_rows > 0 and done_ads == 0:
                stale_empty_fb_campaign_id = existing_fb_campaign_id
                existing_fb_campaign_id = ""
                conn = get_conn()
                try:
                    conn.execute("UPDATE auto_campaigns SET fb_campaign_id=NULL, updated_at=? WHERE id=?", (
                        datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"),
                        campaign_id,
                    ))
                    conn.commit()
                finally:
                    conn.close()
                logger.warning(
                    f"[AutoPilot] campaign_id={campaign_id} 上次失败且没有成功广告，"
                    f"本次不复用旧 Campaign: {stale_empty_fb_campaign_id}"
                )
            else:
                logger.warning(
                    f"[AutoPilot] campaign_id={campaign_id} 已有 fb_campaign_id={existing_fb_campaign_id} "
                    f"但没有成功广告记录，将复用该 Campaign 继续创建 AdSet/Ad（existing_rows={total_rows}）"
                )
        act_id = campaign["act_id"]
        self._active_act_id = act_id
        account = self._load_account(act_id)
        blocked_reason = self._get_account_launch_block_reason(account)
        if blocked_reason:
            logger.warning(
                f"[AutoPilot] campaign_id={campaign_id} launch blocked: "
                f"act_id={act_id}, reason={blocked_reason}"
            )
            self._update_campaign_status(campaign_id, "error", blocked_reason)
            self._update_progress(campaign_id, "error", blocked_reason)
            return

        self._update_campaign_status(campaign_id, "running")

        # ── 构建候选 Token 列表：矩阵内全局轮询 + 冷却避让 ───────────────────────
        _token_candidates = get_exec_token_candidates(act_id, ACTION_CREATE)

        try:
            # 1. 获取操作号 Token
            self._update_progress(campaign_id, "token", "获取操作号 Token...")
            if not _token_candidates:
                raise Exception(
                    f"账户 {act_id} 无可用操作号 Token，CREATE 操作已被拦截。"
                    "请在账户管理中补充操作号后重试。"
                )
            token = self._candidate_token(_token_candidates[0])
            logger.info(f"[AutoPilot] 使用 Token 池起点: {_token_candidates[0]['label']}")

            # 2. 加载素材和 AI 文案
            self._update_progress(campaign_id, "asset", "加载素材和 AI 文案...")
            asset = self._load_asset(campaign["asset_id"])
            if not asset:
                raise Exception(f"素材 ID={campaign['asset_id']} 不存在")

            copy_mode = (campaign.get("copy_mode") or "ai").strip()
            if copy_mode == "empty":
                headlines = [""]
                bodies = [""]
            elif copy_mode == "custom":
                cc = self._parse_json_field(campaign.get("custom_copy"), {})
                if isinstance(cc, dict):
                    headlines = [cc.get("headline", "") or ""]
                    bodies = [cc.get("body", "") or ""]
                else:
                    headlines = [""]
                    bodies = [""]
                if not headlines[0] or not bodies[0]:
                    raise Exception("自定义文案为空，请在素材详情中设置自定义标题和正文")
            else:  # "ai" (default)
                headlines = self._parse_json_field(asset["ai_headlines"], [])
                bodies = self._parse_json_field(asset["ai_bodies"], [])
                if not headlines or not bodies:
                    raise Exception("素材缺少 AI 生成的文案，请先完成 AI 分析")

            interests = self._parse_json_field(asset["ai_interests"], [])
            target_countries = self._parse_json_field(campaign["target_countries"], ["US"])
            regulated_countries = [c for c in target_countries if c in REGULATED_IDENTITY_COUNTRIES]

            # 3. 获取系统配置
            # 账户级 page_id/pixel_id 优先，回退到全局设置
            from core.database import get_conn as _gc
            _c2 = _gc()
            _acc = account
            # ── 需要认证国家的身份读取（按矩阵自动匹配，三层优先级）────────────────
            beneficiary = ""
            payer = ""
            tw_verified_id = ""  # verified_identity_id（regional_regulation_identities 方式）
            _matrix_id = get_matrix_id_for_account(act_id)

            # 第1优先：铺广告时手动指定的认证主页（tw_page_id → tw_certified_pages 表）
            _camp_tw_page = (campaign.get("tw_page_id") or campaign.get("tw_advertiser_id")) if campaign else None
            if _camp_tw_page:
                _cp_row = _c2.execute(
                    """SELECT page_id, page_name, verified_identity_id, matrix_id,
                              page_is_published, page_can_advertise, page_status, page_status_hint
                       FROM tw_certified_pages WHERE page_id=?""",
                    (_camp_tw_page,)
                ).fetchone()
                if _cp_row:
                    page_block_reason = _tw_page_block_reason(_cp_row)
                    if page_block_reason:
                        raise Exception(
                            f"当前选择的认证主页 {_cp_row['page_name'] or _camp_tw_page} 不可投放：{page_block_reason}"
                        )
                    if _matrix_id and _cp_row["matrix_id"] not in (None, _matrix_id):
                        raise Exception(
                            f"当前选择的认证主页 {_camp_tw_page} 属于矩阵 {_cp_row['matrix_id']}，"
                            f"但账户 {act_id} 位于矩阵 {_matrix_id}。需要认证国家的投放必须使用同矩阵且已填写 Verified ID 的主页。"
                        )
                    beneficiary = _cp_row["page_name"] or ""
                    payer = _cp_row["page_name"] or ""
                    tw_verified_id = _normalize_verified_identity_value(_cp_row["verified_identity_id"])
                    if tw_verified_id:
                        logger.info(f"[AutoPilot] 认证身份（手动指定主页）: page={_camp_tw_page}, verified_id={tw_verified_id}")
                    elif regulated_countries:
                        raise Exception(
                            f"当前选择的主页 {_cp_row['page_name'] or _camp_tw_page} 还没有填写 Verified ID，"
                            f"{'/'.join(regulated_countries)} 属于需要认证的国家，当前不能投放。"
                        )
                elif regulated_countries:
                    raise Exception(
                        f"当前选择的认证主页 {_camp_tw_page} 不在主页库中，"
                        f"{'/'.join(regulated_countries)} 属于需要认证的国家，请先录入主页并填写 Verified ID。"
                    )

            # 第2优先：按矩阵自动匹配 tw_certified_pages（核心逻辑）
            if not tw_verified_id:
                if _matrix_id:
                    _matrix_cp = _c2.execute(
                        f"""SELECT page_id, page_name, verified_identity_id
                           FROM tw_certified_pages
                           WHERE matrix_id=? AND verified_identity_id IS NOT NULL
                             AND TRIM(verified_identity_id) != ''
                             AND LOWER(TRIM(verified_identity_id)) NOT IN ('none','null','undefined')
                             AND {_tw_page_not_blocked_sql()}
                           ORDER BY id ASC LIMIT 1""",
                        (_matrix_id,)
                    ).fetchone()
                    if _matrix_cp:
                        tw_verified_id = _normalize_verified_identity_value(_matrix_cp["verified_identity_id"])
                        if not beneficiary:
                            beneficiary = _matrix_cp["page_name"] or ""
                        if not payer:
                            payer = _matrix_cp["page_name"] or ""
                        logger.info(
                            f"[AutoPilot] 认证身份（矩阵{_matrix_id}自动匹配）: "
                            f"page={_matrix_cp['page_id']}, verified_id={tw_verified_id}"
                        )

            # 第3优先：直接存在 accounts 表的 beneficiary/payer 字段（兼容旧数据）
            if not beneficiary and _acc:
                beneficiary = _acc.get("beneficiary", "") or ""
            if not payer and _acc:
                payer = _acc.get("payer", "") or ""
            _c2.close()
            if regulated_countries and not tw_verified_id:
                country_label = "/".join(regulated_countries)
                if _matrix_id:
                    raise Exception(
                        f"矩阵 {_matrix_id} 还没有填写可用的 Verified ID，"
                        f"{country_label} 属于需要认证的国家，当前不能投放。"
                    )
                raise Exception(
                    f"账户 {act_id} 暂未识别到矩阵归属，且 {country_label} 属于需要认证的国家，"
                    "请先配置矩阵内的认证主页与 Verified ID。"
                )
            if tw_verified_id:
                logger.info(f"[AutoPilot] 认证投放身份信息: verified_id={tw_verified_id!r}（将优先使用 Verified ID 方式）")
            elif beneficiary or payer:
                logger.info(f"[AutoPilot] 认证投放身份信息: beneficiary={beneficiary!r}, payer={payer!r}（旧版字符串方式）")
            else:
                logger.warning(f"[AutoPilot] 认证投放身份信息为空，AdSet可能因缺少 advertiser 信息而失败")
            page_id = (campaign.get("page_id_override") or
                       (_acc["page_id"] if _acc and _acc["page_id"] else None) or
                       self._get_setting("autopilot_fb_page_id", ""))
            pixel_id = (campaign.get("pixel_id_override") or
                        (_acc["pixel_id"] if _acc and _acc["pixel_id"] else None) or
                        self._get_setting("autopilot_fb_pixel_id", ""))
            # 优先使用起动时传入的 max_adsets，否则使用全局设置
            max_adsets = int(campaign.get("max_adsets") or
                             self._get_setting("autopilot_max_adsets", "5"))
            # 用户输入的是 USD 预算，需换算为账户货币（FB API 要求账户货币单位）
            budget_usd = float(campaign["daily_budget"] or
                               self._get_setting("autopilot_test_budget", "20"))
            # 获取账户货币
            _acc_currency = (_acc["currency"] if _acc and _acc.get("currency") else "USD").upper()
            if _acc_currency == "USD":
                test_budget = budget_usd
            else:
                test_budget = _usd_to_account_currency(budget_usd, _acc_currency)
                _rate = float(USD_VALUE_BY_CURRENCY.get(_acc_currency, 1.0) or 1.0)
                logger.info(
                    f"[AutoPilot] 预算换算: {budget_usd} USD -> {test_budget} {_acc_currency} "
                    f"(汇率 1 USD = {1/_rate:.4f} {_acc_currency})"
                )
            # 受众定向参数
            age_min = int(campaign.get("age_min") or 18)
            age_max = int(campaign.get("age_max") or 65)
            gender = int(campaign.get("gender") or 0)  # 0=不限, 1=男, 2=女
            # 版位设置
            placements = self._parse_json_field(campaign.get("placements"), None)
            # v4.0: 设备端（mobile/desktop/all）
            device_platforms = campaign.get("device_platforms") or "all"
            # v4.0: 广告语言
            ad_language = campaign.get("ad_language") or "en"
            # 出价策略
            bid_strategy = campaign.get("bid_strategy") or "LOWEST_COST_WITHOUT_CAP"
            one_ad_per_adset = self._campaign_bool(
                campaign,
                "one_ad_per_adset",
                self._setting_enabled("autopilot_one_ad_per_adset", "1"),
            )
            budget_mode = str(campaign.get("budget_mode") or "ABO").strip().upper()
            if budget_mode not in {"ABO", "CBO"}:
                budget_mode = "ABO"
            budget_amount_mode = str(campaign.get("budget_amount_mode") or "per_adset").strip().lower()
            if budget_amount_mode not in {"per_adset", "total"}:
                budget_amount_mode = "per_adset"
            if budget_mode == "CBO":
                budget_amount_mode = "total"
            audience_strategy = str(campaign.get("audience_strategy") or "broad_interest").strip().lower()
            if audience_strategy not in {"broad_interest", "broad_only", "interest_only"}:
                audience_strategy = "broad_interest"
            try:
                audience_interest_chunk_size = max(1, min(int(campaign.get("audience_interest_chunk_size") or 2), 5))
            except Exception:
                audience_interest_chunk_size = 2

            # 落地页链接：弹窗输入 > 素材绑定 > 账户主链接/已绑定主链接 > 全局默认
            # 层內1：铺广告弹窗手动填写
            # 层內2：素材级绑定的链接
            # 层內3：系统全局默认链接
            default_landing_url = self._get_setting("default_landing_url", "")
            _link_conn = get_conn()
            try:
                account_landing_url = resolve_account_landing_link(
                    _link_conn,
                    act_id,
                    _acc or {},
                    default_landing_url,
                )
                _acc_form_link = resolve_account_form_link(_link_conn, act_id, _acc or {}, account_landing_url)
            finally:
                _link_conn.close()
            landing_url = (campaign.get("landing_url") or
                           asset.get("landing_url") or
                           account_landing_url or
                           default_landing_url)
            # 表单链接：潜在客户广告（lead_generation）时使用
            form_link = campaign.get("form_link") or _acc_form_link or landing_url
            landing_url = self._normalize_managed_landing_url_for_launch(landing_url)
            form_link = self._normalize_managed_landing_url_for_launch(form_link)

            if not page_id:
                raise Exception(
                    f"账户 {act_id} 未配置主页 ID。"
                    "请在账户管理中填写该账户的 Facebook 主页 ID，"
                    "或在系统设置中配置全局默认主页 ID（autopilot_fb_page_id）"
                )
            _token_candidates = self._filter_create_tokens_for_page(page_id, _token_candidates)
            token = self._candidate_token(_token_candidates[0], token)
            logger.info(
                f"[AutoPilot] 主页权限预检通过: page_id={page_id}, "
                f"可用 CREATE Token={len(_token_candidates)}"
            )
            if stale_empty_fb_campaign_id:
                if self._delete_fb_campaign_best_effort(stale_empty_fb_campaign_id, token, _token_candidates[0]):
                    logger.info("[AutoPilot] 已清理上次失败残留 Campaign: %s", stale_empty_fb_campaign_id)
            # 落地页检测：流量/转化/互动广告必须有落地页链接
            goal_meta = _get_campaign_goal_meta(
                campaign.get("objective", ""),
                campaign.get("conversion_goal", ""),
            )
            campaign["objective"] = goal_meta["objective"]
            campaign["conversion_goal"] = goal_meta["goal"]
            _LANDING_REQUIRED_OBJ = ("OUTCOME_TRAFFIC", "OUTCOME_SALES", "OUTCOME_ENGAGEMENT")
            _obj_for_check = goal_meta["objective"]
            if goal_meta["landing_required"] and not landing_url:
                _obj_label_map = {
                    "OUTCOME_TRAFFIC": "流量点击（Traffic）",
                    "OUTCOME_SALES": "转化购买（Conversions）",
                    "OUTCOME_ENGAGEMENT": "帖子互动（Engagement）"
                }
                _obj_lbl = _obj_label_map.get(_obj_for_check, _obj_for_check)
                raise Exception(
                    f"❌ {_obj_lbl}广告必须配置落地页链接（landing_url）！\n"
                    f"请在以下任一位置配置：\n"
                    f"1. 铺广告弹窗中的「落地页链接」字段\n"
                    f"2. 「投放链接管理」页面为账户 {act_id} 配置专属落地页\n"
                    f"3. 系统设置中的「全局默认落地页」（default_landing_url）"
                )
            if goal_meta["landing_required"]:
                self._validate_managed_landing_ready_for_launch(landing_url, "落地页链接")
            self._update_progress(campaign_id, "upload", "上传素材到 Facebook...")
            # 4. 上传素材到 FB（获取 image_hash 或 video_id）
            fb_asset_ref, _asset_token_candidate = self._run_with_token_fallback(
                _token_candidates,
                token,
                "上传素材",
                lambda try_token, _: self._upload_asset_to_fb(act_id, asset, try_token),
            )
            token = self._candidate_token(_asset_token_candidate, token)

            # 5. 创建 Campaign（矩阵内 Token 轮询兜底）
            self._update_progress(campaign_id, "campaign", "创建广告系列 (Campaign)...")
            # ── 规范化命名（在轮询循环之前计算）──────────────────────────────────
            _obj_abbr = {
                "OUTCOME_SALES":         "CONV",
                "OUTCOME_LEADS":         "LEAD",
                "OUTCOME_TRAFFIC":       "TRAF",
                "OUTCOME_AWARENESS":     "AWR",
                "OUTCOME_ENGAGEMENT":    "ENG",
                "OUTCOME_MESSAGES":      "MSG",
                "OUTCOME_APP_PROMOTION": "APP",
            }
            _obj_short = _obj_abbr.get(campaign["objective"], "ADS")
            _ctry_list = target_countries or ["XX"]
            _ctry_str  = "-".join(_ctry_list[:2])
            _ast_code  = asset.get("asset_code") or f"AST-{asset['id']:04d}"
            from datetime import datetime as _dt
            try:
                import pytz as _pytz
                _cst = _pytz.timezone("Asia/Shanghai")
                _now_cst = _dt.now(_cst)
            except Exception:
                _now_cst = _dt.now()
            _mmdd = _now_cst.strftime("%m%d")
            _dispatch_src = campaign.get("dispatch_source") or "manual"
            _src_prefix = "A" if _dispatch_src == "global_dispatcher" else "M"
            _campaign_display_name = f"{_src_prefix}-{_obj_short}-{_ctry_str}-{_ast_code}-{_mmdd}"
            # ──────────────────────────────────────────────────────────────────────

            fb_campaign_id = existing_fb_campaign_id
            if fb_campaign_id:
                _campaign_token_candidate = _token_candidates[0]
                _used_token_label = _campaign_token_candidate.get("label", "existing")
                logger.info(f"[AutoPilot] 复用已有 Campaign: {fb_campaign_id}")
            else:
                fb_campaign_id, _campaign_token_candidate = self._run_with_token_fallback(
                    _token_candidates,
                    token,
                    "创建 Campaign",
                    lambda try_token, _: self._create_campaign(
                        act_id, _campaign_display_name, campaign["objective"], try_token,
                        daily_budget=test_budget if budget_mode == "CBO" else None,
                        currency=_acc_currency,
                        budget_mode=budget_mode,
                    ),
                )
                token = self._candidate_token(_campaign_token_candidate, token)
                _used_token_label = _campaign_token_candidate["label"]
                self._update_campaign_field(campaign_id, "fb_campaign_id", fb_campaign_id)
                logger.info(f"[AutoPilot] ✅ Campaign 创建成功 (Token={_used_token_label}): {fb_campaign_id}")

            # 6. 生成受众矩阵（兴趣词分组 + 宽泛受众）
            audience_groups = self._build_audience_groups(
                interests, target_countries, max_adsets,
                age_min=age_min, age_max=age_max, gender=gender,
                token=token,
                rotation_seed=f"{campaign_id}:{asset['id']}:{act_id}:{_mmdd}",
                strategy=audience_strategy,
                chunk_size=audience_interest_chunk_size,
            )
            audience_group_count = max(len(audience_groups), 1)

            # 7. 逐组创建 AdSet + Ad
            total_adsets = 0
            total_ads = 0
            fatal_launch_error = ""

            for group_idx, audience in enumerate(audience_groups):
                if fatal_launch_error:
                    break
                try:
                    if not token:
                        logger.warning(f"[AutoPilot] AdSet {group_idx+1}: 无可用操作号 Token，跳过")
                        continue
                    # 组名：{系列名}-{受众类型}-G{序号}
                    # 受众类型：BROAD=宽泛受众，INT=兴趣受众
                    _aud_name = audience.get("name", "")
                    _aud_type = "BROAD" if audience.get("type") == "broad" or "宽泛" in _aud_name else f"INT{group_idx+1}"
                    adset_name = f"{_campaign_display_name}-{_aud_type}-G{group_idx+1}"
                    # v4.0: 设备端版位覆盖
                    effective_placements = placements.copy() if placements else {}
                    if device_platforms == "mobile":
                        # 尊重用户选择的 publisher_platforms，不强制覆盖
                        # 只添加 device_platforms 限制，不改变用户的平台/版位选择
                        effective_placements["device_platforms"] = ["mobile"]
                        # 如果用户没有选任何平台（空 placements），则不设置版位（使用 FB 自动版位）
                        # 如果用户选了版位，确保 instagram_positions 和 facebook_positions 正确分组
                        _user_pp = effective_placements.get("publisher_platforms", [])
                        _user_fp = effective_placements.get("facebook_positions", [])
                        _user_ip = effective_placements.get("instagram_positions", [])
                        # 清理 facebook_positions 中的 IG 版位（兼容旧数据）
                        _clean_fp = [p for p in _user_fp if p not in ("instagram_feed", "instagram_story", "instagram_reels")]
                        # 清理 instagram_positions 中的 FB 版位（兼容旧数据）
                        _clean_ip = [p for p in _user_ip if p not in ("feed", "story", "right_hand_column", "reels")]
                        if _clean_fp:
                            effective_placements["facebook_positions"] = _clean_fp
                        elif "facebook_positions" in effective_placements:
                            del effective_placements["facebook_positions"]
                        if _clean_ip:
                            effective_placements["instagram_positions"] = _clean_ip
                        elif "instagram_positions" in effective_placements:
                            del effective_placements["instagram_positions"]
                    elif device_platforms == "desktop":
                        effective_placements["publisher_platforms"] = ["facebook"]
                        effective_placements["facebook_positions"] = ["feed", "right_hand_column"]
                        effective_placements["device_platforms"] = ["desktop"]
                        # desktop 不支持 instagram，移除
                        effective_placements.pop("instagram_positions", None)
                    # all: 使用自动版位（不覆盖）
                    audience_budget = test_budget
                    audience_budget_usd = budget_usd
                    if budget_amount_mode == "total":
                        audience_budget = round(float(test_budget) / float(audience_group_count), 2)
                        audience_budget_usd = round(float(budget_usd) / float(audience_group_count), 2)
                    ad_count = self._launch_ad_variant_count(headlines, bodies, audience_budget_usd, one_ad_per_adset)
                    if ad_count <= 0:
                        logger.warning(f"[AutoPilot] AdSet {group_idx+1}: 文案不足，跳过")
                        continue
                    adset_budget = None if budget_mode == "CBO" else audience_budget
                    if one_ad_per_adset:
                        split_budget = None if budget_mode == "CBO" else self._split_adset_budget(audience_budget, ad_count, True)
                        logger.info(
                            "[AutoPilot] 一广告一组已启用: audience=%s, ads=%s, budget_mode=%s, budget_per_adset=%s %s",
                            group_idx + 1, ad_count, budget_mode, split_budget if split_budget is not None else "campaign", _acc_currency
                        )
                        for ad_idx in range(ad_count):
                            headline = headlines[ad_idx]
                            body = bodies[ad_idx]
                            asset_code = asset.get("asset_code") or f"AST-{asset['id']:04d}"
                            ad_name = f"{_ast_code}-{_aud_type}-C{ad_idx+1}"
                            variant_adset_name = f"{adset_name}-C{ad_idx+1}" if ad_count > 1 else adset_name
                            variant_audience = audience
                            fb_adset_id = None
                            try:
                                fb_adset_id, _adset_token_candidate = self._run_with_token_fallback(
                                    _token_candidates,
                                    token,
                                    f"创建 AdSet {group_idx + 1}-{ad_idx + 1}",
                                    lambda try_token, _: self._create_adset(
                                        act_id, fb_campaign_id, variant_adset_name,
                                        variant_audience, split_budget, campaign["target_cpa"],
                                        campaign["objective"], pixel_id, try_token,
                                        bid_strategy=bid_strategy,
                                        budget_mode=budget_mode,
                                        placements=effective_placements if effective_placements else None,
                                        conversion_event=campaign.get("conversion_event") or "PURCHASE",
                                        beneficiary=beneficiary,
                                        payer=payer,
                                        tw_verified_id=tw_verified_id,
                                        page_id=page_id,
                                        conversion_goal=campaign.get("conversion_goal") or ""
                                    ),
                                )
                                token = self._candidate_token(_adset_token_candidate, token)
                                total_adsets += 1
                                self._update_progress(campaign_id, f"adset_{group_idx+1}_{ad_idx+1}", f"AdSet {group_idx+1}-{ad_idx+1}/{len(audience_groups)} 创建成功，正在创建广告...")
                                logger.info(f"[AutoPilot] ✅ AdSet {group_idx+1}-{ad_idx+1} 创建成功: {fb_adset_id}")
                            except Exception as adset_err:
                                audience_targeting = audience.get("targeting", {})
                                has_interests = bool(
                                    audience_targeting.get("flexible_spec") or
                                    audience_targeting.get("interests")
                                )
                                if has_interests:
                                    logger.warning(f"[AutoPilot] AdSet {group_idx+1}-{ad_idx+1} 创建失败，尝试降级宽泛受众: {adset_err}")
                                    try:
                                        variant_audience = {
                                            "name": audience.get("name", f"宽泛受众-{group_idx+1}") + "（降级）",
                                            "targeting": {k: v for k, v in audience_targeting.items() if k not in ("interests", "flexible_spec")}
                                        }
                                        fb_adset_id, _fallback_adset_token_candidate = self._run_with_token_fallback(
                                            _token_candidates,
                                            token,
                                            f"降级创建 AdSet {group_idx + 1}-{ad_idx + 1}",
                                            lambda try_token, _: self._create_adset(
                                                act_id, fb_campaign_id, variant_adset_name + "-FB",
                                                variant_audience, split_budget, campaign["target_cpa"],
                                                campaign["objective"], pixel_id, try_token,
                                                bid_strategy=bid_strategy,
                                                budget_mode=budget_mode,
                                                placements=effective_placements if effective_placements else None,
                                                conversion_event=campaign.get("conversion_event") or "PURCHASE",
                                                beneficiary=beneficiary,
                                                payer=payer,
                                                tw_verified_id=tw_verified_id,
                                                page_id=page_id,
                                                conversion_goal=campaign.get("conversion_goal") or ""
                                            ),
                                        )
                                        token = self._candidate_token(_fallback_adset_token_candidate, token)
                                        total_adsets += 1
                                        ad_name = f"{_ast_code}-BROAD-C{ad_idx+1}-FB"
                                        logger.info(f"[AutoPilot] ✅ AdSet {group_idx+1}-{ad_idx+1} 降级宽泛受众创建成功: {fb_adset_id}")
                                    except Exception as fallback_err:
                                        logger.error(f"[AutoPilot] AdSet {group_idx+1}-{ad_idx+1} 降级宽泛受众也失败: {fallback_err}")
                                        self._insert_campaign_ad(
                                            campaign_id, act_id, campaign["asset_id"],
                                            headline, body,
                                            json.dumps(variant_audience, ensure_ascii=False),
                                            None, None,
                                            status="error", error_msg=f"原始错误: {adset_err}; 降级错误: {fallback_err}",
                                            adset_name=variant_adset_name, ad_name=ad_name
                                        )
                                        continue
                                else:
                                    logger.error(f"[AutoPilot] AdSet {group_idx+1}-{ad_idx+1} 创建失败: {adset_err}")
                                    self._insert_campaign_ad(
                                        campaign_id, act_id, campaign["asset_id"],
                                        headline, body,
                                        json.dumps(variant_audience, ensure_ascii=False),
                                        None, None,
                                        status="error", error_msg=str(adset_err),
                                        adset_name=variant_adset_name, ad_name=ad_name
                                    )
                                    continue
                            try:
                                fb_ad_id, _ad_token_candidate = self._run_with_token_fallback(
                                    _token_candidates,
                                    token,
                                    f"创建广告 {group_idx + 1}-{ad_idx + 1}",
                                    lambda try_token, _: self._create_ad(
                                        act_id, fb_adset_id, ad_name,
                                        headline, body, page_id,
                                        fb_asset_ref, asset["file_type"], try_token,
                                        landing_url=landing_url,
                                        conversion_goal=campaign.get("conversion_goal") or "",
                                        message_template=campaign.get("message_template") or "",
                                        lead_form_id=campaign.get("lead_form_id") or "",
                                        form_link=form_link or "",
                                        asset_info=asset,
                                        cta_type=campaign.get("cta_type") or "",
                                        pixel_id=pixel_id or "",
                                        ad_language=ad_language,
                                        target_countries=target_countries,
                                        fb_campaign_id=fb_campaign_id,
                                        fb_campaign_name=_campaign_display_name,
                                        adset_name=variant_adset_name,
                                    ),
                                )
                                token = self._candidate_token(_ad_token_candidate, token)
                                total_ads += 1
                                self._insert_campaign_ad(
                                    campaign_id, act_id, campaign["asset_id"],
                                    headline, body,
                                    json.dumps(variant_audience, ensure_ascii=False),
                                    fb_adset_id, fb_ad_id,
                                    adset_name=variant_adset_name, ad_name=ad_name
                                )
                                logger.info(f"[AutoPilot] ✅ Ad {group_idx+1}-{ad_idx+1} 创建成功: {fb_ad_id}")
                                time.sleep(0.1)
                            except Exception as ad_err:
                                _ad_err_msg = str(ad_err)
                                logger.error(f"[AutoPilot] Ad {group_idx+1}-{ad_idx+1} 创建失败: {_ad_err_msg}")
                                self._insert_campaign_ad(
                                    campaign_id, act_id, campaign["asset_id"],
                                    headline, body,
                                    json.dumps(variant_audience, ensure_ascii=False),
                                    fb_adset_id, None,
                                    status="error", error_msg=_ad_err_msg,
                                    adset_name=variant_adset_name, ad_name=ad_name
                                )
                                if self._is_app_development_mode_error(_ad_err_msg):
                                    fatal_launch_error = _ad_err_msg
                                    break
                        time.sleep(0.1)
                        continue
                    fb_adset_id, _adset_token_candidate = self._run_with_token_fallback(
                        _token_candidates,
                        token,
                        f"创建 AdSet {group_idx + 1}",
                        lambda try_token, _: self._create_adset(
                            act_id, fb_campaign_id, adset_name,
                            audience, adset_budget, campaign["target_cpa"],
                            campaign["objective"], pixel_id, try_token,
                            bid_strategy=bid_strategy,
                            budget_mode=budget_mode,
                            placements=effective_placements if effective_placements else None,
                            conversion_event=campaign.get("conversion_event") or "PURCHASE",
                            beneficiary=beneficiary,
                            payer=payer,
                            tw_verified_id=tw_verified_id,
                            page_id=page_id,
                            conversion_goal=campaign.get("conversion_goal") or ""
                        ),
                    )
                    token = self._candidate_token(_adset_token_candidate, token)
                    total_adsets += 1
                    self._update_progress(campaign_id, f"adset_{group_idx+1}", f"AdSet {group_idx+1}/{len(audience_groups)} 创建成功，正在创建广告...")
                    logger.info(f"[AutoPilot] ✅ AdSet {group_idx+1} 创建成功: {fb_adset_id}")

                    # 每个 AdSet 创建多条 Ad（文案 × 素材）
                    ad_count = min(len(headlines), len(bodies), 3)
                    for ad_idx in range(ad_count):
                        try:
                            headline = headlines[ad_idx]
                            body = bodies[ad_idx]
                            # v4.0: 广告命名嵌入 asset_code，便于 FB 后台数据归因
                            asset_code = asset.get("asset_code") or f"AST-{asset['id']:04d}"
                            # 广告名：{素材代码}-{受众类型}-C{文案序号}
                            # 例：AST-20260415-001-INT1-C1
                            ad_name = f"{_ast_code}-{_aud_type}-C{ad_idx+1}"

                            # 检查AdSet是否创建成功（检查局部变量，不是数据库记录）
                            if not fb_adset_id:
                                logger.warning(f"[AutoPilot] AdSet未创建成功（fb_adset_id为空），跳过Ad {ad_idx+1} 创建")
                                self._insert_campaign_ad(
                                    campaign_id, act_id, campaign["asset_id"],
                                    headline, body,
                                    json.dumps(audience, ensure_ascii=False),
                                    None, None,
                                    status="skipped", error_msg="AdSet未创建，跳过Ad",
                                    adset_name=adset_name, ad_name=f"{_ast_code}-{_aud_type}-C{ad_idx+1}"
                                )
                                continue
                            fb_ad_id, _ad_token_candidate = self._run_with_token_fallback(
                                _token_candidates,
                                token,
                                f"创建广告 {group_idx + 1}-{ad_idx + 1}",
                                lambda try_token, _: self._create_ad(
                                    act_id, fb_adset_id, ad_name,
                                    headline, body, page_id,
                                    fb_asset_ref, asset["file_type"], try_token,
                                    landing_url=landing_url,
                                    conversion_goal=campaign.get("conversion_goal") or "",
                                    message_template=campaign.get("message_template") or "",
                                    lead_form_id=campaign.get("lead_form_id") or "",
                                    form_link=form_link or "",
                                    asset_info=asset,
                                    cta_type=campaign.get("cta_type") or "",
                                    pixel_id=pixel_id or "",
                                    ad_language=ad_language,
                                    target_countries=target_countries,
                                    fb_campaign_id=fb_campaign_id,
                                    fb_campaign_name=_campaign_display_name,
                                    adset_name=adset_name,
                                ),
                            )
                            token = self._candidate_token(_ad_token_candidate, token)
                            total_ads += 1

                            # 写入 auto_campaign_ads 明细
                            self._insert_campaign_ad(
                                campaign_id, act_id, campaign["asset_id"],
                                headline, body,
                                json.dumps(audience, ensure_ascii=False),
                                fb_adset_id, fb_ad_id,
                                adset_name=adset_name, ad_name=ad_name
                            )
                            logger.info(f"[AutoPilot] ✅ Ad {ad_idx+1} 创建成功: {fb_ad_id}")
                            time.sleep(0.1)  # 轻量错峰，主要限流已交给 token_manager

                        except Exception as ad_err:
                            _ad_err_msg = str(ad_err)
                            logger.error(f"[AutoPilot] Ad {ad_idx+1} 创建失败: {_ad_err_msg}")
                            self._insert_campaign_ad(
                                campaign_id, act_id, campaign["asset_id"],
                                headline, body,
                                json.dumps(audience, ensure_ascii=False),
                                fb_adset_id, None,
                                status="error", error_msg=_ad_err_msg,
                                adset_name=adset_name, ad_name=ad_name
                            )
                            if self._is_app_development_mode_error(_ad_err_msg):
                                fatal_launch_error = _ad_err_msg
                                break

                    time.sleep(0.1)

                except Exception as adset_err:
                    # 如果有兴趣词定向，尝试降级为宽泛受众重试
                    audience_targeting = audience.get("targeting", {})
                    # 兴趣词现在存储在 flexible_spec 中（FB API要求格式）
                    has_interests = bool(
                        audience_targeting.get("flexible_spec") or
                        audience_targeting.get("interests")
                    )
                    if has_interests:
                        logger.warning(f"[AutoPilot] AdSet {group_idx+1} 创建失败（可能兴趣词无效），尝试降级为宽泛受众重试: {adset_err}")
                        try:
                            fallback_audience = {
                                "name": audience.get("name", f"宽泛受众-{group_idx+1}") + "（降级）",
                                "targeting": {k: v for k, v in audience_targeting.items() if k not in ("interests", "flexible_spec")}
                            }
                            fb_adset_id, _fallback_adset_token_candidate = self._run_with_token_fallback(
                                _token_candidates,
                                token,
                                f"降级创建 AdSet {group_idx + 1}",
                                lambda try_token, _: self._create_adset(
                                    act_id, fb_campaign_id, adset_name + "-FB",
                                    fallback_audience, adset_budget, campaign["target_cpa"],
                                    campaign["objective"], pixel_id, try_token,
                                    bid_strategy=bid_strategy,
                                    budget_mode=budget_mode,
                                    placements=effective_placements if effective_placements else None,
                                    conversion_event=campaign.get("conversion_event") or "PURCHASE",
                                    beneficiary=beneficiary,
                                    payer=payer,
                                    tw_verified_id=tw_verified_id,
                                    page_id=page_id,
                                    conversion_goal=campaign.get("conversion_goal") or ""
                                ),
                            )
                            token = self._candidate_token(_fallback_adset_token_candidate, token)
                            total_adsets += 1
                            audience = fallback_audience  # 用降级受众继续创建 Ad
                            self._update_progress(campaign_id, f"adset_{group_idx+1}", f"AdSet {group_idx+1} 降级宽泛受众创建成功，正在创建广告...")
                            logger.info(f"[AutoPilot] ✅ AdSet {group_idx+1} 降级宽泛受众创建成功: {fb_adset_id}")
                            # 降级成功后继续创建 Ad
                            ad_count = min(len(headlines), len(bodies), 3)
                            for ad_idx in range(ad_count):
                                try:
                                    headline = headlines[ad_idx]
                                    body = bodies[ad_idx]
                                    asset_code = asset.get("asset_code") or f"AST-{asset['id']:04d}"
                                    # 降级广告名：{素材代码}-BROAD-C{文案序号}-FB
                                    ad_name = f"{_ast_code}-BROAD-C{ad_idx+1}-FB"
                                    fb_ad_id, _fallback_ad_token_candidate = self._run_with_token_fallback(
                                        _token_candidates,
                                        token,
                                        f"降级创建广告 {group_idx + 1}-{ad_idx + 1}",
                                        lambda try_token, _: self._create_ad(
                                            act_id, fb_adset_id, ad_name,
                                            headline, body, page_id,
                                            fb_asset_ref, asset["file_type"], try_token,
                                            landing_url=landing_url,
                                            conversion_goal=campaign.get("conversion_goal") or "",
                                            message_template=campaign.get("message_template") or "",
                                            lead_form_id=campaign.get("lead_form_id") or "",
                                            form_link=form_link or "",
                                            asset_info=asset,
                                            cta_type=campaign.get("cta_type") or "",
                                            pixel_id=pixel_id or "",
                                            ad_language=ad_language,
                                            target_countries=target_countries,
                                            fb_campaign_id=fb_campaign_id,
                                            fb_campaign_name=_campaign_display_name,
                                            adset_name=adset_name,
                                        ),
                                    )
                                    token = self._candidate_token(_fallback_ad_token_candidate, token)
                                    total_ads += 1
                                    self._insert_campaign_ad(
                                        campaign_id, act_id, campaign["asset_id"],
                                        headline, body,
                                        json.dumps(fallback_audience, ensure_ascii=False),
                                        fb_adset_id, fb_ad_id,
                                        adset_name=adset_name, ad_name=ad_name
                                    )
                                    logger.info(f"[AutoPilot] ✅ Ad {ad_idx+1}（降级）创建成功: {fb_ad_id}")
                                    time.sleep(0.1)
                                except Exception as ad_err2:
                                    _ad_err_msg2 = str(ad_err2)
                                    logger.error(f"[AutoPilot] Ad {ad_idx+1}（降级）创建失败: {_ad_err_msg2}")
                                    self._insert_campaign_ad(
                                        campaign_id, act_id, campaign["asset_id"],
                                        headlines[ad_idx] if ad_idx < len(headlines) else "",
                                        bodies[ad_idx] if ad_idx < len(bodies) else "",
                                        json.dumps(fallback_audience, ensure_ascii=False),
                                        fb_adset_id, None,
                                        status="error", error_msg=_ad_err_msg2,
                                        adset_name=adset_name, ad_name=ad_name
                                    )
                                    if self._is_app_development_mode_error(_ad_err_msg2):
                                        fatal_launch_error = _ad_err_msg2
                                        break
                        except Exception as fallback_err:
                            logger.error(f"[AutoPilot] AdSet {group_idx+1} 降级宽泛受众也失败: {fallback_err}")
                            self._insert_campaign_ad(
                                campaign_id, act_id, campaign["asset_id"],
                                None, None,
                                json.dumps(audience, ensure_ascii=False),
                                None, None,
                                status="error", error_msg=f"原始错误: {adset_err}; 降级错误: {fallback_err}",
                                adset_name=adset_name
                            )
                    else:
                        logger.error(f"[AutoPilot] AdSet {group_idx+1} 创建失败: {adset_err}")
                        self._insert_campaign_ad(
                            campaign_id, act_id, campaign["asset_id"],
                            None, None,
                            json.dumps(audience, ensure_ascii=False),
                            None, None,
                            status="error", error_msg=str(adset_err),
                            adset_name=adset_name
                        )

             # 8. 更新任务完成状态
            # 全部失败时设为 error，部分失败或全部成功时设为 done
            _final_status = "error" if (total_adsets == 0 or total_ads == 0) else "done"
            conn = get_conn()
            conn.execute(
                """UPDATE auto_campaigns SET status=?, total_adsets=?, total_ads=?,
                   updated_at=? WHERE id=?""",
                (_final_status, total_adsets, total_ads,
                 datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
            )
            conn.commit()
            conn.close()
            if total_ads > 0:
                try:
                    _mc = get_conn()
                    _cols = {r[1] for r in _mc.execute("PRAGMA table_info(ad_assets)").fetchall()}
                    _updates = []
                    _params = []
                    _now_dispatch = datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
                    if "dispatch_count" in _cols:
                        _updates.append("dispatch_count=COALESCE(dispatch_count,0)+1")
                    if "last_dispatched_at" in _cols:
                        _updates.append("last_dispatched_at=?")
                        _params.append(_now_dispatch)
                    if _updates:
                        _updates.append("updated_at=?")
                        _params.extend([_now_dispatch, campaign["asset_id"]])
                        _mc.execute(f"UPDATE ad_assets SET {', '.join(_updates)} WHERE id=?", _params)
                        _mc.commit()
                    _mc.close()
                except Exception as _mark_err:
                    logger.warning(f"[AutoPilot] 更新素材铺放计数失败: {_mark_err}")
            # 如果 0 个 AdSet 成功，附带错误原因
            # ── 修复: 收集失败原因，无论有无成功都展示──
            try:
                _ec = get_conn()
                _err_rows = _ec.execute(
                    """SELECT error_msg, COUNT(*) as cnt
                       FROM auto_campaign_ads
                       WHERE campaign_id=? AND status='error' AND error_msg IS NOT NULL
                       GROUP BY error_msg ORDER BY cnt DESC LIMIT 3""",
                    (campaign_id,)
                ).fetchall()
                _ec.close()
                _err_parts = []
                for _row in _err_rows:
                    _msg = _row["error_msg"][:260] + "..." if len(_row["error_msg"]) > 260 else _row["error_msg"]
                    _err_parts.append(f"×{_row['cnt']} {_msg}")
                _err_summary = "\n".join(_err_parts) if _err_parts else ""
            except Exception:
                _err_summary = ""

            _cleanup_note = ""
            if total_ads == 0:
                # 广告全部失败：自动删除 FB Campaign，不留空 Campaign/AdSet 垃圾对象
                if fb_campaign_id:
                    if self._delete_fb_campaign_best_effort(fb_campaign_id, token):
                        logger.info(f"[AutoPilot] 已删除失败的 FB Campaign: {fb_campaign_id}")
                        _cleanup_note = "\n已自动删除失败 Campaign，避免保留空 AdSet。"
                    # 清除数据库中的 fb_campaign_id
                    try:
                        _cc = get_conn()
                        _cc.execute("UPDATE auto_campaigns SET fb_campaign_id=NULL WHERE id=?", (campaign_id,))
                        _cc.commit()
                        _cc.close()
                    except Exception:
                        pass
            if total_adsets == 0:
                _done_msg = f"全部失败！共创建 0 个 AdSet，0 条广告"
                if _err_summary:
                    _done_msg += f"\n⚠️ 失败原因：\n{_err_summary}"
                else:
                    _done_msg += "（请查看日志了解详情）"
                _done_msg += _cleanup_note
                # 全部失败时改为 error 状态
                self._update_progress(campaign_id, "error", _done_msg)
            elif total_ads == 0:
                _done_msg = f"AdSet 已创建但广告全部失败！共 {total_adsets} 个 AdSet，0 条广告"
                if _err_summary:
                    _done_msg += f"\n⚠️ 失败原因：\n{_err_summary}"
                _done_msg += _cleanup_note
                # 广告全部失败时也改为 error
                self._update_progress(campaign_id, "error", _done_msg)
            else:
                _done_msg = f"完成！共创建 {total_adsets} 个 AdSet，{total_ads} 条广告"
                if _err_summary:
                    _done_msg += f"\n⚠️ 部分失败：\n{_err_summary}"
                self._update_progress(campaign_id, "done", _done_msg)
            logger.info(
                f"[AutoPilot] ✅ 任务完成 campaign_id={campaign_id}，"
                f"共创建 {total_adsets} 个 AdSet，{total_ads} 条 Ad"
            )

        except Exception as e:
            logger.error(f"[AutoPilot] ❌ 任务失败 campaign_id={campaign_id}: {e}")
            self._update_campaign_status(campaign_id, "error", str(e))


            # Write to action_logs
            try:
                from core.database import get_conn as _gc2
                _lc = _gc2()
                _lc.execute(
                    """INSERT INTO action_logs
                       (act_id, level, target_id, target_name, action_type,
                        trigger_type, trigger_detail, status, error_msg, operator)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (campaign.get("act_id","") if campaign else "", "campaign",
                     str(campaign_id), (campaign.get("name","") if campaign else "")[:50],
                     "autopilot_launch", "manual", "launch_campaign", "failed", str(e)[:200], "system")
                )
                _lc.commit()
                _lc.close()
            except Exception as _le:
                logger.warning(f"[AutoPilot] log write failed: {_le}")    # ── 素材上传 ──────────────────────────────────────────────────────────────

    def _upload_asset_to_fb(self, act_id: str, asset: dict, token: str) -> dict:
        """
        将本地素材上传到 FB 广告账户。
        图片：返回 {"type": "image", "image_hash": "xxx"}
        视频：返回 {"type": "video", "video_id": "xxx"}
        按 act_id 分别缓存，不同账户重新上传（避免跨账户 image_hash 无效）。
        """
        # 按账户查询缓存（asset_fb_refs 表）
        try:
            _conn = get_conn()
            _row = _conn.execute(
                "SELECT fb_asset_id, fb_type FROM asset_fb_refs WHERE asset_id=? AND act_id=?",
                (asset["id"], act_id)
            ).fetchone()
            _conn.close()
            if _row:
                fb_id, fb_type = _row["fb_asset_id"], _row["fb_type"]
                logger.info(f"[AutoPilot] 素材 {asset['id']} 在账户 {act_id} 已有缓存: {fb_type}={fb_id}")
                if fb_type == "image":
                    return {"type": "image", "image_hash": fb_id}
                else:
                    # 视频缓存中没有缩略图，实时获取
                    _thumb = self._get_video_thumbnail(fb_id, token)
                    return {"type": "video", "video_id": fb_id, "thumbnail_url": _thumb}
        except Exception as _e:
            logger.warning(f"[AutoPilot] 查询 asset_fb_refs 失败（将重新上传）: {_e}")

        file_path = asset["file_path"]
        act_id_num = act_id.replace("act_", "")
        if self._is_local_candidate():
            file_name = str(asset.get("file_name") or file_path).replace("\\", "/").rsplit("/", 1)[-1]
            mime = mimetypes.guess_type(file_name or file_path)[0] or (
                "image/jpeg" if asset["file_type"] == "image" else "video/mp4"
            )
            with open(file_path, "rb") as f:
                encoded_file = base64.b64encode(f.read()).decode("ascii")

            if asset["file_type"] == "image":
                data = self._run_local_graph_task(
                    "graph_upload",
                    f"act_{act_id_num}/adimages",
                    {
                        "graph_host": "graph",
                        "fields": {},
                        "files": [{
                            "field": "filename",
                            "name": file_name,
                            "mime": mime,
                            "base64": encoded_file,
                        }],
                        "_timeout_sec": 180,
                    },
                    timeout_seconds=180,
                )
                if "error" in data:
                    raise Exception(f"Image upload failed: {self._format_fb_error(data['error'])}")
                images = data.get("images", {})
                if not images:
                    raise Exception(f"Image upload returned no hash: {data}")
                image_hash = list(images.values())[0]["hash"]
                self._update_asset_fb_ref(asset["id"], image_hash, "image", act_id)
                return {"type": "image", "image_hash": image_hash}

            data = self._run_local_graph_task(
                "graph_upload",
                f"act_{act_id_num}/advideos",
                {
                    "graph_host": "graph-video",
                    "fields": {"title": file_name},
                    "files": [{
                        "field": "source",
                        "name": file_name,
                        "mime": mime,
                        "base64": encoded_file,
                    }],
                    "_timeout_sec": 360,
                },
                timeout_seconds=360,
            )
            if "error" in data:
                raise Exception(f"Video upload failed: {self._format_fb_error(data['error'])}")
            video_id = data.get("id")
            if not video_id:
                raise Exception(f"Video upload returned no id: {data}")
            video_thumbnail_url = self._get_video_thumbnail(video_id, token)
            self._update_asset_fb_ref(asset["id"], video_id, "video", act_id)
            return {"type": "video", "video_id": video_id, "thumbnail_url": video_thumbnail_url}

        if asset["file_type"] == "image":
            with open(file_path, "rb") as f:
                wait_for_write_slot(token, operation=f"launch_upload_image:{act_id}")
                resp = requests.post(
                    f"{FB_API_BASE}/act_{act_id_num}/adimages",
                    data={"access_token": token},
                    files={"filename": f},
                    timeout=60
                )
            data = resp.json()
            if "error" in data:
                note_write_failure(token, data, operation=f"launch_upload_image:{act_id}")
                raise Exception(f"图片上传失败: {data['error'].get('message', str(data))}")
            images = data.get("images", {})
            if not images:
                raise Exception(f"图片上传响应异常: {data}")
            # 取第一个文件的 hash
            image_hash = list(images.values())[0]["hash"]
            # 写回数据库
            self._update_asset_fb_ref(asset["id"], image_hash, "image", act_id)
            return {"type": "image", "image_hash": image_hash}

        else:  # video
            with open(file_path, "rb") as f:
                wait_for_write_slot(token, operation=f"launch_upload_video:{act_id}")
                resp = requests.post(
                    f"https://graph-video.facebook.com/{FB_API_VERSION}/act_{act_id_num}/advideos",
                    data={"access_token": token, "title": asset["file_name"]},
                    files={"source": f},
                    timeout=300
                )
            data = resp.json()
            if "error" in data:
                note_write_failure(token, data, operation=f"launch_upload_video:{act_id}")
                raise Exception(f"视频上传失败: {data['error'].get('message', str(data))}")
            video_id = data.get("id")
            if not video_id:
                raise Exception(f"视频上传响应异常: {data}")
            # 获取视频缩略图（FB API 要求 video_data 必须包含 image_hash 或 image_url）
            video_thumbnail_url = self._get_video_thumbnail(video_id, token)
            self._update_asset_fb_ref(asset["id"], video_id, "video", act_id)
            return {"type": "video", "video_id": video_id, "thumbnail_url": video_thumbnail_url}

    def _get_video_thumbnail(self, video_id: str, token: str) -> str:
        """
        获取已上传视频的缩略图 URL。
        FB API: GET /{video_id}?fields=picture&access_token=...
        视频刚上传时可能还在处理中，最多等待 30 秒重试。
        """
        for attempt in range(6):  # 最多重试 6 次，每次等 5 秒
            try:
                data = self._fb_get(video_id, token, {"fields": "picture,thumbnails"})
                if "error" in data:
                    logger.warning(f"[AutoPilot] 获取视频缩略图失败: {data['error'].get('message')}")
                    break
                # 优先取 picture 字段（视频封面图 URL）
                picture = data.get("picture")
                if picture:
                    logger.info(f"[AutoPilot] 视频 {video_id} 缩略图: {picture[:60]}...")
                    return picture
                # 尝试 thumbnails 字段
                thumbs = data.get("thumbnails", {}).get("data", [])
                if thumbs:
                    return thumbs[0].get("uri", "")
                # 视频还在处理中，等待后重试
                if attempt < 5:
                    logger.info(f"[AutoPilot] 视频 {video_id} 缩略图暂未就绪，等待 5 秒后重试 ({attempt+1}/6)...")
                    time.sleep(5)
            except Exception as _e:
                logger.warning(f"[AutoPilot] 获取视频缩略图异常: {_e}")
                break
        logger.warning(f"[AutoPilot] 视频 {video_id} 无法获取缩略图，将使用空字符串")
        return ""

    def _update_asset_fb_ref(self, asset_id: int, fb_id: str, fb_type: str, act_id: str = ""):
        conn = get_conn()
        # 写入 asset_fb_refs 表（按账户缓存，避免跨账户复用）
        if act_id:
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO asset_fb_refs (asset_id, act_id, fb_asset_id, fb_type, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (asset_id, act_id, fb_id, fb_type, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
                )
            except Exception as _e:
                logger.warning(f"[AutoPilot] 写入 asset_fb_refs 失败: {_e}")
        # 同时更新 ad_assets 表（兼容旧逻辑）
        conn.execute(
            "UPDATE ad_assets SET fb_asset_id=?, fb_asset_type=?, updated_at=? WHERE id=?",
            (fb_id, fb_type, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), asset_id)
        )
        conn.commit()
        conn.close()

    # ── Campaign 创建 ─────────────────────────────────────────────────────────

    def _create_campaign(
        self,
        act_id: str,
        name: str,
        objective: str,
        token: str,
        daily_budget: Optional[float] = None,
        currency: str = "USD",
        budget_mode: str = "ABO",
    ) -> str:
        """创建 Facebook Campaign"""
        act_id_num = act_id.replace("act_", "")
        # 将内部 objective 值映射为 FB API 接受的标准值
        _OBJECTIVE_MAP = {
            "OUTCOME_VIDEO_VIEWS": "VIDEO_VIEWS",
            "OUTCOME_MESSAGES":    "OUTCOME_ENGAGEMENT",
            "OUTCOME_MESSAGING":   "OUTCOME_ENGAGEMENT",  # 旧版兼容
            "MESSAGES":            "OUTCOME_ENGAGEMENT",  # 前端传MESSAGES→映射为OUTCOME_ENGAGEMENT（FB API v25+不支持MESSAGES）
        }
        fb_objective = _OBJECTIVE_MAP.get(objective, objective)
        payload = {
            "name": name,
            "objective": fb_objective,
            "status": "ACTIVE",
            "special_ad_categories": [],
        }
        # 所有 OUTCOME_* 目标都使用 AUCTION 竞价
        payload["buying_type"] = "AUCTION"
        budget_mode = str(budget_mode or "ABO").strip().upper()
        if budget_mode == "CBO":
            campaign_budget_units = _fb_money_units(float(daily_budget or 0), (currency or "USD").upper())
            if campaign_budget_units <= 0:
                raise ValueError("CBO 模式必须配置大于 0 的系列日预算")
            payload["daily_budget"] = campaign_budget_units
        else:
            payload["is_adset_budget_sharing_enabled"] = False

        data = self._fb_post(f"act_{act_id_num}/campaigns", token, payload)
        return data["id"]

    # ── AdSet 创建 ────────────────────────────────────────────────────────────

    def _create_adset(
        self, act_id: str, campaign_id: str, name: str,
        audience: dict, daily_budget: Optional[float],
        target_cpa: Optional[float], objective: str,
        pixel_id: str, token: str,
        bid_strategy: str = "LOWEST_COST_WITHOUT_CAP",
        budget_mode: str = "ABO",
        placements: Optional[dict] = None,
        conversion_event: str = "PURCHASE",
        beneficiary: str = "",
        payer: str = "",
        tw_verified_id: str = "",
        page_id: str = "",
        conversion_goal: str = ""
    ) -> str:
        """创建 Facebook AdSet"""
        act_id_num = act_id.replace("act_", "")
        # daily_budget is already converted to the account currency by run_campaign.
        # 从账户查询货币类型（通过 act_id 推断）
        _budget_currency = "USD"
        try:
            _act_id_with_prefix = f"act_{act_id_num}"
            _tmp_conn = get_conn()
            _acc_row = _tmp_conn.execute("SELECT currency FROM accounts WHERE act_id=?", (_act_id_with_prefix,)).fetchone()
            _tmp_conn.close()
            if _acc_row:
                _budget_currency = _acc_row["currency"].upper()
        except Exception:
            pass  # 查询失败时默认为 USD
        budget_mode = str(budget_mode or "ABO").strip().upper()
        if budget_mode == "CBO":
            budget_cents = None
        else:
            budget_cents = _fb_money_units(float(daily_budget or 0), _budget_currency)
            if budget_cents <= 0:
                raise ValueError("ABO 模式必须配置大于 0 的广告组日预算")
        # 规范化 objective：将旧版/前端传来的 MESSAGES 映射为 OUTCOME_ENGAGEMENT
        _OBJ_NORMALIZE = {
            "OUTCOME_MESSAGES":  "OUTCOME_ENGAGEMENT",
            "MESSAGES":         "OUTCOME_ENGAGEMENT",
            "OUTCOME_MESSAGING": "OUTCOME_ENGAGEMENT",
        }
        _raw_objective = objective
        _norm_objective, _norm_goal = _normalize_campaign_goal_fields(_raw_objective, conversion_goal)
        conversion_goal = _norm_goal
        objective = _OBJ_NORMALIZE.get(_norm_objective, _norm_objective)

        targeting = dict(audience["targeting"])
        # 如果指定了手动版位，将其合并到 targeting（v4.0: 支持 device_platforms）
        if placements and placements.get("publisher_platforms"):
            targeting["publisher_platforms"] = placements["publisher_platforms"]
            if placements.get("facebook_positions"):
                # ── 修复: FB API 不接受 facebook_positions 中的 "reels"
                # "reels" 是 Instagram Reels，必须放入 instagram_positions
                _raw_fp = placements["facebook_positions"]
                _fb_only = [p for p in _raw_fp if p != "reels"]
                _reels_from_fp = ["reels"] if "reels" in _raw_fp else []
                if _fb_only:
                    targeting["facebook_positions"] = _fb_only
                # 将 reels 合并到 instagram_positions
                if _reels_from_fp:
                    _existing_ip = list(placements.get("instagram_positions") or [])
                    if "reels" not in _existing_ip:
                        _existing_ip.append("reels")
                    placements = dict(placements)  # 避免修改原始对象
                    placements["instagram_positions"] = _existing_ip
            if placements.get("instagram_positions"):
                targeting["instagram_positions"] = placements["instagram_positions"]
            if placements.get("audience_network_positions"):
                targeting["audience_network_positions"] = placements["audience_network_positions"]
            if placements.get("messenger_positions"):
                targeting["messenger_positions"] = placements["messenger_positions"]
        # v4.0: device_platforms 独立处理（不依赖 publisher_platforms，自动版位时也生效）
        if placements and placements.get("device_platforms"):
            targeting["device_platforms"] = placements["device_platforms"]

        # 根据 conversion_goal 决定 optimization_goal
        opt_goal = self._get_optimization_goal(objective, conversion_goal)

        payload = {
            "name": name,
            "campaign_id": campaign_id,
            "billing_event": "IMPRESSIONS",
            "optimization_goal": opt_goal,
            "targeting": targeting,
            "status": "ACTIVE",
            # 不传 start_time → FB 立即生效（避免时区错误导致排期）
        }
        if budget_mode != "CBO":
            payload["daily_budget"] = budget_cents

        # 根据目标类型 + 转化目的设置 promoted_object
        # 测试验证结论（2026-04）：
        # OUTCOME_ENGAGEMENT: 不需要 promoted_object，goal 用 REACH/LINK_CLICKS/IMPRESSIONS/CONVERSATIONS/LANDING_PAGE_VIEWS
        # OUTCOME_TRAFFIC: 不需要 promoted_object（传 pixel 反而报错），goal 用 LINK_CLICKS/LANDING_PAGE_VIEWS/REACH/IMPRESSIONS/CONVERSATIONS
        # OUTCOME_AWARENESS: 不需要 promoted_object，goal 用 REACH/IMPRESSIONS
        # OUTCOME_LEADS: LEAD_GENERATION goal 需要 promoted_object:{page_id}，其他不需要
        # OUTCOME_SALES/LEADS + OFFSITE_CONVERSIONS: 需要 promoted_object:{pixel_id, custom_event_type}
        # MESSAGES（消息互动）: 使用 CONVERSATIONS optimization_goal
        # 组件完整性预检查：提前抛出清晰的中文错误，而不是让 FB API 报英文错误
        if opt_goal in ("OFFSITE_CONVERSIONS", "VALUE"):
            if not pixel_id:
                raise ValueError(
                    f"选择「网站转化」或「转化价値」转化目的时，Pixel 像素为必填。"
                    f"请在账户详情「主页/像素」中配置 Pixel 像素，或在铺广告高级设置中选择像素。"
                    f"当前转化目的：{conversion_goal}"
                )
            # 网站转化：需要真实 Pixel 像素 + 转化事件类型
            po = {"pixel_id": pixel_id}
            if conversion_event:
                po["custom_event_type"] = conversion_event
            payload["promoted_object"] = po
        elif opt_goal in ("MESSAGING_PURCHASE_CONVERSION", "MESSAGING_APPOINTMENT_CONVERSION"):
            if not page_id:
                raise ValueError(
                    f"选择消息类转化目的（{opt_goal}）时，主页 ID 为必填。"
                    f"请在账户详情中配置主页，并确保主页已接入 Messenger 或 WhatsApp Business。"
                )
            # 消息购买/预约：需要 page_id
            payload["promoted_object"] = {"page_id": page_id}
        elif objective == "OUTCOME_LEADS" and opt_goal == "LEAD_GENERATION":
            if not page_id:
                raise ValueError(
                    f"「即时表单」线索目标需要主页 ID。"
                    f"请在账户详情中配置主页，或在铺广告高级设置中选择主页。"
                )
            payload["promoted_object"] = {"page_id": page_id}
            # Lead Form（Instant Forms）广告必须设置 destination_type=ON_AD
            # FB API 要求：Conversion Location = "Instant forms" 对应 destination_type=ON_AD
            payload["destination_type"] = "ON_AD"
        elif opt_goal == "PAGE_LIKES":
            # 主页赞：需要 page_id + destination_type=ON_PAGE
            # FB API 官方文档：PAGE_LIKES → OUTCOME_ENGAGEMENT + destination_type: ON_PAGE
            if not page_id:
                raise ValueError(
                    f"【主页赞】目标需要配置主页 ID。"
                    f"请在账户详情中配置主页，或在铺广告高级设置中选择主页。"
                )
            payload["promoted_object"] = {"page_id": page_id}
            payload["destination_type"] = "ON_PAGE"
        elif opt_goal == "POST_ENGAGEMENT" and page_id:
            # 帖子互动（非主页赞）：有 page_id 时设置 promoted_object
            payload["promoted_object"] = {"page_id": page_id}
        elif objective == "OUTCOME_APP_PROMOTION":
            # 应用推广需要 application_id（暂用 pixel 底底）
            if pixel_id:
                payload["promoted_object"] = {"pixel_id": pixel_id}

        # 购物广告（网站转化）：强制设置 destination_type=WEBSITE，确保只投放网站转化
        # 避免 FB 默认包含 APP 等其他渠道，保证单网站转化逻辑
        if objective == "OUTCOME_SALES" and opt_goal in ("OFFSITE_CONVERSIONS", "VALUE", "LINK_CLICKS", "LANDING_PAGE_VIEWS"):
            payload["destination_type"] = "WEBSITE"
        # 消息类目标：设置 destination_type=MESSENGER，确保 Conversion Location 正确
        # 适用于：CONVERSATIONS（聊天对话）optimization_goal
        if opt_goal == "CONVERSATIONS":
            payload["destination_type"] = "MESSENGER"
            # 消息类需要 page_id 作为 promoted_object
            if page_id:
                payload["promoted_object"] = {"page_id": page_id}
            # FB API要求：使用 destination_type=MESSENGER 时，publisher_platforms 必须包含 messenger
            # 否则报 Invalid parameter。自动补充 messenger 版位。
            _pp = targeting.get("publisher_platforms", [])
            if _pp and "messenger" not in _pp:
                targeting["publisher_platforms"] = list(_pp) + ["messenger"]
                if "messenger_positions" not in targeting:
                    targeting["messenger_positions"] = ["messenger_home"]
            elif not _pp:
                # 自动版位时不强制设置，FB会自动处理
                pass
            payload["targeting"] = targeting
        bid_amount_units = None
        if target_cpa and target_cpa > 0:
            # Frontend stores target CPA in USD. FB bid_amount must use account currency units.
            target_cpa_local = _usd_to_account_currency(float(target_cpa), _budget_currency)
            bid_amount_units = _fb_money_units(target_cpa_local, _budget_currency)
            if _budget_currency != "USD":
                logger.info(
                    "[AutoPilot] 目标 CPA 换算: %s USD -> %s %s -> bid_amount=%s",
                    target_cpa, target_cpa_local, _budget_currency, bid_amount_units,
                )

        # 出价策略：如果设了 target_cpa 且策略为 COST_CAP/BID_CAP，优先使用 CPA
        if target_cpa and target_cpa > 0 and bid_strategy in ("COST_CAP", "BID_CAP"):
            payload["bid_strategy"] = bid_strategy
            payload["bid_amount"] = bid_amount_units
        elif target_cpa and target_cpa > 0:
            # 有 CPA 但策略为自动，使用 COST_CAP
            payload["bid_strategy"] = "COST_CAP"
            payload["bid_amount"] = bid_amount_units
        else:
            if bid_strategy in ("COST_CAP", "BID_CAP"):
                logger.warning(
                    "[AutoPilot] bid_strategy=%s requires target_cpa; fallback to LOWEST_COST_WITHOUT_CAP",
                    bid_strategy,
                )
                payload["bid_strategy"] = "LOWEST_COST_WITHOUT_CAP"
            else:
                payload["bid_strategy"] = bid_strategy

        # 需要认证国家：按国家设置对应的区域声明类别与身份字段。
        countries = targeting.get("geo_locations", {}).get("countries", [])
        regulated_countries = [c for c in countries if c in REGIONAL_REGULATION_CONFIG]
        regional_configs = [REGIONAL_REGULATION_CONFIG[c] for c in regulated_countries]
        regional_categories = sorted({cfg[0] for cfg in regional_configs})
        is_taiwan = "TAIWAN_UNIVERSAL" in regional_categories
        is_singapore = "SINGAPORE_UNIVERSAL" in regional_categories
        needs_regulated_identity = bool(regulated_countries)
        if needs_regulated_identity and not (tw_verified_id or beneficiary):
            raise ValueError(
                f"{'/'.join(regulated_countries)} 属于需要认证的国家，但当前矩阵还没有可用的 Verified ID。"
                "请先在主页库为对应矩阵填写 Verified ID 后再投放。"
            )
        if regional_categories:
            payload["regional_regulated_categories"] = regional_categories
            if tw_verified_id:
                identities = {}
                for _category, beneficiary_key, payer_key in regional_configs:
                    identities[beneficiary_key] = tw_verified_id
                    identities[payer_key] = tw_verified_id
                payload["regional_regulation_identities"] = identities
            elif is_singapore:
                raise ValueError(
                    "新加坡投放需要 Verified Identity ID，"
                    "当前矩阵还没有配置有效的 Verified ID，"
                    "请先在主页库为对应矩阵的主页填写 Verified Identity ID。"
                )
            elif beneficiary:
                payload["beneficiary"] = beneficiary
                if payer:
                    payload["payer"] = payer
        # FB API 要求：必须明确设置 Advantage Audience 标志（0=关闭手动定向，1=开启AI扩量）
        # error_subcode 1870227: To create your ad set, you need to enable or disable the Advantage audience feature
        targeting["targeting_automation"] = {"advantage_audience": 0}
        payload["targeting"] = targeting
        try:
            data = self._fb_post(f"act_{act_id_num}/adsets", token, payload)
        except Exception as e:
            err_msg = str(e)
            err_lower = err_msg.lower()
            should_broad_retry = (
                "2446395" in err_msg
                or "deprecated_interest_id" in err_msg
                or "细分定位选项已合并" in err_msg
                or ("merged" in err_lower and "target" in err_lower)
                or "please update your targeting" in err_lower
            )
            if should_broad_retry:
                _has_interests = bool(
                    targeting.get("flexible_spec") or targeting.get("interests")
                )
                if _has_interests:
                    logger.warning(f"[AutoPilot] 细分定位不可用，自动降级为宽泛受众重试: {name} | {err_msg}")
                    _broad_targeting = {k: v for k, v in targeting.items()
                                        if k not in ("flexible_spec", "interests")}
                    payload["targeting"] = _broad_targeting
                    payload["name"] = name + "-BROAD"
                    try:
                        data = self._fb_post(f"act_{act_id_num}/adsets", token, payload)
                        logger.info(f"[AutoPilot] ✅ 宽泛受众降级成功: {data.get('id')}")
                        return data["id"]
                    except Exception as broad_err:
                        raise Exception(f"细分定位不可用，宽泛受众降级也失败: {broad_err}") from broad_err
            if is_taiwan and "2490408" in err_msg and "optimization_goal" in err_msg:
                # 台湾认证广告（TAIWAN_UNIVERSAL）对 optimization_goal 有严格限制
                # 实测支持：OFFSITE_CONVERSIONS、LINK_CLICKS、LANDING_PAGE_VIEWS、REACH、IMPRESSIONS、CONVERSATIONS
                # 不支持：PAGE_LIKES、POST_ENGAGEMENT、VIDEO_VIEWS、THRUPLAY、LEAD_GENERATION 等
                _tw_supported = "OFFSITE_CONVERSIONS（像素购买）、LINK_CLICKS（流量点击）、LANDING_PAGE_VIEWS（落地页浏览）、REACH（覆盖人数）、CONVERSATIONS（私信对话）"
                raise Exception(
                    f"台湾认证广告不支持当前投放目标（{opt_goal}）。"
                    f"台湾广告（TAIWAN_UNIVERSAL）仅支持以下投放目标：{_tw_supported}。"
                    f"请在铺广告时选择「像素购买」或「流量」目标。"
                )
            if is_taiwan and ("3858495" in err_msg or "3858498" in err_msg or "advertiser" in err_msg.lower() or "taiwan" in err_msg.lower()):
                raise Exception(
                    f"台湾广告合规错误：该广告账户尚未在 Facebook 广告管理后台完成台湾广告声明（TAIWAN_UNIVERSAL）。"
                    f"请前往 https://www.facebook.com/business/help/983527276402621 完成账户级别的广告主身份验证后重试。"
                    f"原始错误：{err_msg}"
                )
            if is_singapore and ("singapore" in err_lower or "新加坡" in err_msg or "regional regulated" in err_lower):
                raise Exception(
                    f"新加坡广告声明缺失或无效：系统已按 SINGAPORE_UNIVERSAL 传入区域声明。"
                    f"请确认当前矩阵认证主页的 Verified ID 已在广告后台通过新加坡通用声明审核。"
                    f"原始错误：{err_msg}"
                )
            raise
        return data["id"]

    def _get_optimization_goal(self, objective: str, conversion_goal: str = "") -> str:
        """
        Facebook Outcome-Based Objectives + conversion_goal → AdSet optimization_goal 映射
        根据 2026-04 实测验证结果，列出每个 Objective 下实际可用的 goal
        """
        objective, conversion_goal = _normalize_campaign_goal_fields(objective, conversion_goal)
        # 每个 Objective 下经过实测验证的可用 optimization_goal
        VALID_GOALS = {
            # 互动：REACH/LINK_CLICKS/IMPRESSIONS/CONVERSATIONS/LANDING_PAGE_VIEWS 完整测试通过
            "OUTCOME_ENGAGEMENT": {
                "reach":              "REACH",
                "link_clicks":        "LINK_CLICKS",
                "impressions":        "IMPRESSIONS",
                "conversations":      "CONVERSATIONS",
                "landing_page_views": "LANDING_PAGE_VIEWS",
                # 视频类目标（需要视频素材）
                "video_views":        "VIDEO_VIEWS",
                "thruplay":           "THRUPLAY",
                # 主页赞：optimization_goal 必须为 PAGE_LIKES，配合 destination_type=ON_PAGE
                "page_likes":         "PAGE_LIKES",
                # 消息类（MESSAGES objective 映射到 OUTCOME_ENGAGEMENT 后使用这些 goal）
                "messaging_purchase_conversion":  "MESSAGING_PURCHASE_CONVERSION",
                "messaging_appointment_conversion": "MESSAGING_APPOINTMENT_CONVERSION",
                # 默认
                "": "REACH",
            },
            # 流量：LINK_CLICKS/LANDING_PAGE_VIEWS/REACH/IMPRESSIONS/CONVERSATIONS 完整测试通过
            "OUTCOME_TRAFFIC": {
                "link_clicks":        "LINK_CLICKS",
                "landing_page_views": "LANDING_PAGE_VIEWS",
                "reach":              "REACH",
                "impressions":        "IMPRESSIONS",
                "conversations":      "CONVERSATIONS",
                "": "LINK_CLICKS",
            },
            # 品牌认知：REACH/IMPRESSIONS 完整测试通过
            "OUTCOME_AWARENESS": {
                "reach":      "REACH",
                "impressions": "IMPRESSIONS",
                "": "REACH",
            },
            # 线索：LEAD_GENERATION(需 page_id)/LINK_CLICKS 完整测试通过
            "OUTCOME_LEADS": {
                "lead_generation":               "LEAD_GENERATION",
                "link_clicks":                   "LINK_CLICKS",
                "landing_page_views":            "LANDING_PAGE_VIEWS",
                "impressions":                   "IMPRESSIONS",
                # 网站转化线索（需要 Pixel）
                "offsite_conversions":           "OFFSITE_CONVERSIONS",
                # 消息线索（Messenger/WhatsApp）
                "messaging_purchase_conversion": "MESSAGING_PURCHASE_CONVERSION",
                "conversations":                 "CONVERSATIONS",
                "": "LEAD_GENERATION",
            },
            # 销售：LINK_CLICKS/REACH 完整测试通过（OFFSITE_CONVERSIONS 需要真实 pixel）
            "OUTCOME_SALES": {
                "link_clicks":                    "LINK_CLICKS",
                "reach":                          "REACH",
                "impressions":                    "IMPRESSIONS",
                "landing_page_views":             "LANDING_PAGE_VIEWS",
                "conversations":                  "CONVERSATIONS",
                # 网站转化（需要真实 Pixel）
                "offsite_conversions":            "OFFSITE_CONVERSIONS",
                "value":                          "VALUE",
                # 消息购买（Messenger/WhatsApp）
                "messaging_purchase_conversion":  "MESSAGING_PURCHASE_CONVERSION",
                # 目录销售（需要商品目录）
                "product_catalog_sales":          "OFFSITE_CONVERSIONS",
                "": "LINK_CLICKS",
            },
            # 应用推广
            "OUTCOME_APP_PROMOTION": {
                "": "APP_INSTALLS",
            },
            # 视频观看（支持 VIDEO_VIEWS 和旧版 OUTCOME_VIDEO_VIEWS）
            "OUTCOME_VIDEO_VIEWS": {
                "video_views": "VIDEO_VIEWS",
                "thruplay":    "THRUPLAY",
                "": "VIDEO_VIEWS",
            },
            "VIDEO_VIEWS": {
                "video_views": "VIDEO_VIEWS",
                "thruplay":    "THRUPLAY",
                "": "VIDEO_VIEWS",
            },
            "OUTCOME_MESSAGES": {
                "conversations": "CONVERSATIONS",
                "": "CONVERSATIONS",
            },
            # 消息互动（FB API 标准值 MESSAGES）
            "MESSAGES": {
                "conversations": "CONVERSATIONS",
                "": "CONVERSATIONS",
            },
            # 门店访问
            "OUTCOME_STORE_VISITS": {
                "": "STORE_VISITS",
            },
        }
        goal_key = (conversion_goal or "").lower().strip()
        obj_map = VALID_GOALS.get(objective, {})
        if goal_key in obj_map:
            return obj_map[goal_key]
        # 如果传入的 conversion_goal 不在映射表中，返回默认值
        return obj_map.get("", "LINK_CLICKS")

    # ── AI 自动生成消息/表单内容 ──────────────────────────────────────────────
    def _call_text_ai(self, prompt: str) -> str:
        """调用文本AI（DeepSeek）生成内容"""
        try:
            from openai import OpenAI
            api_key = self._get_setting("ai_api_key", "")
            api_base = self._get_setting("ai_api_base", "https://api.deepseek.com/v1")
            model = self._get_setting("ai_model", "deepseek-chat")
            if not api_key:
                return ""
            client = OpenAI(api_key=api_key, base_url=api_base)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1024,
                temperature=0.7
            )
            return resp.choices[0].message.content.strip() if resp.choices else ""
        except Exception as e:
            logger.warning(f"[AutoPilot] AI文本生成失败: {e}")
            return ""

    def _ai_gen_msg_template(
        self,
        body: str,
        headline: str,
        asset_info: dict = None,
        ad_language: str = "",
        target_countries=None,
    ) -> dict:
        """根据素材AI分析结果，生成与投放语言一致的 Messenger 欢迎消息模板。"""
        import json as _json
        ai_analysis = ""
        ai_purpose = ""
        if asset_info:
            ai_analysis = asset_info.get("ai_analysis") or ""
            ai_purpose = asset_info.get("ai_purpose") or "general"

        ctx = self._resolve_language_context(ad_language, target_countries, asset_info)
        lang_code = ctx["language"]
        lang_hint = ctx["label"]
        avoid_chinese_hint = "除非目标语言本身是中文，否则绝不要输出中文。" if lang_code not in ("zh", "zh-tw") else ""

        prompt = (
            "你是一位Facebook广告专家，请根据以下广告内容，生成一个高度相关的Messenger欢迎消息模板。\n\n"
            f"广告标题：{headline}\n"
            f"广告文案：{body[:300]}\n"
            f"素材分析：{ai_analysis[:200]}\n"
            f"投放目的：{ai_purpose}\n\n"
            "【核心要求】\n"
            "1. 欢迎语（welcome_text）：\n"
            "   - 必须直接承接广告内容，让用户感觉是广告的自然延续\n"
            "   - 提及广告中的具体产品、机会或卖点，不能泛泛而谈\n"
            f"   - 必须使用 {lang_hint}，50-150字符\n"
            "2. 快捷问题（ice_breakers）：\n"
            "   - 必须是用户针对该广告的具体产品/服务会真实提问的问题\n"
            "   - 禁止使用通用问题\n"
            f"   - 必须使用 {lang_hint}，问题标题不超过80字符，回复不超过300字符\n"
            f"3. {avoid_chinese_hint}\n\n"
            "请用JSON格式返回（3个ice_breaker）：\n"
            '{"welcome_text": "欢迎语", "ice_breakers": [{"title": "具体问题1", "response": "针对性回复1"}, {"title": "具体问题2", "response": "针对性回复2"}, {"title": "具体问题3", "response": "针对性回复3"}]}\n'
            "只返回JSON，不要其他内容。"
        )
        raw = self._call_text_ai(prompt)
        ai_result = {}
        if raw:
            try:
                if "```" in raw:
                    parts = raw.split("```")
                    for part in parts[1::2]:
                        part = part.lstrip("json").strip()
                        if part.startswith("{"):
                            raw = part
                            break
                ai_result = _json.loads(raw)
            except Exception:
                pass

        fallback_template = self._default_msg_template(ctx, headline)
        welcome_text = str(ai_result.get("welcome_text") or "").strip() or fallback_template["welcome_text"]
        if lang_code not in ("zh", "zh-tw") and self._contains_cjk(welcome_text):
            welcome_text = fallback_template["welcome_text"]

        ice_breakers_raw = ai_result.get("ice_breakers") or []
        if not ice_breakers_raw:
            buttons_raw = ai_result.get("buttons") or []
            ice_breakers_raw = [{"title": b, "response": b} for b in buttons_raw[:3]]

        logger.info(f"[AutoPilot] AI生成Messenger欢迎消息: {welcome_text[:60]}... ice_breakers={len(ice_breakers_raw)}个")

        ice_breakers = []
        for item in ice_breakers_raw[:3]:
            if isinstance(item, dict):
                title = str(item.get("title") or "").strip()[:80]
            else:
                title = str(item).strip()[:80]
            if not title:
                continue
            if lang_code not in ("zh", "zh-tw") and self._contains_cjk(title):
                continue
            ice_breakers.append({"title": title, "response": ""})

        if not ice_breakers:
            ice_breakers = fallback_template["ice_breakers"]

        return {
            "type": "VISUAL_EDITOR",
            "version": 2,
            "landing_screen_type": "welcome_message",
            "media_type": "text",
            "text_format": {
                "customer_action_type": "ice_breakers",
                "message": {
                    "ice_breakers": ice_breakers,
                    "quick_replies": [],
                    "text": welcome_text[:300]
                }
            },
            "user_edit": False,
            "surface": "visual_editor_new"
        }

    def _ai_gen_lead_form_content(
        self,
        body: str,
        headline: str,
        asset_info: dict = None,
        ad_language: str = "",
        target_countries=None,
    ) -> dict:
        """根据素材AI分析结果，生成更贴近投放语言的 Lead Form 内容。"""
        import json as _json
        ai_analysis = ""
        ai_purpose = ""
        if asset_info:
            ai_analysis = asset_info.get("ai_analysis") or ""
            ai_purpose = asset_info.get("ai_purpose") or "general"

        ctx = self._resolve_language_context(ad_language, target_countries, asset_info)
        lang_code = ctx["language"]
        fallback_spec = self._localized_lead_form_fallback(ctx)

        prompt = (
            "你是一位Facebook广告转化专家。请先仔细阅读以下广告素材，判断这条广告的转化意图，再生成匹配的Lead Form内容。\n\n"
            f"【广告标题】{headline}\n"
            f"【广告文案】{body}\n"
            f"【素材分析】{ai_analysis or '（无额外分析，请根据标题和文案自行判断）'}\n"
            f"【投放目的】{ai_purpose or 'general'}\n\n"
            "请按以下步骤思考并生成表单内容：\n\n"
            "第1步：先判断这条广告的转化类型，从下面选一个最匹配的：\n"
            "- consultation（咨询/预约通话）\n"
            "- quote（获取报价/估价）\n"
            "- download（下载资料/清单/指南）\n"
            "- contact（添加联系方式/加微信/加WhatsApp）\n"
            "- register（注册/报名/加入）\n"
            "- purchase（下单/购买/限时优惠）\n\n"
            f"第2步：根据上面判断的转化类型，生成以下内容（全部字段不能为空，必须使用 {ctx['label']}）：\n"
            f"  - form_title: 表单标题，要明确告诉用户填完表单能得到什么（不超过40字）\n"
            f"  - description: 2-3句话说明表单价值，结合广告标题和文案来写（不超过150字）\n"
            f"  - question_label: 1个资格判断问题，帮助筛选真正感兴趣的潜在客户（不超过30字）\n"
            f"  - question_description: 引导用户如何回答这句话（不超过30字）\n"
            f"  - option_a / option_b: 两个选项，各不超过15字\n"
            f"  - thank_you_title: 结束页标题（不超过20字）\n"
            f"  - thank_you_body: 结束页正文，根据转化类型写对应动作——\n"
            f"      consultation \u2192 引导添加联系方式预约通话时间\n"
            f"      quote \u2192 引导留下手机号接收报价\n"
            f"      download \u2192 引导点击按钮下载资料\n"
            f"      contact \u2192 引导添加微信/WhatsApp继续咨询\n"
            f"      register \u2192 引导查看注册确认信息\n"
            f"      purchase \u2192 引导前往领取优惠/下单\n"
            f"    不要提及邮件、回电、短信等。不超过150字。\n"
            f"  - button_text: 结束页按钮文字，呼应转化类型（如\u201c立即预约\u201d\u201c下载指南\u201d\u201c添加微信\u201d等，不超过15字）\n\n"
            "3. 所有内容中性、合规，不夸大收益，不用短链或敏感词。\n\n"
            "返回JSON（只返回JSON，不要其他内容）：\n"
            '{\n'
            '  "conversion_type": "转化类型英文key",\n'
            '  "form_title": "...",\n'
            '  "description": "...",\n'
            '  "question_label": "...",\n'
            '  "question_description": "...",\n'
            '  "option_a": "...",\n'
            '  "option_b": "...",\n'
            '  "thank_you_title": "...",\n'
            '  "thank_you_body": "...",\n'
            '  "button_text": "..."\n'
            '}\n'
        )

        raw = self._call_text_ai(prompt)

        ai_result = {"form_title": "", "qualifying_question": ""}
        if raw:
            try:
                if "```" in raw:
                    parts = raw.split("```")
                    for part in parts[1::2]:
                        part = part.lstrip("json").strip()
                        if part.startswith("{"):
                            raw = part
                            break
                ai_result = _json.loads(raw)
            except Exception:
                pass

        _conversion_type = str(ai_result.get("conversion_type") or "").strip().lower()
        form_title = str(ai_result.get("form_title") or "").strip() or fallback_spec["form_title"]
        description = str(ai_result.get("description") or "").strip()
        question_label = str(ai_result.get("question_label") or "").strip() or fallback_spec["qualifying_question"]
        question_description = str(ai_result.get("question_description") or "").strip()[:120]
        option_a = str(ai_result.get("option_a") or "").strip()[:25]
        option_b = str(ai_result.get("option_b") or "").strip()[:25]
        thank_you_title = str(ai_result.get("thank_you_title") or "").strip()
        thank_you_body = str(ai_result.get("thank_you_body") or "").strip()
        button_text = str(ai_result.get("button_text") or "").strip()

        if lang_code not in ("zh", "zh-tw"):
            if self._contains_cjk(form_title):
                form_title = fallback_spec["form_title"]
            if self._contains_cjk(question_label):
                question_label = fallback_spec["qualifying_question"]

        return {
            "form_title": form_title[:80],
            "description": description[:300],
            "question_label": question_label[:120],
            "question_description": question_description or "",
            "option_a": option_a or fallback_spec.get("option_a", ""),
            "option_b": option_b or fallback_spec.get("option_b", ""),
            "thank_you_title": thank_you_title or fallback_spec.get("thank_you_title", ""),
            "thank_you_body": thank_you_body or fallback_spec.get("thank_you_body", ""),
            "button_text": button_text or fallback_spec.get("button_text", ""),
            "privacy_text": fallback_spec["privacy_text"],
            "contact_field": fallback_spec["contact_field"],
            "conversion_type": _conversion_type or "",
            "locale": ctx["locale"],
        }

    # ── Ad 创建 ───────────────────────────────────────────────────────────────

    def _create_ad(
        self, act_id: str, adset_id: str, name: str,
        headline: str, body: str, page_id: str,
        fb_asset_ref: dict, file_type: str, token: str,
        landing_url: str = "",
        conversion_goal: str = "",
        message_template: str = "",
        lead_form_id: str = "",
        form_link: str = "",
        asset_info: dict = None,
        cta_type: str = "",
        pixel_id: str = "",
        ad_language: str = "",
        target_countries=None,
        fb_campaign_id: str = "",
        fb_campaign_name: str = "",
        adset_name: str = "",
    ) -> str:
        """创建 Facebook Ad（含 AdCreative）"""
        import json as _json
        act_id_num = act_id.replace("act_", "")
        _landing_link_reserved = None
        _link_cache = None
        _link_cache_key = ""
        _goal_lower_for_link = (conversion_goal or "").lower().strip()
        _is_lead_ad_type = _goal_lower_for_link == "lead_generation"
        _tracking_base_url = landing_url or (form_link if _is_lead_ad_type else "")
        if _tracking_base_url:
            try:
                if isinstance(asset_info, dict):
                    _link_cache = asset_info.setdefault("_mira_landing_link_cache", {})
                    _link_cache_key = "|".join([str(act_id or ""), str(adset_id or ""), str(name or ""), str(_tracking_base_url or "")])
                    _landing_link_reserved = _link_cache.get(_link_cache_key)
                _acc_for_link = self._load_account(act_id) or {}
                _account_name_for_link = (_acc_for_link.get("name") or act_id or "").strip()
                if not _landing_link_reserved:
                    _reserve_target_url = form_link if _is_lead_ad_type else ""
                    _landing_link_reserved = self._reserve_landing_ad_link(
                        _tracking_base_url,
                        act_id,
                        _account_name_for_link,
                        fb_campaign_id or "",
                        fb_campaign_name or "",
                        adset_id,
                        adset_name or "",
                        name,
                        target_url=_reserve_target_url,
                    )
                    if _link_cache is not None and _landing_link_reserved:
                        _link_cache[_link_cache_key] = _landing_link_reserved
                if _landing_link_reserved and _landing_link_reserved.get("public_url"):
                    _tracked_landing_url = self._landing_ad_click_url(_landing_link_reserved.get("public_url"))
                    if not form_link or self._landing_link_base(form_link) == self._landing_link_base(_tracking_base_url):
                        form_link = _tracked_landing_url
                    if not landing_url or self._landing_link_base(landing_url) == self._landing_link_base(_tracking_base_url):
                        landing_url = _tracked_landing_url
            except Exception as _link_err:
                logger.warning("[AutoPilot] landing ad link auto-bind skipped: %s", _link_err)

        # ── 消息模板：保留原始值，统一在后面消息广告构建阶段处理 ──
        # 不在此处提前解析，避免双重解析导致 int() 失败
        _msg_template_original = message_template  # 保留原始值（可能是模板ID或JSON字符串）

        # 表单广告：如果没有 form_id，用 AI 根据素材内容自动生成并在主页上创建 Lead Form
        _lead_form_resolved = lead_form_id
        _lead_form_error = ""
        if _is_lead_ad_type:
            if lead_form_id and lead_form_id.strip().isdigit():
                # 传入的是模板 ID，尝试从模板创建
                try:
                    from api.ad_templates import create_lead_form_for_page
                    _fb_form_id = create_lead_form_for_page(
                        page_id,
                        int(lead_form_id.strip()),
                        token=token,
                        follow_up_url=form_link or landing_url or "",
                    )
                    if _fb_form_id:
                        _lead_form_resolved = _fb_form_id
                        logger.info(f"[AutoPilot] Lead Form 模板创建成功: form_id={_fb_form_id}")
                    else:
                        _lead_form_resolved = ""
                except Exception as _e:
                    _lead_form_error = str(_e)
                    logger.warning(f"[AutoPilot] Lead Form 模板创建失败: {_e}")
                    _lead_form_resolved = ""
            if not _lead_form_resolved:
                # 没有 form_id 或模板创建失败：用 AI 生成表单内容并在主页上创建
                logger.info(f"[AutoPilot] Lead Form 无 form_id，尝试用 AI 自动生成表单...")
                _lead_cache_key = None
                try:
                    from api.ad_templates import create_custom_lead_form_for_page
                    _lead_cache_key = (
                        str(page_id or ""),
                        str((asset_info or {}).get("id") or ""),
                        str(form_link or landing_url or (asset_info or {}).get("landing_url") or ""),
                    )
                    if _lead_cache_key in self._runtime_lead_form_cache:
                        _lead_form_resolved = self._runtime_lead_form_cache[_lead_cache_key]
                        logger.info(f"[AutoPilot] 复用本次运行已创建的 Lead Form: form_id={_lead_form_resolved}")
                    elif _lead_cache_key in self._runtime_lead_form_error_cache:
                        _lead_form_error = self._runtime_lead_form_error_cache[_lead_cache_key]
                        logger.warning(f"[AutoPilot] 复用本次运行 Lead Form 失败原因，避免重复触发: {_lead_form_error}")
                        _lead_form_resolved = ""
                    else:
                        _lead_form_spec = self._ai_gen_lead_form_content(
                            body,
                            headline,
                            asset_info,
                            ad_language=ad_language,
                            target_countries=target_countries,
                        )
                        _form_title = _lead_form_spec.get("form_title") or "Get More Information"
                        _question_label = _lead_form_spec.get("question_label") or ""
                        _option_a = _lead_form_spec.get("option_a") or ""
                        _option_b = _lead_form_spec.get("option_b") or ""
                        _description = _lead_form_spec.get("description") or ""
                        _thank_you_title = _lead_form_spec.get("thank_you_title") or ""
                        _thank_you_body = _lead_form_spec.get("thank_you_body") or ""
                        _button_text = _lead_form_spec.get("button_text") or ""
                        _contact_field = _lead_form_spec.get("contact_field") or "EMAIL"
                        _privacy_text = _lead_form_spec.get("privacy_text") or "Privacy Policy"
                        _locale = _lead_form_spec.get("locale") or "en_US"
                        _final_questions = []
                        _question_description = _lead_form_spec.get("question_description") or ""
                        if _question_label and _option_a and _option_b:
                            _q_custom = {
                                "type": "CUSTOM",
                                "label": _question_label,
                                "options": [
                                    {"key": "opt_a", "value": _option_a},
                                    {"key": "opt_b", "value": _option_b},
                                ]
                            }
                            if _question_description:
                                _q_custom["description"] = _question_description
                            _final_questions.append(_q_custom)
                        elif _question_label:
                            _q_simple = {"type": "CUSTOM", "label": _question_label}
                            if _question_description:
                                _q_simple["description"] = _question_description
                            _final_questions.append(_q_simple)
                        _final_questions.append({"type": _contact_field})
                        _privacy_url = form_link or (asset_info or {}).get("landing_url") or landing_url or ""
                        _follow_up_url = form_link or landing_url or (asset_info or {}).get("landing_url") or ""
                        # Route description to thank_you_page.body (FB context_card rejects body key)
                        _ty_body = _thank_you_body or _description
                        _context_card = None  # 不需要欢迎页/context_card，直接展示问题
                        _lead_form_resolved = self._create_lead_form_for_page(
                            page_id,
                            _form_title,
                            _final_questions,
                            token=token,
                            privacy_url=_privacy_url,
                            privacy_text=_privacy_text,
                            follow_up_url=_follow_up_url,
                            locale=_locale,
                            context_card=_context_card,
                            thank_you_title=_thank_you_title or _form_title,
                            thank_you_body=_ty_body,
                            button_text=_button_text,
                        )
                        self._runtime_lead_form_cache[_lead_cache_key] = _lead_form_resolved
                        logger.info(f"[AutoPilot] 默认 Lead Form 创建成功: form_id={_lead_form_resolved}")
                except Exception as _ai_e:
                    _lead_form_error = str(_ai_e)
                    try:
                        if not _lead_cache_key:
                            raise ValueError("empty lead form cache key")
                        self._runtime_lead_form_error_cache[_lead_cache_key] = _lead_form_error
                    except Exception:
                        pass
                    logger.warning(f"[AutoPilot] AI 自动创建 Lead Form 异常: {_ai_e}")
                    _lead_form_resolved = ""
        lead_form_id = _lead_form_resolved

        # ── 判断是否为消息类广告（CTM = Click to Messenger/WhatsApp）──────────
        _MSG_GOALS = {
            "conversations", "messaging_purchase_conversion",
            "messaging_appointment_conversion", "messaging_leads"
        }
        is_msg_ad = (conversion_goal or "").lower().strip() in _MSG_GOALS

        # ── 修复 BUG-3: 购物类广告预检 landing_url ────────────────────
        _SALES_GOALS = {"offsite_conversions", "value", "product_catalog_sales"}
        _goal_lower = (conversion_goal or "").lower().strip()
        if _goal_lower in _SALES_GOALS and not landing_url:
            logger.warning(
                f"[AutoPilot] 购物类广告（{conversion_goal}）未提供落地页链接，"
                f"将使用主页链接作为替代。建议在铺广告时填写真实的购物落地页 URL。"
            )

        # ── 修复: 消息广告预检主页私信功能（error 1885187）────────────
        if is_msg_ad and page_id and token:
            try:
                _page_resp = self._fb_get(page_id, token, {"fields": "messaging_feature_status"})
                _msg_status = _page_resp.get("messaging_feature_status", {})
                # 如果返回了 messaging_feature_status 且 USER_MESSAGING 不是 ENABLED，则预警
                if _msg_status and _msg_status.get("USER_MESSAGING") not in ("ENABLED", None):
                    raise Exception(
                        f"主页 {page_id} 未开启私信功能（USER_MESSAGING={_msg_status.get('USER_MESSAGING')}\uff09，"
                        f"消息广告无法创建。请在 FB 主页设置中开启「允许消息」功能后重试。"
                    )
            except Exception as _pre_err:
                _pre_msg = str(_pre_err)
                # 只在明确知道是私信关闭时才抛出，其他预检失败则记录日志不阻断
                if "未开启私信" in _pre_msg:
                    raise
                logger.debug(f"[AutoPilot] 主页私信预检跳过: {_pre_err}")
        # ── 判断是否为 Lead Form 广告 ─────────────────────────────────────────
        is_lead_ad = (conversion_goal or "").lower().strip() == "lead_generation"
        is_page_likes_ad = (conversion_goal or "").lower().strip() == "page_likes"

        # ── 构建 call_to_action ───────────────────────────────────────────────
        # CTA 类型映射：用户选择的 cta_type → FB API 格式
        _CTA_LINK_TYPES = {
            "SHOP_NOW", "LEARN_MORE", "SIGN_UP", "GET_OFFER",
            "DOWNLOAD", "BOOK_TRAVEL", "CONTACT_US", "SUBSCRIBE",
            "GET_QUOTE", "WATCH_MORE", "APPLY_NOW", "GET_DIRECTIONS",
            "ORDER_NOW", "BUY_NOW", "SEE_MENU"
        }
        _CTA_MSG_TYPES = {
            "MESSAGE_PAGE", "SEND_MESSAGE", "WHATSAPP_MESSAGE",
            "MESSENGER_MESSAGE", "INSTAGRAM_MESSAGE"
        }
        def _build_cta(link: str) -> dict:
            """根据转化目的或用户指定的 cta_type 返回正确的 CTA"""
            # 优先使用用户指定的 cta_type
            if cta_type and cta_type.upper() != "AUTO":
                ct = cta_type.upper()
                if is_msg_ad and ct in _CTA_LINK_TYPES:
                    logger.warning(f"[AutoPilot] 消息广告 CTA={ct} 与目标不兼容，已自动回退到消息类 CTA")
                    ct = ""
                elif not is_msg_ad and ct in _CTA_MSG_TYPES:
                    logger.warning(f"[AutoPilot] 非消息广告 CTA={ct} 与目标不兼容，已自动回退到链接类 CTA")
                    ct = ""
            if cta_type and cta_type.upper() != "AUTO" and ct:
                if ct in _CTA_MSG_TYPES:
                    # 消息类 CTA
                    if ct == "WHATSAPP_MESSAGE":
                        return {"type": "WHATSAPP_MESSAGE", "value": {"app_destination": "WHATSAPP"}}
                    return {"type": ct, "value": {"app_destination": "MESSENGER"}}
                elif ct in _CTA_LINK_TYPES:
                    # 链接类 CTA
                    if ct == "SIGN_UP" and lead_form_id:
                        return {"type": "SIGN_UP", "value": {"lead_gen_form_id": lead_form_id}}
                    return {"type": ct, "value": {"link": link}}
                else:
                    # 未知类型，原样传递（让 FB 验证）
                    return {"type": ct, "value": {"link": link}}
            # 未指定 cta_type，按转化目的自动推断
            if is_page_likes_ad:
                # 主页赞广告：使用 LIKE_PAGE CTA
                return {"type": "LIKE_PAGE"}
            elif is_msg_ad:
                goal_lower = (conversion_goal or "").lower().strip()
                if "whatsapp" in goal_lower or "messaging_appointment" in goal_lower:
                    return {"type": "WHATSAPP_MESSAGE", "value": {"app_destination": "WHATSAPP"}}
                return {"type": "MESSAGE_PAGE", "value": {"app_destination": "MESSENGER"}}
            elif is_lead_ad:
                _signup_link = form_link or link
                return {"type": "SIGN_UP", "value": {"lead_gen_form_id": lead_form_id}} if lead_form_id else {"type": "SIGN_UP", "value": {"link": _signup_link}}
            elif landing_url:
                return {"type": "SHOP_NOW", "value": {"link": link}}
            else:
                return {"type": "LEARN_MORE", "value": {"link": link}}

        # ── 构建 AdCreative ───────────────────────────────────────────────────
        if fb_asset_ref["type"] == "image":
            # PAGE_LIKES 广告：link 使用主页URL（不使用落地页）
            _default_link = (f"https://www.facebook.com/{page_id}" if is_page_likes_ad
                             else (landing_url or f"https://www.facebook.com/{page_id}"))
            link_data = {
                "image_hash": fb_asset_ref["image_hash"],
                "message": body,
                "name": headline,
                "link": _default_link,
                "call_to_action": _build_cta(_default_link),
            }
            object_story_spec = {"page_id": page_id, "link_data": link_data}
        else:  # video
            # PAGE_LIKES 广告：link 使用主页URL（不使用落地页）
            _default_link_v = (f"https://www.facebook.com/{page_id}" if is_page_likes_ad
                               else (landing_url or f"https://www.facebook.com/{page_id}"))
            video_data = {
                "video_id": fb_asset_ref["video_id"],
                "message": body,
                "title": headline,
                "call_to_action": _build_cta(_default_link_v),
            }
            # FB API 要求视频广告必须提供缩略图（image_url 或 image_hash）
            _thumbnail = fb_asset_ref.get("thumbnail_url", "")
            if not _thumbnail:
                # 缩略图为空（可能视频刚上传还在处理中），重新尝试获取
                _thumbnail = self._get_video_thumbnail(fb_asset_ref["video_id"], token)
                logger.info(f"[AutoPilot] 重新获取视频缩略图: {_thumbnail[:60] if _thumbnail else '失败'}")
            if _thumbnail:
                video_data["image_url"] = _thumbnail
            else:
                # 最终兜底：使用 FB 静态默认缩略图（避免 FB API 报 1443226 错误）
                video_data["image_url"] = "https://static.xx.fbcdn.net/rsrc.php/v4/yN/r/AAqMW82PqGg.gif"
                logger.warning(f"[AutoPilot] 视频缩略图获取失败，使用默认占位图")
            object_story_spec = {"page_id": page_id, "video_data": video_data}

        creative_payload = {
            "name": f"[AutoPilot] Creative - {name[:30]}",
            "object_story_spec": object_story_spec
        }

        # ── 消息广告：构建 page_welcome_message（放在 link_data/video_data 内部）──
        if is_msg_ad:
            _page_welcome = None
            # 使用原始模板值（避免双重解析）
            _tpl = _msg_template_original or ""
            if _tpl:
                # 尝试作为模板ID解析
                try:
                    _tid = int(str(_tpl).strip())
                    from api.ad_templates import get_msg_template_fb_format
                    _fb_fmt = get_msg_template_fb_format(_tid)
                    if _fb_fmt:
                        _page_welcome = _fb_fmt
                        logger.info(f"[AutoPilot] 使用消息模板 ID={_tid}")
                    else:
                        logger.warning(f"[AutoPilot] 消息模板 ID={_tid} 不存在，将自动生成")
                except (ValueError, TypeError):
                    # 非数字：可能是 JSON 字符串，直接解析
                    try:
                        _stripped = str(_tpl).strip()
                        if _stripped.startswith("{"):
                            _page_welcome = _json.loads(_stripped)
                            logger.info(f"[AutoPilot] 使用 JSON 格式消息模板")
                    except Exception as _je:
                        logger.warning(f"[AutoPilot] 消息模板 JSON 解析失败: {_je}")
            
            if _page_welcome is None:
                # 无模板或模板无效：调用 DeepSeek AI 根据素材信息自动生成欢迎消息
                _page_welcome = self._ai_gen_msg_template(
                    body,
                    headline,
                    asset_info,
                    ad_language=ad_language,
                    target_countries=target_countries,
                )
                logger.info(f"[AutoPilot] 使用 AI 自动生成消息模板")
            
            # 将 page_welcome_message 注入到 link_data 或 video_data 内部
            _pwm_str = _json.dumps(_page_welcome, ensure_ascii=False)
            if "link_data" in object_story_spec:
                object_story_spec["link_data"]["page_welcome_message"] = _pwm_str
            elif "video_data" in object_story_spec:
                object_story_spec["video_data"]["page_welcome_message"] = _pwm_str
            # 同步更新 creative_payload 中的 object_story_spec
            creative_payload["object_story_spec"] = object_story_spec

        # Lead form campaigns must resolve a real form before creative/ad creation.
        if is_lead_ad and not lead_form_id:
            detail = f" 原始原因：{_lead_form_error}" if _lead_form_error else ""
            self._mark_landing_ad_link(_landing_link_reserved, "failed", error_msg="Lead Form unavailable" + detail)
            raise Exception(
                "当前主页无法提供有效的 Lead Form。请先选择已有表单，或换用有 pages_manage_ads 权限的主页/Token 后重试。"
                + detail
            )

        try:
            creative_data = self._fb_post(
                f"act_{act_id_num}/adcreatives", token, creative_payload
            )
            creative_id = creative_data["id"]

            # 创建 Ad
            _ad_payload = {
                "name": name,
                "adset_id": adset_id,
                "creative": {"creative_id": creative_id},
                "status": "ACTIVE",
            }
            ad_data = self._fb_post(
                f"act_{act_id_num}/ads", token, _ad_payload
            )
            ad_id = ad_data["id"]
            self._mark_landing_ad_link(_landing_link_reserved, "active", ad_id=ad_id)
            if _link_cache is not None and _link_cache_key:
                _link_cache.pop(_link_cache_key, None)
            return ad_id
        except Exception as _ad_create_err:
            _ad_create_msg = str(_ad_create_err)
            if self._is_app_development_mode_error(_ad_create_msg):
                _ad_create_err = Exception(self._format_app_development_mode_error(_ad_create_msg))
            self._mark_landing_ad_link(_landing_link_reserved, "failed", error_msg=str(_ad_create_err))
            raise _ad_create_err

    # ── 兴趣词解析 ───────────────────────────────────────────────────────────
    def _resolve_interests(self, interest_names: list, token: str) -> list:
        """将兴趣词名称列表解析为带id的FB兴趣对象列表"""
        import logging
        logger = logging.getLogger("autopilot")
        resolved = []
        for name in interest_names:
            try:
                data = self._fb_get("search", token, {
                    "type": "adinterest",
                    "q": name,
                    "limit": 5
                })
                items = data.get("data", [])
                matched = None
                for item in items:
                    if item.get("name", "").lower() == name.lower():
                        matched = item
                        break
                if not matched and items:
                    matched = items[0]
                if matched:
                    resolved.append({"id": str(matched["id"]), "name": matched["name"]})
                else:
                    logger.warning(f"Interest not found: {name}")
            except Exception as e:
                logger.warning(f"Failed to resolve interest '{name}': {e}")
        return resolved

    # ── 受众矩阵生成 ──────────────────────────────────────────────────────────

    def _build_audience_groups(
        self, interests: list, countries: list, max_adsets: int,
        age_min: int = 18, age_max: int = 65, gender: int = 0,
        token: str = None, rotation_seed: str = "",
        lang_codes: list = None,
        strategy: str = "broad_interest",
        chunk_size: int = 2,
    ) -> list[dict]:
        """
        根据 AI 推荐的兴趣词，生成受众分组。
        策略：
          - 默认宽泛 + 兴趣分组，保持原投放安全口径
          - 可选仅宽泛/仅兴趣；仅兴趣无有效兴趣时自动兜底宽泛
          - 总组数不超过 max_adsets
        """
        groups = []
        try:
            max_adsets = max(1, min(int(max_adsets or 5), 20))
        except Exception:
            max_adsets = 5
        try:
            chunk_size = max(1, min(int(chunk_size or 2), 5))
        except Exception:
            chunk_size = 2
        strategy = str(strategy or "broad_interest").strip().lower()
        if strategy not in {"broad_interest", "broad_only", "interest_only"}:
            strategy = "broad_interest"
        # 性别设置：0=不限, 1=男, 2=女
        genders = [gender] if gender in (1, 2) else []

        def _base_targeting(extra: dict = None) -> dict:
            t = {
                "geo_locations": {"countries": countries},
                "age_min": age_min,
                "age_max": age_max,
            }
            if genders:
                t["genders"] = genders
            if extra:
                t.update(extra)
            return t

        def _append_broad(name: str = "宽泛受众") -> None:
            if len(groups) < max_adsets:
                groups.append({
                    "name": name,
                    "type": "broad",
                    "targeting": _base_targeting()
                })

        # 宽泛受众（默认保留；仅兴趣策略不主动加入）
        if strategy in {"broad_interest", "broad_only"}:
            _append_broad()
        if strategy == "broad_only":
            return groups[:max_adsets]

        # 兴趣词分组（先解析兴趣词ID）
        clean_interests = []
        seen_interests = set()
        for item in interests or []:
            name = str(item or "").strip()
            key = name.lower()
            if not key or key in seen_interests:
                continue
            seen_interests.add(key)
            clean_interests.append(name)
        resolved_interests = []
        if clean_interests and token:
            resolved_interests = self._resolve_interests(clean_interests, token)
        if resolved_interests and rotation_seed:
            try:
                import hashlib
                offset = int(hashlib.sha256(rotation_seed.encode("utf-8")).hexdigest()[:8], 16) % len(resolved_interests)
                resolved_interests = resolved_interests[offset:] + resolved_interests[:offset]
            except Exception:
                pass
        # 注意：不再退化为只传name，因为FB API要求interests必须有id字段
        # 如果没有token或解析失败，resolved_interests保持为空，只使用宽泛受众

        for i in range(0, len(resolved_interests), chunk_size):
            if len(groups) >= max_adsets:
                break
            chunk = resolved_interests[i:i + chunk_size]
            interest_objs = chunk  # 已经是 {"id": ..., "name": ...} 格式
            groups.append({
                "name": f"兴趣: {', '.join(item['name'] for item in chunk)}",
                "type": "interest",
                "targeting": _base_targeting({"flexible_spec": [{"interests": interest_objs}]})
            })

        if not groups:
            _append_broad("宽泛受众（兴趣兜底）")
        return groups[:max_adsets]

    # ── 数据库辅助 ────────────────────────────────────────────────────────────

    def _load_asset(self, asset_id: int) -> Optional[dict]:
        conn = get_conn()
        row = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def _parse_json_field(self, value, default):
        if not value:
            return default
        if isinstance(value, (list, dict)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return default

    def _update_campaign_status(self, campaign_id: int, status: str, error_msg: str = None):
        conn = get_conn()
        if error_msg:
            conn.execute(
                "UPDATE auto_campaigns SET status=?, error_msg=?, updated_at=? WHERE id=?",
                (status, error_msg[:1000], datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
            )
        else:
            conn.execute(
                "UPDATE auto_campaigns SET status=?, updated_at=? WHERE id=?",
                (status, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
            )
        conn.commit()
        conn.close()

    def _update_progress(self, campaign_id: int, step: str, msg: str = ""):
        """更新铺广告进度（供前端轮询显示）"""
        conn = get_conn()
        conn.execute(
            "UPDATE auto_campaigns SET progress_step=?, progress_msg=?, updated_at=? WHERE id=?",
            (step, msg[:300] if msg else "", datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
        )
        conn.commit()
        conn.close()

    def _update_campaign_field(self, campaign_id: int, field: str, value):
        conn = get_conn()
        conn.execute(
            f"UPDATE auto_campaigns SET {field}=?, updated_at=? WHERE id=?",
            (value, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
        )
        conn.commit()
        conn.close()

    def _landing_link_base(self, value: Optional[str]) -> str:
        raw = (value or "").strip()
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

    def _landing_ad_click_url(self, public_url: Optional[str]) -> str:
        raw = str(public_url or "").strip()
        if not raw:
            return ""
        if "ad={{ad.id}}" in raw or "ad=%7B%7Bad.id%7D%7D" in raw:
            return raw
        separator = "&" if "?" in raw else "?"
        return f"{raw}{separator}ad={{{{ad.id}}}}"

    def _landing_domain_status_usable(self, item: dict) -> bool:
        domain = str(item.get("custom_domain") or "").strip()
        if not domain:
            return False
        raw = self._parse_json_field(item.get("raw_response"), {})
        if isinstance(raw, dict) and raw.get("custom_domain_runtime_usable"):
            return True
        err = str(item.get("last_error") or "").strip().lower()
        if any(token in err for token in ("authentication", "permission", "not authorized", "forbidden", "failed")):
            return False
        status = None
        if isinstance(raw, dict):
            status = raw.get("domain_status") or raw.get("custom_domain_result")
        values = []
        if isinstance(status, dict):
            for key in ("status", "verified", "validation_status", "verification_status", "ssl_status"):
                value = status.get(key)
                if value is not None:
                    values.append(str(value).strip().lower())
        elif status is not None:
            values.append(str(status).strip().lower())
        return any(value in {"active", "success", "verified", "ready", "ok"} for value in values)

    def _match_managed_landing_page_for_url(self, landing_url: Optional[str], published_only: bool = True) -> Optional[dict]:
        target = self._landing_link_base(landing_url)
        if not target:
            return None
        conn = get_conn()
        try:
            status_clause = "WHERE status='published'" if published_only else "WHERE 1=1"
            rows = conn.execute(
                f"""SELECT id, status, pages_url, custom_domain, target_urls, team_id, owner_user_id,
                          raw_response, last_error, worker_enabled, edge_runtime_version, link_kind
                   FROM landing_pages
                   {status_clause}
                     AND (COALESCE(pages_url,'')!='' OR COALESCE(custom_domain,'')!='')
                   ORDER BY id DESC LIMIT 500"""
            ).fetchall()
            for row in rows:
                item = dict(row)
                pages_url = str(item.get("pages_url") or "").strip().rstrip("/")
                custom_domain = str(item.get("custom_domain") or "").strip().rstrip("/")
                custom_url = f"https://{custom_domain}" if custom_domain else ""
                pages_match = bool(pages_url and self._landing_link_base(pages_url) == target)
                custom_match = bool(custom_url and self._landing_link_base(custom_url) == target)
                if not pages_match and not custom_match:
                    continue
                if custom_domain and self._landing_domain_status_usable(item):
                    item["public_url"] = custom_url.rstrip("/")
                elif not custom_domain and pages_url:
                    item["public_url"] = pages_url.rstrip("/")
                else:
                    item["public_url"] = ""
                item["matched_pages_fallback"] = pages_match
                item["matched_custom_domain"] = custom_match
                return item
        except Exception as exc:
            logger.warning("[AutoPilot] landing link page lookup failed: %s", exc)
        finally:
            conn.close()
        return None

    def _validate_managed_landing_ready_for_launch(self, landing_url: Optional[str], label: str = "落地页链接") -> None:
        raw = str(landing_url or "").strip()
        if not raw:
            return
        page = self._match_managed_landing_page_for_url(raw, published_only=False)
        if not page:
            return
        page_id = page.get("id")
        status = str(page.get("status") or "").strip()
        if status != "published":
            raise Exception(f"{label} 指向 Mira 托管落地页 #{page_id}，但当前状态不是 published，请先发布后再投放。")
        if not bool(page.get("worker_enabled")):
            raise Exception(f"{label} 指向 Mira 托管落地页 #{page_id}，但动态路由未启用；请重新发布一次落地页后再投放。")
        if not str(page.get("public_url") or "").strip():
            custom_domain = str(page.get("custom_domain") or "").strip()
            if custom_domain:
                raise Exception(f"{label} 指向 Mira 托管落地页 #{page_id}，但正式域名 {custom_domain} 尚未就绪，请等待域名复检或重新发布后再投放。")
            raise Exception(f"{label} 指向 Mira 托管落地页 #{page_id}，但当前没有可投放 public_url，请重新发布后再投放。")

    def _find_published_landing_page_for_url(self, landing_url: Optional[str]) -> Optional[dict]:
        page = self._match_managed_landing_page_for_url(landing_url)
        if page and page.get("public_url"):
            return page
        return None

    def _normalize_managed_landing_url_for_launch(self, landing_url: Optional[str]) -> str:
        raw = str(landing_url or "").strip()
        if not raw:
            return ""
        page = self._match_managed_landing_page_for_url(raw)
        if not page:
            return raw
        public_url = str(page.get("public_url") or "").strip()
        if public_url:
            if self._landing_link_base(raw) != self._landing_link_base(public_url):
                logger.info("[AutoPilot] replaced landing fallback URL with custom domain for page %s", page.get("id"))
            return public_url
        logger.warning("[AutoPilot] managed landing URL ignored because custom domain is not ready: page_id=%s", page.get("id"))
        return ""

    def _new_landing_slug(self, conn) -> str:
        alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        for _ in range(12):
            slug = "".join(secrets.choice(alphabet) for _ in range(6))
            exists = conn.execute("SELECT 1 FROM landing_ad_links WHERE slug=?", (slug,)).fetchone()
            if not exists:
                return slug
        return "".join(secrets.choice(alphabet) for _ in range(10))

    def _reserve_landing_ad_link(
        self,
        landing_url: Optional[str],
        act_id: str,
        account_name: str,
        campaign_id: str,
        campaign_name: str,
        adset_id: str,
        adset_name: str,
        ad_name: str,
        target_url: Optional[str] = None,
    ) -> Optional[dict]:
        page = self._find_published_landing_page_for_url(landing_url)
        if not page:
            return None
        stored_target_url = target_url or ""
        if stored_target_url and self._landing_link_base(stored_target_url) == self._landing_link_base(landing_url):
            stored_target_url = ""
        conn = get_conn()
        try:
            slug = self._new_landing_slug(conn)
            if not stored_target_url:
                page_targets = [
                    u.strip() for u in self._parse_json_field(page.get("target_urls"), [])
                    if isinstance(u, str) and u.strip()
                ]
                if page_targets:
                    existing_count = conn.execute(
                        "SELECT COUNT(*) FROM landing_ad_links WHERE page_id=?",
                        (page["id"],),
                    ).fetchone()[0]
                    stored_target_url = page_targets[int(existing_count or 0) % len(page_targets)]
            public_url = f"{str(page.get('public_url') or '').rstrip('/')}/a/{slug}"
            conn.execute(
                """INSERT INTO landing_ad_links
                   (page_id, slug, public_url, act_id, account_name, campaign_id, campaign_name,
                    adset_id, adset_name, ad_name, target_url, status, note,
                    team_id, owner_user_id, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    page["id"],
                    slug,
                    public_url,
                    str(act_id or "").replace("act_", ""),
                    (account_name or "")[:255],
                    str(campaign_id or "")[:80],
                    (campaign_name or "")[:255],
                    str(adset_id or "")[:80],
                    (adset_name or "")[:255],
                    (ad_name or "")[:255],
                    (stored_target_url or "")[:1000],
                    "reserved",
                    "auto_launch",
                    page.get("team_id"),
                    page.get("owner_user_id"),
                    "launch_engine",
                ),
            )
            link_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()
            return {"id": link_id, "page_id": page["id"], "slug": slug, "public_url": public_url}
        except Exception as exc:
            logger.warning("[AutoPilot] reserve landing ad link failed: %s", exc)
            return None
        finally:
            conn.close()

    def _mark_landing_ad_link(
        self,
        link: Optional[dict],
        status: str,
        ad_id: Optional[str] = None,
        error_msg: Optional[str] = None,
    ) -> None:
        if not link or not link.get("id"):
            return
        conn = get_conn()
        try:
            note = "auto_launch"
            if error_msg:
                note = ("auto_launch_error: " + str(error_msg))[:1000]
            conn.execute(
                """UPDATE landing_ad_links
                   SET status=?, ad_id=COALESCE(NULLIF(?,''), ad_id), note=?,
                       updated_at=datetime('now','+8 hours')
                   WHERE id=?""",
                (status, str(ad_id or "")[:80], note, int(link["id"])),
            )
            conn.commit()
        except Exception as exc:
            logger.warning("[AutoPilot] update landing ad link failed: %s", exc)
        finally:
            conn.close()

    def _insert_campaign_ad(
        self, campaign_id: int, act_id: str, asset_id: int,
        headline: Optional[str], body: Optional[str],
        targeting_json: str,
        fb_adset_id: Optional[str], fb_ad_id: Optional[str],
        status: str = "done", error_msg: str = None,
        adset_name: Optional[str] = None, ad_name: Optional[str] = None
    ):
        conn = get_conn()
        now_str = datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """INSERT INTO auto_campaign_ads
               (campaign_id, act_id, asset_id, headline, body, targeting_json,
                fb_adset_id, fb_ad_id, status, error_msg, created_at, adset_name, ad_name, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (campaign_id, act_id, asset_id, headline, body, targeting_json,
             fb_adset_id, fb_ad_id, status, error_msg, now_str, adset_name, ad_name, now_str)
        )
        if fb_ad_id:
            try:
                campaign = conn.execute(
                    "SELECT fb_campaign_id, name FROM auto_campaigns WHERE id=?",
                    (campaign_id,),
                ).fetchone()
                conn.execute(
                    """UPDATE landing_ad_links
                       SET campaign_id=COALESCE(NULLIF(campaign_id,''), ?),
                           campaign_name=COALESCE(NULLIF(campaign_name,''), ?),
                           adset_id=COALESCE(NULLIF(adset_id,''), ?),
                           adset_name=COALESCE(NULLIF(adset_name,''), ?),
                           ad_name=COALESCE(NULLIF(ad_name,''), ?),
                           updated_at=datetime('now','+8 hours')
                       WHERE ad_id=?""",
                    (
                        (campaign["fb_campaign_id"] if campaign else "") or "",
                        (campaign["name"] if campaign else "") or "",
                        fb_adset_id or "",
                        adset_name or "",
                        ad_name or "",
                        fb_ad_id,
                    ),
                )
            except Exception as exc:
                logger.warning("[AutoPilot] enrich landing ad link failed: %s", exc)
        conn.commit()
        conn.close()


# ── 定时任务入口（供 main.py 调用）────────────────────────────────────────────

def run_pending_campaigns():
    """
    扫描所有 pending 状态的自动铺广告任务并执行。
    由 main.py 的定时任务每 5 分钟调用一次。
    """
    conn = get_conn()
    pending = conn.execute(
        "SELECT id FROM auto_campaigns WHERE status='pending' ORDER BY created_at ASC LIMIT 10"
    ).fetchall()
    conn.close()

    if not pending:
        return

    engine = AutoPilotEngine()
    for row in pending:
        try:
            logger.info(f"[AutoPilot] 开始执行 pending 任务 campaign_id={row['id']}")
            engine.run_campaign(row["id"])
        except Exception as e:
            logger.error(f"[AutoPilot] 任务 {row['id']} 执行异常: {e}")
