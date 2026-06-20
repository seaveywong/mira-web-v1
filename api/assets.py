import logging
logger = logging.getLogger(__name__)
"""
Mira v3.0.1 素材库 API — 修复版
主要变更:
  1. 视觉 AI 独立配置(vision_provider / vision_api_key / vision_model)
     与文本 AI(ai_provider / ai_api_key)完全隔离
  2. 视频支持:视频素材通过抽帧(ffmpeg)提取封面图后送入视觉 AI
  3. 新增 /rename 接口(修改展示名称)
  4. 修复 /analyze 接口:不再依赖 ai_vision_enabled,改用 vision_api_key 是否配置
  5. 删除接口同时清理缩略图
"""
import os, hashlib, json, uuid, mimetypes, threading, subprocess
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from core.auth import get_current_user
from core.database import get_conn
from core.tenancy import apply_account_owner_scope, apply_team_scope, assert_row_access, team_id_for_create


# ── Facebook 广告合规要求(所有目的通用)──
# 禁用词:guaranteed / promise / 100% / risk-free / get rich / make money fast
#         cure / treat / prevent(医疗类) / lose X pounds in Y days
#         你的体重 / 你的财务状况 / 你的个人问题(不能直接指向用户个人)
# 软化原则:用「可能」「有机会」「帮助」代替绝对承诺；用「了解更多」代替「立即购买」
# ── v4.0: AI分析目的配置 ──────────────────────────────────────────────────────
AI_PURPOSE_PROMPTS = {
    "general": (
        "你是一位资深 Facebook 广告投放专家.请分析这张广告素材图片,根据画面内容自动判断最佳广告策略."
        "以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告标题和文案,让受众感受到是图片中的人在直接与他们说话."
        "【合规要求】文案必须符合 Facebook 广告政策:不使用绝对化承诺(如 guaranteed / 100% / promise),"
        "不直接指向用户个人特征(如「你的财务问题」),语气自然、真实,避免夸大宣传."
    ),
    "attract_male": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在直接向男性受众说话."
        "目标是吸引男性用户主动发起互动和私信.风格神秘、有趣、带有好奇心驱动,重点引导用户发送私信联系「我」."
        "例如:「想了解更多关于我的事吗？」「我在等你来找我聊聊...」"
        "【合规要求】避免性暗示或露骨内容,不使用「sexy」「hot」等词,保持暗示性但不违规."
    ),
    "attract_female": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在直接向女性受众说话."
        "目标是吸引女性用户主动发起互动和私信.风格温暖、真实、有亲和力,重点引导用户发送私信联系「我」."
        "例如:「我想和你分享我的故事...」「来找我聊聊吧,我们可以成为朋友」"
        "【合规要求】避免性暗示,不直接指向用户外貌或身材,保持真实感和情感连接."
    ),
    "attract_investors": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在直接向投资者分享经验."
        "目标是吸引股民、投资者和金融用户了解投资机会.风格专业、有说服力,重点引导用户了解更多."
        "例如:「我一直在关注这个市场机会...」「我找到了一个值得深入研究的标的,想了解吗？」"
        "【合规要求】严禁承诺收益或回报(不用 guaranteed returns / make money / get rich),"
        "不使用具体收益数字作为承诺,改用「有潜力」「值得关注」「我在研究」等表述,"
        "避免「立即暴富」「稳赚不赔」等夸大宣传,保持信息分享而非投资建议的语气."
    ),
    "promote_clothing": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在展示并推荐服饰."
        "目标是突出时尚感、品质感和穿搭魅力,引导用户了解和购买."
        "例如:「这是我最近爱穿的一件...」「穿上它让我整个人都不一样了」"
        "【合规要求】不使用「最便宜」「全网最低价」等绝对化表述,避免虚假折扣信息."
    ),
    "promote_beauty": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在分享美妆/护肤心得."
        "目标是突出使用体验和效果,引导用户了解和购买."
        "例如:「我用了这个之后感觉皮肤状态好了很多...」「这是我最近的日常护肤步骤」"
        "【合规要求】不使用医疗声称(如 cure / treat / clinically proven),"
        "不承诺具体效果数字(如「7天美白」),改用「感觉」「体验」「我的变化」等主观表述."
    ),
    "promote_health": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在分享健康生活方式."
        "目标是突出健康生活理念和产品使用体验,引导用户了解和购买."
        "【合规要求】严禁医疗声称(cure / treat / prevent / diagnose),"
        "不使用「减重 X 公斤」等具体承诺,改用「帮助我保持活力」「我的日常健康习惯」等表述,"
        "不直接指向用户的健康问题(不用「你的疾病」「你的体重问题」)."
    ),
    "promote_app": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在推荐App."
        "目标是突出App功能价值和使用体验,引导用户下载/注册."
        "【合规要求】不夸大功能效果,不使用「100%免费」等绝对化表述,如有内购需如实说明."
    ),
    "promote_course": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在分享学习经历."
        "目标是突出学习价值和技能提升,引导用户了解和报名."
        "【合规要求】不承诺具体收入或就业结果(不用「学完月薪过万」),"
        "改用「帮助我提升了...」「我学到了很多实用技能」等真实体验表述."
    ),
    "promote_finance": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在分享理财心得."
        "目标是突出理财理念和产品价值,引导用户了解产品."
        "【合规要求】严禁承诺收益(不用 guaranteed / fixed return / risk-free),"
        "不使用具体收益率作为承诺,改用「我在了解这个理财方式」「值得关注的机会」等表述,"
        "所有投资都有风险,文案语气要体现这一点."
    ),
    "ecommerce": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在推荐商品."
        "目标是突出产品价值和使用体验,引导用户了解和购买."
        "【合规要求】不使用虚假折扣(如「原价999,现价99」但实际从未按原价销售),"
        "不使用「全网最低」等无法核实的绝对化表述,紧迫感要真实."
    ),
    "lead_gen": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在邀请用户了解更多."
        "目标是突出免费价值和专属福利,引导用户填写表单/注册."
        "【合规要求】如果是免费内容需真实免费,不隐藏后续收费,"
        "不使用「你已被选中」「限你专属」等虚假个性化表述."
    ),
    "brand_awareness": (
        "请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在代表品牌说话."
        "目标是突出品牌价值观和情感共鸣,提升品牌好感度."
        "【合规要求】不使用「最好的品牌」「行业第一」等无法核实的绝对化表述,"
        "保持真实、有温度的品牌声音."
    ),
}
AI_LANGUAGE_NAMES = {
    "en": "English", "es": "Spanish (Español)", "pt": "Portuguese (Português)",
    "fr": "French (Français)", "ar": "Arabic (العربية)", "zh": "Simplified Chinese (简体中文)",
    "zh-tw": "Traditional Chinese (繁體中文)", "ja": "Japanese (日本語)", "ko": "Korean (한국어)",
    "de": "German (Deutsch)", "it": "Italian (Italiano)", "ru": "Russian (Русский)",
    "hi": "Hindi (हिन्दी)", "id": "Indonesian (Bahasa Indonesia)", "th": "Thai (ภาษาไทย)",
    "vi": "Vietnamese (Tiếng Việt)", "tr": "Turkish (Türkçe)", "ms": "Malay (Bahasa Melayu)",
    "nl": "Dutch (Nederlands)", "pl": "Polish (Polski)",
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

router = APIRouter()

ASSET_DIR = os.environ.get("MIRA_ASSET_DIR", "/opt/mira/assets")
THUMB_DIR = os.path.join(ASSET_DIR, "thumbs")
os.makedirs(ASSET_DIR, exist_ok=True)
os.makedirs(THUMB_DIR, exist_ok=True)


def _normalize_ai_language_code(raw: str = "") -> str:
    lang = str(raw or "").strip().lower().replace("_", "-")
    if lang in ("zh-cn", "cn", "zh-hans"):
        return "zh"
    if lang in ("zh-tw", "zh-hk", "tw", "hk", "zh-hant"):
        return "zh-tw"
    if "-" in lang and lang not in AI_LANGUAGE_NAMES:
        lang = lang.split("-", 1)[0]
    return lang or "en"


def _parse_country_codes(raw_value) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        values = raw_value
    else:
        text = str(raw_value or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            values = parsed if isinstance(parsed, list) else [text]
        except Exception:
            values = text.split(",")
    return [str(v).strip().upper() for v in values if str(v).strip()]


def _infer_ai_languages_from_countries(raw_value) -> list[str]:
    langs = []
    for country in _parse_country_codes(raw_value):
        lang = COUNTRY_LANGUAGE_MAP.get(country)
        if lang and lang not in langs:
            langs.append(lang)
    return langs or ["en"]


def _localized_ai_error_message(lang_list, category: str, fallback: str = "") -> str:
    primary_lang = _normalize_ai_language_code((lang_list or ["en"])[0] if (lang_list or ["en"]) else "en")
    zh_mode = primary_lang in ("zh", "zh-tw")
    messages = {
        "quota": "API 配额已耗尽，请升级计划或更换 Key"
        if zh_mode else
        "AI quota exhausted. Please upgrade the plan or switch the API key.",
        "auth": "视觉 AI Key 无效，请检查配置"
        if zh_mode else
        "Vision AI key is invalid. Please check the API configuration.",
        "permission": "视觉 AI Key 权限不足，请检查权限范围"
        if zh_mode else
        "Vision AI key does not have enough permission. Please review the key scope.",
        "timeout": "视觉 AI 请求超时，请稍后重试"
        if zh_mode else
        "Vision AI request timed out. Please retry in a moment.",
        "json": "视觉 AI 返回格式异常，请重新分析"
        if zh_mode else
        "Vision AI returned an invalid format. Please re-run the analysis.",
    }
    return messages.get(category, fallback or (fallback if zh_mode else fallback))

ALLOWED_IMAGE = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
ALLOWED_VIDEO = {".mp4", ".mov", ".avi", ".mkv"}
MAX_IMAGE_MB = 30
MAX_VIDEO_MB = 500

# 支持视觉的 provider 及其默认配置
VISION_PROVIDERS = {
    "openai": {
        "label": "OpenAI (GPT-4V)",
        "base_url": "https://api.openai.com/v1",
        "default_model": "gpt-4.1-mini",
        "models": ["gpt-4.1-mini", "gpt-4o", "gpt-4-turbo"]
    },
    "gemini": {
        "label": "Google Gemini",
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai",
        "default_model": "gemini-2.0-flash",
        "models": ["gemini-2.0-flash", "gemini-1.5-pro", "gemini-1.5-flash"]
    },
    "custom": {
        "label": "自定义 (OpenAI兼容)",
        "base_url": "",
        "default_model": "",
        "models": []
    }
}


def _get_setting(key: str, default: str = "") -> str:
    try:
        conn = get_conn()
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row["value"] if row else default
    except Exception:
        return default


def _row_to_dict(row) -> dict:
    d = dict(row)
    for field in ("ai_analysis", "ai_headlines", "ai_bodies", "ai_interests", "target_countries", "tags"):
        if d.get(field) and isinstance(d[field], str):
            try:
                d[field] = json.loads(d[field])
            except Exception:
                if field == "tags":
                    d[field] = [v.strip() for v in d[field].split(",") if v.strip()]
    # 生成可访问的 URL(通过 serve 接口)
    asset_id = d.get("id")
    if asset_id:
        d["file_url"] = f"/api/assets/serve/{asset_id}/file"
        d["thumb_url"] = f"/api/assets/serve/{asset_id}/thumb" if d.get("thumb_path") else ""
    return d


def _asset_matrix_ids(asset: dict, account_matrix_ids: list[int] | None = None) -> list[int]:
    ids = set()
    for mid in [asset.get("matrix_id")] + list(account_matrix_ids or []):
        try:
            parsed = int(mid)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            ids.add(parsed)
    return sorted(ids)


def _act_id_variants(act_id: str) -> list[str]:
    raw = str(act_id or "").strip()
    if not raw:
        return []
    num = raw[4:] if raw.startswith("act_") else raw
    variants = [raw]
    if num and num not in variants:
        variants.append(num)
    prefixed = f"act_{num}" if num else ""
    if prefixed and prefixed not in variants:
        variants.append(prefixed)
    return variants


def _matrix_ids_for_act(conn, act_id: str) -> list[int]:
    candidates = _act_id_variants(act_id)
    if not candidates:
        return []
    placeholders = ",".join("?" for _ in candidates)
    rows = conn.execute(
        f"""
        SELECT t.matrix_id
        FROM accounts a
        JOIN fb_tokens t ON t.id=a.token_id
        WHERE a.act_id IN ({placeholders}) AND t.matrix_id IS NOT NULL
        UNION
        SELECT t.matrix_id
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id=aot.token_id
        WHERE aot.act_id IN ({placeholders}) AND aot.status='active' AND t.matrix_id IS NOT NULL
        """,
        candidates + candidates,
    ).fetchall()
    ids = set()
    for row in rows:
        try:
            ids.add(int(row["matrix_id"]))
        except (TypeError, ValueError):
            continue
    return sorted(ids)


def _ensure_asset_library_columns(conn) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(ad_assets)").fetchall()}
    changed = False
    optional_cols = {
        "folder_name": "TEXT",
        "batch_code": "TEXT",
        "display_name": "TEXT",
        "target_countries": "TEXT",
        "ai_score": "REAL DEFAULT 0",
        "score_reason": "TEXT",
        "best_roas": "REAL",
        "last_active_at": "TEXT",
        "dispatch_count": "INTEGER DEFAULT 0",
        "last_dispatched_at": "TEXT",
        "asset_status": "TEXT DEFAULT 'active'",
        "archived_at": "TEXT",
        "tags": "TEXT",
        "source": "TEXT DEFAULT 'upload'",
        "team_id": "INTEGER",
        "matrix_id": "INTEGER",
    }
    for col, definition in optional_cols.items():
        if col not in cols:
            conn.execute(f"ALTER TABLE ad_assets ADD COLUMN {col} {definition}")
            changed = True
    if changed:
        conn.commit()


def _now_cst() -> str:
    return datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_asset_tags(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        text = str(value or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            raw_items = parsed if isinstance(parsed, list) else text.split(",")
        except Exception:
            raw_items = text.split(",")
    out = []
    seen = set()
    for item in raw_items:
        tag = str(item or "").strip()
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(tag[:40])
    return out[:20]


def _get_vision_client():
    """
    获取视觉 AI 客户端(独立于文本 AI).
    优先使用 vision_api_key,若未配置则尝试 ai_api_key(仅限 openai/gemini provider).
    返回 (client, model, provider) 或 (None, None, None)
    """
    try:
        from openai import OpenAI
        vision_key = _get_setting("vision_api_key", "")
        vision_provider = _get_setting("vision_provider", "openai")
        vision_model = _get_setting("vision_model", "gpt-4.1-mini")

        # 如果没有配置视觉 key,尝试用文本 AI key(仅 openai/gemini 支持视觉)
        if not vision_key:
            text_provider = _get_setting("ai_provider", "deepseek")
            if text_provider in ("openai", "gemini"):
                vision_key = _get_setting("ai_api_key", "")
                vision_provider = text_provider
                # 自动选择视觉模型
                if text_provider == "openai":
                    vision_model = "gpt-4.1-mini"
                elif text_provider == "gemini":
                    vision_model = "gemini-2.5-flash"

        if not vision_key:
            return None, None, None

        _defaults = {
            "openai": "https://api.openai.com/v1",
            "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
        }
        vision_base = _get_setting("vision_api_base", _defaults.get(vision_provider, "https://api.openai.com/v1"))
        vision_proxy = _get_setting("vision_proxy", "").strip()
        # 支持代理:通过 httpx 客户端传入代理配置
        if vision_proxy:
            try:
                import httpx
                transport = httpx.HTTPTransport(proxy=vision_proxy, verify=False)
                http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(60.0))
                client = OpenAI(api_key=vision_key, base_url=vision_base, http_client=http_client)
                logger.info(f'[Vision] 使用代理: {vision_proxy[:30]}...')
            except Exception as proxy_err:
                logger.warning(f'[Vision] 代理初始化失败,尝试直连: {proxy_err}')
                client = OpenAI(api_key=vision_key, base_url=vision_base)
        else:
            client = OpenAI(api_key=vision_key, base_url=vision_base)
        return client, vision_model, vision_provider
    except Exception:
        return None, None, None


def _extract_video_thumb(video_path: str) -> Optional[str]:
    """用 ffmpeg 从视频第 1 秒抽帧,返回缩略图路径(封面用)"""
    try:
        thumb_name = uuid.uuid4().hex + ".jpg"
        thumb_path = os.path.join(THUMB_DIR, thumb_name)
        result = subprocess.run(
            ["ffmpeg", "-y", "-ss", "1", "-i", video_path,
             "-vframes", "1", "-q:v", "2", thumb_path],
            capture_output=True, timeout=30
        )
        if result.returncode == 0 and os.path.exists(thumb_path):
            return thumb_path
    except Exception:
        pass
    return None


def _extract_video_frames(video_path: str, num_frames: int = 4) -> list:
    """
    从视频均匀抽取多帧,返回帧图片路径列表.
    用于 AI 深度分析,让 AI 理解完整视频内容而非只看封面.
    """
    frames = []
    try:
        # 先获取视频时长
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", video_path],
            capture_output=True, timeout=15
        )
        duration = float(probe.stdout.decode().strip() or "10")
        # 均匀分布时间点,避开开头0.5s和结尾0.5s
        duration = max(duration, 2.0)
        step = (duration - 1.0) / max(num_frames - 1, 1)
        timestamps = [0.5 + i * step for i in range(num_frames)]
        for ts in timestamps:
            frame_name = uuid.uuid4().hex + ".jpg"
            frame_path = os.path.join(THUMB_DIR, frame_name)
            result = subprocess.run(
                ["ffmpeg", "-y", "-ss", str(round(ts, 2)), "-i", video_path,
                 "-vframes", "1", "-q:v", "3", "-vf", "scale=640:-1", frame_path],
                capture_output=True, timeout=20
            )
            if result.returncode == 0 and os.path.exists(frame_path):
                frames.append(frame_path)
    except Exception as e:
        logger.warning(f"[视频抽帧] 失败: {e}")
    # 至少返回封面帧兜底
    if not frames:
        cover = _extract_video_thumb(video_path)
        if cover:
            frames.append(cover)
    return frames


# ── 列表 / 查询 ───────────────────────────────────────────────────────────────

@router.get("")
def list_assets(
    act_id: Optional[str] = Query(None),
    file_type: Optional[str] = Query(None),
    upload_status: Optional[str] = Query(None),
    target_country: Optional[str] = Query(None),
    folder_name: Optional[str] = Query(None),
    batch_code: Optional[str] = Query(None),
    matrix_id: Optional[int] = Query(None),
    asset_status: Optional[str] = Query("active"),
    tag: Optional[str] = Query(None),
    performance_filter: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("created_desc"),
    search: Optional[str] = Query(None),
    limit: int = Query(200, le=1000),
    offset: int = Query(0),
    user=Depends(get_current_user)
):
    conn = get_conn()
    _ensure_asset_library_columns(conn)
    where, params = [], []
    if act_id:
        where.append("act_id=?"); params.append(act_id)
    if file_type:
        where.append("file_type=?"); params.append(file_type)
    if upload_status:
        where.append("upload_status=?"); params.append(upload_status)
    status_filter = (asset_status or "active").strip().lower()
    if status_filter == "active":
        where.append("COALESCE(asset_status,'active') NOT IN ('archived','deleted')")
    elif status_filter == "archived":
        where.append("COALESCE(asset_status,'active')='archived'")
    elif status_filter in ("all", "*"):
        pass
    else:
        where.append("COALESCE(asset_status,'active')=?"); params.append(status_filter)
    if target_country:
        where.append("(target_countries LIKE ? OR target_countries LIKE ? OR target_countries LIKE ? OR target_countries=?)")
        params.extend([f"{target_country},%", f"%,{target_country},%", f"%,{target_country}", target_country])
    if folder_name:
        where.append("COALESCE(folder_name,'')=?"); params.append(folder_name)
    if batch_code:
        where.append("COALESCE(batch_code,'')=?"); params.append(batch_code)
    if matrix_id:
        where.append(
            """(
                COALESCE(a.matrix_id,0)=?
                OR
                EXISTS (
                    SELECT 1
                      FROM accounts aa
                      JOIN fb_tokens mt ON mt.id=aa.token_id
                     WHERE mt.matrix_id=?
                       AND REPLACE(COALESCE(aa.act_id,''),'act_','')=REPLACE(COALESCE(a.act_id,''),'act_','')
                )
                OR EXISTS (
                    SELECT 1
                      FROM account_op_tokens aot
                      JOIN fb_tokens ot ON ot.id=aot.token_id
                     WHERE aot.status='active'
                       AND ot.matrix_id=?
                       AND REPLACE(COALESCE(aot.act_id,''),'act_','')=REPLACE(COALESCE(a.act_id,''),'act_','')
                )
            )"""
        )
        params.extend([int(matrix_id), int(matrix_id), int(matrix_id)])
    if tag and tag.strip():
        where.append("COALESCE(tags,'') LIKE ?"); params.append(f"%{tag.strip()}%")
    if performance_filter:
        warmup_name_expr = "(UPPER(COALESCE(display_name,'')) LIKE 'YE%' OR UPPER(COALESCE(file_name,'')) LIKE 'YE%')"
        if performance_filter == "ready_launch":
            where.append("upload_status='ai_done'")
        elif performance_filter == "warmup":
            where.append(f"COALESCE(file_type,'image')='image' AND {warmup_name_expr}")
        elif performance_filter == "spent":
            where.append("COALESCE(total_spend,0)>0")
        elif performance_filter == "scale":
            where.append("COALESCE(total_spend,0)>0 AND COALESCE(score,0)>=72")
        elif performance_filter == "watch":
            where.append("COALESCE(total_spend,0)>0 AND COALESCE(score,0)>=40 AND COALESCE(score,0)<72")
        elif performance_filter == "poor":
            where.append("COALESCE(total_spend,0)>0 AND COALESCE(score,0)<40")
        elif performance_filter == "unscored":
            where.append("COALESCE(score,0)<=0 AND COALESCE(ai_score,0)<=0")
        elif performance_filter == "ai_error":
            where.append("upload_status='ai_error'")
    if search and search.strip():
        kw = f"%{search.strip()}%"
        where.append(
            "(COALESCE(display_name,'') LIKE ? OR COALESCE(file_name,'') LIKE ? OR "
            "COALESCE(asset_code,'') LIKE ? OR COALESCE(note,'') LIKE ? OR "
            "COALESCE(folder_name,'') LIKE ? OR COALESCE(batch_code,'') LIKE ? OR "
            "COALESCE(tags,'') LIKE ?)"
        )
        params.extend([kw, kw, kw, kw, kw, kw, kw])
    apply_team_scope(where, params, user, "a.team_id", include_unassigned=False)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sort_map = {
        "created_desc": "a.created_at DESC, a.id DESC",
        "score_desc": "COALESCE(a.score, a.ai_score, 0) DESC, COALESCE(a.total_spend,0) DESC, a.created_at DESC",
        "spend_desc": "COALESCE(a.total_spend,0) DESC, a.created_at DESC",
        "conv_desc": "COALESCE(a.total_conv,0) DESC, COALESCE(a.total_spend,0) DESC, a.created_at DESC",
        "last_active_desc": "COALESCE(a.last_active_at, a.last_dispatched_at, a.updated_at, a.created_at) DESC",
        "name_asc": "COALESCE(NULLIF(TRIM(a.display_name), ''), a.file_name, '') ASC, a.id DESC",
    }
    order_by = sort_map.get(sort_by or "created_desc", sort_map["created_desc"])
    total = conn.execute(f"SELECT COUNT(*) FROM ad_assets a {clause}", params).fetchone()[0]
    rows = conn.execute(
        f"""SELECT a.*,
                  tm.name AS team_name,
                  (SELECT COUNT(*) FROM auto_campaigns ac WHERE ac.asset_id=a.id) AS campaign_count,
                  (SELECT COUNT(*) FROM auto_campaign_ads aca WHERE aca.asset_id=a.id AND COALESCE(aca.fb_ad_id,'')!='') AS ad_count
           FROM ad_assets a
           LEFT JOIN teams tm ON tm.id=a.team_id
           {clause} ORDER BY {order_by} LIMIT ? OFFSET ?""",
        params + [limit, offset]
    ).fetchall()
    items = [_row_to_dict(r) for r in rows]
    act_ids = sorted({str(item.get("act_id") or "").strip() for item in items if str(item.get("act_id") or "").strip()})
    matrix_map: dict[str, set[int]] = {act: set() for act in act_ids}
    variant_to_assets: dict[str, set[str]] = {}
    for act in act_ids:
        for variant in _act_id_variants(act):
            variant_to_assets.setdefault(variant, set()).add(act)
    variants = sorted(variant_to_assets)
    if variants:
        placeholders = ",".join("?" for _ in variants)
        matrix_rows = conn.execute(
            f"""
            SELECT a.act_id AS matched_act_id, t.matrix_id
            FROM accounts a
            JOIN fb_tokens t ON t.id=a.token_id
            WHERE a.act_id IN ({placeholders})
              AND t.matrix_id IS NOT NULL
            UNION
            SELECT aot.act_id AS matched_act_id, t.matrix_id
            FROM account_op_tokens aot
            JOIN fb_tokens t ON t.id=aot.token_id
            WHERE aot.act_id IN ({placeholders})
              AND aot.status='active'
              AND t.matrix_id IS NOT NULL
            """,
            variants + variants,
        ).fetchall()
        for mr in matrix_rows:
            try:
                mid = int(mr["matrix_id"])
            except (TypeError, ValueError):
                continue
            for source_act in variant_to_assets.get(str(mr["matched_act_id"]), set()):
                matrix_map.setdefault(source_act, set()).add(mid)
    for item in items:
        act_key = str(item.get("act_id") or "").strip()
        item["linked_matrix_ids"] = _asset_matrix_ids(item, sorted(matrix_map.get(act_key, set())))
    conn.close()
    return {"total": total, "items": items}


@router.get("/vision-providers")
def get_vision_providers(user=Depends(get_current_user)):
    """返回支持视觉分析的 AI 厂商列表"""
    return VISION_PROVIDERS


# ── 文件访问(serve)────────────────────────────────────────────────────────────

@router.get("/serve/{asset_id}/file")
def serve_asset_file_v2(asset_id: int):
    """提供素材文件访问(无需认证,文件名有UUID前缀保护)"""
    from fastapi.responses import FileResponse
    conn = get_conn()
    row = conn.execute("SELECT file_path, file_name FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    conn.close()
    if not row or not row["file_path"] or not os.path.exists(row["file_path"]):
        raise HTTPException(404, "文件不存在")
    return FileResponse(row["file_path"], filename=row["file_name"])


@router.get("/serve/{asset_id}/thumb")
def serve_asset_thumb_v2(asset_id: int):
    """提供缩略图访问(无需认证,文件名有UUID前缀保护)"""
    from fastapi.responses import FileResponse
    conn = get_conn()
    row = conn.execute("SELECT thumb_path, file_name FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    conn.close()
    if not row or not row["thumb_path"] or not os.path.exists(str(row["thumb_path"])):
        raise HTTPException(404, "缩略图不存在")
    return FileResponse(row["thumb_path"])






@router.get("/{asset_id:int}")
def get_asset(asset_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    assert_row_access(conn, "ad_assets", asset_id, user, allow_unassigned=False)
    row = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    data = _row_to_dict(row)
    data["linked_matrix_ids"] = _asset_matrix_ids(data, _matrix_ids_for_act(conn, data.get("act_id")))
    conn.close()
    return data


class AssetBatchUpdateBody(BaseModel):
    ids: list[int]
    folder_name: Optional[str] = None
    batch_code: Optional[str] = None
    matrix_id: Optional[int] = None
    tags: Optional[list] = None
    tags_mode: Optional[str] = "replace"  # replace / append
    asset_status: Optional[str] = None


def _resolve_asset_ids_for_write(conn, ids: list[int], user, allow_unassigned: bool = True, claim_unassigned: bool = True) -> list[int]:
    placeholders = ",".join(["?"] * len(ids))
    where = [f"id IN ({placeholders})"]
    params = list(ids)
    apply_team_scope(where, params, user, "team_id", include_unassigned=allow_unassigned)
    rows = conn.execute(
        f"SELECT id FROM ad_assets WHERE {' AND '.join(where)}",
        params,
    ).fetchall()
    allowed_ids = [int(row["id"]) for row in rows]
    if len(allowed_ids) != len(set(ids)):
        raise HTTPException(403, "Some assets are not accessible")
    owner_team_id = team_id_for_create(user)
    if claim_unassigned and owner_team_id is not None and allowed_ids:
        claim_placeholders = ",".join(["?"] * len(allowed_ids))
        conn.execute(
            f"UPDATE ad_assets SET team_id=? WHERE id IN ({claim_placeholders}) AND team_id IS NULL",
            [owner_team_id] + allowed_ids,
        )
    return allowed_ids


def _update_assets_batch(conn, body: AssetBatchUpdateBody, user, allow_unassigned: bool = True) -> int:
    ids = [int(v) for v in (body.ids or []) if int(v) > 0]
    if not ids:
        raise HTTPException(400, "请选择素材")
    if len(ids) > 500:
        raise HTTPException(400, "单次最多整理 500 个素材")

    _ensure_asset_library_columns(conn)
    ids = _resolve_asset_ids_for_write(conn, ids, user, allow_unassigned=allow_unassigned)
    now = _now_cst()
    updates, params = [], []
    if body.folder_name is not None:
        updates.append("folder_name=?")
        params.append((body.folder_name or "").strip() or None)
    if body.batch_code is not None:
        updates.append("batch_code=?")
        params.append((body.batch_code or "").strip() or None)
    if body.matrix_id is not None:
        try:
            matrix_id = int(body.matrix_id or 0)
        except (TypeError, ValueError):
            raise HTTPException(400, "矩阵编号必须是正整数")
        updates.append("matrix_id=?")
        params.append(matrix_id if matrix_id > 0 else None)
    if body.asset_status is not None:
        status = (body.asset_status or "").strip().lower()
        if status not in ("active", "archived"):
            raise HTTPException(400, "素材状态只支持 active / archived")
        updates.append("asset_status=?")
        params.append(status)
        updates.append("archived_at=?")
        params.append(now if status == "archived" else None)
    if body.tags is not None and (body.tags_mode or "replace") != "append":
        updates.append("tags=?")
        params.append(json.dumps(_normalize_asset_tags(body.tags), ensure_ascii=False))

    changed = 0
    placeholders = ",".join(["?"] * len(ids))
    if updates:
        conn.execute(
            f"UPDATE ad_assets SET {', '.join(updates)}, updated_at=? WHERE id IN ({placeholders})",
            params + [now] + ids,
        )
        changed = len(ids)
    if body.tags is not None and (body.tags_mode or "replace") == "append":
        add_tags = _normalize_asset_tags(body.tags)
        rows = conn.execute(
            f"SELECT id, tags FROM ad_assets WHERE id IN ({placeholders})",
            ids,
        ).fetchall()
        for row in rows:
            merged = _normalize_asset_tags(_normalize_asset_tags(row["tags"]) + add_tags)
            conn.execute(
                "UPDATE ad_assets SET tags=?, updated_at=? WHERE id=?",
                (json.dumps(merged, ensure_ascii=False), now, row["id"]),
            )
        changed = len(rows)
    return changed


@router.post("/batch-update")
def batch_update_assets(body: AssetBatchUpdateBody, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        changed = _update_assets_batch(conn, body, user)
        conn.commit()
    finally:
        conn.close()
    return {"updated": changed, "message": "素材整理已保存"}


@router.post("/{asset_id:int}/archive")
def archive_asset(asset_id: int, user=Depends(get_current_user)):
    body = AssetBatchUpdateBody(ids=[asset_id], asset_status="archived")
    conn = get_conn()
    try:
        changed = _update_assets_batch(conn, body, user, allow_unassigned=False)
        conn.commit()
    finally:
        conn.close()
    return {"updated": changed, "asset_status": "archived"}


@router.post("/{asset_id:int}/restore")
def restore_asset(asset_id: int, user=Depends(get_current_user)):
    body = AssetBatchUpdateBody(ids=[asset_id], asset_status="active")
    conn = get_conn()
    try:
        changed = _update_assets_batch(conn, body, user, allow_unassigned=False)
        conn.commit()
    finally:
        conn.close()
    return {"updated": changed, "asset_status": "active"}


@router.get("/duplicates")
def duplicate_assets(limit: int = Query(100, ge=1, le=500), user=Depends(get_current_user)):
    conn = get_conn()
    _ensure_asset_library_columns(conn)
    where = ["COALESCE(file_hash,'')!=''", "COALESCE(asset_status,'active')!='deleted'"]
    params = []
    apply_team_scope(where, params, user, "team_id", include_unassigned=False)
    clause = " AND ".join(where)
    rows = conn.execute(
        f"""
        SELECT file_hash, COUNT(*) AS cnt, MIN(id) AS first_id, MAX(created_at) AS last_created_at
        FROM ad_assets
        WHERE {clause}
        GROUP BY file_hash
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, last_created_at DESC
        LIMIT ?
        """,
        params + [limit],
    ).fetchall()
    groups = []
    for row in rows:
        item_where = ["file_hash=?"]
        item_params = [row["file_hash"]]
        apply_team_scope(item_where, item_params, user, "team_id", include_unassigned=False)
        items = conn.execute(
            f"""SELECT id, file_name, display_name, folder_name, batch_code, asset_status, created_at
               FROM ad_assets WHERE {' AND '.join(item_where)} ORDER BY created_at ASC, id ASC""",
            item_params,
        ).fetchall()
        groups.append({
            "file_hash": row["file_hash"],
            "count": row["cnt"],
            "first_id": row["first_id"],
            "items": [dict(item) for item in items],
        })
    conn.close()
    return {"total_groups": len(groups), "groups": groups}


@router.get("/{asset_id:int}/usage")
def asset_usage(asset_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    _ensure_asset_library_columns(conn)
    assert_row_access(conn, "ad_assets", asset_id, user, allow_unassigned=False)
    asset = conn.execute("SELECT id FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not asset:
        conn.close()
        raise HTTPException(404, "素材不存在")
    campaigns = conn.execute(
        """
        SELECT c.id, c.act_id, COALESCE(a.name, c.act_id) AS act_name,
               c.name, c.objective, c.status, c.fb_campaign_id,
               c.total_adsets, c.total_ads, c.created_at, c.updated_at, c.error_msg
        FROM auto_campaigns c
        LEFT JOIN accounts a ON a.act_id=c.act_id
        WHERE c.asset_id=?
        ORDER BY c.created_at DESC
        LIMIT 30
        """,
        (asset_id,),
    ).fetchall()
    spend_rows = conn.execute(
        """
        SELECT act_id, MAX(act_name) AS act_name, COUNT(*) AS ad_count,
               SUM(spend) AS spend, SUM(conv) AS conv,
               SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               MAX(last_synced_at) AS last_synced_at
        FROM asset_spend_log
        WHERE asset_id=?
        GROUP BY act_id
        ORDER BY spend DESC
        LIMIT 30
        """,
        (asset_id,),
    ).fetchall()
    summary = conn.execute(
        """
        SELECT COUNT(*) AS ad_count, SUM(spend) AS spend, SUM(conv) AS conv,
               SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               MAX(last_synced_at) AS last_synced_at
        FROM asset_spend_log
        WHERE asset_id=?
        """,
        (asset_id,),
    ).fetchone()
    conn.close()
    return {
        "summary": dict(summary) if summary else {},
        "campaigns": [dict(r) for r in campaigns],
        "accounts": [dict(r) for r in spend_rows],
    }


# ── 上传 ──────────────────────────────────────────────────────────────────────

@router.post("")
async def upload_asset(
    file: UploadFile = File(...),
    act_id: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    target_countries: Optional[str] = Form(None),
    folder_name: Optional[str] = Form(None),
    batch_code: Optional[str] = Form(None),
    matrix_id: Optional[int] = Form(None),
    user=Depends(get_current_user)
):
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext in ALLOWED_IMAGE:
        file_type = "image"
        max_mb = MAX_IMAGE_MB
    elif ext in ALLOWED_VIDEO:
        file_type = "video"
        max_mb = MAX_VIDEO_MB
    else:
        raise HTTPException(400, f"不支持的格式 {ext},支持:{sorted(ALLOWED_IMAGE | ALLOWED_VIDEO)}")

    content = await file.read()
    size_mb = len(content) / 1024 / 1024
    if size_mb > max_mb:
        raise HTTPException(400, f"文件过大({size_mb:.1f}MB),{file_type} 最大 {max_mb}MB")

    file_hash = hashlib.md5(content).hexdigest()
    conn = get_conn()
    _ensure_asset_library_columns(conn)
    resource_team_id = team_id_for_create(user)
    existing_where, existing_params = ["file_hash=?"], [file_hash]
    apply_team_scope(existing_where, existing_params, user, "team_id", include_unassigned=False)
    existing = conn.execute(
        f"SELECT id, file_name FROM ad_assets WHERE {' AND '.join(existing_where)}",
        existing_params,
    ).fetchone()
    if existing:
        conn.close()
        return {"id": existing["id"], "file_name": existing["file_name"],
                "duplicate": True, "message": "该素材已存在,返回已有记录"}

    # 保存文件,使用原始文件名(加 uuid 前缀防冲突)
    safe_name = f"{uuid.uuid4().hex[:8]}_{file.filename}"
    save_path = os.path.join(ASSET_DIR, safe_name)
    with open(save_path, "wb") as f:
        f.write(content)

    # 生成缩略图
    thumb_path = None
    if file_type == "image":
        try:
            from PIL import Image
            img = Image.open(save_path)
            img.thumbnail((400, 400))
            thumb_name = uuid.uuid4().hex + ".jpg"
            thumb_path = os.path.join(THUMB_DIR, thumb_name)
            img.convert("RGB").save(thumb_path, "JPEG", quality=85)
        except Exception:
            thumb_path = None
    elif file_type == "video":
        thumb_path = _extract_video_thumb(save_path)
    # —— 异常保护：后续操作失败时清理已保存的文件 ——
    _saved_files = [save_path]
    try:
        now = datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        folder_name = (folder_name or "").strip() or None
        batch_code = (batch_code or "").strip() or None
        clean_matrix_id = None
        if matrix_id is not None:
            try:
                parsed_matrix_id = int(matrix_id or 0)
            except (TypeError, ValueError):
                parsed_matrix_id = 0
            clean_matrix_id = parsed_matrix_id if parsed_matrix_id > 0 else None
        # display_name 用于展示(可重命名),file_name 保留原始文件名
        # v4.0: 生成 asset_code
        try:
            date_str = now[:10].replace("-", "")
            count_today = conn.execute(
                "SELECT COUNT(*) FROM ad_assets WHERE asset_code LIKE ?",
                (f"AST-{date_str}-%",)
            ).fetchone()[0]
            asset_code_new = f"AST-{date_str}-{count_today+1:03d}"
        except Exception:
            asset_code_new = None
        cur = conn.execute(
            """INSERT INTO ad_assets
               (act_id, file_name, display_name, file_type, file_path, thumb_path,
                file_size, file_hash, upload_status, note, target_countries, folder_name, batch_code, asset_code, team_id, matrix_id, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,'local_saved',?,?,?,?,?,?,?,?,?)""",
            (act_id, file.filename, file.filename, file_type, save_path, thumb_path,
             len(content), file_hash, note, target_countries, folder_name, batch_code, asset_code_new, resource_team_id, clean_matrix_id, now, now)
        )
        asset_id = cur.lastrowid
        conn.commit()
        conn.close()
        _saved_files.clear()  # 提交成功，不再清理
    except Exception:
        conn.close()
        for _f in _saved_files:
            if _f and os.path.exists(_f):
                os.remove(_f)
        raise

    # 检查是否配置了视觉 AI,自动触发分析
    client, _, _ = _get_vision_client()
    ai_pending = client is not None
    if ai_pending:
        inferred_languages = _infer_ai_languages_from_countries(target_countries)
        threading.Thread(
            target=_ai_analyze_asset,
            args=(asset_id, "general", inferred_languages),
            daemon=True,
        ).start()

    return {
        "id": asset_id,
        "file_name": file.filename,
        "file_type": file_type,
        "file_size": len(content),
        "thumb_path": thumb_path,
        "upload_status": "local_saved",
        "ai_pending": ai_pending,
        "message": "素材上传成功" + (",AI 分析已自动启动" if ai_pending else ",请手动触发 AI 分析")
    }


# ── AI 视觉分析 ───────────────────────────────────────────────────────────────

# ── 精度档位配置 ──────────────────────────────────────────────────────────────
ANALYSIS_DEPTH_CONFIG = {
    "fast": {
        "label": "快速",
        "detail": "low",
        "temperature": 0.7,
        "video_frames": 1,    # 只看封面
        "max_tokens": 2048,
        "copy_count": 3,      # 生成3条文案
    },
    "standard": {
        "label": "标准",
        "detail": "high",
        "temperature": 0.85,
        "video_frames": 4,    # 均匀抽4帧
        "max_tokens": 4096,
        "copy_count": 3,
    },
    "deep": {
        "label": "深度",
        "detail": "high",
        "temperature": 0.9,
        "video_frames": 6,    # 均匀抽6帧
        "max_tokens": 4096,
        "copy_count": 5,      # 生成5条文案
    },
}


def _ai_analyze_asset(asset_id: int, purpose: str = 'general', languages: list = None, depth: str = 'standard', style: str = 'standard'):
    """
    后台线程:调用视觉 AI 分析素材,写回 ad_assets 表.
    v4.0: 支持目的定制和多语言
    v5.0: 支持精度档位(fast/standard/deep)和视频多帧分析
    """
    import base64
    try:
        client, model, provider = _get_vision_client()
        if not client:
            return

        conn = get_conn()
        row = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
        conn.close()
        if not row:
            return

        if languages is None:
            languages = _infer_ai_languages_from_countries(row["target_countries"])
        lang_list = [_normalize_ai_language_code(l) for l in (languages or ['en']) if str(l).strip()]
        if not lang_list:
            lang_list = _infer_ai_languages_from_countries(row["target_countries"])

        # 精度配置
        dcfg = ANALYSIS_DEPTH_CONFIG.get(depth, ANALYSIS_DEPTH_CONFIG["standard"])
        img_detail = dcfg["detail"]
        temperature = dcfg["temperature"]
        max_tokens = dcfg["max_tokens"]
        copy_count = dcfg["copy_count"]
        video_frames = dcfg["video_frames"]

        # ── 确定要分析的图片(支持视频多帧)──
        image_contents = []   # 将传给 AI 的 image_url content 列表
        is_video = row["file_type"] == "video"

        if row["file_type"] == "image":
            img_path = row["file_path"]
            if not img_path or not os.path.exists(img_path):
                return
            with open(img_path, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()
            mime = mimetypes.guess_type(img_path)[0] or "image/jpeg"
            image_contents.append({
                "type": "image_url",
                "image_url": {"url": f"data:{mime};base64,{img_b64}", "detail": img_detail}
            })

        elif row["file_type"] == "video":
            if video_frames <= 1:
                # 快速模式:只用封面
                img_path = row["thumb_path"] if row["thumb_path"] and os.path.exists(str(row["thumb_path"])) else None
                if not img_path:
                    img_path = _extract_video_thumb(row["file_path"])
                if not img_path:
                    conn = get_conn()
                    conn.execute(
                        "UPDATE ad_assets SET upload_status='ai_error', updated_at=? WHERE id=?",
                        (datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), asset_id)
                    )
                    conn.commit(); conn.close()
                    return
                frame_paths = [img_path]
            else:
                # 标准/深度模式:均匀抽多帧
                frame_paths = _extract_video_frames(row["file_path"], num_frames=video_frames)
                if not frame_paths:
                    conn = get_conn()
                    conn.execute(
                        "UPDATE ad_assets SET upload_status='ai_error', updated_at=? WHERE id=?",
                        (datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), asset_id)
                    )
                    conn.commit(); conn.close()
                    return

            for fp in frame_paths:
                if os.path.exists(fp):
                    with open(fp, "rb") as f:
                        b64 = base64.b64encode(f.read()).decode()
                    image_contents.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": img_detail}
                    })
        else:
            return

        if not image_contents:
            return

        # ── 构建 prompt ──
        if purpose and purpose.startswith('custom:'):
            custom_desc = purpose[7:].strip()
            purpose_prompt = f'请以图片中人物的第一视角(用「我」「我的」等第一人称)生成广告文案,就像图片中的人在直接与受众说话.投放目的:{custom_desc}.请根据此目的生成精准的广告文案,禁止使用第三人称描述图片内容.'
        else:
            purpose_prompt = AI_PURPOSE_PROMPTS.get(purpose or 'general', AI_PURPOSE_PROMPTS['general'])

        lang_names = [AI_LANGUAGE_NAMES.get(l, l) for l in lang_list]
        if len(lang_list) == 1:
            lang_instruction = (
                f"请将 analysis、headlines、bodies、audience_note 全部使用 {lang_names[0]} 输出；"
                f"只有 interests 保持英文。若目标语言不是中文，绝不要输出中文。"
            )
        else:
            lang_instruction = (
                f"请同时生成多语言版本的标题和文案,语言包括:{', '.join(lang_names)}."
                f"每种语言各生成{copy_count}条标题和{copy_count}条文案,按语言分组放入对应的 headlines_XX 和 bodies_XX 字段(XX为语言代码)."
                f"主 headlines、bodies、analysis、audience_note 字段统一使用第一种语言({lang_names[0]})。"
            )

        # 视频多帧时额外说明
        video_hint = ""
        if is_video and len(image_contents) > 1:
            video_hint = f"\n【注意】以下 {len(image_contents)} 张图片是同一段视频按时间顺序均匀截取的帧,请综合理解视频的完整内容、故事线和广告意图后生成文案,不要只描述单帧画面."

        # ── 文案风格强度 ──
        style_guide = {
            "conservative": (
                "【文案风格:保守】"
                "语气温和、安全、不夸大.多用「了解更多」「分享」「体验」等柔和表述,"
                "避免任何可能引起审核的词语,适合对合规要求极高的广告主."
            ),
            "standard": (
                "【文案风格:标准】"
                "语气自然、有感染力,在合规范围内尽量吸引眼球."
                "可用适度的情感化表达和行动召唤,平衡吸引力与合规性."
            ),
            "aggressive": (
                "【文案风格:激进——请写出真正有冲击力的广告文案】\n"
                "目标:让用户看到第一句就停下来、产生强烈好奇心和行动冲动.\n\n"
                "具体写法要求:\n"
                "1. 标题必须具有冲击力,可用以下技巧:\n"
                "   - 具体数字/事实:如 'This $0.38 Stock Could Be the Next $1,000,000 Opportunity'\n"
                "   - 惊叹/不敢相信:如 'I Can\'t Believe This Is Still Under $1...'\n"
                "   - 悬念引导:如 'What I Found in This Chart Shocked Me'\n"
                "   - 紧迫感:如 'This Window Is Closing Fast — Are You In?'\n"
                "   - 内幕感:如 'My Secret Strategy That Wall Street Doesn\'t Want You to Know'\n\n"
                "2. 文案必须具体、有画面感,不要模糊语言:\n"
                "   - 具体化:写出真实场景和数字,不要用「显著收益」这种模糊词\n"
                "   - 情感化:用第一人称分享真实心理状态,如 'I\'ve been watching this for weeks and I can\'t sleep'\n"
                "   - 强烈 CTA:结尾用行动指令,如 'Join my free group now — link in bio'\n\n"
                "禁止事项(仅限以下几条,不要过度限制):\n"
                "- 不要写 'guaranteed returns' 或 'risk-free'(但可以写具体数字和场景)\n"
                "- 不要直接说 'cure/treat disease'(但可以写体验和感受)\n"
                "- 不要用 'you are fat/sick/poor'直接攻击用户(但可以用第一人称分享)"
            ),
        }.get(style or 'standard', '')

        # 合规指引:激进模式下已在 style_guide 中内置了精简禁止事项,不再重复全量合规限制
        if style == 'aggressive':
            compliance_guide = ""  # 激进模式不加额外合规限制,已在 style_guide 中内置
        else:
            compliance_guide = """【Facebook 广告合规指引】
请用真实、自然的语气写文案,避免以下表述:
- 绝对化承诺(guaranteed / 100% / 一定能)→ 改用「有机会」「帮助」「可能」
- 夸大收益(get rich / 月入过万)→ 改用「值得关注」「我在研究」
- 医疗声称(cure / treat)→ 改用「感觉」「体验」「我的变化」
- 直接指向用户问题(你的财务/体重/疾病)→ 改用第一人称分享视角"""

        prompt = f"""{purpose_prompt}{video_hint}

{style_guide}

{lang_instruction}

请用 JSON 格式返回:
{{
  "analysis": "描述画面/视频内容和广告意图(使用主语言,{'50字以内' if depth=='fast' else '100字以内'})",
  "headlines": ["标题1", ...(共{copy_count}条)],
  "bodies": ["文案1(含 CTA)", ...(共{copy_count}条)],
  "interests": ["英文兴趣词1", "英文兴趣词2", "英文兴趣词3", "英文兴趣词4", "英文兴趣词5"],
  "audience_note": "目标受众特征简述(使用主语言)"
}}

要求:
1. 如果图片中有清晰的人物,请以该人物的第一视角(「我」「我的」等第一人称)书写标题和文案；如果图片中没有人物(如纯数字、图表、产品图、风景图),请以旁白/推荐者视角书写,直接向受众说话(如「你」「你的」),或用吸引眼球的陈述句开头
2. 标题不超过 40 字,文案不超过 125 字,语气自然有感染力
3. 兴趣词必须是 Facebook Ads Manager 受众定向中真实存在的英文词,不要造词
4. 兴趣词要与投放目的高度匹配
{compliance_guide}
6. 只返回 JSON,不要其他内容"""

        # ── 构建消息(多帧时多个 image_url)──
        msg_content = image_contents + [{"type": "text", "text": prompt}]

        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": msg_content}],
            max_tokens=max_tokens,
            temperature=temperature
        )
        # v4.1: 安全检查 - 防止 message 为 None(内容安全过滤)
        msg = resp.choices[0].message if resp.choices else None
        if msg is None or msg.content is None:
            # 检查是否有 finish_reason 提示
            fr = resp.choices[0].finish_reason if resp.choices else 'unknown'
            if fr in ('content_filter', 'safety'):
                raise ValueError('图片内容被 AI 安全过滤拦截(内容政策限制).\n建议:①更换图片 ②在设置中切换为 GPT-4 Vision(内容限制更宽松)')
            else:
                raise ValueError(f'AI 返回空响应(finish_reason={fr}).\n可能原因:①图片内容触发安全过滤 ②API配额不足 ③网络超时.\n建议:更换图片,或在设置中切换 AI 模型后重试.')
        raw = msg.content.strip()
        finish_reason = resp.choices[0].finish_reason
        if finish_reason == 'length':
            # 响应被截断,尝试智能修复不完整的JSON
            import re as _re
            json_match = _re.search(r'\{.*', raw, _re.DOTALL)
            if json_match:
                partial = json_match.group(0)
                # 先直接尝试
                try:
                    data = json.loads(partial)
                except:
                    # 智能补全:截断的JSON通常缺少末尾的 ] 和 }
                    # 策略:找到最后一个完整的字段值,截断到那里,然后补全结构
                    fixed = partial
                    # 移除末尾不完整的字段(找最后一个完整的逗号分隔项)
                    # 先尝试补全缺失的括号
                    open_braces = fixed.count('{') - fixed.count('}')
                    open_brackets = fixed.count('[') - fixed.count(']')
                    # 如果末尾有未闭合的字符串,先截断它
                    in_string = False
                    last_complete = 0
                    i = 0
                    while i < len(fixed):
                        c = fixed[i]
                        if c == '"'  and (i == 0 or fixed[i-1] != '\\'):
                            in_string = not in_string
                        if not in_string and c in (',', '}', ']'):
                            last_complete = i + 1
                        i += 1
                    if in_string:
                        # 截断到最后一个完整位置
                        fixed = fixed[:last_complete]
                        # 重新计算括号
                        open_braces = fixed.count('{') - fixed.count('}')
                        open_brackets = fixed.count('[') - fixed.count(']')
                    # 补全括号
                    fixed = fixed.rstrip(',').rstrip()
                    fixed += ']' * open_brackets + '}' * open_braces
                    try:
                        data = json.loads(fixed)
                        logger.warning(f"[AI] 响应被截断但已智能修复JSON (asset_id={asset_id})")
                    except Exception as je:
                        raise ValueError(f'AI响应被截断(max_tokens不足),JSON无法修复: {je}.建议在设置中增大max_tokens或减少语言数量')
            else:
                raise ValueError(f'AI响应被截断且无法提取JSON,请减少语言数量或检查模型配置')
        else:
            if "```" in raw:
                # 提取代码块中的JSON
                parts = raw.split("```")
                for part in parts[1::2]:  # 取奇数索引(代码块内容)
                    part = part.lstrip("json").strip()
                    if part.startswith("{"):
                        raw = part
                        break
            # 尝试直接提取JSON对象
            import re as _re
            json_match = _re.search(r'\{[\s\S]*\}', raw)
            if json_match:
                raw = json_match.group(0)
            data = json.loads(raw)

        now = datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        conn = get_conn()
        # AI 自动命名:从 analysis 中提取简短名称作为 display_name
        analysis_text = data.get("analysis", "")
        auto_name = ""
        if analysis_text:
            # 取 analysis 前 20 字作为展示名,去掉标点
            import re
            clean = re.sub(r'[\s\-—·|/\\]+', '_', analysis_text[:30]).strip('_')
            auto_name = clean[:25] if clean else ""

        # 读取原始文件名,只有当 display_name 还是原始文件名时才覆盖(避免覆盖手动重命名)
        orig_row = conn.execute("SELECT file_name, display_name FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
        should_rename = orig_row and (orig_row["display_name"] == orig_row["file_name"] or not orig_row["display_name"])

            # v4.0: 生成 asset_code(如果还没有)
        existing_code_row = conn.execute("SELECT asset_code, created_at FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
        asset_code = existing_code_row["asset_code"] if existing_code_row and existing_code_row["asset_code"] else None
        if not asset_code:
            try:
                dt_str = (existing_code_row["created_at"] or now)[:10].replace("-", "")
                count_code = conn.execute(
                    "SELECT COUNT(*) FROM ad_assets WHERE asset_code LIKE ?",
                    (f"AST-{dt_str}-%",)
                ).fetchone()[0]
                asset_code = f"AST-{dt_str}-{count_code+1:03d}"
            except Exception:
                asset_code = f"AST-{now[:10].replace('-','')}-{asset_id:04d}"
        # v4.0: 语言字段
        lang_str = ",".join(lang_list) if lang_list else "en"
            # v4.0: 生成 asset_code(如果还没有)
        existing_code_row = conn.execute("SELECT asset_code, created_at FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
        asset_code = existing_code_row["asset_code"] if existing_code_row and existing_code_row["asset_code"] else None
        if not asset_code:
            try:
                dt_str = (existing_code_row["created_at"] or now)[:10].replace("-", "")
                count_code = conn.execute(
                    "SELECT COUNT(*) FROM ad_assets WHERE asset_code LIKE ?",
                    (f"AST-{dt_str}-%",)
                ).fetchone()[0]
                asset_code = f"AST-{dt_str}-{count_code+1:03d}"
            except Exception:
                asset_code = f"AST-{now[:10].replace('-','')}-{asset_id:04d}"
        # v4.0: 语言字段
        lang_str = ",".join(lang_list) if lang_list else "en"
        update_fields = [
            "ai_analysis=?", "ai_headlines=?", "ai_bodies=?", "ai_interests=?",
            "ai_audience_note=?", "ai_purpose=?", "ai_language=?", "asset_code=?",
            "upload_status='ai_done'", "updated_at=?"
        ]
        update_vals = [
            data.get("analysis", ""),
            json.dumps(data.get("headlines", []), ensure_ascii=False),
            json.dumps(data.get("bodies", []), ensure_ascii=False),
            json.dumps(data.get("interests", []), ensure_ascii=False),
            data.get("audience_note", ""),
            purpose or "general",
            lang_str,
            asset_code,
            now
        ]
        if auto_name and should_rename:
            update_fields.append("display_name=?")
            update_vals.append(auto_name)
        conn.execute(
            f"UPDATE ad_assets SET {', '.join(update_fields)} WHERE id=?",
            update_vals + [asset_id]
        )
        conn.commit()
        conn.close()
        # ── Phase 2: 智能评分 hook(全自动系统)──────────────────────────
        try:
            import threading as _t2
            from services.smart_scorer import score_and_infer as _score_infer
            _t2.Thread(target=_score_infer, args=(asset_id,), daemon=True).start()
        except Exception as _se:
            pass  # 评分失败不影响主流程
    except Exception as e:
        import traceback
        err_msg = str(e)
        # 提取可读的错误描述
        if '429' in err_msg or 'quota' in err_msg.lower() or 'rate' in err_msg.lower():
            short_err = 'API 配额已耗尽,请升级计划或更换 Key'
        elif '401' in err_msg or 'auth' in err_msg.lower() or 'invalid' in err_msg.lower():
            short_err = 'API Key 无效,请检查视觉 AI 配置'
        elif '403' in err_msg:
            short_err = 'API 权限不足,请检查 Key 权限'
        elif 'timeout' in err_msg.lower() or 'connect' in err_msg.lower():
            short_err = '网络连接超时,请重试'
        elif 'json' in err_msg.lower():
            short_err = 'AI 返回格式错误,请重试'
        else:
            short_err = err_msg[:80] if len(err_msg) > 80 else err_msg
        logger.error(f'[AI分析错误] asset_id={asset_id}: {err_msg}')
        primary_lang = _normalize_ai_language_code((lang_list or ["en"])[0] if (lang_list or ["en"]) else "en")
        fallback_msg = "AI 分析失败，请重试" if primary_lang in ("zh", "zh-tw") else "AI analysis failed. Please retry."
        err_lower = err_msg.lower()
        if '429' in err_msg or 'quota' in err_lower or 'rate' in err_lower:
            short_err = _localized_ai_error_message(lang_list, "quota", fallback=fallback_msg)
        elif '401' in err_msg or 'auth' in err_lower or 'invalid' in err_lower:
            short_err = _localized_ai_error_message(lang_list, "auth", fallback=fallback_msg)
        elif '403' in err_msg:
            short_err = _localized_ai_error_message(lang_list, "permission", fallback=fallback_msg)
        elif 'timeout' in err_lower or 'connect' in err_lower:
            short_err = _localized_ai_error_message(lang_list, "timeout", fallback=fallback_msg)
        elif 'json' in err_lower:
            short_err = _localized_ai_error_message(lang_list, "json", fallback=fallback_msg)
        elif primary_lang not in ("zh", "zh-tw") and not err_msg.isascii():
            short_err = fallback_msg
        traceback.print_exc()
        try:
            conn = get_conn()
            # 尝试写入错误信息(如果字段存在)
            try:
                conn.execute(
                    "UPDATE ad_assets SET upload_status='ai_error', ai_analysis=?, updated_at=? WHERE id=?",
                    (short_err, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), asset_id)
                )
            except Exception:
                conn.execute(
                    "UPDATE ad_assets SET upload_status='ai_error', updated_at=? WHERE id=?",
                    (datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), asset_id)
                )
            conn.commit(); conn.close()
        except Exception:
            pass


class AiAnalyzeBody(BaseModel):
    purpose: Optional[str] = "general"
    languages: Optional[list] = None
    depth: Optional[str] = "standard"   # fast / standard / deep
    style: Optional[str] = "standard"   # conservative / standard / aggressive

@router.post("/{asset_id:int}/analyze")
def trigger_ai_analyze(asset_id: int, body: AiAnalyzeBody = None, user=Depends(get_current_user)):
    """
    手动触发 AI 视觉分析(v5.0)
    - 支持目的定制、多语言
    - 支持精度档位:fast(快速)/ standard(标准)/ deep(深度)
    - 支持文案风格强度:conservative(保守)/ standard(标准)/ aggressive(激进)
    - 支持重分析(已分析的素材也可重新分析)
    """
    if body is None:
        body = AiAnalyzeBody()
    conn = get_conn()
    assert_row_access(conn, "ad_assets", asset_id, user, allow_unassigned=False)
    row = conn.execute("SELECT id, file_type FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "素材不存在")
    client, model, provider = _get_vision_client()
    if not client:
        raise HTTPException(400,
            "未配置视觉 AI.请在系统设置中配置「视觉 AI」的 API Key(支持 OpenAI / Gemini,DeepSeek 不支持图片分析)")
    purpose = body.purpose or "general"
    row2_conn = get_conn()
    asset_row = row2_conn.execute(
        "SELECT target_countries FROM ad_assets WHERE id=?",
        (asset_id,),
    ).fetchone()
    row2_conn.close()
    languages = body.languages or _infer_ai_languages_from_countries(asset_row["target_countries"] if asset_row else None)
    depth = body.depth if body.depth in ("fast", "standard", "deep") else "standard"
    style = body.style if body.style in ("conservative", "standard", "aggressive") else "standard"
    depth_label = ANALYSIS_DEPTH_CONFIG[depth]["label"]
    conn = get_conn()
    owner_team_id = team_id_for_create(user)
    conn.execute(
        "UPDATE ad_assets SET upload_status='ai_pending', ai_purpose=?, ai_language=?, updated_at=?, team_id=COALESCE(team_id, ?) WHERE id=?",
        (purpose, ",".join(languages),
         datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), owner_team_id, asset_id)
    )
    conn.commit(); conn.close()
    threading.Thread(
        target=_ai_analyze_asset,
        args=(asset_id, purpose, languages, depth, style),
        daemon=True
    ).start()
    lang_str = "/".join(languages)
    style_labels = {"conservative": "保守", "standard": "标准", "aggressive": "激进"}
    return {
        "message": (
            f"AI 视觉分析已启动({provider}/{model},"
            f"目的={purpose},语言={lang_str},"
            f"精度={depth_label},风格={style_labels.get(style, style)}),"
            f"视频抽帧数={ANALYSIS_DEPTH_CONFIG[depth]['video_frames']},"
            f"约 {'30-60' if depth=='fast' else '60-120'} 秒后刷新查看结果"
        )
    }

@router.get("/ai-purposes")
def get_ai_purposes(user=Depends(get_current_user)):
    """返回支持的 AI 分析目的列表"""
    return [
        {"value": "general", "label": "通用(根据图片自动判断)"},
        {"value": "attract_male", "label": "吸引男性用户发起互动/私信"},
        {"value": "attract_female", "label": "吸引女性用户发起互动/私信"},
        {"value": "attract_investors", "label": "吸引投资者/股民/金融用户"},
        {"value": "promote_clothing", "label": "推广服饰/时尚/穿搭产品"},
        {"value": "promote_beauty", "label": "推广美妆/护肤/美容产品"},
        {"value": "promote_health", "label": "推广健康/保健/营养产品"},
        {"value": "promote_app", "label": "推广App下载/注册"},
        {"value": "promote_course", "label": "推广课程/教育/培训"},
        {"value": "promote_finance", "label": "推广金融/理财/投资产品"},
        {"value": "ecommerce", "label": "电商带货(引导购买)"},
        {"value": "lead_gen", "label": "获取线索(引导留资/注册)"},
        {"value": "brand_awareness", "label": "品牌曝光/认知提升"},
    ]

@router.get("/ai-languages")
def get_ai_languages(user=Depends(get_current_user)):
    """返回支持的 AI 生成语言列表"""
    return [{"value": k, "label": v} for k, v in AI_LANGUAGE_NAMES.items()]

@router.get("/ai-depths")
def get_ai_depths(user=Depends(get_current_user)):
    """返回支持的 AI 分析精度档位列表"""
    return [
        {"value": "fast",     "label": "快速",  "desc": "只看封面/单帧,消耗 Token 最少,约 30-60 秒"},
        {"value": "standard", "label": "标准",  "desc": "图片高精度,视频均匀抽 4 帧,推荐默认"},
        {"value": "deep",     "label": "深度",  "desc": "视频抽 6 帧,生成5条文案,消耗 Token 最多,约 60-120 秒"},
    ]


@router.get("/ai-styles")
def get_ai_styles(user=Depends(get_current_user)):
    """返回支持的文案风格强度列表"""
    return [
        {"value": "conservative", "label": "保守",  "desc": "语气温和安全,合规性最高,适合对审核要求严格的广告主"},
        {"value": "standard",     "label": "标准",  "desc": "平衡吸引力与合规性,推荐默认"},
        {"value": "aggressive",   "label": "激进",  "desc": "充满冲击力和诱惑力,大胆吸眼,在合规范围内尽量激情"},
    ]



# ── 重命名 ────────────────────────────────────────────────────────────────────

class RenameBody(BaseModel):
    display_name: str


@router.post("/{asset_id:int}/rename")
def rename_asset(asset_id: int, body: RenameBody, user=Depends(get_current_user)):
    """重命名素材展示名称(不影响实际文件名)"""
    name = body.display_name.strip()
    if not name:
        raise HTTPException(400, "名称不能为空")
    conn = get_conn()
    assert_row_access(conn, "ad_assets", asset_id, user, allow_unassigned=False)
    owner_team_id = team_id_for_create(user)
    row = conn.execute("SELECT id FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    conn.execute(
        "UPDATE ad_assets SET display_name=?, updated_at=?, team_id=COALESCE(team_id, ?) WHERE id=?",
        (name, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), owner_team_id, asset_id)
    )
    conn.commit(); conn.close()
    return {"message": "重命名成功", "display_name": name}


# ── 更新 ──────────────────────────────────────────────────────────────────────

class AssetUpdate(BaseModel):
    note: Optional[str] = None
    ai_headlines: Optional[list] = None
    ai_bodies: Optional[list] = None
    ai_interests: Optional[list] = None
    score: Optional[float] = None
    score_label: Optional[str] = None
    landing_url: Optional[str] = None
    custom_headline: Optional[str] = None
    custom_body: Optional[str] = None  # 素材级落地页链接
    target_countries: Optional[str] = None
    folder_name: Optional[str] = None
    batch_code: Optional[str] = None
    matrix_id: Optional[int] = None
    tags: Optional[list] = None
    asset_status: Optional[str] = None


@router.put("/{asset_id:int}")
def update_asset(asset_id: int, body: AssetUpdate, user=Depends(get_current_user)):
    conn = get_conn()
    _ensure_asset_library_columns(conn)
    assert_row_access(conn, "ad_assets", asset_id, user, allow_unassigned=False)
    row = conn.execute("SELECT id FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    updates, params = [], []
    if body.note is not None:
        updates.append("note=?"); params.append(body.note)
    if body.ai_headlines is not None:
        updates.append("ai_headlines=?")
        params.append(json.dumps(body.ai_headlines, ensure_ascii=False))
    if body.ai_bodies is not None:
        updates.append("ai_bodies=?")
        params.append(json.dumps(body.ai_bodies, ensure_ascii=False))
    if body.ai_interests is not None:
        updates.append("ai_interests=?")
        params.append(json.dumps(body.ai_interests, ensure_ascii=False))
    if body.score is not None:
        updates.append("score=?"); params.append(body.score)
    if body.score_label is not None:
        updates.append("score_label=?"); params.append(body.score_label)
    if body.landing_url is not None:
        updates.append("landing_url=?"); params.append(body.landing_url)
    if body.target_countries is not None:
        updates.append("target_countries=?"); params.append(body.target_countries)
        # 地区修改后标记需要重新评分和重新调度
        updates.append("needs_rescore=?"); params.append(1)
        updates.append("last_dispatched_at=?"); params.append(None)
    if body.folder_name is not None:
        updates.append("folder_name=?"); params.append((body.folder_name or "").strip() or None)
    if body.batch_code is not None:
        updates.append("batch_code=?"); params.append((body.batch_code or "").strip() or None)
    if body.matrix_id is not None:
        try:
            matrix_id = int(body.matrix_id or 0)
        except (TypeError, ValueError):
            conn.close()
            raise HTTPException(400, "矩阵编号必须是正整数")
        updates.append("matrix_id=?"); params.append(matrix_id if matrix_id > 0 else None)
    if body.tags is not None:
        updates.append("tags=?"); params.append(json.dumps(_normalize_asset_tags(body.tags), ensure_ascii=False))
    if body.custom_headline is not None:
        updates.append("custom_headline=?"); params.append(body.custom_headline or None)
    if body.custom_body is not None:
        updates.append("custom_body=?"); params.append(body.custom_body or None)
    if body.asset_status is not None:
        status = (body.asset_status or "").strip().lower()
        if status not in ("active", "archived"):
            conn.close()
            raise HTTPException(400, "素材状态只支持 active / archived")
        updates.append("asset_status=?"); params.append(status)
        updates.append("archived_at=?"); params.append(_now_cst() if status == "archived" else None)
    if updates:
        owner_team_id = team_id_for_create(user)
        updates.append("team_id=COALESCE(team_id, ?)")
        params.append(owner_team_id)
        updates.append("updated_at=?")
        params.append(datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
        params.append(asset_id)
        conn.execute(f"UPDATE ad_assets SET {','.join(updates)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return {"message": "更新成功"}


# ── 删除 ──────────────────────────────────────────────────────────────────────

@router.delete("/{asset_id:int}")
def delete_asset(asset_id: int, user=Depends(get_current_user)):
    """删除素材(同时删除本地文件和缩略图)"""
    conn = get_conn()
    assert_row_access(conn, "ad_assets", asset_id, user, allow_unassigned=False)
    row = conn.execute("SELECT file_path, thumb_path FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    file_path = row["file_path"]
    thumb_path = row["thumb_path"]
    conn.execute("DELETE FROM ad_assets WHERE id=?", (asset_id,))
    conn.commit(); conn.close()
    for p in [file_path, thumb_path]:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass
    return {"message": "素材已删除"}







# ── 素材按账户明细接口 ──────────────────────────────────────────────────────────
class LaunchCampaignBody(BaseModel):
    act_id: Optional[str] = None
    act_ids: Optional[list[str]] = None
    objective: str = "OUTCOME_SALES"
    target_countries: Optional[list[str]] = None
    target_cpa: Optional[float] = None
    daily_budget: float = 20
    age_min: int = 18
    age_max: int = 65
    gender: int = 0
    placements: Optional[dict] = None
    bid_strategy: str = "LOWEST_COST_WITHOUT_CAP"
    max_adsets: int = 5
    page_id: Optional[str] = None
    pixel_id: Optional[str] = None
    conversion_event: Optional[str] = None
    landing_url: Optional[str] = None
    form_link: Optional[str] = None
    device_platforms: str = "all"
    ad_language: str = "en"
    tw_advertiser_id: Optional[int] = None
    tw_page_id: Optional[str] = None
    conversion_goal: Optional[str] = None
    message_template: Optional[str] = None
    lead_form_id: Optional[str] = None
    cta_type: Optional[str] = None
    copy_mode: Optional[str] = "ai"
    custom_headline: Optional[str] = None
    custom_body: Optional[str] = None


class PreCheckBody(LaunchCampaignBody):
    pass


_MESSAGE_OBJECTIVES = {"OUTCOME_MESSAGES", "OUTCOME_MESSAGING", "MESSAGES"}
_MESSAGE_GOALS = {
    "conversations",
    "messaging_purchase_conversion",
    "messaging_appointment_conversion",
    "messaging_leads",
}
_LANDING_REQUIRED_OBJECTIVES = {"OUTCOME_TRAFFIC", "OUTCOME_SALES", "OUTCOME_ENGAGEMENT"}
_PIXEL_REQUIRED_GOALS = {"OFFSITE_CONVERSIONS", "VALUE"}
_REGULATED_IDENTITY_COUNTRIES = {"TW", "HK", "SG"}


def _normalize_launch_body(body: LaunchCampaignBody) -> None:
    body.objective = (body.objective or "OUTCOME_SALES").strip().upper()
    if body.objective in _MESSAGE_OBJECTIVES:
        body.objective = "OUTCOME_MESSAGES"
    if not body.target_countries:
        body.target_countries = ["US"]
    body.target_countries = [str(c).strip().upper() for c in body.target_countries if str(c).strip()]
    if not body.target_countries:
        body.target_countries = ["US"]
    body.act_id = str(body.act_id or "").strip() or None
    body.act_ids = [str(a).strip() for a in (body.act_ids or []) if str(a).strip()]
    body.conversion_goal = (body.conversion_goal or "").strip()
    body.page_id = (body.page_id or "").strip() or None
    body.pixel_id = (body.pixel_id or "").strip() or None
    body.landing_url = (body.landing_url or "").strip() or None
    body.form_link = (body.form_link or "").strip() or None
    body.tw_page_id = (body.tw_page_id or "").strip() or None


def _launch_act_ids(body: LaunchCampaignBody) -> list[str]:
    ids = body.act_ids[:] if body.act_ids else []
    if body.act_id:
        ids.insert(0, body.act_id)
    seen, out = set(), []
    for act_id in ids:
        act_id = str(act_id or "").strip()
        if act_id and act_id not in seen:
            seen.add(act_id)
            out.append(act_id)
    return out


def _launch_goal_meta(body: LaunchCampaignBody) -> dict:
    objective = (body.objective or "OUTCOME_SALES").strip().upper()
    goal = (body.conversion_goal or "").strip().lower()
    is_message = objective in _MESSAGE_OBJECTIVES or goal in _MESSAGE_GOALS
    is_lead = goal == "lead_generation"
    is_page_likes = goal == "page_likes"
    return {
        "is_message": is_message,
        "is_lead": is_lead,
        "is_page_likes": is_page_likes,
        "landing_required": objective in _LANDING_REQUIRED_OBJECTIVES and not is_message and not is_lead and not is_page_likes,
    }


def _account_landing_or_default(conn, act_id: str) -> str:
    row = conn.execute("SELECT landing_url FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    if row and (row["landing_url"] or "").strip():
        return row["landing_url"].strip()
    row = conn.execute("SELECT value FROM settings WHERE key='default_landing_url'").fetchone()
    return (row["value"] if row else "") or ""


def _get_setting_value(conn, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return (row["value"] if row and row["value"] is not None else default) or default


def _ensure_launch_campaign_columns(conn) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(auto_campaigns)").fetchall()}
    if "form_link" not in cols:
        conn.execute("ALTER TABLE auto_campaigns ADD COLUMN form_link TEXT")
        conn.commit()


def _normalize_account_status(raw):
    if raw in (None, ""):
        return None
    if isinstance(raw, str):
        lower = raw.strip().lower()
        if lower in {"active", "ok", "enabled"}:
            return 1
        if lower in {"disabled", "inactive"}:
            return 2
        if lower in {"debt", "payment_failed"}:
            return 3
        if lower in {"api_error", "no_token", "error"}:
            return -1
    try:
        return int(raw)
    except (TypeError, ValueError):
        return -1


def _launch_account_block_reason(acc: dict) -> Optional[str]:
    try:
        if int(acc.get("enabled") or 0) != 1:
            return "账户已暂停巡检/投放"
    except (TypeError, ValueError):
        return "账户启用状态异常"

    status = _normalize_account_status(acc.get("account_status"))
    if status in (None, 1):
        return None
    status_labels = {
        -1: "状态异常",
        2: "已禁用",
        3: "支付失败/欠费",
        7: "政策限制",
        8: "待审核",
        9: "已关闭",
        100: "待处理",
        101: "已关闭",
        201: "权限/支付异常",
    }
    return f"账户状态不允许投放：{status_labels.get(status, status)}"


def _launch_page_block_reason(conn, page_id: str) -> str:
    page_id = str(page_id or "").strip()
    if not page_id:
        return ""
    row = conn.execute(
        """SELECT page_id, page_name, page_is_published, page_can_advertise,
                  page_status, page_status_hint
           FROM tw_certified_pages
           WHERE page_id=?
           LIMIT 1""",
        (page_id,),
    ).fetchone()
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
    if not reasons:
        return ""
    return f"{row['page_name'] or page_id}({page_id}): " + " / ".join(dict.fromkeys(reasons))


def _run_launch_precheck(body: PreCheckBody, user=None) -> dict:
    _normalize_launch_body(body)
    items = []
    act_ids = _launch_act_ids(body)
    if not act_ids:
        items.append({"key": "account", "label": "广告账户", "status": "fail", "msg": "请选择至少一个广告账户"})
        return {"pass": False, "overall": "fail", "items": items}

    conn = get_conn()
    try:
        account_where = [
            "act_id IN (%s)" % ",".join("?" for _ in act_ids)
        ]
        account_params = list(act_ids)
        if user is not None:
            apply_team_scope(account_where, account_params, user, "team_id", include_unassigned=False)
            apply_account_owner_scope(account_where, account_params, user, "owner_user_id")
        rows = conn.execute(
            f"SELECT act_id,name,enabled,account_status,page_id,pixel_id,landing_url,form_link FROM accounts WHERE {' AND '.join(account_where)}",
            account_params,
        ).fetchall()
        account_map = {r["act_id"]: dict(r) for r in rows}
        default_page_id = _get_setting_value(conn, "autopilot_fb_page_id", "").strip()
        default_pixel_id = _get_setting_value(conn, "autopilot_fb_pixel_id", "").strip()
        default_landing_url = _get_setting_value(conn, "default_landing_url", "").strip()
        for act_id in act_ids:
            acc = account_map.get(act_id)
            if not acc:
                items.append({"key": "account", "label": act_id, "status": "fail", "msg": "账户不在系统中"})
                continue
            block_reason = _launch_account_block_reason(acc)
            if block_reason:
                items.append({"key": "account", "label": acc.get("name") or act_id, "status": "fail", "msg": block_reason})
            else:
                items.append({"key": "account", "label": acc.get("name") or act_id, "status": "pass", "msg": "账户状态正常"})

        try:
            from services.token_manager import ACTION_CREATE, get_exec_token_candidates
            token_missing = [
                act_id for act_id in act_ids
                if not get_exec_token_candidates(act_id, ACTION_CREATE, notify_exhausted=False, reserve=False)
            ]
            if token_missing:
                items.append({"key": "token", "label": "操作号 Token", "status": "fail", "msg": "以下账户没有可用 CREATE Token：" + ", ".join(token_missing[:5])})
            else:
                items.append({"key": "token", "label": "操作号 Token", "status": "pass", "msg": "已找到可用于创建广告的操作号"})
        except Exception as e:
            items.append({"key": "token", "label": "操作号 Token", "status": "warn", "msg": f"Token 预检失败，提交时会再次校验：{e}"})

        meta = _launch_goal_meta(body)
        page_missing = [
            (account_map.get(act_id, {}) or {}).get("name") or act_id
            for act_id in act_ids
            if not (
                body.page_id
                or ((account_map.get(act_id, {}) or {}).get("page_id") or "").strip()
                or default_page_id
            )
        ]
        if page_missing:
            items.append({"key": "page", "label": "主页", "status": "fail", "msg": "以下账户缺少主页 ID：" + ", ".join(page_missing[:5])})
        else:
            page_blocks = []
            for act_id in act_ids:
                selected_page = (
                    body.page_id
                    or ((account_map.get(act_id, {}) or {}).get("page_id") or "").strip()
                    or default_page_id
                )
                reason = _launch_page_block_reason(conn, selected_page)
                if reason:
                    page_blocks.append(((account_map.get(act_id, {}) or {}).get("name") or act_id) + ": " + reason)
            if body.tw_page_id:
                tw_reason = _launch_page_block_reason(conn, body.tw_page_id)
                if tw_reason:
                    page_blocks.append("认证主页: " + tw_reason)
            if page_blocks:
                items.append({"key": "page", "label": "主页", "status": "fail", "msg": "主页不可投放：" + "；".join(page_blocks[:5])})
            else:
                items.append({"key": "page", "label": "主页", "status": "pass", "msg": "已选择主页"})

        pixel_missing = [
            (account_map.get(act_id, {}) or {}).get("name") or act_id
            for act_id in act_ids
            if not (
                body.pixel_id
                or ((account_map.get(act_id, {}) or {}).get("pixel_id") or "").strip()
                or default_pixel_id
            )
        ]
        if (body.conversion_goal or "").upper() in _PIXEL_REQUIRED_GOALS and pixel_missing:
            items.append({"key": "pixel", "label": "Pixel", "status": "fail", "msg": "网站转化/转化价值目标缺少 Pixel：" + ", ".join(pixel_missing[:5])})
        elif body.objective == "OUTCOME_SALES" and pixel_missing:
            items.append({"key": "pixel", "label": "Pixel", "status": "warn", "msg": "以下账户未配置 Pixel，转化广告建议补齐：" + ", ".join(pixel_missing[:5])})
        else:
            items.append({"key": "pixel", "label": "Pixel", "status": "pass", "msg": "Pixel 配置可用或当前目标不强制"})

        if meta["landing_required"]:
            landing_missing = [
                (account_map.get(act_id, {}) or {}).get("name") or act_id
                for act_id in act_ids
                if not (
                    body.landing_url
                    or ((account_map.get(act_id, {}) or {}).get("landing_url") or "").strip()
                    or default_landing_url
                )
            ]
            if not landing_missing:
                items.append({"key": "landing_url", "label": "落地页链接", "status": "pass", "msg": "已配置落地页"})
            else:
                items.append({"key": "landing_url", "label": "落地页链接", "status": "fail", "msg": "以下账户缺少落地页链接：" + ", ".join(landing_missing[:5])})
        elif meta["is_lead"] and not body.lead_form_id:
            form_missing = [
                (account_map.get(act_id, {}) or {}).get("name") or act_id
                for act_id in act_ids
                if not (
                    body.form_link
                    or body.landing_url
                    or ((account_map.get(act_id, {}) or {}).get("form_link") or "").strip()
                    or ((account_map.get(act_id, {}) or {}).get("landing_url") or "").strip()
                    or default_landing_url
                )
            ]
            if not form_missing:
                items.append({"key": "form_link", "label": "表单回跳链接", "status": "pass", "msg": "已配置 Lead 表单回跳/隐私链接"})
            else:
                items.append({"key": "form_link", "label": "表单回跳链接", "status": "warn", "msg": "以下账户缺少表单回跳链接，自动建表单时会尝试使用系统兜底：" + ", ".join(form_missing[:5])})

        regulated = [c for c in (body.target_countries or []) if c in _REGULATED_IDENTITY_COUNTRIES]
        if regulated:
            from services.token_manager import get_matrix_id_for_account
            _label = f"合规投放 ({'/'.join(regulated)})"
            _any_fail = False
            _any_pass = False
            _messages = []
            for _act_id in act_ids:
                _mid = None
                try:
                    _mid = get_matrix_id_for_account(_act_id)
                except Exception:
                    pass
                if _mid is None:
                    _any_fail = True
                    _messages.append(f"{_act_id}: 未识别矩阵归属")
                else:
                    _c2 = get_conn()
                    _cp = _c2.execute(
                        "SELECT page_name FROM tw_certified_pages WHERE matrix_id=? AND verified_identity_id IS NOT NULL AND TRIM(verified_identity_id)!='' LIMIT 1",
                        (_mid,)
                    ).fetchone()
                    _c2.close()
                    if _cp:
                        _any_pass = True
                        _messages.append(f"矩阵 {_mid}: {_cp['page_name']} (已认证)")
                    else:
                        _any_fail = True
                        _messages.append(f"矩阵 {_mid}: 无 Verified ID")
            if _any_pass and not _any_fail:
                items.append({"key": "tw_identity", "label": _label, "status": "pass", "msg": "; ".join(_messages)})
            elif _any_fail and _any_pass:
                items.append({"key": "tw_identity", "label": _label, "status": "warn", "msg": "; ".join(_messages)})
            else:
                items.append({"key": "tw_identity", "label": _label, "status": "fail", "msg": "所有账户均无 Verified ID。请先在主页库为对应矩阵的主页填写 Verified Identity ID。"})
    finally:
        conn.close()

    has_fail = any(i["status"] == "fail" for i in items)
    has_warn = any(i["status"] == "warn" for i in items)
    return {"pass": not has_fail, "overall": "fail" if has_fail else ("warn" if has_warn else "pass"), "items": items}


def _build_launch_precheck_block_message(report: dict) -> str:
    fails = [i for i in report.get("items", []) if i.get("status") == "fail"]
    return "；".join(f"{i.get('label')}: {i.get('msg')}" for i in fails[:4]) if fails else ""


def _launch_now() -> str:
    return datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _launch_campaign_name(asset: dict, body: LaunchCampaignBody) -> str:
    objective_abbr = {
        "OUTCOME_SALES": "CONV",
        "OUTCOME_LEADS": "LEAD",
        "OUTCOME_TRAFFIC": "TRAF",
        "OUTCOME_AWARENESS": "AWR",
        "OUTCOME_ENGAGEMENT": "ENG",
        "OUTCOME_MESSAGES": "MSG",
        "OUTCOME_APP_PROMOTION": "APP",
    }.get(body.objective, "ADS")
    countries = "-".join((body.target_countries or ["XX"])[:2])
    asset_code = asset.get("asset_code") or f"AST-{asset.get('id', 0):04d}"
    return f"{objective_abbr}-{countries}-{asset_code}-{datetime.now(tz=timezone(timedelta(hours=8))).strftime('%m%d')}"


def _insert_launch_campaign(conn, asset: dict, act_id: str, body: LaunchCampaignBody) -> int:
    _ensure_launch_campaign_columns(conn)
    now = _launch_now()
    campaign_name = _launch_campaign_name(asset, body)
    cur = conn.execute(
        """INSERT INTO auto_campaigns
           (act_id, asset_id, name, objective, target_countries,
            target_cpa, daily_budget,
            age_min, age_max, gender, placements, bid_strategy, max_adsets,
            page_id_override, pixel_id_override, landing_url, form_link,
            device_platforms, ad_language, conversion_event,
            tw_page_id, conversion_goal, message_template, lead_form_id,
            cta_type, copy_mode, custom_copy, status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?)""",
        (
            act_id, asset["id"], campaign_name, body.objective,
            json.dumps(body.target_countries or ["US"]), body.target_cpa, body.daily_budget,
            body.age_min, body.age_max, body.gender,
            json.dumps(body.placements) if body.placements else None,
            body.bid_strategy, body.max_adsets,
            body.page_id, body.pixel_id, body.landing_url, body.form_link,
            body.device_platforms or "all", body.ad_language or "en",
            body.conversion_event or "PURCHASE",
            body.tw_page_id,
            body.conversion_goal or None,
            body.message_template or None,
            body.lead_form_id or None,
            body.cta_type or "",
            body.copy_mode or "ai",
            json.dumps({"headline": body.custom_headline, "body": body.custom_body}) if (body.custom_headline or body.custom_body) else None,
            now, now
        ),
    )
    return int(cur.lastrowid)


def _trigger_manual_launch(campaign_id: int) -> None:
    import traceback as _tb
    try:
        from services.launch_engine import AutoPilotEngine
        AutoPilotEngine().run_campaign(campaign_id)
    except Exception as e:
        full_err = f"{type(e).__name__}: {e}\n{_tb.format_exc()[-800:]}"
        logger.error(f"[ManualLaunch] task {campaign_id} crashed: {full_err}")
        try:
            conn = get_conn()
            conn.execute(
                "UPDATE auto_campaigns SET status='error', error_msg=?, progress_msg=?, updated_at=? WHERE id=?",
                (full_err[:1000], full_err[:500], _launch_now(), campaign_id),
            )
            conn.commit()
            conn.close()
        except Exception as db_err:
            logger.error(f"[ManualLaunch] failed to persist error state: {db_err}")


@router.post("/precheck-launch")
def precheck_launch(body: PreCheckBody, user=Depends(get_current_user)):
    return _run_launch_precheck(body, user)


@router.post("/{asset_id:int}/launch")
def launch_campaign(asset_id: int, body: LaunchCampaignBody, user=Depends(get_current_user)):
    _normalize_launch_body(body)
    act_ids = _launch_act_ids(body)
    if not act_ids:
        raise HTTPException(400, "请选择广告账户")
    body.act_id = act_ids[0]
    report = _run_launch_precheck(PreCheckBody(**body.dict()), user)
    block_msg = _build_launch_precheck_block_message(report)
    if block_msg:
        raise HTTPException(400, block_msg)

    conn = get_conn()
    assert_row_access(conn, "ad_assets", asset_id, user, allow_unassigned=False)
    asset_row = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not asset_row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    asset = dict(asset_row)
    _copy_mode = (body.copy_mode or "ai").strip()
    if _copy_mode == "ai":
        if not asset.get("ai_headlines") or not asset.get("ai_bodies"):
            conn.close()
            raise HTTPException(400, "素材还未 AI 分析生成文案，请先跑 AI 分析或选择空/自定义文案")
    elif _copy_mode == "custom":
        if not body.custom_headline or not body.custom_body:
            conn.close()
            raise HTTPException(400, "自定义模式需要在素材详情中填写自定义标题和正文")
    results = []
    for act_id in act_ids:
        try:
            campaign_id = _insert_launch_campaign(conn, asset, act_id, body)
            conn.commit()
            threading.Thread(target=_trigger_manual_launch, args=(campaign_id,), daemon=True).start()
            results.append({"act_id": act_id, "asset_id": asset_id, "campaign_id": campaign_id, "status": "pending", "message": "任务已创建"})
        except Exception as e:
            results.append({"act_id": act_id, "asset_id": asset_id, "campaign_id": None, "status": "error", "message": str(e)})
    conn.close()
    success = sum(1 for r in results if r["status"] == "pending")
    resp = {"total": len(act_ids), "success": success, "failed": len(act_ids) - success, "results": results}
    if len(results) == 1:
        resp.update(results[0])
    return resp


@router.post("/{asset_id:int}/batch-launch")
def batch_launch_campaign(asset_id: int, body: LaunchCampaignBody, user=Depends(get_current_user)):
    return launch_campaign(asset_id, body, user)


@router.get("/{asset_id:int}/campaigns/{campaign_id:int}/status")
def get_launch_campaign_status(asset_id: int, campaign_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    assert_row_access(conn, "ad_assets", asset_id, user, allow_unassigned=False)
    row = conn.execute(
        """SELECT c.id, c.status, c.progress_step, c.progress_msg,
                  c.fb_campaign_id, c.total_adsets, c.total_ads,
                  c.error_msg, c.updated_at,
                  (SELECT COUNT(*) FROM auto_campaign_ads WHERE campaign_id=c.id) AS ad_count,
                  (SELECT COUNT(*) FROM auto_campaign_ads WHERE campaign_id=c.id AND status='done') AS ad_done,
                  (SELECT COUNT(*) FROM auto_campaign_ads WHERE campaign_id=c.id AND status='error') AS ad_error
           FROM auto_campaigns c
           WHERE c.id=? AND c.asset_id=?""",
        (campaign_id, asset_id),
    ).fetchone()
    link_rows = []
    if row:
        try:
            fb_campaign_id = (row["fb_campaign_id"] or "").strip()
            link_rows = conn.execute(
                """SELECT id, slug, public_url, target_url, status,
                          act_id, account_name, campaign_id, campaign_name,
                          adset_id, adset_name, ad_id, ad_name, note, updated_at
                   FROM landing_ad_links
                   WHERE (
                       ? <> '' AND COALESCE(campaign_id,'')=?
                   ) OR COALESCE(ad_id,'') IN (
                       SELECT COALESCE(fb_ad_id,'')
                       FROM auto_campaign_ads
                       WHERE campaign_id=? AND COALESCE(fb_ad_id,'')<>''
                   )
                   ORDER BY id ASC
                   LIMIT 200""",
                (fb_campaign_id, fb_campaign_id, campaign_id),
            ).fetchall()
        except Exception as exc:
            logger.warning("failed to load launch landing links: %s", exc)
    conn.close()
    if not row:
        raise HTTPException(404, "任务不存在")
    r = dict(row)
    r["ad_links"] = [dict(x) for x in link_rows]
    step = r.get("progress_step") or ""
    step_map = {"init": 5, "token": 10, "asset": 20, "upload": 35, "campaign": 50, "adset_1": 60, "adset_2": 70, "adset_3": 80, "adset_4": 85, "adset_5": 88, "done": 100}
    if r["status"] == "error":
        r["progress_pct"] = 0
        if not r.get("progress_msg") and r.get("error_msg"):
            r["progress_msg"] = r["error_msg"]
    elif r["status"] == "done":
        r["progress_pct"] = 100
    else:
        ad_count = int(r.get("ad_count") or 0)
        finished = int(r.get("ad_done") or 0) + int(r.get("ad_error") or 0)
        base = step_map.get(step, 5)
        r["progress_pct"] = max(base, min(98, 88 + int((finished / max(ad_count, 1)) * 10))) if ad_count else base
    return r


@router.get("/{asset_id:int}/breakdown")
def get_asset_breakdown(
    asset_id: int,
    force_refresh: bool = False,
    user=Depends(get_current_user)
):
    """
    返回素材在各广告账户的投放明细.

    数据来源策略(v3.1 持久化):
    - 默认:优先从 asset_spend_log 数据库读取(含历史/已移除账户数据)
    - force_refresh=true:强制从 FB API 重新拉取并更新数据库,再返回

    这样即使账户被移除、Token 过期、素材被替换,历史绩效数据永不丢失.
    """
    import json as _json

    guard_conn = get_conn()
    try:
        assert_row_access(guard_conn, "ad_assets", asset_id, user, allow_unassigned=False)
    finally:
        guard_conn.close()

    # ── force_refresh:先触发一次完整的 score_asset 更新数据库 ──────────────
    if force_refresh:
        try:
            from services.asset_scorer import score_asset
            score_asset(asset_id)
        except Exception as e:
            pass  # 即使刷新失败,也继续返回数据库中的历史数据

    conn = get_conn()

    # ── 从 asset_spend_log 按账户聚合 ────────────────────────────────────────
    log_rows = conn.execute(
        """
        SELECT
            act_id,
            MAX(act_name) as act_name,
            MAX(target_countries) as target_countries,
            MAX(objective) as objective,
            MAX(kpi_field) as kpi_field,
            GROUP_CONCAT(DISTINCT matched_field) as matched_fields,
            COUNT(*) as ad_count,
            SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) as ad_active,
            SUM(CASE WHEN is_active=0 THEN 1 ELSE 0 END) as ad_inactive,
            SUM(spend) as spend,
            SUM(conv) as conv,
            SUM(conv_value) as conv_value,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            MAX(last_synced_at) as last_synced_at
        FROM asset_spend_log
        WHERE asset_id = ?
        GROUP BY act_id
        ORDER BY spend DESC
        """,
        (asset_id,)
    ).fetchall()

    # ── 正常路径:从 asset_spend_log 构建响应 ────────────────────────────────
    breakdown = []
    total_spend = 0.0
    total_conv = 0
    total_ads = 0

    for row in log_rows:
        item = dict(row)
        act_id = item["act_id"]
        act_spend = float(item["spend"] or 0)
        act_conv = int(item["conv"] or 0)

        ad_count = item["ad_count"]
        total_ad_count = ad_count

        total_spend += act_spend
        total_conv += act_conv
        total_ads += total_ad_count

        # 解析国家
        tc = item.get("target_countries") or ""
        try:
            countries = _json.loads(tc) if tc.startswith("[") else [c.strip() for c in tc.split(",") if c.strip()]
        except Exception:
            countries = [c.strip() for c in tc.split(",") if c.strip()]

        # 解析 matched_fields
        mf = item.get("matched_fields") or ""
        matched_fields_list = [f.strip() for f in mf.split(",") if f.strip()]

        breakdown.append({
            "act_id": act_id,
            "act_name": item["act_name"] or act_id,
            "ad_count": total_ad_count,
            "ad_synced": ad_count,       # 已成功同步数据的广告数
            "ad_active": item["ad_active"],   # 本次巡检可拉取的广告数
            "ad_inactive": item["ad_inactive"],  # 历史/已移除账户的广告数
            "ad_done": total_ad_count,
            "ad_error": 0,
            "countries": countries,
            "objective": item["objective"],
            "daily_budget": None,
            "target_cpa": None,
            "kpi_field": item["kpi_field"] or "",
            "matched_field": matched_fields_list[0] if matched_fields_list else "",
            "matched_fields": matched_fields_list,
            "spend": round(act_spend, 2),
            "conv": act_conv,
            "impressions": int(item["impressions"] or 0),
            "clicks": int(item["clicks"] or 0),
            "cpa": round(act_spend / act_conv, 2) if act_conv > 0 else None,
            "last_synced_at": item["last_synced_at"],
            "is_cached": True  # 标记为数据库缓存数据
        })

    summary = {
        "total_spend": round(total_spend, 2),
        "total_conv": total_conv,
        "total_ads": total_ads,
        "avg_cpa": round(total_spend / total_conv, 2) if total_conv > 0 else None
    }
    return {"breakdown": breakdown, "summary": summary, "data_source": "cached"}


# ── 账户搜索接口(供铺广告多账户选择使用)──────────────────────────────────────
@router.get("/accounts/search")
def search_accounts(
    q: str = Query("", description="搜索关键词(账户名称或ID)"),
    limit: int = Query(20, ge=1, le=100),
    user=Depends(get_current_user)
):
    """搜索广告账户,支持按账户名称或ID模糊搜索"""
    conn = get_conn()
    keyword = f"%{q}%"
    where = [
        "(name LIKE ? OR act_id LIKE ?)",
        "COALESCE(enabled, 1)=1",
        "CAST(COALESCE(account_status, 1) AS INTEGER)=1",
    ]
    params = [keyword, keyword]
    apply_team_scope(where, params, user, "team_id", include_unassigned=False)
    apply_account_owner_scope(where, params, user, "owner_user_id")
    rows = conn.execute(
        f"""SELECT act_id, name, currency, account_status, balance, timezone
           FROM accounts
           WHERE {' AND '.join(where)}
           ORDER BY name ASC
           LIMIT ?""",
        params + [limit]
    ).fetchall()
    items = []
    for r in rows:
        d = dict(r)
        d["status"] = d.get("account_status")
        d["linked_matrix_ids"] = _matrix_ids_for_act(conn, d.get("act_id"))
        items.append(d)
    conn.close()
    return items
