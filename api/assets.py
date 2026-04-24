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
    for field in ("ai_analysis", "ai_headlines", "ai_bodies", "ai_interests", "target_countries"):
        if d.get(field) and isinstance(d[field], str):
            try:
                d[field] = json.loads(d[field])
            except Exception:
                pass
    # 生成可访问的 URL(通过 serve 接口)
    asset_id = d.get("id")
    if asset_id:
        d["file_url"] = f"/api/assets/serve/{asset_id}/file"
        d["thumb_url"] = f"/api/assets/serve/{asset_id}/thumb" if d.get("thumb_path") else ""
    return d


def _ensure_asset_library_columns(conn) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(ad_assets)").fetchall()}
    changed = False
    if "folder_name" not in cols:
        conn.execute("ALTER TABLE ad_assets ADD COLUMN folder_name TEXT")
        changed = True
    if "batch_code" not in cols:
        conn.execute("ALTER TABLE ad_assets ADD COLUMN batch_code TEXT")
        changed = True
    if changed:
        conn.commit()


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
    if target_country:
        where.append("(target_countries LIKE ? OR target_countries LIKE ? OR target_countries LIKE ? OR target_countries=?)")
        params.extend([f"{target_country},%", f"%,{target_country},%", f"%,{target_country}", target_country])
    if folder_name:
        where.append("COALESCE(folder_name,'')=?"); params.append(folder_name)
    if batch_code:
        where.append("COALESCE(batch_code,'')=?"); params.append(batch_code)
    if search and search.strip():
        kw = f"%{search.strip()}%"
        where.append(
            "(COALESCE(display_name,'') LIKE ? OR COALESCE(file_name,'') LIKE ? OR "
            "COALESCE(asset_code,'') LIKE ? OR COALESCE(note,'') LIKE ? OR "
            "COALESCE(folder_name,'') LIKE ? OR COALESCE(batch_code,'') LIKE ?)"
        )
        params.extend([kw, kw, kw, kw, kw, kw])
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM ad_assets {clause}", params).fetchone()[0]
    rows = conn.execute(
        f"""SELECT a.*,
               (SELECT COUNT(*) FROM auto_campaign_ads aca
                JOIN auto_campaigns ac ON ac.id=aca.campaign_id
                WHERE aca.asset_id=a.id AND aca.fb_ad_id IS NOT NULL) as ad_count,
               (SELECT COUNT(DISTINCT aca2.campaign_id) FROM auto_campaign_ads aca2
                WHERE aca2.asset_id=a.id) as campaign_count
           FROM ad_assets a {clause} ORDER BY a.created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset]
    ).fetchall()
    conn.close()
    return {"total": total, "items": [_row_to_dict(r) for r in rows]}


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




# ── 预热素材库 CRUD ────────────────────────────────────────────────────────────
# 注意：warmup_assets表使用 label 作为名称字段，is_active 控制启用状态
# 兼容旧表结构：id/filename/file_path/thumb_path/label/is_active/use_count/created_at/updated_at/status/enabled/asset_type/file_url/ad_text

class WarmupAssetIn(BaseModel):
    label: str                      # 素材名称/标签
    asset_type: str = "image"       # image / video
    file_url: Optional[str] = None  # 外部链接（可选）
    ad_text: Optional[str] = None   # 广告文案（可选）
    note: Optional[str] = None      # 备注


class WarmupAssetUpdate(BaseModel):
    """预热素材更新（所有字段可选）"""
    name: Optional[str] = None
    label: Optional[str] = None
    active: Optional[bool] = None
    is_active: Optional[int] = None
    enabled: Optional[int] = None
    asset_type: Optional[str] = None
    file_url: Optional[str] = None
    ad_text: Optional[str] = None


# 模块加载时初始化一次（避免每次请求都执行DDL导致锁竞争）
_warmup_table_initialized = False

def _ensure_warmup_table():
    """确保warmup_assets表存在，并补全缺失列（每进程只初始化一次）"""
    global _warmup_table_initialized
    if _warmup_table_initialized:
        return
    conn = None
    try:
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS warmup_assets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                filename    TEXT,
                file_path   TEXT,
                thumb_path  TEXT,
                label       TEXT DEFAULT '预热素材',
                is_active   INTEGER DEFAULT 1,
                use_count   INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now','localtime')),
                updated_at  TEXT DEFAULT (datetime('now','localtime')),
                status      TEXT DEFAULT 'active',
                enabled     INTEGER DEFAULT 1,
                asset_type  TEXT DEFAULT 'image',
                file_url    TEXT,
                ad_text     TEXT
            )
        """)
        # 补全可能缺失的列（向后兼容）
        existing = [r[1] for r in conn.execute('PRAGMA table_info(warmup_assets)').fetchall()]
        for col, defn in [
            ('enabled',    'INTEGER DEFAULT 1'),
            ('asset_type', "TEXT DEFAULT 'image'"),
            ('file_url',   'TEXT'),
            ('ad_text',    'TEXT'),
            ('updated_at', 'TEXT'),
        ]:
            if col not in existing:
                conn.execute(f'ALTER TABLE warmup_assets ADD COLUMN {col} {defn}')
        conn.commit()
        _warmup_table_initialized = True
    except Exception:
        pass  # 表已存在或初始化失败时忽略
    finally:
        if conn:
            conn.close()


def _warmup_row_to_dict(row) -> dict:
    """将warmup_assets行转为统一格式的字典，兼容旧字段"""
    d = dict(row)
    # 统一用 name 字段暴露给前端（映射自 label）
    d['name'] = d.get('label') or d.get('filename') or ''
    # 统一用 active 字段（映射自 is_active 和 enabled）
    d['active'] = bool(d.get('is_active', 1) and d.get('enabled', 1))
    # 生成可访问的缩略图URL（有file_path则提供serve路由）
    if d.get('file_path') and os.path.exists(str(d['file_path'])):
        d['thumb_url'] = f"/api/assets/warmup/{d['id']}/thumb"
    elif d.get('file_url'):
        d['thumb_url'] = d['file_url']
    else:
        d['thumb_url'] = ''
    return d


@router.get("/warmup")
def list_warmup_assets(user=Depends(get_current_user)):
    """获取预热素材列表"""
    _ensure_warmup_table()
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM warmup_assets ORDER BY is_active DESC, use_count DESC, id DESC"
    ).fetchall()
    conn.close()
    return [_warmup_row_to_dict(r) for r in rows]


@router.post("/warmup")
def create_warmup_asset(body: WarmupAssetIn, user=Depends(get_current_user)):
    """添加预热素材（纯文字/链接类型，无需上传文件）"""
    _ensure_warmup_table()
    conn = get_conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
        """INSERT INTO warmup_assets (label, asset_type, file_url, ad_text, is_active, enabled, created_at, updated_at)
           VALUES (?, ?, ?, ?, 1, 1, ?, ?)""",
        (body.label, body.asset_type, body.file_url, body.ad_text, now, now)
    )
    conn.commit()
    new_id = cur.lastrowid
    row = conn.execute("SELECT * FROM warmup_assets WHERE id=?", (new_id,)).fetchone()
    conn.close()
    return _warmup_row_to_dict(row)


@router.patch("/warmup/{warmup_id}")
def update_warmup_asset(warmup_id: int, body: WarmupAssetUpdate, user=Depends(get_current_user)):
    """更新预热素材（启用/停用/修改字段）"""
    _ensure_warmup_table()
    conn = get_conn()
    row = conn.execute("SELECT * FROM warmup_assets WHERE id=?", (warmup_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="预热素材不存在")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 字段映射：前端传 name 映射到 label，传 active 映射到 is_active+enabled
    field_map = {'name': 'label', 'active': 'is_active'}
    allowed_direct = {'label', 'asset_type', 'file_url', 'ad_text', 'is_active', 'enabled'}
    updates = {}
    for k, v in body.model_dump(exclude_none=True).items():
        mapped = field_map.get(k, k)
        if mapped in allowed_direct:
            updates[mapped] = v
            if mapped == 'is_active':  # 同步 enabled 字段
                updates['enabled'] = v
    if not updates:
        conn.close()
        raise HTTPException(status_code=400, detail="无有效更新字段")
    set_clause = ", ".join([f"{k}=?" for k in updates])
    values = list(updates.values()) + [now, warmup_id]
    conn.execute(f"UPDATE warmup_assets SET {set_clause}, updated_at=? WHERE id=?", values)
    conn.commit()
    row = conn.execute("SELECT * FROM warmup_assets WHERE id=?", (warmup_id,)).fetchone()
    conn.close()
    return _warmup_row_to_dict(row)


@router.delete("/warmup/{warmup_id}")
def delete_warmup_asset(warmup_id: int, user=Depends(get_current_user)):
    """删除预热素材"""
    _ensure_warmup_table()
    conn = get_conn()
    row = conn.execute("SELECT id FROM warmup_assets WHERE id=?", (warmup_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="预热素材不存在")
    conn.execute("DELETE FROM warmup_assets WHERE id=?", (warmup_id,))
    conn.commit()
    conn.close()
    return {"ok": True, "message": f"预热素材 {warmup_id} 已删除"}


@router.get("/warmup/{warmup_id}/thumb")
def serve_warmup_thumb(warmup_id: int):
    """提供预热素材的缩略图（无需鉴权，供img标签直接访问）"""
    _ensure_warmup_table()
    conn = get_conn()
    row = conn.execute("SELECT file_path, thumb_path, label FROM warmup_assets WHERE id=?", (warmup_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="预热素材不存在")
    # 优先用thumb_path，降级用file_path
    path = row['thumb_path'] or row['file_path']
    if not path or not os.path.exists(str(path)):
        raise HTTPException(status_code=404, detail="图片文件不存在")
    from fastapi.responses import FileResponse
    return FileResponse(path)


@router.get("/{asset_id}")
def get_asset(asset_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    row = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "素材不存在")
    return _row_to_dict(row)


# ── 上传 ──────────────────────────────────────────────────────────────────────

@router.post("")
async def upload_asset(
    file: UploadFile = File(...),
    act_id: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    target_countries: Optional[str] = Form(None),
    folder_name: Optional[str] = Form(None),
    batch_code: Optional[str] = Form(None),
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
    existing = conn.execute(
        "SELECT id, file_name FROM ad_assets WHERE file_hash=?", (file_hash,)
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
                file_size, file_hash, upload_status, note, target_countries, folder_name, batch_code, asset_code, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,'local_saved',?,?,?,?,?,?,?)""",
            (act_id, file.filename, file.filename, file_type, save_path, thumb_path,
             len(content), file_hash, note, target_countries, folder_name, batch_code, asset_code_new, now, now)
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

@router.post("/{asset_id}/analyze")
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
    conn.execute(
        "UPDATE ad_assets SET upload_status='ai_pending', ai_purpose=?, ai_language=?, updated_at=? WHERE id=?",
        (purpose, ",".join(languages),
         datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), asset_id)
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


@router.post("/{asset_id}/rename")
def rename_asset(asset_id: int, body: RenameBody, user=Depends(get_current_user)):
    """重命名素材展示名称(不影响实际文件名)"""
    name = body.display_name.strip()
    if not name:
        raise HTTPException(400, "名称不能为空")
    conn = get_conn()
    row = conn.execute("SELECT id FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    conn.execute(
        "UPDATE ad_assets SET display_name=?, updated_at=? WHERE id=?",
        (name, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), asset_id)
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
    landing_url: Optional[str] = None  # 素材级落地页链接
    target_countries: Optional[str] = None
    folder_name: Optional[str] = None
    batch_code: Optional[str] = None


@router.put("/{asset_id}")
def update_asset(asset_id: int, body: AssetUpdate, user=Depends(get_current_user)):
    conn = get_conn()
    _ensure_asset_library_columns(conn)
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
    if updates:
        updates.append("updated_at=?")
        params.append(datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
        params.append(asset_id)
        conn.execute(f"UPDATE ad_assets SET {','.join(updates)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return {"message": "更新成功"}


# ── 删除 ──────────────────────────────────────────────────────────────────────

@router.delete("/{asset_id}")
def delete_asset(asset_id: int, user=Depends(get_current_user)):
    """删除素材(同时删除本地文件和缩略图)"""
    conn = get_conn()
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


# ── 一键启动自动铺广告 ────────────────────────────────────────────────────────

class LaunchCampaignBody(BaseModel):
    act_id: str  # 单账户(兼容旧版)
    act_ids: Optional[list] = None  # 多账户批量铺广告(优先级高于 act_id)
    target_countries: list = ["US"]
    target_cpa: Optional[float] = None
    daily_budget: float = 20.0
    objective: str = "OUTCOME_SALES"
    headline_idx: Optional[int] = None
    body_idx: Optional[int] = None
    # 受众定向
    age_min: int = 18
    age_max: int = 65
    gender: int = 0  # 0=不限, 1=男, 2=女
    # 版位设置
    placements: Optional[dict] = None  # None=自动版位
    # 出价策略
    bid_strategy: str = "LOWEST_COST_WITHOUT_CAP"
    max_adsets: int = 5
    # 覆盖账户默认的page_id/pixel_id
    page_id: Optional[str] = None
    pixel_id: Optional[str] = None
    # 转化事件细分(仅 OUTCOME_SALES/OUTCOME_LEADS 有效)
    conversion_event: Optional[str] = None
    # 落地页链接(三层优先级:此处 > 素材绑定 > 全局默认)
    landing_url: Optional[str] = None
    # v4.0: 设备端和语言
    device_platforms: str = "all"   # all / mobile / desktop
    ad_language: str = "en"
    # 台湾认证广告主ID(旧版兼容)
    tw_advertiser_id: Optional[int] = None
     # 台湾认证主页ID(新版:直接传主页ID字符串,优先级高于 tw_advertiser_id)
    tw_page_id: Optional[str] = None
    # 转化目的(ODAX 细分目标,控制 optimization_goal)
    conversion_goal: Optional[str] = None
    # 消息广告欢迎消息模板(纯文本或 JSON 字符串,留空=自动生成)
    message_template: Optional[str] = None
    # 潜在客户表单 ID(Lead Form,留空=使用 SIGN_UP 按钮替代)
    lead_form_id: Optional[str] = None
    cta_type: Optional[str] = None  # 行动号召按钮类型,如 LEARN_MORE/SHOP_NOW/MESSAGE_PAGE 等

# ── 铺广告预检接口 ─────────────────────────────────────────────────────────────
_MSG_GOALS = {
    "conversations",
    "messaging_purchase_conversion",
    "messaging_appointment_conversion",
    "messaging_leads",
}
_MESSAGE_OBJECTIVES = {"OUTCOME_MESSAGES", "OUTCOME_MESSAGING", "MESSAGES"}
_CTA_LINK_TYPES = {
    "SHOP_NOW", "LEARN_MORE", "SIGN_UP", "GET_OFFER",
    "DOWNLOAD", "BOOK_TRAVEL", "CONTACT_US", "SUBSCRIBE",
    "GET_QUOTE", "WATCH_MORE", "APPLY_NOW", "GET_DIRECTIONS",
    "ORDER_NOW", "BUY_NOW", "SEE_MENU",
}
_CTA_MSG_TYPES = {
    "MESSAGE_PAGE", "SEND_MESSAGE", "WHATSAPP_MESSAGE",
    "MESSENGER_MESSAGE", "INSTAGRAM_MESSAGE",
}
_PIXEL_REQUIRED_GOALS = {"offsite_conversions", "value"}
_LANDING_REQUIRED_OBJECTIVES = {"OUTCOME_TRAFFIC", "OUTCOME_SALES", "OUTCOME_ENGAGEMENT"}
_REGULATED_IDENTITY_COUNTRIES = {"TW", "HK", "SG"}


def _clean_launch_value(value: Optional[str]) -> str:
    return (value or "").strip()


def _looks_like_http_url(value: str) -> bool:
    value = _clean_launch_value(value).lower()
    return value.startswith("http://") or value.startswith("https://")


def _is_facebook_domain_url(value: str) -> bool:
    value = _clean_launch_value(value).lower()
    return any(host in value for host in ("facebook.com", "fb.com", "messenger.com", "instagram.com"))


_LEAD_FORM_BLOCKED_HOSTS = {
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


def _get_blocked_lead_form_host(value: str) -> str:
    value = _clean_launch_value(value)
    if not _looks_like_http_url(value):
        return ""
    try:
        host = (urlparse(value).hostname or "").lower().strip(".")
    except Exception:
        return ""
    if not host:
        return ""
    for blocked in _LEAD_FORM_BLOCKED_HOSTS:
        if host == blocked or host.endswith("." + blocked):
            return host
    return ""


def _normalize_launch_objective(value: Optional[str]) -> str:
    objective_norm = (value or "OUTCOME_SALES").strip().upper()
    if objective_norm in _MESSAGE_OBJECTIVES:
        return "OUTCOME_MESSAGES"
    return objective_norm or "OUTCOME_SALES"


def _normalize_launch_goal_fields(objective: Optional[str], conversion_goal: Optional[str]) -> tuple[str, str]:
    objective_norm = _normalize_launch_objective(objective)
    goal_norm = _clean_launch_value(conversion_goal).lower()
    if objective_norm == "OUTCOME_MESSAGES" and not goal_norm:
        goal_norm = "conversations"
    return objective_norm, goal_norm


def _normalize_launch_body_fields(body) -> tuple[str, str]:
    objective_norm, goal_norm = _normalize_launch_goal_fields(
        getattr(body, "objective", None),
        getattr(body, "conversion_goal", None),
    )
    try:
        body.objective = objective_norm
    except Exception:
        pass
    try:
        body.conversion_goal = goal_norm or None
    except Exception:
        pass
    tw_page_id = _clean_launch_value(getattr(body, "tw_page_id", None))
    if not tw_page_id and getattr(body, "tw_advertiser_id", None) not in (None, ""):
        tw_page_id = str(getattr(body, "tw_advertiser_id")).strip()
    try:
        body.tw_page_id = tw_page_id or None
    except Exception:
        pass
    return objective_norm, goal_norm


def _get_launch_goal_meta(objective: Optional[str], conversion_goal: Optional[str]) -> dict:
    objective_norm, goal_norm = _normalize_launch_goal_fields(objective, conversion_goal)
    is_message = objective_norm in _MESSAGE_OBJECTIVES or goal_norm in _MSG_GOALS
    is_lead = goal_norm == "lead_generation"
    landing_required = (
        objective_norm in _LANDING_REQUIRED_OBJECTIVES
        and not is_message
        and not is_lead
        and goal_norm != "page_likes"
    )
    return {
        "objective": objective_norm,
        "goal": goal_norm,
        "is_message": is_message,
        "is_lead": is_lead,
        "pixel_required": goal_norm in _PIXEL_REQUIRED_GOALS,
        "landing_required": landing_required,
    }


def _parse_launch_country_codes(raw_value) -> list[str]:
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


def _get_regulated_identity_countries(raw_value) -> list[str]:
    codes = _parse_launch_country_codes(raw_value)
    return [code for code in codes if code in _REGULATED_IDENTITY_COUNTRIES]


def _normalize_verified_identity_value(value) -> str:
    cleaned = _clean_launch_value("" if value is None else str(value))
    return "" if cleaned.lower() in {"none", "null", "undefined"} else cleaned


def _get_account_verified_identity_context(act_id: str, preferred_page_id: str = "") -> dict:
    conn = get_conn()
    try:
        try:
            from services.token_manager import get_matrix_id_for_account
            matrix_id = get_matrix_id_for_account(act_id)
        except Exception:
            matrix_id = None
        acc_row = conn.execute(
            """
            SELECT a.name
            FROM accounts a
            WHERE a.act_id=?
            """,
            (act_id,),
        ).fetchone()
        matrix_page_count = 0
        if matrix_id is not None:
            matrix_page_count = conn.execute(
                "SELECT COUNT(*) FROM tw_certified_pages WHERE matrix_id=?",
                (matrix_id,),
            ).fetchone()[0]
        preferred_row = None
        if preferred_page_id:
            preferred_row = conn.execute(
                """
                SELECT id, page_id, page_name, verified_identity_id, matrix_id, token_id, note
                FROM tw_certified_pages
                WHERE page_id=?
                LIMIT 1
                """,
                (preferred_page_id,),
            ).fetchone()
        valid_rows = []
        if matrix_id is not None:
            valid_rows = conn.execute(
                """
                SELECT id, page_id, page_name, verified_identity_id, matrix_id, token_id, note
                FROM tw_certified_pages
                WHERE matrix_id=?
                  AND verified_identity_id IS NOT NULL
                  AND TRIM(verified_identity_id) != ''
                  AND LOWER(TRIM(verified_identity_id)) NOT IN ('none','null','undefined')
                ORDER BY id ASC
                """,
                (matrix_id,),
            ).fetchall()
    finally:
        conn.close()

    preferred = dict(preferred_row) if preferred_row else None
    valid_pages = [dict(row) for row in valid_rows]
    usable_page = None
    if preferred and preferred.get("matrix_id") == matrix_id and _normalize_verified_identity_value(preferred.get("verified_identity_id")):
        usable_page = preferred
    elif valid_pages:
        usable_page = valid_pages[0]

    return {
        "matrix_id": matrix_id,
        "matrix_page_count": matrix_page_count,
        "preferred_page": preferred,
        "valid_pages": valid_pages,
        "usable_page": usable_page,
        "account_name": acc_row["name"] if acc_row else act_id,
    }


def _get_account_launch_defaults(act_id: str) -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT act_id, name, page_id, pixel_id, landing_url, form_link
            FROM accounts
            WHERE act_id=?
            """,
            (act_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {
            "act_id": act_id,
            "name": act_id,
            "page_id": "",
            "pixel_id": "",
            "landing_url": "",
            "form_link": "",
        }
    return {
        "act_id": row["act_id"] or act_id,
        "name": row["name"] or act_id,
        "page_id": row["page_id"] or "",
        "pixel_id": row["pixel_id"] or "",
        "landing_url": row["landing_url"] or "",
        "form_link": row["form_link"] or "",
    }


def _parse_page_messaging_enabled(page_data: dict) -> Optional[bool]:
    messaging_feature_status = page_data.get("messaging_feature_status") or {}
    features = page_data.get("features") or []
    if isinstance(messaging_feature_status, dict) and messaging_feature_status:
        for key in ("USER_MESSAGING", "MESSENGER_PLATFORM", "MESSENGER"):
            status = str(messaging_feature_status.get(key, "")).upper()
            if status == "ENABLED":
                return True
        normalized_values = [str(v).upper() for v in messaging_feature_status.values() if v is not None]
        if normalized_values:
            return False
    if features:
        return any("messag" in str(f).lower() or "whatsapp" in str(f).lower() for f in features)
    return None


def _fetch_page_access_context(requests_mod, fb_base: str, token: str, page_id: str) -> dict:
    if not token or not page_id:
        return {
            "found": False,
            "can_use": False,
            "page_name": page_id or "",
            "tasks": [],
            "page_token": "",
            "error": "missing token or page_id",
        }
    try:
        resp = requests_mod.get(
            f"{fb_base}/me/accounts",
            params={
                "access_token": token,
                "fields": "id,name,tasks,access_token",
                "limit": 200,
            },
            timeout=12,
        )
        data = resp.json()
    except Exception as exc:
        return {
            "found": None,
            "can_use": None,
            "page_name": page_id,
            "tasks": [],
            "page_token": "",
            "error": str(exc),
        }

    if "error" in data:
        return {
            "found": None,
            "can_use": None,
            "page_name": page_id,
            "tasks": [],
            "page_token": "",
            "error": data["error"].get("message", "failed to query /me/accounts"),
        }

    for page in data.get("data", []):
        if str(page.get("id")) != str(page_id):
            continue
        tasks = page.get("tasks") or []
        return {
            "found": True,
            "can_use": (not tasks) or ("ADVERTISE" in tasks) or ("MANAGE" in tasks),
            "page_name": page.get("name") or page_id,
            "tasks": tasks,
            "page_token": page.get("access_token") or "",
            "error": "",
        }

    return {
        "found": False,
        "can_use": False,
        "page_name": page_id,
        "tasks": [],
        "page_token": "",
        "error": f"current token cannot find page {page_id} in /me/accounts",
    }


def _probe_page_messaging_state(requests_mod, fb_base: str, token: str, page_ctx: dict, page_id: str) -> dict:
    probe_token = page_ctx.get("page_token") or token
    if not probe_token or not page_id:
        return {"enabled": None, "error": "missing token or page_id"}
    try:
        resp = requests_mod.get(
            f"{fb_base}/{page_id}",
            params={"access_token": probe_token, "fields": "id,name,messaging_feature_status,features"},
            timeout=10,
        )
        data = resp.json()
    except Exception as exc:
        return {"enabled": None, "error": str(exc)}

    if "error" in data:
        return {"enabled": None, "error": data["error"].get("message", "failed to probe page messaging")}

    return {"enabled": _parse_page_messaging_enabled(data), "error": ""}


def _probe_lead_form_permission(requests_mod, fb_base: str, token: str, page_id: str, page_ctx: Optional[dict] = None) -> dict:
    probe_token = (page_ctx or {}).get("page_token") or token
    if not probe_token or not page_id:
        return {"ok": None, "permission_blocked": False, "error": "missing token or page_id"}
    try:
        resp = requests_mod.get(
            f"{fb_base}/{page_id}/leadgen_forms",
            params={"access_token": probe_token, "limit": 1},
            timeout=10,
        )
        data = resp.json()
    except Exception as exc:
        return {"ok": None, "permission_blocked": False, "error": str(exc)}

    if "error" not in data:
        return {"ok": True, "permission_blocked": False, "error": ""}

    err = data["error"]
    err_msg = err.get("message", "leadgen_forms probe failed")
    err_code = err.get("code", 0)
    lower_msg = err_msg.lower()
    permission_blocked = (
        err_code in {10, 190, 200}
        or "permission" in lower_msg
        or "pages_manage_ads" in lower_msg
        or "page access token" in lower_msg
    )
    return {"ok": False, "permission_blocked": permission_blocked, "error": err_msg}


def _build_lead_tos_manual_hint(page_id: str) -> str:
    page_label = f"主页 {page_id}" if page_id else "当前主页"
    return (
        f"{page_label} 的 Lead 服务条款无法再通过旧接口自动探测。"
        "如果该主页是首次投放 Lead 广告，请先以主页身份访问 "
        "https://www.facebook.com/ads/leadgen/tos 完成确认；"
        "否则 Facebook 会在创建 AdSet 时直接拒绝发布。"
    )


def _run_launch_precheck(body) -> dict:
    import requests as _req

    FB_BASE = "https://graph.facebook.com/v21.0"
    _normalize_launch_body_fields(body)
    meta = _get_launch_goal_meta(body.objective, body.conversion_goal)
    default_page_id = _clean_launch_value(_get_setting("autopilot_fb_page_id", ""))
    default_pixel_id = _clean_launch_value(_get_setting("autopilot_fb_pixel_id", ""))
    default_landing_url = _clean_launch_value(_get_setting("default_landing_url", ""))
    default_lead_privacy_url = _clean_launch_value(_get_setting("lead_form_privacy_url", ""))

    act_ids: list[str] = []
    if getattr(body, "act_ids", None):
        act_ids.extend([_clean_launch_value(act_id) for act_id in body.act_ids if _clean_launch_value(act_id)])
    if _clean_launch_value(getattr(body, "act_id", None)):
        act_ids.append(_clean_launch_value(body.act_id))
    act_ids = list(dict.fromkeys(act_ids))
    if not act_ids:
        raise HTTPException(status_code=400, detail="请至少选择一个广告账户")

    def _check_one_account(act_id: str) -> dict:
        account_defaults = _get_account_launch_defaults(act_id)
        results = []
        selected_verified_page_id = _clean_launch_value(getattr(body, "tw_page_id", None))
        regulated_countries = _get_regulated_identity_countries(getattr(body, "target_countries", None))
        identity_ctx = (
            _get_account_verified_identity_context(act_id, selected_verified_page_id)
            if regulated_countries
            else None
        )

        try:
            from services.token_manager import ACTION_READ, get_exec_token
            token = get_exec_token(act_id, ACTION_READ)
        except Exception:
            token = None

        if not token:
            results.append({
                "key": "token",
                "label": "操作 Token",
                "status": "fail",
                "msg": "未找到可用的操作 Token，请先绑定并验证 Token。",
            })
            return {
                "act_id": act_id,
                "account_name": account_defaults.get("name") or act_id,
                "pass": False,
                "overall": "fail",
                "items": results,
            }

        if regulated_countries:
            country_label = "/".join(regulated_countries)
            matrix_id = identity_ctx.get("matrix_id") if identity_ctx else None
            preferred_page = (identity_ctx or {}).get("preferred_page") or {}
            usable_page = (identity_ctx or {}).get("usable_page") or {}
            matrix_page_count = (identity_ctx or {}).get("matrix_page_count") or 0
            if selected_verified_page_id:
                if not preferred_page:
                    results.append({
                        "key": "verified_identity",
                        "label": "认证主页 / Verified ID",
                        "status": "fail",
                        "msg": f"当前选择的认证主页 {selected_verified_page_id} 不在主页库中，请先在主页库录入并填写 Verified ID。",
                    })
                elif matrix_id is None:
                    results.append({
                        "key": "verified_identity",
                        "label": "认证主页 / Verified ID",
                        "status": "fail",
                        "msg": f"当前账户暂未识别矩阵，无法确认主页 {preferred_page.get('page_name') or selected_verified_page_id} 是否属于同矩阵。{country_label} 属于需要认证的国家，请先绑定矩阵操作号或主 Token，再使用同矩阵且已填写 Verified ID 的主页。",
                    })
                elif matrix_id is not None and preferred_page.get("matrix_id") not in (None, matrix_id):
                    results.append({
                        "key": "verified_identity",
                        "label": "认证主页 / Verified ID",
                        "status": "fail",
                        "msg": f"当前选择的认证主页属于矩阵 {preferred_page.get('matrix_id')}，但账户 {act_id} 位于矩阵 {matrix_id}。{country_label} 属于需要认证的国家，请改用同矩阵且已填写 Verified ID 的主页。",
                    })
                elif not _normalize_verified_identity_value(preferred_page.get("verified_identity_id")):
                    results.append({
                        "key": "verified_identity",
                        "label": "认证主页 / Verified ID",
                        "status": "fail",
                        "msg": f"当前选择的主页 {preferred_page.get('page_name') or selected_verified_page_id} 还没有填写 Verified ID，{country_label} 属于需要认证的国家，暂时不能投放。",
                    })
                else:
                    results.append({
                        "key": "verified_identity",
                        "label": "认证主页 / Verified ID",
                        "status": "pass",
                        "msg": f"已选择合规主页 {preferred_page.get('page_name') or selected_verified_page_id}，矩阵 {preferred_page.get('matrix_id') or matrix_id} 已填写 Verified ID，可用于 {country_label} 投放。",
                    })
            elif usable_page:
                results.append({
                    "key": "verified_identity",
                    "label": "认证主页 / Verified ID",
                    "status": "pass",
                    "msg": f"矩阵 {usable_page.get('matrix_id') or matrix_id} 已配置可用 Verified ID，提交时会自动使用主页 {usable_page.get('page_name') or usable_page.get('page_id')} 处理 {country_label} 认证投放。",
                })
            else:
                if matrix_id is None:
                    fail_msg = f"当前账户暂未识别矩阵，{country_label} 属于需要认证的国家，请先绑定矩阵操作号或主 Token，并在对应矩阵的主页库中填写 Verified ID。"
                elif matrix_page_count > 0:
                    fail_msg = f"矩阵 {matrix_id if matrix_id is not None else '-'} 的主页库里还没有填写 Verified ID，{country_label} 属于需要认证的国家，当前不能投放。"
                else:
                    fail_msg = f"矩阵 {matrix_id if matrix_id is not None else '-'} 还没有录入任何认证主页，{country_label} 属于需要认证的国家，请先在主页库中录入主页并填写 Verified ID。"
                results.append({
                    "key": "verified_identity",
                    "label": "认证主页 / Verified ID",
                    "status": "fail",
                    "msg": fail_msg,
                })

        objective_label = {
            "OUTCOME_TRAFFIC": "流量点击",
            "OUTCOME_SALES": "转化购买",
            "OUTCOME_ENGAGEMENT": "帖子互动",
            "OUTCOME_LEADS": "线索收集",
            "OUTCOME_MESSAGES": "消息广告",
            "MESSAGES": "消息广告",
        }.get(meta["objective"], meta["objective"])
        act_id_num = act_id.replace("act_", "")
        page_id = _clean_launch_value(getattr(body, "page_id", None)) or _clean_launch_value(account_defaults.get("page_id")) or default_page_id
        pixel_id = _clean_launch_value(getattr(body, "pixel_id", None)) or _clean_launch_value(account_defaults.get("pixel_id")) or default_pixel_id
        manual_link = _clean_launch_value(getattr(body, "landing_url", None))
        resolved_landing_url = manual_link or _clean_launch_value(account_defaults.get("landing_url")) or default_landing_url
        resolved_form_link = (
            manual_link
            or _clean_launch_value(account_defaults.get("form_link"))
            or _clean_launch_value(account_defaults.get("landing_url"))
            or default_landing_url
        )
        usable_privacy_url = default_lead_privacy_url or resolved_form_link
        if usable_privacy_url and _is_facebook_domain_url(usable_privacy_url):
            usable_privacy_url = ""
        blocked_form_link_host = _get_blocked_lead_form_host(resolved_form_link)
        blocked_privacy_host = _get_blocked_lead_form_host(usable_privacy_url)

        try:
            resp = _req.get(
                f"{FB_BASE}/me",
                params={"access_token": token, "fields": "id,name"},
                timeout=10,
            )
            data = resp.json()
            if "error" in data:
                results.append({
                    "key": "token",
                    "label": "Token 有效性",
                    "status": "fail",
                    "msg": f"Token 无效：{data['error'].get('message', '未知错误')}",
                })
            else:
                results.append({
                    "key": "token",
                    "label": "Token 有效性",
                    "status": "pass",
                    "msg": f"Token 正常（{data.get('name', '未知')})",
                })
        except Exception as exc:
            results.append({
                "key": "token",
                "label": "Token 有效性",
                "status": "fail",
                "msg": f"Token 检测失败：{str(exc)}",
            })

        account_status_map = {
            1: ("pass", "正常"),
            2: ("fail", "已禁用"),
            3: ("fail", "未结清"),
            7: ("warn", "待审核"),
            8: ("fail", "已关闭"),
            9: ("fail", "违规关闭"),
            100: ("warn", "待审核"),
            101: ("warn", "已关闭（可申诉）"),
            201: ("fail", "超出消费限额"),
        }
        try:
            resp = _req.get(
                f"{FB_BASE}/act_{act_id_num}",
                params={"access_token": token, "fields": "account_status,disable_reason,name"},
                timeout=10,
            )
            data = resp.json()
            if "error" in data:
                results.append({
                    "key": "account",
                    "label": "广告账户状态",
                    "status": "warn",
                    "msg": f"无法读取账户状态：{data['error'].get('message', '')}",
                })
            else:
                status_code = data.get("account_status", 0)
                status_info = account_status_map.get(status_code, ("warn", f"未知状态({status_code})"))
                disable_reason = data.get("disable_reason") or ""
                msg = f"账户状态：{status_info[1]}"
                if disable_reason:
                    msg += f"（{disable_reason}）"
                results.append({
                    "key": "account",
                    "label": "广告账户状态",
                    "status": status_info[0],
                    "msg": msg,
                })
        except Exception as exc:
            results.append({
                "key": "account",
                "label": "广告账户状态",
                "status": "warn",
                "msg": f"账户状态检测失败：{str(exc)}",
            })

        page_data = None
        if page_id:
            try:
                resp = _req.get(
                    f"{FB_BASE}/{page_id}",
                    params={"access_token": token, "fields": "id,name,messaging_feature_status,features"},
                    timeout=10,
                )
                page_data = resp.json()
                if "error" in page_data:
                    results.append({
                        "key": "page",
                        "label": "主页权限",
                        "status": "fail",
                        "msg": f"无法访问主页 {page_id}：{page_data['error'].get('message', '')}",
                    })
                    page_data = None
                else:
                    tasks = page_data.get("tasks", [])
                    has_advertise = "ADVERTISE" in tasks or "MANAGE" in tasks or not tasks
                    page_name = page_data.get("name", page_id)
                    results.append({
                        "key": "page",
                        "label": f"主页权限（{page_name}）",
                        "status": "pass" if has_advertise else "warn",
                        "msg": "主页可用于投放" if has_advertise else "主页可能缺少 ADVERTISE 权限，请检查主页角色。",
                    })
            except Exception as exc:
                results.append({
                    "key": "page",
                    "label": "主页权限",
                    "status": "warn",
                    "msg": f"主页检测失败：{str(exc)}",
                })
        else:
            results.append({
                "key": "page",
                "label": "主页 ID",
                "status": "fail",
                "msg": (
                    "当前投放没有可用主页 ID。请在账户详情配置主页，"
                    "或在系统设置中配置全局默认主页（autopilot_fb_page_id）。"
                ),
            })

        page_ctx = None
        results = [item for item in results if item.get("key") != "page"]
        if page_id:
            page_ctx = _fetch_page_access_context(_req, FB_BASE, token, page_id)
            if page_ctx["found"] is True:
                has_advertise = bool(page_ctx.get("can_use"))
                page_name = page_ctx.get("page_name") or page_id
                results.append({
                    "key": "page",
                    "label": f"主页权限（{page_name}）",
                    "status": "pass" if has_advertise else "warn",
                    "msg": (
                        "主页已出现在当前 Token 的 /me/accounts 中，可用于投放。"
                        if has_advertise else
                        "主页已关联到当前 Token，但未看到 ADVERTISE / MANAGE 任务，请在 Facebook 主页角色中复核。"
                    ),
                })
            elif page_ctx["found"] is False:
                results.append({
                    "key": "page",
                    "label": "主页权限",
                    "status": "fail",
                    "msg": page_ctx.get("error") or f"current token cannot manage page {page_id}",
                })
            else:
                results.append({
                    "key": "page",
                    "label": "主页权限",
                    "status": "warn",
                    "msg": f"主页检测失败: {page_ctx.get('error', '')}",
                })
        else:
            results.append({
                "key": "page",
                "label": "主页 ID",
                "status": "fail",
                "msg": (
                    "当前投放没有可用主页 ID。请在账户详情配置主页，"
                    "或在系统设置中配置全局默认主页（autopilot_fb_page_id）。"
                ),
            })

        if meta["is_message"]:
            if not page_id:
                results.append({
                    "key": "messaging",
                    "label": "消息投放前置条件",
                    "status": "fail",
                    "msg": "消息广告必须绑定主页后才能投放。",
                })
            elif page_data:
                messaging_enabled = _parse_page_messaging_enabled(page_data)
                if messaging_enabled is False:
                    results.append({
                        "key": "messaging",
                        "label": "主页消息功能",
                        "status": "fail",
                        "msg": "主页 Messenger/WhatsApp 消息功能未开启，消息广告会直接失败。",
                    })
                elif messaging_enabled is True:
                    results.append({
                        "key": "messaging",
                        "label": "主页消息功能",
                        "status": "pass",
                        "msg": "主页消息功能已开启。",
                    })
                else:
                    results.append({
                        "key": "messaging",
                        "label": "主页消息功能",
                        "status": "warn",
                        "msg": "暂时无法自动确认主页消息开关，请在 Facebook 页面设置中复核。",
                    })

        if meta["is_message"]:
            results = [item for item in results if item.get("key") != "messaging"]
            if not page_id:
                results.append({
                    "key": "messaging",
                    "label": "主页消息能力",
                    "status": "fail",
                    "msg": "消息广告必须先绑定主页后才能投放。",
                })
            elif page_ctx and page_ctx.get("found") is True:
                messaging_probe = _probe_page_messaging_state(_req, FB_BASE, token, page_ctx, page_id)
                messaging_enabled = messaging_probe.get("enabled")
                if messaging_enabled is False:
                    results.append({
                        "key": "messaging",
                        "label": "主页消息能力",
                        "status": "fail",
                        "msg": "主页 Messenger / WhatsApp 消息功能未开启，消息广告会直接失败。",
                    })
                elif messaging_enabled is True:
                    results.append({
                        "key": "messaging",
                        "label": "主页消息能力",
                        "status": "pass",
                        "msg": "主页消息功能已开启。",
                    })
                else:
                    tasks = set(page_ctx.get("tasks") or [])
                    probe_err = messaging_probe.get("error", "")
                    if probe_err and ("pages_messaging" in probe_err.lower() or "page access token is required" in probe_err.lower()):
                        if "MESSAGING" in tasks or "MANAGE" in tasks:
                            probe_err = "已确认当前 Token 能管理该主页，且当前权限链路可用于消息广告；但 Facebook 仍不开放 messaging_feature_status 探测，因此无法自动确认“允许通过消息联系”开关，请在主页设置里手动复核 Messenger / WhatsApp。"
                    results.append({
                        "key": "messaging",
                        "label": "主页消息能力",
                        "status": "warn",
                        "msg": probe_err or "暂时无法自动确认主页消息开关，请在 Facebook 主页设置中复核。",
                    })

        if meta["is_lead"]:
            if page_id:
                results.append({
                    "key": "lead_tos",
                    "label": "Lead 服务条款",
                    "status": "warn",
                    "msg": _build_lead_tos_manual_hint(page_id),
                })
            if _clean_launch_value(getattr(body, "lead_form_id", None)):
                results.append({
                    "key": "lead_form",
                    "label": "Lead 表单",
                    "status": "pass",
                    "msg": "已选择现成 Lead Form 模板/ID。",
                })
            else:
                if blocked_form_link_host:
                    results.append({
                        "key": "lead_form_link",
                        "label": "Lead 表单回跳链接",
                        "status": "fail",
                        "msg": (
                            f"当前表单回跳链接使用了高风险短链域名 {blocked_form_link_host}，"
                            "Facebook 会直接拒绝创建 Lead Form，请改用真实直链。"
                        ),
                    })
                if blocked_privacy_host:
                    results.append({
                        "key": "lead_form_privacy",
                        "label": "Lead 隐私/回跳链接",
                        "status": "fail",
                        "msg": (
                            f"当前 Lead 隐私或回跳链接使用了高风险短链域名 {blocked_privacy_host}，"
                            "Facebook 会直接拒绝创建 Lead Form，请改用真实直链。"
                        ),
                    })
                if not usable_privacy_url:
                    results.append({
                        "key": "lead_form",
                        "label": "Lead 表单自动创建",
                        "status": "fail",
                        "msg": (
                            "未选择现成 Lead Form，且系统没有可用的隐私政策/回跳链接。"
                            "请先选择表单模板，或配置 lead_form_privacy_url，或填写可用表单链接。"
                        ),
                    })
                elif not blocked_form_link_host and not blocked_privacy_host:
                    auto_msg = "未选择现成表单，系统将自动创建 Lead Form。"
                    if _looks_like_http_url(resolved_form_link):
                        auto_msg += f" 提交后将优先回跳到：{resolved_form_link}"
                    else:
                        auto_msg += " 建议补充表单链接，方便用户提交后继续跳转。"
                    results.append({
                        "key": "lead_form",
                        "label": "Lead 表单自动创建",
                        "status": "warn",
                        "msg": auto_msg,
                    })
        elif _clean_launch_value(getattr(body, "lead_form_id", None)):
            results.append({
                "key": "lead_form",
                "label": "Lead 表单",
                "status": "warn",
                "msg": "当前不是即时表单广告，已选 Lead Form 不会生效。",
            })

        if meta["is_lead"]:
            results = [item for item in results if item.get("key") not in {"lead_tos", "lead_access"}]
            selected_lead_form = _clean_launch_value(getattr(body, "lead_form_id", None))
            if page_id:
                results.append({
                    "key": "lead_tos",
                    "label": "Lead 服务条款",
                    "status": "warn",
                    "msg": _build_lead_tos_manual_hint(page_id),
                })
                if not selected_lead_form:
                    lead_probe = _probe_lead_form_permission(_req, FB_BASE, token, page_id, page_ctx)
                    if lead_probe.get("ok") is True:
                        results.append({
                            "key": "lead_access",
                            "label": "Lead 表单权限",
                            "status": "pass",
                            "msg": "当前 Token 可以读取 / 管理该主页的 Lead Form，自动创建表单条件已满足。",
                        })
                    else:
                        blocked = bool(lead_probe.get("permission_blocked"))
                        results.append({
                            "key": "lead_access",
                            "label": "Lead 表单权限",
                            "status": "fail" if blocked else "warn",
                            "msg": (
                                "当前 Token 缺少 pages_manage_ads 等必要权限，系统无法自动创建 Lead Form。请先换用有主页管理权限的 Token，或手动选择已有表单。"
                                if blocked else
                                f"暂时无法确认 Lead 表单权限：{lead_probe.get('error', '')}"
                            ),
                        })
                else:
                    results.append({
                        "key": "lead_access",
                        "label": "Lead 表单权限",
                        "status": "warn",
                        "msg": "已选择现成 Lead Form，本次预检不再强制探测自动创建权限。",
                    })

        should_check_pixel = meta["pixel_required"] or bool(pixel_id) or meta["objective"] in {"OUTCOME_SALES", "OUTCOME_LEADS", "OUTCOME_TRAFFIC"}
        if meta["pixel_required"] and not pixel_id:
            results.append({
                "key": "pixel",
                "label": "Pixel 像素",
                "status": "fail",
                "msg": "当前转化目标必须配置 Pixel，请先选择像素后再启动。",
            })
        elif should_check_pixel and pixel_id:
            try:
                resp = _req.get(
                    f"{FB_BASE}/{pixel_id}",
                    params={"access_token": token, "fields": "id,name,last_fired_time"},
                    timeout=10,
                )
                data = resp.json()
                if "error" in data:
                    results.append({
                        "key": "pixel",
                        "label": "Pixel 像素",
                        "status": "warn" if not meta["pixel_required"] else "fail",
                        "msg": f"Pixel {pixel_id} 无法访问：{data['error'].get('message', '')}",
                    })
                else:
                    last_fired = data.get("last_fired_time")
                    pixel_msg = "Pixel 可用"
                    if last_fired:
                        pixel_msg += f"，最后触发：{last_fired}"
                    else:
                        pixel_msg += "，暂未检测到触发记录"
                    results.append({
                        "key": "pixel",
                        "label": f"Pixel 像素（{data.get('name', pixel_id)}）",
                        "status": "pass",
                        "msg": pixel_msg,
                    })
            except Exception as exc:
                results.append({
                    "key": "pixel",
                    "label": "Pixel 像素",
                    "status": "warn" if not meta["pixel_required"] else "fail",
                    "msg": f"Pixel 检测失败：{str(exc)}",
                })
        elif meta["objective"] == "OUTCOME_SALES":
            results.append({
                "key": "pixel",
                "label": "Pixel 像素",
                "status": "warn",
                "msg": "当前未配置 Pixel。即使不是网站转化目标，也建议配置像素方便后续归因。",
            })

        if meta["landing_required"]:
            if not _looks_like_http_url(resolved_landing_url):
                results.append({
                    "key": "landing_url",
                    "label": "落地页链接",
                    "status": "fail",
                    "msg": (
                        f"{objective_label}广告必须有可用落地页链接。"
                        "请在弹窗填写落地页，或在账户/系统设置中补齐默认链接。"
                    ),
                })
            else:
                results.append({
                    "key": "landing_url",
                    "label": "落地页链接",
                    "status": "pass",
                    "msg": f"已找到可用落地页：{resolved_landing_url}",
                })
        elif meta["is_message"] and manual_link:
            results.append({
                "key": "landing_url",
                "label": "落地页链接",
                "status": "warn",
                "msg": "当前是消息广告，落地页链接不是主要跳转逻辑，系统会优先走消息入口。",
            })
        elif meta["is_lead"] and _looks_like_http_url(resolved_form_link):
            results.append({
                "key": "form_link",
                "label": "表单回跳链接",
                "status": "pass",
                "msg": f"已配置表单回跳/补充链接：{resolved_form_link}",
            })

        cta_type = _clean_launch_value(getattr(body, "cta_type", None)).upper()
        if cta_type and cta_type != "AUTO":
            if meta["is_message"] and cta_type in _CTA_LINK_TYPES:
                results.append({
                    "key": "cta",
                    "label": "CTA 按钮",
                    "status": "fail",
                    "msg": "消息广告不能使用 Shop Now / Learn More 这类链接 CTA，请改为 Auto、Send Message 或 WhatsApp。",
                })
            elif not meta["is_message"] and cta_type in _CTA_MSG_TYPES:
                results.append({
                    "key": "cta",
                    "label": "CTA 按钮",
                    "status": "fail",
                    "msg": "当前不是消息广告，不能使用 MESSAGE_PAGE / WHATSAPP_MESSAGE 这类消息 CTA。",
                })
            elif meta["is_lead"] and cta_type not in {"SIGN_UP"}:
                results.append({
                    "key": "cta",
                    "label": "CTA 按钮",
                    "status": "warn",
                    "msg": "即时表单广告更建议使用 Auto 或 SIGN_UP，避免按钮文案和实际转化路径不一致。",
                })

        if meta["is_message"] and not _clean_launch_value(getattr(body, "message_template", None)):
            results.append({
                "key": "message_template",
                "label": "欢迎消息模板",
                "status": "warn",
                "msg": "未选择欢迎消息模板，系统会自动生成消息内容，建议先确认文案语气是否符合业务。",
            })
        elif not meta["is_message"] and _clean_launch_value(getattr(body, "message_template", None)):
            results.append({
                "key": "message_template",
                "label": "欢迎消息模板",
                "status": "warn",
                "msg": "当前不是消息广告，已选欢迎消息模板不会生效。",
            })

        has_fail = any(item["status"] == "fail" for item in results)
        has_warn = any(item["status"] == "warn" for item in results)
        overall = "fail" if has_fail else ("warn" if has_warn else "pass")
        return {
            "act_id": act_id,
            "account_name": account_defaults.get("name") or act_id,
            "pass": not has_fail,
            "overall": overall,
            "items": results,
        }

    account_reports = [_check_one_account(act_id) for act_id in act_ids]
    if len(account_reports) == 1:
        return account_reports[0]

    fail_count = sum(1 for report in account_reports if report["overall"] == "fail")
    warn_count = sum(1 for report in account_reports if report["overall"] == "warn")
    pass_count = sum(1 for report in account_reports if report["overall"] == "pass")
    overall = "fail" if fail_count else ("warn" if warn_count else "pass")
    summary_msg = f"共检查 {len(account_reports)} 个账户：{pass_count} 个通过"
    if warn_count:
        summary_msg += f"，{warn_count} 个警告"
    if fail_count:
        summary_msg += f"，{fail_count} 个失败"

    return {
        "pass": fail_count == 0,
        "overall": overall,
        "items": [{
            "key": "batch_summary",
            "label": "批量预检汇总",
            "status": overall,
            "msg": summary_msg,
        }],
        "accounts": account_reports,
        "total_accounts": len(account_reports),
    }


def _build_launch_precheck_block_message(precheck_report: dict) -> str:
    if not precheck_report:
        return ""

    lines = []

    def _append_failures(report: dict):
        fail_msgs = [
            str(item.get("msg") or "").strip()
            for item in report.get("items", [])
            if item.get("status") == "fail" and str(item.get("msg") or "").strip()
        ]
        if not fail_msgs:
            return
        label = report.get("account_name") or report.get("act_id") or "当前账户"
        lines.append(f"{label}：{'；'.join(fail_msgs[:3])}")

    if isinstance(precheck_report.get("accounts"), list):
        for account_report in precheck_report.get("accounts", []):
            _append_failures(account_report)
    else:
        _append_failures(precheck_report)

    if not lines:
        return ""
    return "投放前置检查未通过：\n" + "\n".join(lines[:6])


class PreCheckBody(BaseModel):
    act_id: Optional[str] = None
    act_ids: Optional[list[str]] = None
    objective: Optional[str] = "OUTCOME_SALES"
    conversion_goal: Optional[str] = None
    target_countries: Optional[list[str]] = None
    page_id: Optional[str] = None
    pixel_id: Optional[str] = None
    landing_url: Optional[str] = None
    tw_advertiser_id: Optional[int] = None
    tw_page_id: Optional[str] = None
    message_template: Optional[str] = None
    lead_form_id: Optional[str] = None
    cta_type: Optional[str] = None

@router.post("/precheck-launch")
def precheck_launch(body: PreCheckBody, user=Depends(get_current_user)):
    """
    铺广告预检：在正式铺广告前检测账户/主页/Pixel/TOS状态
    返回每个检测项的 pass/warn/fail 状态和修复建议
    """
    return _run_launch_precheck(body)

    import requests as _req
    FB_BASE = "https://graph.facebook.com/v21.0"
    results = []

    # ── 获取 Token ──────────────────────────────────────────────────────────
    try:
        from services.token_manager import get_exec_token, ACTION_READ
        token = get_exec_token(body.act_id, ACTION_READ)
    except Exception:
        token = None

    if not token:
        return {
            "pass": False,
            "items": [{"key": "token", "label": "操作Token", "status": "fail",
                       "msg": "未找到可用的操作Token，请先配置Token"}]
        }

    act_id_num = body.act_id.replace("act_", "")
    is_lead = (body.conversion_goal or "").lower() == "lead_generation"
    page_id = body.page_id

    # 如果没传 page_id，从数据库读取
    if not page_id:
        try:
            conn2 = get_conn()
            acc_row = conn2.execute("SELECT page_id FROM accounts WHERE act_id=?", (body.act_id,)).fetchone()
            conn2.close()
            if acc_row:
                page_id = acc_row["page_id"] or acc_row[0]
        except Exception:
            pass

    # ── 检测1：Token有效性 ────────────────────────────────────────────────────
    try:
        r = _req.get(f"{FB_BASE}/me", params={"access_token": token, "fields": "id,name"}, timeout=10)
        d = r.json()
        if "error" in d:
            results.append({"key": "token", "label": "Token有效性", "status": "fail",
                            "msg": f"Token无效：{d['error'].get('message','未知错误')}"})
        else:
            results.append({"key": "token", "label": "Token有效性", "status": "pass",
                            "msg": f"Token正常（{d.get('name','未知')}）"})
    except Exception as e:
        results.append({"key": "token", "label": "Token有效性", "status": "fail",
                        "msg": f"Token检测失败：{str(e)}"})

    # ── 检测2：广告账户状态 ───────────────────────────────────────────────────
    ACCOUNT_STATUS_MAP = {
        1: ("pass", "正常"),
        2: ("fail", "已禁用"),
        3: ("fail", "未结清"),
        7: ("warn", "待审核"),
        8: ("fail", "已关闭"),
        9: ("fail", "违规关闭"),
        100: ("warn", "待审核"),
        101: ("warn", "已关闭（可申诉）"),
        201: ("fail", "超出消费限额"),
    }
    try:
        r = _req.get(f"{FB_BASE}/act_{act_id_num}",
                     params={"access_token": token, "fields": "account_status,disable_reason,name"},
                     timeout=10)
        d = r.json()
        if "error" in d:
            results.append({"key": "account", "label": "广告账户状态", "status": "warn",
                            "msg": f"无法获取账户状态：{d['error'].get('message','')}"})
        else:
            st = d.get("account_status", 0)
            status_info = ACCOUNT_STATUS_MAP.get(st, ("warn", f"未知状态({st})"))
            results.append({"key": "account", "label": "广告账户状态",
                            "status": status_info[0],
                            "msg": f"账户状态：{status_info[1]}"
                                   + (f"（{d.get('disable_reason','')}）" if d.get("disable_reason") else "")})
    except Exception as e:
        results.append({"key": "account", "label": "广告账户状态", "status": "warn",
                        "msg": f"检测失败：{str(e)}"})

    # ── 检测3：主页权限（仅Lead/消息/主页赞广告需要） ──────────────────────────
    if page_id:
        try:
            r = _req.get(f"{FB_BASE}/{page_id}",
                         params={"access_token": token, "fields": "id,name,tasks"},
                         timeout=10)
            d = r.json()
            if "error" in d:
                results.append({"key": "page", "label": "主页权限", "status": "fail",
                                "msg": f"无法访问主页 {page_id}：{d['error'].get('message','')}"})
            else:
                tasks = d.get("tasks", [])
                has_advertise = "ADVERTISE" in tasks or "MANAGE" in tasks or not tasks
                results.append({"key": "page", "label": f"主页权限（{d.get('name', page_id)}）",
                                "status": "pass" if has_advertise else "warn",
                                "msg": "有广告投放权限" if has_advertise else "可能缺少 ADVERTISE 权限，请检查主页角色"})
        except Exception as e:
            results.append({"key": "page", "label": "主页权限", "status": "warn",
                            "msg": f"检测失败：{str(e)}"})
    else:
        if is_lead or body.objective in ("OUTCOME_LEADS", "OUTCOME_MESSAGES", "OUTCOME_ENGAGEMENT"):
            results.append({"key": "page", "label": "主页权限", "status": "fail",
                            "msg": "此广告目标需要主页 ID，请在账户详情中配置主页"})
        else:
            results.append({"key": "page", "label": "主页权限", "status": "warn",
                            "msg": "未配置主页 ID（建议配置以提升广告效果）"})

    # ── 检测4：Lead Generation TOS（仅Lead广告） ─────────────────────────────
    if is_lead and page_id:
        try:
            r = _req.get(f"{FB_BASE}/{page_id}/leadgen_tos",
                         params={"access_token": token},
                         timeout=10)
            d = r.json()
            if "error" in d:
                err_msg = d["error"].get("message", "")
                err_code = d["error"].get("code", 0)
                if err_code == 100 or "permission" in err_msg.lower():
                    results.append({"key": "lead_tos", "label": "Lead广告服务条款",
                                    "status": "fail",
                                    "msg": "⚠️ 主页尚未接受 Facebook Lead Generation 服务条款！"
                                           "请访问 https://www.facebook.com/ads/leadgen/tos 接受条款后再铺广告。"})
                else:
                    results.append({"key": "lead_tos", "label": "Lead广告服务条款",
                                    "status": "warn",
                                    "msg": f"无法检测TOS状态：{err_msg}"})
            else:
                data_list = d.get("data", [])
                if data_list and data_list[0].get("accepted"):
                    results.append({"key": "lead_tos", "label": "Lead广告服务条款",
                                    "status": "pass",
                                    "msg": "✅ 主页已接受 Lead Generation 服务条款"})
                elif data_list:
                    results.append({"key": "lead_tos", "label": "Lead广告服务条款",
                                    "status": "fail",
                                    "msg": "⚠️ 主页尚未接受 Facebook Lead Generation 服务条款！"
                                           "请访问 https://www.facebook.com/ads/leadgen/tos 接受条款后再铺广告。"})
                else:
                    results.append({"key": "lead_tos", "label": "Lead广告服务条款",
                                    "status": "fail",
                                    "msg": "⚠️ 主页尚未接受 Facebook Lead Generation 服务条款！"
                                           "请访问 https://www.facebook.com/ads/leadgen/tos 接受条款后再铺广告。"})
        except Exception as e:
            results.append({"key": "lead_tos", "label": "Lead广告服务条款", "status": "warn",
                            "msg": f"TOS检测失败：{str(e)}"})
    elif is_lead and not page_id:
        results.append({"key": "lead_tos", "label": "Lead广告服务条款", "status": "fail",
                        "msg": "Lead广告必须配置主页ID才能检测TOS状态"})

    # ── 检测5：Pixel有效性（转化广告建议配置） ────────────────────────────────
    pixel_id = body.pixel_id
    if not pixel_id:
        try:
            conn3 = get_conn()
            px_row = conn3.execute("SELECT pixel_id FROM accounts WHERE act_id=?", (body.act_id,)).fetchone()
            conn3.close()
            if px_row:
                pixel_id = px_row["pixel_id"] or px_row[0]
        except Exception:
            pass

    if pixel_id and body.objective in ("OUTCOME_SALES", "OUTCOME_LEADS", "OUTCOME_TRAFFIC"):
        try:
            r = _req.get(f"{FB_BASE}/{pixel_id}",
                         params={"access_token": token, "fields": "id,name,last_fired_time"},
                         timeout=10)
            d = r.json()
            if "error" in d:
                results.append({"key": "pixel", "label": "Pixel像素", "status": "warn",
                                "msg": f"Pixel {pixel_id} 无法访问：{d['error'].get('message','')}"})
            else:
                last_fired = d.get("last_fired_time")
                results.append({"key": "pixel", "label": f"Pixel像素（{d.get('name', pixel_id)}）",
                                "status": "pass",
                                "msg": f"Pixel正常" + (f"，最后触发：{last_fired}" if last_fired else "，尚未触发")})
        except Exception as e:
            results.append({"key": "pixel", "label": "Pixel像素", "status": "warn",
                            "msg": f"Pixel检测失败：{str(e)}"})
    elif not pixel_id and body.objective in ("OUTCOME_SALES",):
        results.append({"key": "pixel", "label": "Pixel像素", "status": "warn",
                        "msg": "转化广告建议配置Pixel以追踪转化，当前未配置"})

    # ── 检测6：落地页链接（流量/转化广告必填） ────────────────────────────────
    _LANDING_REQUIRED_OBJECTIVES = ("OUTCOME_TRAFFIC", "OUTCOME_SALES", "OUTCOME_ENGAGEMENT")
    _landing_url_val = getattr(body, 'landing_url', None) or ""
    if body.objective in _LANDING_REQUIRED_OBJECTIVES:
        _has_landing = bool(_landing_url_val.strip())
        if not _has_landing:
            try:
                _conn_lu = get_conn()
                _lu_row = _conn_lu.execute(
                    "SELECT landing_url FROM accounts WHERE act_id=?", (body.act_id,)
                ).fetchone()
                _conn_lu.close()
                if _lu_row and (_lu_row[0] if isinstance(_lu_row, tuple) else _lu_row.get("landing_url", "")):
                    _has_landing = True
            except Exception:
                pass
        if not _has_landing:
            try:
                _conn_gs = get_conn()
                _gs_row = _conn_gs.execute(
                    "SELECT value FROM settings WHERE key='default_landing_url'"
                ).fetchone()
                _conn_gs.close()
                if _gs_row and (_gs_row[0] if isinstance(_gs_row, tuple) else _gs_row.get("value", "")):
                    _has_landing = True
            except Exception:
                pass
        _obj_label = {"OUTCOME_TRAFFIC": "流量点击", "OUTCOME_SALES": "转化购买", "OUTCOME_ENGAGEMENT": "帖子互动"}.get(body.objective, body.objective)
        if not _has_landing:
            results.append({
                "key": "landing_url",
                "label": "落地页链接",
                "status": "fail",
                "msg": (
                    f"❌ {_obj_label}广告必须填写落地页链接！"
                    "请在铺广告弹窗中填写「落地页链接」，"
                    "或在「投放链接管理」中为该账户配置默认落地页，"
                    "或在系统设置中配置全局默认落地页（default_landing_url）。"
                )
            })
        else:
            results.append({
                "key": "landing_url",
                "label": "落地页链接",
                "status": "pass",
                "msg": "✅ 落地页链接已配置"
            })
    # ── 汇总结果 ──────────────────────────────────────────────────────────────
    has_fail = any(r["status"] == "fail" for r in results)
    has_warn = any(r["status"] == "warn" for r in results)
    overall = "fail" if has_fail else ("warn" if has_warn else "pass")

    return {
        "pass": not has_fail,
        "overall": overall,
        "items": results
    }

@router.post("/{asset_id}/launch")
def launch_auto_campaign(asset_id: int, body: LaunchCampaignBody, user=Depends(get_current_user)):
    conn = get_conn()
    row = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    if not row["ai_headlines"] or not row["ai_bodies"]:
        conn.close()
        raise HTTPException(400, "请先完成 AI 分析,生成文案后再启动铺广告")
    if _get_setting("autopilot_enabled", "0") != "1":
        conn.close()
        raise HTTPException(400, "全自动铺广告功能未启用,请在系统设置中开启")

    now = datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    precheck_report = _run_launch_precheck(body)
    precheck_block_msg = _build_launch_precheck_block_message(precheck_report)
    if precheck_block_msg:
        conn.close()
        raise HTTPException(400, precheck_block_msg)

    row = dict(row)
    display_name = row.get("display_name") or row.get("file_name") or f"asset_{asset_id}"
    # 命名格式:{target}-{country}-{asset_code}-{MMDD}
    # 实际系列名由 autopilot_engine 在创建 Campaign 时覆盖,这里存的是占位名
    _obj_abbr_api = {
        "OUTCOME_SALES": "CONV", "OUTCOME_LEADS": "LEAD",
        "OUTCOME_TRAFFIC": "TRAF", "OUTCOME_AWARENESS": "AWR",
        "OUTCOME_ENGAGEMENT": "ENG", "OUTCOME_MESSAGES": "MSG",
        "OUTCOME_APP_PROMOTION": "APP",
    }
    _obj_s = _obj_abbr_api.get(body.objective, "ADS")
    _ctry_s = "-".join((body.target_countries or ["XX"])[:2])
    _ast_c = row.get("asset_code") or f"AST-{asset_id:04d}"
    try:
        from datetime import datetime as _dt2
        import pytz as _pytz2
        _now_cst = _dt2.now(_pytz2.timezone("Asia/Shanghai"))
    except Exception:
        from datetime import datetime as _dt2
        _now_cst = _dt2.now()
    _mmdd = _now_cst.strftime("%m%d")
    campaign_name = f"{_obj_s}-{_ctry_s}-{_ast_c}-{_mmdd}"

    cur = conn.execute(
        """INSERT INTO auto_campaigns
           (act_id, asset_id, name, objective, target_countries,
            target_cpa, daily_budget,
            age_min, age_max, gender, placements, bid_strategy, max_adsets,
            page_id_override, pixel_id_override, landing_url,
            device_platforms, ad_language, conversion_event,
            tw_page_id, conversion_goal, message_template, lead_form_id,
            cta_type, status, created_at, updated_at)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?)""",
        (body.act_id, asset_id, campaign_name, body.objective,
         json.dumps(body.target_countries), body.target_cpa, body.daily_budget,
         body.age_min, body.age_max, body.gender,
         json.dumps(body.placements) if body.placements else None,
         body.bid_strategy, body.max_adsets,
         body.page_id, body.pixel_id, body.landing_url,
         getattr(body, 'device_platforms', 'all'),
         getattr(body, 'ad_language', 'en'),
         getattr(body, 'conversion_event', None) or 'PURCHASE',
         getattr(body, 'tw_page_id', None),
         getattr(body, 'conversion_goal', None),
         getattr(body, 'message_template', None),
         getattr(body, 'lead_form_id', None),
         getattr(body, 'cta_type', None) or '',
         now, now)
    )
    # 如果铺广告时指定了台湾认证广告主，临时更新账户配置
    if getattr(body, 'tw_advertiser_id', None):
        conn.execute(
            "UPDATE accounts SET tw_advertiser_id=? WHERE act_id=?",
            (body.tw_advertiser_id, body.act_id)
        )
    campaign_id = cur.lastrowid
    conn.commit(); conn.close()

    threading.Thread(target=_trigger_autopilot, args=(campaign_id,), daemon=True).start()
    return {
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "status": "pending",
        "message": "自动铺广告任务已创建,正在后台执行"
    }


@router.post("/{asset_id}/batch-launch")
def batch_launch_auto_campaign(asset_id: int, body: LaunchCampaignBody, user=Depends(get_current_user)):
    """批量多账户铺广告:为多个广告账户同时创建铺广告任务"""
    conn = get_conn()
    row = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    if not row["ai_headlines"] or not row["ai_bodies"]:
        conn.close()
        raise HTTPException(400, "请先完成 AI 分析,生成文案后再启动铺广告")
    if _get_setting("autopilot_enabled", "0") != "1":
        conn.close()
        raise HTTPException(400, "全自动铺广告功能未启用,请在系统设置中开启")

    _normalize_launch_body_fields(body)
    # 获取账户列表:优先用 act_ids,否则用 act_id
    precheck_report = _run_launch_precheck(body)
    precheck_block_msg = _build_launch_precheck_block_message(precheck_report)
    if precheck_block_msg:
        conn.close()
        raise HTTPException(400, precheck_block_msg)

    act_ids = body.act_ids if body.act_ids else [body.act_id]
    act_ids = [a.strip() for a in act_ids if a and a.strip()]
    if not act_ids:
        conn.close()
        raise HTTPException(400, "请至少指定一个广告账户")

    now = datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    # 批量投放系列名也采用规范格式
    _obj_abbr_b = {
        "OUTCOME_SALES": "CONV", "OUTCOME_LEADS": "LEAD",
        "OUTCOME_TRAFFIC": "TRAF", "OUTCOME_AWARENESS": "AWR",
        "OUTCOME_ENGAGEMENT": "ENG", "OUTCOME_MESSAGES": "MSG",
        "OUTCOME_APP_PROMOTION": "APP",
    }
    _obj_sb = _obj_abbr_b.get(body.objective, "ADS")
    _ctry_sb = "-".join((body.target_countries or ["XX"])[:2])
    _ast_cb = dict(row).get("asset_code") if row else None
    _ast_cb = _ast_cb or f"AST-{asset_id:04d}"
    try:
        from datetime import datetime as _dt3
        import pytz as _pytz3
        _now_cst_b = _dt3.now(_pytz3.timezone("Asia/Shanghai"))
    except Exception:
        from datetime import datetime as _dt3
        _now_cst_b = _dt3.now()
    _mmdd = _now_cst_b.strftime("%m%d")
    results = []

    for act_id in act_ids:
        try:
            campaign_name = f"{_obj_sb}-{_ctry_sb}-{_ast_cb}-{_mmdd}"
            cur = conn.execute(
                """INSERT INTO auto_campaigns
                   (act_id, asset_id, name, objective, target_countries,
                    target_cpa, daily_budget,
                    age_min, age_max, gender, placements, bid_strategy, max_adsets,
                    page_id_override, pixel_id_override, landing_url,
                    device_platforms, ad_language, conversion_event,
                    tw_page_id, conversion_goal, message_template, lead_form_id,
                    cta_type, status, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?)""",
                (act_id, asset_id, campaign_name, body.objective,
                 json.dumps(body.target_countries), body.target_cpa, body.daily_budget,
                 body.age_min, body.age_max, body.gender,
                 json.dumps(body.placements) if body.placements else None,
                 body.bid_strategy, body.max_adsets,
                 body.page_id, body.pixel_id, body.landing_url,
                 getattr(body, 'device_platforms', 'all'),
                 getattr(body, 'ad_language', 'en'),
                 getattr(body, 'conversion_event', None) or 'PURCHASE',
                 getattr(body, 'tw_page_id', None),
                 getattr(body, 'conversion_goal', None),
                 getattr(body, 'message_template', None),
                 getattr(body, 'lead_form_id', None),
                 getattr(body, 'cta_type', None) or '',
                 now, now)
            )
            campaign_id = cur.lastrowid
            conn.commit()
            threading.Thread(target=_trigger_autopilot, args=(campaign_id,), daemon=True).start()
            results.append({
                "act_id": act_id,
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "status": "pending",
                "message": "任务已创建"
            })
        except Exception as e:
            results.append({
                "act_id": act_id,
                "campaign_id": None,
                "status": "error",
                "message": str(e)
            })

    conn.close()
    success_count = sum(1 for r in results if r["status"] == "pending")
    return {
        "total": len(act_ids),
        "success": success_count,
        "failed": len(act_ids) - success_count,
        "results": results,
        "message": f"已为 {success_count}/{len(act_ids)} 个账户创建铺广告任务"
    }

def _trigger_autopilot(campaign_id: int):
    import traceback as _tb
    try:
        from services.autopilot_engine import AutoPilotEngine
        AutoPilotEngine().run_campaign(campaign_id)
    except Exception as e:
        _full_err = f"{type(e).__name__}: {e}\n{_tb.format_exc()[-800:]}"
        logger.error(f"[AutoPilot] 任务 {campaign_id} 崩溃: {_full_err}")
        try:
            conn = get_conn()
            conn.execute(
                "UPDATE auto_campaigns SET status='error', error_msg=?, progress_msg=?, updated_at=? WHERE id=?",
                (_full_err[:1000], _full_err[:500], 
                 datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"),
                 campaign_id)
            )
            conn.commit(); conn.close()
        except Exception as _db_err:
            logger.error(f"[AutoPilot] 写入错误状态失败: {_db_err}")


@router.get("/{asset_id}/campaigns")
def list_asset_campaigns(asset_id: int, user=Depends(get_current_user)):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM auto_campaigns WHERE asset_id=? ORDER BY created_at DESC",
        (asset_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
@router.get("/{asset_id}/campaigns/{campaign_id}/status")
def get_campaign_status(asset_id: int, campaign_id: int, user=Depends(get_current_user)):
    """获取铺广告任务实时进度(供前端轮询)"""
    conn = get_conn()
    row = conn.execute(
        """SELECT c.id, c.status, c.progress_step, c.progress_msg,
                  c.fb_campaign_id, c.total_adsets, c.total_ads,
                  c.error_msg, c.updated_at,
                  (SELECT COUNT(*) FROM auto_campaign_ads WHERE campaign_id=c.id) as ad_count,
                  (SELECT COUNT(*) FROM auto_campaign_ads WHERE campaign_id=c.id AND status='done') as ad_done,
                  (SELECT COUNT(*) FROM auto_campaign_ads WHERE campaign_id=c.id AND status='error') as ad_error
           FROM auto_campaigns c
           WHERE c.id=? AND c.asset_id=?""",
        (campaign_id, asset_id)
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "任务不存在")
    r = dict(row)
    # 计算进度百分比
    step = r.get("progress_step") or ""
    step_map = {
        "init": 5, "token": 10, "asset": 20, "upload": 35,
        "campaign": 50, "adset_1": 60, "adset_2": 70, "adset_3": 80,
        "adset_4": 85, "adset_5": 88, "done": 100
    }
    if r["status"] == "error":
        r["progress_pct"] = 0
        # 确保前端能看到错误信息:如果 progress_msg 为空,将 error_msg 回填进去
        if not r.get("progress_msg") and r.get("error_msg"):
            r["progress_msg"] = r["error_msg"]
    elif r["status"] == "done":
        r["progress_pct"] = 100
    else:
        base_pct = step_map.get(step, 5)
        ad_count = int(r.get("ad_count") or 0)
        ad_done = int(r.get("ad_done") or 0)
        ad_error = int(r.get("ad_error") or 0)
        finished_ads = min(ad_count, ad_done + ad_error)
        if ad_count > 0:
            ad_pct = 88 + int((finished_ads / max(ad_count, 1)) * 10)
            r["progress_pct"] = max(base_pct, min(98, ad_pct))
        else:
            r["progress_pct"] = base_pct
    return r




# ── 素材按账户明细接口 ──────────────────────────────────────────────────────────
@router.get("/{asset_id}/breakdown")
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

    # ── 同时获取 auto_campaign_ads 中的广告总数(含未成功同步的)────────────
    ad_count_rows = conn.execute(
        """
        SELECT aca.act_id,
               COUNT(aca.id) as total_ads,
               SUM(CASE WHEN aca.status='done' THEN 1 ELSE 0 END) as ad_done,
               SUM(CASE WHEN aca.status='error' THEN 1 ELSE 0 END) as ad_error,
               ac.daily_budget, ac.target_cpa
        FROM auto_campaign_ads aca
        JOIN auto_campaigns ac ON ac.id = aca.campaign_id
        WHERE aca.asset_id = ?
        GROUP BY aca.act_id
        """,
        (asset_id,)
    ).fetchall()
    conn.close()

    ad_count_map = {r["act_id"]: dict(r) for r in ad_count_rows}

    # ── 如果 asset_spend_log 没有数据(首次巡检前),降级到实时拉取 ──────────
    if not log_rows:
        # 降级:实时从 FB API 拉取(旧逻辑兜底)
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT aca.act_id, acc.name as act_name,
                   COUNT(aca.id) as ad_count,
                   SUM(CASE WHEN aca.status='done' THEN 1 ELSE 0 END) as ad_done,
                   SUM(CASE WHEN aca.status='error' THEN 1 ELSE 0 END) as ad_error,
                   GROUP_CONCAT(aca.fb_ad_id, ',') as fb_ad_ids,
                   ac.target_countries, ac.objective, ac.daily_budget, ac.target_cpa
            FROM auto_campaign_ads aca
            JOIN auto_campaigns ac ON ac.id = aca.campaign_id
            LEFT JOIN accounts acc ON acc.act_id = aca.act_id
            WHERE aca.asset_id = ?
            GROUP BY aca.act_id ORDER BY ad_count DESC
            """,
            (asset_id,)
        ).fetchall()
        conn.close()

        if not rows:
            return {"breakdown": [], "summary": {}, "data_source": "none"}

        breakdown = []
        total_spend = 0.0
        total_conv = 0
        total_ads = 0

        for row in rows:
            item = dict(row)
            act_id = item["act_id"]
            fb_ad_ids = [x for x in (item.get("fb_ad_ids") or "").split(",") if x.strip()]

            try:
                from core.database import get_conn as _gc
                c2 = _gc()
                kpi_row = c2.execute(
                    "SELECT kpi_field FROM kpi_configs WHERE act_id=? ORDER BY id DESC LIMIT 1",
                    (act_id,)
                ).fetchone()
                c2.close()
                kpi_field = kpi_row["kpi_field"] if kpi_row else None
            except Exception:
                kpi_field = None

            try:
                from services.token_manager import get_exec_token, ACTION_READ
                token = get_exec_token(act_id, ACTION_READ)
            except Exception:
                token = None

            act_spend = 0.0
            act_conv = 0
            matched_field = ""

            if token and fb_ad_ids:
                from services.asset_scorer import _fb_get_ad_insights, _parse_conversions
                for fb_ad_id in fb_ad_ids[:50]:
                    insights = _fb_get_ad_insights(fb_ad_id, token)
                    if not insights:
                        continue
                    spend, conv, conv_value, used_field = _parse_conversions(insights, kpi_field)
                    act_spend += spend
                    act_conv += conv
                    if used_field and not matched_field:
                        matched_field = used_field

            total_spend += act_spend
            total_conv += act_conv
            total_ads += item["ad_count"]

            try:
                countries = _json.loads(item["target_countries"]) if item["target_countries"] else []
            except Exception:
                countries = str(item["target_countries"]).split(",") if item["target_countries"] else []

            breakdown.append({
                "act_id": act_id,
                "act_name": item["act_name"] or act_id,
                "ad_count": item["ad_count"],
                "ad_done": item["ad_done"],
                "ad_error": item["ad_error"],
                "ad_active": item["ad_done"],
                "ad_inactive": 0,
                "countries": countries,
                "objective": item["objective"],
                "daily_budget": item["daily_budget"],
                "target_cpa": item["target_cpa"],
                "kpi_field": kpi_field or "",
                "matched_field": matched_field,
                "spend": round(act_spend, 2),
                "conv": act_conv,
                "cpa": round(act_spend / act_conv, 2) if act_conv > 0 else None,
                "last_synced_at": None,
                "is_cached": False
            })

        summary = {
            "total_spend": round(total_spend, 2),
            "total_conv": total_conv,
            "total_ads": total_ads,
            "avg_cpa": round(total_spend / total_conv, 2) if total_conv > 0 else None
        }
        return {"breakdown": breakdown, "summary": summary, "data_source": "realtime"}

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

        # 合并 auto_campaign_ads 中的广告计数信息
        ad_info = ad_count_map.get(act_id, {})
        ad_count = item["ad_count"]  # spend_log 中的广告数
        ad_done = ad_info.get("ad_done", item["ad_active"])
        ad_error = ad_info.get("ad_error", 0)
        total_ad_count = ad_info.get("total_ads", ad_count)

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
            "ad_done": ad_done,
            "ad_error": ad_error,
            "countries": countries,
            "objective": item["objective"],
            "daily_budget": ad_info.get("daily_budget"),
            "target_cpa": ad_info.get("target_cpa"),
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
    rows = conn.execute(
        """SELECT act_id, name, currency, status, balance, timezone
           FROM accounts
           WHERE (name LIKE ? OR act_id LIKE ?)
             AND status NOT IN ('DISABLED', 'CLOSED')
           ORDER BY name ASC
           LIMIT ?""",
        (keyword, keyword, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
