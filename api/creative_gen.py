"""
creative_gen.py  —  素材自动化生成 API  v3.0
路由前缀: /api/creative-gen

功能:
  GET  /providers          — 获取支持的厂商列表
  GET  /recipes            — 获取场景配方列表（傻瓜向导用）
  POST /parse-brief        — 一句话解析广告需求（AI 解析）
  POST /preview-prompt     — 预览 Prompt（不生成）
  POST /generate           — 发起生成任务（文生图 / 爆款裂变）
  GET  /task/{task_id}     — 查询任务状态
  GET  /pending            — 获取待审核素材列表
  GET  /pending/{id}/image — 获取待审核素材图片
  POST /pending/{id}/approve — 审核通过，纳入素材库
  POST /pending/{id}/reject  — 拒绝
  DELETE /pending/batch-reject — 批量拒绝
  GET  /settings-keys      — 获取当前已配置的图像生成 API Key 状态

v3.0 新增:
  - 12 个场景配方库（RECIPES），每个配方含完整 Prompt 模板 + 最优厂商 + 参数预设
  - 厂商自动路由（auto_route_provider）：根据场景自动选最优厂商
  - 裂变参数优化：根据变体方向动态调整 guidance_scale
  - AI Prompt 增强引擎：读取素材 AI 分析结果注入 Prompt
  - 30+ 国家本地化人物外貌映射
  - 新增 recipe_id 参数，用户只需选配方无需写 Prompt
"""

import os
import base64
import hashlib
import logging
import time
import uuid
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import requests

from core.auth import get_current_user
from core.database import get_conn

logger = logging.getLogger("mira.creative_gen")

router = APIRouter()

ASSET_DIR = os.environ.get("MIRA_ASSET_DIR", "/opt/mira/assets")
PENDING_DIR = os.path.join(ASSET_DIR, "pending_review")
REJECTED_DIR = os.path.join(ASSET_DIR, "rejected_review")
os.makedirs(PENDING_DIR, exist_ok=True)
os.makedirs(REJECTED_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# 30+ 国家本地化人物外貌映射
# ─────────────────────────────────────────────────────────────────────────────

COUNTRY_APPEARANCE = {
    # 东亚
    "TW": "East Asian appearance, Taiwanese style, modern Taipei urban setting, trendy contemporary fashion, clean bright environment",
    "HK": "East Asian appearance, Hong Kong cosmopolitan style, modern city background, sophisticated urban fashion",
    "CN": "East Asian appearance, modern Chinese urban style, contemporary clean setting, fashionable attire",
    "JP": "East Asian appearance, Japanese aesthetic, clean minimalist style, Harajuku or business fashion",
    "KR": "East Asian appearance, Korean style, trendy K-fashion, modern Seoul setting, flawless skin",
    # 东南亚
    "SG": "Southeast Asian or multicultural appearance, modern Singapore city setting, professional and polished look",
    "MY": "Southeast Asian appearance, modern Malaysian urban setting, diverse multicultural look",
    "TH": "Southeast Asian appearance, Thai style, warm vibrant setting, friendly and approachable expression",
    "VN": "Southeast Asian appearance, Vietnamese style, modern Ho Chi Minh City urban setting, youthful look",
    "ID": "Southeast Asian appearance, Indonesian style, modern Jakarta city setting, diverse and energetic",
    "PH": "Southeast Asian appearance, Filipino style, bright cheerful setting, warm friendly smile",
    "MM": "Southeast Asian appearance, Myanmar style, warm natural setting",
    "KH": "Southeast Asian appearance, Cambodian style, warm tropical setting",
    # 南亚
    "IN": "South Asian appearance, Indian style, modern urban setting, vibrant colors, professional or lifestyle look",
    "PK": "South Asian appearance, Pakistani style, modern urban setting",
    "BD": "South Asian appearance, Bangladeshi style, modern setting",
    "LK": "South Asian appearance, Sri Lankan style, tropical setting",
    # 中东/北非
    "AE": "Middle Eastern appearance, UAE cosmopolitan style, luxury modern setting, Dubai aesthetic",
    "SA": "Middle Eastern appearance, Saudi Arabian style, modern urban setting, professional attire",
    "EG": "North African/Middle Eastern appearance, Egyptian style, modern Cairo setting",
    "TR": "Eurasian appearance, Turkish style, modern Istanbul setting, Mediterranean aesthetic",
    "IL": "Mediterranean appearance, Israeli style, modern Tel Aviv setting",
    # 欧美
    "US": "diverse American appearance, modern urban or suburban setting, casual or professional style",
    "CA": "diverse Canadian appearance, clean modern setting, friendly approachable look",
    "UK": "British appearance, urban contemporary London setting, smart casual style",
    "AU": "Caucasian or diverse Australian appearance, bright outdoor or modern setting, relaxed lifestyle",
    "NZ": "Caucasian or diverse New Zealand appearance, clean natural setting",
    "DE": "European appearance, German style, clean modern setting, professional look",
    "FR": "European appearance, French chic style, Paris aesthetic, elegant fashion",
    "ES": "European appearance, Spanish style, warm Mediterranean setting",
    "IT": "European appearance, Italian style, elegant fashion, warm Mediterranean setting",
    "NL": "European appearance, Dutch style, clean modern setting",
    # 拉美
    "BR": "Latin American appearance, Brazilian style, vibrant colorful setting, energetic and warm",
    "MX": "Latin American appearance, Mexican style, warm vibrant setting, friendly expression",
    "AR": "Latin American appearance, Argentine style, modern Buenos Aires setting",
    "CO": "Latin American appearance, Colombian style, warm vibrant setting",
    # 非洲
    "NG": "West African appearance, Nigerian style, modern Lagos setting, vibrant and energetic",
    "ZA": "South African appearance, diverse modern setting, Cape Town or Johannesburg aesthetic",
    "KE": "East African appearance, Kenyan style, modern Nairobi setting",
    # 其他
    "RU": "Eastern European/Russian appearance, modern urban setting, sophisticated style",
}


# ─────────────────────────────────────────────────────────────────────────────
# 12 个场景配方库
# ─────────────────────────────────────────────────────────────────────────────

RECIPES = [
    {
        "id": "person_phone_surprise",
        "intent_desc": "真实感极强的真人看手机惊喜广告，画面中一个普通人（非明星）手持手机，表情呈现出震惊、惊喜或难以置信的反应，手机屏幕内容模糊或不可见，背景是日常生活场景（家里、咖啡厅等），整体传达出发现了某个改变生活的机会或消息的感觉。",
        "name": "真人·看手机惊喜",
        "emoji": "😲",
        "category": "真人",
        "desc": "真人手持手机，表情惊喜/兴奋，适合金融、健康、消息类广告",
        "best_for": ["messaging", "lead", "traffic"],
        "default_provider": "stability_ultra",
        "fallback_provider": "fal_schnell",
        "extra_params": {
            "negative_prompt": "text, watermark, logo, UI, phone screen content, blurry, deformed face",
            "style_preset": "photographic",
        },
        "prompt_template": (
            "A photorealistic close-up portrait of a {appearance} person holding a smartphone, "
            "looking at the screen with a genuine expression of surprise and excitement, "
            "mouth slightly open, eyes wide, eyebrows raised. "
            "The phone screen is completely blurred and shows no readable content. "
            "Soft natural indoor lighting, shallow depth of field, bokeh background. "
            "Professional advertising photography, Canon 5D quality, sharp focus on face. "
            "The person looks like they just discovered something amazing and life-changing."
        ),
    },
    {
        "id": "person_professional_desk",
        "name": "真人·专业顾问坐姿",
        "emoji": "💼",
        "category": "真人",
        "desc": "专业人士坐姿，适合金融、教育、B2B广告",
        "best_for": ["lead", "messaging", "traffic"],
        "default_provider": "stability_ultra",
        "fallback_provider": "stability_core",
        "extra_params": {
            "negative_prompt": "text, watermark, logo, casual clothing, messy background, deformed",
            "style_preset": "photographic",
        },
        "prompt_template": (
            "A photorealistic portrait of a confident {appearance} professional, "
            "sitting at a clean modern desk or in a bright minimalist office, "
            "looking directly at the camera with a warm and trustworthy smile. "
            "Business casual or professional attire, well-groomed appearance. "
            "Clean bright background with soft bokeh, natural window light. "
            "Professional headshot quality, Sony A7 camera aesthetic, sharp and polished."
        ),
    },
    {
        "id": "person_outdoor_natural",
        "name": "真人·户外自然光",
        "emoji": "🌿",
        "category": "真人",
        "desc": "户外自然光真人，适合健康、美容、生活方式广告",
        "best_for": ["engagement", "traffic", "messaging"],
        "default_provider": "stability_ultra",
        "fallback_provider": "stability_core",
        "extra_params": {
            "negative_prompt": "text, watermark, logo, indoor, artificial light, deformed",
            "style_preset": "photographic",
        },
        "prompt_template": (
            "A candid lifestyle photograph of a happy {appearance} person outdoors, "
            "natural golden hour sunlight, genuine warm smile, relaxed and confident posture. "
            "Clean park, beach, or urban street background softly blurred. "
            "Shot on mirrorless camera, warm color grading, authentic real-life moment. "
            "The person looks healthy, vibrant, and full of life."
        ),
    },
    {
        "id": "finance_chart_person",
        "intent_desc": "专业金融顾问或成功投资者坐在办公桌前，背景有多个显示股票图表的屏幕，表情自信专注，适合股票分析服务或投资顾问广告，画面要有权威感和专业感。",
        "name": "金融·图表+人物",
        "emoji": "📈",
        "category": "金融",
        "desc": "金融图表与人物结合，适合股票、理财、投资广告",
        "best_for": ["lead", "traffic", "messaging"],
        "default_provider": "openai",
        "fallback_provider": "stability_ultra",
        "extra_params": {
            "quality": "high",
            "negative_prompt": "blurry, deformed, cartoon, low quality",
        },
        "prompt_template": (
            "A compelling financial advertisement photograph. "
            "A {appearance} person in smart casual clothes, sitting at a modern desk, "
            "looking at dual monitors showing live stock charts with sharp upward green candles. "
            "Their expression shows focused excitement — leaning forward, slight smile, "
            "as if watching their portfolio grow in real time. "
            "One hand on the mouse, the other holding a phone showing a brokerage app. "
            "Background: sleek modern home office, city view through window at dusk. "
            "Mood: confident, successful, in control of financial destiny. "
            "Photorealistic, cinematic lighting, professional advertising photography."
        ),
    },
    {
        "id": "stock_big_text_hook",
        "intent_desc": "Create a VIRAL stock trading advertisement image. Style: dark background (deep navy or black) with a MASSIVE green profit percentage number dominating the center (use real stock data like +847% or +312%). The number must be HUGE — taking up 40% of the image height, in electric green or gold. Top: bold white ALL-CAPS headline (4-6 words, e.g. 'BUY 500 SHARES NOW'). Bottom: urgency text + action CTA. Background: subtle green candlestick chart lines, upward trending. Color scheme: dark + electric green + white + gold. Feel: like a real trading alert that went viral on social media. Ultra sharp text, maximum contrast, impossible to ignore.",
        "name": "股票·大字钩子",
        "emoji": "📈",
        "category": "金融",
        "desc": "大字文案+金融背景，模仿病毒式传播的股票广告风格",
        "best_for": ["traffic", "lead"],
        "default_provider": "ideogram",
        "fallback_provider": "openai",
        "allow_text": True,
        "extra_params": {
            "style_type": "REALISTIC",
            "magic_prompt_option": "AUTO",
            "negative_prompt": "blurry, cartoon, low quality, watermark",
        },
        "prompt_template": (
            "A viral stock market advertisement image in the style of social media trading posts. "
            "Dark background with dramatic green stock chart lines shooting upward. "
            "Bold white text overlay at top: 'BUY 500 SHARES NOW'. "
            "Large bright green text in center: '+847%'. "
            "Smaller text below: 'This stock is about to EXPLODE'. "
            "Bottom: red urgent banner 'LIMITED TIME OPPORTUNITY'. "
            "Style: high contrast, dramatic lighting, looks like a real trading alert screenshot. "
            "Ultra sharp text, professional financial advertisement quality."
        ),
    },
    {
        "id": "stock_celebrity_endorsement",
        "intent_desc": "Create a HIGH-AUTHORITY stock investment advertisement. A confident silver-haired businessman in his 50s-60s wearing a sharp suit, POINTING directly at the camera with a knowing 'I told you so' expression. Split composition: man on left 40%, bold text/chart on right 60%. Text overlay must include: specific stock ticker + dramatic price movement (use real stock data, e.g. 'NVDA: $0.24 → $51.80'). Background: blurred financial news studio or trading floor with screens. Bottom: free analysis CTA. Lighting: dramatic cinematic. Feel: like a legitimate financial news thumbnail that commands immediate trust and attention. Sharp text, authoritative composition.",
        "name": "股票·名人背书风",
        "emoji": "🎯",
        "category": "金融",
        "desc": "名人/专家形象+股票推荐，高权威感广告",
        "best_for": ["traffic", "lead"],
        "default_provider": "ideogram",
        "fallback_provider": "openai",
        "allow_text": True,
        "extra_params": {
            "style_type": "REALISTIC",
            "magic_prompt_option": "AUTO",
            "negative_prompt": "blurry, cartoon, low quality, watermark",
        },
        "prompt_template": (
            "A high-authority stock investment advertisement. "
            "A confident {appearance} businessman in his 50s, wearing a suit, pointing at camera. "
            "Bold headline text overlay: '7 STOCKS READY TO BREAK OUT'. "
            "Subtext: '$0.24 → $51.80 (MY PREDICTION)'. "
            "Background: blurred stock market trading floor or financial news studio. "
            "Bottom banner: 'FREE ANALYSIS — LINK IN BIO'. "
            "Style: looks like a legitimate financial news thumbnail, authoritative and urgent. "
            "Sharp text, cinematic lighting, professional broadcast quality."
        ),
    },
        {
        "id": "finance_number_impact",
        "intent_desc": "Create the most VIRAL stock trading reaction photo possible. A real, ordinary-looking person (NOT a model — pick one: 40s Asian man, 50s Latino man, 35s Black woman, 45s Middle Eastern man) sitting casually at home (kitchen table at night, messy home office, couch). They are holding their phone with BOTH hands, face showing MAXIMUM SHOCK — jaw literally dropped open, eyes as wide as possible, one hand shooting up to cover their mouth OR both hands on cheeks Home Alone style. Their phone screen MUST be clearly visible showing: a stock trading app (Robinhood/TD Ameritrade style) with a MASSIVE green spike chart going nearly vertical + bold profit number (use real stock data, e.g. '+847%' or '$47,200 profit today'). Lighting: imperfect and authentic — phone screen glow illuminating their face in a dim room, OR harsh window light. The image must feel 100% real and candid, like someone just screenshotted their portfolio at 2am and posted it. Every viewer should think: 'wait, is this real? I need to know what stock this is.'",
        "name": "金融·数字冲击感",
        "emoji": "💰",
        "category": "金融",
        "desc": "真人+大数字视觉冲击，适合股票、高收益广告引流",
        "best_for": ["traffic", "lead"],
        "default_provider": "ideogram",
        "fallback_provider": "openai",
        "allow_text": True,
        "extra_params": {
            "style_type": "REALISTIC",
            "magic_prompt_option": "AUTO",
            "negative_prompt": "blurry, cartoon, low quality, deformed, watermark",
        },
        "prompt_template": (
            "A high-impact stock trading advertisement. "
            "A {appearance} man in his 40s, sitting at home, holding a smartphone close to camera. "
            "His face shows pure shock and disbelief — jaw dropped, eyes wide open, one hand on his cheek. "
            "The phone screen is clearly visible in foreground showing a stock app: green candlestick chart spiking upward, "
            "bold text overlay on screen: '+312%' in bright green. "
            "Blurred background: cozy living room, warm light. "
            "Style: ultra-realistic photo, shot on iPhone, candid moment, "
            "the kind of screenshot someone would share on social media after a huge win. "
            "Cinematic depth of field, the phone screen text must be sharp and readable."
        ),
    },
    {
        "id": "beauty_product_closeup",
        "name": "美容·产品特写",
        "emoji": "✨",
        "category": "美容健康",
        "desc": "美容产品特写，适合护肤、美妆广告",
        "best_for": ["purchase", "traffic"],
        "default_provider": "stability_ultra",
        "fallback_provider": "openai",
        "extra_params": {
            "negative_prompt": "text, watermark, logo, deformed, blurry, dirty background",
            "style_preset": "photographic",
        },
        "prompt_template": (
            "A luxury beauty product advertisement photograph, "
            "elegant skincare or cosmetic product displayed on a clean white or marble surface, "
            "with soft natural light creating beautiful highlights and shadows. "
            "Fresh flowers, green leaves, or water droplets as complementary props. "
            "Shallow depth of field, professional product photography, "
            "premium brand aesthetic, ultra high resolution, magazine quality."
        ),
    },
    {
        "id": "beauty_person_using",
        "name": "美容·使用场景",
        "emoji": "💆",
        "category": "美容健康",
        "desc": "真人使用美容产品，适合护肤、健康广告",
        "best_for": ["purchase", "engagement", "messaging"],
        "default_provider": "fal_kontext",
        "fallback_provider": "stability_ultra",
        "extra_params": {
            "guidance_scale": 4.5,
            "negative_prompt": "text, watermark, deformed, blurry",
        },
        "prompt_template": (
            "A lifestyle photograph of a {appearance} woman with glowing healthy skin, "
            "gently applying skincare product to her face or neck, "
            "soft bathroom or bedroom setting with warm natural light. "
            "Her skin looks radiant, smooth, and transformed. "
            "Genuine expression of pleasure and satisfaction. "
            "Clean minimal background, professional beauty photography quality."
        ),
    },
    {
        "id": "ecommerce_product_white",
        "name": "电商·产品白底",
        "emoji": "📦",
        "category": "电商",
        "desc": "产品白底图，适合购买类广告",
        "best_for": ["purchase"],
        "default_provider": "openai",
        "fallback_provider": "stability_core",
        "extra_params": {
            "quality": "high",
        },
        "prompt_template": (
            "A professional e-commerce product photograph on a pure white background, "
            "the product perfectly centered and lit with soft studio lighting, "
            "creating subtle shadows for depth. "
            "Ultra sharp focus, high resolution, clean and minimal composition. "
            "Professional product photography, Amazon or Shopify listing quality. "
            "No props, no people, just the product in its best light."
        ),
    },
    {
        "id": "ecommerce_lifestyle",
        "name": "电商·生活场景",
        "emoji": "🛍️",
        "category": "电商",
        "desc": "产品生活场景图，适合购买类广告",
        "best_for": ["purchase", "engagement"],
        "default_provider": "stability_ultra",
        "fallback_provider": "stability_core",
        "extra_params": {
            "negative_prompt": "text, watermark, logo, deformed, blurry",
            "style_preset": "photographic",
        },
        "prompt_template": (
            "A lifestyle product advertisement photograph showing a {appearance} person "
            "happily using or holding a product in a natural home or outdoor setting. "
            "Warm inviting atmosphere, natural lighting, authentic moment. "
            "The person looks genuinely happy and satisfied with the product. "
            "Clean modern background, professional advertising photography quality."
        ),
    },
    {
        "id": "home_interior_warm",
        "name": "家居·温馨室内",
        "emoji": "🏠",
        "category": "家居生活",
        "desc": "温馨室内场景，适合家居、生活类广告",
        "best_for": ["purchase", "engagement", "traffic"],
        "default_provider": "stability_core",
        "fallback_provider": "fal_schnell",
        "extra_params": {
            "negative_prompt": "text, watermark, people, deformed, dark, gloomy",
            "style_preset": "photographic",
        },
        "prompt_template": (
            "A warm and inviting interior lifestyle photograph of a beautifully decorated "
            "modern living room or bedroom, soft warm lighting, cozy atmosphere. "
            "Clean minimal Scandinavian or contemporary design aesthetic, "
            "natural materials like wood and linen, fresh plants as accents. "
            "Golden hour light streaming through windows, peaceful and aspirational mood. "
            "Professional interior photography, Architectural Digest quality."
        ),
    },
    {
        "id": "app_phone_mockup",
        "name": "App·手持截图",
        "emoji": "📱",
        "category": "App",
        "desc": "手持手机App截图，适合App下载广告",
        "best_for": ["traffic", "lead"],
        "default_provider": "openai",
        "fallback_provider": "stability_ultra",
        "extra_params": {
            "quality": "high",
        },
        "prompt_template": (
            "A professional advertisement photograph of a {appearance} person "
            "holding a modern smartphone showing a clean app interface, "
            "looking at the screen with a satisfied and impressed expression. "
            "The phone screen shows a professional mobile app UI with positive results. "
            "Clean bright background, soft bokeh, professional product photography. "
            "The overall mood conveys ease of use and positive outcomes from using the app."
        ),
    },
    {
        "id": "viral_clone_variation",
        "name": "🔄 爆款裂变·保留主体",
        "emoji": "🔄",
        "category": "裂变",
        "desc": "基于已有爆款素材生成变体，最大程度保留人物/产品一致性",
        "best_for": ["messaging", "lead", "purchase", "traffic", "engagement"],
        "default_provider": "fal_kontext",
        "fallback_provider": "fal_schnell",
        "extra_params": {
            "guidance_scale": 5.0,
            "safety_tolerance": "2",
        },
        "prompt_template": (
            "Keep the main subject (person, face, body, product) IDENTICAL to the reference image. "
            "ONLY change: {variation_focus}. "
            "Maintain the same lighting quality, camera angle, and overall composition style. "
            "High quality, photorealistic, professional advertising photography."
        ),
        "requires_source": True,
    },
]

# 配方 ID → 配方字典（快速查找）
RECIPE_MAP = {r["id"]: r for r in RECIPES}


# ─────────────────────────────────────────────────────────────────────────────
# 厂商自动路由
# ─────────────────────────────────────────────────────────────────────────────

def auto_route_provider(recipe_id: str, gen_mode: str, configured_providers: set) -> str:
    """
    根据场景配方和已配置厂商自动选择最优厂商
    configured_providers: 已配置 API Key 的厂商 ID 集合
    """
    recipe = RECIPE_MAP.get(recipe_id)
    if recipe:
        # 优先用配方推荐的厂商
        preferred = recipe["default_provider"]
        if preferred in configured_providers:
            return preferred
        # 降级到备选厂商
        fallback = recipe.get("fallback_provider", "fal_schnell")
        if fallback in configured_providers:
            return fallback

    # 通用路由逻辑（无配方时）
    if gen_mode == "img2img":
        for p in ["fal_kontext", "openai", "ideogram"]:
            if p in configured_providers:
                return p
    else:
        for p in ["stability_ultra", "openai", "stability_core", "ideogram", "fal_schnell"]:
            if p in configured_providers:
                return p

    # 最终兜底
    if configured_providers:
        return next(iter(configured_providers))
    raise HTTPException(400, "没有已配置 API Key 的图像生成厂商，请在系统设置 → 图像生成中填写")


# ─────────────────────────────────────────────────────────────────────────────
# 裂变方向 → guidance_scale 映射
# ─────────────────────────────────────────────────────────────────────────────

VARIATION_GUIDANCE = {
    "background": (5.0, "the background environment to a completely different location or setting"),
    "lighting": (4.5, "the lighting atmosphere (warm golden hour / cool blue tone / dramatic studio light)"),
    "outfit": (4.0, "the outfit and clothing style while keeping the same person and face"),
    "expression": (3.5, "the facial expression and pose slightly while keeping the same person"),
    "scene": (3.0, "the entire scene and environment to a different location"),
    "style": (2.5, "the overall visual style (more cinematic / more natural / more vibrant)"),
}

# 裂变方向中文标签
VARIATION_LABELS = {
    "background": "换背景",
    "lighting": "换光线",
    "outfit": "换服装",
    "expression": "换表情/姿势",
    "scene": "换场景",
    "style": "换风格",
}


# ─────────────────────────────────────────────────────────────────────────────
# AI Prompt 增强引擎
# ─────────────────────────────────────────────────────────────────────────────

def _enrich_prompt_with_ai_analysis(
    base_prompt: str,
    source_asset_id: Optional[int],
    conn,
) -> str:
    """
    读取素材的 AI 分析结果，将广告意图、受众特征注入 Prompt
    """
    if not source_asset_id:
        return base_prompt

    row = conn.execute(
        "SELECT ai_analysis, ai_audience_note, ai_purpose, ai_titles FROM ad_assets WHERE id=?",
        (source_asset_id,)
    ).fetchone()
    if not row:
        return base_prompt

    ai_analysis = (row[0] or "").strip()
    ai_audience = (row[1] or "").strip()
    ai_purpose = (row[2] or "").strip()

    enrichments = []
    if ai_purpose:
        enrichments.append(f"Ad purpose: {ai_purpose}")
    if ai_audience:
        enrichments.append(f"Target audience: {ai_audience[:120]}")
    if ai_analysis:
        # 只取前150字，避免 Prompt 过长
        enrichments.append(f"Original ad context: {ai_analysis[:150]}")

    if enrichments:
        return base_prompt + " " + ". ".join(enrichments) + "."
    return base_prompt


def _get_appearance_for_countries(target_countries: list) -> str:
    """根据目标国家列表返回人物外貌描述"""
    for c in (target_countries or []):
        c_upper = c.upper().strip()
        if c_upper in COUNTRY_APPEARANCE:
            return COUNTRY_APPEARANCE[c_upper]
    return "diverse, multicultural"


# ─────────────────────────────────────────────────────────────────────────────
# Prompt 构建函数
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# 真实股票数据拉取（Yahoo Finance）
# ─────────────────────────────────────────────────────────────────────────────
_STOCK_CACHE: dict = {}
_STOCK_CACHE_TTL = 3600  # 1小时缓存

def _fetch_real_stock_data(top_n: int = 5) -> str:
    """
    从 Yahoo Finance 拉取近30天涨幅最大的热门股票数据。
    返回格式化字符串，如：NVDA +187.3%, TSLA +94.1%, AAPL +23.5%
    """
    import time
    cache_key = "top_stocks"
    now = time.time()
    if cache_key in _STOCK_CACHE and now - _STOCK_CACHE[cache_key]["ts"] < _STOCK_CACHE_TTL:
        return _STOCK_CACHE[cache_key]["data"]

    # 热门股票池（科技+金融+新能源）
    WATCHLIST = [
        "NVDA", "TSLA", "AAPL", "MSFT", "AMZN", "META", "GOOGL",
        "AMD", "PLTR", "COIN", "MSTR", "SMCI", "ARM", "AVGO",
        "SOFI", "RIVN", "NIO", "BABA", "JD", "PDD"
    ]

    results = []
    if not _YFINANCE_AVAILABLE:
        # yfinance 不可用时返回默认数据
        return "NVDA +187.3% (30d), TSLA +94.1% (30d), AAPL +23.5% (30d)"

    try:
        import datetime
        end = datetime.datetime.now()
        start = end - datetime.timedelta(days=30)

        for ticker in WATCHLIST[:12]:  # 只查前12个，控制时间
            try:
                t = yf.Ticker(ticker)
                hist = t.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
                if len(hist) >= 2:
                    pct = (hist["Close"].iloc[-1] - hist["Close"].iloc[0]) / hist["Close"].iloc[0] * 100
                    results.append((ticker, pct))
            except Exception:
                continue

        if not results:
            return "NVDA +187.3% (30d), TSLA +94.1% (30d), AAPL +23.5% (30d)"

        results.sort(key=lambda x: x[1], reverse=True)
        top = results[:top_n]
        data = ", ".join([f"{t} {'+' if p > 0 else ''}{p:.1f}% (30d)" for t, p in top])
        _STOCK_CACHE[cache_key] = {"ts": now, "data": data}
        return data
    except Exception as e:
        return "NVDA +187.3% (30d), TSLA +94.1% (30d), AAPL +23.5% (30d)"


# ─────────────────────────────────────────────────────────────────────────────
# AI 动态 Prompt 生成引擎
# ─────────────────────────────────────────────────────────────────────────────
def _generate_dynamic_prompt(
    intent_desc: str,
    target_countries: list,
    extra_desc: str,
    allow_text: bool,
    conn,
    use_stock_data: bool = False,
    cta_link: str = "",
    image_provider: str = "ideogram",
) -> str:
    """
    调用 LLM（GPT/Gemini）根据配方意图动态生成图像 Prompt。
    这是新架构的核心函数，替代静态 prompt_template。
    """
    try:
        from openai import OpenAI

        # 获取 LLM 配置
        ai_api_key = _get_setting(conn, "ai_api_key", "")
        ai_api_base = _get_setting(conn, "ai_api_base", "https://api.openai.com/v1")
        ai_model = _get_setting(conn, "ai_model", "gpt-4.1-mini")

        if not ai_api_key:
            return ""  # 没有 key，回退到静态模板

        # 拼装上下文信息
        appearance = _get_appearance_for_countries(target_countries)
        country_str = ", ".join(target_countries) if target_countries else "Global"

        context_parts = [f"目标国家/地区: {country_str}"]
        if appearance and appearance != "diverse, multicultural":
            context_parts.append(f"目标受众外貌特征: {appearance}")

        # 注入真实股票数据（仅金融类配方）
        stock_data = ""
        if use_stock_data:
            stock_data = _fetch_real_stock_data(top_n=5)
            if stock_data:
                context_parts.append(f"近30天真实股票涨幅数据: {stock_data}")

        # CTA 由 AI 自动生成，不固定链接
        context_parts.append("行动号召（CTA）: 请 AI 根据广告意图自动生成合适的行动号召文案，例如引导加群、了解更多、立即行动等")

        if extra_desc:
            context_parts.append(f"用户补充要求: {extra_desc}")

        context_str = "\n".join(context_parts)

        # 图像生成厂商特性说明
        provider_tips = {
            "ideogram": "目标图像生成工具是 Ideogram，它非常擅长在图片中生成清晰准确的文字和数字，请充分利用这一特性，在画面中加入有冲击力的文字标题、数字和 CTA。",
            "stability_ultra": "目标图像生成工具是 Stability AI Ultra，擅长超高质量写实摄影风格，请专注于人物、场景和光线的细节描述，避免要求生成文字。",
            "stability_core": "目标图像生成工具是 Stability AI Core，擅长写实摄影风格，请专注于场景和人物描述，避免要求生成文字。",
            "openai": "目标图像生成工具是 DALL-E 3，擅长写实和艺术风格，可以生成简单的文字，请在 prompt 中明确说明文字内容。",
            "fal_schnell": "目标图像生成工具是 FLUX Schnell，擅长快速生成写实图像，不擅长生成文字，请专注于场景和人物的视觉描述。",
        }
        provider_tip = provider_tips.get(image_provider, "")

        text_instruction = (
            "画面中必须包含清晰可读的文字、数字和 CTA（行动号召），这是广告的核心钩子。"
            if allow_text else
            "画面中不要出现任何文字、数字或 logo，只需要纯视觉场景。"
        )

        # 随机选择视觉风格，增加多样性
        import random as _random
        _visual_styles = [
            {
                "style_name": "CANDID_SHOCK",
                "style_desc": "VISUAL STYLE: Candid Shock Reaction — A real ordinary-looking person (NOT a model) experiencing genuine shock/disbelief at financial gains. Specific: middle-aged man or woman, sitting casually at home (kitchen, couch, bed, car). Expression: jaw literally dropped, eyes wide open, one hand covering mouth OR both hands on cheeks. Phone screen MUST be clearly visible showing green stock chart with massive spike + bold profit number. Lighting: imperfect natural light (window glow, phone screen illuminating face in dim room). Feel: I can't believe this is real. End with: shot on iPhone, candid moment, motion blur on hands, ultra realistic"
            },
            {
                "style_name": "BIG_TEXT_IMPACT",
                "style_desc": "VISUAL STYLE: Bold Text Impact (optimized for Ideogram) — Dark or gradient background (deep navy, charcoal, or black). MASSIVE central number in bright green or gold: the profit percentage (e.g., +847% or +312%). The number should take up 40% of the image height — impossible to miss. Top: short punchy headline in white bold font (ALL CAPS, 4-6 words max). Bottom: urgency subtext in smaller font + CTA. Visual elements: subtle stock chart lines in background, upward arrow, candlestick pattern. Color scheme: dark background + electric green + white + gold accents. Feel: like a real trading alert notification that went viral"
            },
            {
                "style_name": "AUTHORITY_EXPERT",
                "style_desc": "VISUAL STYLE: Authority Expert Endorsement — A confident well-dressed man in his 50s-60s (silver hair, suit or smart casual). He is POINTING directly at camera OR at a chart/number beside him. Split composition: person on left 40%, bold text/chart on right 60%. Background: blurred trading floor, financial news studio, or city skyline at night. Text overlay: stock ticker + dramatic price movement (e.g., NVDA: $0.24 to $51.80). His expression: serious, knowing, I told you so confidence. Lighting: dramatic, cinematic, slightly underlit for authority. Feel: like a legitimate financial news thumbnail that demands attention"
            },
            {
                "style_name": "PHONE_SCREEN_CLOSEUP",
                "style_desc": "VISUAL STYLE: Phone Screen Close-up with Reaction — Extreme close-up of hands holding a phone, thumbs visible. Phone screen fills 60% of frame showing: Robinhood/TD Ameritrade style app with massive green gains. Specific numbers on screen: portfolio value, percentage gain, dollar amount profit. Partial face visible at top — just eyes and forehead showing PURE SHOCK. Background: blurred home environment (couch, desk, bed). The phone screen should look 100% real — like a genuine screenshot. Lighting: phone screen glow as primary light source, very authentic. Feel: someone just screenshotted their portfolio and it is insane"
            },
            {
                "style_name": "BEFORE_AFTER_WEALTH",
                "style_desc": "VISUAL STYLE: Transformation / Before-After Wealth — Single scene showing dramatic lifestyle upgrade. A person in an aspirational setting (nice home office, penthouse view, or luxury car interior) holding phone with trading app showing massive gains. Key element: phone screen clearly showing the specific profit number. Text overlay: specific contrast numbers (Before: $500 / After: $47,200 or similar). Their expression: relaxed confidence, slight smile, disbelief mixed with joy. Feel: aspirational but believable — this could be me in 3 months"
            },
        ]
        _chosen_style = _random.choice(_visual_styles)
        system_prompt = f"""You are a world-class Facebook ad creative director with 15 years of experience creating viral, high-converting stock trading advertisements. You know exactly what makes people stop scrolling, feel FOMO, and click.

Your task: Write a detailed English image generation prompt for an AI image generator.

CRITICAL OUTPUT RULES:
1. Output ONLY the English prompt — NO explanations, NO markdown, NO prefixes, NO "Here is the prompt:"
2. Length: 250-350 words
3. Be EXTREMELY specific — vague prompts create boring images. Name exact colors, exact emotions, exact numbers, exact settings.
4. The image MUST trigger immediate emotional response: FOMO, excitement, disbelief, or aspiration

{_chosen_style['style_desc']}

UNIVERSAL REQUIREMENTS (apply to ALL styles):
- Include at least ONE specific financial number (e.g., "+847%", "$12,450", "3,200% gain")
- The number must feel REAL and specific, not round (use 847% not 800%, use $12,450 not $12,000)
- Include urgency or scarcity signal somewhere in the composition
- Color psychology: green = profit/go, red = urgency/fear of missing out, gold = wealth
- The overall composition should have ONE clear focal point that the eye goes to immediately

WHAT MAKES STOCK ADS GO VIRAL:
- Real people with real reactions > polished models
- Specific numbers > vague claims
- "This could be me" relatability > aspirational luxury
- Authentic imperfection > perfect studio shots
- Emotional peak moment > calm professional scene"""

        user_prompt = f"""AD BRIEF:
Intent: {intent_desc}

TARGET CONTEXT:
{context_str}

TECHNICAL SPECS:
{provider_tip}
{text_instruction}

VISUAL STYLE TO USE: {_chosen_style['style_name']}

REFERENCE EXAMPLES OF WHAT WORKS:
- A 45-year-old Asian man sitting at his kitchen table, holding phone showing "+312%" in massive green text, mouth wide open, eyes bulging, hand on forehead in disbelief. Phone screen shows trading app. Morning light from window. Shot on iPhone.
- Dark background with giant "+847%" in neon green taking up center of image. "BUY 500 SHARES NOW" in white bold caps at top. "THIS STOCK IS ABOUT TO EXPLODE" in smaller text below. Subtle candlestick chart in background.
- Silver-haired businessman in suit, pointing at camera, split screen showing stock chart going vertical. Text overlay: "NVDA: I told you at $0.24. Now look." Dramatic studio lighting.

NOW GENERATE THE PROMPT:
Remember: be extremely specific about colors, expressions, numbers, and environment. The more specific, the better the image."""

        llm_client = OpenAI(api_key=ai_api_key, base_url=ai_api_base)
        response = llm_client.chat.completions.create(
            model=ai_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=500,
            temperature=0.9,
        )
        generated = response.choices[0].message.content.strip()
        # 清理可能的 markdown 代码块
        if generated.startswith("```"):
            generated = generated.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        return generated

    except Exception as e:
        # LLM 调用失败，静默回退
        return ""

def _build_prompt_from_recipe(
    recipe_id: str,
    target_countries: list,
    extra_desc: str,
    variation_direction: str = "background",
    source_asset_id: Optional[int] = None,
    conn=None,
) -> tuple[str, dict]:
    """
    根据场景配方构建 Prompt
    返回: (prompt_str, extra_params_dict)
    """
    recipe = RECIPE_MAP.get(recipe_id)
    if not recipe:
        # 尝试从自定义配方数据库加载
        if conn:
            import json as _json_r
            cr = conn.execute(
                "SELECT recipe_id, name, emoji, category, desc, prompt_template, "
                "default_provider, fallback_provider, allow_text, best_for, extra_params, "
                "COALESCE(intent_desc, '') as intent_desc "
                "FROM custom_recipes WHERE recipe_id=?", (recipe_id,)
            ).fetchone()
            if cr:
                recipe = {
                    "id": cr[0], "name": cr[1], "emoji": cr[2], "category": cr[3],
                    "desc": cr[4], "prompt_template": cr[5],
                    "default_provider": cr[6], "fallback_provider": cr[7],
                    "allow_text": bool(cr[8]),
                    "best_for": _json_r.loads(cr[9]) if cr[9] else [],
                    "extra_params": _json_r.loads(cr[10]) if cr[10] else {},
                    "intent_desc": cr[11] or "",
                }
        if not recipe:
            raise HTTPException(400, f"未知配方 ID: {recipe_id}")

    appearance = _get_appearance_for_countries(target_countries)
    extra_params = dict(recipe.get("extra_params", {}))

    # 裂变配方特殊处理（不走 AI 动态生成）
    if recipe_id == "viral_clone_variation":
        template = recipe.get("prompt_template", "")
        guidance_scale, variation_focus = VARIATION_GUIDANCE.get(
            variation_direction, (4.0, "the background environment")
        )
        prompt = template.format(variation_focus=variation_focus)
        extra_params["guidance_scale"] = guidance_scale
        if extra_desc and extra_desc.strip():
            prompt += f" {extra_desc.strip()}."
    else:
        # ── 新架构：优先使用 AI 动态生成 Prompt ──
        intent_desc = recipe.get("intent_desc", "") or recipe.get("desc", "")
        # 金融类配方自动注入股票数据
        use_stock_data = recipe.get("category", "") in ("金融",) or recipe.get("use_stock_data", False)
        # CTA 由 AI 自动生成，不需要固定链接
        cta_link = ""
        img_provider = recipe.get("default_provider", "ideogram")

        ai_prompt = ""
        if intent_desc and conn:
            ai_prompt = _generate_dynamic_prompt(
                intent_desc=intent_desc,
                target_countries=target_countries,
                extra_desc=extra_desc,
                allow_text=recipe.get("allow_text", False),
                conn=conn,
                use_stock_data=use_stock_data,
                cta_link=cta_link,
                image_provider=img_provider,
            )

        if ai_prompt:
            # AI 动态生成成功，直接使用（AI 已包含质量词和文案要求）
            prompt = ai_prompt
        else:
            # 回退到静态模板
            template = recipe.get("prompt_template", intent_desc or "A high quality advertisement image.")
            try:
                prompt = template.format(appearance=appearance)
            except (KeyError, IndexError):
                prompt = template
            if extra_desc and extra_desc.strip():
                prompt += f" {extra_desc.strip()}."
            # 追加通用质量要求（仅静态模板回退时）
            if recipe.get("allow_text"):
                prompt += (
                    " Ultra high quality, 4K resolution, professional advertising photography. "
                    "NO watermarks, NO logos."
                )
            else:
                prompt += (
                    " Ultra high quality, 4K resolution, professional advertising photography. "
                    "CRITICAL: absolutely NO text, NO words, NO letters, NO watermarks, NO logos, "
                    "NO UI elements, NO phone screens with visible content anywhere in the image."
                )

    # AI 分析增强（裂变时不需要，因为已有参考图）
    if conn and source_asset_id and recipe_id != "viral_clone_variation":
        prompt = _enrich_prompt_with_ai_analysis(prompt, source_asset_id, conn)

    return prompt, extra_params


def _build_prompt_for_variation(
    source_asset: dict,
    variation_direction: str,
    audience_desc: str,
) -> tuple[str, dict]:
    """
    图生图裂变 Prompt（无配方时的通用版本）
    返回: (prompt_str, extra_params_dict)
    """
    guidance_scale, variation_focus = VARIATION_GUIDANCE.get(
        variation_direction, (4.0, "the background environment")
    )

    ai_analysis = (source_asset.get("ai_analysis") or "").strip()
    ai_purpose = (source_asset.get("ai_purpose") or "").strip()

    prompt_parts = [
        f"Keep the main subject (person, face, body, product) IDENTICAL to the reference image. "
        f"ONLY change: {variation_focus}. "
        "Maintain the same lighting quality, camera angle, and overall composition style."
    ]

    if audience_desc:
        prompt_parts.append(f"Target audience: {audience_desc}")
    if ai_purpose:
        prompt_parts.append(f"Ad purpose: {ai_purpose}")
    if ai_analysis:
        prompt_parts.append(f"Original ad context: {ai_analysis[:120]}")

    prompt_parts.append(
        "High quality, photorealistic, professional advertising photography. "
        "NO text, NO watermarks, NO logos."
    )

    extra_params = {"guidance_scale": guidance_scale}
    return " ".join(prompt_parts), extra_params


def _build_prompt_for_new(
    audience_desc: str,
    ad_type: str,
    style: str,
    extra_desc: str,
    target_countries: list,
) -> str:
    """为全新生成构建 Prompt（无配方时的通用版本，保留向后兼容）"""
    ad_type_prompts = {
        "messaging": (
            "A warm and inviting social media advertisement photo showing a happy person "
            "holding a smartphone, smiling naturally, looking directly at the camera, "
            "with a soft blurred indoor or cafe background. The scene conveys friendliness "
            "and approachability, encouraging viewers to reach out and start a conversation"
        ),
        "lead": (
            "A professional and trustworthy advertisement photo of a confident person "
            "sitting at a desk or in a clean bright office environment, looking engaged "
            "and approachable. The scene conveys expertise and reliability, suitable for "
            "a lead generation campaign"
        ),
        "purchase": (
            "An attractive product lifestyle advertisement photo showing a person happily "
            "using or holding a product, with a clean bright background. The scene conveys "
            "desire and satisfaction, suitable for an e-commerce purchase campaign"
        ),
        "traffic": (
            "An eye-catching and dynamic advertisement photo with a strong visual focal point, "
            "vibrant colors, and an energetic composition that immediately grabs attention "
            "and makes viewers want to click to learn more"
        ),
        "engagement": (
            "A fun and relatable social media lifestyle photo showing a genuine moment of "
            "joy or surprise, with natural lighting and authentic emotions. The scene feels "
            "real and shareable, designed to drive likes, comments and shares"
        ),
    }
    style_prompts = {
        "photo": (
            "photorealistic photography, shot on a professional camera, "
            "natural skin tones, sharp focus on subject, soft bokeh background, "
            "studio-quality lighting, Canon or Sony camera aesthetic"
        ),
        "lifestyle": (
            "candid lifestyle photography, natural ambient lighting, "
            "authentic real-life moment, warm color grading, "
            "shot on mirrorless camera, golden hour or soft indoor light"
        ),
        "illustration": (
            "clean modern digital illustration, flat design with subtle gradients, "
            "vibrant but harmonious color palette, vector art style, "
            "no photorealistic elements, Dribbble-quality design"
        ),
        "screenshot": (
            "realistic close-up photo of a person holding a smartphone showing a positive "
            "reaction, clean background, professional product photography style, "
            "the phone screen content should be completely blurred or not visible"
        ),
        "infographic": (
            "clean minimal visual composition with bold shapes and colors, "
            "flat design concept, white or light background, "
            "no text or typography elements rendered"
        ),
    }
    appearance = _get_appearance_for_countries(target_countries)
    parts = [
        ad_type_prompts.get(ad_type, "A high-converting Facebook advertisement image, professional quality"),
        style_prompts.get(style, "photorealistic photography, professional quality"),
    ]
    if appearance and appearance != "diverse, multicultural":
        parts.append(f"Subject and setting: {appearance}")
    if audience_desc:
        parts.append(f"Designed to appeal to: {audience_desc}")
    if extra_desc:
        parts.append(extra_desc)
    parts.append(
        "Ultra high quality, 4K resolution, professional advertising photography. "
        "CRITICAL RULES: absolutely NO text, NO words, NO letters, NO numbers, "
        "NO watermarks, NO logos, NO UI elements, NO phone screens with visible content, "
        "NO app interfaces, NO captions, NO subtitles anywhere in the image. "
        "Pure visual storytelling only, no typography of any kind."
    )
    return " ".join(p.strip().rstrip(".") + "." for p in parts if p.strip())


# ─────────────────────────────────────────────────────────────────────────────
# 数据库初始化（幂等）
# ─────────────────────────────────────────────────────────────────────────────

def init_custom_recipes_table(conn):
    """初始化自定义配方表"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS custom_recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            emoji TEXT DEFAULT '🎨',
            category TEXT DEFAULT '自定义',
            desc TEXT DEFAULT '',
            prompt_template TEXT NOT NULL,
            default_provider TEXT DEFAULT 'ideogram',
            fallback_provider TEXT DEFAULT 'openai',
            allow_text INTEGER DEFAULT 0,
            best_for TEXT DEFAULT '["traffic","lead"]',
            extra_params TEXT DEFAULT '{}',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def init_creative_gen_tables():
    """创建 creative_gen 相关数据库表（幂等）"""
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS creative_pending (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id         TEXT NOT NULL,
            provider        TEXT NOT NULL,
            model           TEXT NOT NULL,
            prompt          TEXT,
            source_asset_id INTEGER,
            gen_mode        TEXT DEFAULT 'txt2img',
            task_type       TEXT DEFAULT '',
            recipe_id       TEXT,
            local_path      TEXT,
            remote_url      TEXT,
            b64_preview     TEXT,
            aspect_ratio    TEXT DEFAULT '1:1',
            cost_usd        REAL DEFAULT 0,
            status          TEXT DEFAULT 'pending',
            reject_reason   TEXT,
            approved_asset_id INTEGER,
            target_countries TEXT DEFAULT '[]',
            note            TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_status ON creative_pending(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_task ON creative_pending(task_id)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS creative_tasks (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id         TEXT UNIQUE NOT NULL,
            provider        TEXT NOT NULL,
            gen_mode        TEXT DEFAULT 'txt2img',
            task_type       TEXT DEFAULT '',
            recipe_id       TEXT,
            source_asset_id INTEGER,
            prompt          TEXT,
            num_images      INTEGER DEFAULT 1,
            status          TEXT DEFAULT 'running',
            total_cost_usd  REAL DEFAULT 0,
            error_msg       TEXT,
            retry_count     INTEGER DEFAULT 0,
            created_by      TEXT DEFAULT 'user',
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    gen_settings = [
        ("img_gen_provider", "fal_kontext", "默认图像生成厂商",
         "素材自动生成使用的默认厂商", "fal_kontext", "img_gen", 1),
        ("img_gen_fal_key", "", "Fal.ai API Key",
         "Fal.ai 图像生成 API Key（FLUX Kontext/Schnell）", "fal_xxxxxxxx", "img_gen", 2),
        ("img_gen_openai_key", "", "OpenAI Image API Key",
         "OpenAI gpt-image-1 API Key", "sk-xxxxxxxx", "img_gen", 3),
        ("img_gen_stability_key", "", "Stability AI API Key",
         "Stability AI 图像生成 API Key", "sk-xxxxxxxx", "img_gen", 4),
        ("img_gen_ideogram_key", "", "Ideogram API Key",
         "Ideogram 3.0 图像生成 API Key", "xxxxxxxx", "img_gen", 5),
        ("img_gen_replicate_key", "", "Replicate API Token",
         "Replicate 图像生成 API Token", "r8_xxxxxxxx", "img_gen", 6),
        ("img_gen_replicate_model", "black-forest-labs/flux-schnell",
         "Replicate 默认模型", "Replicate 调用的模型（owner/name:version）",
         "black-forest-labs/flux-schnell", "img_gen", 7),
    ]
    for row in gen_settings:
        conn.execute(
            "INSERT OR IGNORE INTO settings(key,value,label,description,placeholder,category,sort_order) VALUES(?,?,?,?,?,?,?)",
            row
        )

    # 幂等字段补全
    existing_cols = {row[1] for row in conn.execute('PRAGMA table_info(creative_tasks)').fetchall()}
    needed_cols = {
        'source_asset_id': 'INTEGER', 'prompt': 'TEXT', 'updated_at': 'TEXT',
        'total_cost_usd': 'REAL DEFAULT 0', 'created_by': 'TEXT',
        'task_type': 'TEXT DEFAULT ""', 'retry_count': 'INTEGER DEFAULT 0', 'recipe_id': 'TEXT',
    }
    for col, col_type in needed_cols.items():
        if col not in existing_cols:
            try:
                conn.execute(f'ALTER TABLE creative_tasks ADD COLUMN {col} {col_type}')
            except Exception:
                pass

    existing_pending_cols = {row[1] for row in conn.execute('PRAGMA table_info(creative_pending)').fetchall()}
    needed_pending_cols = {
        'model': 'TEXT', 'b64_preview': 'TEXT', 'cost_usd': 'REAL DEFAULT 0',
        'approved_asset_id': 'INTEGER', 'note': 'TEXT', 'local_path': 'TEXT',
        'remote_url': 'TEXT', 'recipe_id': 'TEXT',
    }
    for col, col_type in needed_pending_cols.items():
        if col not in existing_pending_cols:
            try:
                conn.execute(f'ALTER TABLE creative_pending ADD COLUMN {col} {col_type}')
            except Exception:
                pass

    conn.commit()
    init_custom_recipes_table(conn)
    conn.close()
    logger.info("[creative_gen] 数据库表初始化完成 v3.0")


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _get_setting(conn, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return (row[0] or default) if row else default


def _get_configured_providers(conn) -> set:
    """返回已配置 API Key 的厂商 ID 集合"""
    key_map = {
        "fal_kontext": "img_gen_fal_key",
        "fal_schnell": "img_gen_fal_key",
        "openai": "img_gen_openai_key",
        "stability_core": "img_gen_stability_key",
        "stability_ultra": "img_gen_stability_key",
        "ideogram": "img_gen_ideogram_key",
        "replicate": "img_gen_replicate_key",
    }
    configured = set()
    for provider, setting_key in key_map.items():
        val = _get_setting(conn, setting_key)
        if val and len(val) > 4:
            configured.add(provider)
    return configured


def _get_api_key(conn, provider: str) -> str:
    key_map = {
        "fal_kontext": "img_gen_fal_key",
        "fal_schnell": "img_gen_fal_key",
        "openai": "img_gen_openai_key",
        "stability_core": "img_gen_stability_key",
        "stability_ultra": "img_gen_stability_key",
        "ideogram": "img_gen_ideogram_key",
        "replicate": "img_gen_replicate_key",
    }
    setting_key = key_map.get(provider, "")
    if not setting_key:
        raise HTTPException(400, f"不支持的厂商: {provider}")
    api_key = _get_setting(conn, setting_key)
    if not api_key:
        raise HTTPException(400, f"厂商 {provider} 的 API Key 未配置，请在系统设置 → 图像生成中填写")
    return api_key


# ─────────────────────────────────────────────────────────────────────────────
# 后台生成任务
# ─────────────────────────────────────────────────────────────────────────────

def _run_generation_task(
    task_id: str,
    provider: str,
    api_key: str,
    prompt: str,
    gen_mode: str,
    num_images: int,
    aspect_ratio: str,
    source_asset_id: Optional[int],
    source_asset_local_path: Optional[str],
    source_asset_serve_url: Optional[str],
    target_countries: list,
    extra_params: dict,
    recipe_id: Optional[str] = None,
):
    """后台执行图像生成任务"""
    from services.image_provider import generate_image

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE creative_tasks SET status='running', updated_at=? WHERE task_id=?",
            (datetime.now().isoformat(), task_id)
        )
        conn.commit()

        results = generate_image(
            provider=provider,
            prompt=prompt,
            api_key=api_key,
            reference_image_url=source_asset_serve_url if provider == "fal_kontext" else None,
            reference_image_path=source_asset_local_path if provider in ("openai", "ideogram") else None,
            num_images=num_images,
            aspect_ratio=aspect_ratio,
            extra_params=extra_params,
        )

        total_cost = 0.0
        for i, result in enumerate(results):
            filename = f"pending_{task_id}_{i}_{int(time.time())}.jpg"
            local_path = os.path.join(PENDING_DIR, filename)

            if result.get("b64"):
                with open(local_path, "wb") as f:
                    f.write(base64.b64decode(result["b64"]))
                remote_url = None
            elif result.get("url"):
                try:
                    resp = requests.get(result["url"], timeout=60)
                    resp.raise_for_status()
                    with open(local_path, "wb") as f:
                        f.write(resp.content)
                    remote_url = result["url"]
                except Exception as e:
                    logger.error(f"[creative_gen] 下载图片失败: {e}")
                    continue
            else:
                continue

            # 黑图检测：文件小于 20KB 视为无效（安全过滤导致的黑图）
            if os.path.exists(local_path) and os.path.getsize(local_path) < 20480:
                logger.warning(f"[creative_gen] 图片疑似黑图（{os.path.getsize(local_path)} bytes），跳过: {local_path}")
                try:
                    os.remove(local_path)
                except Exception:
                    pass
                continue

            total_cost += result.get("cost_usd", 0)

            conn.execute("""
                INSERT INTO creative_pending
                (task_id, provider, model, prompt, source_asset_id, gen_mode, recipe_id,
                 local_path, remote_url, aspect_ratio, cost_usd, status,
                 target_countries, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'pending',?,datetime('now'),datetime('now'))
            """, (
                task_id, provider, result.get("model", provider), prompt,
                source_asset_id, gen_mode, recipe_id, local_path, remote_url,
                aspect_ratio, result.get("cost_usd", 0),
                ",".join(target_countries) if target_countries else "",
            ))

        conn.execute(
            "UPDATE creative_tasks SET status='done', total_cost_usd=?, updated_at=? WHERE task_id=?",
            (total_cost, datetime.now().isoformat(), task_id)
        )
        conn.commit()
        logger.info(f"[creative_gen] 任务 {task_id} 完成，生成 {len(results)} 张，花费 ${total_cost:.4f}")

    except Exception as e:
        logger.error(f"[creative_gen] 任务 {task_id} 失败: {e}", exc_info=True)
        conn.execute(
            "UPDATE creative_tasks SET status='failed', error_msg=?, updated_at=? WHERE task_id=?",
            (str(e)[:500], datetime.now().isoformat(), task_id)
        )
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic 模型
# ─────────────────────────────────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    # 通用参数
    provider: str = "auto"                   # 厂商 ID，"auto" 表示自动路由
    gen_mode: str = "txt2img"               # txt2img / img2img（裂变）
    num_images: int = 4                     # 生成数量（1-8）
    aspect_ratio: str = "1:1"              # 宽高比
    target_countries: List[str] = []        # 目标国家

    # 场景配方（v3.0 新增，优先级最高）
    recipe_id: str = ""                     # 配方 ID，为空时用传统参数

    # 文生图参数（无配方时使用）
    audience_desc: str = ""                 # 目标人群描述
    ad_type: str = "messaging"              # 广告类型
    style: str = "photo"                    # 风格
    extra_desc: str = ""
    use_stock_data: bool = False  # 是否注入真实股票数据
    cta_link: str = ""  # 引流链接（如 WhatsApp 群链接）                    # 额外描述（补充说明）
    custom_prompt: str = ""                 # 自定义 Prompt（覆盖所有自动生成）

    # 图生图（裂变）参数
    source_asset_id: Optional[int] = None  # 源素材 ID
    variation_direction: str = "background" # 变体方向

    # 厂商特定参数（高级用户）
    extra_params: dict = {}


class ApproveRequest(BaseModel):
    target_countries: List[str] = []
    note: str = ""
    ai_purpose: str = ""
    ai_language: str = ""


class RejectRequest(BaseModel):
    reason: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# 一句话智能解析（带缓存）
# ─────────────────────────────────────────────────────────────────────────────

import hashlib as _hashlib
import time as _time
_brief_cache: dict = {}
_CACHE_TTL = 3600

class ParseBriefRequest(BaseModel):
    brief: str


@router.post("/parse-brief")
def parse_brief(req: ParseBriefRequest, user=Depends(get_current_user)):
    """用 AI 解析一句话广告需求，返回结构化参数"""
    brief = req.brief.strip()
    if not brief:
        raise HTTPException(status_code=400, detail="请输入广告需求描述")

    cache_key = _hashlib.md5(brief.encode()).hexdigest()
    now = _time.time()
    if cache_key in _brief_cache:
        result, expire_ts = _brief_cache[cache_key]
        if now < expire_ts:
            return {**result, "cached": True}

    from services.ai_advisor import get_ai_client
    client, model = get_ai_client()

    if not client:
        result = _local_parse(brief)
        result["source"] = "local"
        return result

    system_msg = (
        "你是广告投放助手。从用户的一句话需求中提取结构化参数，返回 JSON，不要有其他文字。"
        "字段说明：target_countries(数组,ISO2代码,如['TW','HK']), "
        "audience_desc(人群描述,中文,≤30字), "
        "ad_type(messaging/lead/purchase/traffic/engagement), "
        "style(photo/illustration/screenshot/infographic/lifestyle), "
        "extra_desc(补充描述,英文,≤20词,可空字符串), "
        "suggested_recipe_id(从以下配方ID中选最合适的一个,可空: "
        "person_phone_surprise/person_professional_desk/person_outdoor_natural/"
        "finance_chart_person/finance_number_impact/beauty_product_closeup/"
        "beauty_person_using/ecommerce_product_white/ecommerce_lifestyle/"
        "home_interior_warm/app_phone_mockup/viral_clone_variation)"
    )

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": f"广告需求：{brief}"},
            ],
            temperature=0.1,
            max_tokens=250,
        )
        raw = resp.choices[0].message.content.strip()
        import json as _json, re as _re
        json_match = _re.search(r'\{[^{}]+\}', raw, _re.DOTALL)
        if json_match:
            raw = json_match.group(0)
        data = _json.loads(raw)
        result = {
            "target_countries": data.get("target_countries", []),
            "audience_desc": data.get("audience_desc", brief),
            "ad_type": data.get("ad_type", "messaging"),
            "style": data.get("style", "photo"),
            "extra_desc": data.get("extra_desc", ""),
            "suggested_recipe_id": data.get("suggested_recipe_id", ""),
            "source": "ai",
        }
        _brief_cache[cache_key] = (result, now + _CACHE_TTL)
        expired = [k for k, (_, ts) in _brief_cache.items() if now > ts]
        for k in expired:
            del _brief_cache[k]
        return {**result, "cached": False}
    except Exception as e:
        logger.warning(f"parse-brief AI解析失败，降级本地解析: {e}")
        result = _local_parse(brief)
        result["source"] = "local_fallback"
        return result


def _local_parse(brief: str) -> dict:
    brief_lower = brief.lower()
    country_map = {
        "台湾": "TW", "tw": "TW", "taiwan": "TW",
        "香港": "HK", "hk": "HK", "hongkong": "HK",
        "马来西亚": "MY", "malaysia": "MY", "my": "MY",
        "新加坡": "SG", "singapore": "SG", "sg": "SG",
        "泰国": "TH", "thailand": "TH", "th": "TH",
        "越南": "VN", "vietnam": "VN", "vn": "VN",
        "印尼": "ID", "indonesia": "ID",
        "菲律宾": "PH", "philippines": "PH", "ph": "PH",
        "美国": "US", "usa": "US", "us": "US",
        "日本": "JP", "japan": "JP", "jp": "JP",
        "韩国": "KR", "korea": "KR", "kr": "KR",
        "澳大利亚": "AU", "australia": "AU", "au": "AU",
        "英国": "UK", "uk": "UK", "britain": "UK",
        "加拿大": "CA", "canada": "CA", "ca": "CA",
        "印度": "IN", "india": "IN", "in": "IN",
        "阿联酋": "AE", "uae": "AE", "dubai": "AE",
        "沙特": "SA", "saudi": "SA",
        "德国": "DE", "germany": "DE",
        "法国": "FR", "france": "FR",
    }
    countries = []
    for kw, code in country_map.items():
        if kw in brief_lower and code not in countries:
            countries.append(code)

    ad_type = "messaging"
    if any(k in brief_lower for k in ["私信", "dm", "message", "聊天"]):
        ad_type = "messaging"
    elif any(k in brief_lower for k in ["购买", "电商", "purchase", "buy", "shop"]):
        ad_type = "purchase"
    elif any(k in brief_lower for k in ["线索", "留资", "lead", "表单"]):
        ad_type = "lead"
    elif any(k in brief_lower for k in ["流量", "traffic", "点击", "网站"]):
        ad_type = "traffic"
    elif any(k in brief_lower for k in ["互动", "engagement", "点赞"]):
        ad_type = "engagement"

    style = "photo"
    if any(k in brief_lower for k in ["插画", "illustration", "卡通"]):
        style = "illustration"
    elif any(k in brief_lower for k in ["截图", "screenshot", "评价"]):
        style = "screenshot"
    elif any(k in brief_lower for k in ["信息图", "infographic", "数据"]):
        style = "infographic"
    elif any(k in brief_lower for k in ["生活", "lifestyle", "日常"]):
        style = "lifestyle"

    # 推荐配方
    suggested_recipe = ""
    if any(k in brief_lower for k in ["股票", "金融", "投资", "理财", "stock", "finance"]):
        suggested_recipe = "finance_chart_person"
    elif any(k in brief_lower for k in ["护肤", "美容", "化妆", "beauty", "skincare"]):
        suggested_recipe = "beauty_person_using"
    elif any(k in brief_lower for k in ["产品", "电商", "product", "shop"]):
        suggested_recipe = "ecommerce_lifestyle"
    elif any(k in brief_lower for k in ["手机", "app", "应用", "软件"]):
        suggested_recipe = "app_phone_mockup"
    elif any(k in brief_lower for k in ["惊喜", "看手机", "surprise"]):
        suggested_recipe = "person_phone_surprise"

    return {
        "target_countries": countries,
        "audience_desc": brief[:50],
        "ad_type": ad_type,
        "style": style,
        "extra_desc": "",
        "suggested_recipe_id": suggested_recipe,
    }


# ─────────────────────────────────────────────────────────────────────────────
# API 路由
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/recipes")
def get_recipes(user=Depends(get_current_user)):
    """获取场景配方列表（傻瓜向导用），包含预设+自定义配方"""
    result = []
    for r in RECIPES:
        result.append({
            "id": r["id"],
            "name": r["name"],
            "emoji": r["emoji"],
            "category": r["category"],
            "desc": r["desc"],
            "best_for": r["best_for"],
            "default_provider": r["default_provider"],
            "requires_source": r.get("requires_source", False),
            "is_custom": False,
        })
    # 追加自定义配方
    import json as _json2
    conn = get_conn()
    custom_rows = conn.execute(
        "SELECT recipe_id, name, emoji, category, desc, best_for, default_provider FROM custom_recipes ORDER BY id ASC"
    ).fetchall()
    conn.close()
    for cr in custom_rows:
        result.append({
            "id": cr[0],
            "name": cr[1],
            "emoji": cr[2],
            "category": cr[3],
            "desc": cr[4],
            "best_for": _json2.loads(cr[5]) if cr[5] else ["traffic", "lead"],
            "default_provider": cr[6],
            "requires_source": False,
            "is_custom": True,
        })
    return result


@router.get("/providers")
def get_providers(user=Depends(get_current_user)):
    """获取支持的厂商列表及当前 API Key 配置状态"""
    from services.image_provider import get_provider_info
    conn = get_conn()
    providers = get_provider_info()
    key_map = {
        "fal_kontext": "img_gen_fal_key",
        "fal_schnell": "img_gen_fal_key",
        "openai": "img_gen_openai_key",
        "stability_core": "img_gen_stability_key",
        "stability_ultra": "img_gen_stability_key",
        "ideogram": "img_gen_ideogram_key",
        "replicate": "img_gen_replicate_key",
    }
    for p in providers:
        setting_key = key_map.get(p["id"], "")
        val = _get_setting(conn, setting_key) if setting_key else ""
        p["key_configured"] = bool(val and len(val) > 4)
    conn.close()
    return providers


@router.get("/settings-keys")
def get_settings_keys(user=Depends(get_current_user)):
    """获取图像生成 API Key 配置状态（不返回明文）"""
    conn = get_conn()
    keys = [
        "img_gen_provider", "img_gen_fal_key", "img_gen_openai_key",
        "img_gen_stability_key", "img_gen_ideogram_key",
        "img_gen_replicate_key", "img_gen_replicate_model",
    ]
    result = {}
    for k in keys:
        val = _get_setting(conn, k)
        if "key" in k and val and len(val) > 8:
            result[k] = val[:4] + "****" + val[-4:]
        else:
            result[k] = val
    conn.close()
    return result


@router.post("/preview-prompt")
def preview_prompt(req: GenerateRequest, user=Depends(get_current_user)):
    """预览将要发送给 AI 的 Prompt（不实际生成）"""
    if req.custom_prompt:
        return {"prompt": req.custom_prompt, "source": "custom", "provider": req.provider}

    conn = get_conn()
    configured = _get_configured_providers(conn)

    if req.recipe_id:
        try:
            prompt, extra_params = _build_prompt_from_recipe(
                recipe_id=req.recipe_id,
                target_countries=req.target_countries,
                extra_desc=req.extra_desc,
                variation_direction=req.variation_direction,
                source_asset_id=req.source_asset_id,
                conn=conn,
                )
            provider = req.provider if req.provider != "auto" else auto_route_provider(req.recipe_id, req.gen_mode, configured)
            conn.close()
            return {"prompt": prompt, "source": "recipe", "recipe_id": req.recipe_id, "provider": provider, "extra_params": extra_params}
        except Exception as e:
            conn.close()
            raise

    conn.close()
    if req.gen_mode == "img2img":
        return {"prompt": f"[图生图裂变] 方向: {VARIATION_LABELS.get(req.variation_direction, req.variation_direction)}", "source": "auto", "provider": req.provider}
    prompt = _build_prompt_for_new(req.audience_desc, req.ad_type, req.style, req.extra_desc, req.target_countries)
    return {"prompt": prompt, "source": "auto", "provider": req.provider}


@router.post("/generate")
async def start_generate(
    req: GenerateRequest,
    background_tasks: BackgroundTasks,
    user=Depends(get_current_user),
):
    """发起素材生成任务（异步后台执行）"""
    conn = get_conn()
    num_images = max(1, min(8, req.num_images))

    if req.gen_mode == "img2img" and not req.source_asset_id:
        conn.close()
        raise HTTPException(400, "img2img 模式需要指定 source_asset_id")

    # 获取已配置厂商
    configured = _get_configured_providers(conn)

    # 确定厂商
    if req.provider == "auto" or not req.provider:
        provider = auto_route_provider(req.recipe_id or "", req.gen_mode, configured)
    else:
        provider = req.provider
        # fal_kontext txt2img 自动降级
        if req.gen_mode == "txt2img" and provider == "fal_kontext":
            provider = "fal_schnell"

    api_key = _get_api_key(conn, provider)

    # 构建 Prompt 和 extra_params
    extra_params = dict(req.extra_params)

    if req.custom_prompt:
        prompt = req.custom_prompt
    elif req.recipe_id:
        # 使用场景配方
        prompt, recipe_extra = _build_prompt_from_recipe(
            recipe_id=req.recipe_id,
            target_countries=req.target_countries,
            extra_desc=req.extra_desc,
            variation_direction=req.variation_direction,
            source_asset_id=req.source_asset_id,
            conn=conn,
        )
        # 如果用户指定了 cta_link，覆盖系统设置中的链接（已在 _build_prompt_from_recipe 内处理）
        if req.cta_link and req.cta_link.strip() and not req.custom_prompt:
            # cta_link 已在 _build_prompt_from_recipe 中通过系统设置读取
            # 如果用户额外指定，重新生成 prompt（仅在 AI 模式下有效）
            pass
        # 配方 extra_params 优先，用户 extra_params 可覆盖
        merged = dict(recipe_extra)
        merged.update(extra_params)
        extra_params = merged
    elif req.gen_mode == "img2img" and req.source_asset_id:
        row = conn.execute(
            "SELECT id, file_name, ai_analysis, ai_audience_note, ai_purpose FROM ad_assets WHERE id=?",
            (req.source_asset_id,)
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, f"源素材 {req.source_asset_id} 不存在")
        source_asset = dict(row)
        prompt, variation_extra = _build_prompt_for_variation(
            source_asset, req.variation_direction, req.audience_desc
        )
        extra_params.update(variation_extra)
    else:
        prompt = _build_prompt_for_new(
            req.audience_desc, req.ad_type, req.style,
            req.extra_desc, req.target_countries
        )

    # 获取源素材路径
    source_local_path = None
    source_serve_url = None
    if req.source_asset_id:
        asset_row = conn.execute(
            "SELECT file_path FROM ad_assets WHERE id=?", (req.source_asset_id,)
        ).fetchone()
        if asset_row and asset_row[0] and os.path.exists(asset_row[0]):
            source_local_path = asset_row[0]
        base_domain = _get_setting(conn, "system_domain", "https://shouhu.asia")
        source_serve_url = f"{base_domain}/api/assets/serve/{req.source_asset_id}/file"

    # 创建任务记录
    task_id = str(uuid.uuid4()).replace("-", "")[:16]
    conn.execute("""
        INSERT INTO creative_tasks
        (task_id, provider, gen_mode, recipe_id, source_asset_id, prompt, num_images, status, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,'pending',datetime('now'),datetime('now'))
    """, (task_id, provider, req.gen_mode, req.recipe_id or None, req.source_asset_id, prompt, num_images))
    conn.commit()
    conn.close()

    background_tasks.add_task(
        _run_generation_task,
        task_id=task_id,
        provider=provider,
        api_key=api_key,
        prompt=prompt,
        gen_mode=req.gen_mode,
        num_images=num_images,
        aspect_ratio=req.aspect_ratio,
        source_asset_id=req.source_asset_id,
        source_asset_local_path=source_local_path,
        source_asset_serve_url=source_serve_url,
        target_countries=req.target_countries,
        extra_params=extra_params,
        recipe_id=req.recipe_id or None,
    )

    recipe_name = RECIPE_MAP.get(req.recipe_id, {}).get("name", "") if req.recipe_id else ""
    return {
        "task_id": task_id,
        "status": "pending",
        "prompt": prompt,
        "provider": provider,
        "recipe_id": req.recipe_id or None,
        "recipe_name": recipe_name,
        "num_images": num_images,
        "message": f"生成任务已提交，正在后台处理 {num_images} 张图片" + (f"（配方：{recipe_name}）" if recipe_name else ""),
    }


@router.get("/task/{task_id}")
def get_task_status(task_id: str, user=Depends(get_current_user)):
    """查询生成任务状态"""
    conn = get_conn()
    row = conn.execute("SELECT * FROM creative_tasks WHERE task_id=?", (task_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "任务不存在")
    task = dict(row)
    pending_count = conn.execute(
        "SELECT COUNT(*) FROM creative_pending WHERE task_id=? AND status='pending'", (task_id,)
    ).fetchone()[0]
    task["pending_count"] = pending_count
    conn.close()
    return task


@router.get("/pending")
def list_pending(
    status: str = "pending",
    page: int = 1,
    page_size: int = 20,
    user=Depends(get_current_user),
):
    """获取待审核素材列表"""
    conn = get_conn()
    offset = (page - 1) * page_size
    rows = conn.execute("""
        SELECT cp.*, aa.file_name as source_file_name, aa.score as source_score
        FROM creative_pending cp
        LEFT JOIN ad_assets aa ON cp.source_asset_id = aa.id
        WHERE cp.status=?
        ORDER BY cp.created_at DESC
        LIMIT ? OFFSET ?
    """, (status, page_size, offset)).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM creative_pending WHERE status=?", (status,)).fetchone()[0]
    conn.close()
    items = []
    for row in rows:
        d = dict(row)
        d.pop("b64_preview", None)
        items.append(d)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/pending/{pending_id}/image")
def get_pending_image(
    pending_id: int,
    token: str = None,
    user=None,
    request: Request = None,
):
    """获取待审核素材图片（流式返回）"""
    from core.auth import decode_token
    auth_token = None
    if request:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            auth_token = auth_header[7:]
    if not auth_token and token:
        auth_token = token
    if not auth_token:
        raise HTTPException(401, "未授权")
    try:
        decode_token(auth_token)
    except Exception:
        raise HTTPException(401, "Token 无效或已过期")

    conn = get_conn()
    row = conn.execute("SELECT local_path, remote_url FROM creative_pending WHERE id=?", (pending_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "待审核素材不存在")

    local_path = row[0]
    if local_path and os.path.exists(local_path):
        ext = os.path.splitext(local_path)[1].lower().lstrip(".")
        mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
                "webp": "image/webp", "gif": "image/gif"}.get(ext, "image/jpeg")
        def iter_file():
            with open(local_path, "rb") as f:
                yield from f
        return StreamingResponse(iter_file(), media_type=mime)

    remote_url = row[1]
    if remote_url:
        try:
            resp = requests.get(remote_url, timeout=30, stream=True)
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "image/jpeg")
            return StreamingResponse(resp.iter_content(chunk_size=8192), media_type=ct)
        except Exception as e:
            raise HTTPException(502, f"远程图片获取失败: {e}")

    raise HTTPException(404, "图片文件不存在")


@router.post("/pending/{pending_id}/approve")
def approve_pending(
    pending_id: int,
    req: ApproveRequest,
    user=Depends(get_current_user),
):
    """审核通过，将待审核素材纳入正式素材库"""
    conn = get_conn()
    row = conn.execute("SELECT * FROM creative_pending WHERE id=? AND status='pending'", (pending_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "待审核素材不存在或已处理")

    pending = dict(row)
    local_path = pending.get("local_path", "")
    if not local_path or not os.path.exists(local_path):
        conn.close()
        raise HTTPException(400, "图片文件不存在，无法纳入素材库")

    filename = os.path.basename(local_path)
    ext = os.path.splitext(filename)[1] or ".jpg"
    new_filename = f"gen_{int(time.time())}_{pending_id}{ext}"
    new_path = os.path.join(ASSET_DIR, new_filename)
    os.rename(local_path, new_path)

    with open(new_path, "rb") as f:
        file_data = f.read()
    file_hash = hashlib.md5(file_data).hexdigest()
    file_size = len(file_data)

    countries = req.target_countries
    if not countries and pending.get("target_countries"):
        countries = [c.strip() for c in pending["target_countries"].split(",") if c.strip()]

    recipe_id = pending.get("recipe_id", "")
    recipe_name = RECIPE_MAP.get(recipe_id, {}).get("name", "") if recipe_id else ""
    note = req.note or f"AI 生成素材（{pending['provider']}）"
    if recipe_name:
        note += f" 配方：{recipe_name}"
    if pending.get("prompt"):
        note += f"\nPrompt: {pending['prompt'][:200]}"

    # 生成缩略图
    thumb_path = None
    try:
        from PIL import Image
        import uuid as _uuid
        _thumb_dir = os.path.join(ASSET_DIR, "thumbs")
        os.makedirs(_thumb_dir, exist_ok=True)
        img = Image.open(new_path)
        img.thumbnail((400, 400))
        _thumb_name = _uuid.uuid4().hex + ".jpg"
        thumb_path = os.path.join(_thumb_dir, _thumb_name)
        img.convert("RGB").save(thumb_path, "JPEG", quality=85)
    except Exception as _te:
        logger.warning(f"approve: 生成缩略图失败: {_te}")
        thumb_path = None
    conn.execute("""
        INSERT INTO ad_assets
        (file_name, file_path, file_size, file_hash, upload_status,
         thumb_path, note, target_countries, ai_purpose, ai_language, created_at, updated_at)
        VALUES (?,?,?,?,'ai_pending',?,?,?,?,?,datetime('now'),datetime('now'))
    """, (
        new_filename, new_path, file_size, file_hash,
        thumb_path,
        note,
        ",".join(countries) if countries else "",
        req.ai_purpose or pending.get("gen_mode", ""),
        req.ai_language or "",
    ))
    new_asset_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute("""
        UPDATE creative_pending
        SET status='approved', approved_asset_id=?, updated_at=datetime('now')
        WHERE id=?
    """, (new_asset_id, pending_id))
    conn.commit()
    conn.close()

    # 触发 AI 分析（异步后台线程）
    try:
        from api.assets import _ai_analyze_asset
        import threading as _threading
        _threading.Thread(
            target=_ai_analyze_asset,
            args=(new_asset_id,),
            kwargs={"purpose": req.ai_purpose or "stock_ad"},
            daemon=True
        ).start()
    except Exception as _e:
        logger.warning(f"approve: 启动 AI 分析线程失败: {_e}")

    # 触发实时智能评分（AI 分析完成后自动评级，后台异步执行）
    try:
        from services.smart_scorer import score_asset_after_approve
        score_asset_after_approve(new_asset_id)
    except Exception as _se:
        logger.warning(f"approve: 触发实时评分失败（不影响入库）: {_se}")

    return {
        "success": True,
        "asset_id": new_asset_id,
        "file_name": new_filename,
        "message": f"素材已纳入素材库（ID: {new_asset_id}），AI 分析已自动启动",
    }


@router.delete("/pending/{pending_id}")
def delete_pending(
    pending_id: int,
    user=Depends(get_current_user),
):
    """永久删除已拒绝的素材（删除数据库记录 + 清理文件）"""
    conn = get_conn()
    row = conn.execute("SELECT local_path, status FROM creative_pending WHERE id=?", (pending_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "素材不存在")
    status, local_path = row[1], row[0]
    # 只允许删除已拒绝的素材（pending/approved 不允许直接删除）
    if status == "approved":
        conn.close()
        raise HTTPException(400, "已通过的素材请在素材库中删除")
    # 清理本地文件
    if local_path and os.path.exists(local_path):
        try:
            os.remove(local_path)
        except Exception:
            pass
    conn.execute("DELETE FROM creative_pending WHERE id=?", (pending_id,))
    conn.commit()
    conn.close()
    return {"success": True, "message": "素材已永久删除"}

@router.post("/pending/{pending_id}/reject")
def reject_pending(
    pending_id: int,
    req: RejectRequest,
    user=Depends(get_current_user),
):
    """拒绝待审核素材"""
    conn = get_conn()
    row = conn.execute("SELECT local_path FROM creative_pending WHERE id=? AND status='pending'", (pending_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "待审核素材不存在或已处理")

    local_path = row[0]
    if local_path and os.path.exists(local_path):
        try:
            import shutil as _shutil
            rejected_path = os.path.join(REJECTED_DIR, os.path.basename(local_path))
            _shutil.move(local_path, rejected_path)
            local_path = rejected_path
        except Exception:
            pass

    conn.execute("""
        UPDATE creative_pending
        SET status='rejected', reject_reason=?, local_path=?, updated_at=datetime('now')
        WHERE id=?
    """, (req.reason or "用户拒绝", local_path, pending_id))
    conn.commit()
    conn.close()
    return {"success": True, "message": "已拒绝（文件保留7天后自动清理）"}


@router.delete("/pending/batch-reject")
def batch_reject(ids: List[int], user=Depends(get_current_user)):
    """批量拒绝"""
    conn = get_conn()
    import shutil as _shutil
    for pid in ids:
        row = conn.execute("SELECT local_path FROM creative_pending WHERE id=?", (pid,)).fetchone()
        new_local_path = None
        if row and row[0] and os.path.exists(row[0]):
            try:
                rejected_path = os.path.join(REJECTED_DIR, os.path.basename(row[0]))
                _shutil.move(row[0], rejected_path)
                new_local_path = rejected_path
            except Exception:
                pass
        if new_local_path:
            conn.execute("UPDATE creative_pending SET status='rejected', local_path=?, updated_at=datetime('now') WHERE id=?", (new_local_path, pid))
        else:
            conn.execute("UPDATE creative_pending SET status='rejected', updated_at=datetime('now') WHERE id=?", (pid,))
    conn.commit()
    conn.close()
    return {"success": True, "rejected": len(ids)}


# ─────────────────────────────────────────────────────────────────────────────
# 自定义配方 CRUD
# ─────────────────────────────────────────────────────────────────────────────
import json as _json
import uuid as _uuid

class CustomRecipeCreate(BaseModel):
    name: str
    emoji: str = "🎨"
    category: str = "自定义"
    desc: str = ""
    prompt_template: str
    default_provider: str = "ideogram"
    fallback_provider: str = "openai"
    allow_text: bool = False
    best_for: List[str] = ["traffic", "lead"]
    extra_params: dict = {}

class CustomRecipeUpdate(BaseModel):
    name: Optional[str] = None
    emoji: Optional[str] = None
    category: Optional[str] = None
    desc: Optional[str] = None
    prompt_template: Optional[str] = None
    default_provider: Optional[str] = None
    fallback_provider: Optional[str] = None
    allow_text: Optional[bool] = None
    best_for: Optional[List[str]] = None
    extra_params: Optional[dict] = None


def _db_row_to_recipe(row) -> dict:
    """将数据库行转换为配方字典格式"""
    return {
        "id": row[1],  # recipe_id
        "db_id": row[0],
        "name": row[2],
        "emoji": row[3],
        "category": row[4],
        "desc": row[5],
        "prompt_template": row[6],
        "default_provider": row[7],
        "fallback_provider": row[8],
        "allow_text": bool(row[9]),
        "best_for": _json.loads(row[10]) if row[10] else ["traffic", "lead"],
        "extra_params": _json.loads(row[11]) if row[11] else {},
        "created_at": row[12],
        "updated_at": row[13],
        "is_custom": True,
    }


@router.get("/custom-recipes")
def list_custom_recipes(user=Depends(get_current_user)):
    """获取所有自定义配方"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, recipe_id, name, emoji, category, desc, prompt_template, "
        "default_provider, fallback_provider, allow_text, best_for, extra_params, "
        "created_at, updated_at FROM custom_recipes ORDER BY id DESC"
    ).fetchall()
    conn.close()
    return [_db_row_to_recipe(r) for r in rows]


@router.post("/custom-recipes")
def create_custom_recipe(req: CustomRecipeCreate, user=Depends(get_current_user)):
    """创建自定义配方"""
    recipe_id = "custom_" + _uuid.uuid4().hex[:8]
    conn = get_conn()
    try:
        conn.execute(
            """INSERT INTO custom_recipes
               (recipe_id, name, emoji, category, desc, prompt_template,
                default_provider, fallback_provider, allow_text, best_for, extra_params)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                recipe_id, req.name, req.emoji, req.category, req.desc,
                req.prompt_template, req.default_provider, req.fallback_provider,
                1 if req.allow_text else 0,
                _json.dumps(req.best_for, ensure_ascii=False),
                _json.dumps(req.extra_params, ensure_ascii=False),
            )
        )
        conn.commit()
        row = conn.execute(
            "SELECT id, recipe_id, name, emoji, category, desc, prompt_template, "
            "default_provider, fallback_provider, allow_text, best_for, extra_params, "
            "created_at, updated_at FROM custom_recipes WHERE recipe_id=?", (recipe_id,)
        ).fetchone()
        conn.close()
        return _db_row_to_recipe(row)
    except Exception as e:
        conn.close()
        raise HTTPException(500, str(e))


@router.put("/custom-recipes/{recipe_id}")
def update_custom_recipe(recipe_id: str, req: CustomRecipeUpdate, user=Depends(get_current_user)):
    """更新自定义配方"""
    conn = get_conn()
    row = conn.execute("SELECT id FROM custom_recipes WHERE recipe_id=?", (recipe_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "配方不存在")
    updates = []
    params = []
    if req.name is not None:
        updates.append("name=?"); params.append(req.name)
    if req.emoji is not None:
        updates.append("emoji=?"); params.append(req.emoji)
    if req.category is not None:
        updates.append("category=?"); params.append(req.category)
    if req.desc is not None:
        updates.append("desc=?"); params.append(req.desc)
    if req.prompt_template is not None:
        updates.append("prompt_template=?"); params.append(req.prompt_template)
    if req.default_provider is not None:
        updates.append("default_provider=?"); params.append(req.default_provider)
    if req.fallback_provider is not None:
        updates.append("fallback_provider=?"); params.append(req.fallback_provider)
    if req.allow_text is not None:
        updates.append("allow_text=?"); params.append(1 if req.allow_text else 0)
    if req.best_for is not None:
        updates.append("best_for=?"); params.append(_json.dumps(req.best_for, ensure_ascii=False))
    if req.extra_params is not None:
        updates.append("extra_params=?"); params.append(_json.dumps(req.extra_params, ensure_ascii=False))
    if updates:
        updates.append("updated_at=datetime('now')")
        params.append(recipe_id)
        conn.execute(f"UPDATE custom_recipes SET {', '.join(updates)} WHERE recipe_id=?", params)
        conn.commit()
    updated = conn.execute(
        "SELECT id, recipe_id, name, emoji, category, desc, prompt_template, "
        "default_provider, fallback_provider, allow_text, best_for, extra_params, "
        "created_at, updated_at FROM custom_recipes WHERE recipe_id=?", (recipe_id,)
    ).fetchone()
    conn.close()
    return _db_row_to_recipe(updated)


@router.delete("/custom-recipes/{recipe_id}")
def delete_custom_recipe(recipe_id: str, user=Depends(get_current_user)):
    """删除自定义配方"""
    conn = get_conn()
    row = conn.execute("SELECT id FROM custom_recipes WHERE recipe_id=?", (recipe_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "配方不存在")
    conn.execute("DELETE FROM custom_recipes WHERE recipe_id=?", (recipe_id,))
    conn.commit()
    conn.close()
    return {"success": True, "message": "配方已删除"}
