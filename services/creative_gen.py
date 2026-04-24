"""
creative_gen.py  —  素材自动化生成 API
路由前缀: /api/creative-gen

功能:
  GET  /providers          — 获取支持的厂商列表
  POST /generate           — 发起生成任务（文生图 / 爆款裂变）
  GET  /pending            — 获取待审核素材列表
  POST /pending/{id}/approve — 审核通过，纳入素材库
  POST /pending/{id}/reject  — 拒绝，删除待审核记录
  GET  /pending/{id}/image   — 获取待审核素材图片（代理返回）
  GET  /settings-keys      — 获取当前已配置的图像生成 API Key 状态
"""

import os
import base64
import hashlib
import logging
import time
import uuid
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import requests

from core.auth import get_current_user
from core.database import get_conn

logger = logging.getLogger("mira.creative_gen")

router = APIRouter()

ASSET_DIR = os.environ.get("MIRA_ASSET_DIR", "/opt/mira/assets")
PENDING_DIR = os.path.join(ASSET_DIR, "pending_review")
os.makedirs(PENDING_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# 数据库初始化（幂等）
# ─────────────────────────────────────────────────────────────────────────────

def init_creative_gen_tables():
    """创建 creative_gen 相关数据库表（幂等）"""
    conn = get_conn()
    # 待审核素材池
    conn.execute("""
        CREATE TABLE IF NOT EXISTS creative_pending (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id         TEXT NOT NULL,
            provider        TEXT NOT NULL,
            model           TEXT NOT NULL,
            prompt          TEXT,
            source_asset_id INTEGER,
            gen_mode        TEXT DEFAULT 'txt2img',
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
            created_at      TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at      TEXT DEFAULT (datetime('now','+8 hours'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_status ON creative_pending(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_task ON creative_pending(task_id)")

    # 生成任务记录
    conn.execute("""
        CREATE TABLE IF NOT EXISTS creative_tasks (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id         TEXT UNIQUE NOT NULL,
            provider        TEXT NOT NULL,
            gen_mode        TEXT DEFAULT 'txt2img',
            task_type       TEXT DEFAULT '',
            source_asset_id INTEGER,
            prompt          TEXT,
            num_images      INTEGER DEFAULT 1,
            status          TEXT DEFAULT 'running',
            total_cost_usd  REAL DEFAULT 0,
            error_msg       TEXT,
            created_by      TEXT DEFAULT 'user',
            created_at      TEXT DEFAULT (datetime('now','+8 hours')),
            updated_at      TEXT DEFAULT (datetime('now','+8 hours'))
        )
    """)

    # settings 新增图像生成 API Key 配置项（幂等）
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

    conn.commit()
    conn.close()
    logger.info("[creative_gen] 数据库表初始化完成")


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _get_setting(conn, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return (row[0] or default) if row else default


def _get_api_key(conn, provider: str) -> str:
    """从 settings 表获取指定厂商的 API Key"""
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


def _get_asset_public_url(asset_id: int, conn) -> str:
    """获取素材的可公开访问 URL（用于 fal_kontext 的 image_url 参数）"""
    row = conn.execute(
        "SELECT file_path, file_name FROM ad_assets WHERE id=?", (asset_id,)
    ).fetchone()
    if not row:
        raise HTTPException(404, f"素材 {asset_id} 不存在")
    # 返回系统内部 serve URL，需要在调用前转换为公开 URL
    return f"/api/assets/serve/{asset_id}/file"


def _build_prompt_for_variation(source_asset: dict, variation_direction: str, audience_desc: str) -> str:
    """根据源素材信息和变体方向构建 Prompt"""
    ai_analysis = source_asset.get("ai_analysis", "") or ""
    ai_audience = source_asset.get("ai_audience_note", "") or ""
    ai_purpose = source_asset.get("ai_purpose", "") or ""

    base_context = ""
    if ai_analysis:
        base_context = f"原素材描述: {ai_analysis[:200]}. "
    if ai_audience:
        base_context += f"目标受众: {ai_audience[:100]}. "

    direction_prompts = {
        "background": "Change only the background to a different environment, keep the person/subject identical",
        "expression": "Change the facial expression and pose slightly, keep the same person and setting",
        "lighting": "Change the lighting atmosphere (warm golden hour / cool blue tone / dramatic studio light), keep everything else",
        "outfit": "Change the outfit/clothing style slightly, keep the same person and background",
        "scene": "Change to a completely different scene/location while keeping the same person",
        "style": "Apply a slightly different visual style (more cinematic/more natural/more vibrant), keep the subject",
    }

    direction_en = direction_prompts.get(variation_direction, "Create a variation of this image")

    prompt_parts = [direction_en]
    if audience_desc:
        prompt_parts.append(f"Target audience: {audience_desc}")
    if ai_purpose:
        prompt_parts.append(f"Ad purpose: {ai_purpose}")
    prompt_parts.append("High quality, photorealistic, suitable for Facebook/Instagram advertising")

    return ". ".join(prompt_parts)


def _build_prompt_for_new(
    audience_desc: str,
    ad_type: str,
    style: str,
    extra_desc: str,
    target_countries: list,
) -> str:
    """为全新生成构建 Prompt（含多样性随机元素，避免素材重复）"""
    import random as _random
    import time as _time

    # 每种广告类型提供多个不同场景描述，随机选择
    ad_type_prompts = {
        "messaging": [
            "A warm lifestyle photo of a person happily reading a message on their smartphone, smiling naturally",
            "A close-up of hands holding a phone with a glowing chat interface, cozy home background",
            "Two friends laughing together while looking at a phone screen, bright outdoor setting",
            "A young professional excitedly reacting to a notification on their mobile device",
            "A candid moment of someone typing a message on their phone, coffee shop atmosphere",
        ],
        "lead": [
            "A confident business professional reviewing documents at a modern desk, bright office",
            "A person filling out a digital form on a tablet, clean minimalist background",
            "A satisfied customer testimonial scene with a quote overlay, warm tones",
            "A split-screen showing a problem on the left and a happy solution on the right",
            "A professional woman in a modern workspace looking at a laptop with a satisfied expression",
        ],
        "purchase": [
            "A premium product flat-lay on white marble surface with elegant natural shadows",
            "A happy person unboxing a product with excitement, bright clean background",
            "A before-and-after comparison showing product transformation results",
            "A close-up product detail shot with bokeh background and premium lighting",
            "A lifestyle scene showing the product being used in a real-world setting",
        ],
        "traffic": [
            "A curious person looking at a glowing screen with an intrigued expression, dark background",
            "A bold eye-catching graphic with a bright arrow pointing to a discovery",
            "A dynamic scene suggesting speed and excitement with motion blur elements",
            "A mysterious teaser image that sparks curiosity and drives click-through",
            "A vibrant scene of someone discovering something amazing on their device",
        ],
        "engagement": [
            "A diverse group of happy people laughing and reacting to something on a phone",
            "A relatable everyday moment that sparks recognition and emotional connection",
            "A vibrant community gathering scene with people interacting and smiling",
            "A bold question graphic with colorful interactive poll-style elements",
            "A heartwarming family moment that evokes positive emotions and sharing",
        ],
    }

    # 每种风格提供多个变体
    style_prompts = {
        "photo": [
            "photorealistic DSLR photography, shallow depth of field, natural golden hour lighting",
            "photorealistic, shot on Canon EOS R5, cool blue tones, crisp studio lighting",
            "photorealistic, outdoor candid photography, bright airy natural light",
            "photorealistic, warm indoor lighting, intimate and authentic atmosphere",
        ],
        "illustration": [
            "clean flat digital illustration, bold primary colors, modern minimalist style",
            "vibrant vector illustration, geometric shapes, contemporary design aesthetic",
            "hand-drawn illustration style, warm pastel colors, friendly and approachable",
            "clean digital illustration, gradient purple-blue colors, modern tech aesthetic",
        ],
        "screenshot": [
            "realistic app screenshot mockup on iPhone 15 Pro, clean modern UI",
            "authentic-looking social media testimonial screenshot, credible and real",
            "before-and-after results screenshot with highlighted metrics",
            "app interface screenshot with glowing feature highlights and annotations",
        ],
        "infographic": [
            "clean minimal infographic, bold typography, white background, blue accent colors",
            "data visualization infographic, colorful bar charts, modern sans-serif font",
            "step-by-step numbered process infographic, clean icons, green and white",
            "comparison infographic, two-column layout, contrasting red and green colors",
        ],
        "lifestyle": [
            "authentic lifestyle photography, natural candid moment, warm golden tones",
            "lifestyle photo, diverse multicultural people, bright outdoor urban setting",
            "cozy indoor lifestyle scene, soft window light, relaxed and comfortable mood",
            "active energetic lifestyle photography, vibrant saturated colors, motion",
        ],
    }

    # 随机场景修饰词
    scene_modifiers = [
        "modern urban setting", "cozy home environment", "bright outdoor scene",
        "minimalist studio background", "lush green nature backdrop",
        "bustling city cafe", "serene beach setting", "contemporary office space",
        "vibrant market atmosphere", "peaceful suburban neighborhood",
        "rooftop city view", "warm kitchen interior", "modern gym setting",
    ]

    # 随机构图视角
    composition_hints = [
        "rule of thirds composition", "centered symmetrical composition",
        "dynamic diagonal composition", "close-up portrait framing",
        "wide establishing shot", "overhead flat lay perspective",
        "low angle heroic perspective", "eye-level candid framing",
        "over-the-shoulder perspective", "split-screen dual composition",
    ]

    # 随机色调
    color_moods = [
        "warm golden amber tones", "cool crisp blue and white palette",
        "vibrant saturated colors", "soft pastel color scheme",
        "bold high-contrast colors", "earthy natural organic tones",
        "clean monochromatic with orange accent", "rich deep jewel tones",
        "bright fresh spring colors", "moody cinematic dark tones",
    ]

    # 使用时间戳作为随机种子，确保每次生成不同结果
    seed = int(_time.time() * 1000) % 99999
    rng = _random.Random(seed)

    ad_options = ad_type_prompts.get(ad_type, [
        "A high-converting Facebook advertisement image with strong visual appeal",
        "An engaging social media ad designed to capture attention and drive action",
        "A compelling advertisement image with clear message and professional quality",
    ])
    style_options = style_prompts.get(style, [
        "photorealistic, professional quality photography",
        "high quality digital photography, sharp and vibrant",
    ])

    chosen_ad = rng.choice(ad_options)
    chosen_style = rng.choice(style_options)
    chosen_scene = rng.choice(scene_modifiers)
    chosen_comp = rng.choice(composition_hints)
    chosen_color = rng.choice(color_moods)

    country_context = ""
    if target_countries:
        country_context = f"Localized for {', '.join(target_countries)} market"

    parts = [
        chosen_ad,
        chosen_style,
        chosen_scene,
        chosen_comp,
        chosen_color,
        f"Target audience: {audience_desc}" if audience_desc else "",
        country_context,
        extra_desc if extra_desc else "",
        "High quality, professional, suitable for Facebook/Instagram advertising, no watermarks, no text overlay",
    ]

    return ". ".join(p for p in parts if p)


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

        # 调用生成
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
        saved_count = 0
        failed_count = 0
        fail_reasons = []
        for i, result in enumerate(results):
            # 保存图片到本地
            filename = f"pending_{task_id}_{i}_{int(time.time())}.jpg"
            local_path = os.path.join(PENDING_DIR, filename)

            if result.get("b64"):
                # base64 → 文件
                try:
                    with open(local_path, "wb") as f:
                        f.write(base64.b64decode(result["b64"]))
                    remote_url = None
                except Exception as e:
                    logger.error(f"[creative_gen] base64 解码失败 ({i+1}/{len(results)}): {e}")
                    failed_count += 1
                    fail_reasons.append(f"图片{i+1} base64解码失败: {str(e)[:80]}")
                    continue
            elif result.get("url"):
                # 下载远程 URL
                try:
                    resp = requests.get(result["url"], timeout=60)
                    resp.raise_for_status()
                    with open(local_path, "wb") as f:
                        f.write(resp.content)
                    remote_url = result["url"]
                except Exception as e:
                    logger.error(f"[creative_gen] 下载图片失败 ({i+1}/{len(results)}): {e}")
                    failed_count += 1
                    fail_reasons.append(f"图片{i+1} 下载失败: {str(e)[:80]}")
                    continue
            elif result.get("error"):
                # 生成失败（厂商返回错误）
                logger.error(f"[creative_gen] 图片生成失败 ({i+1}/{len(results)}): {result['error']}")
                failed_count += 1
                fail_reasons.append(f"图片{i+1} 生成失败: {str(result['error'])[:80]}")
                continue
            else:
                logger.warning(f"[creative_gen] 图片 {i+1}/{len(results)} 无有效数据（无 b64/url/error）")
                failed_count += 1
                fail_reasons.append(f"图片{i+1} 无有效数据")
                continue

            total_cost += result.get("cost_usd", 0)
            saved_count += 1

            # 写入待审核池
            conn.execute("""
                INSERT INTO creative_pending
                (task_id, provider, model, prompt, source_asset_id, gen_mode,
                 local_path, remote_url, aspect_ratio, cost_usd, status,
                 target_countries, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,'pending',?,datetime('now','+8 hours'),datetime('now','+8 hours'))
            """, (
                task_id, provider, result.get("model", provider), prompt,
                source_asset_id, gen_mode, local_path, remote_url,
                aspect_ratio, result.get("cost_usd", 0),
                ",".join(target_countries) if target_countries else "",
            ))

        # 根据实际生成数量决定任务状态
        if saved_count == 0:
            _err_detail = "; ".join(fail_reasons[:3]) if fail_reasons else "所有图片生成均失败"
            conn.execute(
                "UPDATE creative_tasks SET status='failed', error_msg=?, total_cost_usd=?, updated_at=? WHERE task_id=?",
                (f"全部 {num_images} 张图片生成失败。{_err_detail}"[:500],
                 total_cost, datetime.now().isoformat(), task_id)
            )
        elif failed_count > 0:
            _partial_msg = f"成功 {saved_count}/{num_images} 张，失败 {failed_count} 张"
            conn.execute(
                "UPDATE creative_tasks SET status='done', error_msg=?, total_cost_usd=?, updated_at=? WHERE task_id=?",
                (_partial_msg[:500], total_cost, datetime.now().isoformat(), task_id)
            )
        else:
            conn.execute(
                "UPDATE creative_tasks SET status='done', total_cost_usd=?, updated_at=? WHERE task_id=?",
                (total_cost, datetime.now().isoformat(), task_id)
            )
        conn.commit()
        logger.info(f"[creative_gen] 任务 {task_id} 完成，成功 {saved_count}/{num_images} 张，花费 ${total_cost:.4f}")

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
    provider: str                           # 厂商 ID
    gen_mode: str = "txt2img"               # txt2img / img2img（裂变）
    num_images: int = 4                     # 生成数量（1-8）
    aspect_ratio: str = "1:1"              # 宽高比
    target_countries: List[str] = []        # 目标国家

    # 文生图参数
    audience_desc: str = ""                 # 目标人群描述
    ad_type: str = "messaging"              # 广告类型
    style: str = "photo"                    # 风格
    extra_desc: str = ""                    # 额外描述
    custom_prompt: str = ""                 # 自定义 Prompt（覆盖自动生成）

    # 图生图（裂变）参数
    source_asset_id: Optional[int] = None  # 源素材 ID
    variation_direction: str = "background" # 变体方向

    # 厂商特定参数
    extra_params: dict = {}


class ApproveRequest(BaseModel):
    target_countries: List[str] = []
    note: str = ""
    ai_purpose: str = ""
    ai_language: str = ""


class RejectRequest(BaseModel):
    reason: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# API 路由
# ─────────────────────────────────────────────────────────────────────────────

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


@router.post("/generate")
async def start_generate(
    req: GenerateRequest,
    background_tasks: BackgroundTasks,
    user=Depends(get_current_user),
):
    """发起素材生成任务（异步后台执行）"""
    conn = get_conn()

    # 参数校验
    num_images = max(1, min(8, req.num_images))
    if req.gen_mode == "img2img" and not req.source_asset_id:
        conn.close()
        raise HTTPException(400, "img2img 模式需要指定 source_asset_id")

    # 获取 API Key
    api_key = _get_api_key(conn, req.provider)

    # 构建 Prompt
    if req.custom_prompt:
        prompt = req.custom_prompt
    elif req.gen_mode == "img2img" and req.source_asset_id:
        # 获取源素材信息
        row = conn.execute(
            "SELECT id, file_name, ai_analysis, ai_audience_note, ai_purpose FROM ad_assets WHERE id=?",
            (req.source_asset_id,)
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, f"源素材 {req.source_asset_id} 不存在")
        source_asset = dict(row)
        prompt = _build_prompt_for_variation(
            source_asset, req.variation_direction, req.audience_desc
        )
    else:
        prompt = _build_prompt_for_new(
            req.audience_desc, req.ad_type, req.style,
            req.extra_desc, req.target_countries
        )

    # 获取源素材本地路径（用于 openai/ideogram 图生图）
    source_local_path = None
    source_serve_url = None
    if req.source_asset_id:
        asset_row = conn.execute(
            "SELECT file_path FROM ad_assets WHERE id=?", (req.source_asset_id,)
        ).fetchone()
        if asset_row and asset_row[0] and os.path.exists(asset_row[0]):
            source_local_path = asset_row[0]
        # fal_kontext 需要公开 URL，使用系统配置的域名
        base_domain = _get_setting(conn, "system_domain", "https://shouhu.asia")
        source_serve_url = f"{base_domain}/api/assets/serve/{req.source_asset_id}/file"

    # 创建任务记录
    task_id = str(uuid.uuid4()).replace("-", "")[:16]
    conn.execute("""
        INSERT INTO creative_tasks
        (task_id, provider, gen_mode, source_asset_id, prompt, num_images, status, created_at, updated_at)
        VALUES (?,?,?,?,?,?,'pending',datetime('now','+8 hours'),datetime('now','+8 hours'))
    """, (task_id, req.provider, req.gen_mode, req.source_asset_id, prompt, num_images))
    conn.commit()
    conn.close()

    # 后台执行
    background_tasks.add_task(
        _run_generation_task,
        task_id=task_id,
        provider=req.provider,
        api_key=api_key,
        prompt=prompt,
        gen_mode=req.gen_mode,
        num_images=num_images,
        aspect_ratio=req.aspect_ratio,
        source_asset_id=req.source_asset_id,
        source_asset_local_path=source_local_path,
        source_asset_serve_url=source_serve_url,
        target_countries=req.target_countries,
        extra_params=req.extra_params,
    )

    return {
        "task_id": task_id,
        "status": "pending",
        "prompt": prompt,
        "provider": req.provider,
        "num_images": num_images,
        "message": f"生成任务已提交，正在后台处理 {num_images} 张图片",
    }


@router.get("/task/{task_id}")
def get_task_status(task_id: str, user=Depends(get_current_user)):
    """查询生成任务状态"""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM creative_tasks WHERE task_id=?", (task_id,)
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "任务不存在")
    task = dict(row)

    # 查询已生成的待审核数量
    pending_count = conn.execute(
        "SELECT COUNT(*) FROM creative_pending WHERE task_id=? AND status='pending'",
        (task_id,)
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

    total = conn.execute(
        "SELECT COUNT(*) FROM creative_pending WHERE status=?", (status,)
    ).fetchone()[0]
    conn.close()

    items = []
    for row in rows:
        d = dict(row)
        # 不返回 b64_preview（太大），前端通过 /pending/{id}/image 获取
        d.pop("b64_preview", None)
        items.append(d)

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/pending/{pending_id}/image")
def get_pending_image(pending_id: int, user=Depends(get_current_user)):
    """获取待审核素材图片（流式返回本地文件）"""
    conn = get_conn()
    row = conn.execute(
        "SELECT local_path, remote_url FROM creative_pending WHERE id=?", (pending_id,)
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "待审核素材不存在")

    local_path = row[0]
    if local_path and os.path.exists(local_path):
        def iter_file():
            with open(local_path, "rb") as f:
                yield from f
        return StreamingResponse(iter_file(), media_type="image/jpeg")

    # 如果本地文件不存在但有远程 URL，代理返回
    remote_url = row[1]
    if remote_url:
        resp = requests.get(remote_url, timeout=30, stream=True)
        return StreamingResponse(resp.iter_content(chunk_size=8192), media_type="image/jpeg")

    raise HTTPException(404, "图片文件不存在")


@router.post("/pending/{pending_id}/approve")
def approve_pending(
    pending_id: int,
    req: ApproveRequest,
    user=Depends(get_current_user),
):
    """审核通过，将待审核素材纳入正式素材库"""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM creative_pending WHERE id=? AND status='pending'", (pending_id,)
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "待审核素材不存在或已处理")

    pending = dict(row)
    local_path = pending.get("local_path", "")

    if not local_path or not os.path.exists(local_path):
        conn.close()
        raise HTTPException(400, "图片文件不存在，无法纳入素材库")

    # 将文件从 pending_review 目录移动到正式 assets 目录
    filename = os.path.basename(local_path)
    # 重命名为更规范的格式
    ext = os.path.splitext(filename)[1] or ".jpg"
    new_filename = f"gen_{int(time.time())}_{pending_id}{ext}"
    new_path = os.path.join(ASSET_DIR, new_filename)
    os.rename(local_path, new_path)

    # 计算文件 hash
    with open(new_path, "rb") as f:
        file_data = f.read()
    file_hash = hashlib.md5(file_data).hexdigest()
    file_size = len(file_data)

    # 确定目标国家
    countries = req.target_countries
    if not countries and pending.get("target_countries"):
        countries = [c.strip() for c in pending["target_countries"].split(",") if c.strip()]

    # 写入 ad_assets 表
    note = req.note or f"AI 生成素材（{pending['provider']}）"
    if pending.get("prompt"):
        note += f"\nPrompt: {pending['prompt'][:200]}"

    conn.execute("""
        INSERT INTO ad_assets
        (file_name, file_path, file_size, file_hash, upload_status,
         note, target_countries, ai_purpose, ai_language, created_at, updated_at)
        VALUES (?,?,?,?,'ai_pending',?,?,?,?,datetime('now','+8 hours'),datetime('now','+8 hours'))
    """, (
        new_filename, new_path, file_size, file_hash,
        note,
        ",".join(countries) if countries else "",
        req.ai_purpose or pending.get("gen_mode", ""),
        req.ai_language or "",
    ))
    new_asset_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # 更新待审核记录状态
    conn.execute("""
        UPDATE creative_pending
        SET status='approved', approved_asset_id=?, updated_at=datetime('now')
        WHERE id=?
    """, (new_asset_id, pending_id))

    conn.commit()
    conn.close()

    return {
        "success": True,
        "asset_id": new_asset_id,
        "file_name": new_filename,
        "message": f"素材已纳入素材库（ID: {new_asset_id}），正在触发 AI 分析...",
    }


@router.post("/pending/{pending_id}/reject")
def reject_pending(
    pending_id: int,
    req: RejectRequest,
    user=Depends(get_current_user),
):
    """拒绝待审核素材"""
    conn = get_conn()
    row = conn.execute(
        "SELECT local_path FROM creative_pending WHERE id=? AND status='pending'", (pending_id,)
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "待审核素材不存在或已处理")

    # 删除本地文件
    local_path = row[0]
    if local_path and os.path.exists(local_path):
        try:
            os.remove(local_path)
        except Exception:
            pass

    conn.execute("""
        UPDATE creative_pending
        SET status='rejected', reject_reason=?, updated_at=datetime('now')
        WHERE id=?
    """, (req.reason or "用户拒绝", pending_id))
    conn.commit()
    conn.close()

    return {"success": True, "message": "已拒绝并删除"}


@router.delete("/pending/batch-reject")
def batch_reject(
    ids: List[int],
    user=Depends(get_current_user),
):
    """批量拒绝"""
    conn = get_conn()
    for pid in ids:
        row = conn.execute(
            "SELECT local_path FROM creative_pending WHERE id=?", (pid,)
        ).fetchone()
        if row and row[0] and os.path.exists(row[0]):
            try:
                os.remove(row[0])
            except Exception:
                pass
        conn.execute(
            "UPDATE creative_pending SET status='rejected', updated_at=datetime('now') WHERE id=?",
            (pid,)
        )
    conn.commit()
    conn.close()
    return {"success": True, "rejected": len(ids)}
