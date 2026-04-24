"""
image_provider.py  —  多厂商图像生成适配层
支持厂商:
  fal_kontext   : Fal.ai FLUX.1 Kontext Pro（图生图/裂变，保持人物一致性）
  fal_schnell   : Fal.ai FLUX.1 Schnell（文生图，快速便宜）
  openai        : OpenAI gpt-image-1（文生图/图生图，文字排版强）
  stability_core: Stability AI Stable Image Core（文生图，快速稳定）
  stability_ultra: Stability AI Stable Image Ultra（文生图，最高质量）
  ideogram      : Ideogram 3.0（文生图，文字+图像融合）
  replicate     : Replicate（通用，支持任意模型）

返回统一格式:
  {"url": str, "b64": str|None, "provider": str, "model": str, "cost_usd": float}
"""

import os
import base64
import logging
import requests
import tempfile
import time
from typing import Optional

logger = logging.getLogger("mira.image_provider")


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _b64_to_url_via_temp(b64_data: str, ext: str = "jpg") -> str:
    """将 base64 数据写入临时文件，返回 data URI（用于前端预览）"""
    return f"data:image/{ext};base64,{b64_data}"


def _save_b64_to_asset_dir(b64_data: str, filename: str, asset_dir: str) -> str:
    """将 base64 数据保存到素材目录，返回本地文件路径"""
    os.makedirs(asset_dir, exist_ok=True)
    filepath = os.path.join(asset_dir, filename)
    with open(filepath, "wb") as f:
        f.write(base64.b64decode(b64_data))
    return filepath


def _download_url_to_asset_dir(url: str, filename: str, asset_dir: str) -> str:
    """将远程图片 URL 下载到素材目录，返回本地文件路径"""
    os.makedirs(asset_dir, exist_ok=True)
    filepath = os.path.join(asset_dir, filename)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    with open(filepath, "wb") as f:
        f.write(resp.content)
    return filepath


# ─────────────────────────────────────────────────────────────────────────────
# Fal.ai 通用调用
# ─────────────────────────────────────────────────────────────────────────────

def _fal_run(endpoint: str, payload: dict, api_key: str, timeout: int = 120) -> dict:
    """
    同步调用 Fal.ai API（使用 fal.run 同步端点）
    文档: https://fal.ai/docs/model-endpoints/rest-api
    """
    url = f"https://fal.run/{endpoint}"
    headers = {
        "Authorization": f"Key {api_key}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"Fal.ai API error {resp.status_code}: {resp.text[:500]}")
    return resp.json()


def generate_fal_kontext(
    prompt: str,
    image_url: str,
    api_key: str,
    guidance_scale: float = 3.5,
    num_images: int = 1,
    aspect_ratio: str = "1:1",
    safety_tolerance: str = "6",
    seed: Optional[int] = None,
) -> list[dict]:
    """
    Fal.ai FLUX.1 Kontext Pro — 图生图（爆款裂变）
    endpoint: fal-ai/flux-pro/kontext
    必填: prompt, image_url
    返回: list of {"url": ..., "b64": None, "provider": "fal_kontext", "model": "flux-pro/kontext", "cost_usd": 0.04}
    """
    payload = {
        "prompt": prompt,
        "image_url": image_url,
        "guidance_scale": guidance_scale,
        "num_images": num_images,
        "output_format": "jpeg",
        "safety_tolerance": safety_tolerance,
        "aspect_ratio": aspect_ratio,
    }
    if seed is not None:
        payload["seed"] = seed

    result = _fal_run("fal-ai/flux-pro/kontext", payload, api_key)
    images = result.get("images", [])
    return [
        {
            "url": img["url"],
            "b64": None,
            "provider": "fal_kontext",
            "model": "flux-pro/kontext",
            "cost_usd": 0.04,
        }
        for img in images
    ]


def generate_fal_schnell(
    prompt: str,
    api_key: str,
    image_size: str = "square_hd",
    num_images: int = 1,
    num_inference_steps: int = 4,
    seed: Optional[int] = None,
) -> list[dict]:
    """
    Fal.ai FLUX.1 Schnell — 文生图（快速便宜）
    endpoint: fal-ai/flux/schnell
    image_size 枚举: square_hd/square/landscape_4_3/landscape_16_9/portrait_4_3/portrait_16_9
    """
    payload = {
        "prompt": prompt,
        "image_size": image_size,
        "num_inference_steps": num_inference_steps,
        "num_images": num_images,
        "output_format": "jpeg",
        "enable_safety_checker": True,
    }
    if seed is not None:
        payload["seed"] = seed

    result = _fal_run("fal-ai/flux/schnell", payload, api_key)
    images = result.get("images", [])
    return [
        {
            "url": img["url"],
            "b64": None,
            "provider": "fal_schnell",
            "model": "flux/schnell",
            "cost_usd": 0.003,
        }
        for img in images
    ]


# ─────────────────────────────────────────────────────────────────────────────
# OpenAI gpt-image-1
# ─────────────────────────────────────────────────────────────────────────────

def generate_openai(
    prompt: str,
    api_key: str,
    size: str = "1024x1024",
    quality: str = "low",
    n: int = 1,
    reference_image_path: Optional[str] = None,
    api_base: Optional[str] = None,
) -> list[dict]:
    """
    OpenAI gpt-image-1 — 文生图或图生图
    文档: https://platform.openai.com/docs/api-reference/images
    size 枚举: 1024x1024 / 1024x1536 / 1536x1024 / auto
    quality 枚举: low / medium / high / auto
    返回 b64_json（gpt-image-1 不返回 url）
    cost_usd: low=$0.011, medium=$0.042, high=$0.167（1024x1024）
    """
    cost_map = {"low": 0.011, "medium": 0.042, "high": 0.167, "auto": 0.042}
    cost = cost_map.get(quality, 0.042)

    base_url = (api_base or "https://api.openai.com/v1").rstrip("/")
    headers = {"Authorization": f"Bearer {api_key}"}

    if reference_image_path and os.path.exists(reference_image_path):
        # 图生图：使用 /images/edits 接口
        url = f"{base_url}/images/edits"
        with open(reference_image_path, "rb") as img_file:
            files = {"image": (os.path.basename(reference_image_path), img_file, "image/jpeg")}
            data = {
                "model": "gpt-image-1",
                "prompt": prompt,
                "size": size,
                "quality": quality,
                "n": str(n),
            }
            resp = requests.post(url, headers=headers, files=files, data=data, timeout=120)
    else:
        # 文生图：使用 /images/generations 接口
        url = f"{base_url}/images/generations"
        payload = {
            "model": "gpt-image-1",
            "prompt": prompt,
            "size": size,
            "quality": quality,
            "n": n,
            "output_format": "jpeg",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=120)

    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI Images API error {resp.status_code}: {resp.text[:500]}")

    result = resp.json()
    items = result.get("data", [])
    return [
        {
            "url": None,
            "b64": item.get("b64_json", ""),
            "provider": "openai",
            "model": "gpt-image-1",
            "cost_usd": cost,
        }
        for item in items
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Stability AI
# ─────────────────────────────────────────────────────────────────────────────

def _stability_generate(
    endpoint: str,
    prompt: str,
    api_key: str,
    negative_prompt: str = "",
    aspect_ratio: str = "1:1",
    output_format: str = "jpeg",
    style_preset: Optional[str] = None,
    seed: Optional[int] = None,
    cost_usd: float = 0.03,
    model_name: str = "stable-image-core",
) -> list[dict]:
    """
    Stability AI 通用调用（multipart/form-data）
    文档: https://platform.stability.ai/docs/api-reference#tag/Generate
    aspect_ratio 枚举: 16:9/1:1/21:9/2:3/3:2/4:5/5:4/9:16/9:21
    style_preset 枚举: 3d-model/anime/cinematic/comic-book/digital-art/enhance/
                       fantasy-art/isometric/line-art/low-poly/modeling-compound/
                       neon-punk/origami/photographic/pixel-art/tile-texture
    返回 base64 JSON
    """
    url = f"https://api.stability.ai/v2beta/stable-image/generate/{endpoint}"
    headers = {
        "authorization": f"Bearer {api_key}",
        "accept": "application/json",
    }
    data = {
        "prompt": prompt,
        "output_format": output_format,
        "aspect_ratio": aspect_ratio,
    }
    if negative_prompt:
        data["negative_prompt"] = negative_prompt
    if style_preset:
        data["style_preset"] = style_preset
    if seed is not None:
        data["seed"] = str(seed)

    # Stability AI 要求 multipart/form-data，files={"none": ""} 是官方示例的必要占位
    resp = requests.post(url, headers=headers, files={"none": ""}, data=data, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Stability AI API error {resp.status_code}: {resp.text[:500]}")

    result = resp.json()
    b64 = result.get("image", "")
    return [
        {
            "url": None,
            "b64": b64,
            "provider": f"stability_{endpoint}",
            "model": model_name,
            "cost_usd": cost_usd,
        }
    ]


def generate_stability_core(
    prompt: str,
    api_key: str,
    negative_prompt: str = "",
    aspect_ratio: str = "1:1",
    style_preset: Optional[str] = None,
    seed: Optional[int] = None,
) -> list[dict]:
    """Stability AI Stable Image Core — 快速稳定，$0.03/张"""
    return _stability_generate(
        "core", prompt, api_key,
        negative_prompt=negative_prompt,
        aspect_ratio=aspect_ratio,
        style_preset=style_preset,
        seed=seed,
        cost_usd=0.03,
        model_name="stable-image-core",
    )


def generate_stability_ultra(
    prompt: str,
    api_key: str,
    negative_prompt: str = "",
    aspect_ratio: str = "1:1",
    style_preset: Optional[str] = None,
    seed: Optional[int] = None,
) -> list[dict]:
    """Stability AI Stable Image Ultra — 最高质量，$0.08/张"""
    return _stability_generate(
        "ultra", prompt, api_key,
        negative_prompt=negative_prompt,
        aspect_ratio=aspect_ratio,
        style_preset=style_preset,
        seed=seed,
        cost_usd=0.08,
        model_name="stable-image-ultra",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Ideogram 3.0
# ─────────────────────────────────────────────────────────────────────────────

def generate_ideogram(
    prompt: str,
    api_key: str,
    aspect_ratio: str = "1:1",
    style_type: str = "REALISTIC",
    rendering_speed: str = "DEFAULT",
    magic_prompt_option: str = "AUTO",
    negative_prompt: str = "",
    num_images: int = 1,
    seed: Optional[int] = None,
    reference_image_path: Optional[str] = None,
    image_weight: float = 0.5,
) -> list[dict]:
    """
    Ideogram 3.0 — 文生图或图生图（Remix）
    文档: https://developer.ideogram.ai/api-reference/api-reference/generate-v3
    endpoint: POST https://api.ideogram.ai/v1/ideogram-v3/generate
    aspect_ratio 枚举: 1x1/16x9/9x16/4x3/3x4/3x2/2x3/16x10/10x16/3x1/1x3
    style_type 枚举: GENERAL/REALISTIC/DESIGN/3D/ANIME
    rendering_speed 枚举: TURBO/DEFAULT/SLOW
    cost: ~$0.05/张
    """
    # Ideogram API 要求 aspect_ratio 用 "x" 分隔（如 "1x1"），而非 ":" 分隔
    aspect_ratio = aspect_ratio.replace(":", "x")
    headers = {"Api-Key": api_key}

    if reference_image_path and os.path.exists(reference_image_path):
        # 图生图：使用 Remix 接口
        url = "https://api.ideogram.ai/v1/ideogram-v3/remix"
        data = {
            "prompt": prompt,
            "aspect_ratio": aspect_ratio,
            "style_type": style_type,
            "rendering_speed": rendering_speed,
            "magic_prompt_option": magic_prompt_option,
            "num_images": str(num_images),
            "image_weight": str(image_weight),
        }
        if negative_prompt:
            data["negative_prompt"] = negative_prompt
        if seed is not None:
            data["seed"] = str(seed)
        with open(reference_image_path, "rb") as img_file:
            files = {"image_file": (os.path.basename(reference_image_path), img_file, "image/jpeg")}
            resp = requests.post(url, headers=headers, data=data, files=files, timeout=120)
    else:
        # 文生图 —— 使用 JSON 格式（Ideogram v3 要求 application/json 或 multipart/form-data）
        url = "https://api.ideogram.ai/v1/ideogram-v3/generate"
        json_data = {
            "prompt": prompt,
            "aspect_ratio": aspect_ratio,
            "style_type": style_type,
            "rendering_speed": rendering_speed,
            "magic_prompt_option": magic_prompt_option,
            "num_images": num_images,
        }
        if negative_prompt:
            json_data["negative_prompt"] = negative_prompt
        if seed is not None:
            json_data["seed"] = seed
        resp = requests.post(url, headers=headers, json=json_data, timeout=120)

    if resp.status_code != 200:
        raise RuntimeError(f"Ideogram API error {resp.status_code}: {resp.text[:500]}")

    result = resp.json()
    items = result.get("data", [])
    return [
        {
            "url": item.get("url", ""),
            "b64": None,
            "provider": "ideogram",
            "model": "ideogram-v3",
            "cost_usd": 0.05,
        }
        for item in items
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Replicate
# ─────────────────────────────────────────────────────────────────────────────

def generate_replicate(
    prompt: str,
    api_key: str,
    model_version: str = "stability-ai/sdxl:39ed52f2319f9b9f4f3d9b5573b4c2b8d5b3e4f5",
    extra_input: Optional[dict] = None,
    timeout: int = 180,
) -> list[dict]:
    """
    Replicate — 通用模型调用
    文档: https://replicate.com/docs/reference/http
    model_version 格式: "owner/name:version_id"
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Prefer": "wait",  # 同步等待结果（最多60秒）
    }

    # 解析 model_version
    if ":" in model_version:
        owner_name, version_id = model_version.rsplit(":", 1)
        create_url = "https://api.replicate.com/v1/predictions"
        payload = {
            "version": version_id,
            "input": {"prompt": prompt, **(extra_input or {})},
        }
    else:
        # 使用官方模型（无版本号）
        owner_name = model_version
        create_url = f"https://api.replicate.com/v1/models/{owner_name}/predictions"
        payload = {
            "input": {"prompt": prompt, **(extra_input or {})},
        }

    resp = requests.post(create_url, json=payload, headers=headers, timeout=timeout)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Replicate API error {resp.status_code}: {resp.text[:500]}")

    prediction = resp.json()

    # 如果 Prefer: wait 没有立即返回结果，则轮询
    poll_url = prediction.get("urls", {}).get("get", "")
    deadline = time.time() + timeout
    while prediction.get("status") not in ("succeeded", "failed", "canceled") and time.time() < deadline:
        time.sleep(3)
        poll_resp = requests.get(poll_url, headers={"Authorization": f"Bearer {api_key}"}, timeout=30)
        prediction = poll_resp.json()

    if prediction.get("status") != "succeeded":
        raise RuntimeError(f"Replicate prediction failed: {prediction.get('error', 'unknown')}")

    output = prediction.get("output", [])
    if isinstance(output, str):
        output = [output]

    return [
        {
            "url": url,
            "b64": None,
            "provider": "replicate",
            "model": model_version,
            "cost_usd": 0.0,  # Replicate 按秒计费，无法预估
        }
        for url in output
        if isinstance(url, str) and url.startswith("http")
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 统一入口函数
# ─────────────────────────────────────────────────────────────────────────────

PROVIDER_MAP = {
    "fal_kontext": "fal_kontext",
    "fal_schnell": "fal_schnell",
    "openai": "openai",
    "stability_core": "stability_core",
    "stability_ultra": "stability_ultra",
    "ideogram": "ideogram",
    "replicate": "replicate",
}


def generate_image(
    provider: str,
    prompt: str,
    api_key: str,
    reference_image_url: Optional[str] = None,
    reference_image_path: Optional[str] = None,
    num_images: int = 1,
    aspect_ratio: str = "1:1",
    extra_params: Optional[dict] = None,
) -> list[dict]:
    """
    统一图像生成入口

    参数:
        provider: 厂商标识（见 PROVIDER_MAP）
        prompt: 生成提示词
        api_key: 对应厂商的 API Key
        reference_image_url: 参考图 URL（用于 fal_kontext）
        reference_image_path: 参考图本地路径（用于 openai/ideogram/replicate 图生图）
        num_images: 生成数量
        aspect_ratio: 宽高比（1:1/16:9/9:16/4:3/3:4）
        extra_params: 厂商特定的额外参数

    返回:
        list of {"url": str|None, "b64": str|None, "provider": str, "model": str, "cost_usd": float}
    """
    extra = extra_params or {}

    if provider == "fal_kontext":
        if not reference_image_url:
            raise ValueError("fal_kontext 需要提供 reference_image_url")
        return generate_fal_kontext(
            prompt=prompt,
            image_url=reference_image_url,
            api_key=api_key,
            num_images=num_images,
            aspect_ratio=aspect_ratio,
            guidance_scale=extra.get("guidance_scale", 3.5),
            safety_tolerance=extra.get("safety_tolerance", "2"),
            seed=extra.get("seed"),
        )

    elif provider == "fal_schnell":
        # aspect_ratio 转 image_size
        ar_to_size = {
            "1:1": "square_hd",
            "16:9": "landscape_16_9",
            "9:16": "portrait_16_9",
            "4:3": "landscape_4_3",
            "3:4": "portrait_4_3",
        }
        image_size = ar_to_size.get(aspect_ratio, "square_hd")
        return generate_fal_schnell(
            prompt=prompt,
            api_key=api_key,
            image_size=image_size,
            num_images=num_images,
            num_inference_steps=extra.get("num_inference_steps", 4),
            seed=extra.get("seed"),
        )

    elif provider == "openai":
        # aspect_ratio 转 size
        ar_to_size = {
            "1:1": "1024x1024",
            "16:9": "1536x1024",
            "9:16": "1024x1536",
            "4:3": "1024x1024",  # OpenAI 无 4:3，用正方形替代
            "3:4": "1024x1024",
        }
        size = ar_to_size.get(aspect_ratio, "1024x1024")
        return generate_openai(
            prompt=prompt,
            api_key=api_key,
            size=size,
            quality=extra.get("quality", "low"),
            n=num_images,
            reference_image_path=reference_image_path,
            api_base=extra.get("api_base"),
        )

    elif provider == "stability_core":
        # aspect_ratio 格式转换（Stability 用冒号格式，与我们一致）
        return generate_stability_core(
            prompt=prompt,
            api_key=api_key,
            negative_prompt=extra.get("negative_prompt", ""),
            aspect_ratio=aspect_ratio,
            style_preset=extra.get("style_preset"),
            seed=extra.get("seed"),
        )

    elif provider == "stability_ultra":
        return generate_stability_ultra(
            prompt=prompt,
            api_key=api_key,
            negative_prompt=extra.get("negative_prompt", ""),
            aspect_ratio=aspect_ratio,
            style_preset=extra.get("style_preset"),
            seed=extra.get("seed"),
        )

    elif provider == "ideogram":
        return generate_ideogram(
            prompt=prompt,
            api_key=api_key,
            aspect_ratio=aspect_ratio,
            style_type=extra.get("style_type", "REALISTIC"),
            rendering_speed=extra.get("rendering_speed", "DEFAULT"),
            magic_prompt_option=extra.get("magic_prompt_option", "AUTO"),
            negative_prompt=extra.get("negative_prompt", ""),
            num_images=num_images,
            seed=extra.get("seed"),
            reference_image_path=reference_image_path,
            image_weight=extra.get("image_weight", 0.5),
        )

    elif provider == "replicate":
        return generate_replicate(
            prompt=prompt,
            api_key=api_key,
            model_version=extra.get("model_version", "black-forest-labs/flux-schnell"),
            extra_input=extra.get("extra_input"),
        )

    else:
        raise ValueError(f"不支持的图像生成厂商: {provider}，可选值: {list(PROVIDER_MAP.keys())}")


def generate_image_safe(
    provider: str,
    prompt: str,
    api_key: str,
    reference_image_url: Optional[str] = None,
    reference_image_path: Optional[str] = None,
    num_images: int = 1,
    aspect_ratio: str = "1:1",
    extra_params: Optional[dict] = None,
) -> list[dict]:
    """
    安全版统一图像生成入口：捕获异常并返回 error 字段，不抛异常。
    用于批量生成场景，避免单张失败导致整个任务崩溃。
    """
    try:
        return generate_image(
            provider=provider,
            prompt=prompt,
            api_key=api_key,
            reference_image_url=reference_image_url,
            reference_image_path=reference_image_path,
            num_images=num_images,
            aspect_ratio=aspect_ratio,
            extra_params=extra_params,
        )
    except Exception as e:
        logger.error(f"[image_provider] 生成失败 ({provider}): {e}")
        return [{"url": None, "b64": None, "error": str(e), "provider": provider, "model": provider, "cost_usd": 0}]


def get_provider_info() -> list[dict]:
    """返回所有支持的厂商信息，供前端下拉框使用"""
    return [
        {
            "id": "fal_kontext",
            "name": "Fal.ai — FLUX Kontext Pro",
            "type": "img2img",
            "desc": "参考图一致性最强，适合爆款素材裂变（保留人物/产品）",
            "cost": "$0.04/张",
            "requires_ref": True,
        },
        {
            "id": "fal_schnell",
            "name": "Fal.ai — FLUX Schnell",
            "type": "txt2img",
            "desc": "极速极便宜，适合批量粗筛",
            "cost": "$0.003/张",
            "requires_ref": False,
        },
        {
            "id": "openai",
            "name": "OpenAI — GPT Image 1",
            "type": "txt2img+img2img",
            "desc": "文字排版能力最强，适合含文字的广告图",
            "cost": "$0.011~$0.167/张",
            "requires_ref": False,
        },
        {
            "id": "stability_core",
            "name": "Stability AI — Stable Image Core",
            "type": "txt2img",
            "desc": "快速稳定，适合通用场景",
            "cost": "$0.03/张",
            "requires_ref": False,
        },
        {
            "id": "stability_ultra",
            "name": "Stability AI — Stable Image Ultra",
            "type": "txt2img",
            "desc": "最高质量，适合高端产品图",
            "cost": "$0.08/张",
            "requires_ref": False,
        },
        {
            "id": "ideogram",
            "name": "Ideogram 3.0",
            "type": "txt2img+img2img",
            "desc": "文字+图像融合最自然，适合带 slogan 的广告图",
            "cost": "$0.05/张",
            "requires_ref": False,
        },
        {
            "id": "replicate",
            "name": "Replicate（通用）",
            "type": "txt2img",
            "desc": "支持任意模型，可跑 LoRA 微调",
            "cost": "按秒计费",
            "requires_ref": False,
        },
    ]
