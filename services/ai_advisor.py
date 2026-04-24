"""
AI 顾问服务 v1.1.0
支持多厂商: DeepSeek / OpenAI / Gemini / Claude / 自定义
AI未配置时静默跳过，不影响其他功能
"""
import json
import logging
from typing import Tuple, Optional

from core.database import get_conn

logger = logging.getLogger("mira.ai")

# 支持的AI厂商配置（供前端下拉框使用）
AI_PROVIDERS = {
    "deepseek": {
        "label": "DeepSeek",
        "base_url": "https://api.deepseek.com/v1",
        "default_model": "deepseek-chat",
        "models": ["deepseek-chat", "deepseek-reasoner"]
    },
    "openai": {
        "label": "OpenAI",
        "base_url": "https://api.openai.com/v1",
        "default_model": "gpt-4o-mini",
        "models": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"]
    },
    "gemini": {
        "label": "Google Gemini",
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai",
        "default_model": "gemini-2.0-flash",
        "models": ["gemini-2.0-flash", "gemini-1.5-pro", "gemini-1.5-flash"]
    },
    "claude": {
        "label": "Anthropic Claude",
        "base_url": "https://api.anthropic.com/v1",
        "default_model": "claude-3-5-haiku-20241022",
        "models": ["claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022", "claude-3-haiku-20240307"]
    },
    "custom": {
        "label": "自定义 (OpenAI兼容)",
        "base_url": "",
        "default_model": "",
        "models": []
    }
}


def _get_setting(key: str, default=None):
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def is_ai_enabled() -> bool:
    """检查AI是否已配置且启用，供外部调用"""
    enabled = _get_setting("ai_enabled", "0")
    api_key = _get_setting("ai_api_key", "")
    return enabled == "1" and bool(api_key)


def get_ai_client():
    """
    获取AI客户端，未配置时返回(None, None)，不抛出异常
    """
    if not is_ai_enabled():
        return None, None

    try:
        from openai import OpenAI
        provider = _get_setting("ai_provider", "deepseek")
        api_key = _get_setting("ai_api_key", "")
        # 根据 provider 自动设置默认 base_url 和 model
        _provider_defaults = {
            "deepseek": ("https://api.deepseek.com/v1", "deepseek-chat"),
            "openai": ("https://api.openai.com/v1", "gpt-4o-mini"),
            "gemini": ("https://generativelanguage.googleapis.com/v1beta/openai", "gemini-2.0-flash"),
        }
        _default_base, _default_model = _provider_defaults.get(provider, ("https://api.deepseek.com/v1", "deepseek-chat"))
        api_base = _get_setting("ai_api_base", _default_base)
        model = _get_setting("ai_model", _default_model)

        if not api_key:
            return None, None

        client = OpenAI(api_key=api_key, base_url=api_base)
        return client, model
    except ImportError:
        logger.warning("openai 包未安装，AI功能不可用")
        return None, None
    except Exception as e:
        logger.warning(f"AI客户端初始化失败（非致命）: {e}")
        return None, None


async def ask_kpi(campaign_meta: dict, actions: list) -> Tuple[str, str]:
    """
    AI推断KPI字段（异步）
    返回 (kpi_field, kpi_label)，失败时返回 ("", "")，不抛出异常
    """
    if not is_ai_enabled():
        return "", ""

    try:
        from openai import AsyncOpenAI
        provider = _get_setting("ai_provider", "deepseek")
        api_key = _get_setting("ai_api_key", "")
        # 根据 provider 自动设置默认 base_url 和 model
        _provider_defaults = {
            "deepseek": ("https://api.deepseek.com/v1", "deepseek-chat"),
            "openai": ("https://api.openai.com/v1", "gpt-4o-mini"),
            "gemini": ("https://generativelanguage.googleapis.com/v1beta/openai", "gemini-2.0-flash"),
        }
        _default_base, _default_model = _provider_defaults.get(provider, ("https://api.deepseek.com/v1", "deepseek-chat"))
        api_base = _get_setting("ai_api_base", _default_base)
        model = _get_setting("ai_model", _default_model)

        if not api_key:
            return "", ""

        client = AsyncOpenAI(api_key=api_key, base_url=api_base)

        sorted_actions = sorted(actions, key=lambda x: float(x.get("value", 0)), reverse=True)[:15]
        actions_summary = "\n".join(
            f"  - {a.get('action_type')}: {a.get('value')} 次"
            for a in sorted_actions
        )

        prompt = f"""你是 Facebook 广告 KPI 分析专家。请根据以下广告配置，判断该广告的核心 KPI 字段。

广告配置：
- 活动目标 (objective): {campaign_meta.get('objective', '未知')}
- 优化目标 (optimization_goal): {campaign_meta.get('optimization_goal', '未知')}
- 自定义事件 (custom_event_type): {campaign_meta.get('custom_event_type', '无')}
- 目标类型 (destination_type): {campaign_meta.get('destination_type', '未知')}
- 今日花费: ${campaign_meta.get('spend', 0):.2f}

今日 Actions 数据（按数量降序）：
{actions_summary if actions_summary else '  暂无数据'}

判断规则：
1. 私信类广告（MESSENGER/INSTAGRAM_DIRECT/CONVERSATIONS）优先选 onsite_conversion.messaging_conversation_started_7d
2. 电商/转化类优先选 offsite_conversion.fb_pixel_purchase
3. 线索类优先选 onsite_conversion.lead_grouped
4. 避免选择辅助/上游指标（如 messaging_welcome_message_view, post_engagement）
5. 优先选择数量最多的核心转化字段

请只返回 JSON 格式，不要有其他文字：
{{"field": "字段名", "label": "中文名称", "reason": "简短理由"}}"""

        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=200
        )
        content = response.choices[0].message.content.strip()
        if "```" in content:
            content = content.split("```")[1].replace("json", "").strip()
        data = json.loads(content)
        field = data.get("field", "").strip()
        label = data.get("label", field)
        logger.info(f"AI KPI 推断结果: {field} ({label}) - {data.get('reason', '')}")
        return field, label
    except Exception as e:
        logger.warning(f"AI KPI推断失败（非致命）: {e}")
        return "", ""


def analyze_ad_performance(ad_data: dict) -> Optional[dict]:
    """
    AI分析广告表现，给出优化建议
    未配置时返回None，不影响其他功能
    """
    client, model = get_ai_client()
    if not client:
        return None

    try:
        prompt = (
            "你是Facebook广告优化专家。分析以下广告数据，给出简洁的优化建议。\n"
            "返回JSON: {\"diagnosis\": \"...\", \"suggestions\": [...], \"risk_level\": \"low/medium/high\"}\n\n"
            f"广告数据: {json.dumps(ad_data, ensure_ascii=False)}"
        )
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=500
        )
        content = response.choices[0].message.content.strip()
        if "```" in content:
            content = content.split("```")[1].replace("json", "").strip()
        return json.loads(content)
    except Exception as e:
        logger.warning(f"AI广告分析失败（非致命）: {e}")
        return None


def get_providers_config() -> dict:
    """返回所有支持的AI厂商配置（供前端下拉框使用）"""
    return AI_PROVIDERS
