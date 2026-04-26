"""
smart_scorer.py — 素材智能评分服务（升级版）
接入 Gemini Vision 模型，对广告素材进行真实质量评估
输出 S/A/B/C 四级评分 + 推荐国家 + 评分理由
"""

import logging
import base64
import json
import os
from datetime import datetime

logger = logging.getLogger("mira.smart_scorer")

# 评分等级定义
GRADE_CRITERIA = """
你是一位有 15 年经验的 Facebook 广告素材评审专家，专注于股票/金融类广告。
请对这张广告图片进行评分，返回 JSON 格式结果。

评分标准：
- S 级（90-100分）：极强视觉冲击力，有真实数字（如+847%、$12,450），人物表情极度震惊/兴奋，构图专业，必定高点击
- A 级（70-89分）：视觉效果好，有明确的金融主题，数字清晰，表情到位，预期点击率高
- B 级（50-69分）：基本合格，主题清晰，但视觉冲击力一般，数字不够突出或表情不够自然
- C 级（0-49分）：视觉效果差，主题模糊，或明显是低质量图片

推荐国家规则：
- 图片有英文文字/美国场景 → US, CA, AU, GB
- 图片有繁体中文/台湾场景 → TW
- 图片有繁体中文/香港场景(如粵語、香港用語) → HK
- 图片无明显文字/通用场景 → US, CA, AU, GB, SG
- 图片有马来文/东南亚场景 → MY, SG, ID

请返回以下 JSON（不要有任何其他文字）：
{
  "grade": "S|A|B|C",
  "score": 0-100,
  "recommended_countries": ["US", "CA"],
  "strengths": "图片的优点（一句话）",
  "weaknesses": "图片的不足（一句话，如无则填null）",
  "reason": "评分理由（50字以内）"
}
"""


def _get_setting(key: str, default: str = "") -> str:
    """从 settings 表读取配置"""
    try:
        from core.database import get_conn
        conn = get_conn()
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row["value"] if row else default
    except Exception:
        return default


def _call_vision_api(image_path: str) -> dict:
    """
    调用 Gemini Vision API 对图片进行评分
    返回评分结果字典
    """
    try:
        from openai import OpenAI

        vision_api_key = _get_setting("vision_api_key", "")
        vision_api_base = _get_setting("vision_api_base", "https://generativelanguage.googleapis.com/v1beta/openai")
        vision_model = _get_setting("vision_model", "gemini-2.5-flash")

        if not vision_api_key:
            logger.warning("[SmartScorer] vision_api_key 未配置，跳过 Vision 评分")
            return {}

        # 读取图片并转为 base64
        if not os.path.exists(image_path):
            logger.warning(f"[SmartScorer] 图片文件不存在: {image_path}")
            return {}

        with open(image_path, "rb") as f:
            image_data = f.read()

        # 判断图片格式
        ext = os.path.splitext(image_path)[1].lower()
        mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                    ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp"}
        mime_type = mime_map.get(ext, "image/jpeg")

        b64_image = base64.b64encode(image_data).decode("utf-8")

        client = OpenAI(api_key=vision_api_key, base_url=vision_api_base)
        response = client.chat.completions.create(
            model=vision_model,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{b64_image}"
                            }
                        },
                        {
                            "type": "text",
                            "text": GRADE_CRITERIA
                        }
                    ]
                }
            ],
            max_tokens=500,
            temperature=0.1
        )

        raw = response.choices[0].message.content.strip()
        # 清理 markdown 代码块
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        logger.info(f"[SmartScorer] Vision 评分完成: grade={result.get('grade')}, score={result.get('score')}")
        return result

    except json.JSONDecodeError as e:
        logger.error(f"[SmartScorer] Vision API 返回非 JSON: {e}")
        return {}
    except Exception as e:
        logger.error(f"[SmartScorer] Vision API 调用失败: {e}")
        return {}


def _fallback_score(asset_row: dict) -> dict:
    """
    当 Vision API 不可用时，用规则引擎打分
    基于 AI 分析结果的关键词匹配
    """
    score = 75  # 默认 A 级（新素材无数据时给予基础信任）
    ai_desc = (asset_row.get("ai_analysis") or "").lower()
    combined = ai_desc

    # 正向加分
    positive_keywords = [
        ("震惊", 10), ("惊讶", 10), ("兴奋", 8), ("暴涨", 12), ("大涨", 10),
        ("%", 8), ("收益", 6), ("盈利", 6), ("翻倍", 10), ("847", 15),
        ("312", 12), ("手持手机", 5), ("真实", 5), ("专业", 4),
    ]
    for kw, pts in positive_keywords:
        if kw in combined:
            score += pts

    # 负向扣分
    negative_keywords = [
        ("模糊", -15), ("低质", -20), ("普通", -10), ("一般", -8),
    ]
    for kw, pts in negative_keywords:
        if kw in combined:
            score += pts

    score = max(0, min(100, score))

    if score >= 90:
        grade = "S"
    elif score >= 70:
        grade = "A"
    elif score >= 50:
        grade = "B"
    else:
        grade = "C"

    # 推断国家
    target = (asset_row.get("target_countries") or "").upper()
    if target:
        countries = [c.strip() for c in target.split(",") if c.strip()]
    else:
        countries = ["US", "CA", "AU", "GB"]

    return {
        "grade": grade,
        "score": score,
        "recommended_countries": countries,
        "strengths": "基于 AI 分析结果推断",
        "weaknesses": None,
        "reason": f"规则引擎评分（Vision API 不可用）: {score}分"
    }


def score_asset(asset_id: int) -> bool:
    """
    对单个素材进行智能评分
    优先使用 Vision API，失败时降级到规则引擎
    返回是否成功
    """
    try:
        from core.database import get_conn
        conn = get_conn()
        asset = conn.execute(
            """SELECT id, file_path, thumb_path, ai_analysis,
                      target_countries, file_size, upload_status as status, ai_grade
               FROM ad_assets WHERE id=?""",
            (asset_id,)
        ).fetchone()
        conn.close()

        if not asset:
            logger.warning(f"[SmartScorer] 素材 {asset_id} 不存在")
            return False

        if asset["status"] not in ("approved", "ai_done"):
            logger.info(f"[SmartScorer] 素材 {asset_id} 状态为 {asset['status']}，跳过评分")
            return False

        # 如果已有评分且不是空，跳过（避免重复评分）
        if asset["ai_grade"] and asset["ai_grade"] in ("S", "A", "B", "C"):
            logger.info(f"[SmartScorer] 素材 {asset_id} 已有评分 {asset['ai_grade']}，跳过")
            return True

        # 确定图片路径
        image_path = None
        if asset["file_path"] and os.path.exists(asset["file_path"]):
            image_path = asset["file_path"]
        elif asset["thumb_path"] and os.path.exists(asset["thumb_path"]):
            image_path = asset["thumb_path"]

        # 尝试 Vision API 评分
        result = {}
        if image_path:
            result = _call_vision_api(image_path)

        # 降级到规则引擎
        if not result or "grade" not in result:
            logger.info(f"[SmartScorer] 素材 {asset_id} 使用规则引擎评分")
            result = _fallback_score(dict(asset))

        grade = result.get("grade", "B")
        score = result.get("score", 50)
        countries = result.get("recommended_countries", ["US"])
        reason = result.get("reason", "")
        strengths = result.get("strengths", "")
        weaknesses = result.get("weaknesses", "")

        # 写回数据库
        conn2 = get_conn()
        conn2.execute(
            """UPDATE ad_assets SET
                ai_grade=?,
                ai_score=?,
                recommended_countries=?,
                score_reason=?,
                scored_at=?
               WHERE id=?""",
            (
                grade,
                score,
                ",".join(countries) if isinstance(countries, list) else countries,
                f"{reason} | 优点: {strengths}" + (f" | 不足: {weaknesses}" if weaknesses else ""),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                asset_id
            )
        )
        conn2.commit()
        conn2.close()

        logger.info(f"[SmartScorer] 素材 {asset_id} 评分完成: {grade}({score}分), 推荐国家: {countries}")
        return True

    except Exception as e:
        logger.error(f"[SmartScorer] 素材 {asset_id} 评分失败: {e}", exc_info=True)
        return False


def run_asset_scoring():
    """
    批量评分所有未评分的已审核素材
    由 scheduler.py 每天凌晨 1 点调用
    """
    try:
        from core.database import get_conn
        conn = get_conn()
        assets = conn.execute(
            """SELECT id FROM ad_assets
               WHERE upload_status IN ('ai_done','approved')
               AND (ai_grade IS NULL OR ai_grade NOT IN ('S','A','B','C'))
               ORDER BY id DESC LIMIT 50"""
        ).fetchall()
        conn.close()

        total = len(assets)
        success = 0
        for asset in assets:
            if score_asset(asset["id"]):
                success += 1

        logger.info(f"[SmartScorer] 批量评分完成: {success}/{total}")
        return success

    except Exception as e:
        logger.error(f"[SmartScorer] 批量评分失败: {e}")
        return 0


def score_asset_after_approve(asset_id: int):
    """
    素材审核通过后立即触发评分（在后台线程中执行）
    由 creative_gen.py 的 approve_pending 函数调用
    """
    import threading

    def _score():
        try:
            import time
            time.sleep(2)  # 等待文件写入完成
            score_asset(asset_id)
        except Exception as e:
            logger.error(f"[SmartScorer] 审核后评分失败: {e}")

    t = threading.Thread(target=_score, daemon=True)
    t.start()
    logger.info(f"[SmartScorer] 素材 {asset_id} 已加入后台评分队列")
