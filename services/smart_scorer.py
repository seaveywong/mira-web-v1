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
请从以下 6 个维度对这张广告图片进行评分，每个维度 0-100 分，然后加权计算总分。

评分标准：
- S 级（90-100分）：极强视觉冲击力，有真实数字（如+847%、$12,450），人物表情极度震惊/兴奋，构图专业，必定高点击
- A 级（70-89分）：视觉效果好，有明确的金融主题，数字清晰，表情到位，预期点击率高
- B 级（50-69分）：基本合格，主题清晰，但视觉冲击力一般，数字不够突出或表情不够自然
- C 级（0-49分）：视觉效果差，主题模糊，或明显是低质量图片

【6 维评分体系】

1. Visual Quality (权重 20%)
   评估构图、清晰度、色彩运用、整体专业感
   - 高分：构图专业、色彩鲜明、清晰度高、有视觉层次感
   - 低分：模糊、昏暗、构图杂乱、分辨率低

2. Copy Resonance (权重 20%)
   评估文案吸引力、数字突出程度、CTA 力度
   - 高分：有具体数字（$、%）、有紧迫感、CTA 明确有力
   - 低分：无数字、文案模糊、CTA 缺失或平淡

3. Emotional Appeal (权重 20%)
   评估人物表情真实度、情感共鸣能力、惊喜感
   - 高分：表情极度震惊/兴奋/惊喜、眼神有感染力、情绪真实
   - 低分：表情僵硬/假笑、无表情、明显摆拍

4. Offer Clarity (权重 15%)
   评估价值主张清晰度、利益点是否一目了然
   - 高分：一眼看懂卖什么、利益点明确、有稀缺感
   - 低分：看不懂在推什么、利益模糊、信息混乱

5. Compliance (权重 10%)
   评估是否符合 FB 广告政策
   - 高分：无明显违规风险、文字占比合理
   - 低分：before/after 对比、过多文字覆盖、可能违规暗示

6. Audience-Creative Fit (权重 15%)
   评估与国家/语言/目标受众的匹配度
   - 高分：场景与目标地区一致、语言正确、文化元素匹配
   - 低分：场景明显不符合目标市场、语言错误

推荐国家规则：
- 图片有英文文字/美国场景 → US, CA, AU, GB
- 图片有繁体中文/台湾场景 → TW
- 图片有繁体中文/香港场景(如粤语、香港用语) → HK
- 图片无明显文字/通用场景 → US, CA, AU, GB, SG
- 图片有马来文/东南亚场景 → MY, SG, ID

请返回以下 JSON（不要有任何其他文字）：
{
  "grade": "S|A|B|C",
  "score": 0-100,
  "dimensions": {
    "visual_quality": 0-100,
    "copy_resonance": 0-100,
    "emotional_appeal": 0-100,
    "offer_clarity": 0-100,
    "compliance": 0-100,
    "audience_fit": 0-100
  },
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
            max_tokens=800,
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


import re as _re


def _fallback_score(asset_row: dict) -> dict:
    """
    当 Vision API 不可用时，用规则引擎打分
    升级版：语义级关键词聚类 + 数值提取
    """
    score = 75  # 默认 A 级（新素材无数据时给予基础信任）
    ai_desc = (asset_row.get("ai_analysis") or "").lower()
    combined = ai_desc

    # 正向关键词聚类（按强度分组，每组只加一次）
    strong_groups = [
        ["震惊", "惊讶", "难以置信", "不敢相信"],
        ["暴涨", "大涨", "飙升", "狂涨"],
        ["翻倍", "翻倍了"],
        ["兴奋", "激动"],
        ["专业", "高级", "高清"],
    ]
    for group in strong_groups:
        for kw in group:
            if kw in combined:
                score += 8
                break

    weak_groups = [
        ["手持手机", "拿手机", "看手机"],
        ["图表", "走势图", "k线", "上涨", "增长"],
        ["点击", "注册", "加入", "立即"],
    ]
    for group in weak_groups:
        for kw in group:
            if kw in combined:
                score += 4
                break

    # 数值提取：找 大额数字（如 $12,450）
    numbers_found = _re.findall(r'\$?\s*(\d{2,}(?:[.,]\d+)?)\s*%?', combined)
    for num_str in numbers_found:
        try:
            num = float(num_str.replace(",", ""))
            if num > 1000:
                score += 10
            elif num > 100:
                score += 6
            elif num > 10:
                score += 3
        except ValueError:
            pass

    # 百分数加分（如 +847%, 312%）
    pct_found = _re.findall(r'(\+?\d+\.?\d*)\s*%', combined)
    for pct_str in pct_found:
        try:
            pct = float(pct_str.replace("+", ""))
            if pct > 50:
                score += 12
            elif pct > 20:
                score += 8
            elif pct > 5:
                score += 4
        except ValueError:
            pass

    # 负向关键词
    negative_keywords = [
        ("模糊", -10), ("低质", -15), ("普通", -6), ("一般", -5),
        ("模糊不清", -12), ("马赛克", -15),
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
        "reason": f"规则引擎评分: {score}分",
    }




def _predict_fatigue(asset_id: int) -> dict:
    """
    预测素材疲劳度：
    - 基于 dispatch_count 和 last_dispatched_at
    - 同目标受众下活跃素材密度
    - 返回 fatigue_score 0-100 和建议
    """
    try:
        from core.database import get_conn
        conn = get_conn()

        asset = conn.execute(
            "SELECT dispatch_count, last_dispatched_at, target_countries FROM ad_assets WHERE id=?",
            (asset_id,)
        ).fetchone()
        if not asset:
            conn.close()
            return {"fatigue": 0, "level": "unknown", "suggestion": ""}

        fatigue = 0
        dispatch_count = asset["dispatch_count"] or 0
        last_at = asset["last_dispatched_at"]
        target = (asset["target_countries"] or "").strip()

        # 投放次数：每投放 3 次 +10 分
        fatigue += min(40, (dispatch_count // 3) * 10)

        # 时间衰减：last_dispatched_at 距今越近越疲劳
        if last_at:
            try:
                last_dt = datetime.strptime(last_at, "%Y-%m-%d %H:%M:%S")
                days_since = (datetime.now() - last_dt).days
                if days_since < 1:
                    fatigue += 20
                elif days_since < 3:
                    fatigue += 10
                elif days_since < 7:
                    fatigue += 5
            except ValueError:
                pass

        # 同受众活跃素材密度
        if target:
            same_target = conn.execute(
                """SELECT COUNT(*) as cnt FROM ad_assets
                   WHERE target_countries=? AND dispatch_count>0
                   AND upload_status IN ('ai_done','approved')""",
                (target,)
            ).fetchone()
            if same_target and same_target["cnt"] > 5:
                fatigue += min(20, (same_target["cnt"] - 5) * 3)

        conn.close()

        fatigue = min(100, max(0, fatigue))

        if fatigue >= 70:
            level = "high"
            suggestion = "建议轮换新素材或更换受众"
        elif fatigue >= 40:
            level = "medium"
            suggestion = "可以继续投放但需关注效果下滑"
        else:
            level = "low"
            suggestion = "素材新鲜度良好"

        return {"fatigue": fatigue, "level": level, "suggestion": suggestion}

    except Exception as e:
        logger.warning(f"[SmartScorer] 疲劳度预测失败: {e}")
        return {"fatigue": 0, "level": "unknown", "suggestion": ""}


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

        # 如果已有评分：检查是否需要重新评分
        if asset["ai_grade"] and asset["ai_grade"] in ("S", "A", "B", "C"):
            # 检查 rescore 标记或是否超过 7 天
            conn_r = get_conn()
            needs = conn_r.execute("SELECT needs_rescore, scored_at FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
            conn_r.close()
            if needs and needs["needs_rescore"] == 1:
                logger.info(f"[SmartScorer] 素材 {asset_id} 标记需重新评分，继续...")
            elif needs and needs["scored_at"]:
                try:
                    from datetime import datetime as _dt
                    scored = _dt.strptime(needs["scored_at"], "%Y-%m-%d %H:%M:%S")
                    if (_dt.now() - scored).days < 7:
                        logger.info(f"[SmartScorer] 素材 {asset_id} 已有评分 {asset['ai_grade']}（7天内），跳过")
                        return True
                except ValueError:
                    pass
            else:
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
        dimensions = result.get("dimensions", {})

        # 构建评分理由（含各维度详情）
        dim_parts = []
        if dimensions:
            dim_labels = {
                "visual_quality": "视觉", "copy_resonance": "文案",
                "emotional_appeal": "情感", "offer_clarity": "价值",
                "compliance": "合规", "audience_fit": "受众"
            }
            for dim_key, dim_label in dim_labels.items():
                dv = dimensions.get(dim_key)
                if dv is not None:
                    dim_parts.append(f"{dim_label}={dv}")
        dim_str = f" | 各维: {', '.join(dim_parts)}" if dim_parts else ""

        # 疲劳度
        fatigue_info = _predict_fatigue(asset_id)
        fatigue_str = f" | 疲劳度: {fatigue_info['level']}({fatigue_info['fatigue']}分)" if fatigue_info.get("level") != "unknown" else ""

        full_reason = f"{reason}{dim_str}{fatigue_str} | 优点: {strengths}" + (f" | 不足: {weaknesses}" if weaknesses else "")

        # 写回数据库
        conn2 = get_conn()
        conn2.execute(
            """UPDATE ad_assets SET
                ai_grade=?,
                ai_score=?,
                recommended_countries=?,
                score_reason=?,
                scored_at=?,
                needs_rescore=0
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
               AND (ai_grade IS NULL OR ai_grade NOT IN ('S','A','B','C') OR needs_rescore=1)
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


def _correlate_with_performance():
    """
    数据反馈环：对比 ai_score vs 实际性能分
    找出偏差大的素材，标记 needs_rescore
    每日凌晨由 scheduler 调用
    """
    try:
        from core.database import get_conn
        conn = get_conn()

        assets = conn.execute("""
            SELECT id, ai_score, score, ai_grade
            FROM ad_assets
            WHERE ai_score IS NOT NULL AND ai_score > 0
              AND score > 0
              AND (needs_rescore IS NULL OR needs_rescore = 0)
        """).fetchall()

        total = len(assets)
        large_deviation = 0
        total_deviation = 0

        for asset in assets:
            ai_s = asset["ai_score"] or 0
            perf_s = asset["score"] or 0
            if perf_s == 0:
                continue

            deviation = abs(ai_s - perf_s)
            total_deviation += deviation

            if perf_s > 0:
                dev_pct = deviation / perf_s
            else:
                dev_pct = 0

            if dev_pct > 0.3 or deviation > 20:
                conn.execute(
                    "UPDATE ad_assets SET needs_rescore=1 WHERE id=?",
                    (asset["id"],)
                )
                large_deviation += 1
                logger.info(f"[SmartScorer] 偏差过大: asset={asset['id']}, ai={ai_s}, perf={perf_s}, dev={deviation:.1f}")

        conn.commit()

        avg_dev = total_deviation / total if total > 0 else 0
        logger.info(f"[SmartScorer] 反馈环完成: 检查 {total} 个素材, 偏差过大 {large_deviation} 个, 平均偏差 {avg_dev:.1f} 分")

        # 清理旧的 needs_rescore 素材
        conn.execute("""
            UPDATE ad_assets SET needs_rescore=0
            WHERE needs_rescore=1 AND scored_at IS NOT NULL
              AND datetime(scored_at) > datetime('now', '-1 hour')
        """)
        conn.commit()
        conn.close()
        return large_deviation

    except Exception as e:
        logger.error(f"[SmartScorer] 反馈环执行失败: {e}", exc_info=True)
        return 0
