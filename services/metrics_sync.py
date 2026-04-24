"""
metrics_sync.py  v1.0
Facebook 广告数据回流服务
────────────────────────────────────────────────
职责：
  1. 每小时从 FB Graph API 拉取所有活跃广告账户的广告数据
  2. 拉取指标：spend（消耗）、impressions（曝光）、clicks（点击）、
               conversions（转化）、cpm（千次展示费用）、ctr（点击率）、
               cpc（每次点击费用）、cpp（每次购买费用）、roas（广告支出回报率）
  3. 将数据写入 ad_metrics 表（act_id, metric_name, value, recorded_at）
  4. 同时更新 accounts 表的 balance 字段（实时余额）

设计原则：
  - 非侵入式：不修改任何现有逻辑，只做数据读取和写入
  - 容错性：单个账户失败不影响其他账户
  - 去重：同一账户同一指标同一小时只写一条记录（ON CONFLICT UPDATE）
  - 历史保留：保留最近 30 天的数据，自动清理过期数据
"""

import logging
import requests
import time
from datetime import datetime, timedelta

logger = logging.getLogger("mira.metrics_sync")

FB_API_BASE = "https://graph.facebook.com/v25.0"


def _get_conn():
    from core.database import get_conn
    return get_conn()


def _get_active_accounts() -> list:
    """获取所有启用的广告账户"""
    try:
        conn = _get_conn()
        rows = conn.execute(
            "SELECT act_id, name FROM accounts WHERE enabled=1"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"[MetricsSync] 获取账户列表失败: {e}")
        return []


def _get_account_token(act_id: str) -> str:
    """获取账户的读取 Token"""
    try:
        from services.token_manager import get_exec_token, ACTION_READ
        token = get_exec_token(act_id, ACTION_READ)
        return token or ""
    except Exception as e:
        logger.warning(f"[MetricsSync] 获取账户 {act_id} Token 失败: {e}")
        return ""


def _fetch_account_insights(act_id: str, token: str) -> dict:
    """
    从 FB Graph API 拉取账户级广告数据（昨天 + 今天）
    返回 {metric_name: value} 字典
    """
    metrics = {}
    try:
        # 拉取今天的数据
        resp = requests.get(
            f"{FB_API_BASE}/{act_id}/insights",
            params={
                "fields": "spend,impressions,clicks,actions,action_values,cpm,ctr,cpc,cpp",
                "date_preset": "today",
                "access_token": token,
                "level": "account",
            },
            timeout=20
        )
        data = resp.json()
        if "error" in data:
            logger.warning(f"[MetricsSync] 账户 {act_id} insights API 错误: {data['error'].get('message', '')}")
            return metrics

        rows = data.get("data", [])
        if not rows:
            # 今天无数据，尝试昨天
            resp2 = requests.get(
                f"{FB_API_BASE}/{act_id}/insights",
                params={
                    "fields": "spend,impressions,clicks,actions,action_values,cpm,ctr,cpc,cpp",
                    "date_preset": "yesterday",
                    "access_token": token,
                    "level": "account",
                },
                timeout=20
            )
            data2 = resp2.json()
            rows = data2.get("data", []) if "error" not in data2 else []

        if not rows:
            return metrics

        row = rows[0]

        # 基础指标
        metrics["spend"] = float(row.get("spend", 0) or 0)
        metrics["impressions"] = float(row.get("impressions", 0) or 0)
        metrics["clicks"] = float(row.get("clicks", 0) or 0)
        metrics["cpm"] = float(row.get("cpm", 0) or 0)
        metrics["ctr"] = float(row.get("ctr", 0) or 0)
        metrics["cpc"] = float(row.get("cpc", 0) or 0)
        metrics["cpp"] = float(row.get("cpp", 0) or 0)

        # 转化数据（从 actions 中提取）
        actions = row.get("actions", [])
        conversions = 0.0
        purchase_value = 0.0
        for action in actions:
            action_type = action.get("action_type", "")
            value = float(action.get("value", 0) or 0)
            if action_type in ("purchase", "offsite_conversion.fb_pixel_purchase",
                               "omni_purchase", "web_in_store_purchase"):
                conversions += value
            elif action_type == "lead":
                # 表单广告的转化
                if conversions == 0:
                    conversions += value
            elif action_type == "onsite_conversion.messaging_conversation_started_7d":
                # 消息广告的转化
                if conversions == 0:
                    conversions += value

        metrics["conversions"] = conversions

        # ROAS（广告支出回报率）
        action_values = row.get("action_values", [])
        total_revenue = 0.0
        for av in action_values:
            if av.get("action_type") in ("purchase", "offsite_conversion.fb_pixel_purchase",
                                          "omni_purchase", "web_in_store_purchase"):
                total_revenue += float(av.get("value", 0) or 0)

        spend = metrics.get("spend", 0)
        if spend > 0 and total_revenue > 0:
            metrics["roas"] = round(total_revenue / spend, 4)
        else:
            metrics["roas"] = 0.0

        metrics["revenue"] = total_revenue

    except Exception as e:
        logger.error(f"[MetricsSync] 拉取账户 {act_id} 数据失败: {e}")

    return metrics


def _fetch_account_balance(act_id: str, token: str) -> float:
    """从 FB API 获取账户余额（USD）"""
    try:
        resp = requests.get(
            f"{FB_API_BASE}/{act_id}",
            params={
                "fields": "balance,currency",
                "access_token": token,
            },
            timeout=15
        )
        data = resp.json()
        if "error" in data:
            return -1.0
        balance_cents = int(data.get("balance", 0) or 0)
        currency = data.get("currency", "USD").upper()
        # FB balance 单位是账户货币的分（cents）
        balance = balance_cents / 100.0
        # 如果不是 USD，需要换算（简单处理：直接返回原始值，由前端显示货币）
        return balance
    except Exception as e:
        logger.warning(f"[MetricsSync] 获取账户 {act_id} 余额失败: {e}")
        return -1.0


def _write_metrics(act_id: str, metrics: dict):
    """将指标数据写入 ad_metrics 表"""
    if not metrics:
        return
    try:
        conn = _get_conn()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 按小时去重：同一账户同一指标同一小时只保留最新值
        hour_str = datetime.now().strftime("%Y-%m-%d %H:00:00")

        for metric_name, value in metrics.items():
            try:
                # 先删除本小时已有记录，再插入新记录
                conn.execute(
                    """DELETE FROM ad_metrics
                       WHERE act_id=? AND metric_name=?
                         AND recorded_at >= ? AND recorded_at < datetime(?, '+1 hour')""",
                    (act_id, metric_name, hour_str, hour_str)
                )
                conn.execute(
                    """INSERT INTO ad_metrics (act_id, metric_name, value, recorded_at)
                       VALUES (?, ?, ?, ?)""",
                    (act_id, metric_name, value, now_str)
                )
            except Exception as e:
                logger.warning(f"[MetricsSync] 写入指标 {metric_name} 失败: {e}")

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[MetricsSync] 写入账户 {act_id} 指标失败: {e}")


def _update_account_balance(act_id: str, balance: float):
    """更新 accounts 表的 balance 字段"""
    if balance < 0:
        return
    try:
        conn = _get_conn()
        conn.execute(
            "UPDATE accounts SET balance=? WHERE act_id=?",
            (balance, act_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[MetricsSync] 更新账户 {act_id} 余额失败: {e}")


def _cleanup_old_metrics():
    """清理 30 天前的历史数据"""
    try:
        conn = _get_conn()
        conn.execute(
            "DELETE FROM ad_metrics WHERE recorded_at < datetime('now', '-30 days')"
        )
        conn.commit()
        conn.close()
        logger.info("[MetricsSync] 清理过期指标数据完成")
    except Exception as e:
        logger.warning(f"[MetricsSync] 清理过期数据失败: {e}")


def run_metrics_sync():
    """
    全量数据同步主函数。
    由 scheduler.py 每小时调用一次。
    """
    logger.info("[MetricsSync] 开始同步 FB 广告数据...")
    start_time = time.time()

    accounts = _get_active_accounts()
    if not accounts:
        logger.warning("[MetricsSync] 无活跃账户，跳过同步")
        return

    success_count = 0
    fail_count = 0

    for acc in accounts:
        act_id = acc["act_id"]
        try:
            token = _get_account_token(act_id)
            if not token:
                logger.warning(f"[MetricsSync] 账户 {act_id} 无可用 Token，跳过")
                fail_count += 1
                continue

            # 拉取广告数据
            metrics = _fetch_account_insights(act_id, token)
            if metrics:
                _write_metrics(act_id, metrics)
                logger.info(
                    f"[MetricsSync] ✅ 账户 {act_id} 同步完成: "
                    f"消耗=${metrics.get('spend', 0):.2f}, "
                    f"转化={metrics.get('conversions', 0):.0f}, "
                    f"ROAS={metrics.get('roas', 0):.2f}"
                )
                success_count += 1
            else:
                logger.info(f"[MetricsSync] 账户 {act_id} 今日无广告数据")
                success_count += 1

            # 更新账户余额
            balance = _fetch_account_balance(act_id, token)
            if balance >= 0:
                _update_account_balance(act_id, balance)

            # 避免 API 频率限制
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"[MetricsSync] 账户 {act_id} 同步失败: {e}")
            fail_count += 1

    # 清理过期数据（每天执行一次，通过时间判断）
    if datetime.now().hour == 3:
        _cleanup_old_metrics()

    elapsed = time.time() - start_time
    logger.info(
        f"[MetricsSync] 同步完成: 成功 {success_count} 个, 失败 {fail_count} 个, "
        f"耗时 {elapsed:.1f}s"
    )


def get_account_metrics_summary(act_id: str, days: int = 7) -> dict:
    """
    获取账户近 N 天的指标汇总（供 API 调用）
    返回 {metric_name: total_value} 字典
    """
    try:
        conn = _get_conn()
        rows = conn.execute(
            """SELECT metric_name, SUM(CAST(value AS REAL)) as total
               FROM ad_metrics
               WHERE act_id=? AND recorded_at > datetime('now', ?)
               GROUP BY metric_name""",
            (act_id, f"-{days} days")
        ).fetchall()
        conn.close()
        return {r["metric_name"]: round(r["total"], 4) for r in rows}
    except Exception as e:
        logger.error(f"[MetricsSync] 获取账户 {act_id} 指标汇总失败: {e}")
        return {}
