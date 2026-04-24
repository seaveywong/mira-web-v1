"""
global_dispatcher.py — 全局调度器（修复版 v2）
读取 global_dispatch_enabled 开关
按素材评级智能撮合账户，自动生成铺放任务

修复内容（v2）：
- _get_pending_assets: 去掉 status='approved' 过滤（ad_assets 无此列），
  改用 upload_status='approved' 或不过滤（只要有 ai_grade 即可）
- _get_eligible_accounts: 修正字段名 act_name→name, balance_usd→balance,
  daily_budget_usd→不存在（去掉），status='active'→enabled=1 AND account_status=1
"""

import logging
import json
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("mira.global_dispatcher")

# 撮合规则：哪个评级的素材分配给哪个生命周期阶段的账户
MATCH_RULES = {
    "S": ["scaling", "testing"],   # S 级素材优先给拉量期，其次测试期
    "A": ["testing", "scaling"],   # A 级素材优先给测试期
    "B": ["testing", "warmup"],    # B 级素材给测试期和预热期
    "C": [],                        # C 级素材不自动铺放
}

# 每次调度最多为每个账户创建的任务数
MAX_TASKS_PER_ACCOUNT = 2
# 每次调度最多处理的素材数
MAX_ASSETS_PER_RUN = 10


def _get_setting(key: str, default: str = "") -> str:
    try:
        from core.database import get_conn
        conn = get_conn()
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row["value"] if row else default
    except Exception:
        return default


def _is_dispatch_enabled() -> bool:
    """检查全局调度开关是否开启"""
    val = _get_setting("global_dispatch_enabled", "0")
    return val == "1"


def _get_pending_assets() -> list:
    """
    获取待铺放的高分素材
    条件：已审核(upload_status=approved) + 有评级(S/A/B) + 近 7 天内没有被调度过
    """
    try:
        from core.database import get_conn
        conn = get_conn()
        assets = conn.execute(
            """SELECT a.id, a.file_path, a.ai_grade, a.ai_score,
                      a.recommended_countries, a.target_countries,
                      a.ai_interests,
                      COALESCE(a.recommended_countries, a.target_countries, 'US') as countries
               FROM ad_assets a
               WHERE a.upload_status IN ('ai_done', 'approved')
               AND a.ai_grade IN ('S', 'A', 'B')
               AND (
                   a.id NOT IN (
                       SELECT DISTINCT asset_id FROM dispatch_log
                       WHERE created_at >= datetime('now', '-7 days')
                   )
               )
               ORDER BY
                   CASE a.ai_grade WHEN 'S' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3 END,
                   COALESCE(a.ai_score, 70) DESC
               LIMIT ?""",
            (MAX_ASSETS_PER_RUN,)
        ).fetchall()
        conn.close()
        return [dict(a) for a in assets]
    except Exception as e:
        logger.error(f"[Dispatcher] 获取待铺素材失败: {e}", exc_info=True)
        return []


def _get_eligible_accounts(lifecycle_stages: list) -> list:
    """
    获取符合条件的账户
    条件：enabled=1 + account_status=1 + 余额充足 + 在指定生命周期阶段
    """
    if not lifecycle_stages:
        return []
    try:
        from core.database import get_conn
        conn = get_conn()
        placeholders = ",".join("?" * len(lifecycle_stages))
        accounts = conn.execute(
            f"""SELECT act_id, name as act_name,
                       COALESCE(lifecycle_stage, 'testing') as lifecycle_stage,
                       COALESCE(balance, 0) as balance_usd
                FROM accounts
                WHERE enabled = 1
                AND account_status = 1
                AND COALESCE(lifecycle_stage, 'testing') IN ({placeholders})
                AND COALESCE(balance, 0) >= 10
                ORDER BY
                    CASE COALESCE(lifecycle_stage, 'testing')
                        WHEN 'scaling' THEN 1
                        WHEN 'testing' THEN 2
                        WHEN 'warmup' THEN 3
                    END,
                    COALESCE(balance, 0) DESC""",
            lifecycle_stages
        ).fetchall()
        conn.close()
        return [dict(a) for a in accounts]
    except Exception as e:
        logger.error(f"[Dispatcher] 获取账户失败: {e}", exc_info=True)
        return []


def _already_dispatched_today(asset_id: int, act_id: str) -> bool:
    """检查今天是否已经为这个素材+账户组合创建过任务"""
    try:
        from core.database import get_conn
        conn = get_conn()
        row = conn.execute(
            """SELECT id FROM dispatch_log
               WHERE asset_id=? AND act_id=?
               AND created_at >= datetime('now', '-1 day')""",
            (asset_id, act_id)
        ).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False


def _log_dispatch(asset_id: int, act_id: str, action: str, reason: str,
                  lifecycle_stage: str, asset_grade: str, campaign_id: str = None):
    """记录调度日志"""
    try:
        from core.database import get_conn
        conn = get_conn()
        conn.execute(
            """INSERT INTO dispatch_log
               (asset_id, act_id, campaign_id, action, reason, lifecycle_stage, asset_grade, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (asset_id, act_id, campaign_id, action, reason, lifecycle_stage, asset_grade,
             datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[Dispatcher] 记录调度日志失败: {e}")


def _create_dispatch_task(asset, account):
    try:
        import json as _json
        from core.database import get_conn
        act_id = account["act_id"]
        asset_id = asset["id"]
        grade = asset.get("ai_grade", "B")
        stage = account.get("lifecycle_stage", "testing")
        budget_map = {
            ("S", "scaling"): 50, ("S", "testing"): 30,
            ("A", "testing"): 20, ("A", "scaling"): 30,
            ("B", "testing"): 15, ("B", "warmup"): 10,
        }
        daily_budget = budget_map.get((grade, stage), 20)
        max_budget = account.get("balance_usd", 50) * 0.3
        daily_budget = min(daily_budget, max(10, max_budget))
        countries_str = asset.get("recommended_countries") or asset.get("target_countries") or "US"
        countries = [c.strip() for c in countries_str.split(",") if c.strip()][:3]
        conn = get_conn()
        acc_row = conn.execute(
            "SELECT target_objective_type, pixel_id, landing_url FROM accounts WHERE act_id=?", (act_id,)
        ).fetchone()
        # ── 配置完整性预检：缺少像素或落地页时跳过，避免铺放后必然失败 ──
        if acc_row:
            missing = []
            if not acc_row["pixel_id"]:
                missing.append("像素ID")
            if not acc_row["landing_url"]:
                missing.append("落地页URL")
            if missing:
                conn.close()
                reason = f"账户配置不完整，缺少: {', '.join(missing)}"
                logger.warning(f"[Dispatcher] 跳过 {act_id}: {reason}")
                _log_dispatch(asset_id, act_id, "skipped", reason, stage, grade)
                return False
        objective = (acc_row["target_objective_type"] if acc_row and acc_row["target_objective_type"]
                     else "OUTCOME_LEADS")
        existing = conn.execute(
            "SELECT id FROM auto_campaigns WHERE act_id=? AND asset_id=? AND status IN ('pending','running') LIMIT 1",
            (act_id, asset_id)
        ).fetchone()
        if existing:
            conn.close()
            logger.info(f"[Dispatcher] asset#{asset_id} on {act_id} already queued, skip")
            return False
        cursor = conn.execute(
            "INSERT INTO auto_campaigns (act_id, asset_id, objective, target_countries, daily_budget, status, created_by, dispatch_source, lifecycle_stage_at_launch) VALUES (?, ?, ?, ?, ?, 'pending', 'system', 'global_dispatcher', ?)",
            (act_id, asset_id, objective, _json.dumps(countries), round(daily_budget, 2), stage)
        )
        campaign_id = cursor.lastrowid
        conn.commit()
        conn.close()
        _log_dispatch(asset_id, act_id, "queued",
                      f"dispatched grade:{grade} stage:{stage} budget:{daily_budget:.0f}",
                      stage, grade, str(campaign_id))
        logger.info(f"[Dispatcher] queued campaign_id={campaign_id} asset#{asset_id}({grade}) -> {act_id}({stage}) budget={daily_budget:.0f}")
        return True
    except Exception as e:
        logger.error(f"[Dispatcher] create task error: {e}", exc_info=True)
        _log_dispatch(asset["id"], account["act_id"], "error", str(e),
                      account.get("lifecycle_stage", "testing"), asset.get("ai_grade", "B"))
        return False

def run_dispatch(force: bool = False):
    """
    全局调度器主函数
    由 scheduler.py 每 30 分钟调用一次
    """
    # 检查开关
    if not force and not _is_dispatch_enabled():
        logger.debug("[Dispatcher] 全局调度已关闭，跳过")
        return {"skipped": True, "reason": "global_dispatch_enabled=0"}

    logger.info("[Dispatcher] 开始全局调度...")

    # 获取待铺素材
    assets = _get_pending_assets()
    if not assets:
        logger.info("[Dispatcher] 没有待铺放的高分素材（upload_status=approved 且有 ai_grade）")
        return {"assets": 0, "tasks_created": 0}

    logger.info(f"[Dispatcher] 找到 {len(assets)} 个待铺素材")

    tasks_created = 0
    tasks_skipped = 0

    for asset in assets:
        grade = asset.get("ai_grade", "B")
        target_stages = MATCH_RULES.get(grade, [])

        if not target_stages:
            logger.info(f"[Dispatcher] 素材#{asset['id']}({grade}) 不符合自动铺放条件")
            continue

        # 获取匹配的账户
        accounts = _get_eligible_accounts(target_stages)
        if not accounts:
            logger.info(f"[Dispatcher] 没有符合条件的账户（需要阶段: {target_stages}）")
            continue

        # 为每个素材最多分配 MAX_TASKS_PER_ACCOUNT 个账户
        dispatched_count = 0
        for account in accounts:
            if dispatched_count >= MAX_TASKS_PER_ACCOUNT:
                break

            act_id = account["act_id"]

            # 检查今天是否已经为这个组合创建过任务
            if _already_dispatched_today(asset["id"], act_id):
                tasks_skipped += 1
                continue

            if _create_dispatch_task(asset, account):
                tasks_created += 1
                dispatched_count += 1
            else:
                tasks_skipped += 1

    logger.info(
        f"[Dispatcher] 调度完成: 处理素材 {len(assets)} 个, "
        f"创建任务 {tasks_created} 个, 跳过 {tasks_skipped} 个"
    )

    return {
        "assets_processed": len(assets),
        "tasks_created": tasks_created,
        "tasks_skipped": tasks_skipped
    }
