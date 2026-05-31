"""
定时任务调度器 v1.1.1
"""
import logging
import threading
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from core.database import get_conn
from services.token_manager import TOKEN_SOURCE_SYSTEM_USER, ensure_token_source_columns

logger = logging.getLogger("mira.scheduler")
_scheduler = None

_guard_lock = threading.Lock()
_warmup_lock = threading.Lock()
_job_state_lock = threading.Lock()
_job_state = {}


def _now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _record_job_state(job_id: str, **updates):
    with _job_state_lock:
        state = dict(_job_state.get(job_id, {}))
        state.update(updates)
        _job_state[job_id] = state


def _summarize_job_result(result):
    if isinstance(result, dict):
        parts = []
        for key in ("status", "total", "checked", "alive", "dead", "paused", "failed", "skipped", "started", "baseline_added"):
            if key in result:
                parts.append(f"{key}={result.get(key)}")
        return " | ".join(parts)[:300]
    if result is None:
        return ""
    return str(result)[:300]


def _run_tracked_job(job_id: str, label: str, func, lock: threading.Lock = None):
    acquired = False
    if lock is not None:
        acquired = lock.acquire(blocking=False)
        if not acquired:
            _record_job_state(
                job_id,
                id=job_id,
                label=label,
                status="skipped",
                last_skipped_at=_now_text(),
                last_message="previous run still active",
            )
            logger.warning("%s previous run still active; skipped", label)
            return {"status": "skipped", "reason": "previous run still active"}
    started = datetime.now()
    _record_job_state(
        job_id,
        id=job_id,
        label=label,
        status="running",
        last_started_at=started.strftime("%Y-%m-%d %H:%M:%S"),
        last_error="",
    )
    try:
        result = func()
        finished = datetime.now()
        _record_job_state(
            job_id,
            id=job_id,
            label=label,
            status="ok",
            last_finished_at=finished.strftime("%Y-%m-%d %H:%M:%S"),
            duration_sec=round((finished - started).total_seconds(), 2),
            last_message=_summarize_job_result(result),
            last_error="",
        )
        return result
    except Exception as e:
        finished = datetime.now()
        _record_job_state(
            job_id,
            id=job_id,
            label=label,
            status="error",
            last_finished_at=finished.strftime("%Y-%m-%d %H:%M:%S"),
            duration_sec=round((finished - started).total_seconds(), 2),
            last_error=str(e)[:500],
        )
        logger.error("%s failed: %s", label, e, exc_info=True)
        return {"status": "error", "error": str(e)}
    finally:
        if acquired:
            lock.release()

def _get_setting(key, default=None):
    """从 settings 表读取配置"""
    try:
        conn = get_conn()
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row["value"] if row else default
    except Exception:
        return default


def get_scheduler_health() -> dict:
    jobs = []
    if _scheduler:
        try:
            for job in _scheduler.get_jobs():
                jobs.append({
                    "id": job.id,
                    "next_run_time": job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job.next_run_time else None,
                })
        except Exception as e:
            jobs.append({"id": "_scheduler", "error": str(e)})
    with _job_state_lock:
        states = {k: dict(v) for k, v in _job_state.items()}
    for job in jobs:
        state = states.setdefault(job["id"], {"id": job["id"], "label": job["id"], "status": "scheduled"})
        state["next_run_time"] = job.get("next_run_time")
    return {
        "running": bool(_scheduler and _scheduler.running),
        "jobs": sorted(states.values(), key=lambda x: str(x.get("id", ""))),
    }

def run_guard():
    inspect_enabled = _get_setting("inspect_enabled", "1")
    if inspect_enabled != "1":
        logger.debug("inspect_enabled=0; guard skipped")
        _record_job_state("guard", id="guard", label="巡检/镜像", status="disabled", last_finished_at=_now_text(), last_message="inspect_enabled=0")
        return {"status": "disabled"}

    def _work():
        from services.guard_engine import GuardEngine
        logger.info("running guard job")
        GuardEngine().run_all()
        return {"status": "ok"}

    return _run_tracked_job("guard", "巡检/镜像", _work, lock=_guard_lock)

def run_asset_scoring():
    """v3.0: 每日凌晨1点批量更新素材得分"""
    try:
        from services.asset_scorer import score_all_assets
        score_all_assets()
    except Exception as e:
        logger.error(f"素材打分任务异常: {e}", exc_info=True)




def run_storage_cleanup():
    """每天凌晨3点自动清理存储空间（已拒绝图片、旧图片、旧缩略图、磁盘告警）"""
    try:
        from services.storage_manager import run_auto_cleanup
        run_auto_cleanup()
    except Exception as e:
        logger.error(f'存储自动清理任务异常: {e}', exc_info=True)

def run_account_status_sync():
    """v3.3: 定期同步所有广告账户的真实状态（account_status、余额）"""
    try:
        from core.database import get_conn
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from services.token_manager import get_exec_token, ACTION_READ
        import requests

        conn = get_conn()
        rows = conn.execute("""
            SELECT a.id, a.act_id FROM accounts a
        """).fetchall()
        conn.close()

        if not rows:
            return

        def fetch_status(row):
            try:
                # 操作号优先，操作号全部失效时回退到管理号兜底
                token = get_exec_token(row['act_id'], ACTION_READ)
                if not token:
                    return row["id"], None
                r = requests.get(
                    "https://graph.facebook.com/v25.0/" + row['act_id'],
                    params={"access_token": token, "fields": "account_status,balance,amount_spent,spend_cap,currency,timezone_name,timezone_offset_hours_utc"},
                    timeout=15
                )
                data = r.json()
                if "error" in data:
                    err_code = data["error"].get("code", 0)
                    err_msg = data["error"].get("message", "")
                    # 这些错误更常见于 token 失效/权限波动。不能据此把账户本地标为禁用，
                    # 否则后续巡检、镜像和预热都会被错误跳过。
                    if err_code in (100, 190, 200, 803):
                        return row["id"], {"_read_error": True, "_error_code": err_code, "_error_message": err_msg}
                    return row["id"], None
                return row["id"], data
            except Exception:
                return row["id"], None

        with ThreadPoolExecutor(max_workers=min(8, len(rows))) as executor:
            futures = {executor.submit(fetch_status, row): row for row in rows}
            results = {}
            for fut in as_completed(futures):
                acc_id, data = fut.result()
                results[acc_id] = data

        conn = get_conn()
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
        if "timezone_name" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN timezone_name TEXT")
        if "timezone_offset_hours_utc" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN timezone_offset_hours_utc REAL")
        updated = 0
        read_errors = 0
        for row in rows:
            data = results.get(row["id"])
            if not data:
                continue
            if data.get("_read_error"):
                read_errors += 1
                logger.warning(
                    "[AccountSync] 账户 %s 状态同步失败，保留本地状态不改写 (error_code=%s, message=%s)",
                    row["act_id"] if row["act_id"] else row["id"],
                    data.get("_error_code"),
                    data.get("_error_message", "")[:180],
                )
                continue
            new_status = data.get("account_status", 1)
            # 检测状态变化：若变为3（支付失败）或7（政策违规），主动发TG提醒
            old_row = conn.execute(
                "SELECT account_status, name, act_id FROM accounts WHERE id=?", (row["id"],)
            ).fetchone()
            if old_row:
                old_status = old_row["account_status"] or 1
                acc_name = old_row["name"] or old_row["act_id"]
                acc_act_id = old_row["act_id"]
                _STATUS_LABELS = {1: "正常", 2: "禁用", 3: "支付失败", 7: "政策违规", 8: "待结算", 9: "宽限期", 100: "待关闭", 101: "已关闭/停用"}
                if old_status != new_status and new_status in (2, 3, 7, 9, 100, 101):
                    try:
                        _tg_token = conn.execute("SELECT value FROM settings WHERE key='tg_bot_token'").fetchone()
                        _tg_chats = conn.execute("SELECT value FROM settings WHERE key='tg_chat_ids'").fetchone()
                        if _tg_token and _tg_chats and _tg_token["value"] and _tg_chats["value"]:
                            _status_emoji = {3: "💳", 7: "🚫", 9: "🔒"}.get(new_status, "⚠️")
                            _msg = (
                                f"{_status_emoji} <b>Mira 账户状态告警</b>\n"
                                f"账户：<code>{acc_name}</code> ({acc_act_id})\n"
                                f"状态变化：{_STATUS_LABELS.get(old_status, str(old_status))} → "
                                f"<b>{_STATUS_LABELS.get(new_status, str(new_status))}</b>\n"
                                + ({2: "账户已被 Meta 标记为禁用，前端将禁止铺广告。",
                                    3: "请检查付款方式，及时充值或更换信用卡。",
                                    7: "账户因违反政策被限制，请检查广告内容。",
                                    9: "账户处于宽限期，请检查付款或账户状态。",
                                    100: "账户处于待关闭状态，请勿继续铺广告。",
                                    101: "账户已关闭或停用，请勿继续铺广告。"}.get(new_status, ""))
                            )
                            for _cid in _tg_chats["value"].split(","):
                                _cid = _cid.strip()
                                if _cid:
                                    try:
                                        requests.post(
                                            f"https://api.telegram.org/bot{_tg_token['value']}/sendMessage",
                                            json={"chat_id": _cid, "text": _msg, "parse_mode": "HTML"},
                                            timeout=10
                                        )
                                    except Exception:
                                        pass
                            logger.warning(f"[AccountSync] 账户 {acc_act_id} 状态变化: {old_status}→{new_status}，已发TG提醒")
                    except Exception as _tg_err:
                        logger.warning(f"[AccountSync] TG提醒发送失败: {_tg_err}")
            conn.execute("""
                UPDATE accounts
                SET account_status=?, balance=?, amount_spent=?,
                    spend_cap=?, spending_limit=?,
                    currency=COALESCE(?, currency),
                    timezone=COALESCE(?, timezone),
                    timezone_name=COALESCE(?, timezone_name),
                    timezone_offset_hours_utc=COALESCE(?, timezone_offset_hours_utc),
                    updated_at=datetime('now')
                WHERE id=?
            """, (
                new_status,
                data.get("balance"),
                data.get("amount_spent"),
                data.get("spend_cap"),
                data.get("spend_cap"),
                data.get("currency"),
                data.get("timezone_name"),
                data.get("timezone_name"),
                data.get("timezone_offset_hours_utc"),
                row["id"]
            ))
            updated += 1
        conn.commit()
        conn.close()
        if updated:
            logger.info(f"[AccountSync] 已同步 {updated} 个账户状态")
        if read_errors:
            logger.warning(f"[AccountSync] {read_errors} 个账户因 token/权限错误未同步，已保留本地状态")
    except Exception as e:
        logger.error(f"账户状态同步任务异常: {e}", exc_info=True)


def run_op_heartbeat():
    """v3.0: 定期检测所有账户的操作号心跳"""
    try:
        from services.token_manager import run_heartbeat_check
        from core.database import get_conn
        conn = get_conn()
        act_ids = [r[0] for r in conn.execute(
            "SELECT DISTINCT act_id FROM account_op_tokens WHERE status='active'"
        ).fetchall()]
        conn.close()
        for act_id in act_ids:
            try:
                result = run_heartbeat_check(act_id)
                if result["dead"] > 0:
                    logger.warning(f"[Heartbeat] 账户 {act_id}: {result['dead']} 个操作号心跳失败")
            except Exception as e:
                logger.error(f"[Heartbeat] 账户 {act_id} 检测异常: {e}")
    except Exception as e:
        logger.error(f"操作号心跳检测异常: {e}", exc_info=True)



def run_token_account_discovery():
    """
    全局 Token-账户自动发现：
    扫描所有活跃 Token × 所有已导入账户，
    调用 FB API 确认权限，自动更新 account_op_tokens。
    不依赖导入来源，完全动态发现。
    """
    import requests as _req
    from core.database import decrypt_token as _decrypt_token
    _FB_API_BASE = 'https://graph.facebook.com/v25.0'
    def _fetch_all_adaccount_ids(token: str):
        ids = set()
        next_url = f"{_FB_API_BASE}/me/adaccounts"
        params = {"access_token": token, "fields": "id", "limit": 200}
        seen_next = set()
        for _ in range(100):
            resp = _req.get(next_url, params=params, timeout=30)
            data = resp.json()
            if "error" in data:
                raise RuntimeError(data["error"].get("message", "Facebook API 未知错误"))
            for item in data.get("data", []):
                item_id = item.get("id")
                if item_id:
                    ids.add(item_id)
            next_url = data.get("paging", {}).get("next")
            if not next_url:
                break
            if next_url in seen_next:
                logger.warning("[TokenDiscover] 检测到重复分页链接，停止继续翻页: %s", next_url)
                break
            seen_next.add(next_url)
            params = {}
        return ids
    logger.info("[TokenDiscover] 开始全局 Token-账户自动发现扫描...")
    try:
        c = get_conn()
        ensure_token_source_columns(c)
        # 获取所有活跃 Token；操作号仅允许 system user 参与自动发现
        all_tokens = c.execute(
            """
            SELECT id, token_alias, token_type, token_source, access_token_enc
            FROM fb_tokens
            WHERE status='active'
              AND (
                token_type != 'operate'
                OR token_source = ?
              )
            """,
            (TOKEN_SOURCE_SYSTEM_USER,),
        ).fetchall()
        # 获取所有已导入账户
        all_accounts = c.execute("SELECT id, act_id, name FROM accounts").fetchall()
        c.close()
        if not all_tokens or not all_accounts:
            logger.info("[TokenDiscover] 无 Token 或无账户，跳过")
            return
        all_act_ids = {a["act_id"] for a in all_accounts}
        total_new = 0
        total_removed = 0
        for tk in all_tokens:
            token_id = tk["id"]
            token_alias = tk["token_alias"]
            try:
                plain = _decrypt_token(tk["access_token_enc"])
                if not plain:
                    continue
                fb_act_ids = _fetch_all_adaccount_ids(plain)
                # 与系统已导入账户取交集
                matched_act_ids = all_act_ids & fb_act_ids
                c2 = get_conn()
                try:
                    # 获取该 Token 当前已关联的账户
                    existing_rows = c2.execute(
                        "SELECT act_id, status, note FROM account_op_tokens WHERE token_id=?", (token_id,)
                    ).fetchall()
                    existing_links = {r["act_id"] for r in existing_rows}
                    existing_status = {
                        r["act_id"]: {"status": r["status"], "note": r["note"] or ""}
                        for r in existing_rows
                    }
                    # 新增：FB 有权限但 account_op_tokens 中没有记录的
                    to_add = matched_act_ids - existing_links
                    for act_id in to_add:
                        max_pri = c2.execute(
                            "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?", (act_id,)
                        ).fetchone()[0] or 0
                        c2.execute(
                            """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                               VALUES (?, ?, ?, 'active', '自动发现', (SELECT token_type FROM fb_tokens WHERE id=?), datetime('now'))""",
                            (act_id, token_id, max_pri + 1, token_id)
                        )
                        total_new += 1
                    for act_id in matched_act_ids & existing_links:
                        meta = existing_status.get(act_id, {})
                        note = str(meta.get("note") or "")
                        if meta.get("status") == "active":
                            continue
                        if not (note.startswith("自动发现") or "权限已撤销" in note):
                            continue
                        restored = c2.execute(
                            """UPDATE account_op_tokens
                               SET status='active', note='自动发现-权限已恢复'
                               WHERE act_id=? AND token_id=? AND status!='active'""",
                            (act_id, token_id)
                        )
                        if restored.rowcount:
                            total_new += 1
                    # 标记失效：account_op_tokens 中有记录但 FB 已无权限的（仅标记 disabled，不删除）
                    to_disable = existing_links - matched_act_ids
                    for act_id in to_disable:
                        # 只标记 active 状态的记录为 disabled（已经 disabled 的不重复操作）
                        disabled = c2.execute(
                            """UPDATE account_op_tokens SET status='disabled', note='自动发现-权限已撤销'
                               WHERE act_id=? AND token_id=? AND status='active'""",
                            (act_id, token_id)
                        )
                        if disabled.rowcount:
                            total_removed += 1
                    c2.commit()
                    logger.info(f"[TokenDiscover] Token[{token_alias}] 新增 {len(to_add)} 个，失效 {len(to_disable)} 个")
                except Exception as e2:
                    c2.rollback()
                    logger.error(f"[TokenDiscover] Token[{token_alias}] 写入失败: {e2}")
                finally:
                    c2.close()
            except Exception as e1:
                logger.error(f"[TokenDiscover] Token[{token_alias}] 扫描失败: {e1}")
        logger.info(f"[TokenDiscover] 扫描完成，共新增 {total_new} 条关联，失效 {total_removed} 条")
    except Exception as e0:
        logger.error(f"[TokenDiscover] 整体扫描失败: {e0}")

def run_creative_task_retry():
    """
    修复: 自动重试卡死的 creative_tasks
    对 status='failed' 且超过 1 小时的任务自动重试（最多 3 次）
    """
    try:
        conn = get_conn()
        # 查找 failed 且重试次数 < 3 的任务
        rows = conn.execute("""
            SELECT id, task_type, payload
            FROM creative_tasks
            WHERE status='failed'
              AND (retry_count IS NULL OR retry_count < 3)
              AND updated_at < datetime('now', '-1 hour')
            ORDER BY id ASC LIMIT 10
        """).fetchall()
        conn.close()
        if not rows:
            return
        logger.info(f"[CreativeRetry] 发现 {len(rows)} 个失败任务，开始重试...")
        for row in rows:
            try:
                conn2 = get_conn()
                # 重置为 pending 并记录重试次数
                conn2.execute("""
                    UPDATE creative_tasks
                    SET status='pending',
                        retry_count=COALESCE(retry_count, 0) + 1,
                        updated_at=datetime('now')
                    WHERE id=?
                """, (row["id"],))
                conn2.commit()
                conn2.close()
                logger.info(f"[CreativeRetry] 任务 {row['id']} ({row['task_type']}) 已重置为 pending")
            except Exception as re:
                logger.error(f"[CreativeRetry] 任务 {row['id']} 重试失败: {re}")
    except Exception as e:
        logger.error(f"creative_task 重试任务异常: {e}", exc_info=True)


def run_currency_rate_refresh():
    """
    修复: 每日自动更新汇率表 currency_rates
    使用免费 exchangerate-api.com API（无需注册）
    """
    import requests as _req
    try:
        resp = _req.get(
            "https://open.er-api.com/v6/latest/USD",
            timeout=15
        )
        data = resp.json()
        if data.get("result") != "success":
            logger.warning(f"[CurrencyRefresh] API 返回异常: {data.get('result')}")
            return
        rates = data.get("rates", {})
        if not rates:
            return
        conn = get_conn()
        # 确保表存在
        conn.execute("""
            CREATE TABLE IF NOT EXISTS currency_rates (
                currency TEXT PRIMARY KEY,
                rate REAL,
                updated_at TEXT
            )
        """)
        updated = 0
        for currency, rate in rates.items():
            conn.execute("""
                INSERT INTO currency_rates (currency, rate, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(currency) DO UPDATE SET rate=excluded.rate, updated_at=excluded.updated_at
            """, (currency, rate))
            updated += 1
        conn.commit()
        conn.close()
        logger.info(f"[CurrencyRefresh] 成功更新 {updated} 种货币汇率")
    except Exception as e:
        logger.error(f"汇率更新任务异常: {e}", exc_info=True)


def run_score_correlation():
    """v3.5: 每日凌晨2:30执行 AI评分 vs 性能数据 反馈环"""
    try:
        from services.smart_scorer import _correlate_with_performance
        _correlate_with_performance()
    except Exception as e:
        logger.error(f"评分反馈环任务异常: {e}", exc_info=True)


def run_warmup_check():
    def _work():
        from services.warmup_engine import check_and_warmup
        return check_and_warmup()

    return _run_tracked_job("warmup_check", "预热扫描", _work, lock=_warmup_lock)


def start_scheduler():
    global _scheduler
    try:
        interval_min = int(_get_setting("inspect_interval", "30"))
    except (ValueError, TypeError):
        interval_min = 30

    try:
        heartbeat_min = int(_get_setting("op_token_heartbeat_interval", "10"))
    except (ValueError, TypeError):
        heartbeat_min = 10

    _scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    _scheduler.add_job(run_guard, IntervalTrigger(minutes=interval_min), id="guard", replace_existing=True)
    # v3.0: 操作号心跳检测
    _scheduler.add_job(run_op_heartbeat, IntervalTrigger(minutes=heartbeat_min), id="op_heartbeat", replace_existing=True)
    # v3.0: 素材打分（每日凌晨1点）
    _scheduler.add_job(run_asset_scoring, CronTrigger(hour=1, minute=0), id="asset_scoring", replace_existing=True)
    # 存储自动清理（每天凌晨3点）
    _scheduler.add_job(run_storage_cleanup, CronTrigger(hour=3, minute=0), id="storage_cleanup", replace_existing=True)
    # Token-账户自动发现（每6小时）
    _scheduler.add_job(run_token_account_discovery, IntervalTrigger(hours=6), id="token_discover", replace_existing=True)
     # v3.3: 账户状态自动同步（30分钟）
    _scheduler.add_job(run_account_status_sync, IntervalTrigger(minutes=30), id="account_sync", replace_existing=True)
    # 修复: 汇率自动更新（每天凌晨 2 点）
    _scheduler.add_job(run_currency_rate_refresh, CronTrigger(hour=2, minute=0), id="currency_refresh", replace_existing=True)
    # v3.5: AI评分 vs 性能数据反馈环（每日凌晨2:30）
    _scheduler.add_job(run_score_correlation, CronTrigger(hour=2, minute=30), id="score_correlation", replace_existing=True)
    # v3.4: FB 广告数据回流（每小时拉取一次）
    def _run_metrics_sync():
        try:
            from services.metrics_sync import run_metrics_sync
            run_metrics_sync()
        except Exception as e:
            logger.error(f"metrics_sync 异常: {e}")
    _scheduler.add_job(_run_metrics_sync, IntervalTrigger(hours=1), id="metrics_sync", replace_existing=True)
    # 哨兵扫描 — 间隔由 sentinel_interval 控制（默认 3 分钟）
    try:
        sentinel_min = int(_get_setting("sentinel_interval", "3"))
    except (ValueError, TypeError):
        sentinel_min = 3
    try:
        hb_timeout = int(_get_setting("heartbeat_timeout", "30"))
    except (ValueError, TypeError):
        hb_timeout = 30
    hb_interval = max(1, hb_timeout // 3)
    from services.guard_engine import sentinel_patrol, heartbeat_check
    def _run_sentinel_patrol():
        return _run_tracked_job("sentinel_patrol", "哨兵扫描", sentinel_patrol)

    def _run_heartbeat_check():
        return _run_tracked_job("heartbeat_check", "心跳守护", heartbeat_check)

    _scheduler.add_job(_run_sentinel_patrol, IntervalTrigger(minutes=sentinel_min), id="sentinel_patrol", replace_existing=True)
    _scheduler.add_job(_run_heartbeat_check, IntervalTrigger(minutes=hb_interval), id="heartbeat_check", replace_existing=True)
    # v3.10: 账户预热检查
    try:
        warmup_interval = int(_get_setting("warmup_check_interval", "30"))
    except (ValueError, TypeError):
        warmup_interval = 30
    _scheduler.add_job(run_warmup_check, IntervalTrigger(minutes=warmup_interval), id="warmup_check", replace_existing=True)
    _scheduler.start()
    logger.info(f"调度器启动 v3.10.0: 安检间隔={interval_min}分钟 | 操作号心跳={heartbeat_min}分钟 | 素材打分=每日1点 | 账户状态同步=30分钟 | Token自动发现=6小时 | 汇率更新=每日2点 | 数据回流=每小时 | 评分反馈环=每日2:30 | 哨兵={sentinel_min}分钟 | 心跳={hb_interval}分钟 | 预热={warmup_interval}分钟")

def trigger_guard_now():
    """手动触发一次巡检（异步）"""
    t = threading.Thread(target=run_guard, daemon=True)
    t.start()
    return True
