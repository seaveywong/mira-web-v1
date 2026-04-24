"""
api/autopilot.py - 全自动铺放系统 API
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from core.auth import get_current_user
from core.database import get_conn
import logging
import threading
router = APIRouter()
logger = logging.getLogger("mira.autopilot_api")
def _to_cst(dt_str):
    """将 UTC 时间字符串转为北京时间（UTC+8）"""
    if not dt_str:
        return dt_str
    try:
        from datetime import datetime, timedelta
        dt = datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
        dt_cst = dt + timedelta(hours=8)
        return dt_cst.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt_str
@router.get("/tasks")
async def get_dispatch_tasks(
    limit: int = Query(50, ge=1, le=200),
    source: str = Query("all"),
    _=Depends(get_current_user)
):
    try:
        conn = get_conn()
        if source == "manual":
            where = "ac.dispatch_source = 'manual'"
        elif source == "global_dispatcher":
            where = "ac.dispatch_source = 'global_dispatcher'"
        else:
            where = "1=1"
        rows = conn.execute(
            f"""SELECT ac.id, ac.act_id, ac.asset_id, ac.status,
                ac.created_at, ac.updated_at, ac.error_msg,
                ac.dispatch_source, ac.objective, ac.conversion_goal,
                ac.fb_campaign_id, ac.total_adsets, ac.total_ads,
                ac.progress_msg, ac.name as campaign_name,
                ac.daily_budget, ac.message_template, ac.lead_form_id,
                ac.cta_type, ac.landing_url, ac.target_countries,
                aa.file_name as asset_name,
                a.name as act_name
               FROM auto_campaigns ac
               LEFT JOIN ad_assets aa ON aa.id = ac.asset_id
               LEFT JOIN accounts a ON a.act_id = ac.act_id
               WHERE {where}
               ORDER BY ac.id DESC
               LIMIT ?""",
            (limit,)
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            d["created_at"] = _to_cst(d.get("created_at"))
            d["updated_at"] = _to_cst(d.get("updated_at"))
            result.append(d)
        return result
    except Exception as e:
        logger.error(f"get_dispatch_tasks error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@router.get("/dispatch-log")
async def get_dispatch_log(
    limit: int = Query(20, ge=1, le=100),
    _=Depends(get_current_user)
):
    try:
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM dispatch_log ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/dispatch-now")
async def trigger_dispatch_now(_=Depends(get_current_user)):
    try:
        from services.global_dispatcher import run_dispatch
        def _run():
            try:
                result = run_dispatch(force=True)
                logger.info(f"manual dispatch result: {result}")
            except Exception as e:
                logger.error(f"manual dispatch error: {e}")
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "message": "调度已触发，后台执行中"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/toggle-dispatch")
async def toggle_dispatch(body: dict, _=Depends(get_current_user)):
    """启用或禁用全局自动调度"""
    try:
        enabled = body.get("enabled", False)
        val = "1" if enabled else "0"
        conn = get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('global_dispatch_enabled', ?)",
            (val,)
        )
        conn.commit()
        conn.close()
        return {"ok": True, "enabled": enabled, "message": "全局调度已" + ("启用" if enabled else "停用")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.get("/status")
async def get_autopilot_status(_=Depends(get_current_user)):
    try:
        conn = get_conn()
        setting = conn.execute(
            "SELECT value FROM settings WHERE key='global_dispatch_enabled'"
        ).fetchone()
        dispatch_enabled = bool(setting and setting["value"] == "1")
        lifecycle_stats = {}
        for r in conn.execute(
            "SELECT COALESCE(lifecycle_stage, 'new') as lifecycle_stage, COUNT(*) as cnt FROM accounts WHERE enabled=1 GROUP BY lifecycle_stage"
        ).fetchall():
            lifecycle_stats[r["lifecycle_stage"]] = r["cnt"]
        grade_stats = {}
        for r in conn.execute(
            "SELECT ai_grade, COUNT(*) as cnt FROM ad_assets WHERE upload_status IN ('ai_done','approved') GROUP BY ai_grade"
        ).fetchall():
            grade_stats[r["ai_grade"] or "unscored"] = r["cnt"]
        recent = conn.execute(
            "SELECT COUNT(*) as cnt FROM auto_campaigns WHERE dispatch_source='global_dispatcher'"
        ).fetchone()
        # 最近任务统计
        task_stats = {}
        for r in conn.execute(
            "SELECT status, COUNT(*) as cnt FROM auto_campaigns GROUP BY status"
        ).fetchall():
            task_stats[r["status"]] = r["cnt"]
        conn.close()
        return {
            "dispatch_enabled": dispatch_enabled,
            "lifecycle_stats": lifecycle_stats,
            "grade_stats": grade_stats,
            "total_dispatch_tasks": recent["cnt"] if recent else 0,
            "task_stats": task_stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/warmup/{act_id}")
async def trigger_warmup(act_id: str, _=Depends(get_current_user)):
    """手动触发账户预热"""
    try:
        from services.lifecycle_manager import start_warmup
        result = start_warmup(act_id)
        if result.get("success"):
            return {"ok": True, "message": f"账户 {act_id} 预热已启动", "data": result}
        else:
            raise HTTPException(status_code=400, detail=result.get("msg", "预热启动失败"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.get("/warmup-status")
async def get_warmup_status(_=Depends(get_current_user)):
    """获取所有账户的预热状态"""
    try:
        conn = get_conn()
        rows = conn.execute(
            """SELECT a.act_id, a.name, COALESCE(a.lifecycle_stage, 'new') as lifecycle_stage,
                      a.enabled,
                      (SELECT COUNT(*) FROM warmup_campaigns wc WHERE wc.act_id=a.act_id) as warmup_count,
                      (SELECT MAX(wc.created_at) FROM warmup_campaigns wc WHERE wc.act_id=a.act_id) as last_warmup
               FROM accounts a
               WHERE a.enabled=1
               ORDER BY a.lifecycle_stage, a.act_id"""
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("last_warmup"):
                try:
                    from datetime import datetime, timedelta
                    dt = datetime.strptime(d["last_warmup"][:19], "%Y-%m-%d %H:%M:%S")
                    d["last_warmup"] = (dt + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
            result.append(d)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/launch-overview")
async def get_launch_overview(_=Depends(get_current_user)):
    """
    铺放中心全景接口：返回每个账户的完整铺放状态
    包含：账户基础信息、缺失配置检查、AI托管状态、最近铺放任务、当前动作
    """
    try:
        conn = get_conn()

        # 1. 获取所有账户的完整信息（含禁用账户，铺放中心需要全景视图）
        accounts = conn.execute("""
            SELECT a.id, a.act_id, a.name, a.currency, a.timezone_name,
                   COALESCE(a.ai_managed, 0) as ai_managed,
                   COALESCE(a.auto_dispatch, 0) as auto_dispatch,
                   COALESCE(a.lifecycle_stage, 'new') as lifecycle_stage,
                   COALESCE(a.enabled, 0) as enabled,
                   a.page_id, a.pixel_id, a.landing_url,
                   a.target_countries, a.target_objective, a.target_objective_type,
                   a.account_status, a.balance_usd, a.available_balance,
                   a.token_alias, a.token_status,
                   a.warmup_completed_at,
                   t.status as token_status_live
            FROM accounts a
            LEFT JOIN fb_tokens t ON t.id = a.token_id
            ORDER BY a.ai_managed DESC, a.enabled DESC, a.lifecycle_stage, a.name
        """).fetchall()

        # 2. 获取每个账户最近3条铺放任务
        recent_tasks = conn.execute("""
            SELECT ac.act_id, ac.id, ac.name, ac.status, ac.dispatch_source,
                   ac.created_at, ac.error_msg, ac.total_adsets, ac.total_ads,
                   ac.objective, ac.progress_step, ac.progress_msg
            FROM auto_campaigns ac
            WHERE ac.id IN (
                SELECT id FROM auto_campaigns ac2
                WHERE ac2.act_id = ac.act_id
                ORDER BY ac2.id DESC LIMIT 3
            )
            ORDER BY ac.act_id, ac.id DESC
        """).fetchall()

        # 3. 获取每个账户当前运行中的任务
        running_tasks = conn.execute("""
            SELECT act_id, id, name, status, dispatch_source, created_at,
                   progress_step, progress_msg
            FROM auto_campaigns
            WHERE status = 'running'
        """).fetchall()

        # 4. 获取全局dispatch开关状态
        dispatch_enabled_row = conn.execute(
            "SELECT value FROM settings WHERE key='global_dispatch_enabled'"
        ).fetchone()
        dispatch_enabled = (dispatch_enabled_row['value'] == '1') if dispatch_enabled_row else False

        conn.close()

        # 整理数据
        tasks_by_account = {}
        for t in recent_tasks:
            d = dict(t)
            act = d['act_id']
            if act not in tasks_by_account:
                tasks_by_account[act] = []
            tasks_by_account[act].append(d)

        running_by_account = {t['act_id']: dict(t) for t in running_tasks}

        result = []
        for a in accounts:
            d = dict(a)
            act_id = d['act_id']

            # 检查缺失配置
            missing = []
            if not d.get('pixel_id'):
                missing.append({'key': 'pixel_id', 'label': '像素 ID', 'severity': 'error', 'desc': '缺少像素ID，无法追踪转化事件，购物广告无法投放'})
            if not d.get('landing_url'):
                missing.append({'key': 'landing_url', 'label': '落地页 URL', 'severity': 'error', 'desc': '缺少落地页，广告无法跳转目标页面'})
            if not d.get('target_countries'):
                missing.append({'key': 'target_countries', 'label': '目标国家', 'severity': 'warning', 'desc': '未设置目标投放国家，将使用全局默认设置'})
            if not d.get('page_id'):
                missing.append({'key': 'page_id', 'label': '主页 ID', 'severity': 'error', 'desc': '缺少Facebook主页，广告无法展示品牌信息'})
            if not d.get('token_alias') or d.get('token_status_live') != 'active':
                missing.append({'key': 'token', 'label': 'Token', 'severity': 'error', 'desc': 'Token未绑定或已失效，无法操作广告账户'})

            # 判断铺放就绪状态
            error_missing = [m for m in missing if m['severity'] == 'error']
            can_launch = len(error_missing) == 0

            # 当前动作状态
            running = running_by_account.get(act_id)
            if running:
                current_action = {
                    'type': 'running',
                    'label': '铺放中',
                    'desc': running.get('progress_msg') or '正在创建广告...',
                    'task_id': running['id'],
                    'source': running.get('dispatch_source', 'manual')
                }
            elif d['ai_managed']:
                current_action = {
                    'type': 'ai_managed',
                    'label': 'AI托管中',
                    'desc': 'AI正在自动监控和优化广告',
                    'task_id': None,
                    'source': 'ai'
                }
            elif not can_launch:
                current_action = {
                    'type': 'config_missing',
                    'label': '配置不完整',
                    'desc': '缺少必要配置：' + '、'.join([m['label'] for m in error_missing]),
                    'task_id': None,
                    'source': None
                }
            elif d['auto_dispatch'] and dispatch_enabled:
                current_action = {
                    'type': 'waiting_dispatch',
                    'label': '等待自动铺放',
                    'desc': '智能铺放已开启，等待下次调度',
                    'task_id': None,
                    'source': 'auto'
                }
            else:
                current_action = {
                    'type': 'idle',
                    'label': '空闲',
                    'desc': '可手动铺放广告',
                    'task_id': None,
                    'source': None
                }

            # 最近任务统计
            recent = tasks_by_account.get(act_id, [])
            last_task = recent[0] if recent else None

            d['missing_configs'] = missing
            d['can_launch'] = can_launch
            d['current_action'] = current_action
            d['recent_tasks'] = recent[:3]
            d['last_task'] = last_task
            d['dispatch_enabled'] = dispatch_enabled
            result.append(d)

        return {
            'accounts': result,
            'dispatch_enabled': dispatch_enabled,
            'total': len(result),
            'ready': sum(1 for a in result if a['can_launch']),
            'missing_config': sum(1 for a in result if not a['can_launch']),
            'ai_managed': sum(1 for a in result if a['ai_managed']),
            'running': len(running_by_account)
        }
    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=str(e) + '\n' + traceback.format_exc())


# ─────────────────────────────────────────────────────────────────────────────
# 账户配置更新接口
# ─────────────────────────────────────────────────────────────────────────────

from pydantic import BaseModel
from typing import Optional

class AccountConfigUpdate(BaseModel):
    pixel_id: Optional[str] = None
    page_id: Optional[str] = None
    landing_url: Optional[str] = None
    target_countries: Optional[str] = None
    target_objective: Optional[str] = None
    target_objective_type: Optional[str] = None
    bm_id: Optional[str] = None
    ai_managed: Optional[int] = None
    auto_dispatch: Optional[int] = None


@router.post("/account-config")
async def update_account_config(
    act_id: str,
    body: AccountConfigUpdate,
    _=Depends(get_current_user)
):
    """
    更新账户配置（pixel_id、page_id、landing_url 等）
    前端铺放中心「补全配置」面板使用
    """
    try:
        conn = get_conn()
        # 检查账户是否存在
        row = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail=f"账户 {act_id} 不存在")

        updates = {}
        if body.pixel_id is not None:
            updates['pixel_id'] = body.pixel_id.strip() or None
        if body.page_id is not None:
            updates['page_id'] = body.page_id.strip() or None
        if body.landing_url is not None:
            updates['landing_url'] = body.landing_url.strip() or None
        if body.target_countries is not None:
            updates['target_countries'] = body.target_countries
        if body.target_objective is not None:
            updates['target_objective'] = body.target_objective
        if body.target_objective_type is not None:
            updates['target_objective_type'] = body.target_objective_type
        if body.bm_id is not None:
            updates['bm_id'] = body.bm_id.strip() or None
        if body.ai_managed is not None:
            updates['ai_managed'] = body.ai_managed
        if body.auto_dispatch is not None:
            updates['auto_dispatch'] = body.auto_dispatch

        if not updates:
            conn.close()
            return {"ok": True, "message": "无需更新"}

        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [act_id]
        conn.execute(f"UPDATE accounts SET {set_clause} WHERE act_id=?", values)
        conn.commit()
        conn.close()

        logger.info(f"[AccountConfig] 账户 {act_id} 配置已更新: {list(updates.keys())}")
        return {"ok": True, "message": f"账户配置已更新（{', '.join(updates.keys())}）", "updated": list(updates.keys())}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# 自动匹配像素/主页接口
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/auto-match")
async def auto_match_assets(
    act_id: str,
    force: bool = False,
    _=Depends(get_current_user)
):
    """
    自动匹配账户的可用像素和主页（智能版）：
    1. 使用 TokenManager 获取解密后的 Token（支持矩阵内轮询）
    2. 调用 FB Graph API 拉取账户下的像素列表
    3. 主页优先从 tw_certified_pages 按矩阵匹配，再从 /me/accounts 获取
    4. force=True 时强制覆盖已有值；默认只填充空值
    5. 自动写入数据库并返回匹配结果
    """
    import requests as _req
    from services.token_manager import get_exec_token, get_matrix_tokens
    try:
        conn = get_conn()
        acc = conn.execute("SELECT * FROM accounts WHERE act_id=?", (act_id,)).fetchone()
        if not acc:
            conn.close()
            raise HTTPException(status_code=404, detail=f"账户 {act_id} 不存在")
        acc = dict(acc)
        matrix_id = acc.get("matrix_id") or conn.execute(
            "SELECT matrix_id FROM fb_tokens WHERE id=?", (acc.get("token_id"),)
        ).fetchone()
        if matrix_id and not isinstance(matrix_id, int):
            matrix_id = matrix_id["matrix_id"]

        # ── 获取可用 Token（优先当前账户绑定的，失败则轮询矩阵内其他） ──
        token = get_exec_token(act_id)
        if not token:
            # 尝试矩阵内其他 Token
            matrix_tokens = get_matrix_tokens(act_id)
            for mt in matrix_tokens:
                if mt.get("plain_token"):
                    token = mt["plain_token"]
                    break
        conn.close()

        if not token:
            raise HTTPException(status_code=400, detail="账户无可用 Token，请先绑定有效 Token")

        result = {
            "act_id": act_id,
            "matched": {},
            "candidates": {"pixels": [], "pages": []},
            "errors": [],
            "auto_applied": []
        }

        # ── 1. 拉取可用像素 ──────────────────────────────────────────────────
        try:
            pixel_resp = _req.get(
                f"https://graph.facebook.com/v19.0/{act_id}/adspixels",
                params={"fields": "id,name,last_fired_time,is_unavailable", "access_token": token, "limit": 50},
                timeout=10
            )
            if pixel_resp.status_code == 200:
                pixels = pixel_resp.json().get("data", [])
                # 过滤掉不可用的
                pixels = [p for p in pixels if not p.get("is_unavailable")]
                result["candidates"]["pixels"] = [
                    {"id": p["id"], "name": p.get("name", p["id"]), "last_fired": p.get("last_fired_time", "")}
                    for p in pixels
                ]
                # 自动选择：优先选最近有触发的
                if pixels:
                    best_pixel = sorted(pixels, key=lambda p: p.get("last_fired_time", ""), reverse=True)[0]
                    result["matched"]["pixel_id"] = best_pixel["id"]
                    result["matched"]["pixel_name"] = best_pixel.get("name", best_pixel["id"])
            else:
                err_msg = pixel_resp.json().get("error", {}).get("message", pixel_resp.text[:100])
                result["errors"].append(f"像素查询失败: {err_msg}")
        except Exception as e:
            result["errors"].append(f"像素查询异常: {str(e)}")

        # ── 2. 拉取可用主页（优先矩阵认证主页，再从 /me/accounts） ──────────
        try:
            # 优先：从 tw_certified_pages 按矩阵获取
            conn2 = get_conn()
            if matrix_id:
                tw_pages = conn2.execute(
                    "SELECT page_id, page_name FROM tw_certified_pages WHERE matrix_id=?",
                    (matrix_id,)
                ).fetchall()
                if tw_pages:
                    result["candidates"]["pages"] = [
                        {"id": p["page_id"], "name": p["page_name"], "source": "tw_certified"}
                        for p in tw_pages
                    ]
                    # 自动选第一个认证主页
                    result["matched"]["page_id"] = tw_pages[0]["page_id"]
                    result["matched"]["page_name"] = tw_pages[0]["page_name"]
                    result["matched"]["page_source"] = "tw_certified"
            conn2.close()

            # 如果没有认证主页，从 /me/accounts 获取
            if not result["candidates"]["pages"]:
                page_resp = _req.get(
                    "https://graph.facebook.com/v19.0/me/accounts",
                    params={"fields": "id,name,fan_count,category", "access_token": token, "limit": 50},
                    timeout=10
                )
                if page_resp.status_code == 200:
                    pages = page_resp.json().get("data", [])
                    result["candidates"]["pages"] = [
                        {"id": p["id"], "name": p.get("name", p["id"]), "fans": p.get("fan_count", 0), "source": "me_accounts"}
                        for p in pages
                    ]
                    if pages:
                        best_page = sorted(pages, key=lambda p: p.get("fan_count", 0), reverse=True)[0]
                        result["matched"]["page_id"] = best_page["id"]
                        result["matched"]["page_name"] = best_page.get("name", best_page["id"])
                        result["matched"]["page_source"] = "me_accounts"
                else:
                    # 尝试通过广告账户关联主页
                    page_resp2 = _req.get(
                        f"https://graph.facebook.com/v19.0/{act_id}/promote_pages",
                        params={"fields": "id,name,fan_count", "access_token": token, "limit": 50},
                        timeout=10
                    )
                    if page_resp2.status_code == 200:
                        pages = page_resp2.json().get("data", [])
                        result["candidates"]["pages"] = [
                            {"id": p["id"], "name": p.get("name", p["id"]), "fans": p.get("fan_count", 0), "source": "promote_pages"}
                            for p in pages
                        ]
                        if pages:
                            best_page = sorted(pages, key=lambda p: p.get("fan_count", 0), reverse=True)[0]
                            result["matched"]["page_id"] = best_page["id"]
                            result["matched"]["page_name"] = best_page.get("name", best_page["id"])
                            result["matched"]["page_source"] = "promote_pages"
                    else:
                        err_msg = page_resp.json().get("error", {}).get("message", page_resp.text[:100])
                        result["errors"].append(f"主页查询失败: {err_msg}")
        except Exception as e:
            result["errors"].append(f"主页查询异常: {str(e)}")

        # ── 3. 自动写入数据库 ────────────────────────────────────────────────
        # force=True：强制覆盖；force=False（默认）：只填充空值
        if result["matched"]:
            conn3 = get_conn()
            updates = {}
            if result["matched"].get("pixel_id"):
                if force or not acc.get("pixel_id"):
                    updates["pixel_id"] = result["matched"]["pixel_id"]
                    result["auto_applied"].append(f"像素 {result['matched']['pixel_id']}")
            if result["matched"].get("page_id"):
                if force or not acc.get("page_id"):
                    updates["page_id"] = result["matched"]["page_id"]
                    result["auto_applied"].append(f"主页 {result['matched']['page_id']}")
            if updates:
                set_clause = ", ".join(f"{k}=?" for k in updates)
                values = list(updates.values()) + [act_id]
                conn3.execute(f"UPDATE accounts SET {set_clause} WHERE act_id=?", values)
                conn3.commit()
                logger.info(f"[AutoMatch] 账户 {act_id} 自动填充: {updates}")
            conn3.close()

        if result["auto_applied"]:
            result["message"] = f"自动匹配完成，已填充：{', '.join(result['auto_applied'])}"
        elif result["matched"]:
            result["message"] = "匹配完成，当前配置已有值（传 force=true 可强制覆盖）"
        else:
            result["message"] = "未找到可用像素或主页，请手动配置"
        return result
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=str(e) + "\n" + traceback.format_exc())

@router.post("/auto-match-all")
async def auto_match_all_accounts(_=Depends(get_current_user)):
    """
    批量自动匹配所有账户的像素/主页（跳过已有配置的账户）
    """
    try:
        conn = get_conn()
        accounts = conn.execute(
            "SELECT act_id FROM accounts WHERE enabled=1"
        ).fetchall()
        conn.close()

        results = []
        for acc_row in accounts:
            act_id = acc_row["act_id"]
            try:
                # 复用单账户匹配逻辑（内部函数调用）
                from services.token_manager import get_exec_token as _get_exec_token_all
                conn2 = get_conn()
                acc = conn2.execute("SELECT * FROM accounts WHERE act_id=?", (act_id,)).fetchone()
                conn2.close()
                if not acc:
                    results.append({"act_id": act_id, "status": "skip", "reason": "账户不存在"})
                    continue
                acc = dict(acc)
                token_check = _get_exec_token_all(act_id)
                if not token_check:
                    results.append({"act_id": act_id, "status": "skip", "reason": "无可用Token"})
                    continue
                if acc.get("pixel_id") and acc.get("page_id"):
                    results.append({"act_id": act_id, "status": "skip", "reason": "已有配置"})
                    continue

                results.append({"act_id": act_id, "status": "queued"})
            except Exception as e:
                results.append({"act_id": act_id, "status": "error", "reason": str(e)})

        return {"ok": True, "total": len(accounts), "results": results,
                "message": "批量匹配已提交，请逐个账户调用 /auto-match 获取详细结果"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
