"""
lifecycle_manager.py  v2.0
账户生命周期管理服务

生命周期阶段:
  new       → 刚绑定，未开始预热
  warmup    → 预热中（跑主页赞，固定美女素材，$5/天，1天）
  testing   → 预热完成，开始铺真实广告测试
  scaling   → 测试稳定，进入拉量阶段
  paused    → 手动暂停
  banned    → 账户被封

预热完成条件（满足任意一个）:
  1. 预热广告运行 >= warmup_days（默认1天）
  2. 预热消耗 >= warmup_budget（默认$5）

预热完成后:
  - 自动暂停预热广告
  - 账户升级为 testing
  - 全局调度器下次运行时，按账户目标配置铺真实广告
"""

import logging
import json
import threading
import time as time_module
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 内部工具函数
# ─────────────────────────────────────────────

def _get_conn():
    from core.database import get_conn
    return get_conn()


def _get_setting(key: str, default: str = "") -> str:
    try:
        conn = _get_conn()
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row[0] if row else default
    except Exception:
        return default


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _get_fb_token(act_id: str) -> str:
    """获取账户的操作 Token（通过 TokenManager，优先操作号，必要时回退管理号）"""
    try:
        from services.token_manager import get_exec_token, ACTION_CREATE, ACTION_PAUSE
        token = get_exec_token(act_id, ACTION_CREATE)
        if token:
            return token
        # CREATE 操作号不可用时，尝试用 PAUSE 级别的管理号兑底
        return get_exec_token(act_id, ACTION_PAUSE) or ""
    except Exception as e:
        logger.warning(f"[Lifecycle] 获取 Token 失败: {e}")
        return ""


def _get_warmup_asset() -> dict:
    """从预热素材库随机选一张素材"""
    try:
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM warmup_assets WHERE is_active=1 ORDER BY use_count ASC, RANDOM() LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            return dict(row)
    except Exception as e:
        logger.error(f"[Lifecycle] 获取预热素材失败: {e}")
    return {}


def _get_best_real_asset(act_id: str) -> dict:
    """获取最优真实广告素材（优先 S/A 级，未在该账户铺过的）"""
    try:
        conn = _get_conn()
        # 优先选 S/A 级，且近7天未在该账户铺过的
        row = conn.execute("""
            SELECT a.* FROM ad_assets a
            WHERE a.upload_status = 'ai_done'
              AND (a.ai_grade IN ('S','A') OR a.ai_grade IS NULL)
              AND a.id NOT IN (
                  SELECT asset_id FROM auto_campaigns
                  WHERE act_id=? AND created_at > datetime('now','-7 days')
              )
            ORDER BY 
              CASE a.ai_grade WHEN 'S' THEN 1 WHEN 'A' THEN 2 ELSE 3 END,
              a.id DESC
            LIMIT 1
        """, (act_id,)).fetchone()
        
        if not row:
            # 降级：选任意已分析的素材
            row = conn.execute("""
                SELECT * FROM ad_assets 
                WHERE upload_status = 'ai_done'
                ORDER BY id DESC LIMIT 1
            """).fetchone()
        
        conn.close()
        return dict(row) if row else {}
    except Exception as e:
        logger.error(f"[Lifecycle] 获取真实素材失败: {e}")
        return {}


def _get_account_config(act_id: str) -> dict:
    """获取账户目标配置"""
    try:
        conn = _get_conn()
        row = conn.execute("SELECT * FROM accounts WHERE act_id=?", (act_id,)).fetchone()
        conn.close()
        if row:
            d = dict(row)
            # 解析 JSON 字段，提供默认值
            try:
                d['target_countries'] = json.loads(d.get('target_countries') or '["TW"]')
            except Exception:
                d['target_countries'] = ["TW"]
            try:
                d['target_placements'] = json.loads(d.get('target_placements') or '["feed"]')
            except Exception:
                d['target_placements'] = ["feed"]
            return d
    except Exception as e:
        logger.error(f"[Lifecycle] 获取账户配置失败: {e}")
    return {}


def _pause_fb_campaign(fb_campaign_id: str, token: str) -> bool:
    """暂停 Facebook 广告系列"""
    try:
        import requests
        resp = requests.post(
            f"https://graph.facebook.com/v19.0/{fb_campaign_id}",
            data={"status": "PAUSED", "access_token": token},
            timeout=10
        )
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"[Lifecycle] 暂停广告失败: {e}")
        return False


def _get_campaign_spend(fb_campaign_id: str, token: str) -> float:
    """获取广告系列的总消耗"""
    try:
        import requests
        resp = requests.get(
            f"https://graph.facebook.com/v19.0/{fb_campaign_id}/insights",
            params={
                "fields": "spend",
                "date_preset": "lifetime",
                "access_token": token
            },
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                return float(data[0].get("spend", 0))
    except Exception as e:
        logger.error(f"[Lifecycle] 获取消耗失败: {e}")
    return 0.0


# ─────────────────────────────────────────────
# 公开 API
# ─────────────────────────────────────────────

def get_account_lifecycle(act_id: str) -> dict:
    """获取账户生命周期信息"""
    try:
        conn = _get_conn()
        row = conn.execute(
            "SELECT lifecycle_stage, lifecycle_updated_at FROM accounts WHERE act_id=?",
            (act_id,)
        ).fetchone()
        conn.close()
        if row:
            return {
                "act_id": act_id,
                "stage": row[0] or "new",
                "updated_at": row[1]
            }
    except Exception as e:
        logger.error(f"[Lifecycle] get_account_lifecycle 失败: {e}")
    return {"act_id": act_id, "stage": "new", "updated_at": None}


def set_account_lifecycle(act_id: str, stage: str) -> bool:
    """手动设置账户生命周期阶段"""
    valid_stages = ["new", "warmup", "testing", "scaling", "paused", "banned"]
    if stage not in valid_stages:
        return False
    try:
        conn = _get_conn()
        conn.execute(
            "UPDATE accounts SET lifecycle_stage=?, lifecycle_updated_at=? WHERE act_id=?",
            (stage, _now_str(), act_id)
        )
        conn.commit()
        conn.close()
        logger.info(f"[Lifecycle] 账户 {act_id} 手动设置为 {stage}")
        return True
    except Exception as e:
        logger.error(f"[Lifecycle] set_account_lifecycle 失败: {e}")
        return False


def start_warmup(act_id: str) -> dict:
    """
    启动账户预热：
    1. 从预热素材库随机选一张美女图片
    2. 在 auto_campaigns 表创建预热任务（OUTCOME_ENGAGEMENT + page_likes）
    3. 后台线程调用 AutopilotEngine 创建 Facebook 广告
    4. 更新账户状态为 warmup
    """
    logger.info(f"[Lifecycle] 开始预热账户 {act_id}")
    
    # 检查是否已在预热中
    conn = _get_conn()
    existing = conn.execute(
        "SELECT id FROM warmup_campaigns WHERE act_id=? AND status='running'",
        (act_id,)
    ).fetchone()
    conn.close()
    if existing:
        return {"success": False, "msg": "账户已在预热中"}
    
    # 获取预热素材
    warmup_asset = _get_warmup_asset()
    if not warmup_asset:
        return {"success": False, "msg": "预热素材库为空，请先上传预热素材"}
    
    # 获取账户配置
    acc = _get_account_config(act_id)
    # 预热门槛：至少运行 3 天且消耗 $10（避免 $5/1天 门槛过低）
    warmup_days = int(acc.get('warmup_days') or _get_setting('warmup_days_default', '3'))
    warmup_budget = float(acc.get('warmup_budget') or _get_setting('warmup_budget_default', '10'))
    warmup_daily = float(acc.get('warmup_daily_budget') or _get_setting('warmup_daily_budget', '5'))
    
    # 获取账户的 page_id
    page_id = acc.get('page_id') or acc.get('fb_page_id') or ""
    
    # 在 warmup_campaigns 表记录预热任务
    conn = _get_conn()
    try:
        # 先把预热素材注册为 ad_assets 中的一条临时记录（如果不存在）
        # 检查是否已有对应记录
        existing_asset = conn.execute(
            "SELECT id FROM ad_assets WHERE file_path=?",
            (warmup_asset['file_path'],)
        ).fetchone()
        
        if existing_asset:
            asset_id = existing_asset[0]
        else:
            # 插入预热素材为 ad_assets 临时记录
            conn.execute("""
                INSERT INTO ad_assets (filename, file_path, thumb_path, upload_status, asset_type, ad_text, ad_title)
                VALUES (?, ?, ?, 'warmup', 'image', '关注我们获取更多精彩内容', '精彩生活')
            """, (
                warmup_asset['filename'],
                warmup_asset['file_path'],
                warmup_asset.get('thumb_path', ''),
            ))
            asset_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        
        # 创建 auto_campaigns 预热任务
        campaign_name = f"[预热] {act_id} {datetime.now().strftime('%m%d%H%M')}"
        conn.execute("""
            INSERT INTO auto_campaigns 
            (act_id, asset_id, name, objective, conversion_goal, target_countries,
             daily_budget, age_min, age_max, gender, placements,
             status, created_by, dispatch_source, lifecycle_stage_at_launch)
            VALUES (?, ?, ?, 'OUTCOME_ENGAGEMENT', 'page_likes', '[]',
                    ?, 18, 65, 0, NULL,
                    'pending', 'lifecycle', 'warmup', 'warmup')
        """, (act_id, asset_id, campaign_name, warmup_daily))
        
        campaign_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        
        # 记录到 warmup_campaigns 表
        conn.execute("""
            INSERT INTO warmup_campaigns 
            (act_id, status, daily_budget, target_countries, objective)
            VALUES (?, 'running', ?, '[]', 'PAGE_LIKES')
        """, (act_id, warmup_daily))
        
        warmup_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        
        # 更新预热素材使用次数
        conn.execute("UPDATE warmup_assets SET use_count=use_count+1 WHERE id=?", (warmup_asset['id'],))
        
        # 更新账户状态为 warmup
        conn.execute(
            "UPDATE accounts SET lifecycle_stage='warmup', lifecycle_updated_at=? WHERE act_id=?",
            (_now_str(), act_id)
        )
        
        conn.commit()
        conn.close()
        
        logger.info(f"[Lifecycle] 账户 {act_id} 预热任务已创建: campaign_id={campaign_id}, warmup_id={warmup_id}")
        
        # 后台线程执行广告创建
        def _run_warmup_campaign():
            try:
                from services.autopilot_engine import AutoPilotEngine
                engine = AutoPilotEngine()
                engine.run_campaign(campaign_id)
                
                # 获取创建结果
                conn2 = _get_conn()
                result = conn2.execute(
                    "SELECT fb_campaign_id, status, error_msg FROM auto_campaigns WHERE id=?",
                    (campaign_id,)
                ).fetchone()
                
                if result and result[0]:
                    # 更新 warmup_campaigns 的 fb_campaign_id
                    conn2.execute(
                        "UPDATE warmup_campaigns SET fb_campaign_id=? WHERE id=?",
                        (result[0], warmup_id)
                    )
                    conn2.commit()
                    logger.info(f"[Lifecycle] 账户 {act_id} 预热广告创建成功: {result[0]}")
                else:
                    err = result[2] if result else "未知错误"
                    conn2.execute(
                        "UPDATE warmup_campaigns SET status='error', error_msg=? WHERE id=?",
                        (err, warmup_id)
                    )
                    conn2.commit()
                    logger.error(f"[Lifecycle] 账户 {act_id} 预热广告创建失败: {err}")
                
                conn2.close()
            except Exception as e:
                logger.error(f"[Lifecycle] 预热广告执行异常: {e}")
        
        t = threading.Thread(target=_run_warmup_campaign, daemon=True)
        t.start()
        
        return {
            "success": True,
            "msg": f"预热已启动，使用素材: {warmup_asset['filename']}，预算: ${warmup_daily}/天，计划预热 {warmup_days} 天",
            "campaign_id": campaign_id,
            "warmup_id": warmup_id
        }
        
    except Exception as e:
        conn.close()
        logger.error(f"[Lifecycle] start_warmup 失败: {e}")
        return {"success": False, "msg": str(e)}


def check_warmup_completion(act_id: str) -> dict:
    """
    检查账户预热是否完成。
    完成条件（满足任意一个）:
    1. 预热广告运行天数 >= warmup_days
    2. 预热消耗 >= warmup_budget
    """
    conn = _get_conn()
    warmup = conn.execute(
        "SELECT * FROM warmup_campaigns WHERE act_id=? AND status='running' ORDER BY id DESC LIMIT 1",
        (act_id,)
    ).fetchone()
    
    acc = conn.execute("SELECT * FROM accounts WHERE act_id=?", (act_id,)).fetchone()
    conn.close()
    
    if not warmup:
        return {"completed": False, "reason": "无进行中的预热任务"}
    
    warmup = dict(warmup)
    acc = dict(acc) if acc else {}
    
    # 预热门槛：至少运行 3 天或消耗 $10（避免 $5/1天 门槛过低）
    warmup_days = int(acc.get('warmup_days') or _get_setting('warmup_days_default', '3'))
    warmup_budget = float(acc.get('warmup_budget') or _get_setting('warmup_budget_default', '10'))
    
    # 计算运行天数
    started_at = warmup.get('started_at') or warmup.get('created_at', '')
    try:
        start_dt = datetime.strptime(started_at[:19], "%Y-%m-%d %H:%M:%S")
        days_running = (datetime.now() - start_dt).total_seconds() / 86400
    except Exception:
        days_running = 0
    
    # 检查消耗（如果有 fb_campaign_id）
    spend = 0.0
    fb_campaign_id = warmup.get('fb_campaign_id')
    if fb_campaign_id:
        token = _get_fb_token(act_id)
        if token:
            spend = _get_campaign_spend(fb_campaign_id, token)
    
    logger.info(f"[Lifecycle] 账户 {act_id} 预热状态: 运行 {days_running:.1f}天/{warmup_days}天, 消耗 ${spend:.2f}/${warmup_budget}")
    
    # 判断是否完成
    completed = False
    reason = ""
    
    # 预热完成条件：必须同时满足「运行时间」AND「消耗金额」，避免门槛过低
    # 或者满足「超长运行时间」（超过 warmup_days * 2 天）强制完成，避免卡死
    if days_running >= warmup_days and spend >= warmup_budget:
        completed = True
        reason = f"预热完成（运行 {days_running:.1f}天 + 消耗 ${spend:.2f}）"
    elif days_running >= warmup_days * 2:
        completed = True
        reason = f"预热超时强制完成（运行 {days_running:.1f} 天，消耗 ${spend:.2f}）"
    
    if completed:
        _complete_warmup(act_id, warmup, reason)
    
    return {
        "completed": completed,
        "reason": reason,
        "days_running": round(days_running, 1),
        "warmup_days": warmup_days,
        "spend": round(spend, 2),
        "warmup_budget": warmup_budget
    }


def _complete_warmup(act_id: str, warmup: dict, reason: str):
    """预热完成处理：暂停广告、升级账户、触发真实广告铺放"""
    logger.info(f"[Lifecycle] 账户 {act_id} 预热完成: {reason}")
    
    # 1. 暂停预热广告
    fb_campaign_id = warmup.get('fb_campaign_id')
    if fb_campaign_id:
        token = _get_fb_token(act_id)
        if token:
            paused = _pause_fb_campaign(fb_campaign_id, token)
            logger.info(f"[Lifecycle] 预热广告 {fb_campaign_id} 暂停: {'成功' if paused else '失败'}")
    
    # 2. 更新 warmup_campaigns 状态
    conn = _get_conn()
    conn.execute(
        "UPDATE warmup_campaigns SET status='completed', completed_at=? WHERE id=?",
        (_now_str(), warmup['id'])
    )
    
    # 3. 升级账户到 testing
    conn.execute(
        "UPDATE accounts SET lifecycle_stage='testing', lifecycle_updated_at=?, warmup_completed_at=? WHERE act_id=?",
        (_now_str(), _now_str(), act_id)
    )
    conn.commit()
    conn.close()
    
    logger.info(f"[Lifecycle] 账户 {act_id} 已升级为 testing 阶段")
    
    # 4. 检查是否自动铺放真实广告
    auto_launch = _get_setting('warmup_auto_launch', '1')
    if auto_launch == '1':
        _auto_launch_real_ad(act_id)


def _auto_launch_real_ad(act_id: str):
    """预热完成后自动铺放真实广告"""
    logger.info(f"[Lifecycle] 账户 {act_id} 开始自动铺放真实广告")
    
    # 获取最优素材
    asset = _get_best_real_asset(act_id)
    if not asset:
        logger.warning(f"[Lifecycle] 账户 {act_id} 无可用真实素材，跳过自动铺放")
        return
    
    # 获取账户目标配置
    acc = _get_account_config(act_id)
    countries = acc.get('target_countries', ['TW'])
    age_min = int(acc.get('target_age_min') or 35)
    age_max = int(acc.get('target_age_max') or 65)
    gender = int(acc.get('target_gender') or 0)
    placements = acc.get('target_placements', ['feed'])
    objective = acc.get('target_objective') or 'OUTCOME_SALES'
    
    # 创建 auto_campaigns 任务
    conn = _get_conn()
    try:
        campaign_name = f"[自动] {act_id} {datetime.now().strftime('%m%d%H%M')}"
        conn.execute("""
            INSERT INTO auto_campaigns 
            (act_id, asset_id, name, objective, target_countries,
             daily_budget, age_min, age_max, gender, placements,
             status, created_by, dispatch_source, lifecycle_stage_at_launch)
            VALUES (?, ?, ?, ?, ?,
                    20, ?, ?, ?, ?,
                    'pending', 'lifecycle', 'auto_warmup_complete', 'testing')
        """, (
            act_id, asset['id'], campaign_name, objective,
            json.dumps(countries),
            age_min, age_max, gender,
            json.dumps(placements)
        ))
        campaign_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        conn.close()
        
        logger.info(f"[Lifecycle] 账户 {act_id} 真实广告任务已创建: campaign_id={campaign_id}")
        
        # 后台线程执行
        def _run():
            try:
                from services.autopilot_engine import AutoPilotEngine
                engine = AutoPilotEngine()
                engine.run_campaign(campaign_id)
                logger.info(f"[Lifecycle] 账户 {act_id} 真实广告执行完成")
            except Exception as e:
                logger.error(f"[Lifecycle] 真实广告执行异常: {e}")
        
        t = threading.Thread(target=_run, daemon=True)
        t.start()
        
    except Exception as e:
        conn.close()
        logger.error(f"[Lifecycle] _auto_launch_real_ad 失败: {e}")


def check_scaling_eligibility(act_id: str) -> dict:
    """
    检查账户是否可以升级到拉量阶段
    
    升级条件（满足任意一个即可）：
      1. 有 S 级素材 且 近7天转化 >= 3 次（降低门槛）
      2. 有 S/A 级素材 且 近7天消耗 >= $50 且 ROAS >= 1.5（有盈利标准）
      3. 近7天消耗 >= $100（账户活跃度标准）
      4. testing 阶段运行 >= 7 天 且 有任意转化（时间维度孝化）
    """
    try:
        conn = _get_conn()
        # 检查是否有 S/A 级素材
        s_assets = conn.execute(
            "SELECT COUNT(*) FROM ad_assets WHERE ai_grade='S' AND upload_status='ai_done'"
        ).fetchone()[0]
        sa_assets = conn.execute(
            "SELECT COUNT(*) FROM ad_assets WHERE ai_grade IN ('S','A') AND upload_status='ai_done'"
        ).fetchone()[0]
        
        # 检查近7天总转化
        conversions = conn.execute("""
            SELECT COALESCE(SUM(CAST(value AS REAL)), 0)
            FROM ad_metrics
            WHERE act_id=? AND metric_name='conversions'
              AND recorded_at > datetime('now', '-7 days')
        """, (act_id,)).fetchone()[0]
        
        # 检查近7天总消耗
        spend_7d = conn.execute("""
            SELECT COALESCE(SUM(CAST(value AS REAL)), 0)
            FROM ad_metrics
            WHERE act_id=? AND metric_name='spend'
              AND recorded_at > datetime('now', '-7 days')
        """, (act_id,)).fetchone()[0]
        
        # 检查近7天 ROAS
        roas_7d = conn.execute("""
            SELECT COALESCE(AVG(CAST(value AS REAL)), 0)
            FROM ad_metrics
            WHERE act_id=? AND metric_name='roas'
              AND recorded_at > datetime('now', '-7 days')
              AND CAST(value AS REAL) > 0
        """, (act_id,)).fetchone()[0]
        
        # 检查账户进入 testing 阶段的时间
        lifecycle_row = conn.execute(
            "SELECT lifecycle_updated_at FROM accounts WHERE act_id=?",
            (act_id,)
        ).fetchone()
        days_in_testing = 0
        if lifecycle_row and lifecycle_row[0]:
            try:
                from datetime import datetime as _dt
                updated_at = _dt.strptime(str(lifecycle_row[0])[:19], "%Y-%m-%d %H:%M:%S")
                days_in_testing = ((_dt.now() - updated_at).total_seconds() / 86400)
            except Exception:
                days_in_testing = 0
        
        conn.close()
        
        # 升级条件判断（满足任意一个）
        reason_parts = []
        eligible = False
        
        # 条件 1：S级素材 + 转化次数较少（降低门槛）
        if s_assets > 0 and conversions >= 3:
            eligible = True
            reason_parts.append(f"S级素材{s_assets}张+转化{int(conversions)}次")
        
        # 条件 2：S/A级素材 + 消耗$50+ + ROAS>=1.5
        if sa_assets > 0 and spend_7d >= 50 and roas_7d >= 1.5:
            eligible = True
            reason_parts.append(f"S/A级素材+消耗${spend_7d:.0f}+ROAS{roas_7d:.2f}")
        
        # 条件 3：消耗$100+（账户活跃度足够）
        if spend_7d >= 100:
            eligible = True
            reason_parts.append(f"近7天消耗${spend_7d:.0f}超过$100")
        
        # 条件 4：testing运行7天+ + 有任意转化
        if days_in_testing >= 7 and conversions > 0:
            eligible = True
            reason_parts.append(f"testing运行{days_in_testing:.0f}天+有转化")
        
        reason = "、".join(reason_parts) if reason_parts else f"S级素材:{s_assets}张, 近7天转化:{int(conversions)}次, 消耗:${spend_7d:.0f}, ROAS:{roas_7d:.2f}"
        if not eligible:
            reason = f"暂不符合升级条件 (转化:{int(conversions)}次, 消耗:${spend_7d:.0f}, ROAS:{roas_7d:.2f}, testing天数:{days_in_testing:.0f})"
        
        return {
            "eligible": eligible,
            "s_assets": s_assets,
            "sa_assets": sa_assets,
            "conversions_7d": round(conversions, 0),
            "spend_7d": round(spend_7d, 2),
            "roas_7d": round(roas_7d, 4),
            "days_in_testing": round(days_in_testing, 1),
            "reason": reason
        }
    except Exception as e:
        logger.error(f"[Lifecycle] check_scaling_eligibility 失败: {e}")
        return {"eligible": False, "reason": str(e)}



def _check_account_needs_warmup(act_id: str) -> bool:
    """
    查询 FB API 判断账户是否需要预热。
    
    判断逻辑（按顺序）：
    1. 查账户近 7 天消耗 → 有消耗(>0) → 无需预热，直接进 testing
    2. 查账户是否有任意历史广告 → 有广告 → 无需预热，直接进 testing
    3. 以上都没有 → 真正新账户 → 需要预热
    
    使用近 7 天而非全生命周期，避免半年前花过钱但现在是新号的误判。
    
    返回 True = 需要预热
    返回 False = 不需要预热
    """
    try:
        token = _get_fb_token(act_id)
        if not token:
            logger.warning(f"[Lifecycle] 账户 {act_id} 无可用 Token，默认需要预热")
            return True

        import requests as _req

        # 1. 查近 7 天消耗
        try:
            r = _req.get(
                f"https://graph.facebook.com/v19.0/{act_id}/insights",
                params={
                    "fields": "spend",
                    "date_preset": "last_7_d",
                    "access_token": token,
                },
                timeout=15
            )
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    spend_7d = float(data[0].get("spend", 0))
                    if spend_7d > 0:
                        logger.info(f"[Lifecycle] 账户 {act_id} 近7天消耗 ${spend_7d:.2f}，无需预热")
                        return False
        except Exception as e:
            logger.warning(f"[Lifecycle] 账户 {act_id} 查询近7天消耗失败: {e}")

        # 2. 查是否有历史广告（任意状态）
        try:
            r2 = _req.get(
                f"https://graph.facebook.com/v19.0/{act_id}/campaigns",
                params={
                    "fields": "id,status",
                    "limit": 1,
                    "access_token": token,
                },
                timeout=15
            )
            if r2.status_code == 200:
                campaigns = r2.json().get("data", [])
                if campaigns:
                    logger.info(f"[Lifecycle] 账户 {act_id} 有历史广告记录，无需预热")
                    return False
        except Exception as e:
            logger.warning(f"[Lifecycle] 账户 {act_id} 查询历史广告失败: {e}")

        # 近7天无消耗且无历史广告 → 需要预热
        logger.info(f"[Lifecycle] 账户 {act_id} 近7天无消耗且无历史广告，需要预热")
        return True

    except Exception as e:
        logger.warning(f"[Lifecycle] 账户 {act_id} 检查预热需求时出错: {e}，默认需要预热")
        return True

def run_lifecycle_check():
    """
    定期巡检所有账户生命周期状态。
    由 scheduler 每 60 分钟调用一次。
    """
    logger.info("[Lifecycle] 开始生命周期巡检...")
    
    try:
        conn = _get_conn()
        accounts = conn.execute(
            """SELECT act_id, COALESCE(lifecycle_stage, 'new') as lifecycle_stage
               FROM accounts
               WHERE enabled=1
                  OR lifecycle_stage IN ('warmup', 'testing')"""
        ).fetchall()
        conn.close()
    except Exception as e:
        logger.error(f"[Lifecycle] 获取账户列表失败: {e}")
        return
    
    warmup_checked = 0
    scaling_checked = 0
    
    for row in accounts:
        act_id, stage = row[0], row[1] or 'new'
        
        try:
            if stage == 'new':
                # 自动发现新账户：查询 FB API 判断是否需要预热
                auto_warmup = _get_setting('warmup_auto_start', '1')
                if auto_warmup == '1':
                    needs_warmup = _check_account_needs_warmup(act_id)
                    if needs_warmup:
                        result = start_warmup(act_id)
                        if result.get('success'):
                            logger.info(f"[Lifecycle] 账户 {act_id} 自动启动预热成功（无历史广告/消耗）")
                        else:
                            logger.warning(f"[Lifecycle] 账户 {act_id} 自动启动预热失败: {result.get('msg')}")
                    else:
                        # 账户已有历史广告/消耗，直接跳到 testing 阶段
                        set_account_lifecycle(act_id, 'testing')
                        logger.info(f"[Lifecycle] 账户 {act_id} 已有历史广告/消耗，跳过预热直接进入测试期")
            
            elif stage == 'warmup':
                # 检查预热是否完成
                result = check_warmup_completion(act_id)
                warmup_checked += 1
                if result['completed']:
                    logger.info(f"[Lifecycle] 账户 {act_id} 预热完成: {result['reason']}")
            
            elif stage == 'testing':
                # 检查是否可以升级到拉量
                result = check_scaling_eligibility(act_id)
                scaling_checked += 1
                if result['eligible']:
                    set_account_lifecycle(act_id, 'scaling')
                    logger.info(f"[Lifecycle] 账户 {act_id} 升级为拉量阶段: {result['reason']}")
                    
        except Exception as e:
            logger.error(f"[Lifecycle] 账户 {act_id} 巡检失败: {e}")
    
    logger.info(f"[Lifecycle] 巡检完成: 预热检查 {warmup_checked} 个, 拉量检查 {scaling_checked} 个")
