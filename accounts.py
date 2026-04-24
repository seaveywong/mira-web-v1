"""
账户管理 API v1.2.0
修复: 导入时数据库锁死问题（先批量调用FB API，再一次性写入DB）
"""
import json
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import APIRouter, File, HTTPException, Depends, UploadFile
from pydantic import BaseModel
from typing import Optional, List

from core.auth import get_current_user
from core.database import get_conn, encrypt_token, decrypt_token

router = APIRouter()


# ── 余额计算辅助函数 ──────────────────────────────────────────
# 汇率缓存（从数据库读取，不存在则用 1.0）
def _to_usd(amount, currency):
    """将金额转换为 USD（从数据库读取汇率）"""
    if amount is None:
        return None
    if currency in (None, '', 'USD'):
        return float(amount)
    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT rate FROM currency_rates WHERE currency=?", (currency.upper(),)
        ).fetchone()
        conn.close()
        if row:
            return round(float(amount) * float(row['rate']), 2)
    except Exception:
        pass
    return float(amount)  # 无汇率时原值返回


def _calc_available_balance(balance, spend_cap, amount_spent, spending_limit, currency):
    """
    计算账户剩余可用余额
    返回: (available_balance, balance_type, amount_spent_usd)
      - available_balance: 剩余可用（USD），None 表示无法计算
      - balance_type: 'spending_limit' | 'prepaid' | 'unlimited'
      - amount_spent_usd: 已消费金额（USD）
    
    FB 账户类型：
    1. Cash/消费上限型：spending_limit > 0，剩余 = spending_limit - amount_spent
    2. 预付费型：balance > 0，剩余 = balance
    3. 无上限型：spending_limit=0 且 balance=0
    """
    # 单位转换：FB API 返回的金额单位是分（cents），需除以 100
    def _cents(v):
        if v is None:
            return None
        try:
            return float(v) / 100.0
        except (TypeError, ValueError):
            return None

    sl = _cents(spending_limit)
    spent = _cents(amount_spent)
    bal = _cents(balance)
    cap = _cents(spend_cap)

    # 已消费金额（USD）
    spent_usd = _to_usd(spent, currency) if spent is not None else None

    # 优先使用 spending_limit（消费上限型）
    if sl and sl > 0:
        avail = sl - (spent or 0)
        avail_usd = _to_usd(avail, currency)
        return (round(avail_usd, 2) if avail_usd is not None else None,
                'spending_limit', spent_usd)

    # 其次使用 spend_cap（账户总上限）
    if cap and cap > 0:
        avail = cap - (spent or 0)
        avail_usd = _to_usd(avail, currency)
        return (round(avail_usd, 2) if avail_usd is not None else None,
                'spending_limit', spent_usd)

    # 预付费余额
    if bal and bal > 0:
        bal_usd = _to_usd(bal, currency)
        return (round(bal_usd, 2) if bal_usd is not None else None,
                'prepaid', spent_usd)

    # 无上限账户
    return (None, 'unlimited', spent_usd)
# ── 余额计算辅助函数 END ──────────────────────────────────────


logger = logging.getLogger("mira.api.accounts")

FB_API_BASE = "https://graph.facebook.com/v25.0"


# ── Pydantic 模型 ──────────────────────────────────────────────────────────

class TokenCreate(BaseModel):
    token_alias: str
    access_token: str
    token_type: str = "user"
    note: Optional[str] = ""
    matrix_id: Optional[int] = None  # 操作号所属矩阵编号，管理号不填


class TokenUpdate(BaseModel):
    access_token: str
    token_alias: Optional[str] = None
    token_type: Optional[str] = None
    note: Optional[str] = None
    page_id: Optional[str] = None
    pixel_id: Optional[str] = None


class TokenTypeUpdate(BaseModel):
    token_type: str  # manage / operate / user


class AccountImport(BaseModel):
    act_ids: List[str]
    token_id: Optional[int] = None  # 路由参数已包含 token_id，body 中可选
    page_id: Optional[str] = None    # 批量导入时统一设置主页ID
    pixel_id: Optional[str] = None   # 批量导入时统一设置像素ID


class AccountUpdate(BaseModel):
    name: Optional[str] = None
    enabled: Optional[int] = None
    note: Optional[str] = None
    page_id: Optional[str] = None
    pixel_id: Optional[str] = None
    beneficiary: Optional[str] = None
    payer: Optional[str] = None
    tw_advertiser_id: Optional[int] = None
    # 智能铺放目标配置
    target_countries: Optional[str] = None      # JSON 字符串，如 '["TW","HK"]'
    target_age_min: Optional[int] = None        # 最小年龄，默认 25
    target_age_max: Optional[int] = None        # 最大年龄，默认 65
    target_gender: Optional[int] = None         # 0=不限 1=男 2=女
    target_placements: Optional[str] = None     # JSON 字符串，如 '["feed","reels"]'
    target_objective: Optional[str] = None      # 真实广告目标，如 OUTCOME_SALES
    warmup_days: Optional[int] = None           # 预热天数，默认 1
    warmup_budget: Optional[float] = None       # 预热消耗阈值（美元），默认 5
    lifecycle_stage: Optional[str] = None       # warmup/testing/scaling/paused
    landing_url: Optional[str] = None           # 账户级默认落地页链接
    target_objective_type: Optional[str] = None  # sales/website/leads/engagement
    ai_managed: Optional[int] = None             # AI 托管开关 0/1


# ── Token 管理 ─────────────────────────────────────────────────────────────


def _auto_detect_token_type(access_token: str) -> str:
    """通过 FB API 自动检测 Token 类型：manage（管理号）或 operate（操作号）"""
    import requests
    try:
        r = requests.get(
            "https://graph.facebook.com/v25.0/me",
            params={"fields": "id,name,type", "access_token": access_token},
            timeout=8
        )
        data = r.json()
        if "error" in data:
            return "manage"
        # 系统用户 type 为 "application"
        if data.get("type") == "application":
            return "operate"
        # 检查是否有 BM 关联的广告账户权限（操作号特征）
        r2 = requests.get(
            "https://graph.facebook.com/v25.0/me/adaccounts",
            params={"access_token": access_token, "limit": 1},
            timeout=8
        )
        d2 = r2.json()
        # 如果有广告账户权限，默认为管理号（个人号通常有 adaccounts）
        return "manage"
    except Exception:
        return "manage"

@router.get("/tokens")
def list_tokens(user=Depends(get_current_user)):
    """获取所有Token列表（脱敏）"""
    conn = get_conn()
    rows = conn.execute("""
        SELECT t.id, t.token_alias, t.token_type, t.status,
               t.last_verified_at, t.note, t.created_at, t.matrix_id,
               (SELECT COUNT(*) FROM account_op_tokens aot WHERE aot.token_id = t.id AND aot.status = 'active') as account_count
        FROM fb_tokens t
        LEFT JOIN accounts a ON a.token_id = t.id
        GROUP BY t.id
        ORDER BY t.created_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/tokens")
def add_token(body: TokenCreate, user=Depends(get_current_user)):
    """添加新Token（加密存储）"""
    if not body.access_token.strip():
        raise HTTPException(400, "Access Token 不能为空")

    ok, info = _verify_fb_token(body.access_token)
    if not ok:
        raise HTTPException(400, f"Token验证失败: {info}")

    enc = encrypt_token(body.access_token.strip())
    conn = get_conn()
    actual_type_for_insert = _auto_detect_token_type(body.access_token) if body.token_type == "auto" else body.token_type
    cursor = conn.execute(
        """INSERT INTO fb_tokens (token_alias, access_token_enc, token_type, status, last_verified_at, note, matrix_id)
           VALUES (?,?,?,?,datetime('now','+8 hours'),?,?)""",
        (body.token_alias, enc,
         actual_type_for_insert,
         "active", body.note or "",
         body.matrix_id if actual_type_for_insert == "operate" else None)
    )
    token_id = cursor.lastrowid
    conn.commit()
    conn.close()
    # v4.1: 操作号自动匹配已导入账户并加入操作号池（后台线程，不阻塞响应）
    actual_type = actual_type_for_insert
    if actual_type in ("operate", "manage"):  # 管理号和操作号都触发自动匹配
        import threading
        _access_token_copy = body.access_token.strip()
        _token_id_copy = token_id
        def _auto_match_op_bg():
            """
            操作号自动匹配逻辑：
            1. 调用 FB API 获取该操作号有权限的所有广告账户
            2. 与系统已导入账户做交集匹配
            3. 匹配到的账户自动将该操作号加入操作号池
            4. 同步更新账户状态（回收/禁用等）
            """
            try:
                import requests as _req
                resp = _req.get(
                    f"{FB_API_BASE}/me/adaccounts",
                    params={"access_token": _access_token_copy,
                            "fields": "id,name,account_status,balance,amount_spent,spend_cap",
                            "limit": 200},
                    timeout=30
                )
                fb_data = resp.json()
                if "error" in fb_data:
                    logger.warning(f"[OpAutoMatch] 操作号 {_token_id_copy} 拉取账户失败: {fb_data['error'].get('message')}")
                    return
                fb_accounts = fb_data.get("data", [])
                if not fb_accounts:
                    logger.info(f"[OpAutoMatch] 操作号 {_token_id_copy} 无可匹配账户")
                    return
                # 构建 FB 账户字典 {act_id: data}
                fb_map = {a["id"]: a for a in fb_accounts}
                c = get_conn()
                try:
                    # 获取系统已导入的所有账户
                    imported = c.execute("SELECT id, act_id, account_status FROM accounts").fetchall()
                    matched = 0
                    status_updated = 0
                    for acc in imported:
                        act_id = acc["act_id"]
                        fb_info = fb_map.get(act_id)
                        if fb_info:
                            # 匹配成功：将操作号加入该账户的操作号池（如未已存在）
                            existing_op = c.execute(
                                "SELECT id FROM account_op_tokens WHERE act_id=? AND token_id=?",
                                (act_id, _token_id_copy)
                            ).fetchone()
                            if not existing_op:
                                # 获取当前最大优先级
                                max_pri = c.execute(
                                    "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?", (act_id,)
                                ).fetchone()[0] or 0
                                c.execute(
                                    """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                                       VALUES (?, ?, ?, 'active', '自动匹配导入', (SELECT token_type FROM fb_tokens WHERE id=?), datetime('now'))""",
                                    (act_id, _token_id_copy, max_pri + 1, _token_id_copy)
                                )
                                matched += 1
                            # 同步更新账户状态（FB返回的最新状态）
                            new_status = fb_info.get("account_status", acc["account_status"])
                            if new_status != acc["account_status"]:
                                c.execute(
                                    "UPDATE accounts SET account_status=?, updated_at=datetime('now') WHERE id=?",
                                    (new_status, acc["id"])
                                )
                                status_updated += 1
                        else:
                            # 该账户不在操作号权限范围内，可能已被回收/禁用
                            # 不强制更新状态（可能只是操作号权限不够，不代表账户真的被禁）
                            pass
                    c.commit()
                    logger.info(f"[OpAutoMatch] 操作号 {_token_id_copy} 自动匹配 {matched} 个账户加入操作号池，更新 {status_updated} 个账户状态")
                except Exception as e:
                    c.rollback()
                    logger.error(f"[OpAutoMatch] 写入失败: {e}")
                finally:
                    c.close()
            except Exception as e:
                logger.error(f"[OpAutoMatch] 操作号自动匹配失败: {e}")
        threading.Thread(target=_auto_match_op_bg, daemon=True).start()
    return {"success": True, "token_id": token_id, "user_info": info, "auto_match_started": actual_type == "operate"}


@router.put("/tokens/{token_id}")
def update_token(token_id: int, body: TokenUpdate, user=Depends(get_current_user)):
    """更新Token（用于Token失效后重新授权）"""
    if not body.access_token.strip():
        raise HTTPException(400, "Access Token 不能为空")

    ok, info = _verify_fb_token(body.access_token)
    if not ok:
        raise HTTPException(400, f"Token验证失败: {info}")

    enc = encrypt_token(body.access_token.strip())
    conn = get_conn()
    updates = ["access_token_enc=?", "status='active'", "last_verified_at=datetime('now','+8 hours')"]
    params = [enc]
    if body.token_alias:
        updates.append("token_alias=?")
        params.append(body.token_alias)
    if body.note is not None:
        updates.append("note=?")
        params.append(body.note)
    if body.token_type:
        updates.append("token_type=?")
        params.append(body.token_type)
    params.append(token_id)
    conn.execute(f"UPDATE fb_tokens SET {', '.join(updates)} WHERE id=?", params)
    conn.execute("UPDATE accounts SET enabled=1 WHERE token_id=?", (token_id,))
    conn.commit()
    conn.close()
    # Token 更新后触发自动发现（后台线程）
    import threading as _th_upd
    def _trigger_discovery_bg():
        try:
            from core.scheduler import run_token_account_discovery
            run_token_account_discovery()
        except Exception as _e:
            logger.warning(f"[TokenUpdate] 触发自动发现失败: {_e}")
    _th_upd.Thread(target=_trigger_discovery_bg, daemon=True).start()
    return {"success": True, "user_info": info}


@router.patch("/tokens/{token_id}/type")
def update_token_type(token_id: int, body: TokenTypeUpdate, user=Depends(get_current_user)):
    """单独修改Token类型（manage/operate/user），无需重新输入Token"""
    allowed = {"manage", "operate", "user"}
    if body.token_type not in allowed:
        raise HTTPException(400, f"token_type 必须是 {allowed} 之一")
    conn = get_conn()
    row = conn.execute("SELECT id FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Token 不存在")
    conn.execute("UPDATE fb_tokens SET token_type=? WHERE id=?", (body.token_type, token_id))
    conn.commit()
    conn.close()
    return {"success": True, "token_type": body.token_type}


@router.delete("/tokens/{token_id}")
def delete_token(token_id: int, user=Depends(get_current_user)):
    """删除Token（检查是否有关联账户）"""
    conn = get_conn()
    count = conn.execute("SELECT COUNT(*) as c FROM accounts WHERE token_id=?", (token_id,)).fetchone()["c"]
    if count > 0:
        conn.close()
        raise HTTPException(400, f"该Token下还有 {count} 个账户，请先删除账户")
    # 同步清理操作号池关联记录（防止幽灵token导致巡检失败）
    conn.execute("DELETE FROM account_op_tokens WHERE token_id=?", (token_id,))
    conn.execute("DELETE FROM fb_tokens WHERE id=?", (token_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.post("/tokens/{token_id}/verify")
def verify_token_now(token_id: int, user=Depends(get_current_user)):
    """立即验证Token有效性"""
    conn = get_conn()
    row = conn.execute("SELECT access_token_enc, token_type FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Token不存在")

    token = decrypt_token(row["access_token_enc"])
    token_type_import = row["token_type"]
    ok, info = _verify_fb_token(token)

    conn = get_conn()
    status = "active" if ok else "expired"
    conn.execute(
        "UPDATE fb_tokens SET status=?, last_verified_at=datetime('now','+8 hours') WHERE id=?",
        (status, token_id)
    )
    if not ok:
        conn.execute("UPDATE accounts SET enabled=0 WHERE token_id=?", (token_id,))
        # Token 失效时实时标记 account_op_tokens 为 disabled
        conn.execute(
            "UPDATE account_op_tokens SET status='disabled' WHERE token_id=?",
            (token_id,)
        )
    else:
        # Token 验证成功时恢复 account_op_tokens 为 active
        conn.execute(
            "UPDATE account_op_tokens SET status='active' WHERE token_id=?",
            (token_id,)
        )
    conn.commit()
    conn.close()
    if ok:
        name = info.get('name', '') if isinstance(info, dict) else ''
        msg = f'Token 验证成功' + (f'（{name}）' if name else '')
    else:
        msg = f'Token 已失效：{info}'
        raise HTTPException(400, msg)
    return {"success": ok, "status": status, "info": info, "message": msg}


@router.post("/tokens/{token_id}/rematch-accounts")
def rematch_op_token_accounts(token_id: int, user=Depends(get_current_user)):
    """手动触发操作号重新匹配已导入账户（用于操作号添加后匹配失败的情况）"""
    conn = get_conn()
    row = conn.execute(
        "SELECT id, token_type, access_token_enc, status FROM fb_tokens WHERE id=?",
        (token_id,)
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Token不存在")
    token = decrypt_token(row["access_token_enc"])
    token_type_import = row["token_type"]
    # 调用 FB API 获取该操作号有权限的广告账户
    try:
        resp = requests.get(
            f"{FB_API_BASE}/me/adaccounts",
            params={"access_token": token,
                    "fields": "id,name,account_status",
                    "limit": 200},
            timeout=20
        )
        fb_data = resp.json()
    except Exception as e:
        raise HTTPException(500, f"调用 Facebook API 失败: {str(e)}")
    if "error" in fb_data:
        raise HTTPException(400, f"Facebook API 错误: {fb_data['error'].get('message', '未知错误')}")
    fb_accounts = fb_data.get("data", [])
    fb_map = {a["id"]: a for a in fb_accounts}
    # 与系统已导入账户做交集匹配
    conn = get_conn()
    try:
        imported = conn.execute("SELECT id, act_id, account_status FROM accounts").fetchall()
        matched = 0
        already = 0
        for acc in imported:
            act_id = acc["act_id"]
            if act_id in fb_map:
                existing = conn.execute(
                    "SELECT id FROM account_op_tokens WHERE act_id=? AND token_id=?",
                    (act_id, token_id)
                ).fetchone()
                if not existing:
                    max_pri = conn.execute(
                        "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?", (act_id,)
                    ).fetchone()[0] or 0
                    conn.execute(
                        """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                           VALUES (?, ?, ?, 'active', '手动重匹配', (SELECT token_type FROM fb_tokens WHERE id=?), datetime('now'))""",
                        (act_id, token_id, max_pri + 1, token_id)
                    )
                    matched += 1
                else:
                    already += 1
        conn.commit()
        logger.info(f"[Rematch] 操作号 {token_id} 手动重匹配: 新增 {matched} 个，已存在 {already} 个，FB账户总数 {len(fb_accounts)}")
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"写入数据库失败: {str(e)}")
    finally:
        conn.close()
    return {
        "success": True,
        "matched": matched,
        "already_linked": already,
        "fb_total": len(fb_accounts),
        "imported_total": len(imported),
        "message": f"匹配完成：新增关联 {matched} 个账户，已有 {already} 个已关联"
    }

@router.get("/tokens/{token_id}/fetch-accounts")
def fetch_token_accounts(token_id: int, user=Depends(get_current_user)):
    """拉取Token授权的所有广告账户列表（供用户勾选导入）"""
    # 先读取token，立即关闭连接
    conn = get_conn()
    row = conn.execute("SELECT access_token_enc, status, token_type FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "Token不存在")
    if row["status"] != "active":
        raise HTTPException(400, "Token已失效，请先更新Token")

    token_type = row["token_type"]
    token = decrypt_token(row["access_token_enc"])
    token_type_import = row["token_type"]

    # 调用FB API（不持有数据库连接），支持分页拉取全部账户
    try:
        fb_accounts = []
        _url = f"{FB_API_BASE}/me/adaccounts"
        _params = {
            "access_token": token,
            "fields": "id,name,currency,timezone_name,account_status,balance,spend_cap,amount_spent",
            "limit": 200
        }
        _page = 0
        while _url:
            _page += 1
            if _page > 50:  # 最多拉 50 页（10000 个账户），防止死循环
                break
            resp = requests.get(_url, params=_params if _page == 1 else None, timeout=30)
            resp.raise_for_status()
            _data = resp.json()
            fb_accounts.extend(_data.get("data", []))
            # 翻页：取 paging.next（已包含所有参数，直接请求）
            _url = _data.get("paging", {}).get("next")
            _params = None  # 后续页直接用 next URL，不再附加 params
    except Exception as e:
        raise HTTPException(400, f"拉取账户失败: {e}")

    # FB API调用完毕后，再开数据库连接
    conn = get_conn()
    imported = {r["act_id"] for r in conn.execute("SELECT act_id FROM accounts").fetchall()}
    conn.close()
    # 操作号导入时：遍历所有管理号Token，拉取其覆盖的账户集合，判断每个账户是否有管理号兜底
    manage_token_status = {}  # act_id -> {"status": "active"|None, "alias": str}
    if token_type == "operate":
        _conn_mgr = get_conn()
        _mgr_tokens = _conn_mgr.execute(
            "SELECT id, access_token_enc, status, token_alias FROM fb_tokens WHERE token_type='manage' AND status='active'"
        ).fetchall()
        _conn_mgr.close()
        # 用每个管理号Token调用FB API，获取其覆盖的账户列表
        import concurrent.futures
        def _fetch_mgr_accounts(mgr_row):
            try:
                _tk = decrypt_token(mgr_row["access_token_enc"])
                _all_ids = []
                _url2 = f"{FB_API_BASE}/me/adaccounts"
                _params2 = {"access_token": _tk, "fields": "id", "limit": 200}
                _pg2 = 0
                while _url2:
                    _pg2 += 1
                    if _pg2 > 50:
                        break
                    _r = requests.get(_url2, params=_params2 if _pg2 == 1 else None, timeout=15)
                    _d2 = _r.json()
                    _all_ids.extend(_d2.get("data", []))
                    _url2 = _d2.get("paging", {}).get("next")
                    _params2 = None
                return [(d["id"], {"status": mgr_row["status"], "alias": mgr_row["token_alias"]}) for d in _all_ids]
            except Exception:
                return []
        if _mgr_tokens:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as _pool:
                _futures = [_pool.submit(_fetch_mgr_accounts, t) for t in _mgr_tokens]
                for _f in concurrent.futures.as_completed(_futures):
                    for _act_id, _info in _f.result():
                        # 已有active管理号则不覆盖
                        if _act_id not in manage_token_status or manage_token_status[_act_id]["status"] != "active":
                            manage_token_status[_act_id] = _info

    result = []
    for acc in fb_accounts:
        act_id = acc["id"]
        result.append({
            "act_id": act_id,
            "name": acc.get("name", ""),
            "currency": acc.get("currency", "USD"),
            "timezone": acc.get("timezone_name", ""),
            "account_status": acc.get("account_status", 1),
            "balance": acc.get("balance"),
            "spend_cap": acc.get("spend_cap"),
            "amount_spent": acc.get("amount_spent"),
            "spending_limit": acc.get("spend_cap"),  # FB API 用 spend_cap 表示消费上限
            "already_imported": act_id in imported
        })
        # 操作号：附带管理号状态（管理号未覆盖的账户展示为不可导入）
        if token_type == "operate":
            _mgr = manage_token_status.get(act_id, {})
            result[-1]["mgr_status"] = _mgr.get("status")
            result[-1]["mgr_alias"] = _mgr.get("alias")
            result[-1]["mgr_ok"] = _mgr.get("status") == "active"

    return {"accounts": result, "total": len(result), "token_type": token_type}


def _fetch_single_account(act_id: str, token: str) -> dict:
    """并发拉取单个账户信息"""
    try:
        resp = requests.get(
            f"{FB_API_BASE}/{act_id}",
            params={"access_token": token, "fields": "id,name,currency,timezone_name,balance,account_status,spend_cap,amount_spent"},
            timeout=10
        )
        info = resp.json()
        if "error" in info:
            return {"act_id": act_id, "error": info["error"].get("message", "未知错误")}
        return {
            "act_id": act_id,
            "name": info.get("name", act_id),
            "currency": info.get("currency", "USD"),
            "timezone": info.get("timezone_name", "UTC"),
            "balance": info.get("balance"),
            "account_status": info.get("account_status", 1),
            "spend_cap": info.get("spend_cap"),
            "amount_spent": info.get("amount_spent"),
            "spending_limit": info.get("spend_cap"),  # FB API 用 spend_cap 表示消费上限
        }
    except Exception as e:
        return {
            "act_id": act_id,
            "name": act_id,
            "currency": "USD",
            "timezone": "UTC",
            "balance": None,
            "account_status": 1,
            "spend_cap": None,
        }


@router.post("/tokens/{token_id}/import-accounts")
def import_accounts(token_id: int, body: AccountImport, user=Depends(get_current_user)):
    """批量导入选中的广告账户
    
    修复: 先批量并发调用FB API获取所有账户信息，再一次性写入数据库
    避免在持有数据库连接时进行网络请求导致的数据库锁死
    """
    if not body.act_ids:
        return {"success": True, "imported": [], "skipped": []}

    # Step 1: 读取token，立即关闭连接
    conn = get_conn()
    row = conn.execute("SELECT access_token_enc, status, token_type FROM fb_tokens WHERE id=?", (token_id,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "Token不存在")

    token = decrypt_token(row["access_token_enc"])
    token_type_import = row["token_type"]

    # 操作号导入时：必须有管理号覆盖才能导入（实时调 FB API 验证）
    if token_type_import == "operate":
        _conn_guard = get_conn()
        _mgr_tokens_guard = _conn_guard.execute(
            "SELECT id, access_token_enc, token_alias FROM fb_tokens WHERE token_type='manage' AND status='active'"
        ).fetchall()
        _conn_guard.close()
        # 并发拉取所有管理号覆盖的账户集合
        import concurrent.futures as _cf
        _mgr_covered = set()
        def _fetch_mgr_ids(mgr_row):
            try:
                _tk = decrypt_token(mgr_row["access_token_enc"])
                _all_ids = []
                _url = f"{FB_API_BASE}/me/adaccounts"
                _params = {"access_token": _tk, "fields": "id", "limit": 200}
                while _url:
                    _r = requests.get(_url, params=_params, timeout=15)
                    _data = _r.json()
                    _all_ids.extend([d["id"] for d in _data.get("data", [])])
                    _url = _data.get("paging", {}).get("next")
                    _params = {}  # 翻页时 URL 已包含参数
                return _all_ids
            except Exception:
                return []
        if _mgr_tokens_guard:
            with _cf.ThreadPoolExecutor(max_workers=5) as _pool:
                for _ids in _pool.map(_fetch_mgr_ids, _mgr_tokens_guard):
                    _mgr_covered.update(_ids)
        _blocked = [_act_id for _act_id in body.act_ids if _act_id not in _mgr_covered]
        if _blocked:
            _blocked_short = ", ".join(_blocked[:5]) + ("..." if len(_blocked) > 5 else "")
            raise HTTPException(400, f"以下账户无管理号覆盖，无法导入：{_blocked_short}。请先导入能覆盖该账户的管理号 Token。")

    # Step 2: 并发调用FB API获取所有账户信息（不持有数据库连接）
    account_infos = {}
    max_workers = min(10, len(body.act_ids))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_single_account, act_id, token): act_id for act_id in body.act_ids}
        for future in as_completed(futures):
            info = future.result()
            account_infos[info["act_id"]] = info

    # Step 3: 所有FB API调用完毕后，一次性写入数据库
    imported = []
    skipped = []

    conn = get_conn()
    try:
        # 获取已存在的账户
        existing_ids = {r["act_id"] for r in conn.execute("SELECT act_id FROM accounts").fetchall()}

        for act_id in body.act_ids:
            if act_id in existing_ids:
                skipped.append(act_id)
                continue

            info = account_infos.get(act_id, {})
            conn.execute(
                """INSERT INTO accounts (act_id, name, currency, timezone, token_id, enabled, balance, account_status, spend_cap, page_id, pixel_id, amount_spent, spending_limit)
                   VALUES (?,?,?,?,?,1,?,?,?,?,?,?,?)""",
                (
                    act_id,
                    info.get("name", act_id),
                    info.get("currency", "USD"),
                    info.get("timezone", "UTC"),
                    token_id,
                    info.get("balance"),
                    info.get("account_status", 1),
                    info.get("spend_cap"),
                    body.page_id or info.get("page_id"),
                    body.pixel_id or info.get("pixel_id"),
                    info.get("amount_spent"),
                    info.get("account_spending_limit"),
                )
            )
            imported.append({"act_id": act_id, "name": info.get("name", act_id)})
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"导入失败: {str(e)}")
    finally:
        conn.close()

    # v4.3: 导入账户后，立即将导入时使用的 token 写入 account_op_tokens（管理号或操作号）
    if imported:
        _c_link = get_conn()
        try:
            for _a in imported:
                _act_id = _a["act_id"]
                _existing_link = _c_link.execute(
                    "SELECT id FROM account_op_tokens WHERE act_id=? AND token_id=?",
                    (_act_id, token_id)
                ).fetchone()
                if not _existing_link:
                    _max_pri = _c_link.execute(
                        "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?", (_act_id,)
                    ).fetchone()[0] or 0
                    _c_link.execute(
                        """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                           VALUES (?, ?, ?, 'active', '导入时绑定', ?, datetime('now'))""",
                        (_act_id, token_id, _max_pri + 1, token_type_import)
                    )
            _c_link.commit()
            logger.info(f"[ImportLink] 导入 token_id={token_id} 关联 {len(imported)} 个账户到 account_op_tokens")
        except Exception as _le:
            _c_link.rollback()
            logger.error(f"[ImportLink] 写入关联失败: {_le}")
        finally:
            _c_link.close()

    # v4.2: 导入账户后，触发所有操作号对新账户的自动匹配
    if imported:
        import threading as _threading
        _imported_act_ids = [a["act_id"] for a in imported]
        def _match_op_tokens_for_new_accounts():
            try:
                _c = get_conn()
                _op_tokens = _c.execute(
                    "SELECT id, access_token_enc FROM fb_tokens WHERE token_type='operate' AND status='active'"
                ).fetchall()
                _c.close()
                for _op_row in _op_tokens:
                    _op_token_id = _op_row["id"]
                    _op_token = decrypt_token(_op_row["access_token_enc"])
                    try:
                        import requests as _req2
                        _resp = _req2.get(
                            f"{FB_API_BASE}/me/adaccounts",
                            params={"access_token": _op_token, "fields": "id", "limit": 200},
                            timeout=20
                        )
                        _fb_data = _resp.json()
                        if "error" in _fb_data:
                            continue
                        _fb_act_ids = {_a["id"] for _a in _fb_data.get("data", [])}
                        _c2 = get_conn()
                        try:
                            _matched = 0
                            for _act_id in _imported_act_ids:
                                if _act_id in _fb_act_ids:
                                    _existing = _c2.execute(
                                        "SELECT id FROM account_op_tokens WHERE act_id=? AND token_id=?",
                                        (_act_id, _op_token_id)
                                    ).fetchone()
                                    if not _existing:
                                        _max_pri = _c2.execute(
                                            "SELECT MAX(priority) FROM account_op_tokens WHERE act_id=?", (_act_id,)
                                        ).fetchone()[0] or 0
                                        _c2.execute(
                                            """INSERT INTO account_op_tokens (act_id, token_id, priority, status, note, token_type, created_at)
                                               VALUES (?, ?, ?, 'active', '导入时自动匹配', (SELECT token_type FROM fb_tokens WHERE id=?), datetime('now'))""",
                                            (_act_id, _op_token_id, _max_pri + 1, _op_token_id)
                                        )
                                        _matched += 1
                        except Exception as _e2:
                            _c2.rollback()
                            logger.error(f"[OpMatch] 写入失败: {_e2}")
                        finally:
                            _c2.close()
                    except Exception as _e1:
                        logger.error(f"[OpMatch] 操作号 {_op_token_id} 匹配失败: {_e1}")
            except Exception as _e0:
                logger.error(f"[OpMatch] 整体匹配失败: {_e0}")
        _threading.Thread(target=_match_op_tokens_for_new_accounts, daemon=True).start()
    return {"success": True, "imported": imported, "skipped": skipped}


# ── 账户管理 ──────────────────────────────────────────────────────────────

# 默认汇率表（1单位外币 = X USD），用于无法获取实时汇率时的备用
_DEFAULT_RATES = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067,
    "CNY": 0.138, "HKD": 0.128, "TWD": 0.031, "SGD": 0.74,
    "AUD": 0.65, "CAD": 0.74, "BRL": 0.20, "MXN": 0.058,
    "CLP": 0.0011, "COP": 0.00025, "PEN": 0.27, "ARS": 0.001,
    "THB": 0.028, "VND": 0.000040, "IDR": 0.000063, "PHP": 0.017,
    "MYR": 0.21, "INR": 0.012, "TRY": 0.031, "ZAR": 0.053,
    # 补充常见货币
    "BDT": 0.0091, "PKR": 0.0036, "LKR": 0.0031, "NPR": 0.0075,
    "KRW": 0.00072, "CHF": 1.12, "NZD": 0.60, "SEK": 0.096,
    "NOK": 0.093, "DKK": 0.145, "PLN": 0.25, "CZK": 0.044,
    "HUF": 0.0028, "RON": 0.22, "BGN": 0.55, "HRK": 0.14,
    "AED": 0.272, "SAR": 0.267, "QAR": 0.275, "KWD": 3.26,
    "BHD": 2.65, "OMR": 2.60, "JOD": 1.41, "EGP": 0.021,
    "MAD": 0.099, "TND": 0.32, "GHS": 0.067, "NGN": 0.00065,
    "KES": 0.0077, "TZS": 0.00038, "UGX": 0.00027, "ETB": 0.0088,
    "UAH": 0.027, "KZT": 0.0022, "UZS": 0.000079, "GEL": 0.37,
    "AMD": 0.0026, "AZN": 0.59, "BYN": 0.31, "MDL": 0.056,
    "RSD": 0.0093, "MKD": 0.018, "ALL": 0.011, "BAM": 0.55,
    "CRC": 0.0019, "GTQ": 0.13, "HNL": 0.040, "NIO": 0.027,
    "PAB": 1.0, "DOP": 0.017, "JMD": 0.0064, "TTD": 0.15,
    "BBD": 0.50, "BSD": 1.0, "BZD": 0.50, "GYD": 0.0048,
    "SRD": 0.029, "UYU": 0.026, "PYG": 0.000135, "BOB": 0.145,
    "VES": 0.000027, "CUP": 0.042,
}

def _to_usd(amount, currency: str) -> float:
    """将任意货币金额转换为USD"""
    if amount is None:
        return 0.0
    rate = _DEFAULT_RATES.get((currency or 'USD').upper(), 1.0)
    return round(float(amount) * rate, 2)


@router.get("")
def list_accounts(user=Depends(get_current_user)):
    """获取所有账户列表"""
    conn = get_conn()
    rows = conn.execute("""
        SELECT a.id, a.act_id, a.name, a.currency, a.timezone,
               a.enabled, a.note, a.page_id, a.pixel_id, a.beneficiary, a.payer, a.tw_advertiser_id, a.created_at,
               a.balance, a.account_status, a.spend_cap, a.amount_spent, a.spending_limit,
               COALESCE(a.ai_managed, 0) as ai_managed,
               COALESCE(a.lifecycle_stage, 'new') as lifecycle_stage,
               a.target_countries, a.target_age_min, a.target_age_max,
               a.target_gender, a.target_placements, a.target_objective_type, a.landing_url,
               t.token_alias, t.status as token_status,
               (SELECT MAX(created_at) FROM action_logs WHERE act_id=a.act_id) as last_inspect_at
        FROM accounts a
        LEFT JOIN fb_tokens t ON t.id = a.token_id
        ORDER BY a.created_at DESC
    """).fetchall()
    # 查询每个账户关联的所有 Token（来自 account_op_tokens，管理号+操作号，动态发现）
    all_tokens_map = {}
    all_token_rows = conn.execute("""
        SELECT aot.act_id, t.token_alias, t.token_type, aot.status as bind_status
        FROM account_op_tokens aot
        JOIN fb_tokens t ON t.id = aot.token_id
        WHERE aot.status = 'active' AND t.status = 'active'
        ORDER BY t.token_type DESC, aot.priority DESC, t.token_alias
    """).fetchall()
    for lr in all_token_rows:
        act_id_key = lr["act_id"]
        if act_id_key not in all_tokens_map:
            all_tokens_map[act_id_key] = []
        all_tokens_map[act_id_key].append({
            "alias": lr["token_alias"],
            "type": lr["token_type"]
        })
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        cur = (d.get('currency') or 'USD').upper()
        bal = d.get('balance')
        amount_spent = d.get('amount_spent')
        spending_limit = d.get('spending_limit')
        cur = d.get('currency', 'USD')
        # 计算可用余额（剩余可投）
        available, bal_type, spent_usd = _calc_available_balance(
            bal, d.get('spend_cap'), amount_spent, spending_limit, cur
        )
        d['available_balance'] = available
        d['balance_type'] = bal_type
        d['amount_spent_usd'] = spent_usd
        # 附带 balance_usd：USD账户直接等值，非USD账户按汇率换算
        if bal is not None:
            d['balance_usd'] = _to_usd(bal, cur) if cur != 'USD' else float(bal)
        else:
            d['balance_usd'] = None
        # 附带 timezone_name（兼容旧字段名 timezone）
        if not d.get('timezone_name'):
            d['timezone_name'] = d.get('timezone', '')
        # 附带关联的所有 Token（来自 account_op_tokens，动态发现，管理号+操作号）
        d['linked_tokens'] = all_tokens_map.get(d.get('act_id'), [])
        result.append(d)
    return result


@router.put("/{account_id}")
def update_account(account_id: int, body: AccountUpdate, user=Depends(get_current_user)):
    """更新账户信息"""
    conn = get_conn()
    updates = []
    params = []
    if body.name is not None:
        updates.append("name=?")
        params.append(body.name)
    if body.enabled is not None:
        updates.append("enabled=?")
        params.append(body.enabled)
    if body.note is not None:
        updates.append("note=?")
        params.append(body.note)
    if body.page_id is not None:
        updates.append("page_id=?")
        params.append(body.page_id)
    if body.pixel_id is not None:
        updates.append("pixel_id=?")
        params.append(body.pixel_id)
    if body.beneficiary is not None:
        updates.append("beneficiary=?")
        params.append(body.beneficiary)
    if body.payer is not None:
        updates.append("payer=?")
        params.append(body.payer)
    if body.tw_advertiser_id is not None:
        # 0 表示清除关联，其他值表示设置关联
        updates.append("tw_advertiser_id=?")
        params.append(None if body.tw_advertiser_id == 0 else body.tw_advertiser_id)
    # 智能铺放目标配置字段
    if body.target_countries is not None:
        updates.append("target_countries=?")
        params.append(body.target_countries)
    if body.target_age_min is not None:
        updates.append("target_age_min=?")
        params.append(body.target_age_min)
    if body.target_age_max is not None:
        updates.append("target_age_max=?")
        params.append(body.target_age_max)
    if body.target_gender is not None:
        updates.append("target_gender=?")
        params.append(body.target_gender)
    if body.target_placements is not None:
        updates.append("target_placements=?")
        params.append(body.target_placements)
    if body.target_objective is not None:
        updates.append("target_objective=?")
        params.append(body.target_objective)
    if body.warmup_days is not None:
        updates.append("warmup_days=?")
        params.append(body.warmup_days)
    if body.warmup_budget is not None:
        updates.append("warmup_budget=?")
        params.append(body.warmup_budget)
    if body.lifecycle_stage is not None:
        updates.append("lifecycle_stage=?")
        params.append(body.lifecycle_stage)
    # 账户级默认落地页和目标类型（之前遗漏处理）
    if body.landing_url is not None:
        updates.append("landing_url=?")
        params.append(body.landing_url)
    if body.target_objective_type is not None:
        updates.append("target_objective_type=?")
        params.append(body.target_objective_type)
    # AI 托管开关（之前遗漏处理）
    if getattr(body, 'ai_managed', None) is not None:
        updates.append("ai_managed=?")
        params.append(1 if body.ai_managed else 0)
    if not updates:
        conn.close()
        raise HTTPException(400, "没有需要更新的字段")
    updates.append("updated_at=datetime('now')")
    params.append(account_id)
    conn.execute(f"UPDATE accounts SET {', '.join(updates)} WHERE id=?", params)
    conn.commit()
    conn.close()
    return {"success": True}



@router.patch("/by-act-id/{act_id_str}")
def patch_account_by_act_id(act_id_str: str, body: AccountUpdate, user=Depends(get_current_user)):
    """通过 act_id 字符串更新账户配置（用于前端批量链接管理等场景）"""
    conn = get_conn()
    row = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id_str,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"账户 {act_id_str} 不存在")
    account_id = row[0]
    updates = []
    params = []
    if body.name is not None:
        updates.append("name=?"); params.append(body.name)
    if body.enabled is not None:
        updates.append("enabled=?"); params.append(body.enabled)
    if body.note is not None:
        updates.append("note=?"); params.append(body.note)
    if body.page_id is not None:
        updates.append("page_id=?"); params.append(body.page_id)
    if body.pixel_id is not None:
        updates.append("pixel_id=?"); params.append(body.pixel_id)
    if body.landing_url is not None:
        updates.append("landing_url=?"); params.append(body.landing_url)
    if body.target_countries is not None:
        updates.append("target_countries=?"); params.append(body.target_countries)
    if body.target_age_min is not None:
        updates.append("target_age_min=?"); params.append(body.target_age_min)
    if body.target_age_max is not None:
        updates.append("target_age_max=?"); params.append(body.target_age_max)
    if body.target_gender is not None:
        updates.append("target_gender=?"); params.append(body.target_gender)
    if body.target_placements is not None:
        updates.append("target_placements=?"); params.append(body.target_placements)
    if body.target_objective_type is not None:
        updates.append("target_objective_type=?"); params.append(body.target_objective_type)
    if getattr(body, 'ai_managed', None) is not None:
        updates.append("ai_managed=?"); params.append(1 if body.ai_managed else 0)
    if not updates:
        conn.close()
        raise HTTPException(400, "没有需要更新的字段")
    updates.append("updated_at=datetime('now')")
    params.append(account_id)
    conn.execute(f"UPDATE accounts SET {', '.join(updates)} WHERE id=?", params)
    conn.commit()
    conn.close()
    return {"success": True, "act_id": act_id_str}

@router.delete("/{account_id}")
def delete_account(account_id: int, user=Depends(get_current_user)):
    """删除账户（不删除Token）"""
    conn = get_conn()
    conn.execute("DELETE FROM accounts WHERE id=?", (account_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.post("/{account_id}/sync-status")
def sync_account_status(account_id: int, user=Depends(get_current_user)):
    """从 FB API 同步单个账户的真实状态（account_status、balance、spend_cap）"""
    conn = get_conn()
    row = conn.execute("""
        SELECT a.act_id, t.access_token_enc, t.status as token_status, t.token_type
        FROM accounts a
        LEFT JOIN fb_tokens t ON t.id = a.token_id
        WHERE a.id=?
    """, (account_id,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "账户不存在")
    if not row["access_token_enc"]:
        raise HTTPException(400, "该账户未关联有效 Token")
    if row["token_status"] != "active":
        raise HTTPException(400, "关联 Token 已失效，请先更新 Token")

    token = decrypt_token(row["access_token_enc"])
    token_type_import = row["token_type"]
    info = _fetch_single_account(row["act_id"], token)

    if "error" in info:
        raise HTTPException(400, f"FB API 返回错误: {info['error']}")

    conn = get_conn()
    conn.execute("""
        UPDATE accounts
        SET account_status=?, balance=?, spend_cap=?, amount_spent=?, spending_limit=?, name=?, updated_at=datetime('now')
        WHERE id=?
    """, (
        info.get("account_status", 1),
        info.get("balance"),
        info.get("spend_cap"),
        info.get("amount_spent"),
        info.get("spending_limit"),  # _fetch_single_account 返回的 key 是 spending_limit
        info.get("name"),
        account_id
    ))
    conn.commit()
    conn.close()

    return {
        "success": True,
        "account_status": info.get("account_status", 1),
        "balance": info.get("balance"),
        "name": info.get("name"),
    }


@router.post("/sync-all-status")
def sync_all_accounts_status(user=Depends(get_current_user)):
    """批量从 FB API 同步所有账户的真实状态（并发执行）"""
    conn = get_conn()
    rows = conn.execute("""
        SELECT a.id, a.act_id, a.name, t.access_token_enc, t.status as token_status
        FROM accounts a
        LEFT JOIN fb_tokens t ON t.id = a.token_id
        WHERE t.status = 'active' AND t.access_token_enc IS NOT NULL
    """).fetchall()
    # 对 name=act_id 的账户，尝试从 account_op_tokens 中找到可用的操作号 token 补充
    _op_token_cache = {}  # act_id -> access_token_enc
    _name_missing = [r for r in rows if (r["name"] or "") == r["act_id"] or not r["name"]]
    if _name_missing:
        _conn_op = get_conn()
        _op_rows = _conn_op.execute("""
            SELECT aot.act_id, t.access_token_enc
            FROM account_op_tokens aot
            JOIN fb_tokens t ON t.id = aot.token_id
            WHERE aot.status='active' AND t.status='active'
            ORDER BY aot.priority DESC
        """).fetchall()
        _conn_op.close()
        for _or in _op_rows:
            if _or["act_id"] not in _op_token_cache:
                _op_token_cache[_or["act_id"]] = _or["access_token_enc"]
    conn.close()

    if not rows:
        return {"success": True, "updated": 0, "failed": 0, "message": "没有可同步的账户"}

    # 并发拉取所有账户信息
    results = {}
    with ThreadPoolExecutor(max_workers=min(10, len(rows))) as executor:
        futures = {}
        for row in rows:
            # 对 name=act_id 的账户，优先用操作号 token 拉取（操作号可能有该账户的名称权限）
            _row_name = row["name"] or ""
            if _row_name == row["act_id"] or not _row_name:
                _alt_enc = _op_token_cache.get(row["act_id"])
                token = decrypt_token(_alt_enc) if _alt_enc else decrypt_token(row["access_token_enc"])
            else:
                token = decrypt_token(row["access_token_enc"])
            fut = executor.submit(_fetch_single_account, row["act_id"], token)
            futures[fut] = row
        for fut in as_completed(futures):
            row = futures[fut]
            info = fut.result()
            results[row["id"]] = info

    # 批量更新数据库
    updated = 0
    failed = 0
    conn = get_conn()
    try:
        for row in rows:
            info = results.get(row["id"], {})
            if "error" in info:
                failed += 1
                continue
            # 如果返回的 name 仍是 act_id 或为空，保留原有名称不覆盖
            _new_name = info.get("name")
            if not _new_name or _new_name == row["act_id"]:
                _new_name = row["name"]  # 保留原有名称
            conn.execute("""
                UPDATE accounts
                SET account_status=?, balance=?, spend_cap=?, amount_spent=?, spending_limit=?, name=?, updated_at=datetime('now')
                WHERE id=?
            """, (
                info.get("account_status", 1),
                info.get("balance"),
                info.get("spend_cap"),
                info.get("amount_spent"),
                info.get("spending_limit"),  # _fetch_single_account 返回的 key 是 spending_limit
                _new_name,
                row["id"]
            ))
            updated += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"批量更新失败: {str(e)}")
    finally:
        conn.close()

    return {"success": True, "updated": updated, "failed": failed}


# ── 工具函数 ──────────────────────────────────────────────────────────────

def _verify_fb_token(token: str):
    """验证FB Token有效性，返回 (ok, info_or_error)"""
    try:
        resp = requests.get(
            f"{FB_API_BASE}/me",
            params={"access_token": token, "fields": "id,name"},
            timeout=10
        )
        data = resp.json()
        if "error" in data:
            err = data["error"]
            return False, f"{err.get('message', '未知错误')} (code={err.get('code')})"
        return True, {"id": data.get("id"), "name": data.get("name")}
    except Exception as e:
        return False, str(e)



@router.get("/{act_id}/fb-pages")
def get_fb_pages(act_id: str, user=Depends(get_current_user)):
    """
    从 Facebook API 拉取该账户操作号（或管理号）有权限管理的主页列表。
    策略：
    1. 优先用操作号 Token（因为主页绑在操作号上）
    2. 若操作号无数据，再用管理号 Token 补充
    3. 多接口尝试：/me/accounts（个人主页）、广告账户关联主页
    返回格式：[{id, name, category, can_use}]
    """
    import requests as _req

    def _get_all_op_tokens(act_id_: str):
        """直接从数据库获取该账户所有操作号的 Token（不经过心跳检测）"""
        conn = get_conn()
        rows = conn.execute("""
            SELECT t.access_token_enc, t.status as token_status
            FROM account_op_tokens aot
            JOIN fb_tokens t ON t.id = aot.token_id
            WHERE aot.act_id = ?
              AND aot.status = 'active'
        """, (act_id_,)).fetchall()
        conn.close()
        result = []
        for row in rows:
            if row["token_status"] == "active":
                plain = decrypt_token(row["access_token_enc"])
                if plain:
                    result.append(plain)
        return result

    def _get_manage_token_direct(act_id_: str):
        """直接从数据库获取管理号 Token"""
        conn = get_conn()
        row = conn.execute("""
            SELECT t.access_token_enc, t.status
            FROM accounts a
            JOIN fb_tokens t ON t.id = a.token_id
            WHERE a.act_id = ?
        """, (act_id_,)).fetchone()
        conn.close()
        if not row or row["status"] != "active":
            return None
        return decrypt_token(row["access_token_enc"])

    def _fetch_pages_with_token(token_: str) -> list:
        """用给定 Token 通过 /me/accounts 拉取主页列表"""
        pages_map = {}
        try:
            r = _req.get(
                f"{FB_API_BASE}/me/accounts",
                params={"access_token": token_, "fields": "id,name,category,is_published,tasks", "limit": 200},
                timeout=15
            )
            d = r.json()
            for p in d.get("data", []):
                pid = p.get("id")
                if pid:
                    tasks = p.get("tasks", [])
                    # 有 ADVERTISE 权限即可投广告；tasks 为空时也视为可用
                    can_adv = (not tasks) or ("ADVERTISE" in tasks)
                    pages_map[pid] = {
                        "id": pid,
                        "name": p.get("name", ""),
                        "category": p.get("category", ""),
                        "is_published": p.get("is_published", True),
                        "can_use": can_adv,
                        "source": "me/accounts"
                    }
        except Exception:
            pass
        return list(pages_map.values())

    # 收集所有 Token（操作号优先，管理号补充）
    all_tokens = _get_all_op_tokens(act_id)
    manage_token = _get_manage_token_direct(act_id)
    if manage_token and manage_token not in all_tokens:
        all_tokens.append(manage_token)

    if not all_tokens:
        raise HTTPException(400, "该账户无可用 Token，无法拉取主页列表")

    # 用所有 Token 拉取，合并去重
    merged = {}
    for tok in all_tokens:
        for p in _fetch_pages_with_token(tok):
            pid = p["id"]
            if pid not in merged:
                merged[pid] = p
            elif p.get("can_use") and not merged[pid].get("can_use"):
                # 有可用的就覆盖不可用的
                merged[pid] = p

    pages = list(merged.values())
    pages.sort(key=lambda x: (0 if x["can_use"] else 1, x.get("name", "")))
    return {"success": True, "pages": pages, "total": len(pages)}


@router.get("/{act_id}/fb-pixels")
def get_fb_pixels(act_id: str, user=Depends(get_current_user)):
    """
    从 Facebook API 拉取该广告账户下的像素列表。
    返回格式：[{id, name, last_fired_time, can_use}]
    """
    import requests as _req
    from services.token_manager import get_exec_token, ACTION_READ
    token = get_exec_token(act_id, ACTION_READ)
    if not token:
        raise HTTPException(400, "该账户无可用 Token，无法拉取像素列表")
    try:
        resp = _req.get(
            f"{FB_API_BASE}/{act_id}/adspixels",
            params={
                "access_token": token,
                "fields": "id,name,last_fired_time,is_unavailable",
                "limit": 50
            },
            timeout=12
        )
        data = resp.json()
    except Exception as e:
        raise HTTPException(502, f"FB API 请求失败: {e}")
    if "error" in data:
        err = data["error"]
        raise HTTPException(400, f"FB API 错误: {err.get('message','未知')} (code={err.get('code')})")
    pixels = []
    for px in data.get("data", []):
        is_unavailable = px.get("is_unavailable", False)
        pixels.append({
            "id": px.get("id"),
            "name": px.get("name", f"Pixel {px.get('id','')}"),
            "last_fired_time": px.get("last_fired_time"),
            "can_use": not is_unavailable
        })
    pixels.sort(key=lambda x: (0 if x["can_use"] else 1, x.get("name", "")))
    return {"success": True, "pixels": pixels}

@router.get("/currency-rates")
async def get_currency_rates(user=Depends(get_current_user)):
    """获取汇率信息（用于非USD货币换算）"""
    try:
        from services.currency import get_rates
        rates = await get_rates()
        return {"success": True, "rates": rates, "base": "USD"}
    except Exception:
        # 返回默认汇率
        return {"success": True, "rates": {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067, "CNY": 0.138, "HKD": 0.128, "TWD": 0.031, "SGD": 0.74, "AUD": 0.65, "CAD": 0.74}, "base": "USD"}


# ── 台湾广告认证身份管理 ──────────────────────────────────────────────────────

class TwAdvertiserCreate(BaseModel):
    name: str
    fb_user_id: Optional[str] = None
    beneficiary: str
    payer: str
    note: Optional[str] = None

class TwAdvertiserUpdate(BaseModel):
    name: Optional[str] = None
    fb_user_id: Optional[str] = None
    beneficiary: Optional[str] = None
    payer: Optional[str] = None
    note: Optional[str] = None
    verified: Optional[int] = None

@router.get("/tw-advertisers")
def list_tw_advertisers(user=Depends(get_current_user)):
    """列出所有台湾广告认证身份"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, name, fb_user_id, beneficiary, payer, note, verified, created_at FROM tw_advertisers ORDER BY id"
    ).fetchall()
    conn.close()
    return {"success": True, "advertisers": [dict(r) for r in rows]}

@router.post("/tw-advertisers")
def create_tw_advertiser(body: TwAdvertiserCreate, user=Depends(get_current_user)):
    """新增台湾广告认证身份"""
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO tw_advertisers (name, fb_user_id, beneficiary, payer, note, verified) VALUES (?,?,?,?,?,1)",
        (body.name, body.fb_user_id, body.beneficiary, body.payer, body.note)
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return {"success": True, "id": new_id}

@router.put("/tw-advertisers/{adv_id}")
def update_tw_advertiser(adv_id: int, body: TwAdvertiserUpdate, user=Depends(get_current_user)):
    """更新台湾广告认证身份"""
    conn = get_conn()
    updates, params = [], []
    if body.name is not None:
        updates.append("name=?"); params.append(body.name)
    if body.fb_user_id is not None:
        updates.append("fb_user_id=?"); params.append(body.fb_user_id)
    if body.beneficiary is not None:
        updates.append("beneficiary=?"); params.append(body.beneficiary)
    if body.payer is not None:
        updates.append("payer=?"); params.append(body.payer)
    if body.note is not None:
        updates.append("note=?"); params.append(body.note)
    if body.verified is not None:
        updates.append("verified=?"); params.append(body.verified)
    if updates:
        params.append(adv_id)
        conn.execute(f"UPDATE tw_advertisers SET {','.join(updates)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return {"success": True}

@router.delete("/tw-advertisers/{adv_id}")
def delete_tw_advertiser(adv_id: int, user=Depends(get_current_user)):
    """删除台湾广告认证身份"""
    conn = get_conn()
    conn.execute("DELETE FROM tw_advertisers WHERE id=?", (adv_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ─────────────────────────────────────────────────────────────────────────────
# 台湾认证主页库（tw_certified_pages）
# 用户手动录入已完成台湾广告认证的主页，铺广告时自动匹配 Token 有权限的认证主页
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/tw-certified-pages/resolve-name")
def resolve_tw_page_name(page_id: str, token_id: int = None, user=Depends(get_current_user)):
    """
    根据主页 ID 自动获取主页名称和 verified_identity_id（主页所属用户的 FB User ID）。
    优先使用指定 token_id 对应的 Token 查询（该 Token 需对主页有管理权限才能获取 owner 信息）。
    verified_identity_id 即 FB 台湾广告透明度声明中的「认证人编号」，等于主页 owner 的 FB User ID。
    如果 Token 对该主页没有管理权限，owner 字段会为空，需手动填写。
    """
    import requests as _req
    conn = get_conn()
    token_plain = None
    token_alias = None

    if token_id:
        # 使用指定 Token
        row = conn.execute(
            "SELECT access_token_enc, token_alias FROM fb_tokens WHERE id=? AND status='active'",
            (token_id,)
        ).fetchone()
        if row:
            token_plain = decrypt_token(row["access_token_enc"])
            token_alias = row["token_alias"]

    if not token_plain:
        # 回退：用任意有效 Token
        op_rows = conn.execute(
            "SELECT access_token_enc, token_alias FROM fb_tokens WHERE status='active' ORDER BY id LIMIT 10"
        ).fetchall()
        for row in op_rows:
            plain = decrypt_token(row["access_token_enc"])
            if plain:
                token_plain = plain
                token_alias = row["token_alias"]
                break
    conn.close()

    if not token_plain:
        raise HTTPException(400, "系统中无有效 Token，无法自动获取主页名称")

    import concurrent.futures as _cf
    try:
        page_name = None
        verified_identity_id = None
        page_token = None
        me_user_id = None
        owner_hint = None
        found_token_alias = token_alias

        conn2 = get_conn()
        all_tokens = conn2.execute(
            "SELECT id, access_token_enc, token_alias FROM fb_tokens WHERE status='active' ORDER BY id"
        ).fetchall()
        conn2.close()

        # 优先用指定 token_id 的 Token，放在最前面
        token_rows = []
        if token_id:
            for r in all_tokens:
                if r["id"] == token_id:
                    token_rows.insert(0, r)
                else:
                    token_rows.append(r)
        else:
            token_rows = list(all_tokens)

        # 将所有 Token 解密，过滤掉无效的
        plain_tokens = []
        for t_row in token_rows:
            plain = decrypt_token(t_row["access_token_enc"])
            if plain:
                plain_tokens.append((t_row["id"], plain, t_row["token_alias"]))

        def try_me_accounts(args):
            tid, plain, alias = args
            try:
                r = _req.get(
                    "https://graph.facebook.com/v25.0/me/accounts",
                    params={"fields": "id,name,access_token", "access_token": plain, "limit": 200},
                    timeout=5
                )
                d = r.json()
                for pg in d.get("data", []):
                    if str(pg.get("id", "")) == str(page_id):
                        return (pg.get("name", ""), pg.get("access_token", ""), alias)
            except Exception:
                pass
            return None

        # 并发查询所有 Token 的 /me/accounts，最多等 8 秒
        with _cf.ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(try_me_accounts, args): args for args in plain_tokens}
            for fut in _cf.as_completed(futures, timeout=8):
                result = fut.result()
                if result:
                    page_name, page_token, found_token_alias = result
                    break

        # 如果 /me/accounts 找不到，尝试直接查公开主页信息（只用前3个 Token）
        if not page_name:
            for _, plain, alias in plain_tokens[:3]:
                try:
                    r_pub = _req.get(
                        f"https://graph.facebook.com/v25.0/{page_id}",
                        params={"fields": "id,name", "access_token": plain},
                        timeout=5
                    )
                    d_pub = r_pub.json()
                    if d_pub.get("name") and "error" not in d_pub:
                        page_name = d_pub["name"]
                        found_token_alias = alias
                        break
                except Exception:
                    continue

        if not page_name:
            raise HTTPException(400, "无法获取主页名称，请检查主页 ID 是否正确，或手动填写主页名称")

        # 如果拿到了 page_token，用它查 owner（台湾认证人编号）
        if page_token:
            try:
                r_own = _req.get(
                    f"https://graph.facebook.com/v25.0/{page_id}",
                    params={"fields": "id,name,owner", "access_token": page_token},
                    timeout=5
                )
                d_own = r_own.json()
                owner = d_own.get("owner", {})
                if isinstance(owner, dict) and owner.get("id"):
                    verified_identity_id = str(owner["id"])
                    owner_hint = "通过主页 owner 字段自动获取"
            except Exception:
                pass

        # 如果还是没有 verified_identity_id，尝试用 /me 获取当前 Token 用户 ID 作参考
        if not verified_identity_id and plain_tokens:
            try:
                _, plain0, _ = plain_tokens[0]
                r3 = _req.get(
                    "https://graph.facebook.com/v25.0/me",
                    params={"fields": "id,name", "access_token": plain0},
                    timeout=5
                )
                d3 = r3.json()
                if d3.get("id") and "error" not in d3:
                    me_user_id = str(d3["id"])
            except Exception:
                pass

        return {
            "success": True,
            "page_id": page_id,
            "page_name": page_name,
            "verified_identity_id": verified_identity_id,
            "me_user_id": me_user_id,
            "owner_hint": owner_hint or ("该主页不属于任何已录入 Token 的管理主页，认证人编号请手动填写" if not verified_identity_id else None),
            "token_used": found_token_alias
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"获取主页名称失败：{e}")


@router.get("/tw-certified-pages")
def list_tw_certified_pages(user=Depends(get_current_user)):
    """列出所有台湾认证主页（含归属矩阵和Token信息）"""
    conn = get_conn()
    # 确保新字段存在（兼容旧数据库）
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN matrix_id INTEGER DEFAULT NULL")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN token_id INTEGER DEFAULT NULL REFERENCES fb_tokens(id)")
        conn.commit()
    except Exception:
        pass
    rows = conn.execute(
        """
        SELECT p.id, p.page_id, p.page_name, p.verified_identity_id, p.note, p.created_at,
               p.matrix_id, p.token_id,
               ft.token_alias
        FROM tw_certified_pages p
        LEFT JOIN fb_tokens ft ON ft.id = p.token_id
        ORDER BY p.id
        """
    ).fetchall()
    conn.close()
    return {"success": True, "pages": [dict(r) for r in rows]}


@router.post("/tw-certified-pages")
def create_tw_certified_page(body: dict, user=Depends(get_current_user)):
    """
    新增台湾认证主页。
    - page_id: 必填
    - page_name: 可选（留空则自动获取）
    - verified_identity_id: 台湾广告认证人编号（FB User ID），可选（留空则尝试自动获取）
    - matrix_id: 归属矩阵 ID（1=矩阵1, 2=矩阵2 等）
    - token_id: 对该主页有管理权限的 Token ID
    - note: 备注
    """
    import requests as _req
    page_id = str(body.get("page_id", "")).strip()
    page_name = str(body.get("page_name", "")).strip()
    note = body.get("note", "")
    verified_identity_id = str(body.get("verified_identity_id", "")).strip() or None
    matrix_id = body.get("matrix_id") or None
    token_id = body.get("token_id") or None
    if not page_id:
        raise HTTPException(400, "page_id 不能为空")

    # 自动获取主页名称（优先用指定 token_id，否则用任意有效 Token）
    conn_tmp = get_conn()
    if token_id:
        token_rows = conn_tmp.execute(
            "SELECT access_token_enc FROM fb_tokens WHERE id=? AND status='active'",
            (token_id,)
        ).fetchall()
    else:
        token_rows = conn_tmp.execute(
            "SELECT access_token_enc FROM fb_tokens WHERE status='active' ORDER BY id LIMIT 10"
        ).fetchall()
    conn_tmp.close()

    for row_tmp in token_rows:
        plain_tmp = decrypt_token(row_tmp["access_token_enc"])
        if plain_tmp:
            try:
                resp_tmp = _req.get(
                    f"https://graph.facebook.com/v25.0/{page_id}",
                    params={"fields": "id,name,owner", "access_token": plain_tmp},
                    timeout=10
                )
                data_tmp = resp_tmp.json()
                if "name" in data_tmp and not page_name:
                    page_name = data_tmp["name"]
                # 尝试通过 owner.id 获取 verified_identity_id
                if not verified_identity_id:
                    owner = data_tmp.get("owner", {})
                    if isinstance(owner, dict) and owner.get("id"):
                        verified_identity_id = str(owner["id"])
            except Exception:
                pass
            break

    if not page_name:
        raise HTTPException(400, "主页名称自动获取失败，请手动填写 page_name")

    conn = get_conn()
    # 确保新字段存在
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN matrix_id INTEGER DEFAULT NULL")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tw_certified_pages ADD COLUMN token_id INTEGER DEFAULT NULL REFERENCES fb_tokens(id)")
        conn.commit()
    except Exception:
        pass
    try:
        cur = conn.execute(
            "INSERT INTO tw_certified_pages (page_id, page_name, verified_identity_id, note, matrix_id, token_id) VALUES (?,?,?,?,?,?)",
            (page_id, page_name, verified_identity_id, note, matrix_id, token_id)
        )
        conn.commit()
        new_id = cur.lastrowid
    except Exception as e:
        conn.close()
        raise HTTPException(400, f"添加失败（主页ID可能已存在）: {e}")
    conn.close()
    return {"success": True, "id": new_id, "page_name": page_name, "verified_identity_id": verified_identity_id, "matrix_id": matrix_id, "token_id": token_id}


@router.put("/tw-certified-pages/{page_db_id}")
def update_tw_certified_page(page_db_id: int, body: dict, user=Depends(get_current_user)):
    """更新台湾认证主页（支持更新 matrix_id、token_id、verified_identity_id 等字段）"""
    conn = get_conn()
    updates, params = [], []
    if "page_name" in body:
        updates.append("page_name=?"); params.append(body["page_name"])
    if "note" in body:
        updates.append("note=?"); params.append(body["note"])
    if "verified_identity_id" in body:
        updates.append("verified_identity_id=?")
        params.append(str(body["verified_identity_id"]).strip() or None)
    if "matrix_id" in body:
        updates.append("matrix_id=?")
        params.append(body["matrix_id"] or None)
    if "token_id" in body:
        updates.append("token_id=?")
        params.append(body["token_id"] or None)
    if updates:
        params.append(page_db_id)
        conn.execute(f"UPDATE tw_certified_pages SET {','.join(updates)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return {"success": True}


@router.delete("/tw-certified-pages/{page_db_id}")
def delete_tw_certified_page(page_db_id: int, user=Depends(get_current_user)):
    """删除台湾认证主页"""
    conn = get_conn()
    conn.execute("DELETE FROM tw_certified_pages WHERE id=?", (page_db_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@router.post("/tw-certified-pages/scan")
def scan_tw_certified_pages(user=Depends(get_current_user)):
    """
    自动扫描：遍历所有 active Token 的 /me/accounts，
    将所有主页入库 tw_certified_pages（已存在则跳过），
    同时尝试通过 page_token 获取 owner.id（台湾认证人编号）。
    返回：新增数量、已存在数量、扫描到的主页总数。
    """
    import requests as _req
    import concurrent.futures as _cf

    conn = get_conn()
    # 确保字段存在
    for col_sql in [
        "ALTER TABLE tw_certified_pages ADD COLUMN matrix_id INTEGER DEFAULT NULL",
        "ALTER TABLE tw_certified_pages ADD COLUMN token_id INTEGER DEFAULT NULL REFERENCES fb_tokens(id)",
    ]:
        try:
            conn.execute(col_sql); conn.commit()
        except Exception:
            pass

    # 获取所有 active Token（含矩阵归属）
    token_rows = conn.execute(
        """
        SELECT ft.id, ft.access_token_enc, ft.token_alias, ft.matrix_id
        FROM fb_tokens ft
        WHERE ft.status = 'active'
        ORDER BY ft.id
        """
    ).fetchall()

    # 获取已存在的主页 ID 集合
    existing = {r["page_id"] for r in conn.execute("SELECT page_id FROM tw_certified_pages").fetchall()}
    conn.close()

    # 解密所有 Token，同时获取 /me 用户 ID（即台湾认证人编号）
    plain_tokens = []
    def _get_token_user_id(t):
        plain = decrypt_token(t["access_token_enc"])
        if not plain:
            return None
        try:
            import requests as _req2
            r = _req2.get(
                "https://graph.facebook.com/v25.0/me",
                params={"fields": "id,name", "access_token": plain},
                timeout=8
            )
            d = r.json()
            return {
                "id": t["id"],
                "plain": plain,
                "alias": t["token_alias"],
                "matrix_id": t["matrix_id"],
                "user_id": str(d.get("id", "")) or None,
                "user_name": d.get("name", "")
            }
        except Exception:
            return {
                "id": t["id"],
                "plain": plain,
                "alias": t["token_alias"],
                "matrix_id": t["matrix_id"],
                "user_id": None,
                "user_name": ""
            }

    import concurrent.futures as _cf2
    with _cf2.ThreadPoolExecutor(max_workers=8) as ex2:
        results2 = list(ex2.map(_get_token_user_id, token_rows, timeout=20))
    plain_tokens = [r for r in results2 if r is not None]

    def fetch_pages_for_token(t_info):
        """获取单个 Token 管理的所有主页"""
        results = []
        try:
            r = _req.get(
                "https://graph.facebook.com/v25.0/me/accounts",
                params={"fields": "id,name,access_token", "access_token": t_info["plain"], "limit": 200},
                timeout=8
            )
            d = r.json()
            for pg in d.get("data", []):
                if pg.get("id") and pg.get("name"):
                    results.append({
                        "page_id": str(pg["id"]),
                        "page_name": pg["name"],
                        "page_token": pg.get("access_token", ""),
                        "token_id": t_info["id"],
                        "token_alias": t_info["alias"],
                        "matrix_id": t_info["matrix_id"]
                    })
        except Exception:
            pass
        return results

    # 并发扫描所有 Token
    all_pages = []
    with _cf.ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch_pages_for_token, t) for t in plain_tokens]
        for fut in _cf.as_completed(futures, timeout=20):
            try:
                all_pages.extend(fut.result())
            except Exception:
                pass

    # 去重（同一主页可能被多个 Token 管理，保留第一个）
    seen_pages = {}
    for pg in all_pages:
        if pg["page_id"] not in seen_pages:
            seen_pages[pg["page_id"]] = pg

    # 构建 token_id -> user_id 映射（用 /me 获取的用户 ID 即为台湾认证人编号）
    token_user_id_map = {t["id"]: t.get("user_id") for t in plain_tokens if t.get("user_id")}

    # 对所有主页填入 verified_identity_id（来自 Token 的 /me 用户 ID）
    all_page_list = list(seen_pages.values())
    for pg in all_page_list:
        tid = pg.get("token_id")
        if tid and tid in token_user_id_map:
            pg["verified_identity_id"] = token_user_id_map[tid]

    # 分为新增和已存在两组
    new_pages = [pg for pg in all_page_list if pg["page_id"] not in existing]

    # 写入数据库
    added = 0
    updated_vid = 0
    skipped = 0
    conn2 = get_conn()
    for pg in all_page_list:
        vid = pg.get("verified_identity_id") or None
        if pg["page_id"] not in existing:
            # 新增
            try:
                conn2.execute(
                    "INSERT OR IGNORE INTO tw_certified_pages "
                    "(page_id, page_name, verified_identity_id, note, matrix_id, token_id) "
                    "VALUES (?,?,?,?,?,?)",
                    (
                        pg["page_id"],
                        pg["page_name"],
                        vid,
                        f"自动扫描（Token: {pg['token_alias']}）",
                        pg.get("matrix_id") or None,
                        pg.get("token_id") or None
                    )
                )
                added += 1
            except Exception:
                pass
        else:
            # 已存在：如果 verified_identity_id 为空则自动填入
            if vid:
                try:
                    conn2.execute(
                        "UPDATE tw_certified_pages SET verified_identity_id=? "
                        "WHERE page_id=? AND (verified_identity_id IS NULL OR verified_identity_id='')",
                        (vid, pg["page_id"])
                    )
                    updated_vid += conn2.execute("SELECT changes()").fetchone()[0]
                except Exception:
                    pass
            skipped += 1
    conn2.commit()
    conn2.close()

    return {
        "success": True,
        "scanned_tokens": len(plain_tokens),
        "total_pages_found": len(seen_pages),
        "added": added,
        "updated_verified_id": updated_vid,
        "skipped_existing": skipped,
        "pages": [
            {
                "page_id": pg["page_id"],
                "page_name": pg["page_name"],
                "verified_identity_id": pg.get("verified_identity_id"),
                "token_alias": pg["token_alias"],
                "matrix_id": pg.get("matrix_id"),
                "is_new": pg["page_id"] not in existing
            }
            for pg in seen_pages.values()
        ]
    }


@router.get("/{act_id}/tw-matched-pages")
def get_tw_matched_pages(act_id: str, user=Depends(get_current_user)):
    """
    自动匹配：用该账户绑定的 Token 查询 FB 主页列表，
    与台湾认证主页库对比，返回匹配到的认证主页
    """
    import requests as _req
    conn = get_conn()

    # 获取认证主页库
    certified = conn.execute(
        "SELECT page_id, page_name, verified_identity_id FROM tw_certified_pages"
    ).fetchall()
    certified_ids = {r["page_id"]: {"page_name": r["page_name"], "verified_identity_id": r["verified_identity_id"]} for r in certified}

    if not certified_ids:
        conn.close()
        return {"success": True, "matched": [], "all_certified": []}

    # 获取该账户的操作号 Token（优先）或管理号 Token
    token_row = None
    # 先找操作号池
    op_rows = conn.execute(
        """SELECT ft.access_token_enc FROM account_op_tokens aot
           JOIN fb_tokens ft ON ft.id = aot.token_id
           WHERE aot.act_id=? AND ft.status='active'
           ORDER BY aot.priority LIMIT 1""",
        (act_id,)
    ).fetchone()
    if op_rows:
        token_row = op_rows
    else:
        # 用管理号 Token
        mgr = conn.execute(
            """SELECT ft.access_token_enc FROM accounts a
               JOIN fb_tokens ft ON ft.id = a.token_id
               WHERE a.act_id=? AND ft.status='active' LIMIT 1""",
            (act_id,)
        ).fetchone()
        if not mgr:
            # 回退：用 fb_tokens 表中任意 active 的 user token
            mgr = conn.execute(
                "SELECT access_token_enc FROM fb_tokens WHERE status='active' LIMIT 1"
            ).fetchone()
        if mgr:
            token_row = mgr

    conn.close()

    if not token_row:
        return {"success": True, "matched": [], "all_certified": list(certified_ids.items()), "error": "无可用Token"}

    try:
        raw = decrypt_token(token_row["access_token_enc"])
    except Exception as e:
        return {"success": True, "matched": [], "error": f"Token解密失败: {e}"}

    # 拉取该 Token 有管理权限的主页列表
    matched = []
    try:
        r = _req.get("https://graph.facebook.com/v25.0/me/accounts", params={
            "fields": "id,name,access_token",
            "access_token": raw,
            "limit": 100
        }, timeout=10)
        data = r.json()
        pages = data.get("data", [])
        for p in pages:
            pid = str(p.get("id", ""))
            if pid in certified_ids:
                cert_info = certified_ids[pid]
                matched.append({
                    "page_id": pid,
                    "page_name": cert_info["page_name"],
                    "verified_identity_id": cert_info["verified_identity_id"],
                    "fb_name": p.get("name", "")
                })
    except Exception as e:
        return {"success": True, "matched": [], "error": f"FB API 调用失败: {e}"}

    return {
        "success": True,
        "matched": matched,
        "all_certified": [
            {"page_id": k, "page_name": v["page_name"], "verified_identity_id": v["verified_identity_id"]}
            for k, v in certified_ids.items()
        ]
    }


# ─────────────────────────────────────────────────────────────────────────────
# 主页消息功能检查（铺广告前置检查）
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/{act_id}/check-page-messaging")
def check_page_messaging(act_id: str, page_id: str, user=Depends(get_current_user)):
    """
    检查指定主页是否开启了消息功能（messaging_feature_status）。
    用于铺消息广告前的前置检查，避免因主页消息关闭导致 Ad 创建失败。
    返回：{messaging_enabled: bool, page_name: str, error: str|null}
    """
    import requests as _req

    def _get_best_token(act_id_: str) -> str | None:
        """获取该账户最佳可用 Token（操作号优先，管理号备用）"""
        conn = get_conn()
        # 操作号 Token
        rows = conn.execute("""
            SELECT t.access_token_enc FROM account_op_tokens aot
            JOIN fb_tokens t ON t.id = aot.token_id
            WHERE aot.act_id = ? AND aot.status = 'active' AND t.status = 'active'
            LIMIT 1
        """, (act_id_,)).fetchall()
        if rows:
            conn.close()
            return decrypt_token(rows[0]["access_token_enc"])
        # 管理号 Token
        row = conn.execute("""
            SELECT t.access_token_enc FROM accounts a
            JOIN fb_tokens t ON t.id = a.token_id
            WHERE a.act_id = ? AND t.status = 'active'
            LIMIT 1
        """, (act_id_,)).fetchone()
        conn.close()
        if row:
            return decrypt_token(row["access_token_enc"])
        return None

    token = _get_best_token(act_id)
    if not token:
        return {"messaging_enabled": None, "page_name": "", "error": "无可用 Token，无法检查主页状态"}

    try:
        # 通过 Graph API 查询主页的 messaging_feature_status 和 name
        r = _req.get(
            f"{FB_API_BASE}/{page_id}",
            params={
                "access_token": token,
                "fields": "id,name,messaging_feature_status,features"
            },
            timeout=10
        )
        data = r.json()

        if "error" in data:
            err = data["error"]
            return {
                "messaging_enabled": None,
                "page_name": "",
                "error": f"FB API 错误: {err.get('message', str(err))}"
            }

        page_name = data.get("name", page_id)
        messaging_feature_status = data.get("messaging_feature_status", {})
        features = data.get("features", [])

        # messaging_feature_status 是一个 dict，key 为功能名，value 为状态
        # 主要检查 "MESSENGER_PLATFORM" 或 "WHATSAPP_PLATFORM" 是否为 "ENABLED"
        # 如果字段不存在，尝试通过 features 列表判断
        messaging_enabled = None

        if messaging_feature_status:
            # 检查 MESSENGER_PLATFORM 状态
            messenger_status = messaging_feature_status.get("MESSENGER_PLATFORM", "")
            if messenger_status:
                messaging_enabled = (messenger_status.upper() == "ENABLED")
            else:
                # 任意消息平台启用即可
                for k, v in messaging_feature_status.items():
                    if "MESSAG" in k.upper() or "WHATSAPP" in k.upper():
                        if str(v).upper() == "ENABLED":
                            messaging_enabled = True
                            break
                if messaging_enabled is None:
                    messaging_enabled = False
        elif features:
            # features 是字符串列表
            messaging_enabled = any(
                "messag" in str(f).lower() or "whatsapp" in str(f).lower()
                for f in features
            )
        else:
            # 无法确定，返回 None（不阻止铺广告，只警告）
            messaging_enabled = None

        return {
            "messaging_enabled": messaging_enabled,
            "page_name": page_name,
            "error": None
        }

    except Exception as e:
        return {
            "messaging_enabled": None,
            "page_name": "",
            "error": f"检查失败: {str(e)}"
        }


# ── Phase 3: 账户生命周期设置 API ──────────────────────────────────────────
@router.post("/{act_id}/lifecycle")
async def set_account_lifecycle_api(act_id: str, body: dict, _=Depends(get_current_user)):
    """手动设置账户生命周期阶段（warmup/testing/scaling/paused）"""
    stage = body.get("stage", "testing")
    valid_stages = ["new", "warmup", "testing", "scaling", "paused", "banned"]
    if stage not in valid_stages:
        raise HTTPException(status_code=400, detail=f"无效的生命周期阶段，有效值: {valid_stages}")
    try:
        from services.lifecycle_manager import set_account_lifecycle
        set_account_lifecycle(act_id, stage)
        return {"ok": True, "act_id": act_id, "stage": stage}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════
# 批量账户配置 / CSV 导入导出 / 重新扫描
# ══════════════════════════════════════════════════════════════

class BatchConfigPayload(BaseModel):
    act_ids: List[str]
    target_countries: Optional[str] = None
    target_age_min: Optional[int] = None
    target_age_max: Optional[int] = None
    target_gender: Optional[int] = None
    target_placements: Optional[str] = None
    target_objective_type: Optional[str] = None
    landing_url: Optional[str] = None


@router.post("/batch-config")
async def batch_config_accounts(
    payload: BatchConfigPayload,
    current_user=Depends(get_current_user)
):
    """批量修改多个账户的投放配置"""
    conn = get_conn()
    try:
        updated = 0
        for act_id in payload.act_ids:
            row = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
            if not row:
                continue
            acc_id = row[0]
            fields = []
            vals = []
            if payload.target_countries is not None:
                fields.append("target_countries=?"); vals.append(payload.target_countries)
            if payload.target_age_min is not None:
                fields.append("target_age_min=?"); vals.append(payload.target_age_min)
            if payload.target_age_max is not None:
                fields.append("target_age_max=?"); vals.append(payload.target_age_max)
            if payload.target_gender is not None:
                fields.append("target_gender=?"); vals.append(payload.target_gender)
            if payload.target_placements is not None:
                fields.append("target_placements=?"); vals.append(payload.target_placements)
            if payload.target_objective_type is not None:
                fields.append("target_objective_type=?"); vals.append(payload.target_objective_type)
            if payload.landing_url is not None:
                fields.append("landing_url=?"); vals.append(payload.landing_url)
            if fields:
                vals.append(acc_id)
                conn.execute(f"UPDATE accounts SET {', '.join(fields)} WHERE id=?", vals)
                updated += 1
        conn.commit()
        return {"updated": updated, "total": len(payload.act_ids)}
    finally:
        conn.close()


@router.get("/export-config")
async def export_account_config(
    token: str = None,
    current_user=None
):
    # Support ?token= query param for direct browser download
    from fastapi import HTTPException
    from core.auth import decode_token
    if current_user is None:
        if not token:
            raise HTTPException(status_code=401, detail="Not authenticated")
        try:
            current_user = decode_token(token)
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token")
    """导出所有账户配置为 CSV"""
    import csv, io
    from fastapi.responses import StreamingResponse

    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT act_id, name, target_countries, target_age_min, target_age_max, "
            "target_gender, target_placements, target_objective_type, landing_url FROM accounts"
        ).fetchall()
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["act_id", "name", "target_countries", "target_age_min", "target_age_max",
                     "target_gender", "target_placements", "target_objective_type", "landing_url"])
    for r in rows:
        writer.writerow([r[0] or "", r[1] or "", r[2] or "", r[3] or 25, r[4] or 65,
                         r[5] or 0, r[6] or "", r[7] or "", r[8] or ""])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=account_config.csv"}
    )


@router.post("/import-config")
async def import_account_config(
    file: UploadFile = File(...),
    current_user=Depends(get_current_user)
):
    """从 CSV 文件批量导入账户配置"""
    import csv, io
    raw = await file.read()
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    conn = get_conn()
    try:
        updated = 0
        errors = []
        for row in reader:
            act_id = row.get("act_id", "").strip()
            if not act_id:
                continue
            acc = conn.execute("SELECT id FROM accounts WHERE act_id=?", (act_id,)).fetchone()
            if not acc:
                errors.append(f"账户 {act_id} 不存在")
                continue
            try:
                fields = []
                vals = []
                if row.get("target_countries"):
                    fields.append("target_countries=?"); vals.append(row["target_countries"].strip())
                if row.get("target_age_min"):
                    fields.append("target_age_min=?"); vals.append(int(row["target_age_min"]))
                if row.get("target_age_max"):
                    fields.append("target_age_max=?"); vals.append(int(row["target_age_max"]))
                if row.get("target_gender") not in (None, ""):
                    fields.append("target_gender=?"); vals.append(int(row.get("target_gender", 0)))
                if row.get("target_placements"):
                    fields.append("target_placements=?"); vals.append(row["target_placements"].strip())
                if row.get("target_objective_type"):
                    _obj_alias = {"sales":"OUTCOME_SALES","website":"OUTCOME_TRAFFIC","leads":"OUTCOME_LEADS","engagement":"OUTCOME_ENGAGEMENT","messages":"MESSAGES"}
                    _obj_val = row["target_objective_type"].strip()
                    _obj_val = _obj_alias.get(_obj_val, _obj_val)
                    fields.append("target_objective_type=?"); vals.append(_obj_val)
                if row.get("landing_url"):
                    fields.append("landing_url=?"); vals.append(row["landing_url"].strip())
                if fields:
                    vals.append(acc[0])
                    conn.execute(f"UPDATE accounts SET {', '.join(fields)} WHERE id=?", vals)
                    updated += 1
            except Exception as e:
                errors.append(f"账户 {act_id} 更新失败: {str(e)}")
        conn.commit()
        return {"updated": updated, "errors": errors}
    finally:
        conn.close()


@router.post("/{act_id}/rescan")
async def rescan_account_assets(
    act_id: str,
    current_user=Depends(get_current_user)
):
    """重新扫描账户素材（将所有素材状态重置为待扫描）"""
    conn = get_conn()
    try:
        try:
            result = conn.execute(
                "UPDATE assets SET scan_status='pending', last_scanned_at=NULL WHERE act_id=?",
                (act_id,)
            )
            conn.commit()
            reset_count = result.rowcount
        except Exception:
            # assets 表可能字段不同
            try:
                row = conn.execute("SELECT COUNT(*) FROM assets WHERE act_id=?", (act_id,)).fetchone()
                reset_count = row[0] if row else 0
            except Exception:
                reset_count = 0
        return {"act_id": act_id, "assets_reset": reset_count, "message": "重新扫描已触发"}
    finally:
        conn.close()

@router.patch("/{token_id}/value")
async def update_token_value(token_id: int, body: dict, current_user=Depends(get_current_user)):
    """Update the access token value and re-verify with Facebook"""
    import requests as req_lib
    new_token = (body.get("access_token") or "").strip()
    if not new_token or len(new_token) < 20:
        raise HTTPException(status_code=400, detail="Token value is invalid")
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, token_alias FROM fb_tokens WHERE id=?", (token_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Token not found")
        # Verify with Facebook
        try:
            resp = req_lib.get(
                "https://graph.facebook.com/v19.0/me",
                params={"access_token": new_token, "fields": "id,name"},
                timeout=10
            )
            fb_data = resp.json()
            if "error" in fb_data:
                raise HTTPException(status_code=400, detail=f"FB verification failed: {fb_data['error'].get('message','unknown')}")
            fb_name = fb_data.get("name", "")
            fb_id = fb_data.get("id", "")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"FB API error: {str(e)}")
        # Encrypt and store
        enc = encrypt_token(new_token)
        conn.execute(
            "UPDATE fb_tokens SET access_token_enc=?, status='active', last_verified_at=datetime('now','+8 hours') WHERE id=?",
            (enc, token_id)
        )
        conn.commit()
        return {
            "success": True,
            "token_id": token_id,
            "fb_id": fb_id,
            "fb_name": fb_name,
            "message": f"Token updated and verified: {fb_name} ({fb_id})"
        }
    finally:
        conn.close()


# ── AI 决策日志 API ──────────────────────────────────────────────────────────
@router.get("/ai-decisions")
def get_ai_decisions(act_id: str = None, limit: int = 100, current_user=Depends(get_current_user)):
    conn = get_conn()
    if act_id:
        rows = conn.execute(
            "SELECT d.*, a.name as account_name FROM ai_decisions d "
            "LEFT JOIN accounts a ON d.act_id = a.act_id "
            "WHERE d.act_id=? ORDER BY d.created_at DESC LIMIT ?",
            (act_id, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT d.*, a.name as account_name FROM ai_decisions d "
            "LEFT JOIN accounts a ON d.act_id = a.act_id "
            "ORDER BY d.created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/{act_id}/toggle-ai")
def toggle_ai_managed(act_id: str, body: dict, current_user=Depends(get_current_user)):
    enabled = 1 if body.get("enabled") else 0
    conn = get_conn()
    conn.execute("UPDATE accounts SET ai_managed=? WHERE act_id=?", (enabled, act_id))
    conn.commit()
    conn.close()
    return {"ok": True, "ai_managed": enabled}
