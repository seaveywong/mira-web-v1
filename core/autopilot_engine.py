"""
Mira v3.0 全自动铺广告执行引擎 (AutoPilot Engine)
────────────────────────────────────────────────
职责：
  1. 从 auto_campaigns 表读取 pending 任务
  2. 根据素材的 AI 文案 + 受众兴趣词，生成"笛卡尔积"广告矩阵
  3. 使用 TokenManager 获取操作号 Token
  4. 调用 Facebook Graph API 批量创建：
       Campaign → AdSet（每组受众一个）→ Ad（每组文案一条）
  5. 将创建结果写回 auto_campaign_ads 表
  6. 更新 auto_campaigns 状态

设计原则：
  - 非侵入式：不修改任何现有巡检逻辑，广告创建后由现有 guard_engine 接管
  - 操作号隔离：全程使用 ACTION_CREATE Token，操作号全灭则拒绝执行
  - 幂等安全：每次执行前检查 fb_campaign_id 是否已存在，避免重复建营
  - 失败隔离：单个 AdSet/Ad 创建失败不影响其他组继续执行
"""

import json
import logging
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

from core.database import get_conn
from services.token_manager import get_exec_token, ACTION_CREATE, suspend_token_by_plain

logger = logging.getLogger("mira.autopilot")

FB_API_BASE = "https://graph.facebook.com/v25.0"
FB_API_VERSION = "v25.0"


class AutoPilotEngine:
    """全自动铺广告执行引擎"""

    def __init__(self):
        self.conn = None

    def _get_setting(self, key: str, default: str = "") -> str:
        try:
            conn = get_conn()
            row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            conn.close()
            return row["value"] if row else default
        except Exception:
            return default

    def _fb_get(self, path: str, token: str, params: dict = None) -> dict:
        """GET 请求到 FB Graph API"""
        p = params or {}
        p["access_token"] = token
        resp = requests.get(f"{FB_API_BASE}/{path}", params=p, timeout=30)
        data = resp.json()
        if "error" in data:
            raise Exception(f"FB API Error: {data['error'].get('message', str(data['error']))}")
        return data

    def _fb_post(self, path: str, token: str, payload: dict) -> dict:
        """POST 请求到 FB Graph API"""
        payload["access_token"] = token
        resp = requests.post(f"{FB_API_BASE}/{path}", json=payload, timeout=30)
        data = resp.json()
        if "error" in data:
            debug_payload = {k: v for k, v in payload.items() if k != "access_token"}
            err = data["error"]
            logger.error(f"[AutoPilot] FB API Error on {path}: {err} | payload={debug_payload}")
            # 检测认证类错误（error_subcode=2859002: Non-discrimination Certification Required）
            # 这类错误说明该 Token 对应的账号需要完成 FB 认证，自动暂停该 Token
            _subcode = err.get("error_subcode") or err.get("error_user_msg", "")
            _is_cert_err = (
                err.get("error_subcode") == 2859002
                or "certification" in str(err.get("error_user_title", "")).lower()
                or "certification" in str(err.get("error_user_msg", "")).lower()
                or "certif" in str(err.get("message", "")).lower()
            )
            if _is_cert_err:
                logger.warning(f"[AutoPilot] Token 需要认证，自动暂停: {err.get('error_user_title', '')}")
                try:
                    suspend_token_by_plain(token, reason="certification_required")
                except Exception as _se:
                    logger.error(f"[AutoPilot] 自动暂停 Token 失败: {_se}")
                raise Exception(f"FB API Error [CertRequired]: {err.get('error_user_title', err.get('message', str(err)))}")
            raise Exception(f"FB API Error: {err.get('message', str(err))}")
        return data

    # ── 主入口 ────────────────────────────────────────────────────────────────

    def run_campaign(self, campaign_id: int):
        """
        执行单个自动铺广告任务。
        由 assets.py 的 _trigger_autopilot 在后台线程中调用。
        """
        conn = get_conn()
        campaign = conn.execute(
            "SELECT * FROM auto_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
        conn.close()
        campaign = dict(campaign) if campaign else None

        if not campaign:
            logger.error(f"[AutoPilot] campaign_id={campaign_id} 不存在")
            return

        if campaign["status"] not in ("pending", "error"):
            logger.info(f"[AutoPilot] campaign_id={campaign_id} 状态为 {campaign['status']}，跳过")
            return

        # 检查是否已建过 Campaign（幂等保护）
        if campaign["fb_campaign_id"]:
            logger.info(f"[AutoPilot] campaign_id={campaign_id} 已有 fb_campaign_id，跳过重复建营")
            # 修复：有fb_campaign_id但状态仍为pending，说明之前执行到一半被中断，更新为done避免永久卡死
            if campaign["status"] == "pending":
                self._update_campaign_status(campaign_id, "done")
                logger.info(f"[AutoPilot] campaign_id={campaign_id} 状态从pending修正为done")
            return

        act_id = campaign["act_id"]
        self._update_campaign_status(campaign_id, "running")

        try:
            # 1. 获取操作号 Token
            token = get_exec_token(act_id, ACTION_CREATE)
            self._update_progress(campaign_id, "token", "获取操作号 Token...")
            if not token:
                raise Exception(
                    f"账户 {act_id} 无可用操作号 Token，CREATE 操作已被拦截。"
                    "请在账户管理中补充操作号后重试。"
                )

            # 2. 加载素材和 AI 文案
            self._update_progress(campaign_id, "asset", "加载素材和 AI 文案...")
            asset = self._load_asset(campaign["asset_id"])
            if not asset:
                raise Exception(f"素材 ID={campaign['asset_id']} 不存在")

            headlines = self._parse_json_field(asset["ai_headlines"], [])
            bodies = self._parse_json_field(asset["ai_bodies"], [])
            interests = self._parse_json_field(asset["ai_interests"], [])
            target_countries = self._parse_json_field(campaign["target_countries"], ["US"])

            if not headlines or not bodies:
                raise Exception("素材缺少 AI 生成的文案，请先完成 AI 分析")

            # 3. 获取系统配置
            # 账户级 page_id/pixel_id 优先，回退到全局设置
            from core.database import get_conn as _gc
            _c2 = _gc()
            _acc_row = _c2.execute(
                "SELECT page_id, pixel_id, currency, beneficiary, payer, tw_advertiser_id FROM accounts WHERE act_id=?", (act_id,)
            ).fetchone()
            _acc = dict(_acc_row) if _acc_row else None
            # 台湾认证身份读取（三层优先级）
            beneficiary = ""
            payer = ""
            tw_verified_id = ""  # verified_identity_id（regional_regulation_identities 方式）
            # 第1优先：铺广告时选择的认证主页（tw_page_id → tw_certified_pages 表）
            _camp_tw_page = campaign.get("tw_page_id") if campaign else None
            if _camp_tw_page:
                _cp_row = _c2.execute(
                    "SELECT page_name FROM tw_certified_pages WHERE page_id=?",
                    (_camp_tw_page,)
                ).fetchone()
                if _cp_row:
                    beneficiary = _cp_row["page_name"] or ""
                    payer = _cp_row["page_name"] or ""
            # 第2优先：账户绑定的认证身份（tw_advertiser_id → tw_advertisers 表，旧版兼容）
            if _acc and _acc.get("tw_advertiser_id"):
                _tw_adv_id = str(_acc["tw_advertiser_id"])
                # 查 tw_advertisers（id/fb_user_id 两种匹配），同时获取 fb_user_id 作为 verified_identity_id
                _tw_row = _c2.execute(
                    "SELECT beneficiary, payer, fb_user_id FROM tw_advertisers WHERE CAST(id AS TEXT)=? OR fb_user_id=? LIMIT 1",
                    (_tw_adv_id, _tw_adv_id)
                ).fetchone()
                if _tw_row:
                    if not beneficiary:
                        beneficiary = _tw_row["beneficiary"] or ""
                    if not payer:
                        payer = _tw_row["payer"] or ""
                    # fb_user_id 即为 verified_identity_id（数字ID），优先用于 regional_regulation_identities
                    tw_verified_id = _tw_row["fb_user_id"] or ""
                    logger.info(f"[AutoPilot] 台湾认证身份（tw_advertisers）: beneficiary={beneficiary}, payer={payer}, verified_id={tw_verified_id}")
                else:
                    # 兼容：tw_advertiser_id 实际存的是 tw_certified_pages.page_id
                    _cp_row2 = _c2.execute(
                        "SELECT page_name FROM tw_certified_pages WHERE page_id=? LIMIT 1",
                        (_tw_adv_id,)
                    ).fetchone()
                    if _cp_row2:
                        if not beneficiary:
                            beneficiary = _cp_row2["page_name"] or ""
                        if not payer:
                            payer = _cp_row2["page_name"] or ""
                        logger.info(f"[AutoPilot] 台湾认证身份（tw_certified_pages）: beneficiary={beneficiary}, payer={payer}")
            # 若 tw_verified_id 仍为空，尝试从 tw_advertisers 全局查找第一条已验证记录
            if not tw_verified_id:
                _any_tw = _c2.execute(
                    "SELECT fb_user_id FROM tw_advertisers WHERE verified=1 AND fb_user_id IS NOT NULL AND fb_user_id!='' LIMIT 1"
                ).fetchone()
                if _any_tw:
                    tw_verified_id = _any_tw["fb_user_id"]
                    logger.info(f"[AutoPilot] 台湾认证身份（全局兜底）: verified_id={tw_verified_id}")
            # 第3优先：直接存在 accounts 表的 beneficiary/payer 字段
            if not beneficiary and _acc:
                beneficiary = _acc.get("beneficiary", "") or ""
            if not payer and _acc:
                payer = _acc.get("payer", "") or ""
            _c2.close()
            if tw_verified_id:
                logger.info(f"[AutoPilot] 台湾广告认证信息: verified_id={tw_verified_id!r}（将使用 regional_regulation_identities 方式）")
            elif beneficiary or payer:
                logger.info(f"[AutoPilot] 台湾广告认证信息: beneficiary={beneficiary!r}, payer={payer!r}（旧版字符串方式）")
            else:
                logger.warning(f"[AutoPilot] 台湾广告认证信息为空，AdSet可能因缺少advertiser信息而失败")
            page_id = (campaign.get("page_id_override") or
                       (_acc["page_id"] if _acc and _acc["page_id"] else None) or
                       self._get_setting("autopilot_fb_page_id", ""))
            pixel_id = (campaign.get("pixel_id_override") or
                        (_acc["pixel_id"] if _acc and _acc["pixel_id"] else None) or
                        self._get_setting("autopilot_fb_pixel_id", ""))
            # 优先使用起动时传入的 max_adsets，否则使用全局设置
            max_adsets = int(campaign.get("max_adsets") or
                             self._get_setting("autopilot_max_adsets", "5"))
            # 用户输入的是 USD 预算，需换算为账户货币（FB API 要求账户货币单位）
            budget_usd = float(campaign["daily_budget"] or
                               self._get_setting("autopilot_test_budget", "20"))
            # 获取账户货币
            _acc_currency = (_acc["currency"] if _acc and _acc.get("currency") else "USD").upper()
            # 汇率表（USD -> 账户货币：1 USD = 1/rate 账户货币）
            _DEFAULT_RATES = {
                "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067,
                "CNY": 0.138, "HKD": 0.128, "TWD": 0.031, "SGD": 0.74,
                "AUD": 0.65, "CAD": 0.74, "BRL": 0.20, "MXN": 0.058,
                "CLP": 0.0011, "COP": 0.00025, "PEN": 0.27, "ARS": 0.001,
                "THB": 0.028, "VND": 0.000040, "IDR": 0.000063, "PHP": 0.017,
                "MYR": 0.21, "INR": 0.012, "TRY": 0.031, "ZAR": 0.053,
                "BDT": 0.0091, "PKR": 0.0036, "LKR": 0.0031, "NPR": 0.0075,
                "KRW": 0.00072, "CHF": 1.12, "NZD": 0.60, "SEK": 0.096,
                "NOK": 0.093, "DKK": 0.145, "PLN": 0.25, "CZK": 0.044,
                "HUF": 0.0028, "RON": 0.22, "BGN": 0.55, "AED": 0.272,
                "SAR": 0.267, "QAR": 0.275, "KWD": 3.26, "BHD": 2.65,
                "OMR": 2.60, "JOD": 1.41, "EGP": 0.021, "MAD": 0.099,
                "TND": 0.32, "GHS": 0.067, "NGN": 0.00065, "KES": 0.0077,
                "UAH": 0.027, "KZT": 0.0022, "GEL": 0.37,
            }
            if _acc_currency == "USD":
                test_budget = budget_usd
            else:
                # USD -> 账户货币：budget_usd / rate_of_acc_currency
                _rate = _DEFAULT_RATES.get(_acc_currency, 1.0)
                if _rate > 0:
                    test_budget = round(budget_usd / _rate, 2)
                else:
                    test_budget = budget_usd
                logger.info(
                    f"[AutoPilot] 预算换算: {budget_usd} USD -> {test_budget} {_acc_currency} "
                    f"(汇率 1 USD = {1/_rate:.4f} {_acc_currency})"
                )
            # 受众定向参数
            age_min = int(campaign.get("age_min") or 18)
            age_max = int(campaign.get("age_max") or 65)
            gender = int(campaign.get("gender") or 0)  # 0=不限, 1=男, 2=女
            # 版位设置
            placements = self._parse_json_field(campaign.get("placements"), None)
            # v4.0: 设备端（mobile/desktop/all）
            device_platforms = campaign.get("device_platforms") or "all"
            # v4.0: 广告语言
            ad_language = campaign.get("ad_language") or "en"
            # 出价策略
            bid_strategy = campaign.get("bid_strategy") or "LOWEST_COST_WITHOUT_CAP"

            # 落地页链接：三层优先级
            # 层內1：铺广告弹窗手动填写
            # 层內2：素材级绑定的链接
            # 层內3：系统全局默认链接
            landing_url = (campaign.get("landing_url") or
                           asset.get("landing_url") or
                           self._get_setting("default_landing_url", ""))
            # 表单链接：lead_generation目标时优先使用账户级form_link
            # 层级1：铺广告弹窗手动填写的landing_url（此时用作表单链接）
            # 层级2：账户级form_link（在投放链接管理页面配置）
            # 层级3：降级使用landing_url
            _acc_form_link = ""
            try:
                import sqlite3 as _sq3
                _db = _sq3.connect("/opt/mira/data/mira.db")
                _row = _db.execute("SELECT form_link FROM accounts WHERE act_id=?", (act_id,)).fetchone()
                _db.close()
                if _row and _row[0]:
                    _acc_form_link = _row[0]
            except Exception:
                pass

            if not page_id:
                raise Exception(
                    f"账户 {act_id} 未配置主页 ID。"
                    "请在账户管理中填写该账户的 Facebook 主页 ID，"
                    "或在系统设置中配置全局默认主页 ID（autopilot_fb_page_id）"
                )

            self._update_progress(campaign_id, "upload", "上传素材到 Facebook...")
            # 4. 上传素材到 FB（获取 image_hash 或 video_id）
            fb_asset_ref = self._upload_asset_to_fb(act_id, asset, token)

            # 5. 创建 Campaign
            self._update_progress(campaign_id, "campaign", "创建广告系列 (Campaign)...")
            # ── 规范化命名 ──────────────────────────────────────────────────────
            # 目标缩写映射
            _obj_abbr = {
                "OUTCOME_SALES":         "CONV",
                "OUTCOME_LEADS":         "LEAD",
                "OUTCOME_TRAFFIC":       "TRAF",
                "OUTCOME_AWARENESS":     "AWR",
                "OUTCOME_ENGAGEMENT":    "ENG",
                "OUTCOME_MESSAGES":      "MSG",
                "OUTCOME_APP_PROMOTION": "APP",
            }
            _obj_short = _obj_abbr.get(campaign["objective"], "ADS")
            # 国家代码（最多2个，用-连接）
            _ctry_list = target_countries or ["XX"]
            _ctry_str  = "-".join(_ctry_list[:2])
            # 素材代码（优先 asset_code，否则 AST-{id:04d}）
            _ast_code  = asset.get("asset_code") or f"AST-{asset['id']:04d}"
            # 日期（MMDD，CST 本地时间）
            from datetime import datetime as _dt
            try:
                import pytz as _pytz
                _cst = _pytz.timezone("Asia/Shanghai")
                _now_cst = _dt.now(_cst)
            except Exception:
                _now_cst = _dt.utcnow()
            _mmdd = _now_cst.strftime("%m%d")
            # 系列名：{前缀}-{目标}-{国家}-{素材代码}-{MMDD}
            # 前缀：M-=手动铺放，A-=智能自动铺放（global_dispatcher）
            _dispatch_src = campaign.get("dispatch_source", "manual") or "manual"
            _prefix = "A" if _dispatch_src == "global_dispatcher" else "M"
            _campaign_display_name = f"{_prefix}-{_obj_short}-{_ctry_str}-{_ast_code}-{_mmdd}"
            # ────────────────────────────────────────────────────────────────────
            fb_campaign_id = self._create_campaign(
                act_id, _campaign_display_name, campaign["objective"], token
            )
            self._update_campaign_field(campaign_id, "fb_campaign_id", fb_campaign_id)
            logger.info(f"[AutoPilot] ✅ Campaign 创建成功: {fb_campaign_id}")

            # 6. 生成受众矩阵（兴趣词分组 + 宽泛受众）
            audience_groups = self._build_audience_groups(
                interests, target_countries, max_adsets,
                age_min=age_min, age_max=age_max, gender=gender,
                token=token
            )

            # 7. 逐组创建 AdSet + Ad
            total_adsets = 0
            total_ads = 0

            for group_idx, audience in enumerate(audience_groups):
                try:
                    # 每组 AdSet 轮询获取新的操作号 Token（Round Robin）
                    rr_token = get_exec_token(act_id, ACTION_CREATE)
                    if not rr_token:
                        logger.warning(f"[AutoPilot] AdSet {group_idx+1}: 无可用操作号 Token，跳过")
                        continue
                    # 组名：{系列名}-{受众类型}-G{序号}
                    # 受众类型：BROAD=宽泛受众，INT=兴趣受众
                    _aud_name = audience.get("name", "")
                    _aud_type = "BROAD" if "宽泛" in _aud_name else f"INT{group_idx+1}"
                    adset_name = f"{_campaign_display_name}-{_aud_type}-G{group_idx+1}"
                    # v4.0: 设备端版位覆盖
                    effective_placements = placements.copy() if placements else {}
                    if device_platforms == "mobile":
                        # 尊重用户选择的 publisher_platforms，不强制覆盖
                        # 只添加 device_platforms 限制，不改变用户的平台/版位选择
                        effective_placements["device_platforms"] = ["mobile"]
                        # 如果用户没有选任何平台（空 placements），则不设置版位（使用 FB 自动版位）
                        # 如果用户选了版位，确保 instagram_positions 和 facebook_positions 正确分组
                        _user_pp = effective_placements.get("publisher_platforms", [])
                        _user_fp = effective_placements.get("facebook_positions", [])
                        _user_ip = effective_placements.get("instagram_positions", [])
                        # 清理 facebook_positions 中的 IG 版位（兼容旧数据）
                        _clean_fp = [p for p in _user_fp if p not in ("instagram_feed", "instagram_story", "instagram_reels")]
                        # 清理 instagram_positions 中的 FB 版位（兼容旧数据）
                        _clean_ip = [p for p in _user_ip if p not in ("feed", "story", "right_hand_column", "reels")]
                        if _clean_fp:
                            effective_placements["facebook_positions"] = _clean_fp
                        elif "facebook_positions" in effective_placements:
                            del effective_placements["facebook_positions"]
                        if _clean_ip:
                            effective_placements["instagram_positions"] = _clean_ip
                        elif "instagram_positions" in effective_placements:
                            del effective_placements["instagram_positions"]
                    elif device_platforms == "desktop":
                        effective_placements["publisher_platforms"] = ["facebook"]
                        effective_placements["facebook_positions"] = ["feed", "right_hand_column"]
                        effective_placements["device_platforms"] = ["desktop"]
                        # desktop 不支持 instagram，移除
                        effective_placements.pop("instagram_positions", None)
                    # all: 使用自动版位（不覆盖）
                    fb_adset_id = self._create_adset(
                        act_id, fb_campaign_id, adset_name,
                        audience, test_budget, campaign["target_cpa"],
                        campaign["objective"], pixel_id, rr_token,
                        bid_strategy=bid_strategy,
                        placements=effective_placements if effective_placements else None,
                        conversion_event=campaign.get("conversion_event") or "PURCHASE",
                        beneficiary=beneficiary,
                        payer=payer,
                        tw_verified_id=tw_verified_id,
                        page_id=page_id,
                        conversion_goal=campaign.get("conversion_goal") or ""
                    )
                    total_adsets += 1
                    self._update_progress(campaign_id, f"adset_{group_idx+1}", f"AdSet {group_idx+1}/{len(audience_groups)} 创建成功，正在创建广告...")
                    logger.info(f"[AutoPilot] ✅ AdSet {group_idx+1} 创建成功: {fb_adset_id}")

                    # 每个 AdSet 创建多条 Ad（文案 × 素材）
                    ad_count = min(len(headlines), len(bodies), 3)
                    for ad_idx in range(ad_count):
                        try:
                            headline = headlines[ad_idx]
                            body = bodies[ad_idx]
                            # v4.0: 广告命名嵌入 asset_code，便于 FB 后台数据归因
                            asset_code = asset.get("asset_code") or f"AST-{asset['id']:04d}"
                            # 广告名：{素材代码}-{受众类型}-C{文案序号}
                            # 例：AST-20260415-001-INT1-C1
                            ad_name = f"{_ast_code}-{_aud_type}-C{ad_idx+1}"

                            # 检查AdSet是否创建成功（检查局部变量，不是数据库记录）
                            if not fb_adset_id:
                                logger.warning(f"[AutoPilot] AdSet未创建成功（fb_adset_id为空），跳过Ad {ad_idx+1} 创建")
                                self._insert_campaign_ad(
                                    campaign_id, act_id, campaign["asset_id"],
                                    headline, body,
                                    json.dumps(audience, ensure_ascii=False),
                                    None, None,
                                    status="skipped", error_msg="AdSet未创建，跳过Ad"
                                )
                                continue
                            fb_ad_id = self._create_ad(
                                act_id, fb_adset_id, ad_name,
                                headline, body, page_id,
                                fb_asset_ref, asset["file_type"], rr_token,
                                landing_url=landing_url,
                                conversion_goal=campaign.get("conversion_goal") or "",
                                message_template=campaign.get("message_template") or "",
                                lead_form_id=campaign.get("lead_form_id") or "",
                                asset_info=asset,
                                cta_type=campaign.get("cta_type") or "",
                                pixel_id=pixel_id or ""
                            )
                            total_ads += 1

                            # 写入 auto_campaign_ads 明细
                            self._insert_campaign_ad(
                                campaign_id, act_id, campaign["asset_id"],
                                headline, body,
                                json.dumps(audience, ensure_ascii=False),
                                fb_adset_id, fb_ad_id
                            )
                            logger.info(f"[AutoPilot] ✅ Ad {ad_idx+1} 创建成功: {fb_ad_id}")
                            time.sleep(0.3)  # 避免 API 频率限制

                        except Exception as ad_err:
                            logger.error(f"[AutoPilot] Ad {ad_idx+1} 创建失败: {ad_err}")
                            self._insert_campaign_ad(
                                campaign_id, act_id, campaign["asset_id"],
                                headline, body,
                                json.dumps(audience, ensure_ascii=False),
                                fb_adset_id, None,
                                status="error", error_msg=str(ad_err)
                            )

                    time.sleep(0.5)

                except Exception as adset_err:
                    # 如果有兴趣词定向，尝试降级为宽泛受众重试
                    audience_targeting = audience.get("targeting", {})
                    # 兴趣词现在存储在 flexible_spec 中（FB API要求格式）
                    has_interests = bool(
                        audience_targeting.get("flexible_spec") or
                        audience_targeting.get("interests")
                    )
                    if has_interests:
                        logger.warning(f"[AutoPilot] AdSet {group_idx+1} 创建失败（可能兴趣词无效），尝试降级为宽泛受众重试: {adset_err}")
                        try:
                            fallback_audience = {
                                "name": audience.get("name", f"宽泛受众-{group_idx+1}") + "（降级）",
                                "targeting": {k: v for k, v in audience_targeting.items() if k not in ("interests", "flexible_spec")}
                            }
                            fb_adset_id = self._create_adset(
                                act_id, fb_campaign_id, adset_name + "-FB",
                                fallback_audience, test_budget, campaign["target_cpa"],
                                campaign["objective"], pixel_id, token,
                                bid_strategy=bid_strategy,
                                placements=effective_placements if effective_placements else None,
                                conversion_event=campaign.get("conversion_event") or "PURCHASE",
                                beneficiary=beneficiary,
                                payer=payer,
                                tw_verified_id=tw_verified_id,
                                page_id=page_id,
                                conversion_goal=campaign.get("conversion_goal") or ""
                            )
                            total_adsets += 1
                            audience = fallback_audience  # 用降级受众继续创建 Ad
                            self._update_progress(campaign_id, f"adset_{group_idx+1}", f"AdSet {group_idx+1} 降级宽泛受众创建成功，正在创建广告...")
                            logger.info(f"[AutoPilot] ✅ AdSet {group_idx+1} 降级宽泛受众创建成功: {fb_adset_id}")
                            # 降级成功后继续创建 Ad
                            ad_count = min(len(headlines), len(bodies), 3)
                            for ad_idx in range(ad_count):
                                try:
                                    headline = headlines[ad_idx]
                                    body = bodies[ad_idx]
                                    asset_code = asset.get("asset_code") or f"AST-{asset['id']:04d}"
                                    # 降级广告名：{素材代码}-BROAD-C{文案序号}-FB
                                    ad_name = f"{_ast_code}-BROAD-C{ad_idx+1}-FB"
                                    fb_ad_id = self._create_ad(
                                        act_id, fb_adset_id, ad_name,
                                        headline, body, page_id,
                                        fb_asset_ref, asset["file_type"], token,
                                        landing_url=landing_url,
                                        conversion_goal=campaign.get("conversion_goal") or "",
                                        message_template=campaign.get("message_template") or "",
                                        lead_form_id=campaign.get("lead_form_id") or "",
                                        asset_info=asset,
                                        cta_type=campaign.get("cta_type") or "",
                                        pixel_id=pixel_id or ""
                                    )
                                    total_ads += 1
                                    self._insert_campaign_ad(
                                        campaign_id, act_id, campaign["asset_id"],
                                        headline, body,
                                        json.dumps(fallback_audience, ensure_ascii=False),
                                        fb_adset_id, fb_ad_id
                                    )
                                    logger.info(f"[AutoPilot] ✅ Ad {ad_idx+1}（降级）创建成功: {fb_ad_id}")
                                    time.sleep(0.3)
                                except Exception as ad_err2:
                                    logger.error(f"[AutoPilot] Ad {ad_idx+1}（降级）创建失败: {ad_err2}")
                                    self._insert_campaign_ad(
                                        campaign_id, act_id, campaign["asset_id"],
                                        headlines[ad_idx] if ad_idx < len(headlines) else "",
                                        bodies[ad_idx] if ad_idx < len(bodies) else "",
                                        json.dumps(fallback_audience, ensure_ascii=False),
                                        fb_adset_id, None,
                                        status="error", error_msg=str(ad_err2)
                                    )
                        except Exception as fallback_err:
                            logger.error(f"[AutoPilot] AdSet {group_idx+1} 降级宽泛受众也失败: {fallback_err}")
                            self._insert_campaign_ad(
                                campaign_id, act_id, campaign["asset_id"],
                                None, None,
                                json.dumps(audience, ensure_ascii=False),
                                None, None,
                                status="error", error_msg=f"原始错误: {adset_err}; 降级错误: {fallback_err}"
                            )
                    else:
                        logger.error(f"[AutoPilot] AdSet {group_idx+1} 创建失败: {adset_err}")
                        self._insert_campaign_ad(
                            campaign_id, act_id, campaign["asset_id"],
                            None, None,
                            json.dumps(audience, ensure_ascii=False),
                            None, None,
                            status="error", error_msg=str(adset_err)
                        )

             # 8. 更新任务完成状态
            conn = get_conn()
            conn.execute(
                """UPDATE auto_campaigns SET status='done', total_adsets=?, total_ads=?,
                   updated_at=? WHERE id=?""",
                (total_adsets, total_ads,
                 datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
            )
            conn.commit()
            conn.close()
            # 如果 0 个 AdSet 成功，附带错误原因
            # ── 修复: 收集失败原因，无论有无成功都展示──
            try:
                _ec = get_conn()
                _err_rows = _ec.execute(
                    """SELECT error_msg, COUNT(*) as cnt
                       FROM auto_campaign_ads
                       WHERE campaign_id=? AND status='error' AND error_msg IS NOT NULL
                       GROUP BY error_msg ORDER BY cnt DESC LIMIT 3""",
                    (campaign_id,)
                ).fetchall()
                _ec.close()
                _err_parts = []
                for _row in _err_rows:
                    _msg = _row["error_msg"][:100] + "..." if len(_row["error_msg"]) > 100 else _row["error_msg"]
                    _err_parts.append(f"×{_row['cnt']} {_msg}")
                _err_summary = "\n".join(_err_parts) if _err_parts else ""
            except Exception:
                _err_summary = ""

            if total_adsets == 0:
                _done_msg = f"全部失败！共创建 0 个 AdSet，0 条广告"
                if _err_summary:
                    _done_msg += f"\n⚠️ 失败原因：\n{_err_summary}"
                else:
                    _done_msg += "（请查看日志了解详情）"
                # 全部失败时改为 error 状态
                self._update_progress(campaign_id, "error", _done_msg)
            elif total_ads == 0:
                _done_msg = f"AdSet 已创建但广告全部失败！共 {total_adsets} 个 AdSet，0 条广告"
                if _err_summary:
                    _done_msg += f"\n⚠️ 失败原因：\n{_err_summary}"
                # 广告全部失败时也改为 error
                self._update_progress(campaign_id, "error", _done_msg)
            else:
                _done_msg = f"完成！共创建 {total_adsets} 个 AdSet，{total_ads} 条广告"
                if _err_summary:
                    _done_msg += f"\n⚠️ 部分失败：\n{_err_summary}"
                self._update_progress(campaign_id, "done", _done_msg)
            logger.info(
                f"[AutoPilot] ✅ 任务完成 campaign_id={campaign_id}，"
                f"共创建 {total_adsets} 个 AdSet，{total_ads} 条 Ad"
            )

        except Exception as e:
            logger.error(f"[AutoPilot] ❌ 任务失败 campaign_id={campaign_id}: {e}")
            self._update_campaign_status(campaign_id, "error", str(e))


            # Write to action_logs
            try:
                from core.database import get_conn as _gc2
                _lc = _gc2()
                _lc.execute(
                    """INSERT INTO action_logs
                       (act_id, level, target_id, target_name, action_type,
                        trigger_type, trigger_detail, status, error_msg, operator)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (campaign.get("act_id","") if campaign else "", "campaign",
                     str(campaign_id), (campaign.get("name","") if campaign else "")[:50],
                     "autopilot_launch", "manual", "launch_campaign", "failed", str(e)[:200], "system")
                )
                _lc.commit()
                _lc.close()
            except Exception as _le:
                logger.warning(f"[AutoPilot] log write failed: {_le}")    # ── 素材上传 ──────────────────────────────────────────────────────────────

    def _upload_asset_to_fb(self, act_id: str, asset: dict, token: str) -> dict:
        """
        将本地素材上传到 FB 广告账户。
        图片：返回 {"type": "image", "image_hash": "xxx"}
        视频：返回 {"type": "video", "video_id": "xxx"}
        按 act_id 分别缓存，不同账户重新上传（避免跨账户 image_hash 无效）。
        """
        # 按账户查询缓存（asset_fb_refs 表）
        try:
            _conn = get_conn()
            _row = _conn.execute(
                "SELECT fb_asset_id, fb_type FROM asset_fb_refs WHERE asset_id=? AND act_id=?",
                (asset["id"], act_id)
            ).fetchone()
            _conn.close()
            if _row:
                fb_id, fb_type = _row["fb_asset_id"], _row["fb_type"]
                logger.info(f"[AutoPilot] 素材 {asset['id']} 在账户 {act_id} 已有缓存: {fb_type}={fb_id}")
                if fb_type == "image":
                    return {"type": "image", "image_hash": fb_id}
                else:
                    # 视频缓存中没有缩略图，实时获取
                    _thumb = self._get_video_thumbnail(fb_id, token)
                    return {"type": "video", "video_id": fb_id, "thumbnail_url": _thumb}
        except Exception as _e:
            logger.warning(f"[AutoPilot] 查询 asset_fb_refs 失败（将重新上传）: {_e}")

        file_path = asset["file_path"]
        act_id_num = act_id.replace("act_", "")

        if asset["file_type"] == "image":
            with open(file_path, "rb") as f:
                resp = requests.post(
                    f"{FB_API_BASE}/act_{act_id_num}/adimages",
                    data={"access_token": token},
                    files={"filename": f},
                    timeout=60
                )
            data = resp.json()
            if "error" in data:
                raise Exception(f"图片上传失败: {data['error'].get('message', str(data))}")
            images = data.get("images", {})
            if not images:
                raise Exception(f"图片上传响应异常: {data}")
            # 取第一个文件的 hash
            image_hash = list(images.values())[0]["hash"]
            # 写回数据库
            self._update_asset_fb_ref(asset["id"], image_hash, "image", act_id)
            return {"type": "image", "image_hash": image_hash}

        else:  # video
            with open(file_path, "rb") as f:
                resp = requests.post(
                    f"https://graph-video.facebook.com/{FB_API_VERSION}/act_{act_id_num}/advideos",
                    data={"access_token": token, "title": asset["file_name"]},
                    files={"source": f},
                    timeout=300
                )
            data = resp.json()
            if "error" in data:
                raise Exception(f"视频上传失败: {data['error'].get('message', str(data))}")
            video_id = data.get("id")
            if not video_id:
                raise Exception(f"视频上传响应异常: {data}")
            # 获取视频缩略图（FB API 要求 video_data 必须包含 image_hash 或 image_url）
            video_thumbnail_url = self._get_video_thumbnail(video_id, token)
            self._update_asset_fb_ref(asset["id"], video_id, "video", act_id)
            return {"type": "video", "video_id": video_id, "thumbnail_url": video_thumbnail_url}

    def _get_video_thumbnail(self, video_id: str, token: str) -> str:
        """
        获取已上传视频的缩略图 URL。
        FB API: GET /{video_id}?fields=picture&access_token=...
        视频刚上传时可能还在处理中，最多等待 30 秒重试。
        """
        for attempt in range(6):  # 最多重试 6 次，每次等 5 秒
            try:
                resp = requests.get(
                    f"{FB_API_BASE}/{video_id}",
                    params={"fields": "picture,thumbnails", "access_token": token},
                    timeout=15
                )
                data = resp.json()
                if "error" in data:
                    logger.warning(f"[AutoPilot] 获取视频缩略图失败: {data['error'].get('message')}")
                    break
                # 优先取 picture 字段（视频封面图 URL）
                picture = data.get("picture")
                if picture:
                    logger.info(f"[AutoPilot] 视频 {video_id} 缩略图: {picture[:60]}...")
                    return picture
                # 尝试 thumbnails 字段
                thumbs = data.get("thumbnails", {}).get("data", [])
                if thumbs:
                    return thumbs[0].get("uri", "")
                # 视频还在处理中，等待后重试
                if attempt < 5:
                    logger.info(f"[AutoPilot] 视频 {video_id} 缩略图暂未就绪，等待 5 秒后重试 ({attempt+1}/6)...")
                    time.sleep(5)
            except Exception as _e:
                logger.warning(f"[AutoPilot] 获取视频缩略图异常: {_e}")
                break
        logger.warning(f"[AutoPilot] 视频 {video_id} 无法获取缩略图，将使用空字符串")
        return ""

    def _update_asset_fb_ref(self, asset_id: int, fb_id: str, fb_type: str, act_id: str = ""):
        conn = get_conn()
        # 写入 asset_fb_refs 表（按账户缓存，避免跨账户复用）
        if act_id:
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO asset_fb_refs (asset_id, act_id, fb_asset_id, fb_type, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (asset_id, act_id, fb_id, fb_type, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
                )
            except Exception as _e:
                logger.warning(f"[AutoPilot] 写入 asset_fb_refs 失败: {_e}")
        # 同时更新 ad_assets 表（兼容旧逻辑）
        conn.execute(
            "UPDATE ad_assets SET fb_asset_id=?, fb_asset_type=?, updated_at=? WHERE id=?",
            (fb_id, fb_type, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), asset_id)
        )
        conn.commit()
        conn.close()

    # ── Campaign 创建 ─────────────────────────────────────────────────────────

    def _create_campaign(self, act_id: str, name: str, objective: str, token: str) -> str:
        """创建 Facebook Campaign"""
        act_id_num = act_id.replace("act_", "")
        # 将内部 objective 值映射为 FB API 接受的标准值
        _OBJECTIVE_MAP = {
            "OUTCOME_VIDEO_VIEWS": "VIDEO_VIEWS",
            "OUTCOME_MESSAGING":   "OUTCOME_ENGAGEMENT",  # 旧版兼容
            "MESSAGES":            "OUTCOME_ENGAGEMENT",  # 前端传MESSAGES→映射为OUTCOME_ENGAGEMENT（FB API v25+不支持MESSAGES）
        }
        fb_objective = _OBJECTIVE_MAP.get(objective, objective)
        payload = {
            "name": name,
            "objective": fb_objective,
            "status": "ACTIVE",
            "special_ad_categories": [],
        }
        # 所有 OUTCOME_* 目标都使用 AUCTION 竞价
        payload["buying_type"] = "AUCTION"
        payload["is_adset_budget_sharing_enabled"] = False

        data = self._fb_post(f"act_{act_id_num}/campaigns", token, payload)
        return data["id"]

    # ── AdSet 创建 ────────────────────────────────────────────────────────────

    def _create_adset(
        self, act_id: str, campaign_id: str, name: str,
        audience: dict, daily_budget: float,
        target_cpa: Optional[float], objective: str,
        pixel_id: str, token: str,
        bid_strategy: str = "LOWEST_COST_WITHOUT_CAP",
        placements: Optional[dict] = None,
        conversion_event: str = "PURCHASE",
        beneficiary: str = "",
        payer: str = "",
        tw_verified_id: str = "",
        page_id: str = "",
        conversion_goal: str = ""
    ) -> str:
        """创建 Facebook AdSet"""
        act_id_num = act_id.replace("act_", "")
        # FB API 预算单位：大多数货币为"分"（×100），无小数位货币（JPY/KRW等）直接传整数
        # 注意：此处 daily_budget 已经是账户货币金额（由 run_campaign 换算完毕）
        _NO_DECIMAL_CURRENCIES = {"JPY", "KRW", "IDR", "VND", "CLP", "COP", "HUF", "PYG", "UGX", "TZS"}
        # 从账户查询货币类型（通过 act_id 推断）
        _budget_currency = "USD"  # 默认，实际由调用方保证已换算
        budget_cents = int(daily_budget * 100)  # FB 预算单位为分（标准货币）
        # 规范化 objective：将旧版/前端传来的 MESSAGES 映射为 OUTCOME_ENGAGEMENT
        _OBJ_NORMALIZE = {
            "MESSAGES":         "OUTCOME_ENGAGEMENT",
            "OUTCOME_MESSAGING": "OUTCOME_ENGAGEMENT",
        }
        objective = _OBJ_NORMALIZE.get(objective, objective)

        targeting = dict(audience["targeting"])
        # 如果指定了手动版位，将其合并到 targeting（v4.0: 支持 device_platforms）
        if placements and placements.get("publisher_platforms"):
            targeting["publisher_platforms"] = placements["publisher_platforms"]
            if placements.get("facebook_positions"):
                # ── 修复: FB API 不接受 facebook_positions 中的 "reels"
                # "reels" 是 Instagram Reels，必须放入 instagram_positions
                _raw_fp = placements["facebook_positions"]
                _fb_only = [p for p in _raw_fp if p != "reels"]
                _reels_from_fp = ["reels"] if "reels" in _raw_fp else []
                if _fb_only:
                    targeting["facebook_positions"] = _fb_only
                # 将 reels 合并到 instagram_positions
                if _reels_from_fp:
                    _existing_ip = list(placements.get("instagram_positions") or [])
                    if "reels" not in _existing_ip:
                        _existing_ip.append("reels")
                    placements = dict(placements)  # 避免修改原始对象
                    placements["instagram_positions"] = _existing_ip
            if placements.get("instagram_positions"):
                targeting["instagram_positions"] = placements["instagram_positions"]
            if placements.get("audience_network_positions"):
                targeting["audience_network_positions"] = placements["audience_network_positions"]
            if placements.get("messenger_positions"):
                targeting["messenger_positions"] = placements["messenger_positions"]
        # v4.0: device_platforms 独立处理（不依赖 publisher_platforms，自动版位时也生效）
        if placements and placements.get("device_platforms"):
            targeting["device_platforms"] = placements["device_platforms"]

        # 根据 conversion_goal 决定 optimization_goal
        opt_goal = self._get_optimization_goal(objective, conversion_goal)

        payload = {
            "name": name,
            "campaign_id": campaign_id,
            "daily_budget": budget_cents,
            "billing_event": "IMPRESSIONS",
            "optimization_goal": opt_goal,
            "targeting": targeting,
            "status": "ACTIVE",
            # 不传 start_time → FB 立即生效（避免时区错误导致排期）
        }

        # 根据目标类型 + 转化目的设置 promoted_object
        # 测试验证结论（2026-04）：
        # OUTCOME_ENGAGEMENT: 不需要 promoted_object，goal 用 REACH/LINK_CLICKS/IMPRESSIONS/CONVERSATIONS/LANDING_PAGE_VIEWS
        # OUTCOME_TRAFFIC: 不需要 promoted_object（传 pixel 反而报错），goal 用 LINK_CLICKS/LANDING_PAGE_VIEWS/REACH/IMPRESSIONS/CONVERSATIONS
        # OUTCOME_AWARENESS: 不需要 promoted_object，goal 用 REACH/IMPRESSIONS
        # OUTCOME_LEADS: LEAD_GENERATION goal 需要 promoted_object:{page_id}，其他不需要
        # OUTCOME_SALES/LEADS + OFFSITE_CONVERSIONS: 需要 promoted_object:{pixel_id, custom_event_type}
        # MESSAGES（消息互动）: 使用 CONVERSATIONS optimization_goal
        # 组件完整性预检查：提前抛出清晰的中文错误，而不是让 FB API 报英文错误
        if opt_goal in ("OFFSITE_CONVERSIONS", "VALUE"):
            if not pixel_id:
                raise ValueError(
                    f"选择「网站转化」或「转化价値」转化目的时，Pixel 像素为必填。"
                    f"请在账户详情「🏠 主页/像素」中配置 Pixel ID，或在铺广告高级设置中选择像素。"
                    f"当前转化目的：{conversion_goal}"
                )
            # 网站转化：需要真实 Pixel ID + 转化事件类型
            po = {"pixel_id": pixel_id}
            if conversion_event:
                po["custom_event_type"] = conversion_event
            payload["promoted_object"] = po
        elif opt_goal in ("MESSAGING_PURCHASE_CONVERSION", "MESSAGING_APPOINTMENT_CONVERSION"):
            if not page_id:
                raise ValueError(
                    f"选择消息类转化目的（{opt_goal}）时，主页 ID 为必填。"
                    f"请在账户详情中配置主页，并确保主页已接入 Messenger 或 WhatsApp Business。"
                )
            # 消息购买/预约：需要 page_id
            payload["promoted_object"] = {"page_id": page_id}
        elif objective == "OUTCOME_LEADS" and opt_goal == "LEAD_GENERATION":
            if not page_id:
                raise ValueError(
                    f"「即时表单」线索目标需要主页 ID。"
                    f"请在账户详情中配置主页，或在铺广告高级设置中选择主页。"
                )
            payload["promoted_object"] = {"page_id": page_id}
        elif opt_goal == "PAGE_LIKES":
            # 主页赞：需要 page_id + destination_type=ON_PAGE
            # FB API 官方文档：PAGE_LIKES → OUTCOME_ENGAGEMENT + destination_type: ON_PAGE
            if not page_id:
                raise ValueError(
                    f"【主页赞】目标需要配置主页 ID。"
                    f"请在账户详情中配置主页，或在铺广告高级设置中选择主页。"
                )
            payload["promoted_object"] = {"page_id": page_id}
            payload["destination_type"] = "ON_PAGE"
        elif opt_goal == "POST_ENGAGEMENT" and page_id:
            # 帖子互动（非主页赞）：有 page_id 时设置 promoted_object
            payload["promoted_object"] = {"page_id": page_id}
        elif objective == "OUTCOME_APP_PROMOTION":
            # 应用推广需要 application_id（暂用 pixel 底底）
            if pixel_id:
                payload["promoted_object"] = {"pixel_id": pixel_id}

        # 消息类目标：设置 destination_type=MESSENGER，确保 Conversion Location 正确
        # 适用于：CONVERSATIONS（聊天对话）optimization_goal
        if opt_goal == "CONVERSATIONS":
            payload["destination_type"] = "MESSENGER"
            # 消息类需要 page_id 作为 promoted_object
            if page_id:
                payload["promoted_object"] = {"page_id": page_id}
            # FB API要求：使用 destination_type=MESSENGER 时，publisher_platforms 必须包含 messenger
            # 否则报 Invalid parameter。自动补充 messenger 版位。
            _pp = targeting.get("publisher_platforms", [])
            if _pp and "messenger" not in _pp:
                targeting["publisher_platforms"] = list(_pp) + ["messenger"]
                if "messenger_positions" not in targeting:
                    targeting["messenger_positions"] = ["messenger_home"]
            elif not _pp:
                # 自动版位时不强制设置，FB会自动处理
                pass
            payload["targeting"] = targeting
        # 购物广告：OUTCOME_SALES 强制设置 destination_type=WEBSITE（单网站转化，避免 FB 默认包含 APP 渠道）
        if objective == "OUTCOME_SALES" and opt_goal not in ("CONVERSATIONS", "MESSAGING_PURCHASE_CONVERSION"):
            payload["destination_type"] = "WEBSITE"
            logger.info(f"[AutoPilot] OUTCOME_SALES 设置 destination_type=WEBSITE")
        # 出价策略：如果设了 target_cpa 且策略为 COST_CAP/BID_CAP，优先使用 CPA
        if target_cpa and target_cpa > 0 and bid_strategy in ("COST_CAP", "BID_CAP"):
            payload["bid_strategy"] = bid_strategy
            payload["bid_amount"] = int(target_cpa * 100)
        elif target_cpa and target_cpa > 0:
            # 有 CPA 但策略为自动，使用 COST_CAP
            payload["bid_strategy"] = "COST_CAP"
            payload["bid_amount"] = int(target_cpa * 100)
        else:
            payload["bid_strategy"] = bid_strategy

        # 台湾广告合规：如果目标地区包含TW，需要添加 TAIWAN_UNIVERSAL 声明
        countries = targeting.get("geo_locations", {}).get("countries", [])
        is_taiwan = "TW" in countries
        if is_taiwan:
            payload["regional_regulated_categories"] = ["TAIWAN_UNIVERSAL"]
            # 优先使用 verified_identity_id（regional_regulation_identities 方式，FB 官方推荐）
            if tw_verified_id:
                payload["regional_regulation_identities"] = {
                    "taiwan_universal_beneficiary": tw_verified_id,
                    "taiwan_universal_payer": tw_verified_id
                }
            elif beneficiary:
                # 兼容旧版字符串方式（仅在无 verified_id 时使用）
                payload["beneficiary"] = beneficiary
                if payer:
                    payload["payer"] = payer
        # FB API 要求：必须明确设置 Advantage Audience 标志（0=关闭手动定向，1=开启AI扩量）
        # error_subcode 1870227: To create your ad set, you need to enable or disable the Advantage audience feature
        targeting["targeting_automation"] = {"advantage_audience": 0}
        payload["targeting"] = targeting
        try:
            data = self._fb_post(f"act_{act_id_num}/adsets", token, payload)
        except Exception as e:
            err_msg = str(e)
            # ── 修复: 受众太窄（2446395）自动降级为宽泛受众重试 ──
            if "2446395" in err_msg:
                _has_interests = bool(
                    targeting.get("flexible_spec") or targeting.get("interests")
                )
                if _has_interests:
                    logger.warning(f"[AutoPilot] 受众太窄（2446395），自动降级为宽泛受众重试: {name}")
                    _broad_targeting = {k: v for k, v in targeting.items()
                                        if k not in ("flexible_spec", "interests")}
                    payload["targeting"] = _broad_targeting
                    payload["name"] = name + "-BROAD"
                    try:
                        data = self._fb_post(f"act_{act_id_num}/adsets", token, payload)
                        logger.info(f"[AutoPilot] ✅ 宽泛受众降级成功: {data.get('id')}")
                        return data["id"]
                    except Exception as broad_err:
                        raise Exception(f"受众太窄且宽泛降级也失败: {broad_err}") from broad_err
            if is_taiwan and "2490408" in err_msg and "optimization_goal" in err_msg:
                # 台湾认证广告（TAIWAN_UNIVERSAL）对 optimization_goal 有严格限制
                # 实测支持：OFFSITE_CONVERSIONS、LINK_CLICKS、LANDING_PAGE_VIEWS、REACH、IMPRESSIONS、CONVERSATIONS
                # 不支持：PAGE_LIKES、POST_ENGAGEMENT、VIDEO_VIEWS、THRUPLAY、LEAD_GENERATION 等
                _tw_supported = "OFFSITE_CONVERSIONS（像素购买）、LINK_CLICKS（流量点击）、LANDING_PAGE_VIEWS（落地页浏览）、REACH（覆盖人数）、CONVERSATIONS（私信对话）"
                raise Exception(
                    f"台湾认证广告不支持当前投放目标（{opt_goal}）。"
                    f"台湾广告（TAIWAN_UNIVERSAL）仅支持以下投放目标：{_tw_supported}。"
                    f"请在铺广告时选择「像素购买」或「流量」目标。"
                )
            if is_taiwan and ("3858495" in err_msg or "3858498" in err_msg or "advertiser" in err_msg.lower() or "taiwan" in err_msg.lower()):
                raise Exception(
                    f"台湾广告合规错误：该广告账户尚未在 Facebook 广告管理后台完成台湾广告声明（TAIWAN_UNIVERSAL）。"
                    f"请前往 https://www.facebook.com/business/help/983527276402621 完成账户级别的广告主身份验证后重试。"
                    f"原始错误：{err_msg}"
                )
            raise
        return data["id"]

    def _get_optimization_goal(self, objective: str, conversion_goal: str = "") -> str:
        """
        Facebook Outcome-Based Objectives + conversion_goal → AdSet optimization_goal 映射
        根据 2026-04 实测验证结果，列出每个 Objective 下实际可用的 goal
        """
        # 每个 Objective 下经过实测验证的可用 optimization_goal
        VALID_GOALS = {
            # 互动：REACH/LINK_CLICKS/IMPRESSIONS/CONVERSATIONS/LANDING_PAGE_VIEWS 完整测试通过
            "OUTCOME_ENGAGEMENT": {
                "reach":              "REACH",
                "link_clicks":        "LINK_CLICKS",
                "impressions":        "IMPRESSIONS",
                "conversations":      "CONVERSATIONS",
                "landing_page_views": "LANDING_PAGE_VIEWS",
                # 视频类目标（需要视频素材）
                "video_views":        "VIDEO_VIEWS",
                "thruplay":           "THRUPLAY",
                # 主页赞：optimization_goal 必须为 PAGE_LIKES，配合 destination_type=ON_PAGE
                "page_likes":         "PAGE_LIKES",
                # 消息类（MESSAGES objective 映射到 OUTCOME_ENGAGEMENT 后使用这些 goal）
                "messaging_purchase_conversion":  "MESSAGING_PURCHASE_CONVERSION",
                "messaging_appointment_conversion": "MESSAGING_APPOINTMENT_CONVERSION",
                # 默认
                "": "REACH",
            },
            # 流量：LINK_CLICKS/LANDING_PAGE_VIEWS/REACH/IMPRESSIONS/CONVERSATIONS 完整测试通过
            "OUTCOME_TRAFFIC": {
                "link_clicks":        "LINK_CLICKS",
                "landing_page_views": "LANDING_PAGE_VIEWS",
                "reach":              "REACH",
                "impressions":        "IMPRESSIONS",
                "conversations":      "CONVERSATIONS",
                "": "LINK_CLICKS",
            },
            # 品牌认知：REACH/IMPRESSIONS 完整测试通过
            "OUTCOME_AWARENESS": {
                "reach":      "REACH",
                "impressions": "IMPRESSIONS",
                "": "REACH",
            },
            # 线索：LEAD_GENERATION(需 page_id)/LINK_CLICKS 完整测试通过
            "OUTCOME_LEADS": {
                "lead_generation":               "LEAD_GENERATION",
                "link_clicks":                   "LINK_CLICKS",
                "landing_page_views":            "LANDING_PAGE_VIEWS",
                "impressions":                   "IMPRESSIONS",
                # 网站转化线索（需要 Pixel）
                "offsite_conversions":           "OFFSITE_CONVERSIONS",
                # 消息线索（Messenger/WhatsApp）
                "messaging_purchase_conversion": "MESSAGING_PURCHASE_CONVERSION",
                "conversations":                 "CONVERSATIONS",
                "": "LEAD_GENERATION",
            },
            # 销售：LINK_CLICKS/REACH 完整测试通过（OFFSITE_CONVERSIONS 需要真实 pixel）
            "OUTCOME_SALES": {
                "link_clicks":                    "LINK_CLICKS",
                "reach":                          "REACH",
                "impressions":                    "IMPRESSIONS",
                "landing_page_views":             "LANDING_PAGE_VIEWS",
                "conversations":                  "CONVERSATIONS",
                # 网站转化（需要真实 Pixel）
                "offsite_conversions":            "OFFSITE_CONVERSIONS",
                "value":                          "VALUE",
                # 消息购买（Messenger/WhatsApp）
                "messaging_purchase_conversion":  "MESSAGING_PURCHASE_CONVERSION",
                # 目录销售（需要商品目录）
                "product_catalog_sales":          "OFFSITE_CONVERSIONS",
                "": "LINK_CLICKS",
            },
            # 应用推广
            "OUTCOME_APP_PROMOTION": {
                "": "APP_INSTALLS",
            },
            # 视频观看（支持 VIDEO_VIEWS 和旧版 OUTCOME_VIDEO_VIEWS）
            "OUTCOME_VIDEO_VIEWS": {
                "video_views": "VIDEO_VIEWS",
                "thruplay":    "THRUPLAY",
                "": "VIDEO_VIEWS",
            },
            "VIDEO_VIEWS": {
                "video_views": "VIDEO_VIEWS",
                "thruplay":    "THRUPLAY",
                "": "VIDEO_VIEWS",
            },
            # 消息互动（FB API 标准值 MESSAGES）
            "MESSAGES": {
                "conversations": "CONVERSATIONS",
                "": "CONVERSATIONS",
            },
            # 门店访问
            "OUTCOME_STORE_VISITS": {
                "": "STORE_VISITS",
            },
        }
        goal_key = (conversion_goal or "").lower().strip()
        obj_map = VALID_GOALS.get(objective, {})
        if goal_key in obj_map:
            return obj_map[goal_key]
        # 如果传入的 conversion_goal 不在映射表中，返回默认值
        return obj_map.get("", "LINK_CLICKS")

    # ── AI 自动生成消息/表单内容 ──────────────────────────────────────────────
    def _call_text_ai(self, prompt: str) -> str:
        """调用文本AI（DeepSeek）生成内容"""
        try:
            from openai import OpenAI
            api_key = self._get_setting("ai_api_key", "")
            api_base = self._get_setting("ai_api_base", "https://api.deepseek.com/v1")
            model = self._get_setting("ai_model", "deepseek-chat")
            if not api_key:
                return ""
            client = OpenAI(api_key=api_key, base_url=api_base)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1024,
                temperature=0.7
            )
            return resp.choices[0].message.content.strip() if resp.choices else ""
        except Exception as e:
            logger.warning(f"[AutoPilot] AI文本生成失败: {e}")
            return ""

    def _ai_gen_msg_template(self, body: str, headline: str, asset_info: dict = None) -> dict:
        """根据素材AI分析结果，调用DeepSeek生成Messenger欢迎消息模板（与广告内容强相关）"""
        import json as _json
        ai_analysis = ""
        ai_purpose = ""
        if asset_info:
            ai_analysis = asset_info.get("ai_analysis") or ""
            ai_purpose = asset_info.get("ai_purpose") or "general"

        # 检测广告文案语言（简单判断是否含中文）
        has_chinese = any('\u4e00' <= c <= '\u9fff' for c in (body + headline))
        lang_hint = "中文" if has_chinese else "英文（与广告文案语言保持一致）"

        prompt = (
            "你是一位Facebook广告专家，请根据以下广告内容，生成一个高度相关的Messenger欢迎消息模板。\n\n"
            f"广告标题：{headline}\n"
            f"广告文案：{body[:300]}\n"
            f"素材分析：{ai_analysis[:200]}\n"
            f"投放目的：{ai_purpose}\n\n"
            "【核心要求】\n"
            "1. 欢迎语（welcome_text）：\n"
            "   - 必须直接承接广告内容，让用户感觉是广告的自然延续\n"
            "   - 提及广告中的具体产品、机会或卖点，不能泛泛而谈\n"
            f"   - 使用{lang_hint}，50-150字符\n"
            "2. 快捷问题（ice_breakers）：\n"
            "   - 必须是用户针对该广告的具体产品/服务会真实提问的问题\n"
            "   - 禁止使用通用问题，例如：\n"
            "     ✗ 'What services do you offer?'\n"
            "     ✗ 'How much does it cost?'\n"
            "     ✗ 'Can I book an appointment?'\n"
            "   - 应该是针对广告内容的具体问题，例如（股票广告）：\n"
            "     ✓ 'What is the $0.38 stock you mentioned?'\n"
            "     ✓ 'How do I join your investor community?'\n"
            "     ✓ 'What returns have you seen so far?'\n"
            "   - 每个问题要有对应的自动回复（response），回复要简洁有力，引导用户进一步互动\n"
            f"   - 使用{lang_hint}，问题标题不超过80字符，回复不超过300字符\n\n"
            "请用JSON格式返回（3个ice_breaker）：\n"
            '{"welcome_text": "欢迎语", "ice_breakers": [{"title": "具体问题1", "response": "针对性回复1"}, {"title": "具体问题2", "response": "针对性回复2"}, {"title": "具体问题3", "response": "针对性回复3"}]}\n'
            "只返回JSON，不要其他内容。"
        )
        raw = self._call_text_ai(prompt)
        ai_result = {}
        if raw:
            try:
                if "```" in raw:
                    parts = raw.split("```")
                    for part in parts[1::2]:
                        part = part.lstrip("json").strip()
                        if part.startswith("{"):
                            raw = part
                            break
                ai_result = _json.loads(raw)
            except Exception:
                pass

        welcome_text = ai_result.get("welcome_text") or f"{headline} - {body[:100]}"
        # 支持新格式（ice_breakers 对象数组）和旧格式（buttons 字符串数组）
        ice_breakers_raw = ai_result.get("ice_breakers") or []
        if not ice_breakers_raw:
            # 旧格式兼容：buttons 字符串数组
            buttons_raw = ai_result.get("buttons") or []
            ice_breakers_raw = [{"title": b, "response": b} for b in buttons_raw[:3]]

        logger.info(f"[AutoPilot] AI生成Messenger欢迎消息: {welcome_text[:60]}... ice_breakers={len(ice_breakers_raw)}个")

        # 构建 ice_breakers，确保每项有 title 和 response
        ice_breakers = []
        for item in ice_breakers_raw[:3]:
            if isinstance(item, dict):
                title = str(item.get("title", ""))[:80]
                response = ""  # 不设置自动回复，避免 Automated response 被填充
            else:
                title = str(item)[:80]
                response = ""  # 不设置自动回复
            if title:
                ice_breakers.append({"title": title, "response": response})

        # 如果 AI 没有生成任何 ice_breakers，用广告标题生成默认值（避免通用问题）
        if not ice_breakers:
            short_headline = headline[:40]
            ice_breakers = [
                {"title": f"Tell me more about {short_headline}", "response": ""},
                {"title": "How do I get started?", "response": ""},
                {"title": "Is this available in my area?", "response": ""}
            ]

        # FB限制：欢迎语最多300字符
        safe_text = welcome_text[:300]
        return {
            "type": "VISUAL_EDITOR",
            "version": 2,
            "landing_screen_type": "welcome_message",
            "media_type": "text",
            "text_format": {
                "customer_action_type": "ice_breakers",
                "message": {
                    "ice_breakers": ice_breakers,
                    "quick_replies": [],
                    "text": safe_text
                }
            },
            "user_edit": False,
            "surface": "visual_editor_new"
        }
    def _ai_gen_lead_form_content(self, body: str, headline: str, asset_info: dict = None) -> dict:
        """根据素材AI分析结果，调用DeepSeek生成Lead Form内容"""
        import json as _json
        ai_analysis = ""
        ai_purpose = ""
        if asset_info:
            ai_analysis = asset_info.get("ai_analysis") or ""
            ai_purpose = asset_info.get("ai_purpose") or "general"

        prompt = (
            "你是一位Facebook广告专家，请根据以下广告素材信息，生成一个Lead Form（潜在客户表单）的内容。\n\n"
            f"广告标题：{headline}\n"
            f"广告文案：{body}\n"
            f"素材分析：{ai_analysis}\n"
            f"投放目的：{ai_purpose}\n\n"
            "请生成适合的表单内容，包含：\n"
            "1. 表单标题（简短有力，英文）\n"
            "2. 2-3个问题（收集有价值的潜在客户信息，英文）\n\n"
            "请用JSON格式返回：\n"
            '{\n  "form_title": "表单标题",\n  "questions": [\n'
            '    {"type": "CUSTOM", "label": "问题1"},\n'
            '    {"type": "CUSTOM", "label": "问题2"}\n'
            "  ]\n}\n"
            "只返回JSON，不要其他内容。"
        )

        raw = self._call_text_ai(prompt)

        ai_result = {"form_title": "", "questions": []}
        if raw:
            try:
                if "```" in raw:
                    parts = raw.split("```")
                    for part in parts[1::2]:
                        part = part.lstrip("json").strip()
                        if part.startswith("{"):
                            raw = part
                            break
                ai_result = _json.loads(raw)
            except Exception:
                pass

        return {
            "form_title": ai_result.get("form_title") or headline[:50],
            "questions": ai_result.get("questions") or [
                {"type": "CUSTOM", "label": "What are you interested in?"},
                {"type": "CUSTOM", "label": "What is your budget?"}
            ]
        }

    # ── Ad 创建 ───────────────────────────────────────────────────────────────

    def _create_ad(
        self, act_id: str, adset_id: str, name: str,
        headline: str, body: str, page_id: str,
        fb_asset_ref: dict, file_type: str, token: str,
        landing_url: str = "",
        conversion_goal: str = "",
        message_template: str = "",
        lead_form_id: str = "",
        asset_info: dict = None,
        cta_type: str = "",
        pixel_id: str = ""
    ) -> str:
        """创建 Facebook Ad（含 AdCreative）"""
        import json as _json
        act_id_num = act_id.replace("act_", "")

        # ── 消息模板：保留原始值，统一在后面消息广告构建阶段处理 ──
        # 不在此处提前解析，避免双重解析导致 int() 失败
        _msg_template_original = message_template  # 保留原始值（可能是模板ID或JSON字符串）

        # 表单广告：如果没有 form_id，用 AI 根据素材内容自动生成并在主页上创建 Lead Form
        _lead_form_resolved = lead_form_id
        _is_lead_ad_type = (conversion_goal or "").lower().strip() == "lead_generation"
        if _is_lead_ad_type:
            if lead_form_id and lead_form_id.strip().isdigit():
                # 传入的是模板 ID，尝试从模板创建
                try:
                    from api.ad_templates import create_lead_form_for_page
                    _fb_form_id = create_lead_form_for_page(page_id, int(lead_form_id.strip()), token=token)
                    if _fb_form_id:
                        _lead_form_resolved = _fb_form_id
                        logger.info(f"[AutoPilot] Lead Form 模板创建成功: form_id={_fb_form_id}")
                    else:
                        _lead_form_resolved = ""
                except Exception as _e:
                    logger.warning(f"[AutoPilot] Lead Form 模板创建失败: {_e}")
                    _lead_form_resolved = ""
            if not _lead_form_resolved:
                # 没有 form_id 或模板创建失败：用 AI 生成表单内容并在主页上创建
                logger.info(f"[AutoPilot] Lead Form 无 form_id，尝试用 AI 自动生成表单...")
                try:
                    import json as _json2, requests as _req
                    # AI 生成表单内容
                    _form_content = self._ai_gen_lead_form_content(body, headline, asset_info)
                    _form_title = _form_content.get("form_title") or headline[:50]
                    _questions = _form_content.get("questions") or [
                        {"type": "EMAIL", "key": "email"},
                        {"type": "PHONE", "key": "phone_number"},
                        {"type": "CUSTOM", "label": "What are you interested in?"}
                    ]
                    # 默认问题加入邮筱和电话（必填字段）
                    _default_fields = [
                        {"type": "EMAIL", "key": "email"},
                        {"type": "PHONE", "key": "phone_number"}
                    ]
                    _custom_qs = [q for q in _questions if q.get("type") not in ("EMAIL", "PHONE")]
                    _final_questions = _default_fields + _custom_qs[:2]  # 最多 4 个问题
                    _privacy_url = (asset_info or {}).get("landing_url") or "https://www.facebook.com/privacy/policy/"
                    _form_payload = {
                        "name": f"[AI] {_form_title[:60]}",
                        "questions": _json2.dumps(_final_questions),
                        "privacy_policy": _json2.dumps({"url": _privacy_url, "link_text": "Privacy Policy"}),
                        "locale": "zh_CN",
                        "access_token": token
                    }
                    _fb_resp = _req.post(
                        f"https://graph.facebook.com/v21.0/{page_id}/leadgen_forms",
                        data=_form_payload, timeout=30
                    ).json()
                    if "id" in _fb_resp:
                        _lead_form_resolved = _fb_resp["id"]
                        logger.info(f"[AutoPilot] AI 生成 Lead Form 成功: form_id={_lead_form_resolved}")
                    else:
                        logger.warning(f"[AutoPilot] AI 生成 Lead Form 失败: {_fb_resp.get('error', {}).get('message', _fb_resp)}")
                        _lead_form_resolved = ""
                except Exception as _ai_e:
                    logger.warning(f"[AutoPilot] AI 自动创建 Lead Form 异常: {_ai_e}")
                    _lead_form_resolved = ""
        lead_form_id = _lead_form_resolved

        # ── 判断是否为消息类广告（CTM = Click to Messenger/WhatsApp）──────────
        _MSG_GOALS = {
            "conversations", "messaging_purchase_conversion",
            "messaging_appointment_conversion", "messaging_leads"
        }
        is_msg_ad = (conversion_goal or "").lower().strip() in _MSG_GOALS

        # ── 修复 BUG-3: 购物类广告预检 landing_url ────────────────────
        _SALES_GOALS = {"offsite_conversions", "value", "product_catalog_sales"}
        _goal_lower = (conversion_goal or "").lower().strip()
        if _goal_lower in _SALES_GOALS and not landing_url:
            logger.warning(
                f"[AutoPilot] 购物类广告（{conversion_goal}）未提供落地页链接，"
                f"将使用主页链接作为替代。建议在铺广告时填写真实的购物落地页 URL。"
            )

        # ── 修复: 消息广告预检主页私信功能（error 1885187）────────────
        if is_msg_ad and page_id and token:
            try:
                _page_resp = requests.get(
                    f"https://graph.facebook.com/{FB_API_VERSION}/{page_id}",
                    params={"fields": "messaging_feature_status", "access_token": token},
                    timeout=10
                ).json()
                _msg_status = _page_resp.get("messaging_feature_status", {})
                # 如果返回了 messaging_feature_status 且 USER_MESSAGING 不是 ENABLED，则预警
                if _msg_status and _msg_status.get("USER_MESSAGING") not in ("ENABLED", None):
                    raise Exception(
                        f"主页 {page_id} 未开启私信功能（USER_MESSAGING={_msg_status.get('USER_MESSAGING')}\uff09，"
                        f"消息广告无法创建。请在 FB 主页设置中开启「允许消息」功能后重试。"
                    )
            except Exception as _pre_err:
                _pre_msg = str(_pre_err)
                # 只在明确知道是私信关闭时才抛出，其他预检失败则记录日志不阻断
                if "未开启私信" in _pre_msg:
                    raise
                logger.debug(f"[AutoPilot] 主页私信预检跳过: {_pre_err}")
        # ── 判断是否为 Lead Form 广告 ─────────────────────────────────────────
        is_lead_ad = (conversion_goal or "").lower().strip() == "lead_generation"
        is_page_likes_ad = (conversion_goal or "").lower().strip() == "page_likes"

        # ── 构建 call_to_action ───────────────────────────────────────────────
        # CTA 类型映射：用户选择的 cta_type → FB API 格式
        _CTA_LINK_TYPES = {
            "SHOP_NOW", "LEARN_MORE", "SIGN_UP", "GET_OFFER",
            "DOWNLOAD", "BOOK_TRAVEL", "CONTACT_US", "SUBSCRIBE",
            "GET_QUOTE", "WATCH_MORE", "APPLY_NOW", "GET_DIRECTIONS",
            "ORDER_NOW", "BUY_NOW", "SEE_MENU"
        }
        _CTA_MSG_TYPES = {
            "MESSAGE_PAGE", "SEND_MESSAGE", "WHATSAPP_MESSAGE",
            "MESSENGER_MESSAGE", "INSTAGRAM_MESSAGE"
        }
        def _build_cta(link: str) -> dict:
            """根据转化目的或用户指定的 cta_type 返回正确的 CTA"""
            # 优先使用用户指定的 cta_type
            if cta_type and cta_type.upper() != "AUTO":
                ct = cta_type.upper()
                if ct in _CTA_MSG_TYPES:
                    # 消息类 CTA
                    if ct == "WHATSAPP_MESSAGE":
                        return {"type": "WHATSAPP_MESSAGE", "value": {"app_destination": "WHATSAPP"}}
                    return {"type": ct, "value": {"app_destination": "MESSENGER"}}
                elif ct in _CTA_LINK_TYPES:
                    # 链接类 CTA
                    if ct == "SIGN_UP" and lead_form_id:
                        return {"type": "SIGN_UP", "value": {"lead_gen_form_id": lead_form_id}}
                    return {"type": ct, "value": {"link": link}}
                else:
                    # 未知类型，原样传递（让 FB 验证）
                    return {"type": ct, "value": {"link": link}}
            # 未指定 cta_type，按转化目的自动推断
            if is_page_likes_ad:
                # 主页赞广告：使用 LIKE_PAGE CTA
                return {"type": "LIKE_PAGE"}
            elif is_msg_ad:
                goal_lower = (conversion_goal or "").lower().strip()
                if "whatsapp" in goal_lower or "messaging_appointment" in goal_lower:
                    return {"type": "WHATSAPP_MESSAGE", "value": {"app_destination": "WHATSAPP"}}
                return {"type": "MESSAGE_PAGE", "value": {"app_destination": "MESSENGER"}}
            elif is_lead_ad:
                if lead_form_id:
                    return {"type": "SIGN_UP", "value": {"lead_gen_form_id": lead_form_id}}
                else:
                    # 无form_id时，优先使用账户级form_link，其次使用landing_url
                    _sign_up_link = _acc_form_link or link
                    return {"type": "SIGN_UP", "value": {"link": _sign_up_link}} if _sign_up_link else {"type": "SIGN_UP"}
            elif landing_url:
                return {"type": "SHOP_NOW", "value": {"link": link}}
            else:
                return {"type": "LEARN_MORE", "value": {"link": link}}

        # ── 构建 AdCreative ───────────────────────────────────────────────────
        if fb_asset_ref["type"] == "image":
            # PAGE_LIKES 广告：link 使用主页URL（不使用落地页）
            _default_link = (f"https://www.facebook.com/{page_id}" if is_page_likes_ad
                             else (landing_url or f"https://www.facebook.com/{page_id}"))
            link_data = {
                "image_hash": fb_asset_ref["image_hash"],
                "message": body,
                "name": headline,
                "link": _default_link,
                "call_to_action": _build_cta(_default_link),
            }
            object_story_spec = {"page_id": page_id, "link_data": link_data}
        else:  # video
            # PAGE_LIKES 广告：link 使用主页URL（不使用落地页）
            _default_link_v = (f"https://www.facebook.com/{page_id}" if is_page_likes_ad
                               else (landing_url or f"https://www.facebook.com/{page_id}"))
            video_data = {
                "video_id": fb_asset_ref["video_id"],
                "message": body,
                "title": headline,
                "call_to_action": _build_cta(_default_link_v),
            }
            # FB API 要求视频广告必须提供缩略图（image_url 或 image_hash）
            _thumbnail = fb_asset_ref.get("thumbnail_url", "")
            if not _thumbnail:
                # 缩略图为空（可能视频刚上传还在处理中），重新尝试获取
                _thumbnail = self._get_video_thumbnail(fb_asset_ref["video_id"], token)
                logger.info(f"[AutoPilot] 重新获取视频缩略图: {_thumbnail[:60] if _thumbnail else '失败'}")
            if _thumbnail:
                video_data["image_url"] = _thumbnail
            else:
                # 最终兜底：使用 FB 静态默认缩略图（避免 FB API 报 1443226 错误）
                video_data["image_url"] = "https://static.xx.fbcdn.net/rsrc.php/v4/yN/r/AAqMW82PqGg.gif"
                logger.warning(f"[AutoPilot] 视频缩略图获取失败，使用默认占位图")
            object_story_spec = {"page_id": page_id, "video_data": video_data}

        creative_payload = {
            "name": f"[AutoPilot] Creative - {name[:30]}",
            "object_story_spec": object_story_spec
        }

        # ── 消息广告：构建 page_welcome_message（放在 link_data/video_data 内部）──
        if is_msg_ad:
            _page_welcome = None
            # 使用原始模板值（避免双重解析）
            _tpl = _msg_template_original or ""
            if _tpl:
                # 尝试作为模板ID解析
                try:
                    _tid = int(str(_tpl).strip())
                    from api.ad_templates import get_msg_template_fb_format
                    _fb_fmt = get_msg_template_fb_format(_tid)
                    if _fb_fmt:
                        _page_welcome = _fb_fmt
                        logger.info(f"[AutoPilot] 使用消息模板 ID={_tid}")
                    else:
                        logger.warning(f"[AutoPilot] 消息模板 ID={_tid} 不存在，将自动生成")
                except (ValueError, TypeError):
                    # 非数字：可能是 JSON 字符串，直接解析
                    try:
                        _stripped = str(_tpl).strip()
                        if _stripped.startswith("{"):
                            _page_welcome = _json.loads(_stripped)
                            logger.info(f"[AutoPilot] 使用 JSON 格式消息模板")
                    except Exception as _je:
                        logger.warning(f"[AutoPilot] 消息模板 JSON 解析失败: {_je}")
            
            if _page_welcome is None:
                # 无模板或模板无效：调用 DeepSeek AI 根据素材信息自动生成欢迎消息
                _page_welcome = self._ai_gen_msg_template(body, headline, asset_info)
                logger.info(f"[AutoPilot] 使用 AI 自动生成消息模板")
            
            # 将 page_welcome_message 注入到 link_data 或 video_data 内部
            _pwm_str = _json.dumps(_page_welcome, ensure_ascii=False)
            if "link_data" in object_story_spec:
                object_story_spec["link_data"]["page_welcome_message"] = _pwm_str
            elif "video_data" in object_story_spec:
                object_story_spec["video_data"]["page_welcome_message"] = _pwm_str
            # 同步更新 creative_payload 中的 object_story_spec
            creative_payload["object_story_spec"] = object_story_spec

        # ── Lead Form 广告：如果没有 form_id，用 SIGN_UP 链接替代 ─────────────
        if is_lead_ad and not lead_form_id:
            logger.warning(
                f"[AutoPilot] 转化目的为 lead_generation 但未提供 lead_form_id，"
                f"将使用 SIGN_UP 链接替代。建议在 FB 后台创建 Lead Form 后填入 ID。"
            )

        creative_data = self._fb_post(
            f"act_{act_id_num}/adcreatives", token, creative_payload
        )
        creative_id = creative_data["id"]

        # 创建 Ad
        _ad_payload = {
            "name": name,
            "adset_id": adset_id,
            "creative": {"creative_id": creative_id},
            "status": "ACTIVE",
        }
        # 有 pixel_id 时加入 tracking_spec，避免 FB 报缺少像素错误
        if pixel_id:
            _ad_payload["tracking_specs"] = [{"action.type": ["offsite_conversion"], "fb_pixel": [pixel_id]}]
        ad_data = self._fb_post(
            f"act_{act_id_num}/ads", token, _ad_payload
        )
        return ad_data["id"]

    # ── 兴趣词解析 ───────────────────────────────────────────────────────────
    def _resolve_interests(self, interest_names: list, token: str) -> list:
        """将兴趣词名称列表解析为带id的FB兴趣对象列表"""
        import logging
        logger = logging.getLogger("autopilot")
        resolved = []
        for name in interest_names:
            try:
                data = self._fb_get("search", token, {
                    "type": "adinterest",
                    "q": name,
                    "limit": 5
                })
                items = data.get("data", [])
                matched = None
                for item in items:
                    if item.get("name", "").lower() == name.lower():
                        matched = item
                        break
                if not matched and items:
                    matched = items[0]
                if matched:
                    resolved.append({"id": str(matched["id"]), "name": matched["name"]})
                else:
                    logger.warning(f"Interest not found: {name}")
            except Exception as e:
                logger.warning(f"Failed to resolve interest '{name}': {e}")
        return resolved

    # ── 受众矩阵生成 ──────────────────────────────────────────────────────────

    def _build_audience_groups(
        self, interests: list, countries: list, max_adsets: int,
        age_min: int = 18, age_max: int = 65, gender: int = 0,
        token: str = None
    ) -> list[dict]:
        """
        根据 AI 推荐的兴趣词，生成受众分组。
        策略：
          - 每 2-3 个兴趣词为一组（窄受众）
          - 最后加一个宽泛受众（无兴趣词限制）
          - 总组数不超过 max_adsets
        """
        groups = []
        # 性别设置：0=不限, 1=男, 2=女
        genders = [gender] if gender in (1, 2) else []

        def _base_targeting(extra: dict = None) -> dict:
            t = {
                "geo_locations": {"countries": countries},
                "age_min": age_min,
                "age_max": age_max,
            }
            if genders:
                t["genders"] = genders
            if extra:
                t.update(extra)
            return t

        # 宽泛受众（必有）
        groups.append({
            "name": "宽泛受众",
            "targeting": _base_targeting()
        })

        # 兴趣词分组（先解析兴趣词ID）
        resolved_interests = []
        if interests and token:
            resolved_interests = self._resolve_interests(interests, token)
        # 注意：不再退化为只传name，因为FB API要求interests必须有id字段
        # 如果没有token或解析失败，resolved_interests保持为空，只使用宽泛受众

        chunk_size = 2
        for i in range(0, len(resolved_interests), chunk_size):
            if len(groups) >= max_adsets:
                break
            chunk = resolved_interests[i:i + chunk_size]
            interest_objs = chunk  # 已经是 {"id": ..., "name": ...} 格式
            groups.append({
                "name": f"兴趣: {', '.join(item['name'] for item in chunk)}",
                "targeting": _base_targeting({"flexible_spec": [{"interests": interest_objs}]})
            })

        return groups[:max_adsets]

    # ── 数据库辅助 ────────────────────────────────────────────────────────────

    def _load_asset(self, asset_id: int) -> Optional[dict]:
        conn = get_conn()
        row = conn.execute("SELECT * FROM ad_assets WHERE id=?", (asset_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def _parse_json_field(self, value, default):
        if not value:
            return default
        if isinstance(value, (list, dict)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return default

    def _update_campaign_status(self, campaign_id: int, status: str, error_msg: str = None):
        conn = get_conn()
        if error_msg:
            conn.execute(
                "UPDATE auto_campaigns SET status=?, error_msg=?, updated_at=? WHERE id=?",
                (status, error_msg[:1000], datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
            )
        else:
            conn.execute(
                "UPDATE auto_campaigns SET status=?, updated_at=? WHERE id=?",
                (status, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
            )
        conn.commit()
        conn.close()

    def _update_progress(self, campaign_id: int, step: str, msg: str = ""):
        """更新铺广告进度（供前端轮询显示）"""
        conn = get_conn()
        conn.execute(
            "UPDATE auto_campaigns SET progress_step=?, progress_msg=?, updated_at=? WHERE id=?",
            (step, msg[:300] if msg else "", datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
        )
        conn.commit()
        conn.close()

    def _update_campaign_field(self, campaign_id: int, field: str, value):
        conn = get_conn()
        conn.execute(
            f"UPDATE auto_campaigns SET {field}=?, updated_at=? WHERE id=?",
            (value, datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), campaign_id)
        )
        conn.commit()
        conn.close()

    def _insert_campaign_ad(
        self, campaign_id: int, act_id: str, asset_id: int,
        headline: Optional[str], body: Optional[str],
        targeting_json: str,
        fb_adset_id: Optional[str], fb_ad_id: Optional[str],
        status: str = "done", error_msg: str = None
    ):
        conn = get_conn()
        conn.execute(
            """INSERT INTO auto_campaign_ads
               (campaign_id, act_id, asset_id, headline, body, targeting_json,
                fb_adset_id, fb_ad_id, status, error_msg, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (campaign_id, act_id, asset_id, headline, body, targeting_json,
             fb_adset_id, fb_ad_id, status, error_msg,
             datetime.now(tz=timezone(timedelta(hours=8))).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
        conn.close()


# ── 定时任务入口（供 main.py 调用）────────────────────────────────────────────

def run_pending_campaigns():
    """
    扫描所有 pending 状态的自动铺广告任务并执行。
    由 main.py 的定时任务每 5 分钟调用一次。
    """
    conn = get_conn()
    pending = conn.execute(
        "SELECT id FROM auto_campaigns WHERE status='pending' ORDER BY created_at ASC LIMIT 10"
    ).fetchall()
    conn.close()

    if not pending:
        return

    engine = AutoPilotEngine()
    for row in pending:
        try:
            logger.info(f"[AutoPilot] 开始执行 pending 任务 campaign_id={row['id']}")
            engine.run_campaign(row["id"])
        except Exception as e:
            logger.error(f"[AutoPilot] 任务 {row['id']} 执行异常: {e}")
