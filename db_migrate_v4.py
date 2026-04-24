"""
Mira v4.0 数据库迁移脚本
修复 auto_campaigns 表缺少大量字段导致铺广告 INSERT 报错的根本问题。
全部使用字段存在检查，保证幂等，可重复执行。
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from core.database import get_conn


def run():
    conn = get_conn()
    print("[migrate v4] 开始执行 Mira v4.0 数据库迁移...")

    # ── 1. auto_campaigns 补全所有缺失字段 ───────────────────────────────────
    cols = [r[1] for r in conn.execute("PRAGMA table_info(auto_campaigns)").fetchall()]
    ac_fields = [
        # 受众定向字段
        ("age_min",              "INTEGER DEFAULT 18"),
        ("age_max",              "INTEGER DEFAULT 65"),
        ("gender",               "INTEGER DEFAULT 0"),
        ("placements",           "TEXT"),
        ("bid_strategy",         "TEXT DEFAULT 'LOWEST_COST_WITHOUT_CAP'"),
        ("max_adsets",           "INTEGER DEFAULT 5"),
        # 账户覆盖字段
        ("page_id_override",     "TEXT"),
        ("pixel_id_override",    "TEXT"),
        ("landing_url",          "TEXT"),
        # 设备端和语言
        ("device_platforms",     "TEXT DEFAULT 'all'"),
        ("ad_language",          "TEXT DEFAULT 'en'"),
        # 转化事件细分
        ("conversion_event",     "TEXT DEFAULT 'PURCHASE'"),
        # 台湾认证广告主
        ("tw_page_id",           "TEXT"),
        # 转化目的（ODAX 细分目标）
        ("conversion_goal",      "TEXT"),
        # 消息广告欢迎消息模板
        ("message_template",     "TEXT"),
        # 潜在客户表单 ID
        ("lead_form_id",         "TEXT"),
        # 行动号召按钮类型
        ("cta_type",             "TEXT DEFAULT ''"),
        # 进度追踪字段
        ("progress_step",        "TEXT"),
        ("progress_msg",         "TEXT"),
        # 调度来源（autopilot/manual/guard）
        ("dispatch_source",      "TEXT DEFAULT 'manual'"),
        # 铺广告时的生命周期阶段快照
        ("lifecycle_stage_at_launch", "TEXT"),
        # fb_adset_id（第一个成功的 AdSet ID，方便快速查询）
        ("fb_adset_id",          "TEXT"),
    ]
    added = 0
    for col, definition in ac_fields:
        if col not in cols:
            conn.execute(f"ALTER TABLE auto_campaigns ADD COLUMN {col} {definition}")
            print(f"[migrate v4] ✅ auto_campaigns.{col} 字段已添加")
            added += 1
    if added == 0:
        print("[migrate v4] ℹ️  auto_campaigns 所有字段已存在，无需添加")
    else:
        print(f"[migrate v4] ✅ auto_campaigns 共添加 {added} 个字段")

    # ── 2. auto_campaign_ads 补全缺失字段 ────────────────────────────────────
    cols2 = [r[1] for r in conn.execute("PRAGMA table_info(auto_campaign_ads)").fetchall()]
    aca_fields = [
        # 广告组名称（方便排查）
        ("adset_name",           "TEXT"),
        # 广告名称
        ("ad_name",              "TEXT"),
        # 广告创建时间（更精确）
        ("updated_at",           "TEXT"),
    ]
    added2 = 0
    for col, definition in aca_fields:
        if col not in cols2:
            conn.execute(f"ALTER TABLE auto_campaign_ads ADD COLUMN {col} {definition}")
            print(f"[migrate v4] ✅ auto_campaign_ads.{col} 字段已添加")
            added2 += 1
    if added2 == 0:
        print("[migrate v4] ℹ️  auto_campaign_ads 所有字段已存在，无需添加")

    # ── 3. ad_assets 补全缺失字段 ────────────────────────────────────────────
    cols3 = [r[1] for r in conn.execute("PRAGMA table_info(ad_assets)").fetchall()]
    asset_fields = [
        # 素材代码（用于广告命名）
        ("asset_code",           "TEXT"),
        # 素材展示名（优先于 file_name 显示）
        ("display_name",         "TEXT"),
        # 素材级落地页链接
        ("landing_url",          "TEXT"),
        # 素材标签（逗号分隔）
        ("tags",                 "TEXT"),
        # 素材状态（active/archived/deleted）
        ("asset_status",         "TEXT DEFAULT 'active'"),
        # 素材来源（upload/ai_gen）
        ("source",               "TEXT DEFAULT 'upload'"),
        # AI 生图任务 ID
        ("gen_task_id",          "TEXT"),
        # 素材尺寸
        ("width",                "INTEGER"),
        ("height",               "INTEGER"),
        # 素材时长（视频用，秒）
        ("duration",             "REAL"),
        # 素材缩略图路径
        ("thumbnail_path",       "TEXT"),
        # 素材 ROAS（来自 asset_spend_log 聚合）
        ("best_roas",            "REAL"),
        # 素材最后活跃时间
        ("last_active_at",       "TEXT"),
    ]
    added3 = 0
    for col, definition in asset_fields:
        if col not in cols3:
            conn.execute(f"ALTER TABLE ad_assets ADD COLUMN {col} {definition}")
            print(f"[migrate v4] ✅ ad_assets.{col} 字段已添加")
            added3 += 1
    if added3 == 0:
        print("[migrate v4] ℹ️  ad_assets 所有字段已存在，无需添加")
    else:
        print(f"[migrate v4] ✅ ad_assets 共添加 {added3} 个字段")

    # ── 4. accounts 表补全 v4 新增字段 ───────────────────────────────────────
    cols4 = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
    acc_fields = [
        # 账户级默认落地页（覆盖全局设置）
        ("landing_url",          "TEXT"),
        # 账户级目标转化类型
        ("target_objective_type","TEXT DEFAULT 'OUTCOME_SALES'"),
        # 账户热身天数和预算
        ("warmup_days",          "INTEGER DEFAULT 3"),
        ("warmup_budget",        "REAL DEFAULT 10"),
        # 账户生命周期阶段（warmup/testing/scaling/paused）
        ("lifecycle_stage",      "TEXT DEFAULT 'testing'"),
        # 账户生命周期更新时间
        ("lifecycle_updated_at", "TEXT"),
        # AI 托管开关
        ("ai_managed",           "INTEGER DEFAULT 0"),
        # 账户货币
        ("currency",             "TEXT DEFAULT 'USD'"),
    ]
    added4 = 0
    for col, definition in acc_fields:
        if col not in cols4:
            conn.execute(f"ALTER TABLE accounts ADD COLUMN {col} {definition}")
            print(f"[migrate v4] ✅ accounts.{col} 字段已添加")
            added4 += 1
    if added4 == 0:
        print("[migrate v4] ℹ️  accounts 所有字段已存在，无需添加")
    else:
        print(f"[migrate v4] ✅ accounts 共添加 {added4} 个字段")

    # ── 5. settings 新增 v4 配置项 ───────────────────────────────────────────
    v4_settings = [
        # 全局调度开关
        ('global_dispatch_enabled', '1', '全局调度总开关',
         '关闭后所有自动调度任务（铺广告/巡检/止损）均暂停', '1', 'guard', 0),
        ('inspect_enabled', '1', '启用广告巡检',
         '开启后系统将定期检查广告表现并执行止损/放量规则', '1', 'guard', 1),
        # 默认落地页
        ('default_landing_url', '', '全局默认落地页链接',
         '铺广告时未填写落地页时使用此链接', 'https://your-shop.com', 'autopilot', 7),
        # AI 消息模板自动生成
        ('ai_msg_template_enabled', '1', '启用 AI 消息模板自动生成',
         '消息广告未选模板时，自动调用 AI 生成欢迎消息', '1', 'ai', 20),
    ]
    for row in v4_settings:
        conn.execute(
            "INSERT OR IGNORE INTO settings(key,value,label,description,placeholder,category,sort_order) VALUES(?,?,?,?,?,?,?)",
            row
        )
    print(f"[migrate v4] ✅ settings 新增 {len(v4_settings)} 条 v4 配置项")

    # ── 6. 创建 tw_certified_pages 表（如果不存在）──────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tw_certified_pages (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id     TEXT NOT NULL UNIQUE,
            page_name   TEXT,
            verified    INTEGER DEFAULT 0,
            note        TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    print("[migrate v4] ✅ tw_certified_pages 表已确认存在")

    # ── 7. 创建 tw_advertisers 表（如果不存在）──────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tw_advertisers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fb_user_id  TEXT,
            beneficiary TEXT,
            payer       TEXT,
            verified    INTEGER DEFAULT 0,
            note        TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    print("[migrate v4] ✅ tw_advertisers 表已确认存在")

    # ── 8. 创建 asset_spend_log 表（如果不存在）─────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS asset_spend_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id        INTEGER REFERENCES ad_assets(id),
            act_id          TEXT,
            act_name        TEXT,
            fb_ad_id        TEXT,
            fb_adset_id     TEXT,
            fb_campaign_id  TEXT,
            target_countries TEXT,
            objective       TEXT,
            kpi_field       TEXT,
            matched_field   TEXT,
            spend           REAL DEFAULT 0,
            conv            INTEGER DEFAULT 0,
            conv_value      REAL DEFAULT 0,
            impressions     INTEGER DEFAULT 0,
            clicks          INTEGER DEFAULT 0,
            is_active       INTEGER DEFAULT 1,
            last_synced_at  TEXT DEFAULT (datetime('now')),
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_asl_asset ON asset_spend_log(asset_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_asl_act ON asset_spend_log(act_id)")
    print("[migrate v4] ✅ asset_spend_log 表已确认存在")

    conn.commit()
    conn.close()
    print("[migrate v4] ✅ Mira v4.0 数据库迁移完成！铺广告功能现在可以正常使用。")


if __name__ == "__main__":
    run()
