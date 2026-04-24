"""
Mira v3.0 数据库迁移脚本
- 新增 account_op_tokens 表（账户与操作号多对多映射）
- fb_tokens.token_type 新增 'operate' 类型支持
- ad_assets 素材库表
- auto_campaigns 自动铺广告任务表
- auto_campaign_ads 广告矩阵明细表
- settings 新增 v3 相关配置项
全部使用 IF NOT EXISTS / 字段存在检查，保证幂等，可重复执行
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from core.database import get_conn

def run():
    conn = get_conn()
    print("[migrate v3] 开始执行 Mira v3.0 数据库迁移...")

    # ── 1. account_op_tokens：账户 ↔ 操作号 多对多映射 ──────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS account_op_tokens (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            act_id      TEXT NOT NULL,
            token_id    INTEGER NOT NULL REFERENCES fb_tokens(id) ON DELETE CASCADE,
            priority    INTEGER DEFAULT 0,
            status      TEXT DEFAULT 'active',
            note        TEXT,
            created_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(act_id, token_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_aot_act ON account_op_tokens(act_id)")
    print("[migrate v3] ✅ account_op_tokens 表已创建")

    # ── 2. ad_assets：素材库 ──────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ad_assets (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            act_id          TEXT,
            file_name       TEXT NOT NULL,
            file_type       TEXT NOT NULL DEFAULT 'image',
            file_path       TEXT NOT NULL,
            file_size       INTEGER,
            file_hash       TEXT,
            fb_asset_id     TEXT,
            fb_asset_type   TEXT,
            fb_page_id      TEXT,
            upload_status   TEXT DEFAULT 'pending',
            ai_analysis     TEXT,
            ai_headlines    TEXT,
            ai_bodies       TEXT,
            ai_interests    TEXT,
            score           REAL DEFAULT 0,
            score_label     TEXT,
            total_spend     REAL DEFAULT 0,
            total_conv      INTEGER DEFAULT 0,
            avg_cpa         REAL,
            avg_roas        REAL,
            note            TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_act ON ad_assets(act_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_hash ON ad_assets(file_hash)")
    print("[migrate v3] ✅ ad_assets 表已创建")

    # ── 3. auto_campaigns：自动铺广告任务 ────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS auto_campaigns (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            act_id          TEXT NOT NULL,
            asset_id        INTEGER REFERENCES ad_assets(id),
            name            TEXT,
            objective       TEXT DEFAULT 'OUTCOME_SALES',
            target_countries TEXT DEFAULT '[]',
            target_cpa      REAL,
            daily_budget    REAL DEFAULT 20,
            status          TEXT DEFAULT 'pending',
            fb_campaign_id  TEXT,
            total_adsets    INTEGER DEFAULT 0,
            total_ads       INTEGER DEFAULT 0,
            error_msg       TEXT,
            created_by      TEXT DEFAULT 'system',
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ac_act ON auto_campaigns(act_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ac_status ON auto_campaigns(status)")
    print("[migrate v3] ✅ auto_campaigns 表已创建")

    # ── 4. auto_campaign_ads：广告矩阵明细 ───────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS auto_campaign_ads (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id     INTEGER NOT NULL REFERENCES auto_campaigns(id),
            act_id          TEXT NOT NULL,
            asset_id        INTEGER REFERENCES ad_assets(id),
            headline        TEXT,
            body            TEXT,
            targeting_json  TEXT,
            fb_adset_id     TEXT,
            fb_ad_id        TEXT,
            status          TEXT DEFAULT 'pending',
            error_msg       TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_aca_campaign ON auto_campaign_ads(campaign_id)")
    print("[migrate v3] ✅ auto_campaign_ads 表已创建")

    # ── 5. action_logs 表补充 level / trigger_type / old_value / new_value 字段 ─
    cols = [r[1] for r in conn.execute("PRAGMA table_info(action_logs)").fetchall()]
    for col, definition in [
        ("level",         "TEXT DEFAULT 'ad'"),
        ("trigger_type",  "TEXT"),
        ("old_value",     "TEXT"),
        ("new_value",     "TEXT"),
        ("operator",      "TEXT DEFAULT 'system'"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE action_logs ADD COLUMN {col} {definition}")
            print(f"[migrate v3] ✅ action_logs.{col} 字段已添加")

    # ── 6. accounts 表补充 v3 字段 ───────────────────────────────────────────
    cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
    for col, definition in [
        ("balance",                  "REAL"),
        ("account_status",           "INTEGER DEFAULT 1"),
        ("spend_cap",                "REAL"),
        ("note",                     "TEXT"),
        ("updated_at",               "TEXT"),
        # v3.1 新增字段
        ("ai_managed",               "INTEGER DEFAULT 0"),   # AI托管开关
        ("lifecycle_stage",          "TEXT DEFAULT 'testing'"),  # warmup/testing/scaling/paused
        ("page_id",                  "TEXT"),
        ("pixel_id",                 "TEXT"),
        ("amount_spent",             "REAL DEFAULT 0"),
        ("spending_limit",           "REAL"),
        ("amount_spent_usd",         "REAL DEFAULT 0"),
        ("balance_usd",              "REAL"),
        ("balance_type",             "TEXT DEFAULT 'prepaid'"),
        ("available_balance",        "REAL"),
        ("timezone_name",            "TEXT"),
        ("target_countries",         "TEXT DEFAULT '[]'"),
        ("target_age_min",           "INTEGER DEFAULT 25"),
        ("target_age_max",           "INTEGER DEFAULT 65"),
        ("target_gender",            "INTEGER DEFAULT 0"),
        ("target_placements",        "TEXT"),
        ("target_objective_type",    "TEXT DEFAULT 'OUTCOME_SALES'"),
        ("landing_url",              "TEXT"),
        ("beneficiary",              "TEXT"),
        ("payer",                    "TEXT"),
        ("tw_advertiser_id",         "TEXT"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE accounts ADD COLUMN {col} {definition}")
            print(f"[migrate v3] ✅ accounts.{col} 字段已添加")

    # ── 7. fb_tokens token_type 说明更新（不改数据，只更新 settings 描述）────
    # token_type 取值：'user'（旧兼容）| 'manage'（管理号）| 'operate'（操作号）
    # 前端 moAddToken 下拉选项将同步更新

    # ── 8. settings 新增 v3 配置项 ───────────────────────────────────────────
    v3_settings = [
        # 操作号调度
        ('op_token_heartbeat_interval', '10', '操作号心跳检测间隔（分钟）',
         '每隔多少分钟检测一次操作号 Token 的有效性', '10', 'guard', 10),
        ('op_token_fail_threshold', '2', '操作号失效判定次数',
         '连续失败多少次后将操作号标记为 invalid', '2', 'guard', 11),
        # 自动铺广告
        ('autopilot_enabled', '1', '启用全自动铺广告',
         '开启后系统将根据素材库自动创建测试广告', '', 'autopilot', 1),
        ('autopilot_test_budget', '20', '测试期单组预算（USD）',
         '每个测试广告组的初始日预算', '20', 'autopilot', 2),
        ('autopilot_stop_loss', '15', '测试期止血金额（USD）',
         '测试广告消耗达到此金额且无转化时自动关闭', '15', 'autopilot', 3),
        ('autopilot_max_adsets', '5', '单次铺广告最大组数',
         '每次自动铺广告最多创建的广告组数量', '5', 'autopilot', 4),
        ('autopilot_fb_page_id', '', '默认 Facebook 主页 ID',
         '自动建广告时使用的默认主页 ID', 'xxxxxxxxx', 'autopilot', 5),
        ('autopilot_fb_pixel_id', '', '默认 Pixel ID',
         '自动建广告时使用的默认 Pixel ID', 'xxxxxxxxx', 'autopilot', 6),
        # AI 视觉分析（v3 新增）
        ('ai_vision_enabled', '0', '启用 AI 视觉分析',
         '上传素材后自动调用多模态 AI 分析画面内容并生成文案', '', 'ai', 10),
        ('ai_vision_model', 'gpt-4.1-mini', 'AI 视觉模型',
         '支持多模态的模型名称', 'gpt-4.1-mini', 'ai', 11),
    ]
    for row in v3_settings:
        conn.execute(
            "INSERT OR IGNORE INTO settings(key,value,label,description,placeholder,category,sort_order) VALUES(?,?,?,?,?,?,?)",
            row
        )
    print(f"[migrate v3] ✅ settings 新增 {len(v3_settings)} 条 v3 配置项")

    conn.commit()
    conn.close()
    print("[migrate v3] ✅ Mira v3.0 数据库迁移完成")

if __name__ == "__main__":
    run()
