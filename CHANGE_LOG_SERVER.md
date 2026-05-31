## v3.10.0 - 2026-05-23 - feat: 账户预热 (Account Warmup)

### Added
1. `services/warmup_engine.py`: 新建预热引擎
   - `check_and_warmup()`: 扫描符合条件的账户并自动创建 page_likes 广告
   - `_warmup_account()`: 单账户预热 (选 YE* 素材 → CREATE token → FB API campaign→adset→adcreative→ad)
   - `_followup_warming()` / `_followup_completed()`: 预热状态跟进（每小时最多查一次 FB）
   - `_ensure_schema()`: 自愈建表 (5 个新列 + 2 个 settings 默认值)
   - 守护模式互斥: sentinel/heartbeat/mirror 任一开启则跳过
2. `api/warmup.py`: 新建 API 路由
   - `POST /api/warmup/scan`: 手动批量扫描（admin/operator 权限）
   - `POST /api/warmup/{act_id}/rewarm`: 单账户重新预热
   - `GET /api/warmup/status`: 查看预热状态
3. `main.py`: 注册 warmup_router
4. `scheduler.py`: 注册 `warmup_check` 定时任务（默认 30 分钟间隔）
5. `accounts.py`: list_accounts SELECT 添加 warmup_state / warmup_triggered_at / warmup_campaign_id
6. `index.html`:
   - 系统设置: warmup_enabled toggle + warmup_check_interval
   - 账户卡片: 预热状态标签（🟡待预热/🔵预热中/🟢预热完成/🟠待重新预热）
   - 账户页顶栏: "扫描全部预热" 按钮
   - 侧边栏: 预热模式 toggle + 状态指示灯
   - JS: renderWarmupBadge / scanAllWarmup / rewarmAccount / toggleWarmupMode

### Technical
- 状态机: NULL → warming → completed → dormant (7天沉睡自动标记)
- CREATE 操作用 ACTION_CREATE（仅操作号，不降级管理号）
- AdSet: PAGE_LIKES + OUTCOME_ENGAGEMENT, $5/天, US 定向, 随机页
- FB API: campaign→adset→adcreative→ad 四步创建, 失败回滚删 campaign
- 预算: $5 USD → 账户货币 (currency_rates 实时汇率)
- TG 通知: 聚合摘要，不逐条通知

### Files Changed
1. services/warmup_engine.py (NEW)
2. api/warmup.py (NEW)
3. main.py
4. services/scheduler.py (→ core/scheduler.py)
5. api/accounts.py
6. frontend/index.html

## v3.9.0 - 2026-05-23 - feat: 移除自动驾驶模块

### Removed
1. 删除 `autopilot_engine.py` / `global_dispatcher.py` / `lifecycle_manager.py` 三个服务文件
2. 删除 `api/autopilot.py` 路由文件
3. 删除 `frontend/batch-launch.js`
4. `guard_engine.py`: 移除 `_is_auto_campaign_ad`、AI 托管决策层、`_run_ai_decision`、`ScaleEngine` 类、止损后评分
5. `scheduler.py`: 移除 autopilot/scale/dispatch/lifecycle 定时任务
6. `main.py`: 移除 autopilot 路由注册
7. `accounts.py`: 移除 `lifecycle_stage`/`ai_managed` 字段和端点
8. `assets.py`: 移除 warmup CRUD、launch 端点、auto_campaign JOINs
9. `rules.py`: 移除 ScaleRuleIn 模型、scale rules CRUD、模板中的 scale_rules
10. `index.html`: 移除铺放中心/预热素材库页面、JS 函数、settings 条目
11. 数据库: DROP TABLE auto_campaigns, auto_campaign_ads, scale_rules, ai_decisions

### Files Changed
1. services/scheduler.py
2. main.py
3. services/guard_engine.py
4. api/accounts.py
5. api/assets.py
6. api/rules.py
7. frontend/index.html

### Deleted Files
1. services/autopilot_engine.py
2. services/global_dispatcher.py
3. services/lifecycle_manager.py
4. api/autopilot.py
5. frontend/batch-launch.js

## v3.8.3 - 2026-05-23 - fix: 消费上限移除口径与余额显示

### Fixed
1. API 侧只允许设置大于 0 的账户消费上限；0、空值或“移除上限”请求会返回明确提示，移除必须到 Meta 后台账单 UI 人工操作。
2. 同步账户状态时不再保留本地 NULL spend_cap，始终以 Meta API 返回的真实 spend_cap / amount_spent / balance 为准，人工移除后点击“同步状态”即可回写 Mira。
3. FB `balance` 不再参与“可用额度”计算；Mira 只用 `spend_cap - amount_spent` 推导剩余可投额度，并把 FB balance 标注为账单余额/欠款口径。
4. `$99,999,999` 这类历史超高上限不再汇总成可用余额，前端显示为“超高上限”，保留真实 limit 信息并提示可去 Meta UI 手动移除或改成合理上限。

### Files Changed
1. api/accounts.py
2. frontend/index.html

## v3.8.2 - 2026-05-23 - fix: 镜像模式采集自愈与降级

### Fixed
1. 镜像 API 增加 schema 自愈：自动确保 `accounts.mirror_enabled`、`mirror_snapshots` 表和索引存在。
2. 镜像快照采集改用最小字段 `id,name,status,effective_status,campaign_id`，避免完整巡检字段变动导致快照失败。
3. 守护巡检完整广告字段失败时，会额外跑一次镜像兜底巡逻，确保镜像保护不被 KPI/insights 字段问题拖死。

### Files Changed
1. api/mirror.py
2. services/guard_engine.py

## v3.8.1 - 2026-05-23 - fix: 余额显示与三守护模式体验

### Fixed
1. 账户金额统一按 FB minor units 计算，修复 balance / amount_spent / spend_cap 分和元混用导致的余额显示偏大。
2. 超高 spend_cap（>= $1,000,000）按“无上限”显示，避免 Mira 把 $99,999,999 sentinel 当作真实可用额度。
3. 设置消费上限时，非零金额正确转换为 FB minor units；当时的移除上限兼容方案已在 v3.8.3 改为“Meta UI 人工移除 + Mira 同步状态”。
4. 心跳模式现在会随管理员 API 活动刷新 last_admin_activity，开启心跳不会立刻按旧时间触发全停；超时触发后也会刷新一次，避免重复刷屏。
5. 哨兵模式过滤不可用账户，增加失败冷却，避免同一无权限系列每 3 分钟重复失败和 TG 告警。
6. 镜像模式单账户开关改为“快照捕获成功后才开启”，全局镜像改为先批量捕获再开启设置，避免开启但无快照的假状态。
7. page_likes 增补 Meta 实际返回的 like / post_net_like 别名，减少 KPI 不对齐噪音和误判风险。

### Files Changed
1. api/accounts.py
2. services/metrics_sync.py
3. services/guard_engine.py
4. api/mirror.py
5. api/settings.py
6. main.py
7. frontend/index.html

## v3.7.8 (2026-04-28)

## v3.5.0 - 2026-05-09 - feat: 镜像模式 (Mirror Mode)

### Added
1. 镜像模式: 开启后自动拦截并暂停不在快照白名单中的活跃广告，防止非授权用户盗刷
2. 全局开关: 左下角安全守护面板，与哨兵/心跳模式并列
3. 账户级开关: accounts.mirror_enabled 列，支持按账户单独启停

### How it works
- 用户开启镜像 -> 立即抓取当前活跃广告ID列表作为快照(白名单)
- 每次巡检(间隔5分钟)检查: 活跃广告在不在白名单中?
- 不在白名单 + 非系统广告 -> 暂停 + TG告警
- 用户B即使反复新建广告，5分钟内就会被拦截
- 关闭镜像后恢复自由，快照保留不删

### Files Changed
1. services/guard_engine.py: +3 helper functions, +mirror check block in inspect_account()
2. api/mirror.py: NEW - 4 endpoints (enable/disable/status/snapshot)
3. main.py: register mirror_router at /api/mirror
4. frontend/index.html: mirror toggle + dot + JS functions
5. migrate_mirror.sql: DB migration (ALTER TABLE accounts + CREATE TABLE mirror_snapshots)

## v3.4.7 - 2026-05-09 - fix: 巡检开关不生效

### Fixed
1. GuardEngine.run_all() 未检查 inspect_enabled 全局开关 — 即使设置页关闭巡检，引擎仍会巡检所有账户
2. GuardEngine.run_all() 未过滤 enabled=0 的账户 — 账户页关闭巡检后（enabled=0），巡检仍继续

### Changed
1. guard_engine.py run_all(): 增加 inspect_enabled 全局开关检查，关闭时跳过巡检
2. guard_engine.py run_all(): accounts 查询增加 WHERE enabled=1，禁用账户不巡检

### Fixed
- **哨兵/心跳开关初始化时序修复**: initSentinelHeartbeat() 在页面底部内联脚本中调用时 token 尚未设置（全局 token 为空），
  导致 GET /api/settings 返回 403，开关状态始终显示关闭。修复：在 initApp() 末尾添加 initSentinelHeartbeat() 调用，
  此时 token 已经过 /auth/me 验证有效，确保开关状态正确加载

### Files Changed
1. frontend/index.html: initApp() 末尾增加 initSentinelHeartbeat() 调用

## v3.7.7 (2026-04-28)

### Fixed
- **哨兵/心跳开关修复**: initSentinelHeartbeat() 响应解析错误 — 后端返回数组 [{key,value}]，前端误当对象读取，
  导致开关状态始终显示为关闭。修复：Array.isArray 检测后转换为 {key:value} 对象
- **调度器 NameError 修复**: sentinel_patrol/heartbeat_check 导入移到 start_scheduler() 内部，
  避免模块级导入可能存在的循环依赖问题

### Files Changed
1. frontend/index.html: initSentinelHeartbeat() 增加数组→对象转换
2. core/scheduler.py: sentinel_patrol/heartbeat_check 改为函数内延迟导入

## v3.7.6 (2026-04-28)

### Fixed
- **诊断冷却指示器始终显示**: 之前只在 in_cooldown=true 时显示冷却状态，现改为始终显示
  - 冷却中: 🕐 冷却中(X分钟) 橙色
  - 未冷却: ⏰ 无冷却 灰色
- 修复后用户可明确判断功能是否正常工作

### Files Changed
1. frontend/index.html: cdText/cdIcon/cdColor 三元表达式覆盖两种情况，移除 if(cd) 条件

## v3.7.5 (2026-04-28)

### Changed
- **诊断增加冷却状态**: diagnose API 返回每个匹配规则的 in_cooldown / cooldown_remaining_sec 字段
- **前端诊断弹窗**: 如果规则处于冷却期，显示 🕐 冷却中(X分钟) 提示，帮助判断为何广告未被关闭

### Files Changed
1. api/kpi.py: 导入 _action_cooldown，在 matching_rules 循环后追加冷却状态计算（读取 op_cooldown_min 设置，计算剩余冷却秒数）
2. frontend/index.html: showDiagnose() 规则渲染增加冷却状态显示，冷却中显示警告色 🕐 图标和剩余分钟数


## v3.7.2 (2026-04-27)

### Hotfix
- **Fix 服务器资源面板不可用**: 修复前端调用  但后端路径为  导致的 404 错误
- **哨兵/心跳 UI 美化**: 侧边栏安全守护区块重新设计，改用 iOS 风格 toggle 开关，增加状态指示灯（绿色脉冲=运行中）和运行参数提示（扫描间隔/超时时间）
- 新增 toggle switch CSS (.tgl-sw / .tgl-tr)，统一样式

### Files Changed
1. frontend/index.html: 添加 toggle CSS，修复 resource API 路径，重设计侧边栏安全守护区块，优化 JS 逻辑

## v3.7.1 (2026-04-27)

### Hotfix
- **哨兵/心跳开关移至侧边栏**：将哨兵模式和心跳模式的开关从系统设置页面移至侧边栏底部"一键紧急暂停"按钮上方，方便快速操作
- 侧边栏新增"安全守护"区块，包含哨兵/心跳 toggle 开关及状态显示
- 页面加载时自动从后端读取开关状态并初始化 toggle 位置

### Files Changed
1. frontend/index.html: 从 SETTINGS_META 移除 4 个安全设置项，移除 security 分类，侧边栏新增 toggle 开关 + JS 函数

## v3.7.0 (2026-04-27)

### Features
- **哨兵模式 (Sentinel Mode)**：开启后定时扫描所有账户，发现活跃系列自动关闭并 TG 告警，防止管理员不在线时广告被盗刷
- **心跳模式 (Heartbeat Mode)**：周期内未检测到管理员 API 操作，自动紧急全停所有系列，防止监控中睡着导致广告失控
- **管理员活动跟踪**：FastAPI 中间件自动记录最近一次用户活动时间戳

### Files Changed
1. ：新增 5 个 settings（sentinel_enabled/interval, heartbeat_enabled/timeout, last_admin_activity）
2. ：新增  和  函数
3. ：注册哨兵扫描和心跳检查两个定时任务
4. ：新增 ActivityMiddleware 中间件记录用户活动时间
5. ：新增安全守护设置区（SETTINGS_META + 分类标签）

### Technical
- sentinel_patrol: 每 N 分钟扫描所有账户，FB API GET /campaigns?effective_status=["ACTIVE"]，发现后 POST pause
- heartbeat_check: 每 (timeout/3) 分钟检查 last_admin_activity，超时调用 emergency_pause_all(level="campaign")
- ActivityMiddleware: 检测 Authorization: Bearer header，写入 last_admin_activity 时间戳

## v3.6.0 (2026-04-27)

### Features
- **Lead Form optimization**: 5 fixes for lead form creation flow
  - flexible_delivery: Auto-enable ON_DELIVERY for CUSTOM questions with options (FB v25 requirement)
  - Greeting: Make greeting field optional in message templates (can be empty)
  - Multiple choice questions: Add MULTIPLE_CHOICE field type support with options input
  - Question description: Add placeholder/description field for CUSTOM questions
  - End page content: Fix context_card structure (body at top level, button_type derived from ad_type)

### Files Changed
1. api/ad_templates.py: Fix _post_lead_form() payload (flexible_delivery + context_card with button_type + body)
   - _normalize_lead_form_questions(): Add flexible_delivery + placeholder support
   - create_lead_form_for_page(): Add body to context_card
2. services/autopilot_engine.py: Upgrade AI prompt to generate qualifying_description + options
   - Questions construction: Build options array (opt_0, opt_1, ...) for multiple choice
   - Context_card building: Derive button_type from ad_type via _BUTTON_TYPE_MAP
3. frontend/index.html: Add MULTIPLE_CHOICE field type in add/save/edit lead form
   - Remove greeting mandatory validation in message templates
   - Add options input (comma-separated) for multiple choice questions
   - Serialize/deserialize options in save/edit flow

### Technical
- flexible_delivery="ON_DELIVERY" set both in question level (normalize) and form level (payload)
- button_type map: leads→SIGN_UP, purchase→SHOP_NOW, messenger→CONTACT_US, traffic→LEARN_MORE
- Frontend edit render preserves existing data, adds options input for MULTIPLE_CHOICE

## v3.5.0 (2026-04-27)

### Features
- **smart_scorer.py 升级**: 多维评分体系，6 维度（Visual/Copy/Emotion/Offer/Compliance/Audience-fit）加权评分
- **数据反馈环**: 新增 `_correlate_with_performance()`，每日对比 AI 评分 vs 实际 FB 性能分，标记偏差大的素材重新评分
- **疲劳度预测**: 新增 `_predict_fatigue()`，基于投放次数、时间衰减、同受众密度预测素材疲劳度
- **Fallback 引擎升级**: 语义级关键词聚类 + 正则数值提取，替代机械关键词匹配
- **zh-hk 粤语内容生成**: 在 AI prompt 中加入粤语白话文示例词汇（係、喺、嘅、唔、咗、啲等），确保香港素材生成地道粤语

### Bug Fixes
- `api/assets.py`: 修复 `zh_mode` 缺少 zh-hk（第 187 行），zh-hk 资产现在正确显示中文错误消息
- `api/assets.py`: 修复 `score_and_infer` 不存在的导入（应为 `score_asset`），AI 分析后评分现在正常触发
- `api/assets.py`: 修复素材评分跳过逻辑，支持 `needs_rescore` 标记和 7 天自动重新评分

### Technical
- `smart_scorer.py`: GRADE_CRITERIA 支持 6 维评分 JSON 输出（含 dimensions 字段）
- `smart_scorer.py`: max_tokens 从 500 提升到 800（应对更长的 prompt）
- `scheduler.py`: 新增每日 2:30 评分反馈环任务
## v3.4.7 - 2026-04-27 - feat: 香港繁体中文(zh-hk) AI 语种独立支持

### Added
1. api/assets.py: AI_LANGUAGE_NAMES 添加 zh-hk；COUNTRY_LANGUAGE_MAP 中 HK → zh-hk（原映射到 zh-tw）
2. services/autopilot_engine.py: 全链路 zh-hk 支持
   - LANGUAGE_LABELS 添加 zh-hk
   - _normalize_language_code() 将 zh-hk 拆分为独立语种（原映射到 zh-tw）
   - locale_map 添加 zh_HK
   - text_map / _default_msg_template / _localized_lead_form_fallback 添加香港粤语用词
   - AI prompt 语言守卫增加 zh-hk，避免繁体中文被误判为台湾用语
3. services/smart_scorer.py: 评分推荐国家规则添加香港繁体中文识别
4. frontend/index.html: 
   - 语言选择器添加 繁體中文(香港) 选项
   - Lead Form 区域设置添加 zh_HK
   - 3 处国家-语言映射表 HK 从 zh-tw 改为 zh-hk

### Impact
- 目标国家包含 HK 时，AI 生成的文案/表单/消息模板将使用香港繁体中文（粤语用词）
- 不再笼统归为「繁体中文/台湾」，解决语气风格偏台湾的问题

## v3.4.6 - 2026-04-27 - fix: 巡检400错误(custom_event_type在v25.0不存在)

### Fixed
1. guard_engine.py: Remove  from FB_AD_FIELDS field list
   - FB API v25.0 dropped custom_event_type on adset, causing (#100) error on ALL patrols
   - Result: ALL accounts failed to fetch ads, guard engine was completely broken
2. kpi_resolver.py: Same fix in scan_and_preset_kpi() FB API field selection
3. scripts/audit_kpi.py: Same fix in audit script field selection

### Changed
1. guard_engine.py: adset{optimization_goal,destination_type,custom_event_type} → adset{optimization_goal,destination_type}
2. kpi_resolver.py: Same field fix in scan_and_preset_kpi() at line 891


## v3.3.12 - 2026-04-26 - KPI规则前后端一致性修复

### Fixed
1. guard rules: Fix rule id=39 rule_name (says "联系" but kpi_filter='leads') → "线索-单次成效费用>$25"
2. guard rules: Delete rule id=44 (kpi_filter='contact') — unreachable, infer_ad_type() never returns 'contact'
3. frontend: Fix cpa_exceed card display — was showing param_ratio ("超标比例: 100%") instead of actual CPA threshold
4. frontend: Add 'contact' to kpiFilterLabel mapping
5. guard rules: Adjust engagement CPA threshold from $0.10 → $0.25 (avg CPA $0.18-0.21, prevents false positives)
6. guard rules: Adjust other(reach) CPA threshold from $0.05 → $0.10

### Changed
1. frontend: Hide ratio display on cpa_exceed card when ratio==1.0 (reduces visual noise)

## v3.4.5 - 2026-04-26 - Action logging for batch launch operations

### Changed
1. api/assets.py: Added _log_action calls in both batch launch endpoints
   - batch_launch_multi_assets: logs success/error per campaign with action_type="batch_launch"
   - batch_launch_auto_campaign: logs success/error per account with action_type="batch_launch"
   - Uses existing _log_action helper from services.guard_engine
   - operator set to current user username from JWT token
## v3.10.1 - 2026-05-23 - fix: 预热模式安全收口与残留清理

### Fixed
1. 素材库列表、素材静态 API、账户搜索接口恢复可用。
2. 规则页不再请求已移除的拉量策略接口，自定义规则模板创建恢复可用。
3. 账户限额同步恢复以 Meta 当前值为准，设置限额走 API，移除限额走 Meta UI 后同步。
4. 缺失 `ai_decisions` 表时账户决策日志接口返回空列表，不再 500。

### Changed
1. 预热素材来源改为素材库 `YE*` 图片，不再使用旧预热素材库。
2. 预热增加并发锁、国家匹配和约 `$5` 消耗后自动暂停。
3. 前端清理旧预热素材库、铺放中心、AI 托管、生命周期和拉量入口。
4. 超高账户限额显示为“超高上限 + 已消费 + 实际上限”，不再展示误导性余额。
