## v3.7.8 (2026-04-28)

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

