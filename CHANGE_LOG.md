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

