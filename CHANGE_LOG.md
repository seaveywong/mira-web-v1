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

