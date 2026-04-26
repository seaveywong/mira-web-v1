"""
audit_kpi.py — 全网 KPI 正确性验证脚本 v3.4.0
验证目标:
  1. 每个活跃广告的 kpi_field 是否有效
  2. kpi_field 是否真实存在于 FB actions[] 中
  3. 未知 action_type 发现
  4. 全链路对齐检测 (composite_rule vs actual)
"""
import json, logging, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("audit_kpi")

from core.database import get_conn, decrypt_token


def _fb_get(path, token, params=None):
    import requests, urllib.parse
    p = dict(params or {})
    p["access_token"] = token
    url = f"https://graph.facebook.com/v25.0/{path}?{urllib.parse.urlencode(p)}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def audit_account(act_id, token):
    """审计单个账户的所有活跃广告"""
    from services.kpi_resolver import _is_valid_kpi_field, _get_composite_rule, get_kpi_label

    results = {"act_id": act_id, "total": 0, "aligned": 0, "misaligned": [], "unknown_types": set(), "zero_conv_with_spend": []}

    fields = (
        "id,name,status,effective_status,adset_id,campaign_id,"
        "campaign{objective},"
        "adset{optimization_goal,destination_type,custom_event_type},"
        "insights.date_preset(last_7d){actions,spend}"
    )
    try:
        data = _fb_get(f"{act_id}/ads", token, {"fields": fields, "limit": 200})
    except Exception as e:
        logger.error(f"  FB API 错误: {e}")
        return results

    ads = data.get("data", [])
    results["total"] = len(ads)

    for ad in ads:
        ad_id = ad["id"]
        ad_name = ad.get("name", ad_id)
        eff = ad.get("effective_status", "")

        insights = ad.get("insights", {}).get("data", [])
        if not insights:
            continue
        actions_raw = insights[0].get("actions", [])
        spend = float(insights[0].get("spend", 0))
        action_types = {a.get("action_type", "") for a in actions_raw}

        camp = ad.get("campaign", {}) or {}
        adset = ad.get("adset", {}) or {}
        obj = camp.get("objective", "")
        opt = adset.get("optimization_goal", "")
        ce = adset.get("custom_event_type", "")
        dst = adset.get("destination_type", "")

        # 已知 action_type 覆盖检查
        for a in actions_raw:
            at = a.get("action_type", "")
            if at and not _is_valid_kpi_field(at, actions_raw):
                results["unknown_types"].add(at)

        # kpi_field 对齐检查
        from services.kpi_resolver import scan_and_preset_kpi
        resolver_field = None
        try:
            composite = _get_composite_rule(obj, opt, ce, dst)
            if composite:
                resolver_field = composite[0]
        except Exception:
            pass

        if not resolver_field:
            continue

        # 检查 resolver_field 是否在 actions 中
        aliases = [resolver_field]
        if resolver_field == "purchase":
            aliases = ["purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase"]
        elif resolver_field == "lead":
            aliases = ["lead", "offsite_conversion.fb_pixel_lead", "onsite_conversion.lead_grouped"]
        elif resolver_field == "contact":
            aliases = ["contact", "offsite_conversion.fb_pixel_contact"]

        has_match = any(at in action_types for at in aliases)
        entry = {
            "ad_id": ad_id, "ad_name": ad_name, "status": eff,
            "objective": obj, "opt_goal": opt, "custom_event": ce, "dest": dst,
            "composite_kpi": resolver_field, "composite_label": get_kpi_label(resolver_field),
            "fb_actions": list(action_types)[:15],
            "matched": has_match, "spend": round(spend, 2)
        }

        if has_match:
            results["aligned"] += 1
        else:
            entry["issue"] = "kpi_field_not_in_actions"
            results["misaligned"].append(entry)
            if spend > 0:
                results["zero_conv_with_spend"].append(entry)

    return results


def main():
    conn = get_conn()
    accounts = conn.execute("SELECT * FROM accounts WHERE enabled=1").fetchall()
    conn.close()

    report = {"accounts": [], "total_ads": 0, "aligned": 0, "misaligned": 0, "unknown_types": set()}

    for acc in accounts:
        acc = dict(acc)
        act_id = acc["act_id"]
        token_id = acc.get("token_id")
        if not token_id:
            logger.warning(f"  {act_id}: 无 token_id，跳过")
            continue
        try:
            conn2 = get_conn()
            row = conn2.execute("SELECT access_token_enc FROM fb_tokens WHERE id=? AND status='active'", (token_id,)).fetchone()
            conn2.close()
            if not row:
                logger.warning(f"  {act_id}: token 无效，跳过")
                continue
            token = decrypt_token(row["access_token_enc"])
        except Exception as e:
            logger.warning(f"  {act_id}: token 解密失败: {e}")
            continue

        logger.info(f"审计账户: {act_id}")
        result = audit_account(act_id, token)
        report["accounts"].append(result)
        report["total_ads"] += result["total"]
        report["aligned"] += result["aligned"]
        report["misaligned"] += len(result["misaligned"])
        report["unknown_types"].update(result["unknown_types"])

        if result["misaligned"]:
            logger.warning(f"  ❌ {len(result['misaligned'])} 条 KPI 不对齐:")
            for m in result["misaligned"]:
                logger.warning(f"    {m['ad_name']}: composite={m['composite_kpi']} actions={m['fb_actions'][:5]}")
        else:
            logger.info(f"  ✅ 全部对齐")

    # 输出报告
    report["unknown_types"] = sorted(report["unknown_types"])
    print("\n" + "=" * 60)
    print(f"KPI 全网审计报告")
    print("=" * 60)
    print(f"总广告数: {report['total_ads']}")
    print(f"KPI 对齐: {report['aligned']} ✅")
    print(f"KPI 不对齐: {report['misaligned']} ❌")
    print(f"未知 action_type: {len(report['unknown_types'])}")
    if report['unknown_types']:
        for t in report['unknown_types']:
            print(f"  - {t}")
    print("=" * 60)

    # 写入文件
    def _sanitize(obj):
        if isinstance(obj, set):
            return sorted(obj)
        if isinstance(obj, dict):
            return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_sanitize(i) for i in obj]
        return obj

    out_path = "/var/log/mira/kpi_audit_report.json"
    try:
        with open(out_path, "w") as f:
            json.dump(_sanitize(report), f, ensure_ascii=False, indent=2)
        logger.info(f"报告已写入 {out_path}")
    except Exception:
        out_path = "kpi_audit_report.json"
        with open(out_path, "w") as f:
            json.dump(_sanitize(report), f, ensure_ascii=False, indent=2)
        logger.info(f"报告已写入 {out_path}")

    return report


if __name__ == "__main__":
    main()
