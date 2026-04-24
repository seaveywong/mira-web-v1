"""
storage_manager.py  —  Mira 存储空间管理模块
功能：
  1. 磁盘使用情况查询（总量/已用/各目录占用）
  2. AI 生成图片自动清理（已拒绝 / 超过保留天数）
  3. 备份文件管理（列出/清理旧备份）
  4. systemd journal 日志限制
  5. 应用日志轮转
  6. 磁盘告警阈值检测（可配置，默认 80%）
"""
import os
import shutil
import subprocess
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("mira")

BASE_DIR = "/opt/mira"
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
PENDING_DIR = os.path.join(ASSETS_DIR, "pending_review")
THUMBS_DIR = os.path.join(ASSETS_DIR, "thumbs")
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")


# ─── 磁盘使用情况 ─────────────────────────────────────────────────────────────

def get_disk_usage() -> dict:
    """获取磁盘总体使用情况"""
    stat = shutil.disk_usage("/")
    total_gb = stat.total / 1024 ** 3
    used_gb = stat.used / 1024 ** 3
    free_gb = stat.free / 1024 ** 3
    pct = stat.used / stat.total * 100
    return {
        "total_gb": round(total_gb, 2),
        "used_gb": round(used_gb, 2),
        "free_gb": round(free_gb, 2),
        "used_pct": round(pct, 1),
        "status": "danger" if pct >= 85 else "warning" if pct >= 70 else "ok",
    }


def get_dir_sizes() -> list:
    """获取各关键目录的占用大小"""
    dirs = [
        ("素材库", ASSETS_DIR),
        ("待审核图片", PENDING_DIR),
        ("缩略图", THUMBS_DIR),
        ("数据库", DATA_DIR),
        ("应用日志", LOGS_DIR),
        ("备份文件", None),   # 特殊处理
        ("Journal 日志", "/var/log/journal"),
        ("Nginx 日志", "/var/log/nginx"),
    ]
    result = []
    for label, path in dirs:
        if label == "备份文件":
            # 统计所有 .bak* 文件
            total = 0
            count = 0
            for root, _, files in os.walk(BASE_DIR):
                for f in files:
                    if ".bak" in f:
                        fp = os.path.join(root, f)
                        try:
                            total += os.path.getsize(fp)
                            count += 1
                        except OSError:
                            pass
            result.append({
                "label": label,
                "path": BASE_DIR + "/**/*.bak*",
                "size_mb": round(total / 1024 ** 2, 2),
                "file_count": count,
                "cleanable": True,
            })
            continue

        if not path or not os.path.exists(path):
            result.append({"label": label, "path": path or "", "size_mb": 0, "file_count": 0, "cleanable": False})
            continue

        total = 0
        count = 0
        for root, _, files in os.walk(path):
            for f in files:
                fp = os.path.join(root, f)
                try:
                    total += os.path.getsize(fp)
                    count += 1
                except OSError:
                    pass

        cleanable = label in ("待审核图片", "缩略图", "备份文件", "Journal 日志")
        result.append({
            "label": label,
            "path": path,
            "size_mb": round(total / 1024 ** 2, 2),
            "file_count": count,
            "cleanable": cleanable,
        })
    return result


def get_storage_summary() -> dict:
    """获取完整存储摘要"""
    disk = get_disk_usage()
    dirs = get_dir_sizes()

    # 待审核图片详情
    pending_detail = _get_pending_detail()

    # 备份文件详情
    backup_detail = _get_backup_detail()

    return {
        "disk": disk,
        "dirs": dirs,
        "pending_detail": pending_detail,
        "backup_detail": backup_detail,
        "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _get_pending_detail() -> dict:
    """获取待审核图片的详细信息"""
    if not os.path.exists(PENDING_DIR):
        return {"total": 0, "rejected": 0, "old_7d": 0, "old_30d": 0}

    now = time.time()
    total = rejected = old_7d = old_30d = 0
    for f in os.listdir(PENDING_DIR):
        fp = os.path.join(PENDING_DIR, f)
        if not os.path.isfile(fp):
            continue
        total += 1
        mtime = os.path.getmtime(fp)
        age_days = (now - mtime) / 86400
        if age_days > 7:
            old_7d += 1
        if age_days > 30:
            old_30d += 1
    return {"total": total, "old_7d": old_7d, "old_30d": old_30d}


def _get_backup_detail() -> list:
    """获取备份文件列表（按大小降序）"""
    backups = []
    for root, _, files in os.walk(BASE_DIR):
        for f in files:
            if ".bak" in f:
                fp = os.path.join(root, f)
                try:
                    size = os.path.getsize(fp)
                    mtime = os.path.getmtime(fp)
                    backups.append({
                        "path": fp,
                        "name": f,
                        "size_kb": round(size / 1024, 1),
                        "modified": datetime.fromtimestamp(mtime).strftime("%Y-%m-%d"),
                    })
                except OSError:
                    pass
    backups.sort(key=lambda x: x["size_kb"], reverse=True)
    return backups[:20]  # 最多返回 20 条


# ─── 清理操作 ─────────────────────────────────────────────────────────────────

def clean_pending_images(mode: str = "rejected", days: int = 30) -> dict:
    """
    清理待审核图片
    mode:
      - "rejected": 清理数据库中 status=rejected 的图片
      - "old": 清理超过 days 天的图片
      - "all": 清理所有待审核图片（谨慎使用）
    """
    from core.database import get_conn

    deleted_files = 0
    freed_bytes = 0
    errors = []

    conn = get_conn()
    try:
        if mode == "rejected":
            rows = conn.execute(
                "SELECT id, COALESCE(local_path, image_local_path) AS file_path FROM creative_pending WHERE status='rejected'"
            ).fetchall()
        elif mode == "old":
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            rows = conn.execute(
                "SELECT id, COALESCE(local_path, image_local_path) AS file_path FROM creative_pending WHERE created_at < ? AND status != 'approved'",
                (cutoff,)
            ).fetchall()
        elif mode == "all":
            rows = conn.execute(
                "SELECT id, COALESCE(local_path, image_local_path) AS file_path FROM creative_pending WHERE status != 'approved'"
            ).fetchall()
        else:
            return {"error": f"未知模式: {mode}"}

        ids_to_delete = []
        for row in rows:
            pid = row[0]
            local_path = row[1]
            ids_to_delete.append(pid)
            if local_path and os.path.exists(local_path):
                try:
                    size = os.path.getsize(local_path)
                    os.remove(local_path)
                    freed_bytes += size
                    deleted_files += 1
                except OSError as e:
                    errors.append(str(e))

        if ids_to_delete:
            placeholders = ",".join("?" * len(ids_to_delete))
            conn.execute(f"DELETE FROM creative_pending WHERE id IN ({placeholders})", ids_to_delete)
            conn.commit()

    finally:
        conn.close()

    # 清理 pending_review 目录中孤立文件（数据库中没有记录的）
    orphan_count, orphan_bytes = _clean_orphan_pending_files()
    deleted_files += orphan_count
    freed_bytes += orphan_bytes

    return {
        "deleted_records": len(ids_to_delete) if 'ids_to_delete' in dir() else 0,
        "deleted_files": deleted_files,
        "freed_mb": round(freed_bytes / 1024 ** 2, 2),
        "errors": errors,
        "orphan_cleaned": orphan_count,
    }


def _clean_orphan_pending_files() -> tuple:
    """清理 pending_review 目录中数据库没有记录的孤立文件"""
    if not os.path.exists(PENDING_DIR):
        return 0, 0

    from core.database import get_conn
    conn = get_conn()
    try:
        known_paths = set(
            r[0] for r in conn.execute(
                "SELECT COALESCE(local_path, image_local_path) FROM creative_pending WHERE COALESCE(local_path, image_local_path) IS NOT NULL"
            ).fetchall()
        )
    finally:
        conn.close()

    count = 0
    freed = 0
    for f in os.listdir(PENDING_DIR):
        fp = os.path.join(PENDING_DIR, f)
        if os.path.isfile(fp) and fp not in known_paths:
            try:
                freed += os.path.getsize(fp)
                os.remove(fp)
                count += 1
            except OSError:
                pass
    return count, freed


def clean_old_backups(keep_latest: int = 2) -> dict:
    """
    清理旧备份文件，每个原始文件只保留最新的 keep_latest 个备份
    """
    from collections import defaultdict

    # 按原始文件名分组
    groups = defaultdict(list)
    for root, _, files in os.walk(BASE_DIR):
        for f in files:
            if ".bak" not in f:
                continue
            fp = os.path.join(root, f)
            # 提取原始文件名（去掉 .bak_xxx 后缀）
            base = f.split(".bak")[0]
            groups[(root, base)].append(fp)

    deleted = 0
    freed_bytes = 0
    for (root, base), paths in groups.items():
        if len(paths) <= keep_latest:
            continue
        # 按修改时间排序，保留最新的
        paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        to_delete = paths[keep_latest:]
        for fp in to_delete:
            try:
                freed_bytes += os.path.getsize(fp)
                os.remove(fp)
                deleted += 1
            except OSError:
                pass

    return {
        "deleted_files": deleted,
        "freed_mb": round(freed_bytes / 1024 ** 2, 2),
    }


def clean_journal_logs(max_size_mb: int = 100) -> dict:
    """限制 systemd journal 日志大小"""
    try:
        result = subprocess.run(
            ["sudo", "journalctl", "--vacuum-size", f"{max_size_mb}M"],
            capture_output=True, text=True, timeout=30
        )
        return {
            "success": result.returncode == 0,
            "output": result.stdout.strip() or result.stderr.strip(),
        }
    except Exception as e:
        return {"success": False, "output": str(e)}


def clean_thumbs(days: int = 90) -> dict:
    """清理超过 days 天未访问的缩略图"""
    if not os.path.exists(THUMBS_DIR):
        return {"deleted_files": 0, "freed_mb": 0}

    now = time.time()
    deleted = 0
    freed = 0
    for f in os.listdir(THUMBS_DIR):
        fp = os.path.join(THUMBS_DIR, f)
        if not os.path.isfile(fp):
            continue
        age_days = (now - os.path.getmtime(fp)) / 86400
        if age_days > days:
            try:
                freed += os.path.getsize(fp)
                os.remove(fp)
                deleted += 1
            except OSError:
                pass
    return {"deleted_files": deleted, "freed_mb": round(freed / 1024 ** 2, 2)}


# ─── 自动清理任务（由 scheduler 调用）────────────────────────────────────────

def run_auto_cleanup():
    """
    定期自动清理任务：
    - 清理已拒绝的待审核图片
    - 清理超过 30 天的待审核图片
    - 清理超过 90 天的缩略图
    - 磁盘使用率 > 80% 时发送 TG 告警
    """
    logger.info("[StorageCleanup] 开始自动存储清理...")

    # 1. 清理已拒绝的待审核图片
    r1 = clean_pending_images(mode="rejected")
    if r1.get("deleted_files", 0) > 0:
        logger.info(f"[StorageCleanup] 清理已拒绝图片: {r1['deleted_files']} 个文件, 释放 {r1['freed_mb']}MB")

    # 2. 清理超过 30 天的待审核图片
    r2 = clean_pending_images(mode="old", days=30)
    if r2.get("deleted_files", 0) > 0:
        logger.info(f"[StorageCleanup] 清理30天旧图片: {r2['deleted_files']} 个文件, 释放 {r2['freed_mb']}MB")

    # 3. 清理超过 90 天的缩略图
    r3 = clean_thumbs(days=90)
    if r3.get("deleted_files", 0) > 0:
        logger.info(f"[StorageCleanup] 清理旧缩略图: {r3['deleted_files']} 个文件, 释放 {r3['freed_mb']}MB")

    # 4. 磁盘告警
    disk = get_disk_usage()
    if disk["status"] in ("warning", "danger"):
        _send_disk_alert(disk)

    logger.info(f"[StorageCleanup] 自动清理完成，当前磁盘使用率: {disk['used_pct']}%")


def _send_disk_alert(disk: dict):
    """发送磁盘空间告警到 Telegram"""
    try:
        from core.database import get_conn
        import requests as req
        conn = get_conn()
        tg_token = conn.execute("SELECT value FROM settings WHERE key='tg_bot_token'").fetchone()
        tg_chats = conn.execute("SELECT value FROM settings WHERE key='tg_chat_ids'").fetchone()
        conn.close()

        if not tg_token or not tg_chats or not tg_token[0] or not tg_chats[0]:
            return

        emoji = "🔴" if disk["status"] == "danger" else "🟡"
        msg = (
            f"{emoji} <b>Mira 磁盘空间告警</b>\n"
            f"使用率：<b>{disk['used_pct']}%</b>\n"
            f"已用：{disk['used_gb']}GB / 总量：{disk['total_gb']}GB\n"
            f"剩余：{disk['free_gb']}GB\n"
            f"{'⚠️ 请及时清理磁盘空间！' if disk['status'] == 'danger' else '建议关注磁盘使用情况。'}"
        )
        for cid in tg_chats[0].split(","):
            cid = cid.strip()
            if cid:
                req.post(
                    f"https://api.telegram.org/bot{tg_token[0]}/sendMessage",
                    json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
                    timeout=10
                )
    except Exception as e:
        logger.warning(f"[StorageCleanup] 磁盘告警发送失败: {e}")
