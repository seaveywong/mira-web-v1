"""
storage.py  —  存储空间管理 API
路由前缀: /api/storage
功能:
  GET  /summary          — 获取存储使用摘要（磁盘/各目录/待审核/备份）
  POST /clean/pending    — 清理待审核图片（mode: rejected/old/all, days: int）
  POST /clean/backups    — 清理旧备份文件（keep_latest: int）
  POST /clean/journal    — 清理 systemd journal 日志（max_size_mb: int）
  POST /clean/thumbs     — 清理旧缩略图（days: int）
  POST /clean/all        — 一键全部清理（安全模式）
"""
import logging
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional
from core.auth import get_current_user

router = APIRouter()
logger = logging.getLogger("mira")


class CleanPendingReq(BaseModel):
    mode: str = "rejected"       # rejected / old / all
    days: int = 30


class CleanBackupsReq(BaseModel):
    keep_latest: int = 2         # 每个文件保留最新的几个备份


class CleanJournalReq(BaseModel):
    max_size_mb: int = 100       # journal 最大保留大小（MB）


class CleanThumbsReq(BaseModel):
    days: int = 90               # 清理超过多少天的缩略图


@router.get("/summary")
def get_storage_summary(user=Depends(get_current_user)):
    """获取完整存储使用摘要"""
    try:
        from services.storage_manager import get_storage_summary
        return get_storage_summary()
    except Exception as e:
        logger.error(f"[Storage] 获取存储摘要失败: {e}", exc_info=True)
        return {"error": str(e)}


@router.post("/clean/pending")
def clean_pending(req: CleanPendingReq, user=Depends(get_current_user)):
    """清理待审核图片"""
    try:
        from services.storage_manager import clean_pending_images
        result = clean_pending_images(mode=req.mode, days=req.days)
        logger.info(f"[Storage] 手动清理待审核图片: mode={req.mode}, 结果={result}")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[Storage] 清理待审核图片失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/clean/backups")
def clean_backups(req: CleanBackupsReq, user=Depends(get_current_user)):
    """清理旧备份文件"""
    try:
        from services.storage_manager import clean_old_backups
        result = clean_old_backups(keep_latest=req.keep_latest)
        logger.info(f"[Storage] 手动清理备份文件: 结果={result}")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[Storage] 清理备份文件失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/clean/journal")
def clean_journal(req: CleanJournalReq, user=Depends(get_current_user)):
    """清理 systemd journal 日志"""
    try:
        from services.storage_manager import clean_journal_logs
        result = clean_journal_logs(max_size_mb=req.max_size_mb)
        logger.info(f"[Storage] 手动清理 journal 日志: 结果={result}")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[Storage] 清理 journal 日志失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/clean/thumbs")
def clean_thumbs(req: CleanThumbsReq, user=Depends(get_current_user)):
    """清理旧缩略图"""
    try:
        from services.storage_manager import clean_thumbs
        result = clean_thumbs(days=req.days)
        logger.info(f"[Storage] 手动清理缩略图: 结果={result}")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[Storage] 清理缩略图失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/clean/all")
def clean_all(user=Depends(get_current_user)):
    """
    一键全部清理（安全模式）：
    - 清理已拒绝的待审核图片
    - 清理超过 30 天的待审核图片
    - 清理旧备份（每个文件保留最新 2 个）
    - 清理超过 90 天的缩略图
    - 限制 journal 日志到 100MB
    """
    try:
        from services.storage_manager import (
            clean_pending_images, clean_old_backups,
            clean_journal_logs, clean_thumbs
        )
        results = {}

        r1 = clean_pending_images(mode="rejected")
        results["pending_rejected"] = r1

        r2 = clean_pending_images(mode="old", days=30)
        results["pending_old"] = r2

        r3 = clean_old_backups(keep_latest=2)
        results["backups"] = r3

        r4 = clean_thumbs(days=90)
        results["thumbs"] = r4

        r5 = clean_journal_logs(max_size_mb=100)
        results["journal"] = r5

        total_freed = sum([
            r1.get("freed_mb", 0),
            r2.get("freed_mb", 0),
            r3.get("freed_mb", 0),
            r4.get("freed_mb", 0),
        ])
        logger.info(f"[Storage] 一键清理完成，共释放 {total_freed:.2f}MB")
        return {
            "success": True,
            "message": f"已清理过期文件，释放 {round(total_freed, 2):.2f}MB",
            "total_freed_mb": round(total_freed, 2),
            "details": results,
        }
    except Exception as e:
        logger.error(f"[Storage] 一键清理失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
