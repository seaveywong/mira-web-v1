#!/bin/bash
# Mira 数据库自动备份脚本
# 每天凌晨4点执行，保留最近7份备份

DB_PATH="/opt/mira/data/mira.db"
BACKUP_DIR="/opt/mira/data/backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/mira_${DATE}.db"

mkdir -p "${BACKUP_DIR}"

# 使用 SQLite .backup 命令进行热备份（不锁库）
sqlite3 "${DB_PATH}" ".backup '${BACKUP_FILE}'"

if [ $? -eq 0 ]; then
    echo "[$(date)] 数据库备份成功: ${BACKUP_FILE}"
    # 压缩备份文件
    gzip "${BACKUP_FILE}"
    # 删除7天前的旧备份
    find "${BACKUP_DIR}" -name "mira_*.db.gz" -mtime +7 -delete
    echo "[$(date)] 已清理7天前的旧备份"
else
    echo "[$(date)] 数据库备份失败！" >&2
fi
