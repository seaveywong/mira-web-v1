#!/bin/bash
# 每次更新前调用此脚本备份当前版本
# 用法: bash /opt/mira/backup_before_update.sh [版本号]
VERSION=${1:-"unknown"}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/opt/mira/backups"
mkdir -p "$BACKUP_DIR"

# 备份前端
cp /opt/mira/frontend/index.html "$BACKUP_DIR/index.html.bak_${VERSION}_${TIMESTAMP}"

# 备份后端关键文件
cp /opt/mira/backend/autopilot.py "$BACKUP_DIR/autopilot.py.bak_${VERSION}_${TIMESTAMP}" 2>/dev/null || true
cp /opt/mira/backend/accounts.py "$BACKUP_DIR/accounts.py.bak_${VERSION}_${TIMESTAMP}" 2>/dev/null || true
cp /opt/mira/backend/settings.py "$BACKUP_DIR/settings.py.bak_${VERSION}_${TIMESTAMP}" 2>/dev/null || true

# 只保留最近5个备份（按时间排序，删除最旧的）
ls -t "$BACKUP_DIR"/index.html.bak_* 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true
ls -t "$BACKUP_DIR"/autopilot.py.bak_* 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true

echo "✅ 备份完成: $BACKUP_DIR/*_${VERSION}_${TIMESTAMP}"
