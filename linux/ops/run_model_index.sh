#!/bin/bash
# 增量建模型索引。cron 每 15 分钟一次；flock 防止与上一轮重叠。
#
# 原本是每天凌晨跑一次，因为当时数据库在慢卷上、一次全量要半小时并且会拖住采集。
# 数据库迁到快卷后增量一次约 0.25 秒，所以改成高频，让控制台的模型统计接近实时。
LOG=/opt/model-index-cron.log
MAX_LINES=5000

exec 9>/var/lock/qq-model-index.lock
flock -n 9 || { echo "$(date "+%F %T") 上一轮仍在运行，跳过"; exit 0; }

echo "===== $(date "+%F %T") 开始 ====="
python3 -u /opt/build_model_index.py
echo "===== $(date "+%F %T") 结束 ====="

# 每 15 分钟一次的日志会无界增长；只保留最近的部分。
if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt "$MAX_LINES" ]; then
  tail -n "$MAX_LINES" "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi
