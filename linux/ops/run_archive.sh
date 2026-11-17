#!/bin/bash
# 每日归档：传新图到 R2，再删掉本地已确认存档的老图。
#
# 上传窗口有意留得比"一天的量"宽很多。日增约 1.5 GB，图库卷单线程只有 3 MB/s、
# 八线程约 10 MB/s，所以一天的量正常十几分钟就传完；余下的时间是给积压和补漏
# 用的——断线补漏会往过去的日期里补图，那天已经传过也会重新出现待传项。
#
# --max-seconds 让这个任务可以被安全打断：传到哪算哪，状态在
# /var/lib/qqai-state/archive_state.sqlite3 里，下一轮接着传。
LOG=/opt/qqai-archive.log
MAX_LINES=20000
BUDGET=${ARCHIVE_MAX_SECONDS:-14400}

exec 9>/var/lock/qqai-archive.lock
flock -n 9 || { echo "$(date "+%F %T") 上一轮仍在运行，跳过"; exit 0; }

echo "===== $(date "+%F %T") 开始 ====="
python3 -u /opt/qq-ai-image-collector-linux-event-a1cfe1a/linux/ops/archive_to_r2.py \
  --upload --purge --index --max-seconds "$BUDGET"
status=$?
echo "===== $(date "+%F %T") 结束 (exit $status) ====="

if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt "$MAX_LINES" ]; then
  tail -n "$MAX_LINES" "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi
exit $status
