#!/bin/bash
# 每日增量建模型索引。flock 防止与上一轮重叠。
exec 9>/var/lock/qq-model-index.lock
flock -n 9 || { echo "$(date "+%F %T") 上一轮仍在运行，跳过"; exit 0; }
echo "===== $(date "+%F %T") 开始 ====="
python3 -u /opt/build_model_index.py
echo "===== $(date "+%F %T") 结束 ====="
