#!/bin/bash
set -e

# namenode 최초 포맷 체크
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "Formatting namenode for the first time..."
    hdfs namenode -format -force
else
    echo "Namenode already formatted, skipping format."
fi

# namenode 기동
hadoop-daemon.sh start namenode

# resourcemanager 기동
yarn-daemon.sh start resourcemanager

# job history server 기동
mr-jobhistory-daemon.sh start historyserver

# foreground 유지
tail -f /dev/null