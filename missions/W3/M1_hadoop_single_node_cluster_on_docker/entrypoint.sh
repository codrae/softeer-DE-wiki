#!/bin/bash
set -e

# 1. SSH 서버 시작 (백그라운드 데몬으로)
service ssh start

# 2. namenode 최초 포맷 여부 확인 후, 안 되어있으면 포맷
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "Formatting namenode for the first time..."
    hdfs namenode -format -force
else
    echo "Namenode already formatted, skipping format."
fi

# 3. HDFS 데몬 시작 (namenode, datanode, secondary namenode)
start-dfs.sh

# 4. YARN 데몬 시작 (resource manager, node manager)
start-yarn.sh

mr-jobhistory-daemon.sh start historyserver

# 5. 컨테이너가 종료되지 않도록 foreground 프로세스 유지
tail -f /dev/null