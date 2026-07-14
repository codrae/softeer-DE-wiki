#!/bin/bash
set -e

# 1. ???  (SSH 관련 줄 — 이제 원격 접속이 필요 없어졌는데, 이 줄을 완전히 삭제할지 남겨둘지 판단해봐)
service ssh start

# 2. namenode 최초 포맷 체크
#    힌트: W3M1과 동일한 로직 — 어떤 디렉토리 존재 여부로 판단했었지?
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "Formatting namenode for the first time..."
    hdfs namenode -format -force
else
    echo "Namenode already formatted, skipping format."
fi


# 3. namenode 기동
#    힌트: start-dfs.sh 대신 뭘 써야 하는지 이미 정했지 (hadoop-daemon.sh)
hadoop-daemon.sh start namenode

# 4. resourcemanager 기동
#    힌트: yarn-daemon.sh 사용, 데몬 이름이 뭐였는지 확인 (yarn.resourcemanager?)
yarn-daemon.sh start resourcemanager

# 5. job history server 기동
#    힌트: 이건 원래도 daemon 스크립트 이름 자체가 mr-jobhistory-daemon.sh였어 (start-dfs.sh 계열이 아니었음) — 그대로 재사용 가능한지 확인해봐
mr-jobhistory-daemon.sh start historyserver

# 6. foreground 유지
tail -f /dev/null