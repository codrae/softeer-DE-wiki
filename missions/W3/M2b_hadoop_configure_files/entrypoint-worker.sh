#!/bin/bash
set -e

# 1. datanode 기동
#    힌트: 데몬 이름이 정확히 뭐였지? (namenode/datanode/secondarynamenode 중)
hadoop-daemon.sh start datanode

# 2. nodemanager 기동
#    힌트: yarn-daemon.sh의 대상 데몬 이름
yarn-daemon.sh start nodemanager

# 3. foreground 유지
tail -f /dev/null