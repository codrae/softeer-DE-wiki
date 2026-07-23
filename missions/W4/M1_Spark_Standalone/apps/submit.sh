#!/bin/bash
# 사용법: ./submit.sh [파티션 수]
# master 컨테이너 안에서 실행하는 걸 전제로 함 (client 모드)
#   예: docker exec -it spark-master ./submit.sh 4

set -e  # 중간에 에러나면 즉시 중단 (에러 무시하고 넘어가지 않도록)

PARTITIONS=${1:-4}

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 4 \
  /opt/spark-apps/pi.py "${PARTITIONS}"