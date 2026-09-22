# W4M1 - Building Apache Spark Standalone Cluster on Docker

Docker Compose로 Spark standalone 클러스터(master 1 + worker 2)를 구성하고,
Monte Carlo 방식의 π 추정 잡을 `spark-submit`으로 제출하는 미션입니다.

## 클러스터 구성

| 서비스 | 역할 | 포트 |
|---|---|---|
| `spark-master` | 클러스터 자원 관리자 | 8080 (Web UI), 7077 (Master URI) |
| `spark-worker-1` | executor 제공 (2 core / 2g) | 8081 (Web UI) |
| `spark-worker-2` | executor 제공 (2 core / 2g) | 8082 (Web UI) |

- master는 `start-master.sh`로 기동하고, 각 worker는 `start-worker.sh spark://spark-master:7077`로
  master에 접속해 "자원이 있다"고 등록한다.
- 세 컨테이너 모두 `./apps`(애플리케이션)와 `../../data`(입출력 데이터)를 볼륨으로 공유한다.
- 네트워크는 `spark-net` bridge.

## 디렉토리 구조

```
.
├── Dockerfile
├── docker-compose.yml
└── apps/
    ├── pi.py        # Monte Carlo π 추정 잡
    └── submit.sh    # spark-submit 래퍼
```

## 실행

```bash
docker compose up -d --build

# 클러스터 상태 확인: http://localhost:8080 에서 worker 2대 등록 확인

# 잡 제출
docker exec -it spark-master bash /opt/spark-apps/submit.sh
```

## `apps/pi.py`

`$SPARK_HOME/examples/src/main/python/pi.py`를 기반으로 확장했습니다.

- 단위 정사각형에 무작위 점을 찍어 반지름 1인 사분원 안에 들어가는 비율로 π를 추정
- `sc.parallelize(range(1, n+1), partitions)`로 시도를 파티션 단위로 분배 → `map` → `reduce`
- 파티션 수 기본값 4 (worker 2대 × 2코어에 맞춤), 파티션당 100,000회 시도
- 원본과 달리 **결과를 DataFrame으로 만들어 `/opt/spark-data/output/pi_result`에 parquet로 저장**

```
[RESULT] Pi is roughly 3.14...
[RESULT] Saved to /opt/spark-data/output/pi_result
```

## 학습 포인트

- master/worker의 역할 분리와 등록 방식 (worker가 master URI로 접속)
- 드라이버가 클러스터와 같은 Docker 네트워크에 있으면 7077을 호스트에 노출할 필요가 없다
- 파티션 수 = 병렬 처리 단위. worker 코어 총합에 맞추는 것이 기본
