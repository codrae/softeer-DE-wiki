# NYC TLC Trip Data 분석 — Spark + Jupyter 설계

## 목표
NYC Taxi and Limousine Commission(TLC) Trip Record Data를 PySpark로 적재/정제하고,
평균 이동시간·거리, 피크아워, 날씨 상관관계를 분석한다. 전체 파이프라인은 Jupyter Notebook에서
단계별 셀로 실행되며, 드라이버는 Spark standalone 클러스터와 같은 Docker 네트워크 안에서 동작한다.

## 범위
- 데이터: 최근 1~2개월치 Yellow Taxi Trip Records (TLC 공식 parquet)
- 날씨: Open-Meteo Historical Weather API — 동일 기간 NYC 좌표의 hourly 기온/강수량
- 실행 환경: 로컬 Docker Compose 기반 Spark standalone 클러스터 (master 1 + worker 2)

## 아키텍처

```
missions/W4/M2_Analysis_Using_Spark/
├── Dockerfile              # Spark 3.5.9 + Python 3.10 + JDK17 베이스에 pandas/pyarrow/requests/
│                           #   scipy/matplotlib/jupyterlab 추가 설치
├── docker-compose.yml
│   ├── spark-master
│   ├── spark-worker-1/2    # 2 core / 2g
│   └── jupyter             # 같은 spark-net에 조인하는 driver 컨테이너.
│                           #   client 모드로 spark://spark-master:7077 에 직접 접속.
│                           #   JupyterLab 8888 포트 노출, ./apps, ./notebooks, ../../data 마운트
├── apps/
│   └── download_data.py    # TLC parquet + Open-Meteo 날씨 다운로드 (노트북 1번 셀에서 호출)
└── notebooks/
    └── analysis.ipynb      # 7단계 셀 구성 (아래 파이프라인 섹션 참조)
```

드라이버(Jupyter)가 같은 Docker 네트워크 안에서 `spark://spark-master:7077`로 접속하므로 7077 포트는
호스트에 노출할 필요가 없다. 호스트에는 master UI `8080:8080`, worker-1 UI `8081:8081`,
worker-2 UI `8082:8082`, JupyterLab `8888:8888`만 매핑한다.

데이터 저장 위치:
- `missions/data/raw/tlc/` — 원본 TLC parquet (연-월별 파일)
- `missions/data/raw/weather/` — Open-Meteo 응답 CSV
- `missions/data/output/` — 최종 분석 결과 (parquet + csv)

원본/출력 데이터는 git에 커밋하지 않고 `.gitignore` 처리한다.

## 파이프라인 (notebooks/analysis.ipynb — 셀 단위)

### 1. 환경설정
- `SparkSession.builder.master("spark://spark-master:7077").appName("NYCTaxiAnalysis").getOrCreate()`
- 클러스터 연결 확인 (executor 수, Spark 버전 출력)

### 2. 데이터 적재
- `download_data.py`의 함수를 호출해 대상 연-월의 TLC parquet과 Open-Meteo hourly 날씨 CSV를 확인/다운로드
  (이미 존재하면 스킵)
- `spark.read.parquet(...)`로 여러 월 파일을 한 번에 로드 (glob 패턴으로 다개월 지원)
- 날씨 CSV는 `spark.read.csv(header=True, inferSchema=True)`로 로드

### 3. 클리닝
- 컬럼 정규화: `tpep_pickup_datetime`/`tpep_dropoff_datetime`을 표준 timestamp로 캐스팅
- 필수 컬럼(pickup/dropoff datetime, trip_distance, fare_amount) null 행 제거
- `trip_duration_min = (dropoff - pickup)`(분) 파생 컬럼 생성
- 비정상 값 필터링: `0 < trip_duration_min <= 180`, `0 < trip_distance <= 100`, `fare_amount >= 0`
- 클리닝 전/후 row count를 로그로 출력 (드롭된 비율 확인용)

### 4. 지표 계산
- 평균 이동시간(분), 평균 이동거리(마일)를 집계
- 결과를 pandas DataFrame으로 변환해 사람이 읽기 좋은 표로 출력

### 5. 피크아워 분석
- `pickup_hour = hour(tpep_pickup_datetime)` 파생 후 `groupBy(pickup_hour).count()`
- 결과를 pandas로 수집해 시간대별 막대그래프(matplotlib)로 시각화
- 상위 N개(예: 3개) 피크아워를 표/텍스트로 하이라이트

### 6. 날씨 상관분석
- 트립 데이터를 `date + hour` 단위로 집계(시간당 트립 수)한 뒤, 같은 키로 날씨 테이블과 join
- 피어슨 상관계수 계산: 시간당 트립 수 vs 기온, 시간당 트립 수 vs 강수량 (Spark `corr` 또는 join 결과를 pandas로 모아 `scipy.stats.pearsonr`)
- 통계적 검증: 강수 유무(강수량 > 0 기준)로 두 그룹을 나눠 시간당 트립 수 평균 차이를 `scipy.stats.ttest_ind`로 검정, p-value 보고
- 산점도(기온/강수량 vs 트립 수)로 시각화

### 7. 시각화/출력
- 위 단계에서 생성한 결과 테이블 4종을 최종 저장:
  - `summary_metrics` (평균 이동시간/거리)
  - `hourly_trip_counts` (피크아워 분석)
  - `hourly_weather_join` (시간당 트립 수 + 날씨 조인 테이블, 원본 조인 결과 그대로)
  - `weather_condition_stats` (피어슨 상관계수 + t-test 결과를 하나의 스칼라 테이블로 통합 저장 — 둘 다 행 단위가 아닌 요약 통계이므로 조인 테이블에 붙이지 않고 여기로 모음)
- 각각 parquet(`/opt/spark-data/output/<name>`)과 사람이 읽기 쉬운 csv로 저장
- 노트북 마지막 셀에서 전체 결과 요약(표 + 그래프 3종: 피크아워 막대그래프, 날씨-트립 산점도, 강수유무 비교 막대그래프)을 한 번에 정리

## 에러 처리
- 다운로드 스크립트: HTTP 상태코드 확인, 실패 시 예외를 던지고 어떤 파일이 실패했는지 명시
- 클리닝 단계: 필터링 전/후 row count 비교 로그로 이상치 제거 비율 가시화 (silent drop 방지)

## 테스트/검증 계획
- 1개월 데이터로 먼저 파이프라인 전체를 실행해 각 셀 결과값이 상식적인 범위인지 확인
  (평균 이동시간 10~20분, 평균 거리 2~3마일 등 NYC 옐로우 택시 일반적 수치와 비교)
- `docker compose up`으로 클러스터 기동 후 Jupyter 접속(`localhost:8888`)해 셀을 순서대로 실행하고
  각 단계 출력(표/그래프)이 정상 렌더링되는지 확인
- 최종 output 디렉토리에 4개 결과 파일(parquet+csv)이 생성되는지 확인
