# NYC TLC Trip Data 분석 — Spark DataFrame + DAG 최적화 설계

## 목표
NYC Taxi and Limousine Commission(TLC) Trip Record Data를 PySpark **DataFrame API**로 적재/정제/변환하여,
Spark 내부 동작(DAG, lazy evaluation, stage 최적화)을 이해하고 시연한다. `missions/W5/M1`의 RDD 기반
파이프라인과 동일한 데이터셋을 사용하되, DataFrame API와 Catalyst 최적화를 활용한다는 점이 차이다.

## 범위
- 데이터: `missions/W5/M1/data/yellow_tripdata_2026-05.parquet` (기존 파일 재사용, 재다운로드 없음)
- 보조 데이터: TLC 공식 Taxi Zone Lookup CSV (`https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`) —
  PULocationID/DOLocationID를 실제 지역명(Borough/Zone)으로 매핑하기 위한 join 대상
- 실행 환경: 상위 디렉토리(`softeer-DE-wiki/.venv`)의 기존 가상환경을 그대로 사용. Docker 클러스터 없이
  `local[4]` 마스터로 로컬 실행 (W5/M1과 동일한 방식)
- pyspark는 해당 venv에 아직 설치되어 있지 않으므로 설치 필요 (Python 3.14 환경에서 `pyspark==4.2.0`이
  빌드/설치 가능함을 사전 확인함)

## 아키텍처

```
missions/W5/M2/
├── main.py                                   # 파이프라인 전체를 담은 단일 실행 스크립트 (W5/M1 스타일)
├── data/
│   ├── yellow_tripdata_2026-05.parquet       # M1 파일 복사본
│   ├── taxi_zone_lookup.csv                  # TLC 공식 zone lookup (다운로드, 소용량)
│   └── output/                               # 최종 결과 (parquet + csv, 3종)
├── report_assets/
│   └── spark_ui_dag.png                      # Spark UI에서 캡처한 DAG 스크린샷
└── REPORT.md                                 # 학습 목표 대비 결과 리포트
```

원본/출력 데이터(`data/`)와 스크린샷(`report_assets/`)은 리포트 첨부 목적의 스크린샷 파일 하나를 제외하면
용량이 크므로 `.gitignore` 처리하고, 최종 리포트에 스크린샷만 커밋한다.

## 파이프라인 (main.py)

### 1. 환경설정
- `SparkSession.builder.appName("NYCTaxiDataFrameAnalysis").master("local[4]").getOrCreate()`
- Spark 버전, 파티션 수 등 기본 정보 로그 출력

### 2. 데이터 로딩
- `spark.read.parquet(...)`로 트립 데이터 로드 (parquet은 자체 스키마를 포함하므로 별도 스키마 정의 불필요)
- `spark.read.csv(header=True, inferSchema=True)`로 taxi zone lookup 로드 (컬럼: LocationID, Borough, Zone, service_zone)
- 로드 직후에는 액션을 호출하지 않고 `df.printSchema()`만 사용 (스키마 확인은 메타데이터 조회로 잡을 트리거하지 않음)

### 3. 클리닝
- 필수 컬럼(tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, fare_amount,
  PULocationID, DOLocationID) null 행 제거
- 파생 컬럼 생성: `trip_duration_min = (dropoff - pickup)` 분 단위, `pickup_date`, `pickup_hour`
- 비정상 값 필터링: `0 < trip_duration_min <= 180`, `0 < trip_distance <= 100`,
  `1 <= passenger_count <= 6`, `fare_amount >= 0`
- 클리닝 전/후 row count를 `count()` 액션으로 로그 출력 (드롭 비율 가시화). 이 두 번의 `count()`가
  본 파이프라인에서 처음 등장하는 액션이며, 동시에 lazy evaluation을 보여주는 지점이기도 하다
  (클리닝 코드 자체는 `count()` 호출 전까지 실행되지 않음)

### 4. 캐싱 (최적화 포인트 ①)
- 클리닝이 끝난 DataFrame을 `cache()` — 이후 여러 갈래(다인승 필터, 날짜별 집계, 시간대별 집계, zone join)에서
  반복 스캔되므로, 캐싱으로 parquet 재파싱/재필터링 비용을 제거한다
- 캐싱 직후 count() 한 번으로 캐시를 materialize

### 5. 변환 (최소 3종)
- **Filtering**: `passenger_count > 1`인 다인승 트립만 추출 → `multi_passenger_df`
- **Aggregation**:
  - 날짜별(`pickup_date`) `groupBy` → 트립 수, 평균 거리, 총 매출(`sum(fare_amount)`) → `daily_summary`
  - 시간대별(`pickup_hour`) `groupBy` → 트립 수 → `hourly_counts` (피크아워 파악용)
- **Join**: zone lookup은 소용량이므로 `broadcast()` 힌트를 사용해 PULocationID = LocationID로 조인,
  pickup borough를 부여한 뒤 borough별 `groupBy` 집계(트립 수, 평균 요금) → `borough_summary`.
  `broadcast join`을 사용하는 이유는 셔플(sort-merge join) 스테이지를 회피하기 위함이며, 이것이
  최적화 포인트 ②로 리포트에서 설명한다

### 6. Lazy Evaluation 시연
- 4~5단계의 변환 코드를 작성한 직후, 어떤 액션도 호출하지 않은 상태에서 로그로
  "다음 변환들은 아직 실행되지 않았음"을 명시하고 `daily_summary.explain()`으로 논리/물리 실행계획만 출력
  (explain은 액션이 아니므로 잡이 발생하지 않음)
- 이후 실제 액션(`collect`, `write`) 호출 직전/직후에 `time.time()` 타임스탬프를 로그로 남겨,
  "정의 시점"과 "실행 시점"이 분리되어 있음을 대비시켜 보여준다

### 7. 액션 (최소 2가지)
- **Collect**: `multi_passenger_df.limit(20).collect()` — 다인승 트립 샘플을 드라이버로 수집해 출력
- **Write**: `daily_summary`, `hourly_counts`, `borough_summary` 3개 결과를 각각
  `data/output/<name>` 경로에 parquet(`mode("overwrite")`)과 사람이 읽기 쉬운 csv(`header=True`)로 저장

### 8. Spark UI 확인
- 모든 액션 실행 후 `input("Enter를 누르면 종료합니다 (그 전에 http://localhost:4040 에서 DAG 확인)...")`로
  프로세스를 일시 정지 (W5/M1과 동일 패턴)
- 이 시점에 Chrome 브라우저 도구로 `localhost:4040`의 Jobs 페이지에 진입해, 셔플 스테이지가 없는 join
  잡(또는 캐시 재사용이 드러나는 잡)의 DAG 시각화를 스크린샷으로 캡처하여 `report_assets/spark_ui_dag.png`로 저장
- 스크린샷 캡처 완료 후 Enter를 눌러 스크립트 종료 (`spark.stop()`)

## 액션과 DAG 구조 정리 (리포트에 포함할 핵심 설명)
- 클리닝 단계의 count() 액션 2회 → 클리닝 전/후 각각 1개 잡
- 캐시 materialize용 count() 1회 → 캐시 잡
- `collect()` 1회 → 다인승 샘플 잡
- `write` 3회 → daily_summary/hourly_counts/borough_summary 각각 1개 잡 (총 3개)
- 캐싱 덕분에 daily_summary/hourly_counts/borough_summary 잡들은 클리닝된 parquet을 매번 다시 읽지 않고
  캐시된 데이터를 재사용 → 스테이지 수 감소
- borough_summary 잡은 broadcast join이므로 shuffle exchange 스테이지가 없음 (sort-merge join이었다면
  양쪽 모두 셔플 스테이지가 필요했을 것)

## 리포트 (REPORT.md) 구성
1. 미션 개요 및 실행 환경 (venv, local[4], pyspark 버전)
2. 파이프라인 단계별 설명 (로딩 → 클리닝 → 캐싱 → 변환 → 액션), 클리닝 전/후 row count
3. Lazy Evaluation 시연 결과 (로그 발췌: 정의 시점 vs 실행 시점 타임스탬프, explain() 출력)
4. DAG 설명 + Spark UI 스크린샷 (`report_assets/spark_ui_dag.png`), 스테이지 수와 그 이유
5. 최적화 설명: cache/persist를 사용한 이유와 효과, broadcast join으로 셔플 스테이지를 없앤 이유
6. 최종 분석 결과 요약 (피크아워, 날짜별 요약, borough별 통계)

## 에러 처리
- Zone lookup CSV 다운로드 실패 시 예외를 던지고 어떤 URL이 실패했는지 명시 (이미 파일이 존재하면 재다운로드 스킵)
- 클리닝 단계: 필터링 전/후 row count 비교 로그로 이상치 제거 비율 가시화 (silent drop 방지)

## 테스트/검증 계획
- 스크립트 전체 실행 후 각 로그 출력값(row count, 피크아워, borough별 통계)이 NYC 옐로우 택시 일반적인
  수치 범위에 있는지 확인 (평균 이동시간 10~20분, 평균 거리 2~3마일 등)
- `data/output/` 아래 3개 결과 디렉토리(parquet+csv)가 정상 생성되는지 확인
- `localhost:4040` Jobs 페이지에서 예상한 잡/스테이지 구조(캐시 재사용, broadcast join으로 인한 셔플 스테이지
  부재)가 실제로 나타나는지 스크린샷으로 확인
- `REPORT.md`에 스크린샷과 설명이 모두 포함되어 있는지 확인
