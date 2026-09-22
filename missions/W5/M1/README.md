# W5M1 - NYC TLC 분석 (Spark RDD API)

NYC TLC Trip Record Data를 **RDD API만으로** 처리해 Spark 내부 동작(RDD, DAG, lazy evaluation)을
이해하는 미션입니다.

## 미션 요구사항

### 학습 목표
Spark가 내부적으로 어떻게 동작하는지 — RDD, DAG, lazy evaluation 개념을 포함해 — 이해하기 위해
NYC TLC Trip Record Data를 분석하는 Spark 애플리케이션을 개발한다.

### 기능 요구사항

| 항목 | 요구 내용 |
|---|---|
| Data Loading | 지정한 경로에서 데이터셋을 읽고, 사용자가 지정한 포맷(CSV, Parquet)을 처리 |
| Data Cleaning | 결측·비정상 데이터가 있는 행을 식별해 제거 |
| Transformation | 요금이 0 이하인 트립 제외, 필요한 컬럼 추출 및 타입 변환, reduce로 총매출·총 트립수 계산, 날짜별 그룹핑 |
| Aggregation | 총 트립 수 / 총 매출 / 평균 이동거리 / 일별 트립 수 / 일별 총 매출 |
| Optimization | 적절한 Spark 설정과 내장 기능으로 실행 시간 단축 |
| Result Storage | 결과를 지정한 저장소에 사용자 친화적 포맷(CSV/Parquet)으로 저장 |

### 프로그래밍 요구사항
- **RDD API**로 transformation과 action을 작성할 것 (DataFrame API 아님)
- 최소 **5가지 transformation** 수행 (filtering, mapping, reducing, joining, aggregating 등)
- Spark UI의 **DAG 시각화 스크린샷**을 리포트에 포함

## 구현 (`main.py`)

함수 단위로 분리되어 있습니다.

| 함수 | 역할 |
|---|---|
| `load_data` | `.parquet`이면 `spark.read.parquet` 후 `.rdd`, `.csv`면 `textFile` + 헤더 제거 + `Row` 매핑 |
| `clean_and_transform` | null/0 이하 요금·거리 제거 → `(date, fare, distance)` 매핑 → 날짜 범위 필터 → `cache()` |
| `compute_summary` | `map` + `reduce` **한 번으로** 총 트립수·총 매출·거리 합을 동시에 계산 |
| `compute_daily` | `reduceByKey` **한 번으로** 일별 트립수와 일별 매출을 동시에 계산 후 `mapValues`로 분리 |
| `overwrite_dir` | 출력 경로가 있으면 지워 재실행 가능하게 함 |

**최적화 포인트**
- 총계 3종을 각각 `count()`/`sum()` 하지 않고 `(1, fare, distance)` 튜플을 만들어
  `reduce` 한 번에 처리
- 일별 트립수와 일별 매출도 `(1, fare)` 튜플로 묶어 `reduceByKey` 한 번에 처리한 뒤
  `mapValues`로 갈라냄 — 같은 셔플을 두 번 하지 않음
- 정제 결과와 일별 집계 결과에 `cache()` 적용
- CSV 경로는 컬럼 순서가 parquet 스키마와 동일하다고 가정하므로, 실제 CSV를 받으면
  헤더와 인덱스를 대조해야 함 (코드에 주석으로 명시)

## 출력

```
data/output/
├── summary/       # total_trip, total_sales, avg_distance
├── daily_trip/    # 날짜별 트립 수
└── daily_sales/   # 날짜별 매출
```

## 실행

```bash
python main.py
# 실행 후 http://localhost:4040 에서 DAG 확인 (Enter를 누르면 종료)
```

> 입력 데이터(`data/yellow_tripdata_2026-05.parquet`)는 `.gitignore` 대상입니다.
> [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)에서
> 원하는 월의 Yellow Taxi parquet을 내려받아 `data/`에 두세요.

## 관련 미션

동일한 데이터를 **DataFrame API**로 재구현하고 실행 계획을 분석한 버전은
[W5M2](../M2)에 있습니다.
