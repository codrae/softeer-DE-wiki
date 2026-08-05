# Design: Airflow 기반 서울시 공공자전거 2일치 이용 요약 ETL

- Status: Approved
- Date: 2026-08-05
- Scope: `missions/W6/M1_Airflow_Bike_ETL/`

## 1. 배경 및 목표

서울 열린데이터광장 `tbCycleRentUseDayInfo`(공공자전거 일별 대여 정보) API에서 2026-06-27,
2026-06-28 2일치 데이터를 수집하여 스테이션 단위로 집계한 뒤 MySQL `station_period_usage`
테이블에 적재하는 멱등(idempotent) Airflow DAG를 만든다. 팀 위키에서 이미 합의된 다음 결정을
그대로 따른다.

- 멱등성 방식: **DELETE 후 INSERT**
- PK(복합키): `station_id` + `period_start_date` + `period_end_date`
- 클렌징 방침: 결측치/이상치(음수 등) **전부 제외**
- 로그: 날짜별 수집 건수, 정제 전/후 건수, API 응답 건수, 성공 여부
- DQ 체크: 수집 단계(스키마/이상 row 수), 정제 단계(정제 비율), 최종 단계(station_id 중복)

이번 브레인스토밍에서 추가로 확정한 사항(개인 구현 범위):

- Seoul Open API 인증키는 이미 발급받은 실 키 사용, `.env`로 주입
- DAG는 **수동 트리거 + DAG params**(`start_date`/`end_date` 기본값 2026-06-27/28), `schedule=None`
- 날짜별 수집은 **Dynamic Task Mapping**(`.expand()`)으로 구성
- 태스크 간 중간 데이터는 **로컬 파일(JSON), 날짜로 덮어쓰기** (XCom에는 요약값만)
- DQ 위반 시 **심각도별 차등 적용**(치명적 → task 실패, 경미 → WARNING 로그만)
- Airflow 메타DB는 **Postgres**, 파이프라인 결과 저장은 **MySQL**로 분리
- Airflow 버전 3.3.0, LocalExecutor
- 단위 테스트는 하지 않고, `tests/test_project_contract.py`에 **계약(contract) 검증만** 가볍게 작성

## 2. 폴더 구조

```
missions/W6/M1_Airflow_Bike_ETL/
├── dags/
│   └── seoul_bike_period_usage.py
├── src/seoul_bike_etl/
│   ├── __init__.py
│   ├── api_client.py      # API 호출 + 페이지네이션 + 재시도
│   ├── cleaner.py         # 결측치/이상치 필터링
│   ├── aggregator.py      # 스테이션별 2일 합산 집계
│   └── config.py          # env/param 로딩, 상수(threshold, 컬럼 스키마 등)
├── sql/
│   ├── 001_create_station_period_usage.sql
│   └── final_result.sql
├── tests/
│   └── test_project_contract.py
├── data/
│   ├── raw/            # {date}.json (원본 수집 결과, 날짜로 덮어쓰기)
│   ├── processed/      # {date}.json (정제 후), aggregated.json (집계 결과)
│   └── rejected/       # {date}.json (제외된 행 + 사유)
├── config/
│   └── connections.json   # airflow-init이 등록할 MySQL Connection 정의
├── logs/                   # Airflow 로그 볼륨 마운트
├── plugins/                # Airflow 플러그인 (현재 비어있음)
├── screenshots/            # 제출용 DAG 실행 스크린샷
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml
├── .env.example
└── README.md
```

## 3. 인프라 구성 (Docker Compose)

| 서비스 | 역할 |
|---|---|
| `postgres` | Airflow 메타데이터 DB |
| `airflow-init` | `airflow db migrate` + admin 계정 생성 + `config/connections.json` 기반 Connection 등록 (1회성) |
| `airflow-webserver` | Airflow UI (LocalExecutor) |
| `airflow-scheduler` | DAG 스케줄링/실행 (LocalExecutor, 별도 worker 불필요) |
| `mysql` | 파이프라인 결과(`station_period_usage`) 저장 |

- `.env`(gitignore) 값: `SEOUL_API_KEY`, `MYSQL_*`(호스트/포트/DB/유저/비번), `AIRFLOW_UID` 등.
- `.env.example`에 키 목록과 설명만 커밋.
- `./dags`, `./src`, `./data`, `./logs`, `./plugins`, `./config`를 Airflow 컨테이너에 볼륨 마운트.
- MySQL 연결은 Airflow Connection(`mysql_target`)을 통해 `MySqlHook`(`apache-airflow-providers-mysql`)으로 사용 — 자격증명을 DAG 코드에 하드코딩하지 않음.

## 4. DAG 설계

파일: `dags/seoul_bike_period_usage.py`, DAG id: `seoul_bike_period_usage`

```
create_table >> build_date_list >> extract_day.expand(date=[...]) >> clean_day.expand(date=[...]) >> aggregate >> load_to_mysql
```

- `schedule=None`, `catchup=False`
- `params`: `start_date`(기본 `2026-06-27`), `end_date`(기본 `2026-06-28`) — Airflow UI "Trigger DAG w/ config"로 다른 기간도 재사용 가능. 단, 이번 과제 요구사항인 "두 날짜를 하나의 DAG run에서 처리"는 기본값 그대로 트리거하면 충족됨.

### 4.1 `create_table`
- `sql/001_create_station_period_usage.sql`을 `MySqlHook`으로 실행. `CREATE TABLE IF NOT EXISTS`,
  PK: (`station_id`, `period_start_date`, `period_end_date`). 여러 번 실행해도 안전.

### 4.2 `build_date_list`
- `params.start_date` ~ `params.end_date` 사이 날짜를 `YYYY-MM-DD` 문자열 리스트로 반환 (이번 과제는 2일).
  이후 `extract_day`/`clean_day`가 이 리스트로 `.expand()`.

### 4.3 `extract_day(date)` — Dynamic Task Mapping
- `src/seoul_bike_etl/api_client.py`의 `fetch_all_pages(date)` 호출:
  - `tbCycleRentUseDayInfo/{KEY}/json/{start_idx}/{end_idx}/{date}` 형태로 페이지 단위(`PAGE_SIZE=1000`,
    `config.py` 상수) 반복 호출, 응답의 `list_total_count`에 도달할 때까지 반복.
  - **재시도**: `requests` 타임아웃/연결 오류/5xx는 지수 백오프(`RETRY_BACKOFF_SECONDS=[2, 4, 8]`,
    최대 3회) 후 재시도. 4xx(인증키 오류 등)는 즉시 실패(재시도 무의미). Airflow task에도
    `retries=2`, `retry_delay=60s`를 안전망으로 설정.
- **DQ(수집 단계)**: 응답 각 row의 컬럼 셋이 기대 스키마(`RENT_STATN_ID`, `RENT_STATN_NM`, `USE_CNT`,
  `MOVE_METER`, `MOVE_TIME` 등)와 일치하는지 확인, 수집된 총 건수와 API `list_total_count` 비교.
  - 컬럼 불일치 또는 0건 수집 등 치명적 이상 → `AirflowException` (task 실패)
  - 건수 소폭 드리프트 등 경미한 이상 → `logging.warning`
- 결과를 `data/raw/{date}.json`에 **덮어쓰기** 저장 (재실행 시 항상 최신 원본으로 교체 → 멱등).
- XCom 반환값은 요약만: `{"date": date, "raw_count": N, "api_reported_total": M}`.

### 4.4 `clean_day(date)` — Dynamic Task Mapping
- `data/raw/{date}.json` 로드 후 `src/seoul_bike_etl/cleaner.py`의 `clean_records()` 적용.
  제외 규칙(모두 팀 합의: 전부 제외):
  - `RENT_STATN_ID`/`RENT_STATN_NM` 결측(null/빈 문자열)
  - `USE_CNT`/`MOVE_METER`/`MOVE_TIME` 숫자 변환 실패
  - 위 세 수치 중 하나라도 음수
- 정상 행 → `data/processed/{date}.json` 덮어쓰기. 제외 행 → `data/rejected/{date}.json`에
  `reason` 필드(`missing_station_id`, `non_numeric_use_cnt`, `negative_move_time` 등)와 함께 저장.
- **DQ(정제 단계)**: 정제 전/후 건수, 제외율(`rejected / raw`) 로그.
  - 전량 제외 또는 필수 컬럼이 데이터 전체에서 아예 없음 → 치명적, task 실패
  - 제외율이 임계치(`REJECTION_RATIO_WARNING_THRESHOLD=0.2`, 즉 20%) 초과 → 경미, WARNING만
- XCom 반환값: `{"date": date, "raw_count": N, "cleaned_count": C, "rejected_count": R, "rejection_ratio": r}`.

### 4.5 `aggregate`
- `data/processed/{start_date}.json`, `data/processed/{end_date}.json`을 합쳐 `station_id` 기준
  `USE_CNT`/`MOVE_METER`/`MOVE_TIME`을 합산 (`src/seoul_bike_etl/aggregator.py`). `station_name`은
  종료일(`end_date`) 레코드의 값을 우선 사용하고, 종료일에 해당 station_id가 없으면 시작일
  (`start_date`) 값을 사용. 결과에 `period_start_date`/`period_end_date` 부여 후
  `data/processed/aggregated.json`에 저장.
- **DQ(집계 단계)**: 집계 결과의 `station_id` 중복 여부 확인. groupby 이후에는 원칙적으로 발생할 수
  없으므로, 발견되면 버그 신호로 보고 **엄격 적용**(치명적, task 실패).

### 4.6 `load_to_mysql`
- `MySqlHook`으로 트랜잭션 내에서:
  1. `DELETE FROM station_period_usage WHERE period_start_date=%s AND period_end_date=%s`
  2. 집계 결과를 `executemany`로 INSERT
  3. `commit()`
- 이 방식으로 동일 기간을 몇 번 재실행해도 중복/충돌 없이 최신 결과로 교체됨(멱등성).
- 삭제/삽입된 행 수, 성공 여부를 로그로 남김.

## 5. 로깅

각 task는 Python `logging`으로 다음을 표준 Airflow task 로그에 남긴다.

- 날짜별 수집 건수, API 응답(`list_total_count`) 대비 실제 수집 건수
- 정제 전/후 건수, 제외 건수 및 비율
- 발생한 HTTP 에러 코드(재시도 포함 시도 횟수)
- 각 단계 성공/실패 여부(실패 시 예외 메시지 포함)

## 6. 에러 처리 요약

| 상황 | 처리 |
|---|---|
| API 타임아웃/연결 오류/5xx | `api_client` 내부 지수 백오프 재시도(최대 3회) → 실패 시 task 예외 발생 → Airflow task 재시도(최대 2회) |
| API 4xx(인증키 오류 등) | 즉시 실패, 재시도 없음 |
| 결측/숫자 변환 실패/음수 값 | 정제 단계에서 전부 제외, `rejected/{date}.json`에 사유와 함께 별도 저장 |
| 동일 기간 재실행 | raw/processed 파일은 날짜 키로 덮어쓰기, MySQL은 DELETE 후 INSERT → 중복/충돌 없음 |
| DQ 위반 | 치명적 → task 실패(AirflowException), 경미 → WARNING 로그 후 진행 |

## 7. 테스트

`tests/test_project_contract.py` (pytest, 단위 로직 커버리지 목적이 아닌 **계약 검증**):

- DAG 모듈이 임포트 오류 없이 로드되고 기대 task id(`create_table`, `build_date_list`,
  `extract_day`, `clean_day`, `aggregate`, `load_to_mysql`)를 갖는지 확인
- `cleaner.clean_records()`가 계약된 입출력 형태(리스트 in → (정상 리스트, 제외 리스트) out)를
  지키는지, null id/음수 값이 섞인 최소 샘플 1건이 실제로 걸러지는지 확인
- `aggregator.aggregate()`가 계약된 컬럼(`station_id`, `station_name`, `total_usage_count`,
  `total_distance_m`, `total_duration_min`, `period_start_date`, `period_end_date`)을 반환하는지 확인

## 8. 제출물 매핑

- 소스 코드/환경 설정: 본 폴더 전체 (`docker-compose.yml`, `Dockerfile`, `dags/`, `src/`, `sql/`)
- README: 설치/실행 방법 + 데이터 클렌징 규칙 설명
- DAG 성공 실행 스크린샷: `screenshots/`
- SQL 쿼리 및 결과: `sql/final_result.sql` 실행 결과

## 9. Out of Scope

- 단위 테스트를 통한 전체 로직 커버리지 (계약 검증만 수행)
- CeleryExecutor/Redis 등 분산 실행 구성 (과제 규모상 LocalExecutor로 충분)
- 3일 이상 기간 자동 백필/스케줄 실행 (수동 트리거 + params로 충분)
