# Airflow 기반 서울시 공공자전거 2일치 이용 요약 ETL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Docker Compose로 띄운 Airflow 3.3.0(LocalExecutor)이 서울 열린데이터광장 `tbCycleRentUseDayInfo` API에서 2026-06-27~28 데이터를 단일 DAG run에서 수집·정제·집계하여 MySQL `station_period_usage` 테이블에 멱등적으로 적재한다.

**Architecture:** Dynamic Task Mapping으로 날짜별 `extract_day`→`clean_day`를 독립 실행하고, 이후 `aggregate`→`load_to_mysql`로 합류하는 단일 TaskFlow DAG. 단계 간 데이터는 날짜 키 파일(JSON, 덮어쓰기)로 주고받아 XCom에는 요약 통계만 남긴다. MySQL 적재는 DELETE 후 INSERT 트랜잭션으로 멱등성을 보장한다.

**Tech Stack:** Apache Airflow 3.3.0(LocalExecutor), Postgres 16(Airflow 메타DB), MySQL 8.0(결과 저장), `apache-airflow-providers-mysql`(mysql-connector-python 드라이버), `requests`, Docker Compose, pytest(계약 검증용).

## Global Constraints

- Airflow 버전 3.3.0 고정 (Docker 이미지 `apache/airflow:3.3.0`).
- DAG는 하나의 DAG run에서 2026-06-27, 2026-06-28 두 날짜를 모두 처리한다 (`schedule=None`, `params.start_date`/`params.end_date` 기본값).
- 최종 결과는 MySQL 테이블 `station_period_usage`에 저장하며 컬럼은 정확히 `period_start_date, period_end_date, station_id, station_name, total_usage_count, total_distance_m, total_duration_min`.
- PK(복합키): `station_id` + `period_start_date` + `period_end_date`.
- 멱등성 방식: 동일 기간 재실행 시 기존 행 **DELETE 후 INSERT**. 중간 산출 파일도 날짜 키로 덮어쓰기.
- 클렌징 방침: 결측치(`RENT_STATN_ID`/`RENT_STATN_NM` 없음), 숫자 변환 실패, 음수 값은 **모두 제외**하고 사유와 함께 별도 저장.
- 로그: 날짜별 수집 건수, 정제 전/후 건수, API 응답 건수, HTTP 에러, 단계별 성공 여부를 모두 남긴다.
- DQ 체크는 심각도 차등 적용: 치명적 → task 실패(`AirflowException`), 경미 → `logging.warning`만.
- 단위 테스트는 하지 않는다. `tests/test_project_contract.py`에서 **계약(contract) 검증만** 가볍게 수행한다 — 이 계획의 각 태스크는 이 원칙에 따라 "실패하는 테스트 먼저" 대신 "구현 후 수동/계약 검증"으로 완료 기준을 삼는다(계약 테스트 태스크만 예외적으로 write-test→verify 순서를 따른다).
- 폴더/파일명은 사용자가 지정한 구조를 그대로 따른다 (아래 Task 1 참고).

---

## Task 1: 프로젝트 스캐폴딩 & Docker 빌드 설정

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/requirements.txt`
- Create: `missions/W6/M1_Airflow_Bike_ETL/requirements-dev.txt`
- Create: `missions/W6/M1_Airflow_Bike_ETL/pyproject.toml`
- Create: `missions/W6/M1_Airflow_Bike_ETL/Dockerfile`
- Create: `missions/W6/M1_Airflow_Bike_ETL/.env.example`
- Create: `missions/W6/M1_Airflow_Bike_ETL/.gitignore`
- Create: `missions/W6/M1_Airflow_Bike_ETL/src/seoul_bike_etl/__init__.py`
- Create: `missions/W6/M1_Airflow_Bike_ETL/data/raw/.gitkeep`, `data/processed/.gitkeep`, `data/rejected/.gitkeep`
- Create: `missions/W6/M1_Airflow_Bike_ETL/logs/.gitkeep`, `plugins/.gitkeep`, `config/.gitkeep`, `screenshots/.gitkeep`

**Interfaces:**
- Produces: `src/seoul_bike_etl` 패키지 루트(이후 Task에서 `config.py`/`api_client.py`/`cleaner.py`/`aggregator.py`가 이 패키지에 추가됨). `requirements.txt`에 `apache-airflow-providers-mysql`, `requests`, `mysql-connector-python` 명시.

- [ ] **Step 1: 디렉터리 구조 생성**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
mkdir -p dags src/seoul_bike_etl sql tests data/raw data/processed data/rejected config logs plugins screenshots
touch src/seoul_bike_etl/__init__.py
touch data/raw/.gitkeep data/processed/.gitkeep data/rejected/.gitkeep
touch logs/.gitkeep plugins/.gitkeep config/.gitkeep screenshots/.gitkeep
```

- [ ] **Step 2: `requirements.txt` 작성**

```text
apache-airflow-providers-mysql>=6.0.0
requests>=2.32.0,<3.0.0
mysql-connector-python>=9.0.0
```

- [ ] **Step 3: `requirements-dev.txt` 작성**

```text
pytest>=8.0.0
```

- [ ] **Step 4: `pyproject.toml` 작성**

```toml
[project]
name = "seoul-bike-etl"
version = "0.1.0"
description = "Seoul public bike 2-day station usage ETL for Airflow"
requires-python = ">=3.9"

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

- [ ] **Step 5: `Dockerfile` 작성**

```dockerfile
ARG AIRFLOW_VERSION=3.3.0
FROM apache/airflow:${AIRFLOW_VERSION}

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
```

- [ ] **Step 6: `.env.example` 작성**

```text
# 서울 열린데이터광장 인증키 (실제 발급받은 키로 교체)
SEOUL_API_KEY=CHANGE_ME

# Docker 볼륨 권한용 (Linux: `id -u` 결과 사용, macOS는 기본값 그대로 두어도 무방)
AIRFLOW_UID=50000

# MySQL (파이프라인 결과 저장용)
MYSQL_ROOT_PASSWORD=change_me_root
MYSQL_DATABASE=bike_dw
MYSQL_USER=bike_etl
MYSQL_PASSWORD=change_me_password
```

- [ ] **Step 7: `.gitignore` 작성**

```text
.env
__pycache__/
*.pyc
.pytest_cache/
data/raw/*.json
data/processed/*.json
data/rejected/*.json
!data/raw/.gitkeep
!data/processed/.gitkeep
!data/rejected/.gitkeep
logs/*
!logs/.gitkeep
```

- [ ] **Step 8: 검증**

```bash
find . -maxdepth 2 -not -path '*/\.git*'
```

Expected: `dags/`, `src/seoul_bike_etl/__init__.py`, `sql/`, `tests/`, `data/{raw,processed,rejected}/.gitkeep`, `config/.gitkeep`, `logs/.gitkeep`, `plugins/.gitkeep`, `screenshots/.gitkeep`, `Dockerfile`, `requirements.txt`, `requirements-dev.txt`, `pyproject.toml`, `.env.example`, `.gitignore`가 모두 존재.

- [ ] **Step 9: 커밋**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W6/M1_Airflow_Bike_ETL
git commit -m "chore(W6M1): 프로젝트 스캐폴딩 및 Docker 빌드 설정"
```

---

## Task 2: `config.py` — 상수/환경변수/경로 헬퍼

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/src/seoul_bike_etl/config.py`

**Interfaces:**
- Consumes: 없음 (최하위 모듈)
- Produces:
  - 상수: `API_BASE_URL`, `API_SERVICE_NAME`, `PAGE_SIZE`, `RETRY_BACKOFF_SECONDS: list[int]`, `REQUEST_TIMEOUT_SECONDS`, `REQUIRED_COLUMNS: list[str]`, `NUMERIC_COLUMNS: list[str]`, `REJECTION_RATIO_WARNING_THRESHOLD: float`, `MYSQL_CONN_ID: str`, `MYSQL_TABLE: str`
  - 함수: `get_api_key() -> str`, `ensure_data_dirs() -> None`, `date_range(start_date: str, end_date: str) -> list[str]`, `to_api_date(date_str: str) -> str`, `raw_path(date_str: str) -> Path`, `processed_path(date_str: str) -> Path`, `rejected_path(date_str: str) -> Path`, `aggregated_path() -> Path`

- [ ] **Step 1: `config.py` 작성**

```python
"""Configuration constants and environment loading for the Seoul bike ETL pipeline."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

# --- Seoul Open API ---
API_BASE_URL = "http://openapi.seoul.go.kr:8088"
API_SERVICE_NAME = "tbCycleRentUseDayInfo"
PAGE_SIZE = 1000
RETRY_BACKOFF_SECONDS = [2, 4, 8]
REQUEST_TIMEOUT_SECONDS = 30

# API가 각 row에 포함해야 하는 필수 컬럼과, 그중 숫자 변환/음수 검사가 필요한 컬럼.
REQUIRED_COLUMNS = ["RENT_STATN_ID", "RENT_STATN_NM", "USE_CNT", "MOVE_METER", "MOVE_TIME"]
NUMERIC_COLUMNS = ["USE_CNT", "MOVE_METER", "MOVE_TIME"]

# --- 데이터 품질 임계치 ---
REJECTION_RATIO_WARNING_THRESHOLD = 0.2  # 20% 초과 시 WARNING

# --- 로컬 파일 경로 (Airflow 컨테이너에 볼륨 마운트됨) ---
DATA_DIR = Path(os.environ.get("SEOUL_BIKE_DATA_DIR", "/opt/airflow/data"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
REJECTED_DIR = DATA_DIR / "rejected"
SQL_DIR = Path(os.environ.get("SEOUL_BIKE_SQL_DIR", "/opt/airflow/sql"))

MYSQL_CONN_ID = "mysql_target"
MYSQL_TABLE = "station_period_usage"


def get_api_key() -> str:
    key = os.environ.get("SEOUL_API_KEY")
    if not key:
        raise RuntimeError(
            "SEOUL_API_KEY 환경변수가 설정되어 있지 않습니다. .env 파일을 확인하세요."
        )
    return key


def ensure_data_dirs() -> None:
    for d in (RAW_DIR, PROCESSED_DIR, REJECTED_DIR):
        d.mkdir(parents=True, exist_ok=True)


def date_range(start_date: str, end_date: str) -> list[str]:
    """start_date~end_date 사이(포함) 'YYYY-MM-DD' 문자열 리스트를 반환한다."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end < start:
        raise ValueError(f"end_date({end_date})가 start_date({start_date})보다 앞설 수 없습니다.")
    days = (end - start).days
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days + 1)]


def to_api_date(date_str: str) -> str:
    """'2026-06-27' -> '20260627' (Seoul Open API 날짜 경로 세그먼트 형식)."""
    return date_str.replace("-", "")


def raw_path(date_str: str) -> Path:
    return RAW_DIR / f"{date_str}.json"


def processed_path(date_str: str) -> Path:
    return PROCESSED_DIR / f"{date_str}.json"


def rejected_path(date_str: str) -> Path:
    return REJECTED_DIR / f"{date_str}.json"


def aggregated_path() -> Path:
    return PROCESSED_DIR / "aggregated.json"
```

- [ ] **Step 2: 로컬에서 임포트/기본 동작 확인**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
SEOUL_API_KEY=dummy python3 -c "
from seoul_bike_etl import config
print(config.date_range('2026-06-27', '2026-06-28'))
print(config.to_api_date('2026-06-27'))
print(config.get_api_key())
"
```

Expected: `['2026-06-27', '2026-06-28']`, `20260627`, `dummy` 출력. (`PYTHONPATH=src`가 없으면 `pip install -e .` 또는 `PYTHONPATH=src python3 ...`로 실행)

- [ ] **Step 3: 커밋**

```bash
git add src/seoul_bike_etl/config.py
git commit -m "feat(W6M1): config 모듈 - 상수/환경변수/경로 헬퍼"
```

---

## Task 3: SQL 스크립트

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/sql/001_create_station_period_usage.sql`
- Create: `missions/W6/M1_Airflow_Bike_ETL/sql/final_result.sql`

**Interfaces:**
- Consumes: 없음
- Produces: DAG의 `create_table` 태스크(Task 7)가 `001_create_station_period_usage.sql`을 읽어 실행. `final_result.sql`은 제출용 수동 조회 쿼리.

- [ ] **Step 1: `001_create_station_period_usage.sql` 작성**

```sql
CREATE TABLE IF NOT EXISTS station_period_usage (
    station_id           VARCHAR(64)  NOT NULL,
    period_start_date    DATE         NOT NULL,
    period_end_date      DATE         NOT NULL,
    station_name         VARCHAR(255) NOT NULL,
    total_usage_count    DOUBLE       NOT NULL,
    total_distance_m     DOUBLE       NOT NULL,
    total_duration_min   DOUBLE       NOT NULL,
    PRIMARY KEY (station_id, period_start_date, period_end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 2: `final_result.sql` 작성**

```sql
SELECT
    period_start_date,
    period_end_date,
    station_id,
    station_name,
    total_usage_count,
    total_distance_m,
    total_duration_min
FROM station_period_usage
WHERE period_start_date = '2026-06-27'
  AND period_end_date = '2026-06-28'
ORDER BY total_usage_count DESC;
```

- [ ] **Step 3: 커밋**

```bash
git add sql/
git commit -m "feat(W6M1): station_period_usage DDL 및 최종 결과 조회 쿼리 추가"
```

---

## Task 4: `api_client.py` — API 호출, 페이지네이션, 재시도, 수집 단계 DQ

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/src/seoul_bike_etl/api_client.py`

**Interfaces:**
- Consumes: `config.API_BASE_URL`, `config.API_SERVICE_NAME`, `config.PAGE_SIZE`, `config.RETRY_BACKOFF_SECONDS`, `config.REQUEST_TIMEOUT_SECONDS`, `config.REQUIRED_COLUMNS`, `config.get_api_key()`, `config.to_api_date()`
- Produces: `fetch_all_pages(date_str: str) -> list[dict]`, `validate_raw_schema(records: list[dict], date_str: str) -> None`, 예외 클래스 `SeoulApiError`, `SeoulApiTransientError`

- [ ] **Step 1: `api_client.py` 작성**

```python
"""Seoul Open API client for tbCycleRentUseDayInfo with pagination and retry."""
from __future__ import annotations

import logging
import time

import requests

from seoul_bike_etl import config

logger = logging.getLogger(__name__)


class SeoulApiError(Exception):
    """치명적이고 재시도 불가능한 API 오류(인증키 오류, 잘못된 요청, 스키마 불일치 등)."""


class SeoulApiTransientError(Exception):
    """일시적 오류(타임아웃, 연결 실패, 5xx) — 재시도 후에도 실패하면 발생."""


def _build_url(api_key: str, start_idx: int, end_idx: int, date_str: str) -> str:
    api_date = config.to_api_date(date_str)
    return (
        f"{config.API_BASE_URL}/{api_key}/json/{config.API_SERVICE_NAME}/"
        f"{start_idx}/{end_idx}/{api_date}"
    )


def _request_page(url: str) -> dict:
    """한 페이지를 요청한다. 타임아웃/연결 오류/5xx는 지수 백오프로 재시도한다."""
    last_error: Exception | None = None
    attempts = [0, *config.RETRY_BACKOFF_SECONDS]
    for attempt, backoff in enumerate(attempts, start=1):
        if backoff:
            logger.warning("재시도 %s/%s, %s초 대기 후 재요청: %s", attempt - 1, len(attempts) - 1, backoff, url)
            time.sleep(backoff)
        try:
            response = requests.get(url, timeout=config.REQUEST_TIMEOUT_SECONDS)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = SeoulApiTransientError(f"네트워크 오류: {exc}")
            logger.warning("API 요청 실패(%s/%s): %s", attempt, len(attempts), exc)
            continue

        if response.status_code >= 500:
            last_error = SeoulApiTransientError(f"HTTP {response.status_code}: {response.text[:200]}")
            logger.warning("API 서버 오류(%s/%s), HTTP %s", attempt, len(attempts), response.status_code)
            continue

        if response.status_code >= 400:
            raise SeoulApiError(f"HTTP {response.status_code}: {response.text[:200]}")

        return response.json()

    raise SeoulApiTransientError(f"최대 재시도({len(config.RETRY_BACKOFF_SECONDS)}회) 초과: {last_error}")


def _extract_page(payload: dict) -> tuple[list[dict], int]:
    """API 응답 payload에서 (해당 페이지 row 리스트, list_total_count)를 추출한다."""
    body = payload.get(config.API_SERVICE_NAME)
    if body is None:
        raise SeoulApiError(f"예상하지 못한 응답 형식(최상위 키 없음): {list(payload.keys())}")

    result = body.get("RESULT", {})
    code = result.get("CODE", "")
    if code == "INFO-200":  # 해당 구간에 더 이상 데이터 없음 (정상 종료 신호)
        return [], int(body.get("list_total_count", 0))
    if code and not code.startswith("INFO-0"):
        raise SeoulApiError(f"API 오류 응답 [{code}]: {result.get('MESSAGE')}")

    rows = body.get("row", [])
    total = int(body.get("list_total_count", 0))
    return rows, total


def fetch_all_pages(date_str: str) -> list[dict]:
    """date_str('YYYY-MM-DD')에 대한 전체 페이지를 수집해 하나의 리스트로 반환한다."""
    api_key = config.get_api_key()
    all_rows: list[dict] = []
    start_idx = 1
    total_count = 0

    while True:
        end_idx = start_idx + config.PAGE_SIZE - 1
        url = _build_url(api_key, start_idx, end_idx, date_str)
        payload = _request_page(url)
        rows, total_count = _extract_page(payload)

        if not rows:
            break
        all_rows.extend(rows)
        logger.info(
            "[%s] 페이지 수집: %s~%s, 누적 %s/%s건",
            date_str, start_idx, end_idx, len(all_rows), total_count,
        )
        if len(all_rows) >= total_count or len(rows) < config.PAGE_SIZE:
            break
        start_idx = end_idx + 1

    logger.info("[%s] 수집 완료: %s건 (API list_total_count=%s)", date_str, len(all_rows), total_count)
    return all_rows


def validate_raw_schema(records: list[dict], date_str: str) -> None:
    """수집 단계 DQ: 필수 컬럼 존재 여부, 비정상 row 수(0건)를 확인한다.

    치명적 이상(0건 수집, 필수 컬럼이 절반 넘게 없음)은 SeoulApiError로 task를 실패시키고,
    경미한 이상(일부 행의 컬럼 누락)은 WARNING 로그만 남긴다.
    """
    if not records:
        raise SeoulApiError(f"[{date_str}] 수집된 레코드가 0건입니다.")

    missing_column_rows = [
        row for row in records if not set(config.REQUIRED_COLUMNS).issubset(row.keys())
    ]
    if missing_column_rows:
        ratio = len(missing_column_rows) / len(records)
        message = (
            f"[{date_str}] 필수 컬럼 {config.REQUIRED_COLUMNS} 중 일부가 없는 행 "
            f"{len(missing_column_rows)}/{len(records)}건({ratio:.1%})"
        )
        if ratio > 0.5:
            raise SeoulApiError(f"치명적 스키마 불일치: {message}")
        logger.warning("경미한 스키마 불일치: %s", message)
```

- [ ] **Step 2: 로컬 페이지네이션/스키마 검증 로직 확인 (실제 API 호출 없이)**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
PYTHONPATH=src python3 -c "
from seoul_bike_etl.api_client import _extract_page, validate_raw_schema, SeoulApiError

payload = {
    'tbCycleRentUseDayInfo': {
        'list_total_count': 2,
        'RESULT': {'CODE': 'INFO-000', 'MESSAGE': 'OK'},
        'row': [
            {'RENT_STATN_ID': 'ST-1', 'RENT_STATN_NM': 'A', 'USE_CNT': '1', 'MOVE_METER': '10', 'MOVE_TIME': '5'},
        ],
    }
}
rows, total = _extract_page(payload)
assert rows[0]['RENT_STATN_ID'] == 'ST-1' and total == 2
print('extract_page OK')

try:
    validate_raw_schema([], '2026-06-27')
    raise SystemExit('expected SeoulApiError for empty records')
except SeoulApiError:
    print('validate_raw_schema empty-list guard OK')
"
```

Expected: `extract_page OK`, `validate_raw_schema empty-list guard OK` 출력. (실제 API 키를 이용한 종단 검증은 Task 11에서 수행)

- [ ] **Step 3: 커밋**

```bash
git add src/seoul_bike_etl/api_client.py
git commit -m "feat(W6M1): Seoul Open API 클라이언트 - 페이지네이션/재시도/수집 DQ"
```

---

## Task 5: `cleaner.py` — 행 단위 정제 규칙

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/src/seoul_bike_etl/cleaner.py`

**Interfaces:**
- Consumes: `config.NUMERIC_COLUMNS`
- Produces: `clean_record(record: dict) -> tuple[dict | None, dict | None]`, `clean_records(records: list[dict]) -> tuple[list[dict], list[dict]]`

- [ ] **Step 1: `cleaner.py` 작성**

```python
"""Row-level cleaning rules for Seoul bike usage records.

정제 규칙 (팀 합의: 결측치/이상치는 모두 제외):
- RENT_STATN_ID, RENT_STATN_NM 결측(null/빈 문자열)
- USE_CNT, MOVE_METER, MOVE_TIME 숫자 변환 실패
- 위 세 수치 중 하나라도 음수
"""
from __future__ import annotations

from seoul_bike_etl import config


def _is_missing(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clean_record(record: dict) -> tuple[dict | None, dict | None]:
    """단일 레코드를 검증한다.

    Returns:
        (cleaned_record, None) — 정상 레코드
        (None, rejected_record_with_reason) — 제외된 레코드 (원본 + 'reason' 필드)
    """
    if _is_missing(record.get("RENT_STATN_ID")):
        return None, {**record, "reason": "missing_station_id"}
    if _is_missing(record.get("RENT_STATN_NM")):
        return None, {**record, "reason": "missing_station_name"}

    numeric_values = {}
    for col in config.NUMERIC_COLUMNS:
        value = _to_float(record.get(col))
        if value is None:
            return None, {**record, "reason": f"non_numeric_{col.lower()}"}
        if value < 0:
            return None, {**record, "reason": f"negative_{col.lower()}"}
        numeric_values[col] = value

    cleaned = {
        "RENT_STATN_ID": str(record["RENT_STATN_ID"]),
        "RENT_STATN_NM": str(record["RENT_STATN_NM"]),
        **numeric_values,
    }
    return cleaned, None


def clean_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """레코드 리스트를 정제해 (정상 리스트, 제외 리스트)를 반환한다."""
    accepted: list[dict] = []
    rejected: list[dict] = []
    for record in records:
        cleaned, reason_row = clean_record(record)
        if cleaned is not None:
            accepted.append(cleaned)
        else:
            rejected.append(reason_row)
    return accepted, rejected
```

- [ ] **Step 2: 로컬 동작 확인**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
PYTHONPATH=src python3 -c "
from seoul_bike_etl.cleaner import clean_records

records = [
    {'RENT_STATN_ID': 'ST-1', 'RENT_STATN_NM': 'A', 'USE_CNT': '3', 'MOVE_METER': '100', 'MOVE_TIME': '20'},
    {'RENT_STATN_ID': None, 'RENT_STATN_NM': 'B', 'USE_CNT': '1', 'MOVE_METER': '10', 'MOVE_TIME': '5'},
    {'RENT_STATN_ID': 'ST-2', 'RENT_STATN_NM': 'C', 'USE_CNT': '-1', 'MOVE_METER': '10', 'MOVE_TIME': '5'},
    {'RENT_STATN_ID': 'ST-3', 'RENT_STATN_NM': 'D', 'USE_CNT': 'abc', 'MOVE_METER': '10', 'MOVE_TIME': '5'},
]
accepted, rejected = clean_records(records)
assert len(accepted) == 1 and accepted[0]['RENT_STATN_ID'] == 'ST-1'
assert len(rejected) == 3
assert {r['reason'] for r in rejected} == {'missing_station_id', 'negative_use_cnt', 'non_numeric_use_cnt'}
print('clean_records OK', accepted, rejected)
"
```

Expected: `clean_records OK ...` 출력, 어설션 통과.

- [ ] **Step 3: 커밋**

```bash
git add src/seoul_bike_etl/cleaner.py
git commit -m "feat(W6M1): 결측치/이상치 정제 로직 (전부 제외 방침)"
```

---

## Task 6: `aggregator.py` — 스테이션별 2일 합산 집계

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/src/seoul_bike_etl/aggregator.py`

**Interfaces:**
- Consumes: `cleaner.clean_records()`가 반환하는 형태의 정제된 레코드(dict, 키: `RENT_STATN_ID`, `RENT_STATN_NM`, `USE_CNT`, `MOVE_METER`, `MOVE_TIME`)
- Produces: `REQUIRED_OUTPUT_COLUMNS: list[str]`, `aggregate(records_by_date: dict[str, list[dict]], start_date: str, end_date: str) -> list[dict]`, `check_duplicate_station_ids(aggregated: list[dict]) -> list[str]`

- [ ] **Step 1: `aggregator.py` 작성**

```python
"""Aggregate cleaned per-day records into one row per station for the period."""
from __future__ import annotations

from collections import OrderedDict

REQUIRED_OUTPUT_COLUMNS = [
    "period_start_date",
    "period_end_date",
    "station_id",
    "station_name",
    "total_usage_count",
    "total_distance_m",
    "total_duration_min",
]


def aggregate(
    records_by_date: dict[str, list[dict]],
    start_date: str,
    end_date: str,
) -> list[dict]:
    """records_by_date: {'2026-06-27': [cleaned_record, ...], '2026-06-28': [...]}

    station_name은 종료일(end_date) 레코드 값을 우선 사용하고, 종료일에 해당
    station_id가 없으면 시작일(start_date) 값을 사용한다.
    """
    totals: "OrderedDict[str, dict]" = OrderedDict()

    for date_str in (start_date, end_date):
        for record in records_by_date.get(date_str, []):
            station_id = record["RENT_STATN_ID"]
            entry = totals.setdefault(
                station_id,
                {
                    "station_id": station_id,
                    "station_name": record["RENT_STATN_NM"],
                    "total_usage_count": 0.0,
                    "total_distance_m": 0.0,
                    "total_duration_min": 0.0,
                },
            )
            if date_str == end_date:
                entry["station_name"] = record["RENT_STATN_NM"]
            entry["total_usage_count"] += record["USE_CNT"]
            entry["total_distance_m"] += record["MOVE_METER"]
            entry["total_duration_min"] += record["MOVE_TIME"]

    result = []
    for entry in totals.values():
        result.append(
            {
                "period_start_date": start_date,
                "period_end_date": end_date,
                "station_id": entry["station_id"],
                "station_name": entry["station_name"],
                "total_usage_count": entry["total_usage_count"],
                "total_distance_m": entry["total_distance_m"],
                "total_duration_min": entry["total_duration_min"],
            }
        )
    return result


def check_duplicate_station_ids(aggregated: list[dict]) -> list[str]:
    """집계 결과에서 중복된 station_id를 찾아 반환한다 (정상이면 빈 리스트)."""
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in aggregated:
        sid = row["station_id"]
        if sid in seen:
            duplicates.add(sid)
        seen.add(sid)
    return sorted(duplicates)
```

- [ ] **Step 2: 로컬 동작 확인**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
PYTHONPATH=src python3 -c "
from seoul_bike_etl.aggregator import aggregate, check_duplicate_station_ids, REQUIRED_OUTPUT_COLUMNS

records_by_date = {
    '2026-06-27': [
        {'RENT_STATN_ID': 'ST-1', 'RENT_STATN_NM': 'Old Name', 'USE_CNT': 3.0, 'MOVE_METER': 100.0, 'MOVE_TIME': 20.0},
    ],
    '2026-06-28': [
        {'RENT_STATN_ID': 'ST-1', 'RENT_STATN_NM': 'New Name', 'USE_CNT': 2.0, 'MOVE_METER': 50.0, 'MOVE_TIME': 10.0},
        {'RENT_STATN_ID': 'ST-2', 'RENT_STATN_NM': 'B', 'USE_CNT': 1.0, 'MOVE_METER': 5.0, 'MOVE_TIME': 2.0},
    ],
}
result = aggregate(records_by_date, '2026-06-27', '2026-06-28')
assert set(result[0].keys()) == set(REQUIRED_OUTPUT_COLUMNS)
st1 = next(r for r in result if r['station_id'] == 'ST-1')
assert st1['station_name'] == 'New Name'
assert st1['total_usage_count'] == 5.0
assert st1['total_distance_m'] == 150.0
assert st1['total_duration_min'] == 30.0
assert check_duplicate_station_ids(result) == []
print('aggregate OK', result)
"
```

Expected: `aggregate OK ...` 출력, 어설션 통과.

- [ ] **Step 3: 커밋**

```bash
git add src/seoul_bike_etl/aggregator.py
git commit -m "feat(W6M1): 스테이션별 2일 합산 집계 및 중복 station_id 검사"
```

---

## Task 7: Docker Compose 인프라 (Postgres + MySQL + Airflow 3.3.0)

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/docker-compose.yml`

**Interfaces:**
- Consumes: Task 1의 `Dockerfile`/`requirements.txt`, Task 2의 `config.py` 환경변수 이름(`SEOUL_API_KEY`, `SEOUL_BIKE_DATA_DIR`, `SEOUL_BIKE_SQL_DIR`, `MYSQL_CONN_ID=mysql_target`)
- Produces: `postgres`, `mysql`, `airflow-init`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor` 서비스. Airflow Connection `mysql_target`은 `AIRFLOW_CONN_MYSQL_TARGET` 환경변수(URI)로 자동 등록됨.

- [ ] **Step 1: `docker-compose.yml` 작성**

```yaml
x-airflow-common: &airflow-common
  build:
    context: .
    args:
      AIRFLOW_VERSION: "3.3.0"
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    PYTHONPATH: /opt/airflow/src
    SEOUL_API_KEY: ${SEOUL_API_KEY}
    SEOUL_BIKE_DATA_DIR: /opt/airflow/data
    SEOUL_BIKE_SQL_DIR: /opt/airflow/sql
    AIRFLOW_CONN_MYSQL_TARGET: "mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@mysql:3306/${MYSQL_DATABASE}?client=mysql-connector-python&charset=utf8mb4"
  volumes:
    - ./dags:/opt/airflow/dags
    - ./src:/opt/airflow/src
    - ./sql:/opt/airflow/sql
    - ./data:/opt/airflow/data
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on: &airflow-common-depends-on
    postgres:
      condition: service_healthy
    mysql:
      condition: service_healthy

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD}
      MYSQL_DATABASE: ${MYSQL_DATABASE}
      MYSQL_USER: ${MYSQL_USER}
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
    ports:
      - "3307:3306"
    volumes:
      - mysql-db-volume:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-p${MYSQL_ROOT_PASSWORD}"]
      interval: 5s
      retries: 10
    restart: always

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command: ["-c", "airflow db migrate"]
    depends_on:
      postgres:
        condition: service_healthy
      mysql:
        condition: service_healthy

  airflow-api-server:
    <<: *airflow-common
    command: api-server
    ports:
      - "8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor
    depends_on:
      airflow-init:
        condition: service_completed_successfully

volumes:
  postgres-db-volume:
  mysql-db-volume:
```

- [ ] **Step 2: 문법 검증**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
cp .env.example .env   # 로컬 검증용, SEOUL_API_KEY는 이후 Task 11에서 실제 키로 교체
docker compose config --quiet && echo "docker-compose.yml OK"
```

Expected: 에러 없이 `docker-compose.yml OK` 출력.

- [ ] **Step 3: 커밋**

```bash
git add docker-compose.yml
git commit -m "feat(W6M1): Postgres/MySQL/Airflow 3.3.0 Docker Compose 구성"
```

---

## Task 8: Airflow DAG — `seoul_bike_period_usage`

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/dags/seoul_bike_period_usage.py`

**Interfaces:**
- Consumes: `config`(Task 2), `api_client.fetch_all_pages`/`validate_raw_schema`/`SeoulApiError`(Task 4), `cleaner.clean_records`(Task 5), `aggregator.aggregate`/`check_duplicate_station_ids`/`REQUIRED_OUTPUT_COLUMNS`(Task 6), `MySqlHook`(`apache-airflow-providers-mysql`)
- Produces: DAG id `seoul_bike_period_usage`, task id: `create_table`, `build_date_list`, `extract_day`(mapped), `clean_day`(mapped), `aggregate`, `load_to_mysql` — Task 9(계약 테스트)가 이 정확한 task id들을 검증한다.

- [ ] **Step 1: `dags/seoul_bike_period_usage.py` 작성**

```python
"""Seoul public bike 2-day station usage summary ETL.

서울 열린데이터광장 tbCycleRentUseDayInfo API에서 지정된 기간(기본
2026-06-27~2026-06-28)의 데이터를 수집, 정제, 스테이션 단위로 집계하여 MySQL
station_period_usage 테이블에 멱등적으로(delete-then-insert) 적재한다.
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import DAG, Param, task

from seoul_bike_etl import config
from seoul_bike_etl.aggregator import (
    REQUIRED_OUTPUT_COLUMNS,
    aggregate,
    check_duplicate_station_ids,
)
from seoul_bike_etl.api_client import fetch_all_pages, validate_raw_schema
from seoul_bike_etl.cleaner import clean_records

logger = logging.getLogger(__name__)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
}

with DAG(
    dag_id="seoul_bike_period_usage",
    description="서울 공공자전거 2일치 스테이션별 이용 요약 ETL",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    params={
        "start_date": Param("2026-06-27", type="string", format="date"),
        "end_date": Param("2026-06-28", type="string", format="date"),
    },
    tags=["seoul-bike", "etl"],
) as dag:

    @task
    def create_table() -> None:
        hook = MySqlHook(mysql_conn_id=config.MYSQL_CONN_ID)
        sql_path = config.SQL_DIR / "001_create_station_period_usage.sql"
        hook.run(sql_path.read_text(encoding="utf-8"))
        logger.info("station_period_usage 테이블 준비 완료")

    @task
    def build_date_list(**context) -> list[str]:
        params = context["params"]
        dates = config.date_range(params["start_date"], params["end_date"])
        logger.info("처리 대상 날짜: %s", dates)
        return dates

    @task
    def extract_day(date: str) -> dict:
        config.ensure_data_dirs()
        records = fetch_all_pages(date)
        validate_raw_schema(records, date)
        config.raw_path(date).write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
        summary = {"date": date, "raw_count": len(records)}
        logger.info("[%s] 수집 요약: %s", date, summary)
        return summary

    @task
    def clean_day(extract_summary: dict) -> dict:
        date = extract_summary["date"]
        records = json.loads(config.raw_path(date).read_text(encoding="utf-8"))
        accepted, rejected = clean_records(records)

        config.processed_path(date).write_text(json.dumps(accepted, ensure_ascii=False), encoding="utf-8")
        config.rejected_path(date).write_text(json.dumps(rejected, ensure_ascii=False), encoding="utf-8")

        raw_count = len(records)
        rejection_ratio = len(rejected) / raw_count if raw_count else 0.0
        summary = {
            "date": date,
            "raw_count": raw_count,
            "cleaned_count": len(accepted),
            "rejected_count": len(rejected),
            "rejection_ratio": rejection_ratio,
        }
        logger.info("[%s] 정제 요약: %s", date, summary)

        if len(accepted) == 0:
            raise AirflowException(f"[{date}] 정제 후 남은 레코드가 0건입니다.")
        if rejection_ratio > config.REJECTION_RATIO_WARNING_THRESHOLD:
            logger.warning(
                "[%s] 제외율(%.1f%%)이 임계치(%.0f%%)를 초과했습니다.",
                date, rejection_ratio * 100, config.REJECTION_RATIO_WARNING_THRESHOLD * 100,
            )
        return summary

    @task(task_id="aggregate")
    def aggregate_task(clean_summaries: list[dict], **context) -> dict:
        params = context["params"]
        start_date, end_date = params["start_date"], params["end_date"]

        records_by_date = {
            summary["date"]: json.loads(config.processed_path(summary["date"]).read_text(encoding="utf-8"))
            for summary in clean_summaries
        }
        aggregated = aggregate(records_by_date, start_date, end_date)

        duplicates = check_duplicate_station_ids(aggregated)
        if duplicates:
            raise AirflowException(f"집계 결과에 중복된 station_id 발견: {duplicates}")

        missing_cols = [c for c in REQUIRED_OUTPUT_COLUMNS if c not in (aggregated[0] if aggregated else {})]
        if missing_cols:
            raise AirflowException(f"집계 결과에 필요한 컬럼이 없습니다: {missing_cols}")

        config.aggregated_path().write_text(json.dumps(aggregated, ensure_ascii=False), encoding="utf-8")
        summary = {"station_count": len(aggregated), "start_date": start_date, "end_date": end_date}
        logger.info("집계 요약: %s", summary)
        return summary

    @task
    def load_to_mysql(aggregate_summary: dict) -> None:
        aggregated = json.loads(config.aggregated_path().read_text(encoding="utf-8"))
        start_date = aggregate_summary["start_date"]
        end_date = aggregate_summary["end_date"]

        hook = MySqlHook(mysql_conn_id=config.MYSQL_CONN_ID)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"DELETE FROM {config.MYSQL_TABLE} WHERE period_start_date = %s AND period_end_date = %s",
                (start_date, end_date),
            )
            deleted = cursor.rowcount
            cursor.executemany(
                f"""
                INSERT INTO {config.MYSQL_TABLE} (
                    station_id, period_start_date, period_end_date, station_name,
                    total_usage_count, total_distance_m, total_duration_min
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        row["station_id"], row["period_start_date"], row["period_end_date"],
                        row["station_name"], row["total_usage_count"],
                        row["total_distance_m"], row["total_duration_min"],
                    )
                    for row in aggregated
                ],
            )
            inserted = cursor.rowcount
            conn.commit()
            logger.info(
                "MySQL 적재 완료: 삭제 %s건, 삽입 %s건 (기간 %s~%s)",
                deleted, inserted, start_date, end_date,
            )
        except Exception:
            conn.rollback()
            logger.exception("MySQL 적재 실패, 롤백했습니다.")
            raise
        finally:
            conn.close()

    table_ready = create_table()
    dates = build_date_list()
    table_ready >> dates

    extract_summaries = extract_day.expand(date=dates)
    clean_summaries = clean_day.expand(extract_summary=extract_summaries)
    agg_summary = aggregate_task(clean_summaries)
    load_to_mysql(agg_summary)
```

- [ ] **Step 2: DAG 임포트 오류 확인 (Airflow 컨테이너 안에서)**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
docker compose run --rm airflow-scheduler airflow dags list-import-errors
```

Expected: `seoul_bike_period_usage.py`에 대한 오류가 없음(빈 목록 또는 다른 DAG만 표시).

- [ ] **Step 3: 커밋**

```bash
git add dags/seoul_bike_period_usage.py
git commit -m "feat(W6M1): seoul_bike_period_usage DAG - dynamic mapping + delete/insert 적재"
```

---

## Task 9: 계약(Contract) 테스트

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/tests/test_project_contract.py`

**Interfaces:**
- Consumes: `dags.seoul_bike_period_usage`(Task 8), `seoul_bike_etl.cleaner.clean_records`(Task 5), `seoul_bike_etl.aggregator.aggregate`/`REQUIRED_OUTPUT_COLUMNS`(Task 6)
- Produces: 없음 (검증 전용)

- [ ] **Step 1: 실패하는 테스트 작성**

```python
"""프로젝트 계약(contract) 검증 — 전체 로직 커버리지가 아닌, 인터페이스가 깨지지 않았는지만 확인한다."""
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

EXPECTED_TASK_IDS = {
    "create_table",
    "build_date_list",
    "extract_day",
    "clean_day",
    "aggregate",
    "load_to_mysql",
}


@pytest.fixture(autouse=True)
def _seoul_api_key(monkeypatch):
    monkeypatch.setenv("SEOUL_API_KEY", "dummy-key-for-tests")


def test_dag_imports_and_has_expected_task_ids():
    import seoul_bike_period_usage as dag_module

    dag = dag_module.dag
    assert set(dag.task_ids) == EXPECTED_TASK_IDS


def test_cleaner_rejects_missing_and_negative_values():
    from seoul_bike_etl.cleaner import clean_records

    records = [
        {"RENT_STATN_ID": "ST-1", "RENT_STATN_NM": "A", "USE_CNT": "3", "MOVE_METER": "100", "MOVE_TIME": "20"},
        {"RENT_STATN_ID": None, "RENT_STATN_NM": "B", "USE_CNT": "1", "MOVE_METER": "10", "MOVE_TIME": "5"},
        {"RENT_STATN_ID": "ST-2", "RENT_STATN_NM": "C", "USE_CNT": "-1", "MOVE_METER": "10", "MOVE_TIME": "5"},
    ]
    accepted, rejected = clean_records(records)
    assert len(accepted) == 1
    assert len(rejected) == 2
    assert all("reason" in row for row in rejected)


def test_aggregator_returns_contracted_columns():
    from seoul_bike_etl.aggregator import REQUIRED_OUTPUT_COLUMNS, aggregate

    records_by_date = {
        "2026-06-27": [
            {"RENT_STATN_ID": "ST-1", "RENT_STATN_NM": "A", "USE_CNT": 1.0, "MOVE_METER": 10.0, "MOVE_TIME": 2.0},
        ],
        "2026-06-28": [],
    }
    result = aggregate(records_by_date, "2026-06-27", "2026-06-28")
    assert len(result) == 1
    assert set(result[0].keys()) == set(REQUIRED_OUTPUT_COLUMNS)
```

- [ ] **Step 2: 실패 확인 (아직 `sys.path`에 `src`가 없는 상태 기준)**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
pip install -r requirements-dev.txt -r requirements.txt
pytest tests/test_project_contract.py -v
```

Expected: `pyproject.toml`의 `pythonpath = ["src"]` 설정이 아직 없다면 `ModuleNotFoundError: seoul_bike_etl`로 실패. (Task 1에서 이미 `pyproject.toml`에 해당 설정을 넣었으므로, 여기서는 대신 `AIRFLOW__CORE__EXECUTOR` 등 Airflow 설정이 없어 DAG 임포트가 실패할 수 있다 — 아래 Step 3 진행 후 재확인)

- [ ] **Step 3: 필요한 최소 환경변수 설정 후 재실행**

```bash
export AIRFLOW_HOME=$(mktemp -d)
export SEOUL_BIKE_SQL_DIR=$(pwd)/sql
export SEOUL_BIKE_DATA_DIR=$(pwd)/data
pytest tests/test_project_contract.py -v
```

Expected: 3개 테스트(`test_dag_imports_and_has_expected_task_ids`, `test_cleaner_rejects_missing_and_negative_values`, `test_aggregator_returns_contracted_columns`) 모두 PASS.

- [ ] **Step 4: 커밋**

```bash
git add tests/test_project_contract.py
git commit -m "test(W6M1): DAG task id / cleaner / aggregator 계약 검증 테스트"
```

---

## Task 10: README.md

**Files:**
- Create: `missions/W6/M1_Airflow_Bike_ETL/README.md`

**Interfaces:**
- Consumes: 앞선 모든 Task의 산출물 경로/명령어
- Produces: 제출 요구사항인 "설치/실행/클렌징 규칙" 문서

- [ ] **Step 1: `README.md` 작성**

```markdown
# Seoul Public Bike 2-Day Station Usage ETL (Airflow)

서울 열린데이터광장 `tbCycleRentUseDayInfo` API에서 2026-06-27, 2026-06-28 2일치 데이터를
단일 Airflow DAG run에서 수집·정제·집계하여 MySQL `station_period_usage` 테이블에
멱등적으로 적재하는 파이프라인입니다.

## 폴더 구조

- `dags/seoul_bike_period_usage.py` — Airflow DAG 정의
- `src/seoul_bike_etl/` — API 클라이언트, 정제, 집계, 설정 모듈
- `sql/` — 테이블 생성 DDL, 제출용 최종 결과 조회 쿼리
- `tests/test_project_contract.py` — 계약(contract) 검증 (전체 로직 커버리지 목적 아님)
- `data/{raw,processed,rejected}/` — 단계별 중간 산출물(날짜 키로 덮어쓰기, 멱등)
- `screenshots/` — DAG 성공 실행 스크린샷 제출용

## 1. 사전 준비

1. Docker, Docker Compose 설치
2. 서울 열린데이터광장에서 `tbCycleRentUseDayInfo` 사용 인증키 발급
3. `.env` 파일 생성:

```bash
cp .env.example .env
# .env를 열어 SEOUL_API_KEY, MYSQL_* 값을 실제 값으로 채운다
```

## 2. 실행

```bash
docker compose up --build -d
```

- Airflow UI: http://localhost:8080 (Airflow 3.x 기본 Simple Auth Manager 계정: `admin` / `admin`.
  로그인이 안 되면 `docker compose logs airflow-api-server | grep -i password`로 자동 생성된
  비밀번호를 확인한다.)
- MySQL: `localhost:3307` (컨테이너 내부에서는 `mysql:3306`)

DAG `seoul_bike_period_usage`를 Airflow UI에서 Unpause 후 "Trigger DAG"로 실행합니다.
기본 파라미터(`start_date=2026-06-27`, `end_date=2026-06-28`)만으로 두 날짜가 하나의
DAG run에서 모두 처리됩니다. 다른 기간을 돌리고 싶다면 "Trigger DAG w/ config"에서
`start_date`/`end_date`를 바꿔서 트리거하면 됩니다.

CLI로 트리거하려면:

```bash
docker compose exec airflow-scheduler airflow dags trigger seoul_bike_period_usage
```

## 3. 데이터 클렌징 규칙

다음 조건에 해당하는 행은 **전부 제외**하고, 제외된 행은 사유(`reason`)와 함께
`data/rejected/{date}.json`에 별도 저장합니다.

| 사유(`reason`) | 조건 |
|---|---|
| `missing_station_id` | `RENT_STATN_ID`가 null 또는 빈 문자열 |
| `missing_station_name` | `RENT_STATN_NM`이 null 또는 빈 문자열 |
| `non_numeric_use_cnt` / `non_numeric_move_meter` / `non_numeric_move_time` | 해당 컬럼이 숫자로 변환 불가 |
| `negative_use_cnt` / `negative_move_meter` / `negative_move_time` | 해당 컬럼 값이 음수 |

정제 전/후 건수와 제외 비율은 `clean_day` task 로그에 남습니다. 제외 비율이 20%를
초과하면 WARNING 로그를 남기되 파이프라인은 계속 진행하고, 정제 후 남은 레코드가
0건이면 치명적 오류로 간주해 task를 실패시킵니다.

## 4. 멱등성

- 원본/정제 결과 파일은 `data/{raw,processed,rejected}/{date}.json`처럼 **날짜를 파일명으로
  사용**해 재실행 시 항상 덮어씁니다.
- MySQL 적재는 대상 기간(`period_start_date`, `period_end_date`)에 해당하는 기존 행을
  **DELETE 후 INSERT**하는 트랜잭션으로 처리합니다. 따라서 동일 기간을 여러 번 재실행해도
  중복되거나 충돌하는 행이 생기지 않습니다.

## 5. 데이터 품질(DQ) 체크

| 단계 | 체크 항목 | 기준 초과 시 |
|---|---|---|
| 수집(`extract_day`) | 응답 스키마 컬럼 일치, 0건 수집 여부 | 치명적 → task 실패 / 경미 → WARNING |
| 정제(`clean_day`) | 정제 전/후 건수, 제외율 | 전량 제외 → task 실패 / 제외율 20% 초과 → WARNING |
| 집계(`aggregate`) | station_id 중복 여부 | 중복 발견 → task 실패 |

## 6. 최종 결과 조회

```bash
docker compose exec mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" \
  < sql/final_result.sql
```

## 7. 테스트

```bash
pip install -r requirements-dev.txt -r requirements.txt
pytest tests/ -v
```
```

- [ ] **Step 2: 커밋**

```bash
git add README.md
git commit -m "docs(W6M1): README 작성 - 설치/실행/클렌징 규칙"
```

---

## Task 11: 종단 실행 검증 및 제출물 수집

**Files:**
- Modify: `missions/W6/M1_Airflow_Bike_ETL/screenshots/` (스크린샷 추가)
- 없음 외 코드 변경 사항 없음 (검증 및 산출물 수집 태스크)

**Interfaces:**
- Consumes: Task 1~10의 모든 산출물
- Produces: 실제 API 키로 동작을 검증한 실행 로그, DAG 성공 스크린샷, MySQL 최종 결과

- [ ] **Step 1: `.env`에 실제 API 키/DB 자격증명 입력 확인**

```bash
cd missions/W6/M1_Airflow_Bike_ETL
grep -q "CHANGE_ME" .env && echo "WARNING: .env에 아직 CHANGE_ME 값이 남아있습니다" || echo ".env 값 확인 완료"
```

Expected: `.env 값 확인 완료` (실제 값으로 모두 교체된 상태).

- [ ] **Step 2: 스택 기동 및 헬스 체크**

```bash
docker compose up --build -d
docker compose ps
```

Expected: `postgres`, `mysql` 컨테이너 STATUS가 `healthy`, `airflow-init`이 `Exited (0)`,
`airflow-api-server`/`airflow-scheduler`/`airflow-dag-processor`가 `Up`.

- [ ] **Step 3: DAG unpause 및 트리거**

```bash
docker compose exec airflow-scheduler airflow dags unpause seoul_bike_period_usage
docker compose exec airflow-scheduler airflow dags trigger seoul_bike_period_usage
```

- [ ] **Step 4: DAG run 완료까지 상태 폴링**

```bash
watch -n 5 'docker compose exec airflow-scheduler airflow dags list-runs -d seoul_bike_period_usage'
```

Expected: 최신 run의 state가 `success`로 바뀔 때까지 대기. `failed`가 뜨면
`docker compose logs airflow-scheduler`와 Airflow UI의 실패 task 로그로 원인을 확인하고
(예: API 4xx, 컬럼명 불일치 등) `src/seoul_bike_etl/api_client.py`의 `REQUIRED_COLUMNS`나
응답 파싱 로직을 실제 API 응답 구조에 맞게 조정한 뒤 재트리거한다.

- [ ] **Step 5: Airflow UI Grid 뷰 스크린샷 저장**

Airflow UI(`http://localhost:8080`)에서 `seoul_bike_period_usage` DAG의 Grid 뷰를 열어
모든 task가 성공(초록색)으로 표시된 화면을 캡처해 저장한다.

```bash
# 스크린샷 파일을 아래 경로에 저장했다고 가정
ls missions/W6/M1_Airflow_Bike_ETL/screenshots/dag_success.png
```

- [ ] **Step 6: MySQL 최종 결과 조회 및 출력 저장**

```bash
docker compose exec mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" \
  < sql/final_result.sql | tee sql/final_result_output.txt
```

Expected: `station_period_usage` 테이블에서 `period_start_date=2026-06-27`,
`period_end_date=2026-06-28`인 행들이 station별로 1건씩, `total_usage_count` 내림차순으로 출력됨.
station_id 중복이 없는지 눈으로 재확인한다.

- [ ] **Step 7: 재실행 멱등성 확인**

```bash
docker compose exec airflow-scheduler airflow dags trigger seoul_bike_period_usage
# 완료 후 재조회
docker compose exec mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" \
  -e "SELECT COUNT(*), COUNT(DISTINCT station_id) FROM station_period_usage WHERE period_start_date='2026-06-27' AND period_end_date='2026-06-28';"
```

Expected: `COUNT(*)`와 `COUNT(DISTINCT station_id)`가 동일(= 중복/충돌 없이 재적재됨).

- [ ] **Step 8: 커밋**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W6/M1_Airflow_Bike_ETL/screenshots missions/W6/M1_Airflow_Bike_ETL/sql/final_result_output.txt
git commit -m "docs(W6M1): DAG 성공 실행 스크린샷 및 최종 결과 쿼리 출력 추가"
```

---

## Self-Review Notes (작성자 메모)

- **Spec coverage:** 설계 문서(`docs/superpowers/specs/2026-08-05-airflow-seoul-bike-etl-design.md`)의 모든 섹션(아키텍처/DAG/DQ/에러처리/테스트/제출물)이 Task 1~11에 매핑됨.
- **API 스키마 가정:** `RENT_STATN_ID`, `RENT_STATN_NM`, `USE_CNT`, `MOVE_METER`, `MOVE_TIME`, 응답 코드 `INFO-000`/`INFO-200`은 서울 열린데이터광장의 일반적인 규약을 따른 가정이다. Task 11 Step 4에서 실제 API 응답과 다르면 `api_client.py`/`config.REQUIRED_COLUMNS`를 조정하도록 명시함.
- **Airflow 3.x API 확인:** `from airflow.sdk import DAG, Param, task`, `api-server`/`dag-processor`가 Airflow 3.0+에서 필수 분리된 서비스라는 점, Simple Auth Manager 기본 계정을 Context7로 공식 문서 대비 확인함.
