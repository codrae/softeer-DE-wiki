"""Configuration constants and environment loading for the Seoul bike ETL pipeline."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

# --- Seoul Open API ---
API_BASE_URL = "http://openapi.seoul.go.kr:8088"
API_SERVICE_NAME = "tbCycleRentUseDayInfo"
# 실제 API 응답의 최상위 키는 URL 서비스명과 다르게 "tb" 접두사가 빠진
# lower camelCase("cycleRentUseDayInfo")로 내려온다. (Task 11 실제 API 연동 검증 중 확인)
API_RESPONSE_ROOT_KEY = "cycleRentUseDayInfo"
PAGE_SIZE = 1000
RETRY_BACKOFF_SECONDS = [2, 4, 8]
REQUEST_TIMEOUT_SECONDS = 30

# API가 각 row에 포함해야 하는 필수 컬럼과, 그중 숫자 변환/음수 검사가 필요한 컬럼.
REQUIRED_COLUMNS = ["RENT_ID", "RENT_NM", "USE_CNT", "MOVE_METER", "MOVE_TIME"]
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
