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
        {"RENT_ID": "ST-1", "RENT_NM": "A", "USE_CNT": "3", "MOVE_METER": "100", "MOVE_TIME": "20"},
        {"RENT_ID": None, "RENT_NM": "B", "USE_CNT": "1", "MOVE_METER": "10", "MOVE_TIME": "5"},
        {"RENT_ID": "ST-2", "RENT_NM": "C", "USE_CNT": "-1", "MOVE_METER": "10", "MOVE_TIME": "5"},
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
