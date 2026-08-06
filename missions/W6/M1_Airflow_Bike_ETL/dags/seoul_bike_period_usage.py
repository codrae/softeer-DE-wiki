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
    max_active_runs=1,
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

        dates_in_period = sorted(records_by_date.keys())
        if len(dates_in_period) != 2:
            raise AirflowException(
                f"이 파이프라인은 정확히 2일 기간만 지원합니다 (요청된 날짜: {dates_in_period})"
            )

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
