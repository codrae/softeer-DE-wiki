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
