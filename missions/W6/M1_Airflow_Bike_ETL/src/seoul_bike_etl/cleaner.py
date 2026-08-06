"""Row-level cleaning rules for Seoul bike usage records.

정제 규칙 (팀 합의: 결측치/이상치는 모두 제외):
- RENT_ID, RENT_NM(원본 API 필드명) 결측(null/빈 문자열)
- USE_CNT, MOVE_METER, MOVE_TIME 숫자 변환 실패
- 위 세 수치 중 하나라도 음수

원본 API 필드명(RENT_ID/RENT_NM)은 내부 계약 필드명(RENT_STATN_ID/RENT_STATN_NM)으로
이 함수에서 변환되어 이후 단계(aggregator 등)에는 항상 RENT_STATN_ID/RENT_STATN_NM으로 전달된다.
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
    if _is_missing(record.get("RENT_ID")):
        return None, {**record, "reason": "missing_station_id"}
    if _is_missing(record.get("RENT_NM")):
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
        "RENT_STATN_ID": str(record["RENT_ID"]),
        "RENT_STATN_NM": str(record["RENT_NM"]),
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
