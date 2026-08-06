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
    body = payload.get(config.API_RESPONSE_ROOT_KEY)
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
