# W1M3 - 국가별 GDP ETL 파이프라인

Wikipedia의 "List of countries by GDP (nominal)" 표에서 국가별 GDP를 스크래핑해 정제한 뒤
JSON과 SQLite에 적재하는 ETL 파이프라인입니다.

## 파일 구성

| 파일 | 설명 |
|---|---|
| `etl_project_gdp.py` | 파일(JSON) 기반 ETL 파이프라인 |
| `etl_project_gdp_with_sql.py` | 동일 파이프라인의 SQLite 적재·조회 버전 |
| `etl_project_log.txt` | 실행 로그 (스크립트가 생성) |
| `raw.json` / `Countries_by_GDP.json` / `Countries_by_Region.json` | 단계별 산출물 |
| `World_Economies.db` | SQLite 결과 DB |

> 산출물(JSON/DB/로그)은 모두 `.gitignore` 대상이며, 스크립트 실행 시 재생성됩니다.

## 파이프라인 단계

1. **init** — 빈 GDP 스캐폴드 DataFrame 생성 + 각주가 없는 리전의 회원국 메타데이터를 미리 적재
   (기준: IMF WEO 2025 April 회원국 정의)
2. **extract** — `BeautifulSoup`으로 GDP 표와 리전 정보를 추출해 `raw.json`에 원본 저장
   - Wikipedia는 기본 `python-requests` User-Agent를 403으로 차단하므로 UA를 명시
   - `Korea` / `South Korea`처럼 표기가 다른 국가는 **링크를 파싱해** 매칭
3. **transform** — 주석·콤마 제거, 결측 판정, 단위를 1B USD로 통일하고 소수점 2자리 반올림,
   GDP 내림차순 정렬
4. **load** — JSON 저장 (또는 SQLite 테이블 적재 후 쿼리 검증)

## 로깅 규약

`log()` 함수가 `시각, 메시지` 형식(쉼표 구분)으로 로그 파일과 화면에 동시 기록합니다.
시간 포맷은 `Year-Monthname-Day-Hour-Minute-Second` (예: `2026-Jul-03-14-22-01`).

## 팀 논의 내용

**Q. Wikipedia가 아닌 IMF 홈페이지에서 직접 가져올 수는 없을까?**
- IMF 사이트는 크롤링이 막혀 있어 직접 스크래핑은 불가능하다.
- 다만 GDP가 기록된 excel 파일이 사이트에 있고(갱신을 계속 해줘야 한다는 단점 존재),
  **IMF가 제공하는 API**가 있으므로 이를 활용해 필요한 정보를 가져오는 방향이 적절하다.

**Q. 데이터가 갱신되면 과거 데이터는 어떻게 해야 할까? 과거 데이터 조회가 필요하다면?**
- GDP 기준 사업성 평가가 목적이라면 기본적으로 과거 데이터를 유지할 필요 없이
  **UPSERT**로 기존 데이터를 갱신하면 된다.
- 과거 조회가 필요하다면 **데이터가 기록(업데이트)된 시점을 함께 기입해 누적 관리**해야 한다.
  가장 단순하게는 현재 행에 반기별 컬럼을 추가하는 수평적 확장도 방법이 된다.
- IMF가 임의로 규정한 **Region의 변동성**까지 보장하려면 Region별 국가 목록의 이력도
  별도로 관리해야 한다 (datetime 컬럼 추가 후 append 방식 적재).
- 더 나아가 **ETL 대신 ELT**로 전환하는 방안도 논의했다. 원본을 먼저 적재한 뒤 변환하고,
  갱신 시 덮어쓰지 않고 append해 시점별 이력을 보존하는 구조다.
  → 이 경우 적재 대상은 데이터 웨어하우스보다 **Data Lake**가 더 적합해 보인다.

## 실행

```bash
python etl_project_gdp.py           # JSON 기반
python etl_project_gdp_with_sql.py  # SQLite 기반
```
