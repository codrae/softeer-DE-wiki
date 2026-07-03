"""
etl_project_gdp_with_sql.py

기존 etl_project_gdp.py의 ETL(init/extract/transform)을 그대로 재사용하고,
load 단계만 SQLite 적재 + SQL 질의 출력으로 대체한 버전.

- 추출·정제 데이터를 World_Economies.db 에 저장
  - Countries_by_GDP    (Country, GDP_USD_billion)  ← 요구 테이블
  - Countries_by_Region (Country, Region)           ← region top5 조인용 보조 테이블
- 화면 출력은 SQL Query로 수행
  - GDP 100B USD 이상 국가
  - Region별 top5 국가 GDP 평균
"""
import sqlite3
import pandas as pd
import etl_project_gdp as etl   # 기존 ETL 재사용 (init/extract/transform/log/상수)

DB_NAME = "World_Economies.db"
GDP_TABLE = "Countries_by_GDP"
REGION_TABLE = "Countries_by_Region"
GDP_THRESHOLD_BILLION = etl.GDP_THRESHOLD_BILLION  # 100

def load_to_db(gdp_df):
    """
    처리완료 GDP(gdp_df)와 리전 매핑(Countries_by_Region.json)을 SQLite 두 테이블로 적재.
    - Countries_by_GDP    : (Country, GDP_USD_billion)  ← 요구 테이블
    - Countries_by_Region : (Country, Region) 매핑 (region top5 조인용)
    :param gdp_df: etl.transform()이 반환한 [Country, GDP_USD_billion] df (국가 유일)
    :return: 열린 sqlite3 Connection
    """
    etl.log("load(SQL) 시작")
    region = pd.read_json(etl.REGION_JSON).rename(columns={"country": "Country", "region": "Region"})

    conn = sqlite3.connect(DB_NAME)
    gdp_df.to_sql(GDP_TABLE, conn, if_exists="replace", index=False)
    region.to_sql(REGION_TABLE, conn, if_exists="replace", index=False)
    conn.commit()
    etl.log(f"load(SQL) 완료: {GDP_TABLE} {len(gdp_df)}행 / {REGION_TABLE} {len(region)}행 -> {DB_NAME}")
    return conn

def report_over_threshold(conn):
    """SQL Query로 GDP 100B USD 이상 국가를 화면 출력."""
    query = f"""
        SELECT Country, GDP_USD_billion
        FROM {GDP_TABLE}
        WHERE GDP_USD_billion >= {GDP_THRESHOLD_BILLION}
        ORDER BY GDP_USD_billion DESC
    """
    rows = conn.execute(query).fetchall()
    print(f"\n=== GDP {GDP_THRESHOLD_BILLION}B USD 이상 국가 ({len(rows)}개국) ===")
    for rank, (country, gdp) in enumerate(rows, 1):
        print(f"{rank:3}. {country:32} {gdp:>12,.2f} B")

def report_region_top5(conn):
    """
    SQL Query(window function)로 Region별 top5 국가 GDP 평균을 화면 출력.
    ROW_NUMBER로 리전별 GDP 순위를 매겨 상위 5개만 평균낸다.
    """
    query = f"""
        SELECT Region, ROUND(AVG(GDP_USD_billion), 2) AS Top5_avg_GDP_billion
        FROM (
            SELECT r.Region AS Region,
                   g.GDP_USD_billion AS GDP_USD_billion,
                   ROW_NUMBER() OVER (
                       PARTITION BY r.Region ORDER BY g.GDP_USD_billion DESC
                   ) AS rn
            FROM {REGION_TABLE} r
            JOIN {GDP_TABLE} g ON r.Country = g.Country
            WHERE g.GDP_USD_billion IS NOT NULL
        )
        WHERE rn <= 5
        GROUP BY Region
        ORDER BY Top5_avg_GDP_billion DESC
    """
    rows = conn.execute(query).fetchall()
    print("\n=== Region별 top5 국가 GDP 평균 ===")
    for region, avg in rows:
        print(f"  {region:42} {avg:>12,.2f} B")

def run_pipe():
    """
    전체 파이프라인(SQL 버전): init -> extract -> transform -> load(DB) -> SQL 출력.
    추출·정제는 기존 etl_project_gdp 모듈을 재사용한다.
    """
    etl.log("===== run_pipe(SQL) 시작 =====")
    _, df_region = etl.init()
    etl.extract(df_region)
    gdp_df = etl.transform()
    conn = load_to_db(gdp_df)
    report_over_threshold(conn)
    report_region_top5(conn)
    conn.close()
    etl.log("===== run_pipe(SQL) 종료 =====")

if __name__ == "__main__":
    run_pipe()
