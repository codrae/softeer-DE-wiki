import json
import re
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime

source_link = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

# Wikipedia는 기본 python-requests User-Agent를 403으로 차단하므로 UA를 명시한다.
HEADERS = {"User-Agent": "Mozilla/5.0 (softeer-de-bootcamp; educational use)"}
GDP_JSON = "Countries_by_GDP.json"
REGION_JSON = "Countries_by_Region.json"
GDP_PROCESSED_JSON = "Countries_by_GDP_processed.json"

def log(message,):
    """
    time,log 형식으로 기록
    시간은 Year-Monthname-Day-Hour-Minute-Second 포맷 사용
    :return:
    """
    ts = datetime.now().strftime("%Y-%b-%d-%H-%M-%S")
    line = f"{ts}\t{message}"
    with open("etl_project_log.txt", "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)

def init():
    """
    초기 데이터 테이블 생성
    Region별 국가 정보 삽입 (메타 데이터)
    :return:
    """
    df_gdp = pd.DataFrame(columns=["Country", "GDP_USD_billion"])
    df_region = pd.DataFrame(columns=["Country", "Region"])

    # 위키 표에 각주(<ref group="r">)가 없는 리전은 회원국 명단을 미리 적재.
    # 국가명은 메인 GDP 표의 표기 그대로 사용해야 이후 join이 됨(예: DR Congo, Ivory Coast).
    # 기준: IMF WEO 2025 April 회원국 정의.
    predefined = {
        "European Union": [
            "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus",
            "Czech Republic", "Denmark", "Estonia", "Finland", "France",
            "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia",
            "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland",
            "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden",
        ],  # 27개국
        "Euro Area": [
            "Austria", "Belgium", "Croatia", "Cyprus", "Estonia", "Finland",
            "France", "Germany", "Greece", "Ireland", "Italy", "Latvia",
            "Lithuania", "Luxembourg", "Malta", "Netherlands", "Portugal",
            "Slovakia", "Slovenia", "Spain",
        ],  # 20개국
        "Latin America & Caribbean": [
            "Brazil", "Mexico", "Colombia", "Peru", "Chile", "Argentina",
            "Ecuador", "Bolivia", "Paraguay", "Uruguay", "Venezuela",
            "Costa Rica", "Guatemala", "Dominican Republic", "Honduras",
            "El Salvador", "Nicaragua", "Panama", "Guyana", "Suriname",
            "Haiti", "Jamaica", "Trinidad and Tobago", "Bahamas", "Barbados",
            "Grenada", "Saint Lucia", "Saint Vincent and the Grenadines",
            "Antigua and Barbuda", "Saint Kitts and Nevis", "Dominica",
        ],  # 31개국
        "Sub-Saharan Africa": [
            "South Africa", "Nigeria", "Kenya", "Ethiopia", "Ghana", "Zambia",
            "Zimbabwe", "Tanzania", "Uganda", "Cameroon", "Angola", "DR Congo",
            "Ivory Coast", "Senegal", "Mali", "Burkina Faso", "Niger", "Chad",
            "Benin", "Gabon", "Congo", "Sierra Leone", "Liberia", "Mozambique",
            "Rwanda", "Namibia", "Madagascar", "Malawi", "Botswana", "Eswatini",
            "Lesotho", "Guinea", "Guinea-Bissau", "Mauritius", "Seychelles",
            "Comoros", "Togo", "Equatorial Guinea", "Burundi",
            "Central African Republic",
        ],  # 40개국
    }
    rows = [(c, r) for r, members in predefined.items() for c in members]
    df_region = pd.DataFrame(rows, columns=["Country", "Region"])

    return df_gdp, df_region

def _get_soup():
    """source_link를 fetch해 BeautifulSoup으로 파싱해 반환한다. (국가·region 추출 공용)"""
    resp = requests.get(source_link, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def extract_gdp(soup=None):
    """
    wiki를 통해서 국가별 GDP정보를 추출 (IMF 추정치 열, 단위 million USD)
    RAW 상태(콤마 포함 문자열) 그대로 Countries_by_GDP.json 에 저장.
    콤마 제거·결측 판정·형변환·단위통일은 transform 담당(정제 로직 분리).
    :param soup: 공유 BeautifulSoup. None이면 직접 fetch(단독 실행용).
    :return: [{"country": str, "gdp_mil_usd": str}, ...]
    """
    log("extract_gdp 시작")
    if soup is None:
        soup = _get_soup()
    table = soup.find("table", class_="wikitable")

    records = []
    # [0]=헤더, [1]=World 합계 행 → 건너뛰고 국가 행부터 파싱
    for tr in table.find_all("tr")[2:]:
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        # 국가명은 <a> 링크 텍스트로 추출해야 각주(China[n 1])가 제거된다
        link = cells[0].find("a")
        country = link.get_text(strip=True) if link else cells[0].get_text(strip=True)
        if not country or country == "World":
            continue
        # cells[1] = IMF 열. 셀 텍스트를 RAW 그대로 보존(정제 안 함).
        # 결측('—N/a' 등)·연도주석('34,497 (2025)')·콤마 정규화는 transform이 일괄 처리.
        records.append({
            "country": country,
            "gdp_mil_usd": cells[1].get_text(strip=True),
        })

    with open(GDP_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    log(f"extract_gdp 완료: {len(records)}개국 -> {GDP_JSON}")
    return records

def _country_slug(href):
    """위키 문서 href -> 국가 슬러그. 접두사를 벗겨 문서명 불규칙을 흡수한다.
    예: 'Economy_of_South_Korea' -> 'South_Korea', 'GDP_of_Romania' -> 'Romania'."""
    slug = href.rsplit("/wiki/", 1)[-1]
    for prefix in ("Economy_of_the_", "Economy_of_", "GDP_of_the_", "GDP_of_"):
        if slug.startswith(prefix):
            return slug[len(prefix):]
    return slug

def extract_region(soup=None, region_seed=None):
    """
    region 그룹 표의 각주에서 리전별 회원국을 추출하고,
    World(전체국가) + 하드코딩 4개 리전과 합쳐
    (Country, Region) 매핑을 Countries_by_Region.json 에 저장한다.

    각주 멤버는 IMF 공식명(Korea, Türkiye 등)이라 표시명이 GDP 표와 다르므로,
    링크 href의 국가 슬러그로 메인 표 표시명에 매핑해 표기 차이를 흡수한다.
    :param soup: 공유 BeautifulSoup. None이면 직접 fetch(단독 실행용).
    :param region_seed: init()이 만든 하드코딩 리전 df. None이면 직접 init() 호출(단독 실행용).
    :return: [{"country": str, "region": str}, ...]
    """
    log("extract_region 시작")
    if soup is None:
        soup = _get_soup()
    tables = soup.find_all("table", class_="wikitable")
    main_table, group_table = tables[0], tables[1]

    # 메인 표에서 {국가 슬러그 -> 표시명} 사전 구축 (조인 정본)
    slug_to_name = {}
    for tr in main_table.find_all("tr")[2:]:
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        a = cells[0].find("a")
        if not a or not a.get("href"):
            continue
        name = a.get_text(strip=True)
        if name == "World":
            continue
        slug_to_name[_country_slug(a["href"])] = name

    rows = []
    # (1) 각주가 있는 리전: 각주 노트의 국가 링크 -> 슬러그 -> 표시명
    for tr in group_table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        ref = cells[0].select_one('sup a[href^="#cite_note"]')
        if not ref:
            continue
        region = cells[0].get_text(strip=True)
        sup = cells[0].find("sup")
        if sup:  # 리전명에서 각주 마커([r 1] 등) 제거
            region = region.replace(sup.get_text(strip=True), "").strip()
        note = soup.find(id=ref["href"][1:])
        for a in note.find_all("a"):
            if a.get_text(strip=True) == "↑" or not a.get("href"):
                continue  # 각주 맨 앞 백링크(↑)는 건너뜀
            name = slug_to_name.get(_country_slug(a["href"]))
            if name:
                rows.append((name, region))
            else:
                log(f"  [region 미매칭] {region}: {a.get_text(strip=True)}")

    # (2) World = 메인 표의 전체 국가
    rows += [(name, "World") for name in slug_to_name.values()]

    # (3) init()의 하드코딩 리전(EU/Euro Area/LAC/SSA) 병합
    if region_seed is None:
        _, region_seed = init()
    rows += list(region_seed.itertuples(index=False, name=None))

    records = [{"country": c, "region": r} for c, r in rows]
    with open(REGION_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    log(f"extract_region 완료: {len(records)}행 -> {REGION_JSON}")
    return records

def extract(region_seed=None):
    """
    extract 단계 오케스트레이션: 페이지를 한 번만 fetch해
    국가 GDP(extract_gdp)와 리전 매핑(extract_region)을 모두 추출한다.
    :param region_seed: init()이 만든 하드코딩 리전 df.
    """
    log("extract 시작")
    soup = _get_soup()                     # 페이지 1회 fetch를 두 추출이 공유
    extract_gdp(soup)
    extract_region(soup, region_seed)
    log("extract 완료")

def _to_billion(text):
    """RAW GDP 문자열 -> billion USD(float, 2자리). 결측/파싱불가 -> None.
    예: '32,383,920' -> 32383.92, '34,497 (2025)' -> 34.5, '—N/a' -> None."""
    text = re.sub(r"\(.*?\)", "", text)      # 연도주석 '(2025)' 제거
    text = text.replace(",", "").strip()     # 천단위 콤마 제거
    if not re.fullmatch(r"\d+(\.\d+)?", text):
        return None                          # 결측('—N/a')·비수치 -> None
    return round(float(text) / 1000, 2)      # million -> billion

def transform():
    """
    RAW(Countries_by_GDP.json) + 리전 매핑(Countries_by_Region.json)을 읽어
    STAGING(Countries_by_GDP_processed.json)을 만든다.
    - GDP 정제: 연도주석/콤마 제거, 결측 판정
    - 단위통일: million -> 1B USD, 소수점 2자리
    - GDP 내림차순 정렬
    - 리전 조인(long-format, 결정 a): 다리전 국가는 리전 수만큼 행 복제
    :return: 병합된 DataFrame [Country, GDP_USD_billion, Region]
    """
    log("transform 시작")
    with open(GDP_JSON, encoding="utf-8") as f:
        gdp = json.load(f)
    with open(REGION_JSON, encoding="utf-8") as f:
        region = json.load(f)

    df = pd.DataFrame(gdp).rename(columns={"country": "Country"})
    df["GDP_USD_billion"] = df["gdp_mil_usd"].map(_to_billion)
    df = df[["Country", "GDP_USD_billion"]]

    df_region = pd.DataFrame(region).rename(columns={"country": "Country", "region": "Region"})
    merged = df.merge(df_region, on="Country", how="left")
    merged = merged.sort_values(
        "GDP_USD_billion", ascending=False, na_position="last"
    ).reset_index(drop=True)

    merged.to_json(GDP_PROCESSED_JSON, orient="records", force_ascii=False, indent=2)
    log(f"transform 완료: {len(merged)}행 -> {GDP_PROCESSED_JSON}")
    return merged

def load():
    """
    최신 정보로 데이터 업데이트
    이후 GDP가 100B USD 이상이 되는 국가만을 구해서 화면에 출력
    각 Region별로 top5 국가의 GDP 평균을 구해서 화면에 출력
    :return:
    """

def run_pipe():
    """
    스케쥴링을 통해 매년 2회 자료를 제공하는 시점에 맞춰 스케줄링.
    혹은 전체 파이프라인을 재실행하여 정보를 업데이트.

    현재 구현: init -> extract (transform/load는 이후 단계)
    :return:
    """
    log("===== run_pipe 시작 =====")
    _, df_region = init()          # 하드코딩 리전 메타데이터 seed
    extract(df_region)             # 국가 GDP + 리전 매핑 (1회 fetch로 둘 다)
    log("===== run_pipe 종료 (extract 단계까지) =====")