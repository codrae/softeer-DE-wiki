import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime

source_link = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

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

def extract():
    """
    wiki를 통해서 국가별 GDP정보를 추출
    Countries_by_GDP.json 이라는 파일로 저장
    :return:
    """

def transform():
    """
    GDP가 높은 순서대로 정렬
    GDP의 단위를 1B USD로 통일
    GDP값은 소수점 2자리까지만 표시
    :return:
    """

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
    :return:
    """