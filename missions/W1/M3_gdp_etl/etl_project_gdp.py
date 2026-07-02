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