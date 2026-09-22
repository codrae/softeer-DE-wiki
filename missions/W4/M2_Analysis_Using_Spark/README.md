# W4M2 - NYC TLC Trip Data 분석 (Spark + Jupyter)

NYC Taxi and Limousine Commission(TLC) Trip Record Data를 PySpark로 적재·정제·분석하고,
Open-Meteo 날씨 데이터와 상관관계를 분석하는 미션입니다.

## 미션 요구사항

### 학습 목표
Apache Spark 지식을 적용해 NYC TLC Trip Record Data를 처리·분석하는 Spark 애플리케이션을
Python으로 작성한다.

### 기능 요구사항

| 항목 | 요구 내용 |
|---|---|
| Data Ingestion | 여러 달/여러 해 데이터를 효율적으로 적재. CSV, Parquet 등 다양한 포맷 처리 |
| Data Cleaning | 결측값 제거 또는 대치, 시간 필드를 표준 timestamp로 변환, 비정상 값(음수 시간/거리) 필터링 |
| Metrics | 평균 이동시간·평균 이동거리 계산, 사람이 읽을 수 있는 형태로 저장·출력 |
| Peak Hours | 시간대별 출발 트립 수로 "피크아워"를 정의하고 분포를 시각화, 최다 시간대 강조 |
| Weather | 기온·강수량 등 날씨와 수요의 상관관계 분석, **통계적 방법으로 검증** |
| Output | 최종 결과를 파일(CSV/Parquet)로 저장, 시각화(막대·선 그래프)로 뒷받침 |

### 프로그래밍 요구사항
- Spark 환경 구성 → TLC 데이터를 Spark DataFrame으로 로드 → 정제 → 지표 계산
  → 피크아워 식별 → 날씨 영향 분석(필요 시 추가 데이터셋 사용)
- **Jupyter Notebook으로 결과를 시각화**할 것

### 팀 활동 요구사항
> 이 데이터가 사람이 운행하는 차량이 아니라 **'자율주행차' 데이터**라면 어떤 Data Product를
> 만들면 좋을까? 아이디어를 수립하고 Prototype을 만들어 보세요.

참고: [Waymo Open Dataset](https://waymo.com/open/), [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## 아키텍처

```
M2_Analysis_Using_Spark/
├── Dockerfile              # Spark 3.5.9 + Python 3.10 + JDK17 베이스에
│                           #   pandas/pyarrow/requests/scipy/matplotlib/jupyterlab 추가
├── docker-compose.yml
│   ├── spark-master
│   ├── spark-worker-1/2    # 2 core / 2g
│   └── jupyter             # 같은 spark-net에 조인하는 driver 컨테이너
├── apps/
│   ├── download_data.py    # TLC parquet + Open-Meteo 날씨 다운로드
│   ├── pipeline.py         # 적재/정제/집계 함수 모듈
│   └── tests/              # pytest
└── notebooks/
    └── analysis.ipynb      # 7단계 셀 구성
```

드라이버(Jupyter)가 같은 Docker 네트워크에서 `spark://spark-master:7077`로 client 모드
접속하므로 7077을 호스트에 노출할 필요가 없습니다. 호스트에는 master UI `8080`,
worker UI `8081`/`8082`, JupyterLab `8888`만 매핑합니다.

데이터 위치 — 원본은 `missions/data/raw/{tlc,weather}/`, 결과는 `missions/data/output/`
(모두 `.gitignore` 대상).

## 파이프라인 (`notebooks/analysis.ipynb`)

1. **환경설정** — SparkSession 생성, 클러스터 연결 확인 (executor 수·Spark 버전 출력)
2. **데이터 적재** — `download_data.py`로 대상 연-월의 TLC parquet과 Open-Meteo hourly 날씨
   CSV를 확인/다운로드 (이미 있으면 스킵)
3. **클리닝** — timestamp 변환, 필수 컬럼 null 제거, 이상치 필터링
   (`trip_duration_min` 0~180분, `trip_distance` 0~100마일, `fare_amount >= 0`)
4. **지표 계산** — 평균 이동시간·평균 이동거리
5. **피크아워 분석**
6. **날씨 상관분석** — `scipy.stats`로 통계 검증
7. **시각화/출력**

## 실행

```bash
docker compose up -d --build
# JupyterLab: http://localhost:8888 → notebooks/analysis.ipynb

# 테스트
pytest apps/tests -v
```

## 설계 문서

코드보다 설계를 먼저 작성하는 워크플로를 따랐습니다.

- [설계(spec)](docs/superpowers/specs/2026-07-23-nyc-tlc-spark-analysis-design.md)
- [구현 계획(plan)](docs/superpowers/plans/2026-07-23-nyc-tlc-spark-analysis.md)

## 4주차 리뷰 피드백 (보고서 작성)

미니 데이터 프로덕트 프로토타입 발표에 대해 받은 피드백입니다.

- **문제 정의와 해결 가능성은 별개다.** "논리적인 문제 정의 레이어"와 "그 문제를 풀 수 있는가
  (예: 데이터를 구할 수 있는가)"는 다른 이야기다. 문제가 정의됐다고 반드시 풀어야 하는 것도
  아니며, **"누구"의 "어떤" 문제**를 해결할지 고민해야 한다.
  - 예: 화장실 대기줄 문제도 청소부 임금이 비싸다면 **고용주의 비용 절감 문제**로 바뀐다.
- **문제를 좁게 만든 뒤 넓히자** (고객 니즈를 세밀하게).
- **Time logging 방식이 아니라 보고서 형식으로 쓸 것.** 두괄식 1문단 → 누구의 어떤 문제를
  풀었는가에 대한 보충 설명 → 어떻게 했는가.
- **HOW와 뒷받침 데이터가 필요하다.** "누구의 어떤 문제를 풀겠습니다 / 얼마나 큰 가치가 있어서
  꼭 풀면 좋겠습니다 / 우리는 이런 방식으로 풀겠습니다" 구조로.
- **범위를 두괄식에 가두지 말 것.** 예를 들어 "대중교통 사각지대 발견"이 주제인데 실제로는
  JFK 공항이라는 niche를 다뤘다면, JFK는 두괄식/주제가 아니라 아래에서 context를 좁히는
  부분에서 언급한다. 비즈니스 가치도 JFK에 한정하지 않고 설명해야 값이 더 커 보인다.
  기대효과로 "다른 공항/도심지로 확장 가능"을 덧붙인다.
- **서술식이 아니라 설명형으로.** "기존 오탐을 제거했다" → "우리 시스템은 오탐 제거 기능을
  가지고 있다".
- **보고서의 Depth는 일정해야 한다.** 추상적인 이야기에서 점점 디테일로 가는 것이 좋고,
  아키텍처나 기술적인 부분은 뒤쪽에 두는 편이 낫다.
- LLM이 쓴 것 중 불필요한 내용은 제거할 것.
