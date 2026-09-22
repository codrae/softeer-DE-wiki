# 현대자동차그룹 소프티어 부트캠프 DE/DA

데이터 엔지니어링 부트캠프(2026-07-01 ~ 2026-08-06) 주차별 미션 수행 기록입니다.
Python ETL → 병렬 처리/컨테이너 배포 → Hadoop MapReduce → Spark(RDD/DataFrame) → Airflow 오케스트레이션
순으로, 배치 데이터 파이프라인을 직접 구축하며 학습한 내용을 정리했습니다.

## 저장소 구조

```
.
├── missions/          # 주차별 미션 (W1 ~ W6)
│   └── data/          # 미션 공용 원본/산출 데이터 (gitignore 대상)
├── TeamWiki/          # 팀 미션 정리 및 공동 작업물
└── data/              # 로컬 데이터셋/출력 (gitignore 대상)
```

각 미션 디렉터리에는 대부분 자체 `README.md`(또는 `REPORT.md`)가 있으며, 실행 방법·설계 근거·결과
해석을 담고 있습니다. 아래 표의 링크를 따라가면 됩니다.

---

## W1 — 데이터 분석 기초와 ETL 파이프라인

| 미션 | 내용 | 산출물 |
|---|---|---|
| [M1 mtcars 분석](missions/W1/M1_analyze_mtcars) | pandas 기반 EDA: 컬럼 보정, `crosstab` 교차표, subplot 히스토그램, 상관계수 탐색 및 시각화 | `mtcars.ipynb` |
| [M2 SQL 튜토리얼](missions/W1/M2_SQL_Tutorial) | w3schools 샘플 DB(SQLite)로 SELECT/JOIN/집계 등 SQL 기본기 실습 | `sql.ipynb`, `w3schools.db` |
| [M3 국가별 GDP ETL](missions/W1/M3_gdp_etl) | Wikipedia GDP 표를 BeautifulSoup으로 스크래핑 → 정제(각주·콤마 제거, 1B USD 단위 통일) → JSON/SQLite 적재. 타임스탬프 로깅 함수 직접 구현 | `etl_project_gdp.py`, `etl_project_gdp_with_sql.py`, `World_Economies.db` |

**학습 포인트** — Extract/Transform/Load 단계 분리, 로깅 규약(`Year-Monthname-Day-H-M-S`),
리전 매핑을 위한 링크 파싱(국가 표기 불일치 해결), 동일 파이프라인의 파일 저장 버전과 RDBMS 버전 비교.

## W2 — 병렬 처리 · 감성 분석 · 컨테이너 배포

| 미션 | 내용 | 산출물 |
|---|---|---|
| [Multiprocessing M1~M4](missions/W2/Multiprocessing) | `Pool.map`(워커 풀), `Process`(인자 전달/`join`), `Queue`(공유 자원), 그리고 이 셋을 합친 all-in-one 워커 패턴 | `pool.py`, `process.py`, `queue.py`, `multiprocessing_all_in_one.py` |
| [M5 감성 분석](missions/W2/M5_Sentiment_Analysis) | Sentiment140 트윗 160만 건 수집·EDA, 긍/부정 분리, 불용어 제거 후 워드클라우드 생성 | `Sentiment_Analysis.ipynb` |
| [M5 추가 - 카카오프렌즈 리뷰 ETL](missions/W2/M5_Sentiment_Analysis/kakao_goods) | Angular SPA 대상 Scrapy + Playwright 크롤링(리뷰 탭 클릭 → '더보기' 반복), ETL 모듈 분리, 캐릭터별 키워드 워드클라우드 | `etl/{extract,transform,load}.py`, `kakao_review_wordcloud.ipynb` |
| [M6 Docker & EC2 배포](missions/W2/M6_Docker_EC2) | JupyterLab + Playwright(Chromium) + 나눔폰트 이미지를 빌드해 ECR 푸시 → EC2 배포. `user-data.sh`로 부팅 시 Docker 자동 설치 | `Dockerfile`, `user-data.sh` |

**학습 포인트** — [워드클라우드 작동 원리 정리](missions/W2/M5_Sentiment_Analysis/extra_assignment.md):
빈도 정렬·정규화 → `relative_scaling` 기반 폰트 크기 절충 → **적분 이미지(2차원 누적합)로 O(1) 충돌 검사** →
랜덤 색칠. "의미 있는 시각 변수는 글자 크기 하나뿐"이라는 해석상 주의점까지 문서화.

## W3 — Hadoop 클러스터 구축과 MapReduce

| 미션 | 내용 | 결과 |
|---|---|---|
| [M1 단일 노드 클러스터](missions/W3/M1_hadoop_single_node_cluster_on_docker) | Ubuntu 20.04 + OpenJDK 8 위에 Hadoop 2.10.2 직접 설치, SSH 자동화, entrypoint로 HDFS/YARN 데몬 자동 기동, 볼륨 영속성 검증 | Pseudodistributed 모드 동작 |
| [M2a 멀티 노드 클러스터](missions/W3/M2a_hadoop_multi_node_cluster_on_docker) | master 1 + worker 3 구성. 역할별 이미지 분리(`Dockerfile.base/master/worker`), SSH 원격 제어 없이 각 컨테이너가 자기 데몬 직접 기동, worker별 독립 named volume | `dfs.replication=3`, 재시작 자동 복구 |
| [M2b 설정 파일 이해](missions/W3/M2b_hadoop_configure_files) | `core/hdfs/mapred/yarn-site.xml`을 파이썬 스크립트로 변경·검증. 타임스탬프 백업 자동 생성 | `modify_config.py`, `verify_config.py` |
| [M3 Word Count](missions/W3/M3_wordcount_using_mapreduce) | Hadoop Streaming(Python mapper/reducer)으로 *Moby Dick* (22,314줄 / 약 215,845 단어) 단어 빈도 집계 | 고유 단어 20,197개 |
| [M4 트위터 감성 분석](missions/W3/M4_Twitter_Sentiment_Analysis_using_MapReduce) | Sentiment140 160만 건을 키워드 사전 기반으로 재분류. `csv.reader`로 텍스트 내 콤마 처리 | positive 401,176 / negative 188,572 / neutral 1,010,252 |
| [M5 영화 평균 평점](missions/W3/M5_Average_Rating_of_Movies_using_MapReduce) | MovieLens 20M `ratings.csv`에서 movieId별 평균 평점 계산 | 영화 26,744편 |
| [M6 Amazon 리뷰 집계](missions/W3/M6_Amazon_Product_Review_using_MapReduce) | Amazon Reviews 2023 `Gift_Cards.jsonl`(152,410건)로 상품별 리뷰 수·평균 평점 산출. InputSplit → Map → Shuffle/Sort → Reduce 내부 흐름 문서화 | 상품 1,894개 (`asin` 기준) |

**학습 포인트** — Apple Silicon(ARM64) `JAVA_HOME` 경로 이슈, `yarn.nodemanager.resource.memory-mb`를
Docker Desktop 할당 메모리에 맞춰 튜닝, `dfs.replication`과 DataNode 수의 관계, 키워드 사전 방식 감성 분류의
한계(neutral 63%)와 TF-IDF/LDA/Word2Vec 대안 검토. 팀 정리는 [TeamWiki/Week3.md](TeamWiki/Week3.md).

## W4 — Spark 클러스터와 분석 파이프라인

| 미션 | 내용 | 산출물 |
|---|---|---|
| [M1 Spark Standalone](missions/W4/M1_Spark_Standalone) | Docker Compose로 master 1 + worker 2(각 2코어/2GB) 클러스터 구성. Monte Carlo π 추정 잡을 `spark-submit`으로 제출하고 결과를 parquet 저장 | `apps/pi.py`, `submit.sh` |
| [M2 Spark 분석](missions/W4/M2_Analysis_Using_Spark) | NYC TLC 옐로우 택시 데이터 + Open-Meteo 날씨 API를 PySpark로 적재·정제·분석. Jupyter 드라이버가 같은 Docker 네트워크에서 client 모드로 클러스터 접속 | `apps/pipeline.py`, `notebooks/analysis.ipynb`, pytest 테스트 |
| M3 Crawling Billion Web Pages | 디렉터리만 존재하며 커밋된 산출물 없음 | — |

**학습 포인트** — 이동시간·거리 지표 계산, 피크아워 분석, 날씨-수요 상관분석(scipy),
이상치 필터링 기준(최대 180분 / 100마일) 설계, [설계 문서](missions/W4/M2_Analysis_Using_Spark/docs/superpowers/specs/)와
[구현 계획](missions/W4/M2_Analysis_Using_Spark/docs/superpowers/plans/)을 코드보다 먼저 작성하는 워크플로.

## W5 — RDD vs DataFrame, 그리고 Lazy Evaluation

| 미션 | 내용 | 산출물 |
|---|---|---|
| [M1 RDD 파이프라인](missions/W5/M1) | 동일한 NYC TLC 데이터를 **RDD API만으로** 처리: `filter`/`map`/`reduce`/`reduceByKey`로 총계·일별 트립수·일별 매출 집계, 날짜 범위 명시 필터링 | `main.py`, `data/output/{summary,daily_trip,daily_sales}` |
| [M2 DataFrame & DAG](missions/W5/M2) | 같은 데이터를 DataFrame API로 재구현하고 실행 계획을 분석. 클리닝 → `cache()` → 필터링/집계/broadcast join 4갈래 → 액션(`collect`, `write`) | `main.py`, [REPORT.md](missions/W5/M2/REPORT.md), `run.log`, Spark UI DAG 스크린샷 |

**학습 포인트** — 원본 4,090,836행 → 정제 후 3,021,419행(26.14% 드롭). `explain()`은 잡을 트리거하지 않는다는
것을 타임스탬프 로그로 증명한 **lazy evaluation 시연**, Spark UI의 "(skipped)" 스테이지가
*셔플 출력 재사용*과 *`cache()` 효과*라는 서로 다른 두 최적화의 결과임을 구분해 해석,
265행짜리 zone lookup을 `broadcast()`로 감싸 큰 테이블 셔플 제거. 결과는 피크 18시(215,728건),
Manhattan 2,636,872건(평균 $16.16) 등. RDD 버전과 달리 DataFrame 버전은 날짜 범위 필터가 없어
원본 데이터 품질 이상치(`2009-01-01` 등)가 남는다는 차이까지 기록.

## W6 — Airflow 오케스트레이션

| 미션 | 내용 | 산출물 |
|---|---|---|
| [M1 서울 공공자전거 ETL](missions/W6/M1_Airflow_Bike_ETL) | 서울 열린데이터광장 `tbCycleRentUseDayInfo` API에서 2일치 데이터를 단일 DAG run으로 수집→정제→집계→MySQL 적재 | `dags/seoul_bike_period_usage.py`, `src/seoul_bike_etl/`, `sql/`, DAG 성공 스크린샷 |

**학습 포인트** — Dynamic task mapping(`extract_day`/`clean_day`를 날짜별로 확장),
**멱등성** 설계(중간 산출물은 날짜 파일명으로 덮어쓰기 + MySQL은 기간 단위 DELETE→INSERT 트랜잭션),
단계별 **데이터 품질 게이트**(스키마 일치/0건 수집/제외율 20% 초과 WARNING/전량 제외 시 실패/station_id 중복 시 실패),
제외 행을 사유와 함께 `data/rejected/`에 분리 보관, 원본 필드명(`RENT_ID`)을 내부 계약 필드명(`RENT_STATN_ID`)으로
번역하는 경계 설계. 실행 결과 2,738개 스테이션이 중복 없이 적재.

---

## 기술 스택

| 영역 | 사용 기술 |
|---|---|
| 언어 | Python 3, SQL |
| 데이터 처리 | pandas, PySpark (RDD / DataFrame), Hadoop MapReduce (Streaming) |
| 수집 | requests, BeautifulSoup, Scrapy + Playwright, kagglehub, 공공 API |
| 저장소 | HDFS, SQLite, MySQL, Parquet, JSON/CSV |
| 오케스트레이션 | Apache Airflow 3.x |
| 인프라 | Docker, Docker Compose, AWS EC2 / ECR |
| 분석·시각화 | matplotlib, scipy, wordcloud, JupyterLab |
| 테스트 | pytest |

## 실행 공통 안내

- 대부분의 미션은 `docker compose up --build`로 기동합니다. 세부 절차와 포트 매핑은 각 미션 README를 참고하세요.
- 원본 데이터셋과 파이프라인 산출물은 용량 문제로 대부분 `.gitignore` 처리되어 있습니다.
  각 미션 README의 데이터 준비 절차(다운로드 URL, HDFS 업로드 명령 등)를 따라 재생성할 수 있습니다.
- Hadoop 미션들은 Apple Silicon(ARM64) 기준으로 `JAVA_HOME`이 설정되어 있어, x86_64 환경에서는
  `java-8-openjdk-arm64` → `java-8-openjdk-amd64` 경로 수정이 필요합니다.
