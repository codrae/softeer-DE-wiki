# NYC TLC Trip Data 분석 (Spark + Jupyter) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** NYC Yellow Taxi trip data와 Open-Meteo 날씨 데이터를 PySpark로 적재/정제하고 평균 이동시간·거리, 피크아워, 날씨 상관관계를 분석해 parquet/csv로 저장하는, Docker화된 Spark standalone 클러스터 + Jupyter 파이프라인을 만든다.

**Architecture:** 순수 변환 로직은 `apps/pipeline.py`(PySpark)와 `apps/download_data.py`(다운로드)에 함수로 작성하고 pytest로 검증한다. `notebooks/analysis.ipynb`는 이 함수들을 7단계 셀(환경설정→데이터적재→클리닝→지표계산→피크아워→날씨상관분석→시각화/출력)로 호출하는 얇은 오케스트레이션 레이어다. 드라이버(Jupyter)는 spark-master/worker와 같은 Docker 네트워크에 있는 별도 컨테이너로, client 모드로 클러스터에 접속한다.

**Tech Stack:** Spark 3.5.9 (standalone), PySpark, Python 3.10, pandas, pyarrow, scipy(통계검정), matplotlib, JupyterLab, pytest, Docker Compose.

## Global Constraints

- Spark 버전 3.5.9, Python 3.10, JDK17 — 기존 이미지 베이스와 동일 버전으로 고정.
- 분석 대상 데이터: 최근 1~2개월치 Yellow Taxi Trip Records (TLC 공식 parquet), Open-Meteo hourly 기온/강수량 (NYC 좌표 40.7128, -74.0060).
- 클리닝 기준: `0 < trip_duration_min <= 180`, `0 < trip_distance <= 100`, `fare_amount >= 0`, 필수 컬럼 null 제거.
- 호스트 포트: master UI `8080:8080`, worker-1 UI `8081:8081`, worker-2 UI `8082:8082`, JupyterLab `8888:8888`. `7077`은 호스트에 노출하지 않음(드라이버가 같은 네트워크 안에서 접속).
- 출력 위치: `/opt/spark-data/output/<table_name>`에 parquet + `<table_name>_csv`에 csv, 총 4개 테이블(`summary_metrics`, `hourly_trip_counts`, `hourly_weather_join`, `weather_condition_stats`).
- 원본/출력 데이터(`*.parquet`, `*.csv`)는 저장소 루트 `.gitignore`에 이미 전역 패턴으로 제외되어 있음 — 별도 gitignore 수정 불필요.
- 테스트는 로컬에 pyspark가 없으므로 항상 `docker compose exec jupyter pytest ...`로 컨테이너 안에서 실행한다.

---

### Task 1: Dockerfile — Spark + 분석용 Python 패키지 이미지

**Files:**
- Create: `missions/W4/M2_Analysis_Using_Spark/Dockerfile`

**Interfaces:**
- Produces: 이미지 안에 `python3`(3.10), `$SPARK_HOME=/opt/spark`(Spark 3.5.9, hadoop3 배포판), pip 패키지 `pyspark==3.5.9 jupyterlab pandas pyarrow requests scipy matplotlib pytest nbformat nbconvert` 가 설치되어 있음. 이후 모든 Task(2~9)가 이 이미지를 `build: .`으로 재사용.

- [ ] **Step 1: Dockerfile 작성**

```dockerfile
FROM python:3.10-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /usr/lib/jvm/default-java

ENV SPARK_VERSION=3.5.9
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=/opt/spark-apps

RUN curl -fsSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

RUN pip install --no-cache-dir \
    pyspark==3.5.9 \
    jupyterlab \
    pandas \
    pyarrow \
    requests \
    scipy \
    matplotlib \
    pytest \
    nbformat \
    nbconvert

WORKDIR /opt/spark-apps

RUN mkdir -p /opt/spark/logs /tmp/spark-events

CMD ["bash"]
```

- [ ] **Step 2: 이미지 빌드**

Run: `cd missions/W4/M2_Analysis_Using_Spark && docker build -t m2-spark-analysis .`
Expected: `Successfully tagged m2-spark-analysis:latest` (다운로드 때문에 수 분 소요될 수 있음)

- [ ] **Step 3: 패키지 설치 검증**

Run: `docker run --rm m2-spark-analysis python3 -c "import pyspark, pandas, pyarrow, requests, scipy, matplotlib, pytest, nbformat, nbconvert; print('OK', pyspark.__version__)"`
Expected: `OK 3.5.9`

- [ ] **Step 4: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/Dockerfile
git commit -m "feat(W4M2): Spark+분석 패키지 Dockerfile 작성"
```

---

### Task 2: docker-compose.yml — 클러스터 + Jupyter 드라이버

**Files:**
- Create: `missions/W4/M2_Analysis_Using_Spark/docker-compose.yml`
- Create: `missions/W4/M2_Analysis_Using_Spark/apps/.gitkeep`
- Create: `missions/W4/M2_Analysis_Using_Spark/notebooks/.gitkeep`

**Interfaces:**
- Consumes: Task 1의 이미지(`build: .`).
- Produces: 4개 실행 중인 컨테이너(`spark-master`, `spark-worker-1`, `spark-worker-2`, `jupyter`)가 `spark-net` 네트워크로 연결됨. `./apps`가 모든 서비스에 `/opt/spark-apps`로, `./notebooks`가 `jupyter`에 `/opt/notebooks`로, 저장소 루트 `data/`가 `/opt/spark-data`로 마운트됨. 이후 Task들은 `docker compose exec jupyter <cmd>`로 이 환경을 사용한다.

- [ ] **Step 1: apps/, notebooks/ 빈 디렉토리 생성**

```bash
mkdir -p missions/W4/M2_Analysis_Using_Spark/apps missions/W4/M2_Analysis_Using_Spark/notebooks
touch missions/W4/M2_Analysis_Using_Spark/apps/.gitkeep missions/W4/M2_Analysis_Using_Spark/notebooks/.gitkeep
```

- [ ] **Step 2: docker-compose.yml 작성**

```yaml
services:

  spark-master:
    build: .
    container_name: spark-master
    hostname: spark-master
    command: >
      bash -c "start-master.sh --host spark-master --port 7077 --webui-port 8080
      && tail -f /opt/spark/logs/*.out"
    ports:
      - "8080:8080"
    volumes:
      - ./apps:/opt/spark-apps
      - ../../data:/opt/spark-data
    networks:
      - spark-net

  spark-worker-1:
    build: .
    container_name: spark-worker-1
    hostname: spark-worker-1
    depends_on:
      - spark-master
    command: >
      bash -c "start-worker.sh spark://spark-master:7077 --webui-port 8081
      && tail -f /opt/spark/logs/*.out"
    ports:
      - "8081:8081"
    volumes:
      - ./apps:/opt/spark-apps
      - ../../data:/opt/spark-data
    networks:
      - spark-net
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g

  spark-worker-2:
    build: .
    container_name: spark-worker-2
    hostname: spark-worker-2
    depends_on:
      - spark-master
    command: >
      bash -c "start-worker.sh spark://spark-master:7077 --webui-port 8082
      && tail -f /opt/spark/logs/*.out"
    ports:
      - "8082:8082"
    volumes:
      - ./apps:/opt/spark-apps
      - ../../data:/opt/spark-data
    networks:
      - spark-net
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g

  jupyter:
    build: .
    container_name: jupyter
    hostname: jupyter
    depends_on:
      - spark-master
    command: >
      bash -c "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
      --NotebookApp.token='' --notebook-dir=/opt/notebooks"
    ports:
      - "8888:8888"
    volumes:
      - ./apps:/opt/spark-apps
      - ./notebooks:/opt/notebooks
      - ../../data:/opt/spark-data
    networks:
      - spark-net
    environment:
      - PYTHONPATH=/opt/spark-apps

networks:
  spark-net:
    driver: bridge
```

- [ ] **Step 3: 클러스터 기동**

Run: `cd missions/W4/M2_Analysis_Using_Spark && docker compose up -d --build`
Expected: 4개 컨테이너(`spark-master`, `spark-worker-1`, `spark-worker-2`, `jupyter`) 모두 `Started`/`Running`

- [ ] **Step 4: worker 2대 등록 확인**

Run:
```bash
sleep 10
curl -s http://localhost:8080/json/ | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['workers']), [w['state'] for w in d['workers']])"
```
Expected: `2 ['ALIVE', 'ALIVE']`

- [ ] **Step 5: Jupyter에서 클러스터 접속 확인**

Run:
```bash
docker compose exec jupyter python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('spark://spark-master:7077').appName('conn-check').getOrCreate()
print('version', spark.version)
print('master', spark.sparkContext.master)
spark.stop()
"
```
Expected: `version 3.5.9` 와 `master spark://spark-master:7077` 출력, 에러 없이 종료

- [ ] **Step 6: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/docker-compose.yml missions/W4/M2_Analysis_Using_Spark/apps/.gitkeep missions/W4/M2_Analysis_Using_Spark/notebooks/.gitkeep
git commit -m "feat(W4M2): Spark 클러스터 + Jupyter 드라이버 docker-compose 작성"
```

---

### Task 3: download_data.py — TLC parquet + Open-Meteo 날씨 다운로드

**Files:**
- Create: `missions/W4/M2_Analysis_Using_Spark/apps/download_data.py`
- Test: `missions/W4/M2_Analysis_Using_Spark/apps/tests/test_download_data.py`

**Interfaces:**
- Produces:
  - `tlc_url(year: int, month: int) -> str`
  - `download_file(url: str, dest_path: Path, timeout: int = 60) -> Path`
  - `download_tlc_months(year_months: list[tuple[int, int]], dest_dir: Path) -> list[Path]`
  - `fetch_weather(start_date: str, end_date: str, dest_path: Path, latitude: float = 40.7128, longitude: float = -74.0060, timeout: int = 60) -> Path`
- Consumes: 이후 Task 9의 notebook이 `download_tlc_months`, `fetch_weather`를 임포트해서 사용.

- [ ] **Step 1: 실패하는 테스트 작성**

```python
# missions/W4/M2_Analysis_Using_Spark/apps/tests/test_download_data.py
from pathlib import Path

import pytest

import download_data


class FakeResponse:
    def __init__(self, status_code, content=b"", json_data=None):
        self.status_code = status_code
        self.content = content
        self._json_data = json_data

    def json(self):
        return self._json_data


def test_tlc_url_builds_expected_cloudfront_url():
    url = download_data.tlc_url(2024, 1)
    assert url == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )


def test_download_file_skips_when_already_exists(tmp_path, monkeypatch):
    dest = tmp_path / "existing.parquet"
    dest.write_bytes(b"already-here")
    calls = []
    monkeypatch.setattr(download_data.requests, "get", lambda *a, **k: calls.append(1))

    result = download_data.download_file("http://example.com/x.parquet", dest)

    assert result == dest
    assert calls == []
    assert dest.read_bytes() == b"already-here"


def test_download_file_downloads_when_missing(tmp_path, monkeypatch):
    dest = tmp_path / "sub" / "new.parquet"
    monkeypatch.setattr(
        download_data.requests, "get",
        lambda *a, **k: FakeResponse(200, content=b"parquet-bytes"),
    )

    result = download_data.download_file("http://example.com/x.parquet", dest)

    assert result == dest
    assert dest.read_bytes() == b"parquet-bytes"


def test_download_file_raises_on_http_error(tmp_path, monkeypatch):
    dest = tmp_path / "missing.parquet"
    monkeypatch.setattr(
        download_data.requests, "get",
        lambda *a, **k: FakeResponse(404),
    )

    with pytest.raises(RuntimeError):
        download_data.download_file("http://example.com/missing.parquet", dest)


def test_download_tlc_months_builds_one_file_per_month(tmp_path, monkeypatch):
    monkeypatch.setattr(
        download_data.requests, "get",
        lambda *a, **k: FakeResponse(200, content=b"data"),
    )

    paths = download_data.download_tlc_months([(2024, 1), (2024, 2)], tmp_path)

    assert [p.name for p in paths] == [
        "yellow_tripdata_2024-01.parquet",
        "yellow_tripdata_2024-02.parquet",
    ]
    assert all(p.exists() for p in paths)


def test_fetch_weather_writes_csv_from_hourly_json(tmp_path, monkeypatch):
    fake_json = {
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [1.0, 2.0],
            "precipitation": [0.0, 0.5],
        }
    }
    monkeypatch.setattr(
        download_data.requests, "get",
        lambda *a, **k: FakeResponse(200, json_data=fake_json),
    )
    dest = tmp_path / "weather" / "nyc_weather.csv"

    result = download_data.fetch_weather("2024-01-01", "2024-01-31", dest)

    assert result == dest
    content = dest.read_text()
    assert "datetime,temperature_2m,precipitation" in content
    assert "2024-01-01T00:00,1.0,0.0" in content
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_download_data.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'download_data'`

- [ ] **Step 3: download_data.py 구현**

```python
# missions/W4/M2_Analysis_Using_Spark/apps/download_data.py
from pathlib import Path

import pandas as pd
import requests

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def tlc_url(year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def download_file(url: str, dest_path, timeout: int = 60) -> Path:
    dest_path = Path(dest_path)
    if dest_path.exists():
        return dest_path
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url}: HTTP {response.status_code}")
    dest_path.write_bytes(response.content)
    return dest_path


def download_tlc_months(year_months, dest_dir) -> list:
    dest_dir = Path(dest_dir)
    paths = []
    for year, month in year_months:
        url = tlc_url(year, month)
        dest = dest_dir / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
        paths.append(download_file(url, dest))
    return paths


def fetch_weather(
    start_date: str,
    end_date: str,
    dest_path,
    latitude: float = NYC_LATITUDE,
    longitude: float = NYC_LONGITUDE,
    timeout: int = 60,
) -> Path:
    dest_path = Path(dest_path)
    if dest_path.exists():
        return dest_path
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation",
        "timezone": "America/New_York",
    }
    response = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch weather: HTTP {response.status_code}")
    hourly = response.json()["hourly"]
    df = pd.DataFrame(
        {
            "datetime": hourly["time"],
            "temperature_2m": hourly["temperature_2m"],
            "precipitation": hourly["precipitation"],
        }
    )
    df.to_csv(dest_path, index=False)
    return dest_path
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_download_data.py -v`
Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/apps/download_data.py missions/W4/M2_Analysis_Using_Spark/apps/tests/test_download_data.py
git commit -m "feat(W4M2): TLC/Open-Meteo 다운로드 함수 작성"
```

---

### Task 4: pipeline.py — 로딩 함수 + 공용 Spark 테스트 fixture

**Files:**
- Create: `missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py`
- Create: `missions/W4/M2_Analysis_Using_Spark/apps/tests/conftest.py`
- Test: `missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py`

**Interfaces:**
- Consumes: 없음 (첫 pipeline 함수들).
- Produces:
  - `spark` pytest fixture (session-scoped local SparkSession), 이후 Task 5~8의 모든 테스트가 재사용.
  - `load_trips(spark: SparkSession, path_glob: str) -> DataFrame`
  - `load_weather(spark: SparkSession, path: str) -> DataFrame` (컬럼: `datetime`(timestamp), `temperature_2m`, `precipitation`)

- [ ] **Step 1: conftest.py 작성 (공용 spark fixture)**

```python
# missions/W4/M2_Analysis_Using_Spark/apps/tests/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pipeline-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
```

- [ ] **Step 2: 실패하는 테스트 작성**

```python
# missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py
import pandas as pd

import pipeline


def test_load_trips_reads_parquet_glob(spark, tmp_path):
    pdf = pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.to_datetime(["2024-01-01 00:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2024-01-01 00:10:00"]),
            "trip_distance": [2.5],
            "fare_amount": [12.0],
        }
    )
    pdf.to_parquet(tmp_path / "part-0.parquet")

    df = pipeline.load_trips(spark, str(tmp_path / "*.parquet"))

    assert df.count() == 1
    assert "trip_distance" in df.columns


def test_load_weather_reads_csv_and_casts_timestamp(spark, tmp_path):
    csv_path = tmp_path / "weather.csv"
    csv_path.write_text(
        "datetime,temperature_2m,precipitation\n"
        "2024-01-01T00:00,1.0,0.0\n"
    )

    df = pipeline.load_weather(spark, str(csv_path))

    row = df.collect()[0]
    assert row["temperature_2m"] == 1.0
    assert row["datetime"] is not None
```

- [ ] **Step 3: 테스트 실행 → 실패 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline'`

- [ ] **Step 4: pipeline.py 초기 구현 (로딩 함수)**

```python
# missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MAX_TRIP_DURATION_MIN = 180
MAX_TRIP_DISTANCE_MI = 100


def load_trips(spark: SparkSession, path_glob: str) -> DataFrame:
    return spark.read.parquet(path_glob)


def load_weather(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True).option("inferSchema", True).csv(path)
        .withColumn("datetime", F.to_timestamp("datetime"))
    )
```

- [ ] **Step 5: 테스트 실행 → 통과 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v`
Expected: `2 passed`

- [ ] **Step 6: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py missions/W4/M2_Analysis_Using_Spark/apps/tests/conftest.py missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py
git commit -m "feat(W4M2): TLC/날씨 로딩 함수 및 공용 Spark 테스트 fixture 작성"
```

---

### Task 5: pipeline.py — clean_trips

**Files:**
- Modify: `missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py`
- Modify: `missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py`

**Interfaces:**
- Consumes: Task 4의 `spark` fixture.
- Produces: `clean_trips(df: DataFrame) -> DataFrame` — 이후 Task 6, 7, 9가 정제된 trips DataFrame의 입력으로 사용. 출력 컬럼에 `trip_duration_min`(float, 분) 파생 컬럼 포함.

- [ ] **Step 1: 실패하는 테스트 추가**

```python
# test_pipeline.py 에 추가
from datetime import datetime


def test_clean_trips_filters_invalid_rows_and_adds_duration(spark):
    columns = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount",
    ]
    rows = [
        (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 10, 0), 2.5, 12.0),  # valid
        (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 50, 0), 2.0, 10.0),  # negative duration
        (datetime(2024, 1, 1, 2, 0, 0), datetime(2024, 1, 1, 2, 5, 0), 0.0, 5.0),    # zero distance
        (datetime(2024, 1, 1, 3, 0, 0), datetime(2024, 1, 1, 3, 5, 0), 150.0, 5.0),  # distance too far
        (datetime(2024, 1, 1, 4, 0, 0), datetime(2024, 1, 1, 4, 5, 0), None, 5.0),   # null distance
        (datetime(2024, 1, 1, 5, 0, 0), datetime(2024, 1, 1, 5, 5, 0), 2.0, -1.0),   # negative fare
    ]
    df = spark.createDataFrame(rows, columns)

    cleaned = pipeline.clean_trips(df)
    result = cleaned.collect()

    assert len(result) == 1
    assert result[0]["trip_distance"] == 2.5
    assert abs(result[0]["trip_duration_min"] - 10.0) < 1e-6
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py::test_clean_trips_filters_invalid_rows_and_adds_duration -v`
Expected: FAIL — `AttributeError: module 'pipeline' has no attribute 'clean_trips'`

- [ ] **Step 3: clean_trips 구현 (pipeline.py에 추가)**

```python
def clean_trips(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
        .dropna(subset=[
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "trip_distance", "fare_amount",
        ])
        .withColumn(
            "trip_duration_min",
            (
                F.col("tpep_dropoff_datetime").cast("long")
                - F.col("tpep_pickup_datetime").cast("long")
            ) / 60.0,
        )
        .filter(
            (F.col("trip_duration_min") > 0)
            & (F.col("trip_duration_min") <= MAX_TRIP_DURATION_MIN)
            & (F.col("trip_distance") > 0)
            & (F.col("trip_distance") <= MAX_TRIP_DISTANCE_MI)
            & (F.col("fare_amount") >= 0)
        )
    )
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py
git commit -m "feat(W4M2): 트립 데이터 클리닝 함수(clean_trips) 작성"
```

---

### Task 6: pipeline.py — 지표 계산 + 피크아워 집계

**Files:**
- Modify: `missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py`
- Modify: `missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py`

**Interfaces:**
- Consumes: Task 5의 `clean_trips` 출력 DataFrame(`trip_duration_min`, `trip_distance`, `tpep_pickup_datetime` 컬럼 보장).
- Produces:
  - `compute_summary_metrics(df: DataFrame) -> DataFrame` (컬럼: `avg_trip_duration_min`, `avg_trip_distance_mi`, `trip_count`)
  - `compute_hourly_trip_counts(df: DataFrame) -> DataFrame` (컬럼: `pickup_hour`(0~23), `trip_count`) — 이후 Task 9의 피크아워 시각화가 사용.

- [ ] **Step 1: 실패하는 테스트 추가**

```python
# test_pipeline.py 에 추가
def _cleaned_sample(spark):
    columns = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount",
    ]
    rows = [
        (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 10, 0), 2.0, 10.0),
        (datetime(2024, 1, 1, 0, 30, 0), datetime(2024, 1, 1, 0, 40, 0), 4.0, 10.0),
        (datetime(2024, 1, 1, 5, 0, 0), datetime(2024, 1, 1, 5, 5, 0), 3.0, 10.0),
    ]
    return pipeline.clean_trips(spark.createDataFrame(rows, columns))


def test_compute_summary_metrics_returns_averages(spark):
    cleaned = _cleaned_sample(spark)

    result = pipeline.compute_summary_metrics(cleaned).collect()[0]

    assert result["trip_count"] == 3
    assert abs(result["avg_trip_distance_mi"] - 3.0) < 1e-6
    assert abs(result["avg_trip_duration_min"] - 8.333333) < 1e-3


def test_compute_hourly_trip_counts_groups_by_hour(spark):
    cleaned = _cleaned_sample(spark)

    result = {
        row["pickup_hour"]: row["trip_count"]
        for row in pipeline.compute_hourly_trip_counts(cleaned).collect()
    }

    assert result == {0: 2, 5: 1}
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v -k "summary_metrics or hourly_trip_counts"`
Expected: FAIL — `AttributeError: module 'pipeline' has no attribute 'compute_summary_metrics'`

- [ ] **Step 3: 구현 추가 (pipeline.py)**

```python
def compute_summary_metrics(df: DataFrame) -> DataFrame:
    return df.agg(
        F.avg("trip_duration_min").alias("avg_trip_duration_min"),
        F.avg("trip_distance").alias("avg_trip_distance_mi"),
        F.count(F.lit(1)).alias("trip_count"),
    )


def compute_hourly_trip_counts(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy("pickup_hour")
    )
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py
git commit -m "feat(W4M2): 평균 지표/피크아워 집계 함수 작성"
```

---

### Task 7: pipeline.py — 날씨 조인 + 상관/통계 검증

**Files:**
- Modify: `missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py`
- Modify: `missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py`

**Interfaces:**
- Consumes: Task 5의 `clean_trips` 출력, Task 4의 `load_weather` 출력(컬럼 `datetime`, `temperature_2m`, `precipitation`).
- Produces:
  - `compute_hourly_trip_series(df: DataFrame) -> DataFrame` (컬럼: `dt_hour`(timestamp), `trip_count`)
  - `join_hourly_weather(hourly_series_df: DataFrame, weather_df: DataFrame) -> DataFrame` (컬럼: `dt_hour`, `trip_count`, `temperature_2m`, `precipitation`)
  - `compute_correlations(pdf: pandas.DataFrame) -> dict` (키: `temperature_r`, `temperature_p`, `precipitation_r`, `precipitation_p`)
  - `compute_weather_condition_stats(pdf: pandas.DataFrame) -> dict` (키: `rainy_mean_trip_count`, `dry_mean_trip_count`, `t_stat`, `p_value`)
  - 이후 Task 8의 `stats_to_dataframe`과 Task 9 notebook이 이 4개 함수를 그대로 사용.

- [ ] **Step 1: 실패하는 테스트 추가**

```python
# test_pipeline.py 에 추가
def test_compute_hourly_trip_series_groups_by_date_and_hour(spark):
    cleaned = _cleaned_sample(spark)

    rows = {
        row["dt_hour"]: row["trip_count"]
        for row in pipeline.compute_hourly_trip_series(cleaned).collect()
    }

    assert len(rows) == 2  # 2024-01-01 00시, 2024-01-01 05시


def test_join_hourly_weather_matches_on_truncated_hour(spark):
    hourly_series = spark.createDataFrame(
        [(datetime(2024, 1, 1, 0, 0, 0), 10), (datetime(2024, 1, 1, 1, 0, 0), 4)],
        ["dt_hour", "trip_count"],
    )
    weather = spark.createDataFrame(
        [
            (datetime(2024, 1, 1, 0, 30, 0), 5.0, 0.0),
            (datetime(2024, 1, 1, 1, 15, 0), -2.0, 3.0),
        ],
        ["datetime", "temperature_2m", "precipitation"],
    )

    joined = pipeline.join_hourly_weather(hourly_series, weather).orderBy("dt_hour").collect()

    assert len(joined) == 2
    assert joined[0]["trip_count"] == 10 and joined[0]["temperature_2m"] == 5.0
    assert joined[1]["trip_count"] == 4 and joined[1]["precipitation"] == 3.0


def test_compute_correlations_perfect_positive_relationship():
    pdf = pd.DataFrame({
        "trip_count": [10, 20, 30, 40],
        "temperature_2m": [1, 2, 3, 4],
        "precipitation": [4, 3, 2, 1],
    })

    result = pipeline.compute_correlations(pdf)

    assert abs(result["temperature_r"] - 1.0) < 1e-6
    assert abs(result["precipitation_r"] - (-1.0)) < 1e-6


def test_compute_weather_condition_stats_detects_lower_trips_on_rainy_hours():
    pdf = pd.DataFrame({
        "trip_count": [100, 105, 98, 20, 25, 18],
        "precipitation": [0, 0, 0, 5, 6, 4],
    })

    result = pipeline.compute_weather_condition_stats(pdf)

    assert result["dry_mean_trip_count"] > result["rainy_mean_trip_count"]
    assert result["p_value"] < 0.05
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v -k "hourly_trip_series or join_hourly_weather or correlations or condition_stats"`
Expected: FAIL — `AttributeError: module 'pipeline' has no attribute 'compute_hourly_trip_series'`

- [ ] **Step 3: 구현 추가 (pipeline.py 상단에 `from scipy import stats` 추가, 함수 추가)**

```python
from scipy import stats as scipy_stats


def compute_hourly_trip_series(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("dt_hour", F.date_trunc("hour", "tpep_pickup_datetime"))
        .groupBy("dt_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy("dt_hour")
    )


def join_hourly_weather(hourly_series_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    weather_hourly = weather_df.withColumn("dt_hour", F.date_trunc("hour", "datetime"))
    return hourly_series_df.join(weather_hourly, on="dt_hour", how="inner").select(
        "dt_hour", "trip_count", "temperature_2m", "precipitation"
    )


def compute_correlations(pdf) -> dict:
    temp_r, temp_p = scipy_stats.pearsonr(pdf["trip_count"], pdf["temperature_2m"])
    precip_r, precip_p = scipy_stats.pearsonr(pdf["trip_count"], pdf["precipitation"])
    return {
        "temperature_r": float(temp_r),
        "temperature_p": float(temp_p),
        "precipitation_r": float(precip_r),
        "precipitation_p": float(precip_p),
    }


def compute_weather_condition_stats(pdf) -> dict:
    rainy = pdf[pdf["precipitation"] > 0]["trip_count"]
    dry = pdf[pdf["precipitation"] == 0]["trip_count"]
    t_stat, p_value = scipy_stats.ttest_ind(rainy, dry, equal_var=False)
    return {
        "rainy_mean_trip_count": float(rainy.mean()),
        "dry_mean_trip_count": float(dry.mean()),
        "t_stat": float(t_stat),
        "p_value": float(p_value),
    }
```

Also add near top of `test_pipeline.py`: `import pandas as pd`.

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v`
Expected: `9 passed`

- [ ] **Step 5: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py
git commit -m "feat(W4M2): 날씨 조인 및 상관/통계 검증 함수 작성"
```

---

### Task 8: pipeline.py — 결과 저장 (parquet + csv)

**Files:**
- Modify: `missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py`
- Modify: `missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py`

**Interfaces:**
- Consumes: Task 6, 7의 결과 DataFrame들.
- Produces:
  - `stats_to_dataframe(spark: SparkSession, stats: dict) -> DataFrame`
  - `write_output_table(df: DataFrame, output_dir: str, name: str) -> None` — `<output_dir>/<name>`에 parquet, `<output_dir>/<name>_csv`에 header 포함 단일 csv 파일 생성. 이후 Task 9 notebook의 마지막 셀이 4개 테이블 각각에 대해 호출.

- [ ] **Step 1: 실패하는 테스트 추가**

```python
# test_pipeline.py 에 추가
def test_write_output_table_creates_parquet_and_csv(spark, tmp_path):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])

    pipeline.write_output_table(df, str(tmp_path), "sample_table")

    parquet_dir = tmp_path / "sample_table"
    csv_dir = tmp_path / "sample_table_csv"
    assert parquet_dir.exists()
    assert csv_dir.exists()

    read_back = spark.read.parquet(str(parquet_dir))
    assert read_back.count() == 2

    csv_files = list(csv_dir.glob("*.csv"))
    assert len(csv_files) == 1
    assert "id,label" in csv_files[0].read_text()


def test_stats_to_dataframe_wraps_dict_as_single_row(spark):
    df = pipeline.stats_to_dataframe(spark, {"a": 1.0, "b": 2.0})

    row = df.collect()[0]
    assert row["a"] == 1.0
    assert row["b"] == 2.0
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v -k "write_output_table or stats_to_dataframe"`
Expected: FAIL — `AttributeError: module 'pipeline' has no attribute 'write_output_table'`

- [ ] **Step 3: 구현 추가 (pipeline.py)**

```python
def write_output_table(df: DataFrame, output_dir: str, name: str) -> None:
    df.write.mode("overwrite").parquet(f"{output_dir}/{name}")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/{name}_csv"
    )


def stats_to_dataframe(spark: SparkSession, stats: dict) -> DataFrame:
    return spark.createDataFrame([stats])
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `docker compose exec jupyter pytest /opt/spark-apps/tests/test_pipeline.py -v`
Expected: `11 passed`

- [ ] **Step 5: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/apps/pipeline.py missions/W4/M2_Analysis_Using_Spark/apps/tests/test_pipeline.py
git commit -m "feat(W4M2): 결과 테이블 저장(parquet+csv) 함수 작성"
```

---

### Task 9: notebooks/analysis.ipynb — 7단계 셀 파이프라인 + 시각화

**Files:**
- Create: `missions/W4/M2_Analysis_Using_Spark/apps/build_notebook.py`
- Create (생성물): `missions/W4/M2_Analysis_Using_Spark/notebooks/analysis.ipynb`

**Interfaces:**
- Consumes: `download_data.download_tlc_months`, `download_data.fetch_weather`, `pipeline.load_trips`, `pipeline.load_weather`, `pipeline.clean_trips`, `pipeline.compute_summary_metrics`, `pipeline.compute_hourly_trip_counts`, `pipeline.compute_hourly_trip_series`, `pipeline.join_hourly_weather`, `pipeline.compute_correlations`, `pipeline.compute_weather_condition_stats`, `pipeline.write_output_table`, `pipeline.stats_to_dataframe` (Task 3~8에서 만든 것 그대로).
- Produces: `/opt/spark-data/output/{summary_metrics,hourly_trip_counts,hourly_weather_join,weather_condition_stats}` (parquet) 및 각각의 `_csv` 디렉토리.

- [ ] **Step 1: build_notebook.py 작성 (nbformat으로 노트북 생성)**

```python
# missions/W4/M2_Analysis_Using_Spark/apps/build_notebook.py
import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []

cells.append(nbf.v4.new_markdown_cell("# NYC TLC Trip Data 분석"))

cells.append(nbf.v4.new_markdown_cell("## 1. 환경설정"))
cells.append(nbf.v4.new_code_cell("""\
import sys
sys.path.append('/opt/spark-apps')

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("NYCTaxiAnalysis")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Master:", spark.sparkContext.master)
"""))

cells.append(nbf.v4.new_markdown_cell("## 2. 데이터 적재"))
cells.append(nbf.v4.new_code_cell("""\
from pathlib import Path
from download_data import download_tlc_months, fetch_weather
from pipeline import load_trips, load_weather

YEAR_MONTHS = [(2024, 1)]
START_DATE, END_DATE = "2024-01-01", "2024-01-31"

RAW_TLC_DIR = Path("/opt/spark-data/raw/tlc")
RAW_WEATHER_PATH = Path("/opt/spark-data/raw/weather/nyc_weather.csv")

tlc_paths = download_tlc_months(YEAR_MONTHS, RAW_TLC_DIR)
weather_path = fetch_weather(START_DATE, END_DATE, RAW_WEATHER_PATH)

trips_df = load_trips(spark, str(RAW_TLC_DIR / "*.parquet"))
weather_df = load_weather(spark, str(weather_path))

print("Trips loaded:", trips_df.count())
print("Weather rows loaded:", weather_df.count())
"""))

cells.append(nbf.v4.new_markdown_cell("## 3. 클리닝"))
cells.append(nbf.v4.new_code_cell("""\
from pipeline import clean_trips

raw_count = trips_df.count()
cleaned_df = clean_trips(trips_df).cache()
cleaned_count = cleaned_df.count()

print(f"Raw rows: {raw_count}")
print(f"Cleaned rows: {cleaned_count} ({cleaned_count / raw_count:.1%} kept)")
"""))

cells.append(nbf.v4.new_markdown_cell("## 4. 지표 계산"))
cells.append(nbf.v4.new_code_cell("""\
from pipeline import compute_summary_metrics

summary_df = compute_summary_metrics(cleaned_df)
summary_df.toPandas()
"""))

cells.append(nbf.v4.new_markdown_cell("## 5. 피크아워 분석"))
cells.append(nbf.v4.new_code_cell("""\
import matplotlib.pyplot as plt
from pipeline import compute_hourly_trip_counts

hourly_counts_df = compute_hourly_trip_counts(cleaned_df)
hourly_counts_pdf = hourly_counts_df.toPandas()

top_hours = hourly_counts_pdf.nlargest(3, "trip_count")
print("Peak hours:")
print(top_hours)

fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(hourly_counts_pdf["pickup_hour"], hourly_counts_pdf["trip_count"])
ax.set_xlabel("Hour of day")
ax.set_ylabel("Trip count")
ax.set_title("Trips by pickup hour")
plt.show()
"""))

cells.append(nbf.v4.new_markdown_cell("## 6. 날씨 상관분석"))
cells.append(nbf.v4.new_code_cell("""\
from pipeline import (
    compute_hourly_trip_series,
    join_hourly_weather,
    compute_correlations,
    compute_weather_condition_stats,
)

hourly_series_df = compute_hourly_trip_series(cleaned_df)
weather_join_df = join_hourly_weather(hourly_series_df, weather_df)
weather_join_pdf = weather_join_df.toPandas()

correlations = compute_correlations(weather_join_pdf)
condition_stats = compute_weather_condition_stats(weather_join_pdf)

print("Correlations:", correlations)
print("Rain vs dry stats:", condition_stats)

fig, axes = plt.subplots(1, 2, figsize=(12, 4))
axes[0].scatter(weather_join_pdf["temperature_2m"], weather_join_pdf["trip_count"])
axes[0].set_xlabel("Temperature (C)")
axes[0].set_ylabel("Trip count")
axes[1].scatter(weather_join_pdf["precipitation"], weather_join_pdf["trip_count"])
axes[1].set_xlabel("Precipitation (mm)")
axes[1].set_ylabel("Trip count")
plt.show()
"""))

cells.append(nbf.v4.new_markdown_cell("## 7. 시각화/출력"))
cells.append(nbf.v4.new_code_cell("""\
from pipeline import write_output_table, stats_to_dataframe

OUTPUT_DIR = "/opt/spark-data/output"

write_output_table(summary_df, OUTPUT_DIR, "summary_metrics")
write_output_table(hourly_counts_df, OUTPUT_DIR, "hourly_trip_counts")
write_output_table(weather_join_df, OUTPUT_DIR, "hourly_weather_join")
write_output_table(
    stats_to_dataframe(spark, {**correlations, **condition_stats}),
    OUTPUT_DIR,
    "weather_condition_stats",
)

print("Saved outputs to", OUTPUT_DIR)

fig, axes = plt.subplots(1, 3, figsize=(15, 4))
axes[0].bar(hourly_counts_pdf["pickup_hour"], hourly_counts_pdf["trip_count"])
axes[0].set_title("Trips by hour")
axes[1].scatter(weather_join_pdf["temperature_2m"], weather_join_pdf["trip_count"])
axes[1].set_title("Temp vs trips")
rain_group = weather_join_pdf.assign(is_rain=weather_join_pdf["precipitation"] > 0)
rain_group.groupby("is_rain")["trip_count"].mean().plot(kind="bar", ax=axes[2])
axes[2].set_title("Rain vs dry avg trips")
plt.tight_layout()
plt.show()
"""))

nb["cells"] = cells

with open("/opt/notebooks/analysis.ipynb", "w") as f:
    nbf.write(nb, f)

print("Notebook written to /opt/notebooks/analysis.ipynb")
```

- [ ] **Step 2: 노트북 생성 실행**

Run: `docker compose exec jupyter python3 /opt/spark-apps/build_notebook.py`
Expected: `Notebook written to /opt/notebooks/analysis.ipynb`, 호스트의 `missions/W4/M2_Analysis_Using_Spark/notebooks/analysis.ipynb` 파일 생성 확인

- [ ] **Step 3: 노트북 전체 실행 (실제 다운로드 + 전체 파이프라인 통합 테스트)**

Run:
```bash
docker compose exec jupyter jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.timeout=1800 /opt/notebooks/analysis.ipynb
```
Expected: 에러 없이 종료 (`ipynb` 저장 완료 메시지). 실제 TLC 1개월 parquet(수백 MB)과 Open-Meteo 응답을 다운로드하므로 수 분 소요될 수 있음.

- [ ] **Step 4: 출력 결과 확인**

Run: `ls /opt/spark-data/output` (호스트에서: `ls ../../data/output` from `missions/W4/M2_Analysis_Using_Spark`)
Expected: `summary_metrics`, `summary_metrics_csv`, `hourly_trip_counts`, `hourly_trip_counts_csv`, `hourly_weather_join`, `hourly_weather_join_csv`, `weather_condition_stats`, `weather_condition_stats_csv` 8개 디렉토리 모두 존재

- [ ] **Step 5: 실행된 노트북에서 평균 지표가 상식적인 범위인지 육안 확인**

Run: `docker compose exec jupyter jupyter nbconvert --to script --stdout /opt/notebooks/analysis.ipynb | tail -5` 또는 `localhost:8888`을 브라우저로 열어 4번 셀(지표 계산) 출력 확인
Expected: `avg_trip_duration_min`이 대략 10~20분, `avg_trip_distance_mi`가 대략 2~4마일 범위 (NYC 옐로우 택시 일반적 수치)

- [ ] **Step 6: Commit**

```bash
git add missions/W4/M2_Analysis_Using_Spark/apps/build_notebook.py missions/W4/M2_Analysis_Using_Spark/notebooks/analysis.ipynb
git commit -m "feat(W4M2): 7단계 분석 노트북(analysis.ipynb) 생성 스크립트 및 실행 결과 작성"
```
