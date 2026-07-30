# NYC TLC DataFrame/DAG Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone PySpark DataFrame pipeline (`main.py`) over NYC TLC trip data that cleans, transforms
(filter/aggregate/broadcast-join), caches, and writes results, demonstrates lazy evaluation, and produces a
`REPORT.md` with a captured Spark UI DAG screenshot.

**Architecture:** Single-file pipeline module `main.py` holding small pure functions (each takes/returns a
`DataFrame`), unit-tested with pytest against a local `SparkSession`, orchestrated by a `run_pipeline()` function
that wires caching + lazy-eval logging + actions, and a thin `main()` that resolves real paths, calls
`run_pipeline()`, prints results, and pauses on `input()` so the Spark UI (`localhost:4040`) can be inspected —
mirroring the existing `missions/W5/M1/main.py` RDD script's style.

**Tech Stack:** Python 3.14, PySpark 4.2.0 (local[4] master), pytest 9.1.1, requests, pandas — all already
installed in the shared venv at `/Users/yong/PycharmProjects/softeer-DE-wiki/.venv`.

## Global Constraints

- Use the existing venv's interpreter for every command: `/Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python`
  (and `.../.venv/bin/pip`, `.../.venv/bin/python -m pytest`). Do not create a new venv.
- All work happens under `/Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2` (referred to below as `M2/`).
  Run every command from this directory unless a step says otherwise.
- Reuse `missions/W5/M1/data/yellow_tripdata_2026-05.parquet` as the trip data source — do not re-download TLC data.
- Zone lookup source: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` (columns:
  `LocationID,Borough,Zone,service_zone`). Download once into `M2/data/taxi_zone_lookup.csv`.
- Cleaning thresholds (exact values, from the approved spec): `0 < trip_duration_min <= 180`,
  `0 < trip_distance <= 100`, `1 <= passenger_count <= 6`, `fare_amount >= 0`. Required non-null columns:
  `tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, fare_amount, PULocationID, DOLocationID`.
  Reject/derive semantics are fixed; do not invent different thresholds while implementing.
  These are enforced by `main.clean_trips` (Task 4) and consumed by every downstream task.
- Root `.gitignore` already ignores `*.parquet`, `*.csv`, `*.log` globally, and `/missions/W5/M1/data/` explicitly.
  It does **not** yet ignore `/missions/W5/M2/data/`, and Spark write outputs include extensionless files
  (`_SUCCESS`, `.*.crc`) that the global patterns won't catch — Task 1 adds an explicit ignore line for this.
- pytest import convention (matches `missions/W4/M2_Analysis_Using_Spark/apps`): run tests as
  `cd M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/ -v` — invoking via
  `python -m pytest` from `M2/` puts `M2/` on `sys.path` so `tests/test_main.py` can `import main` directly
  (no package `__init__.py`, no `sys.path` hacks needed).
- Verified facts (already checked, don't re-derive): pyspark 4.2.0 installs and runs cleanly on this machine's
  Python 3.14 + OpenJDK 25 (Corretto); the parquet file has 4,090,836 rows and the schema listed in Task 3;
  the zone lookup CSV is reachable and has the 4 columns listed above.

---

### Task 1: Environment & Data Setup

**Files:**
- Modify: `/Users/yong/PycharmProjects/softeer-DE-wiki/.gitignore`
- Create: `missions/W5/M2/data/yellow_tripdata_2026-05.parquet` (copy, not authored — not a code artifact)
- Create: `missions/W5/M2/tests/conftest.py`

**Interfaces:**
- Produces: a pytest fixture `spark` (session-scoped `SparkSession`, `local[2]`) available to every test module
  under `M2/tests/`.

- [ ] **Step 1: Confirm dependencies are present in the shared venv**

Run:
```bash
/Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/pip show pyspark pytest pandas requests | grep -E "^(Name|Version):"
```
Expected: `Name: pyspark` / `Version: 4.2.0`, `Name: pytest` / `Version: 9.1.1` (or newer), and entries for
`pandas` and `requests`. If any package is missing, install it with
`/Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/pip install pyspark pytest pandas requests`.

- [ ] **Step 2: Copy the trip data file from W5/M1**

Run:
```bash
mkdir -p /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/data
cp /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M1/data/yellow_tripdata_2026-05.parquet \
   /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/data/yellow_tripdata_2026-05.parquet
```
Verify: `ls -la /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/data/yellow_tripdata_2026-05.parquet`
shows a ~70MB file.

- [ ] **Step 3: Add a scoped ignore rule for M2's data directory**

Append to `/Users/yong/PycharmProjects/softeer-DE-wiki/.gitignore` (after the existing `/missions/W5/M1/data/`
line):
```
/missions/W5/M2/data/
```

- [ ] **Step 4: Create the pytest Spark fixture**

Create `missions/W5/M2/tests/conftest.py`:
```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("w5m2-pipeline-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
```

- [ ] **Step 5: Smoke-test the fixture and the copied data**

Run:
```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2
/Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("smoke").master("local[2]").getOrCreate()
df = spark.read.parquet("data/yellow_tripdata_2026-05.parquet")
assert df.count() == 4090836, df.count()
print("OK", len(df.columns), "columns")
spark.stop()
EOF
```
Expected output ends with `OK 19 columns` and no traceback.

- [ ] **Step 6: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add .gitignore missions/W5/M2/tests/conftest.py
git commit -m "chore(W5M2): 데이터 준비 및 pytest spark fixture 추가"
```
(The copied parquet file itself is gitignored by the rule added in Step 3, so it will not be staged — confirm
with `git status` that only `.gitignore` and `conftest.py` are staged.)

---

### Task 2: Zone Lookup Download

**Files:**
- Create: `missions/W5/M2/main.py`
- Create: `missions/W5/M2/tests/test_main.py`

**Interfaces:**
- Produces: `download_zone_lookup(dest_path, url: str = ZONE_LOOKUP_URL, timeout: int = 60) -> Path` — downloads
  the file if `dest_path` doesn't already exist; raises `RuntimeError` on non-200 responses. Later tasks
  (main()) call this with the real URL and a path under `M2/data/`.

- [ ] **Step 1: Write the failing tests**

Create `missions/W5/M2/tests/test_main.py`:
```python
from pathlib import Path

import pytest

import main


class FakeResponse:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content


def test_download_zone_lookup_skips_when_already_exists(tmp_path, monkeypatch):
    dest = tmp_path / "existing.csv"
    dest.write_bytes(b"already-here")
    calls = []
    monkeypatch.setattr(main.requests, "get", lambda *a, **k: calls.append(1))

    result = main.download_zone_lookup(dest, url="http://example.com/x.csv")

    assert result == dest
    assert calls == []
    assert dest.read_bytes() == b"already-here"


def test_download_zone_lookup_downloads_when_missing(tmp_path, monkeypatch):
    dest = tmp_path / "sub" / "zones.csv"
    monkeypatch.setattr(
        main.requests, "get",
        lambda *a, **k: FakeResponse(200, content=b"LocationID,Borough\n1,EWR\n"),
    )

    result = main.download_zone_lookup(dest, url="http://example.com/zones.csv")

    assert result == dest
    assert dest.read_bytes() == b"LocationID,Borough\n1,EWR\n"


def test_download_zone_lookup_raises_on_http_error(tmp_path, monkeypatch):
    dest = tmp_path / "missing.csv"
    monkeypatch.setattr(main.requests, "get", lambda *a, **k: FakeResponse(404))

    with pytest.raises(RuntimeError):
        main.download_zone_lookup(dest, url="http://example.com/missing.csv")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2
/Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v
```
Expected: collection error (`ModuleNotFoundError: No module named 'main'` or similar) since `main.py` doesn't
exist yet.

- [ ] **Step 3: Create `main.py` with the download function**

Create `missions/W5/M2/main.py`:
```python
from pathlib import Path

import requests
from pyspark.sql import SparkSession

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

MAX_TRIP_DURATION_MIN = 180
MAX_TRIP_DISTANCE_MI = 100
MIN_PASSENGER_COUNT = 1
MAX_PASSENGER_COUNT = 6


def download_zone_lookup(dest_path, url: str = ZONE_LOOKUP_URL, timeout: int = 60) -> Path:
    dest_path = Path(dest_path)
    if dest_path.exists():
        return dest_path
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url}: HTTP {response.status_code}")
    dest_path.write_bytes(response.content)
    return dest_path
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/tests/test_main.py
git commit -m "feat(W5M2): taxi zone lookup 다운로드 함수 추가"
```

---

### Task 3: Load Functions

**Files:**
- Modify: `missions/W5/M2/main.py` (append)
- Modify: `missions/W5/M2/tests/test_main.py` (append)

**Interfaces:**
- Consumes: nothing from earlier tasks (only `pyspark.sql.SparkSession`/`DataFrame`).
- Produces: `load_trips(spark: SparkSession, path: str) -> DataFrame` (thin wrapper over
  `spark.read.parquet`), `load_zone_lookup(spark: SparkSession, path: str) -> DataFrame` (thin wrapper over
  `spark.read.option("header", True).option("inferSchema", True).csv`). Task 9's `run_pipeline` calls both.

- [ ] **Step 1: Write the failing tests**

Append to `missions/W5/M2/tests/test_main.py`:
```python
def test_load_trips_reads_parquet(spark, tmp_path):
    pdf_rows = [(1, 2.5)]
    df = spark.createDataFrame(pdf_rows, ["VendorID", "trip_distance"])
    df.write.parquet(str(tmp_path / "trips.parquet"), mode="overwrite")

    result = main.load_trips(spark, str(tmp_path / "trips.parquet"))

    assert result.count() == 1
    assert "trip_distance" in result.columns


def test_load_zone_lookup_reads_csv_with_header(spark, tmp_path):
    csv_path = tmp_path / "zones.csv"
    csv_path.write_text("LocationID,Borough,Zone,service_zone\n1,EWR,Newark Airport,EWR\n")

    result = main.load_zone_lookup(spark, str(csv_path))

    row = result.collect()[0]
    assert row["LocationID"] == 1
    assert row["Borough"] == "EWR"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v -k "load_trips or load_zone_lookup"`
Expected: FAIL with `AttributeError: module 'main' has no attribute 'load_trips'` (and same for `load_zone_lookup`).

- [ ] **Step 3: Implement the load functions**

Append to `missions/W5/M2/main.py` (below the imports/constants, after `download_zone_lookup`):
```python
from pyspark.sql import DataFrame


def load_trips(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def load_zone_lookup(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)
```
(Move the `from pyspark.sql import DataFrame` import up to the top import block alongside the existing
`from pyspark.sql import SparkSession` line instead of leaving it inline — i.e. the top of `main.py` should read
`from pyspark.sql import DataFrame, SparkSession`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v`
Expected: all tests so far pass (5 total).

- [ ] **Step 5: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/tests/test_main.py
git commit -m "feat(W5M2): trip/zone lookup 로드 함수 추가"
```

---

### Task 4: Cleaning Transformation

**Files:**
- Modify: `missions/W5/M2/main.py` (append)
- Modify: `missions/W5/M2/tests/test_main.py` (append)

**Interfaces:**
- Consumes: raw trips `DataFrame` as produced by `load_trips` (columns: `tpep_pickup_datetime`,
  `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `fare_amount`, `PULocationID`, `DOLocationID`,
  plus others).
- Produces: `clean_trips(df: DataFrame) -> DataFrame` — adds `trip_duration_min` (float minutes),
  `pickup_date` (date), `pickup_hour` (int 0-23) columns and drops null/out-of-range rows per the thresholds in
  Global Constraints. Every downstream transformation task (5, 6, 7) consumes this function's output.

- [ ] **Step 1: Write the failing test**

Append to `missions/W5/M2/tests/test_main.py`:
```python
from datetime import date, datetime


def _trip_row(
    pickup=datetime(2024, 1, 1, 8, 0, 0),
    dropoff=datetime(2024, 1, 1, 8, 10, 0),
    passenger_count=1,
    trip_distance=2.5,
    fare_amount=12.0,
    pu_location_id=100,
    do_location_id=200,
):
    return (pickup, dropoff, passenger_count, trip_distance, fare_amount, pu_location_id, do_location_id)


_TRIP_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "PULocationID", "DOLocationID",
]


def test_clean_trips_filters_invalid_rows_and_adds_derived_columns(spark):
    rows = [
        _trip_row(),  # valid
        _trip_row(dropoff=datetime(2024, 1, 1, 7, 50, 0)),  # negative duration
        _trip_row(trip_distance=0.0),  # zero distance
        _trip_row(trip_distance=150.0),  # distance too far
        _trip_row(trip_distance=None),  # null distance
        _trip_row(fare_amount=-1.0),  # negative fare
        _trip_row(passenger_count=0),  # passenger count too low
        _trip_row(passenger_count=7),  # passenger count too high
        _trip_row(pu_location_id=None),  # null pickup location
    ]
    df = spark.createDataFrame(rows, _TRIP_COLUMNS)

    cleaned = main.clean_trips(df)
    result = cleaned.collect()

    assert len(result) == 1
    row = result[0]
    assert row["trip_distance"] == 2.5
    assert abs(row["trip_duration_min"] - 10.0) < 1e-6
    assert row["pickup_date"] == date(2024, 1, 1)
    assert row["pickup_hour"] == 8
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v -k clean_trips`
Expected: FAIL with `AttributeError: module 'main' has no attribute 'clean_trips'`.

- [ ] **Step 3: Implement `clean_trips`**

Append to `missions/W5/M2/main.py`:
```python
from pyspark.sql import functions as F


def clean_trips(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=[
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "fare_amount", "PULocationID", "DOLocationID",
        ])
        .withColumn(
            "trip_duration_min",
            (
                F.col("tpep_dropoff_datetime").cast("long")
                - F.col("tpep_pickup_datetime").cast("long")
            ) / 60.0,
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .filter(
            (F.col("trip_duration_min") > 0)
            & (F.col("trip_duration_min") <= MAX_TRIP_DURATION_MIN)
            & (F.col("trip_distance") > 0)
            & (F.col("trip_distance") <= MAX_TRIP_DISTANCE_MI)
            & (F.col("passenger_count") >= MIN_PASSENGER_COUNT)
            & (F.col("passenger_count") <= MAX_PASSENGER_COUNT)
            & (F.col("fare_amount") >= 0)
        )
    )
```
(Move `from pyspark.sql import functions as F` up to the top import block.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v`
Expected: all tests pass (6 total).

- [ ] **Step 5: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/tests/test_main.py
git commit -m "feat(W5M2): 트립 클리닝(null/이상치 제거, 파생 컬럼) 구현"
```

---

### Task 5: Filtering Transformation (Multi-Passenger)

**Files:**
- Modify: `missions/W5/M2/main.py` (append)
- Modify: `missions/W5/M2/tests/test_main.py` (append)

**Interfaces:**
- Consumes: the cleaned `DataFrame` produced by `clean_trips` (must have `passenger_count`).
- Produces: `filter_multi_passenger(df: DataFrame) -> DataFrame` — rows with `passenger_count > 1`. Task 9's
  `run_pipeline` uses this as the `collect()` action's source.

- [ ] **Step 1: Write the failing test**

Append to `missions/W5/M2/tests/test_main.py`:
```python
def test_filter_multi_passenger_keeps_only_more_than_one_rider(spark):
    rows = [
        _trip_row(passenger_count=1),
        _trip_row(passenger_count=2),
        _trip_row(passenger_count=3),
    ]
    cleaned = main.clean_trips(spark.createDataFrame(rows, _TRIP_COLUMNS))

    result = main.filter_multi_passenger(cleaned).collect()

    assert sorted(row["passenger_count"] for row in result) == [2, 3]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v -k filter_multi_passenger`
Expected: FAIL with `AttributeError: module 'main' has no attribute 'filter_multi_passenger'`.

- [ ] **Step 3: Implement `filter_multi_passenger`**

Append to `missions/W5/M2/main.py`:
```python
def filter_multi_passenger(df: DataFrame) -> DataFrame:
    return df.filter(F.col("passenger_count") > 1)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v`
Expected: all tests pass (7 total).

- [ ] **Step 5: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/tests/test_main.py
git commit -m "feat(W5M2): 다인승 필터링 변환 추가"
```

---

### Task 6: Aggregation Transformations (Daily Summary, Hourly Counts)

**Files:**
- Modify: `missions/W5/M2/main.py` (append)
- Modify: `missions/W5/M2/tests/test_main.py` (append)

**Interfaces:**
- Consumes: the cleaned `DataFrame` produced by `clean_trips` (needs `pickup_date`, `pickup_hour`,
  `trip_distance`, `fare_amount`).
- Produces: `compute_daily_summary(df: DataFrame) -> DataFrame` (columns: `pickup_date`, `trip_count`,
  `avg_trip_distance_mi`, `total_fare_amount`, ordered by `pickup_date`); `compute_hourly_counts(df: DataFrame)
  -> DataFrame` (columns: `pickup_hour`, `trip_count`, ordered by `pickup_hour`). Task 9's `run_pipeline` writes
  both as output tables.

- [ ] **Step 1: Write the failing tests**

Append to `missions/W5/M2/tests/test_main.py`:
```python
def _cleaned_sample(spark):
    rows = [
        _trip_row(pickup=datetime(2024, 1, 1, 8, 0, 0), trip_distance=2.0, fare_amount=10.0),
        _trip_row(pickup=datetime(2024, 1, 1, 8, 30, 0), trip_distance=4.0, fare_amount=10.0),
        _trip_row(pickup=datetime(2024, 1, 2, 5, 0, 0), trip_distance=3.0, fare_amount=10.0),
    ]
    return main.clean_trips(spark.createDataFrame(rows, _TRIP_COLUMNS))


def test_compute_daily_summary_groups_by_pickup_date(spark):
    cleaned = _cleaned_sample(spark)

    result = {
        row["pickup_date"]: (row["trip_count"], row["avg_trip_distance_mi"], row["total_fare_amount"])
        for row in main.compute_daily_summary(cleaned).collect()
    }

    assert result[date(2024, 1, 1)] == (2, 3.0, 20.0)
    assert result[date(2024, 1, 2)] == (1, 3.0, 10.0)


def test_compute_hourly_counts_groups_by_pickup_hour(spark):
    cleaned = _cleaned_sample(spark)

    result = {
        row["pickup_hour"]: row["trip_count"]
        for row in main.compute_hourly_counts(cleaned).collect()
    }

    assert result == {8: 2, 5: 1}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v -k "daily_summary or hourly_counts"`
Expected: FAIL with `AttributeError` for both missing functions.

- [ ] **Step 3: Implement the aggregation functions**

Append to `missions/W5/M2/main.py`:
```python
def compute_daily_summary(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_date")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance_mi"),
            F.sum("fare_amount").alias("total_fare_amount"),
        )
        .orderBy("pickup_date")
    )


def compute_hourly_counts(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy("pickup_hour")
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v`
Expected: all tests pass (9 total).

- [ ] **Step 5: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/tests/test_main.py
git commit -m "feat(W5M2): 일별/시간대별 집계 변환 추가"
```

---

### Task 7: Join Transformation (Borough Summary via Broadcast Join)

**Files:**
- Modify: `missions/W5/M2/main.py` (append)
- Modify: `missions/W5/M2/tests/test_main.py` (append)

**Interfaces:**
- Consumes: the cleaned trips `DataFrame` (needs `PULocationID`, `fare_amount`) and the zone lookup
  `DataFrame` produced by `load_zone_lookup` (needs `LocationID`, `Borough`).
- Produces: `compute_borough_summary(df: DataFrame, zone_lookup_df: DataFrame) -> DataFrame` (columns:
  `pickup_borough`, `trip_count`, `avg_fare_amount`, ordered by `trip_count` descending). Uses `F.broadcast()`
  on the (small) zone lookup side so the join has no shuffle stage — this is the DAG-optimization point the
  report explains. Task 9's `run_pipeline` writes this as an output table.

- [ ] **Step 1: Write the failing test**

Append to `missions/W5/M2/tests/test_main.py`:
```python
def test_compute_borough_summary_joins_on_pickup_location_and_groups_by_borough(spark):
    trip_rows = [
        _trip_row(pu_location_id=100, fare_amount=10.0),
        _trip_row(pu_location_id=100, fare_amount=20.0),
        _trip_row(pu_location_id=200, fare_amount=5.0),
    ]
    cleaned = main.clean_trips(spark.createDataFrame(trip_rows, _TRIP_COLUMNS))
    zone_lookup = spark.createDataFrame(
        [(100, "Manhattan", "Zone A", "Yellow Zone"), (200, "Queens", "Zone B", "Boro Zone")],
        ["LocationID", "Borough", "Zone", "service_zone"],
    )

    result = {
        row["pickup_borough"]: (row["trip_count"], row["avg_fare_amount"])
        for row in main.compute_borough_summary(cleaned, zone_lookup).collect()
    }

    assert result["Manhattan"] == (2, 15.0)
    assert result["Queens"] == (1, 5.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v -k borough_summary`
Expected: FAIL with `AttributeError: module 'main' has no attribute 'compute_borough_summary'`.

- [ ] **Step 3: Implement `compute_borough_summary`**

Append to `missions/W5/M2/main.py`:
```python
def compute_borough_summary(df: DataFrame, zone_lookup_df: DataFrame) -> DataFrame:
    zone_broadcast = F.broadcast(
        zone_lookup_df.select(
            F.col("LocationID").alias("PULocationID"),
            F.col("Borough").alias("pickup_borough"),
        )
    )
    return (
        df.join(zone_broadcast, on="PULocationID", how="inner")
        .groupBy("pickup_borough")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare_amount"),
        )
        .orderBy(F.col("trip_count").desc())
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v`
Expected: all tests pass (10 total).

- [ ] **Step 5: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/tests/test_main.py
git commit -m "feat(W5M2): zone lookup broadcast join 기반 borough 집계 추가"
```

---

### Task 8: Write Action Helper

**Files:**
- Modify: `missions/W5/M2/main.py` (append)
- Modify: `missions/W5/M2/tests/test_main.py` (append)

**Interfaces:**
- Consumes: any result `DataFrame` (e.g. from Tasks 6/7).
- Produces: `write_output_table(df: DataFrame, output_dir: str, name: str) -> None` — writes
  `{output_dir}/{name}` as parquet (`mode("overwrite")`) and `{output_dir}/{name}_csv` as a single-file CSV
  with header. Task 9's `run_pipeline` calls this once per result table (the plan's "write" action).

- [ ] **Step 1: Write the failing test**

Append to `missions/W5/M2/tests/test_main.py`:
```python
def test_write_output_table_creates_parquet_and_csv(spark, tmp_path):
    df = spark.createDataFrame([(1, "Manhattan"), (2, "Queens")], ["trip_count", "pickup_borough"])

    main.write_output_table(df, str(tmp_path), "borough_summary")

    parquet_dir = tmp_path / "borough_summary"
    csv_dir = tmp_path / "borough_summary_csv"
    assert parquet_dir.exists()
    assert csv_dir.exists()

    read_back = spark.read.parquet(str(parquet_dir))
    assert read_back.count() == 2

    csv_files = list(csv_dir.glob("*.csv"))
    assert len(csv_files) == 1
    assert "trip_count,pickup_borough" in csv_files[0].read_text()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v -k write_output_table`
Expected: FAIL with `AttributeError: module 'main' has no attribute 'write_output_table'`.

- [ ] **Step 3: Implement `write_output_table`**

Append to `missions/W5/M2/main.py`:
```python
def write_output_table(df: DataFrame, output_dir: str, name: str) -> None:
    df.write.mode("overwrite").parquet(f"{output_dir}/{name}")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/{name}_csv")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v`
Expected: all tests pass (11 total).

- [ ] **Step 5: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/tests/test_main.py
git commit -m "feat(W5M2): parquet+csv 결과 저장 헬퍼 추가"
```

---

### Task 9: Pipeline Orchestration (Cache, Lazy-Eval Logging, Actions)

**Files:**
- Modify: `missions/W5/M2/main.py` (append)
- Modify: `missions/W5/M2/tests/test_main.py` (append)

**Interfaces:**
- Consumes: `load_trips`, `load_zone_lookup`, `clean_trips`, `filter_multi_passenger`, `compute_daily_summary`,
  `compute_hourly_counts`, `compute_borough_summary`, `write_output_table` (all from Tasks 3-8).
- Produces: `run_pipeline(spark: SparkSession, trips_path: str, zone_lookup_path: str, output_dir: str) -> dict`
  returning `{"raw_count": int, "cleaned_count": int, "sample_rows": list[dict], "daily_summary": DataFrame,
  "hourly_counts": DataFrame, "borough_summary": DataFrame, "log": list[str]}`. Task 10's `main()` calls this
  with real paths.

- [ ] **Step 1: Write the failing integration test**

Append to `missions/W5/M2/tests/test_main.py`:
```python
def test_run_pipeline_executes_end_to_end(spark, tmp_path):
    trip_rows = [
        _trip_row(pickup=datetime(2024, 1, 1, 8, 0, 0), passenger_count=2, pu_location_id=100),
        _trip_row(pickup=datetime(2024, 1, 1, 9, 0, 0), passenger_count=1, pu_location_id=200),
        _trip_row(trip_distance=None),  # dropped by cleaning
    ]
    trips_path = tmp_path / "trips.parquet"
    spark.createDataFrame(trip_rows, _TRIP_COLUMNS).write.parquet(str(trips_path), mode="overwrite")

    zone_lookup_path = tmp_path / "zones.csv"
    zone_lookup_path.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "100,Manhattan,Zone A,Yellow Zone\n"
        "200,Queens,Zone B,Boro Zone\n"
    )

    output_dir = tmp_path / "output"

    result = main.run_pipeline(spark, str(trips_path), str(zone_lookup_path), str(output_dir))

    assert result["raw_count"] == 3
    assert result["cleaned_count"] == 2
    assert len(result["sample_rows"]) == 1  # only the passenger_count=2 row
    assert any(entry.startswith("[lazy]") for entry in result["log"])
    assert any(entry.startswith("[action]") for entry in result["log"])
    assert (output_dir / "daily_summary").exists()
    assert (output_dir / "hourly_counts").exists()
    assert (output_dir / "borough_summary").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v -k run_pipeline`
Expected: FAIL with `AttributeError: module 'main' has no attribute 'run_pipeline'`.

- [ ] **Step 3: Implement `run_pipeline`**

Append to `missions/W5/M2/main.py`:
```python
import time


def run_pipeline(spark: SparkSession, trips_path: str, zone_lookup_path: str, output_dir: str) -> dict:
    log = []

    def emit(message: str) -> None:
        log.append(message)
        print(message)

    trips_df = load_trips(spark, trips_path)
    zone_df = load_zone_lookup(spark, zone_lookup_path)

    raw_count = trips_df.count()
    emit(f"[action] raw row count = {raw_count}")

    cleaned_df = clean_trips(trips_df)
    cleaned_count = cleaned_df.count()
    emit(f"[action] cleaned row count = {cleaned_count} (dropped {raw_count - cleaned_count})")

    cleaned_df = cleaned_df.cache()
    cleaned_df.count()  # materialize the cache before it's reused below
    emit("[action] cache materialized on cleaned_df")

    multi_passenger_df = filter_multi_passenger(cleaned_df)
    daily_summary_df = compute_daily_summary(cleaned_df)
    hourly_counts_df = compute_hourly_counts(cleaned_df)
    borough_summary_df = compute_borough_summary(cleaned_df, zone_df)

    emit(
        f"[lazy] transformations defined at {time.time():.3f} "
        "-- no Spark job has run for these DataFrames yet"
    )
    emit("[lazy] daily_summary_df physical plan (explain() does not trigger a job):")
    daily_summary_df.explain(mode="extended")

    emit(f"[action] collect() called at {time.time():.3f}")
    sample_rows = [row.asDict() for row in multi_passenger_df.limit(20).collect()]
    emit(f"[action] collect() returned at {time.time():.3f}, {len(sample_rows)} rows")

    for name, result_df in [
        ("daily_summary", daily_summary_df),
        ("hourly_counts", hourly_counts_df),
        ("borough_summary", borough_summary_df),
    ]:
        emit(f"[action] write({name}) called at {time.time():.3f}")
        write_output_table(result_df, output_dir, name)
        emit(f"[action] write({name}) finished at {time.time():.3f}")

    return {
        "raw_count": raw_count,
        "cleaned_count": cleaned_count,
        "sample_rows": sample_rows,
        "daily_summary": daily_summary_df,
        "hourly_counts": hourly_counts_df,
        "borough_summary": borough_summary_df,
        "log": log,
    }
```
(Move `import time` up to the top of the file with the other imports.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/test_main.py -v`
Expected: all tests pass (12 total).

- [ ] **Step 5: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/tests/test_main.py
git commit -m "feat(W5M2): 캐싱/lazy-eval 로깅을 포함한 파이프라인 오케스트레이션 추가"
```

---

### Task 10: CLI Entry Point, Real Run, and Spark UI DAG Screenshot

**Files:**
- Modify: `missions/W5/M2/main.py` (append `main()` + `if __name__ == "__main__"`)
- Create (at run time, not authored): `missions/W5/M2/run.log`, `missions/W5/M2/data/taxi_zone_lookup.csv`,
  `missions/W5/M2/data/output/{daily_summary,daily_summary_csv,hourly_counts,hourly_counts_csv,
  borough_summary,borough_summary_csv}`
- Create: `missions/W5/M2/report_assets/spark_ui_dag.png`

**Interfaces:**
- Consumes: `download_zone_lookup`, `run_pipeline` (Tasks 2, 9).
- Produces: a runnable script (`python main.py`) and, from actually running it, the log file and screenshot
  Task 11's report depends on.

> **Execution note:** this task needs a real Spark UI running at `localhost:4040` plus a Chrome browser
> automation tool to screenshot it. If you are a subagent without Chrome tool access, do steps 1-2 (the code)
> and stop — hand the remaining steps back to the orchestrating session to run interactively.

- [ ] **Step 1: Implement `main()`**

Append to `missions/W5/M2/main.py`:
```python
from pathlib import Path


def main():
    base_dir = Path(__file__).parent
    data_dir = base_dir / "data"
    zone_lookup_path = data_dir / "taxi_zone_lookup.csv"
    trips_path = data_dir / "yellow_tripdata_2026-05.parquet"
    output_dir = data_dir / "output"

    download_zone_lookup(zone_lookup_path)

    spark = (
        SparkSession.builder
        .appName("NYCTaxiDataFrameAnalysis")
        .master("local[4]")
        .getOrCreate()
    )
    print(f"Spark version: {spark.version}")

    result = run_pipeline(spark, str(trips_path), str(zone_lookup_path), str(output_dir))

    print(f"raw_count = {result['raw_count']}")
    print(f"cleaned_count = {result['cleaned_count']}")
    print(f"sample multi-passenger rows (first 5): {result['sample_rows'][:5]}")
    print("daily_summary:")
    result["daily_summary"].show(31, truncate=False)
    print("hourly_counts:")
    result["hourly_counts"].show(24, truncate=False)
    print("borough_summary:")
    result["borough_summary"].show(10, truncate=False)

    input("Enter를 누르면 종료합니다 (그 전에 http://localhost:4040 에서 DAG 확인)...")
    spark.stop()


if __name__ == "__main__":
    main()
```
(`Path` is already imported at the top from Task 2's `download_zone_lookup` — do not add a duplicate import;
if it isn't already at the top, move it there alongside the other imports.)

- [ ] **Step 2: Run the full test suite once more before the real run**

Run: `cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python -m pytest tests/ -v`
Expected: all 12 tests still pass (adding `main()` doesn't change any tested function).

- [ ] **Step 3: Launch the real pipeline in a tmux session (so `input()` can pause on a real TTY)**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2
tmux new-session -d -s w5m2 -x 220 -y 50
tmux send-keys -t w5m2 \
  "cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2 && /Users/yong/PycharmProjects/softeer-DE-wiki/.venv/bin/python main.py 2>&1 | tee run.log" \
  Enter
```

- [ ] **Step 4: Wait for the pipeline to reach the pause prompt**

```bash
until grep -q "Enter를 누르면" /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/run.log 2>/dev/null; do
  sleep 5
done
```
This can take a few minutes on first run (zone lookup download + full 4M-row parquet scan across 6+ Spark
jobs). If it doesn't finish within ~10 minutes, run `tmux capture-pane -t w5m2 -p | tail -80` to check for a
stack trace instead of continuing to wait.

- [ ] **Step 5: Capture the Spark UI DAG screenshot**

Use the Chrome browser automation tools (load them via `ToolSearch` with query
`"select:mcp__claude-in-chrome__tabs_context_mcp,mcp__claude-in-chrome__navigate,mcp__claude-in-chrome__computer,mcp__claude-in-chrome__tabs_create_mcp"`
if not already loaded):
1. Open a new tab to `http://localhost:4040/jobs/`.
2. Find the job corresponding to the `borough_summary` write (it's the one with the join — look for the job
   whose description mentions `write` and is the third of the three `write` jobs, or check `run.log` for the
   order the `[action] write(...)` lines were emitted in).
3. Click into that job's detail page to see its DAG visualization (stages graph).
4. Take a screenshot of the DAG visualization.
5. Save it to `/Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/report_assets/spark_ui_dag.png`.

- [ ] **Step 6: Resume and let the script finish**

```bash
tmux send-keys -t w5m2 Enter
sleep 5
tmux capture-pane -t w5m2 -p | tail -60
tmux kill-session -t w5m2
```

- [ ] **Step 7: Verify the run's artifacts**

```bash
ls /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/data/output/
ls /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/report_assets/
grep -E "^\[action\] (raw|cleaned) row count" /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/run.log
```
Expected: `data/output/` contains `daily_summary`, `daily_summary_csv`, `hourly_counts`, `hourly_counts_csv`,
`borough_summary`, `borough_summary_csv`; `report_assets/` contains `spark_ui_dag.png`; the grep shows both the
raw and cleaned row-count log lines with real numbers.

- [ ] **Step 8: Commit the code change (not the run outputs, which are gitignored)**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/main.py missions/W5/M2/report_assets/spark_ui_dag.png
git commit -m "feat(W5M2): CLI 진입점 추가 및 Spark UI DAG 스크린샷 캡처"
```
(`run.log` is gitignored by the root `*.log` rule and should not be staged — confirm with `git status`.)

---

### Task 11: Write REPORT.md

**Files:**
- Create: `missions/W5/M2/REPORT.md`

**Interfaces:**
- Consumes: `missions/W5/M2/run.log` (Task 10's captured stdout, for real row counts / sample output / timing
  log lines) and `missions/W5/M2/report_assets/spark_ui_dag.png` (Task 10's screenshot).

- [ ] **Step 1: Extract the concrete numbers from the real run**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2
grep -E "^\[(action|lazy)\]" run.log
```
Note the exact `raw_count`, `cleaned_count`, and the three `[lazy]`/timestamped `[action]` lines — these go
into the report verbatim rather than being estimated.

- [ ] **Step 2: Write the report**

Create `missions/W5/M2/REPORT.md` with these sections (fill in the bracketed values using the real numbers
from Step 1 and the `daily_summary`/`hourly_counts`/`borough_summary` tables printed in `run.log`):

```markdown
# NYC TLC Trip Data — Spark DataFrame/DAG Analysis Report

## 실행 환경
- PySpark 4.2.0, `local[4]` master
- venv: `softeer-DE-wiki/.venv` (Python 3.14)
- 데이터: `missions/W5/M1/data/yellow_tripdata_2026-05.parquet` 재사용 + TLC 공식 Taxi Zone Lookup CSV

## 파이프라인 단계
1. **로딩**: parquet(트립)/csv(zone lookup)를 스키마 추론으로 로드
2. **클리닝**: 필수 컬럼 null 제거 + `trip_duration_min`/`pickup_date`/`pickup_hour` 파생 + 이상치 필터링
   - raw row count: [raw_count 채우기]
   - cleaned row count: [cleaned_count 채우기] (드롭 비율 [퍼센트 계산해서 채우기])
3. **캐싱**: 클리닝된 DataFrame을 `cache()` 후 `count()`로 materialize — 이후 4개의 갈래(다인승 필터,
   일별 집계, 시간대별 집계, borough join)가 모두 이 캐시를 재사용
4. **변환 3종**
   - Filtering: `passenger_count > 1` 다인승 트립 추출
   - Aggregation: 일별 트립수/평균거리/총매출, 시간대별 트립수
   - Join: zone lookup을 `broadcast()`로 조인해 borough별 집계 (셔플 스테이지 없음)
5. **액션 2종 이상**: 다인승 샘플 `collect()`, 결과 3종 `write`(parquet+csv)

## Lazy Evaluation 시연
`run.log`에서 발췌:
```
[여기에 run.log의 [lazy]/[action] 라인 3~5개를 그대로 붙여넣기]
```
변환(4번 단계) 코드는 위 로그의 `[lazy] transformations defined at ...` 시점에 이미 다 작성되어 있었지만,
그 시점까지 어떤 Spark 잡도 실행되지 않았다. 실제 잡은 `collect()`/`write()` 호출 시점에야 발생했다
(타임스탬프 차이로 확인 가능).

## DAG 및 스테이지 최적화
![Spark UI DAG](report_assets/spark_ui_dag.png)

- 캐싱: 클리닝 단계(파싱 + 이상치 필터 + 파생 컬럼 계산)가 한 번만 실행되고, 이후 4개 갈래가 모두 캐시된
  결과를 재사용한다. 캐싱이 없었다면 각 갈래가 parquet을 처음부터 다시 읽고 재파싱했을 것이다.
- Broadcast Join: zone lookup(265행 내외의 작은 테이블)을 `F.broadcast()`로 명시해, sort-merge join이었다면
  필요했을 셔플(Exchange) 스테이지를 제거했다. 스크린샷의 DAG에서 join 스테이지에 `BroadcastHashJoin`이
  나타나고 별도의 셔플 스테이지가 없는 것을 확인.

## 결과 요약
- 일별 요약 (daily_summary): [run.log의 show() 출력에서 요약 문구 작성]
- 피크아워 (hourly_counts): [상위 3개 시간대 채우기]
- Borough별 집계 (borough_summary): [상위 borough와 트립수/평균요금 채우기]
```

- [ ] **Step 3: Verify the report is complete**

```bash
grep -c "\[여기에\|채우기\|TBD\|TODO" /Users/yong/PycharmProjects/softeer-DE-wiki/missions/W5/M2/REPORT.md
```
Expected: `0` — every bracketed placeholder must be replaced with real values before this check passes. If it's
not `0`, go back and fill in the remaining placeholders from `run.log`.

- [ ] **Step 4: Commit**

```bash
cd /Users/yong/PycharmProjects/softeer-DE-wiki
git add missions/W5/M2/REPORT.md
git commit -m "docs(W5M2): DAG/lazy-eval 리포트 작성"
```
