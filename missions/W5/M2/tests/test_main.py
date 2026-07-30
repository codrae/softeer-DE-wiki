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
