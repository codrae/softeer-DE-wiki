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
