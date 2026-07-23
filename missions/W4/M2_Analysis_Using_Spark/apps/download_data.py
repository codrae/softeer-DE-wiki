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
