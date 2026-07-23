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
