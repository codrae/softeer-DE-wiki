from pyspark.sql import SparkSession
import datetime
import os
import shutil


def overwrite_dir(path):
    if os.path.exists(path):
        print(f"삭제할 경로: {path}")
        shutil.rmtree(path)

spark = SparkSession.builder.appName("PySpark").master("local[*]").getOrCreate()

df = spark.read.parquet("data/yellow_tripdata_2026-05.parquet")
#df.printSchema()
df = df.select("tpep_pickup_datetime", "fare_amount", "trip_distance")
print("-----------df.select---------------")
#df.printSchema()

rdd = df.rdd

# print("row count", rdd.count())
# for row in rdd.take(5):
#     print(row)

# Data Cleaning
rdd = rdd.filter(lambda x : x["fare_amount"] is not None and x["trip_distance"] is not None)
rdd = rdd.filter(lambda x : x["fare_amount"] > 0 and x["trip_distance"] > 0)

start_date = datetime.date(2026,5,1)
end_date = datetime.date(2026,5,31)


rdd = rdd.map(lambda x : (x["tpep_pickup_datetime"].date(), x["fare_amount"], x["trip_distance"]) )
rdd = rdd.filter(lambda x : end_date >= x[0] >= start_date)

print(f"rdd.first : {rdd.first()}")

# RDD 캐싱
rdd.cache()

# 총 트립 수 / 총 매출 / 평균 트립 거리 (reduce 한 번으로 동시 계산)
trip_count, fare_sum, dist_sum = rdd.map(lambda x: (1, x[1], x[2])) \
    .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))

total_trip = trip_count
total_sales = fare_sum
avg_distance = dist_sum / trip_count
print(f"total_trip = {total_trip}")
print(f"total_sales = {total_sales}")
print(f"avg_distance = {avg_distance}")

# 날짜별 트립 수 / 날짜별 매출 (reduceByKey 한 번으로 동시 계산)
daily_combined = rdd.map(lambda x: (x[0], (1, x[1]))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
daily_combined.cache()

daily_trip = daily_combined.mapValues(lambda v: v[0])
daily_sales = daily_combined.mapValues(lambda v: v[1])
print(f"daily_trip = {daily_trip.collect()}")
print(f"daily_sales = {daily_sales.collect()}")

overwrite_dir("data/output/daily_trip")
daily_trip.map(lambda x: str(x[0]) + "," + str(x[1])).saveAsTextFile("data/output/daily_trip")

overwrite_dir("data/output/daily_sales")
daily_sales.map(lambda x: str(x[0]) + "," + str(x[1])).saveAsTextFile("data/output/daily_sales")

summary = [("total_trip", total_trip), ("total_sales", total_sales), ("avg_distance", avg_distance)]
summary_rdd = spark.sparkContext.parallelize(summary, 1)
overwrite_dir("data/output/summary")
summary_rdd.map(lambda x: str(x[0]) + "," + str(x[1])).saveAsTextFile("data/output/summary")

input("Enter를 누르면 종료합니다 (그 전에 http://localhost:4040 에서 DAG 확인)...")


