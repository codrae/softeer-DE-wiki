"""
Monte Carlo 방법으로 원주율(π)을 추정하는 Spark job.
원본: $SPARK_HOME/examples/src/main/python/pi.py 를 기반으로 확장.
변경점: 계산 결과를 DataFrame으로 만들어 /opt/spark-data/output 에 parquet로 저장.
"""
import sys
import random
from pyspark.sql import SparkSession

def main():
    # ---- 1) SparkSession 생성 ----
    # master URL, appName 등은 spark-submit 커맨드라인 인자로 넘어오므로 여기선 지정 안 함
    spark = SparkSession.builder.appName("MonteCarloPiEstimation").getOrCreate()
    sc = spark.sparkContext

    # ---- 2) 파라미터 ----
    # 파티션 수 = 병렬 처리 단위. 인자로 안 주면 기본 4 (worker 2대 x 2코어에 맞춤)
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    # 파티션당 시도 횟수. 값이 클수록 정확도 상승, 연산 시간 증가
    n = 100000 * partitions

    # ---- 3) Monte Carlo 샘플링 함수 ----
    # 단위 정사각형(1x1) 안에 무작위 점을 찍어서, 반지름 1인 사분원 안에 들어가는 비율로 pi 추정
    def inside(_):
        x, y = random.random(), random.random()
        return 1 if x * x + y * y <= 1 else 0

    # ---- 4) 분산 연산 ----
    # sc.parallelize: n개의 시도를 partitions개로 나눠 executor들에게 분배
    count = sc.parallelize(range(1, n + 1), partitions).map(inside).reduce(lambda a, b: a + b)
    pi_estimate = 4.0 * count / n

    print(f"[RESULT] Pi is roughly {pi_estimate}")

    # ---- 5) 결과를 DataFrame으로 변환 후 parquet 저장 ----
    result_df = spark.createDataFrame(
        [(pi_estimate, n, partitions)],
        ["estimated_pi", "num_samples", "num_partitions"]
    )
    output_path = "/opt/spark-data/output/pi_result"
    result_df.write.mode("overwrite").parquet(output_path)
    print(f"[RESULT] Saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()