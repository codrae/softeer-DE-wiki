FROM python:3.10-slim-bookworm
# ---- 1) 기본 패키지 + Java 설치 ----

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /usr/lib/jvm/default-java

# ---- 2) 환경변수 ----
ENV SPARK_VERSION=3.5.9
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

# ---- 3) Spark 다운로드 & 설치 ----
# with-hadoop 배포판 사용 (별도 Hadoop 클러스터 없이 내장 런타임으로 실행)
RUN curl -fsSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# ---- 4) 작업 디렉토리 (job 스크립트, 데이터, 결과 저장용) ----
WORKDIR /opt/spark-apps

# 로그/이벤트 디렉토리 (Spark Web UI, History 확인용)
RUN mkdir -p /opt/spark/logs /tmp/spark-events

# 컨테이너가 살아있도록 기본 커맨드는 bash로 두고,
# 실제 역할(master/worker)은 docker-compose의 command에서 지정함
CMD ["bash"]