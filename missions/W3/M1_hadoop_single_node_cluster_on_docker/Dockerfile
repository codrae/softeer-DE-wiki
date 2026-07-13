FROM ubuntu:20.04

# 도중 대화형 프롬프트 방지. (자동 빌드)
ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    # start-dfs.sh가 ssh로 로컬에 접속해서 데몬 띄우기 위함
    ssh \
    # 병렬로 여러 노드에 명령 내리기 위함. (싱글 노드에서도 있으면 스크립트 실행에 좋음)
    pdsh \
    wget \
    # 디버깅 편의용
    net-tools \
    vim \
    # 패키지 인덱스 캐시 삭제
    && rm -rf /var/lib/apt/lists/* \

ENV HADOOP_VERSION=2.10.2
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
# hadoop, hdfs와 같은 명령어를 쉘 어디서나 실행 가능하도록
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys \
    # 이후 host key를 신뢰하겠습니까 라는 프롬프트에 대한 답을 미리 적어놓기 (자동화) \
    && echo "Host localhost" >> ~/.ssh/config \
    && echo "    StrictHostKeyChecking no" >> ~/.ssh/config

RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
COPY mapred-site.xml /opt/hadoop/etc/hadoop/mapred-site.xml
COPY yarn-site.xml /opt/hadoop/etc/hadoop/yarn-site.xml