# W3M2a - Hadoop Multi-Node Cluster on Docker

Docker를 이용해 Hadoop 2.10.2 멀티노드(master 1개 + worker 3개) 클러스터를 구성한 프로젝트입니다.
W3M1(single-node)의 자산을 재사용하되, 역할별 이미지 분리와 컨테이너 재시작 시 자동 복구까지
고려한 구조로 확장했습니다.

## 목차
- [환경](#환경)
- [아키텍처](#아키텍처)
- [디렉토리 구조](#디렉토리-구조)
- [빌드 및 실행 방법](#빌드-및-실행-방법)
- [설정 파일 설명 및 근거](#설정-파일-설명-및-근거)
- [클러스터 등록 검증](#클러스터-등록-검증)
- [HDFS 동작 확인](#hdfs-동작-확인)
- [MapReduce 분산 처리 검증](#mapreduce-분산-처리-검증)
- [영속성(Persistence) 검증](#영속성persistence-검증)
- [웹 UI](#웹-ui)
- [알려진 이슈 및 참고사항](#알려진-이슈-및-참고사항)

---

## 환경

| 항목 | 값 |
|---|---|
| Base Image | ubuntu:20.04 |
| Java | OpenJDK 8 (apt 설치) |
| Hadoop | 2.10.2 |
| 노드 구성 | master 1개, worker 3개 (총 4개 컨테이너) |
| 오케스트레이션 | docker-compose |

> **참고**: Apple Silicon(ARM64) 환경 기준으로 `JAVA_HOME`이 설정되어 있습니다.
> x86_64 환경에서는 `Dockerfile.base`의 `JAVA_HOME` 경로를
> `/usr/lib/jvm/java-8-openjdk-arm64` → `/usr/lib/jvm/java-8-openjdk-amd64`로 변경해야 합니다.

## 아키텍처

### 노드 구성
- **master**: NameNode, ResourceManager, JobHistoryServer
- **worker1 / worker2 / worker3**: 각각 DataNode, NodeManager

### 데몬 기동 방식
각 컨테이너(entrypoint-master.sh / entrypoint-worker.sh)가 SSH를 통한 원격 제어 없이,
`hadoop-daemon.sh`/`yarn-daemon.sh`로 자기 역할의 데몬을 직접 기동합니다. 이에 따라
worker에는 SSH가 필요 없으며(원격 접속 주체가 없음), master는 디버깅 편의 목적으로만
SSH를 유지합니다.

### 노드 개수 및 Replication
Worker 3개, `dfs.replication=3`으로 구성했습니다. 각 worker는 독립된 named volume
(`hadoop-datanode1/2/3`)을 사용하여 블록 데이터가 서로 충돌하지 않도록 분리했습니다.

## 디렉토리 구조

```
.
├── Dockerfile.base
├── Dockerfile.master
├── Dockerfile.worker
├── entrypoint-master.sh
├── entrypoint-worker.sh
├── core-site.xml
├── hdfs-site.xml
├── mapred-site.xml
├── yarn-site.xml
├── docker-compose.yml
└── README.md
```

## 빌드 및 실행 방법

### 1. Base 이미지 빌드

```bash
docker build -f Dockerfile.base -t w3m2a-base .
```

`Dockerfile.base`는 표준 이름이 아니므로 `-f` 옵션으로 파일을 명시해야 합니다. Java(OpenJDK 8),
Hadoop 2.10.2 설치, 환경변수 설정, SSH 키 생성까지 master/worker가 공통으로 필요로 하는
부분을 이 단계에서 처리합니다.

### 2. 전체 클러스터 빌드 및 실행

```bash
docker compose up --build -d
```

`docker-compose.yml`이 `Dockerfile.master`/`Dockerfile.worker`를 각각 참조해 4개의
컨테이너(master, worker1, worker2, worker3)를 빌드하고 실행합니다.

### 3. 데몬 정상 기동 확인

```bash
docker exec -it master jps
# 기대: NameNode, ResourceManager, JobHistoryServer

docker exec -it worker1 jps   # worker2, worker3도 동일
# 기대: DataNode, NodeManager
```

## 설정 파일 설명 및 근거

| 파일 | 핵심 설정 | 근거 |
|---|---|---|
| `core-site.xml` | `fs.defaultFS=hdfs://master:9000` | Docker Compose 내장 DNS로 서비스 이름(`master`)이 곧 호스트명이 됨. `localhost`는 컨테이너 자기 자신을 가리켜 사용 불가 |
| `hdfs-site.xml` | `dfs.replication=3` | worker(datanode) 3개 구성에 맞춰 실질적인 분산 복제 검증이 가능하도록 설정 |
| `hdfs-site.xml` | `dfs.namenode.secondary.http-address=master:50090` | secondary namenode 주소도 서비스 이름 기준으로 통일 |
| `hdfs-site.xml` | `dfs.namenode.name.dir`, `dfs.datanode.data.dir` | named volume에 마운트하여 컨테이너 재시작 간 영속성 확보 |
| `mapred-site.xml` | `mapreduce.framework.name=yarn` | 이 설정이 없으면 로컬 모드로 실행되어 분산 처리가 이루어지지 않음 |
| `yarn-site.xml` | `yarn.resourcemanager.hostname=master` | ResourceManager 위치를 컨테이너 서비스 이름으로 지정 |
| `yarn-site.xml` | `yarn.resourcemanager.bind-host=0.0.0.0` | `hostname`(논리 주소)과 `bind-host`(물리 바인딩 주소)를 분리하지 않으면 웹 UI가 loopback에만 바인딩되어 호스트 포트 매핑이 작동하지 않음 |

> 위 값들은 변수화하지 않고 하드코딩했습니다. 클러스터 구성(서비스 이름, worker 개수)이
> 자주 바뀔 계획이 없어 유연성보다 단순성/가독성을 우선했습니다.

## 클러스터 등록 검증

```bash
docker exec -it master hdfs dfsadmin -report
```

`Live datanodes (3)`으로 worker1/2/3이 모두 등록되며, 각 노드의 hostname이
`worker1.<프로젝트명>_hadoop-net` 형태로 Docker Compose의 내부 DNS를 통해 정상 해석됨을
확인했습니다.

웹 UI로도 동일하게 확인 가능합니다.
- NameNode: `http://localhost:50070/dfshealth.html#tab-overview` → Live Nodes = 3
- ResourceManager: `http://localhost:8088/cluster` → Active Nodes = 3

## HDFS 동작 확인

```bash
docker exec -it master bash

# 1. 디렉토리 생성
hadoop fs -mkdir -p /user/test

# 2. 로컬 파일 생성 후 업로드
echo "hello multi-node hadoop" > sample.txt
hadoop fs -put sample.txt /user/test/

# 3. 확인
hadoop fs -ls /user/test/
hadoop fs -cat /user/test/sample.txt

# 4. 재조회
hadoop fs -get /user/test/sample.txt /tmp/downloaded_sample.txt
cat /tmp/downloaded_sample.txt
```

### 분산 저장 증거 (fsck)

```bash
hdfs fsck /user/test/sample.txt -files -blocks -locations
```

- `-files`: 어떤 파일들을 점검했는지 나열
- `-blocks`: 파일이 몇 개의 블록으로 쪼개졌는지 나열
- `-locations`: 각 블록이 어느 datanode에 저장되었는지 나열

결과에서 블록 하나가 `Live_repl=3`이며, 서로 다른 세 datanode(worker1/2/3)의 IP가 모두
나열되어 실제로 3곳에 복제되어 있음을 확인했습니다. `Status: HEALTHY`.

NameNode 웹 UI의 `Datanodes` 탭(`http://localhost:50070/dfshealth.html#tab-datanode`)에서도
각 datanode에 동일한 크기만큼 데이터가 복제되어 있음을 확인했습니다 (파일 1개, 3노드,
replication 3이므로 각 노드에 동일 블록이 하나씩).

## MapReduce 분산 처리 검증

Hadoop 배포판 내장 예제(`hadoop-mapreduce-examples-2.10.2.jar`)로 wordcount job을 실행했습니다.

```bash
echo "the quick brown fox jumps over the lazy dog
the dog barks at the fox
hadoop hadoop hadoop is fun" > wordcount_input.txt

hadoop fs -mkdir -p /user/test/wordcount_input
hadoop fs -put wordcount_input.txt /user/test/wordcount_input/

hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.10.2.jar \
  wordcount /user/test/wordcount_input /user/test/wordcount_output

hadoop fs -cat /user/test/wordcount_output/part-r-00000
```

Job은 `SUCCEEDED`로 종료되었습니다. Job History Server(`http://localhost:19888`)에서
Map/Reduce task를 각각 확인한 결과, **Map task는 worker2, Reduce task는 worker1**에서
실행되어 서로 다른 두 worker가 실제로 분산 처리에 참여했음을 확인했습니다.

- Map task가 특정 노드에 배정되는 것은 데이터 지역성(data locality) 원칙에 따라, 처리할
  입력 블록을 가진 datanode 중 하나가 우선 선택되기 때문입니다.
- Reduce task는 여러 map 출력을 셔플로 모으는 특성상 데이터 지역성 최적화 대상이 아니라서,
  그 시점에 리소스가 여유로운 노드(이번엔 worker1)에 배정되었습니다.

> **알려진 한계**: 이번 검증에 사용한 입력 파일이 작아(1개 블록) map/reduce task가 각각
> 1개씩만 생성되어 worker3는 이번 job에는 참여하지 않았습니다. 입력 크기를 HDFS 블록
> 크기(128MB) 이상으로 늘리면 map task가 여러 개로 분할되어 3개 worker 모두의 참여를
> 유도할 수 있으며, 이번 제출에서는 시간 관계상 이 검증은 생략했습니다.

## 영속성(Persistence) 검증

`docker-compose.yml`에서 namenode/datanode 데이터 디렉토리를 각각 독립된 named volume으로
마운트했습니다. entrypoint 스크립트는 `/hadoop/dfs/name/current` 디렉토리 존재 여부로
최초 기동인지 재시작인지 판단하여, 재시작 시 namenode를 재포맷하지 않고 기존 메타데이터를
그대로 사용합니다.

**검증 절차**

```bash
# 1. 재시작 전 상태 확인
docker exec -it master hadoop fs -cat /user/test/sample.txt
docker exec -it master hdfs dfsadmin -report | grep "Live datanodes"

# 2. 컨테이너 전체 재시작 (컨테이너만 삭제, named volume은 유지됨)
docker compose down
docker compose up -d

# 3. 재시작 완료 후 확인
docker exec -it master jps
docker exec -it worker1 jps
docker exec -it worker2 jps
docker exec -it worker3 jps

docker exec -it master hdfs dfsadmin -report | grep "Live datanodes"   # 여전히 3인지
docker exec -it master hadoop fs -cat /user/test/sample.txt            # 데이터 살아있는지
```

재시작 후에도 4개 컨테이너의 데몬이 모두 정상 기동되고, `Live datanodes`가 3으로 유지되며,
이전에 업로드했던 `sample.txt`가 그대로 조회됨을 확인했습니다.

**동작 원리**: `docker compose down`은 컨테이너 프로세스를 삭제할 뿐 named volume까지
삭제하지 않습니다. namenode 메타데이터와 datanode 블록 파일이 volume에 남아있으므로, 새로
생성된 컨테이너가 같은 volume을 다시 마운트하면서 이전 상태를 그대로 이어받습니다. 반대로
`docker compose down -v`로 volume까지 삭제하면 `/hadoop/dfs/name/current`가 사라져
entrypoint가 namenode를 재포맷하게 되고, 클러스터는 완전히 초기화됩니다 (대조 검증 시
volume이 실제로 영속성을 담당함을 확인 가능).

## 웹 UI

| 서비스 | URL | 확인 포인트 |
|---|---|---|
| NameNode | http://localhost:50070 | Live Nodes = 3 |
| ResourceManager | http://localhost:8088 | Active Nodes = 3, 완료된 job 목록 |
| Job History Server | http://localhost:19888 | job별 map/reduce task가 실행된 Node 확인 |

## 알려진 이슈 및 참고사항

- **`WARN util.NativeCodeLoader`**: ARM64 환경에서 네이티브 라이브러리 미지원으로 인한 정상
  경고이며 기능에 영향 없습니다.
- **SSH 키는 base 이미지 빌드 시점에 생성**되어 master/worker가 동일한 키를 공유합니다.
  실습 환경 한정으로 문제없으나, 실제로는 master에서만 사용됩니다(디버깅 편의 목적).
- **작은 입력의 MapReduce job은 클러스터 전체(3개 worker)를 사용하지 않을 수 있습니다.**
  map/reduce task 수는 입력 크기(HDFS 블록 분할 수)에 따라 결정되며, 이번 제출에서는
  worker1, worker2 두 곳만 사용되었습니다.
- Hadoop 2.x 기준이므로 NameNode 웹 UI 포트가 `50070`입니다 (3.x부터는 `9870`).
- `depends_on`은 컨테이너 시작 "순서"만 보장하며, master의 namenode가 완전히 준비된 것을
  기다려주지는 않습니다. worker의 datanode가 초기 몇 초간 접속 재시도 로그를 남길 수 있으나
  정상 동작입니다.