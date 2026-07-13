# Single-Node Hadoop Cluster on Docker

Docker를 이용해 단일 노드(single-node) Hadoop 2.10.2 클러스터를 구성한 프로젝트입니다.
Ubuntu 20.04 베이스 이미지 위에 Java, Hadoop을 직접 설치하고, SSH 자동화 및 설정 파일을
구성하여 컨테이너 실행 시 HDFS/YARN 데몬이 자동으로 기동되도록 구현했습니다.

## 목차
- [환경](#환경)
- [디렉토리 구조](#디렉토리-구조)
- [빌드 및 실행 방법](#빌드-및-실행-방법)
- [설정 파일 설명](#설정-파일-설명)
- [HDFS 동작 확인](#hdfs-동작-확인)
- [영속성(Persistence) 검증](#영속성persistence-검증)
- [웹 UI](#웹-ui)
- [알려진 이슈 및 참고사항](#알려진-이슈-및-참고사항)

---

## 환경

| 항목 | 값 |
|---|---|
| Base Image | ubuntu:20.04 |
| Java | OpenJDK 8 (apt 설치) |
| Hadoop | 2.10.2 (Apache 아카이브에서 다운로드) |
| 실행 모드 | Pseudodistributed (단일 노드에서 모든 데몬 실행) |
| 오케스트레이션 | docker-compose |

> **참고**: 이 프로젝트는 Apple Silicon(M1, ARM64) 환경 기준으로 `JAVA_HOME` 등이 설정되어
> 있습니다. Intel/AMD(x86_64) 환경에서 빌드할 경우, Dockerfile과 `hadoop-env.sh`에 설정된
> `JAVA_HOME` 경로를 `/usr/lib/jvm/java-8-openjdk-arm64` → `/usr/lib/jvm/java-8-openjdk-amd64`
> 로 변경해야 합니다. (Docker는 호스트 CPU 아키텍처에 맞는 베이스 이미지를 자동으로 선택하지만,
> apt로 설치되는 Java의 실제 경로 문자열은 아키텍처별로 다르기 때문입니다.)

## 디렉토리 구조

```
.
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
├── core-site.xml
├── hdfs-site.xml
├── mapred-site.xml
├── yarn-site.xml
└── README.md
```

## 빌드 및 실행 방법

### 1. 이미지 빌드 + 컨테이너 실행

```bash
docker compose up --build
```

- Dockerfile을 기반으로 이미지를 빌드하고, `entrypoint.sh`가 자동 실행되어
  SSH 데몬 시작 → (최초 1회) namenode 포맷 → HDFS 데몬 시작 → YARN 데몬 시작 →
  Job History Server 시작까지 자동으로 진행됩니다.
- 백그라운드로 실행하려면 `docker compose up -d --build`

### 2. 데몬 정상 기동 확인

```bash
docker exec -it hadoop-single-node jps
```

다음 6개 프로세스가 보여야 정상입니다.

```
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
Jps
```

### 3. 컨테이너 중지 / 삭제

```bash
docker compose down        # 컨테이너만 삭제 (볼륨은 유지 → 데이터 보존)
docker compose down -v     # 볼륨까지 삭제 (데이터 완전 초기화)
```

## 설정 파일 설명

| 파일 | 위치 (컨테이너 내부) | 핵심 설정 |
|---|---|---|
| `core-site.xml` | `/opt/hadoop/etc/hadoop/core-site.xml` | `fs.defaultFS=hdfs://localhost:9000` |
| `hdfs-site.xml` | `/opt/hadoop/etc/hadoop/hdfs-site.xml` | `dfs.replication=1`, `dfs.namenode.name.dir=/hadoop/dfs/name`, `dfs.datanode.data.dir=/hadoop/dfs/data`, `dfs.namenode.secondary.http-address=localhost:50090` |
| `mapred-site.xml` | `/opt/hadoop/etc/hadoop/mapred-site.xml` | `mapreduce.framework.name=yarn` |
| `yarn-site.xml` | `/opt/hadoop/etc/hadoop/yarn-site.xml` | `yarn.resourcemanager.hostname=localhost`, `yarn.resourcemanager.bind-host=0.0.0.0`, `yarn.nodemanager.aux-services=mapreduce_shuffle` |

`yarn.resourcemanager.hostname`과 `yarn.resourcemanager.bind-host`를 분리한 이유:
`hostname`은 클라이언트/노드매니저가 리소스 매니저를 찾을 때 쓰는 논리 주소이고,
`bind-host`는 실제 소켓이 귀 기울이는 물리적 주소입니다. `hostname`만 `localhost`로
두면 웹 UI가 loopback에만 바인딩되어 Docker의 호스트 포트 매핑(`-p 8088:8088`)이
작동하지 않습니다. `bind-host=0.0.0.0`으로 모든 인터페이스에서 받도록 설정해 이를 해결했습니다.

## HDFS 동작 확인

컨테이너 내부에서 아래 순서로 디렉토리 생성 → 파일 업로드 → 조회 → 다운로드를 확인할 수 있습니다.

```bash
docker exec -it hadoop-single-node bash

# 1. 디렉토리 생성
hadoop fs -mkdir -p /user/test

# 2. 로컬 파일 생성 후 HDFS 업로드
echo "hello hadoop" > sample.txt
hadoop fs -put sample.txt /user/test/

# 3. HDFS 상에서 파일 확인
hadoop fs -ls /user/test/
hadoop fs -cat /user/test/sample.txt

# 4. HDFS → 로컬로 재조회
hadoop fs -get /user/test/sample.txt /tmp/downloaded_sample.txt
cat /tmp/downloaded_sample.txt
```

## 영속성(Persistence) 검증

`docker-compose.yml`에서 namenode/datanode 데이터 디렉토리를 각각 named volume으로
분리하여 마운트했습니다.

```yaml
volumes:
  - hadoop-namenode:/hadoop/dfs/name
  - hadoop-datanode:/hadoop/dfs/data
```

`entrypoint.sh`는 `/hadoop/dfs/name/current` 디렉토리 존재 여부로 최초 실행인지
판단하여, 재시작 시 namenode를 다시 포맷하지 않고 기존 메타데이터를 그대로
이어받도록 구현했습니다.

**검증 방법**:
```bash
# 컨테이너 삭제 (볼륨은 유지)
docker compose down
docker compose up -d

# 이전에 올린 파일이 여전히 조회되는지 확인
docker exec -it hadoop-single-node hadoop fs -cat /user/test/sample.txt
# → "hello hadoop" 출력 확인됨 (컨테이너를 지웠다 새로 만들어도 데이터 유지)
```

반대로 `docker compose down -v`(볼륨까지 삭제) 후 재실행하면 해당 파일이
더 이상 조회되지 않는 것을 확인하여, named volume이 실제로 영속성을
담당하고 있음을 대조 검증했습니다.

## 웹 UI

| 서비스 | URL | 확인 포인트 |
|---|---|---|
| NameNode | http://localhost:50070 | Overview에서 Safe mode is off, Summary에서 Live Nodes = 1 |
| ResourceManager | http://localhost:8088 | Cluster Metrics에서 Active Nodes = 1 |
| Job History Server | http://localhost:19888 | 페이지 정상 로딩 (완료된 MapReduce job이 있으면 목록에 표시) |

## 알려진 이슈 및 참고사항

- **`WARN util.NativeCodeLoader: Unable to load native-hadoop library...`**
  Hadoop 배포판에 포함된 네이티브 라이브러리(`.so`)가 x86_64(amd64) 기준으로
  컴파일되어 있어, ARM64(Apple Silicon) 환경에서는 로드되지 않고 순수 Java
  구현으로 대체되는 정상적인 경고입니다. 기능 동작에는 영향이 없습니다.
- **SSH 키는 이미지 빌드 시점에 생성**됩니다. 실습/로컬 환경 한정으로는 문제가
  없으나, 이 이미지를 외부에 공개 배포할 경우 모든 사용자가 동일한 개인키를
  갖게 되므로 프로덕션 환경에서는 컨테이너 실행 시점(entrypoint)에 키를
  생성하는 방식으로 변경해야 합니다.
- Hadoop 2.x 기준이므로 NameNode 웹 UI 포트가 `50070`입니다 (Hadoop 3.x부터는 `9870`).