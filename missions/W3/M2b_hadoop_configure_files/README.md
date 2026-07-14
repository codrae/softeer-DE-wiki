# W3M2b - Understanding of Hadoop Configuration Files

## 개요

본 프로젝트는 Apache Hadoop 2.10.2 기반 멀티노드 클러스터(namenode 1개 + worker 3개, Docker Compose)의
`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml` 설정을 프로그래밍적으로
변경/검증하는 것을 목표로 한다.

- **변경 스크립트**: `modify_config.py`
- **검증 스크립트**: `verify_config.py`
- 이전 미션(W3M2a, Multi-node Hadoop Cluster on Docker)에서 구축한 인프라를 재사용한다.

---

## 사전 준비

- Docker / Docker Compose 설치
- Python 3
- `requests` 라이브러리
  ```bash
  pip3 install requests --break-system-packages
  # 또는 가상환경 사용 시: python3 -m venv venv && source venv/bin/activate && pip install requests
  ```

---

## 디렉토리 구조

```
M2b_hadoop_configure_files/
├── Dockerfile.base            # 공통 베이스 이미지 (ubuntu20.04 + openjdk8 + Hadoop 2.10.2)
├── Dockerfile.namenode        # namenode 역할 이미지
├── Dockerfile.worker           # worker(datanode+nodemanager) 역할 이미지
├── entrypoint-namenode.sh
├── entrypoint-worker.sh
├── docker-compose.yml
├── config/                    # 설정 파일 원본 (호스트 기준 source of truth, Git 관리 대상)
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   └── yarn-site.xml
├── backups/                   # 스크립트 실행 시 자동 생성되는 타임스탬프 백업
├── modify_config.py
├── verify_config.py
└── README.md
```

---

## 빌드 및 실행

```bash
# 1) base 이미지 빌드 (최초 1회, 또는 Dockerfile.base 변경 시)
docker build -f Dockerfile.base -t w3m2b-base:latest .

# 2) 클러스터 기동
docker-compose up -d --build

# 3) 설정 변경 스크립트 실행 (반드시 프로젝트 루트에서 실행)
python3 modify_config.py config/

# 4) 검증 스크립트 실행
python3 verify_config.py
```

---

## 주요 의사결정 및 근거 (A~F)

| # | 항목 | 결정 | 근거 |
|---|---|---|---|
| A | 변경 스크립트 언어 | Python + `ElementTree` | shell/sed는 XML 구조(name-value 대응)를 인식하지 못해 전체 치환 위험이 있고, "없으면 추가" 로직 구현이 어려움. ElementTree는 구조를 인식하고 예외를 파일없음/파싱에러/대상없음으로 세분화해 처리 가능 |
| B | 백업 전략 | `backups/`에 타임스탬프 파일명으로 계속 축적 | 별도의 "원본 전용" 폴더는 두지 않음 — 가장 이른 타임스탬프가 곧 원본이라는 원칙이 자연스럽게 성립하며, Git 커밋 이력과 같은 방식. Retention 정책은 이번 과제 범위에서는 적용하지 않음 (실무라면 Airflow cleanup task/logrotate 등 필요) |
| C | 배포/재시작 아키텍처 | 호스트 Python이 원본 수정 → `docker cp` 이중 루프 배포 → `docker exec`로 데몬 프로세스만 stop/start | 클러스터 전체가 같은 설정을 봐야 통신 가능하므로 4개 컨테이너 모두에 4개 파일 전부 배포(부분집합 금지). 컨테이너를 재생성(`docker restart`)하지 않고 프로세스만 재시작해 entrypoint의 최초 포맷 체크 로직 재실행을 방지. namenode를 경유해 다른 컨테이너에 명령을 전파하지 않고 호스트가 각 컨테이너에 직접 접근(SSH+slaves 패턴 회피) |
| D | 검증 스크립트 설계 | readiness check 선행 → getconf+daemon `/conf` 이중 확인 → replication 3단 분리 → job application ID+상태 확인 → REST API 메모리 조회 | `getconf`(파일 기준)와 데몬 실제 로드값(`/conf`)을 모두 확인해 "파일은 바뀌었는데 재시작이 덜 됐다"는 케이스까지 구분. `/conf`는 XML로 정확히 파싱해 key 단위로 비교(단순 substring 매칭은 다른 프로퍼티 값과 우연히 겹쳐 오탐 가능) |
| E | replication factor 검증 | 설정값 / 연결된 datanode 수 / 실제 테스트 파일 replication을 분리 리포트 | `dfs.replication`은 write 시점에 적용되는 기본값이라 새로 생성한 파일로 검증해야 의미가 있음. 재시작 직후 하트비트 재등록 지연을 감안한 재시도 포함 |
| F | hostname 컨벤션 | `master` → `namenode`로 통일, worker는 `worker1/2/3` 유지 | 원문 요구값(`hdfs://namenode:9000` 등)에 맞춤. worker는 DataNode+NodeManager 역할을 겸하고 있어 역할 확장에 유연한 범용 이름을 유지 |
| Q1 | `mapreduce.job.tracker` 처리 | `mapreduce.jobhistory.address`와 함께 둘 다 검증 | Hadoop 1.x(JobTracker) 시절 설정으로, 이름 자체도 `mapreduce.jobtracker.address`로 deprecated 되었고 YARN 모드에서는 무시됨을 확인(Hadoop 공식 Deprecated Properties 문서). 학습 목적으로 파일에 포함하고 검증 시 "설정되지만 무시됨"으로 별도 표기 |

### 재시작 순서 세부사항
`restart_daemons()`는 "전체 stop → 전체 start" 2단계로 구성된다. stop 순서는 무관하지만, start는 namenode 계열(namenode → resourcemanager → historyserver)이 먼저 뜬 뒤 worker(datanode, nodemanager)가 떠야, datanode/nodemanager가 등록할 대상이 준비된 상태에서 기동되어 불필요한 재시도 로그를 줄일 수 있다.

---

## 로컬 검증 환경에서 발견하여 추가한 설정 (원문 스펙 외)

원문 미션이 요구한 12개 프로퍼티만으로는 실제 클러스터가 정상 동작하지 않았다. 아래 6개는 실제로 클러스터를 기동하고 job을 돌려보는 과정에서 발견하여 `config/*.xml`과 `TARGET_VALUES`/`EXPECTED`에 함께 반영했다.

| 프로퍼티 | 파일 | 값 | 없으면 생기는 문제 |
|---|---|---|---|
| `yarn.resourcemanager.hostname` | yarn-site.xml | `namenode` | `yarn.resourcemanager.address`만 지정하고 hostname을 안 주면, `resource-tracker.address` 등 나머지 RM 하위 주소가 `yarn-default.xml` 기본값(`0.0.0.0`)에 머물러 NodeManager 등록이 영구히 실패함 |
| `yarn.nodemanager.aux-services` | yarn-site.xml | `mapreduce_shuffle` | MapReduce reduce 단계의 shuffle에 필요한 보조 서비스. 없으면 reduce 단계에서 job이 실패할 수 있음 |
| `dfs.datanode.data.dir` | hdfs-site.xml | `/hadoop/dfs/data` | 미지정 시 `${hadoop.tmp.dir}/dfs/data`로 상속되는데, 이 경로가 `docker-compose.yml`의 volume 마운트 경로와 달라 datanode 데이터가 volume이 아닌 컨테이너 휘발성 레이어에 저장되고, 재시작마다 재포맷됨 |
| `mapreduce.map.java.opts` / `mapreduce.reduce.java.opts` | mapred-site.xml | `-Xmx768m` | Hadoop 기본값이 빈 문자열이라 `-Xmx`가 지정되지 않고, Docker Desktop 환경(명시적 `--memory` 제한 없는 컨테이너)에서 JVM이 자체 계산한 기본 힙이 너무 작아 가벼운 job도 `Java heap space`로 실패함 |
| `yarn.nodemanager.resource.memory-mb` (값 조정) | yarn-site.xml | `8192` → `2048` | 원문 값(8192)은 각 데몬이 별도 물리 서버에 배치되는 실제 다중 노드 클러스터를 전제로 한 값. 로컬 Docker Desktop(VM 전체 물리 메모리 8GB)에서 4개 컨테이너가 커널을 공유하는 구조에서는 worker마다 8192MB를 배분 가능하다고 광고하면 실제 가용 자원을 초과(overcommit)해 `Java heap space` 에러 발생. 실제 물리 클러스터(각 노드 8GB 이상 전용 메모리) 배포 시에는 원문 값(8192)으로 복원 가능 (자세한 배경은 "알려진 한계" 참고) |

---

## 트러블슈팅 로그

### 1. Docker 빌드 경로 오류 (`COPY ... not found`)
- **증상**: `Dockerfile.namenode`/`Dockerfile.worker`의 `COPY core-site.xml ...`가 "not found" 에러
- **원인**: XML 파일이 `config/` 하위에 있는데 COPY 경로가 최상위 기준으로 작성됨
- **해결**: `COPY config/core-site.xml ...`로 경로 수정

### 2. Docker 빌드 캐시 스냅샷 오류
- **증상**: `failed to compute cache key`, `snapshot ... does not exist`
- **원인**: BuildKit 캐시 손상 (여러 서비스 병렬 빌드 중 발생)
- **해결**: `docker-compose build --no-cache`

### 3. 컨테이너 이름 충돌
- **증상**: `Conflict. The container name "/worker1" is already in use`
- **원인**: 이전 프로젝트(W3M2a)의 잔재 컨테이너가 동일한 이름(`worker1/2/3`, `master`)을 사용 중
- **해결**: `docker rm`으로 잔재 컨테이너 제거 (이미지는 무해하므로 유지 가능)

### 4. `chown: missing operand` 경고
- **증상**: namenode 로그에 `chown: missing operand after '/opt/hadoop/logs'`
- **원인**: 컨테이너에 `$USER` 환경변수가 없어 `HADOOP_IDENT_STRING`이 빈 문자열이 되고, `hadoop-daemon.sh` 내부 `chown` 호출의 인자가 누락됨
- **해결**: `Dockerfile.base`에 `ENV USER=root` 추가

### 5. SSH 잔재 코드
- **증상**: 로그에 `Starting OpenBSD Secure Shell server sshd`
- **원인**: SSH+slaves 기반 원격 제어 방식(W3M2a 이전 아키텍처)의 흔적이 entrypoint에 남아있었음
- **해결**: `service ssh start` 라인 제거 (현재는 `docker exec` 기반으로 호스트가 각 컨테이너를 직접 제어)

### 6. NodeManager가 ResourceManager에 등록되지 않음 (`Total Nodes:0`)
- **증상**: `hdfs dfsadmin -report`는 정상인데 `yarn node -list`가 0개. MR job 제출 시 `ACCEPTED` 상태에서 멈춤 (`Queue Resource Limit for AM = <memory:0, vCores:0>`)
- **원인**: NodeManager 로그에서 `Connecting to ResourceManager at /0.0.0.0:8031` 확인 → `yarn.resourcemanager.hostname` 미지정으로 resource-tracker 주소가 기본값(`0.0.0.0`)에 머묾
- **해결**: `yarn.resourcemanager.hostname=namenode` 추가 (위 "로컬 검증 환경에서 발견하여 추가한 설정" 참고)

### 7. DataNode가 재시작마다 재포맷됨
- **증상**: worker 로그에 `Storage directory /hadoop/tmp/dfs/data is not formatted ... Formatting...`가 매번 반복
- **원인**: `dfs.datanode.data.dir` 미지정으로 실제 저장 경로(`/hadoop/tmp/dfs/data`)와 volume 마운트 경로(`/hadoop/dfs/data`)가 불일치 — 데이터가 volume이 아닌 컨테이너 휘발성 레이어에 쓰이고 있었음
- **해결**: `dfs.datanode.data.dir=/hadoop/dfs/data` 명시, `docker-compose down -v`로 완전 초기화 후 재기동

### 8. `Java heap space` (1차) — NodeManager 메모리 overcommit
- **증상**: `pi` 예제 map task가 전부 `Error: Java heap space`로 실패
- **원인**: `docker stats` 확인 결과 Docker Desktop VM 전체 물리 메모리가 8GB인데, worker마다 `yarn.nodemanager.resource.memory-mb=8192`로 설정되어 있어 3개 worker 총합 24GB를 배분 가능하다고 광고 — 실제 가용 자원을 크게 초과
- **해결**: `yarn.nodemanager.resource.memory-mb`를 `2048`로 조정

### 9. Safe mode 대기 (정상 동작, 함정 주의)
- **증상**: `SafeModeException: Name node is in safe mode`
- **원인**: 재시작 직후 NameNode가 datanode 블록 리포트를 기다리는 정상적인 보호 모드
- **교훈**: readiness check에 `hdfs dfsadmin -safemode get` 폴링을 추가해, 노드 개수뿐 아니라 safe mode 해제 여부까지 확인하도록 `verify_config.py`를 보강함

### 10. `Java heap space` (2차) — JVM 힙 미지정
- **증상**: NodeManager 메모리를 낮춘 뒤에도 동일한 에러 재현
- **원인**: `mapreduce.map.java.opts`/`mapreduce.reduce.java.opts`의 Hadoop 기본값이 빈 문자열이라 `-Xmx`가 전혀 지정되지 않고, JVM이 자체 계산한 기본 힙이 너무 작게 잡힘
- **해결**: `mapreduce.map.java.opts=-Xmx768m`, `mapreduce.reduce.java.opts=-Xmx768m` 명시

### 11. `TARGET_VALUES`/`EXPECTED` 동기화 누락으로 인한 회귀
- **증상**: `modify_config.py` 실행 시 로컬 환경에 맞게 낮춘 `yarn.nodemanager.resource.memory-mb=2048`이 원문 스펙값(8192)으로 되돌아감
- **원인**: 트러블슈팅 중 `config/*.xml`은 직접 수정했지만, 스크립트의 `TARGET_VALUES`/`EXPECTED` 딕셔너리는 갱신하지 않음
- **해결**: 두 스크립트의 상수를 실제 검증된 최종값으로 동기화

### 12. 검증 스크립트의 거짓 PASS (substring 매칭 결함)
- **증상**: 실제 값은 `2048`인데 `(daemon /conf) yarn.nodemanager.resource.memory-mb=8192`가 PASS로 표시됨
- **원인**: `/conf` 페이지 전체 텍스트에 대해 `expected_value in text` 방식의 substring 검사를 했는데, 페이지 내 다른 무관한 프로퍼티의 값이 우연히 같은 문자열(`8192`)을 포함해 오탐 발생
- **해결**: `/conf` 응답을 XML로 파싱해 `{프로퍼티명: 값}` 딕셔너리로 변환한 뒤, 정확히 이름이 일치하는 값끼리 비교하도록 수정

---

## 알려진 한계

- 백업 파일(`backups/`)에는 retention(보존 기간) 정책이 적용되어 있지 않다. 실무 환경이라면 Airflow cleanup task나 `logrotate`/cron 기반 TTL 정책이 필요하다.
- 설정 배포는 `docker cp` 이중 루프(컨테이너 수 × 파일 수) 방식이다. 이는 단일 Docker Desktop 호스트 환경에서는 공유 volume 마운트로 단순화할 수 있었으나, 실제 물리적으로 분리된 다중 서버 환경에서는 통하지 않는 방식이라 의도적으로 채택하지 않았다. 실무에서는 Ansible/Puppet 같은 설정관리 도구나 Kubernetes ConfigMap을 사용하는 것이 일반적이다.
- readiness check는 datanode/nodemanager 등록 및 safe mode 해제까지만 확인한다. 그 이전 단계(컨테이너 자체의 기동 실패 등)에 대한 장애 복구 로직은 본 스크립트 범위 밖이다.
- `yarn.nodemanager.resource.memory-mb`는 원문 스펙(8192)이 아닌 로컬 환경 제약에 따른 조정값(2048)이 최종 반영되어 있다. 실제 물리 클러스터 배포 시에는 원문 값으로 복원해야 한다.

---

## 팀 활동

4개 XML 파일의 프로퍼티를 각자 분담하여 조사한 뒤, 위키에 정리한다. (분담 및 정리 내용은 팀 위키 별도 문서 참고)