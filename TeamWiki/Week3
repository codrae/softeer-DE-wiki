# W3 팀미션 정리

## W3-M2b: Hadoop 설정 튜닝

### 파일별 설정 목록

#### core-site.xml
| 설정 | 값 |
|---|---|
| fs.defaultFS | hdfs://namenode:9000 |
| hadoop.tmp.dir | /hadoop/tmp |
| io.file.buffer.size | 131072 |

#### hdfs-site.xml
| 설정 | 값 |
|---|---|
| dfs.replication | 2 |
| dfs.blocksize | 134217728 |
| dfs.datanode.data.dir | /hadoop/dfs/data |
| dfs.namenode.name.dir | /hadoop/dfs/name |

#### mapred-site.xml
| 설정 | 값 |
|---|---|
| mapreduce.framework.name | yarn |
| mapreduce.map.java.opts | -Xmx768m |
| mapreduce.reduce.java.opts | -Xmx768m |
| mapreduce.jobhistory.address | namenode:10020 |
| mapreduce.task.io.sort.mb | 256 |
| mapreduce.job.tracker | namenode:9001 *(주석: "Hadoop 1.x 시절 설정, YARN에서는 무시됨")* |

#### yarn-site.xml
| 설정 | 값 |
|---|---|
| yarn.resourcemanager.address | namenode:8032 |
| yarn.nodemanager.resource.memory-mb | 2048 |
| yarn.scheduler.minimum-allocation-mb | 1024 |
| yarn.resourcemanager.hostname | namenode |
| yarn.nodemanager.aux-services | mapreduce_shuffle |

### 주요/유용 설정 논의

**`yarn.nodemanager.resource.memory-mb`**
- 초기값 8GB로 설정 시, DataNode 3개(24GB) 할당을 시도했으나 노트북 전체 RAM 중 Docker Desktop에 8GB만 할당되어 있어 스펙 부족으로 에러 발생.
- 실습을 위해 2GB로 튜닝. 실제 서버 스펙에 맞춰 할당해야 하는 중요한 값.

**`dfs.replication`**
- HDFS 데이터 블록의 복제본 개수를 결정하는 설정. 기본값은 3이지만 과제 요구사항에 따라 2로 변경.
- 실습 환경에 DataNode 2개 구성 → 각 블록의 복제본을 서로 다른 두 DataNode에 저장 가능.
- 하나의 DataNode 장애 시에도 다른 DataNode의 복제본으로 데이터 접근 가능 → 가용성·안정성 향상.

---

## W3-M4: Predefined Keywords 외의 데이터 분류 방법

### 왜 이미 라벨링된 데이터를 키워드로 분류했는가?
- 목적은 분류 자체가 아니라, **내가 만든 분류기가 쓸만한지 검증**하기 위함.
- 라벨링된 데이터는 분류기를 만들고 검증하는 연습장 역할.

### Twitter Sentiment 분류 방법 (단일 노드 기준)
- Rule-Based (TextBlob)
- TF-IDF + KMeans
- 머신러닝 분류기 (SVM, Naive Bayes)

→ 데이터 양이 적을 때는 단일 노드에서 문제 없이 수행 가능.

### Hadoop Multi Cluster 환경에서의 분류

**핵심 문제**: "전역 정보가 필요한 계산"과 "Mapper의 격리된 실행 모델"이 정면으로 충돌.

| 방법 | 적합 여부 |
|---|---|
| Rule-Based | 적합 |
| TF-IDF + KMeans | Job-Chaining 필요 |
| Naive Bayes | Job-Chaining 필요 |

#### Job-Chaining이 필요한 이유
TF-IDF처럼 "단어 통계를 먼저 계산해야 그걸로 분류할 수 있는" 방법은:
1. **Job 1**: 전체 트윗에서 단어 통계 계산 (mapper가 흩어져서 세고, reducer가 합쳐 "전체 통계" 완성)
2. **Job 2**: 완성된 통계 파일을 참고해 각 트윗을 분류

→ 한 번의 job으로 안 되는 이유: Job 1의 reducer가 끝나야 "전체 통계"가 완성되는데, 그 전에는 Job 2의 mapper가 참고할 게 없기 때문. 따라서 Job을 순차적으로 두 개 실행해야 함.

### Naive Bayes + Job-Chaining 실험

```
[잡 1: 학습]                          [잡 2: 분류]
트윗 160만 건 ──→ 단어 점수표 만들기 ──→ 점수표 들고 트윗 분류 ──→ 최종 결과
   (HDFS)        (결과도 HDFS에)      (점수표를 -files로 배포)   positive/negative/neutral 개수
```

🎉 **잡 체이닝 완주**: 합계 검증 742,717 + 218,625 + 638,658 = 정확히 1,600,000건 (누락 0건)

#### 결과 비교

| 구분 | negative | neutral | positive |
|---|---|---|---|
| 정답 라벨 | 800,000 (50%) | 0 (0%) | 800,000 (50%) |
| 키워드 방식 | 232,514 (14.5%) | 990,403 (61.9%) | 377,083 (23.6%) |
| NB 체이닝 (전처리 개선 전) | 743,623 (46.5%) | 218,809 (13.7%) | 637,568 (39.8%) |
| NB 체이닝 (최종) | 742,717 (46.4%) | 218,625 (13.7%) | 638,658 (39.9%) |

#### 분석

1. **neutral 비율 급감 (62% → 14%)**: 키워드 방식은 110개 단어에 걸리지 않으면 전부 neutral로 처리했지만, NB는 데이터에서 학습한 수만 개 단어의 점수를 사용하므로 대부분의 트윗에 판단을 내릴 수 있음. 키워드 방식의 "커버리지 부족" 한계가 데이터 기반 방법으로 해소됨을 수치로 확인.
2. **정답 분포(반반)에 근접**: 키워드 방식(23.6% vs 14.5%)보다 NB 방식(46.4% vs 39.9%)이 정답 비율에 훨씬 가까워짐.
3. **재미있는 반전 (negative 우세)**: 키워드 방식에서는 positive가 1.6배 많았으나(긍정 단어가 정형화되어 잡기 쉬움), NB에서는 뒤집힘. 학습 데이터가 다양한 부정 표현 어휘(headache, fml, gutted 등)까지 점수화하면서, 키워드로 못 잡던 부정 트윗들이 잡히게 됨.
4. **전처리 개선의 영향은 미미**: 토크나이저 개선(어퍼스트로피 잔해 제거, 늘려쓰기 통합)으로 어휘는 깨끗해졌지만, 최종 분류 변화는 약 1,100건(전체의 0.07%)에 불과. 노이즈 토큰 대부분이 (1) MIN_COUNT=5 필터에 이미 걸러졌거나 (2) 양쪽 라벨에 비슷하게 나와 점수가 0 근처였기 때문. → "전처리는 모델의 해석 가능성은 크게 개선하지만, 대량 데이터에서는 집계 결과 자체가 노이즈에 상당히 강건하다"는 발견.

### 남은 한계점

- 정답은 neutral 0인데 여전히 14%가 남음 — 점수 합이 ±0.5 안에 드는 애매한 트윗들. THRESHOLD 값의 트레이드오프(낮추면 neutral 감소, 대신 억지 판정 증가)로, 규칙 기반 → 통계 기반 전환 과정에서 생긴 새로운 하이퍼파라미터.
- Job 제출 자체의 오버헤드가 2배.
- Job 사이의 중간 결과가 HDFS에 쓰이고 복제됨.
- Job 2개는 완전 순차 실행 — 병렬 작업 불가능.

### 결론: 상황에 따른 방식 선택 (Job-Chaining / Distributed Cache / Spark)

| 방식 | 원리 | 장점 | 단점 | 적합한 상황 |
|---|---|---|---|---|
| **Job-Chaining**<br>(Job1: 학습 → Job2: 분류) | Job1이 클러스터 안에서 점수표를 계산해 HDFS에 write(3-replication)하고, Job2는 Job1의 reducer가 100% 끝날 때까지 기다렸다가 그 결과를 -files로 각 노드 로컬 디스크에 복사해 분류 수행. "계산 → 저장 → 재배포 → 소비"가 별개의 Job 2회로 나뉨 | • 항상 최신 데이터로 점수표 재계산 → 데이터 드리프트 반영<br>• 실패 시 Job1 성공했다면 Job2만 재실행 가능 (부분 체크포인트 효과) | • Job 제출 2회 → JVM fork·YARN 협상 비용 2배<br>• Job2는 Job1 완료 전까지 idle 대기 → 순차성으로 인한 지연<br>• 점수표가 커지면 Job1 실행시간 증가<br>• 반복 학습(iteration)마다 Job을 또 쪼개야 해서 사실상 불가 | 점수표를 그 데이터셋 자체에서 매번 새로 학습해야 하는 경우 (예: 매일 새 로그로 감성사전 재학습) |
| **Distributed Cache 단독**<br>(사전 계산 후 배포, Job 1개) | 점수표를 클러스터 밖(로컬/별도 스크립트)에서 미리 계산해두고, 기존 keyword mapper 구조에서 파일만 -files로 교체해 단일 Job으로 분류만 수행 | • 구현/운영 복잡도가 가장 낮음 (파일만 바꿔치기)<br>• Job이 1개라 순차 대기·재실행 문제 없음<br>• 점수표가 커져도 이미 계산 끝났으므로 배포/로딩 비용만 고려하면 됨 | • 오래된 점수표를 그대로 쓸 위험 → 데이터 드리프트 미반영<br>• 점수표 자체를 갱신하려면 외부 파이프라인을 따로 운영해야 함<br>• -files의 기본 10GB 캐시 제한, LRU 삭제 위험 동일하게 존재 | 점수표가 거의 안 바뀌거나 이미 학습된 모델을 재사용할 때 (예: 범용 사전학습 감성사전) |
| **Spark**<br>(broadcast + in-memory, Application 1개) | 클러스터 안에서 점수표를 계산해 바로 broadcast()로 전파(torrent 방식, executor 메모리 상주). Job-Chaining과 달리 하나의 Application 안에서 DAG scheduler가 stage들을 파이프라이닝하므로 "계산"과 "소비"가 같은 lineage 안에서 이어짐 | • 최신 데이터로 재계산 가능하면서도 불필요한 대기 최소화 (DAG 파이프라이닝)<br>• 반복 계산 시 cache()로 디스크 재로딩 없이 재사용 — 반복 학습에 강점<br>• 점수표가 커져도 broadcast가 상대적으로 효율적<br>• 장애 시 lineage 기반 재계산으로 정교한 fault tolerance | • Spark 클러스터 설정, MLlib API 학습이 필요해 구현/운영 복잡도가 가장 높음<br>• broadcast 크기가 executor 메모리 한도를 넘으면 문제 발생 | 반복 계산이 잦거나, 여러 분석(집계+분류 등)이 같은 데이터를 재사용하는 경우 |

#### 확인 포인트

- **Job-Chaining**: "이 데이터로 직접 학습해야 한다"는 요구사항엔 맞지만, Job 2개 관리 비용 + 순차 대기 + HDFS 중간 저장 비용을 떠안음.
- **Distributed Cache 단독**: 구현은 제일 쉽지만, 점수표가 낡아도 못 알아챈다는 게 치명적 — "이미 분류기가 있다"는 전제가 깨지면 못 씀.
- **Spark**: 초기 러닝커브·인프라 구축 비용은 있지만, 반복/재사용이 잦은 워크로드에서 구조적으로 제일 유리 — "job 2개 쪼개기" 문제 자체가 애초에 발생하지 않음.