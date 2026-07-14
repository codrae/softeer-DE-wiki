# W3M5 - Average Rating of Movies using MapReduce

MovieLens 20M 데이터셋(`ratings.csv`)을 이용해, Hadoop Streaming(Python) 기반 MapReduce Job으로 영화별 평균 평점을 계산하는 미션입니다.

---

## 1. 미션 개요

- **목표**: `ratings.csv` (userId, movieId, rating, timestamp)를 입력받아, movieId별 평균 rating을 계산
- **구현 방식**: Hadoop Streaming + Python (Mapper/Reducer)
- **환경**: Docker 기반 Hadoop 클러스터 (namenode, datanode, resourcemanager, nodemanager)

---

## 2. 파일 구성

| 파일 | 설명 |
|---|---|
| `mapper.py` | 각 라인을 파싱해 `(movieId, rating)` 쌍을 emit |
| `reducer.py` | 같은 movieId로 모인 rating들의 평균을 계산해 출력 |
| `README.md` | 본 문서 |

---

## 3. 실행 방법

### 3-1. 환경 준비

컨테이너들이 정상 기동 중인지 확인합니다.

```bash
docker ps
docker exec namenode hdfs dfsadmin -safemode get
```

`Safe mode is ON`이면 아래 명령으로 해제될 때까지 대기합니다.

```bash
docker exec namenode hdfs dfsadmin -safemode wait
```

> 💡 아래 모든 `hadoop`/`hdfs` 명령은 **컨테이너 내부**에서 실행합니다. 먼저 컨테이너 셸로 진입하세요.
> ```bash
> docker exec -it namenode /bin/bash
> ```

### 3-2. 데이터 업로드

맥(호스트)에서 컨테이너로 데이터 파일을 복사한 뒤, HDFS에 업로드합니다.

```bash
# 호스트 터미널에서
docker cp ratings.csv namenode:/ratings.csv
```

```bash
# 컨테이너 내부에서
hadoop fs -mkdir -p /input
hadoop fs -put /ratings.csv /input/
hadoop fs -ls /input/
```

### 3-3. Mapper / Reducer 스크립트 준비

```bash
# 호스트 터미널에서
docker cp mapper.py namenode:/mapper.py
docker cp reducer.py namenode:/reducer.py
```

```bash
# 컨테이너 내부에서
chmod +x /mapper.py /reducer.py
```

### 3-4. (선택) 로컬 파이프라인 사전 테스트

전체 Job을 돌리기 전, 일부 데이터로 먼저 로직을 검증합니다.

```bash
head -n 100000 /ratings.csv | python3 /mapper.py | sort | python3 /reducer.py > /tmp/test_output.txt
head -20 /tmp/test_output.txt
```

### 3-5. Hadoop Streaming Job 제출

```bash
# 기존 output 경로 정리 (재실행 시)
hadoop fs -test -e /output/avg_ratings && hadoop fs -rm -r /output/avg_ratings

# Job 제출
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.2.jar \
  -files /mapper.py,/reducer.py \
  -input /input/ratings.csv \
  -output /output/avg_ratings \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py"
```

정상 종료 시 아래와 같은 로그로 마무리됩니다.

```
map 100% reduce 100%
Job job_XXXXXXXXXXXXX_XXXX completed successfully
Output directory: /output/avg_ratings
```

### 3-6. 결과 확인

```bash
hadoop fs -ls /output/avg_ratings
hadoop fs -cat /output/avg_ratings/part-* | head -20
```

출력 예시 (`movieId  평균평점`):
```
1       3.92
10      3.43
100     3.22
1000    3.11
...
```

결과를 로컬(맥)로 가져오려면:
```bash
# 컨테이너 내부에서
hadoop fs -get /output/avg_ratings /tmp/avg_ratings_result
```
```bash
# 호스트 터미널에서
docker cp namenode:/tmp/avg_ratings_result ./avg_ratings_result
```

---

## 4. 연산 흐름 (Map → Shuffle → Reduce)

이번 Job이 내부적으로 어떻게 처리됐는지, 실제 실행 로그의 수치를 근거로 정리합니다.

```
[HDFS: ratings.csv 533MB]
        │
        ▼
   4개로 분할(split)
        │
   ┌────┼────┬────┐
   ▼    ▼    ▼    ▼
 Map1  Map2  Map3  Map4     ← 4개 map task가 동시에 처리
   │    │    │    │
   └────┴─┬──┴────┘
          ▼
     [ Shuffle & Sort ]     ← movieId 기준으로 재정렬 + 한 곳으로 모으기
          │
          ▼
       Reduce1              ← reducer 1개 (기본값)
          │
          ▼
  [ HDFS: /output/avg_ratings ]
```

### 4-1. Map 단계

- `number of splits:4` → 533MB 파일이 4개의 input split으로 나뉘어 각각 map task(`Launched map tasks=4`)로 실행됨.
- 각 map task는 `mapper.py`로 자신이 맡은 부분의 줄을 읽어 `(movieId, rating)` 쌍을 emit.
- `Map input records=20,000,264` → `Map output records=20,000,263`: 헤더 줄 1개가 스킵되어 1건 감소.

**왜 4개로 나뉘었나 (Split 개수의 원리)**
- Map task 개수는 노드/컨테이너 개수와 무관하며, **파일 크기 ÷ HDFS 블록 크기**로 결정됨.
- HDFS 기본 블록 크기는 128MB이며, `533,444,411 bytes ÷ 134,217,728 bytes ≈ 3.97` → 올림하여 **4개** split이 생성됨.
- 즉 "파일이 몇 개의 HDFS 블록으로 저장되어 있는가"가 곧 map task 개수가 됨.
- 실제 로그의 `Data-local map tasks=4`는 4개 map task 모두 자신이 처리할 데이터가 있는 노드에서 실행되었음을 의미 (네트워크 전송 없이 로컬에서 처리 — Hadoop의 데이터 지역성 원칙이 잘 적용된 사례).
- 참고로 **reduce task 개수는 입력 크기와 무관하게 사용자가 직접 지정**하는 값이며, 이번 실행은 별도 설정 없이 기본값(1개)로 진행됨.

### 4-2. Shuffle 단계 (Map과 Reduce 사이, 자동 처리)

Hadoop이 내부적으로 처리하는 3단계 작업:

1. **Map 쪽 - Partition & 로컬 정렬**: 각 map task 출력은 reducer 개수만큼 partition으로 나뉘고(지금은 reducer 1개라 전부 같은 partition), 각 partition 내에서 movieId 기준으로 미리 정렬됨.
2. **네트워크 전송**: 정렬된 map 결과가 reduce task로 전송됨. 실제 로그의 `Reduce shuffle bytes=216,184,397`(약 206MB)이 이 전송량이며, `Shuffled Maps=4`, `Merged Map outputs=4`는 4개 map 결과가 모두 reduce task로 모였음을 의미.
3. **Reduce 쪽 - Merge**: 이미 각각 정렬된 4개의 map 결과를 병합 정렬(merge sort)하여, movieId 기준으로 그룹핑되고 정렬된 상태로 reducer에 전달.

이 덕분에 `reducer.py`는 "같은 movieId가 항상 연속으로 들어온다"는 전제 하에, movieId가 바뀌는 시점을 감지해 평균을 계산하는 단순한 로직만으로 구현 가능했음.

### 4-3. Reduce 단계

- `Reduce input records=20,000,263` → map output과 동일 → shuffle 과정에서 데이터 유실 없음.
- `Reduce input groups=26,744` → 고유 movieId 개수. reduce 함수가 26,744번 "movieId 전환"을 감지해 평균을 계산.
- `Reduce output records=26,744` → 고유 movieId 수와 정확히 일치 → 누락 없이 전체 처리 완료.
- 결과는 `/output/avg_ratings`에 HDFS 파일로 저장 (`Bytes Written=291,211`).

### 4-4. 요약

| 단계 | mapper.py / reducer.py가 하는 일 | Hadoop이 자동으로 하는 일 |
|---|---|---|
| Map | `(movieId, rating)` emit | 파일을 4개로 split(133MB 블록 기준), 각 split마다 map task 실행 |
| Shuffle | (관여 안 함) | movieId 기준 partition, 정렬(sort), 네트워크 전송, merge |
| Reduce | movieId별 평균 계산 | 정렬된 상태로 reducer에 데이터 공급, 결과를 HDFS에 저장 |

---

## 5. 결과 검증

MapReduce 결과와 원본 데이터를 직접 집계한 값을 비교해 로직의 정확성을 검증했습니다.

```bash
# 맥(호스트)에서, ratings.csv가 있는 디렉토리 안에서 실행
awk -F',' '$2 == "1" {sum+=$3; count++} END {print sum/count}' ratings.csv
```

| 검증 대상 | MapReduce 결과 | 직접 계산 결과 | 일치 여부 |
|---|---|---|---|
| movieId = 1 | 3.92 | 3.92124 | ✅ 일치 |

> 참고: 소수점 둘째 자리에서 반올림한 값이 서로 일치함을 확인했습니다.

**Job 처리 통계 요약**

| 항목 | 값 |
|---|---|
| Map input records | 20,000,264 |
| Map output records | 20,000,263 |
| Reduce input groups (고유 영화 수) | 26,744 |
| Reduce output records | 26,744 |
| Bytes Written | 291,211 |

Reduce input groups(고유 movieId 수)와 Reduce output records 수가 정확히 일치(26,744건)하는 것으로, 모든 영화에 대해 결과가 누락 없이 생성되었음을 확인했습니다.

---

## 6. 부록: 트러블슈팅 기록

진행 과정에서 겪은 이슈와 해결 방법입니다. 동일 환경에서 재현 시 참고하세요.

| # | 증상 | 원인 | 해결 |
|---|---|---|---|
| 1 | NameNode Safe Mode로 쓰기 작업 실패 | DataNode 블록 보고 대기 중 | `hdfs dfsadmin -safemode wait` 또는 자동 해제 대기 |
| 2 | `zsh: command not found: hadoop` | 호스트 터미널에서 실행 시도 | `docker exec`로 컨테이너 내부에서 실행 |
| 3 | `put: ratings.csv No such file or directory` | `docker cp` 전에 `hadoop fs -put` 실행 | `docker cp`로 먼저 컨테이너에 파일 복사 후 `-put` |
| 4 | 로컬 테스트 시 `BrokenPipeError` | `head`가 파이프를 조기 종료 | 결과 값 자체엔 문제없음. 필요시 파일로 저장 후 확인 |
| 5 | YARN 컨테이너가 가상 메모리 초과로 강제 종료 → Job 실패 | Docker+ARM 환경에서 vmem 체크가 오탐 | `yarn-site.xml`에 `yarn.nodemanager.vmem-check-enabled=false` 추가 후 재기동 |
| 6 | 설정 반영을 위해 재빌드 후 `mapper.py`, `reducer.py`, `ratings.csv` 소실 | 컨테이너 재생성 시 미마운트 로컬 파일이 함께 삭제됨 (HDFS 데이터는 별도 볼륨이라 유지) | `docker cp`로 스크립트 재복사 |
| 7 | 결과 검증용 `awk` 명령에서 파일을 못 찾음 | 컨테이너 내부 절대경로(`/ratings.csv`)를 호스트에서 그대로 사용 | 호스트에서는 상대경로(`ratings.csv`)로 실행 |

---

## 7. 참고 사항

- `mapreduce.job.reduces` 설정을 조정하지 않아 기본값(Reducer 1개)으로 실행되었습니다. 데이터 규모가 더 커질 경우 Reducer 수를 늘려 병렬성을 높이는 최적화를 고려할 수 있습니다.
- 평균(average) 집계 특성상 Combiner는 사용하지 않았습니다 (부분 평균의 평균은 전체 평균과 다르기 때문).