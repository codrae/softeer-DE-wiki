# W3M6 - Most Reviewed Products & Average Rating using MapReduce

## 1. 미션 개요

**학습 목표**
Amazon Product Reviews 데이터셋을 이용해 Python 기반 MapReduce 잡을 작성하여,
상품별 리뷰 개수와 평균 평점을 구함으로써 Hadoop MapReduce 프레임워크에 대한
이해를 실습한다.

**기능 요구사항**
| 항목 | 요구 내용 |
|---|---|
| Data Processing | HDFS에서 입력 데이터를 정상적으로 읽어야 함 |
| Mapper | 각 레코드를 처리하여 (product_id, rating) 형태의 key-value emit |
| Reducer | key-value를 집계하여 리뷰 개수(count)와 평균 평점(average)을 계산 |
| Job Execution | 클러스터에서 에러 없이 실행, 효율적으로 처리 |
| Result Output | HDFS에 텍스트 파일로 저장, 한 줄당 `상품ID  리뷰수  평균평점` (탭/공백 구분) |
| Documentation | 컴파일·실행·HDFS 업로드/조회·결과 해석 방법을 담은 README |
| Submission | 소스코드 + README + Docker 파일(이미지 제외) |

**예상 출력 예시**
```
B001E4KFG0    150    4.3
B00813GRG4    200    3.9
```

## 2. 데이터셋

- 이름: **Amazon Reviews 2023** (McAuley Lab, UCSD)
- 출처: https://amazon-reviews-2023.github.io/
- 사용 카테고리: `Gift_Cards`
- 사용 파일: `Gift_Cards.jsonl` (152,410개 리뷰 레코드)

**카테고리 선정 이유**
공식 통계상 Gift_Cards는 상품 수(약 1.1K, parent_asin 기준) 대비 리뷰 수(약 152K)가
많아 상품당 평균 리뷰 수가 130건 이상으로, 다른 카테고리(Digital_Music 등 상품당
평균 2건 미만)보다 "가장 많이 리뷰된 상품"을 찾는 미션 취지에 부합하고, 파일 크기(약
50MB)도 작아 로컬 환경에서 반복 검증하기에 적합함.

**레코드 구조 예시**
```json
{"rating": 5.0, "title": "Great gift", "text": "...", "images": [],
 "asin": "B00IX1I3G6", "parent_asin": "B00IX1I3G6",
 "user_id": "...", "timestamp": 1549866158332,
 "helpful_vote": 0, "verified_purchase": true}
```
- 상품 ID는 `asin` 필드를 기준으로 집계함 (동일 상품의 금액/디자인 변형까지 묶는
  `parent_asin`은 사용하지 않음). `asin` 기준 고유 상품 수는 1,894개로, 공식 통계표의
  `parent_asin` 기준 Item 수(약 1.1K)와는 정의 자체가 달라 차이가 남.

## 3. Map/Reduce 내부 동작 흐름

```
[HDFS: Gift_Cards.jsonl]
        │
        ▼
  ① InputSplit 분할
     파일이 HDFS 블록 단위로 나뉘고, Streaming은 이를 여러 Map Task에 분배
     (본 실행에서는 number of splits: 2 → Map Task 2개 생성)
        │
        ▼
  ② Map 단계 (mapper.py, Task별로 독립 프로세스)
     - Hadoop Streaming이 각 라인을 표준입력(stdin)으로 mapper.py에 전달
       (key는 버리고 value인 라인 텍스트만 전달 — Streaming 텍스트 모드의 기본 동작)
     - mapper.py는 각 라인을 JSON 파싱 → asin, rating 추출
     - "{asin}\t{rating}" 형태로 표준출력(stdout)에 emit
        │
        ▼
  ③ Shuffle & Sort (프레임워크가 자동 수행, 코드로 작성하지 않음)
     - 각 Map Task의 출력을 key(asin) 기준으로 파티셔닝
       (기본 HashPartitioner: hash(asin) % reduce task 수)
     - 같은 asin은 반드시 같은 Reduce Task로 전송됨
     - Reduce Task에 도착한 데이터는 key 기준으로 정렬(sort)되어
       reducer.py에는 같은 asin끼리 뭉쳐서 순서대로 들어옴
        │
        ▼
  ④ Reduce 단계 (reducer.py, 본 실행에서는 Task 1개)
     - 정렬된 (asin, rating) 스트림을 한 줄씩 읽음
     - 이전 줄의 asin과 같으면 count += 1, total += rating으로 누적
     - 다른 asin으로 바뀌는 시점(그룹 경계)에 이전 asin의 결과를
       "asin\tcount\t{total/count:.1f}" 형태로 flush
     - 평균은 부분 평균끼리 다시 평균 내는 방식이 아니라, sum/count로
       마지막에 한 번에 계산 (누적 합/개수 방식이 정확한 평균을 보장함)
        │
        ▼
  ⑤ Output (HDFS)
     /output/gift_cards_result/
       ├─ _SUCCESS
       └─ part-00000   (Reduce Task 1개 → 출력 파일 1개)
```

**본 실행에서 관측된 카운터와 위 흐름의 대응**
| 카운터 | 값 | 의미 |
|---|---|---|
| Map input records | 152,410 | 원본 라인 수, InputSplit 2개가 처리한 총 레코드 |
| Map output records | 152,410 | mapper.py가 emit한 (asin, rating) 쌍의 수 (유실 없음) |
| Reduce input groups | 1,894 | Shuffle & Sort 후 만들어진 고유 asin(key) 그룹 수 |
| Reduce output records | 1,894 | reducer.py가 최종 emit한 상품 수 (그룹당 1줄) |
| Combine input/output records | 0 | Combiner를 사용하지 않음 (본 잡은 combiner 미설정) |

## 4. 파일 구성

```
M6_Amazon_Product_Review_using_MapReduce/
├── mapper.py
├── reducer.py
├── docker-compose.yaml
├── core-site.xml
├── hdfs-site.xml
├── yarn-site.xml
└── README.md
```

### mapper.py
```python
#!/usr/bin/env python
import sys
import json

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        record = json.loads(line)
        asin = record.get("asin")
        rating = record.get("rating")
        if asin is not None and rating is not None:
            print(f"{asin}\t{rating}")
    except json.JSONDecodeError:
        continue
```

### reducer.py
```python
#!/usr/bin/env python
import sys

current_asin = None
count = 0
total = 0.0

for line in sys.stdin:
    line = line.strip()
    try:
        asin, rating = line.split("\t")
        rating = float(rating)
    except ValueError:
        continue

    if current_asin == asin:
        count += 1
        total += rating
    else:
        if current_asin is not None:
            print(f"{current_asin}\t{count}\t{total/count:.1f}")
        current_asin = asin
        count = 1
        total = rating

if current_asin is not None:
    print(f"{current_asin}\t{count}\t{total/count:.1f}")
```

## 5. 전체 실행 명령어

### 5-1. 로컬 사전 검증 (호스트, 프로젝트 디렉토리에서)

```bash
# 데이터 라인 수 확인
wc -l ../../../data/Gift_Cards.jsonl
# 152410 ../../../data/Gift_Cards.jsonl

# mapper 단독 실행 (유실 여부 확인)
cat ../../../data/Gift_Cards.jsonl | python mapper.py | wc -l
# 152410  (입력과 동일 → 유실 없음)

# 전체 파이프라인 로컬 실행
cat ../../../data/Gift_Cards.jsonl | python mapper.py | sort | python reducer.py | head -20

# 결과를 파일로 저장 (나중에 클러스터 결과와 diff 비교용)
cat ../../../data/Gift_Cards.jsonl | python mapper.py | sort | python reducer.py > local_result.tsv
wc -l local_result.tsv
# 1894 local_result.tsv
```

### 5-2. Docker 컨테이너 기동

```bash
docker-compose up -d
docker ps   # namenode, datanode, resourcemanager, nodemanager 기동 확인
```

### 5-3. 파일을 컨테이너로 복사 (호스트에서 실행)

```bash
docker cp ../../../data/Gift_Cards.jsonl namenode:/tmp/Gift_Cards.jsonl
docker cp mapper.py namenode:/tmp/mapper.py
docker cp reducer.py namenode:/tmp/reducer.py
```

### 5-4. 컨테이너 진입 및 HDFS 업로드

```bash
docker exec -it namenode bash

hadoop fs -mkdir -p /input/gift_cards
hadoop fs -put /tmp/Gift_Cards.jsonl /input/gift_cards/
hadoop fs -ls /input/gift_cards
# -rw-r--r--   2 root supergroup   50231035 .../Gift_Cards.jsonl
```

### 5-5. 실행 권한 부여

```bash
chmod +x /tmp/mapper.py /tmp/reducer.py
```

### 5-6. MapReduce 잡 제출

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /input/gift_cards \
  -output /output/gift_cards_result
```
> `python`이 아닌 `python3`로 인터프리터를 명시해야 하는 이유는 6장 트러블슈팅 참고.

잡 제출 후 콘솔에 아래처럼 map/reduce 진행률이 실시간으로 출력됨:
```
map 0% reduce 0%
map 100% reduce 0%
map 100% reduce 100%
Job job_1784028314212_0003 completed successfully
```
동일한 진행 상황은 ResourceManager Web UI(`http://localhost:8088`)의
`The url to track the job:` 링크에서도 확인 가능.

### 5-7. 결과 조회

```bash
hadoop fs -ls /output/gift_cards_result
# _SUCCESS, part-00000

hadoop fs -cat /output/gift_cards_result/part-00000 | head -20
hadoop fs -cat /output/gift_cards_result/part-00000 | wc -l
# 1894
```

### 5-8. 결과 회수 및 로컬 검증 결과와 비교

```bash
# 컨테이너 내부
hadoop fs -get /output/gift_cards_result/part-00000 /tmp/hdfs_result.tsv
exit

# 호스트
docker cp namenode:/tmp/hdfs_result.tsv .
sort hdfs_result.tsv > hdfs_result_sorted.tsv
diff local_result.tsv hdfs_result_sorted.tsv
# 출력 없음 → 로컬/클러스터 결과 완전 일치
```

## 6. 트러블슈팅

### 문제: `Cannot run program "python": error=2, No such file or directory`

**증상**
```
hadoop jar ... -mapper "python mapper.py" -reducer "python reducer.py" ...
```
로 실행 시 Map Task 8개 중 7개가 FAILED, 잡 전체가 `Job not successful!`로 종료됨.
에러 스택 최하단:
```
Caused by: java.io.IOException: Cannot run program "python": error=2, No such file or directory
```

**원인 분석**
- 컨테이너 OS(Ubuntu 20.04 기반, Python 3.8.10)에는 `python3`만 설치되어 있고
  `python` 심볼릭 링크가 없음. 최근 Debian/Ubuntu 배포판에서 흔한 구성.
- Hadoop Streaming은 `-mapper`/`-reducer`에 지정된 커맨드를 각 Task 컨테이너
  내부에서 `ProcessBuilder`로 그대로 실행하므로, 셸의 PATH 상에 해당 실행 파일이
  없으면 Task 자체가 configure 단계에서 즉시 실패함 (Map 0%였다가 갑자기 실패로
  넘어가는 이유).
- YARN이 실패한 Task를 자동으로 재시도(최대 4회)하지만, 원인이 환경 문제이므로
  재시도해도 같은 이유로 계속 실패 → 결국 잡 전체 실패.

**확인**
```bash
which python    # (출력 없음)
which python3   # /usr/bin/python3
python3 --version
# Python 3.8.10
```

**해결**
`-mapper`, `-reducer` 옵션에 인터프리터를 `python3`로 명시적으로 지정:
```bash
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py" \
```

### 문제: 재실행 시 output 디렉토리 충돌

**증상**: 같은 `-output` 경로로 재실행하면 `Output directory already exists` 에러 발생 가능
(FileAlreadyExistsException). Hadoop은 안전을 위해 기존 출력 디렉토리를 덮어쓰지 않음.

**해결**: 재실행 전 기존 출력 삭제
```bash
hadoop fs -rm -r /output/gift_cards_result
```

## 7. 결과 검증 요약

| 검증 항목 | 결과 |
|---|---|
| Map input records = 원본 라인 수 | 152,410 = 152,410 ✅ (유실 없음) |
| Map output records = Map input records | 152,410 = 152,410 ✅ (파싱 실패 레코드 없음) |
| Reduce output records = 로컬 검증 결과 줄 수 | 1,894 = 1,894 ✅ |
| 로컬 결과 vs 클러스터 결과 `diff` | 출력 없음 ✅ (완전 일치) |

## 8. M5(MovieLens 평균 평점)와의 차이점

| 구분 | M5 | M6 |
|---|---|---|
| 데이터셋 | MovieLens ratings.csv | Amazon Reviews 2023 (Gift_Cards) |
| 입력 포맷 파싱 | CSV `split(",")` | JSON `json.loads()` |
| Reducer 출력 | 평균 평점 1개 값 | 리뷰 개수 + 평균 평점 2개 값 |
| 평균 계산 방식 주의점 | - | sum/count 누적 방식 사용 (부분 평균의 평균 금지) |
| Docker/YARN 환경 이슈 | vmem-check 문제 (yarn.nodemanager.vmem-check-enabled=false로 해결) | python 인터프리터 미존재 문제 (python3 명시로 해결) |