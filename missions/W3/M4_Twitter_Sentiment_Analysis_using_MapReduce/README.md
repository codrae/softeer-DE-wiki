# W3M4 - Sentiment Analysis with MapReduce (Sentiment140)

## 개요
Sentiment140 데이터셋(트윗 160만 건)을 Hadoop Streaming + Python MapReduce로
predefined keyword 기반 감성 분류(positive/negative/neutral) 후 카운트 집계.

## 환경
- Hadoop 2.10.2 (Docker Compose 클러스터: namenode + worker1/2/3)
- Hadoop Streaming (hadoop-streaming-2.10.2.jar)
- Python 3

## 데이터 준비
1. Kaggle `kazanova/sentiment140` 다운로드
2. 인코딩 변환: ISO-8859-1 → UTF-8
```bash
   iconv -f ISO-8859-1 -t UTF-8 training.1600000.processed.noemoticon.csv > training_utf8.csv
```
3. namenode 컨테이너로 복사 후 HDFS 업로드
```bash
   docker cp training_utf8.csv namenode:/tmp/training_utf8.csv
   hdfs dfs -mkdir -p /user/root/sentiment140/input
   hdfs dfs -put /tmp/training_utf8.csv /user/root/sentiment140/input/
```

## 구성 파일
- `mapper.py`: CSV의 text 컬럼(6번째 컬럼)을 읽어 predefined keyword set과
  매칭해 positive/negative/neutral로 분류 후 `(sentiment, 1)` emit
  - 원본 CSV의 target 라벨은 사용하지 않고, 키워드 기반으로 재분류함
  - `csv.reader` 사용 이유: text 필드 안에 콤마가 포함된 경우 단순 split으로는 컬럼이 밀림
- `reducer.py`: sentiment별 카운트 합산

## 실행 방법
```bash
docker cp mapper.py namenode:/tmp/mapper.py
docker cp reducer.py namenode:/tmp/reducer.py

docker exec -it namenode bash
cd /tmp
chmod +x mapper.py reducer.py

hdfs dfs -rm -r -f /user/root/sentiment140/output

hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.2.jar \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /user/root/sentiment140/input/training_utf8.csv \
  -output /user/root/sentiment140/output
```

## 결과 확인
```bash
hdfs dfs -ls /user/root/sentiment140/output
hdfs dfs -cat /user/root/sentiment140/output/part-00000
```

## 결과
| Sentiment | Count     |
|-----------|-----------|
| positive  | 401,176   |
| negative  | 188,572   |
| neutral   | 1,010,252 |
| **합계**  | **1,600,000** |

- Map input records = 1,600,000 / Reduce output records = 3 → 데이터 누락 없이 전량 처리 확인
- neutral 비중이 63%로 압도적으로 높음 → predefined keyword 세트에 매칭되는
  단어가 없는 트윗이 전부 neutral로 분류되기 때문 (아래 한계 참고)

## 한계 및 개선점 (팀 토론용, W3M4 슬라이드 6~7 참고)
- Predefined keyword 방식은 문맥/신조어/반어법을 이해하지 못하고,
  키워드 세트에 없는 표현은 전부 neutral로 뭉뚱그려져 정확도가 낮음
  (실제로 neutral이 63%나 되는 것도 이 한계를 보여줌)
- 대안 (W3M4 슬라이드 7 참고):
  - TF-IDF + KMeans (자동 분류, 키워드 불필요)
  - LDA 토픽 모델링 (의미 기반 분류)
  - Word2Vec/FastText (의미적 유사성 기반)
  - Rule-based (TextBlob)
  - 지도학습 분류기 (SVM, RandomForest, Naive Bayes 등)
- 팀 논의는 별도 진행 예정
## 3주차 리뷰 피드백

### Classification을 어디서 수행해야 할까

감성 분류를 파이프라인의 어느 지점에 둘지에 대한 선택지를 함께 검토했다.

- Inside Mapper (이번 미션의 방식)
- Pre-Processing before MapReduce
- External API Call in Mapper
- Spark + MLlib

프로토타이핑 단계라면 **최종 시스템 설계와 완전히 같을 필요는 없다.** 샘플링된 대상으로만
진행하기 때문에 빠르게 결과를 얻는 것이 더 중요하다. 다만 그 프로토타이핑이
*아키텍처가 옳은지 판단하기 위한 것*인지, *데이터 정합성이 유지되는지 판단하기 위한 것*인지에
따라 방식이 달라져야 한다.

### 샘플링 검증

1,000건을 처리할 때 동일한 결과가 나오는지 확인하려면, **100개씩 10세트**와 **1,000개 1세트**
두 가지 샘플링 방식을 모두 돌려 비교 검증해야 한다.

### 원본 라벨을 신뢰하지 말 것

raw data가 우리가 원하는 대로 라벨링되어 있는지 반드시 확인해야 한다. 원본 데이터의 라벨을
**"정답"이 아니라 "표본" 또는 "기존의 판단"**으로 부르는 편이 더 적합하다.
(이 미션에서 원본 CSV의 target 라벨을 쓰지 않고 키워드 기반으로 재분류한 것도 같은 맥락이다.)

### 결론 서술

현재 결론 부분이 비어 있다. **baseline을 먼저 정해야 비교와 결론이 가능하다.**
"이런 걸 배웠습니다" 식의 태도 서술은 의미가 없다 — 틀리지 않으려고만 하지 말 것.
