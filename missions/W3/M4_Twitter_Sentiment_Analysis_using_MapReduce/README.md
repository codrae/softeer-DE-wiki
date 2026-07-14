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