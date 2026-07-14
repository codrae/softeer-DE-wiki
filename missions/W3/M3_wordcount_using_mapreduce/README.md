# W3M3 - MapReduce Word Count (Hadoop Streaming, Python)

## 개요
Hadoop Streaming을 이용해 Python으로 작성한 Mapper/Reducer로 MapReduce Word Count를
수행하는 미션. Java 없이 표준 입출력(stdin/stdout) 기반으로 구현.

## 환경
- Hadoop 2.10.2, Docker Compose 기반 멀티노드 클러스터 (namenode + worker1/2/3)
- W3M2a/M2b에서 구축한 클러스터 재사용
  (yarn.resourcemanager.hostname, mapreduce_shuffle, dfs.datanode.data.dir,
  mapreduce.map/reduce.java.opts=-Xmx768m, yarn.nodemanager.resource.memory-mb=2048 등 설정 완료)

## 입력 데이터 (e-book)
- 제목: *Moby Dick; Or, The Whale* (Herman Melville)
- 출처: Project Gutenberg (public domain)
- URL: https://www.gutenberg.org/cache/epub/2701/pg2701.txt
- 크기: 22,314줄 / 약 215,845 단어 (200페이지 요구사항 충족)

## 파일 구성
- `mapper.py`: 입력 텍스트를 단어 단위로 토큰화하여 `(word, 1)` 형태로 emit
- `reducer.py`: 정렬된 key(단어) 스트림을 받아 같은 word의 값을 합산하여 `(word, total_count)` 출력
- `wordcount_result.txt`: 최종 결과 (HDFS output을 getmerge한 로컬 사본)

## 실행 절차

### 1) 로컬 단위 테스트
\`\`\`bash
chmod +x mapper.py reducer.py
echo -e "hello world\nhello hadoop world" | ./mapper.py | sort | ./reducer.py
# 기대 출력: hadoop 1 / hello 2 / world 2
\`\`\`

### 2) e-book 다운로드
\`\`\`bash
curl -o data/moby_dick.txt https://www.gutenberg.org/cache/epub/2701/pg2701.txt
\`\`\`

### 3) HDFS 업로드
\`\`\`bash
docker cp data/moby_dick.txt namenode:/tmp/moby_dick.txt
docker exec -it namenode bash

hdfs dfs -mkdir -p /user/root/wordcount/input
hdfs dfs -put /tmp/moby_dick.txt /user/root/wordcount/input/
hdfs dfs -ls /user/root/wordcount/input/
\`\`\`

### 4) mapper/reducer 스크립트를 namenode 컨테이너로 복사
\`\`\`bash
# 호스트에서
docker cp missions/W3/M3_wordcount_using_mapreduce/mapper.py namenode:/tmp/mapper.py
docker cp missions/W3/M3_wordcount_using_mapreduce/reducer.py namenode:/tmp/reducer.py

# 컨테이너 안에서
chmod +x /tmp/mapper.py /tmp/reducer.py
\`\`\`

### 5) Hadoop Streaming job 제출
\`\`\`bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -input /user/root/wordcount/input \
  -output /user/root/wordcount/output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py"
\`\`\`

### 6) 결과 확인
\`\`\`bash
hdfs dfs -ls /user/root/wordcount/output/
hdfs dfs -cat /user/root/wordcount/output/part-* | head -20
hdfs dfs -cat /user/root/wordcount/output/part-* | sort -k2 -nr | head -20   # 빈도수 상위 20개
hdfs dfs -cat /user/root/wordcount/output/part-* | wc -l                     # 고유 단어 수 검증

# 로컬로 병합해서 가져오기
hdfs dfs -getmerge /user/root/wordcount/output /tmp/wordcount_result.txt
exit
docker cp namenode:/tmp/wordcount_result.txt ./missions/W3/M3_wordcount_using_mapreduce/wordcount_result.txt
\`\`\`

## 실행 결과 (Job Counters 요약)
| 항목 | 값 |
|---|---|
| Map input records | 22,314 |
| Map output records | 215,798 |
| Reduce input groups (고유 단어 수) | 20,197 |
| Reduce output records | 20,197 |
| Launched map tasks | 2 |
| Launched reduce tasks | 1 |
| Job 상태 | SUCCESS |

`wc -l wordcount_result.txt` = 20197로 Reduce output records와 정확히 일치 → 정합성 검증 완료.

## 트러블슈팅 노트
- `docker cp`는 `data/` gitignore 폴더 안에 있는 파일이어도 문제없이 컨테이너로 복사 가능
  (gitignore는 git 추적 대상에서만 제외, 파일시스템 접근에는 영향 없음)
- `-mapper`/`-reducer`에는 `-files`로 배포한 스크립트를 **파일명만** 지정해야 함
  (절대경로 X — worker의 작업 디렉토리 기준 상대경로로 동작)
- 재실행 시 output 디렉토리가 이미 있으면 `Output directory already exists` 에러 발생 →
  `hdfs dfs -rm -r /user/root/wordcount/output`으로 미리 삭제 필요