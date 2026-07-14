#!/usr/bin/env python3
"""
[잡 2 / 분류] 점수표 기반 감성 분류 Mapper
잡 1 → build_model.py 로 만든 nb_model.tsv 를 로드해
트윗 단어들의 점수 합으로 positive / negative / neutral 을 판별한다.

잡 제출 시 -files 에 nb_model.tsv 를 반드시 포함할 것.
출력 형식은 기본 과제와 동일: "<sentiment>\t1" → reducer.py 재사용
"""
import sys
import csv
import re

WORD_RE = re.compile(r"[a-z']+")
THRESHOLD = 0.5  # |점수 합| ≤ 이 값이면 확신 부족 → neutral

def tokenize(text: str):
    """소문자화 → 단어 추출 → 양끝 어퍼스트로피 제거 → 늘려쓰기 정규화"""
    words = []
    for w in WORD_RE.findall(text.lower()):
        w = w.strip("'")                      # ''gay'' → gay / don't는 유지
        w = re.sub(r"(.)\1{2,}", r"\1\1", w)  # sooooo → soo, zzzzz → zz
        if len(w) > 1 or w in ("i", "u", "a"):
            words.append(w)
    return words


def load_model(path="nb_model.tsv"):
    scores = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                word, score = line.rstrip("\n").split("\t")
                scores[word] = float(score)
            except ValueError:
                continue
    return scores

def classify_sentiment(text, scores):
    total = sum(scores.get(w, 0.0) for w in tokenize(text))
    if total > THRESHOLD:
        return "positive"
    elif total < -THRESHOLD:
        return "negative"
    return "neutral"

def main():
    scores = load_model()  # -files 로 배포되면 태스크 작업 디렉토리에 존재
    stream = open(sys.stdin.fileno(), mode="r", encoding="latin-1", errors="replace")
    for row in csv.reader(stream):
        try:
            tweet_text = row[5] if len(row) >= 6 else row[-1]
            print(f"{classify_sentiment(tweet_text.strip(), scores)}\t1")
        except (IndexError, csv.Error):
            continue

if __name__ == "__main__":
    main()