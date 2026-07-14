#!/usr/bin/env python3
"""
[잡 1 / 학습] 라벨별 단어 카운트 Mapper
Sentiment140의 라벨(0/4)과 트윗 본문을 읽어
"pos:단어\t1" 또는 "neg:단어\t1" 을 emit 한다.
(key에 라벨을 붙인 WordCount)
Reducer는 기본 과제의 reducer.py를 그대로 재사용한다.
"""
import sys
import csv
import re

WORD_RE = re.compile(r"[a-z']+")

def tokenize(text: str):
    """소문자화 → 단어 추출 → 양끝 어퍼스트로피 제거 → 늘려쓰기 정규화"""
    words = []
    for w in WORD_RE.findall(text.lower()):
        w = w.strip("'")                      # ''gay'' → gay / don't는 유지
        w = re.sub(r"(.)\1{2,}", r"\1\1", w)  # sooooo → soo, zzzzz → zz
        if len(w) > 1 or w in ("i", "u", "a"):
            words.append(w)
    return words


def main():
    stream = open(sys.stdin.fileno(), mode="r", encoding="latin-1", errors="replace")
    for row in csv.reader(stream):
        try:
            label, text = row[0], row[5]
        except IndexError:
            continue
        if label == "4":
            prefix = "pos"
        elif label == "0":
            prefix = "neg"
        else:
            continue  # 그 외 라벨은 무시
        for word in tokenize(text):
            print(f"{prefix}:{word}\t1")

if __name__ == "__main__":
    main()