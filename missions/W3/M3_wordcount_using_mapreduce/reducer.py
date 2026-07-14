#!/usr/bin/env python3
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue  # 형식 이상한 줄은 스킵

    if current_word == word:
        current_count += count
    else:
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

# 마지막 word 출력 (이거 빼먹으면 마지막 단어 하나 누락됨 — 흔한 실수)
if current_word is not None:
    print(f"{current_word}\t{current_count}")