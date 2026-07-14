#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    for word in words:
        word = word.strip().lower()
        # 알파벳/숫자만 남기고 구두점 제거 (선택, 원하면 빼도 됨)
        word = ''.join(ch for ch in word if ch.isalnum())
        if word:
            print(f"{word}\t1")