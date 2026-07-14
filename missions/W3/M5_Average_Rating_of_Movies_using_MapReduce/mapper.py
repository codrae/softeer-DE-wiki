#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    fields = line.split(",")

    # 헤더 줄(userId,movieId,rating,timestamp) 스킵
    if fields[0] == "userId":
        continue

    # 컬럼 개수가 안 맞는 이상한 줄도 스킵 (방어 코드)
    if len(fields) != 4:
        continue

    userId, movieId, rating, timestamp = fields
    print(f"{movieId}\t{rating}")