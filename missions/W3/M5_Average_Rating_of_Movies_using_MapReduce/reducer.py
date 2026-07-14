#!/usr/bin/env python3
import sys

current_movie = None
total = 0.0
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    movieId, rating = line.split("\t")
    rating = float(rating)

    if current_movie == movieId:
        total += rating
        count += 1
    else:
        if current_movie is not None:
            print(f"{current_movie}\t{total/count:.2f}")
        current_movie = movieId
        total = rating
        count = 1

# 마지막 movieId 처리
if current_movie is not None:
    print(f"{current_movie}\t{total/count:.2f}")