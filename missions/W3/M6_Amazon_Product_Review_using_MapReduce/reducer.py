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