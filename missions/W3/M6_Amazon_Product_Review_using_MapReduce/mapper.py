#!/usr/bin/env python
import sys
import json

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        record = json.loads(line)
        asin = record.get("asin")
        rating = record.get("rating")
        if asin is not None and rating is not None:
            print(f"{asin}\t{rating}")
    except json.JSONDecodeError:
        continue