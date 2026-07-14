#!/usr/bin/env python3
import sys

def main():
    counts = {"positive": 0, "negative": 0, "neutral": 0}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            sentiment, count = line.split("\t")
            counts[sentiment] = counts.get(sentiment, 0) + int(count)
        except ValueError:
            continue

    for sentiment, total in counts.items():
        print(f"{sentiment}\t{total}")

if __name__ == "__main__":
    main()
