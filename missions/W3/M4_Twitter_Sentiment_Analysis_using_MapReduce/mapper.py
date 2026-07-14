#!/usr/bin/env python3
import sys
import csv
import re
import io

POSITIVE_KEYWORDS = {
    "good", "great", "love", "happy", "awesome", "amazing", "best",
    "excellent", "wonderful", "nice", "fun", "glad", "thanks", "thank",
    "perfect", "beautiful", "enjoy", "enjoyed", "haha", "lol", "yay",
    "cool", "fantastic", "excited", "smile"
}

NEGATIVE_KEYWORDS = {
    "bad", "hate", "sad", "terrible", "worst", "awful", "sucks", "sick",
    "angry", "annoying", "disappointed", "cry", "crying", "pain", "hurt",
    "tired", "sorry", "ugh", "damn", "stupid", "boring", "fail", "broken",
    "miss", "missing"
}

def classify(text: str) -> str:
    tokens = set(re.findall(r"[a-zA-Z']+", text.lower()))
    pos_hit = len(tokens & POSITIVE_KEYWORDS)
    neg_hit = len(tokens & NEGATIVE_KEYWORDS)

    if pos_hit > neg_hit:
        return "positive"
    elif neg_hit > pos_hit:
        return "negative"
    else:
        return "neutral"

def main():
    reader = csv.reader(io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="ignore"))
    for row in reader:
        if len(row) < 6:
            continue
        text = row[5]
        sentiment = classify(text)
        print(f"{sentiment}\t1")

if __name__ == "__main__":
    main()
