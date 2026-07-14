#!/usr/bin/env python3
"""
[중간 변환 / 로컬 실행] 잡 1의 출력(라벨별 단어 카운트)을
단어별 감성 점수표(nb_model.tsv)로 변환한다.

사용법: python3 build_model.py counts.txt
입력 형식: pos:단어\t횟수  /  neg:단어\t횟수
출력: nb_model.tsv (단어\t점수) — 점수 = log P(단어|pos) - log P(단어|neg)
"""
import sys
import math

MIN_COUNT = 5  # 총 등장 횟수가 이 미만인 단어는 제외 (노이즈 제거)

def main():
    if len(sys.argv) < 2:
        sys.exit("사용법: python3 build_model.py <잡1 출력 파일>")

    pos_counts, neg_counts = {}, {}
    pos_total = neg_total = 0

    with open(sys.argv[1], encoding="utf-8") as f:
        for line in f:
            try:
                key, count = line.rstrip("\n").split("\t")
                prefix, word = key.split(":", 1)
                count = int(count)
            except ValueError:
                continue
            if prefix == "pos":
                pos_counts[word] = count
                pos_total += count
            elif prefix == "neg":
                neg_counts[word] = count
                neg_total += count

    vocab = {w for w in (pos_counts.keys() | neg_counts.keys())
             if pos_counts.get(w, 0) + neg_counts.get(w, 0) >= MIN_COUNT}
    V = len(vocab)

    # 라플라스 스무딩(+1): 한쪽에서 0번 나온 단어의 점수가 무한대가 되는 것 방지
    with open("nb_model.tsv", "w", encoding="utf-8") as out:
        for w in sorted(vocab):
            p_pos = (pos_counts.get(w, 0) + 1) / (pos_total + V)
            p_neg = (neg_counts.get(w, 0) + 1) / (neg_total + V)
            out.write(f"{w}\t{math.log(p_pos) - math.log(p_neg):.6f}\n")

    print(f"어휘 {V}개 → nb_model.tsv 저장 (pos 단어 총 {pos_total:,} / neg 단어 총 {neg_total:,})")

if __name__ == "__main__":
    main()