#!/usr/bin/env python3
"""
W3M4 - Twitter Sentiment Analysis (Reducer)

Mapper가 emit한 "<key>\t1" 쌍을 집계한다.
Hadoop의 Sort & Shuffle 단계가 같은 key를 정렬해서 넘겨주므로,
key가 바뀌는 시점마다 누적 카운트를 출력하면 된다.

기본 과제(감성 집계)와 팀 활동 잡 1(라벨별 단어 카운트),
잡 2(감성 집계)에서 모두 동일하게 재사용된다.
"""

import sys


def main():
    current_key = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            key, count = line.split("\t", 1)
            count = int(count)
        except ValueError:
            # 형식이 깨진 줄은 건너뛴다
            continue

        if key == current_key:
            current_count += count
        else:
            # key가 바뀌면 이전 key의 집계 결과를 출력
            if current_key is not None:
                print(f"{current_key}\t{current_count}")
            current_key = key
            current_count = count

    # 마지막 key는 루프가 끝난 뒤 출력
    if current_key is not None:
        print(f"{current_key}\t{current_count}")


if __name__ == "__main__":
    main()