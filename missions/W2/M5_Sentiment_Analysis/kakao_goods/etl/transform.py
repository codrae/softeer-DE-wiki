"""
카카오스토어 베스트 상품 리뷰 ETL - Transform 단계
reviews.jsonl 을 읽어 캐릭터별 · 긍정/부정별 키워드 빈도를 집계해 저장한다.
"""

import json
import re
from collections import Counter

INPUT_PATH = "../../../../../data/sentiment140/kakao_goods/reviews.jsonl"
OUTPUT_PATH = "keywords_by_character.json"
TOP_N = 50

# 캐릭터는 별도 컬럼이 없으므로 상품명(product_name)에 이름이 포함되는지로 판별한다.
# "꿀잠친구_베어라이언" 같은 파생 상품명도 "라이언" 부분 문자열로 매칭되어
# 캐릭터는 항상 이 배열에 있는 정식 이름으로만 집계된다.
# 프로토타입 단계라 우선 춘식이/라이언만 수집한다.
CHARACTERS = ["춘식이", "라이언"]

POSITIVE_MIN_RATING = 3

NEGATIVE_MAX_RATING = 2

STOPWORDS = {
    "너무", "정말", "진짜", "그냥", "완전", "아주", "이제", "많이",
    "제품", "상품", "구매", "배송", "사용",
    "이거", "저거", "그거", "이것", "저것", "그것",
    "합니다", "했어요", "해요", "있어요", "없어요", "같아요", "같은",
    "위해", "통해", "대해", "그리고", "그런데", "하지만", "그래서", "때문에",
    "역시", "완전히", "정도",
}

TOKEN_PATTERN = re.compile(r"[가-힣]{2,}")


def load_reviews(path):
    reviews = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            reviews.append(json.loads(line))
    return reviews


def match_characters(product_name):
    """하드코딩된 CHARACTERS 배열을 상품명에서 부분 문자열로 매칭한다."""
    return [name for name in CHARACTERS if name in product_name]


def classify_sentiment(rating):
    if not isinstance(rating, int):
        return None
    if rating >= POSITIVE_MIN_RATING:
        return "positive"
    if rating <= NEGATIVE_MAX_RATING:
        return "negative"
    return None  # 중립 평점(3점)은 긍/부정 집계에서 제외


def tokenize(text, exclude_words):
    """한글 2음절 이상 토큰만 추출하고 불용어·캐릭터명을 제거한다."""
    tokens = TOKEN_PATTERN.findall(text)
    return [t for t in tokens if t not in STOPWORDS and t not in exclude_words]


def extract_concept_keywords(product_name, exclude_words):
    """상품명에서 매칭된 캐릭터 이름을 제거한 나머지를 '컨셉 키워드'로 추출한다."""
    text = product_name
    for name in exclude_words:
        text = text.replace(name, " ")
    tokens = TOKEN_PATTERN.findall(text)
    return [t for t in tokens if t not in STOPWORDS and t not in exclude_words]


def build_keyword_counters(reviews):
    counters = {name: {"positive": Counter(), "negative": Counter()} for name in CHARACTERS}

    for review in reviews:
        sentiment = classify_sentiment(review.get("rating"))
        if sentiment is None:
            continue

        product_name = review.get("product_name", "")
        matched = match_characters(product_name)
        if not matched:
            continue

        exclude = set(matched)
        review_tokens = set(tokenize(review.get("review_text", ""), exclude_words=exclude))
        concept_tokens = set(extract_concept_keywords(product_name, exclude_words=exclude))  # 추가

        tokens = review_tokens | concept_tokens  # 리뷰 본문 키워드 + 상품명 컨셉 키워드 합침
        for character in matched:
            counters[character][sentiment].update(tokens)

    return counters


def to_serializable(counters, top_n=TOP_N):
    return {
        character: {
            sentiment: counter.most_common(top_n)
            for sentiment, counter in sentiments.items()
        }
        for character, sentiments in counters.items()
    }


def save_keywords(counters, path, top_n=TOP_N):
    data = to_serializable(counters, top_n=top_n)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"캐릭터별 긍정/부정 키워드 저장 완료 -> {path}")


def main():
    reviews = load_reviews(INPUT_PATH)
    counters = build_keyword_counters(reviews)
    save_keywords(counters, OUTPUT_PATH)

    for character, sentiments in counters.items():
        pos_total = sum(sentiments["positive"].values())
        neg_total = sum(sentiments["negative"].values())
        if pos_total or neg_total:
            print(f"{character}: positive={pos_total}개, negative={neg_total}개")


if __name__ == "__main__":
    main()
