# -*- coding: utf-8 -*-
"""
캐릭터별 긍정/부정 키워드 워드클라우드 생성 스크립트.

입력 JSON 형식:
{
  "캐릭터명": {
    "positive": [["단어", 빈도], ...],
    "negative": [["단어", 빈도], ...]
  },
  ...
}

사용법:
    python generate_wordclouds.py keywords_by_character.json --outdir outputs

한글 폰트가 필요합니다. 아래 FONT_CANDIDATES 중 시스템에 설치된 첫 번째 폰트를
자동으로 사용합니다. 못 찾으면 --font 옵션으로 직접 경로를 지정하세요.
"""

import argparse
import json
import os

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# 환경별로 흔히 쓰이는 한글 폰트 경로 후보 (위에서부터 순서대로 탐색)
FONT_CANDIDATES = [
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",  # Linux (Noto)
    "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",          # Linux (Nanum)
    "/System/Library/Fonts/Supplemental/AppleGothic.ttf",       # macOS
    "/System/Library/Fonts/AppleSDGothicNeo.ttc",                # macOS
    "C:/Windows/Fonts/malgun.ttf",                               # Windows
]


def find_korean_font(explicit_path: str | None = None) -> str:
    if explicit_path:
        if not os.path.exists(explicit_path):
            raise FileNotFoundError(f"지정한 폰트 경로를 찾을 수 없습니다: {explicit_path}")
        return explicit_path

    for candidate in FONT_CANDIDATES:
        if os.path.exists(candidate):
            return candidate

    raise FileNotFoundError(
        "한글 폰트를 찾지 못했습니다. --font 옵션으로 폰트 경로(.ttf/.ttc)를 직접 지정해주세요.\n"
        "예: --font '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'"
    )


def register_korean_font(font_path: str) -> str:
    """matplotlib fontManager에 폰트를 등록하고, 등록된 폰트 패밀리명을 반환합니다."""
    fm.fontManager.addfont(font_path)
    font_name = fm.FontProperties(fname=font_path).get_name()
    plt.rcParams["font.family"] = font_name
    plt.rcParams["axes.unicode_minus"] = False  # 마이너스 기호 깨짐 방지
    return font_name


def build_wordcloud(word_freq_pairs, font_path: str, colormap: str) -> WordCloud:
    freq_dict = {word: count for word, count in word_freq_pairs}
    wc = WordCloud(
        font_path=font_path,
        width=800,
        height=600,
        background_color="white",
        colormap=colormap,
        prefer_horizontal=0.9,
        max_words=100,
    )
    wc.generate_from_frequencies(freq_dict)
    return wc


def make_character_figure(character: str, data: dict, font_path: str, outdir: str) -> str:
    positive = data.get("positive", [])
    negative = data.get("negative", [])

    fig, axes = plt.subplots(1, 2, figsize=(14, 7))

    if positive:
        wc_pos = build_wordcloud(positive, font_path, colormap="Blues")
        axes[0].imshow(wc_pos, interpolation="bilinear")
    axes[0].set_title(f"{character} - 긍정 키워드", fontsize=16)
    axes[0].axis("off")

    if negative:
        wc_neg = build_wordcloud(negative, font_path, colormap="Reds")
        axes[1].imshow(wc_neg, interpolation="bilinear")
    axes[1].set_title(f"{character} - 부정 키워드", fontsize=16)
    axes[1].axis("off")

    fig.suptitle(f"'{character}' 리뷰 키워드 워드클라우드", fontsize=18, y=1.02)
    fig.tight_layout()

    safe_name = character.replace("/", "_")
    out_path = os.path.join(outdir, f"wordcloud_{safe_name}.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="캐릭터별 긍정/부정 워드클라우드 생성")
    parser.add_argument("json_path", help="키워드 빈도 JSON 파일 경로")
    parser.add_argument("--outdir", default="outputs", help="이미지 저장 디렉토리 (기본값: outputs)")
    parser.add_argument("--font", default=None, help="한글 폰트(.ttf/.ttc) 경로 직접 지정")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    font_path = find_korean_font(args.font)
    font_name = register_korean_font(font_path)
    print(f"사용 폰트: {font_path} (matplotlib family: {font_name})")

    with open(args.json_path, encoding="utf-8") as f:
        data_by_character = json.load(f)

    saved_paths = []
    for character, data in data_by_character.items():
        out_path = make_character_figure(character, data, font_path, args.outdir)
        saved_paths.append(out_path)
        print(f"저장 완료: {out_path}")

    return saved_paths


if __name__ == "__main__":
    main()