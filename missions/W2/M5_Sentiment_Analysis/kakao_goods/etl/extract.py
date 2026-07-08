# -*- coding: utf-8 -*-
"""
카카오프렌즈 스토어(store.kakao.com) 리뷰 크롤러 (Scrapy + Playwright)

동작 흐름
    1. 카테고리 페이지(리뷰 많은 순 정렬)에서 상품 링크를 카테고리별 상위 N개 추출
    2. 각 상품 페이지에서 리뷰 탭 클릭 -> '더보기' 반복 클릭 -> 리뷰 텍스트/평점/작성일 수집

배경
    - 해당 사이트는 Angular 기반 SPA로 리뷰가 JS로 동적 로딩되므로,
      scrapy-playwright로 실제 브라우저 렌더링과 클릭/스크롤이 필요하다.
    - Twisted reactor는 프로세스당 1회만 시작 가능하므로, 링크 수집과 리뷰 수집을
      콜백 체이닝(parse_category -> parse_review)으로 묶어 스파이더 하나로 처리한다.

사전 설치
    pip install scrapy scrapy-playwright
    playwright install chromium

실행
    python extract.py   # 결과는 reviews.jsonl 에 저장
"""

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy_playwright.page import PageMethod


class KakaoReviewSpider(scrapy.Spider):
    name = "kakao_review"

    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 1.0,
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "LOG_LEVEL": "INFO",   # DEBUG 숨기고 INFO 이상만
    }

    CATEGORY_URLS = [
        "https://store.kakao.com/kakaofriends/category/7478375a1c7c4944befd972314b9e87c?sort=REVIEW_COUNT",
        "https://store.kakao.com/kakaofriends/category/69d5b59721fb456b8f1b1060144ae949?sort=REVIEW_COUNT",
    ]

    LINKS_PER_CATEGORY = 10   # 카테고리별 수집할 상품 수
    MAX_MORE_CLICKS = 200     # '더보기' 최대 클릭 횟수 (무한 루프 방지)
    MAX_SCROLL_TRIES = 15     # 리뷰 탭 탐색 시 최대 스크롤 횟수

    # Scrapy 2.13+ : start_requests() 대신 async start() 사용
    async def start(self):
        for url in self.CATEGORY_URLS:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        # SPA 렌더링 완료까지 상품 링크 대기
                        PageMethod("wait_for_selector", "a.link_thumb", timeout=15000),
                    ],
                },
                callback=self.parse_category,
                errback=self.errback_close_page,
            )

    async def parse_category(self, response):
        """카테고리 페이지에서 상품 링크를 추출해 리뷰 파싱으로 연결한다."""
        await response.meta["playwright_page"].close()

        hrefs = response.css("a.link_thumb::attr(href)").getall()

        # 중복 제거 후 상위 N개, 절대 URL 변환
        product_urls = []
        for href in hrefs:
            full_url = response.urljoin(href)
            if full_url not in product_urls:
                product_urls.append(full_url)
            if len(product_urls) >= self.LINKS_PER_CATEGORY:
                break

        self.logger.info("카테고리 파싱 완료: 상품 %d개 (%s)", len(product_urls), response.url)

        for product_url in product_urls:
            yield scrapy.Request(
                product_url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                },
                callback=self.parse_review,
                errback=self.errback_close_page,
                dont_filter=True,
            )

    async def parse_review(self, response):
        """상품 페이지에서 리뷰 탭을 열고 '더보기'를 반복 클릭하며 리뷰를 수집한다."""
        page = response.meta["playwright_page"]

        try:
            tab_clicked = await self._open_review_tab(page)
            if not tab_clicked:
                self.logger.warning("리뷰 탭 탐색 실패: %s", response.url)

            await self._expand_all_reviews(page)

            product_name = await self._extract_product_name(page)
            reviews = await self._extract_reviews(page)
            self.logger.info("리뷰 수집 완료: %s - %d건", product_name, len(reviews))

            for idx, item in enumerate(reviews):
                yield {
                    "product_url": response.url,
                    "product_name": product_name,
                    "review_index": idx,
                    "review_text": item.get("text"),
                    "rating": item.get("rating"),
                    "date": item.get("date"),
                }
        finally:
            await page.close()

    async def _open_review_tab(self, page) -> bool:
        """페이지를 스크롤하며 리뷰 탭을 찾아 클릭한다. 성공 여부를 반환."""
        selectors = [
            "a[data-tiara-layer='review']",
            "[data-tiara-action-name='리뷰 탭 클릭']",
            "a.link_tab:has-text('리뷰')",
            "text=리뷰",
            "a:has-text('리뷰')",
            "button:has-text('리뷰')",
        ]

        viewport_height = await page.evaluate("window.innerHeight")
        step = max(int(viewport_height * 0.6), 300)

        for _ in range(self.MAX_SCROLL_TRIES):
            for sel in selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.scroll_into_view_if_needed()
                        await el.click()
                        await page.wait_for_timeout(2000)
                        return True
                except Exception:
                    continue
            await page.evaluate(f"window.scrollBy(0, {step})")
            await page.wait_for_timeout(800)
        return False

    async def _expand_all_reviews(self, page) -> None:
        """'더보기' 버튼이 사라질 때까지 반복 클릭한다."""
        more_btn_selector = "a[data-tiara-layer='btn_review_list_more']"

        for click_count in range(1, self.MAX_MORE_CLICKS + 1):
            more_btn = await page.query_selector(more_btn_selector)
            if not more_btn:
                break
            try:
                await more_btn.scroll_into_view_if_needed()
                await more_btn.click()
                await page.wait_for_timeout(1000)
            except Exception as e:
                self.logger.warning("'더보기' 클릭 중단 (%d회차): %s", click_count, e)
                break

    @staticmethod
    async def _extract_product_name(page):
        return await page.evaluate(
            """
            () => {
                const nameEl = document.querySelector('.txt_name');
                if (!nameEl) return null;
                const clone = nameEl.cloneNode(true);
                const screenOut = clone.querySelector('.screen_out');
                if (screenOut) screenOut.remove();
                return clone.textContent.trim();
            }
            """
        )

    @staticmethod
    async def _extract_reviews(page):
        return await page.evaluate(
            """
            () => {
                const items = document.querySelectorAll('li.box_review, li.item-container.box_review');
                const results = [];
                items.forEach(item => {
                    const textEl = item.querySelector('p.txt_review');
                    if (!textEl) return;
                    const text = textEl.textContent.trim();
                    if (!text) return;

                    let rating = null;
                    const scoreEl = item.querySelector('.area_score em.img_shop');
                    if (scoreEl) {
                        const m = scoreEl.textContent.match(/(\\d+)/);
                        if (m) rating = parseInt(m[1], 10);
                    }

                    let date = null;
                    item.querySelectorAll('.list_reviewinfo li').forEach(li => {
                        const label = li.querySelector('strong.screen_out');
                        if (label && label.textContent.trim() === '작성일') {
                            const span = li.querySelector('span.txt_reviewinfo');
                            if (span) date = span.textContent.trim();
                        }
                    });

                    results.push({ text, rating, date });
                });
                return results;
            }
            """
        )

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error("요청 실패: %s (%s)", failure.request.url, failure.value)


if __name__ == "__main__":
    process = CrawlerProcess(settings={
        **KakaoReviewSpider.custom_settings,
        "FEEDS": {"reviews.jsonl": {"format": "jsonlines", "encoding": "utf8"}},
    })
    process.crawl(KakaoReviewSpider)
    process.start()