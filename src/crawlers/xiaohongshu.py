from __future__ import annotations

import re

from playwright.async_api import Page

from .base import BaseCrawler
from .models import CreatorContent, CreatorPost


class XiaohongshuCrawler(BaseCrawler):
    platform = "xiaohongshu"

    async def _crawl_page(self, page: Page, creator_url: str, max_items: int) -> CreatorContent:
        await self._auto_scroll(page, max_items=max_items)
        creator_name = await self._extract_creator_name(page)
        cards = await page.evaluate(
            """
            (maxItems) => {
              const normalizeUrl = (value) => {
                if (!value) return null;
                if (value.startsWith("//")) return `https:${value}`;
                if (value.startsWith("/")) return `https://www.xiaohongshu.com${value}`;
                return value;
              };

              const unique = new Map();
              const links = Array.from(document.querySelectorAll('a[href*="/explore/"]'));
              for (const link of links) {
                const href = normalizeUrl(link.getAttribute("href"));
                if (!href) continue;
                if (unique.has(href)) continue;

                const titleNode = link.querySelector("img[alt], [class*='title'], [class*='desc']");
                const text = (link.innerText || "").trim();
                const title = titleNode?.getAttribute?.("alt") || text.split("\\n")[0] || null;
                const cover = normalizeUrl(link.querySelector("img")?.getAttribute("src") || null);

                unique.set(href, {
                  post_url: href,
                  title: title || null,
                  description: text || null,
                  cover_url: cover,
                });
                if (unique.size >= maxItems) break;
              }
              return Array.from(unique.values());
            }
            """,
            max_items,
        )

        posts = [
            CreatorPost(
                platform=self.platform,
                creator_url=creator_url,
                post_id=self._extract_post_id(card.get("post_url", "")),
                post_url=card.get("post_url", ""),
                title=card.get("title"),
                description=card.get("description"),
                cover_url=card.get("cover_url"),
                raw=card,
            )
            for card in cards
            if card.get("post_url")
        ]

        return CreatorContent.create(
            platform=self.platform,
            creator_url=creator_url,
            creator_name=creator_name,
            posts=posts,
        )

    async def _auto_scroll(self, page: Page, *, max_items: int) -> None:
        previous_height = -1
        for _ in range(8):
            count = await page.evaluate(
                """() => document.querySelectorAll('a[href*="/explore/"]').length"""
            )
            if count >= max_items:
                break
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1800)
            current_height = await page.evaluate("document.body.scrollHeight")
            if current_height == previous_height:
                break
            previous_height = current_height

    async def _extract_creator_name(self, page: Page) -> str | None:
        selectors = [
            "meta[property='og:title']",
            "h1",
            "[class*='user-name']",
            "[class*='nickname']",
        ]
        for selector in selectors:
            element = page.locator(selector).first
            if await element.count():
                if selector.startswith("meta"):
                    value = await element.get_attribute("content")
                else:
                    value = await element.text_content()
                text = (value or "").strip()
                if text:
                    return text
        return None

    def _extract_post_id(self, post_url: str) -> str:
        match = re.search(r"/explore/([a-zA-Z0-9]+)", post_url)
        return match.group(1) if match else post_url
