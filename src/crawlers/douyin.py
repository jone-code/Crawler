from __future__ import annotations

import re

from playwright.async_api import Page

from .base import BaseCrawler
from .models import CreatorContent, CreatorPost


class DouyinCrawler(BaseCrawler):
    platform = "douyin"

    async def _crawl_page(self, page: Page, creator_url: str, max_items: int) -> CreatorContent:
        await self._auto_scroll(page, max_items=max_items)
        creator_name = await self._extract_creator_name(page)
        cards = await page.evaluate(
            """
            (maxItems) => {
              const normalizeUrl = (value) => {
                if (!value) return null;
                let resolved = value;
                if (value.startsWith("//")) resolved = `https:${value}`;
                if (value.startsWith("/")) resolved = `https://www.douyin.com${value}`;
                try {
                  const parsed = new URL(resolved);
                  parsed.search = "";
                  parsed.hash = "";
                  return parsed.toString();
                } catch {
                  return resolved;
                }
              };

              const unique = new Map();
              const links = Array.from(document.querySelectorAll('a[href*="/video/"], a[href*="/note/"]'));
              for (const link of links) {
                const href = normalizeUrl(link.getAttribute("href"));
                if (!href || unique.has(href)) continue;

                const text = (link.innerText || "").trim();
                const cover = normalizeUrl(link.querySelector("img")?.getAttribute("src") || null);
                const title = text.split("\\n")[0] || null;

                unique.set(href, {
                  post_url: href,
                  title,
                  description: text || null,
                  cover_url: cover,
                });
                if (unique.size >= maxItems) break;
              }

              if (unique.size === 0) {
                const html = document.documentElement.innerHTML;
                const matches = html.match(/https?:\\/\\/www\\.douyin\\.com\\/(?:video|note)\\/[0-9]+/g) || [];
                for (const hit of matches) {
                  const href = normalizeUrl(hit);
                  if (!href || unique.has(href)) continue;
                  unique.set(href, {
                    post_url: href,
                    title: null,
                    description: null,
                    cover_url: null,
                  });
                  if (unique.size >= maxItems) break;
                }
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
        for _ in range(10):
            count = await page.evaluate(
                """() => document.querySelectorAll('a[href*="/video/"], a[href*="/note/"]').length"""
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
        page_title = (await page.title()).strip()
        if page_title:
            candidates = page_title.split(" - ")
            if candidates:
                primary = candidates[0].strip()
                if primary and primary not in {"的抖音", "抖音"}:
                    return primary

        selectors = [
            "meta[property='og:title']",
            "h1",
            "[data-e2e='user-info-name']",
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
        match = re.search(r"/(?:video|note)/([0-9]+)", post_url)
        return match.group(1) if match else post_url
