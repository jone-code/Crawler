from __future__ import annotations

import re
from typing import Any

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
                let resolved = value;
                if (value.startsWith("//")) resolved = `https:${value}`;
                if (value.startsWith("/")) resolved = `https://www.xiaohongshu.com${value}`;
                try {
                  const parsed = new URL(resolved);
                  parsed.search = "";
                  parsed.hash = "";
                  return parsed.toString();
                } catch {
                  return resolved;
                }
              };
              const normalizeDetailUrl = (value) => {
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

                const cardRoot = link.parentElement;
                const cardText = (cardRoot?.innerText || "").trim();
                const textLines = cardText
                  .split("\\n")
                  .map(item => item.trim())
                  .filter(Boolean);
                const titleNode = cardRoot?.querySelector("img[alt], [class*='title'], [class*='desc']");
                const profileLink = cardRoot?.querySelector("a.cover[href*='/user/profile/'], a[href*='/user/profile/']") || null;
                const detailHref = normalizeDetailUrl(profileLink?.getAttribute("href") || null);
                const title =
                  titleNode?.getAttribute?.("alt") ||
                  link.getAttribute("title") ||
                  link.getAttribute("aria-label") ||
                  textLines[0] ||
                  null;
                const likeText = textLines.length >= 3 ? textLines[textLines.length - 1] : null;
                const cover = normalizeUrl(
                  cardRoot?.querySelector("img")?.getAttribute("src") ||
                  link.querySelector("img")?.getAttribute("src") ||
                  null
                );

                unique.set(href, {
                  post_url: href,
                  title: title || null,
                  description: title || null,
                  cover_url: cover,
                  detail_url: detailHref,
                  card_text_lines: textLines,
                  like_text: likeText,
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
                like_count=self._parse_count(card.get("like_text")),
                raw=card,
            )
            for card in cards
            if card.get("post_url")
        ]

        await self._enrich_posts_with_details(page, posts)

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
        page_title = (await page.title()).strip()
        if page_title and " - " in page_title:
            primary = page_title.split(" - ")[0].strip()
            if primary and primary not in {"小红书"}:
                return primary

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

    async def _enrich_posts_with_details(self, page: Page, posts: list[CreatorPost]) -> None:
        for post in posts:
            raw_detail_url = post.raw.get("detail_url")
            detail_url = (
                raw_detail_url
                if isinstance(raw_detail_url, str) and raw_detail_url.strip()
                else post.post_url
            )
            if not detail_url:
                continue

            try:
                await page.goto(detail_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                await page.wait_for_timeout(1400)
                details = await page.evaluate(
                    """
                    () => {
                      const pick = (selector, attr = "content") =>
                        document.querySelector(selector)?.getAttribute(attr) || null;
                      const pageTitle = document.title || null;
                      return {
                        title:
                          pick("meta[property='og:title']") ||
                          pick("meta[name='title']") ||
                          null,
                        description:
                          pick("meta[property='og:description']") ||
                          pick("meta[name='description']") ||
                          null,
                        cover_url:
                          pick("meta[property='og:image']") ||
                          pick("meta[name='og:image']") ||
                          null,
                        publish_time:
                          pick("meta[property='article:published_time']") ||
                          pick("meta[name='article:published_time']") ||
                          null,
                        page_title: pageTitle,
                      };
                    }
                    """
                )
            except Exception as exc:  # noqa: BLE001
                post.raw["detail_error"] = str(exc)
                continue

            title = self._clean_title(details.get("title"))
            description = self._clean_description(details.get("description"))
            cover_url = self._normalize_cover_url(details.get("cover_url"))
            publish_time = details.get("publish_time")
            page_title = self._clean_title(details.get("page_title"))

            if title:
                post.title = title
            elif page_title:
                post.title = page_title

            if description and description != "3 亿人的生活经验，都在小红书":
                post.description = description

            if cover_url:
                post.cover_url = cover_url

            if isinstance(publish_time, str) and publish_time.strip():
                post.publish_time = publish_time.strip()

            post.raw["detail_url"] = detail_url
            post.raw["detail_meta"] = details

    def _parse_count(self, value: Any) -> int | None:
        if not isinstance(value, str):
            return None
        raw = value.strip().lower().replace(",", "")
        if not raw:
            return None
        if raw.endswith("w"):
            try:
                return int(float(raw[:-1]) * 10000)
            except ValueError:
                return None
        if raw.endswith("万"):
            try:
                return int(float(raw[:-1]) * 10000)
            except ValueError:
                return None
        digits = re.findall(r"[0-9]+", raw)
        if not digits:
            return None
        try:
            return int("".join(digits))
        except ValueError:
            return None

    def _clean_title(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.endswith(" - 小红书"):
            text = text[: -len(" - 小红书")].strip()
        if text in {"小红书", "小红书 - 你的生活兴趣社区"}:
            return None
        return text or None

    def _clean_description(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        return text or None

    def _normalize_cover_url(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.startswith("//"):
            return f"https:{text}"
        return text
