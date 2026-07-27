from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .models import CreatorContent


class BaseCrawler(ABC):
    platform: str = ""

    def __init__(
        self,
        *,
        headless: bool = True,
        cookies_path: str | None = None,
        timeout_ms: int = 30000,
    ) -> None:
        self.headless = headless
        self.cookies_path = cookies_path
        self.timeout_ms = timeout_ms

    async def crawl(self, creator_url: str, max_items: int = 20) -> CreatorContent:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await self._new_context(browser, creator_url)
            page = await context.new_page()

            await self._open_creator_page(page, creator_url)
            result = await self._crawl_page(page, creator_url, max_items=max_items)

            await context.close()
            await browser.close()
            return result

    async def _new_context(self, browser: Browser, creator_url: str) -> BrowserContext:
        domain = urlparse(creator_url).netloc
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
            viewport={"width": 1440, "height": 900},
        )
        if self.cookies_path:
            await self._load_cookies(context, domain)
        return context

    async def _load_cookies(self, context: BrowserContext, domain: str) -> None:
        cookies_file = Path(self.cookies_path)  # type: ignore[arg-type]
        if not cookies_file.exists():
            raise FileNotFoundError(f"Cookie file not found: {cookies_file}")

        cookies_payload = json.loads(cookies_file.read_text(encoding="utf-8"))
        if not isinstance(cookies_payload, list):
            raise ValueError("Cookie file must be a JSON array")

        normalized_cookies: list[dict[str, Any]] = []
        for item in cookies_payload:
            if not isinstance(item, dict):
                continue
            cookie = dict(item)
            cookie.setdefault("domain", f".{domain}")
            cookie.setdefault("path", "/")
            normalized_cookies.append(cookie)

        if normalized_cookies:
            await context.add_cookies(normalized_cookies)

    async def _open_creator_page(self, page: Page, creator_url: str) -> None:
        await page.goto(creator_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
        await page.wait_for_timeout(2500)

    @abstractmethod
    async def _crawl_page(self, page: Page, creator_url: str, max_items: int) -> CreatorContent:
        raise NotImplementedError
