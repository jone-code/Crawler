from __future__ import annotations

import asyncio
from typing import Literal

from .crawlers import DouyinCrawler, XiaohongshuCrawler

Platform = Literal["xiaohongshu", "douyin"]


def create_crawler(
    platform: Platform,
    *,
    headless: bool = True,
    cookies_path: str | None = None,
):
    if platform == "xiaohongshu":
        return XiaohongshuCrawler(headless=headless, cookies_path=cookies_path)
    if platform == "douyin":
        return DouyinCrawler(headless=headless, cookies_path=cookies_path)
    raise ValueError(f"Unsupported platform: {platform}")


async def crawl_creator(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
) -> dict:
    crawler = create_crawler(
        platform,
        headless=headless,
        cookies_path=cookies_path,
    )
    result = await crawler.crawl(creator_url, max_items=max_items)
    return result.to_dict()


def crawl_creator_sync(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
) -> dict:
    return asyncio.run(
        crawl_creator(
            platform=platform,
            creator_url=creator_url,
            max_items=max_items,
            headless=headless,
            cookies_path=cookies_path,
        )
    )
