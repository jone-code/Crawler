from __future__ import annotations

import asyncio
from typing import Literal

from .crawlers import DouyinCrawler, XiaohongshuCrawler
from .media_downloader import download_post_media
from .storage import (
    get_crawl_checkpoint,
    list_creators,
    mark_creator_crawled,
    register_creator,
    save_creator_content,
    upsert_crawl_checkpoint,
)

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


def build_creator_url(platform: Platform, creator_id: str) -> str:
    if not creator_id.strip():
        raise ValueError("creator_id cannot be empty")
    if platform == "xiaohongshu":
        return f"https://www.xiaohongshu.com/user/profile/{creator_id}"
    if platform == "douyin":
        return f"https://www.douyin.com/user/{creator_id}"
    raise ValueError(f"Unsupported platform: {platform}")


async def crawl_creator(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    use_checkpoint: bool = True,
) -> dict:
    checkpoint: dict | None = None
    if platform == "xiaohongshu" and use_checkpoint and max_items == 0:
        checkpoint = get_crawl_checkpoint(platform=platform, creator_url=creator_url)
    crawler = create_crawler(
        platform,
        headless=headless,
        cookies_path=cookies_path,
    )
    result = await crawler.crawl(creator_url, max_items=max_items, checkpoint=checkpoint)
    return result.to_dict()


def crawl_creator_sync(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    use_checkpoint: bool = True,
) -> dict:
    return asyncio.run(
        crawl_creator(
            platform=platform,
            creator_url=creator_url,
            max_items=max_items,
            headless=headless,
            cookies_path=cookies_path,
            use_checkpoint=use_checkpoint,
        )
    )


def add_creator_id(
    *,
    platform: Platform,
    creator_id: str,
    db_path: str = "data/crawler.db",
    creator_url: str | None = None,
    enabled: bool = True,
    metadata: dict | None = None,
) -> dict:
    resolved_url = creator_url or build_creator_url(platform, creator_id)
    return register_creator(
        platform=platform,
        creator_id=creator_id,
        creator_url=resolved_url,
        db_path=db_path,
        enabled=enabled,
        metadata=metadata,
    )


def list_creator_ids(
    *,
    db_path: str = "data/crawler.db",
    platform: Platform | None = None,
    enabled_only: bool = False,
) -> list[dict]:
    return list_creators(
        db_path=db_path,
        platform=platform,
        enabled_only=enabled_only,
    )


async def crawl_creator_by_id(
    *,
    platform: Platform,
    creator_id: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    use_checkpoint: bool = True,
) -> dict:
    creator_url = build_creator_url(platform, creator_id)
    return await crawl_creator(
        platform=platform,
        creator_url=creator_url,
        max_items=max_items,
        headless=headless,
        cookies_path=cookies_path,
        use_checkpoint=use_checkpoint,
    )


def crawl_creator_by_id_sync(
    *,
    platform: Platform,
    creator_id: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    use_checkpoint: bool = True,
) -> dict:
    return asyncio.run(
        crawl_creator_by_id(
            platform=platform,
            creator_id=creator_id,
            max_items=max_items,
            headless=headless,
            cookies_path=cookies_path,
            use_checkpoint=use_checkpoint,
        )
    )


async def crawl_creator_and_store(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    db_path: str = "data/crawler.db",
    download_media: bool = False,
    media_root: str = "data/media",
    use_checkpoint: bool = True,
) -> dict:
    payload = await crawl_creator(
        platform=platform,
        creator_url=creator_url,
        max_items=max_items,
        headless=headless,
        cookies_path=cookies_path,
        use_checkpoint=use_checkpoint,
    )
    media_result: dict | None = None
    if download_media:
        media_result = download_post_media(payload, media_root=media_root)
    storage = save_creator_content(payload, db_path=db_path)
    _update_checkpoint_after_run(
        platform=platform,
        creator_url=creator_url,
        payload=payload,
        db_path=db_path,
        run_id=storage.get("run_id"),
    )
    return {"crawl": payload, "storage": storage, "media": media_result}


def crawl_creator_and_store_sync(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    db_path: str = "data/crawler.db",
    download_media: bool = False,
    media_root: str = "data/media",
    use_checkpoint: bool = True,
) -> dict:
    return asyncio.run(
        crawl_creator_and_store(
            platform=platform,
            creator_url=creator_url,
            max_items=max_items,
            headless=headless,
            cookies_path=cookies_path,
            db_path=db_path,
            download_media=download_media,
            media_root=media_root,
            use_checkpoint=use_checkpoint,
        )
    )


async def crawl_creator_by_id_and_store(
    *,
    platform: Platform,
    creator_id: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    db_path: str = "data/crawler.db",
    download_media: bool = False,
    media_root: str = "data/media",
    use_checkpoint: bool = True,
) -> dict:
    creator_url = build_creator_url(platform, creator_id)
    # Ensure backend creator registry has this id for later scheduling/management.
    add_creator_id(
        platform=platform,
        creator_id=creator_id,
        creator_url=creator_url,
        db_path=db_path,
    )
    result = await crawl_creator_and_store(
        platform=platform,
        creator_url=creator_url,
        max_items=max_items,
        headless=headless,
        cookies_path=cookies_path,
        db_path=db_path,
        download_media=download_media,
        media_root=media_root,
        use_checkpoint=use_checkpoint,
    )
    crawl_time_utc = result["crawl"].get("crawl_time_utc")
    if isinstance(crawl_time_utc, str) and crawl_time_utc:
        mark_creator_crawled(
            platform=platform,
            creator_id=creator_id,
            crawl_time_utc=crawl_time_utc,
            db_path=db_path,
        )
    return result


def crawl_creator_by_id_and_store_sync(
    *,
    platform: Platform,
    creator_id: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    db_path: str = "data/crawler.db",
    download_media: bool = False,
    media_root: str = "data/media",
    use_checkpoint: bool = True,
) -> dict:
    return asyncio.run(
        crawl_creator_by_id_and_store(
            platform=platform,
            creator_id=creator_id,
            max_items=max_items,
            headless=headless,
            cookies_path=cookies_path,
            db_path=db_path,
            download_media=download_media,
            media_root=media_root,
            use_checkpoint=use_checkpoint,
        )
    )


def _update_checkpoint_after_run(
    *,
    platform: Platform,
    creator_url: str,
    payload: dict,
    db_path: str,
    run_id: int | None,
) -> None:
    posts = payload.get("posts")
    if not isinstance(posts, list):
        return
    post_urls: list[str] = []
    for post in posts:
        if not isinstance(post, dict):
            continue
        url = post.get("post_url")
        if isinstance(url, str) and url.strip():
            post_urls.append(url.strip())
    if not post_urls:
        return
    checkpoint_payload = {
        "known_recent_post_urls": post_urls[:20],
        "last_seen_post_url": post_urls[0],
        "crawl_time_utc": payload.get("crawl_time_utc"),
    }
    upsert_crawl_checkpoint(
        platform=platform,
        creator_url=creator_url,
        checkpoint=checkpoint_payload,
        db_path=db_path,
        run_id=run_id if isinstance(run_id, int) else None,
    )
