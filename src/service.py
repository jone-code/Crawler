from __future__ import annotations

import asyncio
from typing import Literal

from .crawlers import DouyinCrawler, XiaohongshuCrawler
from .media_downloader import download_post_media
from .storage import (
    get_crawl_checkpoint,
    list_crawl_accounts,
    mark_crawl_account_result,
    list_creators,
    mark_creator_crawled,
    register_crawl_account,
    register_creator,
    save_creator_content,
    set_crawl_account_enabled,
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
    checkpoint_db_path: str = "data/crawler.db",
    use_account_pool: bool = False,
    account_pool_db_path: str = "data/crawler.db",
) -> dict:
    if use_account_pool and not cookies_path:
        return await _crawl_with_account_pool(
            platform=platform,
            creator_url=creator_url,
            max_items=max_items,
            headless=headless,
            use_checkpoint=use_checkpoint,
            checkpoint_db_path=checkpoint_db_path,
            account_pool_db_path=account_pool_db_path,
        )

    payload = await _crawl_once(
        platform=platform,
        creator_url=creator_url,
        max_items=max_items,
        headless=headless,
        cookies_path=cookies_path,
        use_checkpoint=use_checkpoint,
        checkpoint_db_path=checkpoint_db_path,
    )
    return payload


async def _crawl_once(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int,
    headless: bool,
    cookies_path: str | None,
    use_checkpoint: bool,
    checkpoint_db_path: str,
) -> dict:
    checkpoint: dict | None = None
    if platform == "xiaohongshu" and use_checkpoint and max_items == 0:
        checkpoint = get_crawl_checkpoint(
            platform=platform,
            creator_url=creator_url,
            db_path=checkpoint_db_path,
        )
    crawler = create_crawler(
        platform,
        headless=headless,
        cookies_path=cookies_path,
    )
    result = await crawler.crawl(creator_url, max_items=max_items, checkpoint=checkpoint)
    return result.to_dict()


async def _crawl_with_account_pool(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int,
    headless: bool,
    use_checkpoint: bool,
    checkpoint_db_path: str,
    account_pool_db_path: str,
) -> dict:
    accounts = list_crawl_accounts(
        platform=platform,
        enabled_only=True,
        db_path=account_pool_db_path,
    )
    if not accounts:
        raise RuntimeError(f"no enabled crawl accounts for platform={platform}")

    errors: list[str] = []
    for account in accounts:
        account_id = account["id"]
        account_name = account["account_name"]
        cookies_path = account["cookies_path"]
        try:
            payload = await _crawl_once(
                platform=platform,
                creator_url=creator_url,
                max_items=max_items,
                headless=headless,
                cookies_path=cookies_path,
                use_checkpoint=use_checkpoint,
                checkpoint_db_path=checkpoint_db_path,
            )
        except Exception as exc:  # noqa: BLE001
            mark_crawl_account_result(
                account_id=account_id,
                success=False,
                health="error",
                error=str(exc),
                db_path=account_pool_db_path,
            )
            errors.append(f"account={account_name} error={exc}")
            continue

        success, health, error = _evaluate_crawl_health(platform=platform, payload=payload)
        mark_crawl_account_result(
            account_id=account_id,
            success=success,
            health=health,
            error=error,
            db_path=account_pool_db_path,
        )
        payload.setdefault("crawler_meta", {})
        payload["crawler_meta"]["account"] = {
            "id": account_id,
            "account_name": account_name,
            "cookies_path": cookies_path,
            "health": health,
            "success": success,
            "error": error,
        }
        if success:
            return payload
        errors.append(f"account={account_name} health={health} error={error}")

    raise RuntimeError("all crawl accounts failed: " + " | ".join(errors))


def _evaluate_crawl_health(*, platform: Platform, payload: dict) -> tuple[bool, str, str | None]:
    crawler_meta = payload.get("crawler_meta", {})
    session = crawler_meta.get("session", {}) if isinstance(crawler_meta, dict) else {}
    session_status = session.get("runtime_status")
    cookie_health = session.get("cookie_health")
    warnings = session.get("warnings")
    posts = payload.get("posts", [])
    post_count = len(posts) if isinstance(posts, list) else 0

    if cookie_health == "expired":
        return False, "expired", "cookie expired"
    if session_status == "limited":
        return False, "limited", "session limited"
    if platform == "xiaohongshu" and post_count == 0:
        warn_text = ", ".join(str(x) for x in warnings) if isinstance(warnings, list) else "no posts"
        return False, "no_data", warn_text
    return True, str(session_status or cookie_health or "ok"), None


def crawl_creator_sync(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    use_checkpoint: bool = True,
    checkpoint_db_path: str = "data/crawler.db",
    use_account_pool: bool = False,
    account_pool_db_path: str = "data/crawler.db",
) -> dict:
    return asyncio.run(
        crawl_creator(
            platform=platform,
            creator_url=creator_url,
            max_items=max_items,
            headless=headless,
            cookies_path=cookies_path,
            use_checkpoint=use_checkpoint,
            checkpoint_db_path=checkpoint_db_path,
            use_account_pool=use_account_pool,
            account_pool_db_path=account_pool_db_path,
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


def add_crawl_account(
    *,
    platform: Platform,
    account_name: str,
    cookies_path: str,
    db_path: str = "data/crawler.db",
    enabled: bool = True,
    priority: int = 100,
    metadata: dict | None = None,
) -> dict:
    return register_crawl_account(
        platform=platform,
        account_name=account_name,
        cookies_path=cookies_path,
        db_path=db_path,
        enabled=enabled,
        priority=priority,
        metadata=metadata,
    )


def list_crawl_account_pool(
    *,
    db_path: str = "data/crawler.db",
    platform: Platform | None = None,
    enabled_only: bool = False,
) -> list[dict]:
    return list_crawl_accounts(
        db_path=db_path,
        platform=platform,
        enabled_only=enabled_only,
    )


def toggle_crawl_account(
    *,
    account_id: int,
    enabled: bool,
    db_path: str = "data/crawler.db",
) -> None:
    set_crawl_account_enabled(account_id=account_id, enabled=enabled, db_path=db_path)


async def crawl_creator_by_id(
    *,
    platform: Platform,
    creator_id: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    use_checkpoint: bool = True,
    checkpoint_db_path: str = "data/crawler.db",
    use_account_pool: bool = False,
    account_pool_db_path: str = "data/crawler.db",
) -> dict:
    creator_url = build_creator_url(platform, creator_id)
    return await crawl_creator(
        platform=platform,
        creator_url=creator_url,
        max_items=max_items,
        headless=headless,
        cookies_path=cookies_path,
        use_checkpoint=use_checkpoint,
        checkpoint_db_path=checkpoint_db_path,
        use_account_pool=use_account_pool,
        account_pool_db_path=account_pool_db_path,
    )


def crawl_creator_by_id_sync(
    *,
    platform: Platform,
    creator_id: str,
    max_items: int = 20,
    headless: bool = True,
    cookies_path: str | None = None,
    use_checkpoint: bool = True,
    checkpoint_db_path: str = "data/crawler.db",
    use_account_pool: bool = False,
    account_pool_db_path: str = "data/crawler.db",
) -> dict:
    return asyncio.run(
        crawl_creator_by_id(
            platform=platform,
            creator_id=creator_id,
            max_items=max_items,
            headless=headless,
            cookies_path=cookies_path,
            use_checkpoint=use_checkpoint,
            checkpoint_db_path=checkpoint_db_path,
            use_account_pool=use_account_pool,
            account_pool_db_path=account_pool_db_path,
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
    use_account_pool: bool = False,
) -> dict:
    payload = await crawl_creator(
        platform=platform,
        creator_url=creator_url,
        max_items=max_items,
        headless=headless,
        cookies_path=cookies_path,
        use_checkpoint=use_checkpoint,
        checkpoint_db_path=db_path,
        use_account_pool=use_account_pool,
        account_pool_db_path=db_path,
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
    use_account_pool: bool = False,
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
            use_account_pool=use_account_pool,
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
    use_account_pool: bool = False,
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
        use_account_pool=use_account_pool,
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
    use_account_pool: bool = False,
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
            use_account_pool=use_account_pool,
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
        "known_recent_post_urls": post_urls[:200],
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
