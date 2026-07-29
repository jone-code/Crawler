from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timezone
from typing import Any, Literal

from playwright.async_api import async_playwright

from .crawlers import DouyinCrawler, XiaohongshuCrawler
from .media_downloader import download_post_media
from .storage import (
    add_pool_health_event,
    get_crawl_checkpoint,
    list_crawl_accounts,
    list_crawl_proxies,
    list_pool_health_events,
    list_pool_health_trends,
    mark_crawl_account_result,
    mark_crawl_proxy_result,
    list_creators,
    mark_creator_crawled,
    register_crawl_account,
    register_crawl_proxy,
    register_creator,
    save_creator_content,
    set_crawl_account_enabled,
    set_crawl_proxy_enabled,
    upsert_crawl_checkpoint,
)

Platform = Literal["xiaohongshu", "douyin"]


def create_crawler(
    platform: Platform,
    *,
    headless: bool = True,
    cookies_path: str | None = None,
    proxy_server: str | None = None,
):
    if platform == "xiaohongshu":
        return XiaohongshuCrawler(
            headless=headless,
            cookies_path=cookies_path,
            proxy_server=proxy_server,
        )
    if platform == "douyin":
        return DouyinCrawler(
            headless=headless,
            cookies_path=cookies_path,
            proxy_server=proxy_server,
        )
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
    use_proxy_pool: bool = False,
    proxy_pool_db_path: str = "data/crawler.db",
) -> dict:
    if use_account_pool or use_proxy_pool:
        return await _crawl_with_pool_rotation(
            platform=platform,
            creator_url=creator_url,
            max_items=max_items,
            headless=headless,
            cookies_path=cookies_path,
            use_checkpoint=use_checkpoint,
            checkpoint_db_path=checkpoint_db_path,
            use_account_pool=use_account_pool,
            account_pool_db_path=account_pool_db_path,
            use_proxy_pool=use_proxy_pool,
            proxy_pool_db_path=proxy_pool_db_path,
        )

    payload = await _crawl_once(
        platform=platform,
        creator_url=creator_url,
        max_items=max_items,
        headless=headless,
        cookies_path=cookies_path,
        proxy_server=None,
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
    proxy_server: str | None,
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
        proxy_server=proxy_server,
    )
    result = await crawler.crawl(creator_url, max_items=max_items, checkpoint=checkpoint)
    return result.to_dict()


async def _crawl_with_pool_rotation(
    *,
    platform: Platform,
    creator_url: str,
    max_items: int,
    headless: bool,
    cookies_path: str | None,
    use_checkpoint: bool,
    checkpoint_db_path: str,
    use_account_pool: bool,
    account_pool_db_path: str,
    use_proxy_pool: bool,
    proxy_pool_db_path: str,
) -> dict:
    now = datetime.now(timezone.utc)
    scheduler_meta: dict[str, Any] = {
        "strategy": "weighted_score_with_cooldown",
        "skipped_cooldown": {"accounts": [], "proxies": []},
    }
    if use_account_pool:
        accounts = list_crawl_accounts(
            platform=platform,
            enabled_only=True,
            db_path=account_pool_db_path,
        )
        if not accounts:
            raise RuntimeError(f"no enabled crawl accounts for platform={platform}")
        account_candidates = _build_account_candidates(accounts, now, scheduler_meta)
        if not account_candidates:
            raise RuntimeError("all enabled crawl accounts are in cooldown")
    else:
        account_candidates = [_build_direct_account_candidate(cookies_path)]

    if use_proxy_pool:
        proxies = list_crawl_proxies(
            platform=platform,
            enabled_only=True,
            db_path=proxy_pool_db_path,
        )
        if not proxies:
            raise RuntimeError(f"no enabled crawl proxies for platform={platform}")
        proxy_candidates = _build_proxy_candidates(proxies, now, scheduler_meta)
        if not proxy_candidates:
            raise RuntimeError("all enabled crawl proxies are in cooldown")
    else:
        proxy_candidates = [_build_direct_proxy_candidate()]

    attempt_plan = _build_attempt_plan(account_candidates, proxy_candidates)
    scheduler_meta["attempt_plan_size"] = len(attempt_plan)

    errors: list[str] = []
    for attempt_index, candidate in enumerate(attempt_plan, start=1):
        account = candidate["account"]
        proxy = candidate["proxy"]
        account_id = account.get("id")
        proxy_id = proxy.get("id")
        started_at = time.monotonic()
        payload: dict[str, Any] = {}
        crawl_exc: Exception | None = None

        try:
            payload = await _crawl_once(
                platform=platform,
                creator_url=creator_url,
                max_items=max_items,
                headless=headless,
                cookies_path=account.get("cookies_path"),
                proxy_server=proxy.get("proxy_server"),
                use_checkpoint=use_checkpoint,
                checkpoint_db_path=checkpoint_db_path,
            )
            success, health, error = _evaluate_crawl_health(platform=platform, payload=payload)
        except Exception as exc:  # noqa: BLE001
            success = False
            health = "error"
            error = str(exc)
            crawl_exc = exc
        latency_ms = int((time.monotonic() - started_at) * 1000)
        failure_kind = (
            None
            if success
            else _classify_failure_kind(
                health=health,
                error=error,
                exc=crawl_exc,
            )
        )
        account_cd, proxy_cd = _cooldown_for_failure_kind(failure_kind)

        if isinstance(account_id, int):
            mark_crawl_account_result(
                account_id=account_id,
                success=success,
                health=health,
                error=error,
                failure_kind=failure_kind,
                cooldown_seconds=account_cd,
                latency_ms=latency_ms,
                db_path=account_pool_db_path,
            )
        if isinstance(proxy_id, int):
            mark_crawl_proxy_result(
                proxy_id=proxy_id,
                success=success,
                health=health,
                error=error,
                failure_kind=failure_kind,
                cooldown_seconds=proxy_cd,
                latency_ms=latency_ms,
                db_path=proxy_pool_db_path,
            )

        if success:
            payload.setdefault("crawler_meta", {})
            payload["crawler_meta"]["account"] = {
                "id": account_id,
                "account_name": account.get("account_name"),
                "cookies_path": account.get("cookies_path"),
                "health": health,
                "success": success,
                "error": error,
                "score": account.get("score"),
            }
            payload["crawler_meta"]["proxy"] = {
                "id": proxy_id,
                "proxy_name": proxy.get("proxy_name"),
                "proxy_server": proxy.get("proxy_server"),
                "health": health,
                "success": success,
                "error": error,
                "latency_ms": latency_ms,
                "score": proxy.get("score"),
            }
            scheduler_meta["attempt_index"] = attempt_index
            scheduler_meta["attempts_total"] = attempt_index
            payload["crawler_meta"]["scheduler"] = scheduler_meta
            return payload

        errors.append(
            f"attempt={attempt_index} account={account.get('account_name')} "
            f"proxy={proxy.get('proxy_name')} kind={failure_kind} health={health} "
            f"latency_ms={latency_ms} error={error}"
        )

    raise RuntimeError("all pool candidates failed: " + " | ".join(errors))


def _build_account_candidates(
    accounts: list[dict[str, Any]],
    now: datetime,
    scheduler_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in accounts:
        cooldown_until = _parse_utc(item.get("cooldown_until_utc"))
        if cooldown_until and cooldown_until > now:
            scheduler_meta["skipped_cooldown"]["accounts"].append(
                {
                    "id": item.get("id"),
                    "account_name": item.get("account_name"),
                    "cooldown_until_utc": item.get("cooldown_until_utc"),
                }
            )
            continue
        output.append(
            {
                "id": item.get("id"),
                "account_name": item.get("account_name"),
                "cookies_path": item.get("cookies_path"),
                "score": _compute_pool_score(
                    priority=item.get("priority"),
                    success_count=item.get("success_count"),
                    fail_count=item.get("fail_count"),
                    consecutive_failures=item.get("consecutive_failures"),
                    last_health=item.get("last_health"),
                    last_latency_ms=item.get("last_latency_ms"),
                    last_used_at_utc=item.get("last_used_at_utc"),
                ),
            }
        )
    return output


def _build_proxy_candidates(
    proxies: list[dict[str, Any]],
    now: datetime,
    scheduler_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in proxies:
        cooldown_until = _parse_utc(item.get("cooldown_until_utc"))
        if cooldown_until and cooldown_until > now:
            scheduler_meta["skipped_cooldown"]["proxies"].append(
                {
                    "id": item.get("id"),
                    "proxy_name": item.get("proxy_name"),
                    "cooldown_until_utc": item.get("cooldown_until_utc"),
                }
            )
            continue
        output.append(
            {
                "id": item.get("id"),
                "proxy_name": item.get("proxy_name"),
                "proxy_server": item.get("proxy_url"),
                "score": _compute_pool_score(
                    priority=item.get("priority"),
                    success_count=item.get("success_count"),
                    fail_count=item.get("fail_count"),
                    consecutive_failures=item.get("consecutive_failures"),
                    last_health=item.get("last_health"),
                    last_latency_ms=item.get("last_latency_ms"),
                    last_used_at_utc=item.get("last_used_at_utc"),
                ),
            }
        )
    return output


def _build_direct_account_candidate(cookies_path: str | None) -> dict[str, Any]:
    return {
        "id": None,
        "account_name": "direct",
        "cookies_path": cookies_path,
        "score": 100.0,
    }


def _build_direct_proxy_candidate() -> dict[str, Any]:
    return {
        "id": None,
        "proxy_name": "direct",
        "proxy_server": None,
        "score": 100.0,
    }


def _build_attempt_plan(
    account_candidates: list[dict[str, Any]],
    proxy_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    combinations: list[dict[str, Any]] = []
    for account in account_candidates:
        for proxy in proxy_candidates:
            combinations.append(
                {
                    "account": account,
                    "proxy": proxy,
                    "weight": max(1.0, float(account["score"]) + float(proxy["score"])),
                }
            )
    return _weighted_order(combinations)


def _weighted_order(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pool = list(items)
    ordered: list[dict[str, Any]] = []
    while pool:
        weights = [max(1.0, float(item.get("weight") or 1.0)) for item in pool]
        picked_index = random.choices(range(len(pool)), weights=weights, k=1)[0]
        ordered.append(pool.pop(picked_index))
    return ordered


def _compute_pool_score(
    *,
    priority: Any,
    success_count: Any,
    fail_count: Any,
    consecutive_failures: Any,
    last_health: Any,
    last_latency_ms: Any,
    last_used_at_utc: Any,
) -> float:
    p = int(priority or 100)
    succ = max(0, int(success_count or 0))
    fail = max(0, int(fail_count or 0))
    streak = max(0, int(consecutive_failures or 0))
    total = succ + fail
    success_rate = (succ + 1) / (total + 2)
    score = 200.0
    score += max(0.0, 240.0 - p * 1.6)
    score += success_rate * 220.0
    score -= min(240.0, streak * 45.0)
    if isinstance(last_health, str) and last_health in {
        "error",
        "limited",
        "expired",
        "rate_limited",
        "access_limited",
        "no_data",
    }:
        score -= 80.0
    if isinstance(last_latency_ms, int) and last_latency_ms > 0:
        score -= min(140.0, last_latency_ms / 25.0)
    minutes_since_used = _minutes_since(last_used_at_utc)
    score += min(80.0, max(0.0, minutes_since_used * 0.8))
    return max(1.0, round(score, 2))


def _minutes_since(timestamp: Any) -> float:
    dt = _parse_utc(timestamp)
    if dt is None:
        return 120.0
    delta = datetime.now(timezone.utc) - dt
    return max(0.0, delta.total_seconds() / 60.0)


def _parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _classify_failure_kind(
    *,
    health: str,
    error: str | None,
    exc: Exception | None,
) -> str:
    normalized_health = (health or "").strip().lower()
    if normalized_health in {"expired"}:
        return "auth_expired"
    if normalized_health in {"limited"}:
        return "access_limited"
    if normalized_health in {"no_data"}:
        return "no_data"
    text = f"{error or ''} {exc or ''}".lower()
    if any(token in text for token in ["proxy", "tunnel", "407", "err_proxy"]):
        return "proxy_error"
    if any(token in text for token in ["timeout", "timed out", "err_timed_out"]):
        return "network_timeout"
    if any(token in text for token in ["429", "too many requests", "rate limit"]):
        return "rate_limited"
    if any(token in text for token in ["captcha", "验证", "forbidden", "denied", "access"]):
        return "access_limited"
    if "cookie" in text and "expired" in text:
        return "auth_expired"
    return "unknown_error"


def _cooldown_for_failure_kind(failure_kind: str | None) -> tuple[int | None, int | None]:
    if failure_kind is None:
        return None, None
    if failure_kind == "auth_expired":
        return 3600, 0
    if failure_kind == "proxy_error":
        return 120, 900
    if failure_kind == "network_timeout":
        return 120, 300
    if failure_kind in {"rate_limited", "access_limited"}:
        return 900, 900
    if failure_kind == "no_data":
        return 180, 300
    return 180, 300


async def _probe_runtime_entry(
    *,
    platform: Platform,
    cookies_path: str | None,
    proxy_server: str | None,
    timeout_ms: int,
    headless: bool,
) -> dict[str, Any]:
    probe_url = _platform_probe_url(platform)
    crawler = create_crawler(
        platform=platform,
        headless=headless,
        cookies_path=cookies_path,
        proxy_server=proxy_server,
    )
    started_at = time.monotonic()
    context = None
    browser = None
    status_code: int | None = None
    title: str | None = None
    try:
        async with async_playwright() as playwright:
            launch_kwargs: dict[str, Any] = {"headless": headless}
            if proxy_server:
                launch_kwargs["proxy"] = crawler._build_proxy_settings(proxy_server)
            browser = await playwright.chromium.launch(**launch_kwargs)
            context = await crawler._new_context(browser, probe_url)
            page = await context.new_page()
            response = await page.goto(probe_url, wait_until="domcontentloaded", timeout=timeout_ms)
            title = await page.title()
            status_code = response.status if response else None
            session = crawler.crawler_meta.get("session", {})
            cookie_health = session.get("cookie_health") if isinstance(session, dict) else None
            if cookie_health == "expired":
                health = "expired"
                error = "cookie expired"
                success = False
            elif status_code in {401, 403, 429} or _looks_access_limited_title(title):
                health = "limited"
                error = f"probe blocked status={status_code}"
                success = False
            else:
                health = "probe_ok"
                error = None
                success = True
    except Exception as exc:  # noqa: BLE001
        success = False
        health = "error"
        error = str(exc)
        failure_kind = _classify_failure_kind(health=health, error=error, exc=exc)
    else:
        failure_kind = None if success else _classify_failure_kind(health=health, error=error, exc=None)
    finally:
        latency_ms = int((time.monotonic() - started_at) * 1000)
        if context is not None:
            try:
                await context.close()
            except Exception:  # noqa: BLE001
                pass
        if browser is not None:
            try:
                await browser.close()
            except Exception:  # noqa: BLE001
                pass

    return {
        "success": success,
        "health": health,
        "error": error,
        "failure_kind": failure_kind,
        "latency_ms": latency_ms,
        "probe_url": probe_url,
        "status_code": status_code,
        "title": title,
    }


def _platform_probe_url(platform: Platform) -> str:
    if platform == "xiaohongshu":
        return "https://www.xiaohongshu.com"
    if platform == "douyin":
        return "https://www.douyin.com"
    raise ValueError(f"Unsupported platform: {platform}")


def _looks_access_limited_title(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    text = value.lower()
    blocked_keywords = [
        "captcha",
        "验证",
        "访问受限",
        "forbidden",
        "denied",
        "安全验证",
    ]
    return any(keyword in text for keyword in blocked_keywords)


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
    if session_status in {"limited", "blocked", "captcha"}:
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
    use_proxy_pool: bool = False,
    proxy_pool_db_path: str = "data/crawler.db",
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
            use_proxy_pool=use_proxy_pool,
            proxy_pool_db_path=proxy_pool_db_path,
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


def add_crawl_proxy(
    *,
    platform: Platform,
    proxy_name: str,
    proxy_url: str,
    db_path: str = "data/crawler.db",
    enabled: bool = True,
    priority: int = 100,
    metadata: dict | None = None,
) -> dict:
    return register_crawl_proxy(
        platform=platform,
        proxy_name=proxy_name,
        proxy_url=proxy_url,
        db_path=db_path,
        enabled=enabled,
        priority=priority,
        metadata=metadata,
    )


def list_crawl_proxy_pool(
    *,
    db_path: str = "data/crawler.db",
    platform: Platform | None = None,
    enabled_only: bool = False,
) -> list[dict]:
    return list_crawl_proxies(
        db_path=db_path,
        platform=platform,
        enabled_only=enabled_only,
    )


def toggle_crawl_proxy(
    *,
    proxy_id: int,
    enabled: bool,
    db_path: str = "data/crawler.db",
) -> None:
    set_crawl_proxy_enabled(proxy_id=proxy_id, enabled=enabled, db_path=db_path)


def list_pool_health_history(
    *,
    db_path: str = "data/crawler.db",
    platform: Platform | None = None,
    resource_type: str | None = None,
    window_hours: int | None = None,
    only_failed: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    return list_pool_health_events(
        db_path=db_path,
        platform=platform,
        resource_type=resource_type,
        window_hours=window_hours,
        only_failed=only_failed,
        limit=limit,
    )


def list_pool_health_trend(
    *,
    db_path: str = "data/crawler.db",
    platform: Platform | None = None,
    resource_type: str | None = None,
    window_hours: int = 24,
    only_anomalies: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    return list_pool_health_trends(
        db_path=db_path,
        platform=platform,
        resource_type=resource_type,
        window_hours=window_hours,
        only_anomalies=only_anomalies,
        limit=limit,
    )


async def probe_pool_health(
    *,
    platform: Platform,
    db_path: str = "data/crawler.db",
    probe_accounts: bool = True,
    probe_proxies: bool = True,
    timeout_ms: int = 12000,
    headless: bool = True,
) -> dict[str, Any]:
    if not probe_accounts and not probe_proxies:
        raise ValueError("at least one of probe_accounts/probe_proxies must be true")

    summary: dict[str, Any] = {
        "platform": platform,
        "probe_time_utc": datetime.now(timezone.utc).isoformat(),
        "timeout_ms": timeout_ms,
        "headless": headless,
        "accounts": [],
        "proxies": [],
    }

    if probe_accounts:
        accounts = list_crawl_accounts(
            platform=platform,
            enabled_only=True,
            db_path=db_path,
        )
        for account in accounts:
            result = await _probe_runtime_entry(
                platform=platform,
                cookies_path=account["cookies_path"],
                proxy_server=None,
                timeout_ms=timeout_ms,
                headless=headless,
            )
            account_row = {
                "id": account["id"],
                "account_name": account["account_name"],
                **result,
            }
            summary["accounts"].append(account_row)
            add_pool_health_event(
                platform=platform,
                resource_type="account",
                resource_id=account["id"],
                resource_name=account["account_name"],
                success=result["success"],
                health=result["health"],
                failure_kind=result["failure_kind"],
                latency_ms=result["latency_ms"],
                status_code=result.get("status_code"),
                probe_url=result.get("probe_url"),
                error=result.get("error"),
                db_path=db_path,
            )
            account_cd, _ = _cooldown_for_failure_kind(result["failure_kind"])
            mark_crawl_account_result(
                account_id=account["id"],
                success=result["success"],
                health=result["health"],
                error=result["error"],
                failure_kind=result["failure_kind"],
                cooldown_seconds=account_cd,
                latency_ms=result["latency_ms"],
                db_path=db_path,
            )

    if probe_proxies:
        proxies = list_crawl_proxies(
            platform=platform,
            enabled_only=True,
            db_path=db_path,
        )
        for proxy in proxies:
            result = await _probe_runtime_entry(
                platform=platform,
                cookies_path=None,
                proxy_server=proxy["proxy_url"],
                timeout_ms=timeout_ms,
                headless=headless,
            )
            proxy_row = {
                "id": proxy["id"],
                "proxy_name": proxy["proxy_name"],
                "proxy_server": proxy["proxy_url"],
                **result,
            }
            summary["proxies"].append(proxy_row)
            add_pool_health_event(
                platform=platform,
                resource_type="proxy",
                resource_id=proxy["id"],
                resource_name=proxy["proxy_name"],
                success=result["success"],
                health=result["health"],
                failure_kind=result["failure_kind"],
                latency_ms=result["latency_ms"],
                status_code=result.get("status_code"),
                probe_url=result.get("probe_url"),
                error=result.get("error"),
                db_path=db_path,
            )
            _, proxy_cd = _cooldown_for_failure_kind(result["failure_kind"])
            mark_crawl_proxy_result(
                proxy_id=proxy["id"],
                success=result["success"],
                health=result["health"],
                error=result["error"],
                failure_kind=result["failure_kind"],
                cooldown_seconds=proxy_cd,
                latency_ms=result["latency_ms"],
                db_path=db_path,
            )

    summary["account_probe_count"] = len(summary["accounts"])
    summary["proxy_probe_count"] = len(summary["proxies"])
    summary["account_ok_count"] = sum(
        1 for item in summary["accounts"] if isinstance(item, dict) and item.get("success")
    )
    summary["proxy_ok_count"] = sum(
        1 for item in summary["proxies"] if isinstance(item, dict) and item.get("success")
    )
    return summary


def probe_pool_health_sync(
    *,
    platform: Platform,
    db_path: str = "data/crawler.db",
    probe_accounts: bool = True,
    probe_proxies: bool = True,
    timeout_ms: int = 12000,
    headless: bool = True,
) -> dict[str, Any]:
    return asyncio.run(
        probe_pool_health(
            platform=platform,
            db_path=db_path,
            probe_accounts=probe_accounts,
            probe_proxies=probe_proxies,
            timeout_ms=timeout_ms,
            headless=headless,
        )
    )


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
    use_proxy_pool: bool = False,
    proxy_pool_db_path: str = "data/crawler.db",
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
        use_proxy_pool=use_proxy_pool,
        proxy_pool_db_path=proxy_pool_db_path,
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
    use_proxy_pool: bool = False,
    proxy_pool_db_path: str = "data/crawler.db",
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
            use_proxy_pool=use_proxy_pool,
            proxy_pool_db_path=proxy_pool_db_path,
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
    use_proxy_pool: bool = False,
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
        use_proxy_pool=use_proxy_pool,
        proxy_pool_db_path=db_path,
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
    use_proxy_pool: bool = False,
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
            use_proxy_pool=use_proxy_pool,
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
    use_proxy_pool: bool = False,
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
        use_proxy_pool=use_proxy_pool,
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
    use_proxy_pool: bool = False,
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
            use_proxy_pool=use_proxy_pool,
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
