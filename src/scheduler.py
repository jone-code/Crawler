from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from .service import crawl_creator_by_id_and_store, probe_pool_health
from .storage import get_scheduler_state, list_creators, upsert_scheduler_state

SCHEDULER_RUNTIME_STATE_KEY = "crawler_scheduler_runtime"
DEFAULT_PLATFORMS = ("xiaohongshu", "douyin")


@dataclass
class SchedulerConfig:
    db_path: str = "data/crawler.db"
    platforms: tuple[str, ...] = DEFAULT_PLATFORMS
    crawl_interval_minutes: int = 180
    health_check_interval_minutes: int = 60
    platform_concurrency: dict[str, int] = field(
        default_factory=lambda: {"xiaohongshu": 2, "douyin": 2}
    )
    max_creators_per_cycle: int = 0
    max_items: int = 20
    headless: bool = True
    download_media: bool = False
    media_root: str = "data/media"
    use_checkpoint: bool = True
    use_account_pool: bool = True
    use_proxy_pool: bool = True
    health_probe_accounts: bool = True
    health_probe_proxies: bool = True
    health_timeout_ms: int = 12000


class CrawlScheduler:
    def __init__(self, config: SchedulerConfig) -> None:
        self.config = config

    async def run_once(
        self,
        *,
        force_crawl: bool = False,
        force_health_check: bool = False,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        runtime_state = get_scheduler_runtime_state(db_path=self.config.db_path)
        last_crawl = _parse_utc(runtime_state.get("last_crawl_cycle_utc"))
        last_health = _parse_utc(runtime_state.get("last_health_check_cycle_utc"))

        crawl_due = force_crawl or _is_due(last_crawl, self.config.crawl_interval_minutes, now)
        health_due = force_health_check or _is_due(
            last_health, self.config.health_check_interval_minutes, now
        )

        result: dict[str, Any] = {
            "run_time_utc": now.isoformat(),
            "force_crawl": force_crawl,
            "force_health_check": force_health_check,
            "crawl_due": crawl_due,
            "health_due": health_due,
            "crawl_result": None,
            "health_result": None,
            "runtime_state_before": runtime_state,
        }

        if health_due:
            result["health_result"] = await self._run_health_check_cycle()
            runtime_state["last_health_check_cycle_utc"] = now.isoformat()
        if crawl_due:
            result["crawl_result"] = await self._run_crawl_cycle()
            runtime_state["last_crawl_cycle_utc"] = now.isoformat()

        runtime_state["last_scheduler_run_utc"] = now.isoformat()
        upsert_scheduler_state(
            state_key=SCHEDULER_RUNTIME_STATE_KEY,
            value=runtime_state,
            db_path=self.config.db_path,
        )
        result["runtime_state_after"] = runtime_state
        return result

    async def _run_health_check_cycle(self) -> dict[str, Any]:
        started_at = time.monotonic()
        platforms = list(self.config.platforms)
        items: list[dict[str, Any]] = []

        for platform in platforms:
            try:
                summary = await probe_pool_health(
                    platform=platform,  # type: ignore[arg-type]
                    db_path=self.config.db_path,
                    probe_accounts=self.config.health_probe_accounts,
                    probe_proxies=self.config.health_probe_proxies,
                    timeout_ms=self.config.health_timeout_ms,
                    headless=self.config.headless,
                )
                items.append(
                    {
                        "platform": platform,
                        "success": True,
                        "summary": summary,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                items.append(
                    {
                        "platform": platform,
                        "success": False,
                        "error": str(exc),
                    }
                )

        total_ms = int((time.monotonic() - started_at) * 1000)
        return {
            "platforms": platforms,
            "items": items,
            "duration_ms": total_ms,
            "success_count": sum(1 for x in items if x.get("success")),
            "failed_count": sum(1 for x in items if not x.get("success")),
        }

    async def _run_crawl_cycle(self) -> dict[str, Any]:
        started_at = time.monotonic()
        creators = list_creators(db_path=self.config.db_path, enabled_only=True)
        platform_set = set(self.config.platforms)
        creators = [
            item
            for item in creators
            if isinstance(item, dict) and item.get("platform") in platform_set
        ]
        creators.sort(key=_creator_sort_key)
        if self.config.max_creators_per_cycle > 0:
            creators = creators[: self.config.max_creators_per_cycle]

        semaphores = {
            platform: asyncio.Semaphore(max(1, int(self.config.platform_concurrency.get(platform, 1))))
            for platform in platform_set
        }
        default_sem = asyncio.Semaphore(1)

        async def run_creator(item: dict[str, Any]) -> dict[str, Any]:
            platform = str(item.get("platform") or "")
            creator_id = str(item.get("creator_id") or "")
            semaphore = semaphores.get(platform, default_sem)
            item_started = time.monotonic()
            async with semaphore:
                try:
                    result = await crawl_creator_by_id_and_store(
                        platform=platform,  # type: ignore[arg-type]
                        creator_id=creator_id,
                        max_items=self.config.max_items,
                        headless=self.config.headless,
                        db_path=self.config.db_path,
                        download_media=self.config.download_media,
                        media_root=self.config.media_root,
                        use_checkpoint=self.config.use_checkpoint,
                        use_account_pool=self.config.use_account_pool,
                        use_proxy_pool=self.config.use_proxy_pool,
                    )
                    return {
                        "platform": platform,
                        "creator_id": creator_id,
                        "success": True,
                        "run_id": (
                            result.get("storage", {}).get("run_id")
                            if isinstance(result, dict)
                            else None
                        ),
                        "duration_ms": int((time.monotonic() - item_started) * 1000),
                    }
                except Exception as exc:  # noqa: BLE001
                    return {
                        "platform": platform,
                        "creator_id": creator_id,
                        "success": False,
                        "error": str(exc),
                        "duration_ms": int((time.monotonic() - item_started) * 1000),
                    }

        tasks = [run_creator(item) for item in creators]
        items = await asyncio.gather(*tasks) if tasks else []
        duration_ms = int((time.monotonic() - started_at) * 1000)

        platform_summary: dict[str, dict[str, int]] = {}
        for item in items:
            platform = str(item.get("platform") or "unknown")
            bucket = platform_summary.setdefault(platform, {"total": 0, "success": 0, "failed": 0})
            bucket["total"] += 1
            if item.get("success"):
                bucket["success"] += 1
            else:
                bucket["failed"] += 1

        return {
            "items": items,
            "platform_summary": platform_summary,
            "total_creators": len(items),
            "success_count": sum(1 for item in items if item.get("success")),
            "failed_count": sum(1 for item in items if not item.get("success")),
            "duration_ms": duration_ms,
        }


def get_scheduler_runtime_state(
    *,
    db_path: str = "data/crawler.db",
) -> dict[str, Any]:
    return get_scheduler_state(
        state_key=SCHEDULER_RUNTIME_STATE_KEY,
        db_path=db_path,
    ) or {}


def run_scheduler_once_sync(
    config: SchedulerConfig,
    *,
    force_crawl: bool = False,
    force_health_check: bool = False,
) -> dict[str, Any]:
    scheduler = CrawlScheduler(config)
    return asyncio.run(
        scheduler.run_once(
            force_crawl=force_crawl,
            force_health_check=force_health_check,
        )
    )


async def run_scheduler_daemon(
    config: SchedulerConfig,
    *,
    tick_seconds: int = 30,
    stop_event: asyncio.Event | None = None,
    max_ticks: int = 0,
) -> dict[str, Any]:
    scheduler = CrawlScheduler(config)
    effective_tick = max(5, tick_seconds)
    ticks = 0
    cycles_executed = 0
    last_result: dict[str, Any] | None = None
    started_at = datetime.now(timezone.utc).isoformat()

    while True:
        if stop_event is not None and stop_event.is_set():
            break
        last_result = await scheduler.run_once()
        if last_result.get("crawl_due") or last_result.get("health_due"):
            cycles_executed += 1
        ticks += 1
        if max_ticks > 0 and ticks >= max_ticks:
            break
        await asyncio.sleep(effective_tick)

    return {
        "started_at_utc": started_at,
        "stopped_at_utc": datetime.now(timezone.utc).isoformat(),
        "ticks": ticks,
        "cycles_executed": cycles_executed,
        "last_result": last_result,
    }


def run_scheduler_daemon_sync(
    config: SchedulerConfig,
    *,
    tick_seconds: int = 30,
    max_ticks: int = 0,
) -> dict[str, Any]:
    return asyncio.run(
        run_scheduler_daemon(
            config=config,
            tick_seconds=tick_seconds,
            max_ticks=max_ticks,
        )
    )


def _is_due(last_time: datetime | None, interval_minutes: int, now: datetime) -> bool:
    if interval_minutes <= 0:
        return True
    if last_time is None:
        return True
    return now >= last_time + timedelta(minutes=interval_minutes)


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


def _creator_sort_key(item: dict[str, Any]) -> tuple[int, str, int]:
    last_crawl = item.get("last_crawl_time_utc")
    if not isinstance(last_crawl, str) or not last_crawl.strip():
        return (0, "", int(item.get("id") or 0))
    return (1, last_crawl, int(item.get("id") or 0))
