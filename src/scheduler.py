from __future__ import annotations

import asyncio
import json
import os
import socket
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from .service import crawl_creator_by_id_and_store, probe_pool_health
from .storage import (
    acquire_scheduler_lock,
    add_scheduler_cycle_run_item,
    create_scheduler_cycle_run,
    finish_scheduler_cycle_run,
    get_scheduler_state,
    list_creators,
    release_scheduler_lock,
    upsert_scheduler_state,
)

SCHEDULER_RUNTIME_STATE_KEY = "crawler_scheduler_runtime"
DEFAULT_PLATFORMS = ("xiaohongshu", "douyin")
DEFAULT_SCHEDULER_NAME = "crawler-main"
DEFAULT_SCHEDULER_LOCK_KEY = "crawler_scheduler_main_lock"


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
    scheduler_name: str = DEFAULT_SCHEDULER_NAME
    lock_key: str = DEFAULT_SCHEDULER_LOCK_KEY
    lock_lease_seconds: int = 1800
    lock_owner_id: str | None = None
    webhook_alert_url: str | None = None
    webhook_alert_on_success: bool = False
    webhook_timeout_seconds: int = 8


class CrawlScheduler:
    def __init__(self, config: SchedulerConfig) -> None:
        self.config = config
        self.lock_owner_id = config.lock_owner_id or _default_lock_owner_id()

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
            "scheduler_name": self.config.scheduler_name,
            "lock_key": self.config.lock_key,
            "lock_owner_id": self.lock_owner_id,
            "force_crawl": force_crawl,
            "force_health_check": force_health_check,
            "crawl_due": crawl_due,
            "health_due": health_due,
            "crawl_result": None,
            "health_result": None,
            "runtime_state_before": runtime_state,
        }
        lock_result = acquire_scheduler_lock(
            lock_key=self.config.lock_key,
            owner_id=self.lock_owner_id,
            lease_seconds=self.config.lock_lease_seconds,
            metadata={
                "scheduler_name": self.config.scheduler_name,
                "run_time_utc": now.isoformat(),
            },
            db_path=self.config.db_path,
        )
        result["lock"] = lock_result
        if not lock_result.get("acquired"):
            runtime_state["last_scheduler_skip_utc"] = now.isoformat()
            runtime_state["last_scheduler_skip_reason"] = "lock_not_acquired"
            runtime_state["last_scheduler_skip_owner"] = lock_result.get("owner_id")
            upsert_scheduler_state(
                state_key=SCHEDULER_RUNTIME_STATE_KEY,
                value=runtime_state,
                db_path=self.config.db_path,
            )
            result["runtime_state_after"] = runtime_state
            result["cycle_status"] = "skipped_lock_not_acquired"
            return result

        cycle_run_id: int | None = None
        cycle_started = time.monotonic()
        cycle_status = "failed"
        cycle_error: str | None = None
        alert_sent = False
        alert_error: str | None = None

        try:
            cycle_run_id = create_scheduler_cycle_run(
                scheduler_name=self.config.scheduler_name,
                lock_key=self.config.lock_key,
                lock_owner_id=self.lock_owner_id,
                status="running",
                force_crawl=force_crawl,
                force_health_check=force_health_check,
                crawl_due=crawl_due,
                health_due=health_due,
                metadata={"runtime_state_before": runtime_state},
                db_path=self.config.db_path,
            )
            result["cycle_run_id"] = cycle_run_id
            cycle_status = "success"

            if health_due:
                result["health_result"] = await self._run_health_check_cycle(cycle_run_id=cycle_run_id)
                runtime_state["last_health_check_cycle_utc"] = now.isoformat()
            if crawl_due:
                result["crawl_result"] = await self._run_crawl_cycle(cycle_run_id=cycle_run_id)
                runtime_state["last_crawl_cycle_utc"] = now.isoformat()

            if not crawl_due and not health_due:
                cycle_status = "no_due"
            else:
                crawl_failed = int((result.get("crawl_result") or {}).get("failed_count") or 0)
                health_failed = int((result.get("health_result") or {}).get("failed_count") or 0)
                if crawl_failed > 0 or health_failed > 0:
                    cycle_status = "partial_failed"

            runtime_state["last_scheduler_run_utc"] = now.isoformat()
            runtime_state["last_cycle_run_id"] = cycle_run_id
            upsert_scheduler_state(
                state_key=SCHEDULER_RUNTIME_STATE_KEY,
                value=runtime_state,
                db_path=self.config.db_path,
            )
            result["runtime_state_after"] = runtime_state

            if cycle_run_id is not None:
                alert_sent, alert_error = await self._send_webhook_alert_if_needed(
                    cycle_status=cycle_status,
                    cycle_run_id=cycle_run_id,
                    result=result,
                )
        except Exception as exc:  # noqa: BLE001
            cycle_status = "failed"
            cycle_error = str(exc)
            result["error"] = cycle_error
            runtime_state["last_scheduler_error_utc"] = now.isoformat()
            runtime_state["last_scheduler_error"] = cycle_error
            runtime_state["last_scheduler_run_utc"] = now.isoformat()
            runtime_state["last_cycle_run_id"] = cycle_run_id
            upsert_scheduler_state(
                state_key=SCHEDULER_RUNTIME_STATE_KEY,
                value=runtime_state,
                db_path=self.config.db_path,
            )
            result["runtime_state_after"] = runtime_state
            if cycle_run_id is not None:
                alert_sent, alert_error = await self._send_webhook_alert_if_needed(
                    cycle_status=cycle_status,
                    cycle_run_id=cycle_run_id,
                    result=result,
                    cycle_error=cycle_error,
                )
        finally:
            if cycle_run_id is not None:
                crawl_success_count, crawl_failed_count = _extract_cycle_counts(
                    result.get("crawl_result")
                )
                health_success_count, health_failed_count = _extract_cycle_counts(
                    result.get("health_result")
                )
                finish_scheduler_cycle_run(
                    cycle_run_id=cycle_run_id,
                    status=cycle_status,
                    crawl_success_count=crawl_success_count,
                    crawl_failed_count=crawl_failed_count,
                    health_success_count=health_success_count,
                    health_failed_count=health_failed_count,
                    alert_sent=alert_sent,
                    alert_error=alert_error,
                    error=cycle_error,
                    metadata={
                        "duration_ms": int((time.monotonic() - cycle_started) * 1000),
                        "runtime_state_after": result.get("runtime_state_after"),
                    },
                    db_path=self.config.db_path,
                )
            result["lock_released"] = release_scheduler_lock(
                lock_key=self.config.lock_key,
                owner_id=self.lock_owner_id,
                db_path=self.config.db_path,
            )

        result["cycle_status"] = cycle_status
        return result

    async def _run_health_check_cycle(self, *, cycle_run_id: int | None = None) -> dict[str, Any]:
        started_at = time.monotonic()
        platforms = list(self.config.platforms)
        items: list[dict[str, Any]] = []

        for platform in platforms:
            item_started = time.monotonic()
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
                        "duration_ms": int((time.monotonic() - item_started) * 1000),
                        "summary": summary,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                items.append(
                    {
                        "platform": platform,
                        "success": False,
                        "duration_ms": int((time.monotonic() - item_started) * 1000),
                        "error": str(exc),
                    }
                )

            if cycle_run_id is not None and cycle_run_id > 0:
                current = items[-1]
                add_scheduler_cycle_run_item(
                    cycle_run_id=cycle_run_id,
                    task_type="health_check",
                    platform=platform,
                    success=bool(current.get("success")),
                    duration_ms=current.get("duration_ms"),
                    error=current.get("error"),
                    details=current.get("summary") if current.get("success") else {},
                    db_path=self.config.db_path,
                )

        total_ms = int((time.monotonic() - started_at) * 1000)
        return {
            "platforms": platforms,
            "items": items,
            "duration_ms": total_ms,
            "success_count": sum(1 for x in items if x.get("success")),
            "failed_count": sum(1 for x in items if not x.get("success")),
        }

    async def _run_crawl_cycle(self, *, cycle_run_id: int | None = None) -> dict[str, Any]:
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
            creator_url = str(item.get("creator_url") or "")
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
                        "creator_url": creator_url,
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
                        "creator_url": creator_url,
                        "success": False,
                        "error": str(exc),
                        "duration_ms": int((time.monotonic() - item_started) * 1000),
                    }

        tasks = [run_creator(item) for item in creators]
        items = await asyncio.gather(*tasks) if tasks else []
        if cycle_run_id is not None and cycle_run_id > 0:
            for entry in items:
                add_scheduler_cycle_run_item(
                    cycle_run_id=cycle_run_id,
                    task_type="crawl_creator",
                    platform=entry.get("platform"),
                    creator_id=entry.get("creator_id"),
                    creator_url=entry.get("creator_url"),
                    success=bool(entry.get("success")),
                    duration_ms=entry.get("duration_ms"),
                    error=entry.get("error"),
                    details={"run_id": entry.get("run_id")},
                    db_path=self.config.db_path,
                )
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

    async def _send_webhook_alert_if_needed(
        self,
        *,
        cycle_status: str,
        cycle_run_id: int,
        result: dict[str, Any],
        cycle_error: str | None = None,
    ) -> tuple[bool, str | None]:
        webhook_url = (self.config.webhook_alert_url or "").strip()
        if not webhook_url:
            return (False, None)
        has_failures = _cycle_has_failures(result=result, cycle_error=cycle_error)
        if not self.config.webhook_alert_on_success and not has_failures:
            return (False, None)
        payload = {
            "event": "scheduler_cycle",
            "scheduler_name": self.config.scheduler_name,
            "cycle_run_id": cycle_run_id,
            "status": cycle_status,
            "run_time_utc": result.get("run_time_utc"),
            "lock_key": self.config.lock_key,
            "lock_owner_id": self.lock_owner_id,
            "force_crawl": bool(result.get("force_crawl")),
            "force_health_check": bool(result.get("force_health_check")),
            "crawl_due": bool(result.get("crawl_due")),
            "health_due": bool(result.get("health_due")),
            "crawl_result": result.get("crawl_result"),
            "health_result": result.get("health_result"),
            "error": cycle_error,
        }
        try:
            await asyncio.to_thread(
                _post_json_webhook,
                webhook_url,
                payload,
                self.config.webhook_timeout_seconds,
            )
            return (True, None)
        except Exception as exc:  # noqa: BLE001
            return (False, str(exc))


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


def _extract_cycle_counts(result: Any) -> tuple[int, int]:
    if not isinstance(result, dict):
        return (0, 0)
    success_count = int(result.get("success_count") or 0)
    failed_count = int(result.get("failed_count") or 0)
    return (max(0, success_count), max(0, failed_count))


def _cycle_has_failures(*, result: dict[str, Any], cycle_error: str | None = None) -> bool:
    if cycle_error:
        return True
    _, crawl_failed = _extract_cycle_counts(result.get("crawl_result"))
    _, health_failed = _extract_cycle_counts(result.get("health_result"))
    return (crawl_failed + health_failed) > 0


def _post_json_webhook(url: str, payload: dict[str, Any], timeout_seconds: int) -> None:
    request = urllib.request.Request(
        url=url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    effective_timeout = max(1, int(timeout_seconds))
    try:
        with urllib.request.urlopen(request, timeout=effective_timeout) as response:
            status_code = int(getattr(response, "status", 0) or 0)
            if status_code < 200 or status_code >= 300:
                raise RuntimeError(f"webhook responded with status={status_code}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"webhook http error: {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"webhook url error: {exc.reason}") from exc


def _default_lock_owner_id() -> str:
    host = socket.gethostname() or "unknown-host"
    pid = os.getpid()
    token = uuid.uuid4().hex[:8]
    return f"{host}:{pid}:{token}"


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
