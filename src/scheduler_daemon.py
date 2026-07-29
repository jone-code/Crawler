from __future__ import annotations

import os

from .scheduler import SchedulerConfig, run_scheduler_daemon_sync


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def main() -> None:
    config = SchedulerConfig(
        db_path=os.getenv("CRAWLER_DB_PATH", "data/crawler.db"),
        crawl_interval_minutes=_int_env("CRAWL_INTERVAL_MINUTES", 180),
        health_check_interval_minutes=_int_env("HEALTH_INTERVAL_MINUTES", 60),
        max_items=_int_env("CRAWL_MAX_ITEMS", 20),
        headless=_bool_env("CRAWL_HEADLESS", True),
        download_media=_bool_env("CRAWL_DOWNLOAD_MEDIA", False),
        use_checkpoint=_bool_env("CRAWL_USE_CHECKPOINT", True),
        use_account_pool=_bool_env("CRAWL_USE_ACCOUNT_POOL", True),
        use_proxy_pool=_bool_env("CRAWL_USE_PROXY_POOL", True),
        max_creators_per_cycle=_int_env("SCHEDULER_MAX_CREATORS_PER_CYCLE", 0),
        health_timeout_ms=_int_env("HEALTH_TIMEOUT_MS", 12000),
    )
    tick_seconds = _int_env("SCHEDULER_TICK_SECONDS", 30)
    max_ticks = _int_env("SCHEDULER_MAX_TICKS", 0)
    result = run_scheduler_daemon_sync(
        config=config,
        tick_seconds=tick_seconds,
        max_ticks=max_ticks,
    )
    print(result)


if __name__ == "__main__":
    main()
