# Creator Crawler (Xiaohongshu + Douyin)

This project crawls creator post lists from:

- Xiaohongshu
- Douyin

## Features

- Unified output schema for different platforms
- Crawl creator profile page and extract post links
- Optional cookie injection for logged-in session crawling
- Python API for async/sync integration
- SQLite persistence for crawl runs and posts
- Backend creator-id registry (add/list creators)
- Optional media downloading (images/videos)
- Built-in admin web page (creator management + crawl runs + diff view)
- Account pool + proxy pool rotation for anti-bot resilience
- P0 resilience scheduler: cooldown circuit-breaker + weighted scoring
- Active pool health check API/admin action for account/proxy probing
- Health-check history and 24h trend view in admin dashboard
- Scheduler cycle support with platform-level concurrency limits
- Scheduler lock + cycle task logs + webhook alerts
- Procurement reference for proxy vendors and acceptance checklist (`proxy_pool_procurement.md`)

## Environment

- Python 3.10+
- Chromium runtime installed by Playwright

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

## Usage

### Admin Web Page

```bash
pip install -r requirements.txt
python -m src.admin_web
```

Then open `http://127.0.0.1:8000` to:

- add/enable/disable creators
- manage crawl account pool (multi-account rotation)
- manage crawl proxy pool (multi-proxy rotation)
- run one-click pool health checks (accounts/proxies)
- run scheduler cycles (independent crawl/health intervals + per-platform concurrency)
- trigger crawl jobs
- inspect per-run diff summary (`new/updated/missing`)
- inspect media download status

### Synchronous Call

```python
from src import crawl_creator_sync

result = crawl_creator_sync(
    platform="xiaohongshu",
    creator_url="https://www.xiaohongshu.com/user/profile/<creator_id>",
    max_items=20,
    cookies_path=None,
)

print(result)
```

### Asynchronous Call

```python
import asyncio

from src import crawl_creator


async def run():
    result = await crawl_creator(
        platform="douyin",
        creator_url="https://www.douyin.com/user/<creator_id>",
        max_items=20,
        cookies_path="cookies/douyin.cookies.json",
    )
    print(result)


asyncio.run(run())
```

Cookie file format should be a JSON array compatible with Playwright `context.add_cookies`.

### Crawl and Save to SQLite

```python
from src import crawl_creator_and_store_sync

result = crawl_creator_and_store_sync(
    platform="douyin",
    creator_url="https://www.douyin.com/user/<creator_id>",
    max_items=20,
    cookies_path="cookies/douyin.cookies.json",
    db_path="data/crawler.db",
)

print(result["storage"])
```

### Backend: Add Creator ID and Crawl by ID

```python
from src import add_creator_id, crawl_creator_by_id_and_store_sync, list_creator_ids

DB_PATH = "data/crawler.db"

add_creator_id(
    platform="xiaohongshu",
    creator_id="<creator_id>",
    db_path=DB_PATH,
    metadata={"source": "admin-panel"},
)

result = crawl_creator_by_id_and_store_sync(
    platform="xiaohongshu",
    creator_id="<creator_id>",
    max_items=20,  # set 0 for continuous deep pagination
    use_checkpoint=True,  # resume from saved pagination checkpoint when max_items=0
    use_account_pool=True,  # use enabled accounts in crawl_accounts table
    use_proxy_pool=True,  # use enabled proxies in crawl_proxies table
    db_path=DB_PATH,
    download_media=True,
    media_root="data/media",
)

print(result["storage"])
print(result["media"])
print(result["storage"]["diff"])
print(list_creator_ids(db_path=DB_PATH, platform="xiaohongshu"))
```

### Add Crawl Accounts (Account Pool)

```python
from src import add_crawl_account, list_crawl_account_pool

DB_PATH = "data/crawler.db"
add_crawl_account(
    platform="xiaohongshu",
    account_name="xhs_main_01",
    cookies_path="cookies/xhs_main_01.json",
    db_path=DB_PATH,
    priority=100,
)
add_crawl_account(
    platform="xiaohongshu",
    account_name="xhs_backup_01",
    cookies_path="cookies/xhs_backup_01.json",
    db_path=DB_PATH,
    priority=200,
)

print(list_crawl_account_pool(db_path=DB_PATH, platform="xiaohongshu", enabled_only=True))
```

### Add Crawl Proxies (Proxy Pool)

```python
from src import add_crawl_proxy, list_crawl_proxy_pool

DB_PATH = "data/crawler.db"
add_crawl_proxy(
    platform="xiaohongshu",
    proxy_name="xhs_proxy_01",
    proxy_url="http://user:pass@127.0.0.1:7890",
    db_path=DB_PATH,
    priority=100,
)
add_crawl_proxy(
    platform="xiaohongshu",
    proxy_name="xhs_proxy_02",
    proxy_url="http://127.0.0.1:7891",
    db_path=DB_PATH,
    priority=200,
)

print(list_crawl_proxy_pool(db_path=DB_PATH, platform="xiaohongshu", enabled_only=True))
```

### Run Pool Health Check (P0)

```python
from src import probe_pool_health_sync

summary = probe_pool_health_sync(
    platform="xiaohongshu",
    db_path="data/crawler.db",
    probe_accounts=True,
    probe_proxies=True,
    timeout_ms=12000,
)
print(summary)
```

### Run Scheduler Daemon (P1)

```bash
export CRAWLER_DB_PATH="data/crawler.db"
export CRAWL_INTERVAL_MINUTES=180
export HEALTH_INTERVAL_MINUTES=60
export SCHEDULER_TICK_SECONDS=30
python -m src.scheduler_daemon
```

Optional environment variables:

- `SCHEDULER_MAX_TICKS` (default `0`, infinite loop)
- `SCHEDULER_MAX_CREATORS_PER_CYCLE` (default `0`, no limit)
- `CRAWL_MAX_ITEMS`
- `CRAWL_USE_ACCOUNT_POOL` / `CRAWL_USE_PROXY_POOL`
- `HEALTH_TIMEOUT_MS`
- `SCHEDULER_NAME` (default `crawler-main`)
- `SCHEDULER_LOCK_KEY` (default `crawler_scheduler_main_lock`)
- `SCHEDULER_LOCK_LEASE_SECONDS` (default `1800`)
- `SCHEDULER_LOCK_OWNER_ID` (optional, auto-generated if empty)
- `SCHEDULER_WEBHOOK_ALERT_URL` (optional, POST JSON webhook)
- `SCHEDULER_WEBHOOK_ALERT_ON_SUCCESS` (default `false`, only push alert on failure)
- `SCHEDULER_WEBHOOK_TIMEOUT_SECONDS` (default `8`)

### Save Existing Payload to SQLite

```python
from src import save_creator_content

payload = {
    "platform": "xiaohongshu",
    "creator_url": "https://www.xiaohongshu.com/user/profile/<creator_id>",
    "creator_name": "demo",
    "crawl_time_utc": "2026-07-27T14:00:00+00:00",
    "posts": [],
}

save_creator_content(payload, db_path="data/crawler.db")
```

## Output Example

```json
{
  "platform": "douyin",
  "creator_url": "https://www.douyin.com/user/xxx",
  "creator_name": "Creator Name",
  "crawl_time_utc": "2026-07-27T14:00:00+00:00",
  "posts": [
    {
      "platform": "douyin",
      "creator_url": "https://www.douyin.com/user/xxx",
      "post_id": "1234567890",
      "post_url": "https://www.douyin.com/video/1234567890",
      "title": "sample title",
      "description": "sample description",
      "cover_url": "https://...",
      "like_count": null,
      "comment_count": null,
      "share_count": null,
      "publish_time": null,
      "raw": {}
    }
  ]
}
```

## Notes

- Target websites can change page structure and anti-bot rules at any time.
- Douyin creator pages often require login/verification, so pass `cookies_path` for stable results.
- SQLite schema is auto-created on first write (`crawl_runs` and `posts` tables).
- Creator registry table (`creators`) is also auto-created for backend id management.
- Media metadata is stored in `post_media`, and downloaded files are stored under `data/media/<platform>/<creator_id>/<post_id>/`.
- Set `max_items=0` to enable continuous pagination probing (scroll until multiple rounds have no new posts).
- For Xiaohongshu deep mode, `use_checkpoint=True` resumes from previous checkpoint to avoid re-scanning old pages.
- Checkpoint stores a recent post URL window (up to 200 URLs) for incremental resume.
- Crawler metadata now includes session diagnostics (`crawler_meta.session`) and pagination diagnostics (`crawler_meta.pagination`).
- Built-in retry/backoff and jitter delays are enabled to reduce transient anti-bot failures.
- Multi-account pool is supported via `crawl_accounts`; when `use_account_pool=True`, accounts are tried in priority order and auto-switched on failure.
- IP proxy pool is supported via `crawl_proxies`; when `use_proxy_pool=True`, proxies are tried in priority order and auto-switched on failure.
- `use_account_pool=True` and `use_proxy_pool=True` can be enabled together to run account + proxy joint rotation.
- Runtime scheduler now applies cooldown circuit-breaker and weighted score selection based on priority, success/failure history, fail streak, and latency.
- Retry backoff is now classified by error type (timeout/proxy/rate-limit/access-limit) with differentiated wait intervals.
- `probe_pool_health_sync` provides active runtime probes and writes health/cooldown updates back to pool records.
- Health probe events are persisted in `pool_health_events` and surfaced as 24h trend + recent event tables in admin.
- Admin health views support platform/type filter, anomaly-only filter, and window switch (24h/72h/7d).
- Scheduler runtime state is persisted in `scheduler_state` and displayed in the admin scheduler card.
- Scheduler lock state is persisted in `scheduler_locks` to avoid duplicate scheduler instances.
- Scheduler cycle headers are persisted in `scheduler_cycle_runs`, and per-task logs are persisted in `scheduler_cycle_run_items`.
- When `SCHEDULER_WEBHOOK_ALERT_URL` is configured, scheduler sends webhook alert payloads for failed/abnormal cycles.
- Diff data for each run is persisted in `crawl_diffs` and `crawl_diff_items`, and also returned in `result["storage"]["diff"]` (`new/updated/unchanged/missing`).
- For stable production crawling, combine browser automation with request-level API parsing and retry strategy.

## Project Records

- Proxy pool procurement guide: `proxy_pool_procurement.md`
