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
    max_items=20,
    db_path=DB_PATH,
)

print(result["storage"])
print(list_creator_ids(db_path=DB_PATH, platform="xiaohongshu"))
```

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
- For stable production crawling, combine browser automation with request-level API parsing and retry strategy.
