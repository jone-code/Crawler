# Creator Crawler (Xiaohongshu + Douyin)

This project crawls creator post lists from:

- Xiaohongshu
- Douyin

## Features

- Unified output schema for different platforms
- Crawl creator profile page and extract post links
- Optional cookie injection for logged-in session crawling
- Python API for async/sync integration

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
- For stable production crawling, combine browser automation with request-level API parsing and retry strategy.
