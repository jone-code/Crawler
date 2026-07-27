from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from crawlers import DouyinCrawler, XiaohongshuCrawler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="creator-crawler",
        description="Crawl creator posts from Xiaohongshu or Douyin.",
    )
    parser.add_argument(
        "--platform",
        required=True,
        choices=["xiaohongshu", "douyin"],
        help="Target platform.",
    )
    parser.add_argument("--url", required=True, help="Creator homepage URL.")
    parser.add_argument(
        "--max-items",
        type=int,
        default=20,
        help="Max number of posts to collect.",
    )
    parser.add_argument(
        "--cookies",
        type=str,
        default=None,
        help="Path to cookie JSON array file.",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Show browser window for debugging.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON path; stdout when omitted.",
    )
    return parser


def create_crawler(platform: str, headless: bool, cookies: str | None):
    if platform == "xiaohongshu":
        return XiaohongshuCrawler(headless=headless, cookies_path=cookies)
    if platform == "douyin":
        return DouyinCrawler(headless=headless, cookies_path=cookies)
    raise ValueError(f"Unsupported platform: {platform}")


async def run(args: argparse.Namespace) -> dict:
    crawler = create_crawler(
        platform=args.platform,
        headless=not args.show_browser,
        cookies=args.cookies,
    )
    result = await crawler.crawl(args.url, max_items=args.max_items)
    return result.to_dict()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    payload = asyncio.run(run(args))
    rendered = json.dumps(payload, indent=2, ensure_ascii=False)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        print(f"Saved crawl result to: {output_path}")
        return

    print(rendered)


if __name__ == "__main__":
    main()
