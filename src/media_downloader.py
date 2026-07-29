from __future__ import annotations

import hashlib
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def download_post_media(
    payload: dict[str, Any],
    *,
    media_root: str = "data/media",
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    posts = payload.get("posts", [])
    if not isinstance(posts, list):
        raise ValueError("payload['posts'] must be a list")

    root = Path(media_root)
    root.mkdir(parents=True, exist_ok=True)

    total_assets = 0
    downloaded = 0
    failed = 0
    seen_download: dict[str, dict[str, Any]] = {}

    for post in posts:
        if not isinstance(post, dict):
            continue
        post_assets = _resolve_post_assets(post)
        enriched_assets: list[dict[str, Any]] = []
        post_id = str(post.get("post_id") or "unknown")
        platform = str(post.get("platform") or "unknown")
        creator_id = _extract_creator_id(str(post.get("creator_url") or ""))
        post_dir = root / platform / creator_id / post_id
        post_dir.mkdir(parents=True, exist_ok=True)

        for index, asset in enumerate(post_assets, start=1):
            total_assets += 1
            media_type = asset["media_type"]
            url = asset["url"]
            if url in seen_download:
                cached = dict(seen_download[url])
                enriched_assets.append(cached)
                if cached.get("status") == "downloaded":
                    downloaded += 1
                else:
                    failed += 1
                continue

            extension = _guess_extension(url, media_type)
            filename = f"{media_type}_{index:03d}_{_short_hash(url)}{extension}"
            file_path = post_dir / filename
            status = "downloaded"
            error: str | None = None
            file_size = 0
            try:
                req = urllib.request.Request(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                    content = response.read()
                file_path.write_bytes(content)
                file_size = len(content)
                downloaded += 1
            except Exception as exc:  # noqa: BLE001
                status = "failed"
                error = str(exc)
                failed += 1

            item = {
                "media_type": media_type,
                "url": url,
                "local_path": str(file_path) if status == "downloaded" else None,
                "status": status,
                "file_size": file_size,
                "error": error,
            }
            seen_download[url] = item
            enriched_assets.append(item)

        post["media_assets"] = enriched_assets
        post["image_urls"] = [
            item["url"] for item in enriched_assets if item.get("media_type") == "image"
        ]
        post["video_urls"] = [
            item["url"] for item in enriched_assets if item.get("media_type") == "video"
        ]

    return {
        "media_root": str(root),
        "total_assets": total_assets,
        "downloaded_assets": downloaded,
        "failed_assets": failed,
    }


def _resolve_post_assets(post: dict[str, Any]) -> list[dict[str, str]]:
    assets = post.get("media_assets")
    parsed: list[dict[str, str]] = []
    if isinstance(assets, list):
        for item in assets:
            if not isinstance(item, dict):
                continue
            media_type = item.get("media_type")
            url = item.get("url")
            if media_type in {"image", "video"} and isinstance(url, str) and url.strip():
                normalized = _normalize_url(url)
                if normalized:
                    parsed.append({"media_type": media_type, "url": normalized})

    if parsed:
        return _dedupe_assets(parsed)

    for image_url in _normalize_urls(post.get("image_urls")):
        parsed.append({"media_type": "image", "url": image_url})
    for video_url in _normalize_urls(post.get("video_urls")):
        parsed.append({"media_type": "video", "url": video_url})
    cover_url = post.get("cover_url")
    if isinstance(cover_url, str) and cover_url.strip():
        normalized_cover = _normalize_url(cover_url)
        if normalized_cover:
            parsed.append({"media_type": "image", "url": normalized_cover})
    return _dedupe_assets(parsed)


def _normalize_urls(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    output: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        normalized = _normalize_url(item)
        if normalized and normalized not in output:
            output.append(normalized)
    return output


def _dedupe_assets(assets: list[dict[str, str]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in assets:
        media_type = item["media_type"]
        url = item["url"]
        key = (media_type, url)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _extract_creator_id(creator_url: str) -> str:
    match = re.search(r"/user/profile/([a-zA-Z0-9_]+)", creator_url)
    if match:
        return match.group(1)
    return "unknown_creator"


def _guess_extension(url: str, media_type: str) -> str:
    try:
        path = urllib.parse.urlparse(url).path
    except Exception:  # noqa: BLE001
        path = ""
    _, ext = os.path.splitext(path)
    ext = ext.lower()
    if ext in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".m3u8"}:
        return ext
    return ".mp4" if media_type == "video" else ".jpg"


def _short_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


def _normalize_url(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    if text.startswith("//"):
        text = f"https:{text}"
    if not (text.startswith("http://") or text.startswith("https://")):
        return None
    return text
