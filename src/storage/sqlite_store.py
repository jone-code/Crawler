from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = "data/crawler.db"


def init_sqlite_db(db_path: str = DEFAULT_DB_PATH) -> None:
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_file) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS creators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                creator_id TEXT NOT NULL,
                creator_url TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                last_crawl_time_utc TEXT,
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(platform, creator_id)
            );

            CREATE INDEX IF NOT EXISTS idx_creators_platform_enabled
              ON creators(platform, enabled);

            CREATE TABLE IF NOT EXISTS crawl_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                creator_url TEXT NOT NULL,
                creator_name TEXT,
                crawl_time_utc TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_crawl_runs_platform_creator
              ON crawl_runs(platform, creator_url);

            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                creator_url TEXT NOT NULL,
                post_id TEXT NOT NULL,
                post_url TEXT NOT NULL,
                title TEXT,
                description TEXT,
                cover_url TEXT,
                like_count INTEGER,
                comment_count INTEGER,
                share_count INTEGER,
                publish_time TEXT,
                raw TEXT NOT NULL,
                inserted_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE,
                UNIQUE(platform, post_url)
            );

            CREATE INDEX IF NOT EXISTS idx_posts_platform_creator
              ON posts(platform, creator_url);

            CREATE TABLE IF NOT EXISTS post_media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                post_url TEXT NOT NULL,
                post_id TEXT NOT NULL,
                media_type TEXT NOT NULL,
                media_url TEXT NOT NULL,
                local_path TEXT,
                download_status TEXT,
                file_size INTEGER,
                error TEXT,
                inserted_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE,
                UNIQUE(platform, post_url, media_url)
            );

            CREATE INDEX IF NOT EXISTS idx_post_media_platform_post
              ON post_media(platform, post_url);

            CREATE TABLE IF NOT EXISTS crawl_diffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL UNIQUE,
                platform TEXT NOT NULL,
                creator_url TEXT NOT NULL,
                new_count INTEGER NOT NULL DEFAULT 0,
                updated_count INTEGER NOT NULL DEFAULT 0,
                unchanged_count INTEGER NOT NULL DEFAULT 0,
                missing_count INTEGER NOT NULL DEFAULT 0,
                summary_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS crawl_diff_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                creator_url TEXT NOT NULL,
                post_url TEXT NOT NULL,
                post_id TEXT,
                change_type TEXT NOT NULL,
                before_payload TEXT,
                after_payload TEXT,
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE,
                UNIQUE(run_id, post_url)
            );

            CREATE INDEX IF NOT EXISTS idx_crawl_diff_items_run_change
              ON crawl_diff_items(run_id, change_type);
            """
        )


def save_creator_content(payload: dict[str, Any], db_path: str = DEFAULT_DB_PATH) -> dict[str, Any]:
    init_sqlite_db(db_path=db_path)
    posts = payload.get("posts", [])
    if not isinstance(posts, list):
        raise ValueError("payload['posts'] must be a list")

    media_rows_saved = 0
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        platform = str(payload.get("platform") or "")
        creator_url = str(payload.get("creator_url") or "")
        existing_posts = _fetch_existing_posts(conn, platform=platform, creator_url=creator_url)
        diff_result = _compute_post_diff(existing_posts=existing_posts, incoming_posts=posts)

        cursor = conn.execute(
            """
            INSERT INTO crawl_runs (
                platform,
                creator_url,
                creator_name,
                crawl_time_utc,
                raw_payload
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                payload.get("platform"),
                payload.get("creator_url"),
                payload.get("creator_name"),
                payload.get("crawl_time_utc"),
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        run_id = cursor.lastrowid

        for post in posts:
            if not isinstance(post, dict):
                continue
            conn.execute(
                """
                INSERT INTO posts (
                    run_id,
                    platform,
                    creator_url,
                    post_id,
                    post_url,
                    title,
                    description,
                    cover_url,
                    like_count,
                    comment_count,
                    share_count,
                    publish_time,
                    raw
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, post_url) DO UPDATE SET
                    run_id = excluded.run_id,
                    creator_url = excluded.creator_url,
                    post_id = excluded.post_id,
                    title = excluded.title,
                    description = excluded.description,
                    cover_url = excluded.cover_url,
                    like_count = excluded.like_count,
                    comment_count = excluded.comment_count,
                    share_count = excluded.share_count,
                    publish_time = excluded.publish_time,
                    raw = excluded.raw,
                    inserted_at_utc = datetime('now')
                """,
                (
                    run_id,
                    post.get("platform"),
                    post.get("creator_url"),
                    post.get("post_id"),
                    post.get("post_url"),
                    post.get("title"),
                    post.get("description"),
                    post.get("cover_url"),
                    post.get("like_count"),
                    post.get("comment_count"),
                    post.get("share_count"),
                    post.get("publish_time"),
                    json.dumps(post.get("raw", {}), ensure_ascii=False),
                ),
            )
            media_rows_saved += _save_post_media(conn, run_id, post)

        _save_diff_records(
            conn,
            run_id=run_id,
            platform=platform,
            creator_url=creator_url,
            diff_result=diff_result,
        )

        conn.commit()

    return {
        "db_path": db_path,
        "run_id": run_id,
        "saved_posts": len(posts),
        "saved_media_rows": media_rows_saved,
        "diff": _serialize_diff_result(diff_result),
    }


def register_creator(
    *,
    platform: str,
    creator_id: str,
    creator_url: str,
    db_path: str = DEFAULT_DB_PATH,
    enabled: bool = True,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not creator_id.strip():
        raise ValueError("creator_id cannot be empty")
    if not creator_url.strip():
        raise ValueError("creator_url cannot be empty")

    init_sqlite_db(db_path=db_path)
    metadata_payload = metadata or {}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO creators (
                platform,
                creator_id,
                creator_url,
                enabled,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(platform, creator_id) DO UPDATE SET
                creator_url = excluded.creator_url,
                enabled = excluded.enabled,
                metadata_json = excluded.metadata_json,
                updated_at_utc = datetime('now')
            """,
            (
                platform,
                creator_id,
                creator_url,
                1 if enabled else 0,
                json.dumps(metadata_payload, ensure_ascii=False),
            ),
        )
        row = conn.execute(
            """
            SELECT
                id,
                platform,
                creator_id,
                creator_url,
                enabled,
                metadata_json,
                last_crawl_time_utc,
                created_at_utc,
                updated_at_utc
            FROM creators
            WHERE platform = ? AND creator_id = ?
            """,
            (platform, creator_id),
        ).fetchone()

    if row is None:
        raise RuntimeError("failed to register creator")
    return _creator_row_to_dict(row)


def list_creators(
    *,
    db_path: str = DEFAULT_DB_PATH,
    platform: str | None = None,
    enabled_only: bool = False,
) -> list[dict[str, Any]]:
    init_sqlite_db(db_path=db_path)
    query = """
        SELECT
            id,
            platform,
            creator_id,
            creator_url,
            enabled,
            metadata_json,
            last_crawl_time_utc,
            created_at_utc,
            updated_at_utc
        FROM creators
    """
    filters: list[str] = []
    params: list[Any] = []
    if platform:
        filters.append("platform = ?")
        params.append(platform)
    if enabled_only:
        filters.append("enabled = 1")
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY id DESC"

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [_creator_row_to_dict(row) for row in rows]


def mark_creator_crawled(
    *,
    platform: str,
    creator_id: str,
    crawl_time_utc: str,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE creators
            SET
                last_crawl_time_utc = ?,
                updated_at_utc = datetime('now')
            WHERE platform = ? AND creator_id = ?
            """,
            (crawl_time_utc, platform, creator_id),
        )
        conn.commit()


def set_creator_enabled(
    *,
    platform: str,
    creator_id: str,
    enabled: bool,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE creators
            SET
                enabled = ?,
                updated_at_utc = datetime('now')
            WHERE platform = ? AND creator_id = ?
            """,
            (1 if enabled else 0, platform, creator_id),
        )
        conn.commit()


def _creator_row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    metadata_text = row[5] if isinstance(row[5], str) else "{}"
    try:
        metadata = json.loads(metadata_text)
    except json.JSONDecodeError:
        metadata = {}

    return {
        "id": row[0],
        "platform": row[1],
        "creator_id": row[2],
        "creator_url": row[3],
        "enabled": bool(row[4]),
        "metadata": metadata,
        "last_crawl_time_utc": row[6],
        "created_at_utc": row[7],
        "updated_at_utc": row[8],
    }


def _save_post_media(conn: sqlite3.Connection, run_id: int, post: dict[str, Any]) -> int:
    platform = str(post.get("platform") or "")
    post_url = str(post.get("post_url") or "")
    post_id = str(post.get("post_id") or "")
    if not platform or not post_url or not post_id:
        return 0

    candidates: list[dict[str, Any]] = []
    assets = post.get("media_assets")
    if isinstance(assets, list):
        for item in assets:
            if isinstance(item, dict):
                candidates.append(item)

    if not candidates:
        for url in _extract_urls(post.get("image_urls")):
            candidates.append({"media_type": "image", "url": url})
        for url in _extract_urls(post.get("video_urls")):
            candidates.append({"media_type": "video", "url": url})

    inserted = 0
    seen: set[tuple[str, str]] = set()
    for item in candidates:
        media_type = item.get("media_type")
        media_url = item.get("url")
        if media_type not in {"image", "video"}:
            continue
        if not isinstance(media_url, str) or not media_url.strip():
            continue
        media_url = media_url.strip()
        key = (media_type, media_url)
        if key in seen:
            continue
        seen.add(key)

        conn.execute(
            """
            INSERT INTO post_media (
                run_id,
                platform,
                post_url,
                post_id,
                media_type,
                media_url,
                local_path,
                download_status,
                file_size,
                error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, post_url, media_url) DO UPDATE SET
                run_id = excluded.run_id,
                post_id = excluded.post_id,
                local_path = excluded.local_path,
                download_status = excluded.download_status,
                file_size = excluded.file_size,
                error = excluded.error,
                inserted_at_utc = datetime('now')
            """,
            (
                run_id,
                platform,
                post_url,
                post_id,
                media_type,
                media_url,
                item.get("local_path"),
                item.get("status"),
                item.get("file_size"),
                item.get("error"),
            ),
        )
        inserted += 1

    return inserted


def _extract_urls(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    output: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        url = item.strip()
        if not url:
            continue
        if url.startswith("//"):
            url = f"https:{url}"
        if url not in output:
            output.append(url)
    return output


def _fetch_existing_posts(
    conn: sqlite3.Connection, *, platform: str, creator_url: str
) -> dict[str, dict[str, Any]]:
    if not platform or not creator_url:
        return {}
    rows = conn.execute(
        """
        SELECT
            post_url,
            post_id,
            title,
            description,
            cover_url,
            like_count,
            comment_count,
            share_count,
            publish_time,
            raw
        FROM posts
        WHERE platform = ? AND creator_url = ?
        """,
        (platform, creator_url),
    ).fetchall()

    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        post_url = row[0]
        if not isinstance(post_url, str) or not post_url:
            continue
        raw_payload = _safe_json_loads(row[9])
        output[post_url] = {
            "post_url": post_url,
            "post_id": row[1],
            "title": row[2],
            "description": row[3],
            "cover_url": row[4],
            "like_count": row[5],
            "comment_count": row[6],
            "share_count": row[7],
            "publish_time": row[8],
            "raw": raw_payload if isinstance(raw_payload, dict) else {},
        }
    return output


def _compute_post_diff(
    *,
    existing_posts: dict[str, dict[str, Any]],
    incoming_posts: list[dict[str, Any]],
) -> dict[str, Any]:
    incoming_map: dict[str, dict[str, Any]] = {}
    for post in incoming_posts:
        if not isinstance(post, dict):
            continue
        post_url = post.get("post_url")
        if not isinstance(post_url, str) or not post_url.strip():
            continue
        incoming_map[post_url] = _normalize_post_payload(post)

    new_items: list[dict[str, Any]] = []
    updated_items: list[dict[str, Any]] = []
    unchanged_count = 0

    for post_url, current_payload in incoming_map.items():
        previous_payload = existing_posts.get(post_url)
        if previous_payload is None:
            new_items.append({"post_url": post_url, "after": current_payload})
            continue
        if _payload_equal(previous_payload, current_payload):
            unchanged_count += 1
            continue
        updated_items.append(
            {
                "post_url": post_url,
                "before": previous_payload,
                "after": current_payload,
            }
        )

    missing_items: list[dict[str, Any]] = []
    for post_url, previous_payload in existing_posts.items():
        if post_url not in incoming_map:
            missing_items.append({"post_url": post_url, "before": previous_payload})

    return {
        "new_items": new_items,
        "updated_items": updated_items,
        "missing_items": missing_items,
        "unchanged_count": unchanged_count,
    }


def _normalize_post_payload(post: dict[str, Any]) -> dict[str, Any]:
    raw_payload = post.get("raw")
    if not isinstance(raw_payload, dict):
        raw_payload = {}
    return {
        "post_url": post.get("post_url"),
        "post_id": post.get("post_id"),
        "title": post.get("title"),
        "description": post.get("description"),
        "cover_url": post.get("cover_url"),
        "like_count": post.get("like_count"),
        "comment_count": post.get("comment_count"),
        "share_count": post.get("share_count"),
        "publish_time": post.get("publish_time"),
        "raw": raw_payload,
    }


def _payload_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return _stable_json(left) == _stable_json(right)


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_json_loads(value: Any) -> Any:
    if not isinstance(value, str):
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _serialize_diff_result(diff_result: dict[str, Any]) -> dict[str, Any]:
    new_items = diff_result["new_items"]
    updated_items = diff_result["updated_items"]
    missing_items = diff_result["missing_items"]
    unchanged_count = diff_result["unchanged_count"]
    return {
        "new_count": len(new_items),
        "updated_count": len(updated_items),
        "unchanged_count": unchanged_count,
        "missing_count": len(missing_items),
        "new_post_urls": [item["post_url"] for item in new_items],
        "updated_post_urls": [item["post_url"] for item in updated_items],
        "missing_post_urls": [item["post_url"] for item in missing_items],
    }


def _save_diff_records(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    platform: str,
    creator_url: str,
    diff_result: dict[str, Any],
) -> None:
    summary = _serialize_diff_result(diff_result)
    conn.execute(
        """
        INSERT INTO crawl_diffs (
            run_id,
            platform,
            creator_url,
            new_count,
            updated_count,
            unchanged_count,
            missing_count,
            summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            platform,
            creator_url,
            summary["new_count"],
            summary["updated_count"],
            summary["unchanged_count"],
            summary["missing_count"],
            json.dumps(summary, ensure_ascii=False),
        ),
    )

    for item in diff_result["new_items"]:
        after_payload = item["after"]
        conn.execute(
            """
            INSERT INTO crawl_diff_items (
                run_id,
                platform,
                creator_url,
                post_url,
                post_id,
                change_type,
                before_payload,
                after_payload
            ) VALUES (?, ?, ?, ?, ?, 'new', NULL, ?)
            """,
            (
                run_id,
                platform,
                creator_url,
                item["post_url"],
                after_payload.get("post_id"),
                json.dumps(after_payload, ensure_ascii=False),
            ),
        )

    for item in diff_result["updated_items"]:
        before_payload = item["before"]
        after_payload = item["after"]
        conn.execute(
            """
            INSERT INTO crawl_diff_items (
                run_id,
                platform,
                creator_url,
                post_url,
                post_id,
                change_type,
                before_payload,
                after_payload
            ) VALUES (?, ?, ?, ?, ?, 'updated', ?, ?)
            """,
            (
                run_id,
                platform,
                creator_url,
                item["post_url"],
                after_payload.get("post_id"),
                json.dumps(before_payload, ensure_ascii=False),
                json.dumps(after_payload, ensure_ascii=False),
            ),
        )

    for item in diff_result["missing_items"]:
        before_payload = item["before"]
        conn.execute(
            """
            INSERT INTO crawl_diff_items (
                run_id,
                platform,
                creator_url,
                post_url,
                post_id,
                change_type,
                before_payload,
                after_payload
            ) VALUES (?, ?, ?, ?, ?, 'missing', ?, NULL)
            """,
            (
                run_id,
                platform,
                creator_url,
                item["post_url"],
                before_payload.get("post_id"),
                json.dumps(before_payload, ensure_ascii=False),
            ),
        )
