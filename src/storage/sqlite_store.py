from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
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

            CREATE TABLE IF NOT EXISTS crawl_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                account_name TEXT NOT NULL,
                cookies_path TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 100,
                fail_count INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_used_at_utc TEXT,
                last_health TEXT,
                last_error TEXT,
                cooldown_until_utc TEXT,
                last_latency_ms INTEGER,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(platform, account_name)
            );

            CREATE INDEX IF NOT EXISTS idx_crawl_accounts_platform_enabled
              ON crawl_accounts(platform, enabled, priority, last_used_at_utc);

            CREATE TABLE IF NOT EXISTS crawl_proxies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                proxy_name TEXT NOT NULL,
                proxy_url TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 100,
                fail_count INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_used_at_utc TEXT,
                last_health TEXT,
                last_error TEXT,
                cooldown_until_utc TEXT,
                last_latency_ms INTEGER,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(platform, proxy_name)
            );

            CREATE INDEX IF NOT EXISTS idx_crawl_proxies_platform_enabled
              ON crawl_proxies(platform, enabled, priority, last_used_at_utc);

            CREATE TABLE IF NOT EXISTS crawl_checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                creator_url TEXT NOT NULL,
                checkpoint_json TEXT NOT NULL,
                last_run_id INTEGER,
                updated_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(platform, creator_url)
            );

            CREATE INDEX IF NOT EXISTS idx_crawl_checkpoints_platform_creator
              ON crawl_checkpoints(platform, creator_url);

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

            CREATE TABLE IF NOT EXISTS pool_health_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id INTEGER,
                resource_name TEXT NOT NULL,
                success INTEGER NOT NULL,
                health TEXT,
                failure_kind TEXT,
                latency_ms INTEGER,
                status_code INTEGER,
                probe_url TEXT,
                error TEXT,
                checked_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_pool_health_events_lookup
              ON pool_health_events(platform, resource_type, resource_id, checked_at_utc);

            CREATE TABLE IF NOT EXISTS scheduler_state (
                state_key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS scheduler_locks (
                lock_key TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                lease_until_utc TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS scheduler_cycle_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheduler_name TEXT NOT NULL,
                lock_key TEXT NOT NULL,
                lock_owner_id TEXT,
                status TEXT NOT NULL,
                force_crawl INTEGER NOT NULL DEFAULT 0,
                force_health_check INTEGER NOT NULL DEFAULT 0,
                crawl_due INTEGER NOT NULL DEFAULT 0,
                health_due INTEGER NOT NULL DEFAULT 0,
                crawl_success_count INTEGER NOT NULL DEFAULT 0,
                crawl_failed_count INTEGER NOT NULL DEFAULT 0,
                health_success_count INTEGER NOT NULL DEFAULT 0,
                health_failed_count INTEGER NOT NULL DEFAULT 0,
                alert_sent INTEGER NOT NULL DEFAULT 0,
                alert_error TEXT,
                error TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                started_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                ended_at_utc TEXT,
                duration_ms INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_scheduler_cycle_runs_recent
              ON scheduler_cycle_runs(started_at_utc DESC, id DESC);

            CREATE TABLE IF NOT EXISTS scheduler_cycle_run_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_run_id INTEGER NOT NULL,
                task_type TEXT NOT NULL,
                platform TEXT,
                creator_id TEXT,
                creator_url TEXT,
                success INTEGER NOT NULL,
                duration_ms INTEGER,
                error TEXT,
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (cycle_run_id) REFERENCES scheduler_cycle_runs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_scheduler_cycle_run_items_run
              ON scheduler_cycle_run_items(cycle_run_id, id ASC);

            CREATE TABLE IF NOT EXISTS admin_action_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                success INTEGER NOT NULL,
                target_type TEXT,
                target_id TEXT,
                details_json TEXT NOT NULL DEFAULT '{}',
                remote_addr TEXT,
                user_agent TEXT,
                created_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_admin_action_logs_recent
              ON admin_action_logs(id DESC);

            CREATE INDEX IF NOT EXISTS idx_admin_action_logs_action_time
              ON admin_action_logs(action, created_at_utc DESC);
            """
        )
        _run_schema_migrations(conn)


def _run_schema_migrations(conn: sqlite3.Connection) -> None:
    _ensure_columns(
        conn,
        "crawl_accounts",
        {
            "consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
            "cooldown_until_utc": "TEXT",
            "last_latency_ms": "INTEGER",
        },
    )
    _ensure_columns(
        conn,
        "crawl_proxies",
        {
            "consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
            "cooldown_until_utc": "TEXT",
            "last_latency_ms": "INTEGER",
        },
    )


def _ensure_columns(
    conn: sqlite3.Connection,
    table_name: str,
    columns: dict[str, str],
) -> None:
    existing = {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        if len(row) > 1
    }
    for column_name, column_ddl in columns.items():
        if column_name in existing:
            continue
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_ddl}")


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
        crawler_meta = payload.get("crawler_meta")
        pagination_meta = crawler_meta.get("pagination", {}) if isinstance(crawler_meta, dict) else {}
        partial_crawl = bool(pagination_meta.get("partial_crawl")) if isinstance(pagination_meta, dict) else False
        existing_posts = _fetch_existing_posts(conn, platform=platform, creator_url=creator_url)
        diff_result = _compute_post_diff(
            existing_posts=existing_posts,
            incoming_posts=posts,
            include_missing=not partial_crawl,
        )

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


def register_crawl_account(
    *,
    platform: str,
    account_name: str,
    cookies_path: str,
    enabled: bool = True,
    priority: int = 100,
    metadata: dict[str, Any] | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    if not account_name.strip():
        raise ValueError("account_name cannot be empty")
    if not cookies_path.strip():
        raise ValueError("cookies_path cannot be empty")

    init_sqlite_db(db_path=db_path)
    metadata_payload = metadata or {}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO crawl_accounts (
                platform,
                account_name,
                cookies_path,
                enabled,
                priority,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, account_name) DO UPDATE SET
                cookies_path = excluded.cookies_path,
                enabled = excluded.enabled,
                priority = excluded.priority,
                metadata_json = excluded.metadata_json,
                updated_at_utc = datetime('now')
            """,
            (
                platform,
                account_name,
                cookies_path,
                1 if enabled else 0,
                priority,
                json.dumps(metadata_payload, ensure_ascii=False),
            ),
        )
        row = conn.execute(
            """
            SELECT
                id,
                platform,
                account_name,
                cookies_path,
                enabled,
                priority,
                fail_count,
                success_count,
                consecutive_failures,
                last_used_at_utc,
                last_health,
                last_error,
                cooldown_until_utc,
                last_latency_ms,
                metadata_json,
                created_at_utc,
                updated_at_utc
            FROM crawl_accounts
            WHERE platform = ? AND account_name = ?
            """,
            (platform, account_name),
        ).fetchone()

    if row is None:
        raise RuntimeError("failed to register crawl account")
    return _crawl_account_row_to_dict(row)


def list_crawl_accounts(
    *,
    platform: str | None = None,
    enabled_only: bool = False,
    db_path: str = DEFAULT_DB_PATH,
) -> list[dict[str, Any]]:
    init_sqlite_db(db_path=db_path)
    query = """
        SELECT
            id,
            platform,
            account_name,
            cookies_path,
            enabled,
            priority,
            fail_count,
            success_count,
            consecutive_failures,
            last_used_at_utc,
            last_health,
            last_error,
            cooldown_until_utc,
            last_latency_ms,
            metadata_json,
            created_at_utc,
            updated_at_utc
        FROM crawl_accounts
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
    query += " ORDER BY priority ASC, COALESCE(last_used_at_utc, ''), id ASC"

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [_crawl_account_row_to_dict(row) for row in rows]


def set_crawl_account_enabled(
    *,
    account_id: int,
    enabled: bool,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE crawl_accounts
            SET
                enabled = ?,
                updated_at_utc = datetime('now')
            WHERE id = ?
            """,
            (1 if enabled else 0, account_id),
        )
        conn.commit()


def mark_crawl_account_result(
    *,
    account_id: int,
    success: bool,
    health: str | None = None,
    error: str | None = None,
    failure_kind: str | None = None,
    cooldown_seconds: int | None = None,
    latency_ms: int | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT consecutive_failures FROM crawl_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        if row is None:
            return
        previous_failures = int(row[0] or 0)
        if success:
            next_failures = 0
            cooldown_until_utc = None
            last_error = None
        else:
            next_failures = previous_failures + 1
            applied_cooldown = _resolve_cooldown_seconds(
                failure_kind=failure_kind,
                consecutive_failures=next_failures,
                requested_seconds=cooldown_seconds,
            )
            cooldown_until_utc = _utc_after_seconds(applied_cooldown)
            last_error = error

        conn.execute(
            """
            UPDATE crawl_accounts
            SET
                success_count = success_count + ?,
                fail_count = fail_count + ?,
                consecutive_failures = ?,
                last_used_at_utc = datetime('now'),
                last_health = ?,
                last_error = ?,
                cooldown_until_utc = ?,
                last_latency_ms = ?,
                updated_at_utc = datetime('now')
            WHERE id = ?
            """,
            (
                1 if success else 0,
                0 if success else 1,
                next_failures,
                health,
                last_error,
                cooldown_until_utc,
                latency_ms,
                account_id,
            ),
        )
        conn.commit()


def register_crawl_proxy(
    *,
    platform: str,
    proxy_name: str,
    proxy_url: str,
    enabled: bool = True,
    priority: int = 100,
    metadata: dict[str, Any] | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    if not proxy_name.strip():
        raise ValueError("proxy_name cannot be empty")
    if not proxy_url.strip():
        raise ValueError("proxy_url cannot be empty")

    init_sqlite_db(db_path=db_path)
    metadata_payload = metadata or {}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO crawl_proxies (
                platform,
                proxy_name,
                proxy_url,
                enabled,
                priority,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, proxy_name) DO UPDATE SET
                proxy_url = excluded.proxy_url,
                enabled = excluded.enabled,
                priority = excluded.priority,
                metadata_json = excluded.metadata_json,
                updated_at_utc = datetime('now')
            """,
            (
                platform,
                proxy_name,
                proxy_url,
                1 if enabled else 0,
                priority,
                json.dumps(metadata_payload, ensure_ascii=False),
            ),
        )
        row = conn.execute(
            """
            SELECT
                id,
                platform,
                proxy_name,
                proxy_url,
                enabled,
                priority,
                fail_count,
                success_count,
                consecutive_failures,
                last_used_at_utc,
                last_health,
                last_error,
                cooldown_until_utc,
                last_latency_ms,
                metadata_json,
                created_at_utc,
                updated_at_utc
            FROM crawl_proxies
            WHERE platform = ? AND proxy_name = ?
            """,
            (platform, proxy_name),
        ).fetchone()

    if row is None:
        raise RuntimeError("failed to register crawl proxy")
    return _crawl_proxy_row_to_dict(row)


def list_crawl_proxies(
    *,
    platform: str | None = None,
    enabled_only: bool = False,
    db_path: str = DEFAULT_DB_PATH,
) -> list[dict[str, Any]]:
    init_sqlite_db(db_path=db_path)
    query = """
        SELECT
            id,
            platform,
            proxy_name,
            proxy_url,
            enabled,
            priority,
            fail_count,
            success_count,
            consecutive_failures,
            last_used_at_utc,
            last_health,
            last_error,
            cooldown_until_utc,
            last_latency_ms,
            metadata_json,
            created_at_utc,
            updated_at_utc
        FROM crawl_proxies
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
    query += " ORDER BY priority ASC, COALESCE(last_used_at_utc, ''), id ASC"

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [_crawl_proxy_row_to_dict(row) for row in rows]


def set_crawl_proxy_enabled(
    *,
    proxy_id: int,
    enabled: bool,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE crawl_proxies
            SET
                enabled = ?,
                updated_at_utc = datetime('now')
            WHERE id = ?
            """,
            (1 if enabled else 0, proxy_id),
        )
        conn.commit()


def mark_crawl_proxy_result(
    *,
    proxy_id: int,
    success: bool,
    health: str | None = None,
    error: str | None = None,
    failure_kind: str | None = None,
    cooldown_seconds: int | None = None,
    latency_ms: int | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT consecutive_failures FROM crawl_proxies WHERE id = ?",
            (proxy_id,),
        ).fetchone()
        if row is None:
            return
        previous_failures = int(row[0] or 0)
        if success:
            next_failures = 0
            cooldown_until_utc = None
            last_error = None
        else:
            next_failures = previous_failures + 1
            applied_cooldown = _resolve_cooldown_seconds(
                failure_kind=failure_kind,
                consecutive_failures=next_failures,
                requested_seconds=cooldown_seconds,
            )
            cooldown_until_utc = _utc_after_seconds(applied_cooldown)
            last_error = error

        conn.execute(
            """
            UPDATE crawl_proxies
            SET
                success_count = success_count + ?,
                fail_count = fail_count + ?,
                consecutive_failures = ?,
                last_used_at_utc = datetime('now'),
                last_health = ?,
                last_error = ?,
                cooldown_until_utc = ?,
                last_latency_ms = ?,
                updated_at_utc = datetime('now')
            WHERE id = ?
            """,
            (
                1 if success else 0,
                0 if success else 1,
                next_failures,
                health,
                last_error,
                cooldown_until_utc,
                latency_ms,
                proxy_id,
            ),
        )
        conn.commit()


def add_pool_health_event(
    *,
    platform: str,
    resource_type: str,
    resource_id: int | None,
    resource_name: str,
    success: bool,
    health: str | None = None,
    failure_kind: str | None = None,
    latency_ms: int | None = None,
    status_code: int | None = None,
    probe_url: str | None = None,
    error: str | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    if resource_type not in {"account", "proxy"}:
        raise ValueError("resource_type must be 'account' or 'proxy'")
    if not resource_name.strip():
        raise ValueError("resource_name cannot be empty")
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO pool_health_events (
                platform,
                resource_type,
                resource_id,
                resource_name,
                success,
                health,
                failure_kind,
                latency_ms,
                status_code,
                probe_url,
                error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                platform,
                resource_type,
                resource_id,
                resource_name.strip(),
                1 if success else 0,
                health,
                failure_kind,
                latency_ms,
                status_code,
                probe_url,
                error,
            ),
        )
        conn.commit()


def list_pool_health_events(
    *,
    db_path: str = DEFAULT_DB_PATH,
    platform: str | None = None,
    resource_type: str | None = None,
    window_hours: int | None = None,
    only_failed: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    init_sqlite_db(db_path=db_path)
    query = """
        SELECT
            id,
            platform,
            resource_type,
            resource_id,
            resource_name,
            success,
            health,
            failure_kind,
            latency_ms,
            status_code,
            probe_url,
            error,
            checked_at_utc
        FROM pool_health_events
    """
    filters: list[str] = []
    params: list[Any] = []
    if platform:
        filters.append("platform = ?")
        params.append(platform)
    if resource_type:
        filters.append("resource_type = ?")
        params.append(resource_type)
    if isinstance(window_hours, int) and window_hours > 0:
        effective_hours = max(1, min(window_hours, 24 * 30))
        filters.append("checked_at_utc >= datetime('now', ?)")
        params.append(f"-{effective_hours} hours")
    if only_failed:
        filters.append("success = 0")
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(max(1, limit))

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        {
            "id": row[0],
            "platform": row[1],
            "resource_type": row[2],
            "resource_id": row[3],
            "resource_name": row[4],
            "success": bool(row[5]),
            "health": row[6],
            "failure_kind": row[7],
            "latency_ms": row[8],
            "status_code": row[9],
            "probe_url": row[10],
            "error": row[11],
            "checked_at_utc": row[12],
        }
        for row in rows
    ]


def list_pool_health_trends(
    *,
    db_path: str = DEFAULT_DB_PATH,
    platform: str | None = None,
    resource_type: str | None = None,
    window_hours: int = 24,
    only_anomalies: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    init_sqlite_db(db_path=db_path)
    effective_hours = max(1, min(window_hours, 24 * 30))
    window_expr = f"-{effective_hours} hours"
    query = """
        SELECT
            e.platform,
            e.resource_type,
            e.resource_id,
            e.resource_name,
            COUNT(*) AS total_checks,
            SUM(CASE WHEN e.success = 1 THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN e.success = 0 THEN 1 ELSE 0 END) AS fail_count,
            ROUND(
                100.0 * SUM(CASE WHEN e.success = 1 THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS success_rate_pct,
            ROUND(AVG(COALESCE(e.latency_ms, 0)), 1) AS avg_latency_ms,
            MAX(e.checked_at_utc) AS last_checked_at_utc,
            (
                SELECT x.health
                FROM pool_health_events x
                WHERE x.platform = e.platform
                  AND x.resource_type = e.resource_type
                  AND x.resource_name = e.resource_name
                ORDER BY x.id DESC
                LIMIT 1
            ) AS last_health,
            (
                SELECT x.failure_kind
                FROM pool_health_events x
                WHERE x.platform = e.platform
                  AND x.resource_type = e.resource_type
                  AND x.resource_name = e.resource_name
                ORDER BY x.id DESC
                LIMIT 1
            ) AS last_failure_kind
        FROM pool_health_events e
        WHERE e.checked_at_utc >= datetime('now', ?)
    """
    params: list[Any] = [window_expr]
    if platform:
        query += " AND e.platform = ?"
        params.append(platform)
    if resource_type:
        query += " AND e.resource_type = ?"
        params.append(resource_type)
    query += """
        GROUP BY
            e.platform,
            e.resource_type,
            e.resource_id,
            e.resource_name
    """
    if only_anomalies:
        query += """
        HAVING SUM(CASE WHEN e.success = 0 THEN 1 ELSE 0 END) > 0
        """
    query += """
        ORDER BY
            success_rate_pct ASC,
            total_checks DESC,
            last_checked_at_utc DESC
        LIMIT ?
    """
    params.append(max(1, limit))

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        {
            "platform": row[0],
            "resource_type": row[1],
            "resource_id": row[2],
            "resource_name": row[3],
            "total_checks": row[4],
            "success_count": row[5],
            "fail_count": row[6],
            "success_rate_pct": row[7],
            "avg_latency_ms": row[8],
            "last_checked_at_utc": row[9],
            "last_health": row[10],
            "last_failure_kind": row[11],
        }
        for row in rows
    ]


def get_scheduler_state(
    *,
    state_key: str,
    db_path: str = DEFAULT_DB_PATH,
) -> dict[str, Any] | None:
    if not state_key.strip():
        raise ValueError("state_key cannot be empty")
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT value_json, updated_at_utc
            FROM scheduler_state
            WHERE state_key = ?
            """,
            (state_key.strip(),),
        ).fetchone()
    if row is None:
        return None
    payload = _safe_json_loads(row[0])
    if not isinstance(payload, dict):
        payload = {}
    payload["updated_at_utc"] = row[1]
    return payload


def upsert_scheduler_state(
    *,
    state_key: str,
    value: dict[str, Any],
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    if not state_key.strip():
        raise ValueError("state_key cannot be empty")
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO scheduler_state (
                state_key,
                value_json
            ) VALUES (?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                value_json = excluded.value_json,
                updated_at_utc = datetime('now')
            """,
            (
                state_key.strip(),
                json.dumps(value, ensure_ascii=False),
            ),
        )
        conn.commit()


def acquire_scheduler_lock(
    *,
    lock_key: str,
    owner_id: str,
    lease_seconds: int = 120,
    metadata: dict[str, Any] | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    if not lock_key.strip():
        raise ValueError("lock_key cannot be empty")
    if not owner_id.strip():
        raise ValueError("owner_id cannot be empty")
    init_sqlite_db(db_path=db_path)
    effective_lease = max(5, int(lease_seconds))
    now = datetime.now(timezone.utc)
    lease_until = (now + timedelta(seconds=effective_lease)).isoformat()
    metadata_payload = metadata or {}

    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT
                lock_key,
                owner_id,
                lease_until_utc,
                metadata_json,
                created_at_utc,
                updated_at_utc
            FROM scheduler_locks
            WHERE lock_key = ?
            """,
            (lock_key.strip(),),
        ).fetchone()

        if row is None:
            conn.execute(
                """
                INSERT INTO scheduler_locks (
                    lock_key,
                    owner_id,
                    lease_until_utc,
                    metadata_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    lock_key.strip(),
                    owner_id.strip(),
                    lease_until,
                    json.dumps(metadata_payload, ensure_ascii=False),
                ),
            )
            conn.commit()
            return {
                "acquired": True,
                "lock_key": lock_key.strip(),
                "owner_id": owner_id.strip(),
                "lease_until_utc": lease_until,
                "replaced_expired_lock": False,
                "previous_owner_id": None,
            }

        current_owner = str(row[1] or "")
        current_lease = _parse_iso_datetime(row[2])
        expired = current_lease is None or current_lease <= now
        owner_matched = current_owner == owner_id.strip()
        if owner_matched or expired:
            conn.execute(
                """
                UPDATE scheduler_locks
                SET
                    owner_id = ?,
                    lease_until_utc = ?,
                    metadata_json = ?,
                    updated_at_utc = datetime('now')
                WHERE lock_key = ?
                """,
                (
                    owner_id.strip(),
                    lease_until,
                    json.dumps(metadata_payload, ensure_ascii=False),
                    lock_key.strip(),
                ),
            )
            conn.commit()
            return {
                "acquired": True,
                "lock_key": lock_key.strip(),
                "owner_id": owner_id.strip(),
                "lease_until_utc": lease_until,
                "replaced_expired_lock": bool(expired and not owner_matched),
                "previous_owner_id": current_owner or None,
            }

        return {
            "acquired": False,
            "lock_key": lock_key.strip(),
            "owner_id": current_owner,
            "lease_until_utc": row[2],
            "metadata": _safe_json_object(row[3]),
        }


def release_scheduler_lock(
    *,
    lock_key: str,
    owner_id: str,
    db_path: str = DEFAULT_DB_PATH,
) -> bool:
    if not lock_key.strip():
        raise ValueError("lock_key cannot be empty")
    if not owner_id.strip():
        raise ValueError("owner_id cannot be empty")
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            DELETE FROM scheduler_locks
            WHERE lock_key = ? AND owner_id = ?
            """,
            (lock_key.strip(), owner_id.strip()),
        )
        conn.commit()
        return cursor.rowcount > 0


def create_scheduler_cycle_run(
    *,
    scheduler_name: str,
    lock_key: str,
    lock_owner_id: str | None,
    status: str,
    force_crawl: bool = False,
    force_health_check: bool = False,
    crawl_due: bool = False,
    health_due: bool = False,
    metadata: dict[str, Any] | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    if not scheduler_name.strip():
        raise ValueError("scheduler_name cannot be empty")
    if not lock_key.strip():
        raise ValueError("lock_key cannot be empty")
    if not status.strip():
        raise ValueError("status cannot be empty")
    init_sqlite_db(db_path=db_path)
    metadata_payload = metadata or {}
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO scheduler_cycle_runs (
                scheduler_name,
                lock_key,
                lock_owner_id,
                status,
                force_crawl,
                force_health_check,
                crawl_due,
                health_due,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scheduler_name.strip(),
                lock_key.strip(),
                lock_owner_id,
                status.strip(),
                1 if force_crawl else 0,
                1 if force_health_check else 0,
                1 if crawl_due else 0,
                1 if health_due else 0,
                json.dumps(metadata_payload, ensure_ascii=False),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def finish_scheduler_cycle_run(
    *,
    cycle_run_id: int,
    status: str,
    crawl_success_count: int = 0,
    crawl_failed_count: int = 0,
    health_success_count: int = 0,
    health_failed_count: int = 0,
    alert_sent: bool = False,
    alert_error: str | None = None,
    error: str | None = None,
    metadata: dict[str, Any] | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    if cycle_run_id <= 0:
        raise ValueError("cycle_run_id must be positive")
    if not status.strip():
        raise ValueError("status cannot be empty")
    init_sqlite_db(db_path=db_path)
    metadata_payload = metadata or {}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE scheduler_cycle_runs
            SET
                status = ?,
                crawl_success_count = ?,
                crawl_failed_count = ?,
                health_success_count = ?,
                health_failed_count = ?,
                alert_sent = ?,
                alert_error = ?,
                error = ?,
                metadata_json = ?,
                ended_at_utc = datetime('now'),
                duration_ms = CAST(
                    (julianday('now') - julianday(started_at_utc)) * 86400000
                    AS INTEGER
                )
            WHERE id = ?
            """,
            (
                status.strip(),
                max(0, int(crawl_success_count)),
                max(0, int(crawl_failed_count)),
                max(0, int(health_success_count)),
                max(0, int(health_failed_count)),
                1 if alert_sent else 0,
                alert_error,
                error,
                json.dumps(metadata_payload, ensure_ascii=False),
                cycle_run_id,
            ),
        )
        conn.commit()


def add_scheduler_cycle_run_item(
    *,
    cycle_run_id: int,
    task_type: str,
    platform: str | None = None,
    creator_id: str | None = None,
    creator_url: str | None = None,
    success: bool,
    duration_ms: int | None = None,
    error: str | None = None,
    details: dict[str, Any] | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    if cycle_run_id <= 0:
        raise ValueError("cycle_run_id must be positive")
    if not task_type.strip():
        raise ValueError("task_type cannot be empty")
    init_sqlite_db(db_path=db_path)
    details_payload = details or {}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO scheduler_cycle_run_items (
                cycle_run_id,
                task_type,
                platform,
                creator_id,
                creator_url,
                success,
                duration_ms,
                error,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cycle_run_id,
                task_type.strip(),
                platform,
                creator_id,
                creator_url,
                1 if success else 0,
                duration_ms,
                error,
                json.dumps(details_payload, ensure_ascii=False),
            ),
        )
        conn.commit()


def list_scheduler_cycle_runs(
    *,
    db_path: str = DEFAULT_DB_PATH,
    limit: int = 100,
    status: str | None = None,
) -> list[dict[str, Any]]:
    init_sqlite_db(db_path=db_path)
    query = """
        SELECT
            id,
            scheduler_name,
            lock_key,
            lock_owner_id,
            status,
            force_crawl,
            force_health_check,
            crawl_due,
            health_due,
            crawl_success_count,
            crawl_failed_count,
            health_success_count,
            health_failed_count,
            alert_sent,
            alert_error,
            error,
            metadata_json,
            started_at_utc,
            ended_at_utc,
            duration_ms
        FROM scheduler_cycle_runs
    """
    params: list[Any] = []
    if isinstance(status, str) and status.strip():
        query += " WHERE status = ?"
        params.append(status.strip())
    query += " ORDER BY id DESC LIMIT ?"
    params.append(max(1, int(limit)))

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        {
            "id": row[0],
            "scheduler_name": row[1],
            "lock_key": row[2],
            "lock_owner_id": row[3],
            "status": row[4],
            "force_crawl": bool(row[5]),
            "force_health_check": bool(row[6]),
            "crawl_due": bool(row[7]),
            "health_due": bool(row[8]),
            "crawl_success_count": row[9],
            "crawl_failed_count": row[10],
            "health_success_count": row[11],
            "health_failed_count": row[12],
            "alert_sent": bool(row[13]),
            "alert_error": row[14],
            "error": row[15],
            "metadata": _safe_json_object(row[16]),
            "started_at_utc": row[17],
            "ended_at_utc": row[18],
            "duration_ms": row[19],
        }
        for row in rows
    ]


def list_scheduler_cycle_run_items(
    *,
    db_path: str = DEFAULT_DB_PATH,
    cycle_run_id: int | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    init_sqlite_db(db_path=db_path)
    query = """
        SELECT
            id,
            cycle_run_id,
            task_type,
            platform,
            creator_id,
            creator_url,
            success,
            duration_ms,
            error,
            details_json,
            created_at_utc
        FROM scheduler_cycle_run_items
    """
    params: list[Any] = []
    if cycle_run_id is not None and cycle_run_id > 0:
        query += " WHERE cycle_run_id = ?"
        params.append(cycle_run_id)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(max(1, int(limit)))

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        {
            "id": row[0],
            "cycle_run_id": row[1],
            "task_type": row[2],
            "platform": row[3],
            "creator_id": row[4],
            "creator_url": row[5],
            "success": bool(row[6]),
            "duration_ms": row[7],
            "error": row[8],
            "details": _safe_json_object(row[9]),
            "created_at_utc": row[10],
        }
        for row in rows
    ]


def add_admin_action_log(
    *,
    actor: str,
    action: str,
    success: bool,
    target_type: str | None = None,
    target_id: str | None = None,
    details: dict[str, Any] | None = None,
    remote_addr: str | None = None,
    user_agent: str | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    if not actor.strip():
        raise ValueError("actor cannot be empty")
    if not action.strip():
        raise ValueError("action cannot be empty")
    init_sqlite_db(db_path=db_path)
    details_payload = details or {}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO admin_action_logs (
                actor,
                action,
                success,
                target_type,
                target_id,
                details_json,
                remote_addr,
                user_agent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                actor.strip(),
                action.strip(),
                1 if success else 0,
                target_type.strip() if isinstance(target_type, str) and target_type.strip() else None,
                target_id.strip() if isinstance(target_id, str) and target_id.strip() else None,
                json.dumps(details_payload, ensure_ascii=False),
                remote_addr,
                user_agent,
            ),
        )
        conn.commit()


def list_admin_action_logs(
    *,
    db_path: str = DEFAULT_DB_PATH,
    limit: int = 200,
    action: str | None = None,
    success: bool | None = None,
) -> list[dict[str, Any]]:
    init_sqlite_db(db_path=db_path)
    query = """
        SELECT
            id,
            actor,
            action,
            success,
            target_type,
            target_id,
            details_json,
            remote_addr,
            user_agent,
            created_at_utc
        FROM admin_action_logs
    """
    filters: list[str] = []
    params: list[Any] = []
    if isinstance(action, str) and action.strip():
        filters.append("action = ?")
        params.append(action.strip())
    if success is not None:
        filters.append("success = ?")
        params.append(1 if success else 0)
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(max(1, int(limit)))
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        {
            "id": row[0],
            "actor": row[1],
            "action": row[2],
            "success": bool(row[3]),
            "target_type": row[4],
            "target_id": row[5],
            "details": _safe_json_object(row[6]),
            "remote_addr": row[7],
            "user_agent": row[8],
            "created_at_utc": row[9],
        }
        for row in rows
    ]


def get_crawl_checkpoint(
    *,
    platform: str,
    creator_url: str,
    db_path: str = DEFAULT_DB_PATH,
) -> dict[str, Any] | None:
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT checkpoint_json, last_run_id, updated_at_utc
            FROM crawl_checkpoints
            WHERE platform = ? AND creator_url = ?
            """,
            (platform, creator_url),
        ).fetchone()
    if row is None:
        return None
    payload = _safe_json_loads(row[0])
    if not isinstance(payload, dict):
        payload = {}
    payload["last_run_id"] = row[1]
    payload["updated_at_utc"] = row[2]
    return payload


def upsert_crawl_checkpoint(
    *,
    platform: str,
    creator_url: str,
    checkpoint: dict[str, Any],
    db_path: str = DEFAULT_DB_PATH,
    run_id: int | None = None,
) -> None:
    init_sqlite_db(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO crawl_checkpoints (
                platform,
                creator_url,
                checkpoint_json,
                last_run_id
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(platform, creator_url) DO UPDATE SET
                checkpoint_json = excluded.checkpoint_json,
                last_run_id = excluded.last_run_id,
                updated_at_utc = datetime('now')
            """,
            (
                platform,
                creator_url,
                json.dumps(checkpoint, ensure_ascii=False),
                run_id,
            ),
        )
        conn.commit()


def _resolve_cooldown_seconds(
    *,
    failure_kind: str | None,
    consecutive_failures: int,
    requested_seconds: int | None,
) -> int:
    if requested_seconds is not None and requested_seconds > 0:
        minimum = requested_seconds
    else:
        minimum = 0
    failure_count = max(1, consecutive_failures)
    kind = (failure_kind or "unknown").lower()
    if kind == "auth_expired":
        dynamic = 3600
    elif kind == "proxy_error":
        dynamic = min(1800, 120 * (2 ** min(failure_count - 1, 4)))
    elif kind == "network_timeout":
        dynamic = min(900, 60 * (2 ** min(failure_count - 1, 3)))
    elif kind in {"rate_limited", "access_limited"}:
        dynamic = min(3600, 300 * (2 ** min(failure_count - 1, 3)))
    elif kind == "no_data":
        dynamic = min(1200, 120 * failure_count)
    else:
        dynamic = min(1800, 90 * (2 ** min(failure_count - 1, 4)))
    return max(minimum, dynamic)


def _utc_after_seconds(seconds: int) -> str | None:
    if seconds <= 0:
        return None
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


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


def _crawl_account_row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    metadata_text = row[14] if isinstance(row[14], str) else "{}"
    try:
        metadata = json.loads(metadata_text)
    except json.JSONDecodeError:
        metadata = {}
    return {
        "id": row[0],
        "platform": row[1],
        "account_name": row[2],
        "cookies_path": row[3],
        "enabled": bool(row[4]),
        "priority": row[5],
        "fail_count": row[6],
        "success_count": row[7],
        "consecutive_failures": row[8],
        "last_used_at_utc": row[9],
        "last_health": row[10],
        "last_error": row[11],
        "cooldown_until_utc": row[12],
        "last_latency_ms": row[13],
        "metadata": metadata,
        "created_at_utc": row[15],
        "updated_at_utc": row[16],
    }


def _crawl_proxy_row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    metadata_text = row[14] if isinstance(row[14], str) else "{}"
    try:
        metadata = json.loads(metadata_text)
    except json.JSONDecodeError:
        metadata = {}
    return {
        "id": row[0],
        "platform": row[1],
        "proxy_name": row[2],
        "proxy_url": row[3],
        "enabled": bool(row[4]),
        "priority": row[5],
        "fail_count": row[6],
        "success_count": row[7],
        "consecutive_failures": row[8],
        "last_used_at_utc": row[9],
        "last_health": row[10],
        "last_error": row[11],
        "cooldown_until_utc": row[12],
        "last_latency_ms": row[13],
        "metadata": metadata,
        "created_at_utc": row[15],
        "updated_at_utc": row[16],
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
    include_missing: bool,
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
    if include_missing:
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


def _parse_iso_datetime(value: Any) -> datetime | None:
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


def _safe_json_loads(value: Any) -> Any:
    if not isinstance(value, str):
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _safe_json_object(value: Any) -> dict[str, Any]:
    payload = _safe_json_loads(value)
    if isinstance(payload, dict):
        return payload
    return {}


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
