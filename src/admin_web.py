from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from flask import Flask, flash, redirect, render_template, request, url_for

from .service import add_creator_id, crawl_creator_by_id_and_store_sync, list_creator_ids
from .storage import init_sqlite_db, set_creator_enabled
from .storage.sqlite_store import DEFAULT_DB_PATH


def create_app(db_path: str | None = None) -> Flask:
    template_dir = Path(__file__).parent / "templates"
    app = Flask(__name__, template_folder=str(template_dir))
    app.secret_key = "crawler-admin-web"
    app.config["DB_PATH"] = db_path or DEFAULT_DB_PATH

    init_sqlite_db(db_path=app.config["DB_PATH"])

    @app.get("/")
    def dashboard():
        creators = list_creator_ids(db_path=app.config["DB_PATH"], enabled_only=False)
        recent_runs = _list_recent_runs(app.config["DB_PATH"], limit=50)
        return render_template(
            "dashboard.html",
            creators=creators,
            recent_runs=recent_runs,
            default_db_path=app.config["DB_PATH"],
        )

    @app.post("/creators")
    def create_creator():
        platform = (request.form.get("platform") or "").strip()
        creator_id = (request.form.get("creator_id") or "").strip()
        metadata_text = (request.form.get("metadata_json") or "").strip()
        enabled = request.form.get("enabled") == "on"

        metadata: dict[str, Any] | None = None
        if metadata_text:
            try:
                metadata = json.loads(metadata_text)
                if not isinstance(metadata, dict):
                    raise ValueError("metadata_json must be object")
            except Exception as exc:  # noqa: BLE001
                flash(f"元数据 JSON 解析失败: {exc}", "error")
                return redirect(url_for("dashboard"))

        try:
            creator = add_creator_id(
                platform=platform,  # type: ignore[arg-type]
                creator_id=creator_id,
                db_path=app.config["DB_PATH"],
                enabled=enabled,
                metadata=metadata,
            )
            flash(
                f"已保存博主: {creator['platform']} / {creator['creator_id']}",
                "success",
            )
        except Exception as exc:  # noqa: BLE001
            flash(f"保存博主失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/creators/toggle")
    def toggle_creator():
        platform = (request.form.get("platform") or "").strip()
        creator_id = (request.form.get("creator_id") or "").strip()
        enabled = request.form.get("enabled") == "1"
        try:
            set_creator_enabled(
                platform=platform,
                creator_id=creator_id,
                enabled=enabled,
                db_path=app.config["DB_PATH"],
            )
            state = "启用" if enabled else "停用"
            flash(f"已{state}博主: {platform} / {creator_id}", "success")
        except Exception as exc:  # noqa: BLE001
            flash(f"更新状态失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/crawl")
    def trigger_crawl():
        platform = (request.form.get("platform") or "").strip()
        creator_id = (request.form.get("creator_id") or "").strip()
        cookies_path = (request.form.get("cookies_path") or "").strip() or None
        media_root = (request.form.get("media_root") or "").strip() or "data/media"
        download_media = request.form.get("download_media") == "on"
        try:
            max_items = int((request.form.get("max_items") or "20").strip())
        except ValueError:
            max_items = 20
        max_items = max(max_items, 0)

        try:
            result = crawl_creator_by_id_and_store_sync(
                platform=platform,  # type: ignore[arg-type]
                creator_id=creator_id,
                max_items=max_items,
                cookies_path=cookies_path,
                db_path=app.config["DB_PATH"],
                download_media=download_media,
                media_root=media_root,
            )
            run_id = result["storage"]["run_id"]
            diff = result["storage"]["diff"]
            flash(
                (
                    f"抓取完成 run_id={run_id} | posts={result['storage']['saved_posts']} | "
                    f"new={diff['new_count']} updated={diff['updated_count']} missing={diff['missing_count']}"
                ),
                "success",
            )
            return redirect(url_for("run_detail", run_id=run_id))
        except Exception as exc:  # noqa: BLE001
            flash(f"抓取失败: {exc}", "error")
            return redirect(url_for("dashboard"))

    @app.get("/runs/<int:run_id>")
    def run_detail(run_id: int):
        run = _get_run(app.config["DB_PATH"], run_id)
        if run is None:
            flash(f"run_id={run_id} 不存在", "error")
            return redirect(url_for("dashboard"))

        diff_items = _list_diff_items(app.config["DB_PATH"], run_id)
        media_stats = _get_media_stats(app.config["DB_PATH"], run_id)
        post_samples = _list_run_posts(app.config["DB_PATH"], run_id, limit=100)
        return render_template(
            "run_detail.html",
            run=run,
            diff_items=diff_items,
            media_stats=media_stats,
            post_samples=post_samples,
        )

    return app


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _list_recent_runs(db_path: str, limit: int = 50) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                r.id,
                r.platform,
                r.creator_url,
                r.creator_name,
                r.crawl_time_utc,
                r.created_at_utc,
                COALESCE(d.new_count, 0) AS new_count,
                COALESCE(d.updated_count, 0) AS updated_count,
                COALESCE(d.unchanged_count, 0) AS unchanged_count,
                COALESCE(d.missing_count, 0) AS missing_count,
                (SELECT COUNT(*) FROM posts p WHERE p.run_id = r.id) AS post_count,
                (SELECT COUNT(*) FROM post_media m WHERE m.run_id = r.id) AS media_count
            FROM crawl_runs r
            LEFT JOIN crawl_diffs d ON d.run_id = r.id
            ORDER BY r.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def _get_run(db_path: str, run_id: int) -> dict[str, Any] | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                r.id,
                r.platform,
                r.creator_url,
                r.creator_name,
                r.crawl_time_utc,
                r.created_at_utc,
                COALESCE(d.new_count, 0) AS new_count,
                COALESCE(d.updated_count, 0) AS updated_count,
                COALESCE(d.unchanged_count, 0) AS unchanged_count,
                COALESCE(d.missing_count, 0) AS missing_count,
                d.summary_json,
                (SELECT COUNT(*) FROM posts p WHERE p.run_id = r.id) AS post_count,
                (SELECT COUNT(*) FROM post_media m WHERE m.run_id = r.id) AS media_count
            FROM crawl_runs r
            LEFT JOIN crawl_diffs d ON d.run_id = r.id
            WHERE r.id = ?
            """,
            (run_id,),
        ).fetchone()
    if row is None:
        return None
    result = dict(row)
    summary_raw = result.get("summary_json")
    if isinstance(summary_raw, str) and summary_raw.strip():
        try:
            result["summary"] = json.loads(summary_raw)
        except json.JSONDecodeError:
            result["summary"] = None
    else:
        result["summary"] = None
    return result


def _list_diff_items(db_path: str, run_id: int) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                post_url,
                post_id,
                change_type
            FROM crawl_diff_items
            WHERE run_id = ?
            ORDER BY
                CASE change_type
                    WHEN 'new' THEN 1
                    WHEN 'updated' THEN 2
                    WHEN 'missing' THEN 3
                    ELSE 9
                END,
                id ASC
            LIMIT 1000
            """,
            (run_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def _list_run_posts(db_path: str, run_id: int, limit: int = 100) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                post_id,
                post_url,
                title,
                like_count,
                publish_time
            FROM posts
            WHERE run_id = ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (run_id, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def _get_media_stats(db_path: str, run_id: int) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                COALESCE(download_status, 'unknown') AS download_status,
                COUNT(*) AS count
            FROM post_media
            WHERE run_id = ?
            GROUP BY COALESCE(download_status, 'unknown')
            ORDER BY count DESC
            """,
            (run_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def main() -> None:
    app = create_app()
    app.run(host="0.0.0.0", port=8000, debug=False)


if __name__ == "__main__":
    main()
