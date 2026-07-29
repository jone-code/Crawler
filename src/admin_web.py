from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from flask import Flask, flash, redirect, render_template, request, url_for

from .service import (
    add_crawl_account,
    add_crawl_proxy,
    add_creator_id,
    crawl_creator_by_id_and_store_sync,
    list_crawl_account_pool,
    list_crawl_proxy_pool,
    list_creator_ids,
    list_pool_health_history,
    list_pool_health_trend,
    probe_pool_health_sync,
    toggle_crawl_account,
    toggle_crawl_proxy,
)
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
        accounts = list_crawl_account_pool(db_path=app.config["DB_PATH"], enabled_only=False)
        proxies = list_crawl_proxy_pool(db_path=app.config["DB_PATH"], enabled_only=False)
        pool_health_history = list_pool_health_history(
            db_path=app.config["DB_PATH"],
            limit=100,
        )
        pool_health_trend = list_pool_health_trend(
            db_path=app.config["DB_PATH"],
            window_hours=24,
            limit=100,
        )
        recent_runs = _list_recent_runs(app.config["DB_PATH"], limit=50)
        return render_template(
            "dashboard.html",
            creators=creators,
            accounts=accounts,
            proxies=proxies,
            pool_health_history=pool_health_history,
            pool_health_trend=pool_health_trend,
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

    @app.post("/accounts")
    def create_account():
        platform = (request.form.get("platform") or "").strip()
        account_name = (request.form.get("account_name") or "").strip()
        cookies_path = (request.form.get("cookies_path") or "").strip()
        metadata_text = (request.form.get("metadata_json") or "").strip()
        enabled = request.form.get("enabled") == "on"
        try:
            priority = int((request.form.get("priority") or "100").strip())
        except ValueError:
            priority = 100
        metadata: dict[str, Any] | None = None
        if metadata_text:
            try:
                parsed = json.loads(metadata_text)
                if isinstance(parsed, dict):
                    metadata = parsed
            except json.JSONDecodeError as exc:
                flash(f"账号 metadata JSON 解析失败: {exc}", "error")
                return redirect(url_for("dashboard"))
        try:
            account = add_crawl_account(
                platform=platform,  # type: ignore[arg-type]
                account_name=account_name,
                cookies_path=cookies_path,
                db_path=app.config["DB_PATH"],
                enabled=enabled,
                priority=priority,
                metadata=metadata,
            )
            flash(
                f"已保存账号: {account['platform']} / {account['account_name']}",
                "success",
            )
        except Exception as exc:  # noqa: BLE001
            flash(f"保存账号失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/accounts/toggle")
    def toggle_account():
        try:
            account_id = int((request.form.get("account_id") or "0").strip())
        except ValueError:
            account_id = 0
        enabled = request.form.get("enabled") == "1"
        if account_id <= 0:
            flash("账号ID无效", "error")
            return redirect(url_for("dashboard"))
        try:
            toggle_crawl_account(
                account_id=account_id,
                enabled=enabled,
                db_path=app.config["DB_PATH"],
            )
            state = "启用" if enabled else "停用"
            flash(f"已{state}账号 id={account_id}", "success")
        except Exception as exc:  # noqa: BLE001
            flash(f"更新账号状态失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/proxies")
    def create_proxy():
        platform = (request.form.get("platform") or "").strip()
        proxy_name = (request.form.get("proxy_name") or "").strip()
        proxy_url = (request.form.get("proxy_url") or "").strip()
        metadata_text = (request.form.get("metadata_json") or "").strip()
        enabled = request.form.get("enabled") == "on"
        try:
            priority = int((request.form.get("priority") or "100").strip())
        except ValueError:
            priority = 100
        metadata: dict[str, Any] | None = None
        if metadata_text:
            try:
                parsed = json.loads(metadata_text)
                if isinstance(parsed, dict):
                    metadata = parsed
            except json.JSONDecodeError as exc:
                flash(f"代理 metadata JSON 解析失败: {exc}", "error")
                return redirect(url_for("dashboard"))
        try:
            proxy = add_crawl_proxy(
                platform=platform,  # type: ignore[arg-type]
                proxy_name=proxy_name,
                proxy_url=proxy_url,
                db_path=app.config["DB_PATH"],
                enabled=enabled,
                priority=priority,
                metadata=metadata,
            )
            flash(
                f"已保存代理: {proxy['platform']} / {proxy['proxy_name']}",
                "success",
            )
        except Exception as exc:  # noqa: BLE001
            flash(f"保存代理失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/proxies/toggle")
    def toggle_proxy():
        try:
            proxy_id = int((request.form.get("proxy_id") or "0").strip())
        except ValueError:
            proxy_id = 0
        enabled = request.form.get("enabled") == "1"
        if proxy_id <= 0:
            flash("代理ID无效", "error")
            return redirect(url_for("dashboard"))
        try:
            toggle_crawl_proxy(
                proxy_id=proxy_id,
                enabled=enabled,
                db_path=app.config["DB_PATH"],
            )
            state = "启用" if enabled else "停用"
            flash(f"已{state}代理 id={proxy_id}", "success")
        except Exception as exc:  # noqa: BLE001
            flash(f"更新代理状态失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/crawl")
    def trigger_crawl():
        platform = (request.form.get("platform") or "").strip()
        creator_id = (request.form.get("creator_id") or "").strip()
        cookies_path = (request.form.get("cookies_path") or "").strip() or None
        media_root = (request.form.get("media_root") or "").strip() or "data/media"
        download_media = request.form.get("download_media") == "on"
        use_account_pool = request.form.get("use_account_pool") == "on"
        use_proxy_pool = request.form.get("use_proxy_pool") == "on"
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
                use_account_pool=use_account_pool,
                use_proxy_pool=use_proxy_pool,
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
            session_meta = (
                result.get("crawl", {})
                .get("crawler_meta", {})
                .get("session", {})
            )
            warnings = session_meta.get("warnings")
            runtime_status = session_meta.get("runtime_status")
            if isinstance(warnings, list):
                for warning in warnings:
                    flash(f"会话告警: {warning}", "error")
            if runtime_status in {"limited", "no_data"}:
                flash(f"会话状态: {runtime_status}", "error")
            account_meta = result.get("crawl", {}).get("crawler_meta", {}).get("account", {})
            if isinstance(account_meta, dict) and account_meta.get("account_name"):
                flash(
                    f"账号池命中: {account_meta.get('account_name')} ({account_meta.get('health')})",
                    "success",
                )
            proxy_meta = result.get("crawl", {}).get("crawler_meta", {}).get("proxy", {})
            if isinstance(proxy_meta, dict) and proxy_meta.get("proxy_name"):
                flash(
                    f"代理池命中: {proxy_meta.get('proxy_name')} ({proxy_meta.get('health')})",
                    "success",
                )
            scheduler_meta = result.get("crawl", {}).get("crawler_meta", {}).get("scheduler", {})
            if isinstance(scheduler_meta, dict):
                attempts_total = scheduler_meta.get("attempts_total")
                if isinstance(attempts_total, int):
                    flash(f"调度尝试次数: {attempts_total}", "success")
            return redirect(url_for("run_detail", run_id=run_id))
        except Exception as exc:  # noqa: BLE001
            flash(f"抓取失败: {exc}", "error")
            return redirect(url_for("dashboard"))

    @app.post("/pool-health-check")
    def pool_health_check():
        platform = (request.form.get("platform") or "").strip()
        probe_accounts = request.form.get("probe_accounts") == "on"
        probe_proxies = request.form.get("probe_proxies") == "on"
        headless = request.form.get("headless") == "on"
        try:
            timeout_ms = int((request.form.get("timeout_ms") or "12000").strip())
        except ValueError:
            timeout_ms = 12000
        timeout_ms = max(3000, min(timeout_ms, 60000))

        try:
            summary = probe_pool_health_sync(
                platform=platform,  # type: ignore[arg-type]
                db_path=app.config["DB_PATH"],
                probe_accounts=probe_accounts,
                probe_proxies=probe_proxies,
                timeout_ms=timeout_ms,
                headless=headless,
            )
            flash(
                (
                    f"健康检查完成: accounts={summary.get('account_ok_count', 0)}/"
                    f"{summary.get('account_probe_count', 0)} | proxies={summary.get('proxy_ok_count', 0)}/"
                    f"{summary.get('proxy_probe_count', 0)}"
                ),
                "success",
            )
            for item in summary.get("accounts", []):
                if isinstance(item, dict) and not item.get("success"):
                    flash(
                        f"账号异常: {item.get('account_name')} | {item.get('failure_kind')} | {item.get('error')}",
                        "error",
                    )
            for item in summary.get("proxies", []):
                if isinstance(item, dict) and not item.get("success"):
                    flash(
                        f"代理异常: {item.get('proxy_name')} | {item.get('failure_kind')} | {item.get('error')}",
                        "error",
                    )
        except Exception as exc:  # noqa: BLE001
            flash(f"健康检查失败: {exc}", "error")
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
