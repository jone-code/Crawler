from __future__ import annotations

import csv
import hmac
import json
import io
import os
import sqlite3
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any

from flask import Flask, Response, flash, redirect, render_template, request, session, url_for

from .scheduler import (
    SchedulerConfig,
    get_scheduler_runtime_state,
    run_scheduler_once_sync,
)
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
from .storage import (
    add_admin_action_log,
    init_sqlite_db,
    list_admin_action_logs,
    list_scheduler_cycle_run_items,
    list_scheduler_cycle_runs,
    set_creator_enabled,
)
from .storage.sqlite_store import DEFAULT_DB_PATH


def create_app(db_path: str | None = None) -> Flask:
    template_dir = Path(__file__).parent / "templates"
    app = Flask(__name__, template_folder=str(template_dir))
    app.secret_key = "crawler-admin-web"
    app.config["DB_PATH"] = db_path or DEFAULT_DB_PATH
    app.config["ADMIN_AUTH_USERNAME"] = (os.getenv("ADMIN_WEB_USERNAME", "admin").strip() or "admin")
    app.config["ADMIN_AUTH_PASSWORD"] = os.getenv("ADMIN_WEB_PASSWORD", "").strip()
    app.config["ADMIN_AUTH_ENABLED"] = bool(app.config["ADMIN_AUTH_PASSWORD"])

    init_sqlite_db(db_path=app.config["DB_PATH"])

    def _current_actor() -> str:
        actor = session.get("admin_user")
        if isinstance(actor, str) and actor.strip():
            return actor.strip()
        return "anonymous"

    def _audit(
        *,
        action: str,
        success: bool,
        target_type: str | None = None,
        target_id: str | None = None,
        details: dict[str, Any] | None = None,
        actor: str | None = None,
    ) -> None:
        try:
            add_admin_action_log(
                actor=(actor.strip() if isinstance(actor, str) and actor.strip() else _current_actor()),
                action=action,
                success=success,
                target_type=target_type,
                target_id=target_id,
                details=details or {},
                remote_addr=request.remote_addr,
                user_agent=request.headers.get("User-Agent"),
                db_path=app.config["DB_PATH"],
            )
        except Exception:
            # Do not block product behavior when audit write fails.
            pass

    def _auth_required(view_func):
        @wraps(view_func)
        def _wrapped(*args, **kwargs):
            if not app.config["ADMIN_AUTH_ENABLED"]:
                return view_func(*args, **kwargs)
            if session.get("admin_authenticated") is True:
                return view_func(*args, **kwargs)
            next_path = request.full_path if request.query_string else request.path
            return redirect(url_for("login", next=next_path))

        return _wrapped

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if not app.config["ADMIN_AUTH_ENABLED"]:
            return redirect(url_for("dashboard"))

        if request.method == "POST":
            username = (request.form.get("username") or "").strip()
            password = request.form.get("password") or ""
            expected_user = str(app.config["ADMIN_AUTH_USERNAME"])
            expected_password = str(app.config["ADMIN_AUTH_PASSWORD"])
            username_ok = hmac.compare_digest(username, expected_user)
            password_ok = hmac.compare_digest(password, expected_password)
            next_path = _safe_next_path(request.form.get("next"))
            if username_ok and password_ok:
                session["admin_authenticated"] = True
                session["admin_user"] = username
                _audit(
                    action="auth.login",
                    success=True,
                    target_type="admin_user",
                    target_id=username,
                    details={"next": next_path},
                    actor=username,
                )
                flash("登录成功", "success")
                return redirect(next_path or url_for("dashboard"))

            _audit(
                action="auth.login",
                success=False,
                target_type="admin_user",
                target_id=username or "unknown",
                details={"reason": "invalid_credentials"},
                actor=username or "anonymous",
            )
            flash("用户名或密码错误", "error")

        next_path = _safe_next_path(request.args.get("next"))
        return render_template(
            "login.html",
            next_path=next_path,
            auth_enabled=bool(app.config["ADMIN_AUTH_ENABLED"]),
            auth_username=str(app.config["ADMIN_AUTH_USERNAME"]),
        )

    @app.post("/logout")
    @_auth_required
    def logout():
        actor = _current_actor()
        session.pop("admin_authenticated", None)
        session.pop("admin_user", None)
        _audit(
            action="auth.logout",
            success=True,
            target_type="admin_user",
            target_id=actor,
            actor=actor,
        )
        flash("已退出登录", "success")
        return redirect(url_for("login"))

    @app.get("/")
    @_auth_required
    def dashboard():
        scheduler_runtime = get_scheduler_runtime_state(db_path=app.config["DB_PATH"])
        health_platform_raw = (request.args.get("health_platform") or "").strip().lower()
        health_platform = health_platform_raw if health_platform_raw in {"xiaohongshu", "douyin"} else None
        health_resource_type_raw = (request.args.get("health_resource_type") or "").strip().lower()
        health_resource_type = (
            health_resource_type_raw
            if health_resource_type_raw in {"account", "proxy"}
            else None
        )
        window_raw = (request.args.get("health_window_hours") or "24").strip()
        try:
            parsed_window = int(window_raw)
        except ValueError:
            parsed_window = 24
        health_window_hours = parsed_window if parsed_window in {24, 72, 168} else 24
        only_abnormal = (request.args.get("only_abnormal") or "").strip() == "1"
        scheduler_status_raw = (request.args.get("scheduler_status") or "").strip().lower()
        scheduler_status = (
            scheduler_status_raw
            if scheduler_status_raw in {"running", "success", "partial_failed", "no_due", "failed"}
            else None
        )
        scheduler_limit_raw = (request.args.get("scheduler_limit") or "20").strip()
        try:
            scheduler_limit_parsed = int(scheduler_limit_raw)
        except ValueError:
            scheduler_limit_parsed = 20
        scheduler_limit = scheduler_limit_parsed if scheduler_limit_parsed in {20, 50, 100} else 20

        creators = list_creator_ids(db_path=app.config["DB_PATH"], enabled_only=False)
        accounts = list_crawl_account_pool(db_path=app.config["DB_PATH"], enabled_only=False)
        proxies = list_crawl_proxy_pool(db_path=app.config["DB_PATH"], enabled_only=False)
        pool_health_history = list_pool_health_history(
            db_path=app.config["DB_PATH"],
            platform=health_platform,  # type: ignore[arg-type]
            resource_type=health_resource_type,
            window_hours=health_window_hours,
            only_failed=only_abnormal,
            limit=100,
        )
        pool_health_trend = list_pool_health_trend(
            db_path=app.config["DB_PATH"],
            platform=health_platform,  # type: ignore[arg-type]
            resource_type=health_resource_type,
            window_hours=health_window_hours,
            only_anomalies=only_abnormal,
            limit=100,
        )
        scheduler_cycles = list_scheduler_cycle_runs(
            db_path=app.config["DB_PATH"],
            limit=scheduler_limit,
            status=scheduler_status,
        )
        selected_cycle_id_raw = (request.args.get("selected_cycle_id") or "").strip()
        try:
            selected_cycle_id = int(selected_cycle_id_raw) if selected_cycle_id_raw else None
        except ValueError:
            selected_cycle_id = None
        latest_cycle_id = (
            int(scheduler_cycles[0]["id"])
            if scheduler_cycles and isinstance(scheduler_cycles[0], dict)
            else None
        )
        if selected_cycle_id is None:
            selected_cycle_id = latest_cycle_id
        scheduler_cycle_items = list_scheduler_cycle_run_items(
            db_path=app.config["DB_PATH"],
            cycle_run_id=selected_cycle_id,
            limit=100,
        )
        failed_scheduler_items = [
            item
            for item in scheduler_cycle_items
            if isinstance(item, dict)
            and item.get("task_type") == "crawl_creator"
            and not item.get("success")
        ]
        admin_action_logs = list_admin_action_logs(
            db_path=app.config["DB_PATH"],
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
            scheduler_runtime=scheduler_runtime,
            health_filter_platform=health_platform_raw or "all",
            health_filter_resource_type=health_resource_type_raw or "all",
            health_filter_window_hours=health_window_hours,
            health_filter_only_abnormal=only_abnormal,
            scheduler_filter_status=scheduler_status_raw or "all",
            scheduler_filter_limit=scheduler_limit,
            scheduler_cycles=scheduler_cycles,
            scheduler_cycle_items=scheduler_cycle_items,
            failed_scheduler_items=failed_scheduler_items,
            admin_action_logs=admin_action_logs,
            latest_cycle_id=latest_cycle_id,
            selected_cycle_id=selected_cycle_id,
            recent_runs=recent_runs,
            default_db_path=app.config["DB_PATH"],
            current_admin_user=_current_actor(),
            auth_enabled=bool(app.config["ADMIN_AUTH_ENABLED"]),
        )

    @app.post("/creators")
    @_auth_required
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
                _audit(
                    action="creator.save",
                    success=False,
                    target_type="creator",
                    target_id=f"{platform}:{creator_id}",
                    details={"error": str(exc), "phase": "metadata_parse"},
                )
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
            _audit(
                action="creator.save",
                success=True,
                target_type="creator",
                target_id=f"{platform}:{creator_id}",
                details={"enabled": enabled},
            )
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="creator.save",
                success=False,
                target_type="creator",
                target_id=f"{platform}:{creator_id}",
                details={"error": str(exc)},
            )
            flash(f"保存博主失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/creators/toggle")
    @_auth_required
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
            _audit(
                action="creator.toggle",
                success=True,
                target_type="creator",
                target_id=f"{platform}:{creator_id}",
                details={"enabled": enabled},
            )
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="creator.toggle",
                success=False,
                target_type="creator",
                target_id=f"{platform}:{creator_id}",
                details={"enabled": enabled, "error": str(exc)},
            )
            flash(f"更新状态失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/accounts")
    @_auth_required
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
                _audit(
                    action="account.save",
                    success=False,
                    target_type="account",
                    target_id=f"{platform}:{account_name}",
                    details={"error": str(exc), "phase": "metadata_parse"},
                )
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
            _audit(
                action="account.save",
                success=True,
                target_type="account",
                target_id=f"{platform}:{account_name}",
                details={"enabled": enabled, "priority": priority},
            )
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="account.save",
                success=False,
                target_type="account",
                target_id=f"{platform}:{account_name}",
                details={"enabled": enabled, "priority": priority, "error": str(exc)},
            )
            flash(f"保存账号失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/accounts/toggle")
    @_auth_required
    def toggle_account():
        try:
            account_id = int((request.form.get("account_id") or "0").strip())
        except ValueError:
            account_id = 0
        enabled = request.form.get("enabled") == "1"
        if account_id <= 0:
            _audit(
                action="account.toggle",
                success=False,
                target_type="account",
                target_id=str(account_id),
                details={"enabled": enabled, "error": "invalid_account_id"},
            )
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
            _audit(
                action="account.toggle",
                success=True,
                target_type="account",
                target_id=str(account_id),
                details={"enabled": enabled},
            )
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="account.toggle",
                success=False,
                target_type="account",
                target_id=str(account_id),
                details={"enabled": enabled, "error": str(exc)},
            )
            flash(f"更新账号状态失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/proxies")
    @_auth_required
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
                _audit(
                    action="proxy.save",
                    success=False,
                    target_type="proxy",
                    target_id=f"{platform}:{proxy_name}",
                    details={"error": str(exc), "phase": "metadata_parse"},
                )
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
            _audit(
                action="proxy.save",
                success=True,
                target_type="proxy",
                target_id=f"{platform}:{proxy_name}",
                details={"enabled": enabled, "priority": priority},
            )
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="proxy.save",
                success=False,
                target_type="proxy",
                target_id=f"{platform}:{proxy_name}",
                details={"enabled": enabled, "priority": priority, "error": str(exc)},
            )
            flash(f"保存代理失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/proxies/toggle")
    @_auth_required
    def toggle_proxy():
        try:
            proxy_id = int((request.form.get("proxy_id") or "0").strip())
        except ValueError:
            proxy_id = 0
        enabled = request.form.get("enabled") == "1"
        if proxy_id <= 0:
            _audit(
                action="proxy.toggle",
                success=False,
                target_type="proxy",
                target_id=str(proxy_id),
                details={"enabled": enabled, "error": "invalid_proxy_id"},
            )
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
            _audit(
                action="proxy.toggle",
                success=True,
                target_type="proxy",
                target_id=str(proxy_id),
                details={"enabled": enabled},
            )
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="proxy.toggle",
                success=False,
                target_type="proxy",
                target_id=str(proxy_id),
                details={"enabled": enabled, "error": str(exc)},
            )
            flash(f"更新代理状态失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/crawl")
    @_auth_required
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
            _audit(
                action="crawl.run",
                success=True,
                target_type="creator",
                target_id=f"{platform}:{creator_id}",
                details={
                    "run_id": run_id,
                    "max_items": max_items,
                    "use_account_pool": use_account_pool,
                    "use_proxy_pool": use_proxy_pool,
                },
            )
            return redirect(url_for("run_detail", run_id=run_id))
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="crawl.run",
                success=False,
                target_type="creator",
                target_id=f"{platform}:{creator_id}",
                details={
                    "max_items": max_items,
                    "use_account_pool": use_account_pool,
                    "use_proxy_pool": use_proxy_pool,
                    "error": str(exc),
                },
            )
            flash(f"抓取失败: {exc}", "error")
            return redirect(url_for("dashboard"))

    @app.post("/pool-health-check")
    @_auth_required
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
            _audit(
                action="pool.health_check",
                success=True,
                target_type="platform",
                target_id=platform,
                details={
                    "probe_accounts": probe_accounts,
                    "probe_proxies": probe_proxies,
                    "timeout_ms": timeout_ms,
                    "account_probe_count": summary.get("account_probe_count", 0),
                    "proxy_probe_count": summary.get("proxy_probe_count", 0),
                },
            )
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="pool.health_check",
                success=False,
                target_type="platform",
                target_id=platform,
                details={
                    "probe_accounts": probe_accounts,
                    "probe_proxies": probe_proxies,
                    "timeout_ms": timeout_ms,
                    "error": str(exc),
                },
            )
            flash(f"健康检查失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/scheduler/run-once")
    @_auth_required
    def scheduler_run_once():
        force_crawl = request.form.get("force_crawl") == "on"
        force_health_check = request.form.get("force_health_check") == "on"
        try:
            crawl_interval_minutes = int((request.form.get("crawl_interval_minutes") or "180").strip())
        except ValueError:
            crawl_interval_minutes = 180
        try:
            health_interval_minutes = int(
                (request.form.get("health_interval_minutes") or "60").strip()
            )
        except ValueError:
            health_interval_minutes = 60
        try:
            max_creators_per_cycle = int(
                (request.form.get("max_creators_per_cycle") or "0").strip()
            )
        except ValueError:
            max_creators_per_cycle = 0
        try:
            xhs_concurrency = int((request.form.get("xhs_concurrency") or "2").strip())
        except ValueError:
            xhs_concurrency = 2
        try:
            dy_concurrency = int((request.form.get("dy_concurrency") or "2").strip())
        except ValueError:
            dy_concurrency = 2

        config = SchedulerConfig(
            db_path=app.config["DB_PATH"],
            crawl_interval_minutes=max(1, crawl_interval_minutes),
            health_check_interval_minutes=max(1, health_interval_minutes),
            max_creators_per_cycle=max(0, max_creators_per_cycle),
            platform_concurrency={
                "xiaohongshu": max(1, xhs_concurrency),
                "douyin": max(1, dy_concurrency),
            },
            use_account_pool=True,
            use_proxy_pool=True,
            use_checkpoint=True,
        )
        try:
            result = run_scheduler_once_sync(
                config,
                force_crawl=force_crawl,
                force_health_check=force_health_check,
            )
            crawl_result = result.get("crawl_result")
            health_result = result.get("health_result")
            if isinstance(crawl_result, dict):
                flash(
                    "调度抓取: total={total} success={succ} failed={fail}".format(
                        total=crawl_result.get("total_creators", 0),
                        succ=crawl_result.get("success_count", 0),
                        fail=crawl_result.get("failed_count", 0),
                    ),
                    "success",
                )
            if isinstance(health_result, dict):
                flash(
                    "调度健康检查: success={succ} failed={fail}".format(
                        succ=health_result.get("success_count", 0),
                        fail=health_result.get("failed_count", 0),
                    ),
                    "success",
                )
            if not isinstance(crawl_result, dict) and not isinstance(health_result, dict):
                flash("调度周期已执行，但未达到到期条件（未触发任务）", "success")
            _audit(
                action="scheduler.run_once",
                success=True,
                target_type="scheduler",
                target_id="crawler-main",
                details={
                    "force_crawl": force_crawl,
                    "force_health_check": force_health_check,
                    "cycle_status": result.get("cycle_status"),
                    "cycle_run_id": result.get("cycle_run_id"),
                },
            )
        except Exception as exc:  # noqa: BLE001
            _audit(
                action="scheduler.run_once",
                success=False,
                target_type="scheduler",
                target_id="crawler-main",
                details={
                    "force_crawl": force_crawl,
                    "force_health_check": force_health_check,
                    "error": str(exc),
                },
            )
            flash(f"调度执行失败: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/scheduler/retry-failed")
    @_auth_required
    def scheduler_retry_failed():
        try:
            cycle_run_id = int((request.form.get("cycle_run_id") or "0").strip())
        except ValueError:
            cycle_run_id = 0
        try:
            max_retry_items = int((request.form.get("max_retry_items") or "10").strip())
        except ValueError:
            max_retry_items = 10
        max_retry_items = max(1, min(max_retry_items, 50))
        try:
            max_items = int((request.form.get("max_items") or "20").strip())
        except ValueError:
            max_items = 20
        max_items = max(0, max_items)

        headless = request.form.get("headless") == "on"
        use_account_pool = request.form.get("use_account_pool") == "on"
        use_proxy_pool = request.form.get("use_proxy_pool") == "on"
        use_checkpoint = request.form.get("use_checkpoint") == "on"
        download_media = request.form.get("download_media") == "on"

        if cycle_run_id <= 0:
            _audit(
                action="scheduler.retry_failed",
                success=False,
                target_type="scheduler_cycle",
                target_id=str(cycle_run_id),
                details={"error": "invalid_cycle_run_id"},
            )
            flash("周期ID无效，无法重试", "error")
            return redirect(url_for("dashboard"))

        cycle_items = list_scheduler_cycle_run_items(
            db_path=app.config["DB_PATH"],
            cycle_run_id=cycle_run_id,
            limit=2000,
        )
        candidates: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for item in cycle_items:
            if not isinstance(item, dict):
                continue
            if item.get("task_type") != "crawl_creator":
                continue
            if bool(item.get("success")):
                continue
            platform = str(item.get("platform") or "").strip().lower()
            creator_id = str(item.get("creator_id") or "").strip()
            if platform not in {"xiaohongshu", "douyin"} or not creator_id:
                continue
            key = (platform, creator_id)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(key)
            if len(candidates) >= max_retry_items:
                break

        if not candidates:
            _audit(
                action="scheduler.retry_failed",
                success=False,
                target_type="scheduler_cycle",
                target_id=str(cycle_run_id),
                details={"error": "no_failed_crawl_items"},
            )
            flash("当前周期没有可重试的失败抓取任务", "error")
            return redirect(url_for("dashboard", selected_cycle_id=cycle_run_id))

        success_count = 0
        failed_count = 0
        failed_messages: list[str] = []
        run_ids: list[int] = []
        for platform, creator_id in candidates:
            try:
                result = crawl_creator_by_id_and_store_sync(
                    platform=platform,  # type: ignore[arg-type]
                    creator_id=creator_id,
                    max_items=max_items,
                    headless=headless,
                    db_path=app.config["DB_PATH"],
                    download_media=download_media,
                    use_checkpoint=use_checkpoint,
                    use_account_pool=use_account_pool,
                    use_proxy_pool=use_proxy_pool,
                )
                run_id = result.get("storage", {}).get("run_id")
                if isinstance(run_id, int) and run_id > 0:
                    run_ids.append(run_id)
                success_count += 1
            except Exception as exc:  # noqa: BLE001
                failed_count += 1
                failed_messages.append(f"{platform}/{creator_id}: {exc}")

        flash(
            (
                f"失败任务重试完成: cycle={cycle_run_id} total={len(candidates)} "
                f"success={success_count} failed={failed_count}"
            ),
            "success" if failed_count == 0 else "error",
        )
        for item in failed_messages[:5]:
            flash(f"重试失败: {item}", "error")

        _audit(
            action="scheduler.retry_failed",
            success=failed_count == 0,
            target_type="scheduler_cycle",
            target_id=str(cycle_run_id),
            details={
                "total": len(candidates),
                "success_count": success_count,
                "failed_count": failed_count,
                "max_retry_items": max_retry_items,
                "max_items": max_items,
                "use_account_pool": use_account_pool,
                "use_proxy_pool": use_proxy_pool,
                "run_ids": run_ids[:20],
                "failed_messages": failed_messages[:10],
            },
        )
        return redirect(url_for("dashboard", selected_cycle_id=cycle_run_id))

    @app.get("/runs/<int:run_id>")
    @_auth_required
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

    @app.get("/runs/<int:run_id>/export.json")
    @_auth_required
    def run_export_json(run_id: int):
        run = _get_run(app.config["DB_PATH"], run_id)
        if run is None:
            _audit(
                action="run.export_json",
                success=False,
                target_type="run",
                target_id=str(run_id),
                details={"error": "run_not_found"},
            )
            flash(f"run_id={run_id} 不存在", "error")
            return redirect(url_for("dashboard"))
        payload = {
            "exported_at_utc": datetime.now(timezone.utc).isoformat(),
            "run": run,
            "diff_items": _list_diff_items(app.config["DB_PATH"], run_id),
            "posts": _list_run_posts_full(app.config["DB_PATH"], run_id),
            "media_items": _list_run_media(app.config["DB_PATH"], run_id),
        }
        body = json.dumps(payload, ensure_ascii=False, indent=2)
        filename = _build_run_export_filename(run, suffix="full", extension="json")
        _audit(
            action="run.export_json",
            success=True,
            target_type="run",
            target_id=str(run_id),
            details={"filename": filename},
        )
        return Response(
            body,
            mimetype="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.get("/runs/<int:run_id>/export.csv")
    @_auth_required
    def run_export_csv(run_id: int):
        run = _get_run(app.config["DB_PATH"], run_id)
        if run is None:
            _audit(
                action="run.export_csv",
                success=False,
                target_type="run",
                target_id=str(run_id),
                details={"error": "run_not_found"},
            )
            flash(f"run_id={run_id} 不存在", "error")
            return redirect(url_for("dashboard"))
        diff_items = _list_diff_items(app.config["DB_PATH"], run_id)
        diff_map: dict[str, str] = {}
        for item in diff_items:
            if not isinstance(item, dict):
                continue
            post_url = item.get("post_url")
            if not isinstance(post_url, str) or not post_url:
                continue
            if post_url in diff_map:
                continue
            diff_map[post_url] = str(item.get("change_type") or "")

        posts = _list_run_posts_full(app.config["DB_PATH"], run_id)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "run_id",
                "platform",
                "creator_url",
                "post_id",
                "post_url",
                "title",
                "description",
                "cover_url",
                "like_count",
                "comment_count",
                "share_count",
                "publish_time",
                "change_type",
            ]
        )
        for post in posts:
            post_url = str(post.get("post_url") or "")
            writer.writerow(
                [
                    run.get("id"),
                    run.get("platform"),
                    run.get("creator_url"),
                    post.get("post_id"),
                    post_url,
                    post.get("title"),
                    post.get("description"),
                    post.get("cover_url"),
                    post.get("like_count"),
                    post.get("comment_count"),
                    post.get("share_count"),
                    post.get("publish_time"),
                    diff_map.get(post_url, ""),
                ]
            )
        filename = _build_run_export_filename(run, suffix="posts", extension="csv")
        _audit(
            action="run.export_csv",
            success=True,
            target_type="run",
            target_id=str(run_id),
            details={"filename": filename, "rows": len(posts)},
        )
        return Response(
            output.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return app


def _safe_next_path(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not text.startswith("/"):
        return ""
    if text.startswith("//"):
        return ""
    if "\n" in text or "\r" in text:
        return ""
    return text


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


def _list_run_posts_full(db_path: str, run_id: int) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                post_id,
                post_url,
                title,
                description,
                cover_url,
                like_count,
                comment_count,
                share_count,
                publish_time
            FROM posts
            WHERE run_id = ?
            ORDER BY id ASC
            """,
            (run_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def _list_run_media(db_path: str, run_id: int) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                post_id,
                post_url,
                media_type,
                media_url,
                local_path,
                download_status,
                file_size,
                error
            FROM post_media
            WHERE run_id = ?
            ORDER BY id ASC
            """,
            (run_id,),
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


def _build_run_export_filename(
    run: dict[str, Any],
    *,
    suffix: str,
    extension: str,
) -> str:
    run_id = int(run.get("id") or 0)
    platform = _safe_file_fragment(run.get("platform"))
    creator_name = _safe_file_fragment(run.get("creator_name"))
    if creator_name == "unknown":
        creator_name = _safe_file_fragment(run.get("creator_url"))
    return f"run-{run_id}-{platform}-{creator_name}-{suffix}.{extension}"


def _safe_file_fragment(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unknown"
    output_chars: list[str] = []
    for ch in text:
        if ("a" <= ch <= "z") or ("0" <= ch <= "9"):
            output_chars.append(ch)
        elif ch in {"-", "_", "."}:
            output_chars.append(ch)
        else:
            output_chars.append("-")
    compact = "".join(output_chars).strip("-")
    return compact[:48] if compact else "unknown"


def main() -> None:
    app = create_app()
    app.run(host="0.0.0.0", port=8000, debug=False)


if __name__ == "__main__":
    main()
