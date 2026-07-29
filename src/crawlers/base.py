from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .models import CreatorContent


class BaseCrawler(ABC):
    platform: str = ""

    def __init__(
        self,
        *,
        headless: bool = True,
        cookies_path: str | None = None,
        proxy_server: str | None = None,
        timeout_ms: int = 30000,
        max_retry_attempts: int = 3,
        retry_backoff_seconds: float = 2.0,
        min_delay_ms: int = 1200,
        max_delay_ms: int = 2400,
    ) -> None:
        self.headless = headless
        self.cookies_path = cookies_path
        self.proxy_server = proxy_server
        self.timeout_ms = timeout_ms
        self.max_retry_attempts = max(1, max_retry_attempts)
        self.retry_backoff_seconds = max(0.5, retry_backoff_seconds)
        self.min_delay_ms = max(0, min_delay_ms)
        self.max_delay_ms = max(self.min_delay_ms, max_delay_ms)
        self.crawler_meta: dict[str, Any] = {
            "retry": {"attempts": 0, "errors": []},
            "session": {"cookie_file": self.cookies_path, "cookie_health": "unknown", "warnings": []},
            "proxy": {"proxy_server": self.proxy_server, "enabled": bool(self.proxy_server)},
        }

    async def crawl(
        self,
        creator_url: str,
        max_items: int = 20,
        checkpoint: dict[str, Any] | None = None,
    ) -> CreatorContent:
        self.crawler_meta = {
            "retry": {"attempts": 0, "errors": []},
            "session": {"cookie_file": self.cookies_path, "cookie_health": "unknown", "warnings": []},
            "pagination": {},
            "proxy": {"proxy_server": self.proxy_server, "enabled": bool(self.proxy_server)},
        }
        async with async_playwright() as p:
            launch_kwargs: dict[str, Any] = {"headless": self.headless}
            if self.proxy_server:
                launch_kwargs["proxy"] = self._build_proxy_settings(self.proxy_server)
            browser = await p.chromium.launch(**launch_kwargs)
            context = await self._new_context(browser, creator_url)
            page = await context.new_page()

            await self._open_creator_page_with_retry(page, creator_url)
            result = await self._crawl_page(
                page, creator_url, max_items=max_items, checkpoint=checkpoint
            )

            await context.close()
            await browser.close()
            return result

    async def _new_context(self, browser: Browser, creator_url: str) -> BrowserContext:
        domain = urlparse(creator_url).netloc
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
            viewport={"width": 1440, "height": 900},
        )
        if self.cookies_path:
            await self._load_cookies(context, domain)
        return context

    def _build_proxy_settings(self, proxy_server: str) -> dict[str, str]:
        value = proxy_server.strip()
        if not value:
            raise ValueError("proxy_server cannot be empty")
        if "://" not in value:
            value = f"http://{value}"
        parsed = urlparse(value)
        if not parsed.scheme or not parsed.hostname:
            raise ValueError(f"invalid proxy_server: {proxy_server}")

        server = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            server += f":{parsed.port}"

        settings: dict[str, str] = {"server": server}
        if parsed.username:
            settings["username"] = parsed.username
        if parsed.password:
            settings["password"] = parsed.password
        self.crawler_meta["proxy"]["resolved_server"] = server
        self.crawler_meta["proxy"]["has_auth"] = bool(parsed.username)
        return settings

    async def _load_cookies(self, context: BrowserContext, domain: str) -> None:
        cookies_file = Path(self.cookies_path)  # type: ignore[arg-type]
        if not cookies_file.exists():
            raise FileNotFoundError(f"Cookie file not found: {cookies_file}")

        cookies_payload = json.loads(cookies_file.read_text(encoding="utf-8"))
        if not isinstance(cookies_payload, list):
            raise ValueError("Cookie file must be a JSON array")

        normalized_cookies: list[dict[str, Any]] = []
        now_ts = datetime.now(timezone.utc).timestamp()
        expiry_values: list[float] = []
        for item in cookies_payload:
            if not isinstance(item, dict):
                continue
            cookie = dict(item)
            cookie.setdefault("domain", f".{domain}")
            cookie.setdefault("path", "/")
            normalized_cookies.append(cookie)
            expires = cookie.get("expires")
            if isinstance(expires, (int, float)):
                expiry_values.append(float(expires))

        if normalized_cookies:
            await context.add_cookies(normalized_cookies)
        if expiry_values:
            latest_expiry = max(expiry_values)
            hours_left = (latest_expiry - now_ts) / 3600
            self.crawler_meta["session"]["max_cookie_expiry_utc"] = datetime.fromtimestamp(
                latest_expiry, tz=timezone.utc
            ).isoformat()
            self.crawler_meta["session"]["cookie_hours_left"] = round(hours_left, 2)
            if hours_left <= 0:
                self.crawler_meta["session"]["cookie_health"] = "expired"
                self.crawler_meta["session"]["warnings"].append("cookie expired")
            elif hours_left < 12:
                self.crawler_meta["session"]["cookie_health"] = "expiring_soon"
                self.crawler_meta["session"]["warnings"].append("cookie will expire within 12h")
            else:
                self.crawler_meta["session"]["cookie_health"] = "valid"
        else:
            self.crawler_meta["session"]["warnings"].append("cookie expiry not found")

    async def _open_creator_page_with_retry(self, page: Page, creator_url: str) -> None:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retry_attempts + 1):
            self.crawler_meta["retry"]["attempts"] = attempt
            try:
                await page.goto(creator_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                await self._sleep_with_jitter()
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                error_kind = self._classify_retry_error(exc)
                backoff_ms = self._compute_retry_backoff_ms(error_kind, attempt)
                self.crawler_meta["retry"]["errors"].append(
                    f"open_creator_page attempt={attempt} kind={error_kind} backoff_ms={backoff_ms} error={exc}"
                )
                if attempt >= self.max_retry_attempts:
                    break
                try:
                    await page.goto("about:blank", wait_until="domcontentloaded", timeout=5000)
                except Exception:  # noqa: BLE001
                    pass
                await page.wait_for_timeout(backoff_ms)
        if last_exc:
            raise last_exc

    async def _sleep_with_jitter(self) -> None:
        await asyncio.sleep(random.randint(self.min_delay_ms, self.max_delay_ms) / 1000)

    def _classify_retry_error(self, exc: Exception) -> str:
        text = str(exc).lower()
        if any(token in text for token in ["timeout", "timed out", "err_timed_out"]):
            return "network_timeout"
        if any(token in text for token in ["proxy", "tunnel", "err_proxy"]):
            return "proxy_error"
        if any(token in text for token in ["429", "too many requests", "rate limit"]):
            return "rate_limited"
        if any(token in text for token in ["captcha", "forbidden", "denied", "access"]):
            return "access_limited"
        return "unknown_error"

    def _compute_retry_backoff_ms(self, error_kind: str, attempt: int) -> int:
        base_ms = int(self.retry_backoff_seconds * 1000)
        if error_kind == "network_timeout":
            multiplier = 1.4
        elif error_kind == "proxy_error":
            multiplier = 1.8
        elif error_kind in {"rate_limited", "access_limited"}:
            multiplier = 2.5
        else:
            multiplier = 1.0
        jitter_ms = random.randint(120, 900)
        return int(base_ms * max(1.0, attempt * multiplier) + jitter_ms)

    @abstractmethod
    async def _crawl_page(
        self,
        page: Page,
        creator_url: str,
        max_items: int,
        checkpoint: dict[str, Any] | None = None,
    ) -> CreatorContent:
        raise NotImplementedError


