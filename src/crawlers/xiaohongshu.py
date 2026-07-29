from __future__ import annotations

import re
from typing import Any

from playwright.async_api import Page

from .base import BaseCrawler
from .models import CreatorContent, CreatorPost


class XiaohongshuCrawler(BaseCrawler):
    platform = "xiaohongshu"

    async def _crawl_page(
        self,
        page: Page,
        creator_url: str,
        max_items: int,
        checkpoint: dict[str, Any] | None = None,
    ) -> CreatorContent:
        cards, pagination_meta = await self._collect_cards_with_pagination(
            page, max_items=max_items, checkpoint=checkpoint
        )
        creator_name = await self._extract_creator_name(page)
        self.crawler_meta["pagination"] = pagination_meta
        self._update_session_runtime_status(
            cards_count=len(cards),
            page_title=(await page.title()).strip(),
        )

        posts = [
            CreatorPost(
                platform=self.platform,
                creator_url=creator_url,
                post_id=self._extract_post_id(card.get("post_url", "")),
                post_url=card.get("post_url", ""),
                title=card.get("title"),
                description=card.get("description"),
                cover_url=card.get("cover_url"),
                like_count=self._parse_count(card.get("like_text")),
                image_urls=[],
                video_urls=[],
                media_assets=[],
                raw=card,
            )
            for card in cards
            if card.get("post_url")
        ]

        await self._enrich_posts_with_details(page, posts)

        return CreatorContent.create(
            platform=self.platform,
            creator_url=creator_url,
            creator_name=creator_name,
            posts=posts,
            crawler_meta=self.crawler_meta,
        )

    async def _collect_cards_with_pagination(
        self, page: Page, *, max_items: int, checkpoint: dict[str, Any] | None
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        target_count = max_items if max_items > 0 else None
        cards_map: dict[str, dict[str, Any]] = {}
        previous_height = -1
        previous_total = 0
        stable_rounds = 0
        anti_bot_events = 0
        rounds = 0
        stop_reason = "unknown"

        known_recent_urls: set[str] = set()
        if isinstance(checkpoint, dict):
            urls = checkpoint.get("known_recent_post_urls")
            if isinstance(urls, list):
                for item in urls:
                    if isinstance(item, str) and item.strip():
                        known_recent_urls.add(item.strip())
        checkpoint_hit = False

        for _ in range(120):
            rounds += 1
            visible_cards = await self._extract_visible_cards(page)
            if not visible_cards and await self._looks_access_limited(page):
                anti_bot_events += 1
                self.crawler_meta["retry"]["errors"].append(
                    f"access limited during pagination round={rounds}"
                )
                if anti_bot_events <= self.max_retry_attempts:
                    await page.reload(wait_until="domcontentloaded", timeout=self.timeout_ms)
                    await self._sleep_with_jitter()
                    continue
            for card in visible_cards:
                post_url = card.get("post_url")
                if not isinstance(post_url, str) or not post_url:
                    continue
                if known_recent_urls and post_url in known_recent_urls:
                    checkpoint_hit = True
                    continue
                existing = cards_map.get(post_url)
                if existing is None:
                    cards_map[post_url] = card
                else:
                    cards_map[post_url] = self._merge_card(existing, card)

            if target_count is not None and len(cards_map) >= target_count:
                stop_reason = "target_reached"
                break
            if target_count is None and known_recent_urls and checkpoint_hit:
                if cards_map:
                    stop_reason = "checkpoint_hit"
                else:
                    stop_reason = "checkpoint_hit_no_new_posts"
                break

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await self._sleep_with_jitter()
            current_height = await page.evaluate("document.body.scrollHeight")
            current_total = len(cards_map)

            if current_height <= previous_height and current_total <= previous_total:
                stable_rounds += 1
            else:
                stable_rounds = 0
            if stable_rounds >= 5:
                stop_reason = "stable_rounds_exceeded"
                break

            previous_height = current_height
            previous_total = current_total
        else:
            stop_reason = "max_rounds_reached"

        cards = list(cards_map.values())
        if stop_reason == "unknown":
            stop_reason = "finished"
        if target_count is not None:
            cards = cards[:target_count]
        pagination_meta = {
            "rounds": rounds,
            "collected_cards": len(cards),
            "checkpoint_used": bool(known_recent_urls),
            "checkpoint_hit": checkpoint_hit,
            "known_recent_post_urls": list(known_recent_urls)[:10],
            "anti_bot_events": anti_bot_events,
            "partial_crawl": bool(known_recent_urls),
            "stop_reason": stop_reason,
        }
        return cards, pagination_meta

    async def _extract_visible_cards(self, page: Page) -> list[dict[str, Any]]:
        return await page.evaluate(
            r"""
            () => {
              const normalizeUrl = (value) => {
                if (!value) return null;
                let resolved = value;
                if (value.startsWith("//")) resolved = `https:${value}`;
                if (value.startsWith("/")) resolved = `https://www.xiaohongshu.com${value}`;
                try {
                  const parsed = new URL(resolved);
                  parsed.search = "";
                  parsed.hash = "";
                  return parsed.toString();
                } catch {
                  return resolved;
                }
              };
              const normalizeDetailUrl = (value) => {
                if (!value) return null;
                if (value.startsWith("//")) return `https:${value}`;
                if (value.startsWith("/")) return `https://www.xiaohongshu.com${value}`;
                return value;
              };

              const unique = new Map();
              const links = Array.from(document.querySelectorAll('a[href*="/explore/"]'));
              for (const link of links) {
                const href = normalizeUrl(link.getAttribute("href"));
                if (!href) continue;
                if (!/\/explore\/[a-zA-Z0-9]+/.test(href)) continue;
                if (unique.has(href)) continue;

                const cardRoot = link.parentElement;
                const cardText = (cardRoot?.innerText || "").trim();
                const textLines = cardText
                  .split("\n")
                  .map(item => item.trim())
                  .filter(Boolean);
                const titleNode = cardRoot?.querySelector("img[alt], [class*='title'], [class*='desc']");
                const profileLink = cardRoot?.querySelector("a.cover[href*='/user/profile/'], a[href*='/user/profile/']") || null;
                const detailHref = normalizeDetailUrl(profileLink?.getAttribute("href") || null);
                const title =
                  titleNode?.getAttribute?.("alt") ||
                  link.getAttribute("title") ||
                  link.getAttribute("aria-label") ||
                  textLines[0] ||
                  null;
                const likeText = textLines.length >= 3 ? textLines[textLines.length - 1] : null;
                const cover = normalizeUrl(
                  cardRoot?.querySelector("img")?.getAttribute("src") ||
                  link.querySelector("img")?.getAttribute("src") ||
                  null
                );

                unique.set(href, {
                  post_url: href,
                  title: title || null,
                  description: title || null,
                  cover_url: cover,
                  detail_url: detailHref,
                  card_text_lines: textLines,
                  like_text: likeText,
                });
              }
              return Array.from(unique.values());
            }
            """
        )

    def _merge_card(self, existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        merged = dict(existing)
        for key in [
            "title",
            "description",
            "cover_url",
            "detail_url",
            "like_text",
        ]:
            existing_value = merged.get(key)
            incoming_value = incoming.get(key)
            if (not existing_value) and incoming_value:
                merged[key] = incoming_value
        if isinstance(incoming.get("card_text_lines"), list) and not merged.get("card_text_lines"):
            merged["card_text_lines"] = incoming["card_text_lines"]
        return merged

    async def _looks_access_limited(self, page: Page) -> bool:
        return await page.evaluate(
            r"""
            () => {
              const title = (document.title || "").trim();
              const generic = title === "小红书 - 你的生活兴趣社区" || title === "小红书";
              const hasLoginHints = /(登录|扫码登录|注册)/.test(document.body?.innerText || "");
              const realExploreLinks = Array.from(document.querySelectorAll("a[href*='/explore/']")).filter(
                (a) => /\/explore\/[a-zA-Z0-9]+/.test(a.getAttribute("href") || "")
              );
              return generic && (hasLoginHints || realExploreLinks.length === 0);
            }
            """
        )

    async def _extract_creator_name(self, page: Page) -> str | None:
        page_title = (await page.title()).strip()
        if page_title and " - " in page_title:
            primary = page_title.split(" - ")[0].strip()
            if primary and primary not in {"小红书"}:
                return primary

        selectors = [
            "meta[property='og:title']",
            "h1",
            "[class*='user-name']",
            "[class*='nickname']",
        ]
        for selector in selectors:
            element = page.locator(selector).first
            if await element.count():
                if selector.startswith("meta"):
                    value = await element.get_attribute("content")
                else:
                    value = await element.text_content()
                text = (value or "").strip()
                if text:
                    return text
        return None

    def _extract_post_id(self, post_url: str) -> str:
        match = re.search(r"/explore/([a-zA-Z0-9]+)", post_url)
        return match.group(1) if match else post_url

    async def _enrich_posts_with_details(self, page: Page, posts: list[CreatorPost]) -> None:
        for post in posts:
            raw_detail_url = post.raw.get("detail_url")
            detail_url = (
                raw_detail_url
                if isinstance(raw_detail_url, str) and raw_detail_url.strip()
                else post.post_url
            )
            if not detail_url:
                continue

            try:
                details = await self._fetch_detail_with_retry(page, detail_url)
            except Exception as exc:  # noqa: BLE001
                post.raw["detail_error"] = str(exc)
                continue

            title = self._clean_title(details.get("title"))
            description = self._clean_description(details.get("description"))
            cover_url = self._normalize_cover_url(details.get("cover_url"))
            publish_time = details.get("publish_time")
            page_title = self._clean_title(details.get("page_title"))
            image_urls = self._normalize_urls(details.get("image_urls"))
            video_urls = self._normalize_urls(details.get("video_urls"))
            media_assets = self._normalize_media_assets(details.get("media_assets"))

            if title:
                post.title = title
            elif page_title:
                post.title = page_title

            if description and description != "3 亿人的生活经验，都在小红书":
                post.description = description

            if cover_url:
                post.cover_url = cover_url

            if isinstance(publish_time, str) and publish_time.strip():
                post.publish_time = publish_time.strip()

            if image_urls:
                post.image_urls = image_urls
            if video_urls:
                post.video_urls = video_urls
            if media_assets:
                post.media_assets = media_assets

            post.raw["detail_url"] = detail_url
            post.raw["detail_meta"] = details

    async def _fetch_detail_with_retry(self, page: Page, detail_url: str) -> dict[str, Any]:
        last_details: dict[str, Any] = {}
        for attempt in range(1, self.max_retry_attempts + 1):
            try:
                await page.goto(detail_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                await self._sleep_with_jitter()
                details = await page.evaluate(
                    """
                    () => {
                      const pick = (selector, attr = "content") =>
                        document.querySelector(selector)?.getAttribute(attr) || null;
                      const pageTitle = document.title || null;
                      const normalize = (value) => {
                        if (!value || typeof value !== "string") return null;
                        const text = value.trim();
                        if (!text) return null;
                        if (text.startsWith("//")) return `https:${text}`;
                        return text;
                      };
                      const pickList = (item, keys) => {
                        const out = [];
                        for (const key of keys) {
                          const value = item?.[key];
                          if (typeof value === "string") out.push(value);
                          if (Array.isArray(value)) {
                            for (const sub of value) {
                              if (typeof sub === "string") out.push(sub);
                              if (sub && typeof sub === "object") {
                                for (const subValue of Object.values(sub)) {
                                  if (typeof subValue === "string") out.push(subValue);
                                }
                              }
                            }
                          }
                        }
                        return out;
                      };
                      const pushUnique = (arr, value) => {
                        const normalized = normalize(value);
                        if (!normalized) return;
                        if (!arr.includes(normalized)) arr.push(normalized);
                      };
                      const looksLikeVideo = (value) => {
                        const text = value.toLowerCase();
                        return (
                          text.includes(".mp4") ||
                          text.includes(".m3u8") ||
                          text.includes("video") ||
                          text.includes("stream")
                        );
                      };
                      const looksLikeImage = (value) => {
                        const text = value.toLowerCase();
                        return (
                          text.includes(".jpg") ||
                          text.includes(".jpeg") ||
                          text.includes(".png") ||
                          text.includes(".webp") ||
                          text.includes("xhscdn.com")
                        );
                      };
                      const state = window.__INITIAL_STATE__ || {};
                      const noteState = state.note || {};
                      const detailMap = noteState.noteDetailMap || {};
                      const detailMapValues = Object.values(detailMap);
                      let noteObject = null;
                      for (const item of detailMapValues) {
                        if (item?.note && Object.keys(item.note).length > 0) {
                          noteObject = item.note;
                          break;
                        }
                      }
                      const imageUrls = [];
                      const videoUrls = [];
                      const mediaAssets = [];
                      const appendMedia = (type, url) => {
                        const normalized = normalize(url);
                        if (!normalized) return;
                        if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) return;
                        if (type === "video") {
                          if (!videoUrls.includes(normalized)) videoUrls.push(normalized);
                        } else {
                          if (!imageUrls.includes(normalized)) imageUrls.push(normalized);
                        }
                        if (!mediaAssets.find(item => item.media_type === type && item.url === normalized)) {
                          mediaAssets.push({ media_type: type, url: normalized });
                        }
                      };

                      if (noteObject) {
                        const imageList = noteObject.imageList || noteObject.images || [];
                        for (const imageItem of imageList) {
                          const candidates = [
                            ...pickList(imageItem, ["urlDefault", "urlPre", "url", "urlLarge", "urlOrigin"]),
                            ...pickList(imageItem, ["infoList"]),
                          ];
                          for (const candidate of candidates) {
                            if (typeof candidate === "string") {
                              appendMedia("image", candidate);
                            }
                          }
                        }

                        const queue = [noteObject.video, noteObject.noteVideo];
                        const visited = new Set();
                        while (queue.length) {
                          const current = queue.pop();
                          if (!current || typeof current !== "object") continue;
                          if (visited.has(current)) continue;
                          visited.add(current);

                          for (const value of Object.values(current)) {
                            if (!value) continue;
                            if (typeof value === "string") {
                              if (value.startsWith("http") || value.startsWith("//")) {
                                if (looksLikeVideo(value)) {
                                  appendMedia("video", value);
                                } else if (looksLikeImage(value)) {
                                  appendMedia("image", value);
                                }
                              }
                            } else if (Array.isArray(value)) {
                              for (const sub of value) {
                                if (sub && typeof sub === "object") queue.push(sub);
                                if (typeof sub === "string" && (sub.startsWith("http") || sub.startsWith("//"))) {
                                  if (looksLikeVideo(sub)) {
                                    appendMedia("video", sub);
                                  } else if (looksLikeImage(sub)) {
                                    appendMedia("image", sub);
                                  }
                                }
                              }
                            } else if (typeof value === "object") {
                              queue.push(value);
                            }
                          }
                        }
                      }

                      const metaCover =
                        pick("meta[property='og:image']") ||
                        pick("meta[name='og:image']") ||
                        null;
                      if (metaCover) appendMedia("image", metaCover);

                      return {
                        title:
                          pick("meta[property='og:title']") ||
                          pick("meta[name='title']") ||
                          null,
                        description:
                          pick("meta[property='og:description']") ||
                          pick("meta[name='description']") ||
                          null,
                        cover_url:
                          pick("meta[property='og:image']") ||
                          pick("meta[name='og:image']") ||
                          null,
                        publish_time:
                          pick("meta[property='article:published_time']") ||
                          pick("meta[name='article:published_time']") ||
                          null,
                        page_title: pageTitle,
                        image_urls: imageUrls,
                        video_urls: videoUrls,
                        media_assets: mediaAssets,
                      };
                    }
                    """
                )
                last_details = details
                if not self._is_detail_limited(details):
                    return details
                self.crawler_meta["retry"]["errors"].append(
                    f"detail limited attempt={attempt} url={detail_url}"
                )
                if attempt < self.max_retry_attempts:
                    await page.wait_for_timeout(
                        int(self.retry_backoff_seconds * 1000 * attempt)
                    )
            except Exception as exc:  # noqa: BLE001
                self.crawler_meta["retry"]["errors"].append(
                    f"detail fetch error attempt={attempt} url={detail_url} error={exc}"
                )
                if attempt >= self.max_retry_attempts:
                    raise
                await page.wait_for_timeout(int(self.retry_backoff_seconds * 1000 * attempt))
        return last_details

    def _is_detail_limited(self, details: dict[str, Any]) -> bool:
        title = self._clean_title(details.get("title"))
        description = self._clean_description(details.get("description"))
        page_title = self._clean_title(details.get("page_title"))
        has_media = bool(details.get("image_urls") or details.get("video_urls"))
        return not any([title, description, page_title, has_media])

    def _update_session_runtime_status(self, *, cards_count: int, page_title: str) -> None:
        session = self.crawler_meta.get("session", {})
        warnings = session.get("warnings")
        if not isinstance(warnings, list):
            warnings = []
            session["warnings"] = warnings
        if cards_count > 0:
            session["runtime_status"] = "ok"
        elif page_title in {"小红书", "小红书 - 你的生活兴趣社区"}:
            session["runtime_status"] = "limited"
            warnings.append("runtime indicates access limited")
        else:
            session["runtime_status"] = "no_data"
        if warnings:
            deduped = list(dict.fromkeys(str(item) for item in warnings))
            session["warnings"] = deduped
        self.crawler_meta["session"] = session

    def _parse_count(self, value: Any) -> int | None:
        if not isinstance(value, str):
            return None
        raw = value.strip().lower().replace(",", "")
        if not raw:
            return None
        if raw.endswith("w"):
            try:
                return int(float(raw[:-1]) * 10000)
            except ValueError:
                return None
        if raw.endswith("万"):
            try:
                return int(float(raw[:-1]) * 10000)
            except ValueError:
                return None
        digits = re.findall(r"[0-9]+", raw)
        if not digits:
            return None
        try:
            return int("".join(digits))
        except ValueError:
            return None

    def _clean_title(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.endswith(" - 小红书"):
            text = text[: -len(" - 小红书")].strip()
        if text in {"小红书", "小红书 - 你的生活兴趣社区"}:
            return None
        return text or None

    def _clean_description(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        return text or None

    def _normalize_cover_url(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.startswith("//"):
            return f"https:{text}"
        return text

    def _normalize_urls(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        output: list[str] = []
        for item in value:
            normalized = self._normalize_cover_url(item)
            if normalized and normalized not in output:
                output.append(normalized)
        return output

    def _normalize_media_assets(self, value: Any) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        output: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for item in value:
            if not isinstance(item, dict):
                continue
            media_type = item.get("media_type")
            url = self._normalize_cover_url(item.get("url"))
            if media_type not in {"image", "video"}:
                continue
            if not url:
                continue
            key = (media_type, url)
            if key in seen:
                continue
            seen.add(key)
            output.append({"media_type": media_type, "url": url})
        return output
