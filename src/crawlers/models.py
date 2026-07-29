from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class CreatorPost:
    platform: str
    creator_url: str
    post_id: str
    post_url: str
    title: str | None = None
    description: str | None = None
    cover_url: str | None = None
    like_count: int | None = None
    comment_count: int | None = None
    share_count: int | None = None
    publish_time: str | None = None
    image_urls: list[str] = field(default_factory=list)
    video_urls: list[str] = field(default_factory=list)
    media_assets: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CreatorContent:
    platform: str
    creator_url: str
    creator_name: str | None
    crawl_time_utc: str
    posts: list[CreatorPost]
    crawler_meta: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        platform: str,
        creator_url: str,
        creator_name: str | None,
        posts: list[CreatorPost],
        crawler_meta: dict[str, Any] | None = None,
    ) -> "CreatorContent":
        return cls(
            platform=platform,
            creator_url=creator_url,
            creator_name=creator_name,
            crawl_time_utc=datetime.now(timezone.utc).isoformat(),
            posts=posts,
            crawler_meta=crawler_meta or {},
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["posts"] = [post.to_dict() for post in self.posts]
        return payload
