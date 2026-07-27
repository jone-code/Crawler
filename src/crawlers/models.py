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

    @classmethod
    def create(
        cls,
        *,
        platform: str,
        creator_url: str,
        creator_name: str | None,
        posts: list[CreatorPost],
    ) -> "CreatorContent":
        return cls(
            platform=platform,
            creator_url=creator_url,
            creator_name=creator_name,
            crawl_time_utc=datetime.now(timezone.utc).isoformat(),
            posts=posts,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["posts"] = [post.to_dict() for post in self.posts]
        return payload
