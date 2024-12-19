import datetime
from functools import lru_cache
from typing import Optional

import pyszuru


class PopularityCachePost:
    def __init__(self, tags: list[str]) -> None:
        self.tags = tags

    def has_tag(self, tag: str) -> bool:
        return tag in self.tags

class PopularityCache:
    BASE_FILTER = "status\\:final"
    MAX_AGE = datetime.timedelta(hours=1)

    def __init__(self, posts: list[PopularityCachePost]) -> None:
        self.posts = posts
        self.date_created = datetime.datetime.now(datetime.timezone.utc)

    @lru_cache
    def filter(self, tag: Optional[str]) -> "PopularityCache":
        if tag is None:
            return self
        return PopularityCache(
            [post for post in self.posts if post.has_tag(tag)]
        )

    @lru_cache
    def popularity(self, tag: str) -> int:
        return len([post for post in self.posts if post.has_tag(tag)])

    def out_of_date(self) -> bool:
        now = datetime.datetime.now(datetime.timezone.utc)
        return now - self.date_created > self.MAX_AGE

    @classmethod
    def create_cache(cls, hoardbooru: pyszuru.API) -> "PopularityCache":
        posts = hoardbooru.search_post(cls.BASE_FILTER, 100)
        cache_posts = []
        for post in posts:
            cache_posts.append(
                PopularityCachePost([name for tag in post.tags for name in tag.names])
            )
        return cls(cache_posts)
