import datetime
from typing import Optional

from hoardbooru_bot.database import CacheEntry, Database


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class TelegramMediaCache:

    def __init__(self, db: Database) -> None:
        self.db = db

    async def save_cache(
            self,
            post_id: int,
            is_photo: bool,
            media_id: int,
            access_hash: int,
            file_url: str,
            mime_type: str,
            is_thumbnail: bool,
    ) -> None:
        cache_entry = CacheEntry(
            post_id,
            is_photo,
            media_id,
            access_hash,
            file_url,
            mime_type,
            now(),
            is_thumbnail,
        )
        await self.db.save_cache_entry(cache_entry)

    async def load_cache(self, post_id: int, *, allow_inline: bool = False) -> Optional[CacheEntry]:
        entry = await self.db.fetch_cache_entry(post_id)
        if entry is None:
            return None
        # Unless this is for inline use, only return full image results
        if not allow_inline and entry.is_thumbnail:
            return None
        return entry
