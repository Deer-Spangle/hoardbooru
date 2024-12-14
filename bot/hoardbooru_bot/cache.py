import datetime
from typing import Optional

from pyszuru import Post
from telethon import TelegramClient
from telethon.tl.types import PeerChannel

from hoardbooru_bot.database import CacheEntry, Database
from hoardbooru_bot.utils import downloaded_file, convert_image


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class TelegramMediaCache:
    TG_MAX_PHOTO_FILE_SIZE = 10_000_000

    def __init__(self, db: Database, client: TelegramClient, cache_channel: PeerChannel) -> None:
        self.db = db
        self.client = client
        self.cache_channel = cache_channel

    async def store_in_cache(self, post: Post) -> CacheEntry:
        is_photo = False
        async with downloaded_file(post.content) as dl_file:
            if post.mime.startswith("image"):
                is_photo = True
                async with convert_image(dl_file.dl_path) as img_path:
                    msg = await self.client.send_file(
                        self.cache_channel,
                        img_path,
                        mime_type=post.mime,
                    )
            elif post.mime == "video/mp4":
                msg = await self.client.send_file(
                    self.cache_channel,
                    dl_file.dl_path,
                    mime_type=post.mime,
                    file_size=dl_file.file_size,
                )
            else:
                msg = await self.client.send_file(
                    self.cache_channel,
                    dl_file.dl_path,
                    force_document=True,
                    mime_type=post.mime,
                    file_size=dl_file.file_size,
                )
        # Build the cache entry
        cache_entry = CacheEntry(
            post.id_,
            is_photo,
            msg.file.media.id,
            msg.file.media.access_hash,
            post.content,
            post.mime,
            now(),
            False,
        )
        await self.db.save_cache_entry(cache_entry)
        return cache_entry

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
