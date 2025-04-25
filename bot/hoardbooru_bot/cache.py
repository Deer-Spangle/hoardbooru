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

    async def store_in_cache(self, post: Post, send_uncompressed: bool) -> CacheEntry:
        is_photo = False
        sent_as_file = False
        async with downloaded_file(post.content) as dl_file:
            msg = None
            if not send_uncompressed:
                if post.mime.startswith("image") and post.mime != "image/gif":
                    is_photo = True
                    async with convert_image(dl_file.dl_path) as img_path:
                        msg = await self.client.send_file(
                            self.cache_channel,
                            img_path,
                            mime_type=post.mime,
                        )
                elif post.mime in ("video/mp4", "image/gif"):
                    msg = await self.client.send_file(
                        self.cache_channel,
                        dl_file.dl_path,
                        mime_type=post.mime,
                        file_size=dl_file.file_size,
                    )
            if msg is None:
                sent_as_file = True
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
            False,
        )
        await self.db.save_cache_entry(cache_entry)
        if sent_as_file:
            # If it was sent as a file, we can save a duplicate entry for that
            cache_entry.sent_as_file = True
            await self.db.save_cache_entry(cache_entry)
            cache_entry.sent_as_file = False
        return cache_entry

    async def log_in_cache_channel(
            self,
            log_message: str,
    ) -> None:
        await self.client.send_message(
            self.cache_channel,
            f"<b>Log:</b> {log_message}",
            parse_mode="html",
        )

    async def load_cache(self, post_id: int, as_document: bool) -> Optional[CacheEntry]:
        entries = await self.db.fetch_cache_entries(post_id)
        for entry in entries:
            if as_document == entry.sent_as_file:
                return entry
        return None

    async def cache_size(self) -> int:
        return await self.db.count_cache_entries()
