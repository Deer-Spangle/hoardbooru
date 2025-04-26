import dataclasses
import datetime
import logging
from pathlib import Path
from typing import Optional

import aiofiles
import aiosqlite
import dateutil.parser
from prometheus_client import Gauge

logger = logging.getLogger(__name__)

total_cache_entries = Gauge(
    "hoardboorubot_db_cache_entries",
    "Number of cache entries in the database",
)


class Database:
    def __init__(self):
        self.db: Optional[aiosqlite.Connection] = None

    async def start(self) -> None:
        self.db = await aiosqlite.connect("hoardbooru_bot_db.db")
        self.db.row_factory = aiosqlite.Row
        directory = Path(__file__).parent
        migration_id = 0
        while True:
            filename = f"db_migration-{migration_id:03}.sql"
            try:
                async with aiofiles.open(directory / filename, "r") as file:
                    db_schema = await file.read()
            except FileNotFoundError:
                break
            logger.info("Applying database migration: %s", filename)
            if db_schema.lower().startswith("-- ignore exceptions"):
                try:
                    await self.db.executescript(db_schema)
                except Exception:
                    logger.debug("Migration %s failed to apply, ignoring", filename)
            else:
                await self.db.executescript(db_schema)
            migration_id += 1
        await self.db.commit()
        logger.info("Database setup complete")
        # TODO: If we're not using prometheus, speed up startup by skipping row count
        # if get_prometheus_port() is not None:
        cache_entry_count = await self.count_all_cache_entries()
        total_cache_entries.set(cache_entry_count)

    async def stop(self) -> None:
        if self.db is not None:
            await self.db.close()

    async def count_all_cache_entries(self) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) FROM cache_entries",
        ) as cursor:
            async for row in cursor:
                return row[0]
            return 0

    async def count_cache_entries(self, post_ids: list[int], sent_as_file: bool) -> int:
        params = ",".join(["?"]*len(post_ids))
        async with self.db.execute(
            f"SELECT COUNT(*) FROM cache_entries WHERE sent_as_file = ? AND post_id IN ({params})",
            (sent_as_file, *post_ids),
        ) as cursor:
            async for row in cursor:
                return row[0]
            return 0

    async def fetch_cache_entries(self, post_id: int) -> list["CacheEntry"]:
        async with self.db.execute(
                "SELECT is_photo, media_id, access_hash, file_url, mime_type, cache_date, is_thumbnail, sent_as_file "
                "FROM cache_entries WHERE post_id = ?",
                (post_id,)
        ) as cursor:
            results = []
            async for row in cursor:
                entry = CacheEntry(
                    post_id,
                    bool(row["is_photo"]),
                    row["media_id"],
                    row["access_hash"],
                    row["file_url"],
                    row["mime_type"],
                    dateutil.parser.parse(row["cache_date"]),
                    bool(row["is_thumbnail"]),
                    bool(row["sent_as_file"]),
                )
                results.append(entry)
            return results

    async def save_cache_entry(
            self,
            cache_entry: "CacheEntry",
    ) -> None:
        await self.db.execute(
            "INSERT INTO cache_entries (post_id, is_photo, media_id, access_hash, file_url, mime_type, cache_date,"
            " is_thumbnail, sent_as_file) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(post_id, sent_as_file) DO UPDATE SET "
            "is_photo=excluded.is_photo, media_id=excluded.media_id, access_hash=excluded.access_hash, "
            "file_url=excluded.file_url, mime_type=excluded.mime_type, cache_date=excluded.cache_date, "
            "is_thumbnail=excluded.is_thumbnail, sent_as_file=excluded.sent_as_file",
            (
                cache_entry.post_id, cache_entry.is_photo, cache_entry.media_id, cache_entry.access_hash,
                cache_entry.file_url, cache_entry.mime_type, cache_entry.cache_date, cache_entry.is_thumbnail,
                cache_entry.sent_as_file,
            )
        )
        await self.db.commit()
        total_cache_entries.inc(1)


@dataclasses.dataclass
class CacheEntry:
    post_id: int
    is_photo: bool
    media_id: int
    access_hash: int
    file_url: str
    mime_type: str
    cache_date: datetime.datetime
    is_thumbnail: bool
    sent_as_file: Optional[bool]
