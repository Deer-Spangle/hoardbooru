import dataclasses
import datetime
from pathlib import Path
from typing import Optional

import aiofiles
import aiosqlite
import dateutil.parser
from prometheus_client import Gauge

total_cache_entries = Gauge(
    "hoardboorubot_db_cache_entries",
    "Number of cache entries in the database",
)

class Database:
    def __init__(self):
        self.db: Optional[aiosqlite.Connection] = None

    async def start(self) -> None:
        self.db = await aiosqlite.connect("journals.db")
        self.db.row_factory = aiosqlite.Row
        directory = Path(__file__).parent
        async with aiofiles.open(directory / "database_schema.sql", "r") as f:
            db_schema = await f.read()
            await self.db.executescript(db_schema)
        await self.db.commit()
        # TODO: If we're not using prometheus, speed up startup by skipping row count
        # if get_prometheus_port() is not None:
        cache_entry_count = await self.count_cache_entries()
        total_cache_entries.set(cache_entry_count)

    async def stop(self) -> None:
        if self.db is not None:
            await self.db.close()

    async def count_cache_entries(self) -> int:
        with self.db.execute(
            "SELECT COUNT(*) FROM cache_entries",
        ) as cursor:
            row = await anext(cursor, None)
            if not row:
                return 0
            return row[0]

    async def fetch_cache_entry(self, post_id: int) -> Optional["CacheEntry"]:
        with self.db.execute(
                "SELECT is_photo, media_id, access_hash, file_url, mime_type, cache_date, is_thumbnail "
                "FROM cache_entries WHERE post_id = ?",
                (post_id,)
        ) as cursor:
            row = await anext(cursor, None)
            if not row:
                return None
            return CacheEntry(
                post_id,
                bool(row["is_photo"]),
                row["media_id"],
                row["access_hash"],
                row["file_url"],
                row["mime_type"],
                dateutil.parser.parse(row["cache_date"]),
                bool(row["is_thumbnail"]),
            )

    async def save_cache_entry(
            self,
            cache_entry: "CacheEntry",
    ) -> None:
        await self.db.execute(
            "INSERT INTO cache_entries (post_id, is_photo, media_id, access_hash, file_url, mime_type, cache_date, is_thumbnail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(post_id) DO UPDATE SET "
            "is_photo=excluded.is_photo, media_id=excluded.media_id, access_hash=excluded.access_hash, "
            "file_url=excluded.file_url, mime_type=excluded.mime_type, cache_date=excluded.cache_date, "
            "is_thumbnail=excluded.is_thumbnail",
            (
                cache_entry.post_id, cache_entry.is_photo, cache_entry.media_id, cache_entry.access_hash, cache_entry.file_url, cache_entry.mime_type, cache_entry.cache_date, cache_entry.is_thumbnail
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
