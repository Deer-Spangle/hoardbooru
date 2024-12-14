import asyncio
import itertools
import logging
from typing import Optional, Coroutine, Any

import aiohttp
import pyszuru
from prometheus_client import Gauge, start_http_server
from telethon import TelegramClient, events, Button
from telethon.tl.custom import InlineResult, InlineBuilder
from telethon.tl.types import InputPhoto, InputDocument, PeerChannel

from hoardbooru_bot.cache import TelegramMediaCache
from hoardbooru_bot.database import Database, CacheEntry
from hoardbooru_bot.utils import file_ext

logger = logging.getLogger(__name__)

PROM_PORT = 7266
start_time = Gauge("hoardboorubot_start_unixtime", "Unix timestamp of the last time the bot was started")


async def _check_sender(evt: events.CallbackQuery.Event, allowed_user_id: int) -> None:
    if evt.sender_id != allowed_user_id:
        await evt.answer("Unauthorized menu use")
        raise events.StopPropagation


class Bot:
    MAX_INLINE_ANSWERS = 30
    MAX_INLINE_FRESH_MEDIA = 1

    def __init__(self, config: dict) -> None:
        self.config = config
        session_name = "hoardbooru_bot"
        if suffix := self.config["telegram"].get("session_suffix"):
            session_name += f"__{suffix}"
        self.client = TelegramClient(
            session_name, self.config["telegram"]["api_id"], self.config["telegram"]["api_hash"]
        )
        self.trusted_users = self.config["trusted_users"]
        self.database = Database()
        cache_channel = PeerChannel(self.config["cache_channel"])
        self.media_cache = TelegramMediaCache(self.database, self.client, cache_channel)
        self.hoardbooru: Optional[pyszuru.API] = None

    async def run(self) -> None:
        start_time.set_to_current_time()
        await self.client.start(bot_token=self.config["telegram"]["bot_token"])
        self.hoardbooru = pyszuru.API(
            self.config["hoardbooru"]["url"],
            username=self.config["hoardbooru"]["username"],
            token=self.config["hoardbooru"]["token"],
        )
        await self.database.start()
        # Register functions
        self.client.add_event_handler(self.start, events.NewMessage(pattern="/start", incoming=True))
        self.client.add_event_handler(self.boop, events.NewMessage(pattern="/beep", incoming=True))
        self.client.add_event_handler(self.inline_search, events.InlineQuery(users=self.trusted_users))
        # Start prometheus server
        start_http_server(PROM_PORT)
        # Start listening
        try:
            # Start bot listening
            logger.info("Starting bot")
            await self.client.run_until_disconnected()
        finally:
            logger.info("Bot sleepy bye-bye time")

    # noinspection PyMethodMayBeStatic
    async def boop(self, event: events.NewMessage.Event) -> None:
        await event.reply("Boop!")
        raise events.StopPropagation

    # noinspection PyMethodMayBeStatic
    async def start(self, event: events.NewMessage.Event) -> None:
        await event.reply("Hey there! I'm not a very good bot yet, I'm quite early in development. I'm gonna be a bot to interface with hoardbooru")
        raise events.StopPropagation

    async def _hoardbooru_post_to_inline_answer(self, builder: InlineBuilder, post: pyszuru.Post) -> InlineResult:
        cache_entry = await self.media_cache.store_in_cache(post)
        return await self._cache_entry_to_inline_answer(builder, cache_entry)

    async def _cache_entry_to_inline_answer(self, builder: InlineBuilder, cache_entry: CacheEntry) -> InlineResult:
        input_media_cls = InputPhoto if cache_entry.is_photo else InputDocument
        input_media = input_media_cls(cache_entry.media_id, cache_entry.access_hash, b"")
        # If thumbnail is cached, add a button
        buttons = None
        if cache_entry.is_thumbnail:
            buttons = [Button.inline("Click for full res", f"neaten_me:{cache_entry.post_id}")]
        # Build the inline answer
        if cache_entry.is_photo:
            return await builder.photo(
                file=input_media,
                id=str(cache_entry.post_id),
                buttons=buttons,
                parse_mode="html",
            )
        mime_type = {
            "mp4": "video/mp4",
            "gif": "video/mp4",
            "webm": "video/mp4",
            "mp3": "audio/mp3",
            "pdf": "application/pdf",
        }.get(file_ext(cache_entry.file_url))
        return await builder.document(
            file=input_media,
            mime_type=mime_type,
            type="gif" if mime_type == "video/mp4" else None,
            id=str(cache_entry.post_id),
            buttons=buttons,
            parse_mode="html",
        )

    async def inline_search(self, event: events.InlineQuery.Event) -> None:
        inline_query = event.text.strip()
        inline_offset = int(event.offset or "0")
        builder = event.builder
        if inline_query == "":
            return
        logger.info("Received inline query: %s", inline_query)
        # Get the biggest possible list of posts
        post_generator = self.hoardbooru.search_post(inline_query)
        posts = list(itertools.islice(post_generator, inline_offset, self.MAX_INLINE_ANSWERS))
        logger.info("Found %s posts for inline query", len(posts))
        # Gather any cache entries which exist
        cache_entries = await asyncio.gather(*[
            self.media_cache.load_cache(post.id_, allow_inline=True)
            for post in posts
        ])
        logger.info(
            "Found %s cache entries for inline query",
            len([c for c in cache_entries if c is not None]),
        )
        # Convert to answers, fetching fresh ones where needed, up to limit
        inline_answers: list[Coroutine[Any, Any, InlineResult]] = []
        num_fresh_media = 0
        for post, cache_entry in zip(posts, cache_entries):
            if cache_entry is None:
                if num_fresh_media >= self.MAX_INLINE_FRESH_MEDIA:
                    break
                num_fresh_media += 1
                inline_answers.append(self._hoardbooru_post_to_inline_answer(builder, post))
            else:
                inline_answers.append(self._cache_entry_to_inline_answer(builder, cache_entry))
        # Send the answers as a gallery
        next_offset = inline_offset + len(inline_answers)
        logger.info("Sending %s results for query: %s", len(inline_answers), inline_query)
        await event.answer(
            await asyncio.gather(*inline_answers),
            next_offset=str(next_offset),
            gallery=True,
        )
