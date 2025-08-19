import asyncio
import itertools
import logging
from typing import Coroutine, Any

import pyszuru
from telethon.tl.custom import InlineResult, InlineBuilder
from telethon import TelegramClient, events, Button
from telethon.events import StopPropagation
from telethon.tl.types import UpdateBotInlineSend

from hoardbooru_bot.database import CacheEntry
from hoardbooru_bot.functionality import Functionality
from hoardbooru_bot.inline_params import InlineParams
from hoardbooru_bot.utils import cache_entry_to_input_media_doc, cache_entry_to_input_doc, file_ext

logger = logging.getLogger(__name__)


class InlineSearchFunctionality(Functionality):
    MAX_INLINE_ANSWERS = 30
    MAX_INLINE_FRESH_MEDIA = 1

    def register_callbacks(self, client: TelegramClient) -> None:
        client.add_event_handler(self.inline_search, events.InlineQuery(users=self.bot.trusted_user_ids()))
        client.add_event_handler(self.inline_sent_callback, events.Raw(UpdateBotInlineSend))
        client.add_event_handler(self.spoiler_button_callback, events.CallbackQuery(pattern="spoiler:"))

    async def inline_search(self, event: events.InlineQuery.Event) -> None:
        inline_query = event.text.strip()
        inline_offset = int(event.offset or "0")
        builder = event.builder
        logger.info("Received inline query: %s, offset: %s", inline_query, inline_offset)
        # Add blocked tags
        user = self.bot.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        inline_query += "".join(f" -{tag}" for tag in user.blocked_tags)
        logger.info("Query with blocked tags is: %s", inline_query)
        query_params = InlineParams()
        inline_query = query_params.parse_inline_query(inline_query)
        # Get the biggest possible list of posts
        post_generator = self.hoardbooru.search_post(inline_query)
        posts = list(itertools.islice(post_generator, inline_offset, inline_offset + self.MAX_INLINE_ANSWERS))
        logger.info("Found %s posts for inline query", len(posts))
        if len(posts) == 0 and inline_offset == 0:
            await self.bot.media_cache.log_in_cache_channel(f"Query returned zero posts: <pre>{inline_query}</pre>")
            logger.info("Logged zero-result query to cache channel")
        # Gather any cache entries which exist
        cache_entries = await asyncio.gather(*[
            self.bot.media_cache.load_cache(post.id_, query_params.file)
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
            if post.content.endswith(".webm"):
                logger.warning("Skipping webm file, post ID %s", post.id_)
                continue
            if cache_entry is None:
                if num_fresh_media >= self.MAX_INLINE_FRESH_MEDIA:
                    break
                num_fresh_media += 1
                inline_answers.append(self._hoardbooru_post_to_inline_answer(builder, post, query_params))
            else:
                inline_answers.append(self._cache_entry_to_inline_answer(builder, cache_entry, query_params))
        # Send the answers as a gallery
        next_offset = inline_offset + len(inline_answers)
        logger.info("Sending %s results for query: %s", len(inline_answers), inline_query)
        await event.answer(
            await asyncio.gather(*inline_answers),
            next_offset=str(next_offset),
            gallery=True,
        )

    async def inline_sent_callback(self, event: UpdateBotInlineSend) -> None:
        logger.info("Received callback for sent inline message with answer ID: '%s'", event.id)
        if event.msg_id is None:
            # If the message is sent without a button, there's no message ID provided, so we can't do much.
            logger.info("Inline answer sent without a button. No action to perform. Answer ID: %s", event.id)
            raise StopPropagation
        if not event.id.endswith(":spoiler"):
            logger.warning("Unrecognised inline answer ID, does not match expected format: %s", event.id)
            raise StopPropagation
        post_id = int(event.id.removesuffix(":spoiler"))
        cache_entry = await self.bot.media_cache.load_cache(post_id, False)
        input_media = cache_entry_to_input_media_doc(cache_entry)
        input_media.spoiler = True
        await self.bot.client.edit_message(
            event.msg_id,
            file=input_media,
            buttons=None,
        )
        raise StopPropagation

    async def spoiler_button_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"spoiler:"):
            return
        logger.warning("Inline answer spoiler button was pressed with data: '%s'", event.data)
        post_id = int(event.data.decode().removeprefix("spoiler:"))
        cache_entry = await self.bot.media_cache.load_cache(post_id, False)
        input_media = cache_entry_to_input_media_doc(cache_entry)
        input_media.spoiler = True
        await self.bot.client.edit_message(
            event.original_update.msg_id,
            file=input_media,
            buttons=None,
        )
        raise StopPropagation

    async def _hoardbooru_post_to_inline_answer(
            self,
            builder: InlineBuilder,
            post: pyszuru.Post,
            inline_params: InlineParams,
    ) -> InlineResult:
        cache_entry = await self.bot.media_cache.store_in_cache(post, inline_params.file)
        return await self._cache_entry_to_inline_answer(builder, cache_entry, inline_params)

    async def _cache_entry_to_inline_answer(
            self,
            builder: InlineBuilder,
            cache_entry: CacheEntry,
            inline_params: InlineParams,
    ) -> InlineResult:
        input_media = cache_entry_to_input_doc(cache_entry)
        answer_id = str(cache_entry.post_id)
        # If thumbnail is cached, add a button
        buttons = None
        caption = None
        if cache_entry.is_thumbnail:
            # TODO: remove this, unused
            buttons = [Button.inline("Click for full res", f"neaten_me:{cache_entry.post_id}")]
        if inline_params.spoiler:
            buttons = [Button.inline("Spoilerise", f"spoiler:{cache_entry.post_id}")]
            answer_id += ":spoiler"
        if inline_params.link:
            caption = self.bot.hoardbooru_post_url(cache_entry.post_id)
        if inline_params.caption:
            caption = inline_params.caption
            if "{link}" in caption:
                post_url = self.bot.hoardbooru_post_url(cache_entry.post_id)
                caption = caption.replace("{link}", post_url)
        # Build the inline answer
        if cache_entry.is_photo:
            return await builder.photo(
                file=input_media,
                id=answer_id,
                buttons=buttons,
                parse_mode="html",
                text=caption,
            )
        post_file_ext = file_ext(cache_entry.file_url)
        mime_type = {
            "mp4": "video/mp4",
            "gif": "video/mp4",
            "webm": "video/mp4",
            "mp3": "audio/mp3",
            "pdf": "application/pdf",
        }.get(post_file_ext)
        return await builder.document(
            file=input_media,
            title=f"{cache_entry.post_id}.{post_file_ext}",
            mime_type=mime_type,
            type="gif" if mime_type == "video/mp4" else None,
            id=answer_id,
            buttons=buttons,
            parse_mode="html",
            text=caption,
        )
