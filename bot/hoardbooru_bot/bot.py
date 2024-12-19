import asyncio
import dataclasses
import glob
import itertools
import logging
from typing import Optional, Coroutine, Any
from zipfile import ZipFile, ZipInfo

import aiofiles
import pyszuru
from prometheus_client import Gauge, start_http_server
from telethon import TelegramClient, events, Button
from telethon.events import StopPropagation
from telethon.tl.custom import InlineResult, InlineBuilder
from telethon.tl.patched import Message
from telethon.tl.types import InputPhoto, InputDocument, PeerChannel, DocumentAttributeFilename

from hoardbooru_bot.cache import TelegramMediaCache
from hoardbooru_bot.database import Database, CacheEntry
from hoardbooru_bot.hidden_data import hidden_data, parse_hidden_data
from hoardbooru_bot.popularity_cache import PopularityCache
from hoardbooru_bot.tag_phases import PHASES
from hoardbooru_bot.utils import file_ext, temp_sandbox_file

logger = logging.getLogger(__name__)

PROM_PORT = 7266
start_time = Gauge("hoardboorubot_start_unixtime", "Unix timestamp of the last time the bot was started")


async def _check_sender(evt: events.CallbackQuery.Event, allowed_user_id: int) -> None:
    if evt.sender_id != allowed_user_id:
        await evt.answer("Unauthorized menu use")
        raise events.StopPropagation


def filter_document(evt: events.NewMessage.Event) -> bool:
    if not evt.message.document:
        return False
    return True


@dataclasses.dataclass
class TrustedUser:
    telegram_id: int
    blocked_tags: list[str]

    @classmethod
    def from_json(cls, data: dict) -> "TrustedUser":
        return cls(
            data["telegram_id"],
            data.get("blocked_tags", []),
        )


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
        self.trusted_users = [
            TrustedUser.from_json(user_data) for user_data in self.config["trusted_users"]
        ]
        self.database = Database()
        cache_channel = PeerChannel(self.config["cache_channel"])
        self.media_cache = TelegramMediaCache(self.database, self.client, cache_channel)
        self.hoardbooru: Optional[pyszuru.API] = None
        self.popularity_cache: Optional[PopularityCache] = None

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
        self.client.add_event_handler(
            self.tag_init, events.NewMessage(pattern="/tag", incoming=True, from_users=self.trusted_user_ids())
        )
        self.client.add_event_handler(self.inline_search, events.InlineQuery(users=self.trusted_user_ids()))
        self.client.add_event_handler(
            self.upload_document,
            events.NewMessage(func=lambda e: filter_document(e), incoming=True, from_users=self.trusted_user_ids()),
        )
        self.client.add_event_handler(self.upload_confirm, events.CallbackQuery(pattern="upload:"))
        self.client.add_event_handler(self.tag_callback, events.CallbackQuery(pattern="tag:"))
        self.client.add_event_handler(self.tag_phase_callback, events.CallbackQuery(pattern="tag_phase:"))
        self.client.add_event_handler(self.tag_order_callback, events.CallbackQuery(pattern="tag_order:"))
        # Start prometheus server
        start_http_server(PROM_PORT)
        # Start listening
        try:
            # Start bot listening
            logger.info("Starting bot")
            await self.client.run_until_disconnected()
        finally:
            logger.info("Bot sleepy bye-bye time")

    def trusted_user_ids(self) -> list[int]:
        return [user.telegram_id for user in self.trusted_users]

    def trusted_user_by_id(self, user_id: int) -> Optional[TrustedUser]:
        for user in self.trusted_users:
            if user.telegram_id == user_id:
                return user
        return None

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
        logger.info("Received inline query: %s, offset: %s", inline_query, inline_offset)
        # Add blocked tags
        user = self.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        inline_query += "".join(f" -{tag}" for tag in user.blocked_tags)
        logger.info("Query with blocked tags is: %s", inline_query)
        # Get the biggest possible list of posts
        post_generator = self.hoardbooru.search_post(inline_query)
        posts = list(itertools.islice(post_generator, inline_offset, inline_offset + self.MAX_INLINE_ANSWERS))
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

    async def _upload_to_hoardbooru(self, file_path: str, file_name: Optional[str]) -> pyszuru.FileToken:
        ext = file_ext(file_name)
        if ext in ["sai", "swf", "xcf"]:
            logger.debug("Zipping up the %s file", ext)
            # Read the file bytes
            async with aiofiles.open(file_path, "rb") as f:
                file_bytes = await f.read()
            # Zip it up
            zip_name = f"{file_name}.zip"
            with ZipFile(zip_name, 'w') as zip_file:
                # Hardcode the timestamp, so that sha1 might detect duplicates
                info = ZipInfo(filename=file_name, date_time=(1980, 1, 1, 0, 0, 0))
                zip_file.writestr(info, file_bytes)
            file_path = zip_name
        logger.debug("Uploading file to hoardbooru: %s", file_name)
        with open(file_path, mode="rb") as fr:
            file_token = self.hoardbooru.upload_file(fr)
        return file_token

    async def upload_document(self, event: events.NewMessage.Event) -> None:
        if not event.message.document:
            return
        logger.info("Received document to upload")
        progress_msg = await event.reply("Uploading and checking for duplicates")
        # Get filename
        file_name = None
        for attribute in event.message.document.attributes:
            if isinstance(attribute, DocumentAttributeFilename):
                file_name = attribute.file_name
        async with temp_sandbox_file(ext=None) as temp_path:
            # Download the document
            await event.message.download_media(temp_path)
            dl_path = glob.glob(f"{temp_path}*")[0]
            # Upload to hoardbooru
            file_token = await self._upload_to_hoardbooru(dl_path, file_name)
        # Create hidden menu data
        menu_data = hidden_data({
            "token": file_token.token,
            "filepath": file_token.filepath,
        })
        # Check for duplicates
        match_results = self.hoardbooru.search_by_image(file_token)
        logger.debug(f"There are {len(match_results)} posts matching this file")
        if match_results:
            # Check for exact matches
            exact_matches = [x for x in match_results if x.exact]
            if exact_matches:
                exact_match: pyszuru.Post = exact_matches[0].post
                post_url = f"https://hoard.lan:8390/post/{exact_match.id_}"
                await event.reply(f"This file already exists on hoardbooru.\nLink: {post_url}")
                await progress_msg.delete()
                raise StopPropagation
            sorted_matches = sorted(match_results, key=lambda x: x.distance, reverse=True)
            match_lines = [
                f"- https://hoard.lan:8390/post/{m.post.id_} ({100*m.distance:.2f}%)" for m in sorted_matches
            ]
            await event.reply(
                f"{menu_data}This file potentially matches {len(sorted_matches)} posts!\n{'\n'.join(match_lines)}\n"
                f"\nAre you sure you want to create a new post?",
                buttons=[
                    Button.inline("Create post", b"upload:yes"),
                    Button.inline("Cancel", b"upload:cancel"),
                ],
                parse_mode="html",
            )
            await progress_msg.delete()
            raise StopPropagation
        # No matches, cool
        await event.reply(
            f"{menu_data}This document has no duplicates.\nWould you like to create a new post?",
            buttons=[
                Button.inline("Create post (SFW)", b"upload:sfw"),
                Button.inline("Create post (NSFW)", b"upload:nsfw"),
                Button.inline("Cancel", b"upload:cancel"),
            ],
            parse_mode="html",
        )
        # Delete progress message
        await progress_msg.delete()

    async def upload_confirm(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"upload:"):
            return
        upload_resp = event.data.replace(b"upload:", b"")
        if upload_resp == b"cancel":
            logger.debug("Hoardbooru upload cancelled")
            await event.delete()
            raise StopPropagation
        logger.debug("Creating hoardbooru post")
        event_msg = await event.get_message()
        original_msg = await event_msg.get_reply_message()
        menu_data = parse_hidden_data(event_msg)
        file_token = pyszuru.FileToken(menu_data["token"], menu_data["filepath"])
        post_rating = {
            b"sfw": "safe",
            b"nsfw": "unsafe",
        }[upload_resp]
        # Create the hoardbooru post
        post = self.hoardbooru.createPost(file_token, post_rating)
        logger.info("Created hoardbooru post: %s", post.id_)
        # Apply some default tags
        check_tag = self.hoardbooru.getTag("tagging:needs_check")
        post.tags = [check_tag]
        post.push()
        # Store in cache
        await self.media_cache.store_in_cache(post)
        # Reply with post link
        await event.delete()
        await original_msg.reply(f"Uploaded to hoardbooru:\nhttps://hoard.lan:8390/post/{post.id_}")
        # Start tagging phase
        tag_menu_data = {
            "post_id": str(post.id_),
            "tag_phase": "comm_status",
            "page": str(0),
            "order": "popular"
        }
        tag_msg = await original_msg.reply("Initialising tag helper")
        await self.post_tag_phase_menu(tag_msg, tag_menu_data)
        raise StopPropagation

    def _build_popularity_cache(self) -> PopularityCache:
        if self.popularity_cache is None or self.popularity_cache.out_of_date():
            logger.info("Building new popularity cache")
            self.popularity_cache = PopularityCache.create_cache(self.hoardbooru)
        return self.popularity_cache

    async def post_tag_phase_menu(self, msg: Message, menu_data: dict[str, str]) -> None:
        phase_cls = PHASES[menu_data["tag_phase"]](self.hoardbooru)
        post = self.hoardbooru.getPost(int(menu_data["post_id"]))
        hidden_link = hidden_data(menu_data)
        # Log
        logger.info("Render the post tag menu: %s", menu_data)
        # Figure out message text
        msg_text = (
            f"{hidden_link}Tagging phase: {phase_cls.name()}"
            f"\nPost: http://hoard.lan:8390/post/{post.id_}"
            f"\n{phase_cls.question()}"
        )
        # Construct buttons
        buttons = []
        # Order buttons
        if phase_cls.allow_ordering:
            pop_tick = "🔘" if menu_data["order"] == "popular" else "⚪"
            alp_tick = "🔘" if menu_data["order"] == "alphabetical" else "⚪"
            buttons += [[
                Button.inline(f"{pop_tick} Popular", "tag_order:popular"),
                Button.inline(f"{alp_tick} Alphabetical", "tag_order:alphabetical"),
            ]]
        # Add the actual tag buttons
        tags = phase_cls.list_tags()
        if phase_cls.allow_ordering and menu_data["order"] == "popular":
            for tag in tags:
                tag.popularity = 0
            popularity_filters = phase_cls.popularity_filter_tags(post) or [None]
            for popularity_filter in popularity_filters:
                popularity_cache = self._build_popularity_cache().filter(popularity_filter)
                for tag in tags:
                    tag.popularity += popularity_cache.popularity(tag.tag_name)
            tags = sorted(tags, key=lambda t: (-t.popularity, t.tag_name))
            logger.info("Sorted tags by popularity")
        if phase_cls.allow_ordering and menu_data["order"] == "alphabetical":
            tags = sorted(tags, key=lambda t: t.tag_name)
            logger.info("Sorted tags by alphabet")
        tag_buttons = [tag.to_button(post.tags) for tag in tags]
        buttons += [
            tag_buttons[n:n+3] for n in range(0, len(tag_buttons), 3)
        ]
        # Cancel button
        buttons += [[Button.inline("🛑 Cancel", b"tag_phase:cancel")]]
        # Next phase button
        next_phase = phase_cls.next_phase()
        if next_phase == "done":
            buttons += [[Button.inline("🏁 Done!", b"tag_phase:done")]]
        else:
            buttons += [[Button.inline("⏭️ Next tagging phase", f"tag_phase:{next_phase}".encode())]]
        # Edit the menu
        await msg.edit(
            text=msg_text,
            buttons=buttons,
            parse_mode="html",
        )
        raise StopPropagation

    async def tag_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        tag_name = event.data[4:].decode()
        # Update the tags
        post = self.hoardbooru.getPost(int(menu_data["post_id"]))
        htag = self.hoardbooru.getTag(tag_name)
        if htag.primary_name in [t.primary_name for t in post.tags]:
            post.tags = [t for t in post.tags if htag.primary_name != t.primary_name]
        else:
            post.tags += [htag]
        post.push()
        # Update the menu
        await self.post_tag_phase_menu(event_msg, menu_data)

    async def tag_phase_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag_phase:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        query_data = event.data[len(b"tag_phase:"):]
        logger.info("Moving tag phase: %s", query_data)
        if query_data == b"cancel":
            await event_msg.edit(
                f"Tagging cancelled.\nPost is http://hoard.lan:8390/post/{menu_data['post_id']}", buttons=None
            )
            raise StopPropagation
        if query_data == b"done":
            await event_msg.edit(
                f"Tagging complete!\nPost is http://hoard.lan:8390/post/{menu_data['post_id']}", buttons=None
            )
            raise StopPropagation
        menu_data["tag_phase"] = query_data.decode()
        menu_data["page"] = "0"
        await self.post_tag_phase_menu(event_msg, menu_data)

    async def tag_order_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag_order:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        query_data = event.data[len(b"tag_order:"):]
        logger.info("Changing tag order to: %s", query_data)
        if query_data == b"popular":
            menu_data["order"] = "popular"
        if query_data == b"alphabetical":
            menu_data["order"] = "alphabetical"
        await self.post_tag_phase_menu(event_msg, menu_data)

    async def tag_init(self, event: events.NewMessage.Event) -> None:
        if not event.message.text.startswith("/tag"):
            return
        post_id = event.message.text[4:].strip()
        post = self.hoardbooru.getPost(int(post_id))
        tag_menu_data = {
            "post_id": str(post.id_),
            "tag_phase": "comm_status",
            "page": str(0),
            "order": "popular"
        }
        tag_msg = await event.message.reply("Initialising tag helper")
        await self.post_tag_phase_menu(tag_msg, tag_menu_data)
        raise StopPropagation