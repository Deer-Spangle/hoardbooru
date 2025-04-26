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
from telethon.errors import MessageNotModifiedError
from telethon.events import StopPropagation, Raw
from telethon.tl.custom import InlineResult, InlineBuilder
from telethon.tl.patched import Message
from telethon.tl.types import InputPhoto, InputDocument, PeerChannel, DocumentAttributeFilename, UpdateBotInlineSend
import telethon.utils

from hoardbooru_bot.cache import TelegramMediaCache
from hoardbooru_bot.database import Database, CacheEntry
from hoardbooru_bot.hidden_data import hidden_data, parse_hidden_data
from hoardbooru_bot.popularity_cache import PopularityCache
from hoardbooru_bot.tag_phases import PHASES, DEFAULT_TAGGING_TAGS, TAGGING_TAG_FORMAT, SPECIAL_BUTTON_CALLBACKS
from hoardbooru_bot.utils import file_ext, temp_sandbox_file
from hoardbooru_bot.inline_params import InlineParams

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


def filter_photo(evt: events.NewMessage.Event) -> bool:
    if not evt.message.photo:
        return False
    return True


async def filter_reply_to_tag_menu(evt: events.NewMessage.Event) -> bool:
    if not evt.message.text:
        return False
    original_msg = await evt.get_reply_message()
    if not original_msg:
        return False
    menu_data = parse_hidden_data(original_msg)
    if not menu_data:
        return False
    return all(key in menu_data for key in ["post_id", "tag_phase", "page", "order"])


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
    MAX_TAG_BUTTON_LINES = 7

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
        self.client.add_event_handler(
            self.list_unfinished,
            events.NewMessage(pattern="/unfinished", incoming=True, from_users=self.trusted_user_ids()),
        )
        self.client.add_event_handler(self.populate_cache, events.NewMessage(pattern="/populate", incoming=True))
        self.client.add_event_handler(self.inline_search, events.InlineQuery(users=self.trusted_user_ids()))
        self.client.add_event_handler(
            self.upload_document,
            events.NewMessage(func=lambda e: filter_document(e), incoming=True, from_users=self.trusted_user_ids()),
        )
        self.client.add_event_handler(
            self.upload_photo,
            events.NewMessage(func=lambda e: filter_photo(e), incoming=True, from_users=self.trusted_user_ids()),
        )
        self.client.add_event_handler(
            self.add_tag_with_reply,
            events.NewMessage(
                func=lambda e: filter_reply_to_tag_menu(e),
                incoming=True,
                from_users=self.trusted_user_ids(),
            ),
        )
        self.client.add_event_handler(self.upload_confirm, events.CallbackQuery(pattern="upload:"))
        self.client.add_event_handler(self.tag_callback, events.CallbackQuery(pattern="tag:"))
        self.client.add_event_handler(self.tag_phase_callback, events.CallbackQuery(pattern="tag_phase:"))
        self.client.add_event_handler(self.tag_order_callback, events.CallbackQuery(pattern="tag_order:"))
        self.client.add_event_handler(self.tag_page_callback, events.CallbackQuery(pattern="tag_page:"))
        self.client.add_event_handler(self.inline_sent_callback, events.Raw(UpdateBotInlineSend))
        self.client.add_event_handler(self.spoiler_button_callback, events.CallbackQuery(pattern="spoiler:"))
        # Start prometheus server
        start_http_server(PROM_PORT)
        # Start listening
        try:
            # Start bot listening
            logger.info("Starting bot")
            await self.client.run_until_disconnected()
        finally:
            logger.info("Bot sleepy bye-bye time")
            await self.database.stop()

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

    async def _hoardbooru_post_to_inline_answer(
            self,
            builder: InlineBuilder,
            post: pyszuru.Post,
            inline_params: InlineParams,
    ) -> InlineResult:
        cache_entry = await self.media_cache.store_in_cache(post, inline_params.file)
        return await self._cache_entry_to_inline_answer(builder, cache_entry, inline_params)

    async def _cache_entry_to_inline_answer(
            self,
            builder: InlineBuilder,
            cache_entry: CacheEntry,
            inline_params: InlineParams,
    ) -> InlineResult:
        input_media_cls = InputPhoto if cache_entry.is_photo else InputDocument
        input_media = input_media_cls(cache_entry.media_id, cache_entry.access_hash, b"")
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
            caption = f"http://hoard.lan:8390/post/{cache_entry.post_id}"
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

    async def inline_search(self, event: events.InlineQuery.Event) -> None:
        inline_query = event.text.strip()
        inline_offset = int(event.offset or "0")
        builder = event.builder
        logger.info("Received inline query: %s, offset: %s", inline_query, inline_offset)
        # Add blocked tags
        user = self.trusted_user_by_id(event.sender_id)
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
            await self.media_cache.log_in_cache_channel(f"Query returned zero posts: <pre>{inline_query}</pre>")
            logger.info("Logged zero-result query to cache channel")
        # Gather any cache entries which exist
        cache_entries = await asyncio.gather(*[
            self.media_cache.load_cache(post.id_, query_params.file)
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

    async def _upload_to_hoardbooru(self, file_path: str, file_name: Optional[str]) -> pyszuru.FileToken:
        ext = None
        if file_name is not None:
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
        # Get filename
        file_name = None
        for attribute in event.message.document.attributes:
            if isinstance(attribute, DocumentAttributeFilename):
                file_name = attribute.file_name
        await self._upload_media(event, file_name)

    async def upload_photo(self, event: events.NewMessage.Event) -> None:
        if not event.message.photo:
            return
        logger.info("Received photo to upload")
        file_name = "photo.jpg"
        await self._upload_media(event, file_name)

    async def _upload_media(self, event: events.NewMessage.Event, file_name: Optional[str]) -> None:
        progress_msg = await event.reply("Uploading and checking for duplicates")
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
                post_url = f"http://hoard.lan:8390/post/{exact_match.id_}"
                await event.reply(f"This file already exists on hoardbooru.\nLink: {post_url}")
                await progress_msg.delete()
                raise StopPropagation
            sorted_matches = sorted(match_results, key=lambda x: x.distance, reverse=True)
            match_lines = "\n".join([
                f"- http://hoard.lan:8390/post/{m.post.id_} ({100*m.distance:.2f}%)" for m in sorted_matches
            ])
            await event.reply(
                f"{menu_data}This file potentially matches {len(sorted_matches)} posts!\n{match_lines}\n"
                "\nAre you sure you want to create a new post?",
                buttons=[
                    [Button.inline("Create post (SFW)", b"upload:sfw")],
                    [Button.inline("Create post (NSFW)", b"upload:nsfw")],
                    [Button.inline("Cancel", b"upload:cancel")],
                ],
                parse_mode="html",
            )
            await progress_msg.delete()
            raise StopPropagation
        # No matches, cool
        await event.reply(
            f"{menu_data}This document has no duplicates.\nWould you like to create a new post?",
            buttons=[
                [Button.inline("Create post (SFW)", b"upload:sfw")],
                [Button.inline("Create post (NSFW)", b"upload:nsfw")],
                [Button.inline("Cancel", b"upload:cancel")],
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
        check_tags = []
        for check_tag_name in DEFAULT_TAGGING_TAGS:
            try:
                check_tag = self.hoardbooru.getTag(check_tag_name)
            except pyszuru.api.SzurubooruHTTPError:
                check_tag = self.hoardbooru.createTag(check_tag_name)
            check_tag.category = "meta-tagging"
            check_tag.push()
            check_tags.append(check_tag)
        post.tags = check_tags
        post.push()
        # Store in cache
        await self.media_cache.store_in_cache(post, False)
        if await self.media_cache.load_cache(post.id_, True) is None:
            await self.media_cache.store_in_cache(post, True)
        # Reply with post link
        await event.delete()
        await original_msg.reply(f"Uploaded to hoardbooru:\nhttp://hoard.lan:8390/post/{post.id_}")
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
        tags = phase_cls.list_tags(post)
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
        tag_button_lines = [
            tag_buttons[n:n+phase_cls.tag_buttons_per_line]
            for n in range(0, len(tag_buttons), phase_cls.tag_buttons_per_line)
        ]
        page_num = int(menu_data["page"])
        buttons += tag_button_lines[page_num*self.MAX_TAG_BUTTON_LINES : (page_num+1)*self.MAX_TAG_BUTTON_LINES]
        # Pagination buttons
        pagination_buttons = []
        if page_num > 0:
            pagination_buttons.append(Button.inline("⬅️ Prev page", f"tag_page:{page_num-1}".encode()))
        if len(tag_button_lines) > (page_num+1)*self.MAX_TAG_BUTTON_LINES:
            pagination_buttons.append(Button.inline("➡️ Next page", f"tag_page:{page_num+1}".encode()))
        if pagination_buttons:
            buttons += [pagination_buttons]
        # Cancel button
        buttons += [[Button.inline("🛑 Cancel", b"tag_phase:cancel")]]
        # Next phase button
        next_phase = phase_cls.next_phase(post)
        if next_phase == "done":
            buttons += [[Button.inline("🏁 Done!", b"tag_phase:done")]]
        else:
            buttons += [[Button.inline("⏭️ Next tagging phase", f"tag_phase:{next_phase}".encode())]]
        # Edit the menu
        try:
            await msg.edit(
                text=msg_text,
                buttons=buttons,
                parse_mode="html",
            )
        except MessageNotModifiedError:
            logger.info("Tag phase menu had no change, so message could not be updated")
            pass

    async def tag_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        tag_name = event.data[4:].decode()
        # Fetch the post
        post = self.hoardbooru.getPost(int(menu_data["post_id"]))
        # Check for special buttons
        if tag_name.startswith("special"):
            special_cmd = tag_name.removeprefix("special:")
            callback = SPECIAL_BUTTON_CALLBACKS[special_cmd]
            await callback(post, event)
            await self.post_tag_phase_menu(event_msg, menu_data)
            raise StopPropagation
        # Update the tags
        try:
            htag = self.hoardbooru.getTag(tag_name)
        except pyszuru.api.SzurubooruHTTPError:
            htag = self.hoardbooru.createTag(tag_name)
            phase = PHASES[menu_data["tag_phase"]](self.hoardbooru)
            htag.category = phase.new_tag_category()
            htag.push()
        implied_tags = list(htag.implications)
        add_tags = [htag] + implied_tags
        if htag.primary_name in [t.primary_name for t in post.tags]:
            add_tag_names = [t.primary_name for t in add_tags]
            post.tags = [t for t in post.tags if t.primary_name not in add_tag_names]
        else:
            post.tags += add_tags
        post.push()
        # Update the menu
        await self.post_tag_phase_menu(event_msg, menu_data)
        raise StopPropagation

    async def tag_phase_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag_phase:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        query_data = event.data[len(b"tag_phase:"):]
        logger.info("Moving tag phase: %s", query_data)
        # If cancelled, exit early
        if query_data == b"cancel":
            await event_msg.edit(
                f"Tagging cancelled.\nPost is http://hoard.lan:8390/post/{menu_data['post_id']}", buttons=None
            )
            raise StopPropagation
        # Mark the current phase complete
        logger.info("Marking current phase complete: %s", menu_data["tag_phase"])
        post = self.hoardbooru.getPost(int(menu_data["post_id"]))
        post.tags = [tag for tag in post.tags if tag.primary_name != TAGGING_TAG_FORMAT.format(menu_data["tag_phase"])]
        post.push()
        # If we're done, close the menu
        if query_data == b"done":
            await event_msg.edit(
                f"Tagging complete!\nPost is http://hoard.lan:8390/post/{menu_data['post_id']}", buttons=None
            )
            raise StopPropagation
        # Check the post_check method
        try:
            phase_cls = PHASES[menu_data["tag_phase"]](self.hoardbooru)
            phase_cls.post_check(post)
        except ValueError as e:
            await event_msg.reply(f"Cannot move to next tag phase, due to error: {e}")
        # Move to next phase
        menu_data["tag_phase"] = query_data.decode()
        menu_data["page"] = "0"
        await self.post_tag_phase_menu(event_msg, menu_data)
        raise StopPropagation

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
        menu_data["page"] = "0"
        await self.post_tag_phase_menu(event_msg, menu_data)
        raise StopPropagation

    async def tag_page_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag_page:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        query_data = event.data[len(b"tag_page:"):]
        logger.info("Changing tag page to: %s", query_data)
        menu_data["page"] = query_data.decode()
        await self.post_tag_phase_menu(event_msg, menu_data)
        raise StopPropagation

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

    async def add_tag_with_reply(self, event: events.NewMessage.Event) -> None:
        if not event.message.text:
            return
        # Fetch menu data
        menu_msg = await event.get_reply_message()
        if not menu_msg:
            logger.info("New tag message is not a reply to a tag phase menu")
            return
        menu_data = parse_hidden_data(menu_msg)
        # Create or fetch new tag
        tag_name = event.message.text.strip().lower()
        tag_is_new = False
        try:
            htag = self.hoardbooru.getTag(tag_name)
            logger.info("Fetched existing tag: %s", tag_name)
        except pyszuru.api.SzurubooruHTTPError:
            htag = self.hoardbooru.createTag(tag_name)
            tag_is_new = True
            logger.info("Created new tag: %s", tag_name)
        # Figure out category for new tag
        phase = PHASES[menu_data["tag_phase"]](self.hoardbooru)
        tag_category = phase.new_tag_category()
        if tag_category is None:
            logger.info("User cannot add a new tag during this phase: %s", menu_data["tag_phase"])
            await event.reply("You cannot add a new tag during this phase")
            raise StopPropagation
        logger.info("Setting new tag category")
        htag.category = tag_category
        htag.push()
        # Update the post
        post = self.hoardbooru.getPost(int(menu_data["post_id"]))
        post.tags += [htag]
        post.push()
        # Send reply
        await event.reply(f"Added {'new' if tag_is_new else 'existing'} ({tag_category}) tag: {tag_name}")
        logger.info("Updating tag phase menu")
        await self.post_tag_phase_menu(menu_msg, menu_data)

    async def list_unfinished(self, event: events.NewMessage.Event) -> None:
        if not event.message.text.startswith("/unfinished"):
            return
        logger.info("Listing unfinished commissions")
        # Send in progress message
        progress_msg = await event.message.reply("Checking for unfinished commissions")
        # List all commission tags
        logger.debug("Listing all commission tags")
        comm_tags = self.hoardbooru.search_tag("category:meta-commissions", page_size=100)
        comm_tag_names = [t.primary_name for t in comm_tags]
        unfinished_comms = comm_tag_names
        # List all final posts
        logger.debug("Listing all final posts to check against commission tags")
        for post in self.hoardbooru.search_post("status\\:final", page_size=100):
            for tag in post.tags:
                if tag.primary_name in unfinished_comms:
                    unfinished_comms.remove(tag.primary_name)
        # Find artists for each
        logger.debug("Gathering artists and characters for commission info")
        unfinished_artists: dict[str, set[str]] = {}
        unfinished_characters: dict[str, set[str]] = {}
        for comm_tag in unfinished_comms:
            unfinished_artists[comm_tag] = set()
            unfinished_characters[comm_tag] = set()
            for post in self.hoardbooru.search_post(comm_tag, page_size=100):
                for tag in post.tags:
                    if tag.category == "artists":
                        unfinished_artists[comm_tag].add(tag.primary_name)
                    if tag.category == "our_characters":
                        unfinished_characters[comm_tag].add(tag.primary_name)
        # List all the unfinished tags
        lines = []
        for unfinished_tag, artists in unfinished_artists.items():
            our_characters = unfinished_characters[unfinished_tag]
            link_url = f"http://hoard.lan:8390/posts/query={unfinished_tag}"
            link_text = unfinished_tag.removeprefix("commission_").lstrip("0")
            link_text += " (" + ", ".join(our_characters) + " by " + ", ".join(artists) + ")"
            lines.append(f"- <a href=\"{link_url}\">{link_text}</a>")
        await event.message.reply("Unfinished commission tags:\n" + "\n".join(lines), parse_mode="html")
        await progress_msg.delete()
        raise StopPropagation

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
        cache_entry = await self.media_cache.load_cache(post_id, False)
        input_doc_cls = InputPhoto if cache_entry.is_photo else InputDocument
        input_doc = input_doc_cls(cache_entry.media_id, cache_entry.access_hash, b"")
        input_media = telethon.utils.get_input_media(input_doc)
        input_media.spoiler = True
        await self.client.edit_message(
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
        cache_entry = await self.media_cache.load_cache(post_id, False)
        input_doc_cls = InputPhoto if cache_entry.is_photo else InputDocument
        input_doc = input_doc_cls(cache_entry.media_id, cache_entry.access_hash, b"")
        input_media = telethon.utils.get_input_media(input_doc)
        input_media.spoiler = True
        await self.client.edit_message(
            event.original_update.msg_id,
            file=input_media,
            buttons=None,
        )
        raise StopPropagation

    async def populate_cache(self, event: events.NewMessage.Event) -> None:
        if not event.message.text.startswith("/populate"):
            return
        logger.info("Populating cache")
        # Parse the input
        populate_count = 10
        populate_search = []
        populate_files = True
        populate_photos = True
        populate_input = event.message.text.removeprefix("/populate").strip().split()
        for populate_term in populate_input:
            try:
                populate_count = int(populate_input)
                continue
            except TypeError:
                pass
            if populate_term in InlineParams.FILE_TERMS:
                populate_files = True
                populate_photos = False
                continue
            if populate_term.startswith("-") and populate_term.removeprefix("-") in InlineParams.FILE_TERMS:
                populate_files = False
                populate_photos = True
                continue
            populate_search.append(populate_term)
        # Work out how many matching posts on hoardbooru
        cache_progress_msg = await event.reply("⏳ Calculating cache size")
        posts = []
        for post in self.hoardbooru.search_post(" ".join(populate_search), page_size=100):
            posts.append(post)
        cache_ids = None
        if populate_search:
            cache_ids = [p.id_ for p in posts]
        cache_size = await self.media_cache.cache_size(cache_ids, populate_files, populate_photos)
        expected_cache_size = len(posts) * (populate_files + populate_photos)
        if cache_size == expected_cache_size:
            await event.reply(f"There are {len(posts)} posts on hoardbooru. The cache is full, at {cache_size} entries")
            await cache_progress_msg.delete()
            raise StopPropagation
        await event.reply(
            f"There are {len(posts)} posts on hoardbooru. Cache size is {cache_size}/{expected_cache_size}"
        )
        await cache_progress_msg.delete()
        # Populate the cache
        progress_msg = await event.reply(f"⏳ Populating {populate_count} cache entries")
        populated = 0
        for post in posts:
            # Check if we've populated enough
            if populated >= populate_count:
                break
            # Populate photo
            if populate_photos:
                if await self.media_cache.load_cache(post.id_, False) is None:
                    await self.media_cache.store_in_cache(post, False)
                    populated += 1
            # Check again if we've populated enough
            if populated >= populate_count:
                break
            # Populate file
            if populate_files:
                if await self.media_cache.load_cache(post.id_, True) is None:
                    await self.media_cache.store_in_cache(post, True)
                    populated += 1
        # Post the completion message
        cache_size = await self.media_cache.cache_size(cache_ids, populate_files, populate_photos)
        await progress_msg.delete()
        await event.reply(f"Populated {populated} cache entries. Cache size: {cache_size}/{expected_cache_size}")
        raise StopPropagation

