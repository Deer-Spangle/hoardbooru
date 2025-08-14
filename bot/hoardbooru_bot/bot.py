import glob
import logging
from typing import Optional
from zipfile import ZipFile, ZipInfo

import aiofiles
import pyszuru
from prometheus_client import Gauge, start_http_server
from telethon import TelegramClient, events, Button
from telethon.events import StopPropagation
from telethon.tl.types import PeerChannel, DocumentAttributeFilename

from hoardbooru_bot.cache import TelegramMediaCache
from hoardbooru_bot.database import Database
from hoardbooru_bot.func_inline_search import InlineSearchFunctionality
from hoardbooru_bot.func_populate import PopulateFunctionality
from hoardbooru_bot.func_tagging import TaggingFunctionality
from hoardbooru_bot.func_unfinished import UnfinishedFunctionality
from hoardbooru_bot.func_unuploaded import UnuploadedFunctionality
from hoardbooru_bot.hidden_data import hidden_data, parse_hidden_data
from hoardbooru_bot.popularity_cache import PopularityCache
from hoardbooru_bot.tag_phases import DEFAULT_TAGGING_TAGS
from hoardbooru_bot.utils import file_ext, temp_sandbox_file
from hoardbooru_bot.users import TrustedUser
from hoardbooru_bot.posted_state import UploadStateCache

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


class Bot:

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
        self.upload_state_cache = UploadStateCache()
        self.functionality_tagging = TaggingFunctionality(self)
        self.functionality_unuploaded = UnuploadedFunctionality(self)
        self.functionality_inline_search = InlineSearchFunctionality(self)
        self.functionality_populate = PopulateFunctionality(self)
        self.functionality_unfinished = UnfinishedFunctionality(self)

    async def run(self) -> None:
        start_time.set_to_current_time()
        await self.client.start(bot_token=self.config["telegram"]["bot_token"])
        self.hoardbooru_url = self.config["hoardbooru"]["url"]
        self.hoardbooru = pyszuru.API(
            self.hoardbooru_url,
            username=self.config["hoardbooru"]["username"],
            token=self.config["hoardbooru"]["token"],
        )
        await self.database.start()
        # Register functions
        self.client.add_event_handler(self.start, events.NewMessage(pattern="/start", incoming=True))
        self.client.add_event_handler(self.boop, events.NewMessage(pattern="/beep", incoming=True))
        self.client.add_event_handler(
            self.upload_document,
            events.NewMessage(func=lambda e: filter_document(e), incoming=True, from_users=self.trusted_user_ids()),
        )
        self.client.add_event_handler(
            self.upload_photo,
            events.NewMessage(func=lambda e: filter_photo(e), incoming=True, from_users=self.trusted_user_ids()),
        )
        self.client.add_event_handler(self.upload_confirm, events.CallbackQuery(pattern="upload:"))
        self.functionality_tagging.register_callbacks(self.client)
        self.functionality_unfinished.register_callbacks(self.client)
        self.functionality_populate.register_callbacks(self.client)
        self.functionality_inline_search.register_callbacks(self.client)
        self.functionality_unuploaded.register_callbacks(self.client)
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

    def hoardbooru_post_url(self, post_id: int) -> str:
        return f"{self.hoardbooru_url}/post/{post_id}"

    # noinspection PyMethodMayBeStatic
    async def boop(self, event: events.NewMessage.Event) -> None:
        await event.reply("Boop!")
        raise events.StopPropagation

    # noinspection PyMethodMayBeStatic
    async def start(self, event: events.NewMessage.Event) -> None:
        await event.reply("Hey there! I'm not a very good bot yet, I'm quite early in development. I'm gonna be a bot to interface with hoardbooru")
        raise events.StopPropagation

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
                post_url = self.hoardbooru_post_url(exact_match.id_)
                await event.reply(f"This file already exists on hoardbooru.\nLink: {post_url}")
                await progress_msg.delete()
                raise StopPropagation
            sorted_matches = sorted(match_results, key=lambda x: x.distance, reverse=True)
            match_lines = "\n".join([
                f"- {self.hoardbooru_post_url(m.post.id_)} ({100*m.distance:.2f}%)" for m in sorted_matches
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
        await original_msg.reply(f"Uploaded to hoardbooru:\n{self.hoardbooru_post_url(post.id_)}")
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
