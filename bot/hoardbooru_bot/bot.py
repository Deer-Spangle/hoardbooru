import logging
from typing import Optional

import pyszuru
from prometheus_client import Gauge, start_http_server
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel

from hoardbooru_bot.cache import TelegramMediaCache
from hoardbooru_bot.database import Database
from hoardbooru_bot.func_inline_search import InlineSearchFunctionality
from hoardbooru_bot.func_populate import PopulateFunctionality
from hoardbooru_bot.func_tagging import TaggingFunctionality
from hoardbooru_bot.func_unfinished import UnfinishedFunctionality
from hoardbooru_bot.func_unuploaded import UnuploadedFunctionality
from hoardbooru_bot.func_upload import UploadFunctionality
from hoardbooru_bot.popularity_cache import PopularityCache
from hoardbooru_bot.users import TrustedUser
from hoardbooru_bot.posted_state import UploadStateCache

logger = logging.getLogger(__name__)

PROM_PORT = 7266
start_time = Gauge("hoardboorubot_start_unixtime", "Unix timestamp of the last time the bot was started")


async def _check_sender(evt: events.CallbackQuery.Event, allowed_user_id: int) -> None:
    if evt.sender_id != allowed_user_id:
        await evt.answer("Unauthorized menu use")
        raise events.StopPropagation

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
        self.functionality_upload = UploadFunctionality(self)
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
        self.functionality_upload.register_callbacks(self.client)
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

    def _build_popularity_cache(self) -> PopularityCache:
        if self.popularity_cache is None or self.popularity_cache.out_of_date():
            logger.info("Building new popularity cache")
            self.popularity_cache = PopularityCache.create_cache(self.hoardbooru)
        return self.popularity_cache
