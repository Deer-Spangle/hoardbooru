import typing
from abc import abstractmethod, ABC

import pyszuru
from telethon import TelegramClient

if typing.TYPE_CHECKING:
    from hoardbooru_bot.bot import Bot


class Functionality(ABC):
    def __init__(self, bot: "Bot") -> None:
        self.bot = bot

    @property
    def hoardbooru(self) -> "pyszuru.API":
        return self.bot.hoardbooru

    @abstractmethod
    def register_callbacks(self, client: TelegramClient) -> None:
        raise NotImplementedError()
