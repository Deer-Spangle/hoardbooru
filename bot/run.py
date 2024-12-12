import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from hoardbooru_bot.bot import Bot


def setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)

    # Gallery DL sub bot log, for diagnosing the bot. Should not contain user information.
    bot_logger = logging.getLogger("hoardbooru_bot")
    file_handler = TimedRotatingFileHandler("logs/hoardbooru_bot.log", when="midnight")
    file_handler.setFormatter(formatter)
    bot_logger.addHandler(file_handler)


if __name__ == '__main__':
    setup_logging()
    with open("config.json", "r") as f:
        config = json.load(f)
    bot = Bot(config)
    bot.run()