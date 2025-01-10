import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import pyszuru

logger = logging.getLogger()


def main(config: dict) -> None:
    hoardbooru = pyszuru.API(
        config["hoardbooru"]["url"],
        username=config["hoardbooru"]["username"],
        token=config["hoardbooru"]["token"],
    )
    fill_in_notion_descriptions(hoardbooru)
    logger.info("Complete")

if __name__ == '__main__':
    # noinspection DuplicatedCode
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")
    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    os.makedirs("logs", exist_ok=True)
    file_handler = TimedRotatingFileHandler("logs/commission_pools.log", when="midnight")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    with open("config.json", "r") as fc:
        c = json.load(fc)
    main(c)
