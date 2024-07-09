import json
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

import pyszuru
from notion_client import Client

from utils.notion_upload.hoardbooru import HoardbooruTagType, HoardbooruTag, PostToUpload, create_pool, TagCache, \
    upload_post, set_parent_id
from utils.notion_upload.notion import mark_card_uploaded

logger = logging.getLogger(__name__)


class Uploader:
    def __init__(self, notion: Client, hoardbooru: pyszuru.API, notion_db_id: str) -> None:
        self.notion = notion
        self.hoardbooru = hoardbooru
        self.notion_db_id = notion_db_id
        self.tag_cache = TagCache(hoardbooru)

    def run(self) -> None:
        art_db_resp = self.notion.databases.retrieve(self.notion_db_id)
        cards = self.list_cards(art_db_resp)
        logger.info(f"Found {len(cards)} cards")
        for card in cards:
            card_to_hoardbooru_posts(self.notion, self.hoardbooru, self.tag_cache, card)

    def list_cards(self, db_resp: dict) -> list[dict]:
        next_token = None
        results = []
        while True:
            logger.debug("Fetching a page of art results")
            resp = self.notion.databases.query(
                db_resp["id"],
                start_cursor=next_token,
                filter={
                    "property": "Uploaded to hoardbooru",
                    "checkbox": {
                        "equals": False
                    }
                },
                sorts=[
                    {
                        "property": "Card created",
                        "direction": "ascending",
                    }
                ]
            )
            results += resp["results"]
            next_token = resp.get("next_cursor")
            if next_token is None:
                return results


def card_to_hoardbooru_posts(
        notion: Client,
        hoardbooru: pyszuru.API,
        tag_cache: TagCache,
        card: dict
) -> dict[int, PostToUpload]:
    title = card["properties"]["Name"]["title"][0]["plain_text"]
    logger.info("Processing card: %s", title)
    logger.info("Card link: %s", card["url"])
    artist_tags = [
        HoardbooruTag(artist["name"], HoardbooruTagType.ARTISTS)
        for artist in card["properties"]["Artist"]["multi_select"]
    ]
    character_tags = [
        HoardbooruTag(char["name"], HoardbooruTagType.CHARACTERS)
        for char in card["properties"]["Characters"]["multi_select"]
    ]
    owner_tags = [
        HoardbooruTag(owner["name"], HoardbooruTagType.OWNERS)
        for owner in card["properties"]["Character owners"]["multi_select"]
    ]
    group_meta_tags = [HoardbooruTag("tagging:needs_check", HoardbooruTagType.META)]
    uploaded_to_tags = [
        HoardbooruTag("uploaded_to:" + site["name"].lower().replace(" ", "_"), HoardbooruTagType.META)
        for site in card["properties"]["Posted to"]["multi_select"]
    ]
    misc_tags = [
        HoardbooruTag(tag["name"], HoardbooruTagType.DEFAULT) for tag in card["properties"]["Tags"]["multi_select"]
    ]
    is_nsfw = card["properties"]["NSFW"]["checkbox"]
    multiple_version = card["properties"]["Multiple versions/images"]["checkbox"]
    # Result and progressive initialisation
    results: dict[int, PostToUpload] = {}
    parent_id: Optional[int] = None
    pool_post_ids: list[int] = []
    logger.info("Uploading WIPs")
    for wip in card["properties"]["Attachments (WIPs)"]["files"]:
        if "file" not in wip:
            logger.debug("Skipping non-file WIP")
            continue
        wip_url = wip["file"]["url"]
        meta_tags = group_meta_tags + [HoardbooruTag("status:wip", HoardbooruTagType.META)]
        post = PostToUpload(
            wip_url,
            artist_tags,
            character_tags,
            owner_tags,
            meta_tags,
            misc_tags,
            is_nsfw,
            parent_id,
        )
        post_id = upload_post(hoardbooru, tag_cache, post, card["url"])
        results[post_id] = post
    logger.info("Updating finals")
    for final in card["properties"]["Final"]["files"]:
        if "file" not in final:
            logger.debug("Skipping non-file final")
            continue
        final_url = final["file"]["url"]
        meta_tags = group_meta_tags + uploaded_to_tags + [HoardbooruTag("status:final", HoardbooruTagType.META)]
        post = PostToUpload(
            final_url,
            artist_tags,
            character_tags,
            owner_tags,
            meta_tags,
            misc_tags,
            is_nsfw,
            parent_id,
        )
        post_id = upload_post(hoardbooru, tag_cache, post, card["url"])

        results[post_id] = post
        if parent_id is None:
            parent_id = post_id
            # Set parent for already uploaded posts
            logger.debug("Setting parent for already uploaded wips")
            for posted_id in results.keys():
                if parent_id == posted_id:
                    continue
                set_parent_id(hoardbooru, posted_id, parent_id)
        # Add to pool list
        pool_post_ids.append(post_id)
    # Create pool if applicable
    if multiple_version:
        # Create pool
        create_pool(hoardbooru, title, pool_post_ids)
    mark_card_uploaded(notion, card["id"])
    logger.info("Completed card: %s", card["url"])
    return results


def main(config: dict) -> None:
    notion = Client(auth=config["notion"]["integration_secret"])
    hoardbooru = pyszuru.API(
        config["hoardbooru"]["url"],
        username=config["hoardbooru"]["username"],
        token=config["hoardbooru"]["token"],
    )
    uploader = Uploader(notion, hoardbooru, config["notion"]["art_db_id"])
    uploader.run()
    logger.info("Complete")


if __name__ == '__main__':
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")
    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    file_handler = TimedRotatingFileHandler("logs/notion_upload.log", when="midnight")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    with open("config.json", "r") as fc:
        c = json.load(fc)
    main(c)
