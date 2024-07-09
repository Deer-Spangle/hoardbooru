import json
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

import pyszuru
from notion_client import Client

from utils.notion_upload.hoardbooru import HoardbooruTagType, HoardbooruTag, PostToUpload, create_pool, TagCache, \
    upload_post, add_source, UploadedPost, set_relationship
from utils.notion_upload.notion import mark_card_uploaded, Card

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
            self.process_card(Card(card))

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

    def process_card(self, card: Card) -> dict[int, UploadedPost]:
        logger.info("Processing card: %s", card.title)
        logger.info("Card link: %s", card.url)
        # Gather tags
        artist_tags = [HoardbooruTag(artist["name"], HoardbooruTagType.ARTISTS) for artist in card.artists]
        character_tags = [HoardbooruTag(char["name"], HoardbooruTagType.CHARACTERS) for char in card.characters]
        owner_tags = [HoardbooruTag(owner["name"], HoardbooruTagType.OWNERS) for owner in card.owners]
        group_meta_tags = [HoardbooruTag("tagging:needs_check", HoardbooruTagType.META)]
        uploaded_to_tags = [
            HoardbooruTag("uploaded_to:" + site["name"].lower().replace(" ", "_"), HoardbooruTagType.META)
            for site in card.posted_to
        ]
        misc_tags = [HoardbooruTag(tag["name"], HoardbooruTagType.DEFAULT) for tag in card.tags]
        # Other properties
        is_nsfw = card.is_nsfw
        multiple_version = card.has_multiple_versions
        sources = [card.url]
        # Result and progressive initialisation
        results: dict[int, UploadedPost] = {}
        parent_post: Optional[pyszuru.Post] = None
        pool_post_ids: list[int] = []
        # Upload WIPs
        logger.info("Uploading WIPs")
        for wip in card.wip_files:
            if "file" not in wip:
                logger.debug("Skipping non-file WIP")
                continue
            wip_url = wip["file"]["url"]
            meta_tags = group_meta_tags + [HoardbooruTag("status:wip", HoardbooruTagType.META)]
            all_tags = artist_tags + character_tags + owner_tags + meta_tags + misc_tags
            post = PostToUpload(
                wip_url,
                all_tags,
                is_nsfw,
                sources,
                parent_post,
            )
            hpost = upload_post(self.hoardbooru, self.tag_cache, post)
            results[hpost.id_] = UploadedPost(post, hpost)
        # Upload final files
        logger.info("Updating finals")
        for final in card.final_files:
            if "file" not in final:
                logger.debug("Skipping non-file final")
                continue
            final_url = final["file"]["url"]
            meta_tags = group_meta_tags + uploaded_to_tags + [HoardbooruTag("status:final", HoardbooruTagType.META)]
            all_tags = artist_tags + character_tags + owner_tags + meta_tags + misc_tags
            post = PostToUpload(
                final_url,
                all_tags,
                is_nsfw,
                sources,
                parent_post,
            )
            hpost = upload_post(self.hoardbooru, self.tag_cache, post)
            results[hpost.id_] = UploadedPost(post, hpost)
            if parent_post is None:
                parent_post = hpost
                # Set parent for already uploaded posts
                self._handle_new_parent(parent_post, results)
            # Add to pool list
            pool_post_ids.append(hpost.id_)
        # Create pool if applicable
        if multiple_version:
            # Create pool
            create_pool(self.hoardbooru, card.title, pool_post_ids)
        mark_card_uploaded(self.notion, card.card_id)
        logger.info("Completed card: %s", card.url)
        return results

    # noinspection PyMethodMayBeStatic
    def _handle_new_parent(self, new_parent: pyszuru.Post, results_so_far: dict[int, UploadedPost]) -> None:
        logger.debug("Setting parent for already uploaded posts: ", new_parent)
        for uploaded in results_so_far.values():
            if new_parent.id_ == uploaded.hpost.id_:
                continue
            set_relationship(uploaded.hpost, new_parent)



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
