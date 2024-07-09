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
            card_uploader = CardUploader(self, Card(card))
            card_uploader.run()

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


class CardUploader:
    def __init__(self, uploader: Uploader, card: Card) -> None:
        self.uploader = uploader
        self.card = card
        self.results: dict[int, UploadedPost] = {}

    def run(self) -> dict[int, UploadedPost]:
        logger.info("Processing card: %s", self.card.title)
        logger.info("Card link: %s", self.card.url)
        # Gather tags
        base_tags = self._base_tags()
        uploaded_to_tags = [
            HoardbooruTag("uploaded_to:" + site["name"].lower().replace(" ", "_"), HoardbooruTagType.META)
            for site in self.card.posted_to
        ]
        # Other properties
        sources = {self.card.url}
        # Result and progressive initialisation
        results: dict[int, UploadedPost] = {}
        parent_post: Optional[pyszuru.Post] = None
        pool_post_ids: list[int] = []
        # Upload WIPs
        logger.info("Uploading WIPs")
        for wip in self.card.wip_files:
            if wip["type"] == "external":
                logger.info("Adding URL WIP as source")
                new_source = wip["url"]
                sources.add(new_source)
                self._handle_new_source(new_source, results)
                continue
            # Check for other types of file
            if "file" not in wip:
                raise ValueError(f"Unrecognised type of file, not external or file: {wip['type']}")
            # Handle normal files
            wip_url = wip["file"]["url"]
            wip_tags = [HoardbooruTag("status:wip", HoardbooruTagType.META)]
            all_tags = base_tags + wip_tags
            post = PostToUpload(
                wip_url,
                all_tags,
                self.card.is_nsfw,
                parent_post,
                list(sources),
            )
            hpost = upload_post(self.uploader.hoardbooru, self.uploader.tag_cache, post)
            results[hpost.id_] = UploadedPost(post, hpost)
        # Upload final files
        logger.info("Updating finals")
        for final in self.card.final_files:
            if final["type"] == "external":
                logger.info("Adding URL WIP as source")
                new_source = final["url"]
                sources.add(new_source)
                self._handle_new_source(new_source, results)
                continue
            # Check for other types of file
            if "file" not in final:
                raise ValueError(f"Unrecognised type of file, not external or file: {final['type']}")
            # Handle normal files
            final_url = final["file"]["url"]
            final_tags = uploaded_to_tags + [HoardbooruTag("status:final", HoardbooruTagType.META)]
            all_tags = base_tags + final_tags
            post = PostToUpload(
                final_url,
                all_tags,
                self.card.is_nsfw,
                parent_post,
                list(sources),
            )
            hpost = upload_post(self.uploader.hoardbooru, self.uploader.tag_cache, post)
            results[hpost.id_] = UploadedPost(post, hpost)
            if parent_post is None:
                parent_post = hpost
                # Set parent for already uploaded posts
                self._handle_new_parent(parent_post, results)
            # Add to pool list
            pool_post_ids.append(hpost.id_)
        # Create pool if applicable
        if self.card.has_multiple_versions:
            # Create pool
            create_pool(self.uploader.hoardbooru, self.card.title, pool_post_ids)
        mark_card_uploaded(self.uploader.notion, self.card.card_id)
        logger.info("Completed card: %s", self.card.url)
        return results

    def _base_tags(self) -> list[HoardbooruTag]:
        artist_tags = [HoardbooruTag(artist["name"], HoardbooruTagType.ARTISTS) for artist in self.card.artists]
        character_tags = [HoardbooruTag(char["name"], HoardbooruTagType.CHARACTERS) for char in self.card.characters]
        owner_tags = [HoardbooruTag(owner["name"], HoardbooruTagType.OWNERS) for owner in self.card.owners]
        group_meta_tags = [HoardbooruTag("tagging:needs_check", HoardbooruTagType.META)]
        misc_tags = [HoardbooruTag(tag["name"], HoardbooruTagType.DEFAULT) for tag in self.card.tags]
        return artist_tags + character_tags + owner_tags + group_meta_tags + misc_tags

    # noinspection PyMethodMayBeStatic
    def _handle_new_source(self, new_source: str, results_so_far: dict[int, UploadedPost]) -> None:
        logger.debug("Setting source for already uploaded posts: %s", new_source)
        # Add to previous posts
        for uploaded in results_so_far.values():
            add_source(uploaded.hpost, new_source)

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
