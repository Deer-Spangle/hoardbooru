import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

import pyszuru
from notion_client import Client

from utils.notion_upload.hoardbooru import HoardbooruTagType, HoardbooruTag, PostToUpload, create_pool, TagCache, \
    upload_post, UploadedPost, update_post
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
        self.parent_post: Optional[UploadedPost] = None
        self.sources: set[str] = {self.card.url}
        self.final_post_ids: list[int] = []

    def run(self) -> dict[int, UploadedPost]:
        logger.info("Processing card: %s", self.card.title)
        logger.info("Card link: %s", self.card.url)
        # Upload WIPs
        logger.info("Uploading WIPs")
        for wip in self.card.wip_files:
            self._upload_file(wip, False)
        # Upload final files
        logger.info("Updating finals")
        for final in self.card.final_files:
            self._upload_file(final, True)
        # Create pool if applicable
        if self.card.has_multiple_versions:
            create_pool(self.uploader.hoardbooru, self.card.title, self.final_post_ids)
        # Mark the notion card as complete
        mark_card_uploaded(self.uploader.notion, self.card.card_id)
        logger.info("Completed card: %s", self.card.url)
        return self.results

    def _base_tags(self) -> list[HoardbooruTag]:
        artist_tags = [HoardbooruTag(artist["name"], HoardbooruTagType.ARTISTS) for artist in self.card.artists]
        character_tags = [HoardbooruTag(char["name"], HoardbooruTagType.CHARACTERS) for char in self.card.characters]
        owner_tags = [HoardbooruTag(owner["name"], HoardbooruTagType.OWNERS) for owner in self.card.owners]
        group_meta_tags = [HoardbooruTag("tagging:needs_check", HoardbooruTagType.META)]
        misc_tags = [HoardbooruTag(tag["name"], HoardbooruTagType.DEFAULT) for tag in self.card.tags]
        return artist_tags + character_tags + owner_tags + group_meta_tags + misc_tags

    def _upload_destination_tags(self) -> list[HoardbooruTag]:
        return [
            HoardbooruTag("uploaded_to:" + site["name"].lower().replace(" ", "_"), HoardbooruTagType.META)
            for site in self.card.posted_to
        ]

    def _upload_file(self, file_data: dict, is_final: bool) -> Optional[UploadedPost]:
        if file_data["type"] == "external":
            logger.info("Adding external file URL as source")
            new_source = file_data["url"]
            self._handle_new_source(new_source)
            return
        # Check for other types of file
        if "file" not in file_data:
            raise ValueError(f"Unrecognised type of file, not external or file: {file_data['type']}")
        # Handle normal files
        file_url = file_data["file"]["url"]
        # Generate the full tag list
        base_tags = self._base_tags()
        status_tags = [HoardbooruTag("status:wip", HoardbooruTagType.META)]
        if is_final:
            status_tags = self._upload_destination_tags() + [HoardbooruTag("status:final", HoardbooruTagType.META)]
        all_tags = base_tags + status_tags
        # Create the post on hoardbooru
        post = PostToUpload(
            file_url,
            all_tags,
            self.card.is_nsfw,
            self.parent_post,
            self.sources.copy(),
        )
        hpost = upload_post(self.uploader.hoardbooru, self.uploader.tag_cache, post)
        self.results[hpost.id_] = UploadedPost(post, hpost)
        # Set parent and pool IDs if it's a "final" file
        if is_final:
            if self.parent_post is None:
                # Set parent for already uploaded posts
                self._handle_new_parent(hpost)
                # Add to pool list
            self.final_post_ids.append(hpost.id_)

    def _handle_new_source(self, new_source: str) -> None:
        # Note source for new posts
        self.sources.add(new_source)
        logger.debug("Setting source for already uploaded posts: %s", new_source)
        # Add to previous posts
        for uploaded in self.results.values():
            uploaded.to_upload.sources.add(new_source)
            update_post(self.uploader.tag_cache, uploaded.to_upload, uploaded.hpost)

    def _handle_new_parent(self, new_parent: pyszuru.Post) -> None:
        # Note parent for new posts
        self.parent_post = new_parent
        logger.debug("Setting parent for already uploaded posts: %s", new_parent)
        # Set parent for previous posts
        for uploaded in self.results.values():
            if new_parent.id_ == uploaded.hpost.id_:
                continue
            uploaded.to_upload.parent = new_parent
            update_post(self.uploader.tag_cache, uploaded.to_upload, uploaded.hpost)


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
    os.makedirs("logs", exist_ok=True)
    file_handler = TimedRotatingFileHandler("logs/notion_upload.log", when="midnight")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    with open("config.json", "r") as fc:
        c = json.load(fc)
    main(c)
