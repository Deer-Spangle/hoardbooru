import dataclasses
import enum
import json
import logging
import sys
import tempfile
from typing import Optional, Dict

import pyszuru
import requests
from notion_client import Client


logger = logging.getLogger(__name__)


def list_cards(notion: Client, db_resp: dict) -> list[dict]:
    next_token = None
    results = []
    while True:
        logger.debug("Fetching a page of art results")
        resp = notion.databases.query(
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


class HoardbooruTagType(enum.Enum):
    ARTISTS = enum.auto()
    CHARACTERS = enum.auto()
    OWNERS = enum.auto()
    META = enum.auto()
    DEFAULT = enum.auto()


@dataclasses.dataclass
class HoardbooruTag:
    name: str
    type: HoardbooruTagType

    @property
    def name_clean(self) -> str:
        if self.name.lower() == "zephyr" and type == HoardbooruTagType.OWNERS:
            return "zephyr_(owner)"
        return self.name.lower().replace(" ", "_")

    @property
    def type_clean(self) -> str:
        if self.name.lower() in ["animation", "gift", "got original", "uncoloured"]:
            return HoardbooruTagType.META.name.lower()
        return self.type.name.lower()


@dataclasses.dataclass
class PostToUpload:
    url: str
    artist_tags: list[HoardbooruTag]
    character_tags: list[HoardbooruTag]
    owner_tags: list[HoardbooruTag]
    meta_tags: list[HoardbooruTag]
    misc_tags: list[HoardbooruTag]
    is_nsfw: bool
    parent_id: Optional[int]

    @property
    def file_ext(self) -> str:
        url, _ = self.url.split("?", 1)
        _, ext = url.rsplit(".", 1)
        return ext

    @property
    def post_safety(self) -> str:
        return "unsafe" if self.is_nsfw else "safe"

    @property
    def all_tags(self) -> list[HoardbooruTag]:
        return self.artist_tags + self.character_tags + self.owner_tags + self.meta_tags + self.misc_tags


def create_pool(hoardbooru: pyszuru.API, title: str, post_ids: list[int]) -> None:
    logger.debug("Creating hoardbooru pool: %s", title)
    hoardbooru._call(
        "POST",
        ["pool"],
        body={
            "names": [title],
            "category": "default",
            "posts": [post_ids]
        }
    )


def mark_card_uploaded(notion: Client, card_id: str) -> None:
    logger.debug("Marking card as uploaded to hoardbooru: %s", card_id)
    notion.pages.update(
        page_id=card_id,
        properties={
            "Uploaded to hoardbooru": {"checkbox": True}
        }
    )


class TagCache:
    def __init__(self, hoardbooru: pyszuru.API):
        self.api = hoardbooru
        self.tags: dict[str, pyszuru.Tag] = {}

    def get_tag(self, tag: HoardbooruTag) -> pyszuru.Tag:
        htag = self.tags.get(tag.name_clean)
        if htag is not None:
            return htag
        logger.debug("Fetching tag object: %s", tag.name_clean)
        try:
            htag = self.api.getTag(tag.name_clean)
        except pyszuru.SzurubooruHTTPError as e:
            logger.debug("Creating new tag: %s", tag.name_clean)
            htag = self.api.createTag(tag.name_clean)
        htag.category = tag.type_clean
        htag.push()
        self.tags[tag.name_clean] = htag
        return htag


def link_to_post(hoardbooru_post: pyszuru.Post) -> str:
    scheme = hoardbooru_post.api._api_scheme
    domain = hoardbooru_post.api._url_netloc
    post_id = hoardbooru_post.id_
    return f"{scheme}://{domain}/post/{post_id}"


def upload_post(hoardbooru: pyszuru.API, tag_cache: TagCache, post: PostToUpload, notion_url: str) -> int:
    logger.debug("Downloading file from notion: %s", post.url)
    file_resp = requests.get(post.url)
    with tempfile.NamedTemporaryFile(suffix=f".{post.file_ext}", mode="wb", delete_on_close=False) as f:
        f.write(file_resp.content)
        f.close()
        logger.debug("Uploading file to hoardbooru: %s", f.name)
        with open(f.name, mode="rb") as fr:
            file_token = hoardbooru.upload_file(fr)
    # Check for duplicates
    match_results = hoardbooru.search_by_image(file_token)
    logger.debug(f"There are {len(match_results)} posts matching this file")
    if match_results:
        logger.debug("Found matches: %s", match_results)
        exact_matches = [x for x in match_results if x.exact]
        if exact_matches:
            exact_match = exact_matches[0].post
            logger.error("Found an exact match!: %s", link_to_post(exact_match))
            logger.info("Updating post")
            tags = [
                tag_cache.get_tag(tag) for tag in post.all_tags
            ]
            exact_match.tags = tags
            if post.parent_id:  # TODO: notion source
                exact_match.relations.append(post.parent_id)
            exact_match.push()
            return exact_match.id_
        closest = min(match_results, key=lambda x: x.distance)
        logger.warning("Closest match has a distance of %s: %s", closest.distance, closest.post)
        choice = input("How to proceed?")
        raise ValueError("No idea how to proceed")
    # Create the post
    logger.debug("Creating hoardbooru post")
    hoardbooru_post = hoardbooru.createPost(file_token, post.post_safety)
    logger.info("Created hoardbooru post: %s", link_to_post(hoardbooru_post))
    logger.debug("Adding tags")
    tags = [
        tag_cache.get_tag(tag) for tag in post.all_tags
    ]
    hoardbooru_post.tags = tags
    if notion_url not in hoardbooru_post.source:
        logger.debug("Adding notion URL to sources")
        hoardbooru_post.source.append(notion_url)
    if post.parent_id:
        if post.parent_id not in [p.id_ for p in hoardbooru_post.relations]:
            logger.debug("Setting parent ID")
            parent_hpost = hoardbooru.getPost(post.parent_id)
            hoardbooru_post.relations.append(parent_hpost)
    hoardbooru_post.push()
    return hoardbooru_post.id_


def set_parent_id(hoardbooru: pyszuru.API, posted_id: int, parent_id: int) -> None:
    logger.debug("Setting parent of post %s to %s", posted_id, parent_id)
    hpost = hoardbooru.getPost(posted_id)
    hpost_relation_ids = [p.id_ for p in hpost.relations]
    if parent_id in hpost_relation_ids:
        logger.debug("Already set")
        return
    parent_hpost = hoardbooru.getPost(parent_id)
    hpost.relations.append(parent_hpost)
    hpost.push()


def card_to_hoardbooru_posts(notion: Client, hoardbooru: pyszuru.API, tag_cache: TagCache, card: dict) -> dict[int, PostToUpload]:
    title = card["properties"]["Name"]["title"][0]["plain_text"]
    logger.info("Processing card: %s", title)
    logger.info("Card link: %s", card["url"])
    artist_tags = [
        HoardbooruTag(artist["name"], HoardbooruTagType.ARTISTS) for artist in card["properties"]["Artist"]["multi_select"]
    ]
    character_tags = [
        HoardbooruTag(char["name"], HoardbooruTagType.CHARACTERS) for char in card["properties"]["Characters"]["multi_select"]
    ]
    owner_tags = [
        HoardbooruTag(owner["name"], HoardbooruTagType.OWNERS) for owner in card["properties"]["Character owners"]["multi_select"]
    ]
    group_meta_tags = [HoardbooruTag("tagging:needs_check", HoardbooruTagType.META)]
    uploaded_to_tags = [
        HoardbooruTag("uploaded_to:"+site["name"].lower().replace(" ", "_"), HoardbooruTagType.META)
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
    tag_cache = TagCache(hoardbooru)
    art_db_resp = notion.databases.retrieve(config["notion"]["art_db_id"])
    cards = list_cards(notion, art_db_resp)
    logger.info(f"Found {len(cards)} cards")
    for card in cards:
        card_to_hoardbooru_posts(notion, hoardbooru, tag_cache, card)
    logger.info("Complete")


if __name__ == '__main__':
    logger.addHandler(logging.StreamHandler(sys.stderr))
    logger.setLevel(logging.DEBUG)
    with open("config.json", "r") as fc:
        c = json.load(fc)
    main(c)
