import dataclasses
import enum
import logging
import tempfile
from typing import Optional

import pyszuru
import requests


logger = logging.getLogger(__name__)


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
    all_tags: list[HoardbooruTag]
    is_nsfw: bool
    parent: Optional[pyszuru.Post]
    sources: set[str]

    @property
    def file_ext(self) -> str:
        url, _ = self.url.split("?", 1)
        _, ext = url.rsplit(".", 1)
        return ext

    @property
    def post_safety(self) -> str:
        return "unsafe" if self.is_nsfw else "safe"


@dataclasses.dataclass
class UploadedPost:
    to_upload: PostToUpload
    hpost: pyszuru.Post


def create_pool(hoardbooru: pyszuru.API, title: str, post_ids: list[int]) -> None:
    logger.debug("Creating hoardbooru pool: %s", title)
    # noinspection PyProtectedMember
    hoardbooru._call(
        "POST",
        ["pool"],
        body={
            "names": [title],
            "category": "default",
            "posts": [post_ids]
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
        except pyszuru.SzurubooruHTTPError:
            logger.debug("Creating new tag: %s", tag.name_clean)
            htag = self.api.createTag(tag.name_clean)
        htag.category = tag.type_clean
        htag.push()
        self.tags[tag.name_clean] = htag
        return htag


# noinspection PyProtectedMember
def link_to_post(hoardbooru_post: pyszuru.Post) -> str:
    scheme = hoardbooru_post.api._api_scheme
    domain = hoardbooru_post.api._url_netloc
    post_id = hoardbooru_post.id_
    return f"{scheme}://{domain}/post/{post_id}"


def upload_post(hoardbooru: pyszuru.API, tag_cache: TagCache, post: PostToUpload) -> pyszuru.Post:
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
            update_post(tag_cache, post, exact_match)
            return exact_match
        closest = min(match_results, key=lambda x: x.distance)
        logger.warning("Closest match has a distance of %s: %s", closest.distance, closest.post)
        choice = input("How to proceed?")
        raise ValueError("No idea how to proceed")
    # Create the post
    logger.debug("Creating hoardbooru post")
    hoardbooru_post = hoardbooru.createPost(file_token, post.post_safety)
    logger.info("Created hoardbooru post: %s", link_to_post(hoardbooru_post))
    update_post(tag_cache, post, hoardbooru_post)
    return hoardbooru_post


def update_post(tag_cache: TagCache, post: PostToUpload, hpost: pyszuru.Post) -> None:
    logger.info("Updating post")
    # Update safety
    hpost.safety = post.post_safety
    # Update tags
    tags = [
        tag_cache.get_tag(tag) for tag in post.all_tags
    ]
    hpost.tags = tags
    # Update parent relation
    if post.parent and post.parent.id_ != hpost.id_:
        if post.parent.id_ not in [p.id_ for p in hpost.relations]:
            hpost.relations.append(post.parent)
    # Update sources
    for source in post.sources:
        if source not in hpost.source:
            hpost.source.append(source)
    hpost.push()
