import json
import logging
import os
import queue
import sys
from logging.handlers import TimedRotatingFileHandler

import pyszuru


logger = logging.getLogger()

COMM_POOL_CATEGORY = "commissions"
COMM_POOL_PREFIX = "commission_"


def has_post_been_handled(post: pyszuru.Post) -> bool:
    # noinspection PyProtectedMember
    logger.debug("Checking if post ID: %s has been assigned to a comm pool", post.id_)
    post_pools = post._generic_getter("pools")
    if not post_pools:
        return False
    for pool in post_pools:
        if pool["category"] == COMM_POOL_CATEGORY:
            return True
    return False


def gather_post_relation_web(post: pyszuru.Post) -> list[pyszuru.Post]:
    """Figure out the full web of posts related to this one"""
    logger.info("Finding full relation web for post: %s", post.id_)
    # Set up the structures
    checked_posts: dict[int, pyszuru.Post] = {}
    posts_to_check = queue.Queue()
    unchecked_posts: list[pyszuru.Post] = [] # could be a queue
    seen_ids: set[int] = set()
    # Start from the first post
    for related_post in post.relations:
        posts_to_check.put(related_post)
        seen_ids.add(related_post.id_)
    # Mark first post as checked
    checked_posts[post.id_] = post
    seen_ids.add(post.id_)
    # Go through queue
    while not posts_to_check.empty():
        # Get next post
        next_post = posts_to_check.get()
        # If we already checked that, skip
        if next_post.id_ in checked_posts:
            continue
        # For each related post, add to the queue, if not already seen
        for related_post in next_post.relations:
            if related_post.id_ in seen_ids:
                continue
            posts_to_check.put(related_post)
            seen_ids.add(related_post.id_)
        # Record this post as checked
        checked_posts[next_post.id_] = next_post
    # Return the list
    return list(checked_posts.values())


def _fetch_comm_pools_page(hoardbooru: pyszuru, offset: int = 0) -> dict:
    # noinspection PyProtectedMember
    return hoardbooru._call(
        "GET",
        ["pools"],
        urlquery={
            "category": COMM_POOL_CATEGORY,
            "offset": offset,
        }
    )


def _fetch_pool(hoardbooru: pyszuru, pool_id: int) -> dict:
    # noinspection PyProtectedMember
    return hoardbooru._call(
        "GET",
        ["pool", pool_id],
    )


def list_all_comm_pools(hoardbooru: pyszuru.API) -> list[dict]:
    logger.debug("Listing all hoardbooru commission pools")
    all_results = []
    resp = _fetch_comm_pools_page(hoardbooru)
    all_results += resp["results"]
    while resp["total"] == resp["limit"]:
        offset = resp["offset"]
        resp = _fetch_comm_pools_page(hoardbooru, offset)
        all_results += resp["results"]
    return all_results


def find_highest_pool_id(hoardbooru: pyszuru.API) -> int:
    """Figure out the current highest pool ID"""
    logger.debug("Listing hoardbooru pools")
    all_pools = list_all_comm_pools(hoardbooru)
    highest_comm_id = 0
    for pool in all_pools:
        if pool["category"] != COMM_POOL_CATEGORY:
            continue
        if not pool["names"][0].startswith(COMM_POOL_PREFIX):
            continue
        comm_id = int(pool["names"][0].removeprefix(COMM_POOL_PREFIX))
        highest_comm_id = max(comm_id, highest_comm_id)
    return highest_comm_id


def create_pool(hoardbooru: pyszuru.API, title: str, post_ids: list[int]) -> None:
    logger.debug("Creating hoardbooru pool: %s", title)
    # noinspection PyProtectedMember
    hoardbooru._call(
        "POST",
        ["pool"],
        body={
            "names": [title.replace(" ", "_")],
            "category": "commissions",
            "posts": post_ids,
        }
    )

def convert_relations_to_pools(hoardbooru: pyszuru.API) -> None:
    highest_comm_pool_id = find_highest_pool_id(hoardbooru)
    logger.info("Current highest commission pool ID: %s", highest_comm_pool_id)
    for post in hoardbooru.search_post("-sort:id"):
        if has_post_been_handled(post):
            logger.info("Skipping already-handled post: %s", post.id_)
            continue
        logger.info("Handling post: %s", post.id_)
        related_posts = gather_post_relation_web(post)
        related_ids = [p.id_ for p in related_posts]
        logger.info("Post ID %s has %s related posts: %s", post.id_, len(related_ids), related_ids)
        if len(related_ids) == 1:
            logger.warning(
                f"http://hoard.lan:8390/post/{post.id_} has no related posts. Maybe relations were not set up."
            )
            resp = input("Should I create a commission pool for it? [yN] ")
            if resp.lower().strip() not in ["y", "yes"]:
                logger.warning(f"Skipping post: {post.id_}")
                continue
        next_comm_pool_id = highest_comm_pool_id + 1
        comm_pool_title = COMM_POOL_PREFIX + str(next_comm_pool_id).zfill(5)
        create_pool(hoardbooru, comm_pool_title, related_ids)
        highest_comm_pool_id = next_comm_pool_id
        logger.info("Created pool: %s", comm_pool_title)
    logger.info("Converted relations to pools")


def main(config: dict) -> None:
    hoardbooru = pyszuru.API(
        config["hoardbooru"]["url"],
        username=config["hoardbooru"]["username"],
        token=config["hoardbooru"]["token"],
    )
    convert_relations_to_pools(hoardbooru)
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
