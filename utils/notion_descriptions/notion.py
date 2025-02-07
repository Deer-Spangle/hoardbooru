import logging
import tempfile
from typing import Optional
from zipfile import ZipFile, ZipInfo

import pyszuru
import requests
from notion_client import Client

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def fill_in_notion_descriptions(hoardbooru: pyszuru.API, notion: Client, art_db_id: str) -> None:
    art_db_resp = notion.databases.retrieve(art_db_id)
    cards = list_cards(notion, art_db_resp)
    for card_data in cards:
        process_card(card_data, hoardbooru)
    print("All complete, all cards checked!")


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


def process_card(card_data: dict, hoardbooru: pyszuru.API) -> None:
    card_title = card_data["properties"]["Name"]["title"][0]["plain_text"]
    card_url = card_data["url"]
    logger.info("Processing card: \"%s\"" % card_title)
    logger.info("Card URL: %s", card_url)
    for wip_data in card_data["properties"]["Attachments (WIPs)"]["files"]:
        process_file(wip_data, hoardbooru)
    for final_data in card_data["properties"]["Final"]["files"]:
        process_file(final_data, hoardbooru)
    logger.info("Finished processing card")


def process_file(file_data: dict, hoardbooru: pyszuru.API) -> None:
    if file_data["type"] == "external":
        logger.info("Skipping URL-type file")
        return None
    file_token = transfer_notion_to_hoardbooru(file_data, hoardbooru)
    if post := match_on_hoardbooru(file_token, hoardbooru):
        pass  # TODO


def match_on_hoardbooru(file_token: pyszuru.FileToken, hoardbooru: pyszuru.API) -> Optional[pyszuru.Post]:
    match_results = hoardbooru.search_by_image(file_token)
    exact_matches = [x for x in match_results if x.exact]
    if exact_matches:
        exact_match = exact_matches[0].post
        logger.error("Found an exact match for this file!: %s", link_to_post(exact_match))
        return exact_match
    return None


def link_to_post(hoardbooru_post: pyszuru.Post) -> str:
    scheme = hoardbooru_post.api._api_scheme
    domain = hoardbooru_post.api._url_netloc
    post_id = hoardbooru_post.id_
    return f"{scheme}://{domain}/post/{post_id}"

def transfer_notion_to_hoardbooru(file_data: dict, hoardbooru: pyszuru.API) -> pyszuru.FileToken:
    file_url = file_data["file"]["url"]
    url_no_params, _ = file_url.split("?", 1)
    _, file_name = url_no_params.rsplit("/", 1)
    _, file_ext = file_name.lower().rsplit(".", 1)
    logger.debug("Downloading file from notion: %s", file_url)
    file_resp = requests.get(file_url)
    with tempfile.NamedTemporaryFile(suffix=f".{file_ext}", mode="wb", delete_on_close=False) as temp_f:
        dl_file_name = temp_f.name
        if file_ext in ["sai", "swf", "xcf"]:
            logger.debug("Zipping up the %s file", file_ext)
            zip_name = f"{dl_file_name}.zip"
            with ZipFile(zip_name, 'w') as zipf:
                # Hardcode the timestamp, so that sha1 might detect duplicates
                info = ZipInfo(filename=file_name, date_time=(1980, 1, 1, 0, 0, 0))
                zipf.writestr(info, file_resp.content)
            dl_file_name = zip_name
        else:
            temp_f.write(file_resp.content)
            temp_f.close()
        logger.debug("Uploading file to hoardbooru: %s", dl_file_name)
        with open(dl_file_name, mode="rb") as fr:
            return hoardbooru.upload_file(fr)
