import logging

from notion_client import Client

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def mark_card_uploaded(notion: Client, card_id: str) -> None:
    logger.debug("Marking card as uploaded to hoardbooru: %s", card_id)
    notion.pages.update(
        page_id=card_id,
        properties={
            "Uploaded to hoardbooru": {"checkbox": True}
        }
    )


class Card:
    def __init__(self, card: dict) -> None:
        self.data = card

    @property
    def title(self) -> str:
        return self.data["properties"]["Name"]["title"][0]["plain_text"]

    @property
    def url(self) -> str:
        return self.data["url"]

    @property
    def card_id(self) -> str:
        return self.data["id"]

    @property
    def artists(self) -> list[dict]:
        return self.data["properties"]["Artist"]["multi_select"]

    @property
    def characters(self) -> list[dict]:
        return self.data["properties"]["Characters"]["multi_select"]

    @property
    def owners(self) -> list[dict]:
        return self.data["properties"]["Character owners"]["multi_select"]

    @property
    def posted_to(self) -> list[dict]:
        return self.data["properties"]["Posted to"]["multi_select"]

    @property
    def tags(self) -> list[dict]:
        return self.data["properties"]["Tags"]["multi_select"]

    @property
    def is_nsfw(self) -> bool:
        return self.data["properties"]["NSFW"]["checkbox"]

    @property
    def has_multiple_versions(self) -> bool:
        return self.data["properties"]["Multiple versions/images"]["checkbox"]

    @property
    def wip_files(self) -> list[dict]:
        return self.data["properties"]["Attachments (WIPs)"]["files"]

    @property
    def final_files(self) -> list[dict]:
        return self.data["properties"]["Final"]["files"]
