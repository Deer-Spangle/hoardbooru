import logging

from notion_client import Client

logger = logging.getLogger(__name__)


def mark_card_uploaded(notion: Client, card_id: str) -> None:
    logger.debug("Marking card as uploaded to hoardbooru: %s", card_id)
    notion.pages.update(
        page_id=card_id,
        properties={
            "Uploaded to hoardbooru": {"checkbox": True}
        }
    )
