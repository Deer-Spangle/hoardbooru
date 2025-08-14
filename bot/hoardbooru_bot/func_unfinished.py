import logging

from telethon import TelegramClient, events
from telethon.events import StopPropagation

from hoardbooru_bot.functionality import Functionality

logger = logging.getLogger(__name__)


class UnfinishedFunctionality(Functionality):

    def register_callbacks(self, client: TelegramClient) -> None:
        client.add_event_handler(
            self.list_unfinished,
            events.NewMessage(pattern="/unfinished", incoming=True, from_users=self.bot.trusted_user_ids()),
        )

    async def list_unfinished(self, event: events.NewMessage.Event) -> None:
        if not event.message.text.startswith("/unfinished"):
            return
        logger.info("Listing unfinished commissions")
        # Send in progress message
        progress_msg = await event.message.reply("Checking for unfinished commissions")
        # List all commission tags
        logger.debug("Listing all commission tags")
        comm_tags = self.hoardbooru.search_tag("category:meta-commissions", page_size=100)
        comm_tag_names = [t.primary_name for t in comm_tags]
        unfinished_comms = comm_tag_names
        # List all final posts
        logger.debug("Listing all final posts to check against commission tags")
        for post in self.hoardbooru.search_post("status\\:final", page_size=100):
            for tag in post.tags:
                if tag.primary_name in unfinished_comms:
                    unfinished_comms.remove(tag.primary_name)
        # Find artists for each
        logger.debug("Gathering artists and characters for commission info")
        unfinished_artists: dict[str, set[str]] = {}
        unfinished_characters: dict[str, set[str]] = {}
        for comm_tag in unfinished_comms:
            unfinished_artists[comm_tag] = set()
            unfinished_characters[comm_tag] = set()
            for post in self.hoardbooru.search_post(comm_tag, page_size=100):
                for tag in post.tags:
                    if tag.category == "artists":
                        unfinished_artists[comm_tag].add(tag.primary_name)
                    if tag.category == "our_characters":
                        unfinished_characters[comm_tag].add(tag.primary_name)
        # List all the unfinished tags
        lines = []
        for unfinished_tag, artists in unfinished_artists.items():
            our_characters = unfinished_characters[unfinished_tag]
            link_url = f"{self.bot.hoardbooru_url}/posts/query={unfinished_tag}"
            link_text = unfinished_tag.removeprefix("commission_").lstrip("0")
            link_text += " (" + ", ".join(our_characters) + " by " + ", ".join(artists) + ")"
            lines.append(f"- <a href=\"{link_url}\">{link_text}</a>")
        await event.message.reply("Unfinished commission tags:\n" + "\n".join(lines), parse_mode="html")
        await progress_msg.delete()
        raise StopPropagation
