import logging
from typing import Optional, TYPE_CHECKING

import pyszuru
from telethon import TelegramClient, events, Button
from telethon.errors import MessageNotModifiedError
from telethon.events import StopPropagation
from telethon.tl.patched import Message

from hoardbooru_bot.functionality import Functionality
from hoardbooru_bot.hidden_data import hidden_data, parse_hidden_data
from hoardbooru_bot.popularity_cache import PopularityCache
from hoardbooru_bot.tag_phases import PHASES, TAGGING_TAG_FORMAT, SPECIAL_BUTTON_CALLBACKS
from hoardbooru_bot.utils import filter_reply_to_menu_with_fields

if TYPE_CHECKING:
    from hoardbooru_bot.bot import Bot


logger = logging.getLogger(__name__)


async def filter_reply_to_tag_menu(evt: events.NewMessage.Event) -> bool:
    return await filter_reply_to_menu_with_fields(evt, ["post_id", "tag_phase", "page", "order"])


class TaggingFunctionality(Functionality):
    MAX_TAG_BUTTON_LINES = 7

    def __init__(self, bot: "Bot") -> None:
        super().__init__(bot)
        self.popularity_cache: Optional[PopularityCache] = None

    def register_callbacks(self, client: TelegramClient) -> None:
        client.add_event_handler(
            self.tag_init, events.NewMessage(pattern="/tag", incoming=True, from_users=self.bot.trusted_user_ids())
        )
        client.add_event_handler(
            self.add_tag_with_reply,
            events.NewMessage(
                func=lambda e: filter_reply_to_tag_menu(e),
                incoming=True,
                from_users=self.bot.trusted_user_ids(),
            ),
        )
        client.add_event_handler(self.tag_callback, events.CallbackQuery(pattern="tag:"))
        client.add_event_handler(self.tag_phase_callback, events.CallbackQuery(pattern="tag_phase:"))
        client.add_event_handler(self.tag_order_callback, events.CallbackQuery(pattern="tag_order:"))
        client.add_event_handler(self.tag_page_callback, events.CallbackQuery(pattern="tag_page:"))

    async def tag_init(self, event: events.NewMessage.Event) -> None:
        if not event.message.text.startswith("/tag"):
            return
        post_id = event.message.text[4:].strip()
        if not post_id:
            await event.reply("Please specify the ID of a post you wish to tag")
            raise StopPropagation
        post = self.hoardbooru.getPost(int(post_id))
        tag_menu_data = {
            "post_id": str(post.id_),
            "tag_phase": "comm_status",
            "page": str(0),
            "order": "popular"
        }
        tag_msg = await event.message.reply("Initialising tag helper")
        await self.post_tag_phase_menu(tag_msg, tag_menu_data)
        raise StopPropagation

    async def add_tag_with_reply(self, event: events.NewMessage.Event) -> None:
        if not event.message.text:
            return
        # Fetch menu data
        menu_msg = await event.get_reply_message()
        if not menu_msg:
            logger.info("New tag message is not a reply to a tag phase menu")
            return
        menu_data = parse_hidden_data(menu_msg)
        # Create or fetch new tag
        tag_name = event.message.text.strip().lower()
        tag_is_new = False
        try:
            htag = self.hoardbooru.getTag(tag_name)
            logger.info("Fetched existing tag: %s", tag_name)
        except pyszuru.api.SzurubooruHTTPError:
            htag = self.hoardbooru.createTag(tag_name)
            tag_is_new = True
            logger.info("Created new tag: %s", tag_name)
        # Figure out category for new tag
        phase = PHASES[menu_data["tag_phase"]](self.hoardbooru, self.bot.trusted_users)
        tag_category = phase.new_tag_category()
        if tag_category is None:
            logger.info("User cannot add a new tag during this phase: %s", menu_data["tag_phase"])
            await event.reply("You cannot add a new tag during this phase")
            raise StopPropagation
        logger.info("Setting new tag category")
        htag.category = tag_category
        htag.push()
        # Update the post
        post = self.hoardbooru.getPost(int(menu_data["post_id"]))
        post.tags += [htag]
        post.push()
        # Send reply
        await event.reply(f"Added {'new' if tag_is_new else 'existing'} ({tag_category}) tag: {tag_name}")
        logger.info("Updating tag phase menu")
        await self.post_tag_phase_menu(menu_msg, menu_data)

    async def tag_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        tag_name = event.data[4:].decode()
        # Fetch the post
        post = self.hoardbooru.getPost(int(menu_data["post_id"]))
        # Check for special buttons
        if tag_name.startswith("special"):
            special_cmd = tag_name.removeprefix("special:")
            callback = SPECIAL_BUTTON_CALLBACKS[special_cmd]
            await callback(post, event)
            await self.post_tag_phase_menu(event_msg, menu_data)
            raise StopPropagation
        # Update the tags
        try:
            htag = self.hoardbooru.getTag(tag_name)
        except pyszuru.api.SzurubooruHTTPError:
            htag = self.hoardbooru.createTag(tag_name)
            phase = PHASES[menu_data["tag_phase"]](self.hoardbooru, self.bot.trusted_users)
            htag.category = phase.new_tag_category()
            htag.push()
        implied_tags = list(htag.implications)
        add_tags = [htag] + implied_tags
        if htag.primary_name in [t.primary_name for t in post.tags]:
            add_tag_names = [t.primary_name for t in add_tags]
            post.tags = [t for t in post.tags if t.primary_name not in add_tag_names]
        else:
            post.tags += add_tags
        post.push()
        # Update the menu
        await self.post_tag_phase_menu(event_msg, menu_data)
        raise StopPropagation

    async def tag_phase_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag_phase:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        post_id = int(menu_data["post_id"])
        query_data = event.data[len(b"tag_phase:"):]
        logger.info("Moving tag phase: %s", query_data)
        # If cancelled, exit early
        if query_data == b"cancel":
            await event_msg.edit(
                f"Tagging cancelled.\nPost is {self.bot.hoardbooru_post_url(post_id)}", buttons=None
            )
            raise StopPropagation
        # Mark the current phase complete
        logger.info("Marking current phase complete: %s", menu_data["tag_phase"])
        post = self.hoardbooru.getPost(post_id)
        post.tags = [tag for tag in post.tags if tag.primary_name != TAGGING_TAG_FORMAT.format(menu_data["tag_phase"])]
        post.push()
        # If we're done, close the menu
        if query_data == b"done":
            await event_msg.edit(
                f"Tagging complete!\nPost is {self.bot.hoardbooru_post_url(post_id)}", buttons=None
            )
            raise StopPropagation
        # Check the post_check method
        try:
            phase_cls = PHASES[menu_data["tag_phase"]](self.hoardbooru, self.bot.trusted_users)
            phase_cls.post_check(post)
        except ValueError as e:
            await event_msg.reply(f"Cannot move to next tag phase, due to error: {e}")
        # Move to next phase
        menu_data["tag_phase"] = query_data.decode()
        menu_data["page"] = "0"
        await self.post_tag_phase_menu(event_msg, menu_data)
        raise StopPropagation

    async def tag_order_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag_order:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        query_data = event.data[len(b"tag_order:"):]
        logger.info("Changing tag order to: %s", query_data)
        if query_data == b"popular":
            menu_data["order"] = "popular"
        if query_data == b"alphabetical":
            menu_data["order"] = "alphabetical"
        menu_data["page"] = "0"
        await self.post_tag_phase_menu(event_msg, menu_data)
        raise StopPropagation

    async def tag_page_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"tag_page:"):
            return
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        query_data = event.data[len(b"tag_page:"):]
        logger.info("Changing tag page to: %s", query_data)
        menu_data["page"] = query_data.decode()
        await self.post_tag_phase_menu(event_msg, menu_data)
        raise StopPropagation

    async def post_tag_phase_menu(self, msg: Message, menu_data: dict[str, str]) -> None:
        phase_cls = PHASES[menu_data["tag_phase"]](self.hoardbooru, self.bot.trusted_users)
        post = self.hoardbooru.getPost(int(menu_data["post_id"]))
        hidden_link = hidden_data(menu_data)
        # Log
        logger.info("Render the post tag menu: %s", menu_data)
        # Figure out message text
        msg_text = (
            f"{hidden_link}Tagging phase: {phase_cls.name()}"
            f"\nPost: {self.bot.hoardbooru_post_url(post.id_)}"
            f"\n{phase_cls.question()}"
        )
        # Construct buttons
        buttons = []
        # Order buttons
        if phase_cls.allow_ordering:
            pop_tick = "🔘" if menu_data["order"] == "popular" else "⚪"
            alp_tick = "🔘" if menu_data["order"] == "alphabetical" else "⚪"
            buttons += [[
                Button.inline(f"{pop_tick} Popular", "tag_order:popular"),
                Button.inline(f"{alp_tick} Alphabetical", "tag_order:alphabetical"),
            ]]
        # Add the actual tag buttons
        tags = phase_cls.list_tags(post) # TODO
        if phase_cls.allow_ordering and menu_data["order"] == "popular":
            for tag in tags:
                tag.popularity = 0
            popularity_filters = phase_cls.popularity_filter_tags(post) or [None]
            for popularity_filter in popularity_filters:
                popularity_cache = self._build_popularity_cache().filter(popularity_filter)
                for tag in tags:
                    tag.popularity += popularity_cache.popularity(tag.tag_name)
            tags = sorted(tags, key=lambda t: (-t.popularity, t.tag_name))
            logger.info("Sorted tags by popularity")
        if phase_cls.allow_ordering and menu_data["order"] == "alphabetical":
            tags = sorted(tags, key=lambda t: t.tag_name)
            logger.info("Sorted tags by alphabet")
        tag_buttons = [tag.to_button(post.tags) for tag in tags]
        tag_button_lines = [
            tag_buttons[n:n+phase_cls.tag_buttons_per_line]
            for n in range(0, len(tag_buttons), phase_cls.tag_buttons_per_line)
        ]
        page_num = int(menu_data["page"])
        buttons += tag_button_lines[page_num*self.MAX_TAG_BUTTON_LINES : (page_num+1)*self.MAX_TAG_BUTTON_LINES]
        # Pagination buttons
        pagination_buttons = []
        if page_num > 0:
            pagination_buttons.append(Button.inline("⬅️ Prev page", f"tag_page:{page_num-1}".encode()))
        if len(tag_button_lines) > (page_num+1)*self.MAX_TAG_BUTTON_LINES:
            pagination_buttons.append(Button.inline("➡️ Next page", f"tag_page:{page_num+1}".encode()))
        if pagination_buttons:
            buttons += [pagination_buttons]
        # Cancel button
        buttons += [[Button.inline("🛑 Cancel", b"tag_phase:cancel")]]
        # Next phase button
        next_phase = phase_cls.next_phase(post)
        if next_phase == "done":
            buttons += [[Button.inline("🏁 Done!", b"tag_phase:done")]]
        else:
            buttons += [[Button.inline("⏭️ Next tagging phase", f"tag_phase:{next_phase}".encode())]]
        # Edit the menu
        try:
            await msg.edit(
                text=msg_text,
                buttons=buttons,
                parse_mode="html",
            )
        except MessageNotModifiedError:
            logger.info("Tag phase menu had no change, so message could not be updated")
            pass

    def _build_popularity_cache(self) -> PopularityCache:
        if self.popularity_cache is None or self.popularity_cache.out_of_date():
            logger.info("Building new popularity cache")
            self.popularity_cache = PopularityCache.create_cache(self.hoardbooru)
        return self.popularity_cache
