import html
import logging
import re
import typing

import pyszuru
from telethon import TelegramClient, events, Button
from telethon.events import StopPropagation
from telethon.tl.patched import Message

from hoardbooru_bot.hidden_data import parse_hidden_data, hidden_data
from hoardbooru_bot.post_descriptions import set_post_description, UploadLinkUploaderType, get_post_description, \
    UploadDataPostDocument, UploadLink
from hoardbooru_bot.posted_state import PostUploadState
from hoardbooru_bot.users import TrustedUser
from hoardbooru_bot.utils import filter_reply_to_menu_with_fields, tick_cross_if_true, cache_entry_to_input_media_doc, \
    tick_if_true, bold_if_true, links_in_msg

if typing.TYPE_CHECKING:
    from hoardbooru_bot.bot import Bot

logger = logging.getLogger(__name__)


async def filter_reply_to_upload_propose_menu(evt: events.NewMessage.Event) -> bool:
    return await filter_reply_to_menu_with_fields(evt, ["query", "user_infix", "uploaded_only", "post_id", "proposed_field"], precise=True)


async def filter_reply_to_upload_link_menu(evt: events.NewMessage.Event) -> bool:
    return await filter_reply_to_menu_with_fields(evt, ["query", "user_infix", "uploaded_only", "post_id", "proposed_field", "upload_link_num"])


class UnuploadedFunctionality:
    def __init__(self, bot: "Bot") -> None:
        self.bot = bot

    @property
    def hoardbooru(self) -> "pyszuru.API":
        return self.bot.hoardbooru

    def register_callbacks(self, client: TelegramClient) -> None:
        client.add_event_handler(
            self.list_unuploaded,
            events.NewMessage(pattern="/unuploaded", incoming=True, from_users=self.bot.trusted_user_ids())
        )
        client.add_event_handler(
            self.propose_with_reply,
            events.NewMessage(
                func=lambda e: filter_reply_to_upload_propose_menu(e),
                incoming=True,
                from_users=self.bot.trusted_user_ids(),
            )
        )
        client.add_event_handler(
            self.upload_link_info_with_reply,
            events.NewMessage(
                func=lambda e: filter_reply_to_upload_link_menu(e),
                incoming=True,
                from_users=self.bot.trusted_user_ids(),
            )
        )
        client.add_event_handler(self.unuploaded_page_callback, events.CallbackQuery(pattern="unuploaded:"))
        client.add_event_handler(self.upload_tag_callback, events.CallbackQuery(pattern="upload_tag:"))
        client.add_event_handler(self.upload_propose_callback, events.CallbackQuery(pattern="upload_propose:"))
        client.add_event_handler(self.upload_link_callback, events.CallbackQuery(pattern="upload_link:"))
        client.add_event_handler(self.upload_link_type_callback, events.CallbackQuery(pattern="upload_link_type:"))
        client.add_event_handler(self.upload_link_delete_callback, events.CallbackQuery(pattern="upload_link_delete"))

    async def list_unuploaded(self, event: events.NewMessage.Event) -> None:
        if not event.message.text.startswith("/unuploaded"):
            return
        # Find the right user data
        user = self.bot.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        user_infix = user.upload_tag_infix
        # Construct the list of all applicable posts
        query_tags = event.message.text.removeprefix("/unuploaded").strip().split()
        if "final" not in query_tags:
            query_tags.append("final")
        for trusted_user in self.bot.trusted_users:
            if trusted_user.owner_tag in query_tags:
                user_infix = trusted_user.upload_tag_infix
        if user_infix == user.upload_tag_infix and user.owner_tag not in query_tags:
            query_tags.append(user.owner_tag)
        # If "uploaded" is specified, invert normal behaviour
        uploaded_only = "uploaded" in query_tags
        if uploaded_only:
            query_tags.remove("uploaded")
        query_str = " ".join(query_tags)
        logger.info(f"Got %sunuploaded command with query: {query_str}", "inverted " if uploaded_only else "")
        # Gather posts into which are uploaded where
        upload_states = self.bot.upload_state_cache.list_by_state(self.hoardbooru, query_str, user_infix, refresh=True)
        # Get list of posts in search
        if uploaded_only:
            posts_in_search = upload_states.posts_not_to_upload
            button_text = "Categorise uploaded only"
        else:
            posts_in_search = upload_states.posts_to_upload
            button_text = "Categorise unuploaded"
        # Post the message saying the current state of things.
        inverted_text = "<b>inverted</b> " if uploaded_only else ""
        msg_sections = [f"There are a total of {len(upload_states.all_posts)} posts matching this {inverted_text}search (\"{query_str}\")"]
        e621_section_lines = ["e621 upload state:"]
        e621_section_lines += [f"- {len(upload_states.e6_uploaded)} Uploaded"]
        e621_section_lines += [f"- {len(upload_states.e6_not_uploading)} Not to upload"]
        e621_section_lines += [f"- {len(upload_states.e6_to_upload)} Remaining to upload"]
        msg_sections += ["\n".join(e621_section_lines)]
        fa_section_lines = [f"{user_infix.title()} FA upload state:"]
        fa_section_lines += [f"- {len(upload_states.fa_uploaded)} Uploaded"]
        fa_section_lines += [f"- {len(upload_states.fa_not_uploading)} Not to upload"]
        fa_section_lines += [f"- {len(upload_states.fa_to_upload)} Remaining to upload"]
        msg_sections += ["\n".join(fa_section_lines)]
        msg_sections += [f"In total, {len(posts_in_search)} to upload or categorise"]
        msg_text = "\n\n".join(msg_sections)
        # Construct menu info and buttons
        menu_data = {
            "query": query_str,
            "user_infix": user_infix,
            "uploaded_only": str(uploaded_only),
        }
        menu_data_str = hidden_data(menu_data, ["query", "user_infix", "uploaded_only"])
        earliest_post = min(posts_in_search, key=lambda post: post.id_)
        buttons = [Button.inline(button_text, f"unuploaded:{earliest_post.id_}")]
        await event.reply(menu_data_str + msg_text, buttons=buttons, parse_mode="html")
        raise StopPropagation

    async def unuploaded_page_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"unuploaded:"):
            return
        user = self.bot.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        event_msg = await event.get_message()
        callback_data = event.data[len(b"unuploaded:"):].decode()
        logger.info("Unuploaded menu callback data: %s", callback_data)
        # Handle cancel callbacks
        if callback_data == "cancel":
            menu_data = parse_hidden_data(event_msg)
            post_id = int(menu_data["post_id"])
            post_url = self.bot.hoardbooru_post_url(post_id)
            await event_msg.edit(f"Unuploaded media handling cancelled on Post {post_id}\n{post_url}", buttons=None)
            raise StopPropagation
        # Handle post ID callbacks
        post_id = int(callback_data)
        await self.render_unuploaded_page_menu(event_msg, post_id, user)
        raise StopPropagation

    async def upload_tag_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"upload_tag:"):
            return
        user = self.bot.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        # Log callback data
        callback_data = event.data[len(b"upload_tag:"):].decode()
        logger.info("Upload tag menu callback data: %s", callback_data)
        # Find the right post
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        query_str = menu_data["query"]
        user_infix = menu_data["user_infix"]
        post_id = int(menu_data["post_id"])
        # Fetch the post
        post = self.hoardbooru.getPost(post_id)
        # Update the tags
        tag_name = f"uploaded_to:{callback_data}"
        htag = self.hoardbooru.getTag(tag_name)
        if htag.primary_name in [t.primary_name for t in post.tags]:
            post.tags = [t for t in post.tags if t.primary_name != htag.primary_name]
        else:
            post.tags.append(htag)
        post.push()
        # Update in posts cache
        states = self.bot.upload_state_cache.list_by_state(self.hoardbooru, query_str, user_infix)
        states.update_post(post)
        # Update the menu
        await self.render_unuploaded_page_menu(event_msg, post.id_, user)
        raise StopPropagation

    async def render_unuploaded_page_menu(self, msg: Message, post_id: int, user: TrustedUser) -> None:
        menu_data = parse_hidden_data(msg)
        menu_data["post_id"] = str(post_id)
        post = self.hoardbooru.getPost(post_id)
        url_line = f"{self.bot.hoardbooru_post_url(post_id)}"
        # Fetch the post media
        cache_entry = await self.bot.media_cache.load_cache(post_id, False)
        if cache_entry is None:
            cache_entry = await self.bot.media_cache.store_in_cache(post, False)
        input_media = cache_entry_to_input_media_doc(cache_entry)
        # Construct the upload state buttons and text
        user_infix = menu_data["user_infix"]
        post_status = PostUploadState(post, user_infix)
        state_buttons = []
        state_buttons += [[Button.inline(
            f"{tick_cross_if_true(post_status.e6_uploaded)} e621: Uploaded",
            f"upload_tag:e621",
        )]]
        state_buttons += [[Button.inline(
            f"{tick_cross_if_true(post_status.e6_not_uploading)} e621: Not uploading",
            f"upload_tag:e621_not_posting",
        )]]
        state_buttons += [[Button.inline(
            f"{tick_cross_if_true(post_status.fa_uploaded)} FA: Uploaded",
            f"upload_tag:{user_infix}_fa",
        )]]
        state_buttons += [[Button.inline(
            f"{tick_cross_if_true(post_status.fa_not_uploading)} FA: Not uploading",
            f"upload_tag:{user_infix}_not_posting",
        )]]
        state_lines = [
            f"e621 State: {bold_if_true(post_status.e6_state, post_status.e6_to_upload)}",
            f"FA State: {bold_if_true(post_status.fa_state, post_status.fa_to_upload)}"
        ]
        # Construct pagination buttons and lines
        query = menu_data["query"]
        user_infix = menu_data["user_infix"]
        upload_only = menu_data["uploaded_only"] == "True"
        upload_states = self.bot.upload_state_cache.list_by_state(self.hoardbooru, query, user_infix)
        if upload_only:
            posts_to_upload = upload_states.posts_not_to_upload
        else:
            posts_to_upload = upload_states.posts_to_upload
        next_posts = [p for p in posts_to_upload if p.id_ > post_id]
        prev_posts = [p for p in posts_to_upload if p.id_ < post_id]
        pagination_button_row = []
        if prev_posts:
            prev_post = max(prev_posts, key=lambda p: p.id_)
            pagination_button_row.append(Button.inline("⬅️ Prev", f"unuploaded:{prev_post.id_}"))
        pagination_button_row.append(Button.inline("🛑 Cancel", f"unuploaded:cancel"))
        if next_posts:
            next_post = min(next_posts, key=lambda p: p.id_)
            pagination_button_row.append(Button.inline("➡️ Next", f"unuploaded:{next_post.id_}"))
        total_to_upload = len(posts_to_upload)
        menu_data_str = hidden_data(menu_data, ["query", "user_infix", "uploaded_only", "post_id"])
        title_line = f"{menu_data_str}Showing menu for Post {post_id} (#{len(prev_posts) + 1}/{total_to_upload})"
        # Parse post description data
        post_description = get_post_description(post)
        gallery_upload_data = post_description.get_or_create_doc_matching_type(UploadDataPostDocument)
        # Construct alts line
        alts_line = []
        alts_buttons = []
        commission_tags = [t for t in post.tags if t.category == "meta-commissions"]
        if len(commission_tags) == 1:
            commission_tag_name = commission_tags[0].primary_name
            list_alts = upload_states.list_alts(commission_tag_name, upload_only)
            if len(list_alts) > 1:
                alts_line = [f"This post is 1 of {len(list_alts)} alts in this list."]
                if desc := gallery_upload_data.alt_description:
                    alts_line += [f"Alt description: {desc}"]
                alts_buttons = [[Button.inline("✏️ Alt description", "upload_propose:alt_description")]]
        # Construct proposed data buttons and lines
        proposed_lines = []
        edit_buttons = []
        if proposed_title := gallery_upload_data.proposed_title:
            proposed_lines += [f"<b>Proposed title:</b> {html.escape(proposed_title)}"]
        edit_buttons += [Button.inline("✏️Title", "upload_propose:title")]
        if proposed_description := gallery_upload_data.proposed_description:
            proposed_lines += ["<b>Proposed description:</b>", html.escape(proposed_description)]
        edit_buttons += [Button.inline("✏️ Description", "upload_propose:description")]
        if proposed_tags := gallery_upload_data.proposed_tags:
            proposed_lines += ["<b>Proposed tags:</b>", html.escape(", ".join(proposed_tags))]
        edit_buttons += [Button.inline("✏️ Tags", "upload_propose:tags")]
        if upload_links := gallery_upload_data.upload_links:
            proposed_lines += ["<b>Upload links:</b>", *["- " + html.escape(link.to_string()) for link in upload_links]]
        links_buttons = [[Button.inline("🔗 Modify upload links", "upload_propose:links")]]
        # Construct message text
        lines = [title_line, url_line, *alts_line, *state_lines, *proposed_lines]
        buttons = state_buttons + alts_buttons + [edit_buttons] + links_buttons + [pagination_button_row]
        await msg.edit(
            text = "\n".join(lines),
            file = input_media,
            buttons = buttons,
            parse_mode = "html",
        )

    async def upload_propose_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"upload_propose:"):
            return
        user = self.bot.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        # Log callback data
        callback_data = event.data[len(b"upload_propose:"):].decode()
        logger.info("Upload propose menu callback data: %s", callback_data)
        # Find the right post
        event_msg = await event.get_message()
        await self.render_upload_propose_menu(event_msg, callback_data)
        raise StopPropagation

    async def render_upload_propose_menu(self, msg: Message, field: str) -> None:
        menu_data = parse_hidden_data(msg)
        post_id = int(menu_data["post_id"])
        menu_data["proposed_field"] = field
        # Fetch the post
        post = self.hoardbooru.getPost(post_id)
        post_description = get_post_description(post)
        gallery_upload_data = post_description.get_or_create_doc_matching_type(UploadDataPostDocument)
        # Get current field value
        reply_action: typing.Optional[str] = None
        extra_buttons = []
        if field == "title":
            current_value = gallery_upload_data.proposed_title
        elif field == "description":
            current_value = gallery_upload_data.proposed_description
        elif field == "tags":
            proposed_tags = gallery_upload_data.proposed_tags
            current_value = ", ".join(proposed_tags) if proposed_tags else None
        elif field == "alt_description":
            current_value = gallery_upload_data.alt_description
        elif field == "links":
            upload_links = gallery_upload_data.upload_links
            link_lines = []
            for n, upload_link in enumerate(upload_links, start = 1):
                link_lines += [f"{n}: {html.escape(upload_link.to_string())}"]
            current_value = "\n".join(link_lines)
            reply_action = "add a new upload link, or use the menu to modify a link"
            link_buttons = [
                Button.inline(f"{n}", f"upload_link:{n}")
                for n in range(1, len(upload_links) + 1)
            ]
            extra_buttons = [link_buttons[n:n+4] for n in range(0, len(link_buttons), 4)]
        else:
            raise ValueError(f"Unrecognised field for proposed upload data: {field}")
        reply_action = reply_action or f"set a new {field}"
        # Build the message
        menu_data_str = hidden_data(menu_data, ["query", "user_infix", "uploaded_only", "post_id", "proposed_field"])
        lines = []
        lines += [f"{menu_data_str}Editing field: {field}"]
        lines += [f"Post ID: {post_id} {self.bot.hoardbooru_post_url(post_id)}"]
        lines += [f"<b>Current {field}:</b>", html.escape(str(current_value))]
        lines += ["<b>---</b>", f"Reply to this message to {reply_action}"]
        await msg.edit(
            text = "\n".join(lines),
            buttons = extra_buttons + [[Button.inline("Return to page", f"unuploaded:{post_id}")]],
            parse_mode = "html",
        )
        raise StopPropagation

    async def propose_with_reply(self, event: events.NewMessage.Event) -> None:
        if not event.message.text:
            return
        # Fetch menu data
        menu_msg = await event.get_reply_message()
        if not menu_msg:
            logger.info("New tag message is not a reply to a tag phase menu")
            return
        # Gather menu data
        menu_data = parse_hidden_data(menu_msg)
        post_id = int(menu_data["post_id"])
        proposed_field = menu_data["proposed_field"]
        # Gather post data
        post = self.hoardbooru.getPost(post_id)
        post_desc = get_post_description(post)
        upload_data = post_desc.get_or_create_doc_matching_type(UploadDataPostDocument)
        # Set the proposed field
        if proposed_field == "title":
            upload_data.proposed_title = event.message.text
            resp_text = f"Set title to:\n{upload_data.proposed_title}"
        elif proposed_field == "description":
            upload_data.proposed_description = event.message.text
            resp_text = f"Set description to:\n{upload_data.proposed_description}"
        elif proposed_field == "tags":
            msg_text = event.message.text
            upload_data.proposed_tags = re.split(r"[\s,]+", msg_text)
            resp_text = f"Set tags to:\n{', '.join(upload_data.proposed_tags)}"
        elif proposed_field == "alt_description":
            upload_data.alt_description = event.message.text
            resp_text = f"Set the alt description to: {upload_data.alt_description}"
        elif proposed_field == "links":
            msg_text = event.message.text
            links = links_in_msg(event.message)
            if msg_text[:4].lower() == "bulk" or len(links) > 1:
                try:
                    new_links = UploadLink.from_bulk_links(links, post)
                except Exception as e:
                    await event.reply(f"Failed to parse bulk upload links:\n{e!r}")
                    raise StopPropagation
            else:
                try:
                    new_links = [UploadLink.from_string(event.message.text, post)]
                except Exception as e:
                    await event.reply(f"Failed to parse upload link:\n{e!r}")
                    raise StopPropagation
            resp_lines = ["Added new upload link:"]
            for new_link in new_links:
                upload_data.add_upload_link(new_link)
                resp_lines.append(new_link.to_string())
            resp_text = "\n".join(resp_lines)
        else:
            raise ValueError(f"Could not set proposed field, unrecognised field: {proposed_field}")
        # Save the data
        set_post_description(post, post_desc)
        await event.reply(resp_text, link_preview=False)
        await self.render_upload_propose_menu(menu_msg, proposed_field)
        raise StopPropagation

    async def upload_link_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"upload_link:"):
            return
        user = self.bot.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        # Log callback data
        callback_data = event.data[len(b"upload_link:"):].decode()
        logger.info("Upload link menu callback data: %s", callback_data)
        # Render the menu
        event_msg = await event.get_message()
        await self.render_upload_link_menu(event_msg, callback_data)
        raise StopPropagation

    async def render_upload_link_menu(self, msg: Message, link_num: str) -> None:
        menu_data = parse_hidden_data(msg)
        post_id = int(menu_data["post_id"])
        menu_data["upload_link_num"] = link_num
        # Fetch the post
        post = self.hoardbooru.getPost(post_id)
        post_description = get_post_description(post)
        gallery_upload_data = post_description.get_or_create_doc_matching_type(UploadDataPostDocument)
        # Get the right upload link
        upload_link = gallery_upload_data.upload_links[int(link_num) - 1]
        menu_data_str = hidden_data(menu_data, ["query", "user_infix", "uploaded_only", "post_id", "proposed_field", "upload_link_num"])
        lines = []
        lines += [f"{menu_data_str}Editing upload link"]
        lines += [f"Post ID: {post_id} {self.bot.hoardbooru_post_url(post_id)}"]
        lines += ["Modifying upload link:"]
        lines += [html.escape(upload_link.to_string())]
        lines += ["Use menu to set upload link type, or delete link, and reply to this message to set the upload link info"]
        link_type_buttons = []
        for link_type in UploadLinkUploaderType:
            if link_type == UploadLinkUploaderType.E621:
                continue
            link_type_text = tick_if_true(upload_link.uploader_type == link_type) + " " + link_type.name.title()
            link_type_buttons += [Button.inline(link_type_text, f"upload_link_type:{link_type.value}")]
        buttons = [link_type_buttons[n:n+2] for n in range(0, len(link_type_buttons), 2)]
        buttons += [[Button.inline("❌ Delete link", "upload_link_delete")]]
        buttons += [[Button.inline("⏎ Return to upload links", "upload_propose:links")]]
        await msg.edit(
            text = "\n".join(lines),
            buttons = buttons,
            parse_mode = "html",
        )

    async def upload_link_type_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"upload_link_type:"):
            return
        user = self.bot.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        # Log callback data
        callback_data = event.data[len(b"upload_link_type:"):].decode()
        logger.info("Upload link type menu callback data: %s", callback_data)
        # Find the right post
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        post_id = int(menu_data["post_id"])
        link_num = menu_data["upload_link_num"]
        link_idx = int(link_num) - 1
        # Fetch the post
        post = self.hoardbooru.getPost(post_id)
        post_description = get_post_description(post)
        gallery_upload_data = post_description.get_or_create_doc_matching_type(UploadDataPostDocument)
        # Get the right upload link
        upload_link = gallery_upload_data.upload_links[link_idx]
        # Set the type on the upload link
        upload_link_type = UploadLinkUploaderType(callback_data)
        upload_link.uploader_type = upload_link_type
        gallery_upload_data.set_upload_link(link_idx, upload_link)
        set_post_description(post, post_description)
        # Render the menu
        await self.render_upload_link_menu(event_msg, link_num)
        raise StopPropagation

    async def upload_link_delete_callback(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"upload_link_delete"):
            return
        user = self.bot.trusted_user_by_id(event.sender_id)
        if user is None:
            return
        # Log callback data
        logger.info("Upload link delete menu callback")
        # Find the right post
        event_msg = await event.get_message()
        menu_data = parse_hidden_data(event_msg)
        post_id = int(menu_data["post_id"])
        link_num = menu_data["upload_link_num"]
        link_idx = int(link_num) - 1
        # Fetch the post
        post = self.hoardbooru.getPost(post_id)
        post_description = get_post_description(post)
        gallery_upload_data = post_description.get_or_create_doc_matching_type(UploadDataPostDocument)
        # Delete the upload link
        gallery_upload_data.remove_upload_link(link_idx)
        set_post_description(post, post_description)
        # Render the menu
        await self.render_upload_propose_menu(event_msg, "links")
        raise StopPropagation

    async def upload_link_info_with_reply(self, event: events.NewMessage.Event) -> None:
        if not event.message.text:
            return
        # Fetch menu data
        menu_msg = await event.get_reply_message()
        if not menu_msg:
            logger.info("New upload link info message is not a reply to a upload link info menu")
            return
        # Gather menu data
        menu_data = parse_hidden_data(menu_msg)
        post_id = int(menu_data["post_id"])
        link_num = menu_data["upload_link_num"]
        link_idx = int(link_num) - 1
        # Gather post data
        post = self.hoardbooru.getPost(post_id)
        post_desc = get_post_description(post)
        upload_data = post_desc.get_or_create_doc_matching_type(UploadDataPostDocument)
        # Find the upload link
        upload_link = upload_data.upload_links[link_idx]
        # Update link info
        link_info = event.message.text
        if link_info.lower() in ["spangle", "zephyr"]:
            link_info = link_info.lower()
        upload_link.uploader_type_info = link_info
        upload_data.set_upload_link(link_idx, upload_link)
        set_post_description(post, post_desc)
        # Send reply and update menu
        await event.reply(f"Set upload link info to: {link_info}", link_preview=False)
        await self.render_upload_link_menu(menu_msg, link_num)
        raise StopPropagation
