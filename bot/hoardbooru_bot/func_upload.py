import glob
import logging
from typing import Optional
from zipfile import ZipFile, ZipInfo

import aiofiles
import pyszuru
from telethon import TelegramClient, events, Button
from telethon.events import StopPropagation
from telethon.tl.types import DocumentAttributeFilename

from hoardbooru_bot.functionality import Functionality
from hoardbooru_bot.hidden_data import hidden_data, parse_hidden_data
from hoardbooru_bot.tag_phases import DEFAULT_TAGGING_TAGS
from hoardbooru_bot.utils import file_ext, temp_sandbox_file

logger = logging.getLogger(__name__)


def filter_document(evt: events.NewMessage.Event) -> bool:
    if not evt.message.document:
        return False
    return True


def filter_photo(evt: events.NewMessage.Event) -> bool:
    if not evt.message.photo:
        return False
    return True



class UploadFunctionality(Functionality):
    def register_callbacks(self, client: TelegramClient) -> None:
        client.add_event_handler(
            self.upload_document,
            events.NewMessage(func=lambda e: filter_document(e), incoming=True, from_users=self.bot.trusted_user_ids()),
        )
        client.add_event_handler(
            self.upload_photo,
            events.NewMessage(func=lambda e: filter_photo(e), incoming=True, from_users=self.bot.trusted_user_ids()),
        )
        client.add_event_handler(self.upload_confirm, events.CallbackQuery(pattern="upload:"))

    async def upload_document(self, event: events.NewMessage.Event) -> None:
        if not event.message.document:
            return
        logger.info("Received document to upload")
        # Get filename
        file_name = None
        for attribute in event.message.document.attributes:
            if isinstance(attribute, DocumentAttributeFilename):
                file_name = attribute.file_name
        await self._upload_media(event, file_name)

    async def upload_photo(self, event: events.NewMessage.Event) -> None:
        if not event.message.photo:
            return
        logger.info("Received photo to upload")
        file_name = "photo.jpg"
        await self._upload_media(event, file_name)

    async def upload_confirm(self, event: events.CallbackQuery.Event) -> None:
        if not event.data.startswith(b"upload:"):
            return
        upload_resp = event.data.replace(b"upload:", b"")
        if upload_resp == b"cancel":
            logger.debug("Hoardbooru upload cancelled")
            await event.delete()
            raise StopPropagation
        logger.debug("Creating hoardbooru post")
        event_msg = await event.get_message()
        original_msg = await event_msg.get_reply_message()
        menu_data = parse_hidden_data(event_msg)
        file_token = pyszuru.FileToken(menu_data["token"], menu_data["filepath"])
        post_rating = {
            b"sfw": "safe",
            b"nsfw": "unsafe",
        }[upload_resp]
        # Create the hoardbooru post
        post = self.hoardbooru.createPost(file_token, post_rating)
        logger.info("Created hoardbooru post: %s", post.id_)
        # Apply some default tags
        check_tags = []
        for check_tag_name in DEFAULT_TAGGING_TAGS:
            try:
                check_tag = self.hoardbooru.getTag(check_tag_name)
            except pyszuru.api.SzurubooruHTTPError:
                check_tag = self.hoardbooru.createTag(check_tag_name)
            check_tag.category = "meta-tagging"
            check_tag.push()
            check_tags.append(check_tag)
        post.tags = check_tags
        post.push()
        # Store in cache
        await self.bot.media_cache.store_in_cache(post, False)
        if await self.bot.media_cache.load_cache(post.id_, True) is None:
            await self.bot.media_cache.store_in_cache(post, True)
        # Reply with post link
        await event.delete()
        await original_msg.reply(f"Uploaded to hoardbooru:\n{self.bot.hoardbooru_post_url(post.id_)}")
        # Start tagging phase
        tag_menu_data = {
            "post_id": str(post.id_),
            "tag_phase": "comm_status",
            "page": str(0),
            "order": "popular"
        }
        tag_msg = await original_msg.reply("Initialising tag helper")
        await self.bot.functionality_tagging.post_tag_phase_menu(tag_msg, tag_menu_data)
        raise StopPropagation

    async def _upload_to_hoardbooru(self, file_path: str, file_name: Optional[str]) -> pyszuru.FileToken:
        ext = None
        if file_name is not None:
            ext = file_ext(file_name)
        if ext in ["sai", "swf", "xcf"]:
            logger.debug("Zipping up the %s file", ext)
            # Read the file bytes
            async with aiofiles.open(file_path, "rb") as f:
                file_bytes = await f.read()
            # Zip it up
            zip_name = f"{file_name}.zip"
            with ZipFile(zip_name, 'w') as zip_file:
                # Hardcode the timestamp, so that sha1 might detect duplicates
                info = ZipInfo(filename=file_name, date_time=(1980, 1, 1, 0, 0, 0))
                zip_file.writestr(info, file_bytes)
            file_path = zip_name
        logger.debug("Uploading file to hoardbooru: %s", file_name)
        with open(file_path, mode="rb") as fr:
            file_token = self.hoardbooru.upload_file(fr)
        return file_token

    async def _upload_media(self, event: events.NewMessage.Event, file_name: Optional[str]) -> None:
        progress_msg = await event.reply("Uploading and checking for duplicates")
        async with temp_sandbox_file(ext=None) as temp_path:
            # Download the document
            await event.message.download_media(temp_path)
            dl_path = glob.glob(f"{temp_path}*")[0]
            # Upload to hoardbooru
            file_token = await self._upload_to_hoardbooru(dl_path, file_name)
        # Create hidden menu data
        menu_data = hidden_data({
            "token": file_token.token,
            "filepath": file_token.filepath,
        })
        # Check for duplicates
        match_results = self.hoardbooru.search_by_image(file_token)
        logger.debug(f"There are {len(match_results)} posts matching this file")
        if match_results:
            # Check for exact matches
            exact_matches = [x for x in match_results if x.exact]
            if exact_matches:
                exact_match: pyszuru.Post = exact_matches[0].post
                post_url = self.bot.hoardbooru_post_url(exact_match.id_)
                await event.reply(f"This file already exists on hoardbooru.\nLink: {post_url}")
                await progress_msg.delete()
                raise StopPropagation
            sorted_matches = sorted(match_results, key=lambda x: x.distance, reverse=True)
            match_lines = "\n".join([
                f"- {self.bot.hoardbooru_post_url(m.post.id_)} ({100*m.distance:.2f}%)" for m in sorted_matches
            ])
            await event.reply(
                f"{menu_data}This file potentially matches {len(sorted_matches)} posts!\n{match_lines}\n"
                "\nAre you sure you want to create a new post?",
                buttons=[
                    [Button.inline("Create post (SFW)", b"upload:sfw")],
                    [Button.inline("Create post (NSFW)", b"upload:nsfw")],
                    [Button.inline("Cancel", b"upload:cancel")],
                ],
                parse_mode="html",
            )
            await progress_msg.delete()
            raise StopPropagation
        # No matches, cool
        await event.reply(
            f"{menu_data}This document has no duplicates.\nWould you like to create a new post?",
            buttons=[
                [Button.inline("Create post (SFW)", b"upload:sfw")],
                [Button.inline("Create post (NSFW)", b"upload:nsfw")],
                [Button.inline("Cancel", b"upload:cancel")],
            ],
            parse_mode="html",
        )
        # Delete progress message
        await progress_msg.delete()
