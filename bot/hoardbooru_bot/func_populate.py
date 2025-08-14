import logging

from telethon import TelegramClient, events
from telethon.events import StopPropagation

from hoardbooru_bot.functionality import Functionality
from hoardbooru_bot.inline_params import InlineParams

logger = logging.getLogger(__name__)


class PopulateFunctionality(Functionality):

    def register_callbacks(self, client: TelegramClient) -> None:
        client.add_event_handler(
            self.populate_cache,
            events.NewMessage(pattern="/populate", incoming=True, from_users=self.bot.trusted_user_ids()),
        )


    async def populate_cache(self, event: events.NewMessage.Event) -> None:
        if not event.message.text.startswith("/populate"):
            return
        logger.info("Populating cache")
        # Parse the input
        populate_count = 10
        populate_search = []
        populate_files = True
        populate_photos = True
        populate_input = event.message.text.removeprefix("/populate").strip().split()
        for populate_term in populate_input:
            try:
                populate_count = int(populate_term)
                continue
            except ValueError:
                pass
            if populate_term in InlineParams.FILE_TERMS:
                populate_files = True
                populate_photos = False
                continue
            if populate_term.startswith("-") and populate_term.removeprefix("-") in InlineParams.FILE_TERMS:
                populate_files = False
                populate_photos = True
                continue
            populate_search.append(populate_term)
        logger.info(
            "Aiming to populate %s items, (files=%s, photos=%s) for search query: %s",
            populate_count, populate_files, populate_photos, " ".join(populate_search)
        )
        # Work out how many matching posts on hoardbooru
        cache_progress_msg = await event.reply("⏳ Calculating cache size")
        posts = []
        for post in self.hoardbooru.search_post(" ".join(populate_search), page_size=100):
            posts.append(post)
        cache_ids = None
        if populate_search:
            cache_ids = [p.id_ for p in posts]
        cache_size = await self.bot.media_cache.cache_size(cache_ids, populate_files, populate_photos)
        expected_cache_size = len(posts) * (populate_files + populate_photos)
        if cache_size == expected_cache_size:
            await event.reply(f"There are {len(posts)} posts on hoardbooru. The cache is full, at {cache_size} entries")
            await cache_progress_msg.delete()
            raise StopPropagation
        await event.reply(
            f"There are {len(posts)} posts on hoardbooru. Cache size is {cache_size}/{expected_cache_size}"
        )
        await cache_progress_msg.delete()
        # Populate the cache
        progress_msg = await event.reply(f"⏳ Populating {populate_count} cache entries")
        populated = 0
        for post in posts:
            # Check if we've populated enough
            if populated >= populate_count:
                break
            # Populate photo
            if populate_photos:
                if await self.bot.media_cache.load_cache(post.id_, False) is None:
                    await self.bot.media_cache.store_in_cache(post, False)
                    populated += 1
            # Check again if we've populated enough
            if populated >= populate_count:
                break
            # Populate file
            if populate_files:
                if await self.bot.media_cache.load_cache(post.id_, True) is None:
                    await self.bot.media_cache.store_in_cache(post, True)
                    populated += 1
        # Post the completion message
        cache_size = await self.bot.media_cache.cache_size(cache_ids, populate_files, populate_photos)
        await progress_msg.delete()
        await event.reply(f"Populated {populated} cache entries. Cache size: {cache_size}/{expected_cache_size}")
        raise StopPropagation
