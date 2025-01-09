import dataclasses
from abc import ABC, abstractmethod
from typing import Optional, Iterator, Type

import pyszuru
from telethon import Button, events
from telethon.events import StopPropagation


def _list_our_characters_in_post(current_post: pyszuru.Post) -> list[str]:
    character_tags = []
    for tag in current_post.tags:
        if tag.category == "our_characters":
            character_tags.append(tag.primary_name)
    return character_tags


def _list_artists_in_post(current_post: pyszuru.Post) -> list[str]:
    artist_tags = []
    for tag in current_post.tags:
        if tag.category == "artists":
            artist_tags.append(tag.primary_name)
    return artist_tags


class Buttonable(ABC):
    def to_button(self, post_tags: list[pyszuru.Tag]) -> Button:
        raise NotImplementedError


@dataclasses.dataclass
class TagEntry(Buttonable):
    tag_name: str
    button_name: str
    popularity: Optional[int] = dataclasses.field(default=None)

    def to_button(self, post_tags: list[pyszuru.Tag]) -> Button:
        tag_names = [n for t in post_tags for n in t.names]
        tick = "✅" if self.tag_name in tag_names else ""
        button_text = f"{tick}{self.button_name}"
        if self.popularity is not None:
            button_text += f" ({self.popularity})"
        return Button.inline(
            button_text,
            f"tag:{self.tag_name}".encode(),
        )


class NewCommissionButton(Buttonable):
    def to_button(self, post_tags: list[pyszuru.Tag]) -> Button:
        return Button.inline(
            "New commission",
            f"tag:special:new_commission",
        )

    @staticmethod
    def on_press(post: pyszuru.Post, press_evt: events.CallbackQuery.Event) -> None:
        # Check if already commission tagged
        comm_tags = [t for t in post.tags if t.category == "meta-commissions"]
        if comm_tags:
            press_evt.respond("This post already has a commission")
            raise StopPropagation
        # Find the latest commission tag name
        hoardbooru: pyszuru.API = post.api
        latest_comm_tags = hoardbooru.search_tag("category:meta-commissions -sort:name -usage-count:0")
        latest_comm_tag = next(latest_comm_tags, None)
        if latest_comm_tag is None:
            latest_comm_number = 0
        else:
            latest_comm_number = int(latest_comm_tag.primary_name.removeprefix("commission_"))
        new_comm_tag_name = "commission_" + str(latest_comm_number+1).zfill(5)
        # Create or get the tag
        try:
            htag = hoardbooru.getTag(new_comm_tag_name)
        except pyszuru.api.SzurubooruHTTPError:
            htag = hoardbooru.createTag(new_comm_tag_name)
            htag.category = "meta-commissions"
            htag.push()
        # Add tag to the post
        post.tags += [htag]
        post.push()


SPECIAL_BUTTON_CALLBACKS = {
    "new_commission": NewCommissionButton.on_press,
}


def _tags_to_tag_entries(tags: Iterator[pyszuru.Tag]) -> list[TagEntry]:
    return [TagEntry(
        tag.primary_name,
        tag.primary_name,
    ) for tag in tags]


class TagPhase(ABC):
    allow_ordering = True

    def __init__(self, hoardbooru: pyszuru.API) -> None:
        self.hoardbooru = hoardbooru

    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def question(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        raise NotImplementedError()

    def new_tag_category(self) -> Optional[str]:
        return None

    @abstractmethod
    def next_phase(self, current_post: pyszuru.Post) -> str:
        raise NotImplementedError()

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        raise NotImplementedError()

    def post_check(self, current_post: pyszuru.Post) -> None:
        pass


class CommStatus(TagPhase):
    allow_ordering = False

    def name(self) -> str:
        return "Commission status"

    def question(self) -> str:
        return "Is this a WIP or final?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        return [
            TagEntry("status:wip", "wip"),
            TagEntry("status:final", "final"),
        ]

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "our_characters"

    def post_check(self, current_post: pyszuru.Post) -> None:
        is_final = False
        is_wip = False
        for tag in current_post.tags:
            if tag.primary_name == "status:final":
                is_final = True
            if tag.primary_name == "status:wip":
                is_wip = True
        if is_final and is_wip:
            raise ValueError("Post cannot be both final and WIP")
        if not is_final and not is_wip:
            raise ValueError("Post must be at least one of final and WIP")
        if is_final:
            current_post.tags = [t for t in current_post.tags if t.primary_name != TAGGING_TAG_FORMAT.format("wip_tags")]
            current_post.push()
        if is_wip:
            current_post.tags = [t for t in current_post.tags if t.primary_name != TAGGING_TAG_FORMAT.format("upload")]
            current_post.push()


class OurCharacters(TagPhase):

    def name(self) -> str:
        return "Our characters"

    def question(self) -> str:
        return "Which of our characters does this include?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        tags = self.hoardbooru.search_tag("category:our_characters")
        return _tags_to_tag_entries(tags)

    def new_tag_category(self) -> Optional[str]:
        return "our_characters"

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "other_characters"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return []


class OtherCharacters(TagPhase):

    def name(self) -> str:
        return "Other characters"

    def question(self) -> str:
        return "Which other characters appear in this?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        tags = self.hoardbooru.search_tag("category:characters")
        return _tags_to_tag_entries(tags)

    def new_tag_category(self) -> Optional[str]:
        return "characters"

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "artist"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


class Artist(TagPhase):

    def name(self) -> str:
        return "Artist"

    def question(self) -> str:
        return "Who is the artist (or artists) of this piece?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        tags = self.hoardbooru.search_tag("category:artists")
        return _tags_to_tag_entries(tags)

    def new_tag_category(self) -> Optional[str]:
        return "artists"

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "meta_commissions"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


class MetaCommission(TagPhase):
    allow_ordering = False

    def name(self) -> str:
        return "Commission pool"

    def question(self) -> str:
        return "Is this a new commission or part of an existing one from this artist?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        # Figure out existing comm tags
        artists = _list_artists_in_post(current_post)
        artist_search = ",".join(artists)
        previous_posts = self.hoardbooru.search_post(f"{artist_search} -id:{current_post.id_}")
        comm_tags = set()
        for post in previous_posts:
            for tag in post.tags:
                if tag.category == "meta-commissions":
                    comm_tags.add(tag.primary_name)
        # Construct buttons
        return [NewCommissionButton()] + sorted(
            [TagEntry(tag, tag) for tag in comm_tags],
            key=lambda tag_entry: tag_entry.tag_name,
        )

    def new_tag_category(self) -> Optional[str]:
        return "meta-commissions"

    def next_phase(self, current_post: pyszuru.Post) -> str:
        for tag in current_post.tags:
            if tag.primary_name == "status:wip":
                return "wip_tags"
        return "meta"


class WipTags(TagPhase):
    allow_ordering = False

    def name(self) -> str:
        return "WIP-specific tags"

    def question(self) -> str:
        return "Do any of these wip-specific tags apply?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        tags = self.hoardbooru.search_tag("category:meta-wip")
        return sorted(
            [TagEntry(
                tag.primary_name,
                tag.primary_name
            ) for tag in tags],
            key=lambda tag_entry: tag_entry.tag_name,
        )

    def new_tag_category(self) -> Optional[str]:
        return "meta-wip"

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "meta"


class MetaTags(TagPhase):

    def name(self) -> str:
        return "Meta tags"

    def question(self) -> str:
        return "Do any of these meta tags, about the nature of the commission, apply?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        tags = self.hoardbooru.search_tag("category:meta")
        return _tags_to_tag_entries(tags)

    def new_tag_category(self) -> Optional[str]:
        return "meta"

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "kink"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


class KinkTags(TagPhase):

    def name(self) -> str:
        return "Kinks and themes"

    def question(self) -> str:
        return "Which of these kink and theme tags apply?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        tags = self.hoardbooru.search_tag("category:default")
        return _tags_to_tag_entries(tags)

    def new_tag_category(self) -> Optional[str]:
        return "default"

    def next_phase(self, current_post: pyszuru.Post) -> str:
        for tag in current_post.tags:
            if tag.primary_name == "status:final":
                return "upload"
        return "done"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


class UploadedTo(TagPhase):

    def name(self) -> str:
        return "Uploaded to"

    def question(self) -> str:
        return "Which sites has (or hasn't) this been uploaded to?"

    def list_tags(self, current_post: pyszuru.Post) -> list[Buttonable]:
        tags = self.hoardbooru.search_tag("category:meta-uploads")
        return [TagEntry(
            tag.primary_name,
            tag.primary_name.removeprefix("uploaded_to:"),
        ) for tag in tags]

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "done"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


PHASES: dict[str, Type[TagPhase]] = {
    "comm_status": CommStatus,
    "our_characters": OurCharacters,
    "other_characters": OtherCharacters,
    "artist": Artist,
    "meta_commissions": MetaCommission,
    "wip_tags": WipTags,
    "meta": MetaTags,
    "kink": KinkTags,
    "upload": UploadedTo,
}

TAGGING_TAG_FORMAT = "tagging:needs_{}"

DEFAULT_TAGGING_TAGS = [
    TAGGING_TAG_FORMAT.format(phase)
    for phase in PHASES.keys()
] + ["tagging:needs_relations"]
