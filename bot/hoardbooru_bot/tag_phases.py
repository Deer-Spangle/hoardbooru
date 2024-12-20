import dataclasses
from abc import ABC, abstractmethod
from typing import Optional, Iterator

import pyszuru
from telethon import Button


def _list_our_characters_in_post(current_post: pyszuru.Post) -> list[str]:
    character_tags = []
    for tag in current_post.tags:
        if tag.category == "our_characters":
            character_tags.append(tag.primary_name)
    return character_tags


@dataclasses.dataclass
class TagEntry:
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
    def list_tags(self) -> list[TagEntry]:
        raise NotImplementedError()

    @abstractmethod
    def next_phase(self, current_post: pyszuru.Post) -> str:
        raise NotImplementedError()

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        raise NotImplementedError()


class CommStatus(TagPhase):
    allow_ordering = False

    def name(self) -> str:
        return "Commission status"

    def question(self) -> str:
        return "Is this a WIP or final?"

    def list_tags(self) -> list[TagEntry]:
        return [
            TagEntry("status:wip", "wip"),
            TagEntry("status:final", "final"),
        ]

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "our_characters"


class OurCharacters(TagPhase):

    def name(self) -> str:
        return "Our characters"

    def question(self) -> str:
        return "Which of our characters does this include?"

    def list_tags(self) -> list[TagEntry]:
        tags = self.hoardbooru.search_tag("category:our_characters")
        return _tags_to_tag_entries(tags)

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "other_characters"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return []


class OtherCharacters(TagPhase):

    def name(self) -> str:
        return "Other characters"

    def question(self) -> str:
        return "Which other characters appear in this?"

    def list_tags(self) -> list[TagEntry]:
        tags = self.hoardbooru.search_tag("category:characters")
        return _tags_to_tag_entries(tags)

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "artist"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


class Artist(TagPhase):

    def name(self) -> str:
        return "Artist"

    def question(self) -> str:
        return "Who is the artist (or artists) of this piece?"

    def list_tags(self) -> list[TagEntry]:
        tags = self.hoardbooru.search_tag("category:artists")
        return _tags_to_tag_entries(tags)

    def next_phase(self, current_post: pyszuru.Post) -> str:
        for tag in current_post.tags:
            if tag.primary_name == "status\\:wip":
                return "wip_tags"
        return "meta"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


class WipTags(TagPhase):
    allow_ordering = False

    def name(self) -> str:
        return "WIP-specific tags"

    def question(self) -> str:
        return "Do any of these wip-specific tags apply?"

    def list_tags(self) -> list[TagEntry]:
        tags = self.hoardbooru.search_tag("category:meta-wip")
        return sorted(
            [TagEntry(
                tag.primary_name,
                tag.primary_name
            ) for tag in tags],
            key=lambda tag_entry: tag_entry.tag_name,
        )

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "meta"


class MetaTags(TagPhase):

    def name(self) -> str:
        return "Meta tags"

    def question(self) -> str:
        return "Do any of these meta tags, about the nature of the commission, apply?"

    def list_tags(self) -> list[TagEntry]:
        tags = self.hoardbooru.search_tag("category:meta")
        return _tags_to_tag_entries(tags)

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "kink"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


class KinkTags(TagPhase):

    def name(self) -> str:
        return "Kinks and themes"

    def question(self) -> str:
        return "Which of these kink and theme tags apply?"

    def list_tags(self) -> list[TagEntry]:
        tags = self.hoardbooru.search_tag("category:default")
        return _tags_to_tag_entries(tags)

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "upload"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


class UploadedTo(TagPhase):

    def name(self) -> str:
        return "Uploaded to"

    def question(self) -> str:
        return "Which sites has (or hasn't) this been uploaded to?"

    def list_tags(self) -> list[TagEntry]:
        tags = self.hoardbooru.search_tag("category:meta-uploads")
        return [TagEntry(
            tag.primary_name,
            tag.primary_name.removeprefix("uploaded_to:"),
        ) for tag in tags]

    def next_phase(self, current_post: pyszuru.Post) -> str:
        return "done"

    def popularity_filter_tags(self, current_post: pyszuru.Post) -> list[str]:
        return _list_our_characters_in_post(current_post)


PHASES = {
    "comm_status": CommStatus,
    "our_characters": OurCharacters,
    "other_characters": OtherCharacters,
    "artist": Artist,
    "wip_tags": WipTags,
    "meta": MetaTags,
    "kink": KinkTags,
    "upload": UploadedTo,
}
