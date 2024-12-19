import dataclasses
from abc import ABC, abstractmethod
from typing import Optional

import pyszuru
from telethon import Button


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
    def next_phase(self) -> str:
        raise NotImplementedError()

    def popularity_filter_tag(self, current_post: pyszuru.Post) -> Optional[str]:
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

    def next_phase(self) -> str:
        return "our_characters"


class OurCharacters(TagPhase):

    def name(self) -> str:
        return "Our characters"

    def question(self) -> str:
        return "Which of our characters does this include?"

    def list_tags(self) -> list[TagEntry]:
        tags = self.hoardbooru.search_tag("category:our_characters")
        return [TagEntry(
            tag.primary_name,
            tag.primary_name,
        ) for tag in tags]

    def next_phase(self) -> str:
        return "other_characters"

    def popularity_filter_tag(self, current_post: pyszuru.Post) -> Optional[str]:
        return None


PHASES = {
    "comm_status": CommStatus,
    "our_characters": OurCharacters,
}
