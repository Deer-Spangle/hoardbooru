import dataclasses
from abc import ABC, abstractmethod

import pyszuru
from telethon import Button


@dataclasses.dataclass
class TagEntry:
    tag_name: str
    button_name: str

    def to_button(self, post_tags: list[pyszuru.Tag]) -> Button:
        tag_names = [n for t in post_tags for n in t.names]
        tick = "✅" if self.tag_name in tag_names else ""
        return Button.inline(
            f"{tick}{self.button_name}",
            f"tag:{self.tag_name}".encode(),
        )


class TagPhase(ABC):
    allow_ordering = True

    def name(self) -> str:
        raise NotImplementedError()

    def question(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def list_tags(self) -> list[TagEntry]:
        raise NotImplementedError()

    def next_phase(self) -> str:
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



PHASES = {
    "comm_status": CommStatus
}