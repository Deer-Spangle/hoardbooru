import dataclasses


@dataclasses.dataclass
class TrustedUser:
    telegram_id: int
    blocked_tags: list[str]
    owner_tag: str
    upload_tag_infix: str

    @classmethod
    def from_json(cls, data: dict) -> "TrustedUser":
        return cls(
            data["telegram_id"],
            data.get("blocked_tags", []),
            data["owner_tag"],
            data["upload_tag_infix"],
        )
