import dataclasses
import enum
import json
import re
import urllib.parse
# noinspection DuplicatedCode
from abc import ABC, abstractmethod
import datetime
from typing import Type, Optional, TypeVar

import pyszuru
import requests
import yaml


class PostDocument(ABC):

    @abstractmethod
    def to_string(self) -> str:
        raise NotImplementedError()

    @classmethod
    def parse_text(cls, raw_text: str) -> "PostDocument":
        yaml_text = raw_text.removeprefix("---\n").removeprefix("```\n").removesuffix("\n---").removesuffix("\n```")
        try:
            yaml_doc = yaml.safe_load(yaml_text)
        except yaml.YAMLError:
            return RawTextPostDocument(raw_text)
        data_type = yaml_doc["data_type"]
        return {
            "notion": NotionPostDocument.from_yaml,
            "telegram": TelegramPostDocument,
            "upload_data": UploadDataPostDocument,
        }[data_type](yaml_doc)


class RawTextPostDocument(PostDocument):

    def __init__(self, raw_text: str = "") -> None:
        self.raw_text = raw_text

    def to_string(self) -> str:
        return self.raw_text


class YamlPostDocument(PostDocument, ABC):

    def __init__(self, yaml_doc: dict = None) -> None:
        self.yaml_doc = yaml_doc or {}

    def to_string(self) -> str:
        if not self.yaml_doc:
            return "---"
        return yaml.dump(self.yaml_doc, sort_keys=False)


# noinspection DuplicatedCode
class NotionPostDocument(YamlPostDocument):

    def __init__(self, card_data: dict, file_property_name: str, file_idx: int, sync_datetime: datetime.datetime) -> None:
        card_title = card_data["properties"]["Name"]["title"][0]["plain_text"]
        card_url = card_data["url"]
        yaml_doc = {
            "data_type": "notion",
            "sync_datetime": sync_datetime.isoformat(),
            "card_title": card_title,
            "card_url": card_url,
            "file_property_name": file_property_name,
            "file_idx": file_idx,
            "card_data": card_data,
        }
        super().__init__(yaml_doc)

    @classmethod
    def from_yaml(cls, data: dict) -> "NotionPostDocument":
        card_data = data["card_data"]
        file_property_name = data["file_property_name"]
        file_idx = data["file_idx"]
        sync_datetime = data["sync_datetime"]
        return cls(card_data, file_property_name, file_idx, sync_datetime)


class TelegramPostDocument(YamlPostDocument):
    """
    data_type: telegram
    upload_via: document/photo/fa_url/e6_url/url
    document_filename: TaeMlim_Zeph.png
    msg_datetime: 2024-12-26T00:00:00
    forwarded_from:
      username: @Taerynir
      display_name: Taerynir
      user_id: 12345
    message_text: "Added a few more detail lines❤️"
    """
    pass


class UploadLinkUploaderType(enum.Enum):
    UNKNOWN = "unknown"
    OURS = "ours"
    ARTIST = "artist"
    E621 = "e621"
    OTHER_CHARACTER = "other_character"


def extract_upload_link_info(link: str, website: str) -> Optional[str]:
    if website == "e621":
        return None
    if website == "weasyl":
        profile_name = re.search(r"/~([^/]+)/", link).group(1)
        if profile_name == "deerspangle":
            return "spangle"
        return profile_name
    if website == "twitter":
        return re.search(r"https://twitter.com/([^/]+)/status", link).group(1)
    if website == "bluesky":
        return re.search(r"bsky.app/profile/([^/]+)/post", link).group(1)
    if website == "furaffinity":
        post_id = re.search(r"/view/([0-9]+)/?$", link).group(1)
        resp = requests.get(f"https://faexport.spangle.org.uk/submission/{post_id}.json").json()
        profile_name = resp["profile_name"]
        # TODO: improve with automation on tag data
        if profile_name == "dr-spangle":
            return "spangle"
        if profile_name == "zephyr42":
            return "zephyr"
        return profile_name


@dataclasses.dataclass
class UploadLink:
    link: str
    uploader_type: UploadLinkUploaderType
    uploader_type_info: Optional[str]
    website: str

    def to_string(self) -> str:
        info = self.uploader_type_info
        type_str = self.uploader_type.value
        if info:
            type_str += f" ({info})"
        return f"{type_str}: {self.link}"

    @classmethod
    def from_string(cls, user_input: str, post: pyszuru.Post) -> "UploadLink":
        pattern = re.compile(r"^((?P<type>[A-Za-z0-9_]+)( *\((?P<info>.+)\))? *: +)?(?P<link>[\S]+)$")
        match = pattern.match(user_input)
        if not match:
            raise ValueError("Could not parse upload")
        link_str = match.group("link")
        type_str = match.group("type")
        uploader_type = UploadLinkUploaderType.UNKNOWN
        if type_str:
            if type_str.lower() == "other":
                type_str = "other_character"
            uploader_type = UploadLinkUploaderType(type_str.lower())
        info_str = match.group("info")
        if info_str is not None and info_str.lower() in ["spangle", "zephyr"]:
            info_str = info_str.lower()
        parsed_url = urllib.parse.urlparse(link_str)
        website = {
            "furaffinity.net": "furaffinity",
            "e621.net": "e621",
            "weasyl.com": "weasyl",
            "sofurry.com": "sofurry",
            "furrynetwork.com": "furrynetwork",
            "inkbunny.net": "inkbunny",
            "twitter.com": "twitter",
            "bsky.app": "bliuesky",
        }.get(parsed_url.netloc.removeprefix("www."))
        if website is None:
            raise ValueError("Unrecognized website")
        if website == "e621" and uploader_type == UploadLinkUploaderType.UNKNOWN:
            uploader_type = UploadLinkUploaderType.E621
        if info_str is None:
            info_str = extract_upload_link_info(link_str, website)
        if info_str is not None and uploader_type == UploadLinkUploaderType.UNKNOWN:
            if info_str in ["spangle", "zephyr"]:
                uploader_type = UploadLinkUploaderType.OURS
            else:
                for tag in post.tags:
                    for tag_name in tag.names:
                        if tag_name.lower() != info_str.lower():
                            continue
                        if tag.category == "artists":
                            uploader_type = UploadLinkUploaderType.ARTIST
                            info_str = tag.primary_name
                        if tag.category == "characters":
                            uploader_type = UploadLinkUploaderType.UNKNOWN
                            info_str = tag.primary_name
        return cls(
            link=link_str,
            uploader_type=uploader_type,
            uploader_type_info=info_str,
            website=website,
        )

    def to_dict(self) -> dict:
        data = {
            "uploader_type": self.uploader_type.value,
            "link": self.link,
            "website": self.website,
        }
        if self.uploader_type_info:
            data["uploader_type_info"] = self.uploader_type_info
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "UploadLink":
        return cls(
            data["link"],
            UploadLinkUploaderType(data["uploader_type"]),
            data.get("uploader_type_info"),
            data["website"],
        )


class UploadDataPostDocument(YamlPostDocument):
    """
    data_type: upload_data
    proposed_data:
        title: This pic is wooo
        description: AAaaaa
        tags: a, b, c
    uploads:
      - uploader_type: artist
        website: furaffinity
        link: http://fa
      - uploader_type: e621
      - uploader_type: other_character
      - uploader_type: ours
        uploader_type_info: spangle
      - uploader_type: ours
        uploader_type_info: zephyr
    """

    def set_data_type(self) -> None:
        if "data_type" not in self.yaml_doc:
            self.yaml_doc["data_type"] = "upload_data"

    @property
    def proposed_title(self) -> Optional[str]:
        return self.yaml_doc.get("proposed_data", {}).get("title")

    @proposed_title.setter
    def proposed_title(self, new_title: str) -> None:
        self.set_data_type()
        if "proposed_data" not in self.yaml_doc:
            self.yaml_doc["proposed_data"] = {}
        self.yaml_doc["proposed_data"]["title"] = new_title

    @property
    def proposed_description(self) -> Optional[str]:
        return self.yaml_doc.get("proposed_data", {}).get("description")

    @proposed_description.setter
    def proposed_description(self, new_description: str) -> None:
        self.set_data_type()
        if "proposed_data" not in self.yaml_doc:
            self.yaml_doc["proposed_data"] = {}
        self.yaml_doc["proposed_data"]["description"] = new_description

    @property
    def proposed_tags(self) -> Optional[list[str]]:
        return self.yaml_doc.get("proposed_data", {}).get("tags")

    @proposed_tags.setter
    def proposed_tags(self, new_tags: list[str]) -> None:
        self.set_data_type()
        if "proposed_data" not in self.yaml_doc:
            self.yaml_doc["proposed_data"] = {}
        self.yaml_doc["proposed_data"]["tags"] = new_tags

    @property
    def upload_links(self) -> list[UploadLink]:
        link_data = self.yaml_doc.get("upload_links", [])
        return [UploadLink.from_dict(d) for d in link_data]

    def save_upload_links(self, upload_links: list[UploadLink]) -> None:
        self.set_data_type()
        self.yaml_doc["upload_links"] = [link.to_dict() for link in upload_links]

    def add_upload_link(self, link: UploadLink) -> None:
        upload_links = self.upload_links
        upload_links.append(link)
        self.save_upload_links(upload_links)

    def set_upload_link(self, link_idx: int, upload_link: UploadLink) -> None:
        upload_links = self.upload_links
        upload_links[link_idx] = upload_link
        self.save_upload_links(upload_links)

    def remove_upload_link(self, link_idx: int) -> None:
        upload_links = self.upload_links
        del upload_links[link_idx]
        self.save_upload_links(upload_links)


T = TypeVar("T", bound=PostDocument)


# noinspection DuplicatedCode
class PostDescription:
    def __init__(self, raw_text: str) -> None:
        self.documents = self._parse_documents(raw_text)

    @staticmethod
    def _parse_documents(raw_text: Optional[str]) -> list[PostDocument]:
        if raw_text is None:
            return []
        documents: list[PostDocument] = []
        current_doc_lines: list[str] = []
        # Go through lines constructing documents
        for line in raw_text.split("\n"):
            if line == "---" and current_doc_lines:
                current_doc_text = "\n".join(current_doc_lines)
                current_doc = PostDocument.parse_text(current_doc_text)
                documents.append(current_doc)
                current_doc_lines = []
            else:
                current_doc_lines.append(line)
        # Add the final document
        final_doc_text = "\n".join(current_doc_lines)
        if final_doc_text:
            final_doc = PostDocument.parse_text(final_doc_text)
            documents.append(final_doc)
        return documents

    def to_string(self) -> Optional[str]:
        raw_doc_strings = []
        yaml_doc_strings = []
        for document in self.documents:
            if isinstance(document, YamlPostDocument):
                yaml_doc_strings.append("---\n```\n" + document.to_string() + "\n```\n---")
            else:
                raw_doc_strings.append(document.to_string())
        # Ensure raw text documents go first, then YAML
        doc_strings = raw_doc_strings + yaml_doc_strings
        output = "\n".join(doc_strings)
        # Remove any empty documents
        while "```\n```\n" in output:
            output = output.replace("```\n```\n", "")
        while "---\n---" in output:
            output = output.replace("---\n---", "---")
        # Don't return just a document separator
        if output == "---":
            output = ""
        # Don't return an empty string
        if output == "":
            return None
        return output

    def get_doc_matching_type(self, doc_type: Type[T]) -> Optional[T]:
        for document in self.documents:
            if isinstance(document, doc_type):
                return document
        return None

    def has_doc_matching_type(self, doc_type: Type[PostDocument]) -> bool:
        return self.get_doc_matching_type(doc_type) is not None

    def set_document_for_type(self, new_document: PostDocument) -> None:
        """
        Adds the specified document, or replaces an existing document if one of the same type already exists.
        """
        new_doc_type = type(new_document)
        for idx, document in enumerate(self.documents[:]):
            if isinstance(document, new_doc_type):
                self.documents[idx] = new_document
                return
        self.documents.append(new_document)

    def get_or_create_doc_matching_type(self, doc_type: Type[T]) -> T:
        existing = self.get_doc_matching_type(doc_type)
        if existing:
            return existing
        new_doc = doc_type()
        self.documents.append(new_doc)
        return new_doc


def get_post_description(post: pyszuru.Post) -> PostDescription:
    description_raw = post._generic_getter("description")
    description = PostDescription(description_raw)
    return description


# noinspection PyProtectedMember
def set_post_description(post: pyszuru.Post, description: PostDescription) -> None:
    raw_description = description.to_string()
    update_body = {"description": raw_description}
    if "version" in post._json and post._json["version"]:
        update_body["version"] = post._json["version"]
    data = post._api._call("PUT", post._get_instance_urlparts(), body=update_body)
    post._update_json(data, force=True)
