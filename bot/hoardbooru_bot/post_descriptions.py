# noinspection DuplicatedCode
from abc import ABC, abstractmethod
import datetime
from typing import Type, Optional, TypeVar

import pyszuru
import yaml


class PostDocument(ABC):

    @abstractmethod
    def to_string(self) -> str:
        raise NotImplementedError()

    @classmethod
    def parse_text(cls, raw_text: str) -> "PostDocument":
        try:
            yaml_doc = yaml.safe_load(raw_text)
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


class UploadDataPostDocument(YamlPostDocument):
    """
    data_type: upload_data
    proposed_data:
        title: This pic is wooo
        description: AAaaaa
        tags: a, b, c
    uploads:
      - uploader_type: artist
        link: http://fa
      - uploader_type: e621
      - uploader_type: other_character
      - uploader_type: spangle
      - uploader_type: zephyr
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
                yaml_doc_strings.append("---\n" + document.to_string() + "\n---")
            else:
                raw_doc_strings.append(document.to_string())
        # Ensure raw text documents go first, then YAML
        doc_strings = raw_doc_strings + yaml_doc_strings
        output = "\n".join(doc_strings)
        # Remove any empty documents
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
