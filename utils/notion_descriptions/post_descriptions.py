from abc import ABC, abstractmethod

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
        except yaml.YAMLError as exc:
            return RawTextPostDocument(raw_text)
        data_type = yaml_doc["data_type"]
        return {
            "notion": NotionPostDocument(yaml_doc),
            "telegram": TelegramPostDocument(yaml_doc),
            "upload_links": UploadLinksPostDocument(yaml_doc),
        }[data_type]


class RawTextPostDocument(PostDocument):

    def __init__(self, raw_text: str) -> None:
        self.raw_text = raw_text

    def to_string(self) -> str:
        return self.raw_text


class YamlPostDocument(PostDocument, ABC):

    def __init__(self, yaml_doc: dict) -> None:
        self.yaml_doc = yaml_doc

    def to_string(self) -> str:
        return yaml.dump(self.yaml_doc)


class NotionPostDocument(YamlPostDocument):
    pass


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


class UploadLinksPostDocument(YamlPostDocument):
    """
    data_type: upload links
    uploads:
      - uploader_type: artist
        link: http://fa
      - uploader_type: e621
      - uploader_type: other_character
      - uploader_type: spangle
      - uploader_type: zephyr
    """
    pass


class PostDescription:
    def __init__(self, raw_text: str) -> None:
        self.documents = self._parse_documents(raw_text)

    @staticmethod
    def _parse_documents(raw_text: str) -> list[PostDocument]:
        documents = []
        current_doc_lines = []
        for line in raw_text.split("\n"):
            if line == "---" and current_doc_lines:
                documents.append(PostDocument.parse_text("\n".join(current_doc_lines)))
                current_doc_lines = []
            else:
                current_doc_lines.append(line)
        documents.append(PostDocument.parse_text("\n".join(current_doc_lines)))
        return documents

    def to_string(self) -> str:
        doc_strings = []
        for document in self.documents:
            if isinstance(document, YamlPostDocument):
                doc_strings.append("---\n" + document.to_string() + "\n---")
            else:
                doc_strings.append(document.to_string())
        output = "\n".join(doc_strings)
        while "---\n---" in output:
            output = output.replace("---\n---", "---")
        return output

def get_post_description(post: pyszuru.Post) -> PostDescription:
    pass  # TODO


def set_post_description(post: pyszuru.Post, description: PostDescription) -> None:
    pass  # TODO
