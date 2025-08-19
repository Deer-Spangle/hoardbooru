from typing import Optional


class InlineParams:
    FILE_TERMS = ["file", "doc", "uncompressed", "raw"]
    def __init__(self) -> None:
        self.spoiler = False
        self.link = False
        self.file = False
        self.caption: Optional[str] = None

    def parse_inline_query(self, query: str) -> str:
        if "caption" in query:
            query, caption = query.split("caption", maxsplit=1)
            query = query.strip()
            self.caption = caption.lstrip(": ").rstrip()
        query_terms = []
        for query_term in query.split():
            if query_term in ["spoiler", "spoil", "spoile"]:
                self.spoiler = True
                continue
            if query_term == "link":
                self.link = True
                continue
            if query_term in self.FILE_TERMS:
                self.file = True
                continue
            query_terms.append(query_term)
        return " ".join(query_terms)
