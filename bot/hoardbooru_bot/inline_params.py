
class InlineParams:
    FILE_TERMS = ["file", "doc", "uncompressed", "raw"]
    def __init__(self) -> None:
        self.spoiler = False
        self.link = False
        self.file = False

    def parse_inline_query(self, query: str) -> str:
        query_terms = query.split()
        for query_term in query_terms[:]:
            if query_term in ["spoiler", "spoil", "spoile"]:
                self.spoiler = True
                query_terms.remove(query_term)
            if query_term == "link":
                self.link = True
                query_terms.remove(query_term)
            if query_term in self.FILE_TERMS:
                self.file = True
                query_terms.remove(query_term)
        return " ".join(query_terms)
