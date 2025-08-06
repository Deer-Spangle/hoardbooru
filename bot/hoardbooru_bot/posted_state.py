import dataclasses
import datetime
from functools import lru_cache

import pyszuru


@dataclasses.dataclass
class PostUploadState:
    post: pyszuru.Post
    user_infix: str

    def __hash__(self) -> int:
        return hash((PostUploadState, self.post.id_, self.user_infix))

    @lru_cache
    def tag_names(self) -> list[str]:
        return [n for t in self.post.tags for n in t.names]

    @property
    def e6_uploaded(self) -> bool:
        return "uploaded_to:e621" in self.tag_names()

    @property
    def e6_not_uploading(self) -> bool:
        return "uploaded_to:e621_not_posting" in self.tag_names()

    @property
    def e6_to_upload(self) -> bool:
        return not self.e6_uploaded and not self.e6_not_uploading

    @property
    def e6_state(self) -> str:
        if self.e6_uploaded:
            return "Uploaded"
        if self.e6_not_uploading:
            return "Not uploading"
        return "To upload"

    @property
    def fa_uploaded(self) -> bool:
        return f"uploaded_to:{self.user_infix}_fa" in self.tag_names()

    @property
    def fa_not_uploading(self) -> bool:
        return f"uploaded_to:{self.user_infix}_not_posting" in self.tag_names()

    @property
    def fa_to_upload(self) -> bool:
        return not self.fa_uploaded and not self.fa_not_uploading

    @property
    def fa_state(self) -> str:
        if self.fa_uploaded:
            return "Uploaded"
        if self.fa_not_uploading:
            return "Not uploading"
        return "To upload"

    @property
    def to_upload(self) -> bool:
        return self.e6_to_upload or self.fa_to_upload


@dataclasses.dataclass
class PostsByUploadedState:
    all_posts: list[pyszuru.Post]
    posts_to_upload: list[pyszuru.Post]
    e6_uploaded: list[pyszuru.Post]
    e6_to_upload: list[pyszuru.Post]
    e6_not_uploading: list[pyszuru.Post]
    fa_uploaded: list[pyszuru.Post]
    fa_to_upload: list[pyszuru.Post]
    fa_not_uploading: list[pyszuru.Post]

    @classmethod
    def list_by_state(cls, api: pyszuru.API, query: str, user_infix: str) -> "PostsByUploadedState":
        all_posts: list[pyszuru.Post] = []
        posts_to_upload: list[pyszuru.Post] = []
        e6_uploaded: list[pyszuru.Post] = []
        e6_to_upload: list[pyszuru.Post] = []
        e6_not_uploading: list[pyszuru.Post] = []
        fa_uploaded: list[pyszuru.Post] = []
        fa_to_upload: list[pyszuru.Post] = []
        fa_not_uploading: list[pyszuru.Post] = []
        for post in api.search_post(query, page_size=100):
            post_state = PostUploadState(post, user_infix)
            all_posts.append(post)
            if post_state.e6_uploaded:
                e6_uploaded.append(post)
            if post_state.e6_not_uploading:
                e6_not_uploading.append(post)
            if post_state.e6_to_upload:
                e6_to_upload.append(post)
            if post_state.fa_uploaded:
                fa_uploaded.append(post)
            if post_state.fa_not_uploading:
                fa_not_uploading.append(post)
            if post_state.fa_to_upload:
                fa_to_upload.append(post)
            if post_state.to_upload:
                posts_to_upload.append(post)
        return cls(
            all_posts,
            posts_to_upload,
            e6_uploaded,
            e6_to_upload,
            e6_not_uploading,
            fa_uploaded,
            fa_to_upload,
            fa_not_uploading,
        )


@dataclasses.dataclass(eq=True, frozen=True)
class UploadStateCacheKey:
    query: str
    user_infix: str


@dataclasses.dataclass
class UploadStateCacheEntry:
    creation_datetime: datetime.datetime
    posts: PostsByUploadedState

    def age(self) -> datetime.timedelta:
        return datetime.datetime.now(datetime.timezone.utc) - self.creation_datetime


class UploadStateCache:
    MAX_AGE = datetime.timedelta(hours=1)

    def __init__(self):
        self.cache: dict[UploadStateCacheKey, UploadStateCacheEntry] = {}

    def list_by_state(
            self,
            api: pyszuru.API,
            query: str,
            user_infix: str,
            refresh: bool = False
    ) -> PostsByUploadedState:
        key = UploadStateCacheKey(query, user_infix)
        if key in self.cache and not refresh:
            entry = self.cache[key]
            if entry.age() < self.MAX_AGE:
                return entry.posts
        entry = UploadStateCacheEntry(
            datetime.datetime.now(datetime.timezone.utc),
            PostsByUploadedState.list_by_state(api, query, user_infix),
        )
        self.cache[key] = entry
        return entry.posts
