import dataclasses

import pyszuru


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
            all_posts.append(post)
            marked_to_upload = False
            tag_names = [n for t in post.tags for n in t.names]
            if "uploaded_to:e621" in tag_names:
                e6_uploaded.append(post)
            elif "uploaded_to:e621_not_posting" in tag_names:
                e6_not_uploading.append(post)
            else:
                e6_to_upload.append(post)
                posts_to_upload.append(post)
                marked_to_upload = True
            if f"uploaded_to:{user_infix}_fa" in tag_names:
                fa_uploaded.append(post)
            elif f"uploaded_to:{user_infix}_not_posting" in tag_names:
                fa_not_uploading.append(post)
            else:
                fa_to_upload.append(post)
                if not marked_to_upload:
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