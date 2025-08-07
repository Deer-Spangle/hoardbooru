import urllib.parse
from typing import Optional, Union

from telethon import events
from telethon.tl.types import MessageEntityTextUrl, Message

HIDDEN_DOMAIN = "example.com"


def hidden_data(data: dict[str, str], keys: list[str] = None) -> str:
    trim_data = data
    if keys is not None:
        trim_data = {}
        for key in keys:
            trim_data[key] = data[key]
    params = urllib.parse.urlencode(trim_data)
    url = f"https://{HIDDEN_DOMAIN}?{params}"
    link = f"<a href=\"{url}\">​</a>"
    return link


def parse_hidden_data(evt: Union[events.NewMessage.Event, Message]) -> Optional[dict[str, str]]:
    for url_entity, inner_text in evt.get_entities_text(MessageEntityTextUrl):
        url = url_entity.url
        url_parse = urllib.parse.urlparse(url)
        if url_parse.netloc != HIDDEN_DOMAIN:
            continue
        if not url_parse.query:
            continue
        qs = urllib.parse.parse_qs(url_parse.query)
        try:
            return {key: vals[0] for key, vals in qs.items()}
        except ValueError:
            continue