import dataclasses
import uuid
from contextlib import asynccontextmanager
from typing import Generator, Optional, TYPE_CHECKING, Union

import PIL
import aiofiles.os
import aiohttp
from PIL import Image
from telethon.tl.types import InputPhoto, InputDocument, InputMediaPhoto, InputMediaDocument
import telethon.utils

if TYPE_CHECKING:
    from bot.hoardbooru_bot.database import CacheEntry

SANDBOX_DIR = "sandbox"
TG_IMG_SEMIPERIMETER_LIMIT = 10_000
IMG_TRANSPARENCY_COlOUR = (255, 255, 255)  # Colour to mask out transparency with when sending images

def file_ext(url: str) -> str:
    return url.split(".")[-1].lower()


async def download_file_bytes(file_url: str) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.get(file_url) as resp:
            if resp.status == 200:
                return await resp.read()


@asynccontextmanager
async def temp_sandbox_file(ext: Optional[str]) -> Generator[str, None, None]:
    await aiofiles.os.makedirs(SANDBOX_DIR, exist_ok=True)
    suffix = f".{ext}" if ext is not None else ""
    temp_path = f"{SANDBOX_DIR}/{uuid.uuid4()}{suffix}"
    try:
        yield temp_path
    finally:
        try:
            await aiofiles.os.remove(temp_path)
        except FileNotFoundError:
            pass


@dataclasses.dataclass
class DownloadedFile:
    dl_path: str
    file_size: int


@asynccontextmanager
async def downloaded_file(url: str) -> Generator[DownloadedFile, None, None]:
    async with temp_sandbox_file(file_ext(url)) as dl_path:
        session = aiohttp.ClientSession()
        dl_filesize = 0
        async with session.get(url) as resp:
            resp.raise_for_status()
            with open(dl_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(8192):
                    f.write(chunk)
                    dl_filesize += len(chunk)
        yield DownloadedFile(dl_path, dl_filesize)


def _img_has_transparency(img: Image) -> bool:
    if img.info.get("transparency", None) is not None:
        return True
    if img.mode == "P":
        transparent = img.info.get("transparency", -1)
        for _, index in img.getcolors():
            if index == transparent:
                return True
    elif img.mode == "RGBA":
        extrema = img.getextrema()
        if extrema[3][0] < 255:
            return True
    return False


@asynccontextmanager
async def convert_image(img_path: str) -> Generator[str, None, None]:
        with Image.open(img_path) as img:

            # Check image resolution and scale
            width, height = img.size
            semiperimeter = width + height
            if semiperimeter > TG_IMG_SEMIPERIMETER_LIMIT:
                scale_factor = TG_IMG_SEMIPERIMETER_LIMIT / semiperimeter
                new_width = int(width * scale_factor)
                new_height = int(height * scale_factor)
                img = img.resize((new_width, new_height), PIL.Image.LANCZOS)

            # Mask out transparency
            if img.mode == 'P':
                img = img.convert('RGBA')
            alpha_index = img.mode.find('A')
            if alpha_index != -1:
                result = Image.new('RGB', img.size, IMG_TRANSPARENCY_COlOUR)
                result.paste(img, mask=img.split()[alpha_index])
                img = result

            # Convert colour pallete
            if img.mode != 'RGB':
                img = img.convert('RGB')

            # Save image as jpg
            async with temp_sandbox_file(ext="jpg") as output_path:
                img.save(output_path, 'JPEG', progressive=True, quality=95)
                yield output_path


def cache_enty_to_inline_media(cache_entry: "CacheEntry") -> Union[InputMediaPhoto, InputMediaDocument]:
    input_doc_cls = InputPhoto if cache_entry.is_photo else InputDocument
    input_doc = input_doc_cls(cache_entry.media_id, cache_entry.access_hash, b"")
    return telethon.utils.get_input_media(input_doc)


def bold_if_true(text: str, is_true: bool) -> str:
    if is_true:
        return f"<b>{text}</b>"
    return text


def tick_if_true(is_true: bool) -> str:
    return "✅" if is_true else ""
