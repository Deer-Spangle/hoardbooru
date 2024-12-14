import uuid
from contextlib import asynccontextmanager
from typing import Generator

import aiofiles.os
import aiohttp


SANDBOX_DIR = "sandbox"

def file_ext(url: str) -> str:
    return url.split(".")[-1].lower()


async def download_file_bytes(file_url: str) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.get(file_url) as resp:
            if resp.status == 200:
                return await resp.read()


@asynccontextmanager
async def temp_sandbox_file(ext: str) -> Generator[str, None, None]:
    await aiofiles.os.makedirs(SANDBOX_DIR, exist_ok=True)
    temp_path = f"{SANDBOX_DIR}/{uuid.uuid4()}.{ext}"
    try:
        yield temp_path
    finally:
        try:
            await aiofiles.os.remove(temp_path)
        except FileNotFoundError:
            pass


@asynccontextmanager
async def downloaded_file(url: str) -> Generator[str, None, None]:
    async with temp_sandbox_file(file_ext(url)) as dl_path:
        session = aiohttp.ClientSession()
        dl_filesize = 0
        async with session.get(url) as resp:
            resp.raise_for_status()
            with open(dl_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(8192):
                    f.write(chunk)
                    dl_filesize += len(chunk)
        yield dl_path
