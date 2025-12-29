from types import SimpleNamespace
from os.path import realpath, splitext
from datetime import datetime, timezone
import re
from json import (
    loads as from_jsons,
    load as from_json,
    dumps as to_jsons,
    dump as to_json,
)
from xml.parsers.expat import model
from requests import get as webget, Response as HTTPResponse, ConnectionError
import mimetypes

from reddit_spider_db import *

# =================================================================================================

def get_timestamp_utc():
    datetime.now(timezone.utc).isoformat()

# =================================================================================================

def read_json_file(file) -> object:
    with open(str(file), mode="r", encoding="utf-8") as fp:
        return from_json(fp)


def write_json_file(file, data) -> Path:
    file = Path(file)
    file.parent.mkdir(parents=True, exist_ok=True)
    with open(str(file), mode="w", encoding="utf-8") as fp:
        to_json(data, fp, indent=2)
    return file


# =================================================================================================

MEDIA_TYPE_BIN = "application/octet-stream"
MEDIA_TYPE_TEXT = "text/plain"
MEDIA_TYPE_JSON = "application/json"
MEDIA_TYPE_PNG = "image/png"
MEDIA_TYPE_JPEG = "image/jpeg"
MEDIA_TYPE_GIF = "image/gif"
MEDIA_TYPE_WEBP = "image/webp"
MEDIA_TYPE_MP4 = "video/mp4"
MEDIA_TYPE_MPEG = "video/mpeg"
MEDIA_TYPE_WEBM = "video/webm"

HTTP_CODE_SUCCESS = 200
HTTP_CODE_NOCONTENT = 204
HTTP_CODE_NOTMODIFIED = 304
HTTP_CODE_BADREQUEST = 400
HTTP_CODE_UNAUTHORIZED = 401
HTTP_CODE_FORBIDDEN = 403
HTTP_CODE_NOTFOUND = 404
HTTP_CODE_UNAVAILABLE = 410
HTTP_CODE_RATELIMIT = 429
HTTP_CODE_ERROR = 500

WEB_USER_AGENT = " ".join(
    [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "AppleWebKit/537.36 (KHTML, like Gecko)",
        "Chrome/66.0.3359.181",
        "Safari/537.36",
    ]
)


def get_path_ext(url):
    parts = splitext(url)
    return parts[1] if len(parts) == 2 else None


def get_path_mime(url):
    return mimetypes.guess_type(url)[0]


def fetch_data(url, assert_status=True) -> HTTPResponse:
    hdrs = {"Accept": MEDIA_TYPE_JSON, "User-Agent": WEB_USER_AGENT}
    print(f"HTTP GET {url}")
    res = None
    try:
        res = webget(url, headers=hdrs, verify=True)
    except ConnectionError as err:
        print(f"Failure to connect to {url}")
        if assert_status:
            raise err
    if res and assert_status:
        if res.status_code == HTTP_CODE_RATELIMIT and "Retry-After" in res.headers:
            millis = res.headers["Retry-After"]
            raise Exception(
                f"Failure to fetch data. Retry after {millis} ms."
                + (
                    f" Code: {res.status_code}, Reason: {res.reason}, Url: {url}"
                    if res
                    else f"Url: {url}"
                )
            )
        if res.status_code != HTTP_CODE_SUCCESS:
            raise Exception(
                f"Failure to fetch data."
                + (
                    f" Code: {res.status_code}, Reason: {res.reason}, Url: {url}"
                    if res
                    else f"Url: {url}"
                )
            )
    return res


def fetch_text(url, assert_status=True) -> object:
    res = fetch_data(url, assert_status)
    try:
        return res.text if res else None
    except:
        raise Exception(
            f"Failure to fetch json."
            + (
                f" Code: {res.status_code}, Reason: {res.reason}, Url: {url}"
                if res
                else f"Url: {url}"
            )
        )


def fetch_json(url, assert_status=True) -> object:
    res = fetch_data(url, assert_status)
    try:
        return from_jsons(res.text) if res else None
    except:
        raise Exception(
            f"Failure to fetch json."
            + (
                f" Code: {res.status_code}, Reason: {res.reason}, Url: {url}"
                if res
                else f"Url: {url}"
            )
        )


def fetch_file(url, file, assert_status=True) -> Path:
    try:
        res = fetch_data(url, assert_status)
        if res:
            path = Path(file)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(str(path), "wb") as fp:
                fp.write(res.content)
            return path
        else:
            return None
    except:
        raise Exception(
            f"Failure to fetch file."
            + (
                f" Code: {res.status_code}, Reason: {res.reason}, Url: {url}"
                if res
                else f"Url: {url}"
            )
        )
