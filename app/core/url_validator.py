from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse


PROFILE_RE = re.compile(r"^/(?P<username>[A-Za-z0-9._]+)/?$")
REJECT_PATH_PREFIXES = {
    "/p/",
    "/reel/",
    "/tv/",
    "/stories/",
    "/explore/",
    "/accounts/",
    "/direct/",
}
ALLOWED_HOSTS = {
    "instagram.com",
    "www.instagram.com",
}


@dataclass(frozen=True)
class NormalizedProfileUrl:
    input_url: str
    normalized_url: str
    username: str


class InvalidInstagramUrl(ValueError):
    pass


def _coerce_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        raise InvalidInstagramUrl("Input URL is empty")
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    return value


def normalize_instagram_profile_url(raw_url: str) -> NormalizedProfileUrl:
    value = _coerce_url(raw_url)
    parsed = urlparse(value)

    if parsed.netloc.lower() not in ALLOWED_HOSTS:
        raise InvalidInstagramUrl("URL is not an Instagram domain")

    path = parsed.path or "/"
    for prefix in REJECT_PATH_PREFIXES:
        if path.lower().startswith(prefix):
            raise InvalidInstagramUrl("URL is not a profile URL")

    match = PROFILE_RE.match(path)
    if not match:
        raise InvalidInstagramUrl("Invalid Instagram profile URL format")

    username = match.group("username").strip(".")
    if not username:
        raise InvalidInstagramUrl("Invalid username segment")

    normalized = f"https://www.instagram.com/{username}/"
    return NormalizedProfileUrl(input_url=raw_url.strip(), normalized_url=normalized, username=username)

