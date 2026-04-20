from app.collectors.about_scraper import _extract_by_label, _extract_following_line
from app.collectors.grid_enumerator import extract_media_links_from_html
from app.collectors.post_detail_scraper import (
    _extract_like_comment_from_json_payload,
    _extract_repost_count_from_json_payload,
    _extract_views_from_json_payload,
    _parse_counts_from_text,
)
from app.anti_block.challenge_handler import detect_challenge
from app.collectors.profile_scraper import (
    _parse_counts_from_og_description,
    detect_private_profile_from_text,
)


PROFILE_PRIVATE_HTML = """
<html><body><h2>This account is private</h2><p>Follow to see their photos and videos.</p></body></html>
"""

ABOUT_SNAPSHOT_TEXT = """
Date joined
January 2020
Account based in
India
This account has active ads
Verified
March 2021
"""

GRID_HTML = """
<article>
    <a href="/indriyajewels/p/DEF456/"><img src="0.jpg"/></a>
  <a href="/p/ABC123/"><img src="1.jpg"/></a>
  <a href="/reel/XYZ987/"><img src="2.jpg"/></a>
  <a href="/p/ABC123/"><img src="dup.jpg"/></a>
</article>
"""

POST_DETAIL_TEXT = "1,234 likes, 78 comments, 45.6k views"
POST_DETAIL_TEXT_PLAYS = "1,234 likes, 78 comments, 45.6k plays"
OG_DESCRIPTION_TEXT = (
    "1M Followers, 2 Following, 710 Posts - "
    "See Instagram photos and videos from Indriya Jewels (@indriyajewels)"
)


def test_profile_private_parser_contract():
    assert detect_private_profile_from_text(PROFILE_PRIVATE_HTML) is True


def test_about_parser_contract():
    assert _extract_by_label(ABOUT_SNAPSHOT_TEXT, "Date joined") == "January 2020"
    assert _extract_by_label(ABOUT_SNAPSHOT_TEXT, "Verified") == "March 2021"
    assert (
        _extract_following_line(ABOUT_SNAPSHOT_TEXT, ["Account based in", "Based in"])
        == "India"
    )


def test_grid_parser_contract():
    out = extract_media_links_from_html(GRID_HTML)
    assert len(out) == 3
    assert out[0][0] == "DEF456"
    assert out[0][2] == "image_post"
    assert out[1][0] == "ABC123"
    assert out[1][2] == "image_post"
    assert out[2][0] == "XYZ987"
    assert out[2][2] == "reel"


def test_post_detail_count_parser_contract():
    likes, comments, views = _parse_counts_from_text(POST_DETAIL_TEXT)
    assert likes == 1234
    assert comments == 78
    assert views == 45600


def test_post_detail_count_parser_supports_plays_label():
    likes, comments, views = _parse_counts_from_text(POST_DETAIL_TEXT_PLAYS)
    assert likes == 1234
    assert comments == 78
    assert views == 45600


class _MockPageJsonViews:
    def content(self) -> str:
        return (
            '{"items":[{"code":"ABC123","like_count":10,'
            '"comment_count":2,"play_count":12345}]}'
        )


def test_extract_views_from_json_payload_contract():
    views = _extract_views_from_json_payload(_MockPageJsonViews(), "ABC123")
    assert views == 12345


class _MockPageJsonLikesComments:
    def content(self) -> str:
        return (
            '{"feed":[{"code":"XYZ9","edge_media_preview_like":{"count":656},'
            '"edge_media_to_parent_comment":{"count":82},"like_count":999,'
            '"comment_count":888}]}'
        )


def test_extract_like_comment_from_json_payload_contract():
    likes, comments = _extract_like_comment_from_json_payload(
        _MockPageJsonLikesComments(), "XYZ9"
    )
    assert likes == 656
    assert comments == 82


class _MockPageJsonRepost:
    def content(self) -> str:
        return '{"items":[{"code":"RS123","media_repost_count":431}]}'


def test_extract_repost_count_from_json_payload_contract():
    reposts = _extract_repost_count_from_json_payload(_MockPageJsonRepost(), "RS123")
    assert reposts == 431


def test_profile_og_count_parser_contract():
    out = _parse_counts_from_og_description(OG_DESCRIPTION_TEXT)
    assert out["followers_count"] == 1_000_000
    assert out["following_count"] == 2
    assert out["total_posts_count"] == 710


class _MockPage429:
    url = "https://www.instagram.com/reel/ABC123/"

    def inner_text(self, selector: str, timeout: int = 0) -> str:
        _ = selector
        _ = timeout
        return "This page isn't working HTTP ERROR 429"

    def title(self) -> str:
        return "www.instagram.com"

    def locator(self, selector: str):
        _ = selector

        class _Noop:
            def count(self) -> int:
                return 0

        return _Noop()

    def get_by_role(self, *_args, **_kwargs):
        class _Noop:
            def count(self) -> int:
                return 0

        return _Noop()


def test_challenge_detector_catches_http_429_page():
    hit, pattern = detect_challenge(_MockPage429())
    assert hit is True
    assert pattern == "http_error_429"
