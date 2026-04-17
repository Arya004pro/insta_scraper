from app.collectors.about_scraper import _extract_by_label
from app.collectors.grid_enumerator import extract_media_links_from_html
from app.collectors.post_detail_scraper import _parse_counts_from_text
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
OG_DESCRIPTION_TEXT = (
    "1M Followers, 2 Following, 710 Posts - "
    "See Instagram photos and videos from Indriya Jewels (@indriyajewels)"
)


def test_profile_private_parser_contract():
    assert detect_private_profile_from_text(PROFILE_PRIVATE_HTML) is True


def test_about_parser_contract():
    assert _extract_by_label(ABOUT_SNAPSHOT_TEXT, "Date joined") == "January 2020"
    assert _extract_by_label(ABOUT_SNAPSHOT_TEXT, "Verified") == "March 2021"


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


def test_profile_og_count_parser_contract():
    out = _parse_counts_from_og_description(OG_DESCRIPTION_TEXT)
    assert out["followers_count"] == 1_000_000
    assert out["following_count"] == 2
    assert out["total_posts_count"] == 710
