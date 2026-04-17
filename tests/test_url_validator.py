import pytest

from app.core.url_validator import InvalidInstagramUrl, normalize_instagram_profile_url


def test_normalize_profile_url():
    out = normalize_instagram_profile_url("instagram.com/indriyajewels")
    assert out.normalized_url == "https://www.instagram.com/indriyajewels/"
    assert out.username == "indriyajewels"


@pytest.mark.parametrize(
    "bad_url",
    [
        "https://www.instagram.com/p/ABC123/",
        "https://www.instagram.com/reel/ABC123/",
        "https://www.instagram.com/stories/test/1/",
        "https://example.com/test/",
    ],
)
def test_invalid_instagram_profile_url(bad_url: str):
    with pytest.raises(InvalidInstagramUrl):
        normalize_instagram_profile_url(bad_url)

