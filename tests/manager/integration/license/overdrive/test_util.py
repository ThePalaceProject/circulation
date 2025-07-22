from palace.manager.integration.license.overdrive.util import _make_link_safe


def test__make_link_safe() -> None:
    # Unsafe characters are escaped.
    assert "http://foo.com?q=%2B%3A%7B%7D" == _make_link_safe("http://foo.com?q=+:{}")

    # Links to version 1 of the availability API are converted
    # to links to version 2.
    v1 = "https://qa.api.overdrive.com/v1/collections/abcde/products/12345/availability"
    v2 = "https://qa.api.overdrive.com/v2/collections/abcde/products/12345/availability"
    assert v2 == _make_link_safe(v1)

    # We also handle the case of a trailing slash, just in case Overdrive
    # starts serving links with trailing slashes.
    v1 = v1 + "/"
    v2 = v2 + "/"
    assert v2 == _make_link_safe(v1)

    # Links to other endpoints are not converted
    leave_alone = "https://qa.api.overdrive.com/v1/collections/abcde/products/12345"
    assert leave_alone == _make_link_safe(leave_alone)
