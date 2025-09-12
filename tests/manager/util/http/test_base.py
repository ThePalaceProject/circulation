from palace.manager.util.http.base import get_series


def test_get_series() -> None:
    assert get_series(201) == "2xx"
    assert get_series(399) == "3xx"
    assert get_series(500) == "5xx"
