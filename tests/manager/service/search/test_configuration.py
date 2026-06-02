import pytest

from palace.manager.service.search.configuration import SearchConfiguration


class TestSearchConfiguration:
    def test_defaults(self):
        config = SearchConfiguration(url="http://localhost:9200")
        assert config.write_timeout == 20
        assert config.read_timeout == 10
        assert config.read_max_retries == 0
        assert config.read_retry_on_timeout is False

    def test_write_timeout_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PALACE_SEARCH_WRITE_TIMEOUT", "11")
        config = SearchConfiguration(url="http://localhost:9200")
        assert config.write_timeout == 11
