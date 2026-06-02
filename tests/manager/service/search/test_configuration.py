import logging

import pytest

from palace.manager.service.search.configuration import SearchConfiguration


class TestSearchConfiguration:
    def test_defaults(self):
        config = SearchConfiguration(url="http://localhost:9200")
        assert config.write_timeout == 20
        assert config.read_timeout == 4

    def test_write_timeout_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PALACE_SEARCH_WRITE_TIMEOUT", "11")
        config = SearchConfiguration(url="http://localhost:9200")
        assert config.write_timeout == 11

    def test_write_timeout_deprecated_env_still_honored(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ):
        """The deprecated PALACE_SEARCH_TIMEOUT is still accepted, with a warning."""
        monkeypatch.setenv("PALACE_SEARCH_TIMEOUT", "22")
        with caplog.at_level(logging.WARNING):
            config = SearchConfiguration(url="http://localhost:9200")
        assert config.write_timeout == 22
        assert "PALACE_SEARCH_TIMEOUT is deprecated" in caplog.text

    def test_write_timeout_new_env_takes_precedence(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ):
        """When both names are set the new one wins and no warning is emitted."""
        monkeypatch.setenv("PALACE_SEARCH_TIMEOUT", "22")
        monkeypatch.setenv("PALACE_SEARCH_WRITE_TIMEOUT", "33")
        with caplog.at_level(logging.WARNING):
            config = SearchConfiguration(url="http://localhost:9200")
        assert config.write_timeout == 33
        assert "deprecated" not in caplog.text
