from __future__ import annotations

from typing import Any

import pytest
from frozendict import frozendict

from palace.manager.api.admin.problem_details import INVALID_CONFIGURATION_OPTION
from palace.manager.integration.license.bibliotheca.constants import (
    BIBLIOTHECA_DEFAULT_BASE_URL,
)
from palace.manager.integration.license.bibliotheca.settings import (
    BibliothecaSettings,
)
from palace.manager.util.problem_detail import ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture

QA_URL = "https://partnerqa.yourcloudlibrary.com"


class TestBibliothecaSettings:
    REQUIRED = frozendict(
        {"username": "a", "password": "b", "external_account_id": "c"}
    )

    @pytest.mark.parametrize(
        "settings, expected, saved",
        [
            pytest.param({}, BIBLIOTHECA_DEFAULT_BASE_URL, False, id="missing"),
            pytest.param(
                {"server_url": None}, BIBLIOTHECA_DEFAULT_BASE_URL, False, id="none"
            ),
            pytest.param(
                {"server_url": ""}, BIBLIOTHECA_DEFAULT_BASE_URL, False, id="blank"
            ),
            pytest.param(
                {"server_url": f"{BIBLIOTHECA_DEFAULT_BASE_URL}/"},
                BIBLIOTHECA_DEFAULT_BASE_URL,
                False,
                id="production-trailing-slash",
            ),
            pytest.param({"server_url": QA_URL}, QA_URL, True, id="qa"),
            pytest.param(
                {"server_url": f"{QA_URL}/"}, QA_URL, True, id="qa-trailing-slash"
            ),
            pytest.param(
                {"server_url": f"{QA_URL}:8443/"},
                f"{QA_URL}:8443",
                True,
                id="port",
            ),
            pytest.param(
                {"url": "not a url"},
                BIBLIOTHECA_DEFAULT_BASE_URL,
                False,
                id="legacy-url-key-ignored",
            ),
        ],
    )
    def test_server_url(
        self, settings: dict[str, Any], expected: str, saved: bool
    ) -> None:
        result = BibliothecaSettings(**self.REQUIRED, **settings)
        assert result.server_url == expected
        dumped = result.model_dump()
        if saved:
            assert dumped["server_url"] == expected
        else:
            assert "server_url" not in dumped

    @pytest.mark.parametrize(
        "server_url",
        [
            pytest.param("not a url", id="not-a-url"),
            pytest.param("ftp://partnerqa.yourcloudlibrary.com", id="ftp"),
            pytest.param("http://partnerqa.yourcloudlibrary.com", id="http"),
            pytest.param(f"{QA_URL}/cirrus", id="path"),
            pytest.param(f"{QA_URL}/?x=1", id="query"),
            pytest.param(f"{QA_URL}#f", id="fragment"),
            pytest.param(
                "https://user:pw@partnerqa.yourcloudlibrary.com", id="credentials"
            ),
        ],
    )
    def test_server_url_invalid(self, server_url: str) -> None:
        with pytest.raises(ProblemDetailException) as excinfo:
            BibliothecaSettings(**self.REQUIRED, server_url=server_url)
        problem_detail = excinfo.value.problem_detail
        assert problem_detail.uri == INVALID_CONFIGURATION_OPTION.uri
        assert "'Server URL' validation error" in str(problem_detail.detail)

    def test_server_url_form(self, db: DatabaseTransactionFixture) -> None:
        [field] = [
            f
            for f in BibliothecaSettings.configuration_form(db.session)
            if f["key"] == "server_url"
        ]
        assert field["default"] == BIBLIOTHECA_DEFAULT_BASE_URL
        assert field["required"] is False
        assert BIBLIOTHECA_DEFAULT_BASE_URL in field["description"]
