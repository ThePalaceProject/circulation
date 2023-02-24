from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import pytest
from flask import Flask, request, url_for

from api.clever import (
    CLEVER_NOT_ELIGIBLE,
    CLEVER_UNKNOWN_SCHOOL,
    UNSUPPORTED_CLEVER_USER_TYPE,
    CleverAuthenticationAPI,
    external_type_from_clever_grade,
)
from api.problem_details import INVALID_CREDENTIALS
from core.model import ExternalIntegration
from core.util.datetime_helpers import utc_now
from core.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture

if TYPE_CHECKING:
    from core.model.library import Library


class MockAPI(CleverAuthenticationAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = []

    def queue_response(self, response):
        self.queue.insert(0, response)

    def _get_token(self, payload, headers):
        return self.queue.pop()

    def _get(self, url, headers):
        return self.queue.pop()

    def _server_redirect_uri(self):
        return ""

    def _internal_authenticate_url(self):
        return ""


class TestClever:
    def test_external_type_from_clever_grade(self):
        """
        GIVEN: A string representing a student grade level supplied by the Clever API
        WHEN:  That string is present in api.clever.CLEVER_GRADE_TO_EXTERNAL_TYPE_MAP
        THEN:  The matching external_type value should be returned, or None if the match fails
        """
        for e_grade in [
            "InfantToddler",
            "Preschool",
            "PreKindergarten",
            "TransitionalKindergarten",
            "Kindergarten",
            "1",
            "2",
            "3",
        ]:
            assert external_type_from_clever_grade(e_grade) == "E"

        for m_grade in ["4", "5", "6", "7", "8"]:
            assert external_type_from_clever_grade(m_grade) == "M"

        for h_grade in ["9", "10", "11", "12", "13", "PostGraduate"]:
            assert external_type_from_clever_grade(h_grade) == "H"

        for none_grade in ["Other", "Ungraded", None, "NOT A VALID GRADE STRING"]:
            assert external_type_from_clever_grade(none_grade) is None


class CleverAuthenticationFixture:
    db: DatabaseTransactionFixture
    api: MockAPI
    app: Flask

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.api = MockAPI(db.default_library(), self.mock_integration())

        from api.app import app

        self.app = app

    def mock_integration(self):
        """Make a fake ExternalIntegration that can be used to configure a CleverAuthenticationAPI"""
        integration = self.db.external_integration(
            protocol="OAuth",
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            username="fake_client_id",
            password="fake_client_secret",
        )
        integration.setting(MockAPI.OAUTH_TOKEN_EXPIRATION_DAYS).value = 20
        return integration


@pytest.fixture(scope="function")
def clever_fixture(
    db: DatabaseTransactionFixture, monkeypatch: pytest.MonkeyPatch
) -> CleverAuthenticationFixture:
    monkeypatch.setenv("AUTOINITIALIZE", "False")

    return CleverAuthenticationFixture(db)


class TestCleverAuthenticationAPI:
    def test_authenticated_patron(self, clever_fixture: CleverAuthenticationFixture):
        """An end-to-end test of authenticated_patron()."""
        assert (
            clever_fixture.api.authenticated_patron(
                clever_fixture.db.session, "not a valid token"
            )
            is None
        )

        # This patron has a valid clever token.
        patron = clever_fixture.db.patron()
        (credential, _) = clever_fixture.api.create_token(
            clever_fixture.db.session, patron, "test"
        )
        assert patron == clever_fixture.api.authenticated_patron(
            clever_fixture.db.session, "test"
        )

        # If the token is expired, the patron has to log in again.
        credential.expires = utc_now() - datetime.timedelta(days=1)
        assert (
            clever_fixture.api.authenticated_patron(clever_fixture.db.session, "test")
            is None
        )

    def test_remote_exchange_code_for_bearer_token(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        # Test success.
        clever_fixture.api.queue_response(dict(access_token="a token"))
        with clever_fixture.app.test_request_context("/"):
            assert (
                clever_fixture.api.remote_exchange_code_for_bearer_token(
                    clever_fixture.db.session, "code"
                )
                == "a token"
            )

        # Test failure.
        clever_fixture.api.queue_response(None)
        with clever_fixture.app.test_request_context("/"):
            problem = clever_fixture.api.remote_exchange_code_for_bearer_token(
                clever_fixture.db.session, "code"
            )
        assert INVALID_CREDENTIALS.uri == problem.uri

        clever_fixture.api.queue_response(dict(something_else="not a token"))
        with clever_fixture.app.test_request_context("/"):
            problem = clever_fixture.api.remote_exchange_code_for_bearer_token(
                clever_fixture.db.session, "code"
            )
        assert INVALID_CREDENTIALS.uri == problem.uri

    def test_remote_exchange_payload(self, clever_fixture: CleverAuthenticationFixture):
        """Test the content of the document sent to Clever when exchanging tokens"""
        with clever_fixture.app.test_request_context("/"):
            payload = clever_fixture.api._remote_exchange_payload(
                clever_fixture.db.session, "a code"
            )

            expect_uri = url_for(
                "oauth_callback",
                library_short_name=clever_fixture.db.default_library().name,
                _external=True,
            )
            assert "authorization_code" == payload["grant_type"]
            assert expect_uri == payload["redirect_uri"]
            assert "a code" == payload["code"]

    def test_remote_patron_lookup_unsupported_user_type(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        clever_fixture.api.queue_response(
            dict(type="district_admin", data=dict(id="1234"))
        )
        token = clever_fixture.api.remote_patron_lookup("token")
        assert UNSUPPORTED_CLEVER_USER_TYPE == token

    def test_remote_patron_lookup_ineligible(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        clever_fixture.api.queue_response(
            dict(
                type="student",
                data=dict(id="1234"),
                links=[dict(rel="canonical", uri="test")],
            )
        )
        clever_fixture.api.queue_response(
            dict(data=dict(school="1234", district="1234"))
        )
        clever_fixture.api.queue_response(dict(data=dict(nces_id="I am not Title I")))

        token = clever_fixture.api.remote_patron_lookup("")
        assert CLEVER_NOT_ELIGIBLE == token

    def test_remote_patron_lookup_missing_nces_id(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        clever_fixture.api.queue_response(
            dict(
                type="student",
                data=dict(id="1234"),
                links=[dict(rel="canonical", uri="test")],
            )
        )
        clever_fixture.api.queue_response(
            dict(data=dict(school="1234", district="1234"))
        )
        clever_fixture.api.queue_response(dict(data=dict()))

        token = clever_fixture.api.remote_patron_lookup("")
        assert CLEVER_UNKNOWN_SCHOOL == token

    def test_remote_patron_unknown_student_grade(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        clever_fixture.api.queue_response(
            dict(
                type="student",
                data=dict(id="2"),
                links=[dict(rel="canonical", uri="test")],
            )
        )
        clever_fixture.api.queue_response(
            dict(data=dict(school="1234", district="1234", name="Abcd", grade=""))
        )
        clever_fixture.api.queue_response(dict(data=dict(nces_id="44270647")))

        patrondata = clever_fixture.api.remote_patron_lookup("token")
        assert patrondata.external_type is None

    def test_remote_patron_lookup_title_i(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        clever_fixture.api.queue_response(
            dict(
                type="student",
                data=dict(id="5678"),
                links=[dict(rel="canonical", uri="test")],
            )
        )
        clever_fixture.api.queue_response(
            dict(data=dict(school="1234", district="1234", name="Abcd", grade="10"))
        )
        clever_fixture.api.queue_response(dict(data=dict(nces_id="44270647")))

        patrondata = clever_fixture.api.remote_patron_lookup("token")
        assert patrondata.personal_name is None
        assert "5678" == patrondata.permanent_id
        assert "5678" == patrondata.authorization_identifier

    def test_remote_patron_lookup_external_type(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        # Teachers have an external type of 'A' indicating all access.
        clever_fixture.api.queue_response(
            dict(
                type="teacher",
                data=dict(id="1"),
                links=[dict(rel="canonical", uri="test")],
            )
        )
        clever_fixture.api.queue_response(
            dict(data=dict(school="1234", district="1234", name="Abcd"))
        )
        clever_fixture.api.queue_response(dict(data=dict(nces_id="44270647")))

        patrondata = clever_fixture.api.remote_patron_lookup("teacher token")
        assert "A" == patrondata.external_type

        # Student type is based on grade
        def queue_student(grade):
            clever_fixture.api.queue_response(
                dict(
                    type="student",
                    data=dict(id="2"),
                    links=[dict(rel="canonical", uri="test")],
                )
            )
            clever_fixture.api.queue_response(
                dict(
                    data=dict(school="1234", district="1234", name="Abcd", grade=grade)
                )
            )
            clever_fixture.api.queue_response(dict(data=dict(nces_id="44270647")))

        queue_student(grade="1")
        patrondata = clever_fixture.api.remote_patron_lookup("token")
        assert "E" == patrondata.external_type

        queue_student(grade="6")
        patrondata = clever_fixture.api.remote_patron_lookup("token")
        assert "M" == patrondata.external_type

        queue_student(grade="9")
        patrondata = clever_fixture.api.remote_patron_lookup("token")
        assert "H" == patrondata.external_type

    def test_oauth_callback_creates_patron(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        """Test a successful run of oauth_callback."""
        clever_fixture.api.queue_response(dict(access_token="bearer token"))
        clever_fixture.api.queue_response(
            dict(
                type="teacher",
                data=dict(id="1"),
                links=[dict(rel="canonical", uri="test")],
            )
        )
        clever_fixture.api.queue_response(
            dict(data=dict(school="1234", district="1234", name="Abcd"))
        )
        clever_fixture.api.queue_response(dict(data=dict(nces_id="44270647")))

        with clever_fixture.app.test_request_context("/"):
            response = clever_fixture.api.oauth_callback(
                clever_fixture.db.session, dict(code="teacher code")
            )
            credential, patron, patrondata = response

        # The bearer token was turned into a Credential.
        expect_credential, ignore = clever_fixture.api.create_token(
            clever_fixture.db.session, patron, "bearer token"
        )
        assert credential == expect_credential

        # Since the patron is a teacher, their external_type was set to 'A'.
        assert "A" == patron.external_type

        # Clever provided personal name information, but we don't include it in the PatronData.
        assert patrondata.personal_name is None

    def test_oauth_callback_problem_detail_if_bad_token(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        clever_fixture.api.queue_response(dict(something_else="not a token"))
        with clever_fixture.app.test_request_context("/"):
            response = clever_fixture.api.oauth_callback(
                clever_fixture.db.session, dict(code="teacher code")
            )
        assert isinstance(response, ProblemDetail)
        assert INVALID_CREDENTIALS.uri == response.uri

    def test_oauth_callback_problem_detail_if_remote_patron_lookup_fails(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        clever_fixture.api.queue_response(dict(access_token="token"))
        clever_fixture.api.queue_response(dict())

        with clever_fixture.app.test_request_context("/"):
            response = clever_fixture.api.oauth_callback(
                clever_fixture.db.session, dict(code="teacher code")
            )

        assert isinstance(response, ProblemDetail)
        assert INVALID_CREDENTIALS.uri == response.uri

    def test_external_authenticate_url(
        self, clever_fixture: CleverAuthenticationFixture
    ):
        """Verify that external_authenticate_url is generated properly"""
        # We're about to call url_for, so we must create an application context.
        my_api = CleverAuthenticationAPI(
            clever_fixture.db.default_library(), clever_fixture.mock_integration()
        )

        with clever_fixture.app.test_request_context("/"):
            request.library: Library = clever_fixture.db.default_library()  # type: ignore
            params = my_api.external_authenticate_url(
                "state", clever_fixture.db.session
            )
            expected_redirect_uri = url_for(
                "oauth_callback",
                library_short_name=clever_fixture.db.default_library().short_name,
                _external=True,
            )
            expected = (
                "https://clever.com/oauth/authorize"
                "?response_type=code&client_id=fake_client_id&redirect_uri=%s&state=state"
            ) % expected_redirect_uri
            assert params == expected
