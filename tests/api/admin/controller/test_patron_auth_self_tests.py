from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from flask import Response

from api.admin.controller.patron_auth_service_self_tests import (
    PatronAuthServiceSelfTestsController,
)
from api.admin.problem_details import (
    FAILED_TO_RUN_SELF_TESTS,
    MISSING_IDENTIFIER,
    MISSING_SERVICE,
)
from core.model import Library
from core.selftest import HasSelfTestsIntegrationConfiguration
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch
    from flask.ctx import RequestContext

    from tests.fixtures.authenticator import SimpleAuthIntegrationFixture
    from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> PatronAuthServiceSelfTestsController:
    return PatronAuthServiceSelfTestsController(db.session)


class TestPatronAuthSelfTests:
    def test_patron_auth_self_tests_with_no_identifier(
        self, controller: PatronAuthServiceSelfTestsController
    ):
        response = controller.process_patron_auth_service_self_tests(None)
        assert isinstance(response, ProblemDetail)
        assert response.title == MISSING_IDENTIFIER.title
        assert response.detail == MISSING_IDENTIFIER.detail
        assert response.status_code == 400

    def test_patron_auth_self_tests_with_no_auth_service_found(
        self,
        controller: PatronAuthServiceSelfTestsController,
        get_request_context: RequestContext,
    ):
        response = controller.process_patron_auth_service_self_tests(-1)
        assert isinstance(response, ProblemDetail)
        assert response == MISSING_SERVICE
        assert response.status_code == 404

    def test_patron_auth_self_tests_get_with_no_libraries(
        self,
        controller: PatronAuthServiceSelfTestsController,
        get_request_context: RequestContext,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        auth_service, _ = create_simple_auth_integration()
        response_obj = controller.process_patron_auth_service_self_tests(
            auth_service.id
        )
        assert isinstance(response_obj, Response)
        response = json.loads(response_obj.response[0])  # type: ignore[index]
        results = response.get("self_test_results", {}).get("self_test_results")
        assert results.get("disabled") is True
        assert (
            results.get("exception")
            == "You must associate this service with at least one library before you can run self tests for it."
        )

    def test_patron_auth_self_tests_test_get_no_results(
        self,
        controller: PatronAuthServiceSelfTestsController,
        get_request_context: RequestContext,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        default_library: Library,
    ):
        auth_service, _ = create_simple_auth_integration(library=default_library)

        # Make sure that we return the correct response when there are no results
        response_obj = controller.process_patron_auth_service_self_tests(
            auth_service.id
        )
        assert isinstance(response_obj, Response)
        response = json.loads(response_obj.response[0])  # type: ignore[index]
        response_auth_service = response.get("self_test_results", {})

        assert response_auth_service.get("name") == auth_service.name
        assert response_auth_service.get("protocol") == auth_service.protocol
        assert response_auth_service.get("id") == auth_service.id
        assert auth_service.goal is not None
        assert response_auth_service.get("goal") == auth_service.goal.value
        assert response_auth_service.get("self_test_results") == "No results yet"

    def test_patron_auth_self_tests_test_get(
        self,
        controller: PatronAuthServiceSelfTestsController,
        get_request_context: RequestContext,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        monkeypatch: MonkeyPatch,
        default_library: Library,
    ):
        expected_results = dict(
            duration=0.9,
            start="2018-08-08T16:04:05Z",
            end="2018-08-08T16:05:05Z",
            results=[],
        )
        mock = MagicMock(return_value=expected_results)
        monkeypatch.setattr(
            HasSelfTestsIntegrationConfiguration, "load_self_test_results", mock
        )
        auth_service, _ = create_simple_auth_integration(library=default_library)

        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's self tests object.
        response_obj = controller.process_patron_auth_service_self_tests(
            auth_service.id
        )
        assert isinstance(response_obj, Response)
        response = json.loads(response_obj.response[0])  # type: ignore[index]
        response_auth_service = response.get("self_test_results", {})

        assert response_auth_service.get("name") == auth_service.name
        assert response_auth_service.get("protocol") == auth_service.protocol
        assert response_auth_service.get("id") == auth_service.id
        assert auth_service.goal is not None
        assert response_auth_service.get("goal") == auth_service.goal.value
        assert response_auth_service.get("self_test_results") == expected_results
        mock.assert_called_once_with(auth_service)

    def test_patron_auth_self_tests_post_with_no_libraries(
        self,
        controller: PatronAuthServiceSelfTestsController,
        post_request_context: RequestContext,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        auth_service, _ = create_simple_auth_integration()
        response = controller.process_patron_auth_service_self_tests(auth_service.id)
        assert isinstance(response, ProblemDetail)
        assert response.title == FAILED_TO_RUN_SELF_TESTS.title
        assert response.detail is not None
        assert "Failed to run self tests" in response.detail
        assert response.status_code == 400

    def test_patron_auth_self_tests_test_post(
        self,
        controller: PatronAuthServiceSelfTestsController,
        post_request_context: RequestContext,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        monkeypatch: MonkeyPatch,
        db: DatabaseTransactionFixture,
    ):
        expected_results = ("value", "results")
        mock = MagicMock(return_value=expected_results)
        monkeypatch.setattr(
            HasSelfTestsIntegrationConfiguration, "run_self_tests", mock
        )
        library = db.default_library()
        auth_service, _ = create_simple_auth_integration(library=library)

        response = controller.process_patron_auth_service_self_tests(auth_service.id)
        assert isinstance(response, Response)
        assert response.status == "200 OK"
        assert "Successfully ran new self tests" == response.get_data(as_text=True)

        assert mock.call_count == 1
        assert mock.call_args.args[0] == db.session
        assert mock.call_args.args[1] is None
        assert mock.call_args.args[2] == library.id
        assert mock.call_args.args[3] == auth_service.id
