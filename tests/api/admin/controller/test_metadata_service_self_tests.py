from unittest.mock import MagicMock, create_autospec

import pytest
from _pytest.monkeypatch import MonkeyPatch
from flask import Response

from api.admin.controller.metadata_service_self_tests import (
    MetadataServiceSelfTestsController,
)
from api.admin.problem_details import *
from api.nyt import NYTBestSellerAPI
from core.util.problem_detail import ProblemDetail
from tests.api.admin.controller.test_metadata_services import MetadataServicesFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class MetadataServiceSelfTestsFixture(MetadataServicesFixture):
    def __init__(self, db: DatabaseTransactionFixture):
        super().__init__(db)
        manager = MagicMock()
        manager._db = db.session
        self.controller = MetadataServiceSelfTestsController(manager)
        self.db = db


@pytest.fixture
def metadata_services_fixture(
    db: DatabaseTransactionFixture,
) -> MetadataServiceSelfTestsFixture:
    return MetadataServiceSelfTestsFixture(db)


class TestMetadataServiceSelfTests:
    def test_metadata_service_self_tests_with_no_identifier(
        self, metadata_services_fixture: MetadataServiceSelfTestsFixture
    ):
        response = (
            metadata_services_fixture.controller.process_metadata_service_self_tests(
                None
            )
        )
        assert isinstance(response, ProblemDetail)
        assert response.title == MISSING_IDENTIFIER.title
        assert response.detail == MISSING_IDENTIFIER.detail
        assert response.status_code == 400

    def test_metadata_service_self_tests_with_no_metadata_service_found(
        self,
        metadata_services_fixture: MetadataServiceSelfTestsFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context("/"):
            response = metadata_services_fixture.controller.process_metadata_service_self_tests(
                -1
            )
        assert response == MISSING_SERVICE
        assert response.status_code == 404

    def test_metadata_service_self_tests_test_get(
        self,
        metadata_services_fixture: MetadataServiceSelfTestsFixture,
        flask_app_fixture: FlaskAppFixture,
        monkeypatch: MonkeyPatch,
    ):
        metadata_service = metadata_services_fixture.create_nyt_integration()
        mock_prior_test_results = create_autospec(
            NYTBestSellerAPI.prior_test_results, return_value={"test": "results"}
        )
        monkeypatch.setattr(
            NYTBestSellerAPI, "prior_test_results", mock_prior_test_results
        )

        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's self tests object.
        with flask_app_fixture.test_request_context("/"):
            response_data = metadata_services_fixture.controller.process_metadata_service_self_tests(
                metadata_service.id
            )
            assert isinstance(response_data, dict)
            response_metadata_service = response_data.get("self_test_results", {})

            assert response_metadata_service.get("id") == metadata_service.id
            assert response_metadata_service.get("name") == metadata_service.name
            assert (
                response_metadata_service.get("protocol").get("label")
                == NYTBestSellerAPI.NAME
            )
            assert response_metadata_service.get("goal") == metadata_service.goal
            assert response_metadata_service.get("self_test_results") == {
                "test": "results"
            }

    def test_metadata_service_self_tests_post(
        self,
        metadata_services_fixture: MetadataServiceSelfTestsFixture,
        flask_app_fixture: FlaskAppFixture,
        monkeypatch: MonkeyPatch,
        db: DatabaseTransactionFixture,
    ):
        metadata_service = metadata_services_fixture.create_nyt_integration()
        mock_run_self_tests = create_autospec(
            NYTBestSellerAPI.run_self_tests, return_value=(dict(test="results"), None)
        )
        monkeypatch.setattr(NYTBestSellerAPI, "run_self_tests", mock_run_self_tests)

        controller = metadata_services_fixture.controller
        with flask_app_fixture.test_request_context("/", method="POST"):
            response = controller.process_metadata_service_self_tests(
                metadata_service.id
            )
            assert isinstance(response, Response)
            assert response.status_code == 200
            assert "Successfully ran new self tests" == response.get_data(as_text=True)

        mock_run_self_tests.assert_called_once_with(
            db.session, NYTBestSellerAPI.from_config, db.session
        )
