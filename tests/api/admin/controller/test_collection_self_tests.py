from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch

from api.admin.controller.collection_self_tests import CollectionSelfTestsController
from api.admin.problem_details import (
    FAILED_TO_RUN_SELF_TESTS,
    MISSING_IDENTIFIER,
    MISSING_SERVICE,
    UNKNOWN_PROTOCOL,
)
from api.integration.registry.license_providers import LicenseProvidersRegistry
from api.selftest import HasCollectionSelfTests
from core.selftest import HasSelfTestsIntegrationConfiguration
from core.util.problem_detail import ProblemDetail, ProblemError
from tests.api.mockapi.axis import MockAxis360API
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> CollectionSelfTestsController:
    return CollectionSelfTestsController(db.session)


class TestCollectionSelfTests:
    def test_collection_self_tests_with_no_identifier(
        self, controller: CollectionSelfTestsController
    ):
        response = controller.process_collection_self_tests(None)
        assert isinstance(response, ProblemDetail)
        assert response.title == MISSING_IDENTIFIER.title
        assert response.detail == MISSING_IDENTIFIER.detail
        assert response.status_code == 400

    def test_collection_self_tests_with_no_collection_found(
        self, controller: CollectionSelfTestsController
    ):
        with pytest.raises(ProblemError) as excinfo:
            controller.self_tests_process_get(-1)
        assert excinfo.value.problem_detail == MISSING_SERVICE

    def test_collection_self_tests_with_unknown_protocol(
        self, db: DatabaseTransactionFixture, controller: CollectionSelfTestsController
    ):
        collection = db.collection(protocol="test")
        assert collection.integration_configuration.id is not None
        with pytest.raises(ProblemError) as excinfo:
            controller.self_tests_process_get(collection.integration_configuration.id)
        assert excinfo.value.problem_detail == UNKNOWN_PROTOCOL

    def test_collection_self_tests_with_unsupported_protocol(
        self, db: DatabaseTransactionFixture, controller: CollectionSelfTestsController
    ):
        registry = LicenseProvidersRegistry()
        registry.register(object, canonical="mock_api")  # type: ignore[arg-type]
        collection = db.collection(protocol="mock_api")
        controller = CollectionSelfTestsController(db.session, registry)
        assert collection.integration_configuration.id is not None
        result = controller.self_tests_process_get(
            collection.integration_configuration.id
        )

        assert result.status_code == 200
        assert isinstance(result.json, dict)
        assert result.json["self_test_results"]["self_test_results"] == {
            "disabled": True,
            "exception": "Self tests are not supported for this integration.",
        }

    def test_collection_self_tests_test_get(
        self,
        db: DatabaseTransactionFixture,
        controller: CollectionSelfTestsController,
        monkeypatch: MonkeyPatch,
    ):
        collection = MockAxis360API.mock_collection(
            db.session,
            db.default_library(),
        )

        self_test_results = dict(
            duration=0.9,
            start="2018-08-08T16:04:05Z",
            end="2018-08-08T16:05:05Z",
            results=[],
        )
        mock = MagicMock(return_value=self_test_results)
        monkeypatch.setattr(
            HasSelfTestsIntegrationConfiguration, "load_self_test_results", mock
        )

        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's collection object.
        assert collection.integration_configuration.id is not None
        response = controller.self_tests_process_get(
            collection.integration_configuration.id
        )

        data = response.json
        assert isinstance(data, dict)
        test_results = data.get("self_test_results")
        assert isinstance(test_results, dict)

        assert test_results.get("id") == collection.integration_configuration.id
        assert test_results.get("name") == collection.name
        assert test_results.get("protocol") == collection.protocol
        assert test_results.get("self_test_results") == self_test_results
        assert mock.call_count == 1

    def test_collection_self_tests_failed_post(
        self,
        db: DatabaseTransactionFixture,
        controller: CollectionSelfTestsController,
        monkeypatch: MonkeyPatch,
    ):
        collection = MockAxis360API.mock_collection(
            db.session,
            db.default_library(),
        )

        # This makes HasSelfTests.run_self_tests return no values
        self_test_results = (None, None)
        mock = MagicMock(return_value=self_test_results)
        monkeypatch.setattr(
            HasSelfTestsIntegrationConfiguration, "run_self_tests", mock
        )

        # Failed to run self tests
        assert collection.integration_configuration.id is not None

        with pytest.raises(ProblemError) as excinfo:
            controller.self_tests_process_post(collection.integration_configuration.id)

        assert excinfo.value.problem_detail == FAILED_TO_RUN_SELF_TESTS

    def test_collection_self_tests_run_self_tests_unsupported_collection(
        self,
        db: DatabaseTransactionFixture,
    ):
        registry = LicenseProvidersRegistry()
        registry.register(object, canonical="mock_api")  # type: ignore[arg-type]
        collection = db.collection(protocol="mock_api")
        controller = CollectionSelfTestsController(db.session, registry)
        response = controller.run_self_tests(collection.integration_configuration)
        assert response is None

    def test_collection_self_tests_post(
        self,
        db: DatabaseTransactionFixture,
    ):
        mock = MagicMock()

        class MockApi(HasCollectionSelfTests):
            def __new__(cls, *args, **kwargs):
                nonlocal mock
                return mock(*args, **kwargs)

            @property
            def collection(self) -> None:
                return None

        registry = LicenseProvidersRegistry()
        registry.register(MockApi, canonical="Foo")  # type: ignore[arg-type]

        collection = db.collection(protocol="Foo")
        controller = CollectionSelfTestsController(db.session, registry)

        assert collection.integration_configuration.id is not None
        response = controller.self_tests_process_post(
            collection.integration_configuration.id
        )

        assert response.get_data(as_text=True) == "Successfully ran new self tests"
        assert response.status_code == 200

        mock.assert_called_once_with(db.session, collection)
        mock()._run_self_tests.assert_called_once_with(db.session)
        assert mock().store_self_test_results.call_count == 1
