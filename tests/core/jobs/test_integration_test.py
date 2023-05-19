from io import StringIO
from typing import List
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from core.jobs.integration_test import (
    FailedIntegrationTest,
    IntegrationTest,
    IntegrationTestDetails,
)
from tests.fixtures.database import DatabaseTransactionFixture

BASIC_YAML = """
---
- name: Test
  endpoint: localhost
"""


class IntegrationTestFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.script = IntegrationTest(db.session)

    def patch_config(self, data: List[dict]):
        self.script._read_config = MagicMock(return_value=data)


@pytest.fixture(scope="function")
def integration_test(db: DatabaseTransactionFixture):
    return IntegrationTestFixture(db)


class TestIntegrationTest:
    def test_read_config(self, integration_test: IntegrationTestFixture):
        basic_yaml_dict = [{"name": "Test", "endpoint": "localhost"}]

        with patch("core.jobs.integration_test.open") as open:
            open.return_value = StringIO(BASIC_YAML)
            data = integration_test.script._read_config("on/disk/filepath")

            assert open.call_count == 1
            assert open.call_args == call("on/disk/filepath", "r")
            assert data == basic_yaml_dict

        with patch(
            "core.jobs.integration_test.HTTP.get_with_timeout"
        ) as get_with_timeout:
            response = get_with_timeout.return_value
            response.status_code = 400
            pytest.raises(Exception, integration_test.script._read_config, "http://...")
            assert get_with_timeout.call_count == 1

            get_with_timeout.reset_mock()
            response.status_code = 200
            response.content = StringIO(BASIC_YAML)
            data = integration_test.script._read_config("http://...")

            assert get_with_timeout.call_count == 1
            assert get_with_timeout.call_args == call("http://...")
            assert data == basic_yaml_dict

    def test__run_test(self, integration_test: IntegrationTestFixture):
        with patch("core.jobs.integration_test.HTTP.request_with_timeout") as request:
            request.return_value = Mock(
                status_code=204, json=lambda: dict(status="created")
            )
            details = IntegrationTestDetails(
                name="test",
                endpoint="http://...",
                method="POST",
                request_headers=dict(Authorization="Basic Auth"),
                request_body=dict(key="value"),
                expected_json=dict(status="created"),
                expected_status_code=204,
            )
            integration_test.script._run_test(details)

            assert request.call_count == 1
            assert request.call_args == call(
                "POST",
                "http://...",
                headers=details.request_headers,
                data=details.request_body,
            )

            # Error tests
            # Incorrect status code
            request.reset_mock()
            request.return_value.status_code = 200
            with pytest.raises(FailedIntegrationTest) as raised:
                integration_test.script._run_test(details)

            assert str(raised.value) == "Status code 200 != 204"

            # Incorrect response body
            request.reset_mock()
            request.return_value.status_code = 204
            request.return_value.json = lambda: dict(status="deleted")

            with pytest.raises(FailedIntegrationTest) as raised:
                integration_test.script._run_test(details)

            assert (
                str(raised.value)
                == "JSON response did not match the expected: {'status': 'deleted'} != {'status': 'created'}"
            )
