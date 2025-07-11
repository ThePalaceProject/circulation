import json

import pytest

from palace.manager.api.opds.exception import OpdsResponseException
from tests.mocks.mock import MockRequestsResponse


class TestOpdsResponseException:
    @pytest.mark.parametrize(
        "code,type,data,none_response",
        [
            pytest.param(400, None, "Error", True, id="no content type"),
            pytest.param(
                500, "application/json", "Error", True, id="unsupported content type"
            ),
            pytest.param(
                404,
                "application/problem+json",
                "{}",
                True,
                id="missing required fields",
            ),
            pytest.param(
                420,
                "application/api-problem+json",
                "hot garbage",
                True,
                id="invalid json",
            ),
            pytest.param(
                404,
                "application/problem+json",
                json.dumps(
                    {
                        "type": "http://problem-uri",
                        "title": "Robot overlords on strike",
                        "status": 404,
                    }
                ),
                False,
                id="missing required fields",
            ),
        ],
    )
    def test_from_response(
        self, code: int, type: str, data: str, none_response: bool
    ) -> None:
        headers = {}
        if type:
            headers["Content-Type"] = type
        response = MockRequestsResponse(code, headers, data)
        exception = OpdsResponseException.from_response(response)

        if none_response:
            assert exception is None
        else:
            assert isinstance(exception, OpdsResponseException)
            assert exception.status == code
            assert exception.problem_detail.response[0] == data
