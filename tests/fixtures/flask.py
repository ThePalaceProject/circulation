from typing import Generator

import pytest
from flask.ctx import RequestContext
from flask_babel import Babel

from api.util.flask import PalaceFlask


@pytest.fixture
def mock_app() -> PalaceFlask:
    app = PalaceFlask(__name__)
    Babel(app)
    return app


@pytest.fixture
def get_request_context(mock_app: PalaceFlask) -> Generator[RequestContext, None, None]:
    with mock_app.test_request_context("/") as mock_request_context:
        yield mock_request_context


@pytest.fixture
def post_request_context(
    mock_app: PalaceFlask,
) -> Generator[RequestContext, None, None]:
    with mock_app.test_request_context("/", method="POST") as mock_request_context:
        yield mock_request_context
