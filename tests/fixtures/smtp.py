from smtplib import SMTP

import pytest


class MockSMTP(SMTP):
    def __init__(self):
        pass


@pytest.fixture(scope="function")
def smtp_fixture() -> SMTP:
    return MockSMTP
