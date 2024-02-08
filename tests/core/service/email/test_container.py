from unittest.mock import create_autospec

import pytest
from redmail import EmailSender

from core.service.email.container import Email


@pytest.fixture
def container() -> Email:
    email = Email()
    email.config.from_dict(
        {
            "server": "test_server.com",
            "port": 587,
            "username": "username",
            "password": "password",
            "sender": "test@test.com",
        }
    )
    return email


def test_emailer(container: Email):
    emailer = container.emailer()
    assert isinstance(emailer, EmailSender)
    assert emailer.host == "test_server.com"
    assert emailer.port == 587
    assert emailer.username == "username"
    assert emailer.password == "password"


def test_send_email(container: Email):
    mock_emailer = create_autospec(EmailSender)
    container.emailer.override(mock_emailer)
    container.send_email(subject="subject", receivers=["x@y.com", "a@b.com"])
    mock_emailer.send.assert_called_once_with(
        subject="subject",
        sender="test@test.com",
        receivers=["x@y.com", "a@b.com"],
        text=None,
        html=None,
        attachments=None,
    )
