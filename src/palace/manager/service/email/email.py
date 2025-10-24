import os
from collections.abc import Mapping
from email.message import EmailMessage
from typing import Any, Protocol

from redmail.email.sender import EmailSender

from palace.manager.core.config import CannotLoadConfiguration


def emailer_factory(
    host: str | None, port: int, username: str | None, password: str | None
) -> EmailSender:
    if host is None:
        raise CannotLoadConfiguration(
            "Mail server must be provided. Please set PALACE_MAIL_SERVER."
        )

    return EmailSender(
        host=host,
        port=port,
        # Username and password are ignored here because the emailer library has them
        # as required, but defaults them to None. So their types are not correct.
        # PR here to fix the type hint upstream: https://github.com/Miksus/red-mail/pull/90
        username=username,  # type: ignore[arg-type]
        password=password,  # type: ignore[arg-type]
    )


def send_email(
    *,
    emailer: EmailSender,
    sender: str,
    subject: str,
    receivers: list[str] | str,
    html: str | None = None,
    text: str | None = None,
    attachments: Mapping[str, str | os.PathLike[Any] | bytes] | None = None,
) -> EmailMessage:
    return emailer.send(
        subject=subject,
        sender=sender,
        receivers=receivers,
        text=text,
        html=html,
        # Attachments are not typed correctly in the library, so we need to ignore the
        # type hint here. Since no one has responded to the PR above to fix the other
        # type hint issue, I'm not going to bother submitting a PR to fix this one.
        # We define attachments as a Mapping rather than a dict so that its covariant
        # and can accept a dict of the types that the library accepts.
        # See: https://mypy.readthedocs.io/en/stable/common_issues.html#variance
        attachments=attachments,  # type: ignore[arg-type]
    )


class SendEmailCallable(Protocol):
    def __call__(
        self,
        *,
        subject: str,
        receivers: list[str] | str,
        html: str | None = None,
        text: str | None = None,
        attachments: Mapping[str, str | os.PathLike[Any] | bytes] | None = None,
    ) -> EmailMessage: ...
