"""Simple helper library for generating problem detail documents.

As per http://datatracker.ietf.org/doc/draft-ietf-appsawg-http-problem/
"""
from __future__ import annotations

import json as j
import logging
from abc import ABC, abstractmethod

from flask_babel import LazyString
from pydantic import BaseModel

from core.exceptions import BasePalaceException

JSON_MEDIA_TYPE = "application/api-problem+json"


def json(
    type: str,
    status: int | None,
    title: str | None,
    detail: str | None = None,
    debug_message: str | None = None,
) -> str:
    d = dict(type=type, title=str(title), status=status)
    if detail:
        d["detail"] = str(detail)
    if debug_message:
        d["debug_message"] = debug_message
    return j.dumps(d)


class ProblemDetailModel(BaseModel):
    type: str | None = None
    status: int | None = None
    title: str | None = None
    detail: str | None = None
    debug_message: str | None = None


class ProblemDetail:

    """A common type of problem."""

    JSON_MEDIA_TYPE = JSON_MEDIA_TYPE

    def __init__(
        self,
        uri: str,
        status_code: int | None = None,
        title: str | None = None,
        detail: str | None = None,
        debug_message: str | None = None,
    ):
        self.uri = uri
        self.title = title
        self.status_code = status_code
        self.detail = detail
        self.debug_message = debug_message

    @property
    def response(self) -> tuple[str, int, dict[str, str]]:
        """Create a Flask-style response."""
        return (
            json(
                self.uri,
                self.status_code,
                self.title,
                self.detail,
                self.debug_message,
            ),
            self.status_code or 400,
            {"Content-Type": JSON_MEDIA_TYPE},
        )

    def detailed(
        self,
        detail: str,
        status_code: int | None = None,
        title: str | None = None,
        debug_message: str | None = None,
    ) -> ProblemDetail:
        """Create a ProblemDetail for a more specific occurrence of an existing
        ProblemDetail.

        The detailed error message will be shown to patrons.
        """

        # Title and detail must be LazyStrings from Flask-Babel that are
        # localized when they are first used as strings.
        if title and not isinstance(title, LazyString):
            logging.warning('"%s" has not been internationalized' % title)
        if detail and not isinstance(detail, LazyString):
            logging.warning('"%s" has not been internationalized' % detail)

        return ProblemDetail(
            self.uri,
            status_code or self.status_code,
            title or self.title,
            detail,
            debug_message,
        )

    def with_debug(
        self,
        debug_message: str,
        detail: str | None = None,
        status_code: int | None = None,
        title: str | None = None,
    ) -> ProblemDetail:
        """Insert debugging information into a ProblemDetail.

        The original ProblemDetail's error message will be shown to
        patrons, but a more specific error message will be visible to
        those who inspect the problem document.
        """
        return ProblemDetail(
            self.uri,
            status_code or self.status_code,
            title or self.title,
            detail or self.detail,
            debug_message,
        )

    def __repr__(self) -> str:
        return "<ProblemDetail(uri={}, title={}, status_code={}, detail={}, debug_message={}".format(
            self.uri,
            self.title,
            self.status_code,
            self.detail,
            self.debug_message,
        )

    def __eq__(self, other: object) -> bool:
        """Compares two ProblemDetail objects.

        :param other: ProblemDetail object
        :return: Boolean value indicating whether two items are equal
        """
        if not isinstance(other, ProblemDetail):
            return False

        return (
            self.uri == other.uri
            and self.title == other.title
            and self.status_code == other.status_code
            and self.detail == other.detail
            and self.debug_message == other.debug_message
        )


class BaseProblemDetailException(BasePalaceException, ABC):
    """Mixin for exceptions that can be converted into a ProblemDetail."""

    @property
    @abstractmethod
    def problem_detail(self) -> ProblemDetail:
        """Convert this object into a ProblemDetail."""
        ...


class ProblemDetailException(BaseProblemDetailException):
    """Exception class allowing to raise and catch ProblemDetail objects."""

    def __init__(self, problem_detail: ProblemDetail) -> None:
        """Initialize a new instance of ProblemError class.

        :param problem_detail: ProblemDetail object
        """
        if not isinstance(problem_detail, ProblemDetail):
            raise ValueError(
                'Argument "problem_detail" must be an instance of ProblemDetail class'
            )
        super().__init__(problem_detail.title)
        self._problem_detail = problem_detail

    @property
    def problem_detail(self) -> ProblemDetail:
        """Return the ProblemDetail object associated with this exception.

        :return: ProblemDetail object associated with this exception
        """
        return self._problem_detail
