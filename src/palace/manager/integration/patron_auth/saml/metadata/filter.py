from __future__ import annotations

import logging

from palace.util.exceptions import BasePalaceException

from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLNameID,
    SAMLSubject,
)
from palace.manager.util.filter import FilterExpression, FilterExpressionError


class SAMLSubjectFilterError(BasePalaceException):
    """Raised in the case of any errors during execution of a filter expression."""

    def __init__(self, inner_exception: Exception) -> None:
        super().__init__(f"Incorrect filter expression: {str(inner_exception)}")


class SAMLSubjectFilter:
    """Executes filter expressions against SAML subjects using FilterExpression."""

    _SAFE_TYPES: frozenset[type] = frozenset(
        {SAMLSubject, SAMLNameID, SAMLAttributeStatement, SAMLAttribute}
    )

    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def execute(self, expression: str, subject: SAMLSubject) -> bool:
        """Apply the expression to the subject and return whether it passes.

        :param expression: String containing the filter expression
        :param subject: SAML subject
        :raises SAMLSubjectFilterError: on any evaluation error
        """
        if not expression or not isinstance(expression, str):
            raise ValueError("Argument 'expression' must be a non-empty string")
        if not isinstance(subject, SAMLSubject):
            raise ValueError("Argument 'subject' must an instance of Subject class")

        self._logger.info(f"Started applying expression '{expression}' to {subject}")

        try:
            result = FilterExpression(
                expression, extra_safe_types=list(self._SAFE_TYPES)
            ).evaluate({"subject": subject})
        except FilterExpressionError as exc:
            raise SAMLSubjectFilterError(exc) from exc

        self._logger.info(
            f"Finished applying expression '{expression}' to {subject}: {result}"
        )
        return result

    def validate(self, expression: str) -> None:
        """Validate the filter expression by checking its syntax.

        Note: only syntax is checked; names used in the expression are not
        verified against the evaluation context. An expression that references
        undefined names will pass this check but raise at evaluation time.

        :param expression: String containing the filter expression
        :raises SAMLSubjectFilterError: on any syntax error
        """
        if not expression or not isinstance(expression, str):
            raise ValueError("Argument 'expression' must be a non-empty string")

        try:
            FilterExpression(expression).check_syntax()
        except FilterExpressionError as exc:
            raise SAMLSubjectFilterError(exc) from exc
