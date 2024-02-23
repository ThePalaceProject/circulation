import logging

from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLNameID,
    SAMLSubject,
)
from core.exceptions import BasePalaceException
from core.python_expression_dsl.evaluator import DSLEvaluator


class SAMLSubjectFilterError(BasePalaceException):
    """Raised in the case of any errors during execution of a filter expression."""

    def __init__(self, inner_exception: Exception) -> None:
        """Initialize a new instance of SAMLSubjectFilterError class."""
        message = f"Incorrect filter expression: {str(inner_exception)}"

        super().__init__(message)


class SAMLSubjectFilter:
    """Executes filter expressions."""

    def __init__(self, dsl_evaluator):
        """Initialize a new instance of SAMLSubjectFilter class.

        :param dsl_evaluator: DSL evaluator
        :type dsl_evaluator: core.python_expression_dsl.evaluator.DSLEvaluator
        """
        if not isinstance(dsl_evaluator, DSLEvaluator):
            raise ValueError(
                "Argument 'dsl_evaluator' must be an instance of {} class".format(
                    DSLEvaluator
                )
            )

        self._dsl_evaluator = dsl_evaluator
        self._logger = logging.getLogger(__name__)

    def execute(self, expression, subject):
        """Apply the expression to the subject and return a boolean value indicating whether it's a valid subject.

        :param expression: String containing the filter expression
        :type expression: str

        :param subject: SAML subject
        :type subject: api.saml.metadata.model.SAMLSubject

        :return: Boolean value indicating whether it's a valid subject
        :rtype: bool

        :raise SAMLSubjectFilterError: in the case of any errors occurred during expression evaluation
        """
        if not expression or not isinstance(expression, str):
            raise ValueError("Argument 'expression' must be a non-empty string")
        if not isinstance(subject, SAMLSubject):
            raise ValueError("Argument 'subject' must an instance of Subject class")

        self._logger.info(f"Started applying expression '{expression}' to {subject}")

        try:
            result = self._dsl_evaluator.evaluate(
                expression,
                context={"subject": subject},
                safe_classes=[
                    SAMLSubject,
                    SAMLNameID,
                    SAMLAttributeStatement,
                    SAMLAttribute,
                ],
            )
        except Exception as exception:
            raise SAMLSubjectFilterError(exception) from exception

        self._logger.info(
            "Finished applying expression '{}' to {}: {}".format(
                expression, subject, result
            )
        )

        result = bool(result)

        return result

    def validate(self, expression):
        """Validate the filter expression.

        Try to apply the expression to a dummy Subject object containing all the known SAML attributes.

        :param expression: String containing the filter expression
        :type expression: str

        :raise: SAMLSubjectFilterError
        """
        if not expression or not isinstance(expression, str):
            raise ValueError("Argument 'expression' must be a non-empty string")

        try:
            self._dsl_evaluator.parser.parse(expression)
        except Exception as exception:
            raise SAMLSubjectFilterError(exception) from exception
