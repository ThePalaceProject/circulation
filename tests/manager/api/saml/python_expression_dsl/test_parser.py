import pytest

from palace.manager.api.saml.python_expression_dsl.parser import (
    DSLParseError,
    DSLParser,
)


class TestDSLParser:
    @pytest.mark.parametrize(
        "expression,expected_error_message",
        [
            pytest.param(
                "?", "Unexpected symbol '?' at position 0", id="incorrect_expression"
            ),
            pytest.param(
                "(+", "Unexpected symbol '+' at position 1", id="incorrect_expression_2"
            ),
            pytest.param(
                "(1 +",
                "Unexpected symbol '+' at position 3",
                id="incorrect_expression_3",
            ),
        ],
    )
    def test_parse_generates_correct_error_message(
        self, expression, expected_error_message
    ):
        # Arrange
        parser = DSLParser()

        # Act
        with pytest.raises(DSLParseError) as exception_context:
            parser.parse(expression)

        # Assert
        assert expected_error_message == str(exception_context.value)
