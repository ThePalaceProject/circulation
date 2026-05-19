import pytest

from palace.manager.util.filter.parser import DSLParseError, DSLParser


class TestDSLParser:
    @pytest.mark.parametrize(
        "expression,expected_error",
        [
            pytest.param(
                "?", "Unexpected symbol '?' at position 0", id="unknown-symbol"
            ),
            pytest.param(
                "(+", "Unexpected symbol '+' at position 1", id="unexpected-operator"
            ),
            pytest.param(
                "(1 +",
                "Unexpected symbol '+' at position 3",
                id="incomplete-expression",
            ),
        ],
    )
    def test_parse_error_message(self, expression, expected_error):
        parser = DSLParser()

        with pytest.raises(DSLParseError) as exc_info:
            parser.parse(expression)

        assert expected_error == str(exc_info.value)

    @pytest.mark.parametrize(
        "expression",
        [
            pytest.param("42", id="integer-literal"),
            pytest.param("3.14", id="float-literal"),
            pytest.param("'hello'", id="single-quoted-string"),
            pytest.param('"hello"', id="double-quoted-string"),
            pytest.param("x", id="identifier"),
            pytest.param("x.y", id="dot-access"),
            pytest.param("x.y.z", id="chained-dot-access"),
            pytest.param("arr[0]", id="subscript"),
            pytest.param("x.y[0].z", id="mixed-dot-and-subscript"),
            pytest.param("fn()", id="function-call-no-args"),
            pytest.param("fn(1, 2)", id="function-call-with-args"),
            pytest.param("x.method()", id="method-call"),
            pytest.param("x.method('arg')", id="method-call-with-string-arg"),
            pytest.param("-x", id="unary-negation"),
            pytest.param("x + y", id="addition"),
            pytest.param("x - y", id="subtraction"),
            pytest.param("x * y", id="multiplication"),
            pytest.param("x / y", id="division"),
            pytest.param("x ** y", id="exponentiation"),
            pytest.param("x == y", id="equality"),
            pytest.param("x != y", id="inequality"),
            pytest.param("x < y", id="less-than"),
            pytest.param("x <= y", id="less-than-or-equal"),
            pytest.param("x > y", id="greater-than"),
            pytest.param("x >= y", id="greater-than-or-equal"),
            pytest.param("x in y", id="in-operator"),
            pytest.param("not x", id="boolean-not"),
            pytest.param("x and y", id="boolean-and"),
            pytest.param("x or y", id="boolean-or"),
            pytest.param("(x + y) * z", id="parenthesized-expression"),
            # Note: subscript after a function-call result is not yet supported
            # by the grammar, so s.split('/')[0] style expressions are invalid.
        ],
    )
    def test_valid_expression_parses(self, expression):
        parser = DSLParser()
        node = parser.parse(expression)
        assert node is not None
