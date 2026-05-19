from contextlib import nullcontext
from dataclasses import dataclass

import pytest

from palace.manager.util.filter.expression import (
    FilterExpression,
    FilterExpressionError,
)


@dataclass(frozen=True)
class _Library:
    """Fixture dataclass for testing extra_safe_types."""

    short_name: str
    name: str

    def display(self) -> str:
        return f"{self.short_name}: {self.name}"


class TestFilterExpression:
    # ------------------------------------------------------------------ validate

    @pytest.mark.parametrize(
        "expression,raises",
        [
            pytest.param("x == 1", False, id="valid-comparison"),
            pytest.param("x and y", False, id="valid-boolean"),
            pytest.param(
                "claim.ou.lower() == 'student'", False, id="valid-method-chain"
            ),
            pytest.param("?", True, id="invalid-symbol"),
            pytest.param("(1 +", True, id="incomplete-expression"),
        ],
    )
    def test_validate(self, expression, raises):
        fe = FilterExpression(expression)
        ctx = pytest.raises(FilterExpressionError) if raises else nullcontext()

        with ctx:
            fe.validate()

    # ------------------------------------------------------------------ evaluate

    @pytest.mark.parametrize(
        "expression,context,extra_safe_types,expected,raises",
        [
            # Basic comparisons
            pytest.param("x == 1", {"x": 1}, None, True, False, id="equality-true"),
            pytest.param("x == 2", {"x": 1}, None, False, False, id="equality-false"),
            pytest.param("x > 0", {"x": 5}, None, True, False, id="greater-than"),
            # Boolean operators
            pytest.param(
                "x == 1 and y == 2",
                {"x": 1, "y": 2},
                None,
                True,
                False,
                id="conjunction-true",
            ),
            pytest.param(
                "x == 1 and y == 3",
                {"x": 1, "y": 2},
                None,
                False,
                False,
                id="conjunction-false",
            ),
            pytest.param(
                "x == 9 or y == 2",
                {"x": 1, "y": 2},
                None,
                True,
                False,
                id="disjunction-true",
            ),
            # Dict dot-access (claim-style context)
            pytest.param(
                "claim.ou == 'student'",
                {"claim": {"ou": "student"}},
                None,
                True,
                False,
                id="dict-dot-access-equality",
            ),
            pytest.param(
                "claim.ou.lower() == 'student'",
                {"claim": {"ou": "STUDENT"}},
                None,
                True,
                False,
                id="dict-dot-access-string-method",
            ),
            # Dict .get() method
            pytest.param(
                "lookup.get(claim.dept) == 'bhs'",
                {"claim": {"dept": "bartletths"}, "lookup": {"bartletths": "bhs"}},
                None,
                True,
                False,
                id="dict-get-lookup-table",
            ),
            # in operator
            pytest.param(
                "'eresources' in claim.entitlements",
                {"claim": {"entitlements": ["eresources", "journals"]}},
                None,
                True,
                False,
                id="in-operator-list",
            ),
            # Subscript (SAML-style attribute access)
            pytest.param(
                "claim.attributes.entitlements[0] == 'eresources'",
                {"claim": {"attributes": {"entitlements": ["eresources"]}}},
                None,
                True,
                False,
                id="saml-like-attribute-subscript",
            ),
            # extra_safe_types — custom type allowed
            pytest.param(
                "library.short_name == 'bhs'",
                {"library": _Library(short_name="bhs", name="Bartlett High")},
                [_Library],
                True,
                False,
                id="extra-safe-type-attribute-access",
            ),
            # extra_safe_types — calling a method on an unlisted type raises.
            # Note: plain attribute reads (library.short_name) are always allowed;
            # safe_types only gates bound method *calls*.
            pytest.param(
                "library.display()",
                {"library": _Library(short_name="bhs", name="Bartlett High")},
                None,
                None,
                True,
                id="unsafe-type-method-call-raises",
            ),
            pytest.param(
                "library.display()",
                {"library": _Library(short_name="bhs", name="Bartlett High")},
                [_Library],
                True,
                False,
                id="safe-type-method-call-succeeds",
            ),
            # Errors surface as FilterExpressionError
            pytest.param(
                "?",
                {},
                None,
                None,
                True,
                id="syntax-error-raises-filter-expression-error",
            ),
            pytest.param(
                "unknown_var",
                {},
                None,
                None,
                True,
                id="missing-context-key-raises",
            ),
        ],
    )
    def test_evaluate(self, expression, context, extra_safe_types, expected, raises):
        fe = FilterExpression(expression, extra_safe_types=extra_safe_types)
        ctx = pytest.raises(FilterExpressionError) if raises else nullcontext()

        with ctx:
            result = fe.evaluate(context)

        if not raises:
            assert result == expected

    def test_evaluate_always_returns_bool(self):
        """evaluate() coerces non-bool truthy/falsy values to bool."""
        fe = FilterExpression("x")
        assert fe.evaluate({"x": 1}) is True
        assert fe.evaluate({"x": 0}) is False
        assert fe.evaluate({"x": "hello"}) is True
        assert fe.evaluate({"x": ""}) is False

    def test_evaluate_raises_filter_expression_error_not_dsl_error(self):
        """Errors are wrapped in FilterExpressionError, not leaking DSL internals."""
        fe = FilterExpression("?")
        with pytest.raises(FilterExpressionError):
            fe.evaluate({})

    def test_validate_raises_filter_expression_error_not_dsl_error(self):
        """validate() errors are wrapped in FilterExpressionError."""
        fe = FilterExpression("(1 +")
        with pytest.raises(FilterExpressionError):
            fe.validate()
