from contextlib import nullcontext
from dataclasses import dataclass, field

import pytest

from palace.manager.util.filter import (
    FilterExpression,
    FilterExpressionError,
)


@dataclass(frozen=True)
class _Library:
    """Fixture dataclass for testing extra_safe_types."""

    short_name: str
    name: str
    entitlements: tuple[str, ...] = field(default_factory=tuple)

    def display(self) -> str:
        return f"{self.short_name}: {self.name}"

    @staticmethod
    def code() -> str:
        return "lib"

    @classmethod
    def category(cls) -> str:
        return "library"


class TestFilterExpression:
    # ------------------------------------------------------------------ check_syntax

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
    def test_check_syntax(self, expression, raises):
        fe = FilterExpression(expression)
        ctx = pytest.raises(FilterExpressionError) if raises else nullcontext()

        with ctx:
            fe.check_syntax()

    def test_check_syntax_passes_non_bool_expression(self):
        """check_syntax() accepts a non-bool expression that evaluate() rejects."""
        fe = FilterExpression("1 + 1")
        fe.check_syntax()  # no exception — syntax is valid
        with pytest.raises(FilterExpressionError):
            fe.evaluate({})

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
                "lookup.get(claim.dept) == 'bchs'",
                {"claim": {"dept": "bigcityhs"}, "lookup": {"bigcityhs": "bchs"}},
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
            # Subscript access on a nested dict
            pytest.param(
                "claim.attributes.entitlements[0] == 'eresources'",
                {"claim": {"attributes": {"entitlements": ["eresources"]}}},
                None,
                True,
                False,
                id="nested-dict-subscript",
            ),
            # Arithmetic
            pytest.param(
                "1 + 2 == 3", {}, None, True, False, id="arithmetic-in-comparison"
            ),
            pytest.param("-x > 0", {"x": -5}, None, True, False, id="negation"),
            # String methods (str is always safe)
            pytest.param(
                "s.upper() == 'HELLO'",
                {"s": "hello"},
                None,
                True,
                False,
                id="string-upper",
            ),
            pytest.param(
                "s.removeprefix('/') == 'path'",
                {"s": "/path"},
                None,
                True,
                False,
                id="string-removeprefix",
            ),
            # Subscript after a function-call result
            pytest.param(
                "s.split('/')[0] == 'a'",
                {"s": "a/b"},
                None,
                True,
                False,
                id="subscript-after-function-call",
            ),
            # Dict .get() with a default value
            pytest.param(
                "d.get('missing', 'default') == 'default'",
                {"d": {}},
                None,
                True,
                False,
                id="dict-get-with-default",
            ),
            # Dict .keys() combined with len()
            pytest.param(
                "len(d.keys()) == 2",
                {"d": {"a": 1, "b": 2}},
                None,
                True,
                False,
                id="dict-keys-len",
            ),
            # List read-only methods — index and count are allowed
            pytest.param(
                "items.index('b') == 1",
                {"items": ["a", "b", "c"]},
                None,
                True,
                False,
                id="list-index-method",
            ),
            pytest.param(
                "items.count('a') == 2",
                {"items": ["a", "b", "a"]},
                None,
                True,
                False,
                id="list-count-method",
            ),
            # Mutating methods are blocked for all types
            pytest.param(
                "items.append('d')",
                {"items": ["a", "b"]},
                None,
                None,
                True,
                id="list-append-raises",
            ),
            pytest.param(
                "d.update({'k': 'v'})",
                {"d": {"a": 1}},
                None,
                None,
                True,
                id="dict-update-raises",
            ),
            # Dunder attributes are blocked (simpleeval DISALLOW_PREFIXES guard)
            pytest.param(
                "items.__setitem__(0, 'evil')",
                {"items": ["a", "b"]},
                None,
                None,
                True,
                id="dunder-setitem-blocked",
            ),
            pytest.param(
                "d.__setitem__('k', 'v')",
                {"d": {"a": 1}},
                None,
                None,
                True,
                id="dict-dunder-setitem-blocked",
            ),
            # Builtin functions
            pytest.param(
                "min(x, y) == 1", {"x": 1, "y": 5}, None, True, False, id="builtin-min"
            ),
            pytest.param(
                "max(x, y) == 5", {"x": 1, "y": 5}, None, True, False, id="builtin-max"
            ),
            pytest.param("abs(x) == 5", {"x": -5}, None, True, False, id="builtin-abs"),
            # in operator against a non-dict object attribute
            pytest.param(
                "'eresources' in library.entitlements",
                {
                    "library": _Library(
                        short_name="bchs",
                        name="BigCity High",
                        entitlements=("eresources", "journals"),
                    )
                },
                None,
                True,
                False,
                id="in-operator-object-attribute",
            ),
            # extra_safe_types — custom type allowed
            pytest.param(
                "library.short_name == 'bchs'",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                [_Library],
                True,
                False,
                id="extra-safe-type-attribute-access",
            ),
            # extra_safe_types — calling a method on an unlisted type raises.
            pytest.param(
                "library.display()",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                None,
                None,
                True,
                id="unsafe-type-method-call-raises",
            ),
            pytest.param(
                "library.display() == 'bchs: BigCity High'",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                [_Library],
                True,
                False,
                id="safe-type-method-call-succeeds",
            ),
            # Static method: blocked when type not in safe_types, allowed when listed.
            pytest.param(
                "library.code()",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                None,
                None,
                True,
                id="unsafe-type-static-method-raises",
            ),
            pytest.param(
                "library.code() == 'lib'",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                [_Library],
                True,
                False,
                id="safe-type-static-method-succeeds",
            ),
            # Classmethod: blocked when type not in safe_types, allowed when listed.
            pytest.param(
                "library.category()",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                None,
                None,
                True,
                id="unsafe-type-classmethod-raises",
            ),
            pytest.param(
                "library.category() == 'library'",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                [_Library],
                True,
                False,
                id="safe-type-classmethod-succeeds",
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

    @pytest.mark.parametrize(
        "expression,context",
        [
            pytest.param("x", {"x": 1}, id="int"),
            pytest.param("x", {"x": "hello"}, id="str"),
            pytest.param("x", {"x": None}, id="none"),
        ],
    )
    def test_evaluate_raises_for_non_bool_result(self, expression, context):
        """evaluate() raises FilterExpressionError when result is not a bool."""
        fe = FilterExpression(expression)
        with pytest.raises(FilterExpressionError):
            fe.evaluate(context)

    @pytest.mark.parametrize(
        "expression,context,missing_attribute_returns_false,expected,raises",
        [
            # Default (False): missing dict sub-key raises.
            pytest.param(
                "claim.missing == 'value'",
                {"claim": {"ou": "student"}},
                False,
                None,
                True,
                id="missing-dict-subkey-raises",
            ),
            # True: missing dict sub-key returns False from the expression.
            pytest.param(
                "claim.missing == 'value'",
                {"claim": {"ou": "student"}},
                True,
                False,
                False,
                id="missing-dict-subkey-returns-false",
            ),
            # Default (False): missing object attribute raises.
            pytest.param(
                "library.nonexistent == 'value'",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                False,
                None,
                True,
                id="missing-object-attr-raises",
            ),
            # True: missing object attribute returns False from the expression.
            pytest.param(
                "library.nonexistent == 'value'",
                {"library": _Library(short_name="bchs", name="BigCity High")},
                True,
                False,
                False,
                id="missing-object-attr-returns-false",
            ),
        ],
    )
    def test_evaluate_missing_attribute(
        self,
        expression,
        context,
        missing_attribute_returns_false,
        expected,
        raises,
    ):
        fe = FilterExpression(
            expression,
            missing_attribute_returns_false=missing_attribute_returns_false,
        )
        ctx = pytest.raises(FilterExpressionError) if raises else nullcontext()
        with ctx:
            result = fe.evaluate(context)
        if not raises:
            assert result == expected

    def test_evaluate_name_not_defined_message(self):
        """NameNotDefined error message includes the undefined name."""
        fe = FilterExpression("missing_name == 1")
        with pytest.raises(FilterExpressionError, match="'missing_name'"):
            fe.evaluate({"x": 1, "y": 2})
