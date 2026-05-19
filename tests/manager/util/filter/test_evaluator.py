from contextlib import nullcontext

import pytest

from palace.manager.util.filter.evaluator import (
    DSLEvaluationError,
    DSLEvaluationVisitor,
    DSLEvaluator,
)
from palace.manager.util.filter.parser import DSLParseError, DSLParser


class _Subject:
    """Fixture object for testing safe-class method call enforcement."""

    def __init__(self, attributes: list):
        self._attributes = attributes

    @property
    def attributes(self):
        return self._attributes

    def get_attribute_value(self, index: int):
        return self._attributes[index]


# Parser limitation: subscript access after a function-call result
# (e.g. s.split('/')[0]) is not yet supported by the grammar. Expressions
# requiring this pattern must restructure their logic until the parser is
# extended.


@pytest.fixture
def evaluator():
    return DSLEvaluator(DSLParser(), DSLEvaluationVisitor())


class TestDSLEvaluator:
    @pytest.mark.parametrize(
        "expression,context,safe_classes,expected,exception",
        [
            # Parse errors
            pytest.param("?", None, None, None, DSLParseError, id="invalid-syntax"),
            # Literals
            pytest.param("9", None, None, 9, None, id="integer-literal"),
            pytest.param("9.5", None, None, 9.5, None, id="float-literal"),
            # Identifiers
            pytest.param(
                "foo", None, None, None, DSLEvaluationError, id="unknown-identifier"
            ),
            pytest.param("foo", {"foo": 9}, None, 9, None, id="known-identifier"),
            # Dot access — objects
            pytest.param(
                "foo.bar",
                {"foo": 9},
                None,
                None,
                DSLEvaluationError,
                id="bad-nested-identifier",
            ),
            # Dot access — dicts (resolves key lookup automatically)
            pytest.param(
                "foo.bar",
                {"foo": {"bar": 9}},
                None,
                9,
                None,
                id="dict-dot-access",
            ),
            pytest.param(
                "foo.bar.baz",
                {"foo": {"bar": {"baz": 9}}},
                None,
                9,
                None,
                id="deeply-nested-dict",
            ),
            pytest.param(
                "foo.bar[0].baz",
                {"foo": {"bar": [{"baz": 9}]}},
                None,
                9,
                None,
                id="dict-with-list-subscript",
            ),
            # Arithmetic
            pytest.param("-9", None, None, -9, None, id="negation"),
            pytest.param("9 + 3", None, None, 12, None, id="addition"),
            pytest.param("9 + 3 + 3", None, None, 15, None, id="addition-three-terms"),
            pytest.param("9 - 3", None, None, 6, None, id="subtraction"),
            pytest.param("9 * 3", None, None, 27, None, id="multiplication"),
            pytest.param("9 / 3", None, None, 3.0, None, id="division"),
            pytest.param("9 ** 2", None, None, 81, None, id="exponentiation"),
            pytest.param(
                "2 ** 3 ** 3",
                None,
                None,
                2**3**3,
                None,
                id="chained-exponentiation",
            ),
            # Comparisons
            pytest.param("9 < 3", None, None, False, None, id="less-than-false"),
            pytest.param("3 < 9", None, None, True, None, id="less-than-true"),
            pytest.param("3 <= 3", None, None, True, None, id="less-than-or-equal"),
            pytest.param("9 > 3", None, None, True, None, id="greater-than"),
            pytest.param("3 >= 2", None, None, True, None, id="greater-than-or-equal"),
            pytest.param("9 == 9", None, None, True, None, id="equality-true"),
            pytest.param("9 == 3", None, None, False, None, id="equality-false"),
            pytest.param("9 != 3", None, None, True, None, id="not-equal"),
            pytest.param(
                "3 in lst", {"lst": [1, 2, 3]}, None, True, None, id="in-operator"
            ),
            # Boolean
            pytest.param("not 9 < 3", None, None, True, None, id="not"),
            pytest.param("9 == 9 and 3 == 3", None, None, True, None, id="conjunction"),
            pytest.param("9 == 3 or 3 == 3", None, None, True, None, id="disjunction"),
            # Subscript
            pytest.param(
                "arr[1] == 12",
                {"arr": [1, 12, 3]},
                None,
                True,
                None,
                id="subscript",
            ),
            # String methods (str is always safe)
            pytest.param(
                "s.upper()",
                {"s": "hello"},
                None,
                "HELLO",
                None,
                id="string-upper",
            ),
            pytest.param(
                "s.lower()",
                {"s": "HELLO"},
                None,
                "hello",
                None,
                id="string-lower",
            ),
            pytest.param(
                "s.removeprefix('/')",
                {"s": "/path"},
                None,
                "path",
                None,
                id="string-removeprefix",
            ),
            # Dict methods (dict is always safe — new in this module)
            pytest.param(
                "d.get('key')",
                {"d": {"key": "value"}},
                None,
                "value",
                None,
                id="dict-get-existing-key",
            ),
            pytest.param(
                "d.get('missing', 'default')",
                {"d": {}},
                None,
                "default",
                None,
                id="dict-get-missing-key-with-default",
            ),
            pytest.param(
                "len(d.keys())",
                {"d": {"a": 1, "b": 2}},
                None,
                2,
                None,
                id="dict-keys-len",
            ),
            # Builtin functions
            pytest.param("min(1, 2)", None, None, 1, None, id="builtin-min"),
            pytest.param("max(1, 2)", None, None, 2, None, id="builtin-max"),
            pytest.param("len('abc')", None, None, 3, None, id="builtin-len"),
            pytest.param("abs(-5)", None, None, 5, None, id="builtin-abs"),
            # Safe / unsafe class method calls
            pytest.param(
                "subject.get_attribute_value(0)",
                {"subject": _Subject(["eresources"])},
                None,
                None,
                DSLEvaluationError,
                id="unsafe-class-method-call",
            ),
            pytest.param(
                "subject.get_attribute_value(0)",
                {"subject": _Subject(["eresources"])},
                [_Subject],
                "eresources",
                None,
                id="safe-class-method-call",
            ),
            # in operator with object attribute
            pytest.param(
                "'eresources' in subject.attributes",
                {"subject": _Subject(["eresources"])},
                None,
                True,
                None,
                id="in-operator-object-attribute",
            ),
        ],
    )
    def test_evaluate(
        self, evaluator, expression, context, safe_classes, expected, exception
    ):
        ctx = nullcontext() if exception is None else pytest.raises(exception)

        with ctx:
            result = evaluator.evaluate(
                expression, context, safe_classes if safe_classes is not None else []
            )

        if exception is None:
            assert result == expected
