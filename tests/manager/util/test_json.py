import json
from functools import partial

import pytest

from palace.manager.util.json import json_canonical, json_hash


def test_json_canonical() -> None:
    dumps = partial(json.dumps, separators=(",", ":"))

    # Lists are sorted
    assert json_canonical([3, 2, 1]) == dumps((1, 2, 3))
    assert json_canonical([1, 10, 100, 12]) == dumps((1, 10, 12, 100))
    assert json_canonical(["b", "a", "c"]) == dumps(("a", "b", "c"))
    assert json_canonical([True, False, True]) == dumps((False, True, True))
    assert json_canonical((3, 2, 1)) == dumps((1, 2, 3))

    # Super contrived example, but tests complex nested structures and type comparison
    data = [
        [1, 2, 4],
        (3, 2, 1),
        2,
        1,
        3,
        {
            1: {
                None: [1, "a", True, None, 65, 56.4, {"a": "b"}, 0.1000002],
                "3": "f",
                2: [3, 2, 1],
            }
        },
    ]

    assert json_canonical(data, sort_sequences=True) == dumps(
        (
            1,
            2,
            3,
            (1, 2, 3),
            (1, 2, 4),
            {
                1: {
                    2: (1, 2, 3),
                    "3": "f",
                    None: (True, 0.1, 1, 56.4, 65, "a", {"a": "b"}, None),
                },
            },
        )
    )

    assert json_canonical(data, sort_sequences=False) == dumps(
        (
            (1, 2, 4),
            (3, 2, 1),
            2,
            1,
            3,
            {
                1: {
                    2: (3, 2, 1),
                    "3": "f",
                    None: (1, "a", True, None, 65, 56.4, {"a": "b"}, 0.1),
                }
            },
        )
    )

    # Nested lists are sorted
    assert json_canonical([[3, 2], [1]]) == dumps([(1,), (2, 3)])

    # Dicts are sorted by key, and values are canonicalized
    assert json_canonical({"b": [3, 2], "a": {"d": 4, "c": 3}}) == dumps(
        {
            "a": {"c": 3, "d": 4},
            "b": (2, 3),
        }
    )

    # Mixed structures are handled correctly
    assert json_canonical(
        {"b": [{"y": 2, "x": 1}, 3, True, False], "a": {"d": 4, "c": [3, 2]}}
    ) == dumps({"a": {"c": (2, 3), "d": 4}, "b": (False, True, 3, {"x": 1, "y": 2})})

    assert json_canonical(
        [{"b": [3, 2], "a": {"d": 4, "c": 3}}, {"b": [1, 2], "a": {"d": 1, "c": 2}}]
    ) == dumps(
        (
            {"a": {"c": 2, "d": 1}, "b": (1, 2)},
            {"a": {"c": 3, "d": 4}, "b": (2, 3)},
        )
    )

    assert json_canonical(0.1 + 0.2) == json_canonical(0.3)

    assert json_canonical(0.1 + 0.2, round_float=False) != json_canonical(
        0.3, round_float=False
    )

    assert json_canonical(
        [
            {"a": 1, "b": 2, "c": 4},
            {"c": 3, "a": 1, "b": 2, "d": 4},
        ]
    ) == dumps(
        [
            {"a": 1, "b": 2, "c": 3, "d": 4},
            {"a": 1, "b": 2, "c": 4},
        ]
    )

    # Multiple None values in a sequence must not crash (None is not comparable).
    # None has the highest precedence (5 = last), so it sorts after ints and strings.
    assert json_canonical([None, None, None]) == dumps([None, None, None])
    assert json_canonical([None, 1, None, "a", None]) == dumps(
        [1, "a", None, None, None]
    )


def test_json_canonical_unsupported_type_raises() -> None:
    """Passing an unsupported type inside a sequence should raise TypeError.

    Unsupported types are detected by ``_canonicalize_sort_key`` when it is
    called during sorting; they are not detected at the top level (which would
    just produce a json.dumps error).  Wrapping the value in a list triggers
    the sort path and therefore our custom error message.
    """
    with pytest.raises(TypeError, match="Unsupported type for canonicalization"):
        json_canonical([{1, 2, 3}])

    with pytest.raises(TypeError, match="Unsupported type for canonicalization"):
        json_canonical([1, object()])


def test_json_hash() -> None:
    """json_hash should be deterministic and sensitive to content differences."""
    # Same content always produces the same hash.
    assert json_hash({"a": 1, "b": 2}) == json_hash({"b": 2, "a": 1})
    assert json_hash([3, 2, 1]) == json_hash([1, 2, 3])

    # Different content produces different hashes.
    assert json_hash({"a": 1}) != json_hash({"a": 2})
    assert json_hash([1, 2]) != json_hash([1, 2, 3])

    # The result is a 64-character lowercase hex string (SHA-256).
    digest = json_hash("hello")
    assert len(digest) == 64
    assert digest == digest.lower()
    assert all(c in "0123456789abcdef" for c in digest)

    # Floats that are equal up to float_precision round to the same hash.
    assert json_hash(0.1 + 0.2) == json_hash(0.3)
    assert json_hash(0.1 + 0.2, round_float=False) != json_hash(0.3, round_float=False)
