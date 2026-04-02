import json
from functools import partial

from palace.manager.util.json import json_canonical


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
