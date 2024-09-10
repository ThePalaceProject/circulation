import json
import re
import string

import pytest

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.service.redis.escape import JsonPathEscapeMixin


class TestPathEscapeMixin:
    @pytest.mark.parametrize(
        "path",
        [
            "",
            "test",
            string.printable,
            "test/test1/?$xyz.abc",
            "`",
            "```",
            "/~`\\",
            "`\\~/``/",
            "a",
            "/",
            "~",
            " ",
            '"',
            "💣ü",
        ],
    )
    def test_escape_path(self, path: str) -> None:
        # Test a round trip
        escaper = JsonPathEscapeMixin()
        escaped = escaper._escape_path(path)
        unescaped = escaper._unescape_path(escaped)
        assert unescaped == path

        # Test a round trip with ElastiCache escaping. The json.loads is done implicitly by ElastiCache,
        # when using these strings in a JsonPath query. We add a json.loads here to simulate that.
        escaped = escaper._escape_path(path, elasticache=True)
        unescaped = escaper._unescape_path(json.loads(f'"{escaped}"'))
        assert unescaped == path

        # Test that we can handle escaping the escaped path multiple times
        escaped = path
        for _ in range(10):
            escaped = escaper._escape_path(escaped)

        unescaped = escaped
        for _ in range(10):
            unescaped = escaper._unescape_path(unescaped)

        assert unescaped == path

    def test_unescape(self) -> None:
        escaper = JsonPathEscapeMixin()
        assert escaper._unescape_path("") == ""

        with pytest.raises(
            PalaceValueError, match=re.escape("Invalid escape sequence '`?'")
        ):
            escaper._unescape_path("test `?")

        with pytest.raises(
            PalaceValueError, match=re.escape("Invalid escape sequence '` '")
        ):
            escaper._unescape_path("``` test")

        with pytest.raises(
            PalaceValueError, match=re.escape("Unterminated escape sequence")
        ):
            escaper._unescape_path("`")
