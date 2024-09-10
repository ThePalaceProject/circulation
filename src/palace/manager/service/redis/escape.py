from __future__ import annotations

import json
from functools import cached_property

from palace.manager.core.exceptions import PalaceValueError


class JsonPathEscapeMixin:
    r"""
    Mixin to provide methods for escaping and unescaping JsonPaths for use in Redis / ElastiCache.

    This is necessary because some characters in object keys are not handled well by AWS ElastiCache,
    and other characters seem problematic in Redis.

    This mixin provides methods to escape and unescape these characters, so that they can be used in
    object keys, and the keys can be queried via JSONPath without issue.

    In ElastiCache when ~ is used in a key, the key is never updated, despite returning a success. And
    when a / is used in a key, the key is interpreted as a nested path, nesting a new key for every
    slash in the path. This is not the behavior we want, so we need to escape these characters.

    In Redis, the \ character is used as an escape character, and the " character is used to denote
    the end of a string for the JSONPath. This means that these characters need to be escaped as well.

    Characters are escaped by prefixing them with a backtick character, followed by a single character
    from _MAPPING that represents the escaped character. The backtick character itself is escaped by
    prefixing it with another backtick character.
    """

    _ESCAPE_CHAR = "`"

    _MAPPING = {
        "/": "s",
        "\\": "b",
        '"': "'",
        "~": "t",
    }

    @cached_property
    def _FORWARD_MAPPING(self) -> dict[str, str]:
        mapping = {k: "".join((self._ESCAPE_CHAR, v)) for k, v in self._MAPPING.items()}
        mapping[self._ESCAPE_CHAR] = "".join((self._ESCAPE_CHAR, self._ESCAPE_CHAR))
        return mapping

    @cached_property
    def _REVERSE_MAPPING(self) -> dict[str, str]:
        mapping = {v: k for k, v in self._MAPPING.items()}
        mapping[self._ESCAPE_CHAR] = self._ESCAPE_CHAR
        return mapping

    def _escape_path(self, path: str, elasticache: bool = False) -> str:
        escaped = "".join([self._FORWARD_MAPPING.get(c, c) for c in path])
        if elasticache:
            # As well as the simple escaping we have defined here, for ElastiCache we need to fully
            # escape the path as if it were a JSON string. So we call json.dumps to do this. We
            # strip the leading and trailing quotes from the result, as we only want the escaped
            # string, not the quotes.
            escaped = json.dumps(escaped)[1:-1]
        return escaped

    def _unescape_path(self, path: str) -> str:
        in_escape = False
        unescaped = []
        for char in path:
            if in_escape:
                if char not in self._REVERSE_MAPPING:
                    raise PalaceValueError(
                        f"Invalid escape sequence '{self._ESCAPE_CHAR}{char}'"
                    )
                unescaped.append(self._REVERSE_MAPPING[char])
                in_escape = False
            else:
                if char == self._ESCAPE_CHAR:
                    in_escape = True
                else:
                    unescaped.append(char)

        if in_escape:
            raise PalaceValueError("Unterminated escape sequence.")

        return "".join(unescaped)
