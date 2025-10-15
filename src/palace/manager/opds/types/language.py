from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any, cast, overload

import pycountry
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

from palace.manager.util.log import LoggerMixin


class LanguageTag(str, LoggerMixin):
    """
    Parses a subset of IETF BCP 47 language tags.

    https://datatracker.ietf.org/doc/bcp47/

    The class expects the `primary language` to be a 2-letter or 3-letter ISO 639-2 language
    code. It splits the language tag into its sub-tags, parses the primary language tag,
    validates it's a valid language code, and normalizes it to a 3-letter code.

    This handles the most common cases that we see in feeds we parse.

    TODO: We may want to replace this with a more complete implementation of
      BCP 47. Especially if we are able to find a library that we can leverage.
      I did some research, but at the time of writing, I wasn't able to find one.
    """

    __slots__ = ("_original", "_subtags", "_name")

    _original: str
    _subtags: tuple[str, ...]
    _name: str

    def __new__(
        cls,
        value: str | LanguageTag,
    ) -> LanguageTag:
        if isinstance(value, LanguageTag):
            return value

        if not isinstance(value, str):
            raise ValueError(
                f"Language tag must be a string, got {type(value).__name__}"
            )

        if len(value) == 0:
            raise ValueError("Language tag cannot be empty")

        # First we split the language tag into its sub-tags.
        subtags = tuple([t.lower() for t in value.replace("_", "-").split("-")])
        primary_language = subtags[0]

        # This parser is intentionally lenient. It accepts both 2-letter and 3-letter
        # language codes as well as full language names. While RFC 5646 specifies stricter
        # parsing rules, we've implemented this approach to handle the non-standard codes
        # frequently encountered in our feeds.
        # See: https://datatracker.ietf.org/doc/rfc5646/
        if len(primary_language) == 2:
            code = pycountry.languages.get(alpha_2=primary_language)
        elif len(primary_language) == 3:
            code = pycountry.languages.get(alpha_3=primary_language)
            if code is None:
                # Some languages have two three-letter codes. A bibliographic
                # code and a terminological code. We try the terminological code
                # first, which is stored in alpha_3, and if that fails, we
                # fall back to try the bibliographic code.
                # See: https://en.wikipedia.org/wiki/ISO_639-2#B_and_T_codes
                code = pycountry.languages.get(bibliographic=primary_language)
                if code is not None:
                    cls.logger().warning(
                        f"Using bibliographic code '{primary_language}' for language '{code.name}'. "
                        f"This is not BCP47 compliant, use the terminological code '{code.alpha_3}' instead."
                    )
        else:
            # Fall back to looking up the language by name.
            # TODO: We may want to add a strict mode, that raises an error in this case.
            code = pycountry.languages.get(name=value)
            if code is not None:
                subtags = (code.alpha_3,)
                cls.logger().warning(
                    f"Using language name '{value}' instead of a BCP47 language tag. "
                    f"This is not BCP47 compliant, use the 3-letter code '{code.alpha_3}' instead."
                )

        if code is None:
            raise ValueError(f"Invalid language code '{primary_language}'")

        language_code = super().__new__(cls, code.alpha_3)
        language_code._original = value
        language_code._subtags = subtags
        language_code._name = code.name
        return language_code

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _: type[Any], __: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """
        Return a Pydantic CoreSchema validating a 2-letter or 3-letter ISO 639-2 language code.
        """
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.str_schema(
                serialization=core_schema.plain_serializer_function_ser_schema(
                    lambda t: t.original, when_used="json"
                ),
            ),
        )

    @property
    def original(self) -> str:
        """
        Return the original language tag that was passed to the constructor.
        """
        return self._original

    @property
    def subtags(self) -> tuple[str, ...]:
        """
        Return the sub-tags of the language tag.
        """
        return self._subtags

    @property
    def primary_language(self) -> str:
        """
        Return the primary language of the language tag.
        """
        return self._subtags[0]

    @property
    def code(self) -> str:
        """
        Return the 3-letter ISO 639-2 language code.
        """
        return str(self)

    @property
    def name(self) -> str:
        """
        Return the name of the language.
        """
        return self._name

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self._original}>"


class LanguageMap(Mapping[str, str]):
    """
    A string or map of strings that provides values in multiple languages.

    This is meant to implement the multilingual strings in the RWPM standard.
    https://github.com/readium/webpub-manifest/blob/master/schema/language-map.schema.json
    """

    __slots__ = ("_mapping", "_default_language", "_hash")

    def __init__(self, value: str | Mapping[str, str]) -> None:
        """
        Create a new LanguageMap instance.

        :param value: Either a string or a dictionary with language codes as keys
            and translations as values.
        """
        self._mapping: dict[LanguageTag | None, str] = {}
        self._hash: int | None = None

        if isinstance(value, str):
            self._mapping[None] = value
        else:
            if len(value) == 0:
                raise ValueError("Must provide at least one translation")

            self._mapping = {
                LanguageTag(lang): translation for lang, translation in value.items()
            }
        self._default_language = next(iter(self._mapping.keys()))

    @property
    def default_language(self) -> LanguageTag | None:
        return self._default_language

    @staticmethod
    def _serialize(instance: LanguageMap) -> str | dict[str, str]:
        # If the default language is None, we serialize as a string
        if instance.default_language is None:
            return instance.get()

        # Otherwise, we serialize as a dictionary
        return {
            lang: translation
            for lang, translation in instance.items()
            if lang is not None
        }

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type[Any], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """
        This method validates the LanguageMap type when used in
        Pydantic. It is validated as 'str | dict[str, str]' the value is
        then passed into the class constructor, which does some
        further validation, raising TypeErrors if the input is invalid.
        """
        from_str_dict_schema = core_schema.chain_schema(
            [
                core_schema.union_schema(
                    [
                        core_schema.str_schema(),
                        core_schema.dict_schema(
                            core_schema.str_schema(),
                            core_schema.str_schema(),
                        ),
                    ]
                ),
                core_schema.no_info_plain_validator_function(cls),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=from_str_dict_schema,
            python_schema=core_schema.union_schema(
                [
                    # check if it's an instance first before doing any further work
                    core_schema.is_instance_schema(cls),
                    from_str_dict_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls._serialize, when_used="json"
            ),
        )

    @overload
    def get(self) -> str: ...

    @overload
    def get(self, language: str | None) -> str | None: ...

    @overload
    def get[T](self, language: str | None, default: str | T) -> str | T: ...

    def get[T](
        self, language: str | None = None, default: str | T | None = None
    ) -> str | T | None:
        """
        Return the translation for the specified language.

        :param language: The language code to return the translation for. This can
            be either a 2-letter or 3-letter ISO 639-2 code.
        :param default: The default translation to return if the specified language
            is not found. If not provided, None is returned.

        :return: The translation for the specified language, or the default translation.

        :raises ValueError: If the language code is invalid.
        """
        language_code = LanguageTag(language) if language else self.default_language
        return self._mapping.get(language_code, default)

    def __getitem__(self, language: str | None) -> str:
        translation = self.get(language)
        if translation is None:
            raise KeyError(
                f"Language code '{language}' not found in {self.__class__.__name__}"
            )
        return translation

    def __iter__(self) -> Iterator[str]:
        if None in self._mapping:
            return iter([""])
        return cast(Iterator[str], self._mapping.__iter__())

    def __len__(self) -> int:
        return len(self._mapping)

    def __str__(self) -> str:
        return self.get()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {json.dumps(self._mapping)}>"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, LanguageMap):
            return self._mapping == other._mapping
        if isinstance(other, str):
            return other in set(self.values())
        if isinstance(other, Mapping):
            try:
                return self == LanguageMap(other)
            except ValueError:
                return False
        return NotImplemented

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(frozenset(self.items()))
        return self._hash
