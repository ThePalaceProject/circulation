from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any, TypeVar, cast, overload

import pycountry
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema


class LanguageTag(str):
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

    __slots__ = ("_original", "_subtags")

    _original: str
    _subtags: tuple[str, ...]

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

        code = None
        if len(primary_language) == 2:
            code = pycountry.languages.get(alpha_2=primary_language)
        elif len(primary_language) == 3:
            code = pycountry.languages.get(alpha_3=primary_language)

        if code is None:
            raise ValueError(f"Invalid language code '{primary_language}'")

        language_code = super().__new__(cls, code.alpha_3)
        language_code._original = value
        language_code._subtags = subtags
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

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self._original}>"


T = TypeVar("T")


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
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.union_schema(
                [
                    core_schema.str_schema(),
                    core_schema.dict_schema(
                        core_schema.str_schema(),
                        core_schema.str_schema(),
                    ),
                ],
                serialization=core_schema.plain_serializer_function_ser_schema(
                    cls._serialize, when_used="json"
                ),
            ),
        )

    @overload
    def get(self) -> str: ...

    @overload
    def get(self, language: str | None) -> str | None: ...

    @overload
    def get(self, language: str | None, default: str | T) -> str | T: ...

    def get(
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
