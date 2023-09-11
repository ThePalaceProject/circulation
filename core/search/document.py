from abc import ABC, abstractmethod
from typing import Dict


class SearchMappingFieldType(ABC):
    """
    The type of field types. Subclasses of this class implement the serialization
    behaviour for specific types.

    https://opensearch.org/docs/latest/field-types/supported-field-types/index/
    """

    @abstractmethod
    def serialize(self) -> dict:
        pass


class SearchMappingFieldTypeScalar(SearchMappingFieldType):
    """
    A scalar field type such as "boolean", "long", "integer", etc.

    See: https://opensearch.org/docs/latest/field-types/supported-field-types/index/
    """

    def __init__(self, name: str):
        self._name = name

    def serialize(self) -> dict:
        return {"type": self._name}


# See: https://opensearch.org/docs/latest/field-types/supported-field-types/binary/
BINARY: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("binary")

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/boolean/
BOOLEAN: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("boolean")

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
BYTE: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("byte")

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
DOUBLE: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("double")

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
FLOAT: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("float")

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
HALF_FLOAT: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("half_float")

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
INTEGER: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("integer")

# See: https://opensearch.org/docs/latest/opensearch/supported-field-types/ip/
IP: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("ip")

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
LONG: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("long")

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/unsigned-long/
UNSIGNED_LONG: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar(
    "unsigned_long"
)

# See: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
SHORT: SearchMappingFieldTypeScalar = SearchMappingFieldTypeScalar("short")


class SearchMappingFieldTypeParameterized(SearchMappingFieldType):
    """The base class for types that have parameters (date, keyword, etc)"""

    _parameters: Dict[str, str]

    def __init__(self, name: str):
        self._name = name
        self._parameters = {}

    @property
    def parameters(self) -> Dict[str, str]:
        return self._parameters

    def serialize(self) -> dict:
        output = dict(self._parameters)
        output["type"] = self._name
        return output


# See: https://opensearch.org/docs/latest/field-types/supported-field-types/date/
def date() -> SearchMappingFieldTypeParameterized:
    return SearchMappingFieldTypeParameterized("date")


# See: https://opensearch.org/docs/latest/field-types/supported-field-types/keyword/
def keyword() -> SearchMappingFieldTypeParameterized:
    mapping = SearchMappingFieldTypeParameterized("keyword")
    mapping.parameters["normalizer"] = "filterable_string"
    return mapping


# See: https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu-collation-keyword-field.html
def icu_collation_keyword() -> SearchMappingFieldTypeParameterized:
    return SearchMappingFieldTypeParameterized("icu_collation_keyword")


def sort_author_keyword() -> SearchMappingFieldTypeParameterized:
    t = SearchMappingFieldTypeParameterized("text")
    t.parameters["analyzer"] = "en_sort_author_analyzer"
    t.parameters["fielddata"] = "true"
    return t


class SearchMappingFieldTypeObject(SearchMappingFieldType):
    """See: https://opensearch.org/docs/latest/field-types/supported-field-types/object/"""

    _properties: Dict[str, SearchMappingFieldType]

    def __init__(self, type: str):
        self._type = type
        self._properties = {}

    @property
    def properties(self) -> Dict[str, SearchMappingFieldType]:
        return self._properties

    def add_property(self, name, type: SearchMappingFieldType):
        self.properties[name] = type

    def serialize(self) -> dict:
        output_properties: dict = {}
        for name, prop in self._properties.items():
            output_properties[name] = prop.serialize()

        return {"type": self._type, "properties": output_properties}


def nested() -> SearchMappingFieldTypeObject:
    """See: https://opensearch.org/docs/latest/field-types/supported-field-types/object/"""
    return SearchMappingFieldTypeObject("nested")


class SearchMappingFieldTypeCustom(SearchMappingFieldType, ABC):
    """The base class for our custom Opensearch types."""


class SearchMappingFieldTypeCustomBasicText(SearchMappingFieldTypeCustom):
    """The custom 'basic_text' property type.

    This type does not exist in Opensearch. It's our name for a
    text field that is indexed three times: once using our default
    English analyzer ("title"), once using an analyzer with
    minimal stemming ("title.minimal") for close matches, and once
    using an analyzer that leaves stopwords in place, for searches
    that rely on stopwords.
    """

    def serialize(self) -> dict:
        return {
            "type": "text",
            "analyzer": "en_default_text_analyzer",
            "fields": {
                "minimal": {"type": "text", "analyzer": "en_minimal_text_analyzer"},
                "with_stopwords": {
                    "type": "text",
                    "analyzer": "en_with_stopwords_text_analyzer",
                },
            },
        }


BASIC_TEXT: SearchMappingFieldTypeCustomBasicText = (
    SearchMappingFieldTypeCustomBasicText()
)


class SearchMappingFieldTypeCustomFilterable(SearchMappingFieldTypeCustom):
    """The custom 'filterable_text' property type.

    This type does not exist in Opensearch. It's our name for a
    text field that can be used in both queries and filters.

    This field is indexed _four_ times -- the three ways a normal
    text field is indexed, plus again as an unparsed keyword that
    can be used in filters.
    """

    def __init__(self):
        self._basic = SearchMappingFieldTypeCustomBasicText()

    def serialize(self) -> dict:
        output = self._basic.serialize()
        output["fields"]["keyword"] = {
            "type": "keyword",
            "index": True,
            "store": False,
            "normalizer": "filterable_string",
        }
        return output


FILTERABLE_TEXT: SearchMappingFieldTypeCustomFilterable = (
    SearchMappingFieldTypeCustomFilterable()
)


class SearchMappingFieldTypeCustomKeyword(SearchMappingFieldTypeCustom):
    """A custom extension to the keyword type that ensures case-insensitivity."""

    def __init__(self):
        self._base = keyword()

    def serialize(self) -> dict:
        output = self._base.serialize()
        output["normalizer"] = "filterable_string"
        return output


CUSTOM_KEYWORD: SearchMappingFieldTypeCustomKeyword = (
    SearchMappingFieldTypeCustomKeyword()
)


class SearchMappingDocument:
    """
    A top-level Opensearch mapping document.

    See: https://opensearch.org/docs/latest/field-types/index/
    """

    def __init__(self):
        self._settings: Dict[str, dict] = {}
        self._fields: Dict[str, SearchMappingFieldType] = {}
        self._scripts: Dict[str, str] = {}

    @property
    def settings(self) -> Dict[str, dict]:
        return self._settings

    @property
    def scripts(self) -> Dict[str, str]:
        return self._scripts

    @property
    def properties(self) -> Dict[str, SearchMappingFieldType]:
        return self._fields

    @properties.setter
    def properties(self, fields: Dict[str, SearchMappingFieldType]):
        self._fields = dict(fields)

    def serialize(self) -> dict:
        output_properties = self.serialize_properties()
        output_mappings = {"properties": output_properties}
        return {"settings": self.settings, "mappings": output_mappings}

    def serialize_properties(self):
        return {name: prop.serialize() for name, prop in self._fields.items()}
