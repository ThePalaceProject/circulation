import json
import re
from collections.abc import Generator
from importlib.abc import Traversable
from typing import Any
from urllib.parse import urlparse

from jsonschema import Draft7Validator, validators
from jsonschema._utils import extras_msg
from jsonschema.exceptions import ValidationError
from jsonschema.protocols import Validator
from referencing import Registry
from referencing.retrieval import to_cached_resource

from palace.manager.api.odl2 import ODL2ImportMonitor
from palace.manager.core.opds2_import import OPDS2ImportMonitor
from palace.manager.util.log import LoggerMixin
from palace.manager.util.resources import resources_dir


def opds2_schema_resources() -> Traversable:
    return resources_dir("opds2_schema")


@to_cached_resource(loads=json.loads)
def opds2_cached_retrieve(uri: str) -> str:
    """
    Fetch files from the resources directory or from local cache.

    If the uri is a file:// uri, fetch the file from the resources directory. Otherwise,
    fetch the file from the local cache in the 'cached' directory.

    To refresh the cache, delete the 'cached' directory uncomment the code below and re-run
    the tests. This will force the function to download any necessary files into the cache.
    """
    parsed = urlparse(uri)
    resources = opds2_schema_resources()
    if parsed.scheme == "file":
        filename = f"{parsed.netloc}{parsed.path}"
        package_file = resources / filename
    else:
        netloc_dir = parsed.netloc
        filename = parsed.path.removeprefix("/").replace("/", "_")
        package_file = resources / "cached" / netloc_dir / filename
        # if not package_file.is_file():
        #     cached_dir = resources / "cached" / netloc_dir
        #     cached_dir.mkdir(parents=True, exist_ok=True)
        #     (cached_dir / filename).write_text(requests.get(uri).text)

    return package_file.read_text()


def opds2_regex_replace(pattern: str) -> str:
    """
    Replace named groups in a regex pattern.

    The OPDS2 schema uses a regex pattern using named groups, which is a valid PCRE pattern,
    but not valid in Python's re module. This function converts the named groups to use the
    Python specific ?P<name> syntax.
    """
    return re.sub(r"\?<(.+?)>", r"?P<\1>", pattern)


def opds2_pattern_validator(
    validator: Validator, patrn: str, instance: Any, schema: dict[str, Any]
) -> Generator[ValidationError, None, None]:
    """
    Validation function to validate a patten element.

    The bulk of this function is copied from the jsonschema library. It was copied from
    jsonschema._keywords.pattern. They put their validation functions in a private module,
    and the docs mention not to extending them. So we copied the function here.
    """
    patrn = opds2_regex_replace(patrn)
    if validator.is_type(instance, "string") and not re.search(patrn, instance):
        yield ValidationError(f"{instance!r} does not match {patrn!r}")


def opds2_pattern_properties_validator(
    validator: Validator,
    patternProperties: dict[str, Any],
    instance: dict[str, Any],
    schema: dict[str, Any],
) -> Generator[ValidationError, None, None]:
    """
    Validation function to validate a pattenProperties element.

    The bulk of this function is copied from the jsonschema library. It was copied from
    jsonschema._keywords.patternProperties. They put their validation functions in a private module,
    and the docs mention not to extending them. So we copied the function here.
    """
    if not validator.is_type(instance, "object"):
        return

    for pattern, subschema in patternProperties.items():
        pattern = opds2_regex_replace(pattern)
        for k, v in instance.items():
            if re.search(pattern, k):
                yield from validator.descend(
                    v,
                    subschema,
                    path=k,
                    schema_path=pattern,
                )


def opds2_additional_properties_validator(
    validator: Validator,
    aP: dict[str, Any],
    instance: dict[str, Any],
    schema: dict[str, Any],
) -> Generator[ValidationError, None, None]:
    """
    Validation function to validate a pattenProperties element.

    The bulk of this function is copied from the jsonschema library. It was copied from
    jsonschema._keywords.additionalProperties. They put their validation functions in a private module,
    and the docs mention not to extending them. So we copied the function here.
    """

    def additional_properties(
        instance: dict[str, Any], schema: dict[str, Any]
    ) -> Generator[str, None, None]:
        properties = schema.get("properties", {})
        patterns = "|".join(schema.get("patternProperties", {}))
        patterns = opds2_regex_replace(patterns)
        for property in instance:
            if property not in properties:
                if patterns and re.search(patterns, property):
                    continue
                yield property

    if not validator.is_type(instance, "object"):
        return

    extras = set(additional_properties(instance, schema))

    if validator.is_type(aP, "object"):
        for extra in extras:
            yield from validator.descend(instance[extra], aP, path=extra)
    elif not aP and extras:
        if "patternProperties" in schema:
            verb = "does" if len(extras) == 1 else "do"
            joined = ", ".join(repr(each) for each in sorted(extras))
            patterns = ", ".join(
                repr(each) for each in sorted(schema["patternProperties"])
            )
            error = f"{joined} {verb} not match any of the regexes: {patterns}"
            yield ValidationError(error)
        else:
            error = "Additional properties are not allowed (%s %s unexpected)"
            yield ValidationError(error % extras_msg(sorted(extras, key=str)))


def opds2_schema_registry() -> Registry:
    """
    Create a Registry that loads schemas with the opds2_cached_retrieve function.
    """
    # See https://github.com/python-jsonschema/referencing/issues/61 for details on
    # why we needed the type ignore here.
    return Registry(retrieve=opds2_cached_retrieve)  # type: ignore[call-arg]


def opds2_schema_validator(schema: dict[str, Any]) -> Validator:
    """
    This returns a jsonschema Draft7Validator modified to use the opds2_pattern_validator
    function for the pattern keyword.
    """

    registry = opds2_schema_registry()
    validator_cls = validators.extend(
        Draft7Validator,
        version="draft7",
        validators={
            "pattern": opds2_pattern_validator,
            "patternProperties": opds2_pattern_properties_validator,
            "additionalProperties": opds2_additional_properties_validator,
        },
    )
    return validator_cls(schema, registry=registry)


class OPDS2SchemaValidationMixin(LoggerMixin):
    def validate_schema(self, schema_url: str, feed: dict[str, Any]) -> None:
        schema = {"$ref": schema_url}
        schema_validator = opds2_schema_validator(schema)
        try:
            schema_validator.validate(feed)
        except ValidationError as e:
            self.log.error("Validation failed for feed")
            for attr in ["message", "path", "schema_path", "validator_value"]:
                self.log.error(f"{attr}: {getattr(e, attr, None)}")
            raise


class OPDS2SchemaValidation(OPDS2ImportMonitor, OPDS2SchemaValidationMixin):
    def import_one_feed(self, feed):
        if type(feed) in (str, bytes):
            feed = json.loads(feed)
        self.validate_schema("https://drafts.opds.io/schema/feed.schema.json", feed)
        return [], []

    def follow_one_link(self, url, do_get=None):
        """We don't need all pages, the first page should be fine for validation"""
        next_links, feed = super().follow_one_link(url, do_get)
        return [], feed

    def feed_contains_new_data(self, feed):
        return True


class ODL2SchemaValidation(ODL2ImportMonitor, OPDS2SchemaValidationMixin):
    def import_one_feed(self, feed):
        feed = json.loads(feed)
        self.validate_schema("file://odl-feed.schema.json", feed)
        return [], []

    def follow_one_link(self, url, do_get=None):
        """We don't need all pages, the first page should be fine for validation"""
        next_links, feed = super().follow_one_link(url, do_get)
        return [], feed

    def feed_contains_new_data(self, feed):
        return True
