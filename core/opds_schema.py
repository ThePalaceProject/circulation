import json
import os

import requests
from jsonschema import Draft7Validator, RefResolver
from jsonschema.exceptions import ValidationError

from api.odl2 import ODL2ImportMonitor
from core.opds2_import import OPDS2ImportMonitor


class OPDS2SchemaValidationMixin:
    def get_ref_resolver(self, json_schema):
        dir_ = os.path.dirname(os.path.realpath(__file__))
        handlers = {
            "https": OPDS2RefHandler.fetch_file,
        }

        resolver = RefResolver("file://" + dir_ + "/", json_schema, handlers=handlers)
        return resolver

    def validate_schema(self, schema_path: str, feed: dict):
        with open(schema_path) as fp:
            opds2_schema = json.load(fp)

        resolver = self.get_ref_resolver(opds2_schema)
        schema_validator = Draft7Validator(opds2_schema, resolver=resolver)
        try:
            schema_validator.validate(feed, opds2_schema)
        except ValidationError as e:
            self.log.error("Validation failed for feed")  # type: ignore
            for attr in ["message", "path", "schema_path", "validator_value"]:
                self.log.error(f"{attr}: {getattr(e, attr, None)}")  # type: ignore
            raise


class OPDS2SchemaValidation(OPDS2ImportMonitor, OPDS2SchemaValidationMixin):
    def import_one_feed(self, feed):
        if type(feed) in (str, bytes):
            feed = json.loads(feed)
        self.validate_schema("core/resources/opds2_schema/feed.schema.json", feed)
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
        self.validate_schema("core/resources/opds2_schema/odl-feed.schema.json", feed)
        return [], []

    def follow_one_link(self, url, do_get=None):
        """We don't need all pages, the first page should be fine for validation"""
        next_links, feed = super().follow_one_link(url, do_get)
        return [], feed

    def feed_contains_new_data(self, feed):
        return True


class OPDS2RefHandler:
    @classmethod
    def fetch_file(cls, name: str):
        """Fetch file from local filesystem if present, else fetch remotely"""
        dir = os.path.dirname(os.path.realpath(__file__))
        filename = name.split("/")[-1]
        localpath = f"{dir}/resources/opds2_schema"

        if os.path.exists(f"{localpath}/{filename}"):
            with open(f"{localpath}/{filename}") as fp:
                return json.load(fp)
        else:
            return requests.get(name).json()
