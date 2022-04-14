import json
import os

import requests
from jsonschema import Draft7Validator, RefResolver

from core.opds2_import import OPDS2ImportMonitor


class OPDS2SchemaValidation(OPDS2ImportMonitor):
    def import_one_feed(self, feed):
        opds2_schema = None
        with open("core/resources/opds2_schema/feed.schema.json") as fp:
            opds2_schema = json.load(fp)

        dir = os.path.dirname(os.path.realpath(__file__))

        handlers = {
            "https": OPDS2RefHandler.fetch_file,
        }

        resolver = RefResolver("file://" + dir + "/", opds2_schema, handlers=handlers)
        schema_validator = Draft7Validator(opds2_schema, resolver=resolver)
        schema_validator.validate(feed, opds2_schema)

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
