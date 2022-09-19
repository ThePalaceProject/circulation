from typing import Dict

from flask import url_for

from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.resource import Hyperlink
from core.opds2 import OPDS2Annotator


class OPDS2PublicationsAnnotator(OPDS2Annotator):
    """API level implementation for the publications feed OPDS2 annotator"""

    def loan_link(self, edition: Edition) -> Dict:
        identifier: Identifier = edition.primary_identifier
        return {
            "href": url_for(
                "borrow",
                identifier_type=identifier.type,
                identifier=identifier.identifier,
                library_short_name=self.library.short_name,
            ),
            "rel": Hyperlink.BORROW,
        }

    def self_link(self, edition: Edition) -> Dict:
        identifier: Identifier = edition.primary_identifier
        return {
            "href": url_for(
                "permalink",
                identifier_type=identifier.type,
                identifier=identifier.identifier,
                library_short_name=self.library.short_name,
            ),
            "rel": "self",
        }


class OPDS2NavigationsAnnotator(OPDS2Annotator):
    """API level implementation for the navigation feed OPDS2 annotator"""

    def navigation_collection(self) -> Dict:
        """The OPDS2 navigation collection, currently only serves the publications link"""
        return [
            {
                "href": url_for(
                    "opds2_publications", library_short_name=self.library.short_name
                ),
                "title": "OPDS2 Publications Feed",
                "type": self.OPDS2_TYPE,
            }
        ]

    def feed_metadata(self):
        return {"title": self.title}

    def feed_links(self):
        return [
            {"href": self.url, "rel": "self", "type": self.OPDS2_TYPE},
        ]
