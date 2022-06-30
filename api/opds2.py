from typing import Dict

from flask import url_for

from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.resource import Hyperlink
from core.opds2 import OPDS2Annotator


class OPDS2PublicationsAnnotator(OPDS2Annotator):
    def loan_link(self, edition: Edition) -> Dict:
        identifier: Identifier = edition.primary_identifier
        return {
            "href": url_for(
                "borrow",
                identifier_type=identifier.type,
                identifier=identifier.identifier,
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
            ),
            "rel": "self",
        }
