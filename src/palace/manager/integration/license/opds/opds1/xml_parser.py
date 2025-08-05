from __future__ import annotations

from palace.manager.util.xmlparser import XMLParser


class OPDSXMLParser(XMLParser):
    NAMESPACES = {
        "simplified": "http://librarysimplified.org/terms/",
        "app": "http://www.w3.org/2007/app",
        "dcterms": "http://purl.org/dc/terms/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "opds": "http://opds-spec.org/2010/catalog",
        "schema": "http://schema.org/",
        "atom": "http://www.w3.org/2005/Atom",
        "drm": "http://librarysimplified.org/terms/drm",
        "palace": "http://palaceproject.io/terms",
    }
