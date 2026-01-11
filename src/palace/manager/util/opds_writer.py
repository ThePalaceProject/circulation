import datetime
from typing import Any, cast

import pytz
from lxml import builder, etree
from lxml.etree import _Element

from palace.manager.util.datetime_helpers import utc_now


class ElementMaker(builder.ElementMaker):  # type: ignore[misc]
    """A helper object for creating etree elements."""

    def __getstate__(self) -> dict[str, Any]:
        # Remove default_typemap from the dictionary -- it contains functions
        # that can't be pickled.
        return {
            k: v
            for k, v in super(ElementMaker, self).__dict__.items()
            if k != "default_typemap"
        }


class AtomFeed:
    ATOM_TYPE = "application/atom+xml"

    ATOM_LIKE_TYPES = [ATOM_TYPE, "application/xml"]

    TIME_FORMAT_UTC = "%Y-%m-%dT%H:%M:%S+00:00"
    TIME_FORMAT_NAIVE = "%Y-%m-%dT%H:%M:%SZ"

    ATOM_NS = "http://www.w3.org/2005/Atom"
    APP_NS = "http://www.w3.org/2007/app"
    # xhtml_ns = 'http://www.w3.org/1999/xhtml'
    DCTERMS_NS = "http://purl.org/dc/terms/"
    OPDS_NS = "http://opds-spec.org/2010/catalog"
    SCHEMA_NS = "http://schema.org/"
    DRM_NS = "http://librarysimplified.org/terms/drm"
    OPF_NS = "http://www.idpf.org/2007/opf"
    OPENSEARCH_NS = "http://a9.com/-/spec/opensearch/1.1/"

    SIMPLIFIED_NS = "http://librarysimplified.org/terms/"
    BIBFRAME_NS = "http://bibframe.org/vocab/"
    BIB_SCHEMA_NS = "http://bib.schema.org/"

    LCP_NS = "http://readium.org/lcp-specs/ns"

    ODL_NS = "http://drafts.opds.io/odl-1.0#"

    PALACE_REL_NS = "http://palaceproject.io/terms/rel/"
    PALACE_PROPS_NS = "http://palaceproject.io/terms/properties/"

    PALACE_REL_SORT = PALACE_REL_NS + "sort"
    PALACE_PROPERTIES_DEFAULT = PALACE_PROPS_NS + "default"

    nsmap = {
        None: ATOM_NS,
        "app": APP_NS,
        "dcterms": DCTERMS_NS,
        "opds": OPDS_NS,
        "opf": OPF_NS,
        "drm": DRM_NS,
        "schema": SCHEMA_NS,
        "simplified": SIMPLIFIED_NS,
        "bibframe": BIBFRAME_NS,
        "bib": BIB_SCHEMA_NS,
        "opensearch": OPENSEARCH_NS,
        "lcp": LCP_NS,
        "palaceproperties": PALACE_PROPS_NS,
        "odl": ODL_NS,
    }

    E = ElementMaker(nsmap=nsmap)
    SIMPLIFIED = ElementMaker(nsmap=nsmap, namespace=SIMPLIFIED_NS)
    SCHEMA = ElementMaker(nsmap=nsmap, namespace=SCHEMA_NS)
    ODL = ElementMaker(nsmap=nsmap, namespace=ODL_NS)

    @classmethod
    def _strftime(cls, date: datetime.date | datetime.datetime) -> str:
        """
        Format a date the way Atom likes it.

        'A Date construct is an element whose content MUST conform to the
        "date-time" production in [RFC3339].  In addition, an uppercase "T"
        character MUST be used to separate date and time, and an uppercase
        "Z" character MUST be present in the absence of a numeric time zone
        offset.' (https://tools.ietf.org/html/rfc4287#section-3.3)
        """
        if isinstance(date, datetime.datetime) and date.tzinfo is not None:
            # Convert to UTC to make the formatting easier.
            fmt = cls.TIME_FORMAT_UTC
            date = date.astimezone(pytz.UTC)
        else:
            fmt = cls.TIME_FORMAT_NAIVE

        return date.strftime(fmt)

    @classmethod
    def add_link_to_entry(
        cls, entry: _Element, children: list[_Element] | None = None, **kwargs: Any
    ) -> None:
        if "title" in kwargs:
            kwargs["title"] = str(kwargs["title"])
        link = cls.E.link(**kwargs)
        entry.append(link)
        if children:
            for i in children:
                link.append(i)

    @classmethod
    def link(cls, *args: Any, **kwargs: Any) -> _Element:
        return cls.E.link(*args, **kwargs)

    @classmethod
    def tlink(cls, *args: Any, **kwargs: Any) -> _Element:
        return cls.ODL.tlink(*args, **kwargs)

    @classmethod
    def category(cls, *args: Any, **kwargs: Any) -> _Element:
        return cls.E.category(*args, **kwargs)

    @classmethod
    def entry(cls, *args: Any, **kwargs: Any) -> _Element:
        return cls.E.entry(*args, **kwargs)

    def __init__(self, title: str, url: str, **kwargs: Any) -> None:
        """Constructor.

        :param title: The title of this feed.
        :param url: The URL at which clients can expect to find this feed.
        """
        self.feed: _Element = self.E.feed(
            self.E.id(url),
            self.E.title(str(title)),
            self.E.updated(self._strftime(utc_now())),
            self.E.link(href=url, rel="self"),
        )
        super().__init__(**kwargs)

    def __str__(self) -> str:
        if self.feed is None:
            return ""
        # etree.tostring with encoding="unicode" returns str
        return cast(
            str, etree.tostring(self.feed, encoding="unicode", pretty_print=True)
        )


class OPDSFeed(AtomFeed):
    ACQUISITION_FEED_TYPE = (
        AtomFeed.ATOM_TYPE + ";profile=opds-catalog;kind=acquisition"
    )
    NAVIGATION_FEED_TYPE = AtomFeed.ATOM_TYPE + ";profile=opds-catalog;kind=navigation"
    ENTRY_TYPE = AtomFeed.ATOM_TYPE + ";type=entry;profile=opds-catalog"

    GROUP_REL = "collection"
    FEATURED_REL = "http://opds-spec.org/featured"
    RECOMMENDED_REL = "http://opds-spec.org/recommended"
    POPULAR_REL = "http://opds-spec.org/sort/popular"
    OPEN_ACCESS_REL = "http://opds-spec.org/acquisition/open-access"
    ACQUISITION_REL = "http://opds-spec.org/acquisition"
    BORROW_REL = "http://opds-spec.org/acquisition/borrow"
    FULL_IMAGE_REL = "http://opds-spec.org/image"
    EPUB_MEDIA_TYPE = "application/epub+zip"

    REVOKE_LOAN_REL = "http://librarysimplified.org/terms/rel/revoke"
    NO_TITLE = "http://librarysimplified.org/terms/problem/no-title"

    # Most types of OPDS feeds can be cached client-side for at least ten
    # minutes.
    DEFAULT_MAX_AGE = 60 * 10

    def __init__(self, title: str, url: str) -> None:
        super().__init__(title, url)


class OPDSMessage:
    """An indication that an <entry> could not be created for an
    identifier.

    Inserted into an OPDS feed as an extension tag.
    """

    def __init__(self, urn: str, status_code: int | str | None, message: str) -> None:
        self.urn = urn
        self.status_code: int | None = int(status_code) if status_code else None
        self.message = message

    def __str__(self) -> str:
        # etree.tostring with encoding="unicode" returns str
        return cast(str, etree.tostring(self.tag, encoding="unicode"))

    def __repr__(self) -> str:
        # etree.tostring with default encoding returns bytes
        return str(etree.tostring(self.tag))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True

        if not isinstance(other, OPDSMessage):
            return False

        if (
            self.urn != other.urn
            or self.status_code != other.status_code
            or self.message != other.message
        ):
            return False
        return True

    @property
    def tag(self) -> _Element:
        message_tag = AtomFeed.SIMPLIFIED.message()
        identifier_tag = AtomFeed.E.id()
        identifier_tag.text = self.urn
        message_tag.append(identifier_tag)

        status_tag = AtomFeed.SIMPLIFIED.status_code()
        status_tag.text = str(self.status_code)
        message_tag.append(status_tag)

        description_tag = AtomFeed.SCHEMA.description()
        description_tag.text = str(self.message)
        message_tag.append(description_tag)
        return message_tag
