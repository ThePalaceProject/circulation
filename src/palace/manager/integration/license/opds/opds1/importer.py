from __future__ import annotations

import logging
import traceback
from collections.abc import Generator, Sequence
from datetime import datetime
from io import BytesIO
from typing import Any, overload
from urllib.parse import urljoin, urlparse
from xml.etree.ElementTree import Element

import dateutil
import feedparser
from feedparser import FeedParserDict
from flask_babel import lazy_gettext as _
from lxml import etree
from sqlalchemy.orm import Session

from palace.manager.core.coverage import CoverageFailure
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.opds.base.importer import BaseOPDSImporter
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds1.settings import (
    IdentifierSource,
    OPDSImporterSettings,
)
from palace.manager.integration.license.opds.opds1.xml_parser import OPDSXMLParser
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import RightsStatus
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from palace.manager.util.opds_writer import OPDSFeed, OPDSMessage


class OPDSImporter(BaseOPDSImporter[OPDSImporterSettings]):
    """Imports editions and license pools from an OPDS feed.
    Creates Edition, LicensePool and Work rows in the database, if those
    don't already exist.

    Should be used when a circulation server asks for data from
    our internal content server, and also when our content server asks for data
    from external content servers.
    """

    NAME = OPDSAPI.label()
    DESCRIPTION = _("Import books from a publicly-accessible OPDS feed.")

    # Subclasses of OPDSImporter may define a different parser class that's
    # a subclass of OPDSXMLParser. For example, a subclass may want to use
    # tags from an additional namespace.
    PARSER_CLASS = OPDSXMLParser

    @classmethod
    def settings_class(cls) -> type[OPDSImporterSettings]:
        return OPDSImporterSettings

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        data_source_name: str | None = None,
    ):
        """:param collection: LicensePools created by this OPDS import
        will be associated with the given Collection. If this is None,
        no LicensePools will be created -- only Editions.

        :param data_source_name: Name of the source of this OPDS feed.
        All Editions created by this import will be associated with
        this DataSource. If there is no DataSource with this name, one
        will be created. NOTE: If `collection` is provided, its
        .data_source will take precedence over any value provided
        here. This is only for use when you are importing OPDS
        metadata without any particular Collection in mind.
        """
        super().__init__(_db, collection, data_source_name)

        self.primary_identifier_source = self.settings.primary_identifier_source

    def extract_next_links(self, feed: str | bytes | FeedParserDict) -> list[str]:
        if isinstance(feed, (bytes, str)):
            parsed = feedparser.parse(feed)
        else:
            parsed = feed
        feed = parsed["feed"]
        next_links = []
        if feed and "links" in feed:
            next_links = [
                link["href"] for link in feed["links"] if link["rel"] == "next"
            ]
        return next_links

    def extract_last_update_dates(
        self, feed: str | bytes | FeedParserDict
    ) -> list[tuple[str | None, datetime | None]]:
        if isinstance(feed, (bytes, str)):
            parsed_feed = feedparser.parse(feed)
        else:
            parsed_feed = feed
        dates = [
            self.last_update_date_for_feedparser_entry(entry)
            for entry in parsed_feed["entries"]
        ]
        return [x for x in dates if x and x[1]]

    def extract_feed_data(
        self, feed: str | bytes, feed_url: str | None = None
    ) -> tuple[dict[str, BibliographicData], dict[str, list[CoverageFailure]]]:
        """Turn an OPDS feed into lists of BibliographicData and CirculationData objects,
        with associated messages and next_links.
        """
        data_source = self.data_source
        fp_bibliographic, fp_failures = self.extract_data_from_feedparser(
            feed=feed, data_source=data_source
        )
        # gets: medium, measurements, links, contributors, etc.
        xml_data_bibliographic, xml_failures = (
            self.extract_bibliographic_from_elementtree(
                feed, data_source=data_source, feed_url=feed_url
            )
        )

        # translate the id in failures to identifier.urn
        identified_failures = {}
        for urn, failure in list(fp_failures.items()) + list(xml_failures.items()):
            identifier, failure = self.handle_failure(urn, failure)
            identified_failures[identifier.urn] = [failure]

        # Use one loop for both, since the id will be the same for both dictionaries.
        bibliographic = {}
        _id: str
        for _id, bibliographic_data_dict in list(fp_bibliographic.items()):
            xml_data_dict = xml_data_bibliographic.get(_id, {})

            external_identifier = None
            dcterms_ids = xml_data_dict.pop("dcterms_identifiers", [])
            if self.primary_identifier_source == IdentifierSource.DCTERMS_IDENTIFIER:
                # If it should use <dcterms:identifier> as the primary identifier, it must use the
                # first value from the dcterms identifier, that came from the bibliographic data as an
                # IdentifierData object and it must be validated as a foreign_id before be used
                # as and external_identifier.
                if len(dcterms_ids) > 0:
                    external_identifier, ignore = Identifier.for_foreign_id(
                        self._db, dcterms_ids[0].type, dcterms_ids[0].identifier
                    )
                    # the external identifier will be add later, so it must be removed at this point
                    new_identifiers = dcterms_ids[1:]
                    # Id must be in the identifiers with lower weight.
                    id_type, id_identifier = Identifier.type_and_identifier_for_urn(_id)
                    id_weight = 1
                    new_identifiers.append(
                        IdentifierData(
                            type=id_type, identifier=id_identifier, weight=id_weight
                        )
                    )
                    xml_data_dict["identifiers"] = new_identifiers

            if external_identifier is None:
                external_identifier, ignore = Identifier.parse_urn(self._db, _id)

            # Don't process this item if there was already an error
            if external_identifier.urn in list(identified_failures.keys()):
                continue

            identifier_obj = IdentifierData.from_identifier(external_identifier)

            # form the BibliographicData object
            combined_bibliographic = self.combine(
                bibliographic_data_dict, xml_data_dict
            )
            if combined_bibliographic.get("data_source_name") is None:
                combined_bibliographic["data_source_name"] = self.data_source_name

            combined_bibliographic["primary_identifier_data"] = identifier_obj

            bibliographic[external_identifier.urn] = BibliographicData(
                **combined_bibliographic
            )

            # Form the CirculationData that would correspond to this BibliographicData,
            # assuming there is a Collection to hold the LicensePool that
            # would result.
            c_data_dict = None
            if self.collection:
                c_circulation_dict = bibliographic_data_dict.get("circulation")
                xml_circulation_dict = xml_data_dict.get("circulation", {})
                c_data_dict = self.combine(c_circulation_dict, xml_circulation_dict)

            # Unless there's something useful in c_data_dict, we're
            # not going to put anything under bibliographic.circulation,
            # and any partial data that got added to
            # bibliographic.circulation is going to be removed.
            bibliographic[external_identifier.urn].circulation = None
            if c_data_dict:
                circ_links_dict = {}
                # extract just the links to pass to CirculationData constructor
                if "links" in xml_data_dict:
                    circ_links_dict["links"] = xml_data_dict["links"]
                combined_circ = self.combine(c_data_dict, circ_links_dict)
                if combined_circ.get("data_source") is None:
                    combined_circ["data_source"] = self.data_source_name

                combined_circ["primary_identifier_data"] = identifier_obj

                if (
                    combined_circ["should_track_playtime"]
                    and xml_data_dict["medium"] != Edition.AUDIO_MEDIUM
                ):
                    combined_circ["should_track_playtime"] = False
                    self.log.warning(
                        f"Ignoring the time tracking flag for entry {identifier_obj.identifier}"
                    )

                circulation = CirculationData(**combined_circ)

                self._add_format_data(circulation)

                if circulation.formats:
                    bibliographic[external_identifier.urn].circulation = circulation
                else:
                    # If the CirculationData has no formats, it
                    # doesn't really offer any way to actually get the
                    # book, and we don't want to create a
                    # LicensePool. All the circulation data is
                    # useless.
                    #
                    # TODO: This will need to be revisited when we add
                    # ODL support.
                    pass
        return bibliographic, identified_failures

    @overload
    def handle_failure(
        self, urn: str, failure: Identifier
    ) -> tuple[Identifier, Identifier]: ...

    @overload
    def handle_failure(
        self, urn: str, failure: CoverageFailure
    ) -> tuple[Identifier, CoverageFailure]: ...

    def handle_failure(
        self, urn: str, failure: Identifier | CoverageFailure
    ) -> tuple[Identifier, CoverageFailure | Identifier]:
        """Convert a URN and a failure message that came in through
        an OPDS feed into an Identifier and a CoverageFailure object.

        The 'failure' may turn out not to be a CoverageFailure at
        all -- if it's an Identifier, that means that what a normal
        OPDSImporter would consider 'failure' is considered success.
        """
        external_identifier, ignore = Identifier.parse_urn(self._db, urn)
        if isinstance(failure, Identifier):
            # The OPDSImporter does not actually consider this a
            # failure. Signal success by returning the internal
            # identifier as the 'failure' object.
            failure = external_identifier
        else:
            # This really is a failure. Associate the internal
            # identifier with the CoverageFailure object.
            failure.obj = external_identifier
            failure.collection = self.collection
        return external_identifier, failure

    @classmethod
    def _add_format_data(cls, circulation: CirculationData) -> None:
        """Subclasses that specialize OPDS Import can implement this
        method to add formats to a CirculationData object with
        information that allows a patron to actually get a book
        that's not open access.
        """

    @classmethod
    def combine(
        cls, d1: dict[str, Any] | None, d2: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Combine two dictionaries that can be used as keyword arguments to
        the BibliographicData constructor.
        """
        if not d1 and not d2:
            return dict()
        if not d1:
            return dict(d2)  # type: ignore[arg-type]
        if not d2:
            return dict(d1)
        new_dict = dict(d1)
        for k, v in list(d2.items()):
            if k not in new_dict:
                # There is no value from d1. Even if the d2 value
                # is None, we want to set it.
                new_dict[k] = v
            elif v != None:
                # d1 provided a value, and d2 provided a value other
                # than None.
                if isinstance(v, list):
                    # The values are lists. Merge them.
                    new_dict[k].extend(v)
                elif isinstance(v, dict):
                    # The values are dicts. Merge them by with
                    # a recursive combine() call.
                    new_dict[k] = cls.combine(new_dict[k], v)
                else:
                    # Overwrite d1's value with d2's value.
                    new_dict[k] = v
            else:
                # d1 provided a value and d2 provided None.  Do
                # nothing.
                pass
        return new_dict

    def extract_data_from_feedparser(
        self, feed: str | bytes, data_source: DataSource
    ) -> tuple[dict[str, Any], dict[str, CoverageFailure]]:
        feedparser_parsed = feedparser.parse(feed)
        values = {}
        failures = {}
        for entry in feedparser_parsed["entries"]:
            identifier, detail, failure = self.data_detail_for_feedparser_entry(
                entry=entry, data_source=data_source
            )
            if failure:
                failure.collection = self.collection

            if identifier:
                if failure:
                    failures[identifier] = failure
                else:
                    if detail:
                        values[identifier] = detail
            else:
                # That's bad. Can't make an item-specific error message, but write to
                # log that something very wrong happened.
                logging.error(
                    f"Tried to parse an element without a valid identifier.  feed={feed!r}"
                )
        return values, failures

    @classmethod
    def extract_bibliographic_from_elementtree(
        cls,
        feed: bytes | str,
        data_source: DataSource,
        feed_url: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, CoverageFailure]]:
        """Parse the OPDS as XML and extract all author and subject
        information, as well as ratings and medium.

        All the stuff that Feedparser can't handle so we have to use lxml.

        :return: a dictionary mapping IDs to dictionaries. The inner
            dictionary can be used as keyword arguments to the BibliographicData
            constructor.
        """
        values = {}
        failures = {}
        parser = cls.PARSER_CLASS()
        if isinstance(feed, bytes):
            inp = BytesIO(feed)
        else:
            inp = BytesIO(feed.encode("utf-8"))
        root = etree.parse(inp)

        # Some OPDS feeds (eg Standard Ebooks) contain relative urls,
        # so we need the feed's self URL to extract links. If none was
        # passed in, we still might be able to guess.
        #
        # TODO: Section 2 of RFC 4287 says we should check xml:base
        # for this, so if anyone actually uses that we'll get around
        # to checking it.
        if not feed_url:
            links = [child.attrib for child in root.getroot() if "link" in child.tag]
            self_links = [link["href"] for link in links if link.get("rel") == "self"]
            if self_links:
                feed_url = self_links[0]

        # First, turn Simplified <message> tags into CoverageFailure
        # objects.
        for failure in cls.coveragefailures_from_messages(data_source, parser, root):
            if isinstance(failure, Identifier):
                # The Simplified <message> tag does not actually
                # represent a failure -- it was turned into an
                # Identifier instead of a CoverageFailure.
                urn = failure.urn
            else:
                urn = failure.obj.urn
            failures[urn] = failure

        # Then turn Atom <entry> tags into BibliographicData objects.
        for entry in parser._xpath(root, "/atom:feed/atom:entry"):
            identifier, detail, failure_entry = cls.detail_for_elementtree_entry(
                parser, entry, data_source, feed_url
            )
            if identifier:
                if failure_entry:
                    failures[identifier] = failure_entry
                if detail:
                    values[identifier] = detail
        return values, failures

    @classmethod
    def _datetime(cls, entry: dict[str, str], key: str) -> datetime | None:
        value = entry.get(key, None)
        if not value:
            return None
        return datetime_utc(*value[:6])

    def last_update_date_for_feedparser_entry(
        self, entry: dict[str, Any]
    ) -> tuple[str | None, datetime | None]:
        identifier = entry.get("id")
        updated = self._datetime(entry, "updated_parsed")
        return identifier, updated

    @classmethod
    def data_detail_for_feedparser_entry(
        cls, entry: dict[str, str], data_source: DataSource
    ) -> tuple[str | None, dict[str, Any] | None, CoverageFailure | None]:
        """Turn an entry dictionary created by feedparser into dictionaries of data
        that can be used as keyword arguments to the BibliographicData and CirculationData constructors.

        :return: A 3-tuple (identifier, kwargs for BibliographicData constructor, failure)
        """
        identifier = entry.get("id")
        if not identifier:
            return None, None, None

        # At this point we can assume that we successfully got some
        # metadata, and possibly a link to the actual book.
        try:
            kwargs_meta = cls._data_detail_for_feedparser_entry(entry, data_source)
            return identifier, kwargs_meta, None
        except Exception as e:
            _db = Session.object_session(data_source)
            identifier_obj, ignore = Identifier.parse_urn(_db, identifier)
            failure = CoverageFailure(
                identifier_obj, traceback.format_exc(), data_source, transient=True
            )
            return identifier, None, failure

    @classmethod
    def _data_detail_for_feedparser_entry(
        cls, entry: dict[str, Any], metadata_data_source: DataSource
    ) -> dict[str, Any]:
        """Helper method that extracts bibliographic data and circulation data from a feedparser
        entry. This method can be overridden in tests to check that callers handle things
        properly when it throws an exception.
        """
        title = entry.get("title", None)
        if title == OPDSFeed.NO_TITLE:
            title = None
        subtitle = entry.get("schema_alternativeheadline", None)

        # Generally speaking, a data source will provide either
        # bibliographic data (e.g. the Simplified metadata wrangler) or both
        # bibliographic and circulation data (e.g. a publisher's ODL feed).
        #
        # However, there is at least one case (the Simplified
        # open-access content server) where one server provides
        # circulation data from a _different_ data source
        # (e.g. Project Gutenberg).
        #
        # In this case we want the data source of the LicensePool to
        # be Project Gutenberg, but the data source of the pool's
        # presentation to be the open-access content server.
        #
        # The open-access content server uses a
        # <bibframe:distribution> tag to keep track of which data
        # source provides the circulation data.
        circulation_data_source = metadata_data_source
        circulation_data_source_tag = entry.get("bibframe_distribution")
        if circulation_data_source_tag:
            circulation_data_source_name = circulation_data_source_tag.get(
                "bibframe:providername"
            )
            if circulation_data_source_name:
                _db = Session.object_session(metadata_data_source)
                # We know this data source offers licenses because
                # that's what the <bibframe:distribution> is there
                # to say.
                circulation_data_source = DataSource.lookup(
                    _db,
                    circulation_data_source_name,
                    autocreate=True,
                    offers_licenses=True,
                )
                if not circulation_data_source:
                    raise ValueError(
                        "Unrecognized circulation data source: %s"
                        % (circulation_data_source_name)
                    )
        last_opds_update = cls._datetime(entry, "updated_parsed")

        publisher = entry.get("publisher", None)
        if not publisher:
            publisher = entry.get("dcterms_publisher", None)

        language = entry.get("language", None)
        if not language:
            language = entry.get("dcterms_language", None)

        links = []

        def summary_to_linkdata(detail: dict[str, str] | None) -> LinkData | None:
            if not detail:
                return None
            if not "value" in detail or not detail["value"]:
                return None

            content = detail["value"]
            media_type = detail.get("type", "text/plain")
            return cls.make_link_data(
                rel=Hyperlink.DESCRIPTION, media_type=media_type, content=content
            )

        summary_detail = entry.get("summary_detail", None)
        link = summary_to_linkdata(summary_detail)
        if link:
            links.append(link)

        for content_detail in entry.get("content", []):
            link = summary_to_linkdata(content_detail)
            if link:
                links.append(link)

        rights_uri = cls.rights_uri_from_feedparser_entry(entry)

        kwargs_meta = dict(
            title=title,
            subtitle=subtitle,
            language=language,
            publisher=publisher,
            links=links,
            # refers to when was updated in opds feed, not our db
            data_source_last_updated=last_opds_update,
        )

        # Although we always provide the CirculationData, it will only
        # be used if the OPDSImporter has a Collection to hold the
        # LicensePool that will result from importing it.
        kwargs_circ = dict(
            data_source_name=circulation_data_source.name,
            links=list(links),
            default_rights_uri=rights_uri,
        )
        kwargs_meta["circulation"] = kwargs_circ
        return kwargs_meta

    @classmethod
    def rights_uri(cls, rights_string: str) -> str:
        """Determine the URI that best encapsulates the rights status of
        the downloads associated with this book.
        """
        return RightsStatus.rights_uri_from_string(rights_string)

    @classmethod
    def rights_uri_from_feedparser_entry(cls, entry: dict[str, str]) -> str:
        """Extract a rights URI from a parsed feedparser entry.

        :return: A rights URI.
        """
        rights = entry.get("rights", "")
        return cls.rights_uri(rights)

    @classmethod
    def rights_uri_from_entry_tag(cls, entry: Element) -> str | None:
        """Extract a rights string from an lxml <entry> tag.

        :return: A rights URI.
        """
        rights = cls.PARSER_CLASS._xpath1(entry, "rights")
        if rights is None:
            return None
        return cls.rights_uri(rights)

    @classmethod
    def extract_messages(
        cls, parser: OPDSXMLParser, feed_tag: str
    ) -> Generator[OPDSMessage]:
        """Extract <simplified:message> tags from an OPDS feed and convert
        them into OPDSMessage objects.
        """
        path = "/atom:feed/simplified:message"
        for message_tag in parser._xpath(feed_tag, path):
            # First thing to do is determine which Identifier we're
            # talking about.
            identifier_tag = parser._xpath1(message_tag, "atom:id")
            if identifier_tag is None:
                urn = None
            else:
                urn = identifier_tag.text

            # What status code is associated with the message?
            status_code_tag = parser._xpath1(message_tag, "simplified:status_code")
            if status_code_tag is None:
                status_code = None
            else:
                try:
                    status_code = int(status_code_tag.text)
                except ValueError:
                    status_code = None

            # What is the human-readable message?
            description_tag = parser._xpath1(message_tag, "schema:description")
            if description_tag is None:
                description = ""
            else:
                description = description_tag.text

            yield OPDSMessage(urn, status_code, description)

    @classmethod
    def coveragefailures_from_messages(
        cls, data_source: DataSource, parser: OPDSXMLParser, feed_tag: str
    ) -> Generator[CoverageFailure]:
        """Extract CoverageFailure objects from a parsed OPDS document. This
        allows us to determine the fate of books which could not
        become <entry> tags.
        """
        for message in cls.extract_messages(parser, feed_tag):
            failure = cls.coveragefailure_from_message(data_source, message)
            if failure:
                yield failure

    @classmethod
    def coveragefailure_from_message(
        cls, data_source: DataSource, message: OPDSMessage
    ) -> CoverageFailure | None:
        """Turn a <simplified:message> tag into a CoverageFailure."""

        _db = Session.object_session(data_source)

        # First thing to do is determine which Identifier we're
        # talking about. If we can't do that, we can't create a
        # CoverageFailure object.
        urn = message.urn
        try:
            identifier, ignore = Identifier.parse_urn(_db, urn)
        except ValueError as e:
            identifier = None

        if not identifier:
            # We can't associate this message with any particular
            # Identifier so we can't turn it into a CoverageFailure.
            return None

        if message.status_code == 200:
            # By default, we treat a message with a 200 status code
            # as though nothing had been returned at all.
            return None

        description = message.message
        status_code = message.status_code
        if description and status_code:
            exception = f"{status_code}: {description}"
        elif status_code:
            exception = str(status_code)
        elif description:
            exception = description
        else:
            exception = "No detail provided."

        # All these CoverageFailures are transient because ATM we can
        # only assume that the server will eventually have the data.
        return CoverageFailure(identifier, exception, data_source, transient=True)

    @classmethod
    def detail_for_elementtree_entry(
        cls,
        parser: OPDSXMLParser,
        entry_tag: Element,
        data_source: DataSource,
        feed_url: str | None = None,
    ) -> tuple[str | None, dict[str, Any] | None, CoverageFailure | None]:
        """Turn an <atom:entry> tag into a dictionary of bibliographic data that can be
        used as keyword arguments to the BibliographicData contructor.

        :return: A 2-tuple (identifier, kwargs)
        """
        identifier = parser._xpath1(entry_tag, "atom:id")
        if identifier is None or not identifier.text:
            # This <entry> tag doesn't identify a book so we
            # can't derive any information from it.
            return None, None, None
        identifier = identifier.text

        try:
            data = cls._detail_for_elementtree_entry(parser, entry_tag, feed_url)
            return identifier, data, None

        except Exception as e:
            _db = Session.object_session(data_source)
            identifier_obj, ignore = Identifier.parse_urn(_db, identifier)
            failure = CoverageFailure(
                identifier_obj, traceback.format_exc(), data_source, transient=True
            )
            return identifier, None, failure

    @classmethod
    def _detail_for_elementtree_entry(
        cls,
        parser: OPDSXMLParser,
        entry_tag: Element,
        feed_url: str | None = None,
    ) -> dict[str, Any]:
        """Helper method that extracts bibliographic and circulation data from an elementtree
        entry. This method can be overridden in tests to check that callers handle things
        properly when it throws an exception.
        """
        # We will fill this dictionary with all the information
        # we can find.
        data: dict[str, Any] = dict()

        alternate_identifiers = []
        for id_tag in parser._xpath(entry_tag, "dcterms:identifier"):
            v = cls.extract_identifier(id_tag)
            if v:
                alternate_identifiers.append(v)
        data["dcterms_identifiers"] = alternate_identifiers

        # If exist another identifer, add here
        data["identifiers"] = data["dcterms_identifiers"]

        data["contributors"] = []
        for author_tag in parser._xpath(entry_tag, "atom:author"):
            contributor = cls.extract_contributor(parser, author_tag)
            if contributor is not None:
                data["contributors"].append(contributor)

        data["subjects"] = [
            cls.extract_subject(parser, category_tag)
            for category_tag in parser._xpath(entry_tag, "atom:category")
        ]

        ratings = []
        for rating_tag in parser._xpath(entry_tag, "schema:Rating"):
            measurement = cls.extract_measurement(rating_tag)
            if measurement:
                ratings.append(measurement)
        data["measurements"] = ratings
        rights_uri = cls.rights_uri_from_entry_tag(entry_tag)

        data["links"] = cls.consolidate_links(
            [
                cls.extract_link(link_tag, feed_url, rights_uri)
                for link_tag in parser._xpath(entry_tag, "atom:link")
            ]
        )

        derived_medium = cls.get_medium_from_links(data["links"])
        data["medium"] = cls.extract_medium(entry_tag, derived_medium)

        series_tag = parser._xpath(entry_tag, "schema:Series")
        if series_tag:
            data["series"], data["series_position"] = cls.extract_series(series_tag[0])

        issued_tag = parser._xpath(entry_tag, "dcterms:issued")
        if issued_tag:
            date_string = issued_tag[0].text
            # By default, the date for strings that only have a year will
            # be set to January 1 rather than the current date.
            default = datetime_utc(utc_now().year, 1, 1)
            try:
                data["published"] = dateutil.parser.parse(date_string, default=default)
            except Exception as e:
                # This entry had an issued tag, but it was in a format we couldn't parse.
                pass

        circulation_data = data.get("circulation", {})
        circulation_data["should_track_playtime"] = False
        time_tracking_tag = parser._xpath(entry_tag, "palace:timeTracking")
        if time_tracking_tag:
            circulation_data["should_track_playtime"] = (
                time_tracking_tag[0].text.lower() == "true"
            )
        data["circulation"] = circulation_data
        return data

    @classmethod
    def get_medium_from_links(cls, links: list[LinkData]) -> str | None:
        """Get medium if derivable from information in an acquisition link."""
        derived = None
        for link in links:
            if (
                not link.rel
                or not link.media_type
                or not link.rel.startswith("http://opds-spec.org/acquisition/")
            ):
                continue
            derived = Edition.medium_from_media_type(link.media_type)
            if derived:
                break
        return derived

    @classmethod
    def extract_identifier(cls, identifier_tag: Element) -> IdentifierData | None:
        """Turn a <dcterms:identifier> tag into an IdentifierData object."""
        try:
            if identifier_tag.text is None:
                return None
            type, identifier = Identifier.type_and_identifier_for_urn(
                identifier_tag.text.lower()
            )
            return IdentifierData(type=type, identifier=identifier)
        except ValueError:
            return None

    @classmethod
    def extract_medium(
        cls, entry_tag: Element | None, default: str | None = Edition.BOOK_MEDIUM
    ) -> str | None:
        """Derive a value for Edition.medium from schema:additionalType or
        from a <dcterms:format> subtag.

        :param entry_tag: A <atom:entry> tag.
        :param default: The value to use if nothing is found.
        """
        if entry_tag is None:
            return default

        medium = None
        additional_type = entry_tag.get("{http://schema.org/}additionalType")
        if additional_type:
            medium = Edition.additional_type_to_medium.get(additional_type, None)
        if not medium:
            format_tag = entry_tag.find("{http://purl.org/dc/terms/}format")
            if format_tag is not None:
                media_type = format_tag.text
                medium = Edition.medium_from_media_type(media_type)
        return medium or default

    @classmethod
    def extract_contributor(
        cls, parser: OPDSXMLParser, author_tag: Element
    ) -> ContributorData | None:
        """Turn an <atom:author> tag into a ContributorData object."""
        subtag = parser.text_of_optional_subtag
        sort_name = subtag(author_tag, "simplified:sort_name")
        display_name = subtag(author_tag, "atom:name")
        family_name = subtag(author_tag, "simplified:family_name")
        wikipedia_name = subtag(author_tag, "simplified:wikipedia_name")
        # TODO: we need a way of conveying roles. I believe Bibframe
        # has the answer.

        # TODO: Also collect VIAF and LC numbers if present.  This
        # requires parsing the URIs. Only the metadata wrangler will
        # provide this information.

        viaf = None
        if sort_name or display_name or viaf:
            return ContributorData(
                sort_name=sort_name,
                display_name=display_name,
                family_name=family_name,
                wikipedia_name=wikipedia_name,
            )

        logging.info(
            "Refusing to create ContributorData for contributor with no sort name, display name, or VIAF."
        )
        return None

    @classmethod
    def extract_subject(
        cls, parser: OPDSXMLParser, category_tag: Element
    ) -> SubjectData:
        """Turn an <atom:category> tag into a SubjectData object."""
        attr = category_tag.attrib

        # Retrieve the type of this subject - FAST, Dewey Decimal,
        # etc.
        scheme = attr.get("scheme")
        subject_type = Subject.by_uri.get(scheme)  # type: ignore[arg-type]
        if not subject_type:
            # We can't represent this subject because we don't
            # know its scheme. Just treat it as a tag.
            subject_type = Subject.TAG

        # Retrieve the term (e.g. "827") and human-readable name
        # (e.g. "English Satire & Humor") for this subject.
        term = attr.get("term")
        name = attr.get("label")
        default_weight = 1

        weight = attr.get("{http://schema.org/}ratingValue", default_weight)
        try:
            weight = int(weight)
        except ValueError as e:
            weight = default_weight

        return SubjectData(type=subject_type, identifier=term, name=name, weight=weight)

    @classmethod
    def extract_link(
        cls,
        link_tag: Element,
        feed_url: str | None = None,
        entry_rights_uri: str | None = None,
    ) -> LinkData | None:
        """Convert a <link> tag into a LinkData object.

        :param feed_url: The URL to the enclosing feed, for use in resolving
            relative links.

        :param entry_rights_uri: A URI describing the rights advertised
            in the entry. Unless this specific link says otherwise, we
            will assume that the representation on the other end of the link
            if made available on these terms.
        """
        attr = link_tag.attrib
        rel = attr.get("rel")
        media_type = attr.get("type")
        href = attr.get("href")
        if not href or not rel:
            # The link exists but has no destination, or no specified
            # relationship to the entry.
            return None
        rights = attr.get("{%s}rights" % OPDSXMLParser.NAMESPACES["dcterms"])
        rights_uri = entry_rights_uri
        if rights:
            # Rights associated with the link override rights
            # associated with the entry.
            rights_uri = cls.rights_uri(rights)

        if feed_url and not urlparse(href).netloc:
            # This link is relative, so we need to get the absolute url
            href = urljoin(feed_url, href)
        return cls.make_link_data(rel, href, media_type, rights_uri)

    @classmethod
    def make_link_data(
        cls,
        rel: str,
        href: str | None = None,
        media_type: str | None = None,
        rights_uri: str | None = None,
        content: str | None = None,
    ) -> LinkData:
        """Hook method for creating a LinkData object.

        Intended to be overridden in subclasses.
        """
        return LinkData(
            rel=rel,
            href=href,
            media_type=media_type,
            rights_uri=rights_uri,
            content=content,
        )

    @classmethod
    def consolidate_links(cls, links: Sequence[LinkData | None]) -> list[LinkData]:
        """Match up image links with their corresponding thumbnails.

        Scans through a list of links to find image and thumbnail pairs:
        - If an image link is followed by a thumbnail link, they are paired
        - If a thumbnail link is followed by an image link, they are paired

        When a pair is found, the thumbnail is associated with the image, and
        the consolidated link is added to the result.
        """
        valid_links = [link for link in links if link is not None]
        result = []
        i = 0

        while i < len(valid_links):
            current_link = valid_links[i]

            # If not an image or thumbnail, add as-is and continue
            if current_link.rel not in (Hyperlink.THUMBNAIL_IMAGE, Hyperlink.IMAGE):
                result.append(current_link)
                i += 1
                continue

            # Check if we have a next link to potentially form a pair
            if i + 1 < len(valid_links):
                next_link = valid_links[i + 1]

                # Case 1: Current is thumbnail, next is image
                if (
                    current_link.rel == Hyperlink.THUMBNAIL_IMAGE
                    and next_link.rel == Hyperlink.IMAGE
                ):
                    result.append(next_link.set_thumbnail(current_link))
                    i += 2  # Skip both links as they're now handled
                    continue

                # Case 2: Current is image, next is thumbnail
                if (
                    current_link.rel == Hyperlink.IMAGE
                    and next_link.rel == Hyperlink.THUMBNAIL_IMAGE
                ):
                    result.append(current_link.set_thumbnail(next_link))
                    i += 2  # Skip both links as they're now handled
                    continue

            # If we're here, this link doesn't form a pair with the next link
            result.append(current_link)
            i += 1

        return result

    @classmethod
    def extract_measurement(cls, rating_tag: Element) -> MeasurementData | None:
        type = rating_tag.get("{http://schema.org/}additionalType")
        value = rating_tag.get("{http://schema.org/}ratingValue")
        if not value:
            value = rating_tag.attrib.get("{http://schema.org}ratingValue")
        if not type:
            type = Measurement.RATING

        if value is None:
            return None

        try:
            float_value = float(value)
            return MeasurementData(
                quantity_measured=type,
                value=float_value,
            )
        except ValueError:
            return None

    @classmethod
    def extract_series(cls, series_tag: Element) -> tuple[str | None, str | None]:
        attr = series_tag.attrib
        series_name = attr.get("{http://schema.org/}name", None)
        series_position = attr.get("{http://schema.org/}position", None)
        return series_name, series_position
