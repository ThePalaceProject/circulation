from __future__ import annotations

import logging
import traceback
import urllib
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Generator, Iterable, Sequence
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload
from urllib.parse import urljoin, urlparse
from xml.etree.ElementTree import Element

import dateutil
import feedparser
from feedparser import FeedParserDict
from flask_babel import lazy_gettext as _
from lxml import etree
from pydantic import AnyHttpUrl
from sqlalchemy.orm.session import Session

from api.circulation import (
    BaseCirculationAPI,
    BaseCirculationApiSettings,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
)
from api.circulation_exceptions import CurrentlyAvailable, FormatNotAvailable, NotOnHold
from api.saml.credential import SAMLCredentialManager
from core.classifier import Classifier
from core.connection_config import ConnectionSetting
from core.coverage import CoverageFailure
from core.integration.base import integration_settings_load
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.metadata_layer import (
    CirculationData,
    ContributorData,
    IdentifierData,
    LinkData,
    MeasurementData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
    TimestampData,
)
from core.model import (
    Collection,
    CoverageRecord,
    DataSource,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Measurement,
    Patron,
    Representation,
    RightsStatus,
    Subject,
    get_one,
)
from core.model.formats import FormatPrioritiesSettings
from core.monitor import CollectionMonitor
from core.saml.wayfless import (
    SAMLWAYFlessConstants,
    SAMLWAYFlessFulfillmentError,
    SAMLWAYFlessSetttings,
)
from core.util import base64
from core.util.datetime_helpers import datetime_utc, to_utc, utc_now
from core.util.http import HTTP, BadResponseException
from core.util.log import LoggerMixin
from core.util.opds_writer import OPDSFeed, OPDSMessage
from core.util.xmlparser import XMLParser

if TYPE_CHECKING:
    from core.model import Work


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


class OPDSImporterSettings(
    ConnectionSetting,
    SAMLWAYFlessSetttings,
    FormatPrioritiesSettings,
    BaseCirculationApiSettings,
):
    external_account_id: AnyHttpUrl = FormField(
        form=ConfigurationFormItem(
            label=_("URL"),
            required=True,
        )
    )

    data_source: str = FormField(
        form=ConfigurationFormItem(label=_("Data source name"), required=True)
    )

    default_audience: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            label=_("Default audience"),
            description=_(
                "If the vendor does not specify the target audience for their books, "
                "assume the books have this target audience."
            ),
            type=ConfigurationFormItemType.SELECT,
            options={
                **{None: _("No default audience")},
                **{audience: audience for audience in sorted(Classifier.AUDIENCES)},
            },
            required=False,
        ),
    )

    username: str | None = FormField(
        form=ConfigurationFormItem(
            label=_("Username"),
            description=_(
                "If HTTP Basic authentication is required to access the OPDS feed (it usually isn't), enter the username here."
            ),
            weight=-1,
        )
    )

    password: str | None = FormField(
        form=ConfigurationFormItem(
            label=_("Password"),
            description=_(
                "If HTTP Basic authentication is required to access the OPDS feed (it usually isn't), enter the password here."
            ),
            weight=-1,
        )
    )

    custom_accept_header: str = FormField(
        default=",".join(
            [
                OPDSFeed.ACQUISITION_FEED_TYPE,
                "application/atom+xml;q=0.9",
                "application/xml;q=0.8",
                "*/*;q=0.1",
            ]
        ),
        form=ConfigurationFormItem(
            label=_("Custom accept header"),
            required=False,
            description=_(
                "Some servers expect an accept header to decide which file to send. You can use */* if the server doesn't expect anything."
            ),
            weight=-1,
        ),
    )

    primary_identifier_source: str | None = FormField(
        form=ConfigurationFormItem(
            label=_("Identifer"),
            required=False,
            description=_("Which book identifier to use as ID."),
            type=ConfigurationFormItemType.SELECT,
            options={
                "": _("(Default) Use <id>"),
                ExternalIntegration.DCTERMS_IDENTIFIER: _(
                    "Use <dcterms:identifier> first, if not exist use <id>"
                ),
            },
        )
    )


class OPDSImporterLibrarySettings(BaseSettings):
    pass


class BaseOPDSAPI(
    BaseCirculationAPI[OPDSImporterSettings, OPDSImporterLibrarySettings], ABC
):
    def __init__(self, _db: Session, collection: Collection):
        super().__init__(_db, collection)
        self.saml_wayfless_url_template = self.settings.saml_wayfless_url_template
        self.saml_credential_manager = SAMLCredentialManager()

    def checkin(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # All the CM side accounting for this loan is handled by CirculationAPI
        # since we don't have any remote API we need to call this method is
        # just a no-op.
        pass

    def release_hold(self, patron: Patron, pin: str, licensepool: LicensePool) -> None:
        # Since there is no such thing as a hold, there is no such
        # thing as releasing a hold.
        raise NotOnHold()

    def place_hold(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        notification_email_address: str | None,
    ) -> HoldInfo:
        # Because all OPDS content is assumed to be simultaneously
        # available to all patrons, there is no such thing as a hold.
        raise CurrentlyAvailable()

    def update_availability(self, licensepool: LicensePool) -> None:
        # We already know all the availability information we're going
        # to know, so we don't need to do anything.
        pass

    def fulfill_saml_wayfless(
        self, template: str, patron: Patron, fulfillment: FulfillmentInfo
    ) -> FulfillmentInfo:
        self.log.debug(f"WAYFless acquisition link template: {template}")

        db = Session.object_session(patron)
        saml_credential = self.saml_credential_manager.lookup_saml_token_by_patron(
            db, patron
        )

        self.log.debug(f"SAML credentials: {saml_credential}")

        if not saml_credential:
            raise SAMLWAYFlessFulfillmentError(
                f"There are no existing SAML credentials for patron {patron}"
            )

        saml_subject = self.saml_credential_manager.extract_saml_token(saml_credential)

        self.log.debug(f"SAML subject: {saml_subject}")

        if not saml_subject.idp:
            raise SAMLWAYFlessFulfillmentError(
                f"SAML subject {saml_subject} does not contain an IdP's entityID"
            )

        acquisition_link = template.replace(
            SAMLWAYFlessConstants.IDP_PLACEHOLDER,
            urllib.parse.quote(saml_subject.idp, safe=""),
        )
        if fulfillment.content_link is None:
            self.log.warning(
                f"Fulfillment {fulfillment} has no content link, unable to transform it"
            )
            content_link = ""
        else:
            content_link = fulfillment.content_link

        acquisition_link = acquisition_link.replace(
            SAMLWAYFlessConstants.ACQUISITION_LINK_PLACEHOLDER,
            urllib.parse.quote(content_link, safe=""),
        )

        self.log.debug(
            f"Old acquisition link {fulfillment.content_link} has been transformed to {acquisition_link}"
        )

        fulfillment.content_link = acquisition_link
        return fulfillment

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> FulfillmentInfo:
        requested_mechanism = delivery_mechanism.delivery_mechanism
        fulfillment = None
        for lpdm in licensepool.delivery_mechanisms:
            if (
                lpdm.resource is None
                or lpdm.resource.representation is None
                or lpdm.resource.representation.public_url is None
            ):
                # This LicensePoolDeliveryMechanism can't actually
                # be used for fulfillment.
                continue
            if lpdm.delivery_mechanism == requested_mechanism:
                # We found it! This is how the patron wants
                # the book to be delivered.
                fulfillment = lpdm
                break

        if not fulfillment:
            # There is just no way to fulfill this loan the way the
            # patron wants.
            raise FormatNotAvailable()

        rep = fulfillment.resource.representation
        content_link = rep.public_url
        media_type = rep.media_type

        fulfillment_info = FulfillmentInfo(
            licensepool.collection,
            licensepool.data_source.name,
            identifier_type=licensepool.identifier.type,
            identifier=licensepool.identifier.identifier,
            content_link=content_link,
            content_type=media_type,
            content=None,
            content_expires=None,
        )

        if self.saml_wayfless_url_template:
            fulfillment_info = self.fulfill_saml_wayfless(
                self.saml_wayfless_url_template, patron, fulfillment_info
            )

        return fulfillment_info

    def checkout(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> LoanInfo:
        return LoanInfo(licensepool.collection, None, None, None, None, None)

    def can_fulfill_without_loan(
        self,
        patron: Patron | None,
        pool: LicensePool,
        lpdm: LicensePoolDeliveryMechanism,
    ) -> bool:
        return True


SettingsType = TypeVar("SettingsType", bound=OPDSImporterSettings, covariant=True)


class BaseOPDSImporter(
    Generic[SettingsType],
    LoggerMixin,
    ABC,
):
    def __init__(
        self,
        _db: Session,
        collection: Collection,
        data_source_name: str | None,
        http_get: Callable[..., tuple[int, Any, bytes]] | None = None,
    ):
        self._db = _db
        if collection.id is None:
            raise ValueError(
                f"Unable to create importer for Collection with id = None. Collection: {collection.name}."
            )
        self._collection_id = collection.id
        self._integration_configuration_id = collection.integration_configuration_id
        if data_source_name is None:
            # Use the Collection data_source for OPDS import.
            data_source = self.collection.data_source
            if data_source:
                data_source_name = data_source.name
            else:
                raise ValueError(
                    "Cannot perform an OPDS import on a Collection that has no associated DataSource!"
                )
        self.data_source_name = data_source_name

        # In general, we are cautious when mirroring resources so that
        # we don't, e.g. accidentally get our IP banned from
        # gutenberg.org.
        self.http_get = http_get or Representation.cautious_http_get
        self.settings = integration_settings_load(
            self.settings_class(), collection.integration_configuration
        )

    @classmethod
    @abstractmethod
    def settings_class(cls) -> type[SettingsType]:
        ...

    @abstractmethod
    def extract_feed_data(
        self, feed: str | bytes, feed_url: str | None = None
    ) -> tuple[dict[str, Metadata], dict[str, list[CoverageFailure]]]:
        ...

    @abstractmethod
    def extract_last_update_dates(
        self, feed: str | bytes | FeedParserDict
    ) -> list[tuple[str | None, datetime | None]]:
        ...

    @abstractmethod
    def extract_next_links(self, feed: str | bytes) -> list[str]:
        ...

    @overload
    def parse_identifier(self, identifier: str) -> Identifier:
        ...

    @overload
    def parse_identifier(self, identifier: str | None) -> Identifier | None:
        ...

    def parse_identifier(self, identifier: str | None) -> Identifier | None:
        """Parse the identifier and return an Identifier object representing it.

        :param identifier: String containing the identifier

        :return: Identifier object
        """
        parsed_identifier = None

        try:
            result = Identifier.parse_urn(self._db, identifier)
            if result is not None:
                parsed_identifier, _ = result
        except Exception:
            self.log.error(
                f"An unexpected exception occurred during parsing identifier {identifier}"
            )

        return parsed_identifier

    @property
    def data_source(self) -> DataSource:
        """Look up or create a DataSource object representing the
        source of this OPDS feed.
        """
        offers_licenses = self.collection is not None
        return DataSource.lookup(  # type: ignore[no-any-return]
            self._db,
            self.data_source_name,
            autocreate=True,
            offers_licenses=offers_licenses,
        )

    @property
    def collection(self) -> Collection:
        collection = Collection.by_id(self._db, self._collection_id)
        if collection is None:
            raise ValueError("Unable to load collection.")
        return collection

    def import_edition_from_metadata(self, metadata: Metadata) -> Edition:
        """For the passed-in Metadata object, see if can find or create an Edition
        in the database. Also create a LicensePool if the Metadata has
        CirculationData in it.
        """
        # Locate or create an Edition for this book.
        edition, is_new_edition = metadata.edition(self._db)

        policy = ReplacementPolicy(
            subjects=True,
            links=True,
            contributions=True,
            rights=True,
            link_content=True,
            formats=True,
            even_if_not_apparently_updated=True,
        )
        metadata.apply(
            edition=edition,
            collection=self.collection,
            replace=policy,
        )

        return edition  # type: ignore[no-any-return]

    def update_work_for_edition(
        self,
        edition: Edition,
        is_open_access: bool = True,
    ) -> tuple[LicensePool | None, Work | None]:
        """If possible, ensure that there is a presentation-ready Work for the
        given edition's primary identifier.

        :param edition: The edition whose license pool and work we're interested in.
        :param is_open_access: Whether this is an open access edition.
        :return: 2-Tuple of license pool (optional) and work (optional) for edition.
        """

        work = None

        # Looks up a license pool for the primary identifier associated with
        # the given edition. If this is not an open access book, then the
        # collection is also used as criteria for the lookup. Open access
        # books don't require a collection match, according to this explanation
        # from prior work:
        #   Find a LicensePool for the primary identifier. Any LicensePool will
        #   do--the collection doesn't have to match, since all
        #   LicensePools for a given identifier have the same Work.
        #
        # If we have CirculationData, a pool was created when we
        # imported the edition. If there was already a pool from a
        # different data source or a different collection, that's fine
        # too.
        collection_criteria = {} if is_open_access else {"collection": self.collection}
        pool = get_one(
            self._db,
            LicensePool,
            identifier=edition.primary_identifier,
            on_multiple="interchangeable",
            **collection_criteria,
        )

        if pool:
            if not pool.work or not pool.work.presentation_ready:
                # There is no presentation-ready Work for this
                # LicensePool. Try to create one.
                work, ignore = pool.calculate_work()
            else:
                # There is a presentation-ready Work for this LicensePool.
                # Use it.
                work = pool.work

        # If a presentation-ready Work already exists, there's no
        # rush. We might have new metadata that will change the Work's
        # presentation, but when we called Metadata.apply() the work
        # was set up to have its presentation recalculated in the
        # background, and that's good enough.
        return pool, work

    def import_from_feed(
        self, feed: str | bytes, feed_url: str | None = None
    ) -> tuple[
        list[Edition],
        list[LicensePool],
        list[Work],
        dict[str, list[CoverageFailure]],
    ]:
        # Keep track of editions that were imported. Pools and works
        # for those editions may be looked up or created.
        imported_editions = {}
        pools = {}
        works = {}

        # If parsing the overall feed throws an exception, we should address that before
        # moving on. Let the exception propagate.
        metadata_objs, extracted_failures = self.extract_feed_data(feed, feed_url)
        failures = defaultdict(list, extracted_failures)
        # make editions.  if have problem, make sure associated pool and work aren't created.
        for key, metadata in metadata_objs.items():
            # key is identifier.urn here

            # If there's a status message about this item, don't try to import it.
            if key in list(failures.keys()):
                continue

            try:
                # Create an edition. This will also create a pool if there's circulation data.
                edition = self.import_edition_from_metadata(metadata)
                if edition:
                    imported_editions[key] = edition
            except Exception as e:
                # Rather than scratch the whole import, treat this as a failure that only applies
                # to this item.
                self.log.error("Error importing an OPDS item", exc_info=e)
                data_source = self.data_source
                primary_id: IdentifierData = metadata.primary_identifier
                identifier, ignore = Identifier.for_foreign_id(
                    self._db, primary_id.type, primary_id.identifier
                )
                failure = CoverageFailure(
                    identifier,
                    traceback.format_exc(),
                    data_source=data_source,
                    transient=False,
                    collection=self.collection,
                )
                failures[key].append(failure)
                # clean up any edition might have created
                if key in imported_editions:
                    del imported_editions[key]
                # Move on to the next item, don't create a work.
                continue

            try:
                pool, work = self.update_work_for_edition(edition)
                if pool:
                    pools[key] = pool
                if work:
                    works[key] = work
            except Exception as e:
                collection_name = self.collection.name if self.collection else "None"
                logging.warning(
                    f"Non-fatal exception: Failed to import item - import will continue: "
                    f"identifier={key}; collection={collection_name}/{self._collection_id}; "
                    f"data_source={self.data_source}; exception={e}",
                    stack_info=True,
                )
                identifier, ignore = Identifier.parse_urn(self._db, key)
                data_source = self.data_source
                failure = CoverageFailure(
                    identifier,
                    traceback.format_exc(),
                    data_source=data_source,
                    transient=False,
                    collection=self.collection,
                )
                failures[key].append(failure)

        return (
            list(imported_editions.values()),
            list(pools.values()),
            list(works.values()),
            failures,
        )


class OPDSAPI(BaseOPDSAPI):
    @classmethod
    def settings_class(cls) -> type[OPDSImporterSettings]:
        return OPDSImporterSettings

    @classmethod
    def library_settings_class(cls) -> type[OPDSImporterLibrarySettings]:
        return OPDSImporterLibrarySettings

    @classmethod
    def description(cls) -> str:
        return "Import books from a publicly-accessible OPDS feed."

    @classmethod
    def label(cls) -> str:
        return "OPDS Import"


class OPDSImporter(BaseOPDSImporter[OPDSImporterSettings]):
    """Imports editions and license pools from an OPDS feed.
    Creates Edition, LicensePool and Work rows in the database, if those
    don't already exist.

    Should be used when a circulation server asks for data from
    our internal content server, and also when our content server asks for data
    from external content servers.
    """

    NAME = ExternalIntegration.OPDS_IMPORT
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
        http_get: Callable[..., tuple[int, Any, bytes]] | None = None,
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

        :param http_get: Use this method to make an HTTP GET request. This
        can be replaced with a stub method for testing purposes.
        """
        super().__init__(_db, collection, data_source_name)

        self.primary_identifier_source = self.settings.primary_identifier_source

        # In general, we are cautious when mirroring resources so that
        # we don't, e.g. accidentally get our IP banned from
        # gutenberg.org.
        self.http_get = http_get or Representation.cautious_http_get

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
    ) -> tuple[dict[str, Metadata], dict[str, list[CoverageFailure]]]:
        """Turn an OPDS feed into lists of Metadata and CirculationData objects,
        with associated messages and next_links.
        """
        data_source = self.data_source
        fp_metadata, fp_failures = self.extract_data_from_feedparser(
            feed=feed, data_source=data_source
        )
        # gets: medium, measurements, links, contributors, etc.
        xml_data_meta, xml_failures = self.extract_metadata_from_elementtree(
            feed, data_source=data_source, feed_url=feed_url, do_get=self.http_get
        )

        # translate the id in failures to identifier.urn
        identified_failures = {}
        for urn, failure in list(fp_failures.items()) + list(xml_failures.items()):
            identifier, failure = self.handle_failure(urn, failure)
            identified_failures[identifier.urn] = [failure]

        # Use one loop for both, since the id will be the same for both dictionaries.
        metadata = {}
        _id: str
        for _id, m_data_dict in list(fp_metadata.items()):
            xml_data_dict = xml_data_meta.get(_id, {})

            external_identifier = None
            if self.primary_identifier_source == ExternalIntegration.DCTERMS_IDENTIFIER:
                # If it should use <dcterms:identifier> as the primary identifier, it must use the
                # first value from the dcterms identifier, that came from the metadata as an
                # IdentifierData object and it must be validated as a foreign_id before be used
                # as and external_identifier.
                dcterms_ids = xml_data_dict.get("dcterms_identifiers", [])
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
                        IdentifierData(id_type, id_identifier, id_weight)
                    )
                    xml_data_dict["identifiers"] = new_identifiers

            if external_identifier is None:
                external_identifier, ignore = Identifier.parse_urn(self._db, _id)

            # Don't process this item if there was already an error
            if external_identifier.urn in list(identified_failures.keys()):
                continue

            identifier_obj = IdentifierData(
                type=external_identifier.type, identifier=external_identifier.identifier
            )

            # form the Metadata object
            combined_meta = self.combine(m_data_dict, xml_data_dict)
            if combined_meta.get("data_source") is None:
                combined_meta["data_source"] = self.data_source_name

            combined_meta["primary_identifier"] = identifier_obj

            metadata[external_identifier.urn] = Metadata(**combined_meta)

            # Form the CirculationData that would correspond to this Metadata,
            # assuming there is a Collection to hold the LicensePool that
            # would result.
            c_data_dict = None
            if self.collection:
                c_circulation_dict = m_data_dict.get("circulation")
                xml_circulation_dict = xml_data_dict.get("circulation", {})
                c_data_dict = self.combine(c_circulation_dict, xml_circulation_dict)

            # Unless there's something useful in c_data_dict, we're
            # not going to put anything under metadata.circulation,
            # and any partial data that got added to
            # metadata.circulation is going to be removed.
            metadata[external_identifier.urn].circulation = None
            if c_data_dict:
                circ_links_dict = {}
                # extract just the links to pass to CirculationData constructor
                if "links" in xml_data_dict:
                    circ_links_dict["links"] = xml_data_dict["links"]
                combined_circ = self.combine(c_data_dict, circ_links_dict)
                if combined_circ.get("data_source") is None:
                    combined_circ["data_source"] = self.data_source_name

                combined_circ["primary_identifier"] = identifier_obj

                combined_circ["should_track_playtime"] = xml_data_dict.get(
                    "should_track_playtime", False
                )
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
                    metadata[external_identifier.urn].circulation = circulation
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
        return metadata, identified_failures

    @overload
    def handle_failure(
        self, urn: str, failure: Identifier
    ) -> tuple[Identifier, Identifier]:
        ...

    @overload
    def handle_failure(
        self, urn: str, failure: CoverageFailure
    ) -> tuple[Identifier, CoverageFailure]:
        ...

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
        self, d1: dict[str, Any] | None, d2: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Combine two dictionaries that can be used as keyword arguments to
        the Metadata constructor.
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
                    new_dict[k] = self.combine(new_dict[k], v)
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
    def extract_metadata_from_elementtree(
        cls,
        feed: bytes | str,
        data_source: DataSource,
        feed_url: str | None = None,
        do_get: Callable[..., tuple[int, Any, bytes]] | None = None,
    ) -> tuple[dict[str, Any], dict[str, CoverageFailure]]:
        """Parse the OPDS as XML and extract all author and subject
        information, as well as ratings and medium.

        All the stuff that Feedparser can't handle so we have to use lxml.

        :return: a dictionary mapping IDs to dictionaries. The inner
            dictionary can be used as keyword arguments to the Metadata
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

        # Then turn Atom <entry> tags into Metadata objects.
        for entry in parser._xpath(root, "/atom:feed/atom:entry"):
            identifier, detail, failure_entry = cls.detail_for_elementtree_entry(
                parser, entry, data_source, feed_url, do_get=do_get
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
        that can be used as keyword arguments to the Metadata and CirculationData constructors.

        :return: A 3-tuple (identifier, kwargs for Metadata constructor, failure)
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
        """Helper method that extracts metadata and circulation data from a feedparser
        entry. This method can be overridden in tests to check that callers handle things
        properly when it throws an exception.
        """
        title = entry.get("title", None)
        if title == OPDSFeed.NO_TITLE:
            title = None
        subtitle = entry.get("schema_alternativeheadline", None)

        # Generally speaking, a data source will provide either
        # metadata (e.g. the Simplified metadata wrangler) or both
        # metadata and circulation data (e.g. a publisher's ODL feed).
        #
        # However there is at least one case (the Simplified
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
            data_source=circulation_data_source.name,
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
    ) -> Generator[OPDSMessage, None, None]:
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
    ) -> Generator[CoverageFailure, None, None]:
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
        do_get: Callable[..., tuple[int, Any, bytes]] | None = None,
    ) -> tuple[str | None, dict[str, Any] | None, CoverageFailure | None]:
        """Turn an <atom:entry> tag into a dictionary of metadata that can be
        used as keyword arguments to the Metadata contructor.

        :return: A 2-tuple (identifier, kwargs)
        """
        identifier = parser._xpath1(entry_tag, "atom:id")
        if identifier is None or not identifier.text:
            # This <entry> tag doesn't identify a book so we
            # can't derive any information from it.
            return None, None, None
        identifier = identifier.text

        try:
            data = cls._detail_for_elementtree_entry(
                parser, entry_tag, feed_url, do_get=do_get
            )
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
        do_get: Callable[..., tuple[int, Any, bytes]] | None = None,
    ) -> dict[str, Any]:
        """Helper method that extracts metadata and circulation data from an elementtree
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

        data["should_track_playtime"] = False
        time_tracking_tag = parser._xpath(entry_tag, "palace:timeTracking")
        if time_tracking_tag:
            data["should_track_playtime"] = time_tracking_tag[0].text.lower() == "true"
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
            return IdentifierData(type, identifier)
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
                roles=None,
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
        """Try to match up links with their thumbnails.

        If link n is an image and link n+1 is a thumbnail, then the
        thumbnail is assumed to be the thumbnail of the image.

        Similarly, if link n is a thumbnail and link n+1 is an image.
        """
        # Strip out any links that didn't get turned into LinkData objects
        # due to missing `href` or whatever.
        new_links = [x for x in links if x]

        # Make a new list of links from that list, to iterate over --
        # we'll be modifying new_links in place so we can't iterate
        # over it.
        _links = list(new_links)

        next_link_already_handled = False
        for i, link in enumerate(_links):
            if link.rel not in (Hyperlink.THUMBNAIL_IMAGE, Hyperlink.IMAGE):
                # This is not any kind of image. Ignore it.
                continue

            if next_link_already_handled:
                # This link and the previous link were part of an
                # image-thumbnail pair.
                next_link_already_handled = False
                continue

            if i == len(_links) - 1:
                # This is the last link. Since there is no next link
                # there's nothing to do here.
                continue

            # Peek at the next link.
            next_link = _links[i + 1]

            if (
                link.rel == Hyperlink.THUMBNAIL_IMAGE
                and next_link.rel == Hyperlink.IMAGE
            ):
                # This link is a thumbnail and the next link is
                # (presumably) the corresponding image.
                thumbnail_link = link
                image_link = next_link
            elif (
                link.rel == Hyperlink.IMAGE
                and next_link.rel == Hyperlink.THUMBNAIL_IMAGE
            ):
                thumbnail_link = next_link
                image_link = link
            else:
                # This link and the next link do not form an
                # image-thumbnail pair. Do nothing.
                continue

            image_link.thumbnail = thumbnail_link
            new_links.remove(thumbnail_link)
            next_link_already_handled = True

        return new_links

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


class OPDSImportMonitor(CollectionMonitor):
    """Periodically monitor a Collection's OPDS archive feed and import
    every title it mentions.
    """

    SERVICE_NAME = "OPDS Import Monitor"

    # The first time this Monitor is invoked we want to get the
    # entire OPDS feed.
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    # The protocol this Monitor works with. Subclasses that
    # specialize OPDS import should override this.
    PROTOCOL = ExternalIntegration.OPDS_IMPORT

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: type[BaseOPDSImporter[OPDSImporterSettings]],
        force_reimport: bool = False,
        **import_class_kwargs: Any,
    ) -> None:
        if not collection:
            raise ValueError(
                "OPDSImportMonitor can only be run in the context of a Collection."
            )

        if collection.protocol != self.PROTOCOL:
            raise ValueError(
                "Collection %s is configured for protocol %s, not %s."
                % (collection.name, collection.protocol, self.PROTOCOL)
            )

        data_source = self.data_source(collection)
        if not data_source:
            raise ValueError(
                "Collection %s has no associated data source." % collection.name
            )

        self.force_reimport = force_reimport

        self.importer = import_class(_db, collection=collection, **import_class_kwargs)
        settings = self.importer.settings
        self.username = settings.username
        self.password = settings.password
        self.feed_url = settings.external_account_id

        self.custom_accept_header = settings.custom_accept_header
        self._max_retry_count = settings.max_retry_count

        parsed_url = urlparse(self.feed_url)
        self._feed_base_url = f"{parsed_url.scheme}://{parsed_url.hostname}{(':' + str(parsed_url.port)) if parsed_url.port else ''}/"
        super().__init__(_db, collection)

    def _get(
        self, url: str, headers: dict[str, str]
    ) -> tuple[int, dict[str, str], bytes]:
        """Make the sort of HTTP request that's normal for an OPDS feed.

        Long timeout, raise error on anything but 2xx or 3xx.
        """

        headers = self._update_headers(headers)
        kwargs = dict(
            timeout=120,
            max_retry_count=self._max_retry_count,
            allowed_response_codes=["2xx", "3xx"],
        )
        if not url.startswith("http"):
            url = urljoin(self._feed_base_url, url)
        response = HTTP.get_with_timeout(url, headers=headers, **kwargs)
        return response.status_code, response.headers, response.content  # type: ignore[return-value]

    def _get_accept_header(self) -> str:
        return ",".join(
            [
                OPDSFeed.ACQUISITION_FEED_TYPE,
                "application/atom+xml;q=0.9",
                "application/xml;q=0.8",
                "*/*;q=0.1",
            ]
        )

    def _update_headers(self, headers: dict[str, str] | None) -> dict[str, str]:
        headers = dict(headers) if headers else {}
        if self.username and self.password and not "Authorization" in headers:
            headers["Authorization"] = "Basic %s" % base64.b64encode(
                f"{self.username}:{self.password}"
            )

        if self.custom_accept_header:
            headers["Accept"] = self.custom_accept_header
        elif not "Accept" in headers:
            headers["Accept"] = self._get_accept_header()

        return headers

    def data_source(self, collection: Collection) -> DataSource | None:
        """Returns the data source name for the given collection.

        By default, this URL is stored as a setting on the collection, but
        subclasses may hard-code it.
        """
        return collection.data_source

    def feed_contains_new_data(self, feed: bytes | str) -> bool:
        """Does the given feed contain any entries that haven't been imported
        yet?
        """
        if self.force_reimport:
            # We don't even need to check. Always treat the feed as
            # though it contained new data.
            return True

        # For every item in the last page of the feed, check when that
        # item was last updated.
        last_update_dates = self.importer.extract_last_update_dates(feed)

        new_data = False
        for raw_identifier, remote_updated in last_update_dates:
            identifier = self.importer.parse_identifier(raw_identifier)
            if not identifier:
                # Maybe this is new, maybe not, but we can't associate
                # the information with an Identifier, so we can't do
                # anything about it.
                self.log.info(
                    f"Ignoring {raw_identifier} because unable to turn into an Identifier."
                )
                continue

            if self.identifier_needs_import(identifier, remote_updated):
                new_data = True
                break
        return new_data

    def identifier_needs_import(
        self, identifier: Identifier | None, last_updated_remote: datetime | None
    ) -> bool:
        """Does the remote side have new information about this Identifier?

        :param identifier: An Identifier.
        :param last_update_remote: The last time the remote side updated
            the OPDS entry for this Identifier.
        """
        if not identifier:
            return False

        record = CoverageRecord.lookup(
            identifier,
            self.importer.data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=self.collection,
        )

        if not record:
            # We have no record of importing this Identifier. Import
            # it now.
            self.log.info(
                "Counting %s as new because it has no CoverageRecord.", identifier
            )
            return True

        # If there was a transient failure last time we tried to
        # import this book, try again regardless of whether the
        # feed has changed.
        if record.status == CoverageRecord.TRANSIENT_FAILURE:
            self.log.info(
                "Counting %s as new because previous attempt resulted in transient failure: %s",
                identifier,
                record.exception,
            )
            return True

        # If our last attempt was a success or a persistent
        # failure, we only want to import again if something
        # changed since then.

        if record.timestamp:
            # We've imported this entry before, so don't import it
            # again unless it's changed.

            if not last_updated_remote:
                # The remote isn't telling us whether the entry
                # has been updated. Import it again to be safe.
                self.log.info(
                    "Counting %s as new because remote has no information about when it was updated.",
                    identifier,
                )
                return True

            if to_utc(last_updated_remote) >= to_utc(record.timestamp):
                # This book has been updated.
                self.log.info(
                    "Counting %s as new because its coverage date is %s and remote has %s.",
                    identifier,
                    record.timestamp,
                    last_updated_remote,
                )
                return True
        return False

    def _verify_media_type(
        self, url: str, status_code: int, headers: dict[str, str], feed: bytes
    ) -> None:
        # Make sure we got an OPDS feed, and not an error page that was
        # sent with a 200 status code.
        media_type = headers.get("content-type")
        if not media_type or not any(
            x in media_type for x in (OPDSFeed.ATOM_LIKE_TYPES)
        ):
            message = "Expected Atom feed, got %s" % media_type
            raise BadResponseException(
                url, message=message, debug_message=feed, status_code=status_code
            )

    def follow_one_link(
        self, url: str, do_get: Callable[..., tuple[int, Any, bytes]] | None = None
    ) -> tuple[list[str], bytes | None]:
        """Download a representation of a URL and extract the useful
        information.

        :return: A 2-tuple (next_links, feed). `next_links` is a list of
            additional links that need to be followed. `feed` is the content
            that needs to be imported.
        """
        self.log.info("Following next link: %s", url)
        get = do_get or self._get
        status_code, headers, feed = get(url, {})

        self._verify_media_type(url, status_code, headers, feed)

        new_data = self.feed_contains_new_data(feed)

        if new_data:
            # There's something new on this page, so we need to check
            # the next page as well.
            next_links = self.importer.extract_next_links(feed)
            return next_links, feed
        else:
            # There's nothing new, so we don't need to import this
            # feed or check the next page.
            self.log.info("No new data.")
            return [], None

    def import_one_feed(
        self, feed: bytes | str
    ) -> tuple[list[Edition], dict[str, list[CoverageFailure]]]:
        """Import every book mentioned in an OPDS feed."""

        # Because we are importing into a Collection, we will immediately
        # mark a book as presentation-ready if possible.
        imported_editions, pools, works, failures = self.importer.import_from_feed(
            feed, feed_url=self.feed_url
        )

        # Create CoverageRecords for the successful imports.
        for edition in imported_editions:
            record = CoverageRecord.add_for(
                edition,
                self.importer.data_source,
                CoverageRecord.IMPORT_OPERATION,
                status=CoverageRecord.SUCCESS,
                collection=self.collection,
            )

        # Create CoverageRecords for the failures.
        for urn, failure_items in list(failures.items()):
            for failure_item in failure_items:
                failure_item.to_coverage_record(
                    operation=CoverageRecord.IMPORT_OPERATION
                )

        return imported_editions, failures

    def _get_feeds(self) -> Iterable[tuple[str, bytes]]:
        feeds = []
        queue = [cast(str, self.feed_url)]
        seen_links = set()

        # First, follow the feed's next links until we reach a page with
        # nothing new. If any link raises an exception, nothing will be imported.

        while queue:
            new_queue = []

            for link in queue:
                if link in seen_links:
                    continue
                next_links, feed = self.follow_one_link(link)
                new_queue.extend(next_links)
                if feed:
                    feeds.append((link, feed))
                seen_links.add(link)

            queue = new_queue

        # Start importing at the end. If something fails, it will be easier to
        # pick up where we left off.
        return reversed(feeds)

    def run_once(self, progress: TimestampData) -> TimestampData:
        feeds = self._get_feeds()
        total_imported = 0
        total_failures = 0

        for link, feed in feeds:
            self.log.info("Importing next feed: %s", link)
            imported_editions, failures = self.import_one_feed(feed)
            total_imported += len(imported_editions)
            total_failures += len(failures)
            self._db.commit()

        achievements = "Items imported: %d. Failures: %d." % (
            total_imported,
            total_failures,
        )

        return TimestampData(achievements=achievements)
