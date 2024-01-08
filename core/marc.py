from __future__ import annotations

import re
import urllib.parse
from collections.abc import Mapping
from datetime import datetime
from io import BytesIO
from uuid import UUID, uuid4

import pytz
from pydantic import NonNegativeInt
from pymarc import Field, Record, Subfield
from sqlalchemy import select
from sqlalchemy.engine import ScalarResult
from sqlalchemy.orm.session import Session

from core.classifier import Classifier
from core.integration.base import HasLibraryIntegrationConfiguration
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.model import (
    Collection,
    DeliveryMechanism,
    Edition,
    Identifier,
    Library,
    LicensePool,
    MarcFile,
    Representation,
    Work,
    create,
)
from core.service.storage.s3 import S3Service
from core.util import LanguageCodes
from core.util.datetime_helpers import utc_now
from core.util.log import LoggerMixin
from core.util.uuid import uuid_encode


class Annotator(LoggerMixin):
    """The Annotator knows how to add information about a Work to
    a MARC record."""

    # From https://www.loc.gov/standards/valuelist/marctarget.html
    AUDIENCE_TERMS: Mapping[str, str] = {
        Classifier.AUDIENCE_CHILDREN: "Juvenile",
        Classifier.AUDIENCE_YOUNG_ADULT: "Adolescent",
        Classifier.AUDIENCE_ADULTS_ONLY: "Adult",
        Classifier.AUDIENCE_ADULT: "General",
    }

    # TODO: Add remaining formats. Maybe there's a better place to
    # store this so it's easier to keep up-to-date.
    # There doesn't seem to be any particular vocabulary for this.
    FORMAT_TERMS: Mapping[tuple[str | None, str | None], str] = {
        (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM): "EPUB eBook",
        (
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
        ): "Adobe EPUB eBook",
        (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM): "PDF eBook",
        (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM): "Adobe PDF eBook",
    }

    def __init__(
        self,
        cm_url: str,
        library_short_name: str,
        web_client_urls: list[str],
        organization_code: str | None,
        include_summary: bool,
        include_genres: bool,
    ) -> None:
        self.cm_url = cm_url
        self.library_short_name = library_short_name
        self.web_client_urls = web_client_urls
        self.organization_code = organization_code
        self.include_summary = include_summary
        self.include_genres = include_genres

    def annotate_work_record(
        self,
        revised: bool,
        work: Work,
        active_license_pool: LicensePool,
        edition: Edition,
        identifier: Identifier,
    ) -> Record:
        """Add metadata from this work to a MARC record.

        :param revised: Whether this record is being revised.
        :param work: The Work whose record is being annotated.
        :param active_license_pool: Of all the LicensePools associated with this
           Work, the client has expressed interest in this one.
        :param edition: The Edition to use when associating bibliographic
           metadata with this entry.
        :param identifier: Of all the Identifiers associated with this
           Work, the client has expressed interest in this one.

        :return: A pymarc Record object.
        """
        record = Record(leader=self.leader(revised), force_utf8=True)
        self.add_control_fields(record, identifier, active_license_pool, edition)
        self.add_isbn(record, identifier)

        # TODO: The 240 and 130 fields are for translated works, so they can be grouped even
        #  though they have different titles. We do not group editions of the same work in
        #  different languages, so we can't use those yet.

        self.add_title(record, edition)
        self.add_contributors(record, edition)
        self.add_publisher(record, edition)
        self.add_physical_description(record, edition)
        self.add_audience(record, work)
        self.add_series(record, edition)
        self.add_system_details(record)
        self.add_ebooks_subject(record)
        self.add_distributor(record, active_license_pool)
        self.add_formats(record, active_license_pool)

        if self.organization_code:
            self.add_marc_organization_code(record, self.organization_code)

        if self.include_summary:
            self.add_summary(record, work)

        if self.include_genres:
            self.add_genres(record, work)

        self.add_web_client_urls(
            record,
            identifier,
            self.library_short_name,
            self.cm_url,
            self.web_client_urls,
        )

        return record

    @classmethod
    def leader(cls, revised: bool) -> str:
        # The record length is automatically updated once fields are added.
        initial_record_length = "00000"

        if revised:
            record_status = "c"  # Corrected or revised
        else:
            record_status = "n"  # New record

        # Distributors consistently seem to use type "a" - language material - for
        # ebooks, though there is also type "m" for computer files.
        record_type = "a"
        bibliographic_level = "m"  # Monograph/item

        leader = (
            initial_record_length + record_status + record_type + bibliographic_level
        )
        # Additional information about the record that's always the same.
        leader += "  2200000   4500"
        return leader

    @classmethod
    def add_control_fields(
        cls, record: Record, identifier: Identifier, pool: LicensePool, edition: Edition
    ) -> None:
        # Unique identifier for this record.
        record.add_field(Field(tag="001", data=identifier.urn))

        # Field 003 (MARC organization code) is library-specific, so it's added separately.

        record.add_field(Field(tag="005", data=utc_now().strftime("%Y%m%d%H%M%S.0")))

        # Field 006: m = computer file, d = the file is a document
        record.add_field(Field(tag="006", data="m        d        "))

        # Field 007: more details about electronic resource
        # Since this depends on the pool, it might be better not to cache it.
        # But it's probably not a huge problem if it's outdated.
        # File formats: a=one format, m=multiple formats, u=unknown
        if len(pool.delivery_mechanisms) == 1:
            file_formats_code = "a"
        else:
            file_formats_code = "m"
        record.add_field(
            Field(tag="007", data="cr cn ---" + file_formats_code + "nuuu")
        )

        # Field 008 (fixed-length data elements):
        data = utc_now().strftime("%y%m%d")
        publication_date = edition.issued or edition.published
        if publication_date:
            date_type = "s"  # single known date
            # Not using strftime because some years are pre-1900.
            date_value = "%04i" % publication_date.year
        else:
            date_type = "n"  # dates unknown
            date_value = "    "
        data += date_type + date_value
        data += "    "
        # TODO: Start tracking place of publication when available. Since we don't have
        # this yet, assume everything was published in the US.
        data += "xxu"
        data += "                 "
        language = "eng"
        if edition.language:
            language = LanguageCodes.string_to_alpha_3(edition.language)
        data += language
        data += "  "
        record.add_field(Field(tag="008", data=data))

    @classmethod
    def add_marc_organization_code(cls, record: Record, marc_org: str) -> None:
        record.add_field(Field(tag="003", data=marc_org))

    @classmethod
    def add_isbn(cls, record: Record, identifier: Identifier) -> None:
        # Add the ISBN if we have one.
        isbn = None
        if identifier.type == Identifier.ISBN:
            isbn = identifier
        if not isbn:
            _db = Session.object_session(identifier)
            identifier_ids = identifier.equivalent_identifier_ids()[identifier.id]
            isbn = (
                _db.query(Identifier)
                .filter(Identifier.type == Identifier.ISBN)
                .filter(Identifier.id.in_(identifier_ids))
                .order_by(Identifier.id)
                .first()
            )
        if isbn and isbn.identifier:
            record.add_field(
                Field(
                    tag="020",
                    indicators=[" ", " "],
                    subfields=[
                        Subfield(code="a", value=isbn.identifier),
                    ],
                )
            )

    @classmethod
    def add_title(cls, record: Record, edition: Edition) -> None:
        # Non-filing characters are used to indicate when the beginning of a title
        # should not be used in sorting. This code tries to identify them by comparing
        # the title and the sort_title.
        non_filing_characters = 0
        if (
            edition.title != edition.sort_title
            and edition.sort_title is not None
            and ("," in edition.sort_title)
        ):
            stemmed = edition.sort_title[: edition.sort_title.rindex(",")]
            if edition.title is None:
                cls.logger().warning(
                    "Edition %s has a sort title, but no title.", edition.id
                )
                non_filing_characters = 0
            else:
                non_filing_characters = edition.title.index(stemmed)
                # MARC only supports up to 9 non-filing characters, but if we got more
                # something is probably wrong anyway.
                if non_filing_characters > 9:
                    cls.logger().warning(
                        "Edition %s has %s non-filing characters, but MARC only supports up to 9.",
                        edition.id,
                        non_filing_characters,
                    )
                    non_filing_characters = 0

        subfields = [Subfield("a", str(edition.title or ""))]
        if edition.subtitle:
            subfields += [Subfield("b", str(edition.subtitle))]
        if edition.author:
            subfields += [Subfield("c", str(edition.author))]
        record.add_field(
            Field(
                tag="245",
                indicators=["0", str(non_filing_characters)],
                subfields=subfields,
            )
        )

    @classmethod
    def add_contributors(cls, record: Record, edition: Edition) -> None:
        """Create contributor fields for this edition.

        TODO: Use canonical names from LoC.
        """
        # If there's one author, use the 100 field.
        if edition.sort_author and len(edition.contributions) == 1:
            record.add_field(
                Field(
                    tag="100",
                    indicators=["1", " "],
                    subfields=[
                        Subfield("a", str(edition.sort_author)),
                    ],
                )
            )

        if len(edition.contributions) > 1:
            for contribution in edition.contributions:
                contributor = contribution.contributor
                if contributor.sort_name and contribution.role:
                    record.add_field(
                        Field(
                            tag="700",
                            indicators=["1", " "],
                            subfields=[
                                Subfield("a", str(contributor.sort_name)),
                                Subfield("e", contribution.role),
                            ],
                        )
                    )

    @classmethod
    def add_publisher(cls, record: Record, edition: Edition) -> None:
        if edition.publisher:
            publication_date = edition.issued or edition.published
            year = ""
            if publication_date:
                year = str(publication_date.year)
            record.add_field(
                Field(
                    tag="264",
                    indicators=[" ", "1"],
                    subfields=[
                        Subfield("a", "[Place of publication not identified]"),
                        Subfield("b", str(edition.publisher or "")),
                        Subfield("c", year),
                    ],
                )
            )

    @classmethod
    def add_distributor(cls, record: Record, pool: LicensePool) -> None:
        # Distributor
        record.add_field(
            Field(
                tag="264",
                indicators=[" ", "2"],
                subfields=[Subfield("b", str(pool.data_source.name))],
            )
        )

    @classmethod
    def add_physical_description(cls, record: Record, edition: Edition) -> None:
        # These 3xx fields are for a physical description of the item.
        if edition.medium == Edition.BOOK_MEDIUM:
            record.add_field(
                Field(
                    tag="300",
                    indicators=[" ", " "],
                    subfields=[
                        Subfield("a", "1 online resource"),
                    ],
                )
            )

            record.add_field(
                Field(
                    tag="336",
                    indicators=[" ", " "],
                    subfields=[
                        Subfield("a", "text"),
                        Subfield("b", "txt"),
                        Subfield("2", "rdacontent"),
                    ],
                )
            )
        elif edition.medium == Edition.AUDIO_MEDIUM:
            record.add_field(
                Field(
                    tag="300",
                    indicators=[" ", " "],
                    subfields=[
                        Subfield("a", "1 sound file"),
                        Subfield("b", "digital"),
                    ],
                )
            )

            record.add_field(
                Field(
                    tag="336",
                    indicators=[" ", " "],
                    subfields=[
                        Subfield("a", "spoken word"),
                        Subfield("b", "spw"),
                        Subfield("2", "rdacontent"),
                    ],
                )
            )

        record.add_field(
            Field(
                tag="337",
                indicators=[" ", " "],
                subfields=[
                    Subfield("a", "computer"),
                    Subfield("b", "c"),
                    Subfield("2", "rdamedia"),
                ],
            )
        )

        record.add_field(
            Field(
                tag="338",
                indicators=[" ", " "],
                subfields=[
                    Subfield("a", "online resource"),
                    Subfield("b", "cr"),
                    Subfield("2", "rdacarrier"),
                ],
            )
        )

        file_type = None
        if edition.medium == Edition.BOOK_MEDIUM:
            file_type = "text file"
        elif edition.medium == Edition.AUDIO_MEDIUM:
            file_type = "audio file"
        if file_type:
            record.add_field(
                Field(
                    tag="347",
                    indicators=[" ", " "],
                    subfields=[
                        Subfield("a", file_type),
                        Subfield("2", "rda"),
                    ],
                )
            )

        # Form of work
        form = None
        if edition.medium == Edition.BOOK_MEDIUM:
            form = "eBook"
        elif edition.medium == Edition.AUDIO_MEDIUM:
            # This field doesn't seem to be used for audio.
            pass
        if form:
            record.add_field(
                Field(
                    tag="380",
                    indicators=[" ", " "],
                    subfields=[
                        Subfield("a", "eBook"),
                        Subfield("2", "tlcgt"),
                    ],
                )
            )

    @classmethod
    def add_audience(cls, record: Record, work: Work) -> None:
        work_audience = work.audience or Classifier.AUDIENCE_ADULT
        audience = cls.AUDIENCE_TERMS.get(work_audience, "General")
        record.add_field(
            Field(
                tag="385",
                indicators=[" ", " "],
                subfields=[
                    Subfield("a", audience),
                    Subfield("2", "tlctarget"),
                ],
            )
        )

    @classmethod
    def add_series(cls, record: Record, edition: Edition) -> None:
        if edition.series:
            subfields = [Subfield("a", str(edition.series))]
            if edition.series_position:
                subfields.extend([Subfield("v", str(edition.series_position))])
            record.add_field(
                Field(
                    tag="490",
                    indicators=["0", " "],
                    subfields=subfields,
                )
            )

    @classmethod
    def add_system_details(cls, record: Record) -> None:
        record.add_field(
            Field(
                tag="538",
                indicators=[" ", " "],
                subfields=[Subfield("a", "Mode of access: World Wide Web.")],
            )
        )

    @classmethod
    def add_formats(cls, record: Record, pool: LicensePool) -> None:
        for lpdm in pool.delivery_mechanisms:
            dm = lpdm.delivery_mechanism
            format = cls.FORMAT_TERMS.get((dm.content_type, dm.drm_scheme))
            if format:
                record.add_field(
                    Field(
                        tag="538",
                        indicators=[" ", " "],
                        subfields=[
                            Subfield("a", format),
                        ],
                    )
                )

    @classmethod
    def add_summary(cls, record: Record, work: Work) -> None:
        summary = work.summary_text
        if summary:
            stripped = re.sub("<[^>]+?>", " ", summary)
            record.add_field(
                Field(
                    tag="520",
                    indicators=[" ", " "],
                    subfields=[Subfield("a", stripped)],
                )
            )

    @classmethod
    def add_genres(cls, record: Record, work: Work) -> None:
        """Create subject fields for this work."""
        genres = work.genres

        for genre in genres:
            record.add_field(
                Field(
                    tag="650",
                    indicators=["0", "7"],
                    subfields=[
                        Subfield("a", genre.name),
                        Subfield("2", "Library Simplified"),
                    ],
                )
            )

    @classmethod
    def add_ebooks_subject(cls, record: Record) -> None:
        # This is a general subject that can be added to all records.
        record.add_field(
            Field(
                tag="655",
                indicators=[" ", "0"],
                subfields=[
                    Subfield("a", "Electronic books."),
                ],
            )
        )

    @classmethod
    def add_web_client_urls(
        cls,
        record: Record,
        identifier: Identifier,
        library_short_name: str,
        cm_url: str,
        web_client_urls: list[str],
    ) -> None:
        qualified_identifier = urllib.parse.quote(
            f"{identifier.type}/{identifier.identifier}", safe=""
        )

        for web_client_base_url in web_client_urls:
            link = "{}/{}/works/{}".format(
                cm_url,
                library_short_name,
                qualified_identifier,
            )
            encoded_link = urllib.parse.quote(link, safe="")
            url = f"{web_client_base_url}/book/{encoded_link}"
            record.add_field(
                Field(
                    tag="856",
                    indicators=["4", "0"],
                    subfields=[Subfield(code="u", value=url)],
                )
            )


class MarcExporterSettings(BaseSettings):
    # This setting (in days) controls how often MARC files should be
    # automatically updated. Since the crontab in docker isn't easily
    # configurable, we can run a script daily but check this to decide
    # whether to do anything.
    update_frequency: NonNegativeInt = FormField(
        30,
        form=ConfigurationFormItem(
            label="Update frequency (in days)",
            type=ConfigurationFormItemType.NUMBER,
            required=True,
        ),
        alias="marc_update_frequency",
    )


class MarcExporterLibrarySettings(BaseSettings):
    # MARC organization codes are assigned by the
    # Library of Congress and can be found here:
    # http://www.loc.gov/marc/organizations/org-search.php
    organization_code: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            label="The MARC organization code for this library (003 field).",
            description="MARC organization codes are assigned by the Library of Congress.",
            type=ConfigurationFormItemType.TEXT,
        ),
        alias="marc_organization_code",
    )

    web_client_url: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            label="The base URL for the web catalog for this library, for the 856 field.",
            description="If using a library registry that provides a web catalog, this can be left blank.",
            type=ConfigurationFormItemType.TEXT,
        ),
        alias="marc_web_client_url",
    )

    include_summary: bool = FormField(
        False,
        form=ConfigurationFormItem(
            label="Include summaries in MARC records (520 field)",
            type=ConfigurationFormItemType.SELECT,
            options={"false": "Do not include summaries", "true": "Include summaries"},
        ),
    )

    include_genres: bool = FormField(
        False,
        form=ConfigurationFormItem(
            label="Include Palace Collection Manager genres in MARC records (650 fields)",
            type=ConfigurationFormItemType.SELECT,
            options={
                "false": "Do not include Palace Collection Manager genres",
                "true": "Include Palace Collection Manager genres",
            },
        ),
        alias="include_simplified_genres",
    )


class MARCExporter(
    HasLibraryIntegrationConfiguration[
        MarcExporterSettings, MarcExporterLibrarySettings
    ],
    LoggerMixin,
):
    """Turn a work into a record for a MARC file."""

    # The minimum size each piece of a multipart upload should be
    MINIMUM_UPLOAD_BATCH_SIZE_BYTES = 5 * 1024 * 1024  # 5MB

    def __init__(
        self,
        _db: Session,
        storage_service: S3Service,
    ):
        self._db = _db
        self.storage_service = storage_service

    @classmethod
    def label(cls) -> str:
        return "MARC Export"

    @classmethod
    def description(cls) -> str:
        return (
            "Export metadata into MARC files that can be imported into an ILS manually."
        )

    @classmethod
    def settings_class(cls) -> type[MarcExporterSettings]:
        return MarcExporterSettings

    @classmethod
    def library_settings_class(cls) -> type[MarcExporterLibrarySettings]:
        return MarcExporterLibrarySettings

    @classmethod
    def create_record(
        cls,
        revised: bool,
        work: Work,
        annotator: Annotator,
    ) -> Record | None:
        """Build a complete MARC record for a given work."""
        pool = work.active_license_pool()
        if not pool:
            return None

        edition = pool.presentation_edition
        identifier = pool.identifier

        return annotator.annotate_work_record(revised, work, pool, edition, identifier)

    @staticmethod
    def _date_to_string(date: datetime) -> str:
        return date.astimezone(pytz.UTC).strftime("%Y-%m-%d")

    def _file_key(
        self,
        uuid: UUID,
        library: Library,
        collection: Collection,
        creation_time: datetime,
        since_time: datetime | None = None,
    ) -> str:
        """The path to the hosted MARC file for the given library, collection,
        and date range."""
        root = "marc"
        short_name = str(library.short_name)
        creation = self._date_to_string(creation_time)

        if since_time:
            file_type = f"delta.{self._date_to_string(since_time)}.{creation}"
        else:
            file_type = f"full.{creation}"

        uuid_encoded = uuid_encode(uuid)
        collection_name = collection.name.replace(" ", "_")
        filename = f"{collection_name}.{file_type}.{uuid_encoded}.mrc"
        parts = [root, short_name, filename]
        return "/".join(parts)

    def query_works(
        self,
        collection: Collection,
        since_time: datetime | None,
        creation_time: datetime,
        batch_size: int,
    ) -> ScalarResult:
        query = (
            select(Work)
            .join(LicensePool)
            .join(Collection)
            .where(
                Collection.id == collection.id,
                Work.last_update_time <= creation_time,
            )
        )

        if since_time is not None:
            query = query.where(Work.last_update_time >= since_time)

        return self._db.execute(query).unique().yield_per(batch_size).scalars()

    def records(
        self,
        library: Library,
        collection: Collection,
        annotator: Annotator,
        *,
        creation_time: datetime,
        since_time: datetime | None = None,
        batch_size: int = 500,
    ) -> None:
        """
        Create and export a MARC file for the books in a collection.
        """
        uuid = uuid4()
        key = self._file_key(uuid, library, collection, creation_time, since_time)

        with self.storage_service.multipart(
            key,
            content_type=Representation.MARC_MEDIA_TYPE,
        ) as upload:
            this_batch = BytesIO()

            works = self.query_works(collection, since_time, creation_time, batch_size)
            for work in works:
                # Create a record for each work and add it to the MARC file in progress.
                record = self.create_record(
                    since_time is not None,
                    work,
                    annotator,
                )
                if record:
                    record_bytes = record.as_marc()
                    this_batch.write(record_bytes)
                    if (
                        this_batch.getbuffer().nbytes
                        >= self.MINIMUM_UPLOAD_BATCH_SIZE_BYTES
                    ):
                        # We've reached or exceeded the upload threshold.
                        # Upload one part of the multipart document.
                        upload.upload_part(this_batch.getvalue())
                        this_batch.seek(0)
                        this_batch.truncate()

            # Upload the final part of the multi-document, if
            # necessary.
            if this_batch.getbuffer().nbytes > 0:
                upload.upload_part(this_batch.getvalue())

        if upload.complete:
            create(
                self._db,
                MarcFile,
                id=uuid,
                library=library,
                collection=collection,
                created=creation_time,
                since=since_time,
                key=key,
            )
        else:
            if upload.exception:
                # Log the exception and move on to the next file. We will try again next script run.
                self.log.error(
                    f"Failed to upload MARC file for {library.short_name}/{collection.name}: {upload.exception}",
                    exc_info=upload.exception,
                )
            else:
                # There were no records to upload. This is not an error, but we should log it.
                self.log.info(
                    f"No MARC records to upload for {library.short_name}/{collection.name}."
                )
