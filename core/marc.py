from __future__ import annotations

import re
from datetime import datetime
from io import BytesIO
from typing import Callable, Mapping, Optional, Tuple

from pydantic import NonNegativeInt
from pymarc import Field, Record, Subfield
from sqlalchemy.orm.session import Session

from core.classifier import Classifier
from core.external_search import ExternalSearchIndex, Filter, SortKeyPagination
from core.integration.base import HasLibraryIntegrationConfiguration
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.lane import BaseFacets, Lane, WorkList
from core.model import (
    CachedMARCFile,
    DeliveryMechanism,
    Edition,
    Identifier,
    Library,
    LicensePool,
    Representation,
    Work,
    get_one_or_create,
)
from core.service.storage.s3 import MultipartS3ContextManager, S3Service
from core.util import LanguageCodes
from core.util.datetime_helpers import utc_now
from core.util.log import LoggerMixin


class Annotator(LoggerMixin):
    """The Annotator knows how to add information about a Work to
    a MARC record."""

    marc_cache_field = Work.marc_record.name

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
    FORMAT_TERMS: Mapping[Tuple[Optional[str], Optional[str]], str] = {
        (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM): "EPUB eBook",
        (
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
        ): "Adobe EPUB eBook",
        (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM): "PDF eBook",
        (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM): "Adobe PDF eBook",
    }

    def annotate_work_record(
        self,
        work: Work,
        active_license_pool: LicensePool,
        edition: Edition,
        identifier: Identifier,
        record: Record,
        settings: MarcExporterLibrarySettings | None,
    ) -> None:
        """Add metadata from this work to a MARC record.

        :param work: The Work whose record is being annotated.
        :param active_license_pool: Of all the LicensePools associated with this
           Work, the client has expressed interest in this one.
        :param edition: The Edition to use when associating bibliographic
           metadata with this entry.
        :param identifier: Of all the Identifiers associated with this
           Work, the client has expressed interest in this one.
        :param record: A MARCRecord object to be annotated.
        """
        self.add_distributor(record, active_license_pool)
        self.add_formats(record, active_license_pool)

    @classmethod
    def leader(cls, work: Work) -> str:
        # The record length is automatically updated once fields are added.
        initial_record_length = "00000"

        record_status = "n"  # New record
        if getattr(work, cls.marc_cache_field):
            record_status = "c"  # Corrected or revised

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
        if isbn:
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
                indicators=["0", non_filing_characters],
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
    def add_simplified_genres(cls, record: Record, work: Work) -> None:
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


class MARCExporterFacets(BaseFacets):
    """A faceting object used to configure the search engine so that
    it only works updated since a certain time.
    """

    def __init__(self, start_time: Optional[datetime]):
        self.start_time = start_time

    def modify_search_filter(self, filter: Filter) -> None:
        filter.order = self.SORT_ORDER_TO_OPENSEARCH_FIELD_NAME[self.ORDER_LAST_UPDATE]
        filter.order_ascending = True
        filter.updated_after = self.start_time


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
    organization_code: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="The MARC organization code for this library (003 field).",
            description="MARC organization codes are assigned by the Library of Congress.",
            type=ConfigurationFormItemType.TEXT,
        ),
        alias="marc_organization_code",
    )

    web_client_url: Optional[str] = FormField(
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
    ]
):
    """Turn a work into a record for a MARC file."""

    # The minimum size each piece of a multipart upload should be
    MINIMUM_UPLOAD_BATCH_SIZE_BYTES = 5 * 1024 * 1024  # 5MB

    def __init__(
        self,
        _db: Session,
        library: Library,
        settings: MarcExporterSettings,
        library_settings: MarcExporterLibrarySettings,
    ):
        self._db = _db
        self.library = library
        self.settings = settings
        self.library_settings = library_settings

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
        work: Work,
        annotator: Annotator | Callable[[], Annotator],
        settings: MarcExporterSettings | None = None,
        library_settings: MarcExporterLibrarySettings | None = None,
        force_create: bool = False,
    ) -> Optional[Record]:
        """Build a complete MARC record for a given work."""
        if callable(annotator):
            annotator = annotator()

        pool = work.active_license_pool()
        if not pool:
            return None

        edition = pool.presentation_edition
        identifier = pool.identifier

        _db = Session.object_session(work)

        record = None
        existing_record = getattr(work, annotator.marc_cache_field)
        if existing_record and not force_create:
            record = Record(data=existing_record.encode("utf-8"), force_utf8=True)

        if not record:
            record = Record(leader=annotator.leader(work), force_utf8=True)
            annotator.add_control_fields(record, identifier, pool, edition)
            annotator.add_isbn(record, identifier)

            # TODO: The 240 and 130 fields are for translated works, so they can be grouped even
            #  though they have different titles. We do not group editions of the same work in
            #  different languages, so we can't use those yet.

            annotator.add_title(record, edition)
            annotator.add_contributors(record, edition)
            annotator.add_publisher(record, edition)
            annotator.add_physical_description(record, edition)
            annotator.add_audience(record, work)
            annotator.add_series(record, edition)
            annotator.add_system_details(record)
            annotator.add_ebooks_subject(record)

            data = record.as_marc()
            setattr(work, annotator.marc_cache_field, data.decode("utf8"))

        # Add additional fields that should not be cached.
        annotator.annotate_work_record(
            work, pool, edition, identifier, record, settings=library_settings
        )
        return record

    def _file_key(
        self,
        library: Library,
        lane: Lane | WorkList,
        end_time: datetime,
        start_time: Optional[datetime] = None,
    ) -> str:
        """The path to the hosted MARC file for the given library, lane,
        and date range."""
        root = str(library.short_name)
        if start_time:
            time_part = str(start_time) + "-" + str(end_time)
        else:
            time_part = str(end_time)
        parts = [root, time_part, lane.display_name]
        return "/".join(parts) + ".mrc"

    def records(
        self,
        lane: Lane | WorkList,
        annotator: Annotator | Callable[[], Annotator],
        storage_service: Optional[S3Service],
        start_time: Optional[datetime] = None,
        force_refresh: bool = False,
        search_engine: Optional[ExternalSearchIndex] = None,
        query_batch_size: int = 500,
    ) -> None:
        """
        Create and export a MARC file for the books in a lane.

        :param lane: The Lane to export books from.
        :param annotator: The Annotator to use when creating MARC records.
        :param storage_service: The storage service integration to use for MARC files.
        :param start_time: Only include records that were created or modified after this time.
        :param force_refresh: Create new records even when cached records are available.
        :param query_batch_size: Number of works to retrieve with a single Opensearch query.
        """

        # We store the content, if it's not empty. If it's empty, we create a CachedMARCFile
        # and Representation, but don't actually store it.
        if storage_service is None:
            raise Exception("No storage service is configured")

        search_engine = search_engine or ExternalSearchIndex(self._db)

        # End time is before we start the query, because if any records are changed
        # during the processing we may not catch them, and they should be handled
        # again on the next run.
        end_time = utc_now()

        facets = MARCExporterFacets(start_time=start_time)
        pagination = SortKeyPagination(size=query_batch_size)

        key = self._file_key(self.library, lane, end_time, start_time)

        with storage_service.multipart(
            key,
            content_type=Representation.MARC_MEDIA_TYPE,
        ) as upload:
            this_batch = BytesIO()
            while pagination is not None:
                # Retrieve one 'page' of works from the search index.
                works = lane.works(
                    self._db,
                    pagination=pagination,
                    facets=facets,
                    search_engine=search_engine,
                )
                for work in works:
                    # Create a record for each work and add it to the
                    # MARC file in progress.
                    record = self.create_record(
                        work,
                        annotator,
                        self.settings,
                        self.library_settings,
                        force_refresh,
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
                    self._upload_batch(this_batch, upload)
                    this_batch = BytesIO()
                pagination = pagination.next_page

            # Upload the final part of the multi-document, if
            # necessary.
            self._upload_batch(this_batch, upload)  # type: ignore[unreachable]

        representation, ignore = get_one_or_create(
            self._db,
            Representation,
            url=upload.url,
            media_type=Representation.MARC_MEDIA_TYPE,
        )
        representation.fetched_at = end_time
        if not upload.exception:
            cached, is_new = get_one_or_create(
                self._db,
                CachedMARCFile,
                library=self.library,
                lane=(lane if isinstance(lane, Lane) else None),
                start_time=start_time,
                create_method_kwargs=dict(representation=representation),
            )
            if not is_new:
                cached.representation = representation
            cached.end_time = end_time
            representation.set_as_mirrored(upload.url)
        else:
            representation.mirror_exception = str(upload.exception)

    def _upload_batch(self, output: BytesIO, upload: MultipartS3ContextManager) -> None:
        """Upload a batch of MARC records as one part of a multi-part upload."""
        content = output.getvalue()
        if content:
            upload.upload_part(content)
        output.close()
