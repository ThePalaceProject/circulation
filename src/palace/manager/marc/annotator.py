from __future__ import annotations

import re
import urllib.parse
from collections.abc import Mapping, Sequence

from pymarc import Field, Indicators, Record, Subfield
from sqlalchemy.orm import Session

from palace.manager.core.classifier import Classifier
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, LicensePool
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.log import LoggerMixin


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

    @classmethod
    def marc_record(cls, work: Work, license_pool: LicensePool) -> Record:
        edition = license_pool.presentation_edition
        identifier = license_pool.identifier

        record = cls._record()
        cls.add_control_fields(record, identifier, license_pool, edition)
        cls.add_isbn(record, identifier)

        # TODO: The 240 and 130 fields are for translated works, so they can be grouped even
        #  though they have different titles. We do not group editions of the same work in
        #  different languages, so we can't use those yet.

        cls.add_title(record, edition)
        cls.add_contributors(record, edition)
        cls.add_publisher(record, edition)
        cls.add_physical_description(record, edition)
        cls.add_audience(record, work)
        cls.add_series(record, edition)
        cls.add_system_details(record)
        cls.add_ebooks_subject(record)
        cls.add_distributor(record, license_pool)
        cls.add_formats(record, license_pool)
        cls.add_summary(record, work)
        cls.add_genres(record, work)

        return record

    @classmethod
    def library_marc_record(
        cls,
        record: Record,
        identifier: Identifier,
        base_url: str,
        library_short_name: str,
        web_client_urls: Sequence[str],
        organization_code: str | None,
        include_summary: bool,
        include_genres: bool,
    ) -> Record:
        record = cls._copy_record(record)

        if organization_code:
            cls.add_marc_organization_code(record, organization_code)

        fields_to_remove = []

        if not include_summary:
            fields_to_remove.append("520")

        if not include_genres:
            fields_to_remove.append("650")

        if fields_to_remove:
            record.remove_fields(*fields_to_remove)

        cls.add_web_client_urls(
            record,
            identifier,
            library_short_name,
            base_url,
            web_client_urls,
        )

        return record

    @classmethod
    def _record(cls, leader: str | None = None) -> Record:
        leader = leader or cls.leader()
        return Record(leader=leader, force_utf8=True)

    @classmethod
    def _copy_record(cls, record: Record) -> Record:
        copied = cls._record(record.leader)
        copied.add_field(*record.get_fields())
        return copied

    @classmethod
    def set_revised(cls, record: Record, revised: bool = True) -> Record:
        record.leader.record_status = "c" if revised else "n"
        return record

    @classmethod
    def leader(cls, revised: bool = False) -> str:
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
                    indicators=Indicators(" ", " "),
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
                indicators=Indicators("0", str(non_filing_characters)),
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
                    indicators=Indicators("1", " "),
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
                            indicators=Indicators("1", " "),
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
                    indicators=Indicators(" ", "1"),
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
                indicators=Indicators(" ", "2"),
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
                    indicators=Indicators(" ", " "),
                    subfields=[
                        Subfield("a", "1 online resource"),
                    ],
                )
            )

            record.add_field(
                Field(
                    tag="336",
                    indicators=Indicators(" ", " "),
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
                    indicators=Indicators(" ", " "),
                    subfields=[
                        Subfield("a", "1 sound file"),
                        Subfield("b", "digital"),
                    ],
                )
            )

            record.add_field(
                Field(
                    tag="336",
                    indicators=Indicators(" ", " "),
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
                indicators=Indicators(" ", " "),
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
                indicators=Indicators(" ", " "),
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
                    indicators=Indicators(" ", " "),
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
                    indicators=Indicators(" ", " "),
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
                indicators=Indicators(" ", " "),
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
                    indicators=Indicators("0", " "),
                    subfields=subfields,
                )
            )

    @classmethod
    def add_system_details(cls, record: Record) -> None:
        record.add_field(
            Field(
                tag="538",
                indicators=Indicators(" ", " "),
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
                        indicators=Indicators(" ", " "),
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
                    indicators=Indicators(" ", " "),
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
                    indicators=Indicators("0", "7"),
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
                indicators=Indicators(" ", "0"),
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
        base_url: str,
        web_client_urls: Sequence[str],
    ) -> None:
        qualified_identifier = urllib.parse.quote(
            f"{identifier.type}/{identifier.identifier}", safe=""
        )
        link = "{}/{}/works/{}".format(
            base_url,
            library_short_name,
            qualified_identifier,
        )
        encoded_link = urllib.parse.quote(link, safe="")

        for web_client_base_url in web_client_urls:
            url = f"{web_client_base_url}/book/{encoded_link}"
            record.add_field(
                Field(
                    tag="856",
                    indicators=Indicators("4", "0"),
                    subfields=[Subfield(code="u", value=url)],
                )
            )
