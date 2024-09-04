import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from palace.manager.core.equivalents_coverage import (
    EquivalentIdentifiersCoverageProvider,
)
from palace.manager.sqlalchemy.model.identifier import Equivalency, Identifier
from palace.manager.sqlalchemy.model.time_tracking import (
    PlaytimeEntry,
    PlaytimeSummary,
    _isbn_for_identifier,
    _title_for_identifier,
)
from palace.manager.sqlalchemy.util import create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestPlaytimeEntries:
    def test_create(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        now = utc_now()
        entry, _ = create(
            db.session,
            PlaytimeEntry,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            identifier_str=identifier.urn,
            collection_name=collection.name,
            library_name=library.name,
            timestamp=now,
            total_seconds_played=30,
            tracking_id="tracking-id",
            loan_identifier="loan-id",
        )

        assert entry.identifier == identifier
        assert entry.collection == collection
        assert entry.library == library
        assert entry.identifier_str == identifier.urn
        assert entry.collection_name == collection.name
        assert entry.library_name == library.name
        assert entry.total_seconds_played == 30
        assert entry.timestamp == now
        assert entry.tracking_id == "tracking-id"
        assert entry.loan_identifier == "loan-id"

    def test_constraints(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        now = utc_now()
        loan_id = "loan-id"

        # > 60 second playtime (per minute)
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                PlaytimeEntry,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                identifier_str=identifier.urn,
                collection_name=collection.name,
                library_name=library.name,
                timestamp=now,
                total_seconds_played=61,
                tracking_id="tracking-id",
                loan_identifier=loan_id,
            )
        assert "max_total_seconds_played_constraint" in raised.exconly()
        db.session.rollback()

        # rollback means we need the data again
        collection = db.collection()
        library = db.library()
        identifier = db.identifier()
        identifier_2 = db.identifier()

        create(
            db.session,
            PlaytimeEntry,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            identifier_str=identifier.urn,
            collection_name=collection.name,
            library_name=library.name,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-0",
            loan_identifier=loan_id,
        )
        # Different identifier same tracking id is ok
        create(
            db.session,
            PlaytimeEntry,
            identifier_id=identifier_2.id,
            collection_id=collection.id,
            library_id=library.id,
            identifier_str=identifier_2.urn,
            collection_name=collection.name,
            library_name=library.name,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-0",
            loan_identifier="loan-id-2",
        )
        # Same identifier different tracking id is ok
        create(
            db.session,
            PlaytimeEntry,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            identifier_str=identifier.urn,
            collection_name=collection.name,
            library_name=library.name,
            timestamp=now,
            total_seconds_played=60,
            tracking_id="tracking-id-1",
            loan_identifier=loan_id,
        )
        with pytest.raises(IntegrityError) as raised:
            # Same identifier same tracking id is not ok
            create(
                db.session,
                PlaytimeEntry,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                identifier_str=identifier.urn,
                collection_name=collection.name,
                library_name=library.name,
                timestamp=now,
                total_seconds_played=60,
                tracking_id="tracking-id-0",
                loan_identifier=loan_id,
            )
        assert (
            f"Key (tracking_id, identifier_str, collection_name, library_name)=(tracking-id-0, {identifier.urn}, {collection.name}, {library.name}) already exists"
            in raised.exconly()
        )


class TestPlaytimeSummaries:
    def test_contraints(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        loan_id = "loan-id"

        create(
            db.session,
            PlaytimeSummary,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            identifier_str=identifier.urn,
            collection_name=collection.name,
            library_name=library.name,
            timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
            total_seconds_played=600,
            loan_identifier=loan_id,
        )

        # Same identifier string with same timestamp
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                PlaytimeSummary,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                identifier_str=identifier.urn,
                collection_name=collection.name,
                library_name=library.name,
                timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
                total_seconds_played=600,
                loan_identifier=loan_id,
            )
        assert (
            f'Key ("timestamp", identifier_str, collection_name, library_name, loan_identifier)=(2000-01-01 12:00:00+00, {identifier.urn}, {collection.name}, {library.name}, {loan_id}) already exists'
            in raised.exconly()
        )

        db.session.rollback()

        # timestamp not at the minute boundary
        with pytest.raises(IntegrityError) as raised:
            create(
                db.session,
                PlaytimeSummary,
                identifier_id=identifier.id,
                collection_id=collection.id,
                library_id=library.id,
                identifier_str=identifier.urn,
                collection_name=collection.name,
                library_name=library.name,
                timestamp=datetime.datetime(2000, 1, 1, 12, 00, 1),
                total_seconds_played=600,
                loan_identifier=loan_id,
            )
        assert "timestamp_minute_boundary_constraint" in raised.exconly()

    def test_cascades(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        loan_id = "loan-id"
        urn = identifier.urn
        entry, _ = create(
            db.session,
            PlaytimeSummary,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            identifier_str=identifier.urn,
            collection_name=collection.name,
            library_name=library.name,
            timestamp=datetime.datetime(2000, 1, 1, 12, 00, 00),
            total_seconds_played=600,
            loan_identifier=loan_id,
        )

        assert entry.identifier == identifier
        assert entry.identifier_str == urn

        db.session.delete(identifier)
        db.session.refresh(entry)

        assert entry.identifier_str == urn
        assert entry.identifier is None


class TestHelpers:
    @pytest.mark.parametrize(
        "id_key, equivalents, expected_isbn",
        [
            # If the identifier is an ISBN, we will not use an equivalency.
            [
                "i1",
                (("g1", "g2", 1), ("g2", "i1", 1), ("g1", "i2", 0.5)),
                "080442957X",
            ],
            [
                "i2",
                (("g1", "g2", 1), ("g2", "i1", 0.5), ("g1", "i2", 1)),
                "9788175257665",
            ],
            ["i1", (("i1", "i2", 200),), "080442957X"],
            ["i2", (("i2", "i1", 200),), "9788175257665"],
            # If identifier is not an ISBN, but has an equivalency that is, use the strongest match.
            [
                "g2",
                (("g1", "g2", 1), ("g2", "i1", 1), ("g1", "i2", 0.5)),
                "080442957X",
            ],
            [
                "g2",
                (("g1", "g2", 1), ("g2", "i1", 0.5), ("g1", "i2", 1)),
                "9788175257665",
            ],
            # If we don't find an equivalent ISBN identifier, then we should get None.
            ["g2", (), None],
            ["g1", (("g1", "g2", 1),), None],
            # If identifier is None, expect default value.
            [None, (), None],
        ],
    )
    def test__isbn_for_identifier(
        self,
        db: DatabaseTransactionFixture,
        id_key: str | None,
        equivalents: tuple[tuple[str, str, int | float]],
        expected_isbn: str,
    ):
        ids: dict[str, Identifier] = {
            "i1": db.identifier(
                identifier_type=Identifier.ISBN, foreign_id="080442957X"
            ),
            "i2": db.identifier(
                identifier_type=Identifier.ISBN, foreign_id="9788175257665"
            ),
            "g1": db.identifier(identifier_type=Identifier.GUTENBERG_ID),
            "g2": db.identifier(identifier_type=Identifier.GUTENBERG_ID),
        }
        equivalencies = [
            Equivalency(
                input_id=ids[equivalent[0]].id,
                output_id=ids[equivalent[1]].id,
                strength=equivalent[2],
            )
            for equivalent in equivalents
        ]
        test_identifier: Identifier | None = ids[id_key] if id_key is not None else None
        if test_identifier is not None:
            test_identifier.equivalencies = equivalencies

        # We're using the RecursiveEquivalencyCache, so must refresh it.
        EquivalentIdentifiersCoverageProvider(db.session).run()

        # Act
        result = _isbn_for_identifier(test_identifier)
        # Assert
        assert result == expected_isbn

    def test__title_for_identifier_multiple_editions(
        self,
        db: DatabaseTransactionFixture,
    ):
        identifier = db.identifier()
        test_title = "test title"
        e1 = db.edition(title=test_title)
        e2 = db.edition(title=test_title, data_source_name="another datasource")
        e1.primary_identifier = identifier
        e2.primary_identifier = identifier
        assert e1.id != e2.id
        result = _title_for_identifier(identifier)
        assert result == test_title

    def test__title_for_identifier_no_edition(
        self,
        db: DatabaseTransactionFixture,
    ):
        identifier = db.identifier()
        assert not _title_for_identifier(identifier)
