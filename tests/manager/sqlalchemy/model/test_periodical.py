import pytest
from sqlalchemy.exc import IntegrityError

from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.periodical import (
    Periodical,
    PeriodicalVolume,
    SerialType,
)
from palace.manager.sqlalchemy.model.resource import Resource
from tests.fixtures.database import DatabaseTransactionFixture


class TestPeriodical:
    def test_create(self, db: DatabaseTransactionFixture) -> None:
        identifier, _ = Identifier.for_foreign_id(
            db.session, Identifier.ISSN, "0317-8471"
        )
        periodical = Periodical(
            identifier=identifier,
            serial_type=SerialType.MAGAZINE,
            title="The Palace Review",
            sort_title="Palace Review, The",
            publisher="Palace Press",
            language="eng",
            description="A magazine about libraries.",
        )
        db.session.add(periodical)
        db.session.flush()

        assert periodical.id is not None
        assert periodical.identifier == identifier
        assert periodical.serial_type == SerialType.MAGAZINE
        assert periodical.title == "The Palace Review"
        assert periodical.sort_title == "Palace Review, The"
        assert periodical.publisher == "Palace Press"
        assert periodical.language == "eng"
        assert periodical.description == "A magazine about libraries."

        # `created` is filled in for us; `updated` is left alone until
        # something changes the row.
        assert periodical.created is not None
        assert periodical.updated is None

        # The identifier can find its way back.
        assert identifier.periodicals == [periodical]

    def test_everything_but_the_id_is_optional(
        self, db: DatabaseTransactionFixture
    ) -> None:
        # We get magazines from feeds that tell us almost nothing about them,
        # so an empty Periodical is allowed.
        periodical = Periodical()
        db.session.add(periodical)
        db.session.flush()

        assert periodical.id is not None
        assert periodical.identifier is None
        assert periodical.serial_type is None
        assert periodical.title is None
        assert periodical.data_source is None

    @pytest.mark.parametrize(
        "serial_type,expected_value",
        [
            pytest.param(SerialType.MAGAZINE, "magazine", id="magazine"),
            pytest.param(SerialType.JOURNAL, "journal", id="journal"),
            pytest.param(SerialType.NEWSPAPER, "newspaper", id="newspaper"),
            pytest.param(SerialType.PERIODICAL, "periodical", id="periodical"),
        ],
    )
    def test_serial_type_round_trips(
        self,
        db: DatabaseTransactionFixture,
        serial_type: SerialType,
        expected_value: str,
    ) -> None:
        assert serial_type.value == expected_value

        periodical = Periodical(serial_type=serial_type)
        db.session.add(periodical)
        db.session.flush()
        db.session.expire(periodical)

        assert periodical.serial_type == serial_type

    def test_data_source_is_provenance_only(
        self, db: DatabaseTransactionFixture
    ) -> None:
        # Two periodicals can share an ISSN identifier across data sources.
        # Nothing in the schema ties identity to the data source.
        identifier, _ = Identifier.for_foreign_id(
            db.session, Identifier.ISSN, "0378-5955"
        )
        source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        periodical = Periodical(identifier=identifier, data_source=source)
        db.session.add(periodical)
        db.session.flush()

        assert periodical.data_source == source
        assert periodical in source.periodicals

    def test_cover(self, db: DatabaseTransactionFixture) -> None:
        resource = Resource(url="http://example.com/cover.png")
        periodical = Periodical(title="The Palace Review", cover=resource)
        db.session.add_all([resource, periodical])
        db.session.flush()

        assert periodical.cover == resource
        assert resource.cover_periodicals == [periodical]

    def test_volumes(self, db: DatabaseTransactionFixture) -> None:
        periodical = Periodical(title="The Palace Review")
        first = PeriodicalVolume(periodical=periodical, label="Volume 1", position=1)
        second = PeriodicalVolume(periodical=periodical, label="Volume 2", position=2)
        db.session.add_all([periodical, first, second])
        db.session.flush()

        assert set(periodical.volumes) == {first, second}

    def test_editions(self, db: DatabaseTransactionFixture) -> None:
        periodical = Periodical(title="The Palace Review")
        volume = PeriodicalVolume(periodical=periodical, label="Volume 1", position=1)
        edition = db.edition(title="March 2026")
        edition.periodical = periodical
        edition.periodical_volume = volume
        edition.issue_position = 3
        db.session.add_all([periodical, volume])
        db.session.flush()

        assert periodical.editions == [edition]
        assert volume.editions == [edition]
        assert edition.issue_position == 3

    def test_repr(self, db: DatabaseTransactionFixture) -> None:
        periodical = Periodical(title="The Palace Review")
        db.session.add(periodical)
        db.session.flush()

        assert repr(periodical) == (
            f"<Periodical id={periodical.id} title='The Palace Review' "
            f"identifier_id=None>"
        )


class TestPeriodicalVolume:
    def test_create(self, db: DatabaseTransactionFixture) -> None:
        periodical = Periodical(title="The Palace Review")
        volume = PeriodicalVolume(periodical=periodical, label="2026", position=4)
        db.session.add_all([periodical, volume])
        db.session.flush()

        assert volume.id is not None
        assert volume.periodical == periodical
        assert volume.periodical_id == periodical.id
        assert volume.label == "2026"
        assert volume.position == 4
        assert volume.created is not None
        assert volume.updated is None

    def test_periodical_is_required(self, db: DatabaseTransactionFixture) -> None:
        volume = PeriodicalVolume(label="2026")
        db.session.add(volume)
        with pytest.raises(IntegrityError):
            db.session.flush()

    def test_repr(self, db: DatabaseTransactionFixture) -> None:
        periodical = Periodical(title="The Palace Review")
        volume = PeriodicalVolume(periodical=periodical, label="2026")
        db.session.add_all([periodical, volume])
        db.session.flush()

        assert repr(volume) == (
            f"<PeriodicalVolume id={volume.id} "
            f"periodical_id={periodical.id} label='2026'>"
        )
