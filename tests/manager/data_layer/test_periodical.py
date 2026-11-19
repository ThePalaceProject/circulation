import pytest
from pydantic import ValidationError

from palace.opds.rwpm import SerialType as OpdsSerialType

from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.periodical import PeriodicalData
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.periodical import SerialType


class TestPeriodicalData:
    def test_defaults(self) -> None:
        # Every field is optional, so an empty PeriodicalData is valid.
        data = PeriodicalData()
        assert data.serial_type is None
        assert data.identifier is None
        assert data.title is None
        assert data.sort_title is None
        assert data.publisher is None
        assert data.language is None
        assert data.volume_label is None
        assert data.volume_position is None
        assert data.issue_position is None

    def test_constructor(self) -> None:
        identifier = IdentifierData(type=Identifier.ISSN, identifier="0017-789X")
        data = PeriodicalData(
            serial_type=SerialType.MAGAZINE,
            identifier=identifier,
            title="Harper's Magazine",
            sort_title="Harper's Magazine",
            publisher="Harper's Magazine Foundation",
            language="eng",
            volume_label="Volume 350",
            volume_position=350,
            issue_position=3,
        )
        assert data.serial_type is SerialType.MAGAZINE
        assert data.identifier == identifier
        assert data.title == "Harper's Magazine"
        assert data.sort_title == "Harper's Magazine"
        assert data.publisher == "Harper's Magazine Foundation"
        assert data.language == "eng"
        assert data.volume_label == "Volume 350"
        assert data.volume_position == 350
        assert data.issue_position == 3

    def test_immutable(self) -> None:
        data = PeriodicalData(title="Harper's Magazine")
        with pytest.raises(ValidationError):
            data.title = "The Atlantic"  # type: ignore[misc] # the point of the test

    def test_hash(self) -> None:
        # PeriodicalData is hashable, including the nested identifier.
        hash(
            PeriodicalData(
                title="Harper's Magazine",
                identifier=IdentifierData(type=Identifier.ISSN, identifier="0017-789X"),
            )
        )

    def test_serial_type_matches_the_opds_parser(self) -> None:
        # The OPDS parser has its own copy of this enum, and an extractor
        # passes what it read straight through. Every member of that copy has
        # to be a value we can store, so that adding one there and not here
        # fails this test rather than every feed that uses it.
        for opds_serial_type in OpdsSerialType:
            assert PeriodicalData(
                serial_type=opds_serial_type
            ).serial_type == SerialType(opds_serial_type)

        # Anything else is rejected.
        with pytest.raises(ValidationError):
            PeriodicalData(serial_type="comic")

    @pytest.mark.parametrize(
        "given,expected",
        [
            pytest.param("en", "eng", id="two_letter_code"),
            pytest.param("eng", "eng", id="three_letter_code"),
            pytest.param("English", "eng", id="name"),
            pytest.param(None, None, id="nothing"),
        ],
    )
    def test_language_is_normalized(
        self, given: str | None, expected: str | None
    ) -> None:
        # A magazine's language is stored the same way its issues' language
        # is, so the two never disagree over the same feed.
        assert PeriodicalData(language=given).language == expected

    @pytest.mark.parametrize(
        "position",
        [
            pytest.param(0, id="zero"),
            pytest.param(14, id="whole_number"),
            pytest.param(14.1, id="decimal"),
            pytest.param(20261124, id="date_as_number"),
        ],
    )
    def test_issue_position_accepts_decimals(self, position: int | float) -> None:
        # An issue sitting between two numbered issues needs a decimal to sort
        # in the right place, so we take one.
        assert PeriodicalData(issue_position=position).issue_position == position
