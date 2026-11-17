import json
from typing import Any

import pytest

from palace.opds.opds2 import PublicationFeed
from palace.opds.rwpm import BelongsTo, Metadata, SerialReference, SerialType

from tests.fixtures.files import PeriodicalFilesFixture


class TestBelongsTo:
    @pytest.mark.parametrize(
        "key, expected_type",
        [
            pytest.param("magazine", SerialType.MAGAZINE, id="magazine"),
            pytest.param("journal", SerialType.JOURNAL, id="journal"),
            pytest.param("newspaper", SerialType.NEWSPAPER, id="newspaper"),
            pytest.param("periodical", SerialType.PERIODICAL, id="periodical"),
            pytest.param(
                "schema:Periodical", SerialType.PERIODICAL, id="schema-periodical"
            ),
            pytest.param(
                "schema:periodical",
                SerialType.PERIODICAL,
                id="schema-periodical-lowercase",
            ),
            pytest.param(
                "http://schema.org/Periodical",
                SerialType.PERIODICAL,
                id="schema-org-http",
            ),
            pytest.param(
                "https://schema.org/Periodical",
                SerialType.PERIODICAL,
                id="schema-org-https",
            ),
        ],
    )
    def test_serial_keys(self, key: str, expected_type: SerialType) -> None:
        """Every key we accept lands on the same canonical serial."""
        belongs_to = BelongsTo.model_validate(
            {key: {"name": "Palace Monthly", "identifier": "urn:issn:1234-5679"}}
        )

        assert belongs_to.serial_type == expected_type
        assert belongs_to.serial is not None
        assert belongs_to.serial.name == "Palace Monthly"
        assert belongs_to.serial.identifier == "urn:issn:1234-5679"

    def test_no_serial(self) -> None:
        """A publication that is not an issue of anything says so."""
        belongs_to = BelongsTo.model_validate(
            {"series": {"name": "The Lord of the Rings", "position": 2}}
        )

        assert belongs_to.serial is None
        assert belongs_to.serial_type is None
        assert belongs_to.volume is None
        assert belongs_to.serials == ()
        assert belongs_to.series[0].name == "The Lord of the Rings"

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("Palace Monthly", id="string"),
            pytest.param({"name": "Palace Monthly"}, id="object"),
            pytest.param([{"name": "Palace Monthly"}], id="array-of-objects"),
            pytest.param(["Palace Monthly"], id="array-of-strings"),
        ],
    )
    def test_single_or_array(self, value: Any) -> None:
        """A serial can arrive on its own or in a list."""
        belongs_to = BelongsTo.model_validate({"magazine": value})

        assert belongs_to.serial_type == SerialType.MAGAZINE
        assert belongs_to.serial is not None
        assert belongs_to.serial.name == "Palace Monthly"

    def test_volume_in_own_key(self) -> None:
        belongs_to = BelongsTo.model_validate(
            {"journal": {"name": "Annals"}, "volume": {"name": "12", "position": 12}}
        )

        assert belongs_to.volume == "12"
        assert belongs_to.volumes[0].position == 12

    def test_volume_as_string(self) -> None:
        belongs_to = BelongsTo.model_validate({"journal": "Annals", "volume": "12"})

        assert belongs_to.volume == "12"

    def test_volume_flattened_onto_serial(self) -> None:
        """Some feeds put the volume on the serial instead of in its own key."""
        belongs_to = BelongsTo.model_validate(
            {"magazine": [{"name": "Palace Monthly", "volume": "12"}]}
        )

        assert belongs_to.volume == "12"

    def test_volume_own_key_wins(self) -> None:
        belongs_to = BelongsTo.model_validate(
            {"magazine": {"name": "Palace Monthly", "volume": "11"}, "volume": "12"}
        )

        assert belongs_to.volume == "12"

    def test_specific_key_wins(self) -> None:
        """A feed sending both a specific and a generic key is read as the specific one."""
        belongs_to = BelongsTo.model_validate(
            {
                "periodical": {"name": "Generic"},
                "magazine": {"name": "Palace Monthly"},
            }
        )

        assert belongs_to.serial_type == SerialType.MAGAZINE
        assert belongs_to.serial is not None
        assert belongs_to.serial.name == "Palace Monthly"
        assert [str(serial.name) for serial in belongs_to.serials] == [
            "Palace Monthly",
            "Generic",
        ]

    def test_serial_reference_fields(self) -> None:
        """A serial reference is a contributor, so it keeps the contributor fields."""
        serial = SerialReference.model_validate(
            {
                "name": "Palace Monthly",
                "sortAs": "Palace Monthly",
                "identifier": "urn:issn:1234-5679",
                "position": 3,
                "volume": "12",
            }
        )

        assert serial.name == "Palace Monthly"
        assert serial.sort_as == "Palace Monthly"
        assert serial.identifier == "urn:issn:1234-5679"
        assert serial.position == 3
        assert serial.volume == "12"

    def test_empty(self) -> None:
        """An empty belongsTo serializes to nothing, so existing feeds are unchanged."""
        assert BelongsTo().serialize() == {}

    def test_round_trip(self) -> None:
        """What we read, we write back under the key it came in on."""
        data = {
            "schema:Periodical": {
                "name": "Palace Monthly",
                "identifier": "urn:issn:1234-5679",
            },
            "volume": "12",
        }
        belongs_to = BelongsTo.model_validate(data)

        serialized = belongs_to.serialize()
        assert serialized == data
        assert BelongsTo.model_validate(serialized).serial_type == SerialType.PERIODICAL


class TestMetadata:
    def test_type_is_a_free_string(self) -> None:
        """An issue sent as a Book still validates."""
        metadata = Metadata.model_validate(
            {
                "@type": "http://schema.org/Book",
                "title": "Palace Monthly, March 2026",
                "belongsTo": {"magazine": {"name": "Palace Monthly"}},
            }
        )

        assert metadata.type == "http://schema.org/Book"
        assert metadata.belongs_to.serial_type == SerialType.MAGAZINE


class TestPeriodicalFeeds:
    """Whole feeds, in each of the shapes we have seen a serial written in."""

    @pytest.mark.parametrize(
        "filename, expected_type",
        [
            pytest.param(
                "serial_as_schema_periodical.json",
                SerialType.PERIODICAL,
                id="schema-periodical-key",
            ),
            pytest.param(
                "serial_as_magazine_array.json",
                SerialType.MAGAZINE,
                id="magazine-array",
            ),
        ],
    )
    def test_canonical_serial(
        self,
        filename: str,
        expected_type: SerialType,
        periodical_files_fixture: PeriodicalFilesFixture,
    ) -> None:
        """
        One feed names the serial with the schema.org key and gives the volume
        its own key; the other uses a magazine array with the volume flattened
        onto it. Both end up as the same magazine, issue and volume.
        """
        feed = PublicationFeed.model_validate_json(
            periodical_files_fixture.sample_data(filename)
        )
        assert len(feed.publications) == 1

        belongs_to = feed.publications[0].metadata.belongs_to
        serial = belongs_to.serial
        assert serial is not None
        assert serial.name == "Palace Monthly"
        assert serial.identifier == "urn:issn:1234-5679"
        assert serial.position == 3
        assert belongs_to.volume == "12"
        assert belongs_to.serial_type == expected_type

    @pytest.mark.parametrize(
        "filename",
        ["serial_as_schema_periodical.json", "serial_as_magazine_array.json"],
        ids=["schema-periodical-key", "magazine-array"],
    )
    def test_round_trip(
        self, filename: str, periodical_files_fixture: PeriodicalFilesFixture
    ) -> None:
        """Serializing a parsed feed gives back the document we read."""
        raw = periodical_files_fixture.sample_data(filename)
        feed = PublicationFeed.model_validate_json(raw)

        serialized = feed.serialize()
        assert serialized == json.loads(raw)

        reparsed = PublicationFeed.model_validate(serialized)
        assert (
            reparsed.publications[0].metadata.belongs_to.serial
            == feed.publications[0].metadata.belongs_to.serial
        )
