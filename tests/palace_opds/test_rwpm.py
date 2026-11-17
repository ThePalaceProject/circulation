import json
from typing import Any

import pytest

from palace.opds.opds2 import PublicationFeed
from palace.opds.rwpm import BelongsTo, Metadata, SerialReference, SerialType

from tests.fixtures.files import PeriodicalFilesFixture

CONFORMANT = "magazine_with_volume.json"
TOLERATED = "schema_periodical_flattened_volume.json"


class TestBelongsTo:
    @pytest.mark.parametrize(
        "key, expected_type",
        [
            pytest.param("magazine", SerialType.MAGAZINE, id="magazine"),
            pytest.param("journal", SerialType.JOURNAL, id="journal"),
            pytest.param("newspaper", SerialType.NEWSPAPER, id="newspaper"),
            pytest.param("periodical", SerialType.PERIODICAL, id="periodical"),
            # Not an RWPM term. Feeds are asked not to send it, and the ones
            # that do are always sending us a magazine.
            pytest.param(
                "schema:Periodical", SerialType.MAGAZINE, id="schema-periodical"
            ),
            pytest.param(
                "http://schema.org/Periodical",
                SerialType.MAGAZINE,
                id="schema-org-http",
            ),
            pytest.param(
                "https://schema.org/Periodical",
                SerialType.MAGAZINE,
                id="schema-org-https",
            ),
        ],
    )
    def test_serial_keys(self, key: str, expected_type: SerialType) -> None:
        """Every key we accept lands on the same canonical serial."""
        belongs_to = BelongsTo.model_validate(
            {
                key: {
                    "name": "Example Science Monthly",
                    "identifier": "urn:issn:1234-5679",
                }
            }
        )

        assert belongs_to.serial_type == expected_type
        assert belongs_to.serial is not None
        assert belongs_to.serial.name == "Example Science Monthly"
        assert belongs_to.serial.identifier == "urn:issn:1234-5679"

    def test_no_serial(self) -> None:
        """A publication that is not an issue of anything says so.

        A series is not a serial. A book in a series uses the same key, so
        reading it as a serial would turn every one of them into a magazine.
        """
        belongs_to = BelongsTo.model_validate(
            {"series": {"name": "Example Comics Adventures", "position": 1}}
        )

        assert belongs_to.serial is None
        assert belongs_to.serial_type is None
        assert belongs_to.volume is None
        assert belongs_to.serials == ()
        assert belongs_to.series[0].name == "Example Comics Adventures"

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("Example Science Monthly", id="string"),
            pytest.param({"name": "Example Science Monthly"}, id="object"),
            pytest.param([{"name": "Example Science Monthly"}], id="array-of-objects"),
            pytest.param(["Example Science Monthly"], id="array-of-strings"),
        ],
    )
    def test_single_or_array(self, value: Any) -> None:
        """A serial can arrive on its own or in a list."""
        belongs_to = BelongsTo.model_validate({"magazine": value})

        assert belongs_to.serial_type == SerialType.MAGAZINE
        assert belongs_to.serial is not None
        assert belongs_to.serial.name == "Example Science Monthly"

    def test_volume_in_own_key(self) -> None:
        """The volume in a key of its own, which is what we ask feeds for."""
        belongs_to = BelongsTo.model_validate(
            {
                "magazine": {"name": "Example Science Monthly"},
                "volume": {"name": "2026", "position": 2026},
            }
        )

        assert belongs_to.volume == "2026"

    def test_volume_as_string(self) -> None:
        belongs_to = BelongsTo.model_validate(
            {"journal": "Journal of Example Studies", "volume": "42"}
        )

        assert belongs_to.volume == "42"

    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param("2026", "2026", id="string"),
            pytest.param(2026, "2026", id="number"),
            pytest.param(2026.0, "2026", id="whole-number-written-as-a-decimal"),
        ],
    )
    def test_volume_flattened_onto_serial(self, value: Any, expected: str) -> None:
        """Some feeds put the volume on the serial, as a plain value."""
        belongs_to = BelongsTo.model_validate(
            {"magazine": [{"name": "Example Science Monthly", "volume": value}]}
        )

        assert belongs_to.volume == expected

    def test_volume_own_key_wins(self) -> None:
        belongs_to = BelongsTo.model_validate(
            {
                "magazine": {"name": "Example Science Monthly", "volume": 2025},
                "volume": "2026",
            }
        )

        assert belongs_to.volume == "2026"

    def test_no_volume(self) -> None:
        """Most consumer magazines have no volume, and must not be given one."""
        belongs_to = BelongsTo.model_validate(
            {"magazine": {"name": "Example Science Monthly"}}
        )

        assert belongs_to.volume is None

    @pytest.mark.parametrize(
        "position, expected_type",
        [
            pytest.param(11, int, id="issue-number"),
            pytest.param(20261124, int, id="date-encoded"),
            pytest.param(14.1, float, id="decimal-between-two-issues"),
            pytest.param(0.25, float, id="decimal-before-the-first-issue"),
        ],
    )
    def test_position(self, position: Any, expected_type: type) -> None:
        """
        An issue's position may be a decimal, for issues that sit between two
        numbered ones. We keep the number as it was sent, because it is only a
        sort key.
        """
        belongs_to = BelongsTo.model_validate(
            {"magazine": {"name": "Example Science Monthly", "position": position}}
        )

        assert belongs_to.serial is not None
        assert belongs_to.serial.position == position
        assert isinstance(belongs_to.serial.position, expected_type)

    def test_specific_key_wins(self) -> None:
        """A feed sending both a specific and a generic key is read as the specific one."""
        belongs_to = BelongsTo.model_validate(
            {
                "periodical": {"name": "Generic"},
                "journal": {"name": "Journal of Example Studies"},
            }
        )

        assert belongs_to.serial_type == SerialType.JOURNAL
        assert belongs_to.serial is not None
        assert belongs_to.serial.name == "Journal of Example Studies"
        assert [str(serial.name) for serial in belongs_to.serials] == [
            "Journal of Example Studies",
            "Generic",
        ]

    def test_serial_reference_fields(self) -> None:
        """A serial reference is a contributor, so it keeps the contributor fields."""
        serial = SerialReference.model_validate(
            {
                "name": "Example Science Monthly",
                "sortAs": "Example Science Monthly",
                "identifier": "urn:issn:1234-5679",
                "position": 11,
                "volume": 2026,
            }
        )

        assert serial.name == "Example Science Monthly"
        assert serial.sort_as == "Example Science Monthly"
        assert serial.identifier == "urn:issn:1234-5679"
        assert serial.position == 11
        assert serial.volume == 2026

    def test_empty(self) -> None:
        """An empty belongsTo serializes to nothing, so existing feeds are unchanged."""
        assert BelongsTo().serialize() == {}

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(
                {
                    "magazine": {
                        "name": "Example Science Monthly",
                        "identifier": "urn:issn:1234-5679",
                        "position": 11,
                    },
                    "volume": {"name": "2026", "position": 2026},
                },
                id="conformant",
            ),
            pytest.param(
                {
                    "schema:Periodical": [
                        {"name": "Example Science Monthly", "volume": 2026}
                    ]
                },
                id="schema-periodical-with-flattened-volume",
            ),
            pytest.param(
                {"magazine": {"name": "Example Comics Adventures", "position": 14.1}},
                id="decimal-position",
            ),
        ],
    )
    def test_round_trip(self, data: dict[str, Any]) -> None:
        """What we read, we write back under the key it came in on, unchanged."""
        belongs_to = BelongsTo.model_validate(data)

        serialized = belongs_to.serialize()
        assert serialized == data
        assert BelongsTo.model_validate(serialized).volume == belongs_to.volume


class TestMetadata:
    def test_type_is_a_free_string(self) -> None:
        """
        An issue sent as a Book still parses, and is still an issue.

        Feeds are asked to type an issue as a PublicationIssue, but we work out
        that something is an issue from its belongsTo, not from its type.
        """
        metadata = Metadata.model_validate(
            {
                "@type": "http://schema.org/Book",
                "title": "November 2026",
                "belongsTo": {"magazine": [{"name": "Example Science Monthly"}]},
            }
        )

        assert metadata.type == "http://schema.org/Book"
        assert metadata.belongs_to.serial_type == SerialType.MAGAZINE


class TestPeriodicalFeeds:
    """Whole feeds, in each of the shapes a serial gets written in."""

    @pytest.mark.parametrize(
        "filename",
        [CONFORMANT, TOLERATED],
        ids=["conformant", "schema-periodical-flattened-volume"],
    )
    def test_canonical_serial(
        self, filename: str, periodical_files_fixture: PeriodicalFilesFixture
    ) -> None:
        """
        One feed uses the magazine key with the volume beside it, the other the
        schema.org key with the volume flattened onto the magazine. Both end up
        as the same magazine, the same issue and the same volume.
        """
        feed = PublicationFeed.model_validate_json(
            periodical_files_fixture.sample_data(filename)
        )
        assert len(feed.publications) == 1

        belongs_to = feed.publications[0].metadata.belongs_to
        serial = belongs_to.serial
        assert serial is not None
        assert serial.name == "Example Science Monthly"
        assert serial.sort_as == "Example Science Monthly"
        assert serial.identifier == "urn:issn:1234-5679"
        assert serial.position == 11
        assert belongs_to.volume == "2026"
        assert belongs_to.serial_type == SerialType.MAGAZINE

    @pytest.mark.parametrize(
        "filename",
        [CONFORMANT, TOLERATED],
        ids=["conformant", "schema-periodical-flattened-volume"],
    )
    def test_round_trip(
        self, filename: str, periodical_files_fixture: PeriodicalFilesFixture
    ) -> None:
        """Serializing a parsed feed gives back the belongsTo we read."""
        raw = json.loads(periodical_files_fixture.sample_data(filename))
        feed = PublicationFeed.model_validate_json(
            periodical_files_fixture.sample_data(filename)
        )

        serialized = feed.serialize()
        assert (
            serialized["publications"][0]["metadata"]["belongsTo"]
            == raw["publications"][0]["metadata"]["belongsTo"]
        )

        reparsed = PublicationFeed.model_validate(serialized)
        assert (
            reparsed.publications[0].metadata.belongs_to.serial
            == feed.publications[0].metadata.belongs_to.serial
        )
