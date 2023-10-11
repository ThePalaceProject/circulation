import datetime
from unittest.mock import PropertyMock, create_autospec

import pytest

from core.model import PresentationCalculationPolicy
from core.model.constants import MediaTypes
from core.model.datasource import DataSource
from core.model.edition import Edition
from core.model.identifier import (
    Identifier,
    ProQuestIdentifierParser,
    RecursiveEquivalencyCache,
)
from core.model.resource import Hyperlink
from tests.core.models.test_coverage import ExampleEquivalencyCoverageRecordFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestIdentifier:
    def test_for_foreign_id(self, db: DatabaseTransactionFixture):
        identifier_type = Identifier.ISBN
        isbn = "3293000061"

        # Getting the data automatically creates a database record.
        identifier, was_new = Identifier.for_foreign_id(
            db.session, identifier_type, isbn
        )
        assert Identifier.ISBN == identifier.type
        assert isbn == identifier.identifier
        assert True == was_new

        # If we get it again we get the same data, but it's no longer new.
        identifier2, was_new = Identifier.for_foreign_id(
            db.session, identifier_type, isbn
        )
        assert identifier == identifier2
        assert False == was_new

        # If we pass in no data we get nothing back.
        assert None == Identifier.for_foreign_id(db.session, None, None)

    def test_for_foreign_id_by_deprecated_type(self, db: DatabaseTransactionFixture):
        threem_id, is_new = Identifier.for_foreign_id(
            db.session, "3M ID", db.fresh_str()
        )
        assert Identifier.BIBLIOTHECA_ID == threem_id.type
        assert Identifier.BIBLIOTHECA_ID != "3M ID"

    def test_for_foreign_id_rejects_invalid_identifiers(
        self, db: DatabaseTransactionFixture
    ):
        with pytest.raises(ValueError) as excinfo:
            Identifier.for_foreign_id(db.session, Identifier.BIBLIOTHECA_ID, "foo/bar")
        assert '"foo/bar" is not a valid Bibliotheca ID.' in str(excinfo.value)

    def test_valid_as_foreign_identifier(self, db: DatabaseTransactionFixture):
        m = Identifier.valid_as_foreign_identifier

        assert True == m(Identifier.BIBLIOTHECA_ID, "bhhot389")
        assert False == m(Identifier.BIBLIOTHECA_ID, "bhhot389/open_book")
        assert False == m(Identifier.BIBLIOTHECA_ID, "bhhot389,bhhot389")

        assert True == m(Identifier.BIBLIOTHECA_ID, "0015142259")
        assert False == m(Identifier.BIBLIOTHECA_ID, "0015142259,0015187940")

    def test_for_foreign_id_without_autocreate(self, db: DatabaseTransactionFixture):
        identifier_type = Identifier.ISBN
        isbn = db.fresh_str()

        # We don't want to auto-create a database record, so we set
        # autocreate=False
        identifier, was_new = Identifier.for_foreign_id(
            db.session, identifier_type, isbn, autocreate=False
        )
        assert None == identifier
        assert False == was_new

    def test_from_asin(self, db: DatabaseTransactionFixture):
        isbn10 = "1449358063"
        isbn13 = "9781449358068"
        asin = "B0088IYM3C"
        isbn13_with_dashes = "978-144-935-8068"

        i_isbn10, new1 = Identifier.from_asin(db.session, isbn10)
        i_isbn13, new2 = Identifier.from_asin(db.session, isbn13)
        i_asin, new3 = Identifier.from_asin(db.session, asin)
        i_isbn13_2, new4 = Identifier.from_asin(db.session, isbn13_with_dashes)

        # The three ISBNs are equivalent, so they got turned into the same
        # Identifier, using the ISBN13.
        assert i_isbn10 == i_isbn13
        assert i_isbn13_2 == i_isbn13
        assert Identifier.ISBN == i_isbn10.type
        assert isbn13 == i_isbn10.identifier
        assert True == new1
        assert False == new2
        assert False == new4

        assert Identifier.ASIN == i_asin.type
        assert asin == i_asin.identifier

    def test_urn(self, db: DatabaseTransactionFixture):
        # ISBN identifiers use the ISBN URN scheme.
        identifier, ignore = Identifier.for_foreign_id(
            db.session, Identifier.ISBN, "9781449358068"
        )
        assert "urn:isbn:9781449358068" == identifier.urn

        # URI identifiers don't need a URN scheme.
        identifier, ignore = Identifier.for_foreign_id(
            db.session, Identifier.URI, "http://example.com/"
        )
        assert identifier.identifier == identifier.urn

        # Gutenberg identifiers use Gutenberg's URL-based sceheme
        identifier = db.identifier(Identifier.GUTENBERG_ID)
        assert (
            Identifier.GUTENBERG_URN_SCHEME_PREFIX + identifier.identifier
            == identifier.urn
        )

        # All other identifiers use our custom URN scheme.
        identifier = db.identifier(Identifier.OVERDRIVE_ID)
        assert identifier.urn.startswith(Identifier.URN_SCHEME_PREFIX)

    def test_parse_urns(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        fake_urn = "what_even_is_this"
        new_urn = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/nosuchidentifier"
        # Also create a different URN that would result in the same identifier.
        same_new_urn = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/NOSUCHidentifier"
        urns = [identifier.urn, fake_urn, new_urn, same_new_urn]

        results = Identifier.parse_urns(db.session, urns, autocreate=False)
        identifiers_by_urn, failures = results

        # By default, no new identifiers are created. All URNs for identifiers
        # that aren't in the db are included in the list of failures.
        assert sorted([fake_urn, new_urn, same_new_urn]) == sorted(failures)

        # Only the existing identifier is included in the results.
        assert 1 == len(identifiers_by_urn)
        assert {identifier.urn: identifier} == identifiers_by_urn

        # By default, new identifiers are created, too.
        results = Identifier.parse_urns(db.session, urns)
        identifiers_by_urn, failures = results

        # Only the fake URN is returned as a failure.
        assert [fake_urn] == failures

        # Only two additional identifiers have been created.
        assert 2 == len(identifiers_by_urn)

        # One is the existing identifier.
        assert identifier == identifiers_by_urn[identifier.urn]

        # And the new identifier has been created.
        new_identifier = identifiers_by_urn[new_urn]
        assert isinstance(new_identifier, Identifier)
        assert new_identifier in db.session
        assert Identifier.OVERDRIVE_ID == new_identifier.type
        assert "nosuchidentifier" == new_identifier.identifier

        # By passing in a list of allowed_types we can stop certain
        # types of Identifiers from being looked up, even if they
        # already exist.
        isbn_urn = "urn:isbn:9781453219539"
        urns = [new_urn, isbn_urn]

        success, failure = Identifier.parse_urns(
            db.session, urns, allowed_types=[Identifier.OVERDRIVE_ID]
        )
        assert new_urn in success
        assert isbn_urn in failure

        success, failure = Identifier.parse_urns(
            db.session, urns, allowed_types=[Identifier.OVERDRIVE_ID, Identifier.ISBN]
        )
        assert new_urn in success
        assert isbn_urn in success
        assert [] == failure

        # If the allowed_types is empty, no URNs can be looked up
        # -- this is most likely the caller's mistake.
        success, failure = Identifier.parse_urns(db.session, urns, allowed_types=[])
        assert new_urn in failure
        assert isbn_urn in failure

    def test_parse_urn(self, db: DatabaseTransactionFixture):
        # We can parse our custom URNs back into identifiers.
        identifier = db.identifier()
        db.session.commit()
        new_identifier, ignore = Identifier.parse_urn(db.session, identifier.urn)
        assert identifier == new_identifier

        # We can parse urn:isbn URNs into ISBN identifiers. ISBN-10s are
        # converted to ISBN-13s.
        identifier, ignore = Identifier.for_foreign_id(
            db.session, Identifier.ISBN, "9781449358068"
        )
        isbn_urn = "urn:isbn:1449358063"
        isbn_identifier, ignore = Identifier.parse_urn(db.session, isbn_urn)
        assert isinstance(isbn_identifier, Identifier)
        assert Identifier.ISBN == isbn_identifier.type
        assert "9781449358068" == isbn_identifier.identifier

        isbn_urn = "urn:isbn:9781449358068"
        isbn_identifier2, ignore = Identifier.parse_urn(db.session, isbn_urn)
        assert isbn_identifier2 == isbn_identifier

        # We can parse ordinary http: or https: URLs into URI
        # identifiers.
        http_identifier, ignore = Identifier.parse_urn(db.session, "http://example.com")
        assert isinstance(http_identifier, Identifier)
        assert Identifier.URI == http_identifier.type
        assert "http://example.com" == http_identifier.identifier

        https_identifier, ignore = Identifier.parse_urn(
            db.session, "https://example.com"
        )
        assert isinstance(https_identifier, Identifier)
        assert Identifier.URI == https_identifier.type
        assert "https://example.com" == https_identifier.identifier

        # we can parse Gutenberg identifiers
        gut_identifier = "http://www.gutenberg.org/ebooks/9781449358068"
        gut_identifier2, ignore = Identifier.parse_urn(db.session, gut_identifier)
        assert isinstance(gut_identifier2, Identifier)
        assert gut_identifier2.type == Identifier.GUTENBERG_ID
        assert gut_identifier2.identifier == "9781449358068"

        # we can parse ProQuest identifiers
        pq_identifier = "urn:proquest.com/document-id/1543720"
        pq_identifier2, ignore = Identifier.parse_urn(db.session, pq_identifier)
        assert isinstance(pq_identifier2, Identifier)
        assert pq_identifier2.type == Identifier.PROQUEST_ID
        assert pq_identifier2.identifier == "1543720"

        # We can parse UUIDs.
        uuid_identifier, ignore = Identifier.parse_urn(
            db.session, "urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952"
        )
        assert isinstance(uuid_identifier, Identifier)
        assert Identifier.URI == uuid_identifier.type
        assert (
            "urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952"
            == uuid_identifier.identifier
        )

        # A URN we can't handle raises an exception.
        ftp_urn = "ftp://example.com"
        pytest.raises(ValueError, Identifier.parse_urn, db.session, ftp_urn)

        # An invalid ISBN raises an exception.
        pytest.raises(
            ValueError, Identifier.parse_urn, db.session, "urn:isbn:notanisbn"
        )

        # Pass in None and you get None.
        assert (None, None) == Identifier.parse_urn(db.session, None)

    def parse_urn_must_support_license_pools(self, db: DatabaseTransactionFixture):
        # We have no way of associating ISBNs with license pools.
        # If we try to parse an ISBN URN in a context that only accepts
        # URNs that can have associated license pools, we get an exception.
        isbn_urn = "urn:isbn:1449358063"
        pytest.raises(
            Identifier.UnresolvableIdentifierException,
            Identifier.parse_urn,
            db.session,
            isbn_urn,
            must_support_license_pools=True,
        )

    def test_recursively_equivalent_identifier_ids(
        self, db: DatabaseTransactionFixture
    ):
        identifier = db.identifier()
        data_source = DataSource.lookup(db.session, DataSource.MANUAL)

        strong_equivalent = db.identifier()
        identifier.equivalent_to(data_source, strong_equivalent, 0.9)

        weak_equivalent = db.identifier()
        identifier.equivalent_to(data_source, weak_equivalent, 0.2)

        level_2_equivalent = db.identifier()
        strong_equivalent.equivalent_to(data_source, level_2_equivalent, 0.5)

        level_3_equivalent = db.identifier()
        level_2_equivalent.equivalent_to(data_source, level_3_equivalent, 0.9)

        level_4_equivalent = db.identifier()
        level_3_equivalent.equivalent_to(data_source, level_4_equivalent, 0.6)

        unrelated = db.identifier()

        # With a low threshold and enough levels, we find all the identifiers.
        high_levels_low_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=5, equivalent_identifier_threshold=0.1
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [identifier.id], policy=high_levels_low_threshold
        )
        assert {
            identifier.id,
            strong_equivalent.id,
            weak_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id,
        } == set(equivs[identifier.id])

        # If we only look at one level, we don't find the level 2, 3, or 4 identifiers.
        one_level = PresentationCalculationPolicy(
            equivalent_identifier_levels=1, equivalent_identifier_threshold=0.1
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [identifier.id], policy=one_level
        )
        assert {identifier.id, strong_equivalent.id, weak_equivalent.id} == set(
            equivs[identifier.id]
        )

        # If we raise the threshold, we don't find the weak identifier.
        one_level_high_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=1, equivalent_identifier_threshold=0.4
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [identifier.id], policy=one_level_high_threshold
        )
        assert {identifier.id, strong_equivalent.id} == set(equivs[identifier.id])

        # For deeper levels, the strength is the product of the strengths
        # of all the equivalencies in between the two identifiers.

        # In this example:
        # identifier - level_2_equivalent = 0.9 * 0.5 = 0.45
        # identifier - level_3_equivalent = 0.9 * 0.5 * 0.9 = 0.405
        # identifier - level_4_equivalent = 0.9 * 0.5 * 0.9 * 0.6 = 0.243

        # With a threshold of 0.5, level 2 and all subsequent levels are too weak.
        high_levels_high_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=5, equivalent_identifier_threshold=0.5
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [identifier.id], policy=high_levels_high_threshold
        )
        assert {identifier.id, strong_equivalent.id} == set(equivs[identifier.id])

        # With a threshold of 0.25, level 2 is strong enough, but level
        # 4 is too weak.
        high_levels_lower_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=5, equivalent_identifier_threshold=0.25
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [identifier.id], policy=high_levels_lower_threshold
        )
        assert {
            identifier.id,
            strong_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
        } == set(equivs[identifier.id])

        # It also works if we start from other identifiers.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [strong_equivalent.id], policy=high_levels_low_threshold
        )
        assert {
            identifier.id,
            strong_equivalent.id,
            weak_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id,
        } == set(equivs[strong_equivalent.id])

        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [level_4_equivalent.id], policy=high_levels_low_threshold
        )
        assert {
            identifier.id,
            strong_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id,
        } == set(equivs[level_4_equivalent.id])

        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [level_4_equivalent.id], policy=high_levels_high_threshold
        )
        assert {
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id,
        } == set(equivs[level_4_equivalent.id])

        # A chain of very strong equivalents can keep a high strength
        # even at deep levels. This wouldn't work if we changed the strength
        # threshold by level instead of accumulating a strength product.
        another_identifier = db.identifier()
        l2 = db.identifier()
        l3 = db.identifier()
        l4 = db.identifier()
        l2.equivalent_to(data_source, another_identifier, 1)
        l3.equivalent_to(data_source, l2, 1)
        l4.equivalent_to(data_source, l3, 0.9)
        high_levels_fairly_high_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=5, equivalent_identifier_threshold=0.89
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [another_identifier.id], high_levels_fairly_high_threshold
        )
        assert {another_identifier.id, l2.id, l3.id, l4.id} == set(
            equivs[another_identifier.id]
        )

        # We can look for multiple identifiers at once.
        two_levels_high_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=2, equivalent_identifier_threshold=0.8
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session,
            [identifier.id, level_3_equivalent.id],
            policy=two_levels_high_threshold,
        )
        assert {identifier.id, strong_equivalent.id} == set(equivs[identifier.id])
        assert {level_2_equivalent.id, level_3_equivalent.id} == set(
            equivs[level_3_equivalent.id]
        )

        # By setting a cutoff, you can say to look deep in the tree,
        # but stop looking as soon as you have a certain number of
        # equivalents.
        with_cutoff = PresentationCalculationPolicy(
            equivalent_identifier_levels=5,
            equivalent_identifier_threshold=0.1,
            equivalent_identifier_cutoff=1,
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [identifier.id], policy=with_cutoff
        )

        # The cutoff was set to 1, but we always go at least one level
        # deep, and that gives us three equivalent identifiers. We
        # don't artificially trim it back down to 1.
        assert 3 == len(equivs[identifier.id])

        # Increase the cutoff, and we get more identifiers.
        with_cutoff.equivalent_identifier_cutoff = 5
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db.session, [identifier.id], policy=with_cutoff
        )
        assert len(equivs[identifier.id]) > 3

        # The query() method uses the same db function, but returns
        # equivalents for all identifiers together so it can be used
        # as a subquery.
        query = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, policy=high_levels_low_threshold
        )
        query = query.where(Identifier.id == identifier.id)
        results = db.session.execute(query)
        equivalent_ids = [r[0] for r in results]
        assert {
            identifier.id,
            strong_equivalent.id,
            weak_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id,
        } == set(equivalent_ids)

        query = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, policy=two_levels_high_threshold
        )
        query = query.where(Identifier.id.in_([identifier.id, level_3_equivalent.id]))
        results = db.session.execute(query)
        equivalent_ids = [r[0] for r in results]
        assert {
            identifier.id,
            strong_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
        } == set(equivalent_ids)

    def test_licensed_through_collection(self, db: DatabaseTransactionFixture):
        c1 = db.default_collection()
        c2 = db.collection()
        c3 = db.collection()

        edition, lp1 = db.edition(collection=c1, with_license_pool=True)
        lp2 = db.licensepool(collection=c2, edition=edition)

        identifier = lp1.identifier
        assert lp2.identifier == identifier

        assert lp1 == identifier.licensed_through_collection(c1)
        assert lp2 == identifier.licensed_through_collection(c2)
        assert None == identifier.licensed_through_collection(c3)

    def test_missing_coverage_from(self, db: DatabaseTransactionFixture):
        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        oclc = DataSource.lookup(db.session, DataSource.OCLC)
        web = DataSource.lookup(db.session, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, ignore = Edition.for_foreign_id(
            db.session, gutenberg, Identifier.GUTENBERG_ID, "1"
        )

        g2, ignore = Edition.for_foreign_id(
            db.session, gutenberg, Identifier.GUTENBERG_ID, "2"
        )

        # One of them has coverage from OCLC Classify
        c1 = db.coverage_record(g1, oclc)

        # The other has coverage from a specific operation on OCLC Classify
        c2 = db.coverage_record(g2, oclc, "some operation")

        # Here's a web record, just sitting there.
        w, ignore = Edition.for_foreign_id(
            db.session, web, Identifier.URI, "http://www.foo.com/"
        )

        # If we run missing_coverage_from we pick up the Gutenberg
        # record with no generic OCLC coverage. It doesn't pick up the
        # other Gutenberg record, it doesn't pick up the web record,
        # and it doesn't pick up the OCLC coverage for a specific
        # operation.
        [in_gutenberg_but_not_in_oclc] = Identifier.missing_coverage_from(
            db.session, [Identifier.GUTENBERG_ID], oclc
        ).all()

        assert g2.primary_identifier == in_gutenberg_but_not_in_oclc

        # If we ask about a specific operation, we get the Gutenberg
        # record that has coverage for that operation, but not the one
        # that has generic OCLC coverage.

        [has_generic_coverage_only] = Identifier.missing_coverage_from(
            db.session, [Identifier.GUTENBERG_ID], oclc, "some operation"
        ).all()
        assert g1.primary_identifier == has_generic_coverage_only

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Identifier.missing_coverage_from(
            db.session, [Identifier.URI], oclc
        ).all()
        assert w.primary_identifier == in_web_but_not_in_oclc

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        assert [g1.primary_identifier.id, g2.primary_identifier.id] == sorted(
            x.id
            for x in Identifier.missing_coverage_from(
                db.session, [Identifier.GUTENBERG_ID], web
            )
        )

    def test_missing_coverage_from_with_collection(
        self, db: DatabaseTransactionFixture
    ):
        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        identifier = db.identifier()
        collection1 = db.default_collection()
        collection2 = db.collection()
        db.coverage_record(identifier, gutenberg, collection=collection1)

        # The Identifier has coverage in collection 1.
        assert (
            []
            == Identifier.missing_coverage_from(
                db.session, [identifier.type], gutenberg, collection=collection1
            ).all()
        )

        # It is missing coverage in collection 2.
        assert [identifier] == Identifier.missing_coverage_from(
            db.session, [identifier.type], gutenberg, collection=collection2
        ).all()

        # If no collection is specified, we look for a CoverageRecord
        # that also has no collection specified, and the Identifier is
        # not treated as covered.
        assert [identifier] == Identifier.missing_coverage_from(
            db.session, [identifier.type], gutenberg
        ).all()

    def test_missing_coverage_from_with_cutoff_date(
        self, db: DatabaseTransactionFixture
    ):
        gutenberg = DataSource.lookup(db.session, DataSource.GUTENBERG)
        oclc = DataSource.lookup(db.session, DataSource.OCLC)
        web = DataSource.lookup(db.session, DataSource.WEB)

        # Here's an Edition with a coverage record from OCLC classify.
        gutenberg, ignore = Edition.for_foreign_id(
            db.session, gutenberg, Identifier.GUTENBERG_ID, "1"
        )
        identifier = gutenberg.primary_identifier
        oclc = DataSource.lookup(db.session, DataSource.OCLC)
        coverage = db.coverage_record(gutenberg, oclc)

        # The CoverageRecord knows when the coverage was provided.
        timestamp = coverage.timestamp
        assert isinstance(timestamp, datetime.datetime)

        # If we ask for Identifiers that are missing coverage records
        # as of that time, we see nothing.
        assert (
            []
            == Identifier.missing_coverage_from(
                db.session, [identifier.type], oclc, count_as_missing_before=timestamp
            ).all()
        )

        # But if we give a time one second later, the Identifier is
        # missing coverage.
        assert [identifier] == Identifier.missing_coverage_from(
            db.session,
            [identifier.type],
            oclc,
            count_as_missing_before=timestamp + datetime.timedelta(seconds=1),
        ).all()

    @pytest.mark.parametrize(
        "_,identifier_type,identifier,title",
        [
            ("ascii_type_ascii_identifier_no_title", "a", "a", None),
            ("ascii_type_non_ascii_identifier_no_title", "a", "ą", None),
            ("non_ascii_type_ascii_identifier_no_title", "ą", "a", None),
            ("non_ascii_type_non_ascii_identifier_no_title", "ą", "ą", None),
            ("ascii_type_ascii_identifier_ascii_title", "a", "a", "a"),
            ("ascii_type_non_ascii_identifier_ascii_title", "a", "ą", "a"),
            ("non_ascii_type_ascii_identifier_ascii_title", "ą", "a", "a"),
            ("non_ascii_type_non_ascii_identifier_ascii_title", "ą", "ą", "a"),
            ("ascii_type_ascii_identifier_non_ascii_title", "a", "a", "ą"),
            ("ascii_type_non_ascii_identifier_non_ascii_title", "a", "ą", "ą"),
            ("non_ascii_type_ascii_identifier_non_ascii_title", "ą", "a", "ą"),
            ("non_ascii_type_non_ascii_identifier_non_ascii_title", "ą", "ą", "ą"),
        ],
    )
    def test_repr(self, _, identifier_type, identifier, title):
        """Test that Identifier.__repr__ correctly works with both ASCII and non-ASCII symbols.

        :param _: Name of the test case
        :type _: str

        :param identifier_type: Type of the identifier
        :type identifier_type: str

        :param identifier: Identifier's value
        :type identifier: str

        :param title: Presentation edition's title
        :type title: str
        """
        # Arrange
        identifier = Identifier(type=identifier_type, identifier=identifier)

        if title:
            edition = create_autospec(spec=Edition)
            edition.title = PropertyMock(return_value=title)

            identifier.primarily_identifies = PropertyMock(return_value=[edition])

        # Act
        # NOTE: we are not interested in the result returned by repr,
        # we just want to make sure that repr doesn't throw any unexpected exceptions
        _ = repr(identifier)

    def test_add_link(self, db: DatabaseTransactionFixture):
        identifier: Identifier = db.identifier()
        datasource = DataSource.lookup(db.session, DataSource.GUTENBERG)
        identifier.add_link(
            Hyperlink.SAMPLE,
            "http://example.org/sample",
            datasource,
            media_type=MediaTypes.EPUB_MEDIA_TYPE,
        )

        assert len(identifier.links) == 1
        link: Hyperlink = identifier.links[0]
        assert link.rel == Hyperlink.SAMPLE
        assert link.resource.url == "http://example.org/sample"
        assert link.resource.representation.media_type == MediaTypes.EPUB_MEDIA_TYPE

        # Changing only the media type should update the same link's representation.
        identifier.add_link(
            Hyperlink.SAMPLE,
            "http://example.org/sample",
            datasource,
            media_type=MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
        )
        assert len(identifier.links) == 1
        assert (
            identifier.links[0].resource.representation.media_type
            == MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE
        )


@pytest.fixture()
def example_equivalency_coverage_record_fixture(
    db,
) -> ExampleEquivalencyCoverageRecordFixture:
    return ExampleEquivalencyCoverageRecordFixture(db)


class TestRecursiveEquivalencyCache:
    def test_is_parent(
        self,
        example_equivalency_coverage_record_fixture: ExampleEquivalencyCoverageRecordFixture,
    ):
        data = example_equivalency_coverage_record_fixture
        session = data.transaction.session

        rec_eq = (
            session.query(RecursiveEquivalencyCache)
            .filter(
                RecursiveEquivalencyCache.parent_identifier_id == data.identifiers[0].id
            )
            .first()
        )
        assert isinstance(rec_eq, RecursiveEquivalencyCache)
        assert rec_eq.is_parent == True

    def test_identifier_delete_cascade_parent(
        self,
        example_equivalency_coverage_record_fixture: ExampleEquivalencyCoverageRecordFixture,
    ):
        data = example_equivalency_coverage_record_fixture
        session = data.transaction.session

        all_recursives = session.query(RecursiveEquivalencyCache).all()
        assert len(all_recursives) == 4  # all selfs

        session.delete(data.identifiers[0])
        session.commit()

        # RecursiveEquivalencyCache was deleted by cascade
        all_recursives = session.query(RecursiveEquivalencyCache).all()
        assert len(all_recursives) == 3


class TestProQuestIdentifierParser:
    @pytest.mark.parametrize(
        "_,identifier_string,expected_result",
        [
            (
                "incorrect_identifier",
                "urn:librarysimplified.org/terms/id/Overdrive%20ID/adfcc11a-cc5b-4c82-8048-e005e4a90222",
                None,
            ),
            (
                "correct_identifier",
                "urn:proquest.com/document-id/12345",
                (Identifier.PROQUEST_ID, "12345"),
            ),
        ],
    )
    def test_parse(self, _, identifier_string, expected_result):
        parser = ProQuestIdentifierParser()
        result = parser.parse(identifier_string)
        assert expected_result == result
