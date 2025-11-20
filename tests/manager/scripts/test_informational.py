from __future__ import annotations

from io import StringIO
from unittest.mock import call, create_autospec

from palace.manager.integration.goals import Goals
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.scripts.informational import (
    Explain,
    LanguageListScript,
    ShowCollectionsScript,
    ShowIntegrationsScript,
    ShowLanesScript,
    ShowLibrariesScript,
    WhereAreMyBooksScript,
)
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import LicensePoolStatus
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import EndToEndSearchFixture


class TestShowLibrariesScript:
    def test_with_no_libraries(self, db: DatabaseTransactionFixture):
        output = StringIO()
        ShowLibrariesScript().do_run(db.session, output=output)
        assert "No libraries found.\n" == output.getvalue()

    def test_with_multiple_libraries(self, db: DatabaseTransactionFixture):
        l1 = db.library(name="Library 1", short_name="L1")
        l1.library_registry_shared_secret = "a"
        l2 = db.library(
            name="Library 2",
            short_name="L2",
        )
        l2.library_registry_shared_secret = "b"

        # The output of this script is the result of running explain()
        # on both libraries.
        output = StringIO()
        ShowLibrariesScript().do_run(db.session, output=output)
        expect_1 = "\n".join(l1.explain(include_secrets=False))
        expect_2 = "\n".join(l2.explain(include_secrets=False))

        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()

        # We can tell the script to only list a single library.
        output = StringIO()
        ShowLibrariesScript().do_run(
            db.session, cmd_args=["--short-name=L2"], output=output
        )
        assert expect_2 + "\n" == output.getvalue()

        # We can tell the script to include the library registry
        # shared secret.
        output = StringIO()
        ShowLibrariesScript().do_run(
            db.session, cmd_args=["--show-secrets"], output=output
        )
        expect_1 = "\n".join(l1.explain(include_secrets=True))
        expect_2 = "\n".join(l2.explain(include_secrets=True))
        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()


class TestShowCollectionsScript:
    def test_with_no_collections(self, db: DatabaseTransactionFixture):
        output = StringIO()
        ShowCollectionsScript().do_run(db.session, output=output)
        assert "No collections found.\n" == output.getvalue()

    def test_with_multiple_collections(self, db: DatabaseTransactionFixture):
        c1 = db.collection(name="Collection 1", protocol=OverdriveAPI)
        c2 = db.collection(name="Collection 2", protocol=BibliothecaAPI)

        # The output of this script is the result of running explain()
        # on both collections.
        output = StringIO()
        ShowCollectionsScript().do_run(db.session, output=output)
        expect_1 = "\n".join(c1.explain(include_secrets=False))
        expect_2 = "\n".join(c2.explain(include_secrets=False))

        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()

        # We can tell the script to only list a single collection.
        output = StringIO()
        ShowCollectionsScript().do_run(
            db.session, cmd_args=["--name=Collection 2"], output=output
        )
        assert expect_2 + "\n" == output.getvalue()

        # We can tell the script to include the collection password
        output = StringIO()
        ShowCollectionsScript().do_run(
            db.session, cmd_args=["--show-secrets"], output=output
        )
        expect_1 = "\n".join(c1.explain(include_secrets=True))
        expect_2 = "\n".join(c2.explain(include_secrets=True))
        assert expect_1 + "\n" + expect_2 + "\n" == output.getvalue()


class TestShowIntegrationsScript:
    def test_with_no_integrations(self, db: DatabaseTransactionFixture):
        output = StringIO()
        ShowIntegrationsScript().do_run(db.session, output=output)
        assert "No integrations found.\n" == output.getvalue()

    def test_with_multiple_integrations(self, db: DatabaseTransactionFixture):
        i1 = db.integration_configuration(
            name="Integration 1", goal=Goals.LICENSE_GOAL, protocol="Test Protocol 1"
        )
        i1.settings_dict = {"url": "http://url1", "username": "user1"}

        i2 = db.integration_configuration(
            name="Integration 2", goal=Goals.LICENSE_GOAL, protocol="Test Protocol 2"
        )
        i2.settings_dict = {"url": "http://url2", "password": "password"}

        # The output of this script is the result of running explain()
        # on both integrations.
        output = StringIO()
        ShowIntegrationsScript().do_run(db.session, output=output)
        expect_1 = "\n".join(i1.explain(include_secrets=False))
        expect_2 = "\n".join(i2.explain(include_secrets=False))

        assert expect_1 + "\n\n" + expect_2 + "\n\n" == output.getvalue()

        # We can tell the script to only list a single integration.
        output = StringIO()
        ShowIntegrationsScript().do_run(
            db.session, cmd_args=["--name=Integration 2"], output=output
        )
        assert expect_2 + "\n\n" == output.getvalue()

        # We can tell the script to include the integration secrets
        output = StringIO()
        ShowIntegrationsScript().do_run(
            db.session, cmd_args=["--show-secrets"], output=output
        )
        expect_1 = "\n".join(i1.explain(include_secrets=True))
        expect_2 = "\n".join(i2.explain(include_secrets=True))
        assert expect_1 + "\n\n" + expect_2 + "\n\n" == output.getvalue()


class TestShowLanesScript:
    def test_with_no_lanes(self, db: DatabaseTransactionFixture):
        output = StringIO()
        ShowLanesScript().do_run(db.session, output=output)
        assert "No lanes found.\n" == output.getvalue()

    def test_with_multiple_lanes(self, db: DatabaseTransactionFixture):
        l1 = db.lane()
        l2 = db.lane()

        # The output of this script is the result of running explain()
        # on both lanes.
        output = StringIO()
        ShowLanesScript().do_run(db.session, output=output)
        expect_1 = "\n".join(l1.explain())
        expect_2 = "\n".join(l2.explain())

        assert expect_1 + "\n\n" + expect_2 + "\n\n" == output.getvalue()

        # We can tell the script to only list a single lane.
        output = StringIO()
        ShowLanesScript().do_run(
            db.session, cmd_args=["--id=%s" % l2.id], output=output
        )
        assert expect_2 + "\n\n" == output.getvalue()


class MockWhereAreMyBooks(WhereAreMyBooksScript):
    """A mock script that keeps track of its output in an easy-to-test
    form, so we don't have to mess around with StringIO.
    """

    def __init__(self, search: ExternalSearchIndex, _db=None, output=None):
        # In most cases a list will do fine for `output`.
        output = output or []

        super().__init__(_db, output, search)
        self.output = []

    def out(self, s, *args):
        print(">>>> Out: ", s, *args)
        if args:
            self.output.append((s, list(args)))
        else:
            self.output.append(s)


class TestWhereAreMyBooksScript:
    def test_overall_structure(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Verify that run() calls the methods we expect.
        script = MockWhereAreMyBooks(
            _db=db.session, search=end_to_end_search_fixture.external_search_index
        )
        mock_check_library = create_autospec(script.check_library)
        script.check_library = mock_check_library
        mock_explain_collection = create_autospec(script.explain_collection)
        script.explain_collection = mock_explain_collection

        # If there are no libraries in the system, that's a big problem.
        script.run()
        assert [
            "There are no libraries in the system -- that's a problem.",
            "\n",
        ] == script.output
        script.output = []

        # Make some libraries and some collections, and try again.
        # The db fixture creates all three of these if any of them is present,
        # so if one is present, we must account for them all.
        default_library = db.default_library()
        default_active_collection = db.default_collection()
        default_inactive_collection = db.default_inactive_collection()
        # And we'll add a couple more items to the mix.
        other_library = db.library()
        other_collection = db.collection()

        libraries = [default_library, other_library]
        collections = [
            default_active_collection,
            default_inactive_collection,
            other_collection,
        ]

        # We expect one output newline per library, one more per collection,
        # and one more between the library and collection sections.
        expected_newlines = len(libraries) + len(collections) + 1

        script.run()

        # Every library in the collection was checked.
        mock_check_library.assert_has_calls(
            [call(library) for library in libraries], any_order=True
        )

        # Every collection in the database was explained.
        mock_explain_collection.assert_has_calls(
            [call(collection) for collection in collections], any_order=True
        )

        # We got the expected number of newlines.
        assert ["\n"] * expected_newlines == script.output

        # Finally, verify the ability to use the command line to limit
        # the check to specific collections. (This isn't terribly useful
        # since checks now run very quickly.)
        mock_explain_collection.reset_mock()
        script.run(cmd_args=["--collection=%s" % other_collection.name])
        mock_explain_collection.assert_called_once_with(other_collection)

    def test_check_library(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # Give the library an active collection and a lane.
        library = db.library()
        collection = db.collection(library=library)
        lane = db.lane(library=library)

        script = MockWhereAreMyBooks(
            _db=db.session, search=end_to_end_search_fixture.external_search_index
        )
        script.check_library(library)

        checking, has_collection, has_lanes = script.output
        assert ("Checking library %s", [library.name]) == checking
        assert (
            has_collection
            == f" Associated with collection {collection.name} (active=True)."
        )
        assert (" Associated with %s lanes.", [1]) == has_lanes

        # Now we'll add an inactive collection to the library.
        collection2 = db.collection(library=library, inactive=True)
        script.output = []
        script.check_library(library)
        checking, has_one_collection, has_another_collection, has_lanes = script.output
        assert ("Checking library %s", [library.name]) == checking
        assert {has_one_collection, has_another_collection} == {
            f" Associated with collection {collection.name} (active=True).",
            f" Associated with collection {collection2.name} (active=False).",
        }
        assert (" Associated with %s lanes.", [1]) == has_lanes

        # This library has no collections and no lanes.
        library2 = db.library()
        script.output = []
        script.check_library(library2)
        checking, no_collection, no_lanes = script.output
        assert ("Checking library %s", [library2.name]) == checking
        assert (
            no_collection
            == " This library has no associated collections -- that's a problem."
        )
        assert " This library has no lanes -- that's a problem." == no_lanes

        # This library has a collection, but it is inactive.
        library3 = db.library()
        collection3 = db.collection(library=library3, inactive=True)

        script.output = []
        script.check_library(library3)
        checking, no_active_collection, no_lanes = script.output
        assert ("Checking library %s", [library3.name]) == checking
        assert (
            no_active_collection
            == " This library has no active collections -- that's a problem."
        )

    @staticmethod
    def check_explanation(
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
        presentation_ready=1,
        not_presentation_ready=0,
        no_delivery_mechanisms=0,
        suppressed=0,
        not_owned=0,
        in_search_index=0,
        **kwargs,
    ):
        """Runs explain_collection() and verifies expected output."""
        script = MockWhereAreMyBooks(
            _db=db.session,
            search=end_to_end_search_fixture.external_search_index,
            **kwargs,
        )
        script.explain_collection(db.default_collection())
        out = script.output
        assert isinstance(out, list)

        # This always happens.
        assert (
            'Examining collection "%s"',
            [db.default_collection().name],
        ) == out.pop(0)
        assert (" %d presentation-ready works.", [presentation_ready]) == out.pop(0)
        assert (
            " %d works not presentation-ready.",
            [not_presentation_ready],
        ) == out.pop(0)

        # These totals are only given if the numbers are nonzero.
        #
        if no_delivery_mechanisms:
            assert (
                " %d works are missing delivery mechanisms and won't show up.",
                [no_delivery_mechanisms],
            ) == out.pop(0)

        if suppressed:
            assert (
                " %d works have suppressed LicensePools and won't show up.",
                [suppressed],
            ) == out.pop(0)

        if not_owned:
            assert (
                " %d non-open-access works have no owned licenses and won't show up.",
                [not_owned],
            ) == out.pop(0)

        # Search engine statistics are always shown.
        assert (
            " %d works in the search index, expected around %d.",
            [in_search_index, presentation_ready],
        ) == out.pop(0)

    def test_no_presentation_ready_works(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # This work is not presentation-ready.
        work = db.work(with_license_pool=True)
        work.presentation_ready = False
        end_to_end_search_fixture.populate_search_index()
        MockWhereAreMyBooks(
            _db=db.session, search=end_to_end_search_fixture.external_search_index
        )
        self.check_explanation(
            end_to_end_search_fixture=end_to_end_search_fixture,
            presentation_ready=0,
            not_presentation_ready=1,
            db=db,
        )

    def test_no_delivery_mechanisms(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # This work has a license pool, but no delivery mechanisms.
        work = db.work(with_license_pool=True)
        for lpdm in work.license_pools[0].delivery_mechanisms:
            db.session.delete(lpdm)
        end_to_end_search_fixture.populate_search_index()
        self.check_explanation(
            no_delivery_mechanisms=1,
            in_search_index=1,
            db=db,
            end_to_end_search_fixture=end_to_end_search_fixture,
        )

    def test_suppressed_pool(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # This work has a license pool, but it's suppressed.
        work = db.work(with_license_pool=True)
        work.license_pools[0].suppressed = True
        end_to_end_search_fixture.populate_search_index()
        self.check_explanation(
            suppressed=1,
            db=db,
            end_to_end_search_fixture=end_to_end_search_fixture,
        )

    def test_no_licenses(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        # This work has a license pool, but it is exhausted.
        work = db.work(with_license_pool=True)
        work.license_pools[0].licenses_owned = 0
        work.license_pools[0].status = LicensePoolStatus.EXHAUSTED
        end_to_end_search_fixture.populate_search_index()
        self.check_explanation(
            not_owned=1,
            db=db,
            end_to_end_search_fixture=end_to_end_search_fixture,
        )

    def test_search_engine(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        work = db.work(with_license_pool=True)
        work.presentation_ready = True

        end_to_end_search_fixture.populate_search_index()

        # This search index will always claim there is one result.
        self.check_explanation(
            in_search_index=1,
            db=db,
            end_to_end_search_fixture=end_to_end_search_fixture,
        )


class TestExplain:
    def test_explain(self, db: DatabaseTransactionFixture):
        """Make sure the Explain script runs without crashing."""
        work = db.work(with_license_pool=True, genre="Science Fiction")
        [pool] = work.license_pools
        edition = work.presentation_edition
        identifier = pool.identifier
        source = DataSource.lookup(db.session, DataSource.OCLC_LINKED_DATA)
        CoverageRecord.add_for(identifier, source, "an operation")
        input = StringIO()
        io_output = StringIO()
        args = ["--identifier-type", "Database ID", str(identifier.id)]
        Explain(db.session).do_run(cmd_args=args, stdin=input, stdout=io_output)
        output = io_output.getvalue()

        # The script ran. Spot-check that it provided various
        # information about the work, without testing the exact
        # output.
        assert pool.collection.name in output
        assert "Available to libraries: default" in output
        assert work.title in output
        assert "Science Fiction" in output
        for contributor in edition.contributors:
            assert contributor.sort_name in output

        # CoverageRecords associated with the primary identifier were
        # printed out.
        assert "OCLC Linked Data | an operation | success" in output

        # There is an active LicensePool that is fulfillable and has
        # copies owned.
        assert "%s owned" % pool.licenses_owned in output
        assert "Fulfillable" in output
        assert "ACTIVE" in output


class TestLanguageListScript:
    def test_languages(self, db: DatabaseTransactionFixture):
        """Test the method that gives this script the bulk of its output."""
        english = db.work(language="eng", with_open_access_download=True)
        tagalog = db.work(language="tgl", with_license_pool=True)
        [pool] = tagalog.license_pools
        db.add_generic_delivery_mechanism(pool)
        script = LanguageListScript(db.session)
        output = list(script.languages(db.default_library()))

        # English is ignored because all its works are open-access.
        # Tagalog shows up with the correct estimate.
        assert ["tgl 1 (Tagalog)"] == output
