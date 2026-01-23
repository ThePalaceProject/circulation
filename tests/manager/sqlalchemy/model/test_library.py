import datetime
from unittest.mock import patch

import pytest
from bidict import frozenbidict
from Crypto.PublicKey.RSA import RsaKey, import_key

from palace.manager.feed.facets.constants import FacetConstants
from palace.manager.integration.base import integration_settings_update
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds1.settings import OPDSImporterSettings
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.library import Library
from tests.fixtures.database import DatabaseTransactionFixture


class TestLibrary:
    def test_library_registry_short_name(self, db: DatabaseTransactionFixture):
        library = db.default_library()

        # Short name is always uppercased.
        library.library_registry_short_name = "foo"
        assert "FOO" == library.library_registry_short_name

        # Short name cannot contain a pipe character.
        def set_to_pipe():
            library.library_registry_short_name = "foo|bar"

        pytest.raises(ValueError, set_to_pipe)

        # You can set the short name to None. This isn't
        # recommended, but it's not an error.
        library.library_registry_short_name = None

    def test_lookup(self, db: DatabaseTransactionFixture):
        library = db.default_library()
        name = library.short_name
        assert name == library.cache_key()

        # Cache is empty.
        cache = Library._cache_from_session(db.session)
        assert len(cache.id) == 0
        assert len(cache.key) == 0

        assert library == Library.lookup(db.session, name)

        # Cache is populated.
        assert library == cache.key[name]

    def test_default(self, db: DatabaseTransactionFixture):
        # We start off with no libraries.
        assert None == Library.default(db.session)

        # Let's make a couple libraries.
        l1 = db.default_library()
        l2 = db.library()

        # None of them are the default according to the database.
        assert False == l1.is_default
        assert False == l2.is_default

        # If we call Library.default, the library with the lowest database
        # ID is made the default.
        assert l1 == Library.default(db.session)
        assert True == l1.is_default
        assert False == l2.is_default

        # We can set is_default to change the default library.
        l2.is_default = True
        assert False == l1.is_default
        assert True == l2.is_default

        # If ever there are multiple default libraries, calling default()
        # will set the one with the lowest database ID to the default.
        l1._is_default = True
        l2._is_default = True
        assert l1 == Library.default(db.session)
        assert True == l1.is_default
        assert False == l2.is_default
        with pytest.raises(ValueError) as excinfo:
            l1.is_default = False
        assert (
            "You cannot stop a library from being the default library; you must designate a different library as the default."
            in str(excinfo.value)
        )

    def test_has_root_lanes(self, db: DatabaseTransactionFixture):
        # A library has root lanes if any of its lanes are the root for any
        # patron type(s).
        library = db.default_library()
        lane = db.lane()
        assert False == library.has_root_lanes

        # If a library goes back and forth between 'has root lanes'
        # and 'doesn't have root lanes', has_root_lanes continues to
        # give the correct result so long as there was a database
        # flush in between.
        #
        # (This is because there's a listener that resets
        # Library._has_default_lane_cache whenever lane configuration
        # changes.)
        lane.root_for_patron_type = ["1", "2"]
        db.session.flush()
        assert True == library.has_root_lanes

        lane.root_for_patron_type = None
        db.session.flush()
        assert False == library.has_root_lanes

    def test_collections(self, db: DatabaseTransactionFixture):
        library = db.default_library()

        parent = db.collection()
        db.default_collection().parent_id = parent.id

        assert {
            db.default_collection(),
            db.default_inactive_collection(),
        } == set(library.associated_collections)
        assert [db.default_collection()] == library.active_collections

    def test_estimated_holdings_by_language(self, db: DatabaseTransactionFixture):
        library = db.default_library()

        # Here's an open-access English book.
        english = db.work(language="eng", with_open_access_download=True)

        # Here's a non-open-access Tagalog book with a delivery mechanism.
        tagalog = db.work(language="tgl", with_license_pool=True)
        [pool] = tagalog.license_pools
        db.add_generic_delivery_mechanism(pool)

        # Here's an open-access book that improperly has no language set.
        no_language = db.work(with_open_access_download=True)
        no_language.presentation_edition.language = None

        # estimated_holdings_by_language counts the English and the
        # Tagalog works. The work with no language is ignored.
        estimate = library.estimated_holdings_by_language()
        assert dict(eng=1, tgl=1) == estimate

        # If we disqualify open-access works, it only counts the Tagalog.
        estimate = library.estimated_holdings_by_language(include_open_access=False)
        assert dict(tgl=1) == estimate

        # If we remove the default collection from the default library,
        # it loses all its works.
        db.default_collection().associated_libraries = []
        estimate = library.estimated_holdings_by_language(include_open_access=False)
        assert dict() == estimate

    def test_explain(self, db: DatabaseTransactionFixture):
        """Test that Library.explain gives all relevant information
        about a Library.
        """
        session = db.session
        library = db.default_library()
        library.uuid = "uuid"
        library.name = "The Library"
        library.short_name = "Short"
        library.library_registry_short_name = "SHORT"
        library.library_registry_shared_secret = "secret"

        expect = """Library UUID: "uuid"
Name: "The Library"
Short name: "Short"
Short name (for library registry): "SHORT"

Configuration settings:
-----------------------
website='http://library.com'
allow_holds='True'
enabled_entry_points='['Book']'
featured_lane_size='15'
minimum_featured_quality='0.65'
facets_enabled_order='['author', 'title', 'added']'
facets_default_order='author'
facets_enabled_available='['all', 'now', 'always']'
facets_default_available='all'
help_web='http://library.com/support'
default_notification_email_address='noreply@thepalaceproject.org'
color_scheme='blue'
web_primary_color='#377F8B'
web_secondary_color='#D53F34'
web_header_links='[]'
web_header_labels='[]'
hidden_content_types='[]'
filtered_audiences='[]'
filtered_genres='[]'
"""
        actual = library.explain()
        assert expect == "\n".join(actual)

        with_secrets = library.explain(True)
        assert 'Shared secret (for library registry): "secret"' in with_secrets

    def test_generate_keypair(self, db: DatabaseTransactionFixture):
        # Test the ability to create a public/private key pair
        public_key, private_key = Library.generate_keypair()
        assert "BEGIN PUBLIC KEY" in public_key
        key = import_key(private_key)
        assert isinstance(key, RsaKey)
        assert public_key == key.public_key().export_key().decode("utf-8")

    def test_settings(self, db: DatabaseTransactionFixture):
        library = db.default_library()

        # If our settings dict gets set to something other than a dict,
        # we raise an error.
        library.settings_dict = []
        with pytest.raises(ValueError):
            library.settings

        # Test with a properly formatted settings dict.
        library2 = db.library()
        assert library2.settings.website == "http://library.com"


class TestLibraryCollections:
    # TODO: Pydantic and FreezeGun don't play well together, so we'll use
    #  dates well into the past and into the future to avoid any flakiness.
    @pytest.mark.parametrize(
        "activation_date, expiration_date, expect_active",
        (
            pytest.param(None, None, True, id="no start/end dates"),
            pytest.param(None, datetime.date(2222, 8, 31), True, id="no start date"),
            pytest.param(datetime.date(1960, 8, 1), None, True, id="no end date"),
            pytest.param(
                datetime.date(1960, 8, 1),
                datetime.date(2222, 8, 31),
                True,
                id="both dates",
            ),
            pytest.param(
                datetime.date(1960, 8, 1),
                datetime.date(1961, 8, 15),
                False,
                id="ends before today",
            ),
            pytest.param(
                datetime.date(2222, 9, 1),
                None,
                False,
                id="starts after today",
            ),
        ),
    )
    def test_active_collections(
        self,
        db: DatabaseTransactionFixture,
        activation_date: datetime.date | None,
        expiration_date: datetime.date | None,
        expect_active: bool,
    ):
        library = db.default_library()

        # Collection subscription settings.
        subscription_test_settings = (
            {"subscription_activation_date": activation_date} if activation_date else {}
        ) | (
            {"subscription_expiration_date": expiration_date} if expiration_date else {}
        )

        # Our library is associated with three collections, one of whose
        # subscriptions settings we're testing.
        forever_collection = db.default_collection()
        never_collection = db.default_inactive_collection()
        test_collection = db.collection(
            name="Test Collection", protocol=OPDSAPI, library=library
        )

        assert set(library.associated_collections) == {
            forever_collection,
            never_collection,
            test_collection,
        }

        # Initially there are no subscription settings for the test collection.
        test_integration = test_collection.integration_configuration
        initial_settings = test_integration.settings_dict
        assert "subscription_activation_date" not in initial_settings
        assert "subscription_expiration_date" not in initial_settings

        # And without subscription settings, the collections is active by default.
        assert forever_collection in library.active_collections
        assert test_collection in library.active_collections

        # The "never" collection is inactive, as it always should be.
        assert never_collection not in library.active_collections

        # Now we apply the settings for the test collection.
        integration_settings_update(
            OPDSImporterSettings,
            test_integration,
            subscription_test_settings,
            merge=True,
        )

        # All collections are still associated with the library,...
        assert set(library.associated_collections) == {
            forever_collection,
            never_collection,
            test_collection,
        }
        # ... and the forever collection is still active for the library, ....
        assert forever_collection in library.active_collections
        # ... and the "never" collection is still inactive for the library, ....
        assert never_collection not in library.active_collections
        # ... the test collection is only active when we expect it to be.
        assert (test_collection in library.active_collections) == expect_active

    def test_enabled_facets_distributor(self, db: DatabaseTransactionFixture) -> None:
        library = db.library()
        collection1 = db.collection(library=library, protocol=OPDSAPI)
        collection2 = db.collection(library=library, protocol=OverdriveAPI)

        # The enabled_facets method should return the names of the data sources
        assert set(
            library.enabled_facets(FacetConstants.DISTRIBUTOR_FACETS_GROUP_NAME)
        ) == {collection1.data_source.name, collection2.data_source.name}

        # If one of the data sources is deprecated, its new name should be returned instead.
        with patch.object(
            DataSource,
            "DEPRECATED_NAMES",
            frozenbidict({collection2.data_source.name: "New Data Source"}),
        ):
            assert set(
                library.enabled_facets(FacetConstants.DISTRIBUTOR_FACETS_GROUP_NAME)
            ) == {collection1.data_source.name, "New Data Source"}
