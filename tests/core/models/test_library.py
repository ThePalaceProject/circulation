import pytest
from Crypto.PublicKey.RSA import RsaKey, import_key

from core.model.configuration import ConfigurationSetting
from core.model.library import Library
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

    def test_all_collections(self, db: DatabaseTransactionFixture):
        library = db.default_library()

        parent = db.collection()
        db.default_collection().parent_id = parent.id

        assert [db.default_collection()] == library.collections
        assert {db.default_collection(), parent} == set(library.all_collections)

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
        db.default_library().collections = []
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

        integration = db.external_integration("protocol", "goal")
        integration.url = "http://url/"
        integration.username = "someuser"
        integration.password = "somepass"
        integration.setting("somesetting").value = "somevalue"

        # Different libraries specialize this integration differently.
        ConfigurationSetting.for_library_and_externalintegration(
            session, "library-specific", library, integration
        ).value = "value for library1"

        library2 = db.library()
        ConfigurationSetting.for_library_and_externalintegration(
            session, "library-specific", library2, integration
        ).value = "value for library2"

        library.integrations.append(integration)

        expect = (
            """Library UUID: "uuid"
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
facets_enabled_collection='['full', 'featured']'
facets_default_collection='full'
help_web='http://library.com/support'
default_notification_email_address='noreply@thepalaceproject.org'
color_scheme='blue'
web_primary_color='#377F8B'
web_secondary_color='#D53F34'
web_header_links='[]'
web_header_labels='[]'
hidden_content_types='[]'

External integrations:
----------------------
ID: %s
Protocol/Goal: protocol/goal
library-specific='value for library1' (applies only to The Library)
somesetting='somevalue'
url='http://url/'
username='someuser'
"""
            % integration.id
        )
        actual = library.explain()
        assert expect == "\n".join(actual)

        with_secrets = library.explain(True)
        assert 'Shared secret (for library registry): "secret"' in with_secrets
        assert "password='somepass'" in with_secrets

    def test_generate_keypair(self, db: DatabaseTransactionFixture):
        # Test the ability to create a public/private key pair

        # If you pass in a ConfigurationSetting that is missing its
        # value, or whose value is not a public key pair, a new key
        # pair is created.
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
