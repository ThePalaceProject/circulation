from datetime import date, timedelta
from decimal import Decimal
from functools import partial
from typing import Any, Callable, List
from urllib import parse

import pytest

from api.authentication.base import PatronData
from api.authentication.basic import Keyboards
from api.millenium_patron import (
    AuthenticationMode,
    MilleniumPatronAPI,
    MilleniumPatronLibrarySettings,
    MilleniumPatronSettings,
    NeighborhoodMode,
)
from core.model import Patron
from core.util.datetime_helpers import utc_now
from tests.fixtures.api_millenium_files import MilleniumFilesFixture
from tests.fixtures.database import DatabaseTransactionFixture


class MockResponse:
    def __init__(self, content):
        self.status_code = 200
        self.content = content


class MockAPI(MilleniumPatronAPI):
    queue: List[Any]
    requests_made: List[Any]

    def __init__(
        self,
        library_id,
        integration_id,
        settings,
        library_settings,
        files: MilleniumFilesFixture,
    ):
        super().__init__(library_id, integration_id, settings, library_settings)
        self.files = files
        self.queue = []
        self.requests_made = []

    def enqueue(self, filename):
        data = self.files.sample_data(filename)
        self.queue.append(data)

    def request(self, *args, **kwargs) -> MockResponse:
        self.requests_made.append((args, kwargs))
        response = self.queue[0]
        self.queue = self.queue[1:]
        return MockResponse(response)

    def request_post(self, *args, **kwargs) -> MockResponse:
        self.requests_made.append((args, kwargs))
        response = self.queue[0]
        self.queue = self.queue[1:]
        return MockResponse(response)

    def sample_data(self, filename) -> bytes:
        return self.files.sample_data(filename)


@pytest.fixture
def mock_library_id() -> int:
    return 20


@pytest.fixture
def mock_integration_id() -> int:
    return 20


@pytest.fixture
def create_library_settings() -> Callable[..., MilleniumPatronLibrarySettings]:
    return partial(MilleniumPatronLibrarySettings)


@pytest.fixture
def create_settings() -> Callable[..., MilleniumPatronSettings]:
    return partial(
        MilleniumPatronSettings,
        url="http://url.com/",
        test_identifier="barcode",
    )


@pytest.fixture
def create_provider(
    mock_library_id: int,
    mock_integration_id: int,
    create_settings: Callable[..., MilleniumPatronSettings],
    create_library_settings: Callable[..., MilleniumPatronLibrarySettings],
    api_millenium_patron_files_fixture: MilleniumFilesFixture,
) -> Callable[..., MockAPI]:
    return partial(
        MockAPI,
        integration_id=mock_integration_id,
        library_id=mock_library_id,
        settings=create_settings(),
        library_settings=create_library_settings(),
        files=api_millenium_patron_files_fixture,
    )


class TestMilleniumPatronAPI:
    def test_constructor(
        self,
        create_settings: Callable[..., MilleniumPatronSettings],
        create_provider: Callable[..., MockAPI],
    ):
        settings = create_settings(
            url="http://example.com/", identifier_blacklist=["a", "b"]
        )
        provider = create_provider(settings=settings)
        assert "http://example.com/" == provider.root
        assert ["a", "b"] == [x.pattern for x in provider.blacklist]

    def test__remote_patron_lookup_no_such_patron(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        api = create_provider()
        api.enqueue("dump.no such barcode.html")
        patrondata = PatronData(authorization_identifier="bad barcode")
        assert api.remote_patron_lookup(patrondata) is None

    def test_remote_patron_lookup_success(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        provider = create_provider()
        provider.enqueue("dump.success.html")
        data = PatronData(authorization_identifier="good barcode")
        patrondata = provider.remote_patron_lookup(data)

        # Although "good barcode" was successful in lookup this patron
        # up, it didn't show up in their patron dump as a barcode, so
        # the authorization_identifier from the patron dump took
        # precedence.
        assert isinstance(patrondata, PatronData)
        assert "6666666" == patrondata.permanent_id
        assert "44444444444447" == patrondata.authorization_identifier
        assert "alice" == patrondata.username
        assert Decimal(0) == patrondata.fines
        assert date(2059, 4, 1) == patrondata.authorization_expires
        assert "SHELDON, ALICE" == patrondata.personal_name
        assert "alice@sheldon.com" == patrondata.email_address
        assert PatronData.NO_VALUE == patrondata.block_reason

    def test_remote_patron_lookup_success_nonsensical_labels(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        provider = create_provider()
        provider.enqueue("dump.success_nonsensical_labels.html")
        data = PatronData(authorization_identifier="good barcode")
        patrondata = provider.remote_patron_lookup(data)

        # The barcode is correctly captured from the "NONSENSE[pb]" element.
        # This checks that we care about the 'pb' code and don't care about
        # what comes before it.
        assert isinstance(patrondata, PatronData)
        assert "6666666" == patrondata.permanent_id
        assert "44444444444447" == patrondata.authorization_identifier
        assert "alice" == patrondata.username
        assert Decimal(0) == patrondata.fines
        assert date(2059, 4, 1) == patrondata.authorization_expires
        assert "SHELDON, ALICE" == patrondata.personal_name
        assert "alice@sheldon.com" == patrondata.email_address
        assert PatronData.NO_VALUE == patrondata.block_reason

    def test_remote_patron_lookup_success_alternative_identifier(
        self,
        create_settings: Callable[..., MilleniumPatronSettings],
        create_provider: Callable[..., MockAPI],
    ):
        settings = create_settings(field_used_as_patron_identifier="pu")
        provider = create_provider(settings=settings)
        provider.enqueue("dump.success_alternative_identifier.html")
        data = PatronData(authorization_identifier="good barcode")
        patrondata = provider.remote_patron_lookup(data)

        # The identifier is correctly captured from the "MENINX[pu]" element.
        assert isinstance(patrondata, PatronData)
        assert "6666666" == patrondata.permanent_id
        assert "alice" == patrondata.authorization_identifier
        assert "alice" == patrondata.username
        assert Decimal(0) == patrondata.fines
        assert date(2059, 4, 1) == patrondata.authorization_expires
        assert "SHELDON, ALICE" == patrondata.personal_name
        assert "alice@sheldon.com" == patrondata.email_address
        assert PatronData.NO_VALUE == patrondata.block_reason

    def test_remote_patron_lookup_barcode_spaces(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        provider = create_provider()
        provider.enqueue("dump.success_barcode_spaces.html")
        data = PatronData(authorization_identifier="44444444444447")
        patrondata = provider.remote_patron_lookup(data)
        assert isinstance(patrondata, PatronData)
        assert "44444444444447" == patrondata.authorization_identifier
        assert [
            "44444444444447",
            "4 444 4444 44444 7",
        ] == patrondata.authorization_identifiers

    def test_remote_patron_lookup_block_rules(
        self,
        create_settings: Callable[..., MilleniumPatronSettings],
        create_provider: Callable[..., MockAPI],
    ):
        """This patron has a value of "m" in MBLOCK[56], which generally
        means they are blocked.
        """
        # Default behavior -- anything other than '-' means blocked.
        provider = create_provider()
        provider.enqueue("dump.blocked.html")
        data = PatronData(authorization_identifier="good barcode")
        patrondata = provider.remote_patron_lookup(data)
        assert isinstance(patrondata, PatronData)
        assert PatronData.UNKNOWN_BLOCK == patrondata.block_reason

        # If we set custom block types that say 'm' doesn't really
        # mean the patron is blocked, they're not blocked.
        settings = create_settings(block_types="abcde")
        provider = create_provider(settings=settings)
        provider.enqueue("dump.blocked.html")
        data = PatronData(authorization_identifier="good barcode")
        patrondata = provider.remote_patron_lookup(data)
        assert isinstance(patrondata, PatronData)
        assert PatronData.NO_VALUE == patrondata.block_reason

        # If we set custom block types that include 'm', the patron
        # is blocked.
        settings = create_settings(block_types="lmn")
        provider = create_provider(settings=settings)
        provider.enqueue("dump.blocked.html")
        data = PatronData(authorization_identifier="good barcode")
        patrondata = provider.remote_patron_lookup(data)
        assert isinstance(patrondata, PatronData)
        assert PatronData.UNKNOWN_BLOCK == patrondata.block_reason

    def test_parse_poorly_behaved_dump(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        """The HTML parser is able to handle HTML embedded in
        field values.
        """
        provider = create_provider()
        provider.enqueue("dump.embedded_html.html")
        data = PatronData(authorization_identifier="good barcode")
        patrondata = provider.remote_patron_lookup(data)
        assert isinstance(patrondata, PatronData)
        assert "abcd" == patrondata.authorization_identifier

    def test_incoming_authorization_identifier_retained(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        provider = create_provider()

        # This patron has two barcodes.
        dump = provider.files.sample_data("dump.two_barcodes.html")

        # Let's say they authenticate with the first one.
        patrondata = provider.patron_dump_to_patrondata("FIRST-barcode", dump)
        # Their Patron record will use their first barcode as authorization
        # identifier, because that's what they typed in.
        assert "FIRST-barcode" == patrondata.authorization_identifier

        # Let's say they authenticate with the second barcode.
        patrondata = provider.patron_dump_to_patrondata("SECOND-barcode", dump)
        # Their Patron record will use their second barcode as authorization
        # identifier, because that's what they typed in.
        assert "SECOND-barcode" == patrondata.authorization_identifier

        # Let's say they authenticate with a username.
        patrondata = provider.patron_dump_to_patrondata("username", dump)
        # Their Patron record will suggest the second barcode as
        # authorization identifier, because it's likely to be the most
        # recently added one.
        assert "SECOND-barcode" == patrondata.authorization_identifier

    def test_remote_authenticate_no_such_barcode(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        provider = create_provider()
        provider.enqueue("pintest.no such barcode.html")
        assert provider.remote_authenticate("wrong barcode", "pin") is None

    def test_remote_authenticate_wrong_pin(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        provider = create_provider()
        provider.enqueue("pintest.bad.html")
        assert provider.remote_authenticate("barcode", "wrong pin") is None

    def test_remote_authenticate_correct_pin(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        api = create_provider()
        api.enqueue("pintest.good.html")
        barcode = "barcode1234567!"
        pin = "!correct pin<>@/"
        patrondata = api.remote_authenticate(barcode, pin)
        assert isinstance(patrondata, PatronData)
        # The return value includes everything we know about the
        # authenticated patron, which isn't much.
        assert "barcode1234567!" == patrondata.authorization_identifier

        # The PIN went out URL-encoded. The barcode did not.
        [args, kwargs] = api.requests_made.pop()
        [url] = args
        assert kwargs == {}
        assert url == "http://url.com/{}/{}/pintest".format(
            barcode, parse.quote(pin, safe="")
        )

        # In particular, verify that the slash character in the PIN was encoded;
        # by default, parse.quote leaves it alone.
        assert "%2F" in url

    def test_remote_authenticate_correct_pin_POST(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        """Test that a POST request is made if we ask for it. There's no need to repeat the entirety
        of the tests for GET, as the code takes the same path for everything other than the initial
        request."""
        api = create_provider()
        api.use_post = True
        api.enqueue("pintest.good.html")
        barcode = "barcode1234567!"
        pin = "!correct pin<>@/"
        patrondata = api.remote_authenticate(barcode, pin)
        # The return value includes everything we know about the
        # authenticated patron, which isn't much.
        assert patrondata is not None
        auth_id = patrondata.authorization_identifier
        assert auth_id is not None
        assert "barcode1234567!" == auth_id

        # The PIN went out URL-encoded. The barcode did not.
        # XXX: Do we actually want URL encoding? Does this make sense if the pin is
        #      now inside the body of a POST?
        [args, kwargs] = api.requests_made.pop()
        [url] = args
        assert kwargs == {
            "data": "number=barcode1234567!&pin=%21correct%20pin%3C%3E%40%2F",
            "headers": {"content-type": "application/x-www-form-urlencoded"},
        }
        assert url == "http://url.com/pintest"

    def test_remote_authenticate_username_none(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        """If the username is none, we get none as a return value."""
        api = create_provider()
        assert api.remote_authenticate(None, "pin") is None

    def test_authentication_updates_patron_authorization_identifier(
        self,
        create_provider: Callable[..., MockAPI],
        db: DatabaseTransactionFixture,
    ):
        """Verify that Patron.authorization_identifier is updated when
        necessary and left alone when not necessary.

        This is an end-to-end test. Its components are tested in
        test_authenticator.py (especially TestPatronData) and
        elsewhere in this file. In theory, this test can be removed,
        but it has exposed bugs before.
        """
        library = db.default_library()
        provider = create_provider(library_id=library.id)
        p = db.patron()
        p.external_identifier = "6666666"

        # If the patron is new, and logged in with a username, we'll
        # use the last barcode in the list as their authorization
        # identifier.
        p.authorization_identifier = None
        p.last_external_sync = None
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.two_barcodes.html")
        p2 = provider.authenticated_patron(
            db.session, dict(username="alice", password="pin")
        )
        assert p2 == p
        assert "SECOND-barcode" == p.authorization_identifier

        # If the patron is new, and logged in with a barcode, their
        # authorization identifier will be the barcode they used.
        p.authorization_identifier = None
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.two_barcodes.html")
        provider.authenticated_patron(
            db.session, dict(username="FIRST-barcode", password="pin")
        )
        assert "FIRST-barcode" == p.authorization_identifier

        p.authorization_identifier = None
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.two_barcodes.html")
        provider.authenticated_patron(
            db.session, dict(username="SECOND-barcode", password="pin")
        )
        assert "SECOND-barcode" == p.authorization_identifier

        # If the patron authorizes with their username, we will leave
        # their authorization identifier alone.
        p.authorization_identifier = "abcd"
        provider.enqueue("pintest.good.html")
        provider.authenticated_patron(
            db.session, dict(username="alice", password="pin")
        )
        assert "abcd" == p.authorization_identifier
        assert "alice" == p.username

        # If the patron authorizes with an unrecognized identifier
        # that is not their username, we will immediately sync their
        # metadata with the server. This can correct a case like the
        # one where the patron's authorization identifier is
        # incorrectly set to their username.
        p.authorization_identifier = "alice"
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.two_barcodes.html")
        provider.authenticated_patron(
            db.session, dict(username="FIRST-barcode", password="pin")
        )
        assert "FIRST-barcode" == p.authorization_identifier

        # Or to the case where the patron's authorization identifier is
        # simply not used anymore.
        p.authorization_identifier = "OLD-barcode"
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.two_barcodes.html")
        provider.authenticated_patron(
            db.session, dict(username="SECOND-barcode", password="pin")
        )
        assert "SECOND-barcode" == p.authorization_identifier

        # If the patron has an authorization identifier, and it _is_
        # one of their barcodes, we'll keep it.
        p.authorization_identifier = "FIRST-barcode"
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.two_barcodes.html")
        provider.authenticated_patron(
            db.session, dict(username="alice", password="pin")
        )
        assert "FIRST-barcode" == p.authorization_identifier

        # We'll keep the patron's authorization identifier constant
        # even if the patron has started authenticating with some
        # other identifier.  Third-party services may be tracking the
        # patron with this authorization identifier, and changing it
        # could cause them to lose books.
        #
        # TODO: Keeping a separate field for 'identifier we send to
        # third-party services that don't check the ILS', and using
        # the permanent ID in there, would alleviate this problem for
        # new patrons.
        p.authorization_identifier = "SECOND-barcode"
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.two_barcodes.html")
        provider.authenticated_patron(
            db.session, dict(username="FIRST-barcode", password="pin")
        )
        assert "SECOND-barcode" == p.authorization_identifier

    def test_authenticated_patron_success(
        self,
        create_provider: Callable[..., MockAPI],
        db: DatabaseTransactionFixture,
    ):
        """This test can probably be removed -- it mostly tests functionality
        from BasicAuthAuthenticator.
        """
        library = db.default_library()
        provider = create_provider(library_id=library.id)

        # Patron is valid, but not in our database yet
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.success.html")
        alice = provider.authenticate(
            db.session, dict(username="alice", password="4444")
        )
        assert isinstance(alice, Patron)
        assert "44444444444447" == alice.authorization_identifier
        assert "alice" == alice.username

        # Create another patron who has a different barcode and username,
        # to verify that our authentication mechanism chooses the right patron
        # and doesn't look up whoever happens to be in the database.
        p = db.patron()
        p.username = "notalice"
        p.authorization_identifier = "111111111111"
        db.session.commit()

        # Patron is in the db, now authenticate with barcode
        provider.enqueue("pintest.good.html")
        alice = provider.authenticated_patron(
            db.session,
            dict(username="44444444444447", password="4444"),
        )
        assert isinstance(alice, Patron)
        assert "44444444444447" == alice.authorization_identifier
        assert "alice" == alice.username

        # Authenticate with username again
        provider.enqueue("pintest.good.html")
        alice = provider.authenticated_patron(
            db.session, dict(username="alice", password="4444")
        )
        assert isinstance(alice, Patron)
        assert "44444444444447" == alice.authorization_identifier
        assert "alice" == alice.username

    def test_authenticated_patron_renewed_card(
        self,
        create_provider: Callable[..., MockAPI],
        db: DatabaseTransactionFixture,
    ):
        """This test can be removed -- authenticated_patron is
        tested in test_authenticator.py.
        """
        library = db.default_library()
        provider = create_provider(library_id=library.id)
        now = utc_now()
        one_hour_ago = now - timedelta(seconds=3600)
        one_week_ago = now - timedelta(days=7)

        # Patron is in the database.
        p = db.patron()
        p.authorization_identifier = "44444444444447"

        # We checked them against the ILS one hour ago.
        p.last_external_sync = one_hour_ago

        # Normally, calling authenticated_patron only performs a sync
        # and updates last_external_sync if the last sync was twelve
        # hours ago.
        provider.enqueue("pintest.good.html")
        auth = dict(username="44444444444447", password="4444")
        p2 = provider.authenticated_patron(db.session, auth)
        assert isinstance(p2, Patron)
        assert p2 == p
        assert p2.last_external_sync == one_hour_ago

        # However, if the card has expired, a sync is performed every
        # few seconds.
        ten_seconds_ago = now - timedelta(seconds=10)
        p.authorization_expires = one_week_ago
        p.last_external_sync = ten_seconds_ago
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.success.html")
        p2 = provider.authenticated_patron(db.session, auth)
        assert isinstance(p2, Patron)
        assert p2 == p
        assert p2.last_external_sync is not None

        # Since the sync was performed, last_external_sync was updated.
        assert p2.last_external_sync > one_hour_ago

        # And the patron's card is no longer expired.
        expiration = date(2059, 4, 1)
        assert expiration == p.authorization_expires

    def test_authentication_patron_invalid_expiration_date(
        self,
        create_provider: Callable[..., MockAPI],
        db: DatabaseTransactionFixture,
    ):
        library = db.default_library()
        provider = create_provider(library_id=library.id)
        p = db.patron()
        p.authorization_identifier = "44444444444447"
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.invalid_expiration.html")
        auth = dict(username="44444444444447", password="4444")
        p2 = provider.authenticated_patron(db.session, auth)
        assert p2 == p
        assert p.authorization_expires is None

    def test_authentication_patron_invalid_fine_amount(
        self,
        create_provider: Callable[..., MockAPI],
        db: DatabaseTransactionFixture,
    ):
        library = db.default_library()
        provider = create_provider(library_id=library.id)
        p = db.patron()
        p.authorization_identifier = "44444444444447"
        provider.enqueue("pintest.good.html")
        provider.enqueue("dump.invalid_fines.html")
        auth = dict(username="44444444444447", password="4444")
        p2 = provider.authenticated_patron(db.session, auth)
        assert p2 == p
        assert 0 == p.fines

    def test_patron_dump_to_patrondata(
        self,
        create_provider: Callable[..., MockAPI],
    ):
        api = create_provider()
        content = api.files.sample_data("dump.success.html")
        patrondata = api.patron_dump_to_patrondata("alice", content)
        assert "44444444444447" == patrondata.authorization_identifier
        assert "alice" == patrondata.username
        assert patrondata.library_identifier is None

    def test_patron_dump_to_patrondata_restriction_field(
        self,
        create_provider: Callable[..., MockAPI],
        create_library_settings: Callable[..., MilleniumPatronLibrarySettings],
    ):
        library_settings = create_library_settings(
            library_identifier_field="HOME LIBR[p53]"
        )
        patron = create_provider(library_settings=library_settings)
        content = patron.sample_data("dump.success.html")
        patrondata = patron.patron_dump_to_patrondata("alice", content)
        assert "mm" == patrondata.library_identifier
        library_settings = create_library_settings(
            library_identifier_field="P TYPE[p47]"
        )
        patron = create_provider(library_settings=library_settings)
        content = patron.sample_data("dump.success.html")
        patrondata = patron.patron_dump_to_patrondata("alice", content)
        assert "10" == patrondata.library_identifier

    def test_neighborhood(
        self,
        create_provider: Callable[..., MockAPI],
        create_settings: Callable[..., MilleniumPatronSettings],
    ):
        # The value of PatronData.neighborhood depends on the 'neighborhood mode' setting.

        # Default behavior is not to gather neighborhood information at all.
        settings = create_settings()
        provider = create_provider(settings=settings)
        content = provider.sample_data("dump.success.html")
        patrondata = provider.patron_dump_to_patrondata("alice", content)
        assert PatronData.NO_VALUE == patrondata.neighborhood

        # Patron neighborhood may be the identifier of their home library branch.
        settings = create_settings(neighborhood_mode=NeighborhoodMode.HOME_BRANCH)
        provider = create_provider(settings=settings)
        content = provider.sample_data("dump.success.html")
        patrondata = provider.patron_dump_to_patrondata("alice", content)
        assert "mm" == patrondata.neighborhood

        # Or it may be the ZIP code of their home address.
        settings = create_settings(neighborhood_mode=NeighborhoodMode.POSTAL_CODE)
        provider = create_provider(settings=settings)
        patrondata = provider.patron_dump_to_patrondata("alice", content)
        assert "10001" == patrondata.neighborhood

    def test_authorization_identifier_blacklist(
        self,
        create_provider: Callable[..., MockAPI],
        create_settings: Callable[..., MilleniumPatronSettings],
    ):
        """A patron has two authorization identifiers. Ordinarily the second
        one (which would normally be preferred), but it contains a
        blacklisted string, so the first takes precedence.
        """
        provider = create_provider()
        content = provider.files.sample_data("dump.two_barcodes.html")
        patrondata = provider.patron_dump_to_patrondata("alice", content)
        assert "SECOND-barcode" == patrondata.authorization_identifier

        settings = create_settings(identifier_blacklist=["second"])
        provider = create_provider(settings=settings)
        patrondata = provider.patron_dump_to_patrondata("alice", content)
        assert "FIRST-barcode" == patrondata.authorization_identifier

    def test_blacklist_may_remove_every_authorization_identifier(
        self,
        create_provider: Callable[..., MockAPI],
        create_settings: Callable[..., MilleniumPatronSettings],
    ):
        """A patron may end up with no authorization identifier whatsoever
        because they're all blacklisted.
        """
        settings = create_settings(identifier_blacklist=["barcode"])
        provider = create_provider(settings=settings)
        content = provider.sample_data("dump.two_barcodes.html")
        patrondata = provider.patron_dump_to_patrondata("alice", content)
        assert patrondata.NO_VALUE == patrondata.authorization_identifier
        assert [] == patrondata.authorization_identifiers

    def test_verify_certificate(
        self,
        create_provider: Callable[..., MockAPI],
        create_settings: Callable[..., MilleniumPatronSettings],
    ):
        """Test the ability to bypass verification of the Millenium Patron API
        server's SSL certificate.
        """
        api = create_provider()

        # By default, verify_certificate is True.
        assert api.verify_certificate is True

        settings = create_settings(verify_certificate=False)
        api = create_provider(settings=settings)
        assert api.verify_certificate is False

        # Test that the value of verify_certificate becomes the
        # 'verify' argument when _modify_request_kwargs() is called.
        settings = create_settings(verify_certificate=True)
        api = create_provider(settings=settings)
        kwargs = dict(verify=False)
        api._update_request_kwargs(kwargs)
        assert kwargs["verify"] is True

        # NOTE: We can't automatically test that request() actually
        # calls _modify_request_kwargs() because request() is the
        # method we override for mock purposes.

    def test_patron_block_reason(self):
        m = MilleniumPatronAPI._patron_block_reason
        blocked = PatronData.UNKNOWN_BLOCK
        unblocked = PatronData.NO_VALUE

        # Our default behavior.
        assert blocked == m(None, "a")
        assert unblocked == m(None, None)
        assert unblocked == m(None, "-")
        assert unblocked == m(None, " ")

        # Behavior with custom block values.
        assert blocked == m("abcd", "b")
        assert unblocked == m("abcd", "e")
        assert unblocked == m("", "-")

        # This is unwise but allowed.
        assert blocked == m("ab-c", "-")

    def test_family_name_match(self):
        m = MilleniumPatronAPI.family_name_match
        assert m(None, None) is False
        assert m(None, "") is False
        assert m("", None) is False
        assert m("", "") is True
        assert m("cher", "cher") is True
        assert m("chert", "cher") is False
        assert m("cher", "chert") is False
        assert m("cherryh, c.j.", "cherryh") is True
        assert m("c.j. cherryh", "cherryh") is True
        assert m("caroline janice cherryh", "cherryh") is True

    def test_authorization_without_password(
        self,
        create_provider: Callable[..., MockAPI],
        create_settings: Callable[..., MilleniumPatronSettings],
    ):
        """Test authorization when no password is required, only
        patron identifier.
        """
        settings = create_settings(password_keyboard=Keyboards.NULL)
        api = create_provider(settings=settings)

        assert api.collects_password is False
        # If the patron lookup succeeds, the user is authenticated
        # as that patron.
        api.enqueue("dump.success.html")
        patrondata = api.remote_authenticate("44444444444447", None)
        assert isinstance(patrondata, PatronData)
        assert "44444444444447" == patrondata.authorization_identifier

        # If it fails, the user is not authenticated.
        api.enqueue("dump.no such barcode.html")
        patrondata = api.remote_authenticate("44444444444447", None)
        assert patrondata is None

    def test_authorization_family_name_success(
        self,
        create_provider: Callable[..., MockAPI],
        create_settings: Callable[..., MilleniumPatronSettings],
    ):
        """Test authenticating against the patron's family name, given the
        correct name (case-insensitive)
        """
        settings = create_settings(authentication_mode=AuthenticationMode.FAMILY_NAME)
        api = create_provider(settings=settings)
        api.enqueue("dump.success.html")
        patrondata = api.remote_authenticate("44444444444447", "Sheldon")
        assert isinstance(patrondata, PatronData)
        assert "44444444444447" == patrondata.authorization_identifier

        # Since we got a full patron dump, the PatronData we get back
        # is complete.
        assert patrondata.complete is True

    def test_authorization_family_name_failure(
        self,
        create_provider: Callable[..., MockAPI],
        create_settings: Callable[..., MilleniumPatronSettings],
    ):
        """Test authenticating against the patron's family name, given the
        incorrect name
        """
        settings = create_settings(authentication_mode=AuthenticationMode.FAMILY_NAME)
        api = create_provider(settings=settings)
        api.enqueue("dump.success.html")
        assert api.remote_authenticate("44444444444447", "wrong name") is None

    def test_authorization_family_name_no_such_patron(
        self,
        create_provider: Callable[..., MockAPI],
        create_settings: Callable[..., MilleniumPatronSettings],
    ):
        """If no patron is found, authorization based on family name cannot
        proceed.
        """
        settings = create_settings(authentication_mode=AuthenticationMode.FAMILY_NAME)
        api = create_provider(settings=settings)
        api.enqueue("dump.no such barcode.html")
        assert api.remote_authenticate("44444444444447", "somebody") is None

    def test_extract_postal_code(self):
        # Test our heuristics for extracting postal codes from address fields.
        m = MilleniumPatronAPI.extract_postal_code
        assert "93203" == m("1 Main Street$Arvin CA 93203")
        assert "93203" == m("1 Main Street\nArvin CA 93203")
        assert "93203" == m("10145 Main Street$Arvin CA 93203")
        assert "93203" == m("10145 Main Street$Arvin CA$93203")
        assert "93203" == m("10145-6789 Main Street$Arvin CA 93203-1234")
        assert "93203" == m("10145-6789 Main Street$Arvin CA 93203-1234 (old address)")
        assert "93203" == m("10145-6789 Main Street$Arvin CA 93203 (old address)")
        assert "93203" == m(
            "10145-6789 Main Street Apartment #12345$Arvin CA 93203 (old address)"
        )

        assert m("10145 Main Street Apartment 123456$Arvin CA") is None
        assert m("10145 Main Street$Arvin CA") is None
        assert m("123 Main Street") is None

        # Some cases where we incorrectly detect a ZIP code where there is none.
        assert "12345" == m("10145 Main Street, Apartment #12345$Arvin CA")
