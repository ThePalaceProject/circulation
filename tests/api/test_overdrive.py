from __future__ import annotations

import base64
import json
import os
import random
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Dict
from unittest.mock import MagicMock, create_autospec

import pytest
from requests import Response
from sqlalchemy.orm.exc import StaleDataError

from api.circulation import CirculationAPI, FulfillmentInfo, HoldInfo, LoanInfo
from api.circulation_exceptions import *
from api.config import Configuration
from api.overdrive import (
    NewTitlesOverdriveCollectionMonitor,
    OverdriveAPI,
    OverdriveCirculationMonitor,
    OverdriveCollectionReaper,
    OverdriveFormatSweep,
    OverdriveManifestFulfillmentInfo,
    RecentOverdriveCollectionMonitor,
)
from core.config import CannotLoadConfiguration
from core.metadata_layer import TimestampData
from core.model import (
    ConfigurationSetting,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Identifier,
    LicensePool,
    MediaTypes,
    Representation,
    RightsStatus,
)
from core.overdrive import OverdriveConfiguration
from core.util.datetime_helpers import datetime_utc, utc_now
from tests.api.mockapi.overdrive import MockOverdriveAPI
from tests.core.mock import DummyHTTPClient, MockRequestsResponse

if TYPE_CHECKING:
    from ..fixtures.api_overdrive_files import OverdriveAPIFilesFixture
    from ..fixtures.authenticator import AuthProviderFixture
    from ..fixtures.database import DatabaseTransactionFixture
    from ..fixtures.time import Time


class OverdriveAPIFixture:
    def __init__(self, db: DatabaseTransactionFixture, data: OverdriveAPIFilesFixture):
        self.db = db
        self.data = data
        library = db.default_library()
        self.collection = MockOverdriveAPI.mock_collection(db.session)
        self.circulation = CirculationAPI(
            db.session,
            library,
            api_map={ExternalIntegration.OVERDRIVE: MockOverdriveAPI},
        )
        self.api: OverdriveAPI = self.circulation.api_for_collection[self.collection.id]
        os.environ[
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}"
        ] = "TestingKey"
        os.environ[
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}"
        ] = "TestingSecret"

    def error_message(self, error_code, message=None, token=None):
        """Create a JSON document that simulates the message served by
        Overdrive given a certain error condition.
        """
        message = message or self.db.fresh_str()
        token = token or self.db.fresh_str()
        data = dict(errorCode=error_code, message=message, token=token)
        return json.dumps(data)

    def sample_json(self, filename):
        data = self.data.sample_data(filename)
        return data, json.loads(data)


@pytest.fixture(scope="function")
def overdrive_api_fixture(
    db: DatabaseTransactionFixture,
    api_overdrive_files_fixture: OverdriveAPIFilesFixture,
) -> OverdriveAPIFixture:
    return OverdriveAPIFixture(db, api_overdrive_files_fixture)


class TestOverdriveAPI:
    def test_external_integration(self, overdrive_api_fixture: OverdriveAPIFixture):
        assert (
            overdrive_api_fixture.collection.external_integration
            == overdrive_api_fixture.api.external_integration(
                overdrive_api_fixture.db.session
            )
        )

    def test_lock_in_format(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Verify which formats do or don't need to be locked in before
        # fulfillment.
        needs_lock_in = overdrive_api_fixture.api.LOCK_IN_FORMATS

        # Streaming and manifest-based formats are exempt; all
        # other formats need lock-in.
        exempt = list(overdrive_api_fixture.api.STREAMING_FORMATS) + list(
            overdrive_api_fixture.api.MANIFEST_INTERNAL_FORMATS
        )
        for i in overdrive_api_fixture.api.FORMATS:
            if i not in exempt:
                assert i in needs_lock_in
        for i in exempt:
            assert i not in needs_lock_in

    def test__run_self_tests(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
    ):
        # Verify that OverdriveAPI._run_self_tests() calls the right
        # methods.
        db = overdrive_api_fixture.db

        class Mock(MockOverdriveAPI):
            "Mock every method used by OverdriveAPI._run_self_tests."

            # First we will call check_creds() to get a fresh credential.
            mock_credential = object()

            def check_creds(self, force_refresh=False):
                self.check_creds_called_with = force_refresh
                return self.mock_credential

            # Then we will call get_advantage_accounts().
            mock_advantage_accounts = [object(), object()]

            def get_advantage_accounts(self):
                return self.mock_advantage_accounts

            # Then we will call get() on the _all_products_link.
            def get(self, url, extra_headers, exception_on_401=False):
                self.get_called_with = (url, extra_headers, exception_on_401)
                return 200, {}, json.dumps(dict(totalItems=2010))

            # Finally, for every library associated with this
            # collection, we'll call get_patron_credential() using
            # the credentials of that library's test patron.
            mock_patron_credential = object()
            get_patron_credential_called_with = []

            def get_patron_credential(self, patron, pin):
                self.get_patron_credential_called_with.append((patron, pin))
                return self.mock_patron_credential

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = db.library()
        overdrive_api_fixture.collection.libraries.append(no_default_patron)

        with_default_patron = db.default_library()
        create_simple_auth_integration(with_default_patron)

        # Now that everything is set up, run the self-test.
        api = Mock(db.session, overdrive_api_fixture.collection)
        results = sorted(api._run_self_tests(db.session), key=lambda x: x.name)
        [
            no_patron_credential,
            default_patron_credential,
            global_privileges,
            collection_size,
            advantage,
        ] = results

        # Verify that each test method was called and returned the
        # expected SelfTestResult object.
        assert (
            "Checking global Client Authentication privileges" == global_privileges.name
        )
        assert True == global_privileges.success
        assert api.mock_credential == global_privileges.result

        assert "Looking up Overdrive Advantage accounts" == advantage.name
        assert True == advantage.success
        assert "Found 2 Overdrive Advantage account(s)." == advantage.result

        assert "Counting size of collection" == collection_size.name
        assert True == collection_size.success
        assert "2010 item(s) in collection" == collection_size.result
        url, headers, error_on_401 = api.get_called_with
        assert api._all_products_link == url

        assert (
            "Acquiring test patron credentials for library %s" % no_default_patron.name
            == no_patron_credential.name
        )
        assert False == no_patron_credential.success
        assert "Library has no test patron configured." == str(
            no_patron_credential.exception
        )

        assert (
            "Checking Patron Authentication privileges, using test patron for library %s"
            % with_default_patron.name
            == default_patron_credential.name
        )
        assert True == default_patron_credential.success
        assert api.mock_patron_credential == default_patron_credential.result

        # Although there are two libraries associated with this
        # collection, get_patron_credential was only called once, because
        # one of the libraries doesn't have a default patron.
        [(patron1, password1)] = api.get_patron_credential_called_with
        assert "username1" == patron1.authorization_identifier
        assert "password1" == password1

    def test_run_self_tests_short_circuit(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """If OverdriveAPI.check_creds can't get credentials, the rest of
        the self-tests aren't even run.

        This probably doesn't matter much, because if check_creds doesn't
        work we won't be able to instantiate the OverdriveAPI class.
        """

        def explode(*args, **kwargs):
            raise Exception("Failure!")

        overdrive_api_fixture.api.check_creds = explode

        # Only one test will be run.
        [check_creds] = overdrive_api_fixture.api._run_self_tests(
            overdrive_api_fixture.db.session
        )
        assert "Failure!" == str(check_creds.exception)

    def test_default_notification_email_address(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """Test the ability of the Overdrive API to detect an email address
        previously given by the patron to Overdrive for the purpose of
        notifications.
        """
        db = overdrive_api_fixture.db

        ignore, patron_with_email = overdrive_api_fixture.sample_json(
            "patron_info.json"
        )
        overdrive_api_fixture.api.queue_response(200, content=patron_with_email)
        patron = db.patron()

        # The site default for notification emails will never be used.
        configuration_setting = ConfigurationSetting.for_library(
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS, db.default_library()
        )
        configuration_setting.value = "notifications@example.com"

        # If the patron has used a particular email address to put
        # books on hold, use that email address, not the site default.
        assert (
            "foo@bar.com"
            == overdrive_api_fixture.api.default_notification_email_address(
                patron, "pin"
            )
        )

        # If the patron's email address according to Overdrive _is_
        # the site default, it is ignored. This can only happen if
        # this patron placed a hold using an older version of the
        # circulation manager.
        patron_with_email["lastHoldEmail"] = configuration_setting.value
        overdrive_api_fixture.api.queue_response(200, content=patron_with_email)
        assert None == overdrive_api_fixture.api.default_notification_email_address(
            patron, "pin"
        )

        # If the patron has never before put an Overdrive book on
        # hold, their JSON object has no `lastHoldEmail` key. In this
        # case we return None -- again, ignoring the site default.
        patron_with_no_email = dict(patron_with_email)
        del patron_with_no_email["lastHoldEmail"]
        overdrive_api_fixture.api.queue_response(200, content=patron_with_no_email)
        assert None == overdrive_api_fixture.api.default_notification_email_address(
            patron, "pin"
        )

        # If there's an error getting the information from Overdrive,
        # we return None.
        overdrive_api_fixture.api.queue_response(404)
        assert None == overdrive_api_fixture.api.default_notification_email_address(
            patron, "pin"
        )

    def test_scope_string(self, overdrive_api_fixture: OverdriveAPIFixture):
        # scope_string() puts the website ID of the Overdrive
        # integration and the ILS name associated with the library
        # into the form expected by Overdrive.
        db = overdrive_api_fixture.db
        expect = "websiteid:{} authorizationname:{}".format(
            overdrive_api_fixture.api.website_id().decode("utf-8"),
            overdrive_api_fixture.api.ils_name(db.default_library()),
        )
        assert expect == overdrive_api_fixture.api.scope_string(db.default_library())

    def test_checkout(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Verify the process of checking out a book.
        db = overdrive_api_fixture.db
        patron = object()
        pin = object()
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)
        identifier = pool.identifier

        class Mock(MockOverdriveAPI):
            MOCK_EXPIRATION_DATE = object()
            PROCESS_CHECKOUT_ERROR_RESULT = Exception(
                "exception in _process_checkout_error"
            )

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.extract_expiration_date_called_with = []
                self._process_checkout_error_called_with = []

            def extract_expiration_date(self, loan):
                self.extract_expiration_date_called_with.append(loan)
                return self.MOCK_EXPIRATION_DATE

            def _process_checkout_error(self, patron, pin, licensepool, data):
                self._process_checkout_error_called_with.append(
                    (patron, pin, licensepool, data)
                )
                result = self.PROCESS_CHECKOUT_ERROR_RESULT
                if isinstance(result, Exception):
                    raise result
                return result

        # First, test the successful path.
        api = Mock(db.session, overdrive_api_fixture.collection)
        api_response = json.dumps("some data")
        api.queue_response(201, content=api_response)
        loan = api.checkout(patron, pin, pool, "internal format is ignored")

        # Verify that a good-looking patron request went out.
        endpoint, ignore, kwargs = api.requests.pop()
        assert endpoint.endswith("/me/checkouts")
        assert patron == kwargs.pop("_patron")
        extra_headers = kwargs.pop("extra_headers")
        assert {"Content-Type": "application/json"} == extra_headers
        data = json.loads(kwargs.pop("data"))
        assert {
            "fields": [{"name": "reserveId", "value": pool.identifier.identifier}]
        } == data

        # The API response was passed into extract_expiration_date.
        #
        # The most important thing here is not the content of the response but the
        # fact that the response code was not 400.
        assert "some data" == api.extract_expiration_date_called_with.pop()

        # The return value is a LoanInfo object with all relevant info.
        assert isinstance(loan, LoanInfo)
        assert pool.collection.id == loan.collection_id
        assert pool.data_source.name == loan.data_source_name
        assert identifier.type == loan.identifier_type
        assert identifier.identifier == loan.identifier
        assert None == loan.start_date
        assert api.MOCK_EXPIRATION_DATE == loan.end_date

        # _process_checkout_error was not called
        assert [] == api._process_checkout_error_called_with

        # Now let's test error conditions.

        # Most of the time, an error simply results in an exception.
        api.queue_response(400, content=api_response)
        with pytest.raises(Exception) as excinfo:
            api.checkout(patron, pin, pool, "internal format is ignored")
        assert "exception in _process_checkout_error" in str(excinfo.value)
        assert (
            patron,
            pin,
            pool,
            "some data",
        ) == api._process_checkout_error_called_with.pop()

        # However, if _process_checkout_error is able to recover from
        # the error and ends up returning something, the return value
        # is propagated from checkout().
        api.PROCESS_CHECKOUT_ERROR_RESULT = "Actually, I was able to recover"
        api.queue_response(400, content=api_response)
        assert "Actually, I was able to recover" == api.checkout(
            patron, pin, pool, "internal format is ignored"
        )
        assert (
            patron,
            pin,
            pool,
            "some data",
        ) == api._process_checkout_error_called_with.pop()

    def test__process_checkout_error(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Verify that _process_checkout_error handles common API-side errors,
        # making follow-up API calls if necessary.
        db = overdrive_api_fixture.db

        class Mock(MockOverdriveAPI):
            MOCK_LOAN = object()
            MOCK_EXPIRATION_DATE = object()

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.update_licensepool_called_with = []
                self.get_loan_called_with = []
                self.extract_expiration_date_called_with = []

            def update_licensepool(self, identifier):
                self.update_licensepool_called_with.append(identifier)

            def get_loan(self, patron, pin, identifier):
                self.get_loan_called_with.append((patron, pin, identifier))
                return self.MOCK_LOAN

            def extract_expiration_date(self, loan):
                self.extract_expiration_date_called_with.append(loan)
                return self.MOCK_EXPIRATION_DATE

        patron = object()
        pin = object()
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)
        identifier = pool.identifier
        api = Mock(db.session, overdrive_api_fixture.collection)
        m = api._process_checkout_error

        # Most of the error handling is pretty straightforward.
        def with_error_code(code):
            # Simulate the response of the Overdrive API with a given error code.
            error = dict(errorCode=code)

            # Handle the error.
            return m(patron, pin, pool, error)

        # Errors not specifically known become generic CannotLoan exceptions.
        with pytest.raises(CannotLoan) as excinfo:  # type: ignore
            with_error_code("WeirdError")
        assert "WeirdError" in str(excinfo.value)

        # If the data passed in to _process_checkout_error is not what
        # the real Overdrive API would send, the error is even more
        # generic.
        with pytest.raises(CannotLoan) as excinfo:  # type: ignore
            m(patron, pin, pool, "Not a dict")
        assert "Unknown Error" in str(excinfo.value)
        with pytest.raises(CannotLoan) as excinfo:  # type: ignore
            m(patron, pin, pool, dict(errorCodePresent=False))
        assert "Unknown Error" in str(excinfo.value)

        # Some known errors become specific subclasses of CannotLoan.
        pytest.raises(
            PatronLoanLimitReached, with_error_code, "PatronHasExceededCheckoutLimit"  # type: ignore
        )
        pytest.raises(
            PatronLoanLimitReached,  # type: ignore
            with_error_code,
            "PatronHasExceededCheckoutLimit_ForCPC",
        )

        # There are two cases where we need to make follow-up API
        # requests as the result of a failure during the loan process.

        # First, if the error is "NoCopiesAvailable", we know we have
        # out-of-date availability information and we need to call
        # update_licensepool before raising NoAvailbleCopies().
        pytest.raises(NoAvailableCopies, with_error_code, "NoCopiesAvailable")  # type: ignore
        assert identifier.identifier == api.update_licensepool_called_with.pop()

        # If the error is "TitleAlreadyCheckedOut", then the problem
        # is that the patron tried to take out a new loan instead of
        # fulfilling an existing loan. In this case we don't raise an
        # exception at all; we fulfill the loan and return a LoanInfo
        # object.
        loan = with_error_code("TitleAlreadyCheckedOut")

        # get_loan was called with the patron's details.
        assert (patron, pin, identifier.identifier) == api.get_loan_called_with.pop()

        # extract_expiration_date was called on the return value of get_loan.
        assert api.MOCK_LOAN == api.extract_expiration_date_called_with.pop()

        # And a LoanInfo was created with all relevant information.
        assert isinstance(loan, LoanInfo)
        assert pool.collection.id == loan.collection_id
        assert pool.data_source.name == loan.data_source_name
        assert identifier.type == loan.identifier_type
        assert identifier.identifier == loan.identifier
        assert None == loan.start_date
        assert api.MOCK_EXPIRATION_DATE == loan.end_date

    def test_extract_expiration_date(self):
        # Test the code that finds and parses a loan expiration date.
        m = OverdriveAPI.extract_expiration_date

        # Success
        assert datetime_utc(2020, 1, 2, 3, 4, 5) == m(
            dict(expires="2020-01-02T03:04:05Z")
        )

        # Various failure cases.
        assert None == m(dict(expiresPresent=False))
        assert None == m(dict(expires="Wrong date format"))
        assert None == m("Not a dict")
        assert None == m(None)

    def test_place_hold(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # Verify that an appropriate request is made to HOLDS_ENDPOINT
        # to create a hold.
        #
        # The request will include different form fields depending on
        # whether default_notification_email_address returns something.
        class Mock(MockOverdriveAPI):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.DEFAULT_NOTIFICATION_EMAIL_ADDRESS = None

            def default_notification_email_address(self, patron, pin):
                self.default_notification_email_address_called_with = (patron, pin)
                return self.DEFAULT_NOTIFICATION_EMAIL_ADDRESS

            def fill_out_form(self, **form_fields):
                # Record the form fields and return some dummy values.
                self.fill_out_form_called_with = form_fields
                return "headers", "filled-out form"

            def patron_request(self, *args, **kwargs):
                # Pretend to make a request to an API endpoint.
                self.patron_request_called_with = (args, kwargs)
                return "A mock response"

            def process_place_hold_response(self, response, patron, pin, licensepool):
                self.process_place_hold_response_called_with = (
                    response,
                    patron,
                    pin,
                    licensepool,
                )
                return "OK, I processed it."

        # First, test the case where no notification email address is
        # provided and there is no default.
        patron = object()
        pin = object()
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)
        api = Mock(db.session, overdrive_api_fixture.collection)
        response = api.place_hold(patron, pin, pool, None)

        # Now we can trace the path of the input through the method calls.

        # The patron and PIN were passed into
        # default_notification_email_address.
        assert (patron, pin) == api.default_notification_email_address_called_with

        # The return value was None, and so 'ignoreHoldEmail' was
        # added to the form to be filled out, rather than
        # 'emailAddress' being added.
        fields = api.fill_out_form_called_with
        identifier = str(pool.identifier.identifier)
        assert dict(ignoreHoldEmail=True, reserveId=identifier) == fields

        # patron_request was called with the filled-out form and other
        # information necessary to authenticate the request.
        args, kwargs = api.patron_request_called_with
        assert (patron, pin, api.HOLDS_ENDPOINT, "headers", "filled-out form") == args
        assert {} == kwargs

        # Finally, process_place_hold_response was called on
        # the return value of patron_request
        assert (
            "A mock response",
            patron,
            pin,
            pool,
        ) == api.process_place_hold_response_called_with
        assert "OK, I processed it." == response

        # Now we need to test two more cases.
        #
        # First, the patron has a holds notification address
        # registered with Overdrive.
        email = "holds@patr.on"
        api.DEFAULT_NOTIFICATION_EMAIL_ADDRESS = email
        response = api.place_hold(patron, pin, pool, None)

        # Same result.
        assert "OK, I processed it." == response

        # Different variables were passed in to fill_out_form.
        fields = api.fill_out_form_called_with
        assert dict(emailAddress=email, reserveId=identifier) == fields

        # Finally, test that when a specific address is passed in, it
        # takes precedence over the patron's holds notification address.

        response = api.place_hold(patron, pin, pool, "another@addre.ss")
        assert "OK, I processed it." == response
        fields = api.fill_out_form_called_with
        assert dict(emailAddress="another@addre.ss", reserveId=identifier) == fields

    def test_process_place_hold_response(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # Verify that we can handle various error and non-error responses
        # to a HOLDS_ENDPOINT request.

        ignore, successful_hold = overdrive_api_fixture.sample_json(
            "successful_hold.json"
        )

        class Mock(MockOverdriveAPI):
            def get_hold(self, patron, pin, overdrive_id):
                # Return a sample hold representation rather than
                # making another API request.
                self.get_hold_called_with = (patron, pin, overdrive_id)
                return successful_hold

        api = Mock(db.session, overdrive_api_fixture.collection)

        def process_error_response(message):
            # Attempt to process a response that resulted in an error.
            if isinstance(message, (bytes, str)):
                data = dict(errorCode=message)
            else:
                data = message
            response = MockRequestsResponse(400, content=data)
            return api.process_place_hold_response(response, None, None, None)

        # Some error messages result in specific CirculationExceptions.
        pytest.raises(CannotRenew, process_error_response, "NotWithinRenewalWindow")  # type: ignore
        pytest.raises(
            PatronHoldLimitReached, process_error_response, "PatronExceededHoldLimit"  # type: ignore
        )

        # An unrecognized error message results in a generic
        # CannotHold.
        pytest.raises(CannotHold, process_error_response, "SomeOtherError")  # type: ignore

        # Same if the error message is missing or the response can't be
        # processed.
        pytest.raises(CannotHold, process_error_response, dict())  # type: ignore
        pytest.raises(CannotHold, process_error_response, None)  # type: ignore

        # Same if the error code isn't in the 4xx or 2xx range
        # (which shouldn't happen in real life).
        response = MockRequestsResponse(999)
        pytest.raises(
            CannotHold, api.process_place_hold_response, response, None, None, None  # type: ignore
        )

        # At this point patron and book details become important --
        # we're going to return a HoldInfo object and potentially make
        # another API request.
        patron = db.patron()
        pin = object()
        licensepool = db.licensepool(edition=None)

        # The remaining tests will end up running the same code on the
        # same data, so they will return the same HoldInfo. Define a
        # helper method to make this easier.
        def assert_correct_holdinfo(x):
            assert isinstance(x, HoldInfo)
            assert licensepool.collection == x.collection(db.session)
            assert licensepool.data_source.name == x.data_source_name
            assert identifier.identifier == x.identifier
            assert identifier.type == x.identifier_type
            assert datetime_utc(2015, 3, 26, 11, 30, 29) == x.start_date
            assert None == x.end_date
            assert 1 == x.hold_position

        # Test the case where the 'error' is that the book is already
        # on hold.
        already_on_hold = dict(errorCode="AlreadyOnWaitList")
        response = MockRequestsResponse(400, content=already_on_hold)
        result = api.process_place_hold_response(response, patron, pin, licensepool)

        # get_hold() was called with the arguments we expect.
        identifier = licensepool.identifier
        assert (patron, pin, identifier.identifier) == api.get_hold_called_with

        # The result was converted into a HoldInfo object. The
        # effective result is exactly as if we had successfully put
        # the book on hold.
        assert_correct_holdinfo(result)

        # Finally, let's test the case where there was no hold and now
        # there is.
        api.get_hold_called_with = None
        response = MockRequestsResponse(200, content=successful_hold)
        result = api.process_place_hold_response(response, patron, pin, licensepool)
        assert_correct_holdinfo(result)

        # Here, get_hold was _not_ called, because the hold didn't
        # already exist.
        assert None == api.get_hold_called_with

    def test_checkin(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        class Mock(MockOverdriveAPI):
            EARLY_RETURN_SUCCESS = False

            def perform_early_return(self, *args):
                self.perform_early_return_call = args
                return self.EARLY_RETURN_SUCCESS

            def patron_request(self, *args, **kwargs):
                self.patron_request_call = (args, kwargs)

        overdrive = Mock(db.session, overdrive_api_fixture.collection)
        overdrive.perform_early_return_call = None

        # In most circumstances we do not bother calling
        # perform_early_return; we just call patron_request.
        pool = db.licensepool(None)
        patron = db.patron()
        pin = object()
        expect_url = overdrive.endpoint(
            overdrive.CHECKOUT_ENDPOINT, overdrive_id=pool.identifier.identifier
        )

        def assert_no_early_return():
            """Call this to verify that patron_request is
            called within checkin() instead of perform_early_return.
            """
            overdrive.checkin(patron, pin, pool)

            # perform_early_return was not called.
            assert None == overdrive.perform_early_return_call

            # patron_request was called in an attempt to
            # DELETE an active loan.
            args, kwargs = overdrive.patron_request_call
            assert (patron, pin, expect_url) == args
            assert dict(method="DELETE") == kwargs
            overdrive.patron_request_call = None

        # If there is no loan, there is no perform_early_return.
        assert_no_early_return()

        # Same if the loan is not fulfilled...
        loan, ignore = pool.loan_to(patron)
        assert_no_early_return()

        # If the loan is fulfilled but its LicensePoolDeliveryMechanism has
        # no DeliveryMechanism for some reason...
        loan.fulfillment = pool.delivery_mechanisms[0]
        dm = loan.fulfillment.delivery_mechanism
        loan.fulfillment.delivery_mechanism = None
        assert_no_early_return()

        # If the loan is fulfilled but the delivery mechanism uses DRM...
        loan.fulfillment.delivery_mechanism = dm
        assert_no_early_return()

        # If the loan is fulfilled with a DRM-free delivery mechanism,
        # perform_early_return _is_ called.
        dm.drm_scheme = DeliveryMechanism.NO_DRM
        overdrive.checkin(patron, pin, pool)

        assert (patron, pin, loan) == overdrive.perform_early_return_call

        # But if it fails, patron_request is _also_ called.
        args, kwargs = overdrive.patron_request_call
        assert (patron, pin, expect_url) == args
        assert dict(method="DELETE") == kwargs

        # Finally, if the loan is fulfilled with a DRM-free delivery mechanism
        # and perform_early_return succeeds, patron_request_call is not
        # called -- the title was already returned.
        overdrive.patron_request_call = None
        overdrive.EARLY_RETURN_SUCCESS = True
        overdrive.checkin(patron, pin, pool)
        assert (patron, pin, loan) == overdrive.perform_early_return_call
        assert None == overdrive.patron_request_call

    def test_perform_early_return(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        class Mock(MockOverdriveAPI):

            EARLY_RETURN_URL = "http://early-return/"

            def get_fulfillment_link(self, *args):
                self.get_fulfillment_link_call = args
                return ("http://fulfillment/", "content/type")

            def _extract_early_return_url(self, *args):
                self._extract_early_return_url_call = args
                return self.EARLY_RETURN_URL

        overdrive = Mock(db.session, overdrive_api_fixture.collection)

        # This patron has a loan.
        pool = db.licensepool(None)
        patron = db.patron()
        pin = object()
        loan, ignore = pool.loan_to(patron)

        # The loan has been fulfilled and now the patron wants to
        # do early return.
        loan.fulfillment = pool.delivery_mechanisms[0]

        # Our mocked perform_early_return will make two HTTP requests.
        # The first will be to the fulfill link returned by our mock
        # get_fulfillment_link. The response to this request is a
        # redirect that includes an early return link.
        http = DummyHTTPClient()
        http.responses.append(
            MockRequestsResponse(
                302, dict(location="http://fulfill-this-book/?or=return-early")
            )
        )

        # The second HTTP request made will be to the early return
        # link 'extracted' from that link by our mock
        # _extract_early_return_url. The response here is a copy of
        # the actual response Overdrive sends in this situation.
        http.responses.append(MockRequestsResponse(200, content="Success"))

        # Do the thing.
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)

        # The title was 'returned'.
        assert True == success

        # It worked like this:
        #
        # get_fulfillment_link was called with appropriate arguments.
        assert (
            patron,
            pin,
            pool.identifier.identifier,
            "ebook-epub-adobe",
        ) == overdrive.get_fulfillment_link_call

        # The URL returned by that method was 'requested'.
        assert "http://fulfillment/" == http.requests.pop(0)

        # The resulting URL was passed into _extract_early_return_url.
        assert (
            "http://fulfill-this-book/?or=return-early",
        ) == overdrive._extract_early_return_url_call

        # Then the URL returned by _that_ method was 'requested'.
        assert "http://early-return/" == http.requests.pop(0)

        # If no early return URL can be extracted from the fulfillment URL,
        # perform_early_return has no effect.
        #
        overdrive._extract_early_return_url_call = None
        overdrive.EARLY_RETURN_URL = None  # type: ignore
        http.responses.append(
            MockRequestsResponse(302, dict(location="http://fulfill-this-book/"))
        )
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        assert False == success

        # extract_early_return_url_call was called, but since it returned
        # None, no second HTTP request was made.
        assert "http://fulfillment/" == http.requests.pop(0)
        assert (
            "http://fulfill-this-book/",
        ) == overdrive._extract_early_return_url_call
        assert [] == http.requests

        # If we can't map the delivery mechanism to one of Overdrive's
        # internal formats, perform_early_return has no effect.
        #
        loan.fulfillment.delivery_mechanism.content_type = "not-in/overdrive"
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        assert False == success

        # In this case, no HTTP requests were made at all, since we
        # couldn't figure out which arguments to pass into
        # get_fulfillment_link.
        assert [] == http.requests

        # If the final attempt to hit the return URL doesn't result
        # in a 200 status code, perform_early_return has no effect.
        http.responses.append(
            MockRequestsResponse(
                302, dict(location="http://fulfill-this-book/?or=return-early")
            )
        )
        http.responses.append(MockRequestsResponse(401, content="Unauthorized!"))
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        assert False == success

    def test_extract_early_return_url(self):
        m = OverdriveAPI._extract_early_return_url
        assert None == m("http://no-early-return/")
        assert None == m("")
        assert None == m(None)

        # This is based on a real Overdrive early return URL.
        has_early_return = "https://openepub-gk.cdn.overdrive.com/OpenEPUBStore1/1577-1/%7B5880F6D0-48AC-44DE-8BF1-FD1CE62E97A8%7DFzr418.epub?e=1518753718&loanExpirationDate=2018-03-01T17%3a12%3a33Z&loanEarlyReturnUrl=https%3a%2f%2fnotifications-ofs.contentreserve.com%2fEarlyReturn%2fnypl%2f037-1374147-00279%2f5480F6E1-48F3-00DE-96C1-FD3CE32D94FD-312%3fh%3dVgvxBQHdQxtsbgb43AH6%252bEmpni9LoffkPczNiUz7%252b10%253d&sourceId=nypl&h=j7nGk7qxE71X2ZcdLw%2bqa04jqEw%3d"
        assert (
            "https://notifications-ofs.contentreserve.com/EarlyReturn/nypl/037-1374147-00279/5480F6E1-48F3-00DE-96C1-FD3CE32D94FD-312?h=VgvxBQHdQxtsbgb43AH6%2bEmpni9LoffkPczNiUz7%2b10%3d"
            == m(has_early_return)
        )

    def test_place_hold_raises_exception_if_patron_over_hold_limit(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        over_hold_limit = overdrive_api_fixture.error_message(
            "PatronExceededHoldLimit",
            "Patron cannot place any more holds, already has maximum holds placed.",
        )

        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
        )
        overdrive_api_fixture.api.queue_response(400, content=over_hold_limit)
        pytest.raises(
            PatronHoldLimitReached,  # type: ignore
            overdrive_api_fixture.api.place_hold,
            db.patron(),
            "pin",
            pool,
            notification_email_address="foo@bar.com",
        )

    def test_place_hold_looks_up_notification_address(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
        )

        # The first request we make will be to get patron info,
        # so that we know that the most recent email address used
        # to put a book on hold is foo@bar.com.
        ignore, patron_with_email = overdrive_api_fixture.sample_json(
            "patron_info.json"
        )

        # The second request we make will be to put a book on hold,
        # and when we do so we will ask for the notification to be
        # sent to foo@bar.com.
        ignore, successful_hold = overdrive_api_fixture.sample_json(
            "successful_hold.json"
        )

        overdrive_api_fixture.api.queue_response(200, content=patron_with_email)
        overdrive_api_fixture.api.queue_response(200, content=successful_hold)
        hold = overdrive_api_fixture.api.place_hold(
            db.patron(), "pin", pool, notification_email_address=None
        )

        # The book was placed on hold.
        assert 1 == hold.hold_position
        assert pool.identifier.identifier == hold.identifier

        # And when we placed it on hold, we passed in foo@bar.com
        # as the email address -- not notifications@example.com.
        url, positional_args, kwargs = overdrive_api_fixture.api.requests[-1]
        headers, body = positional_args
        assert '{"name": "emailAddress", "value": "foo@bar.com"}' in body

    def test_fulfill_returns_fulfillmentinfo_if_returned_by_get_fulfillment_link(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # If get_fulfillment_link returns a FulfillmentInfo, it is returned
        # immediately and the rest of fulfill() does not run.

        fulfillment = FulfillmentInfo(overdrive_api_fixture.collection, *[None] * 7)

        class MockAPI(OverdriveAPI):
            def get_fulfillment_link(*args, **kwargs):
                return fulfillment

        # Since most of the data is not provided, if fulfill() tried
        # to actually run to completion, it would crash.
        edition, pool = db.edition(with_license_pool=True)
        api = MockAPI(db.session, overdrive_api_fixture.collection)
        result = api.fulfill(None, None, pool, None)
        assert fulfillment == result

    def test_fulfill_raises_exception_and_updates_formats_for_outdated_format(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
        )

        # This pool has a format that's no longer available from overdrive.
        pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        ignore, loan = overdrive_api_fixture.sample_json("single_loan.json")

        ignore, lock_in_format_not_available = overdrive_api_fixture.sample_json(
            "lock_in_format_not_available.json"
        )

        # We will get the loan, try to lock in the format, and fail.
        overdrive_api_fixture.api.queue_response(200, content=loan)
        overdrive_api_fixture.api.queue_response(
            400, content=lock_in_format_not_available
        )

        # Trying to get a fulfillment link raises an exception.
        pytest.raises(
            FormatNotAvailable,  # type: ignore
            overdrive_api_fixture.api.get_fulfillment_link,
            db.patron(),
            "pin",
            pool.identifier.identifier,
            "ebook-epub-adobe",
        )

        # Fulfill will also update the formats.
        ignore, bibliographic = overdrive_api_fixture.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the correct Identifier.
        bibliographic["id"] = pool.identifier.identifier

        # If we have the LicensePool available (as opposed to just the
        # identifier), we will get the loan, try to lock in the
        # format, fail, and then update the bibliographic information.
        overdrive_api_fixture.api.queue_response(200, content=loan)
        overdrive_api_fixture.api.queue_response(
            400, content=lock_in_format_not_available
        )
        overdrive_api_fixture.api.queue_response(200, content=bibliographic)

        pytest.raises(
            FormatNotAvailable,  # type: ignore
            overdrive_api_fixture.api.fulfill,
            db.patron(),
            "pin",
            pool,
            "ebook-epub-adobe",
        )

        # The delivery mechanisms have been updated.
        assert 4 == len(pool.delivery_mechanisms)
        assert {
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.KINDLE_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
        } == {lpdm.delivery_mechanism.content_type for lpdm in pool.delivery_mechanisms}
        assert {
            DeliveryMechanism.ADOBE_DRM,
            DeliveryMechanism.KINDLE_DRM,
            DeliveryMechanism.LIBBY_DRM,
            DeliveryMechanism.STREAMING_DRM,
        } == {lpdm.delivery_mechanism.drm_scheme for lpdm in pool.delivery_mechanisms}

    def test_get_fulfillment_link_from_download_link(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        patron = db.patron()

        ignore, streaming_fulfill_link = overdrive_api_fixture.sample_json(
            "streaming_fulfill_link_response.json"
        )

        overdrive_api_fixture.api.queue_response(200, content=streaming_fulfill_link)

        href, type = overdrive_api_fixture.api.get_fulfillment_link_from_download_link(
            patron, "1234", "http://download-link", fulfill_url="http://fulfill"
        )
        assert (
            "https://fulfill.contentreserve.com/PerfectLife9780345530967.epub-sample.overdrive.com?RetailerID=nypl&Expires=1469825647&Token=dd0e19b4-eb70-439d-8c50-a65201060f4c&Signature=asl67/G154KeeUsL1mHPwEbZfgc="
            == href
        )
        assert "text/html" == type

    def test_get_fulfillment_link_returns_fulfillmentinfo_for_manifest_format(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # When the format requested would result in a link to a
        # manifest file, the manifest link is returned as-is (wrapped
        # in an OverdriveFulfillmentInfo) rather than being retrieved
        # and processed.

        # To keep things simple, our mock API will always return the same
        # fulfillment link.
        loan_info = {"isFormatLockedIn": False}

        class MockAPI(MockOverdriveAPI):
            def get_loan(self, patron, pin, overdrive_id):
                self.get_loan_called_with = (patron, pin, overdrive_id)
                return loan_info

            def get_download_link(self, loan, format_type, error_url):
                self.get_download_link_called_with = (loan, format_type, error_url)
                return "http://fulfillment-link/"

            def get_fulfillment_link_from_download_link(self, *args, **kwargs):
                # We want to verify that this method is never called.
                raise Exception("explode!")

        api = MockAPI(db.session, overdrive_api_fixture.collection)
        api.queue_response(200, content=json.dumps({"some": "data"}))

        # Randomly choose one of the formats that must be fulfilled as
        # a link to a manifest.
        overdrive_format = random.choice(list(OverdriveAPI.MANIFEST_INTERNAL_FORMATS))

        # Get the fulfillment link.
        patron = db.patron()
        fulfillmentinfo = api.get_fulfillment_link(
            patron,
            "1234",
            "http://download-link",
            overdrive_format,
        )
        assert isinstance(fulfillmentinfo, OverdriveManifestFulfillmentInfo)

        # Before looking at the OverdriveManifestFulfillmentInfo,
        # let's see how we got there.

        # First, our mocked get_loan() was called.
        assert (
            patron,
            "1234",
            "http://download-link",
        ) == api.get_loan_called_with

        # It returned a dictionary that contained no information
        # except isFormatLockedIn: false.

        # Since the manifest formats do not lock the loan, this
        # skipped most of the code in get_fulfillment_link, and the
        # loan info was passed into our mocked get_download_link.

        assert (
            loan_info,
            overdrive_format,
            api.DEFAULT_ERROR_URL,
        ) == api.get_download_link_called_with

        # Since the manifest formats cannot be retrieved by the
        # circulation manager, the result of get_download_link was
        # wrapped in an OverdriveManifestFulfillmentInfo and returned.
        # get_fulfillment_link_from_download_link was never called.
        assert "http://fulfillment-link/" == fulfillmentinfo.content_link
        assert None == fulfillmentinfo.content_type

    def test_update_formats(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # Create a LicensePool with an inaccurate delivery mechanism
        # and the wrong medium.
        edition, pool = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        edition.medium = Edition.PERIODICAL_MEDIUM

        # Add the bad delivery mechanism.
        pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        # Prepare the bibliographic information.
        ignore, bibliographic = overdrive_api_fixture.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the new pool's Identifier.
        bibliographic["id"] = pool.identifier.identifier

        overdrive_api_fixture.api.queue_response(200, content=bibliographic)

        overdrive_api_fixture.api.update_formats(pool)

        # The delivery mechanisms have been updated.
        assert 4 == len(pool.delivery_mechanisms)
        assert {
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.KINDLE_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
        } == {lpdm.delivery_mechanism.content_type for lpdm in pool.delivery_mechanisms}
        assert {
            DeliveryMechanism.ADOBE_DRM,
            DeliveryMechanism.KINDLE_DRM,
            DeliveryMechanism.LIBBY_DRM,
            DeliveryMechanism.STREAMING_DRM,
        } == {lpdm.delivery_mechanism.drm_scheme for lpdm in pool.delivery_mechanisms}

        # The Edition's medium has been corrected.
        assert Edition.BOOK_MEDIUM == edition.medium

    def test_update_availability(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # Test the Overdrive implementation of the update_availability
        # method defined by the CirculationAPI interface.

        # Create a LicensePool that needs updating.
        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=overdrive_api_fixture.collection,
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to make sure
        # it gets replaced.
        pool.licenses_owned = 10
        pool.licenses_available = 4
        pool.patrons_in_hold_queue = 3
        assert None == pool.last_checked

        # Prepare availability information.
        ignore, availability = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        # Since this is the first time we've seen this book,
        # we'll also be updating the bibliographic information.
        ignore, bibliographic = overdrive_api_fixture.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the new pool's Identifier.
        availability["id"] = pool.identifier.identifier
        bibliographic["id"] = pool.identifier.identifier

        overdrive_api_fixture.api.queue_response(200, content=availability)
        overdrive_api_fixture.api.queue_response(200, content=bibliographic)

        overdrive_api_fixture.api.update_availability(pool)

        # The availability information has been updated, as has the
        # date the availability information was last checked.
        assert 5 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue
        assert pool.last_checked is not None

    def test_circulation_lookup(self, overdrive_api_fixture: OverdriveAPIFixture):
        """Test the method that actually looks up Overdrive circulation
        information.
        """
        db = overdrive_api_fixture.db
        overdrive_api_fixture.api.queue_response(200, content="foo")

        # If passed an identifier, we'll use the endpoint() method to
        # construct a v2 availability URL and make a request to
        # it.
        book, (
            status_code,
            headers,
            content,
        ) = overdrive_api_fixture.api.circulation_lookup("an-identifier")
        assert dict(id="an-identifier") == book
        assert 200 == status_code
        assert b"foo" == content

        request_url, ignore1, ignore2 = overdrive_api_fixture.api.requests.pop()
        expect_url = overdrive_api_fixture.api.endpoint(
            overdrive_api_fixture.api.AVAILABILITY_ENDPOINT,
            collection_token=overdrive_api_fixture.api.collection_token,
            product_id="an-identifier",
        )
        assert request_url == expect_url
        assert "/v2/collections" in request_url

        # If passed the result of an API call that includes an
        # availability link, we'll clean up the URL in the link and
        # use it to get our availability data.
        overdrive_api_fixture.api.queue_response(200, content="foo")
        v1 = "https://qa.api.overdrive.com/v1/collections/abcde/products/12345/availability"
        v2 = "https://qa.api.overdrive.com/v2/collections/abcde/products/12345/availability"
        previous_result = dict(availability_link=v1)
        book, (
            status_code,
            headers,
            content,
        ) = overdrive_api_fixture.api.circulation_lookup(previous_result)
        assert previous_result == book
        assert 200 == status_code
        assert b"foo" == content
        request_url, ignore1, ignore2 = overdrive_api_fixture.api.requests.pop()

        # The v1 URL was converted to a v2 url.
        assert v2 == request_url

    def test_update_licensepool_error(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # Create an identifier.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        ignore, availability = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        overdrive_api_fixture.api.queue_response(500, content="An error occured.")
        book = dict(id=identifier.identifier, availability_link=db.fresh_url())
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(book)
        assert None == pool

    def test_update_licensepool_not_found(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # If the Overdrive API says a book is not found in the
        # collection, that's treated as useful information, not an error.
        # Create an identifier.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        ignore, not_found = overdrive_api_fixture.sample_json(
            "overdrive_availability_not_found.json"
        )

        # Queue the 'not found' response twice -- once for the circulation
        # lookup and once for the metadata lookup.
        overdrive_api_fixture.api.queue_response(404, content=not_found)
        overdrive_api_fixture.api.queue_response(404, content=not_found)

        book = dict(id=identifier.identifier, availability_link=db.fresh_url())
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(book)
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue

    def test_update_licensepool_provides_bibliographic_coverage(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # Create an identifier.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)

        # Prepare bibliographic and availability information
        # for this identifier.
        ignore, availability = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        ignore, bibliographic = overdrive_api_fixture.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the newly created Identifier.
        availability["id"] = identifier.identifier
        bibliographic["id"] = identifier.identifier

        overdrive_api_fixture.api.queue_response(200, content=availability)
        overdrive_api_fixture.api.queue_response(200, content=bibliographic)

        # Now we're ready. When we call update_licensepool, the
        # OverdriveAPI will retrieve the availability information,
        # then the bibliographic information. It will then trigger the
        # OverdriveBibliographicCoverageProvider, which will
        # create an Edition and a presentation-ready Work.
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(
            identifier.identifier
        )
        assert True == was_new
        assert availability["copiesOwned"] == pool.licenses_owned

        edition = pool.presentation_edition
        assert "Ancillary Justice" == edition.title

        assert True == pool.work.presentation_ready
        assert pool.work.cover_thumbnail_url.startswith(
            "http://images.contentreserve.com/"
        )

        # The book has been run through the bibliographic coverage
        # provider.
        coverage = [
            x
            for x in identifier.coverage_records
            if x.operation is None and x.data_source.name == DataSource.OVERDRIVE
        ]
        assert 1 == len(coverage)

        # Call update_licensepool on an identifier that is missing a work and make
        # sure that it provides bibliographic coverage in that case.
        db.session.delete(pool.work)
        db.session.commit()
        pool, is_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            Identifier.OVERDRIVE_ID,
            identifier.identifier,
            collection=overdrive_api_fixture.collection,
        )
        assert not pool.work
        overdrive_api_fixture.api.queue_response(200, content=availability)
        overdrive_api_fixture.api.queue_response(200, content=bibliographic)
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(
            identifier.identifier
        )
        assert False == was_new
        assert True == pool.work.presentation_ready

    def test_update_new_licensepool(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )

        # Create an identifier
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)

        # Make it look like the availability information is for the
        # newly created Identifier.
        raw["reserveId"] = identifier.identifier

        pool, was_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            identifier.type,
            identifier.identifier,
            collection=overdrive_api_fixture.collection,
        )

        (
            pool,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, pool, was_new
        )
        assert True == was_new
        assert True == changed

        db.session.commit()

        assert raw["copiesOwned"] == pool.licenses_owned
        assert raw["copiesAvailable"] == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert raw["numberOfHolds"] == pool.patrons_in_hold_queue

    def test_update_existing_licensepool(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )

        # Create a LicensePool.
        wr, pool = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )

        # Make it look like the availability information is for the
        # newly created LicensePool.
        raw["id"] = pool.identifier.identifier

        wr.title = "The real title."
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

        (
            p2,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, pool, False
        )
        assert False == was_new
        assert True == changed
        assert p2 == pool
        # The title didn't change to that title given in the availability
        # information, because we already set a title for that work.
        assert "The real title." == wr.title
        assert raw["copiesOwned"] == pool.licenses_owned
        assert raw["copiesAvailable"] == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert raw["numberOfHolds"] == pool.patrons_in_hold_queue

    def test_update_new_licensepool_when_same_book_has_pool_in_different_collection(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        old_edition, old_pool = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        old_pool.calculate_work()
        collection = db.collection()

        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )

        # Make it look like the availability information is for the
        # old pool's Identifier.
        identifier = old_pool.identifier
        raw["id"] = identifier.identifier

        new_pool, was_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            identifier.type,
            identifier.identifier,
            collection=collection,
        )
        # The new pool doesn't have a presentation edition yet,
        # but it will be updated to share the old pool's edition.
        assert None == new_pool.presentation_edition

        (
            new_pool,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, new_pool, was_new
        )
        assert True == was_new
        assert True == changed
        assert old_edition == new_pool.presentation_edition
        assert old_pool.work == new_pool.work

    def test_update_licensepool_with_holds(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_availability_information_holds.json"
        )
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        raw["id"] = identifier.identifier

        license_pool, is_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            identifier.type,
            identifier.identifier,
            collection=db.default_collection(),
        )
        (
            pool,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, license_pool, is_new
        )
        assert 10 == pool.patrons_in_hold_queue
        assert True == changed

    def test_refresh_patron_access_token(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """Verify that patron information is included in the request
        when refreshing a patron access token.
        """
        db = overdrive_api_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        credential = db.credential(patron=patron)

        data, raw = overdrive_api_fixture.sample_json("patron_token.json")
        overdrive_api_fixture.api.queue_response(200, content=raw)

        # Try to refresh the patron access token with a PIN, and
        # then without a PIN.
        overdrive_api_fixture.api.refresh_patron_access_token(
            credential, patron, "a pin"
        )

        overdrive_api_fixture.api.refresh_patron_access_token(credential, patron, None)

        # Verify that the requests that were made correspond to what
        # Overdrive is expecting.
        with_pin, without_pin = overdrive_api_fixture.api.access_token_requests
        url, payload, headers, kwargs = with_pin
        assert "https://oauth-patron.overdrive.com/patrontoken" == url
        assert "barcode" == payload["username"]
        expect_scope = "websiteid:{} authorizationname:{}".format(
            overdrive_api_fixture.api.website_id().decode("utf-8"),
            overdrive_api_fixture.api.ils_name(patron.library),
        )
        assert expect_scope == payload["scope"]
        assert "a pin" == payload["password"]
        assert not "password_required" in payload

        url, payload, headers, kwargs = without_pin
        assert "https://oauth-patron.overdrive.com/patrontoken" == url
        assert "barcode" == payload["username"]
        assert expect_scope == payload["scope"]
        assert "false" == payload["password_required"]
        assert "[ignore]" == payload["password"]

    def test_refresh_patron_access_token_is_fulfillment(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """Verify that patron information is included in the request
        when refreshing a patron access token.
        """
        db = overdrive_api_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        credential = db.credential(patron=patron)
        db.default_collection().external_integration.protocol = "Overdrive"
        db.default_collection().external_account_id = 1
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_CLIENT_KEY
        ).value = "user"
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_CLIENT_SECRET
        ).value = "password"
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_WEBSITE_ID
        ).value = "100"

        # Mocked testing credentials
        encoded_auth = base64.b64encode(b"TestingKey:TestingSecret")

        # use a real Overdrive API
        od_api = OverdriveAPI(db.session, db.default_collection())
        od_api._server_nickname = OverdriveConfiguration.TESTING_SERVERS
        # but mock the request methods
        od_api._do_post = MagicMock()
        od_api._do_get = MagicMock()
        response_credential = od_api.refresh_patron_access_token(
            credential, patron, "a pin", is_fulfillment=True
        )

        # Posted once, no gets
        od_api._do_post.assert_called_once()
        od_api._do_get.assert_not_called()

        # What did we Post?
        call_args = od_api._do_post.call_args[0]
        assert "/patrontoken" in call_args[0]  # url
        assert (
            call_args[2]["Authorization"] == f"Basic {encoded_auth.decode()}"
        )  # Basic header should be that of the fulfillment keys
        assert response_credential == credential

    def test_cannot_fulfill_error_audiobook(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        db.default_collection().external_integration.protocol = "Overdrive"
        db.default_collection().external_account_id = 1
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_CLIENT_KEY
        ).value = "user"
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_CLIENT_SECRET
        ).value = "password"
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_WEBSITE_ID
        ).value = "100"

        # use a real Overdrive API
        od_api = OverdriveAPI(db.session, db.default_collection())
        od_api._server_nickname = OverdriveConfiguration.TESTING_SERVERS
        od_api.get_loan = MagicMock(return_value={"isFormatLockedIn": True})
        od_api.get_download_link = MagicMock(return_value=None)

        exc = pytest.raises(
            CannotFulfill,  # type: ignore
            od_api.get_fulfillment_link,
            *(patron, "pin", "odid", "audiobook-overdrive-manifest"),
        )
        assert exc.match("No download link for")

        # Cannot fulfill error within the get auth function
        os.environ.pop(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}"
        )
        with pytest.raises(CannotFulfill):  # type: ignore
            od_api.fulfillment_authorization_header

    def test_no_drm_fulfillment(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        patron = db.patron()
        work = db.work(with_license_pool=True)
        patron.authorization_identifier = "barcode"
        db.default_collection().external_integration.protocol = "Overdrive"
        db.default_collection().external_account_id = 1
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_CLIENT_KEY
        ).value = "user"
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_CLIENT_SECRET
        ).value = "password"
        db.default_collection().external_integration.setting(
            OverdriveConfiguration.OVERDRIVE_WEBSITE_ID
        ).value = "100"

        od_api = OverdriveAPI(db.session, db.default_collection())
        od_api._server_nickname = OverdriveConfiguration.TESTING_SERVERS

        # Load the mock API data
        with open("tests/api/files/overdrive/no_drm_fulfill.json") as fp:
            api_data = json.load(fp)

        # Mock out the flow
        od_api.get_loan = MagicMock(return_value=api_data["loan"])

        mock_lock_in_response = create_autospec(Response)
        mock_lock_in_response.status_code = 200
        mock_lock_in_response.json.return_value = api_data["lock_in"]
        od_api.lock_in_format = MagicMock(return_value=mock_lock_in_response)

        od_api.get_fulfillment_link_from_download_link = MagicMock(
            return_value=(
                "https://example.org/epub-redirect",
                "application/epub+zip",
            )
        )

        fulfill = od_api.fulfill(
            patron, "pin", work.active_license_pool(), "ebook-epub-open"
        )

        assert fulfill.content_link_redirect == True
        assert fulfill.content_link == "https://example.org/epub-redirect"


class TestOverdriveAPICredentials:
    def test_patron_correct_credentials_for_multiple_overdrive_collections(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        # Verify that the correct credential will be used
        # when a library has more than one OverDrive collection.

        def _optional_value(self, obj, key):
            return obj.get(key, "none")

        def _make_token(scope, username, password, grant_type="password"):
            return f"{grant_type}|{scope}|{username}|{password}"

        class MockAPI(MockOverdriveAPI):
            def token_post(
                self, url, payload, is_fulfillment=False, headers={}, **kwargs
            ):
                url = self.endpoint(url)
                self.access_token_requests.append((url, payload, headers, kwargs))
                token = _make_token(
                    _optional_value(self, payload, "scope"),
                    _optional_value(self, payload, "username"),
                    _optional_value(self, payload, "password"),
                    grant_type=_optional_value(self, payload, "grant_type"),
                )
                response = self.mock_access_token_response(token)

                from core.util.http import HTTP

                return HTTP._process_response(url, response, **kwargs)

        library = db.default_library()
        patron = db.patron(library=library)
        patron.authorization_identifier = "patron_barcode"
        pin = "patron_pin"

        # clear out any collections added before we add ours
        library.collections = []

        # Distinct credentials for the two OverDrive collections in which our
        # library has membership.
        library_collection_properties = [
            dict(
                library=library,
                name="Test OD Collection 1",
                client_key="client_key_1",
                client_secret="client_secret_1",
                library_id="lib_id_1",
                website_id="ws_id_1",
                ils_name="lib1_coll1_ils",
            ),
            dict(
                library=library,
                name="Test OD Collection 2",
                client_key="client_key_2",
                client_secret="client_secret_2",
                library_id="lib_id_2",
                website_id="ws_id_2",
                ils_name="lib1_coll2_ils",
            ),
        ]

        # These are the credentials we'll expect for each of our collections.
        expected_credentials = {
            props["name"]: _make_token(
                "websiteid:%s authorizationname:%s"
                % (props["website_id"], props["ils_name"]),
                patron.authorization_identifier,
                pin,
            )
            for props in library_collection_properties
        }

        # Add the collections.
        collections = [
            MockAPI.mock_collection(db.session, **props)
            for props in library_collection_properties
        ]

        circulation = CirculationAPI(
            db.session, library, api_map={ExternalIntegration.OVERDRIVE: MockAPI}
        )
        od_apis = {
            api.collection.name: api
            for api in list(circulation.api_for_collection.values())
        }

        # Ensure that we have the correct number of OverDrive collections.
        assert len(library_collection_properties) == len(od_apis)

        # Verify that the expected credentials match what we got.
        for name in list(expected_credentials.keys()) + list(
            reversed(list(expected_credentials.keys()))
        ):
            credential = od_apis[name].get_patron_credential(patron, pin)
            assert expected_credentials[name] == credential.credential

    def test_fulfillment_credentials_testing_keys(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        test_key = "tk"
        test_secret = "ts"

        os.environ[
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}"
        ] = test_key
        os.environ[
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}"
        ] = test_secret

        testing_credentials = Configuration.overdrive_fulfillment_keys(testing=True)
        assert testing_credentials["key"] == test_key
        assert testing_credentials["secret"] == test_secret

        prod_key = "pk"
        prod_secret = "ps"

        os.environ[
            f"{Configuration.OD_PREFIX_PRODUCTION_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}"
        ] = prod_key
        os.environ[
            f"{Configuration.OD_PREFIX_PRODUCTION_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}"
        ] = prod_secret

        prod_credentials = Configuration.overdrive_fulfillment_keys()
        assert prod_credentials["key"] == prod_key
        assert prod_credentials["secret"] == prod_secret

    def test_fulfillment_credentials_cannot_load(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        os.environ.pop(
            f"{Configuration.OD_PREFIX_PRODUCTION_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}"
        )
        pytest.raises(CannotLoadConfiguration, Configuration.overdrive_fulfillment_keys)

        os.environ.pop(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}"
        )
        pytest.raises(
            CannotLoadConfiguration,
            Configuration.overdrive_fulfillment_keys,
            testing=True,
        )


class TestExtractData:
    def test_get_download_link(self, overdrive_api_fixture: OverdriveAPIFixture):
        data, json = overdrive_api_fixture.sample_json(
            "checkout_response_locked_in_format.json"
        )
        url = MockOverdriveAPI.get_download_link(
            json, "ebook-epub-adobe", "http://foo.com/"
        )
        assert (
            "http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/"
            == url
        )

        pytest.raises(
            NoAcceptableFormat,  # type: ignore
            MockOverdriveAPI.get_download_link,
            json,
            "no-such-format",
            "http://foo.com/",
        )

    def test_get_download_link_raises_exception_if_loan_fulfilled_on_incompatible_platform(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        data, json = overdrive_api_fixture.sample_json(
            "checkout_response_book_fulfilled_on_kindle.json"
        )
        pytest.raises(
            FulfilledOnIncompatiblePlatform,  # type: ignore
            MockOverdriveAPI.get_download_link,
            json,
            "ebook-epub-adobe",
            "http://foo.com/",
        )

    def test_get_download_link_for_manifest_format(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        # If you ask for the download link for an 'x-manifest' format,
        # it's treated as a variant of the 'x' format.
        data, json = overdrive_api_fixture.sample_json(
            "checkout_response_book_fulfilled_on_kindle.json"
        )

        # This is part of the URL from `json` that we expect
        # get_download_link to use as a base.
        base_url = "http://patron.api.overdrive.com/v1/patrons/me/checkouts/98EA8135-52C0-4480-9C0E-1D0779670D4A/formats/ebook-overdrive/downloadlink"

        # First, let's ask for the streaming format.
        link = MockOverdriveAPI.get_download_link(
            json, "ebook-overdrive", "http://foo.com/"
        )

        # The base URL is returned, with {errorpageurl} filled in and
        # {odreadauthurl} left for other code to fill in.
        assert (
            base_url + "?errorpageurl=http://foo.com/&odreadauthurl={odreadauthurl}"
            == link
        )

        # Now let's ask for the manifest format.
        link = MockOverdriveAPI.get_download_link(
            json, "ebook-overdrive-manifest", "http://bar.com/"
        )

        # The {errorpageurl} and {odreadauthurl} parameters
        # have been removed, and contentfile=true has been appended.
        assert base_url + "?contentfile=true" == link

    def test_extract_download_link(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Verify that extract_download_link can or cannot find a
        # download link for a given format subdocument.

        class Mock(OverdriveAPI):
            called_with = None

            @classmethod
            def make_direct_download_link(cls, download_link):
                cls.called_with = download_link
                return "http://manifest/"

        m = Mock.extract_download_link
        error_url = "http://error/"

        # Here we don't even know the name of the format.
        empty: Dict[str, Any] = dict()
        with pytest.raises(IOError) as excinfo:
            m(empty, error_url)
        assert "No linkTemplates for format (unknown)" in str(excinfo.value)

        # Here we know the name, but there are no link templates.
        no_templates = dict(formatType="someformat")
        with pytest.raises(IOError) as excinfo:
            m(no_templates, error_url)
        assert "No linkTemplates for format someformat" in str(excinfo.value)

        # Here there's a link template structure, but no downloadLink
        # inside.
        no_download_link = dict(formatType="someformat", linkTemplates=dict())
        with pytest.raises(IOError) as excinfo:
            m(no_download_link, error_url)
        assert "No downloadLink for format someformat" in str(excinfo.value)

        # Here there's a downloadLink structure, but no href inside.
        href_is_missing = dict(
            formatType="someformat", linkTemplates=dict(downloadLink=dict())
        )
        with pytest.raises(IOError) as excinfo:
            m(href_is_missing, error_url)
        assert "No downloadLink href for format someformat" in str(excinfo.value)

        # Now we finally get to the cases where there is an actual
        # download link.  The behavior is different based on whether
        # or not we want to return a link to the manifest file.

        working = dict(
            formatType="someformat",
            linkTemplates=dict(
                downloadLink=dict(href="http://download/?errorpageurl={errorpageurl}")
            ),
        )

        # If we don't want a manifest, make_direct_download_link is
        # not called.
        do_not_fetch_manifest = m(working, error_url, fetch_manifest=False)
        assert None == Mock.called_with

        # The errorpageurl template is filled in.
        assert "http://download/?errorpageurl=http://error/" == do_not_fetch_manifest

        # If we do want a manifest, make_direct_download_link is called
        # without errorpageurl being affected.
        do_fetch_manifest = m(working, error_url, fetch_manifest=True)
        assert "http://download/?errorpageurl={errorpageurl}" == Mock.called_with
        assert "http://manifest/" == do_fetch_manifest

    def test_make_direct_download_link(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        # Verify that make_direct_download_link handles various more
        # or less weird URLs that the Overdrive might or might not
        # serve.
        base = "http://overdrive/downloadlink"
        m = OverdriveAPI.make_direct_download_link
        assert base + "?contentfile=true" == m(base)
        assert base + "?contentfile=true" == m(base + "?odreadauthurl={odreadauthurl}")
        assert base + "?other=other&contentfile=true" == m(
            base + "?odreadauthurl={odreadauthurl}&other=other"
        )

    def test_extract_data_from_checkout_resource(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        data, json = overdrive_api_fixture.sample_json(
            "checkout_response_locked_in_format.json"
        )
        expires, url = MockOverdriveAPI.extract_data_from_checkout_response(
            json, "ebook-epub-adobe", "http://foo.com/"
        )
        assert 2013 == expires.year
        assert 10 == expires.month
        assert 4 == expires.day
        assert (
            "http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/"
            == url
        )

    def test_process_checkout_data(self, overdrive_api_fixture: OverdriveAPIFixture):
        data, json = overdrive_api_fixture.sample_json(
            "shelf_with_book_already_fulfilled_on_kindle.json"
        )
        [on_kindle, not_on_kindle] = json["checkouts"]

        # The book already fulfilled on Kindle doesn't get turned into
        # LoanInfo at all.
        assert None == MockOverdriveAPI.process_checkout_data(
            on_kindle, overdrive_api_fixture.collection
        )

        # The book not yet fulfilled does show up as a LoanInfo.
        loan_info = MockOverdriveAPI.process_checkout_data(
            not_on_kindle, overdrive_api_fixture.collection
        )
        assert "2fadd2ac-a8ec-4938-a369-4c3260e8922b" == loan_info.identifier

        # Since there are two usable formats (Adobe EPUB and Adobe
        # PDF), the LoanInfo is not locked to any particular format.
        assert None == loan_info.locked_to

        # A book that's on loan and locked to a specific format has a
        # DeliveryMechanismInfo associated with that format.
        data, format_locked_in = overdrive_api_fixture.sample_json(
            "checkout_response_locked_in_format.json"
        )
        loan_info = MockOverdriveAPI.process_checkout_data(
            format_locked_in, overdrive_api_fixture.collection
        )
        delivery = loan_info.locked_to
        assert Representation.EPUB_MEDIA_TYPE == delivery.content_type
        assert DeliveryMechanism.ADOBE_DRM == delivery.drm_scheme

        # This book is on loan and the choice between Kindle and Adobe
        # EPUB has not yet been made, but as far as we're concerned,
        # Adobe EPUB is the only *usable* format, so it's effectively
        # locked.
        data, no_format_locked_in = overdrive_api_fixture.sample_json(
            "checkout_response_no_format_locked_in.json"
        )
        loan_info = MockOverdriveAPI.process_checkout_data(
            no_format_locked_in, overdrive_api_fixture.collection
        )
        assert loan_info != None
        delivery = loan_info.locked_to
        assert Representation.EPUB_MEDIA_TYPE == delivery.content_type
        assert DeliveryMechanism.ADOBE_DRM == delivery.drm_scheme

        # TODO: In the future both of these tests should return a
        # LoanInfo with appropriate FulfillmentInfo. The calling code
        # would then decide whether or not to show the loan.


class TestSyncBookshelf:
    def test_sync_bookshelf_creates_local_loans(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        loans_data, json_loans = overdrive_api_fixture.sample_json(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data, json_holds = overdrive_api_fixture.sample_json("no_holds.json")

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        patron = db.patron()
        loans, holds = overdrive_api_fixture.circulation.sync_bookshelf(
            patron, "dummy pin"
        )

        # All four loans in the sample data were created.
        assert 4 == len(loans)
        assert loans.sort() == patron.loans.sort()

        # We have created previously unknown LicensePools and
        # Identifiers.
        identifiers = [loan.license_pool.identifier.identifier for loan in loans]
        assert sorted(
            [
                "a5a3d737-34d4-4d69-aad8-eba4e46019a3",
                "99409f99-45a5-4238-9e10-98d1435cde04",
                "993e4b33-823c-40af-8f61-cac54e1cba5d",
                "a2ec6f3a-ebfe-4c95-9638-2cb13be8de5a",
            ]
        ) == sorted(identifiers)

        # We have recorded a new DeliveryMechanism associated with
        # each loan.
        mechanisms = []
        for loan in loans:
            if loan.fulfillment:
                mechanism = loan.fulfillment.delivery_mechanism
                mechanisms.append((mechanism.content_type, mechanism.drm_scheme))
        assert [
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        ] == mechanisms

        # There are no holds.
        assert [] == holds

        # Running the sync again leaves all four loans in place.
        patron.last_loan_activity_sync = None
        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)
        loans, holds = overdrive_api_fixture.circulation.sync_bookshelf(
            patron, "dummy pin"
        )
        assert 4 == len(loans)
        assert loans.sort() == patron.loans.sort()

    def test_sync_bookshelf_removes_loans_not_present_on_remote(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        loans_data, json_loans = overdrive_api_fixture.sample_json(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data, json_holds = overdrive_api_fixture.sample_json("no_holds.json")

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        # Create a loan not present in the sample data.
        patron = db.patron()
        overdrive_edition, new = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=overdrive_api_fixture.collection,
        )
        [pool] = overdrive_edition.license_pools
        overdrive_loan, new = pool.loan_to(patron)
        yesterday = utc_now() - timedelta(days=1)
        overdrive_loan.start = yesterday

        # Sync with Overdrive, and the loan not present in the sample
        # data is removed.
        loans, holds = overdrive_api_fixture.circulation.sync_bookshelf(
            patron, "dummy pin"
        )

        assert 4 == len(loans)
        assert set(loans) == set(patron.loans)
        assert overdrive_loan not in patron.loans

    def test_sync_bookshelf_ignores_loans_from_other_sources(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        patron = db.patron()
        gutenberg, new = db.edition(
            data_source_name=DataSource.GUTENBERG, with_license_pool=True
        )
        [pool] = gutenberg.license_pools
        gutenberg_loan, new = pool.loan_to(patron)
        loans_data, json_loans = overdrive_api_fixture.sample_json(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data, json_holds = overdrive_api_fixture.sample_json("no_holds.json")

        # Overdrive doesn't know about the Gutenberg loan, but it was
        # not destroyed, because it came from another source.
        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        loans, holds = overdrive_api_fixture.circulation.sync_bookshelf(
            patron, "dummy pin"
        )
        assert 5 == len(patron.loans)
        assert gutenberg_loan in patron.loans

    def test_sync_bookshelf_creates_local_holds(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        loans_data, json_loans = overdrive_api_fixture.sample_json("no_loans.json")
        holds_data, json_holds = overdrive_api_fixture.sample_json("holds.json")

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)
        patron = db.patron()

        loans, holds = overdrive_api_fixture.circulation.sync_bookshelf(
            patron, "dummy pin"
        )
        # All four loans in the sample data were created.
        assert 4 == len(holds)
        assert sorted(holds) == sorted(patron.holds)

        # Running the sync again leaves all four holds in place.
        patron.last_loan_activity_sync = None
        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)
        loans, holds = overdrive_api_fixture.circulation.sync_bookshelf(
            patron, "dummy pin"
        )
        assert 4 == len(holds)
        assert sorted(holds) == sorted(patron.holds)

    def test_sync_bookshelf_removes_holds_not_present_on_remote(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        loans_data, json_loans = overdrive_api_fixture.sample_json("no_loans.json")
        holds_data, json_holds = overdrive_api_fixture.sample_json("holds.json")

        patron = db.patron()
        overdrive_edition, new = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=overdrive_api_fixture.collection,
        )
        [pool] = overdrive_edition.license_pools
        overdrive_hold, new = pool.on_hold_to(patron)

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        # The hold not present in the sample data has been removed
        loans, holds = overdrive_api_fixture.circulation.sync_bookshelf(
            patron, "dummy pin"
        )
        assert 4 == len(holds)
        assert holds == patron.holds
        assert overdrive_hold not in patron.loans

    def test_sync_bookshelf_ignores_holds_from_other_collections(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        loans_data, json_loans = overdrive_api_fixture.sample_json("no_loans.json")
        holds_data, json_holds = overdrive_api_fixture.sample_json("holds.json")
        patron = db.patron()

        # This patron has an Overdrive book on hold, but it derives
        # from an Overdrive Collection that's not managed by
        # self.circulation.
        overdrive, new = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=db.collection(),
        )
        [pool] = overdrive.license_pools
        overdrive_hold, new = pool.on_hold_to(patron)

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        # overdrive_api_fixture.api doesn't know about the hold, but it was not
        # destroyed, because it came from a different collection.
        loans, holds = overdrive_api_fixture.circulation.sync_bookshelf(
            patron, "dummy pin"
        )
        assert 5 == len(patron.holds)
        assert overdrive_hold in patron.holds


class TestOverdriveManifestFulfillmentInfo:
    def test_as_response(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # An OverdriveManifestFulfillmentInfo just links the client
        # directly to the manifest file, bypassing normal FulfillmentInfo
        # processing.
        info = OverdriveManifestFulfillmentInfo(
            db.default_collection(),
            "http://content-link/",
            "abcd-efgh",
            "scope string",
            "access token",
        )
        response = info.as_response
        assert 302 == response.status_code
        assert "" == response.get_data(as_text=True)
        headers = response.headers
        assert "text/plain" == headers["Content-Type"]

        # These are the important headers; the location of the manifest file
        # and the scope necessary to initiate Patron Authentication for
        # it.
        assert "scope string" == headers["X-Overdrive-Scope"]
        assert "Bearer access token" == headers["X-Overdrive-Patron-Authorization"]
        assert "http://content-link/" == headers["Location"]


class TestOverdriveCirculationMonitor:
    def test_run(self, overdrive_api_fixture: OverdriveAPIFixture, time_fixture: Time):
        db = overdrive_api_fixture.db

        # An end-to-end test verifying that this Monitor manages its
        # state across multiple runs.
        #
        # This tests a lot of code that's technically not in Monitor,
        # but when the Monitor API changes, it may require changes to
        # this particular monitor, and it's good to have a test that
        # will fail if that's true.
        class Mock(OverdriveCirculationMonitor):
            def catch_up_from(self, start, cutoff, progress):
                self.catch_up_from_called_with = (start, cutoff, progress)

        monitor = Mock(db.session, overdrive_api_fixture.collection)

        monitor.run()
        start, cutoff, progress = monitor.catch_up_from_called_with
        now = utc_now()

        # The first time this Monitor is called, its 'start time' is
        # the current time, and we ask for an overlap of one minute.
        # This isn't very effective, but we have to start somewhere.
        #
        # (This isn't how the Overdrive collection is initially
        # populated, BTW -- that's NewTitlesOverdriveCollectionMonitor.)
        time_fixture.time_eq(start, now - monitor.OVERLAP)
        time_fixture.time_eq(cutoff, now)
        timestamp = monitor.timestamp()
        assert start == timestamp.start
        assert cutoff == timestamp.finish

        # The second time the Monitor is called, its 'start time'
        # is one minute before the previous cutoff time.
        monitor.run()
        new_start, new_cutoff, new_progress = monitor.catch_up_from_called_with
        now = utc_now()
        assert new_start == cutoff - monitor.OVERLAP
        time_fixture.time_eq(new_cutoff, now)

    def test_catch_up_from(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # catch_up_from() asks Overdrive about recent changes by
        # calling recently_changed_ids().
        #
        # It mirrors those changes locally by calling
        # update_licensepool().
        #
        # If this is our first time encountering a book, a
        # DISTRIBUTOR_TITLE_ADD analytics event is sent out.
        #
        # The method stops when should_stop() -- called on every book
        # -- returns True.
        class MockAPI:
            def __init__(self, *ignore, **kwignore):
                self.licensepools = []
                self.update_licensepool_calls = []

            def update_licensepool(self, book_id):
                pool, is_new, is_changed = self.licensepools.pop(0)
                self.update_licensepool_calls.append((book_id, pool))
                return pool, is_new, is_changed

        class MockAnalytics:
            def __init__(self, _db):
                self.db = _db
                self.events = []

            def collect_event(self, *args):
                self.events.append(args)

        class MockMonitor(OverdriveCirculationMonitor):

            recently_changed_ids_called_with = None
            should_stop_calls = []

            def recently_changed_ids(self, start, cutoff):
                self.recently_changed_ids_called_with = (start, cutoff)
                return [1, 2, None, 3, 4]

            def should_stop(self, start, book, is_changed):
                # We're going to stop after the third valid book,
                # ensuring that we never ask 'Overdrive' for the
                # fourth book.
                self.should_stop_calls.append((start, book, is_changed))
                if book == 3:
                    return True
                return False

        monitor = MockMonitor(
            db.session,
            overdrive_api_fixture.collection,
            api_class=MockAPI,
            analytics_class=MockAnalytics,
        )
        api = monitor.api

        # A MockAnalytics object was created and is ready to receive analytics
        # events.
        assert isinstance(monitor.analytics, MockAnalytics)
        assert db.session == monitor.analytics.db

        # The 'Overdrive API' is ready to tell us about four books,
        # but only one of them (the first) represents a change from what
        # we already know.
        lp1 = db.licensepool(None)
        lp1.last_checked = utc_now()
        lp2 = db.licensepool(None)
        lp3 = db.licensepool(None)
        lp4 = object()
        api.licensepools.append((lp1, True, True))
        api.licensepools.append((lp2, False, False))
        api.licensepools.append((lp3, False, True))
        api.licensepools.append(lp4)

        progress = TimestampData()
        start = object()
        cutoff = object()
        monitor.catch_up_from(start, cutoff, progress)

        # The monitor called recently_changed_ids with the start and
        # cutoff times. It returned five 'books', one of which was None --
        # simulating a lack of data from Overdrive.
        assert (start, cutoff) == monitor.recently_changed_ids_called_with

        # The monitor ignored the empty book and called
        # update_licensepool on the first three valid 'books'. The
        # mock API delivered the first three LicensePools from the
        # queue.
        assert [(1, lp1), (2, lp2), (3, lp3)] == api.update_licensepool_calls

        # After each book was processed, should_stop was called, using
        # the LicensePool, the start date, plus information about
        # whether the LicensePool was changed (or created) during
        # update_licensepool().
        assert [
            (start, 1, True),
            (start, 2, False),
            (start, 3, True),
        ] == monitor.should_stop_calls

        # should_stop returned True on the third call, and at that
        # point we gave up.

        # The fourth (bogus) LicensePool is still in api.licensepools,
        # because we never asked for it.
        assert [lp4] == api.licensepools

        # A single analytics event was sent out, for the first LicensePool,
        # the one that update_licensepool said was new.
        #
        # No more DISTRIBUTOR events
        assert len(monitor.analytics.events) == 0

        # The incoming TimestampData object was updated with
        # a summary of what happened.
        #
        # We processed four books: 1, 2, None (which was ignored)
        # and 3.
        assert "Books processed: 4." == progress.achievements

    def test_catch_up_from_with_failures_retried(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """Check that book failures are retried."""
        db = overdrive_api_fixture.db

        class MockAPI:
            tries: Dict[str, int] = {}

            def __init__(self, *ignore, **kwignore):
                self.licensepools = []
                self.update_licensepool_calls = []

            def recently_changed_ids(self, start, cutoff):
                return [1, 2, 3]

            def update_licensepool(self, book_id):
                current_count = self.tries.get(str(book_id)) or 0
                current_count = current_count + 1
                self.tries[str(book_id)] = current_count

                if current_count < 2:
                    raise StaleDataError("Ouch!")

                pool, is_new, is_changed = self.licensepools.pop(0)
                self.update_licensepool_calls.append((book_id, pool))
                return pool, is_new, is_changed

        class MockAnalytics:
            def __init__(self, _db):
                self._db = _db
                self.events = []

            def collect_event(self, *args):
                self.events.append(args)

        monitor = OverdriveCirculationMonitor(
            db.session,
            overdrive_api_fixture.collection,
            api_class=MockAPI,
            analytics_class=MockAnalytics,
        )
        api = monitor.api

        # A MockAnalytics object was created and is ready to receive analytics
        # events.
        assert isinstance(monitor.analytics, MockAnalytics)
        assert db.session == monitor.analytics._db

        lp1 = db.licensepool(None)
        lp1.last_checked = utc_now()
        lp2 = db.licensepool(None)
        lp3 = db.licensepool(None)
        api.licensepools.append((lp1, True, True))
        api.licensepools.append((lp2, False, False))
        api.licensepools.append((lp3, False, True))

        progress = TimestampData()
        start = object()
        cutoff = object()
        monitor.catch_up_from(start, cutoff, progress)

        assert api.tries["1"] == 2
        assert api.tries["2"] == 2
        assert api.tries["3"] == 2
        assert not progress.is_failure

    def test_catch_up_from_with_failures_all(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """If an individual book fails, the import continues, but ends in failure after handling all the books."""
        db = overdrive_api_fixture.db

        class MockAPI:
            tries: Dict[str, int] = {}

            def __init__(self, *ignore, **kwignore):
                self.licensepools = []
                self.update_licensepool_calls = []

            def recently_changed_ids(self, start, cutoff):
                return [1, 2, 3]

            def update_licensepool(self, book_id):
                current_count = self.tries.get(str(book_id)) or 0
                current_count = current_count + 1
                self.tries[str(book_id)] = current_count
                raise StaleDataError("Ouch!")

        class MockAnalytics:
            def __init__(self, _db):
                self._db = _db
                self.events = []

            def collect_event(self, *args):
                self.events.append(args)

        monitor = OverdriveCirculationMonitor(
            db.session,
            overdrive_api_fixture.collection,
            api_class=MockAPI,
            analytics_class=MockAnalytics,
        )
        api = monitor.api

        # A MockAnalytics object was created and is ready to receive analytics
        # events.
        assert isinstance(monitor.analytics, MockAnalytics)
        assert db.session == monitor.analytics._db

        lp1 = db.licensepool(None)
        lp1.last_checked = utc_now()
        lp2 = db.licensepool(None)
        lp3 = db.licensepool(None)
        api.licensepools.append((lp1, True, True))
        api.licensepools.append((lp2, False, False))
        api.licensepools.append((lp3, False, True))

        progress = TimestampData()
        start = object()
        cutoff = object()
        monitor.catch_up_from(start, cutoff, progress)

        assert api.tries["1"] == 3
        assert api.tries["2"] == 3
        assert api.tries["3"] == 3
        assert progress.is_failure


class TestNewTitlesOverdriveCollectionMonitor:
    def test_recently_changed_ids(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        class MockAPI:
            def __init__(self, *args, **kwargs):
                pass

            def all_ids(self):
                return "all of the ids"

        monitor = NewTitlesOverdriveCollectionMonitor(
            db.session, overdrive_api_fixture.collection, api_class=MockAPI
        )
        assert "all of the ids" == monitor.recently_changed_ids(object(), object())

    def test_should_stop(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        monitor = NewTitlesOverdriveCollectionMonitor(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )

        m = monitor.should_stop

        # If the monitor has never run before, we need to keep going
        # until we run out of books.
        assert False == m(None, object(), object())
        assert False == m(monitor.NEVER, object(), object())

        # If information is missing or invalid, we assume that we
        # should keep going.
        start = datetime_utc(2018, 1, 1)
        assert False == m(start, {}, object())
        assert False == m(start, {"date_added": None}, object())
        assert False == m(start, {"date_added": "Not a date"}, object())

        # Here, we're actually comparing real dates, using the date
        # format found in the Overdrive API. A date that's after the
        # `start` date means we should keep going backwards. A date before
        # the `start` date means we should stop.
        assert False == m(
            start, {"date_added": "2019-07-12T11:06:38.157+01:00"}, object()
        )
        assert True == m(
            start, {"date_added": "2017-07-12T11:06:38.157-04:00"}, object()
        )


class TestNewTitlesOverdriveCollectionMonitor2:
    def test_should_stop(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        monitor = RecentOverdriveCollectionMonitor(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )
        assert 0 == monitor.consecutive_unchanged_books
        m = monitor.should_stop

        # This book hasn't been changed, but we're under the limit, so we should
        # keep going.
        assert False == m(object(), object(), False)
        assert 1 == monitor.consecutive_unchanged_books

        assert False == m(object(), object(), False)
        assert 2 == monitor.consecutive_unchanged_books

        # This book has changed, so our counter gets reset.
        assert False == m(object(), object(), True)
        assert 0 == monitor.consecutive_unchanged_books

        # When we're at the limit, and another book comes along that hasn't
        # been changed, _then_ we decide to stop.
        monitor.consecutive_unchanged_books = (
            monitor.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS
        )
        assert True == m(object(), object(), False)
        assert (
            monitor.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS + 1
            == monitor.consecutive_unchanged_books
        )


class TestOverdriveFormatSweep:
    def test_process_item(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        # Validate the standard CollectionMonitor interface.
        monitor = OverdriveFormatSweep(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )
        monitor.api.queue_collection_token()
        # We're not testing that the work actually gets done (that's
        # tested in test_update_formats), only that the monitor
        # implements the expected process_item API without crashing.
        monitor.api.queue_response(404)
        edition, pool = db.edition(with_license_pool=True)
        monitor.process_item(pool.identifier)

    def test_process_item_multiple_licence_pools(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        # Make sure that we only call update_formats once when an item
        # is part of multiple licensepools.

        class MockApi(MockOverdriveAPI):
            update_format_calls = 0

            def update_formats(self, licensepool):
                self.update_format_calls += 1

        monitor = OverdriveFormatSweep(
            db.session, overdrive_api_fixture.collection, api_class=MockApi
        )
        monitor.api.queue_collection_token()
        monitor.api.queue_response(404)

        edition = db.edition()
        collection1 = db.collection(name="Collection 1")
        pool1 = db.licensepool(edition, collection=collection1)

        collection2 = db.collection(name="Collection 2")
        pool2 = db.licensepool(edition, collection=collection2)

        monitor.process_item(pool1.identifier)
        assert 1 == monitor.api.update_format_calls


class TestReaper:
    def test_instantiate(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        # Validate the standard CollectionMonitor interface.
        monitor = OverdriveCollectionReaper(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )
