"""Test the self-test functionality.

Self-tests are not unit tests -- they are executed at runtime on a
specific installation. They verify that that installation is properly
configured, not that the code is correct.
"""

import datetime
from collections.abc import Generator
from unittest.mock import MagicMock, create_autospec

from _pytest.monkeypatch import MonkeyPatch
from sqlalchemy.orm import Session

from core.integration.goals import Goals
from core.model import IntegrationConfiguration
from core.selftest import HasSelfTests, SelfTestResult
from core.util.datetime_helpers import utc_now
from core.util.http import IntegrationException
from tests.fixtures.database import DatabaseTransactionFixture


class TestSelfTestResult:
    now = utc_now()
    future = now + datetime.timedelta(seconds=5)

    def test_success_representation(self, db: DatabaseTransactionFixture):
        """Show the string and dictionary representations of a successful
        test result.
        """
        # A successful result
        result = SelfTestResult("success1")
        result.start = self.now
        result.end = self.future
        result.result = "The result"
        result.success = True
        assert (
            "<SelfTestResult: name='success1' duration=5.00sec success=True result='The result'>"
            == repr(result)
        )

        # A SelfTestResult may have an associated Collection.
        db.default_collection().integration_configuration.name = "CollectionA"
        result.collection = db.default_collection()
        assert (
            "<SelfTestResult: name='success1' collection='CollectionA' duration=5.00sec success=True result='The result'>"
            == repr(result)
        )

        d = result.to_dict
        assert "success1" == d["name"]
        assert "The result" == d["result"]
        assert 5.0 == d["duration"]
        assert True == d["success"]
        assert None == d["exception"]
        assert "CollectionA" == d["collection"]

        # A test result can be either a string (which will be displayed
        # in a fixed-width font) or a list of strings (which will be hidden
        # behind an expandable toggle).
        list_result = ["list", "of", "strings"]
        result.result = list_result
        d = result.to_dict
        assert list_result == d["result"]

        # Other .result values don't make it into the dictionary because
        # it's not defined how to display them.
        result.result = {"a": "dictionary"}
        d = result.to_dict
        assert None == d["result"]

    def test_repr_failure(self):
        """Show the string representation of a failed test result."""

        exception = IntegrationException("basic info", "debug info")

        result = SelfTestResult("failure1")
        result.start = self.now
        result.end = self.future
        result.exception = exception
        result.result = "The result"
        assert (
            "<SelfTestResult: name='failure1' duration=5.00sec success=False exception='basic info' debug='debug info' result='The result'>"
            == repr(result)
        )

        d = result.to_dict
        assert "failure1" == d["name"]
        assert "The result" == d["result"]
        assert 5.0 == d["duration"]
        assert False == d["success"]
        assert "IntegrationException" == d["exception"]["class"]
        assert "basic info" == d["exception"]["message"]
        assert "debug info" == d["exception"]["debug_message"]


class MockSelfTest(HasSelfTests):
    _integration: IntegrationConfiguration | None = None

    def __init__(self, *args, **kwargs):
        self.called_with_args = args
        self.called_with_kwargs = kwargs

    def integration(self, _db: Session) -> IntegrationConfiguration | None:
        return self._integration

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult, None, None]:
        raise Exception("oh no")


class TestHasSelfTests:
    def test_run_self_tests(
        self, db: DatabaseTransactionFixture, monkeypatch: MonkeyPatch
    ):
        """See what might happen when run_self_tests tries to instantiate an
        object and run its self-tests.
        """
        mock_db = MagicMock(spec=Session)

        # This integration will be used to store the test results.
        integration = db.integration_configuration(
            protocol="test", goal=Goals.PATRON_AUTH_GOAL
        )

        # By default, the default constructor is instantiated and its
        # _run_self_tests method is called.
        mock__run_self_tests = create_autospec(MockSelfTest._run_self_tests)
        mock__run_self_tests.return_value = [SelfTestResult("a test result")]
        monkeypatch.setattr(MockSelfTest, "_run_self_tests", mock__run_self_tests)
        monkeypatch.setattr(MockSelfTest, "_integration", integration)

        data, [setup, test] = MockSelfTest.run_self_tests(mock_db, extra_arg="a value")
        assert mock__run_self_tests.call_count == 1
        assert isinstance(mock__run_self_tests.call_args.args[0], MockSelfTest)
        assert mock__run_self_tests.call_args.args[1] == mock_db

        # There are two results -- `setup` from the initial setup
        # and `test` from the _run_self_tests call.
        assert setup.name == "Initial setup."
        assert setup.success is True
        assert isinstance(setup.result, MockSelfTest)
        assert setup.result.called_with_args == ()
        assert setup.result.called_with_kwargs == dict(extra_arg="a value")
        assert test.name == "a test result"

        # The `data` variable contains a dictionary describing the test
        # suite as a whole.
        assert data["duration"] < 1
        for key in "start", "end":
            assert key in data

        # `data['results']` contains dictionary versions of the self-tests
        # that were returned separately.
        r1, r2 = data["results"]
        assert r1 == setup.to_dict
        assert r2 == test.to_dict

        # A JSON version of `data` is stored in the
        # Integration returned by the integration()
        # method.
        assert integration.self_test_results == data

        # Remove the testing integration to show what happens when
        # HasSelfTests doesn't support the storage of test results.
        monkeypatch.setattr(MockSelfTest, "_integration", None)

        # You can specify a different class method to use as the
        # constructor. Once the object is instantiated, the same basic
        # code runs.
        integration.self_test_results = "this value will not be changed"
        data, [setup, test] = MockSelfTest.run_self_tests(
            mock_db,
            lambda **kwargs: MockSelfTest(extra_extra_arg="foo", **kwargs),
            extra_arg="a value",
        )
        assert "Initial setup." == setup.name
        assert setup.success is True
        assert setup.result.called_with_args == ()
        assert setup.result.called_with_kwargs == dict(
            extra_extra_arg="foo", extra_arg="a value"
        )
        assert "a test result" == test.name

        # Since the HasSelfTests object no longer has an associated
        # Integration, the test results are not persisted
        # anywhere.
        assert integration.self_test_results == "this value will not be changed"

        # If there's an exception in the constructor, the result is a
        # single SelfTestResult describing that failure. Since there is
        # no instance, _run_self_tests can't be called.
        data, [result] = MockSelfTest.run_self_tests(
            mock_db,
            MagicMock(side_effect=Exception("I don't work!")),
        )
        assert isinstance(result, SelfTestResult)
        assert result.success is False
        assert str(result.exception) == "I don't work!"

    def test_exception_in_has_self_tests(self):
        """An exception raised in has_self_tests itself is converted into a
        test failure.
        """

        status, [init, failure] = MockSelfTest.run_self_tests(MagicMock())
        assert init.name == "Initial setup."

        assert failure.name == "Uncaught exception in the self-test method itself."
        assert failure.success is False
        # The Exception was turned into an IntegrationException so that
        # its traceback could be included as debug_message.
        assert isinstance(failure.exception, IntegrationException)
        assert str(failure.exception) == "oh no"
        assert failure.exception.debug_message.startswith("Traceback")

    def test_run_test_success(self):
        mock = MockSelfTest()

        # This self-test method will succeed.
        def successful_test(arg, kwarg):
            return arg, kwarg

        result = mock.run_test(
            "A successful test", successful_test, "arg1", kwarg="arg2"
        )
        assert result.success is True
        assert result.name == "A successful test"
        assert result.result == ("arg1", "arg2")
        assert (result.end - result.start).total_seconds() < 1

    def test_run_test_failure(self):
        mock = MockSelfTest()

        # This self-test method will fail.
        def unsuccessful_test(arg, kwarg):
            raise IntegrationException(arg, kwarg)

        result = mock.run_test(
            "An unsuccessful test", unsuccessful_test, "arg1", kwarg="arg2"
        )
        assert result.success is False
        assert result.name == "An unsuccessful test"
        assert result.result is None
        assert str(result.exception) == "arg1"
        assert result.exception.debug_message == "arg2"
        assert (result.end - result.start).total_seconds() < 1

    def test_test_failure(self):
        mock = MockSelfTest()

        # You can pass in an Exception...
        exception = Exception("argh")
        now = utc_now()
        result = mock.test_failure("a failure", exception)

        # ...which will be turned into an IntegrationException.
        assert result.name == "a failure"
        assert isinstance(result.exception, IntegrationException)
        assert str(result.exception) == "argh"
        assert (result.start - now).total_seconds() < 1

        # ... or you can pass in arguments to an IntegrationException
        result = mock.test_failure("another failure", "message", "debug")
        assert isinstance(result.exception, IntegrationException)
        assert str(result.exception) == "message"
        assert result.exception.debug_message == "debug"

        # Since no test code actually ran, the end time is the
        # same as the start time.
        assert result.start == result.end
