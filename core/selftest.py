"""Define the interfaces used by ExternalIntegration self-tests.
"""
from __future__ import annotations

import json
import logging
import sys
import traceback
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from datetime import datetime
from typing import Any, ParamSpec, TypeVar

from sqlalchemy.orm import Session

from core.model import Collection, ExternalIntegration
from core.model.integration import IntegrationConfiguration
from core.util.datetime_helpers import utc_now
from core.util.http import IntegrationException
from core.util.log import LoggerMixin
from core.util.opds_writer import AtomFeed

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class SelfTestResult:
    """The result of running a single self-test.

    HasSelfTest.run_self_tests() returns a list of these
    """

    def __init__(self, name: str | None):
        # Name of the test.
        self.name = name

        # Set to True when the test runs without raising an exception.
        self.success = False

        # The exception raised, if any.
        self.exception: Exception | None = None

        # The return value of the test method, assuming it ran to
        # completion.
        self.result: Any = None

        # Start time of the test.
        self.start: datetime = utc_now()

        # End time of the test.
        self.end: datetime | None = None

        # Collection associated with the test
        self.collection: Collection | None = None

    @property
    def to_dict(self) -> dict[str, Any]:
        """Convert this SelfTestResult to a dictionary for use in
        JSON serialization.
        """
        # Time formatting method
        f = AtomFeed._strftime
        if self.exception:
            exception = {
                "class": self.exception.__class__.__name__,
                "message": str(self.exception),
                "debug_message": self.debug_message,
            }
        else:
            exception = None
        value: dict[str, Any] = dict(
            name=self.name,
            success=self.success,
            duration=self.duration,
            exception=exception,
        )
        if self.start:
            value["start"] = f(self.start)
        if self.end:
            value["end"] = f(self.end)

        if self.collection:
            value["collection"] = self.collection.name

        # String results will be displayed in a fixed-width font.
        # Lists of strings will be hidden behind an expandable toggle.
        # Other return values have no defined method of display.
        if isinstance(self.result, str) or isinstance(self.result, list):
            value["result"] = self.result
        else:
            value["result"] = None
        return value

    def __repr__(self) -> str:
        if self.exception:
            if isinstance(self.exception, IntegrationException):
                exception = " exception={!r} debug={!r}".format(
                    str(self.exception),
                    self.debug_message,
                )
            else:
                exception = " exception=%r" % self.exception
        else:
            exception = ""
        if self.collection:
            collection = " collection=%r" % self.collection.name
        else:
            collection = ""
        return "<SelfTestResult: name={!r}{} duration={:.2f}sec success={!r}{} result={!r}>".format(
            self.name,
            collection,
            self.duration,
            self.success,
            exception,
            self.result,
        )

    @property
    def duration(self) -> float:
        """How long the test took to run."""
        if not self.start or not self.end:
            return 0
        return (self.end - self.start).total_seconds()

    @property
    def debug_message(self) -> str | None:
        """The debug message associated with the Exception, if any."""
        if not self.exception:
            return None
        return getattr(self.exception, "debug_message", None)


T = TypeVar("T")
P = ParamSpec("P")


class BaseHasSelfTests(ABC):
    """An object capable of verifying its own setup by running a
    series of self-tests.
    """

    @classmethod
    def run_self_tests(
        cls: type[Self],
        _db: Session,
        constructor_method: Callable[..., Self] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[dict[str, Any], list[SelfTestResult]]:
        """Instantiate this class and call _run_self_tests on it.

        :param _db: A database connection. Will be passed into `_run_self_tests`.
            This connection may need to be used again
            in args, if the constructor needs it.

        :param constructor_method: Method to use to instantiate the
            class, if different from the default constructor.

        :param args: Positional arguments to pass into the constructor.
        :param kwargs: Keyword arguments to pass into the constructor.

        :return: A 2-tuple (results_dict, results_list) `results_dict`
            is a JSON-serializable dictionary describing the results of
            the self-test. `results_list` is a list of SelfTestResult
            objects.

        """
        constructor_method = constructor_method or cls
        start = utc_now()
        result = SelfTestResult("Initial setup.")
        instance = None
        results = []
        # Treat the construction of the integration code as its own
        # test.
        try:
            instance = constructor_method(*args, **kwargs)
            result.success = True
            result.result = instance
        except Exception as e:
            result.exception = e
            result.success = False
        finally:
            result.end = utc_now()
        results.append(result)
        if instance:
            try:
                for result in instance._run_self_tests(_db):
                    results.append(result)

            except Exception as e:
                # This should only happen when there's a bug in the
                # self-test method itself.
                failure = instance.test_failure(
                    "Uncaught exception in the self-test method itself.", e
                )
                results.append(failure)

        end = utc_now()

        # Format the results in a useful way.

        value = dict(
            start=AtomFeed._strftime(start),
            end=AtomFeed._strftime(end),
            duration=(end - start).total_seconds(),
            results=[x.to_dict for x in results],
        )
        # Store the formatted results in the database, if we can find
        # a place to store them.
        if instance is not None:
            instance.store_self_test_results(_db, value, results)

        return value, results

    @staticmethod
    def run_test(
        name: str, method: Callable[P, T], *args: P.args, **kwargs: P.kwargs
    ) -> SelfTestResult:
        """Run a test method, record any exception that happens, and keep
        track of how long the test takes to run.

        :param name: The name of the test to be run.
        :param method: A method to call to run the test.
        :param args: Positional arguments to `method`.
        :param kwargs: Keyword arguments to `method`.

        :return: A filled-in SelfTestResult.
        """
        result = SelfTestResult(name)
        try:
            return_value = method(*args, **kwargs)
            result.success = True
            result.result = return_value
        except Exception as e:
            result.exception = e
            result.success = False
            result.result = None
        finally:
            if not result.end:
                result.end = utc_now()

        return result

    @classmethod
    def test_failure(
        cls,
        name: str,
        message: str | None | Exception,
        debug_message: str | None = None,
    ) -> SelfTestResult:
        """Create a SelfTestResult for a known failure.

        This is useful when you can't even get the data necessary to
        run a test method.
        """
        result = SelfTestResult(name)
        result.end = result.start
        result.success = False
        if isinstance(message, Exception):
            exception = message
            message = str(exception)
            if not debug_message:
                debug_message = traceback.format_exc()
        exception = IntegrationException(message, debug_message)
        result.exception = exception
        return result

    @abstractmethod
    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult, None, None]:
        """Run the self-tests.

        :return: A generator that yields SelfTestResult objects.
        """
        ...

    @abstractmethod
    def store_self_test_results(
        self, _db: Session, value: dict[str, Any], results: list[SelfTestResult]
    ) -> None:
        ...


class HasSelfTests(BaseHasSelfTests, ABC):
    """An object capable of verifying its own setup by running a
    series of self-tests.
    """

    # Self-test results are stored in a ConfigurationSetting with this name,
    # associated with the appropriate ExternalIntegration.
    SELF_TEST_RESULTS_SETTING = "self_test_results"

    def store_self_test_results(
        self, _db: Session, value: dict[str, Any], results: list[SelfTestResult]
    ) -> None:
        """Store the results of a self-test in the database."""
        integration: ExternalIntegration | None
        from core.external_search import ExternalSearchIndex

        if isinstance(self, ExternalSearchIndex):
            integration = self.search_integration(_db)
            for idx, result in enumerate(value.get("results")):  # type: ignore[arg-type]
                if isinstance(results[idx].result, list):
                    result["result"] = results[idx].result
        else:
            integration = self.external_integration(_db)

        if integration is not None:
            integration.setting(self.SELF_TEST_RESULTS_SETTING).value = json.dumps(
                value
            )

    @classmethod
    def prior_test_results(
        cls: type[Self],
        _db: Session,
        constructor_method: Callable[..., Self] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any] | None | str:
        """Retrieve the last set of test results from the database.

        The arguments here are the same as the arguments to run_self_tests.
        """
        constructor_method = constructor_method or cls
        instance = constructor_method(*args, **kwargs)
        integration: ExternalIntegration | None

        from core.external_search import ExternalSearchIndex

        if isinstance(instance, ExternalSearchIndex):
            integration = instance.search_integration(_db)
        else:
            integration = instance.external_integration(_db)

        if integration:
            return (
                integration.setting(cls.SELF_TEST_RESULTS_SETTING).json_value
                or "No results yet"
            )

        return None

    def external_integration(self, _db: Session) -> ExternalIntegration | None:
        """Locate the ExternalIntegration associated with this object.
        The status of the self-tests will be stored as a ConfigurationSetting
        on this ExternalIntegration.

        By default, there is no way to get from an object to its
        ExternalIntegration, and self-test status will not be stored.
        """
        logger = logging.getLogger("Self-test system")
        logger.error(
            "No ExternalIntegration was found.  Self-test results will not be stored."
        )
        return None


class HasSelfTestsIntegrationConfiguration(BaseHasSelfTests, LoggerMixin, ABC):
    def store_self_test_results(
        self, _db: Session, value: dict[str, Any], results: list[SelfTestResult]
    ) -> None:
        integration = self.integration(_db)
        if integration is None:
            self.logger().error(
                "No IntegrationConfiguration was found. Self-test results will not be stored."
            )
        else:
            integration.self_test_results = value

    @classmethod
    def load_self_test_results(
        cls, integration: IntegrationConfiguration | None
    ) -> dict[str, Any] | None:
        if integration is None:
            cls.logger().error(
                "No IntegrationConfiguration was found. Self-test results could not be loaded."
            )
            return None

        if not isinstance(integration.self_test_results, dict):
            cls.logger().error(
                "Self-test results were not stored as a dict. Self-test results could not be loaded."
            )
            return None

        return integration.self_test_results

    @classmethod
    def prior_test_results(
        cls: type[Self],
        _db: Session,
        constructor_method: Callable[..., Self] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any] | None | str:
        """Retrieve the last set of test results from the database.

        The arguments here are the same as the arguments to run_self_tests.
        """
        constructor_method = constructor_method or cls
        instance = constructor_method(*args, **kwargs)
        integration: IntegrationConfiguration | None = instance.integration(_db)
        return cls.load_self_test_results(integration) or "No results yet"

    @abstractmethod
    def integration(self, _db: Session) -> IntegrationConfiguration | None:
        ...
