import sys
from collections.abc import Mapping
from typing import Any, TextIO

from sqlalchemy.orm import Session

from palace.manager.core.selftest import SelfTestResult
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.sqlalchemy.model.collection import Collection


class RunSelfTestsScript(LibraryInputScript):
    """Run the self-tests for every collection in the given library
    where that's possible.
    """

    def __init__(self, _db: Session | None = None, output: TextIO = sys.stdout) -> None:
        super().__init__(_db)
        self.out = output

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            api_map = self.services.integration_registry.license_providers()
            self.out.write(f"Testing {library.name}\n")
            for collection in library.associated_collections:
                try:
                    self.test_collection(collection, api_map)
                except Exception as e:
                    self.out.write(f"  Exception while running self-test: '{e}'\n")

    def test_collection(
        self,
        collection: Collection,
        api_map: Mapping[str, Any],
        extra_args: dict[type[Any], list[Any]] | None = None,
    ) -> None:
        tester = api_map.get(collection.protocol)
        if not tester:
            self.out.write(
                f" Cannot find a self-test for {collection.name}, ignoring.\n"
            )
            return

        self.out.write(f" Running self-test for {collection.name}.\n")
        extra_args = extra_args or {}
        extra = extra_args.get(tester, [])
        constructor_args = [self._db, collection] + list(extra)
        results_dict, results_list = tester.run_self_tests(
            self._db, None, *constructor_args
        )
        for result in results_list:
            self.process_result(result)

    def process_result(self, result: SelfTestResult) -> None:
        """Process a single TestResult object."""
        if result.success:
            success = "SUCCESS"
        else:
            success = "FAILURE"
        self.out.write(f"  {success} {result.name} ({result.duration:.1f}sec)\n")
        if isinstance(result.result, str):
            result_text = result.result
        elif result.result is not None:
            result_text = f"{result.result!r}"
        else:
            result_text = None
        if result_text is not None:
            self.out.write(f"   Result: {result_text}\n")
        if result.exception:
            self.out.write(f"   Exception: '{result.exception}'\n")
