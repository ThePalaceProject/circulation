import sys

from palace.manager.scripts.input import LibraryInputScript


class RunSelfTestsScript(LibraryInputScript):
    """Run the self-tests for every collection in the given library
    where that's possible.
    """

    def __init__(self, _db=None, output=sys.stdout):
        super().__init__(_db)
        self.out = output

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            api_map = self.services.integration_registry.license_providers()
            self.out.write("Testing %s\n" % library.name)
            for collection in library.associated_collections:
                try:
                    self.test_collection(collection, api_map)
                except Exception as e:
                    self.out.write("  Exception while running self-test: '%s'\n" % e)

    def test_collection(self, collection, api_map, extra_args=None):
        tester = api_map.get(collection.protocol)
        if not tester:
            self.out.write(
                " Cannot find a self-test for %s, ignoring.\n" % collection.name
            )
            return

        self.out.write(" Running self-test for %s.\n" % collection.name)
        extra_args = extra_args or {}
        extra = extra_args.get(tester, [])
        constructor_args = [self._db, collection] + list(extra)
        results_dict, results_list = tester.run_self_tests(
            self._db, None, *constructor_args
        )
        for result in results_list:
            self.process_result(result)

    def process_result(self, result):
        """Process a single TestResult object."""
        if result.success:
            success = "SUCCESS"
        else:
            success = "FAILURE"
        self.out.write(f"  {success} {result.name} ({result.duration:.1f}sec)\n")
        if isinstance(result.result, (bytes, str)):
            self.out.write("   Result: %s\n" % result.result)
        if result.exception:
            self.out.write("   Exception: '%s'\n" % result.exception)
