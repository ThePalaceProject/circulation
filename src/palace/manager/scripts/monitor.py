import logging

from sqlalchemy.orm import Session

from palace.manager.core.monitor import CollectionMonitor, ReaperMonitor
from palace.manager.scripts.base import Script
from palace.manager.scripts.input import CollectionArgumentsScript
from palace.manager.sqlalchemy.session import production_session


class RunMonitorScript(Script):
    def __init__(self, monitor, _db=None, **kwargs):
        super().__init__(_db)
        if issubclass(monitor, CollectionMonitor):
            self.collection_monitor = monitor
            self.collection_monitor_kwargs = kwargs
            self.monitor = None
            self.name = self.collection_monitor.SERVICE_NAME
        else:
            self.collection_monitor = None
            if callable(monitor):
                monitor = monitor(self._db, **kwargs)
            self.monitor = monitor
            self.name = self.monitor.service_name

    def do_run(self):
        if self.monitor:
            self.monitor.run()
        elif self.collection_monitor:
            logging.warning(
                "Running a CollectionMonitor by delegating to RunCollectionMonitorScript. "
                "It would be better if you used RunCollectionMonitorScript directly."
            )
            RunCollectionMonitorScript(
                self.collection_monitor, self._db, **self.collection_monitor_kwargs
            ).run()


class RunMultipleMonitorsScript(Script):
    """Run a number of monitors in sequence.

    Currently the Monitors are run one at a time. It should be
    possible to take a command-line argument that runs all the
    Monitors in batches, each in its own thread. Unfortunately, it's
    tough to know in a given situation that this won't overload the
    system.
    """

    def __init__(self, _db=None, **kwargs):
        """Constructor.

        :param kwargs: Keyword arguments to pass into the `monitors` method
            when building the Monitor objects.
        """
        super().__init__(_db)
        self.kwargs = kwargs

    def monitors(self, **kwargs):
        """Find all the Monitors that need to be run.

        :return: A list of Monitor objects.
        """
        raise NotImplementedError()

    def do_run(self):
        for monitor in self.monitors(**self.kwargs):
            try:
                monitor.run()
            except Exception as e:
                # This is bad, but not so bad that we should give up trying
                # to run the other Monitors.
                if monitor.collection:
                    collection_name = monitor.collection.name
                else:
                    collection_name = None
                monitor.exception = e
                self.log.error(
                    "Error running monitor %s for collection %s: %s",
                    self.name,
                    collection_name,
                    e,
                    exc_info=e,
                )


class RunReaperMonitorsScript(RunMultipleMonitorsScript):
    """Run all the monitors found in ReaperMonitor.REGISTRY"""

    name = "Run all reaper monitors"

    def monitors(self, **kwargs):
        return [cls(self._db, **kwargs) for cls in ReaperMonitor.REGISTRY]


class RunCollectionMonitorScript(RunMultipleMonitorsScript, CollectionArgumentsScript):
    """Run a CollectionMonitor on every Collection that comes through a
    certain protocol.
    """

    @property
    def _db(self) -> Session:
        if not hasattr(self, "_session"):
            self._session = production_session(self.monitor_class.__class__)
        return self._session

    def __init__(self, monitor_class, _db=None, cmd_args=None, **kwargs):
        """Constructor.

        :param monitor_class: A class object that derives from
            CollectionMonitor.
        :type monitor_class: CollectionMonitor

        :param cmd_args: Optional command line arguments. These will be
            passed on to the command line parser.
        :type cmd_args: Optional[List[str]]

        :param kwargs: Keyword arguments to pass into the `monitor_class`
            constructor each time it's called.

        """
        super().__init__(_db, **kwargs)
        self.monitor_class = monitor_class
        self.name = self.monitor_class.SERVICE_NAME
        parsed = vars(self.parse_command_line(self._db, cmd_args=cmd_args))
        parsed.pop("collection_names", None)
        self.collections = parsed.pop("collections", None)
        self.kwargs.update(parsed)

    def monitors(self, **kwargs):
        return self.monitor_class.all(self._db, collections=self.collections, **kwargs)
