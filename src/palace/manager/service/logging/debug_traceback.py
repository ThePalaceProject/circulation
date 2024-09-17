import faulthandler


def setup_debug_traceback(interval: int) -> None:
    """
    Sets up the faulthandler module to dump a traceback to stderr at the specified interval.

    :param interval: The interval (in seconds) at which to dump the traceback. If 0, the feature is disabled.
    """

    if interval > 0:
        faulthandler.dump_traceback_later(interval, repeat=True)
