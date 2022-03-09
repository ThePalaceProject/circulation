import time

import sqlalchemy


class DBStatementCounter:
    """
    Use as a context manager to count the number of execute()'s performed
    against the given sqlalchemy connection.

    Usage:
        with DBStatementCounter(conn) as ctr:
            conn.execute("SELECT 1")
            conn.execute("SELECT 1")
        assert ctr.get_count() == 2
    """

    def __init__(self, conn):
        self.conn = conn
        self.count = 0
        # Will have to rely on this since sqlalchemy 0.8 does not support
        # removing event listeners
        self.do_count = False
        sqlalchemy.event.listen(conn, "after_execute", self.callback)

    def __enter__(self):
        self.do_count = True
        return self

    def __exit__(self, *_):
        self.do_count = False

    def get_count(self):
        return self.count

    def callback(self, *_):
        if self.do_count:
            self.count += 1


class PerfTimer:
    """Performance timer to wrap around blocks of code

    Usage:
        <code we don't want timed>
        ....
        with PerfTimer() as pt:
            <do code we need to time>
            ....
        print ("Time taken:", pt.execution_time)
    """

    def __enter__(self):
        self.start = time.perf_counter()
        self.execution_time = 0
        return self

    def __exit__(self, *args):
        self.execution_time = time.perf_counter() - self.start
