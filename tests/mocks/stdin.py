class MockStdin:
    """Mock a list of identifiers passed in on standard input."""

    def __init__(self, *lines):
        self.lines = lines

    def readlines(self):
        lines = self.lines
        self.lines = []
        return lines
