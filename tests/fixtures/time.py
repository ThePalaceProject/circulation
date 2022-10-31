class Time:
    @staticmethod
    def time_eq(a, b):
        """Assert that two times are *approximately* the same -- within 2 seconds."""
        if a < b:
            delta = b - a
        else:
            delta = a - b
        total_seconds = delta.total_seconds()
        assert total_seconds < 2, "Delta was too large: %.2f seconds." % total_seconds
