import faulthandler
from unittest.mock import patch

from palace.manager.service.logging.debug_traceback import setup_debug_traceback


def test_setup_debug_traceback():
    with patch.object(
        faulthandler, "dump_traceback_later"
    ) as mock_dump_traceback_later:
        # If the interval is 0, the feature should be disabled.
        setup_debug_traceback(0)
        mock_dump_traceback_later.assert_not_called()

        # Otherwise, we should call dump_traceback_later with the correct arguments.
        setup_debug_traceback(5)
        mock_dump_traceback_later.assert_called_once_with(5, repeat=True)
