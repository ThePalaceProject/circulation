from palace.brainfuck_vm.palace_brainfuck import run_palace_brainfuck

from palace.brainfuck_utils.sort_title_for import (
    SORT_TITLE_FOR_BF,
    _make_match_stopword_callback,
    move_matching_prefix_to_suffix,
)


class TestMoveMatchingPrefixToSuffix:
    """`TitleProcessor.sort_title_for` (tests/manager/util/test_util.py)
    covers each stopword and the partial-match case through the public
    function; these tests cover what it does not.
    """

    STOPWORDS = ["The ", "A ", "An "]

    def test_titles_containing_newlines_are_moved_intact(self):
        # The remainder and the moved prefix are handed to the formatting
        # program NUL-separated, so a newline inside the title is just
        # another byte to copy rather than the end of a field.
        assert move_matching_prefix_to_suffix("The A\nB", self.STOPWORDS) == "A\nB, The"
        assert move_matching_prefix_to_suffix("Zed\nB", self.STOPWORDS) == "Zed\nB"

    def test_non_ascii_titles_round_trip(self):
        assert (
            move_matching_prefix_to_suffix("The Æon Flux", self.STOPWORDS)
            == "Æon Flux, The"
        )

    def test_program_and_callback_directly(self):
        callback = _make_match_stopword_callback(self.STOPWORDS)
        output = run_palace_brainfuck(
            SORT_TITLE_FOR_BF, b"The Hobbit", functions={"s": callback}
        )
        assert output == b"Hobbit, The"
