import re
from collections import Counter
from re import Pattern

from palace.manager.util import Bigrams, english_bigrams
from palace.manager.util.stopwords import ENGLISH_STOPWORDS

# Splits text into candidate word tokens. A token must start with a letter and
# may contain apostrophes thereafter (so contractions like "don't" survive while
# numbers, punctuation, and apostrophe-only runs like "'''" are dropped).
_WORD_RE = re.compile(r"[a-z][a-z']*")

# Matches a run of sentence-terminating punctuation followed by whitespace or
# the end of the string. Used as a lightweight sentence counter in place of a
# full NLP tokenizer.
_SENTENCE_END = re.compile(r"[.!?]+(?=\s|$)")


def _content_words(text: str) -> set[str]:
    """Return the set of meaningful (non-stopword) words in ``text``.

    We use a set rather than a list so that a single summary repeating a word
    cannot inflate that word's importance -- each word counts once per summary.
    """
    return {
        word
        for word in _WORD_RE.findall(text.lower())
        if len(word) > 2 and word not in ENGLISH_STOPWORDS
    }


def _count_sentences(text: str) -> int:
    """Approximate the number of sentences in ``text``.

    This is a heuristic (it can be fooled by abbreviations like "Mr.") that
    replaces the NLP sentence tokenizer we used to rely on. A summary always
    counts as at least one sentence.
    """
    return max(1, len(_SENTENCE_END.findall(text.strip())))


class SummaryEvaluator:
    """Evaluate summaries of a book to find a usable summary.

    A usable summary will have good coverage of the words used across all
    summaries of the book, weighted toward the words that recur most (a proxy
    for being on-topic rather than generic marketing copy), will have an
    approximate length of four sentences (this is customizable), and will not
    mention words that indicate it's a summary of a specific edition of the book.

    All else being equal, a shorter summary is better.

    A summary is penalized for apparently not being in English.
    """

    # These phrases are indicative of a description we can't use for
    # whatever reason.
    default_bad_phrases = {
        "version of",
        "retelling of",
        "abridged",
        "retelling",
        "condensed",
        "adaptation of",
        "look for",
        "new edition",
        "excerpts",
        "version",
        "edition",
        "selections",
        "complete texts",
        "in one volume",
        "contains",
        "--container",
        "--original container",
        "playaway",
        "complete novels",
        "all rights reserved",
    }

    bad_res: set[Pattern[str]] = {
        re.compile("the [^ ]+ Collection"),
        re.compile("Includes"),
        re.compile("This is"),
    }

    # "Shorter is better" is only a tie-breaker, so we express it as a bounded
    # nudge: length maps to a multiplier in ``[1 - length_nudge, 1]`` rather
    # than the old unbounded ``1 / (1 + len / n)`` that kept shrinking toward
    # zero. ``length_nudge`` is the most a long summary can be scaled down (5%),
    # small enough that it only decides between summaries whose word-coverage and
    # other signals are already within a few percent -- it cannot overturn a
    # meaningful coverage difference.
    length_nudge: float = 0.05
    # Characters at which the nudge reaches half of ``length_nudge``. Sets how
    # quickly the (bounded) preference ramps in; it does not change the cap.
    length_half_life: int = 1000

    def __init__(
        self,
        optimal_number_of_sentences: int = 4,
        bad_phrases: set[str] | None = None,
    ) -> None:
        self.optimal_number_of_sentences = optimal_number_of_sentences
        self.summaries: list[str] = []
        # Maps each summary to its set of content words.
        self.word_sets: dict[str, set[str]] = {}
        # Counts, for each content word, the number of summaries it appears in.
        self.content_words: Counter[str] = Counter()
        # The sum of every content word's weight, i.e. the maximum coverage score
        # a summary could earn. ``None`` until ``ready()`` computes it.
        self.total_word_weight: float | None = None
        if bad_phrases is None:
            self.bad_phrases = self.default_bad_phrases
        else:
            self.bad_phrases = bad_phrases

    @staticmethod
    def _word_weight(document_frequency: int) -> int:
        """Weight a content word by how many summaries it appears in.

        The weight is the square of the document frequency, so words shared
        across many summaries dominate the score while incidental words still
        contribute a little. Squaring (rather than a hard "appears in >= 2
        summaries" cut-off) keeps the coverage signal meaningful even when a
        work has only two candidate descriptions -- where every shared word is
        trivially present in both, so a binary consensus set cannot discriminate.
        """
        return document_frequency * document_frequency

    def add(self, summary: str | bytes) -> None:
        if isinstance(summary, bytes):
            summary = summary.decode("utf8")
        if summary in self.word_sets:
            # We already evaluated this summary. Don't count it more than once
            return
        words = _content_words(summary)
        self.word_sets[summary] = words
        self.summaries.append(summary)
        for word in words:
            self.content_words[word] += 1

    def ready(self) -> None:
        """We are done adding to the corpus and ready to start evaluating.

        The words that characterize a work are the ones that recur across its
        summaries, so each content word is weighted by its recurrence (see
        :meth:`_word_weight`). We precompute the total available weight here so
        that :meth:`score` can express each summary's coverage as a fraction of
        it. This is what lets an on-topic summary outrank generic marketing copy,
        which shares little vocabulary with the work's other descriptions.
        """
        self.total_word_weight = sum(
            self._word_weight(count) for count in self.content_words.values()
        )

    def best_choice(self) -> tuple[str, float] | tuple[None, None]:
        c = self.best_choices(1)
        if c:
            return c[0]
        else:
            return None, None

    def best_choices(self, n: int = 3) -> list[tuple[str, float]]:
        """Choose the best `n` choices among the current summaries."""
        scores: dict[str, float] = {}
        for summary in self.summaries:
            scores[summary] = self.score(summary)
        return sorted(scores.items(), key=lambda item: item[1], reverse=True)[:n]

    def score(self, summary: str | bytes, apply_language_penalty: bool = True) -> float:
        """Score a summary relative to our current view of the dataset."""
        if isinstance(summary, bytes):
            summary = summary.decode("utf8")

        if self.total_word_weight is None:
            self.ready()
        if self.total_word_weight:
            words = self.word_sets.get(summary)
            if words is None:
                words = _content_words(summary)
            # Coverage: the share of the corpus's total (recurrence-weighted)
            # vocabulary that this summary accounts for. A word the summary does
            # not contain, or that never appeared in the corpus, contributes 0.
            covered = sum(self._word_weight(self.content_words[w]) for w in words)
            score = covered / self.total_word_weight
        else:
            # No summary contained any content words, so there is nothing to
            # measure against. Start from a neutral score and let the penalties
            # below -- length, bad phrases, and non-English text -- decide.
            score = 1.0

        sentences = _count_sentences(summary)
        off_from_optimal: int | float = abs(
            sentences - self.optimal_number_of_sentences
        )
        if off_from_optimal == 1:
            off_from_optimal = 1.5
        if off_from_optimal:
            # This summary is too long or too short.
            score /= off_from_optimal**1.5

        bad_phrases = 0
        l = summary.lower()
        for phrase in self.bad_phrases:
            if phrase in l:
                bad_phrases += 1

        for pattern in self.bad_res:
            if pattern.search(summary):
                bad_phrases += 1

        if l.count(" -- ") > 3:
            bad_phrases += l.count(" -- ") - 3

        score *= 0.5**bad_phrases

        if apply_language_penalty:
            language_difference = english_bigrams.difference_from(
                Bigrams.from_string(summary)
            )
            if language_difference > 1:
                score *= 0.5 ** (language_difference - 1)

        # All else being equal, prefer a shorter summary. This is a bounded
        # tie-breaker: ``saturation`` grows from 0 toward 1 with length, so the
        # multiplier stays within ``[1 - length_nudge, 1]`` no matter how long
        # the summary is (~0.976x at 1000 chars, ~0.963x at 3000, never below
        # 0.95x). See ``length_nudge`` for why this can only break ties.
        saturation = len(summary) / (len(summary) + self.length_half_life)
        score *= 1 - self.length_nudge * saturation

        return score
