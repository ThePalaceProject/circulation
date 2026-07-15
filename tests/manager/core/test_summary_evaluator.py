"""Test the code that evaluates the quality of summaries."""

from palace.manager.util.summary import SummaryEvaluator


class TestSummaryEvaluator:
    def _best(self, *summaries):
        e = SummaryEvaluator()
        for s in summaries:
            e.add(s)
        e.ready()
        return e.best_choice()[0]

    def test_four_sentences_is_better_than_three(self):
        s1 = "Hey, this is Sentence one. And now, here is Sentence two."
        s2 = "Sentence one. Sentence two. Sentence three. Sentence four."
        assert s2 == self._best(s1, s2)

    def test_four_sentences_is_better_than_five(self):
        s1 = "Sentence 1. Sentence 2. Sentence 3. Sentence 4. Sentence 5."
        s2 = "Sentence one. Sentence two. Sentence three.  Sentence four."
        assert s2 == self._best(s1, s2)

    def test_shorter_is_better(self):
        # The two summaries have identical word coverage, sentence count, and
        # language profile, so only their length differs -- the shorter one is
        # preferred.
        s1 = "The White Rabbit and the Mock Turtle meet Alice."
        s2 = (
            "The White Rabbit and the Mock Turtle meet Alice, "
            "the White Rabbit and the Mock Turtle."
        )
        assert s1 == self._best(s1, s2)

    def test_word_coverage_is_important(self):
        # All three summaries are a single sentence, but s3 mentions more of
        # the words that recur across the summaries, so it is preferred.
        s1 = "The story of Alice and the White Rabbit."
        s2 = "The story of Alice and the Mock Turtle."
        s3 = "Alice meets the Mock Turtle and the White Rabbit."
        assert s3 == self._best(s1, s2, s3)

    def test_generic_marketing_copy_loses_to_on_topic_summary(self):
        # The generic blurb names none of the recurring characters, so it
        # covers none of the top words and is not chosen.
        generic = "A sweeping tale of love and loss. A must-read."
        on_topic = (
            "Elizabeth Bennet clashes with the proud Mr. Darcy in a comedy of "
            "manners about marriage."
        )
        second = "Elizabeth Bennet and Mr. Darcy overcome pride to find love."
        assert generic != self._best(generic, on_topic, second)

    def test_length_tiebreaker_does_not_override_word_coverage(self):
        # A short summary that covers only some of the consensus vocabulary must
        # not beat a longer one that covers all of it: the length preference is
        # only a tie-breaker, not a signal strong enough to invert coverage.
        short_partial = "Alice meets the Rabbit."
        full_coverage = (
            "Alice follows the White Rabbit down the hole, meets the Mock "
            "Turtle, and the Cheshire Cat, before waking from the dream." * 3
        )
        # A third summary so the recurring words become consensus vocabulary.
        second = (
            "Alice, the White Rabbit, the Mock Turtle, and the Cheshire Cat "
            "appear in the dream."
        )
        assert full_coverage == self._best(short_partial, full_coverage, second)

    def test_length_nudge_is_bounded(self):
        # The length preference is a bounded nudge, not an unbounded penalty:
        # however long a summary is, the length term scales its score by at most
        # ``length_nudge`` (i.e. never below ``1 - length_nudge``). This pins the
        # property that keeps length from ever overriding word coverage.
        #
        # Isolate the length term: four sentences (the optimal count) zeroes the
        # sentence penalty, "cat" carries no bad phrase, and disabling the
        # language penalty removes the only other factor -- so the score is
        # exactly the length multiplier.
        huge = "One cat. Two cats. Three cats. Four cats." + " cat" * 20000
        evaluator = SummaryEvaluator()
        evaluator.add(huge)
        evaluator.ready()
        assert len(huge) > 50000  # a genuinely enormous summary...
        score = evaluator.score(huge, apply_language_penalty=False)
        assert 1 - evaluator.length_nudge <= score < 1.0  # ...yet still bounded

    def test_bytes_summaries_are_decoded(self):
        # Summaries may arrive as bytes; they should be decoded and ranked just
        # like str summaries.
        s1 = b"Sentence one. Sentence two. Sentence three. Sentence four."
        s2 = b"Only one sentence."
        assert s1.decode("utf8") == self._best(s1, s2)

    def test_duplicate_summaries_are_only_counted_once(self):
        summary = "Alice and the White Rabbit and the Mock Turtle."
        evaluator = SummaryEvaluator()
        evaluator.add(summary)
        evaluator.add(summary)
        assert evaluator.summaries == [summary]

    def test_scoring_an_unseen_summary(self):
        # score() may be called with a summary that was never add()ed; its
        # content words are computed on the fly rather than looked up.
        evaluator = SummaryEvaluator()
        evaluator.add("Alice meets the White Rabbit and the Mock Turtle.")
        evaluator.add("The White Rabbit leads Alice to the Mock Turtle.")
        evaluator.ready()
        unseen = "Alice, the White Rabbit, and the Mock Turtle."
        assert evaluator.score(unseen) > 0

    def test_bad_phrases_are_penalized(self):
        # A summary containing a bad phrase is scored below an otherwise
        # equivalent summary without one.
        clean = "Alice meets the White Rabbit and the Mock Turtle."
        abridged = "An abridged version of Alice and the White Rabbit."
        evaluator = SummaryEvaluator()
        evaluator.add(clean)
        evaluator.add(abridged)
        evaluator.ready()
        assert evaluator.score(clean) > evaluator.score(abridged)

    def test_best_choice_with_no_summaries(self):
        evaluator = SummaryEvaluator()
        evaluator.ready()
        assert evaluator.best_choice() == (None, None)

    def test_custom_bad_phrases_replace_the_defaults(self):
        # Two evaluators with the same corpus but different bad-phrase sets. By
        # scoring the same summaries under both, every scoring term except the
        # bad-phrase penalty is held constant, so a score difference isolates
        # the effect of the bad-phrase set.
        custom_bad = "A forbidden summary of Alice and the White Rabbit."
        default_bad = "An abridged summary of Alice and the Mock Turtle."
        corpus = [custom_bad, default_bad]

        default_evaluator = SummaryEvaluator()
        custom_evaluator = SummaryEvaluator(bad_phrases={"forbidden"})
        for evaluator in (default_evaluator, custom_evaluator):
            for summary in corpus:
                evaluator.add(summary)
            evaluator.ready()

        # "abridged" is a default bad phrase but not in the custom set; the
        # custom evaluator therefore stops penalizing it.
        assert custom_evaluator.score(default_bad) > default_evaluator.score(
            default_bad
        )
        # "forbidden" is only in the custom set; only the custom evaluator
        # penalizes it.
        assert custom_evaluator.score(custom_bad) < default_evaluator.score(custom_bad)

    def test_score_calls_ready_and_accepts_bytes(self):
        # score() called before ready() triggers ready() itself, and it accepts
        # bytes just like add().
        evaluator = SummaryEvaluator()
        evaluator.add("Alice meets the White Rabbit and the Mock Turtle.")
        assert evaluator.top_words is None
        score = evaluator.score(b"Alice and the White Rabbit.")
        assert evaluator.top_words is not None
        assert score > 0

    def test_regex_bad_patterns_and_dashes_are_penalized(self):
        # The regex-based bad patterns and an excess of " -- " separators both
        # push a summary's score down.
        clean = "Alice meets the White Rabbit and the Mock Turtle."
        bad_regex = "Includes Alice and the White Rabbit and the Mock Turtle."
        dashes = "Alice -- the White -- Rabbit -- Mock -- Turtle -- again."
        evaluator = SummaryEvaluator()
        for summary in (clean, bad_regex, dashes):
            evaluator.add(summary)
        evaluator.ready()
        assert evaluator.score(clean) > evaluator.score(bad_regex)
        assert evaluator.score(clean) > evaluator.score(dashes)

    def test_non_english_is_penalized(self):
        """If description text appears not to be in English, it is rated down
        for its deviations from average English bigram distribution.
        """
        dutch = "Op haar nieuwe school leert de jarige Bella (ik-figuur) een mysterieuze jongen kennen op wie ze ogenblikkelijk verliefd wordt. Hij blijkt een groot geheim te hebben. Vanaf ca. jaar."

        evaluator = SummaryEvaluator()
        evaluator.add(dutch)
        evaluator.ready()

        dutch_no_language_penalty = evaluator.score(dutch, apply_language_penalty=False)
        dutch_language_penalty = evaluator.score(dutch, apply_language_penalty=True)

        assert dutch_language_penalty < dutch_no_language_penalty

    def test_english_is_not_penalized(self):
        """If description text appears to be in English, it is not rated down
        for its deviations from average English bigram distribution.
        """

        english = "After the warrior cat Clans settle into their new homes, the harmony they once had disappears as the clans start fighting each other, until the day their common enemy the badger."

        evaluator = SummaryEvaluator()
        evaluator.add(english)
        evaluator.ready()

        english_no_language_penalty = evaluator.score(
            english, apply_language_penalty=False
        )
        english_language_penalty = evaluator.score(english, apply_language_penalty=True)
        assert english_language_penalty == english_no_language_penalty
