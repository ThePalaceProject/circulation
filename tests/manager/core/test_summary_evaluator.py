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
