from __future__ import annotations

from palace.manager.core.classifier import Classifier
from palace.manager.scripts.identifier import AddClassificationScript
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.stdin import MockStdin


class TestAddClassificationScript:
    def test_end_to_end(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        stdin = MockStdin(identifier.identifier)
        assert Classifier.AUDIENCE_ADULT == work.audience

        cmd_args = [
            "--identifier-type",
            identifier.type,
            "--subject-type",
            Classifier.FREEFORM_AUDIENCE,
            "--subject-identifier",
            Classifier.AUDIENCE_CHILDREN,
            "--weight",
            "42",
            "--create-subject",
        ]
        script = AddClassificationScript(_db=db.session, cmd_args=cmd_args, stdin=stdin)
        script.do_run()

        # The identifier has been classified under 'children'.
        [classification] = identifier.classifications
        assert 42 == classification.weight
        subject = classification.subject
        assert Classifier.FREEFORM_AUDIENCE == subject.type
        assert Classifier.AUDIENCE_CHILDREN == subject.identifier

        # The work has been reclassified and is now known as a
        # children's book.
        assert Classifier.AUDIENCE_CHILDREN == work.audience

    def test_autocreate(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        identifier = work.license_pools[0].identifier
        stdin = MockStdin(identifier.identifier)
        assert Classifier.AUDIENCE_ADULT == work.audience

        cmd_args = [
            "--identifier-type",
            identifier.type,
            "--subject-type",
            Classifier.TAG,
            "--subject-identifier",
            "some random tag",
        ]
        script = AddClassificationScript(_db=db.session, cmd_args=cmd_args, stdin=stdin)
        script.do_run()

        # Nothing has happened. There was no Subject with that
        # identifier, so we assumed there was a typo and did nothing.
        assert [] == identifier.classifications

        # If we stick the 'create-subject' onto the end of the
        # command-line arguments, the Subject is created and the
        # classification happens.
        stdin = MockStdin(identifier.identifier)
        cmd_args.append("--create-subject")
        script = AddClassificationScript(_db=db.session, cmd_args=cmd_args, stdin=stdin)
        script.do_run()

        [classification] = identifier.classifications
        subject = classification.subject
        assert "some random tag" == subject.identifier
