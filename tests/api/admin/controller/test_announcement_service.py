import json
import uuid

from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.announcement_service import AnnouncementSettings
from core.model.announcements import Announcement, AnnouncementData
from core.problem_details import INVALID_INPUT
from core.util.problem_detail import ProblemDetail
from tests.fixtures.announcements import AnnouncementFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class TestAnnouncementService:
    def test_get(
        self,
        flask_app_fixture: FlaskAppFixture,
        announcement_fixture: AnnouncementFixture,
        db: DatabaseTransactionFixture,
    ):
        session = db.session
        a1 = announcement_fixture.active_announcement(session)
        a2 = announcement_fixture.expired_announcement(session)
        a3 = announcement_fixture.forthcoming_announcement(session)

        expected_start = []
        expected_finish = []
        for a in [a2, a1, a3]:
            assert a.start is not None
            assert a.finish is not None
            expected_start.append(a.start.strftime(announcement_fixture.format))
            expected_finish.append(a.finish.strftime(announcement_fixture.format))

        global_announcements = (
            session.execute(Announcement.global_announcements()).scalars().all()
        )

        with flask_app_fixture.test_request_context("/", method="GET"):
            response = AnnouncementSettings(db.session).process_many()
        assert isinstance(response, dict)

        assert set(response.keys()) == {"settings", "announcements"}
        announcements_in_response = response["announcements"]

        # All announcements should be available for management.
        assert 3 == len(global_announcements)
        assert len(global_announcements) == len(announcements_in_response)

        assert [str(a2.id), str(a1.id), str(a3.id)] == [
            a["id"] for a in announcements_in_response
        ]
        assert [a2.content, a1.content, a3.content] == [
            a["content"] for a in announcements_in_response
        ]
        assert expected_start == [a["start"] for a in announcements_in_response]
        assert expected_finish == [a["finish"] for a in announcements_in_response]

    def test_post(
        self,
        flask_app_fixture: FlaskAppFixture,
        announcement_fixture: AnnouncementFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context("/", method="POST") as ctx:
            data = AnnouncementData(
                id=uuid.uuid4(),
                start=announcement_fixture.yesterday,
                finish=announcement_fixture.tomorrow,
                content="This is a test announcement.",
            )
            ctx.request.form = ImmutableMultiDict(
                [("announcements", json.dumps([data.as_dict()]))]
            )
            response = AnnouncementSettings(db.session).process_many()

        assert response == {"success": True}
        session = db.session
        announcements = (
            session.execute(Announcement.global_announcements()).scalars().all()
        )
        assert len(announcements) == 1
        assert announcements[0].id == data.id
        assert announcements[0].start == data.start
        assert announcements[0].finish == data.finish
        assert announcements[0].content == data.content

    def test_post_edit(
        self,
        flask_app_fixture: FlaskAppFixture,
        announcement_fixture: AnnouncementFixture,
        db: DatabaseTransactionFixture,
    ):
        # Two existing announcements.
        session = db.session
        a1 = announcement_fixture.active_announcement(session)
        a2 = announcement_fixture.active_announcement(session)

        with flask_app_fixture.test_request_context("/", method="POST") as ctx:
            # a1 is edited, a2 is deleted, a3 is added.
            a1_edited = a1.to_data()
            a1_edited.content = "This is an edited announcement."
            a3 = AnnouncementData(
                id=uuid.uuid4(),
                start=announcement_fixture.yesterday,
                finish=announcement_fixture.tomorrow,
                content="This is new test announcement.",
            )
            ctx.request.form = ImmutableMultiDict(
                [("announcements", json.dumps([a1_edited.as_dict(), a3.as_dict()]))]
            )
            response = AnnouncementSettings(db.session).process_many()

        assert response == {"success": True}
        announcements = (
            session.execute(Announcement.global_announcements()).scalars().all()
        )
        assert len(announcements) == 2
        for actual, expected in zip(announcements, [a3, a1_edited]):
            assert actual.id == expected.id
            assert actual.start == expected.start
            assert actual.finish == expected.finish
            assert actual.content == expected.content

    def test_post_errors(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context("/", method="POST") as ctx:
            ctx.request.form = ImmutableMultiDict()
            response = AnnouncementSettings(db.session).process_many()
            assert response == INVALID_INPUT

            ctx.request.form = ImmutableMultiDict([("somethingelse", json.dumps([]))])
            response = AnnouncementSettings(db.session).process_many()
            assert response == INVALID_INPUT

            ctx.request.form = ImmutableMultiDict(
                [("announcements", json.dumps([{"id": str(uuid.uuid4())}]))]
            )
            response = AnnouncementSettings(db.session).process_many()
            assert isinstance(response, ProblemDetail)
            assert "Missing required field: content" == response.detail
