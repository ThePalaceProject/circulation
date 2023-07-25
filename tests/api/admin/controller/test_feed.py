import feedparser
import pytest

from api.admin.exceptions import AdminNotAuthorized
from core.classifier import genres
from core.model import AdminRole


class TestFeedController:
    def test_suppressed(self, admin_librarian_fixture):
        suppressed_work = admin_librarian_fixture.ctrl.db.work(
            with_open_access_download=True
        )
        suppressed_work.license_pools[0].suppressed = True

        unsuppressed_work = admin_librarian_fixture.ctrl.db.work()

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = (
                admin_librarian_fixture.manager.admin_feed_controller.suppressed()
            )
            feed = feedparser.parse(response.get_data(as_text=True))
            entries = feed["entries"]
            assert 1 == len(entries)
            assert suppressed_work.title == entries[0]["title"]

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_feed_controller.suppressed,
            )

    def test_genres(self, admin_librarian_fixture):
        with admin_librarian_fixture.ctrl.app.test_request_context("/"):
            response = admin_librarian_fixture.manager.admin_feed_controller.genres()

            for name in genres:
                top = "Fiction" if genres[name].is_fiction else "Nonfiction"
                assert response[top][name] == dict(
                    {
                        "name": name,
                        "parents": [parent.name for parent in genres[name].parents],
                        "subgenres": [
                            subgenre.name for subgenre in genres[name].subgenres
                        ],
                    }
                )
