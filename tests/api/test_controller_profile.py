from __future__ import annotations

import json
from typing import Any

import pytest

from palace.api.authenticator import CirculationPatronProfileStorage
from palace.core.model import Annotation, Patron
from palace.core.user_profile import ProfileController, ProfileStorage
from palace.core.util.problem_detail import ProblemDetail
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.vendor_id import VendorIDFixture


class ProfileFixture(ControllerFixture):
    auth: dict[Any, Any]
    other_patron: Patron

    def __init__(
        self, db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
    ):
        super().__init__(db, vendor_id_fixture, setup_cm=True)
        # Nothing will happen to this patron. This way we can verify
        # that a patron can only see/modify their own profile.
        self.other_patron = db.patron()
        self.other_patron.synchronize_annotations = False
        self.auth = dict(Authorization=self.valid_auth)


@pytest.fixture(scope="function")
def profile_fixture(db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture):
    return ProfileFixture(db, vendor_id_fixture)


class TestProfileController:
    """Test that a client can interact with the User Profile Management
    Protocol.
    """

    def test_controller_uses_circulation_patron_profile_storage(
        self, profile_fixture: ProfileFixture
    ):
        """Verify that this controller uses circulation manager-specific extensions."""
        with profile_fixture.request_context_with_library(
            "/", method="GET", headers=profile_fixture.auth
        ):
            assert isinstance(
                profile_fixture.manager.profiles._controller.storage,
                CirculationPatronProfileStorage,
            )

    def test_get(self, profile_fixture: ProfileFixture):
        """Verify that a patron can see their own profile."""
        with profile_fixture.request_context_with_library(
            "/", method="GET", headers=profile_fixture.auth
        ):
            patron = profile_fixture.controller.authenticated_patron_from_request()
            patron.synchronize_annotations = True
            response = profile_fixture.manager.profiles.protocol()
            assert "200 OK" == response.status
            data = json.loads(response.get_data(as_text=True))
            settings = data["settings"]
            assert True == settings[ProfileStorage.SYNCHRONIZE_ANNOTATIONS]

    def test_put(self, profile_fixture: ProfileFixture):
        """Verify that a patron can modify their own profile."""
        payload = {"settings": {ProfileStorage.SYNCHRONIZE_ANNOTATIONS: True}}

        request_patron = None
        identifier = profile_fixture.db.identifier()
        with profile_fixture.request_context_with_library(
            "/",
            method="PUT",
            headers=profile_fixture.auth,
            content_type=ProfileController.MEDIA_TYPE,
            data=json.dumps(payload),
        ):
            # By default, a patron has no value for synchronize_annotations.
            request_patron = (
                profile_fixture.controller.authenticated_patron_from_request()
            )
            assert None == request_patron.synchronize_annotations

            # This means we can't create annotations for them.
            pytest.raises(
                ValueError,
                Annotation.get_one_or_create,
                profile_fixture.db.session,
                patron=request_patron,
                identifier=identifier,
            )

            # But by sending a PUT request...
            response = profile_fixture.manager.profiles.protocol()

            # ...we can change synchronize_annotations to True.
            assert True == request_patron.synchronize_annotations

            # The other patron is unaffected.
            assert False == profile_fixture.other_patron.synchronize_annotations

        # Now we can create an annotation for the patron who enabled
        # annotation sync.
        annotation = Annotation.get_one_or_create(
            profile_fixture.db.session, patron=request_patron, identifier=identifier
        )
        assert 1 == len(request_patron.annotations)

        # But if we make another request and change their
        # synchronize_annotations field to False...
        payload["settings"][ProfileStorage.SYNCHRONIZE_ANNOTATIONS] = False
        with profile_fixture.request_context_with_library(
            "/",
            method="PUT",
            headers=profile_fixture.auth,
            content_type=ProfileController.MEDIA_TYPE,
            data=json.dumps(payload),
        ):
            response = profile_fixture.manager.profiles.protocol()

            # ...the annotation goes away.
            profile_fixture.db.session.commit()
            assert False == request_patron.synchronize_annotations
            assert 0 == len(request_patron.annotations)

    def test_problemdetail_on_error(self, profile_fixture: ProfileFixture):
        """Verify that an error results in a ProblemDetail being returned
        from the controller.
        """
        with profile_fixture.request_context_with_library(
            "/",
            method="PUT",
            headers=profile_fixture.auth,
            content_type="text/plain",
        ):
            response = profile_fixture.manager.profiles.protocol()
            assert isinstance(response, ProblemDetail)
            assert 415 == response.status_code
            assert "Expected vnd.librarysimplified/user-profile+json" == response.detail
