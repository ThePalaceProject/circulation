import datetime
from unittest.mock import MagicMock, patch

import flask
import pytest
from bidict import frozenbidict
from flask import Response
from werkzeug.datastructures import Authorization

from palace.manager.api.circulation.exceptions import RemoteInitiatedServerError
from palace.manager.api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    FORBIDDEN_BY_POLICY,
    LIBRARY_NOT_FOUND,
    NO_LICENSES,
    NO_SUCH_LANE,
    NOT_AGE_APPROPRIATE,
    REMOTE_INTEGRATION_FAILED,
)
from palace.manager.core.classifier import Classifier
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePoolDeliveryMechanism,
    LicensePoolType,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.sqlalchemy.util import create, tuple_to_numericrange
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.library import LibraryFixture


class TestBaseController:
    def test_unscoped_session(self, circulation_fixture: CirculationControllerFixture):
        """Compare to TestScopedSession.test_scoped_session to see
        how database sessions will be handled in production.
        """
        # Both requests used the circulation_fixture.db.session session used by most unit tests.
        with circulation_fixture.request_context_with_library("/"):
            response1 = circulation_fixture.manager.index_controller()
            assert circulation_fixture.app.manager._db == circulation_fixture.db.session

        with circulation_fixture.request_context_with_library("/"):
            response2 = circulation_fixture.manager.index_controller()
            assert circulation_fixture.app.manager._db == circulation_fixture.db.session

    def test_request_patron(self, circulation_fixture: CirculationControllerFixture):
        # Test the method that finds the currently authenticated patron
        # for the current request, triggering the authentication process
        # if necessary.

        # If flask.request.patron is present, whatever value is in
        # there is returned.
        o1 = object()
        with circulation_fixture.app.test_request_context("/"):
            flask.request.patron = o1  # type: ignore
            assert o1 == circulation_fixture.controller.request_patron

        # If not, authenticated_patron_from_request is called; it's
        # supposed to set flask.request.patron.
        o2 = object()

        def set_patron():
            flask.request.patron = o2

        mock = MagicMock(
            side_effect=set_patron, return_value="return value will be ignored"
        )
        circulation_fixture.controller.authenticated_patron_from_request = mock
        with circulation_fixture.app.test_request_context("/"):
            assert o2 == circulation_fixture.controller.request_patron

    def test_authenticated_patron_from_request(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # Test the method that attempts to authenticate a patron
        # for the current request.

        # First, test success.
        with circulation_fixture.request_context_with_library(
            "/", headers=dict(Authorization=circulation_fixture.valid_auth)
        ):
            result = circulation_fixture.controller.authenticated_patron_from_request()
            assert result == circulation_fixture.default_patron
            assert (
                getattr(flask.request, "patron") == circulation_fixture.default_patron
            )

        # No authorization header -> 401 error.
        with (
            patch.object(
                circulation_fixture.controller,
                "authorization_header",
            ) as mock_auth_header,
            circulation_fixture.request_context_with_library("/"),
        ):
            mock_auth_header.return_value = None
            result = circulation_fixture.controller.authenticated_patron_from_request()
            assert isinstance(result, Response)
            assert result.status_code == 401
            assert getattr(flask.request, "patron") is None

        with (
            patch.object(
                circulation_fixture.manager.auth,
                "authenticated_patron",
            ) as mock_auth_patron,
            circulation_fixture.request_context_with_library(
                "/", headers=dict(Authorization=circulation_fixture.valid_auth)
            ),
        ):
            # Exception contacting the authentication authority -> ProblemDetail
            mock_auth_patron.side_effect = RemoteInitiatedServerError("argh", "service")
            result = circulation_fixture.controller.authenticated_patron_from_request()
            assert isinstance(result, ProblemDetail)
            assert result.uri == REMOTE_INTEGRATION_FAILED.uri
            assert result.detail == "Error in authentication service"
            assert getattr(flask.request, "patron") is None

            # Any other ProblemDetailException -> ProblemDetail
            pd = ProblemDetail("uri")
            mock_auth_patron.side_effect = ProblemDetailException(pd)
            result = circulation_fixture.controller.authenticated_patron_from_request()
            assert result is pd
            assert getattr(flask.request, "patron") is None

            # Credentials provided but don't identify anyone in particular
            # -> 401 error.
            mock_auth_patron.side_effect = None
            mock_auth_patron.return_value = None
            result = circulation_fixture.controller.authenticated_patron_from_request()
            assert isinstance(result, ProblemDetail)
            assert result.status_code == 401
            assert getattr(flask.request, "patron") is None

    def test_authenticated_patron_invalid_credentials(
        self, circulation_fixture: CirculationControllerFixture
    ):
        from palace.manager.api.problem_details import INVALID_CREDENTIALS

        with circulation_fixture.request_context_with_library("/"):
            value = circulation_fixture.controller.authenticated_patron(
                Authorization(
                    auth_type="basic", data=dict(username="user1", password="password2")
                )
            )
            assert value == INVALID_CREDENTIALS

    def test_authenticated_patron_can_authenticate_with_expired_credentials(
        self, circulation_fixture: CirculationControllerFixture
    ):
        """A patron can authenticate even if their credentials have
        expired -- they just can't create loans or holds.
        """
        one_year_ago = utc_now() - datetime.timedelta(days=365)
        with circulation_fixture.request_context_with_library("/"):
            patron = circulation_fixture.controller.authenticated_patron(
                circulation_fixture.valid_credentials
            )
            patron.expires = one_year_ago

            patron = circulation_fixture.controller.authenticated_patron(
                circulation_fixture.valid_credentials
            )
            assert one_year_ago == patron.expires

    def test_authenticated_patron_correct_credentials(
        self, circulation_fixture: CirculationControllerFixture
    ):
        with circulation_fixture.request_context_with_library("/"):
            value = circulation_fixture.controller.authenticated_patron(
                circulation_fixture.valid_credentials
            )
            assert isinstance(value, Patron)

    def test_authentication_sends_proper_headers(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # Make sure the realm header has quotes around the realm name.
        # Without quotes, some iOS versions don't recognize the header value.

        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.controller.authenticate()
            assert response.headers["WWW-Authenticate"] == 'Basic realm="Library card"'

        with circulation_fixture.request_context_with_library(
            "/", headers={"X-Requested-With": "XMLHttpRequest"}
        ):
            response = circulation_fixture.controller.authenticate()
            assert None == response.headers.get("WWW-Authenticate")

    @pytest.mark.parametrize(
        "is_inactive",
        (
            pytest.param(True, id="inactive collection"),
            pytest.param(False, id="active collection"),
        ),
    )
    def test_load_licensepools(
        self, circulation_fixture: CirculationControllerFixture, is_inactive: bool
    ):
        # Here's a Library that has two Collections.
        library = circulation_fixture.library
        assert len(library.associated_collections) == 2
        c1 = (
            circulation_fixture.db.default_inactive_collection()
            if is_inactive
            else circulation_fixture.db.default_collection()
        )
        c2 = circulation_fixture.db.collection()
        c2.associated_libraries.append(library)

        # Here's a Collection not affiliated with any Library.
        c3 = circulation_fixture.db.collection()

        # All three Collections have LicensePools for this Identifier,
        # from various sources.
        i1 = circulation_fixture.db.identifier()
        e1, lp1 = circulation_fixture.db.edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=i1.type,
            identifier_id=i1.identifier,
            with_license_pool=True,
            collection=c1,
        )
        e2, lp2 = circulation_fixture.db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=i1.type,
            identifier_id=i1.identifier,
            with_license_pool=True,
            collection=c2,
        )
        e3, lp3 = circulation_fixture.db.edition(
            data_source_name=DataSource.BIBLIOTHECA,
            identifier_type=i1.type,
            identifier_id=i1.identifier,
            with_license_pool=True,
            collection=c3,
        )

        # The first collection also has a LicensePool for a totally
        # different Identifier.
        e4, lp4 = circulation_fixture.db.edition(
            data_source_name=DataSource.GUTENBERG, with_license_pool=True, collection=c1
        )

        # Same for the third collection
        e5, lp5 = circulation_fixture.db.edition(
            data_source_name=DataSource.GUTENBERG, with_license_pool=True, collection=c3
        )

        # Now let's try to load LicensePools for the first Identifier
        # from the default Library.
        loaded = circulation_fixture.controller.load_licensepools(
            circulation_fixture.db.default_library(), i1.type, i1.identifier
        )
        assert not isinstance(loaded, ProblemDetail)

        # Two LicensePools were loaded: the LicensePool for the first
        # Identifier in Collection 1, and the LicensePool for the same
        # identifier in Collection 2.
        assert lp1 in loaded
        assert lp2 in loaded
        assert 2 == len(loaded)
        assert all([lp.identifier == i1 for lp in loaded])

        # Note that the LicensePool in c3 was not loaded, even though
        # the Identifier matches, because that collection is not
        # associated with this Library.

        # LicensePool l4 was not loaded, even though it's in a Collection
        # that matches, because the Identifier doesn't match.

        # Let's test loading a license pool using a deprecated identifier type.
        with patch.object(
            Identifier,
            "DEPRECATED_NAMES",
            frozenbidict({"deprecated": Identifier.GUTENBERG_ID}),
        ):
            loaded = circulation_fixture.controller.load_licensepools(
                circulation_fixture.db.default_library(), "deprecated", i1.identifier
            )
        assert not isinstance(loaded, ProblemDetail)
        assert set(loaded) == {lp1, lp2}

        # Now we test various failures.

        # Try a totally bogus identifier.
        problem_detail = circulation_fixture.controller.load_licensepools(
            circulation_fixture.db.default_library(),
            "bad identifier type",
            i1.identifier,
        )
        assert isinstance(problem_detail, ProblemDetail)
        assert NO_LICENSES.uri == problem_detail.uri
        expect = (
            "The item you're asking about (bad identifier type/%s) isn't in this collection."
            % i1.identifier
        )
        assert expect == problem_detail.detail

        # Try an identifier that would work except that it's not in a
        # Collection associated with the given Library.
        problem_detail = circulation_fixture.controller.load_licensepools(
            circulation_fixture.db.default_library(),
            lp5.identifier.type,
            lp5.identifier.identifier,
        )
        assert isinstance(problem_detail, ProblemDetail)
        assert NO_LICENSES.uri == problem_detail.uri

    def test_load_work(self, circulation_fixture: CirculationControllerFixture):
        # Create a Work with two LicensePools.
        work = circulation_fixture.db.work(with_license_pool=True)
        [pool1] = work.license_pools
        pool2 = circulation_fixture.db.licensepool(None)
        work.license_pools.append(pool2)

        # Either identifier suffices to identify the Work.
        for i in [pool1.identifier, pool2.identifier]:
            with circulation_fixture.request_context_with_library("/"):
                assert work == circulation_fixture.controller.load_work(
                    circulation_fixture.db.default_library(), i.type, i.identifier
                )

        # If a patron is authenticated, the requested Work must be
        # age-appropriate for that patron, or this method will return
        # a problem detail.
        headers = dict(Authorization=circulation_fixture.valid_auth)
        for retval, expect in ((True, work), (False, NOT_AGE_APPROPRIATE)):
            work.age_appropriate_for_patron = MagicMock(return_value=retval)
            with circulation_fixture.request_context_with_library("/", headers=headers):
                assert expect == circulation_fixture.controller.load_work(
                    circulation_fixture.db.default_library(),
                    pool1.identifier.type,
                    pool1.identifier.identifier,
                )
                work.age_appropriate_for_patron.assert_called_once_with(
                    circulation_fixture.default_patron
                )

    def test_load_licensepooldelivery(
        self, circulation_fixture: CirculationControllerFixture
    ):
        licensepool = circulation_fixture.db.licensepool(
            edition=None, with_open_access_download=True
        )

        # Set a delivery mechanism that we won't be looking up, so we
        # can demonstrate that we find the right match thanks to more
        # than random chance.
        licensepool.set_delivery_mechanism(
            Representation.MOBI_MEDIA_TYPE, None, None, None
        )

        # If there is one matching delivery mechanism that matches the
        # request, we load it.
        lpdm = licensepool.delivery_mechanisms[0]
        delivery = circulation_fixture.controller.load_licensepooldelivery(
            licensepool, lpdm.delivery_mechanism.id
        )
        assert lpdm == delivery

        # If there are multiple matching delivery mechanisms (that is,
        # multiple ways of getting a book with the same media type and
        # DRM scheme) we pick one arbitrarily.
        new_lpdm, is_new = create(
            circulation_fixture.db.session,
            LicensePoolDeliveryMechanism,
            identifier=licensepool.identifier,
            data_source=licensepool.data_source,
            delivery_mechanism=lpdm.delivery_mechanism,
        )
        assert True == is_new

        assert new_lpdm.delivery_mechanism == lpdm.delivery_mechanism
        underlying_mechanism = lpdm.delivery_mechanism

        delivery = circulation_fixture.controller.load_licensepooldelivery(
            licensepool, lpdm.delivery_mechanism.id
        )
        assert not isinstance(delivery, ProblemDetail)

        # We don't know which LicensePoolDeliveryMechanism this is,
        # but we know it's one of the matches.
        assert underlying_mechanism == delivery.delivery_mechanism

        # If there is no matching delivery mechanism, we return a
        # problem detail.
        adobe_licensepool = circulation_fixture.db.licensepool(
            edition=None, with_open_access_download=False
        )
        problem_detail = circulation_fixture.controller.load_licensepooldelivery(
            adobe_licensepool, lpdm.delivery_mechanism.id
        )
        assert isinstance(problem_detail, ProblemDetail)
        assert BAD_DELIVERY_MECHANISM.uri == problem_detail.uri

    def test_apply_borrowing_policy_succeeds_for_unlimited_access_books(
        self, circulation_fixture: CirculationControllerFixture
    ):
        with circulation_fixture.request_context_with_library("/"):
            # Arrange
            patron = circulation_fixture.controller.authenticated_patron(
                circulation_fixture.valid_credentials
            )
            work = circulation_fixture.db.work(
                with_license_pool=True, with_open_access_download=False
            )
            [pool] = work.license_pools
            pool.open_access = False
            pool.type = LicensePoolType.UNLIMITED

            # Act
            problem = circulation_fixture.controller.apply_borrowing_policy(
                patron, pool
            )

            # Assert
            assert problem is None

    def test_apply_borrowing_policy_when_holds_prohibited(
        self,
        circulation_fixture: CirculationControllerFixture,
        library_fixture: LibraryFixture,
    ):
        with circulation_fixture.request_context_with_library("/"):
            patron = circulation_fixture.controller.authenticated_patron(
                circulation_fixture.valid_credentials
            )
            # This library does not allow holds.
            library = circulation_fixture.db.default_library()
            library_fixture.settings(library).allow_holds = False

            # This is an open-access work.
            work = circulation_fixture.db.work(
                with_license_pool=True, with_open_access_download=True
            )
            [pool] = work.license_pools
            pool.licenses_available = 0
            pool.licenses_owned = 0
            assert pool.type is LicensePoolType.UNLIMITED
            assert True == pool.open_access

            # It can still be borrowed even though it has no
            # 'licenses' available.
            problem = circulation_fixture.controller.apply_borrowing_policy(
                patron, pool
            )
            assert None == problem

            # If it weren't an unlimited-access work, there'd be a big
            # problem.
            pool.type = LicensePoolType.METERED
            problem = circulation_fixture.controller.apply_borrowing_policy(
                patron, pool
            )
            assert isinstance(problem, ProblemDetail)
            assert FORBIDDEN_BY_POLICY.uri == problem.uri

    def test_apply_borrowing_policy_for_age_inappropriate_book(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # apply_borrowing_policy() prevents patrons from checking out
        # books that are not age-appropriate.

        # Set up lanes for different patron types.
        children_lane = circulation_fixture.db.lane()
        children_lane.audiences = [
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_YOUNG_ADULT,
        ]
        children_lane.target_age = tuple_to_numericrange((9, 12))
        children_lane.root_for_patron_type = ["child"]

        adults_lane = circulation_fixture.db.lane()
        adults_lane.audiences = [Classifier.AUDIENCE_ADULT]
        adults_lane.root_for_patron_type = ["adult"]

        # This book is age-appropriate for anyone 13 years old or older.
        work = circulation_fixture.db.work(with_license_pool=True)
        work.audience = Classifier.AUDIENCE_CHILDREN
        work.target_age = tuple_to_numericrange((13, 15))
        [pool] = work.license_pools

        with circulation_fixture.request_context_with_library("/"):
            patron = circulation_fixture.controller.authenticated_patron(
                circulation_fixture.valid_credentials
            )
            # This patron is restricted to a lane in which the 13-year-old
            # book would not appear.
            patron.external_type = "child"

            # Therefore the book is not age-appropriate for the patron.
            problem = circulation_fixture.controller.apply_borrowing_policy(
                patron, pool
            )
            assert isinstance(problem, ProblemDetail)
            assert FORBIDDEN_BY_POLICY.uri == problem.uri

            # If the lane is expanded to allow the book's age range, there's
            # no problem.
            children_lane.target_age = tuple_to_numericrange((9, 13))
            assert None == circulation_fixture.controller.apply_borrowing_policy(
                patron, pool
            )

            # Similarly if the patron has an external type
            # corresponding to a root lane in which the given book
            # _is_ age-appropriate.
            children_lane.target_age = tuple_to_numericrange((9, 12))
            patron.external_type = "adult"
            assert None == circulation_fixture.controller.apply_borrowing_policy(
                patron, pool
            )

    def test_library_for_request(
        self, circulation_fixture: CirculationControllerFixture
    ):
        with circulation_fixture.app.test_request_context("/"):
            value = circulation_fixture.controller.library_for_request("not-a-library")
            assert LIBRARY_NOT_FOUND == value

        with circulation_fixture.app.test_request_context("/") as ctx:
            value = circulation_fixture.controller.library_for_request(
                circulation_fixture.db.default_library().short_name
            )
            assert circulation_fixture.db.default_library() == value
            assert circulation_fixture.db.default_library() == getattr(
                ctx.request, "library"
            )

        # If you don't specify a library, the default library is used.
        with circulation_fixture.app.test_request_context("/") as ctx:
            value = circulation_fixture.controller.library_for_request(None)
            expect_default = Library.default(circulation_fixture.db.session)
            assert expect_default == value
            assert expect_default == getattr(ctx.request, "library")

    def test_load_lane(self, circulation_fixture: CirculationControllerFixture):
        # Verify that requests for specific lanes are mapped to
        # the appropriate lane.

        # TODO: The case where the top-level lane is a WorkList rather
        # than a Lane is not tested.

        lanes = circulation_fixture.db.default_library().lanes

        with circulation_fixture.request_context_with_library("/"):
            top_level = circulation_fixture.controller.load_lane(None)
            expect = circulation_fixture.controller.manager.top_level_lanes[
                circulation_fixture.db.default_library().id
            ]

            # expect and top_level are different ORM objects
            # representing the same lane. (They're different objects
            # because the lane stored across requests inside the
            # CirculationManager object was merged into the request's
            # database session.)
            assert isinstance(top_level, Lane)
            assert expect.id == top_level.id

            # A lane can be looked up by ID.
            for l in lanes:
                found = circulation_fixture.controller.load_lane(l.id)
                assert l == found

            # If a lane cannot be looked up by ID, a problem detail
            # is returned.
            for bad_id in ("nosuchlane", -1):
                not_found = circulation_fixture.controller.load_lane(bad_id)  # type: ignore[arg-type]
                assert isinstance(not_found, ProblemDetail)
                assert not_found.uri == NO_SUCH_LANE.uri
                assert (
                    "Lane %s does not exist or is not associated with library %s"
                    % (bad_id, circulation_fixture.db.default_library().id)
                    == not_found.detail
                )

        # If the requested lane exists but is not visible to the
        # authenticated patron, the server _acts_ like the lane does
        # not exist.

        # Any lane will do here.
        lane = lanes[0]

        # Mock Lane.accessible_to so that it always returns
        # false.
        lane.accessible_to = MagicMock(return_value=False)
        headers = dict(Authorization=circulation_fixture.valid_auth)
        with circulation_fixture.request_context_with_library(
            "/", headers=headers, library=circulation_fixture.db.default_library()
        ):
            # The lane exists, but visible_to says it's not
            # visible to the authenticated patron, so the controller
            # denies it exists.
            result = circulation_fixture.controller.load_lane(lane.id)
            assert isinstance(result, ProblemDetail)
            assert result.uri == NO_SUCH_LANE.uri
            lane.accessible_to.assert_called_once_with(
                circulation_fixture.default_patron
            )
