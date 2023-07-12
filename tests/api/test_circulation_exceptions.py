from flask_babel import lazy_gettext as _

from api.circulation_exceptions import *
from api.problem_details import *
from core.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture


class TestCirculationExceptions:
    def test_as_problem_detail_document(self):
        """Verify that circulation exceptions can be turned into ProblemDetail
        documents.
        """

        e = RemoteInitiatedServerError("message", "some service")
        doc = e.as_problem_detail_document()
        assert "Integration error communicating with some service" == doc.detail

        e = AuthorizationExpired()
        assert EXPIRED_CREDENTIALS == e.as_problem_detail_document()

        e = AuthorizationBlocked()
        assert BLOCKED_CREDENTIALS == e.as_problem_detail_document()

        e = PatronHoldLimitReached()
        assert HOLD_LIMIT_REACHED == e.as_problem_detail_document()

        e = NoLicenses()
        assert NO_LICENSES == e.as_problem_detail_document()


class TestLimitReached:
    """Test LimitReached, which may send different messages depending on the value of a
    library ConfigurationSetting.
    """

    def test_as_problem_detail_document(self, db: DatabaseTransactionFixture):
        generic_message = _(
            "You exceeded the limit, but I don't know what the limit was."
        )
        pd = ProblemDetail("http://uri/", 403, _("Limit exceeded."), generic_message)

        class Mock(LimitReached):
            BASE_DOC = pd
            MESSAGE_WITH_LIMIT = _("The limit was %(limit)d.")

        # No limit -> generic message.
        ex = Mock()
        pd = ex.as_problem_detail_document()
        assert ex.limit is None
        assert generic_message == pd.detail

        # Limit -> specific message.
        ex = Mock(limit=14)
        assert 14 == ex.limit
        pd = ex.as_problem_detail_document()
        assert "The limit was 14." == pd.detail
