from palace.manager.core.entrypoint import AudiobooksEntryPoint, EbooksEntryPoint
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.feed.facets.base import FacetsWithEntryPoint
from palace.manager.feed.facets.feed import Facets
from palace.manager.search.filter import Filter
from palace.manager.sqlalchemy.model.edition import Edition


class TestFacetsWithEntryPoint:
    class MockFacetConfig:
        """Pass this in when you call FacetsWithEntryPoint.from_request
        but you don't care which EntryPoints are configured.
        """

        entrypoints: list = []

    def test_items(self):
        ep = AudiobooksEntryPoint
        f = FacetsWithEntryPoint(ep)
        expect_items = (f.ENTRY_POINT_FACET_GROUP_NAME, ep.INTERNAL_NAME)
        assert [expect_items] == list(f.items())
        assert "%s=%s" % expect_items == f.query_string

        expect_items = [
            (f.ENTRY_POINT_FACET_GROUP_NAME, ep.INTERNAL_NAME),
        ]
        assert expect_items == list(f.items())

    def test_modify_database_query(self):
        class MockEntryPoint:
            def modify_database_query(self, _db, qu):
                self.called_with = (_db, qu)

        ep = MockEntryPoint()
        f = FacetsWithEntryPoint(ep)
        _db = object()
        qu = object()
        f.modify_database_query(_db, qu)
        assert (_db, qu) == ep.called_with

    def test_navigate(self):
        # navigate creates a new FacetsWithEntryPoint.

        old_entrypoint = object()
        kwargs = dict(extra_key="extra_value")
        facets = FacetsWithEntryPoint(
            old_entrypoint, entrypoint_is_default=True, **kwargs
        )
        new_entrypoint = object()
        new_facets = facets.navigate(new_entrypoint)

        # A new FacetsWithEntryPoint was created.
        assert isinstance(new_facets, FacetsWithEntryPoint)

        # It has the new entry point.
        assert new_entrypoint == new_facets.entrypoint

        # Since navigating from one Facets object to another is a choice,
        # the new Facets object is not using a default EntryPoint.
        assert False == new_facets.entrypoint_is_default

        # The keyword arguments used to create the original faceting
        # object were propagated to its constructor.
        assert kwargs == new_facets.constructor_kwargs

    def test_from_request(self):
        # from_request just calls the _from_request class method
        expect = object()

        class Mock(FacetsWithEntryPoint):
            @classmethod
            def _from_request(cls, *args, **kwargs):
                cls.called_with = (args, kwargs)
                return expect

        result = Mock.from_request(
            "library",
            "facet config",
            "get_argument",
            "get_header",
            "worklist",
            "default entrypoint",
            extra="extra argument",
        )

        # The arguments given to from_request were propagated to _from_request.
        args, kwargs = Mock.called_with
        assert (
            "facet config",
            "get_argument",
            "get_header",
            "worklist",
            "default entrypoint",
        ) == args
        assert dict(extra="extra argument") == kwargs

        # The return value of _from_request was propagated through
        # from_request.
        assert expect == result

    def test__from_request(self):
        # _from_request calls load_entrypoint() and instantiates
        # the class with the result.

        class MockFacetsWithEntryPoint(FacetsWithEntryPoint):
            # Mock load_entrypoint() to
            # return whatever values we have set up ahead of time.

            @classmethod
            def selectable_entrypoints(cls, facet_config):
                cls.selectable_entrypoints_called_with = facet_config
                return ["Selectable entrypoints"]

            @classmethod
            def load_entrypoint(cls, entrypoint_name, entrypoints, default=None):
                cls.load_entrypoint_called_with = (
                    entrypoint_name,
                    entrypoints,
                    default,
                )
                return cls.expect_load_entrypoint

        # Mock the functions that pull information out of an HTTP
        # request.

        # EntryPoint.load_entrypoint pulls the facet group name and
        # the maximum cache age out of the 'request' and passes those
        # values into load_entrypoint()
        def get_argument(key, default):
            if key == Facets.ENTRY_POINT_FACET_GROUP_NAME:
                return "entrypoint name from request"

        # FacetsWithEntryPoint.load_entrypoint does not use
        # get_header().
        def get_header(name):
            raise Exception("I'll never be called")

        config = self.MockFacetConfig
        mock_worklist = object()
        default_entrypoint = object()

        def m():
            return MockFacetsWithEntryPoint._from_request(
                config,
                get_argument,
                get_header,
                mock_worklist,
                default_entrypoint=default_entrypoint,
                extra="extra kwarg",
            )

        # First, test failure. If load_entrypoint() returns a
        # ProblemDetail, that object is returned instead of the
        # faceting class.
        MockFacetsWithEntryPoint.expect_load_entrypoint = INVALID_INPUT
        assert INVALID_INPUT == m()

        expect_entrypoint = object()
        expect_is_default = object()
        MockFacetsWithEntryPoint.expect_load_entrypoint = (
            expect_entrypoint,
            expect_is_default,
        )

        # Next, test success. The return value of load_entrypoint() is
        # is passed as 'entrypoint' into the FacetsWithEntryPoint
        # constructor.
        #
        # The object returned by load_entrypoint() does not need to be a
        # currently enabled entrypoint for the library.
        facets = m()
        assert isinstance(facets, FacetsWithEntryPoint)
        assert expect_entrypoint == facets.entrypoint
        assert expect_is_default == facets.entrypoint_is_default
        assert (
            "entrypoint name from request",
            ["Selectable entrypoints"],
            default_entrypoint,
        ) == MockFacetsWithEntryPoint.load_entrypoint_called_with
        assert dict(extra="extra kwarg") == facets.constructor_kwargs
        assert MockFacetsWithEntryPoint.selectable_entrypoints_called_with == config

    def test_load_entrypoint(self):
        audio = AudiobooksEntryPoint
        ebooks = EbooksEntryPoint

        # These are the allowable entrypoints for this site -- we'll
        # be passing this in to load_entrypoint every time.
        entrypoints = [audio, ebooks]

        worklist = object()
        m = FacetsWithEntryPoint.load_entrypoint

        # This request does not ask for any particular entrypoint, and
        # it doesn't specify a default, so it gets the first available
        # entrypoint.
        audio_default, is_default = m(None, entrypoints)
        assert audio == audio_default
        assert True == is_default

        # This request does not ask for any particular entrypoint, so
        # it gets the specified default.
        default = object()
        assert (default, True) == m(None, entrypoints, default)

        # This request asks for an entrypoint and gets it.
        assert (ebooks, False) == m(ebooks.INTERNAL_NAME, entrypoints)

        # This request asks for an entrypoint that is not available,
        # and gets the default.
        assert (audio, True) == m("no such entrypoint", entrypoints)

        # If no EntryPoints are available, load_entrypoint returns
        # nothing.
        assert (None, True) == m(audio.INTERNAL_NAME, [])

    def test_selectable_entrypoints(self):
        """The default implementation of selectable_entrypoints just returns
        the worklist's entrypoints.
        """

        class MockWorkList:
            def __init__(self, entrypoints):
                self.entrypoints = entrypoints

        mock_entrypoints = object()
        worklist = MockWorkList(mock_entrypoints)

        m = FacetsWithEntryPoint.selectable_entrypoints
        assert mock_entrypoints == m(worklist)
        assert [] == m(None)

    def test_modify_search_filter(self):
        # When an entry point is selected, search filters are modified so
        # that they only find works that fit that entry point.
        filter = Filter()
        facets = FacetsWithEntryPoint(AudiobooksEntryPoint)
        facets.modify_search_filter(filter)
        assert [Edition.AUDIO_MEDIUM] == filter.media

        # If no entry point is selected, the filter is not modified.
        filter = Filter()
        facets = FacetsWithEntryPoint()
        facets.modify_search_filter(filter)
        assert None == filter.media
