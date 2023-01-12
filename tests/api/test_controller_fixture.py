from tests.fixtures.api_controller import ControllerFixture


class TestControllerFixture:
    """Check that multiple tests can be executed with the controller fixture."""

    def test_fixture_0(self, controller_fixture: ControllerFixture):
        pass

    def test_fixture_1(self, controller_fixture: ControllerFixture):
        pass

    def test_fixture_2(self, controller_fixture: ControllerFixture):
        pass
