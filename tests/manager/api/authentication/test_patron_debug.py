"""Tests for patron debug authentication mixin."""

from abc import ABC

from palace.manager.api.authentication.base import PatronAuthResult
from palace.manager.api.authentication.patron_debug import HasPatronDebug


class TestHasPatronDebug:
    def test_is_abstract(self):
        """HasPatronDebug is abstract and cannot be instantiated directly."""
        assert issubclass(HasPatronDebug, ABC)

    def test_implementer_must_define_patron_debug(self):
        """A concrete implementer must provide patron_debug()."""

        class ConcreteDebug(HasPatronDebug):
            def patron_debug(self, username, password=None):
                return [PatronAuthResult(label="Step 1", success=True, details="ok")]

        impl = ConcreteDebug()
        results = impl.patron_debug("user123", "pass456")
        assert len(results) == 1
        assert results[0].label == "Step 1"
        assert results[0].success is True
