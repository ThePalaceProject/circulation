from tests.api.admin.fixtures.dummy_validator import (
    DummyAuthenticationProviderValidator,
)


def validator_factory():
    return DummyAuthenticationProviderValidator()
