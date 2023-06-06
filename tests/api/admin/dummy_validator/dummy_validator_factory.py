from tests.api.admin.dummy_validator.dummy_validator import (
    DummyAuthenticationProviderValidator,
)


def validator_factory():
    return DummyAuthenticationProviderValidator()
