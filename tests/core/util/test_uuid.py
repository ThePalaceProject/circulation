from uuid import UUID

import pytest

from core.util.uuid import uuid_decode, uuid_encode


@pytest.mark.parametrize(
    "uuid,expected",
    [
        ("804184d9-ac4f-4cd3-8ad0-a362d71a7431", "gEGE2axPTNOK0KNi1xp0MQ"),
        ("e34f3186-c563-4211-a52a-3a866b214963", "408xhsVjQhGlKjqGayFJYw"),
        ("c4b0e2a0-9e4a-4b0e-8f4e-2d6d9d5a8a1e", "xLDioJ5KSw6PTi1tnVqKHg"),
        ("55ff6224-8ced-41f8-9fb2-eda74657ff56", "Vf9iJIztQfifsu2nRlf_Vg"),
    ],
)
def test_uuid_encode_decode(uuid: str, expected: str):
    # Create a UUID object from the string
    uuid_obj = UUID(uuid)

    # Test that we can encode the uuid and get the expected result
    encoded = uuid_encode(uuid_obj)
    assert len(encoded) == 22
    assert encoded == expected

    # Test that we can round-trip the encoded string back to a UUID
    decoded = uuid_decode(encoded)
    assert isinstance(decoded, UUID)
    assert str(decoded) == uuid
    assert decoded == uuid_obj


def test_uuid_decode_error():
    # Invalid length
    with pytest.raises(ValueError):
        uuid_decode("gE")

    # Invalid characters
    with pytest.raises(ValueError):
        uuid_decode("/~")
