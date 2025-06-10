from unittest.mock import patch

from palace.manager.scripts.saml import UpdateSamlMetadata


def test_saml_update_script():

    with patch(
        "palace.manager.scripts.saml.update_saml_federation_idps_metadata"
    ) as update:
        UpdateSamlMetadata().run()
        assert update.delay.call_count == 1
