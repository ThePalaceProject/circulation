"""Builders for SAML responses used in tests.

These are plain helpers, not pytest fixtures.
"""

import base64

# A KeyInfo naming a key by retrieval method, without the attributes OneLogin
# reads, so decryption fails with a KeyError.
KEY_INFO_RETRIEVAL_METHOD_WITHOUT_TYPE = "<ds:RetrievalMethod/>"

# A KeyInfo naming a key, with cipher data that cannot be decrypted, so
# decryption fails in xmlsec.
KEY_INFO_KEY_NAME = "<ds:KeyName>key</ds:KeyName>"


def encode_saml_response(
    response_issuer: str | None = None,
    assertion_issuer: str | None = None,
    encrypted_assertion_key_info: str | None = None,
) -> str:
    """Returns a base64-encoded, unsigned SAML response.

    :param response_issuer: Issuer of the response, if any
    :param assertion_issuer: Issuer of a plain assertion, if any
    :param encrypted_assertion_key_info: KeyInfo content of an undecryptable
        encrypted assertion, if one should be included
    """
    issuer = f"<saml:Issuer>{response_issuer}</saml:Issuer>" if response_issuer else ""
    assertion = (
        '<saml:Assertion ID="assertion" Version="2.0" IssueInstant="2026-01-01T00:00:00Z">'
        f"<saml:Issuer>{assertion_issuer}</saml:Issuer>"
        "<saml:Subject><saml:NameID>patron</saml:NameID></saml:Subject>"
        "</saml:Assertion>"
        if assertion_issuer
        else ""
    )
    encrypted_assertion = (
        "<saml:EncryptedAssertion>"
        '<xenc:EncryptedData xmlns:xenc="http://www.w3.org/2001/04/xmlenc#">'
        '<ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">'
        f"{encrypted_assertion_key_info}"
        "</ds:KeyInfo>"
        "<xenc:CipherData><xenc:CipherValue>AAAA</xenc:CipherValue></xenc:CipherData>"
        "</xenc:EncryptedData>"
        "</saml:EncryptedAssertion>"
        if encrypted_assertion_key_info is not None
        else ""
    )
    xml = (
        '<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" '
        'xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" '
        'ID="response" Version="2.0" IssueInstant="2026-01-01T00:00:00Z">'
        f"{issuer}{assertion}{encrypted_assertion}</samlp:Response>"
    )
    return base64.b64encode(xml.encode()).decode()
