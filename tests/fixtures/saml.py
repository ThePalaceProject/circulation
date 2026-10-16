"""Builders for SAML responses used in tests.

These are plain helpers, not pytest fixtures.
"""

import base64
import datetime
from dataclasses import dataclass

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from onelogin.saml2.utils import OneLogin_Saml2_Utils

from palace.util.datetime_helpers import utc_now

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


@dataclass(frozen=True)
class SAMLSigningKey:
    """A key pair that an IdP in a test signs its responses with.

    :param private_key: PEM-encoded private key
    :param certificate: Base64-encoded DER certificate, as IdP metadata holds it
    """

    private_key: str
    certificate: str


def create_saml_signing_key() -> SAMLSigningKey:
    """Returns a new self-signed key pair."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "idp.example.com")])
    now = utc_now()
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    return SAMLSigningKey(
        private_key=key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ).decode(),
        certificate=base64.b64encode(
            certificate.public_bytes(serialization.Encoding.DER)
        ).decode(),
    )


def encode_signed_saml_response(
    issuer: str,
    destination: str,
    audience: str,
    signing_key: SAMLSigningKey,
) -> str:
    """Returns a base64-encoded SAML response with a signed assertion.

    The response is valid now, and has no InResponseTo, as an IdP-initiated
    response has.

    :param issuer: Issuer of the response and its assertion
    :param destination: URL that the response is sent to
    :param audience: Entity ID of the service provider that the assertion is for
    :param signing_key: Key pair that signs the assertion
    """
    now = utc_now()
    issue_instant = OneLogin_Saml2_Utils.parse_time_to_SAML(now.timestamp())
    not_before = OneLogin_Saml2_Utils.parse_time_to_SAML(
        (now - datetime.timedelta(minutes=5)).timestamp()
    )
    not_on_or_after = OneLogin_Saml2_Utils.parse_time_to_SAML(
        (now + datetime.timedelta(hours=1)).timestamp()
    )
    assertion = (
        '<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" '
        f'ID="assertion" Version="2.0" IssueInstant="{issue_instant}">'
        f"<saml:Issuer>{issuer}</saml:Issuer>"
        "<saml:Subject>"
        "<saml:NameID>patron</saml:NameID>"
        '<saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">'
        f'<saml:SubjectConfirmationData NotOnOrAfter="{not_on_or_after}" '
        f'Recipient="{destination}"/>'
        "</saml:SubjectConfirmation>"
        "</saml:Subject>"
        f'<saml:Conditions NotBefore="{not_before}" NotOnOrAfter="{not_on_or_after}">'
        "<saml:AudienceRestriction>"
        f"<saml:Audience>{audience}</saml:Audience>"
        "</saml:AudienceRestriction>"
        "</saml:Conditions>"
        f'<saml:AuthnStatement AuthnInstant="{issue_instant}" SessionIndex="session">'
        "<saml:AuthnContext>"
        "<saml:AuthnContextClassRef>"
        "urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport"
        "</saml:AuthnContextClassRef>"
        "</saml:AuthnContext>"
        "</saml:AuthnStatement>"
        "<saml:AttributeStatement>"
        '<saml:Attribute Name="uid"><saml:AttributeValue>patron</saml:AttributeValue>'
        "</saml:Attribute>"
        "</saml:AttributeStatement>"
        "</saml:Assertion>"
    )
    signed_assertion = OneLogin_Saml2_Utils.add_sign(
        assertion,
        signing_key.private_key,
        OneLogin_Saml2_Utils.format_cert(signing_key.certificate),
    ).decode()
    xml = (
        '<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" '
        'xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" '
        f'ID="response" Version="2.0" IssueInstant="{issue_instant}" '
        f'Destination="{destination}">'
        f"<saml:Issuer>{issuer}</saml:Issuer>"
        "<samlp:Status>"
        '<samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>'
        "</samlp:Status>"
        f"{signed_assertion}"
        "</samlp:Response>"
    )
    return base64.b64encode(xml.encode()).decode()
