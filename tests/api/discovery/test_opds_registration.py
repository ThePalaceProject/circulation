import base64
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any
from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA
from requests_mock import Mocker

from api.config import Configuration
from api.discovery.opds_registration import OpdsRegistrationService
from api.discovery.registration_script import LibraryRegistrationScript
from api.problem_details import *
from core.model import ConfigurationSetting, Library, create, get_one
from core.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStage,
    RegistrationStatus,
)
from core.util.problem_detail import JSON_MEDIA_TYPE as PROBLEM_DETAIL_JSON_MEDIA_TYPE
from core.util.problem_detail import ProblemDetail, ProblemError
from tests.api.mockapi.circulation import MockCirculationManager
from tests.core.mock import MockRequestsResponse
from tests.fixtures.database import (
    DatabaseTransactionFixture,
    IntegrationConfigurationFixture,
)
from tests.fixtures.library import LibraryFixture


class RemoteRegistryFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        integration_configuration: IntegrationConfigurationFixture,
    ):
        self.db = db
        # Create an ExternalIntegration that can be used as the basis for
        # a OpdsRegistrationService.
        self.registry_url = "http://registry.com/"
        self.integration = integration_configuration.discovery_service(
            url=self.registry_url
        )
        assert self.integration.protocol is not None
        self.protocol = self.integration.protocol
        assert self.integration.goal is not None
        self.goal = self.integration.goal

        self.registry = OpdsRegistrationService.for_integration(
            db.session, self.integration
        )

    def create_registration(
        self, library: Library | None = None
    ) -> DiscoveryServiceRegistration:
        obj, _ = create(
            self.db.session,
            DiscoveryServiceRegistration,
            library=library or self.db.default_library(),
            integration=self.integration,
        )
        return obj


@pytest.fixture(scope="function")
def remote_registry_fixture(
    db: DatabaseTransactionFixture,
    create_integration_configuration: IntegrationConfigurationFixture,
) -> RemoteRegistryFixture:
    return RemoteRegistryFixture(db, create_integration_configuration)


class TestOpdsRegistrationService:
    def test_constructor(self):
        integration = MagicMock()
        settings = MagicMock()
        registry = OpdsRegistrationService(integration, settings)
        assert integration == registry.integration
        assert settings == registry.settings

    def test_for_integration(self, remote_registry_fixture: RemoteRegistryFixture):
        """Test the ability to build a Registry for an ExternalIntegration
        given its ID.
        """
        db = remote_registry_fixture.db
        m = OpdsRegistrationService.for_integration
        assert remote_registry_fixture.integration.id is not None
        registry = m(
            db.session,
            remote_registry_fixture.integration.id,
        )
        assert isinstance(registry, OpdsRegistrationService)
        assert remote_registry_fixture.integration == registry.integration

        # If the ID doesn't exist you get None.
        assert m(db.session, -1) is None

        # You can also pass in the IntegrationConfiguration object itself.
        registry = m(db.session, remote_registry_fixture.integration)
        assert isinstance(registry, OpdsRegistrationService)
        assert remote_registry_fixture.integration == registry.integration

    def test_for_protocol_goal_and_url(
        self, remote_registry_fixture: RemoteRegistryFixture
    ):
        db = remote_registry_fixture.db
        m = OpdsRegistrationService.for_protocol_goal_and_url

        registry = m(
            db.session,
            remote_registry_fixture.protocol,
            remote_registry_fixture.goal,
            remote_registry_fixture.registry_url,
        )
        assert isinstance(registry, OpdsRegistrationService)
        assert remote_registry_fixture.integration == registry.integration

        # If the ExternalIntegration doesn't exist, we get None.
        registry = m(
            db.session,
            remote_registry_fixture.protocol,
            remote_registry_fixture.goal,
            "http://registry2.com",
        )
        assert registry is None

    def test_registrations(self, remote_registry_fixture: RemoteRegistryFixture):
        db = remote_registry_fixture.db

        # Associate the default library with the registry.
        remote_registry_fixture.create_registration(db.default_library())

        # Create another library not associated with the registry.
        library2 = db.library()

        # registrations() finds a single Registration.
        [registration] = list(remote_registry_fixture.registry.registrations)
        assert isinstance(registration, DiscoveryServiceRegistration)
        assert db.default_library() == registration.library

    def test_fetch_catalog(
        self, remote_registry_fixture: RemoteRegistryFixture, requests_mock: Mocker
    ):
        # The behavior of fetch_catalog() depends on what comes back
        # when we ask the remote registry for its root catalog.
        requests_mock.get(remote_registry_fixture.registry_url, text="A root catalog")

        # Test our ability to retrieve essential information from a
        # remote registry's root catalog.
        func_mock = MagicMock(return_value="Essential information")
        remote_registry_fixture.registry._extract_catalog_information = func_mock

        # If the response looks good, it's passed into
        # _extract_catalog_information(), and the result of _that_
        # method is the return value of fetch_catalog.
        assert (
            "Essential information" == remote_registry_fixture.registry.fetch_catalog()
        )
        assert requests_mock.called_once
        assert requests_mock.last_request is not None
        assert requests_mock.last_request.url == remote_registry_fixture.registry_url
        assert remote_registry_fixture.registry._extract_catalog_information.called
        assert func_mock.call_args.args[0].text == "A root catalog"

    def test__extract_catalog_information(
        self, remote_registry_fixture: RemoteRegistryFixture
    ):
        # Test our ability to extract a registration link and an
        # Adobe Vendor ID from an OPDS 2 catalog.
        def mock_request(document, type=OpdsRegistrationService.OPDS_2_TYPE) -> Any:
            data = json.dumps(document) if isinstance(document, dict) else document
            return MockRequestsResponse(200, {"Content-Type": type}, data)

        m = OpdsRegistrationService._extract_catalog_information

        # OPDS 2 feed with link and Adobe Vendor ID.
        link = {"rel": "register", "href": "register url"}
        metadata = {"adobe_vendor_id": "vendorid"}
        request = mock_request(dict(links=[link], metadata=metadata))
        assert ("register url", "vendorid") == m(request)

        # OPDS 2 feed with link and no Adobe Vendor ID
        request = mock_request(dict(links=[link]))
        assert ("register url", None) == m(request)

        # OPDS 2 feed with no link.
        with pytest.raises(ProblemError) as excinfo:
            request = mock_request(dict(metadata=metadata))
            m(request)

        detail = excinfo.value.problem_detail
        assert detail.detail is not None
        assert (
            "The service at http://url/ did not provide a register link."
            in detail.detail
        )
        assert REMOTE_INTEGRATION_FAILED.uri == detail.uri

        # Non-OPDS document.
        with pytest.raises(ProblemError) as excinfo:
            request = mock_request("plain text here", "text/plain")
            m(request)

        detail = excinfo.value.problem_detail
        assert detail.detail is not None
        assert "The service at http://url/ did not return OPDS." in detail.detail
        assert REMOTE_INTEGRATION_FAILED.uri == detail.uri

    def test_fetch_registration_document_error_catalog(
        self, remote_registry_fixture: RemoteRegistryFixture
    ):
        # Test our ability to retrieve terms-of-service information
        # from a remote registry, assuming the registry makes that
        # information available.

        # First, test the case where we can't even get the catalog
        # document.
        remote_registry_fixture.registry.fetch_catalog = MagicMock(
            side_effect=ProblemError(problem_detail=REMOTE_INTEGRATION_FAILED)
        )
        with pytest.raises(ProblemError) as excinfo:
            remote_registry_fixture.registry.fetch_registration_document()

        # The fetch_catalog method raised a ProblemError,
        # which propagated up to fetch_registration_document.
        assert REMOTE_INTEGRATION_FAILED == excinfo.value.problem_detail
        remote_registry_fixture.registry.fetch_catalog.assert_called_once()

    def test_fetch_registration_document_error_registration_document(
        self, remote_registry_fixture: RemoteRegistryFixture, requests_mock: Mocker
    ):
        # Test the case where we get the catalog document, but we can't
        # get the registration document.
        requests_mock.get(
            "http://register-here/",
            status_code=REMOTE_INTEGRATION_FAILED.status_code,  # type: ignore[arg-type]
            headers={"Content-Type": PROBLEM_DETAIL_JSON_MEDIA_TYPE},
            text=REMOTE_INTEGRATION_FAILED.response[0],
        )
        remote_registry_fixture.registry.fetch_catalog = MagicMock(
            return_value=("http://register-here/", "vendor id")
        )

        with pytest.raises(ProblemError) as excinfo:
            remote_registry_fixture.registry.fetch_registration_document()

        # A request was made to the registration URL mentioned in the catalog.
        assert requests_mock.called_once
        assert requests_mock.last_request is not None
        assert "http://register-here/" == requests_mock.last_request.url

        # But the request returned a problem detail, which became a ProblemError
        assert REMOTE_INTEGRATION_FAILED.uri == excinfo.value.problem_detail.uri
        assert excinfo.value.problem_detail.detail is not None
        assert (
            str(REMOTE_INTEGRATION_FAILED.detail) in excinfo.value.problem_detail.detail
        )
        assert "Remote service returned" in excinfo.value.problem_detail.detail

    def test_fetch_registration_document(
        self, remote_registry_fixture: RemoteRegistryFixture, requests_mock: Mocker
    ):
        # Finally, test the case where we can get both documents.
        remote_registry_fixture.registry.fetch_catalog = MagicMock(
            return_value=("http://register-here/", "vendor id")
        )
        remote_registry_fixture.registry._extract_registration_information = MagicMock(
            return_value=("TOS link", "TOS HTML data")
        )

        requests_mock.get("http://register-here/", text="a registration document")
        result = remote_registry_fixture.registry.fetch_registration_document()

        # Another request was made to the registration URL.
        assert requests_mock.called_once
        assert requests_mock.last_request is not None
        assert "http://register-here/" == requests_mock.last_request.url
        remote_registry_fixture.registry.fetch_catalog.assert_called_once()

        # Our mock of _extract_registration_information was called
        # with the mock response to that request.
        remote_registry_fixture.registry._extract_registration_information.assert_called_once()
        assert (
            remote_registry_fixture.registry._extract_registration_information.call_args.args[
                0
            ].text
            == "a registration document"
        )

        # The return value of _extract_registration_information was
        # propagated as the return value of
        # fetch_registration_document.
        assert ("TOS link", "TOS HTML data") == result

    def test__extract_registration_information(
        self, remote_registry_fixture: RemoteRegistryFixture
    ):
        # Test our ability to extract terms-of-service information --
        # a link and/or some HTML or textual instructions -- from a
        # registration document.

        def data_link(data, type="text/html"):
            encoded = base64.b64encode(data.encode("utf-8")).decode("utf-8")
            return dict(rel="terms-of-service", href=f"data:{type};base64,{encoded}")

        class Mock(OpdsRegistrationService):
            decoded: str

            @classmethod
            def _decode_data_url(cls, url):
                cls.decoded = url
                return "Decoded: " + OpdsRegistrationService._decode_data_url(url)

        def extract(document, type=OpdsRegistrationService.OPDS_2_TYPE):
            if type == OpdsRegistrationService.OPDS_2_TYPE:
                document = json.dumps(dict(links=document))
            response = MockRequestsResponse(200, {"Content-Type": type}, document)
            return Mock._extract_registration_information(response)

        # OPDS 2 feed with TOS in http: and data: links.
        tos_link = dict(rel="terms-of-service", href="http://tos/")
        tos_data = data_link("<p>Some HTML</p>")
        assert ("http://tos/", "Decoded: <p>Some HTML</p>") == extract(
            [tos_link, tos_data]
        )

        # At this point it's clear that the data: URL found in
        # `tos_data` was run through `_decode_data()`. This gives us
        # permission to test all the fiddly bits of `_decode_data` in
        # isolation, below.
        assert tos_data["href"] == Mock.decoded

        # OPDS 2 feed with http: link only.
        assert ("http://tos/", None) == extract([tos_link])

        # OPDS 2 feed with data: link only.
        assert (None, "Decoded: <p>Some HTML</p>") == extract([tos_data])

        # OPDS 2 feed with no links.
        assert (None, None) == extract([])

        # Non-OPDS document.
        assert (None, None) == extract("plain text here", "text/plain")

        # Unrecognized URI schemes are ignored.
        ftp_link = dict(rel="terms-of-service", href="ftp://tos/")
        assert (None, None) == extract([ftp_link])

    def test__decode_data_url(self, remote_registry_fixture: RemoteRegistryFixture):
        # Test edge cases of decoding data: URLs.
        m = OpdsRegistrationService._decode_data_url

        def data_url(data, type="text/html"):
            encoded = base64.b64encode(data.encode("utf-8")).decode("utf-8")
            return f"data:{type};base64,{encoded}"

        # HTML is okay.
        html = data_url("some <strong>HTML</strong>", "text/html;charset=utf-8")
        assert "some <strong>HTML</strong>" == m(html)

        # Plain text is okay.
        text = data_url("some plain text", "text/plain")
        assert "some plain text" == m(text)

        # No other media type is allowed.
        image = data_url("an image!", "image/png")
        with pytest.raises(ValueError) as excinfo:
            m(image)
        assert "Unsupported media type in data: URL: image/png" in str(excinfo.value)

        # Incoming HTML is sanitized.
        dirty_html = data_url("<script>alert!</script><p>Some HTML</p>")
        assert "<p>Some HTML</p>" == m(dirty_html)

        # Now test various malformed data: URLs.
        no_header = "foobar"
        with pytest.raises(ValueError) as excinfo:
            m(no_header)
        assert "Not a data: URL: foobar" in str(excinfo.value)

        no_comma = "data:blah"
        with pytest.raises(ValueError) as excinfo:
            m(no_comma)
        assert "Invalid data: URL: data:blah" in str(excinfo.value)

        too_many_commas = "data:blah,blah,blah"
        with pytest.raises(ValueError) as excinfo:
            m(too_many_commas)
        assert "Invalid data: URL: data:blah,blah,blah" in str(excinfo.value)

        # data: URLs don't have to be base64-encoded, but those are the
        # only kind we support.
        not_encoded = "data:blah,content"
        with pytest.raises(ValueError) as excinfo:
            m(not_encoded)
        assert "data: URL not base64-encoded: data:blah,content" in str(excinfo.value)

    def test_register_library(
        self,
        remote_registry_fixture: RemoteRegistryFixture,
        library_fixture: LibraryFixture,
    ):
        db = remote_registry_fixture.db

        # Test the other methods orchestrated by the register_library() method.
        registry = remote_registry_fixture.registry
        registry.fetch_catalog = MagicMock(return_value=("register_url", "vendor_id"))
        registry._create_registration_payload = MagicMock(
            return_value={"payload": "this is it"}
        )
        registry._create_registration_headers = MagicMock(
            return_value=dict(Header="Value")
        )
        registry._send_registration_request = MagicMock(
            return_value=MockRequestsResponse(200, content=json.dumps("you did it!"))
        )
        registry._process_registration_result = MagicMock(return_value=True)

        library = library_fixture.library()
        stage = RegistrationStage.TESTING
        url_for = MagicMock()

        register_library = partial(registry.register_library, library, stage, url_for)

        # Kick off the registration process, and make sure we get expected return.
        result = register_library()
        assert result is True

        # But there were many steps towards this result.

        # First, fetch_catalog() was called, in an attempt
        # to find the registration URL inside the root catalog.
        registry.fetch_catalog.assert_called_once()

        # fetch_catalog() returned a registration URL and
        # a vendor ID. The registration URL was used later on...
        #
        # The vendor ID was set on the registration in the database.
        registration = get_one(
            db.session, DiscoveryServiceRegistration, library=library
        )
        assert registration is not None
        assert "vendor_id" == registration.vendor_id

        # _create_registration_payload was called to create the body
        # of the registration request.
        registry._create_registration_payload.assert_called_once_with(
            library, stage, url_for
        )

        # _create_registration_headers was called to create the headers
        # sent along with the request.
        registry._create_registration_headers.assert_called_once()

        # Then _send_registration_request was called, POSTing the
        # payload to "register_url", the registration URL we got earlier.
        registry._send_registration_request.assert_called_once_with(
            "register_url", {"Header": "Value"}, dict(payload="this is it")
        )

        # Finally, the return value of that method was loaded as JSON
        # and passed into _process_registration_result, along with
        # a cipher created from the private key. (That cipher would be used
        # to decrypt anything the foreign site signed using this site's
        # public key.)
        registry._process_registration_result.assert_called_once()
        (
            actual_registration,
            message,
            cipher,
            actual_stage,
        ) = registry._process_registration_result.call_args.args
        assert registration == actual_registration
        assert "you did it!" == message
        assert cipher._key.export_key("DER") == library.private_key
        assert actual_stage == stage

        # Now in reverse order, let's replace the mocked methods so
        # that they raise ProblemError exceptions. This tests that if
        # there is a failure at any stage, the ProblemError is
        # propagated.
        def create_exception(message: str) -> ProblemError:
            return ProblemError(problem_detail=INVALID_REGISTRATION.detailed(message))

        registry._process_registration_result = MagicMock(
            side_effect=create_exception("could not process registration result")
        )
        with pytest.raises(ProblemError) as excinfo:
            register_library()
        assert (
            "could not process registration result"
            == excinfo.value.problem_detail.detail
        )

        registry._send_registration_request = MagicMock(
            side_effect=create_exception("could not send registration request")
        )
        with pytest.raises(ProblemError) as excinfo:
            register_library()
        assert (
            "could not send registration request" == excinfo.value.problem_detail.detail
        )

        registry._create_registration_payload = MagicMock(
            side_effect=create_exception("could not create registration payload")
        )
        with pytest.raises(ProblemError) as excinfo:
            register_library()
        assert (
            "could not create registration payload"
            == excinfo.value.problem_detail.detail
        )

        registry.fetch_catalog = MagicMock(
            side_effect=create_exception("could not fetch catalog")
        )
        with pytest.raises(ProblemError) as excinfo:
            register_library()
        assert "could not fetch catalog" == excinfo.value.problem_detail.detail

    def test__create_registration_payload(
        self,
        remote_registry_fixture: RemoteRegistryFixture,
        library_fixture: LibraryFixture,
    ):
        m = remote_registry_fixture.registry._create_registration_payload

        # Mock url_for to create good-looking callback URLs.
        def url_for(controller, library_short_name, **kwargs):
            return f"http://server/{library_short_name}/{controller}"

        # First, test with no configuration contact configured for the
        # library.
        library = library_fixture.library()
        stage = RegistrationStage.PRODUCTION
        expect_url = url_for(
            "authentication_document",
            library.short_name,
        )
        expect_payload = dict(url=expect_url, stage=stage.value)
        assert expect_payload == m(library, stage, url_for)

        # If a contact is configured, it shows up in the payload.
        contact = "mailto:ohno@library.org"
        settings = library_fixture.settings(library)
        settings.configuration_contact_email_address = contact  # type: ignore[assignment]
        expect_payload["contact"] = contact
        assert expect_payload == m(library, stage, url_for)

    def test_create_registration_headers(
        self, remote_registry_fixture: RemoteRegistryFixture
    ):
        db = remote_registry_fixture.db
        m = remote_registry_fixture.registry._create_registration_headers

        # If no shared secret is configured, no custom headers are provided.
        registration = remote_registry_fixture.create_registration()
        assert {} == m(registration)

        # If a shared secret is configured, it shows up as part of
        # the Authorization header.
        registration.shared_secret = "a secret"
        assert {"Authorization": "Bearer a secret"} == m(registration)

    def test__send_registration_request(
        self, remote_registry_fixture: RemoteRegistryFixture, requests_mock: Mocker
    ):
        # If everything goes well, the return value of do_post is
        # passed through.
        url = "http://url.com"
        requests_mock.post(url, text="all good")
        payload = {"payload": "payload"}
        headers = {"headers": ""}
        m = remote_registry_fixture.registry._send_registration_request

        result = m(url, headers, payload)
        assert "all good" == result.text

        # Error handling is expected to be handled by post_request
        # raising a ProblemError exception.

        # The remote sends a 401 response with a problem detail.
        requests_mock.post(
            url,
            status_code=401,
            headers={"Content-Type": PROBLEM_DETAIL_JSON_MEDIA_TYPE},
            text=json.dumps(dict(detail="this is a problem detail")),
        )
        with pytest.raises(ProblemError) as excinfo:
            m(url, headers, payload)
        assert REMOTE_INTEGRATION_FAILED.uri == excinfo.value.problem_detail.uri
        assert excinfo.value.problem_detail.detail is not None
        assert (
            'Remote service returned a problem detail document: \'{"detail": "this is a problem detail"}\''
            in excinfo.value.problem_detail.detail
        )

        # The remote sends some other kind of 401 response.
        requests_mock.post(
            url,
            status_code=401,
            headers={"Content-Type": "text/html"},
            text="log in why don't you",
        )
        with pytest.raises(ProblemError) as excinfo:
            m(url, headers, payload)

        assert REMOTE_INTEGRATION_FAILED.uri == excinfo.value.problem_detail.uri
        assert (
            '401 response from integration server: "log in why don\'t you"'
            == excinfo.value.problem_detail.detail
        )

    def test__decrypt_shared_secret(
        self, remote_registry_fixture: RemoteRegistryFixture
    ):
        key = RSA.generate(2048)
        encryptor = PKCS1_OAEP.new(key)

        key2 = RSA.generate(2048)
        encryptor2 = PKCS1_OAEP.new(key2)

        shared_secret = os.urandom(24)
        encrypted_secret = base64.b64encode(encryptor.encrypt(shared_secret)).decode(
            "utf-8"
        )

        # Success.
        m = remote_registry_fixture.registry._decrypt_shared_secret
        assert shared_secret == m(encryptor, encrypted_secret)

        # If we try to decrypt using the wrong key, a ProblemError is
        # raised explaining the problem.
        with pytest.raises(ProblemError) as excinfo:
            m(encryptor2, encrypted_secret)

        assert SHARED_SECRET_DECRYPTION_ERROR.uri == excinfo.value.problem_detail.uri
        assert excinfo.value.problem_detail.detail is not None
        assert encrypted_secret in excinfo.value.problem_detail.detail

    def test__process_registration_result(
        self, remote_registry_fixture: RemoteRegistryFixture, monkeypatch: MonkeyPatch
    ):
        db = remote_registry_fixture.db
        m = remote_registry_fixture.registry._process_registration_result
        stage = RegistrationStage.TESTING
        encryptor = MagicMock()

        reg = MagicMock(spec=DiscoveryServiceRegistration)

        # Result must be a dictionary.
        with pytest.raises(ProblemError) as excinfo:
            m(reg, "not a dictionary", encryptor, stage)

        problem = excinfo.value.problem_detail
        assert INTEGRATION_ERROR.uri == problem.uri
        assert (
            "Remote service served 'not a dictionary', which I can't make sense of as an OPDS document."
            == problem.detail
        )

        # When the result is empty, the registration is marked as successful.
        result = m(reg, dict(), encryptor, stage)
        assert result is True
        reg.status = RegistrationStatus.SUCCESS

        # The stage field has been set to the requested value.
        reg.stage = stage

        # Now try with a result that includes a short name,
        # a shared secret, and a web client URL.
        mock = MagicMock(return_value="👉 cleartext 👈".encode())
        monkeypatch.setattr(OpdsRegistrationService, "_decrypt_shared_secret", mock)

        catalog = dict(
            metadata=dict(short_name="SHORT", shared_secret="ciphertext", id="uuid"),
            links=[dict(href="http://web/library", rel="self", type="text/html")],
        )
        result = m(reg, catalog, encryptor, RegistrationStage.PRODUCTION)
        assert result is True

        # Short name is set.
        assert reg.short_name == "SHORT"

        # Shared secret was decrypted, decoded from UTF-8 and is set.
        mock.assert_called_once_with(encryptor, "ciphertext")
        assert reg.shared_secret == "👉 cleartext 👈"

        # Web client URL is set.
        assert reg.web_client == "http://web/library"

        assert reg.stage == RegistrationStage.PRODUCTION

        # Now simulate a problem decrypting the shared secret.
        mock.side_effect = ProblemError(problem_detail=SHARED_SECRET_DECRYPTION_ERROR)
        with pytest.raises(ProblemError) as excinfo:
            m(reg, catalog, encryptor, stage)

        assert SHARED_SECRET_DECRYPTION_ERROR == excinfo.value.problem_detail


class TestLibraryRegistrationScript:
    def test_do_run(
        self,
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
        remote_registry_fixture: RemoteRegistryFixture,
    ):
        @dataclass
        class Processed:
            registry: OpdsRegistrationService
            library: Library
            stage: RegistrationStage
            url_for: Callable[..., str]

        class Mock(LibraryRegistrationScript):
            processed: list[Processed] = []

            def process_library(  # type: ignore[override]
                self,
                registry: OpdsRegistrationService,
                library: Library,
                stage: RegistrationStage,
                url_for: Callable[..., str],
            ):
                self.processed.append(Processed(registry, library, stage, url_for))

        script = Mock(db.session)

        base_url_setting = ConfigurationSetting.sitewide(
            db.session, Configuration.BASE_URL_KEY
        )
        base_url_setting.value = "http://test-circulation-manager/"

        library = library_fixture.library()
        library2 = library_fixture.library()

        cmd_args = [
            str(library.short_name),
            "--stage=testing",
            "--registry-url=http://registry.com/",
        ]
        manager = MockCirculationManager(db.session, MagicMock())
        script.do_run(cmd_args=cmd_args, manager=manager)

        # One library was processed.
        processed = script.processed.pop()
        assert [] == script.processed
        assert library == processed.library
        assert RegistrationStage.TESTING == processed.stage

        # Let's say the other library was earlier registered in production.
        registration = remote_registry_fixture.create_registration(library2)
        registration.stage = RegistrationStage.PRODUCTION

        # Now run the script again without specifying a particular
        # library or the --stage argument.
        script.do_run(cmd_args=["--registry-url=http://registry.com/"], manager=manager)

        # Every library was processed.
        assert {library, library2} == {x.library for x in script.processed}

        # Since no stage was provided, each library was registered
        # using the stage already associated with it.
        assert {RegistrationStage.TESTING, RegistrationStage.PRODUCTION} == {
            x.stage for x in script.processed
        }

        # Every library was registered with the specified registry.
        assert {"http://registry.com/", "http://registry.com/"} == {
            x.registry.settings.url for x in script.processed
        }

    def test_process_library(
        self,
        db: DatabaseTransactionFixture,
        remote_registry_fixture: RemoteRegistryFixture,
        library_fixture: LibraryFixture,
    ):
        """Test the things that might happen when process_library is called."""
        script = LibraryRegistrationScript(db.session)
        library = library_fixture.library()
        registry = remote_registry_fixture.registry

        # First, simulate success.
        registry.register_library = MagicMock(return_value=True)
        stage = MagicMock()
        url_for = MagicMock()
        assert script.process_library(registry, library, stage, url_for) is True

        # The stage and url_for values were passed into register_library()
        registry.register_library.assert_called_once_with(library, stage, url_for)

        # Next, simulate an exception raised during register_library()
        # This can happen in real situations, though the next case
        # we'll test is more common.
        registry.register_library = MagicMock(side_effect=Exception("boo"))

        # We get False rather than the exception being propagated.
        # Useful information about the exception is added to the logs,
        # where someone actually running the script will see it.
        assert script.process_library(registry, library, stage, url_for) is False

        # Next, simulate register_library() returning a problem detail document.
        registry.register_library = MagicMock(
            side_effect=ProblemError(problem_detail=INVALID_INPUT.detailed("oops"))
        )

        result = script.process_library(registry, library, stage, url_for)

        # The problem document is returned. Useful information about
        # the exception is also added to the logs, where someone
        # actually running the script will see it.
        assert isinstance(result, ProblemDetail)
        assert INVALID_INPUT.uri == result.uri
        assert "oops" == result.detail
