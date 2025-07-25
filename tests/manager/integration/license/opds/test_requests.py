from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import nullcontext
from datetime import timedelta
from functools import partial
from typing import Any
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.core.exceptions import IntegrationException, PalaceValueError
from palace.manager.integration.license.opds.requests import (
    BaseOpdsHttpRequest,
    BasicAuthOpdsRequest,
    NoAuthOpdsRequest,
    OAuthOpdsRequest,
    OPDS2AuthType,
    get_opds_requests,
)
from tests.fixtures.http import MockHttpClientFixture
from tests.mocks.mock import MockRequestsResponse


class OpdsRequestFixture:
    def __init__(self, http_client: MockHttpClientFixture) -> None:
        self.username = "username"
        self.password = "password"
        self.token = "token"
        self.feed_url = "http://example.com/feed"
        self.auth_url = "http://authenticate.example.com"
        self.request_url = "http://example.com/123"
        self.headers = {"header": "value"}
        self.client = http_client

        self.get_opds_requests = partial(
            get_opds_requests,
            feed_url=self.feed_url,
            username=self.username,
            password=self.password,
        )
        oauth_make_request = self.get_opds_requests(OPDS2AuthType.OAUTH)
        assert isinstance(oauth_make_request, OAuthOpdsRequest)
        self.oauth_make_request = oauth_make_request
        no_auth_make_request = self.get_opds_requests(OPDS2AuthType.NONE)
        assert isinstance(no_auth_make_request, NoAuthOpdsRequest)
        self.no_auth_make_request = no_auth_make_request
        basic_auth_make_request = self.get_opds_requests(OPDS2AuthType.BASIC)
        assert isinstance(basic_auth_make_request, BasicAuthOpdsRequest)
        self.basic_auth_make_request = basic_auth_make_request

        self.responses = {
            "auth_document_401": MockRequestsResponse(
                401,
                {"Content-Type": "application/vnd.opds.authentication.v1.0+json"},
                json.dumps(self.auth_document),
            ),
            "other_401": MockRequestsResponse(
                401,
                {"Content-Type": "text/plain"},
                "Unauthorized",
            ),
            "token_grant": MockRequestsResponse(200, {}, json.dumps(self.token_grant)),
            "data": MockRequestsResponse(200, {}, "Data"),
        }

        self.valid_token = OAuthTokenResponse(
            access_token=self.token,
            expires_in=3600,
            token_type="Bearer",
        )
        with freeze_time(timedelta(seconds=-3600)):
            self.expired_token = OAuthTokenResponse(
                access_token="expired_token",
                expires_in=50,
                token_type="Bearer",
            )

    def initialize_oauth_token(self, *, expired: bool = False) -> OAuthOpdsRequest:
        # Set the token url and session token so that oauth_make_request can make requests
        # without first going through the refresh process
        make_request = self.oauth_make_request
        make_request._token_url = self.auth_url
        make_request.session_token = (
            self.valid_token if not expired else self.expired_token
        )
        return make_request

    @property
    def auth_document(self) -> dict[str, Any]:
        return {
            "id": "http://example.com/auth",
            "title": "Authentication Document",
            "authentication": [
                {
                    "type": "http://opds-spec.org/auth/oauth/client_credentials",
                    "links": [
                        {
                            "rel": "authenticate",
                            "href": self.auth_url,
                        },
                    ],
                }
            ],
        }

    @property
    def token_grant(self) -> dict[str, Any]:
        return {
            "access_token": self.token,
            "token_type": "Bearer",
            "expires_in": 3600,
        }


@pytest.fixture
def opds_request_fixture(
    http_client: MockHttpClientFixture,
) -> Generator[OpdsRequestFixture]:
    yield OpdsRequestFixture(http_client)


class TestNoAuthOpdsRequest:
    def test_request(self, opds_request_fixture: OpdsRequestFixture) -> None:
        make_request = opds_request_fixture.no_auth_make_request
        opds_request_fixture.client.queue_response(
            opds_request_fixture.responses["data"]
        )
        response = make_request(
            "GET",
            opds_request_fixture.request_url,
            headers=opds_request_fixture.headers,
        )
        assert response.status_code == 200
        assert response.text == "Data"
        assert opds_request_fixture.client.requests == [
            opds_request_fixture.request_url
        ]
        assert opds_request_fixture.client.requests_methods == ["GET"]
        assert opds_request_fixture.client.requests_args == [
            {
                "headers": opds_request_fixture.headers,
            }
        ]

    def test_request_with_parser(
        self, opds_request_fixture: OpdsRequestFixture
    ) -> None:
        make_request = opds_request_fixture.no_auth_make_request
        parser = MagicMock()
        opds_request_fixture.client.queue_response(
            opds_request_fixture.responses["data"]
        )
        response = make_request(
            "GET",
            opds_request_fixture.request_url,
            headers=opds_request_fixture.headers,
            parser=parser,
        )
        assert response == parser.return_value
        parser.assert_called_once_with(opds_request_fixture.responses["data"].content)
        assert opds_request_fixture.client.requests == [
            opds_request_fixture.request_url
        ]
        assert opds_request_fixture.client.requests_methods == ["GET"]
        assert opds_request_fixture.client.requests_args == [
            {
                "headers": opds_request_fixture.headers,
            }
        ]


class TestBasicAuthOpdsRequest:
    def test_request(self, opds_request_fixture: OpdsRequestFixture) -> None:
        make_request = opds_request_fixture.basic_auth_make_request
        opds_request_fixture.client.queue_response(
            opds_request_fixture.responses["data"]
        )
        response = make_request(
            "GET",
            opds_request_fixture.request_url,
            headers=opds_request_fixture.headers,
        )
        assert response.status_code == 200
        assert response.text == "Data"
        assert opds_request_fixture.client.requests == [
            opds_request_fixture.request_url
        ]
        assert opds_request_fixture.client.requests_methods == ["GET"]
        assert opds_request_fixture.client.requests_args == [
            {
                "headers": opds_request_fixture.headers,
                "auth": (
                    opds_request_fixture.username,
                    opds_request_fixture.password,
                ),
            }
        ]


class TestOAuthOpdsRequest:
    @pytest.mark.parametrize(
        "authentication,expected",
        [
            pytest.param(
                [
                    {
                        "type": "http://opds-spec.org/auth/oauth/client_credentials",
                        "links": [
                            {
                                "rel": "authenticate",
                                "href": "http://authenticate.example.com",
                            },
                        ],
                    },
                ],
                "http://authenticate.example.com",
                id="valid",
            ),
            pytest.param(
                [
                    {
                        "type": "http://opds-spec.org/auth/oauth/client_credentials",
                        "links": [
                            {
                                "rel": "authenticate",
                                "href": "http://authenticate.example.com",
                            },
                        ],
                    },
                    {
                        "type": "http://opds-spec.org/auth/basic",
                        "links": [
                            {
                                "rel": "authenticate",
                                "href": "http://authenticate2.example.com",
                            },
                        ],
                    },
                ],
                "http://authenticate.example.com",
                id="multiple different",
            ),
            pytest.param(
                [
                    {
                        "type": "http://opds-spec.org/auth/oauth/client_credentials",
                        "links": [
                            {
                                "rel": "authenticate",
                                "href": "http://authenticate.example.com",
                            },
                        ],
                    },
                    {
                        "type": "http://opds-spec.org/auth/oauth/client_credentials",
                        "links": [
                            {
                                "rel": "authenticate",
                                "href": "http://authenticate3.example.com",
                            },
                        ],
                    },
                ],
                IntegrationException,
                id="multiple same",
            ),
            pytest.param([], IntegrationException, id="empty"),
            pytest.param(
                [
                    {
                        "type": "http://opds-spec.org/auth/oauth/client_credentials",
                    },
                ],
                IntegrationException,
                id="missing links",
            ),
            pytest.param(
                [
                    {
                        "type": "http://opds-spec.org/auth/oauth/client_credentials",
                        "links": [],
                    },
                ],
                IntegrationException,
                id="empty links",
            ),
            pytest.param(
                [
                    {
                        "type": "http://opds-spec.org/auth/oauth/client_credentials",
                        "links": [
                            {
                                "rel": "authenticate",
                                "href": "http://authenticate.example.com",
                            },
                            {
                                "rel": "authenticate",
                                "href": "http://authenticate2.example.com",
                            },
                        ],
                    },
                ],
                IntegrationException,
                id="multiple links",
            ),
        ],
    )
    def test__get_oauth_url_from_auth_document(
        self,
        opds_request_fixture: OpdsRequestFixture,
        authentication: list[dict[str, Any]],
        expected: type[Exception] | str,
    ) -> None:
        auth_document = opds_request_fixture.auth_document
        auth_document["authentication"] = authentication
        context = (
            nullcontext() if isinstance(expected, str) else pytest.raises(expected)
        )

        with context:
            assert (
                OAuthOpdsRequest._get_oauth_url_from_auth_document(
                    json.dumps(auth_document)
                )
                == expected
            )

    @pytest.mark.parametrize(
        "data,expected",
        [
            ("{}", IntegrationException),
            ('{"access_token":"token"}', IntegrationException),
            ('{"token_type":"Bearer"}', IntegrationException),
            ('{"expires_in":3600}', IntegrationException),
            (
                '{"access_token":"token", "token_type":"invalid", "expires_in":3600}',
                IntegrationException,
            ),
            (
                '{"access_token":"token", "token_type":"Bearer", "expires_in":-320}',
                IntegrationException,
            ),
            ('{"access_token":"token","token_type":"Bearer"}', IntegrationException),
            (
                '{"access_token":"token","token_type":"Bearer","expires_in":3600}',
                OAuthTokenResponse(
                    access_token="token",
                    expires_in=3600,
                    token_type="Bearer",
                ),
            ),
        ],
    )
    @freeze_time("2021-01-01")
    def test__oauth_session_token_refresh(
        self,
        opds_request_fixture: OpdsRequestFixture,
        data: str,
        expected: OAuthTokenResponse | type[Exception],
    ) -> None:
        opds_request_fixture.client.queue_response(200, content=data)
        context = (
            nullcontext()
            if isinstance(expected, OAuthTokenResponse)
            else pytest.raises(expected)
        )

        with context:
            token = OAuthOpdsRequest._oauth_session_token_refresh(
                opds_request_fixture.auth_url,
                opds_request_fixture.username,
                opds_request_fixture.password,
            )
            assert token.access_token == expected.access_token
            assert token.token_type == expected.token_type
            assert token.expires_in == expected.expires_in
        assert opds_request_fixture.client.requests == [opds_request_fixture.auth_url]
        assert opds_request_fixture.client.requests_methods == ["POST"]
        assert opds_request_fixture.client.requests_args == [
            {
                "headers": {"Content-Type": "application/x-www-form-urlencoded"},
                "data": {"grant_type": "client_credentials"},
                "auth": (
                    opds_request_fixture.username,
                    opds_request_fixture.password,
                ),
                "allowed_response_codes": ["2xx"],
            }
        ]

    def test__fetch_auth_document_failure(
        self, opds_request_fixture: OpdsRequestFixture
    ) -> None:
        """
        If the auth document request fails, an exception is raised.
        """
        make_request = OAuthOpdsRequest(
            opds_request_fixture.feed_url,
            opds_request_fixture.username,
            opds_request_fixture.password,
        )
        opds_request_fixture.client.queue_response(
            401,
            content="Unauthorized",
        )
        with pytest.raises(
            IntegrationException, match="Unable to fetch OPDS authentication document"
        ) as exc_info:
            make_request(
                "GET",
                opds_request_fixture.request_url,
                headers=opds_request_fixture.headers,
            )

    @pytest.mark.parametrize(
        "responses,requests,initialized,expired",
        [
            pytest.param(
                ["auth_document_401", "token_grant", "data"],
                [
                    "feed_url",
                    "auth_url",
                    "request_url",
                ],
                False,
                False,
                id="first request - full token refresh",
            ),
            pytest.param(
                ["data"],
                ["request_url"],
                True,
                False,
                id="second request - token already initialized - directly make request",
            ),
            pytest.param(
                ["token_grant", "data"],
                [
                    "auth_url",
                    "request_url",
                ],
                True,
                True,
                id="expired token - do refresh with already known url",
            ),
            pytest.param(
                ["other_401", "token_grant", "other_401"],
                [
                    "request_url",
                    "auth_url",
                    "request_url",
                ],
                True,
                False,
                id="unexpected 401 - refresh and try again - still 401 response",
            ),
            pytest.param(
                ["auth_document_401", "token_grant", "data"],
                [
                    "request_url",
                    "auth_url",
                    "request_url",
                ],
                True,
                False,
                id="unexpected 401 - refresh and try again",
            ),
        ],
    )
    def test_request(
        self,
        opds_request_fixture: OpdsRequestFixture,
        responses: list[str],
        requests: list[str],
        initialized: bool,
        expired: bool,
    ) -> None:
        if initialized:
            make_request = opds_request_fixture.initialize_oauth_token(expired=expired)
        else:
            make_request = opds_request_fixture.oauth_make_request
        final_response = None
        for response_name in responses:
            response = opds_request_fixture.responses[response_name]
            opds_request_fixture.client.queue_response(response)
            final_response = response

        assert (
            make_request(
                "GET",
                opds_request_fixture.request_url,
                headers=opds_request_fixture.headers,
            )
            == final_response
        )
        assert opds_request_fixture.client.requests == [
            getattr(opds_request_fixture, r) for r in requests
        ]

    def test__oauth_request_allowed_response_codes(
        self, opds_request_fixture: OpdsRequestFixture
    ) -> None:
        """
        Calling with allowed_response_codes should still allow a token refresh, but if the refresh fails an
        exception will be raised.
        """
        make_request = opds_request_fixture.initialize_oauth_token()
        http_client = opds_request_fixture.client

        http_client.queue_response(opds_request_fixture.responses["other_401"])
        http_client.queue_response(opds_request_fixture.responses["token_grant"])
        http_client.queue_response(opds_request_fixture.responses["other_401"])

        with pytest.raises(
            IntegrationException,
            match="Got status code 401 from external server, but can only continue on: 2xx",
        ):
            make_request(
                "GET",
                opds_request_fixture.request_url,
                allowed_response_codes=["2xx"],
            )

        assert http_client.requests == [
            opds_request_fixture.request_url,
            opds_request_fixture.auth_url,
            opds_request_fixture.request_url,
        ]


class TestGetOpdsRequests:
    @pytest.mark.parametrize(
        "authentication,expected",
        [
            pytest.param(
                OPDS2AuthType.BASIC,
                BasicAuthOpdsRequest,
                id="basic auth",
            ),
            pytest.param(
                OPDS2AuthType.OAUTH,
                OAuthOpdsRequest,
                id="oauth",
            ),
            pytest.param(
                OPDS2AuthType.NONE,
                NoAuthOpdsRequest,
                id="no auth",
            ),
        ],
    )
    def test_get_opds_requests(
        self,
        opds_request_fixture: OpdsRequestFixture,
        authentication: OPDS2AuthType,
        expected: type[BaseOpdsHttpRequest],
    ) -> None:
        assert isinstance(
            opds_request_fixture.get_opds_requests(authentication), expected
        )

    def test_invalid_auth_type(self, opds_request_fixture: OpdsRequestFixture) -> None:
        with pytest.raises(
            PalaceValueError,
            match="Unsupported authentication type: InvalidAuthType",
        ):
            opds_request_fixture.get_opds_requests("InvalidAuthType")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "authentication,kwargs,match",
        [
            pytest.param(
                OPDS2AuthType.BASIC,
                {"username": None, "password": None},
                "Username and password are required for basic auth.",
                id="basic auth missing credentials",
            ),
            pytest.param(
                OPDS2AuthType.BASIC,
                {"username": None},
                "Username and password are required for basic auth.",
                id="basic auth missing username",
            ),
            pytest.param(
                OPDS2AuthType.BASIC,
                {"password": None},
                "Username and password are required for basic auth.",
                id="basic auth missing password",
            ),
            pytest.param(
                OPDS2AuthType.OAUTH,
                {"username": None, "password": None, "feed_url": None},
                "Username, password and feed_url are required for OAuth.",
                id="oauth missing credentials",
            ),
            pytest.param(
                OPDS2AuthType.OAUTH,
                {"username": None},
                "Username, password and feed_url are required for OAuth.",
                id="oauth missing username",
            ),
            pytest.param(
                OPDS2AuthType.OAUTH,
                {"password": None},
                "Username, password and feed_url are required for OAuth.",
                id="oauth missing password",
            ),
            pytest.param(
                OPDS2AuthType.OAUTH,
                {"feed_url": None},
                "Username, password and feed_url are required for OAuth.",
                id="oauth missing feed_url",
            ),
        ],
    )
    def test_invalid_auth_parameters(
        self,
        opds_request_fixture: OpdsRequestFixture,
        authentication: OPDS2AuthType,
        kwargs: dict[str, Any],
        match: str,
    ) -> None:
        with pytest.raises(PalaceValueError, match=match):
            opds_request_fixture.get_opds_requests(authentication, **kwargs)
