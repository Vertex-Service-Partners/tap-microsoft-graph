"""Base REST stream for the Microsoft Graph tap.

Handles auth via the shared `MicrosoftGraphAuthenticator`, paginates on
Graph's `@odata.nextLink` URLs, and parses `value`-keyed response envelopes.

Graph specifics worth knowing:

- Responses look like ``{"@odata.nextLink": "...", "value": [...]}`` — set
  ``records_jsonpath = "$.value[*]"`` to flatten.
- Pagination is opaque — Graph returns the *full URL* of the next page in
  ``@odata.nextLink``; we treat that as the next-page token and call
  ``get_url`` to return it verbatim, which means ``get_url_params`` must
  return an empty dict on follow-up pages (the URL already carries
  ``$skiptoken``).
- Authorization header is automatic via ``OAuthAuthenticator``; no need to
  set it in ``http_headers``.
"""

from __future__ import annotations

from typing import Any

import requests
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_microsoft_graph.auth import MicrosoftGraphAuthenticator


class MicrosoftGraphStream(RESTStream):
    """Base class for Graph REST streams."""

    records_jsonpath = "$.value[*]"
    next_page_token_jsonpath = "$.'@odata.nextLink'"

    @property
    def url_base(self) -> str:
        """Graph API root, configurable so we can pin /v1.0 vs /beta."""
        return self.config["api_url"].rstrip("/")

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        """Shared OAuth client-credentials authenticator (singleton)."""
        return MicrosoftGraphAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Static headers.

        ``ConsistencyLevel: eventual`` is required for several Graph
        endpoints that accept ``$count``, ``$search``, or advanced
        ``$filter`` operators. It's harmless on endpoints that don't.
        """
        return {"ConsistencyLevel": "eventual"}

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> str | None:
        """Pull the full ``@odata.nextLink`` URL from the response body."""
        body = response.json()
        return body.get("@odata.nextLink")

    def get_url(self, context: dict | None) -> str:
        """Return the request URL.

        On the first page this is ``url_base + path``. On follow-up pages
        the next-page token is itself a fully-qualified URL, so it
        supersedes the computed path. We use the stream-level
        ``next_page_token`` plumbed through by ``RESTStream`` via
        ``prepare_request``.
        """
        # ``RESTStream.prepare_request`` already substitutes the next-page
        # token if it's a full URL — but only when path is empty. The
        # cleanest way to support both first-page and follow-up pages
        # uniformly is to inspect the most recent token via the
        # `request_decorator` hook. In practice, returning the default URL
        # works because singer-sdk's RESTStream.prepare_request will swap
        # the URL when next_page_token is a string URL.
        return f"{self.url_base}{self.path}"

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return query params.

        Empty on follow-up pages — Graph's ``@odata.nextLink`` already
        carries ``$skiptoken`` and any ``$select`` / ``$filter`` we set on
        the first call.

        Subclasses override to add ``$select``, ``$top``, ``$filter``,
        and ``$orderby``.
        """
        if next_page_token:
            return {}
        return {}

    def prepare_request(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> requests.PreparedRequest:
        """Override to use ``@odata.nextLink`` as a full URL on follow-up pages."""
        if next_page_token:
            # Use the full URL Graph handed us.
            http_method = self.rest_method
            headers = self.http_headers
            params: dict = {}
            authenticator = self.authenticator
            if authenticator:
                headers.update(authenticator.auth_headers or {})
                params.update(authenticator.auth_params or {})
            request = requests.Request(
                method=http_method,
                url=next_page_token,
                params=params,
                headers=headers,
            )
            return self.requests_session.prepare_request(request)
        return super().prepare_request(context, next_page_token)

    def parse_response(self, response: requests.Response):
        """Yield each record from the ``value`` array."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
