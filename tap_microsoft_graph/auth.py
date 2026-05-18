"""Microsoft Graph authentication — OAuth2 client_credentials flow.

Production target is the Azure AD app-only flow against a tenant-specific
token endpoint with the `.default` scope, which mints an app-only access
token bearing whatever application permissions the app reg has been
granted admin consent for (in this tap's case: `Mail.Read`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta

if TYPE_CHECKING:
    from singer_sdk.streams import Stream

# Microsoft Graph requires this scope value verbatim when using
# client_credentials — it's the app-only equivalent of "all permissions
# granted to this app reg". Per-permission scopes (e.g. `Mail.Read`) are
# delegated-only.
GRAPH_DEFAULT_SCOPE = "https://graph.microsoft.com/.default"


class MicrosoftGraphAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """OAuth2 client_credentials authenticator for Microsoft Graph."""

    @property
    def oauth_request_body(self) -> dict:
        """OAuth token request body (form-encoded)."""
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "scope": GRAPH_DEFAULT_SCOPE,
            "grant_type": "client_credentials",
        }

    @classmethod
    def create_for_stream(cls, stream: Stream) -> MicrosoftGraphAuthenticator:
        """Build an authenticator pointed at this tenant's token endpoint.

        Tenant-specific URL is derived from `tenant_id`. Streams share one
        authenticator via `SingletonMeta`, so the token is fetched once
        per process and reused across paginated requests.
        """
        tenant_id = stream.config["tenant_id"]
        auth_endpoint = (
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        )
        return cls(stream=stream, auth_endpoint=auth_endpoint, oauth_scopes="")
