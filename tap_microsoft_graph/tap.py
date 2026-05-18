"""tap-microsoft-graph — Singer tap for Microsoft Graph mail endpoints.

Built for the Vertex Service Partners Marketing Spend Agent's email-invoice
ingest pipeline. Pulls every mail message and attachment metadata from a
shared Outlook mailbox into a Singer-compatible target (typically
target-snowflake landing in ``ARCH_RAW.OUTLOOK_MARKETING_INBOX``).
"""

from __future__ import annotations

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_microsoft_graph.streams import AttachmentsStream, MessagesStream

STREAM_TYPES: list[type[Stream]] = [MessagesStream, AttachmentsStream]


class TapMicrosoftGraph(Tap):
    """Singer tap for Microsoft Graph mail."""

    name = "tap-microsoft-graph"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,
            description="Azure AD app registration client ID.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            description="Azure AD app registration client secret.",
        ),
        th.Property(
            "tenant_id",
            th.StringType,
            required=True,
            description=(
                "Azure AD tenant GUID. Used to build the token endpoint "
                "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token."
            ),
        ),
        th.Property(
            "shared_mailbox_id",
            th.StringType,
            required=True,
            description=(
                "User principal name or object id of the shared mailbox to "
                "read, e.g. 'marketingagent@vertexservicepartners.com'. The "
                "app reg's Mail.Read permission must be scoped to this "
                "mailbox via ApplicationAccessPolicy."
            ),
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://graph.microsoft.com/v1.0",
            description="Graph API root. Override to '/beta' only if you need beta-only fields.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2024-01-01T00:00:00Z",
            description="Earliest receivedDateTime to sync on the first run.",
        ),
        th.Property(
            "stage_upload_enabled",
            th.BooleanType,
            default=False,
            description=(
                "When true, the attachments stream PUTs attachment bytes to "
                "the Snowflake stage @ARCH_RAW.OUTLOOK_MARKETING_INBOX."
                "MARKETING_EMAIL_ATTACHMENTS and records stage_path. Requires "
                "the stage to exist and SNOWFLAKE_* connection env vars to be "
                "set. Disabled by default — flip on after the stage lands."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapMicrosoftGraph.cli()
