"""Stream: messages — list mail messages from a shared Outlook mailbox.

Pulls from ``GET /users/{shared_mailbox_id}/messages`` with incremental
replication on ``receivedDateTime``.

The body is requested as ``text`` (Graph's ``Prefer: outlook.body-content-type="text"``
header) so we don't have to strip HTML downstream. The schema is intentionally
flat — Graph returns deeply-nested ``from``, ``sender``, ``toRecipients`` etc.
that we project to scalar/JSON columns in ``post_process`` to avoid the
singer-sdk nested-property GOTCHA (sub-fields not enumerated in the schema
are silently dropped).
"""

from __future__ import annotations

from typing import Any

from singer_sdk import typing as th

from tap_microsoft_graph.client import MicrosoftGraphStream

# Fields we request from Graph. Keep tight — every field adds payload size
# and a few are PII we don't need (``bodyPreview`` is redundant with ``body``).
_SELECT = ",".join(
    [
        "id",
        "internetMessageId",
        "conversationId",
        "subject",
        "from",
        "sender",
        "toRecipients",
        "ccRecipients",
        "bccRecipients",
        "receivedDateTime",
        "sentDateTime",
        "hasAttachments",
        "importance",
        "categories",
        "isRead",
        "isDraft",
        "body",
    ]
)


class MessagesStream(MicrosoftGraphStream):
    """One row per mail message in the configured shared mailbox."""

    name = "messages"
    primary_keys = ("message_id",)
    replication_key = "received_at"
    is_sorted = True  # Graph returns ascending by receivedDateTime with $orderby.

    @property
    def path(self) -> str:
        """Per-mailbox path. ``shared_mailbox_id`` may be UPN or GUID."""
        mailbox = self.config["shared_mailbox_id"]
        return f"/users/{mailbox}/messages"

    schema = th.PropertiesList(
        th.Property("message_id", th.StringType, required=True, description="Graph message id (immutable per mailbox)."),
        th.Property("internet_message_id", th.StringType, description="RFC 2822 Message-ID header."),
        th.Property("conversation_id", th.StringType, description="Thread identifier."),
        th.Property("subject", th.StringType),
        th.Property("from_address", th.StringType, description="Sender email address (lowercased)."),
        th.Property("from_name", th.StringType, description="Sender display name."),
        th.Property("sender_address", th.StringType, description="Envelope sender (may differ from from_address when sent on behalf)."),
        th.Property("to_recipients", th.ArrayType(th.ObjectType(
            th.Property("address", th.StringType),
            th.Property("name", th.StringType),
        )), description="Recipient list, projected to (address, name) pairs."),
        th.Property("cc_recipients", th.ArrayType(th.ObjectType(
            th.Property("address", th.StringType),
            th.Property("name", th.StringType),
        ))),
        th.Property("bcc_recipients", th.ArrayType(th.ObjectType(
            th.Property("address", th.StringType),
            th.Property("name", th.StringType),
        ))),
        th.Property("received_at", th.DateTimeType, required=True, description="Replication key."),
        th.Property("sent_at", th.DateTimeType),
        th.Property("has_attachments", th.BooleanType),
        th.Property("importance", th.StringType, description="low|normal|high"),
        th.Property("categories", th.ArrayType(th.StringType)),
        th.Property("is_read", th.BooleanType),
        th.Property("is_draft", th.BooleanType),
        th.Property("body_content_type", th.StringType, description="text|html — usually text after Prefer header."),
        th.Property("body_text", th.StringType, description="Plain-text body (HTML-stripped server-side via Prefer header)."),
    ).to_dict()

    @property
    def http_headers(self) -> dict:
        """Ask Graph to return the body as plain text, not HTML."""
        headers = super().http_headers
        headers['Prefer'] = 'outlook.body-content-type="text"'
        return headers

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """First-page params: select, orderby, top, and incremental filter."""
        if next_page_token:
            # Follow-up pages come back with @odata.nextLink, which already
            # carries all query state.
            return {}

        starting = self.get_starting_replication_key_value(context)
        params: dict[str, Any] = {
            "$select": _SELECT,
            "$orderby": "receivedDateTime asc",
            "$top": 50,
        }
        if starting:
            # Graph filter against the same field we orderby — required for
            # stable incremental pagination.
            params["$filter"] = f"receivedDateTime ge {starting}"
        return params

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        """Flatten Graph's nested ``from`` / ``recipients`` / ``body`` shapes."""

        def _addr_pairs(items: list[dict] | None) -> list[dict]:
            if not items:
                return []
            return [
                {
                    "address": (i.get("emailAddress") or {}).get("address", "").lower() or None,
                    "name": (i.get("emailAddress") or {}).get("name"),
                }
                for i in items
            ]

        from_obj = (row.get("from") or {}).get("emailAddress") or {}
        sender_obj = (row.get("sender") or {}).get("emailAddress") or {}
        body = row.get("body") or {}

        return {
            "message_id": row.get("id"),
            "internet_message_id": row.get("internetMessageId"),
            "conversation_id": row.get("conversationId"),
            "subject": row.get("subject"),
            "from_address": (from_obj.get("address") or "").lower() or None,
            "from_name": from_obj.get("name"),
            "sender_address": (sender_obj.get("address") or "").lower() or None,
            "to_recipients": _addr_pairs(row.get("toRecipients")),
            "cc_recipients": _addr_pairs(row.get("ccRecipients")),
            "bcc_recipients": _addr_pairs(row.get("bccRecipients")),
            "received_at": row.get("receivedDateTime"),
            "sent_at": row.get("sentDateTime"),
            "has_attachments": row.get("hasAttachments"),
            "importance": row.get("importance"),
            "categories": row.get("categories") or [],
            "is_read": row.get("isRead"),
            "is_draft": row.get("isDraft"),
            "body_content_type": body.get("contentType"),
            "body_text": body.get("content"),
        }

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Plumb message_id + has_attachments to the attachments child stream."""
        if not record.get("has_attachments"):
            return None
        return {"message_id": record["message_id"]}
