"""Smoke tests for tap-microsoft-graph.

These tests exercise the tap's plumbing without hitting the network. A real
end-to-end test requires live Graph credentials and a shared mailbox — see
the project README for the manual smoke-test recipe.
"""

from __future__ import annotations

from tap_microsoft_graph.streams.attachments import AttachmentsStream
from tap_microsoft_graph.streams.messages import MessagesStream
from tap_microsoft_graph.tap import TapMicrosoftGraph

_BASE_CONFIG = {
    "client_id": "fake-client-id",
    "client_secret": "fake-secret",
    "tenant_id": "fake-tenant",
    "shared_mailbox_id": "marketingagent@vertexservicepartners.com",
}


def _tap(extra: dict | None = None) -> TapMicrosoftGraph:
    config = {**_BASE_CONFIG, **(extra or {})}
    return TapMicrosoftGraph(config=config, catalog={"streams": []}, state={})


def test_tap_loads_two_streams():
    tap = _tap()
    names = {s.name for s in tap.discover_streams()}
    assert names == {"messages", "attachments"}


def test_messages_post_process_handles_missing_fields():
    stream = MessagesStream(tap=_tap())
    raw = {
        "id": "AAMkAGI...",
        "subject": "iSpot.tv April invoice",
        "receivedDateTime": "2026-04-12T14:21:00Z",
        # No `from`, no `body`, no recipients — these are common for
        # system-generated emails or drafts.
    }
    row = stream.post_process(raw, context=None)
    assert row["message_id"] == "AAMkAGI..."
    assert row["from_address"] is None
    assert row["body_text"] is None
    assert row["to_recipients"] == []
    assert row["received_at"] == "2026-04-12T14:21:00Z"


def test_messages_post_process_flattens_nested_recipients():
    stream = MessagesStream(tap=_tap())
    raw = {
        "id": "x",
        "subject": "Test",
        "receivedDateTime": "2026-04-12T14:21:00Z",
        "from": {"emailAddress": {"address": "Billing@iSpot.TV", "name": "iSpot Billing"}},
        "toRecipients": [
            {"emailAddress": {"address": "marketingagent@vertexservicepartners.com", "name": "Marketing"}},
        ],
        "body": {"contentType": "text", "content": "Your April invoice is attached."},
    }
    row = stream.post_process(raw, context=None)
    assert row["from_address"] == "billing@ispot.tv"  # lowercased
    assert row["from_name"] == "iSpot Billing"
    assert row["to_recipients"] == [
        {"address": "marketingagent@vertexservicepartners.com", "name": "Marketing"},
    ]
    assert row["body_text"] == "Your April invoice is attached."


def test_attachments_post_process_when_stage_upload_disabled():
    stream = AttachmentsStream(tap=_tap({"stage_upload_enabled": False}))
    raw = {
        "id": "att-1",
        "name": "invoice.pdf",
        "contentType": "application/pdf",
        "size": 12345,
        "isInline": False,
        "@odata.type": "#microsoft.graph.fileAttachment",
    }
    row = stream.post_process(raw, context={"message_id": "msg-1"})
    assert row["attachment_id"] == "att-1"
    assert row["message_id"] == "msg-1"
    assert row["stage_path"] is None  # side-channel disabled
    assert row["content_type"] == "application/pdf"


def test_safe_filename_strips_dangerous_chars():
    assert (
        AttachmentsStream._safe_filename("Invoice 5/12 (TV).pdf")
        == "Invoice_5_12__TV_.pdf"
    )
