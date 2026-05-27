"""Stream: attachments — metadata for attachments on each mail message.

Child of :class:`MessagesStream`. For every parent ``message_id`` with
``has_attachments=True``, hit
``GET /users/{shared_mailbox_id}/messages/{message_id}/attachments``.

**Bytes are NOT emitted through Singer.** Attachment payloads can be tens
of megabytes; round-tripping them as base64 strings through the Singer
JSON Lines protocol bloats target latency and warehouse storage. Instead
this stream is intended to be paired with a Snowflake-stage ``PUT``
side-channel that streams the bytes directly into
``@ARCH_RAW.OUTLOOK_MARKETING_INBOX.MARKETING_EMAIL_ATTACHMENTS/...`` and
records only the resulting ``stage_path`` here.

The side-channel writer is scaffolded under
:func:`upload_to_snowflake_stage` but disabled by default — enabling it
requires Snowflake creds (``SNOWFLAKE_*`` env vars), the target stage to
exist, and the ``stage_upload_enabled`` config flag to be true.
"""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import logging
import os
import tempfile
from typing import Any, ClassVar

from singer_sdk import typing as th

from tap_microsoft_graph.client import MicrosoftGraphStream
from tap_microsoft_graph.streams.messages import MessagesStream

LOGGER = logging.getLogger(__name__)

# Target Snowflake stage that receives attachment bytes. Pinned here (rather
# than configurable) because PARSE_DOCUMENT in the dbt arch_prep layer also
# hardcodes the same path — keep them in sync.
STAGE_FQN = "ARCH_RAW.OUTLOOK_MARKETING_INBOX.MARKETING_EMAIL_ATTACHMENTS"


class AttachmentsStream(MicrosoftGraphStream):
    """One row per attachment, with metadata only (bytes go to a Snowflake stage)."""

    name = "attachments"
    parent_stream_type = MessagesStream
    primary_keys = ("attachment_id",)
    # No replication_key — attachments are immutable once received; we sync
    # them once per parent message and rely on the parent's incremental
    # cursor to bound the work.
    state_partitioning_keys: ClassVar[list[str]] = []  # don't partition state per message_id

    @property
    def path(self) -> str:
        mailbox = self.config["shared_mailbox_id"]
        return f"/users/{mailbox}/messages/{{message_id}}/attachments"

    schema = th.PropertiesList(
        th.Property("attachment_id", th.StringType, required=True),
        th.Property("message_id", th.StringType, required=True),
        th.Property("name", th.StringType, description="Attachment filename."),
        th.Property("content_type", th.StringType, description="MIME type."),
        th.Property("size_bytes", th.IntegerType),
        th.Property("is_inline", th.BooleanType, description="True for inline images (signatures, embedded media)."),
        th.Property(
            "stage_path",
            th.StringType,
            description=(
                "Relative path inside the Snowflake stage where the bytes "
                "landed, e.g. '2026/05/18/<msg_id>/<att_id>__invoice.pdf'. "
                "Null when stage_upload_enabled=false."
            ),
        ),
        th.Property("odata_type", th.StringType, description="Graph attachment subtype: fileAttachment | itemAttachment | referenceAttachment."),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        if next_page_token:
            return {}
        # NOTE: @odata.type can NOT be in $select (Graph 400s on it), but it
        # IS returned automatically on polymorphic types like attachments
        # (fileAttachment vs itemAttachment vs referenceAttachment), so we
        # still get it in the response body for post_process to read.
        return {
            "$select": "id,name,contentType,size,isInline",
            "$top": 50,
        }

    def get_records(self, context: dict | None = None):
        """Skip cleanly when no parent context — singer-sdk sometimes invokes
        a child stream's sync with no context (e.g. when no parent record had
        has_attachments=True). Without this, the {message_id} placeholder
        stays unresolved in the URL and Graph 400s.
        """
        if not context or not context.get("message_id"):
            return
        yield from super().get_records(context)

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        message_id = (context or {}).get("message_id")
        attachment_id = row.get("id")
        record = {
            "attachment_id": attachment_id,
            "message_id": message_id,
            "name": row.get("name"),
            "content_type": row.get("contentType"),
            "size_bytes": row.get("size"),
            "is_inline": row.get("isInline"),
            "odata_type": row.get("@odata.type"),
            "stage_path": None,
        }

        if (
            self.config.get("stage_upload_enabled")
            and row.get("@odata.type", "").endswith("fileAttachment")
        ):
            try:
                record["stage_path"] = self._upload_to_snowflake_stage(
                    message_id=message_id,
                    attachment_id=attachment_id,
                    filename=row.get("name") or attachment_id,
                )
            except Exception:
                # Fail open — log and emit metadata row with stage_path=None
                # so downstream knows the upload didn't happen.
                LOGGER.exception(
                    "Snowflake stage upload failed for message_id=%s attachment_id=%s",
                    message_id,
                    attachment_id,
                )

        return record

    _SAFE_FILENAME_ALLOWED = frozenset(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    )

    @classmethod
    def _safe_filename(cls, name: str) -> str:
        return "".join(c if c in cls._SAFE_FILENAME_ALLOWED else "_" for c in name)

    def _fetch_attachment_bytes(self, message_id: str, attachment_id: str) -> bytes:
        """Download attachment bytes via Graph and base64-decode.

        For fileAttachments, Graph returns the bytes in the response body's
        ``contentBytes`` field as base64. Item/referenceAttachments don't
        have ``contentBytes`` and raise.
        """
        mailbox = self.config["shared_mailbox_id"]
        url = (
            f"{self.url_base}/users/{mailbox}/messages/"
            f"{message_id}/attachments/{attachment_id}"
        )
        auth_headers = (self.authenticator.auth_headers or {}) if self.authenticator else {}
        resp = self.requests_session.get(url, headers=auth_headers, timeout=120)
        resp.raise_for_status()
        body = resp.json()
        b64 = body.get("contentBytes")
        if not b64:
            raise ValueError(
                f"Attachment {attachment_id} on {message_id} has no contentBytes "
                "(likely an itemAttachment or referenceAttachment, not fileAttachment)."
            )
        return base64.b64decode(b64)

    def _snowflake_conn(self):
        """Lazy-init shared Snowflake connection for stage PUTs.

        Reuses the same env vars that target-snowflake reads
        (``TARGET_SNOWFLAKE_{ACCOUNT,USER,PASSWORD,ROLE,WAREHOUSE,DATABASE}``)
        so config in Dagster+ stays a single source of truth.
        """
        cached = getattr(self, "_sf_conn_cache", None)
        if cached is not None:
            return cached
        # Import lazily — snowflake-connector-python is heavy and only needed
        # when stage_upload_enabled=true.
        import snowflake.connector
        cached = snowflake.connector.connect(
            account=os.environ["TARGET_SNOWFLAKE_ACCOUNT"],
            user=os.environ["TARGET_SNOWFLAKE_USER"],
            password=os.environ["TARGET_SNOWFLAKE_PASSWORD"],
            role=os.environ["TARGET_SNOWFLAKE_ROLE"],
            warehouse=os.environ["TARGET_SNOWFLAKE_WAREHOUSE"],
        )
        self._sf_conn_cache = cached
        return cached

    def _upload_to_snowflake_stage(
        self,
        *,
        message_id: str | None,
        attachment_id: str | None,
        filename: str,
    ) -> str:
        """Stream attachment bytes from Graph into the Snowflake internal stage.

        Returns the relative ``stage_path`` (without the leading ``@stage``)
        where the file landed, e.g. ``2026/05/27/<msg>/<att>__invoice.pdf``.
        Downstream ``PARSE_DOCUMENT`` references this path.

        Snowflake's PUT command needs a local file path — write to a
        per-call tempfile, PUT it, then delete.
        """
        # Date-partition by today's UTC date (close enough to received_at
        # for our hourly tap; avoids needing to plumb received_at through
        # the child context).
        today = dt.datetime.now(dt.UTC)
        date_prefix = f"{today.year:04d}/{today.month:02d}/{today.day:02d}"
        safe_msg = self._safe_filename(message_id or "unknown_msg")
        safe_att = self._safe_filename(attachment_id or "unknown_att")
        safe_name = self._safe_filename(filename)
        stage_path = f"{date_prefix}/{safe_msg}/{safe_att}__{safe_name}"

        # 1. Fetch bytes from Graph.
        contents = self._fetch_attachment_bytes(message_id, attachment_id)

        # 2. Write to a temp DIR (not tempfile) so we control the basename.
        #    Snowflake's PUT preserves the local file's basename in the
        #    stage, so the local filename IS the final stage filename.
        final_basename = f"{safe_att}__{safe_name}"
        tmpdir = tempfile.mkdtemp()
        local_path = os.path.join(tmpdir, final_basename)
        try:
            with open(local_path, "wb") as f:
                f.write(contents)
            # PUT — file:// URL on the LOCAL side, @stage path on the
            # remote side. AUTO_COMPRESS=FALSE so PARSE_DOCUMENT can read
            # the original (PDF/XLSX are already compressed; doubling
            # adds nothing and breaks PARSE_DOCUMENT's content-type sniff).
            # OVERWRITE=TRUE in case of re-sync.
            stage_dir = f"@{STAGE_FQN}/{date_prefix}/{safe_msg}"
            sql = (
                f"PUT 'file://{local_path}' {stage_dir} "
                "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            cur = self._snowflake_conn().cursor()
            try:
                cur.execute(sql)
            finally:
                cur.close()
        finally:
            with contextlib.suppress(OSError):
                os.remove(local_path)
            with contextlib.suppress(OSError):
                os.rmdir(tmpdir)

        return stage_path
