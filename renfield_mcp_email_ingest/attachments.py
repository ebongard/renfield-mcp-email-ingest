"""Parse a raw RFC-822 message into its provenance headers + real attachments.

Pure stdlib (``email``), so it is fully unit-testable without an IMAP server —
the IMAP I/O is isolated in the provider; everything content-related lives here.

**Attachments only, skip inline (decision #2).** A leaf part is an attachment if
it has a filename and is not explicitly ``Content-Disposition: inline`` — this
captures both ``disposition=attachment`` parts and parts that carry only a
``Content-Type`` name, while excluding inline images (signatures, logos, embedded
HTML images) which are noise, not documents. ``multipart/*`` containers and the
text/html bodies are never attachments.
"""

from __future__ import annotations

import email
import hashlib
from dataclasses import dataclass
from email import policy
from email.message import EmailMessage


@dataclass(frozen=True)
class Attachment:
    filename: str
    content: bytes
    mime: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()


@dataclass(frozen=True)
class ParsedMessage:
    message_id: str  # RFC-822 Message-ID (fabricated from the body hash if absent)
    sender: str
    subject: str
    attachments: tuple[Attachment, ...]


def _decode_header(msg: EmailMessage, name: str) -> str:
    """A best-effort, exception-proof header read (RFC-2047 already handled by the
    ``default`` policy). Returns ``""`` for a missing/garbled header."""
    try:
        value = msg.get(name)
        return str(value).strip() if value is not None else ""
    except Exception:  # noqa: BLE001 - a malformed header must never crash ingest
        return ""


def parse_message(raw: bytes) -> ParsedMessage:
    """Parse raw message bytes into :class:`ParsedMessage`. Never raises on a
    malformed message — returns whatever could be extracted (possibly no
    attachments)."""
    msg = email.message_from_bytes(raw, policy=policy.default)

    message_id = _decode_header(msg, "Message-ID")
    if not message_id:
        # Fabricate a STABLE id from the body hash so the backend's idempotency
        # ledger key (mailbox_id, message_id, sha) stays consistent across
        # re-pushes of the same message that happens to lack a Message-ID.
        message_id = f"<sha256:{hashlib.sha256(raw).hexdigest()}@no-message-id>"

    attachments: list[Attachment] = []
    for part in msg.walk():
        if part.is_multipart():
            continue
        filename = part.get_filename()
        if not filename:
            continue
        # Skip inline parts (logos / signature images / embedded HTML images).
        if (part.get_content_disposition() or "").lower() == "inline":
            continue
        try:
            payload = part.get_payload(decode=True)
        except Exception:  # noqa: BLE001 - a single bad part must not lose the rest
            payload = None
        if not payload:
            continue
        attachments.append(
            Attachment(
                filename=filename.strip(),
                content=payload,
                mime=part.get_content_type() or "application/octet-stream",
            )
        )

    return ParsedMessage(
        message_id=message_id,
        sender=_decode_header(msg, "From"),
        subject=_decode_header(msg, "Subject"),
        attachments=tuple(attachments),
    )