"""The cross-repo email-ingest contract — the wire shape this watcher and the
Renfield backend (``api/routes/email_ingest.py`` + ``services/email_ingest.py``)
BOTH depend on.

Keep ``EMAIL_INGEST_CONTRACT_VERSION`` + the status names in lock-step with the
backend's ``EMAIL_INGEST_CONTRACT_VERSION``. Bump the version on ANY change to
the request/response shape or the status set, and update the backend's matching
constant.

The 4-state status is identical to folder-ingest (the backend reuses the same
``ingest_document`` pipeline + ``IngestStatus``); only the *unit* differs — here
the watcher decides what to do with an EMAIL (move to an IMAP folder / leave /
mark seen) by AGGREGATING the per-attachment statuses (see ``engine.py``), since
one email fans out into N attachment pushes.
"""

from __future__ import annotations

from enum import Enum

# Mirror of the backend's EMAIL_INGEST_CONTRACT_VERSION. Sent on every push in
# the request header below; the backend echoes its own version in the response.
EMAIL_INGEST_CONTRACT_VERSION = "1"
CONTRACT_HEADER = "X-Email-Ingest-Contract"

# The push + health endpoint paths on the Renfield backend.
INGEST_PATH = "/api/email-ingest/document"
HEALTH_PATH = "/api/email-ingest/health"


class IngestStatus(str, Enum):
    """The 4-state per-attachment response the backend returns. Names are part of
    the cross-repo seam — do not rename without a contract-version bump."""

    INGESTED = "ingested"
    DUPLICATE = "duplicate"
    RETRY = "retry"
    FAILED = "failed"


class AttachmentMove(str, Enum):
    """Per-attachment push decision (analogous to folder-ingest's MoveAction) —
    the engine aggregates these across an email into a :class:`MessageDisposition`."""

    PROCESSED = "processed"  # ingested|duplicate — this attachment is done
    FAILED = "failed"  # backend terminally rejected this attachment
    LEAVE = "leave"  # transient — re-push this attachment later


class MessageDisposition(str, Enum):
    """What the watcher does with the *email* after pushing all its attachments."""

    PROCESSED = "processed"  # move → processed IMAP folder (≥1 attachment ingested)
    FAILED = "failed"  # move → failed IMAP folder (couldn't ingest any real doc)
    LEAVE = "leave"  # leave UNSEEN in the inbox; re-process later (transient)
    SKIP = "skip"  # mark \\Seen + leave (no ingestable attachments — not our concern)


# Per-attachment status → attachment move. An UNKNOWN status (contract skew) maps
# to LEAVE so an attachment is never dropped on a version we don't understand.
_ATTACHMENT_MOVE_BY_STATUS = {
    IngestStatus.INGESTED: AttachmentMove.PROCESSED,
    IngestStatus.DUPLICATE: AttachmentMove.PROCESSED,
    IngestStatus.FAILED: AttachmentMove.FAILED,
    IngestStatus.RETRY: AttachmentMove.LEAVE,
}


def attachment_move_for(status: str) -> AttachmentMove:
    """Map a backend status string to an attachment move. Unknown / unparseable →
    LEAVE (re-push later) so contract skew never drops an attachment."""
    try:
        return _ATTACHMENT_MOVE_BY_STATUS[IngestStatus(status)]
    except ValueError:
        return AttachmentMove.LEAVE