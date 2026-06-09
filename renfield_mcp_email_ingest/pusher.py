"""The REST push client — POSTs one email attachment to Renfield's email-ingest
endpoint and resolves the response into a per-attachment move decision.

Transport mapping (the cross-repo contract, identical to folder-ingest):
  - 200 body ``{status, document_id, detail, contract_version}`` → move by status
    (ingested|duplicate → PROCESSED, failed → FAILED, retry → LEAVE).
  - 401/403 → FATAL config error (wrong/missing token): the daemon surfaces it
    loudly and stops touching this mailbox.
  - 503 / network error / any other code → treat as LEAVE (transient retry) — an
    attachment is never dropped on a transient or unknown outcome.

The watcher sends only the routing ``mailbox_id`` (+ message provenance), NEVER a
tier/owner — the backend owns the sphere routing.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import httpx

from .contract import (
    CONTRACT_HEADER,
    EMAIL_INGEST_CONTRACT_VERSION,
    INGEST_PATH,
    AttachmentMove,
    attachment_move_for,
)

logger = logging.getLogger("renfield-mcp-email-ingest.pusher")


@dataclass
class PushOutcome:
    move: AttachmentMove
    fatal: bool = False  # 401/403 — operator must fix the token; stop the mailbox
    status: str | None = None
    document_id: int | None = None
    detail: str | None = None
    http_status: int | None = None


class RenfieldPusher:
    def __init__(self, renfield_url: str, ingest_token: str, timeout_seconds: float = 120.0):
        self._url = renfield_url.rstrip("/") + INGEST_PATH
        self._headers = {
            "Authorization": f"Bearer {ingest_token}",
            CONTRACT_HEADER: EMAIL_INGEST_CONTRACT_VERSION,
        }
        self._timeout = timeout_seconds

    async def push(
        self,
        *,
        file_bytes: bytes,
        filename: str,
        mailbox_id: str,
        message_id: str,
        sha256: str,
        mime: str | None,
        sender: str | None = None,
        subject: str | None = None,
    ) -> PushOutcome:
        metadata = json.dumps(
            {
                "filename": filename,
                "mailbox_id": mailbox_id,
                "message_id": message_id,
                "sha256": sha256,
                "mime": mime,
                "sender": sender,
                "subject": subject,
            }
        )
        files = {"file": (filename, file_bytes, mime or "application/octet-stream")}
        data = {"metadata": metadata}
        ref = f"{mailbox_id}/{message_id}/{filename}"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(
                    self._url, headers=self._headers, files=files, data=data
                )
        except httpx.HTTPError as exc:
            logger.warning("push for %s failed at transport: %s", ref, exc)
            return PushOutcome(move=AttachmentMove.LEAVE, detail=f"transport_error: {exc}")

        if resp.status_code in (401, 403):
            logger.error(
                "push for %s rejected with HTTP %s — token/config error (fatal)",
                ref, resp.status_code,
            )
            return PushOutcome(
                move=AttachmentMove.LEAVE, fatal=True, http_status=resp.status_code
            )

        if resp.status_code != 200:
            # 503 (disabled / worker down) and anything else → leave + retry.
            logger.info(
                "push for %s got HTTP %s — leaving to retry", ref, resp.status_code
            )
            return PushOutcome(move=AttachmentMove.LEAVE, http_status=resp.status_code)

        try:
            body = resp.json()
        except (json.JSONDecodeError, ValueError):
            logger.warning("push for %s: 200 but unparseable body; will retry", ref)
            return PushOutcome(move=AttachmentMove.LEAVE, http_status=200)

        status = body.get("status")
        move = attachment_move_for(str(status))
        skew = body.get("contract_version")
        if skew and skew != EMAIL_INGEST_CONTRACT_VERSION:
            logger.warning(
                "contract skew: backend %s, MCP %s (processing leniently)",
                skew, EMAIL_INGEST_CONTRACT_VERSION,
            )
        return PushOutcome(
            move=move,
            status=str(status) if status is not None else None,
            document_id=body.get("document_id"),
            detail=body.get("detail"),
            http_status=200,
        )