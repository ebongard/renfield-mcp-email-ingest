"""Operator notification — never quieter on failure than on success.

An email landing in ``failed/``, a retry-backlog give-up, a fatal token error, or
an IMAP disconnect all reach the operator. The default :class:`LogNotifier` emits
loud structured ERROR logs (what container log alerting watches); a
:class:`WebhookNotifier` additionally POSTs to a configured webhook when
``EMAIL_NOTIFY_WEBHOOK_URL`` is set.
"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger("renfield-mcp-email-ingest.notify")

_SOURCE = "renfield-mcp-email-ingest"


class Notifier:
    async def failure(self, mailbox: str, ref: str, reason: str) -> None:
        raise NotImplementedError

    async def fatal(self, mailbox: str, reason: str) -> None:
        raise NotImplementedError

    async def disconnect(self, mailbox: str, reason: str) -> None:
        raise NotImplementedError


class LogNotifier(Notifier):
    async def failure(self, mailbox: str, ref: str, reason: str) -> None:
        logger.error("OPERATOR-NOTIFY failure mailbox=%s ref=%s reason=%s", mailbox, ref, reason)

    async def fatal(self, mailbox: str, reason: str) -> None:
        logger.error("OPERATOR-NOTIFY fatal mailbox=%s reason=%s", mailbox, reason)

    async def disconnect(self, mailbox: str, reason: str) -> None:
        logger.error("OPERATOR-NOTIFY disconnect mailbox=%s reason=%s", mailbox, reason)


class WebhookNotifier(Notifier):
    """Logs (via an inner LogNotifier) AND best-effort POSTs a compact JSON
    notification to ``webhook_url``. A webhook failure never raises — operator
    notification must not break the ingest path."""

    def __init__(self, webhook_url: str, token: str | None = None, timeout: float = 10.0):
        self._url = webhook_url
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._timeout = timeout
        self._log = LogNotifier()

    async def _post(self, event: str, **fields) -> None:
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                await client.post(
                    self._url,
                    headers=self._headers,
                    json={"source": _SOURCE, "event": event, **fields},
                )
        except httpx.HTTPError as exc:
            logger.warning("notify webhook POST failed: %s", exc)

    async def failure(self, mailbox: str, ref: str, reason: str) -> None:
        await self._log.failure(mailbox, ref, reason)
        await self._post("failure", mailbox=mailbox, ref=ref, reason=reason)

    async def fatal(self, mailbox: str, reason: str) -> None:
        await self._log.fatal(mailbox, reason)
        await self._post("fatal", mailbox=mailbox, reason=reason)

    async def disconnect(self, mailbox: str, reason: str) -> None:
        await self._log.disconnect(mailbox, reason)
        await self._post("disconnect", mailbox=mailbox, reason=reason)


def make_notifier(webhook_url: str | None, token: str | None = None) -> Notifier:
    return WebhookNotifier(webhook_url, token) if webhook_url else LogNotifier()