"""The ``MailboxProvider`` abstraction — one interface, per-transport impls (only
IMAP for now). Detection is **event-driven** (``watch()`` yields new-message
events as the IMAP server reports them over IDLE); it is NEVER a poll loop.
``list_unseen`` exists only for one-shot startup reconciliation and the on-demand
MCP tool, never as the watch mechanism.

The *unit* a provider deals in is a MESSAGE, addressed by its IMAP ``uid`` (stable
within the inbox folder until UIDVALIDITY changes). The engine fetches the raw
message, extracts attachments itself (``attachments.py``), and then asks the
provider to move / flag the message by the aggregated outcome.
"""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator
from dataclasses import dataclass


@dataclass(frozen=True)
class NewMessage:
    """An event-driven signal that a message is present + unprocessed (UNSEEN) in
    the watched inbox and ready to be ingested. ``uid`` is the IMAP UID."""

    uid: str


class MailboxProvider(abc.ABC):
    """Access boundary to one IMAP mailbox. Holds the IMAP credentials/session
    the backend must not have."""

    def __init__(self, mailbox_id: str):
        self.mailbox_id = mailbox_id
        self._disconnect_hook = None

    def set_disconnect_hook(self, cb) -> None:
        """Register ``async cb(mailbox_id, reason)`` called when the provider loses
        its connection (before it reconnects). Used for operator notify."""
        self._disconnect_hook = cb

    @property
    def last_error(self) -> str | None:
        """The most recent connection/watch error, or None if healthy."""
        return None

    async def connect(self) -> None:
        """Establish IMAP readiness (login, select inbox, ensure the move folders
        exist) WITHOUT starting IDLE. Used by dry-run to list without side effects.
        ``start()`` calls this first. Default: no-op."""

    @abc.abstractmethod
    async def start(self) -> None:
        """connect() + start the IMAP IDLE event source. Idempotent."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Tear down IDLE + the IMAP session."""

    @abc.abstractmethod
    def watch(self) -> AsyncIterator[NewMessage]:
        """Event-driven async iterator of new-message events. Implementations MUST
        surface messages via IMAP IDLE (NOT a poll) and re-issue IDLE within the
        server's timeout (RFC 2177)."""

    @abc.abstractmethod
    async def list_unseen(self) -> list[str]:
        """UIDs of UNSEEN messages in the inbox (for one-shot startup
        reconciliation + the on-demand tool — NOT the watch loop)."""

    @abc.abstractmethod
    async def fetch_raw(self, uid: str) -> bytes | None:
        """Fetch the full RFC-822 bytes of a message, or None if it no longer
        exists (already moved / expunged)."""

    @abc.abstractmethod
    async def move_message(self, uid: str, folder: str) -> None:
        """Move a message into ``folder`` (created if missing). Removes it from the
        inbox so it is never re-processed."""

    @abc.abstractmethod
    async def mark_seen(self, uid: str) -> None:
        """Flag a message ``\\Seen`` (so reconciliation skips it) without moving
        it. Used for messages with no ingestable attachments."""

    @property
    @abc.abstractmethod
    def connected(self) -> bool:
        """Whether the IDLE event source / IMAP session is currently healthy."""