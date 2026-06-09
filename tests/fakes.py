"""Shared test doubles: a fake MailboxProvider + a fake pusher."""

from __future__ import annotations

from collections.abc import AsyncIterator

from renfield_mcp_email_ingest.providers.base import MailboxProvider, NewMessage


class FakeProvider(MailboxProvider):
    def __init__(self, messages: dict[str, bytes], *, unseen=(), events=()):
        super().__init__("mbox")
        self.messages = dict(messages)  # uid -> raw bytes
        self._unseen = list(unseen)
        self._events = list(events)
        self.moved: list[tuple[str, str]] = []  # (uid, folder)
        self.seen: list[str] = []  # uids marked \Seen
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        pass

    async def watch(self) -> AsyncIterator[NewMessage]:
        for uid in self._events:
            yield NewMessage(uid)

    async def list_unseen(self):
        return [u for u in self._unseen if u in self.messages]

    async def fetch_raw(self, uid):
        return self.messages.get(uid)

    async def move_message(self, uid, folder):
        self.moved.append((uid, folder))
        self.messages.pop(uid, None)

    async def mark_seen(self, uid):
        self.seen.append(uid)

    @property
    def connected(self):
        return True


class FakePusher:
    """Returns a fixed PushOutcome, or one chosen by a callable per call. ``by_name``
    maps a filename to its outcome (for mixed-attachment tests)."""

    def __init__(self, outcome=None, *, by_name=None):
        self._outcome = outcome
        self._by_name = by_name or {}
        self.calls: list[dict] = []

    async def push(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["filename"] in self._by_name:
            return self._by_name[kwargs["filename"]]
        return self._outcome(kwargs) if callable(self._outcome) else self._outcome
