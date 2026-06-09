"""Daemon diff/lifecycle tests with a fake provider (no real IMAP)."""

from collections.abc import AsyncIterator

import pytest

from renfield_mcp_email_ingest.config import Config, Mailbox
from renfield_mcp_email_ingest.daemon import MailboxDaemonManager
from renfield_mcp_email_ingest.providers.base import MailboxProvider, NewMessage


class _IdleProvider(MailboxProvider):
    """A provider whose watch() blocks forever (so the engine task stays alive,
    letting us exercise the add/remove/restart diff)."""

    instances: list["_IdleProvider"] = []

    def __init__(self, mailbox, idle_renew_seconds=1500):
        super().__init__(mailbox.id)
        self.mailbox = mailbox
        self.stopped = False
        _IdleProvider.instances.append(self)

    async def start(self):
        pass

    async def stop(self):
        self.stopped = True

    async def watch(self) -> AsyncIterator[NewMessage]:
        import asyncio
        await asyncio.Event().wait()  # block forever
        yield NewMessage("never")  # pragma: no cover

    async def list_unseen(self):
        return []

    async def fetch_raw(self, uid):
        return None

    async def move_message(self, uid, folder):
        pass

    async def mark_seen(self, uid):
        pass

    @property
    def connected(self):
        return True


def _mbox(mid, **kw):
    base = dict(id=mid, host="h", username_env="U", password_env="P")
    base.update(kw)
    return Mailbox(**base)


def _config(mailboxes, path="/tmp/does-not-exist.yaml"):
    return Config(
        renfield_url="http://x", ingest_token="t",
        allowed_extensions=("pdf",), mailboxes_path=path, mailboxes=mailboxes,
    )


def _manager(mailboxes):
    _IdleProvider.instances.clear()
    return MailboxDaemonManager(
        _config(mailboxes), provider_factory=_IdleProvider,
    )


def test_build_creates_providers():
    mgr = _manager([_mbox("a"), _mbox("b")])
    assert sorted(mgr.names()) == ["a", "b"]
    assert mgr.get("a") is not None


async def test_apply_adds_and_removes():
    mgr = _manager([_mbox("a")])
    await mgr.start()
    try:
        await mgr._apply([_mbox("a"), _mbox("b")])  # add b
        assert sorted(mgr.names()) == ["a", "b"]
        await mgr._apply([_mbox("b")])  # remove a
        assert mgr.names() == ["b"]
    finally:
        await mgr.stop()


async def test_apply_restarts_changed_mailbox():
    mgr = _manager([_mbox("a", inbox="INBOX")])
    await mgr.start()
    try:
        first = mgr.get("a")
        await mgr._apply([_mbox("a", inbox="Archive")])  # changed → rebuilt
        second = mgr.get("a")
        assert second is not first  # provider rebuilt
        assert first.stopped is True
    finally:
        await mgr.stop()


async def test_stop_stops_all_providers():
    mgr = _manager([_mbox("a"), _mbox("b")])
    await mgr.start()
    await mgr.stop()
    assert all(p.stopped for p in _IdleProvider.instances)
    assert mgr.names() == []