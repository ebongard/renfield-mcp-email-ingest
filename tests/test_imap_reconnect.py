"""Regression tests for the IMAP watch-loop reconnect.

Bug (prod, 2026-06-10): after a server BYE-timeout ended the IDLE, the reconnect
path reset only the IDLE connection and left the command connection dead. Every
subsequent SEARCH UNSEEN then threw, so the loop re-armed IDLE forever but never
recovered — mail stopped being detected until a pod restart. The fix resets BOTH
connections on a disconnect.
"""

import asyncio

from renfield_mcp_email_ingest.config import Mailbox
from renfield_mcp_email_ingest.providers import imap as imap_mod
from renfield_mcp_email_ingest.providers.imap import ImapProvider


def _mailbox() -> Mailbox:
    return Mailbox(id="buchhaltung", host="imap.example.com",
                   username_env="U", password_env="P")


class FakeClient:
    def __init__(self) -> None:
        self.logged_out = False

    async def logout(self) -> None:
        self.logged_out = True


async def test_reset_cmd_client_closes_and_nulls():
    p = ImapProvider(_mailbox())
    cmd = FakeClient()
    p._cmd = cmd
    await p._reset_cmd_client()
    assert cmd.logged_out is True
    assert p._cmd is None


async def test_reconnect_resets_both_idle_and_cmd(monkeypatch):
    # Make the backoff instant so the test doesn't wait real seconds.
    monkeypatch.setattr(imap_mod, "reconnect_delay", lambda *a, **k: 0)

    p = ImapProvider(_mailbox())
    idle, cmd = FakeClient(), FakeClient()
    p._idle, p._cmd = idle, cmd

    calls = 0

    async def fake_idle_loop():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("BYE timeout")  # the disconnect
        await asyncio.Event().wait()  # block on the post-reconnect cycle

    monkeypatch.setattr(p, "_idle_loop", fake_idle_loop)

    task = asyncio.create_task(p._watch_with_reconnect())
    try:
        # Wait until the disconnect handler has reset BOTH connections.
        for _ in range(500):
            if p._idle is None and p._cmd is None:
                break
            await asyncio.sleep(0.01)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # The regression: previously cmd.logged_out stayed False and p._cmd stayed set.
    assert idle.logged_out is True, "idle connection should be reset on reconnect"
    assert cmd.logged_out is True, "command connection MUST be reset on reconnect"
    assert p._idle is None and p._cmd is None
