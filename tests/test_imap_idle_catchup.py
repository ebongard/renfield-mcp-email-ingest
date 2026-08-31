"""Regression test for the IMAP-IDLE catch-up scan (prod, xidra buchhaltung).

Bug (2026-08-31): when several documents arrived in quick succession, only the
first was processed. An ``EXISTS`` push that lands while the connection is briefly
NOT idling — the gap between the previous wake's ``idle_done`` and the next
``idle_start``, during which the loop ran SEARCH UNSEEN and re-armed — is
delivered with no active waiter, so aioimaplib logs it as an "ignored untagged
response" and drops it. Because the post-wake SEARCH already ran before that mail
was appended, the message sat UNSEEN until the NEXT push or the 25-min renew.

Fix: SEARCH UNSEEN on the dedicated ``_cmd`` connection RIGHT AFTER (re-)arming
``_idle`` — before waiting for the next push — so a gap arrival is caught on the
very next cycle. This test pins that ordering.
"""

import asyncio

import pytest

from renfield_mcp_email_ingest.config import Mailbox
from renfield_mcp_email_ingest.providers.imap import ImapProvider


def _mailbox() -> Mailbox:
    return Mailbox(id="buchhaltung", host="imap.example.com",
                   username_env="U", password_env="P")


class _Break(Exception):
    """Sentinel raised from wait_server_push to break the otherwise-infinite loop
    after exactly one iteration."""


class _FakeIdle:
    """Minimal aioimaplib-idle stand-in. ``idle_start`` returns an already-resolved
    future (so the loop's ``finally`` drain doesn't block), and the first wait
    raises ``_Break`` to end the loop right after the post-arm catch-up scan."""

    def __init__(self) -> None:
        self.order: list[str] = []

    async def idle_start(self, timeout):
        self.order.append("arm")
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        fut.set_result(None)
        return fut

    async def wait_server_push(self):
        self.order.append("wait")
        raise _Break()

    def has_pending_idle(self) -> bool:
        return False

    def idle_done(self) -> None:  # pragma: no cover - not reached (no pending idle)
        self.order.append("done")


async def test_idle_loop_scans_immediately_after_arming(monkeypatch):
    p = ImapProvider(_mailbox())
    fake_idle = _FakeIdle()
    p._idle = fake_idle
    p._connected = True

    search_positions: list[int] = []

    async def fake_search():
        # Record how far the idle loop had progressed when the search ran, so we
        # can assert the catch-up search happened after arming but before waiting.
        search_positions.append(len(fake_idle.order))
        return ["7"] if len(search_positions) == 1 else []

    monkeypatch.setattr(p, "_search_unseen", fake_search)

    with pytest.raises(_Break):
        await p._idle_loop()

    # The catch-up search ran while only "arm" had happened (order == ['arm']),
    # i.e. immediately after arming and BEFORE wait_server_push.
    assert search_positions and search_positions[0] == 1
    assert fake_idle.order[:2] == ["arm", "wait"]

    # …and it enqueued the "gap arrival" so the engine will dispatch it.
    assert not p._queue.empty()
    assert p._queue.get_nowait().uid == "7"
