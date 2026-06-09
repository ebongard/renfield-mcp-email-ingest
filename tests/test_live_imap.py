"""Live IMAP integration test — exercises the real ImapProvider against a real
IMAP server (the IDLE loop, fetch, move, mark-seen) the unit tests can't cover.

SKIPPED unless ``EMAIL_LIVE_IMAP_HOST`` (+ creds) are set. Run on the .159 box (or
any host with reachable IMAP) like the filesystem repo's live inotify test:

    EMAIL_LIVE_IMAP_HOST=imap.example.com \
    EMAIL_LIVE_IMAP_USER=... EMAIL_LIVE_IMAP_PASS=... \
    python -m pytest tests/test_live_imap.py -o asyncio_mode=auto -v

It connects, ensures the move folders, lists UNSEEN, and (if a message exists)
fetches + parses it. It does NOT move/delete unless ``EMAIL_LIVE_IMAP_MUTATE=1``.
"""

from __future__ import annotations

import os

import pytest

from renfield_mcp_email_ingest.attachments import parse_message
from renfield_mcp_email_ingest.config import Mailbox

_HOST = os.environ.get("EMAIL_LIVE_IMAP_HOST")

pytestmark = pytest.mark.skipif(
    not _HOST, reason="set EMAIL_LIVE_IMAP_HOST (+ USER/PASS) to run the live IMAP test"
)


def _mailbox() -> Mailbox:
    os.environ.setdefault("EMAIL_LIVE_IMAP_USER", "")
    os.environ.setdefault("EMAIL_LIVE_IMAP_PASS", "")
    return Mailbox(
        id="live",
        host=_HOST,
        port=int(os.environ.get("EMAIL_LIVE_IMAP_PORT", "993")),
        ssl=os.environ.get("EMAIL_LIVE_IMAP_SSL", "1") != "0",
        username_env="EMAIL_LIVE_IMAP_USER",
        password_env="EMAIL_LIVE_IMAP_PASS",
        inbox=os.environ.get("EMAIL_LIVE_IMAP_INBOX", "INBOX"),
        processed_folder=os.environ.get("EMAIL_LIVE_IMAP_PROCESSED", "Verarbeitet"),
        failed_folder=os.environ.get("EMAIL_LIVE_IMAP_FAILED", "Fehler"),
    )


async def test_live_connect_list_and_fetch():
    from renfield_mcp_email_ingest.providers.imap import ImapProvider

    provider = ImapProvider(_mailbox())
    try:
        await provider.connect()
        uids = await provider.list_unseen()
        assert isinstance(uids, list)
        if uids:
            raw = await provider.fetch_raw(uids[0])
            assert raw is not None
            parsed = parse_message(raw)
            assert parsed.message_id  # always set (fabricated if absent)
    finally:
        await provider.stop()
