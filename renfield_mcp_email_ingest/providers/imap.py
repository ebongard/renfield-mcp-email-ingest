"""IMAP provider — event-driven via IMAP IDLE (RFC 2177), NEVER polling.

Design: TWO connections. One is dedicated to IDLE on the inbox (it only ever
waits for the server to push an unsolicited ``EXISTS``/``RECENT``/``EXPUNGE``);
the other runs the commands (SEARCH/FETCH/MOVE/STORE) — you cannot issue commands
on a connection that is mid-IDLE, and a single shared connection would force us to
leave + re-enter IDLE around every fetch. On an IDLE wake we ``SEARCH UNSEEN`` on
the command connection and enqueue any UID; the engine de-dups by UID.

IDLE is re-issued within ``idle_renew_seconds`` (well inside the server's ~30-min
limit). On any disconnect/error the watch loop notifies + reconnects with
exponential backoff (the #1 real-world IMAP failure).

The UID-list + RFC-822-literal parsing is pure (module functions, unit-tested);
the live IDLE loop, fetch, and move require a real IMAP server and are exercised
by the .159 E2E (mirrors how the SMB provider's CHANGE_NOTIFY loop is validated).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

from ..config import Mailbox
from .base import MailboxProvider, NewMessage

logger = logging.getLogger("renfield-mcp-email-ingest.imap")


def reconnect_delay(attempt: int, base: float = 2.0, cap: float = 60.0) -> float:
    """Exponential backoff for IMAP reconnect attempts, capped."""
    return min(base * (2 ** attempt), cap)


def parse_uid_search(lines) -> list[str]:
    """Extract UIDs from an IMAP (UID) SEARCH response. aioimaplib returns the
    matched ids as space-separated tokens in the data line(s); the trailing line
    is the human-readable completion text. We keep only pure-digit tokens, in
    order, de-duplicated — robust to the completion line and to empty results."""
    uids: list[str] = []
    seen: set[str] = set()
    for line in lines or []:
        if isinstance(line, (bytes, bytearray)):
            try:
                text = line.decode("ascii", "ignore")
            except Exception:  # noqa: BLE001
                continue
        else:
            text = str(line)
        for tok in text.split():
            if tok.isdigit() and tok not in seen:
                seen.add(tok)
                uids.append(tok)
    return uids


def extract_rfc822(lines) -> bytes | None:
    """Pull the raw message bytes out of an IMAP FETCH response. aioimaplib
    delivers a literal as a ``bytearray`` element among the response lines (the
    other lines are the ``... FETCH (...`` envelope + the completion text). We
    return the longest bytearray; falling back to the longest bytes line that
    looks like a message. None if nothing message-like is present."""
    bytearrays = [ln for ln in (lines or []) if isinstance(ln, bytearray) and len(ln) > 0]
    if bytearrays:
        return bytes(max(bytearrays, key=len))
    # Fallback: some response shapes deliver the literal as bytes. Pick the
    # longest line that contains a header separator and isn't a status line.
    candidates = [
        ln for ln in (lines or [])
        if isinstance(ln, bytes) and b":" in ln and not ln.rstrip().endswith(b")")
    ]
    if candidates:
        return bytes(max(candidates, key=len))
    return None


def _quote(folder: str) -> str:
    """Quote an IMAP mailbox name for a command argument."""
    return '"' + folder.replace("\\", "\\\\").replace('"', '\\"') + '"'


class ImapProvider(MailboxProvider):
    def __init__(self, mailbox: Mailbox, idle_renew_seconds: int = 1500):
        super().__init__(mailbox.id)
        self._mailbox = mailbox
        self._idle_renew = idle_renew_seconds
        self._idle = None  # aioimaplib client dedicated to IDLE
        self._cmd = None  # aioimaplib client for SEARCH/FETCH/MOVE
        self._cmd_lock = asyncio.Lock()
        self._queue: asyncio.Queue[NewMessage] = asyncio.Queue()
        self._watch_task: asyncio.Task | None = None
        self._connected = False
        self._last_error: str | None = None
        self._known_folders: set[str] = set()

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def connected(self) -> bool:
        return self._connected

    # -- lifecycle --

    async def _new_client(self):
        import aioimaplib

        m = self._mailbox
        creds = m.credentials()
        if m.ssl:
            client = aioimaplib.IMAP4_SSL(host=m.host, port=m.port, timeout=30)
        else:
            client = aioimaplib.IMAP4(host=m.host, port=m.port, timeout=30)
        await client.wait_hello_from_server()
        resp = await client.login(creds.username, creds.password)
        if resp.result != "OK":
            raise RuntimeError(f"IMAP login failed: {resp.result} {resp.lines!r}")
        await client.select(m.inbox)
        return client

    async def connect(self) -> None:
        """Establish the command connection + ensure the move folders exist.
        Does NOT start IDLE (used by dry-run + as the first half of start())."""
        if self._cmd is None:
            self._cmd = await self._new_client()
        for folder in (self._mailbox.processed_folder, self._mailbox.failed_folder):
            await self._ensure_folder(folder)

    async def start(self) -> None:
        await self.connect()
        self._connected = True
        if self._watch_task is None:
            self._watch_task = asyncio.create_task(self._watch_with_reconnect())

    async def stop(self) -> None:
        self._connected = False
        if self._watch_task is not None:
            self._watch_task.cancel()
            self._watch_task = None
        for client in (self._idle, self._cmd):
            if client is not None:
                try:
                    await client.logout()
                except Exception:  # noqa: BLE001
                    pass
        self._idle = None
        self._cmd = None

    async def watch(self) -> AsyncIterator[NewMessage]:
        while True:
            yield await self._queue.get()

    # -- IDLE loop (event-driven) with reconnect/backoff --

    async def _watch_with_reconnect(self) -> None:
        attempt = 0
        while True:
            try:
                await self._idle_loop()  # runs until an error/disconnect
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._connected = False
                self._last_error = str(exc)
                logger.error("mailbox %s: IMAP watch disconnected: %s", self.mailbox_id, exc)
                if self._disconnect_hook is not None:
                    try:
                        await self._disconnect_hook(self.mailbox_id, str(exc))
                    except Exception:  # noqa: BLE001
                        pass
                await self._reset_idle_client()
                delay = reconnect_delay(attempt)
                attempt += 1
                await asyncio.sleep(delay)
                continue
            attempt = 0

    async def _reset_idle_client(self) -> None:
        if self._idle is not None:
            try:
                await self._idle.logout()
            except Exception:  # noqa: BLE001
                pass
            self._idle = None

    async def _idle_loop(self) -> None:
        """Enter IDLE, wait for a server push (or the renew timeout), then SEARCH
        UNSEEN on the command connection and enqueue new UIDs. Event-driven: the
        IDLE waits server-side until the server reports a change."""
        if self._idle is None:
            self._idle = await self._new_client()
        self._connected = True
        self._last_error = None
        while True:
            idle = await self._idle.idle_start(timeout=self._idle_renew)
            await self._idle.wait_server_push()
            if self._idle.has_pending_idle():
                self._idle.idle_done()
            await asyncio.wait_for(idle, timeout=15)
            # A wake means "something changed" — re-scan UNSEEN (this SEARCH is
            # triggered by the server's push, not a timer; not a poll).
            for uid in await self._search_unseen():
                self._queue.put_nowait(NewMessage(uid))

    async def _search_unseen(self) -> list[str]:
        async with self._cmd_lock:
            if self._cmd is None:
                self._cmd = await self._new_client()
            resp = await self._cmd.uid_search("UNSEEN")
        if resp.result != "OK":
            logger.warning("mailbox %s: UID SEARCH UNSEEN -> %s", self.mailbox_id, resp.result)
            return []
        return parse_uid_search(resp.lines)

    # -- provider API used by the engine --

    async def list_unseen(self) -> list[str]:
        return await self._search_unseen()

    async def fetch_raw(self, uid: str) -> bytes | None:
        async with self._cmd_lock:
            if self._cmd is None:
                self._cmd = await self._new_client()
            resp = await self._cmd.uid("fetch", uid, "(RFC822)")
        if resp.result != "OK":
            return None
        return extract_rfc822(resp.lines)

    async def move_message(self, uid: str, folder: str) -> None:
        async with self._cmd_lock:
            if self._cmd is None:
                self._cmd = await self._new_client()
            await self._ensure_folder(folder)
            # Prefer server-side MOVE (RFC 6851); fall back to COPY + \Deleted +
            # EXPUNGE so it works on servers without the MOVE capability.
            resp = await self._cmd.uid("move", uid, _quote(folder))
            if resp.result == "OK":
                return
            logger.info(
                "mailbox %s: UID MOVE unsupported (%s); COPY+EXPUNGE fallback",
                self.mailbox_id, resp.result,
            )
            copy = await self._cmd.uid("copy", uid, _quote(folder))
            if copy.result != "OK":
                raise RuntimeError(f"IMAP COPY failed: {copy.result} {copy.lines!r}")
            await self._cmd.uid("store", uid, "+FLAGS", "(\\Deleted)")
            try:
                await self._cmd.uid("expunge", uid)  # UIDPLUS — only this uid
            except Exception:  # noqa: BLE001 - server lacks UIDPLUS
                await self._cmd.expunge()

    async def mark_seen(self, uid: str) -> None:
        async with self._cmd_lock:
            if self._cmd is None:
                self._cmd = await self._new_client()
            await self._cmd.uid("store", uid, "+FLAGS", "(\\Seen)")

    async def _ensure_folder(self, folder: str) -> None:
        if folder in self._known_folders or self._cmd is None:
            self._known_folders.add(folder)
            return
        # CREATE is idempotent enough — a NO on an existing folder is fine.
        try:
            await self._cmd.create(_quote(folder))
        except Exception as exc:  # noqa: BLE001
            logger.debug("mailbox %s: CREATE %s -> %s (likely exists)", self.mailbox_id, folder, exc)
        self._known_folders.add(folder)
