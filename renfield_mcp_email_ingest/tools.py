"""Interactive MCP tool implementations (mcp-free, so they're unit-testable).

Read-only ops/visibility tools: which mailboxes are configured + connected, and
what is currently UNSEEN (unprocessed) in a mailbox. These never drive the watch
loop — that's event-driven via IMAP IDLE.
"""

from __future__ import annotations

from .daemon import MailboxDaemonManager


def _err(msg: str) -> dict:
    return {"error": msg}


async def list_mailboxes(reg: MailboxDaemonManager) -> dict:
    mailboxes = []
    for name in reg.names():
        p = reg.get(name)
        mailboxes.append(
            {
                "id": name,
                "connected": p.connected if p else False,
                "last_error": p.last_error if p else "unknown mailbox",
            }
        )
    return {"mailboxes": mailboxes}


async def list_unseen(reg: MailboxDaemonManager, mailbox: str) -> dict:
    provider = reg.get(mailbox)
    if provider is None:
        return _err(f"unknown mailbox: {mailbox!r}")
    uids = await provider.list_unseen()
    return {"mailbox": mailbox, "unseen_uids": uids, "count": len(uids)}
