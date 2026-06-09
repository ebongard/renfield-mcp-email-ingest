#!/usr/bin/env python3
"""renfield-mcp-email-ingest — the FastMCP server.

Runs the streamable-http MCP server (read-only ops tools) AND launches one
event-driven IMAP-IDLE watcher daemon per configured mailbox (via an explicit
asyncio launch). The watch loop pushes email attachments into Renfield; the tools
expose connection + unseen-count visibility. Core logic lives in mcp-free modules
(config/registry/tools/engine); this file is the thin MCP + asyncio shell.

Env: RENFIELD_URL, RENFIELD_INGEST_TOKEN, EMAIL_MAILBOXES_YAML, EMAIL_* (see
config), EMAIL_MCP_HOST (default 0.0.0.0), EMAIL_MCP_PORT (default 8080).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from mcp.server.fastmcp import FastMCP

from . import tools as t
from .config import load_config
from .daemon import MailboxDaemonManager
from .notify import make_notifier

logging.basicConfig(
    level=os.environ.get("EMAIL_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,  # MCP stdio safety + container logs
)
logger = logging.getLogger("renfield-mcp-email-ingest")

_manager: MailboxDaemonManager | None = None


mcp = FastMCP(
    "renfield-mcp-email-ingest",
    host=os.environ.get("EMAIL_MCP_HOST", "0.0.0.0"),
    port=int(os.environ.get("EMAIL_MCP_PORT", "8080")),
)


def _reg() -> MailboxDaemonManager:
    if _manager is None:
        raise RuntimeError("daemon manager not initialised")
    return _manager


@mcp.tool()
async def list_mailboxes() -> dict:
    """List the configured email mailboxes and whether each is currently connected."""
    return await t.list_mailboxes(_reg())


@mcp.tool()
async def list_unseen(mailbox: str) -> dict:
    """List the UIDs of UNSEEN (unprocessed) messages in a mailbox's inbox."""
    return await t.list_unseen(_reg(), mailbox)


async def _serve() -> None:
    """Launch the watcher daemons AND the streamable-http MCP server in one event
    loop. The daemons must run at process startup (the auto-push path is the
    primary job) — FastMCP's ``lifespan`` is the per-session MCP-protocol lifespan,
    NOT the ASGI startup hook, so we start the daemons explicitly here."""
    global _manager
    config = load_config()
    notifier = make_notifier(
        os.environ.get("EMAIL_NOTIFY_WEBHOOK_URL") or None,
        os.environ.get("EMAIL_NOTIFY_WEBHOOK_TOKEN") or None,
    )
    _manager = MailboxDaemonManager(config, notifier=notifier)
    await _manager.start()
    logger.info("watch daemons started for mailboxes: %s", _manager.names())
    try:
        await mcp.run_streamable_http_async()
    finally:
        await _manager.stop()


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
