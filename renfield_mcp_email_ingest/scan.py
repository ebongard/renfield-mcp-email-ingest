"""Dry-run scanner CLI — connect to each configured mailbox, list the UNSEEN
messages, parse their attachments, and report what WOULD be pushed vs skipped
(and why). NO side effects: it never pushes, moves, or flags anything. Use it to
validate ``mailboxes.yaml`` + credentials + the gate before going live.

    renfield-mcp-email-ingest-scan            # all mailboxes
    renfield-mcp-email-ingest-scan <id>       # one mailbox
"""

from __future__ import annotations

import asyncio
import logging
import sys

from .attachments import parse_message
from .config import Config, load_config
from .gate import classify
from .registry import make_provider

logger = logging.getLogger("renfield-mcp-email-ingest.scan")


async def scan_mailbox(config: Config, mailbox_id: str) -> dict:
    mailbox = config.mailbox_by_id(mailbox_id)
    if mailbox is None:
        return {"mailbox": mailbox_id, "error": "unknown mailbox"}
    provider = make_provider(mailbox, config.idle_renew_seconds)
    result: dict = {"mailbox": mailbox_id, "messages": []}
    try:
        await provider.connect()
        uids = await provider.list_unseen()
        result["unseen_count"] = len(uids)
        for uid in uids:
            raw = await provider.fetch_raw(uid)
            if raw is None:
                continue
            parsed = parse_message(raw)
            atts = []
            for att in parsed.attachments:
                ok, reason = classify(config, att.filename, len(att.content))
                atts.append(
                    {
                        "filename": att.filename,
                        "size": len(att.content),
                        "accepted": ok,
                        "reason": reason,
                    }
                )
            result["messages"].append(
                {
                    "uid": uid,
                    "message_id": parsed.message_id,
                    "subject": parsed.subject,
                    "attachments": atts,
                }
            )
    except Exception as exc:  # noqa: BLE001
        result["error"] = str(exc)
    finally:
        await provider.stop()
    return result


async def _run(argv: list[str]) -> int:
    config = load_config()
    targets = argv or [m.id for m in config.mailboxes]
    if not targets:
        print("no mailboxes configured (set EMAIL_MAILBOXES_YAML)")
        return 1
    for mailbox_id in targets:
        res = await scan_mailbox(config, mailbox_id)
        print(f"\n=== mailbox: {res['mailbox']} ===")
        if res.get("error"):
            print(f"  ERROR: {res['error']}")
            continue
        print(f"  unseen messages: {res.get('unseen_count', 0)}")
        for msg in res["messages"]:
            print(f"  - uid {msg['uid']}: {msg['subject']!r}")
            for a in msg["attachments"]:
                mark = "PUSH" if a["accepted"] else f"SKIP ({a['reason']})"
                print(f"      [{mark}] {a['filename']} ({a['size']} B)")
    return 0


def main() -> None:
    logging.basicConfig(level="INFO", format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    raise SystemExit(asyncio.run(_run(sys.argv[1:])))


if __name__ == "__main__":
    main()
