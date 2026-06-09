"""Provider factory — maps a Mailbox config to its MailboxProvider impl."""

from __future__ import annotations

from .config import Mailbox
from .providers.base import MailboxProvider


def make_provider(mailbox: Mailbox, idle_renew_seconds: int = 1500) -> MailboxProvider:
    from .providers.imap import ImapProvider

    return ImapProvider(mailbox, idle_renew_seconds=idle_renew_seconds)
