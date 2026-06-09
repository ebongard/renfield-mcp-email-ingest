"""The local accept/reject gate for an individual attachment, shared by the
engine (before any push) and the dry-run. One definition so they never diverge.

An attachment that fails the gate is NOISE (an inline logo saved as a part, a
signature image, an oversize blob) — it is skipped, never pushed, and does NOT
by itself fail the email. Only a backend push FAILED, or an email whose
attachments are *all* gate-rejected, dispositions the email to ``failed`` (see
``engine.py``)."""

from __future__ import annotations

from .config import Config


def classify(config: Config, filename: str, size: int) -> tuple[bool, str | None]:
    """Return ``(accepted, reason)`` for one attachment. A rejected attachment is
    terminal (it won't become valid) → the engine skips it without pushing."""
    if size == 0:
        return False, "empty"
    if size > config.max_file_size_bytes:
        return False, "oversize"
    if not config.extension_allowed(filename):
        return False, "extension_not_allowed"
    return True, None