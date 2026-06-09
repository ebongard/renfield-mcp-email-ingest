"""Configuration: global settings from env + the watched MAILBOXES from a mounted
``mailboxes.yaml`` (so the YAML can be a ConfigMap and IMAP credentials stay in a
Secret, referenced by env-var NAME). Mailboxes are reloadable at runtime — the
loader is pure, so the daemon can re-read on a file change without a redeploy.

CRITICAL SECURITY INVARIANT (mirrors the backend's design): the watcher config
holds ONLY the IMAP connection + a routing ``id`` per mailbox. It does NOT hold
owner / tier / knowledge-base — those are *server-authoritative* (the backend
resolves ``id`` → owner/tier/kb). So a leaked push token can never escalate a
mailbox's filing tier, and this YAML is safe to keep in a ConfigMap.
"""

from __future__ import annotations

import os

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

DEFAULT_EXTENSIONS = "pdf,docx,doc,txt,md,html,pptx,xlsx,png,jpg,jpeg"
# RFC 2177: a server may drop an IDLE that runs longer than ~30 min. Re-issue
# well within that. The watcher exits + re-enters IDLE on this interval.
DEFAULT_IDLE_RENEW_SECONDS = 1500  # 25 min


class ImapCredentials(BaseModel):
    username: str
    password: str


class Mailbox(BaseModel):
    """One watched IMAP mailbox. ``id`` is the routing key sent to the backend —
    NOT owner/tier (those are server-side). Credentials are referenced by ENV-VAR
    NAME (kept in a Secret), never inlined."""

    id: str
    host: str
    port: int = 993
    ssl: bool = True
    username_env: str
    password_env: str
    inbox: str = "INBOX"
    # IMAP folders the watcher moves messages into by the aggregated outcome.
    # They are created on connect if missing.
    processed_folder: str = "Verarbeitet"
    failed_folder: str = "Fehler"

    @field_validator("id")
    @classmethod
    def _id_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("mailbox id must be non-empty")
        return v.strip()

    @model_validator(mode="after")
    def _folders_distinct(self):
        if self.processed_folder == self.failed_folder:
            raise ValueError("processed_folder and failed_folder must differ")
        if self.inbox in (self.processed_folder, self.failed_folder):
            raise ValueError("inbox must differ from processed_folder/failed_folder")
        return self

    def credentials(self) -> ImapCredentials:
        """Resolve the referenced env vars at use time. Raises if a referenced
        var is unset — fail loud rather than attempt an anonymous login."""
        username = os.environ.get(self.username_env)
        password = os.environ.get(self.password_env)
        if not username or not password:
            missing = [
                e
                for e, v in ((self.username_env, username), (self.password_env, password))
                if not v
            ]
            raise ValueError(
                f"mailbox {self.id!r}: credential env var(s) unset: {missing}"
            )
        return ImapCredentials(username=username, password=password)


class MailboxesFile(BaseModel):
    mailboxes: list[Mailbox] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_ids(self):
        ids = [m.id for m in self.mailboxes]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"duplicate mailbox ids: {sorted(dupes)}")
        return self


class Config(BaseModel):
    renfield_url: str
    ingest_token: str
    allowed_extensions: tuple[str, ...]
    max_file_size_mb: int = 50
    push_timeout_seconds: float = 120.0
    idle_renew_seconds: int = DEFAULT_IDLE_RENEW_SECONDS
    mailboxes_path: str | None = None  # the mounted mailboxes.yaml (for reload)
    mailboxes: list[Mailbox] = Field(default_factory=list)

    @property
    def max_file_size_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024

    def extension_allowed(self, filename: str) -> bool:
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        return bool(ext) and ext in self.allowed_extensions

    def mailbox_by_id(self, mailbox_id: str) -> Mailbox | None:
        return next((m for m in self.mailboxes if m.id == mailbox_id), None)


def _parse_extensions(raw: str) -> tuple[str, ...]:
    return tuple(e.strip().lower() for e in raw.split(",") if e.strip())


def load_mailboxes(mailboxes_path: str) -> list[Mailbox]:
    """Parse + validate the mailboxes YAML. Raises on malformed config (fail loud
    at startup / reload rather than watch nothing silently)."""
    with open(mailboxes_path) as f:
        data = yaml.safe_load(f) or {}
    return MailboxesFile.model_validate(data).mailboxes


def load_config() -> Config:
    """Build the Config from env (+ the mailboxes YAML if ``EMAIL_MAILBOXES_YAML``
    is set)."""
    renfield_url = os.environ.get("RENFIELD_URL", "").rstrip("/")
    if not renfield_url:
        raise ValueError("RENFIELD_URL is required")
    ingest_token = os.environ.get("RENFIELD_INGEST_TOKEN", "")
    if not ingest_token:
        raise ValueError("RENFIELD_INGEST_TOKEN is required")

    mailboxes_path = os.environ.get("EMAIL_MAILBOXES_YAML") or None
    mailboxes = load_mailboxes(mailboxes_path) if mailboxes_path else []

    return Config(
        renfield_url=renfield_url,
        ingest_token=ingest_token,
        allowed_extensions=_parse_extensions(
            os.environ.get("EMAIL_ALLOWED_EXTENSIONS", DEFAULT_EXTENSIONS)
        ),
        max_file_size_mb=int(os.environ.get("EMAIL_MAX_FILE_SIZE_MB", "50")),
        push_timeout_seconds=float(os.environ.get("EMAIL_PUSH_TIMEOUT_SECONDS", "120")),
        idle_renew_seconds=int(
            os.environ.get("EMAIL_IDLE_RENEW_SECONDS", str(DEFAULT_IDLE_RENEW_SECONDS))
        ),
        mailboxes_path=mailboxes_path,
        mailboxes=mailboxes,
    )