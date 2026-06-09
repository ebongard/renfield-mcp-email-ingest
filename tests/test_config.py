import pytest
from pydantic import ValidationError

from renfield_mcp_email_ingest.config import (
    Config,
    Mailbox,
    MailboxesFile,
    load_mailboxes,
)


def _mbox(**kw):
    base = dict(
        id="buchhaltung", host="imap.example.com",
        username_env="U", password_env="P",
    )
    base.update(kw)
    return Mailbox(**base)


def test_defaults():
    m = _mbox()
    assert m.port == 993 and m.ssl is True and m.inbox == "INBOX"
    assert m.processed_folder == "Verarbeitet" and m.failed_folder == "Fehler"


def test_empty_id_rejected():
    with pytest.raises(ValidationError):
        _mbox(id="  ")


def test_processed_failed_must_differ():
    with pytest.raises(ValidationError):
        _mbox(processed_folder="Done", failed_folder="Done")


def test_inbox_must_differ_from_move_folders():
    with pytest.raises(ValidationError):
        _mbox(inbox="Verarbeitet")


def test_duplicate_ids_rejected():
    with pytest.raises(ValidationError):
        MailboxesFile(mailboxes=[_mbox(id="x"), _mbox(id="x")])


def test_credentials_resolved_from_env(monkeypatch):
    monkeypatch.setenv("U", "user1")
    monkeypatch.setenv("P", "pass1")
    creds = _mbox().credentials()
    assert creds.username == "user1" and creds.password == "pass1"


def test_credentials_missing_env_raises(monkeypatch):
    monkeypatch.delenv("U", raising=False)
    monkeypatch.delenv("P", raising=False)
    with pytest.raises(ValueError, match="credential env var"):
        _mbox().credentials()


def test_extension_allowed():
    cfg = Config(
        renfield_url="http://x", ingest_token="t",
        allowed_extensions=("pdf", "txt"),
    )
    assert cfg.extension_allowed("a.PDF") is True
    assert cfg.extension_allowed("a.exe") is False
    assert cfg.extension_allowed("noext") is False


def test_load_mailboxes(tmp_path):
    p = tmp_path / "mailboxes.yaml"
    p.write_text(
        "mailboxes:\n"
        "  - id: buchhaltung\n"
        "    host: imap.example.com\n"
        "    username_env: U\n"
        "    password_env: P\n"
    )
    mbs = load_mailboxes(str(p))
    assert len(mbs) == 1 and mbs[0].id == "buchhaltung"


def test_load_mailboxes_malformed_raises(tmp_path):
    p = tmp_path / "mailboxes.yaml"
    p.write_text("mailboxes:\n  - id: x\n")  # missing host/creds
    with pytest.raises(ValidationError):
        load_mailboxes(str(p))
