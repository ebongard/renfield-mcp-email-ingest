from renfield_mcp_email_ingest.config import Config
from renfield_mcp_email_ingest.gate import classify


def _cfg(max_mb=50):
    return Config(
        renfield_url="http://x", ingest_token="t",
        allowed_extensions=("pdf", "txt"), max_file_size_mb=max_mb,
    )


def test_accepts_allowed_pdf():
    assert classify(_cfg(), "a.pdf", 100) == (True, None)


def test_rejects_empty():
    assert classify(_cfg(), "a.pdf", 0) == (False, "empty")


def test_rejects_oversize():
    ok, reason = classify(_cfg(max_mb=1), "a.pdf", 2 * 1024 * 1024)
    assert ok is False and reason == "oversize"


def test_rejects_bad_extension():
    ok, reason = classify(_cfg(), "a.exe", 100)
    assert ok is False and reason == "extension_not_allowed"
