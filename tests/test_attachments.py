import hashlib

from renfield_mcp_email_ingest.attachments import parse_message

from .msgbuild import build_message


def test_extracts_single_pdf_attachment():
    raw = build_message(attachments=[("rechnung.pdf", b"%PDF-1.4 data", "application", "pdf")])
    parsed = parse_message(raw)
    assert parsed.message_id == "<abc123@acme.example>"
    assert "billing@acme.example" in parsed.sender
    assert parsed.subject == "Re: Rechnung"
    assert len(parsed.attachments) == 1
    att = parsed.attachments[0]
    assert att.filename == "rechnung.pdf"
    assert att.content == b"%PDF-1.4 data"
    assert att.mime == "application/pdf"
    assert att.sha256 == hashlib.sha256(b"%PDF-1.4 data").hexdigest()


def test_extracts_multiple_attachments():
    raw = build_message(attachments=[
        ("a.pdf", b"%PDF a", "application", "pdf"),
        ("b.docx", b"docx-bytes", "application", "vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ])
    parsed = parse_message(raw)
    names = sorted(a.filename for a in parsed.attachments)
    assert names == ["a.pdf", "b.docx"]


def test_plain_body_is_not_an_attachment():
    raw = build_message(attachments=[])
    parsed = parse_message(raw)
    assert parsed.attachments == ()


def test_inline_images_are_skipped():
    # A real PDF attachment + an inline logo. Only the PDF should surface.
    raw = build_message(
        attachments=[("rechnung.pdf", b"%PDF", "application", "pdf")],
        inline=[("logo.png", b"\x89PNG logo", "image", "png")],
    )
    parsed = parse_message(raw)
    assert [a.filename for a in parsed.attachments] == ["rechnung.pdf"]


def test_inline_pdf_is_kept():
    # An inline-disposition PDF (Apple Mail / forwards) is a REAL document and
    # must be ingested, not dropped as if it were an inline image.
    raw = build_message(
        body="see attached",
        inline=[("vertrag.pdf", b"%PDF inline", "application", "pdf")],
    )
    parsed = parse_message(raw)
    assert [a.filename for a in parsed.attachments] == ["vertrag.pdf"]


def test_missing_message_id_is_fabricated_and_stable():
    raw = build_message(message_id=None, attachments=[("x.pdf", b"%PDF", "application", "pdf")])
    a = parse_message(raw)
    b = parse_message(raw)
    assert a.message_id.startswith("<sha256:")
    assert a.message_id == b.message_id  # stable across re-parse (ledger key safety)


def test_malformed_bytes_do_not_raise():
    parsed = parse_message(b"this is not a real mime message at all")
    assert parsed.attachments == ()