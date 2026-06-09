"""Helpers to build raw RFC-822 messages for the attachment + engine tests."""

from __future__ import annotations

from email.message import EmailMessage


def build_message(
    *,
    subject="Re: Rechnung",
    sender="Acme GmbH <billing@acme.example>",
    message_id="<abc123@acme.example>",
    body="Bitte finden Sie die Rechnung im Anhang.",
    attachments=(),  # iterable of (filename, bytes, maintype, subtype)
    inline=(),  # iterable of (filename, bytes, maintype, subtype) — Content-Disposition inline
) -> bytes:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = "inbox@example.com"
    if message_id is not None:
        msg["Message-ID"] = message_id
    msg.set_content(body)
    for filename, data, maintype, subtype in attachments:
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)
    for filename, data, maintype, subtype in inline:
        msg.add_attachment(
            data, maintype=maintype, subtype=subtype, filename=filename,
            disposition="inline",
        )
    return msg.as_bytes()
