import json

import httpx
import pytest

import renfield_mcp_email_ingest.pusher as pusher_mod
from renfield_mcp_email_ingest.contract import CONTRACT_HEADER, AttachmentMove
from renfield_mcp_email_ingest.pusher import RenfieldPusher


def _patch_transport(monkeypatch, handler):
    real = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs.pop("timeout", None)
        return real(transport=httpx.MockTransport(handler), timeout=5)

    monkeypatch.setattr(pusher_mod.httpx, "AsyncClient", _factory)


def _pusher():
    return RenfieldPusher("http://renfield", "tok", timeout_seconds=5)


async def _push(p):
    return await p.push(
        file_bytes=b"%PDF", filename="a.pdf", mailbox_id="buchhaltung",
        message_id="<m1@x>", sha256="deadbeef", mime="application/pdf",
        sender="Acme <b@acme>", subject="Rechnung",
    )


@pytest.mark.parametrize("status,expected", [
    ("ingested", AttachmentMove.PROCESSED),
    ("duplicate", AttachmentMove.PROCESSED),
    ("failed", AttachmentMove.FAILED),
    ("retry", AttachmentMove.LEAVE),
])
async def test_200_status_maps_to_move(monkeypatch, status, expected):
    def handler(request):
        assert request.headers["authorization"] == "Bearer tok"
        assert request.headers[CONTRACT_HEADER.lower()] == "1"
        return httpx.Response(200, json={"status": status, "document_id": 7, "contract_version": "1"})

    _patch_transport(monkeypatch, handler)
    out = await _push(_pusher())
    assert out.move is expected and out.status == status and out.fatal is False
    if expected is AttachmentMove.PROCESSED:
        assert out.document_id == 7


async def test_metadata_carries_mailbox_and_message(monkeypatch):
    captured = {}

    def handler(request):
        body = request.content.decode("latin-1")
        start = body.index("{")
        # find the matching brace for the metadata object (no nested braces here)
        captured["meta"] = json.loads(body[start:body.index("}", start) + 1])
        return httpx.Response(200, json={"status": "ingested", "contract_version": "1"})

    _patch_transport(monkeypatch, handler)
    await _push(_pusher())
    m = captured["meta"]
    assert m["filename"] == "a.pdf"
    assert m["mailbox_id"] == "buchhaltung"
    assert m["message_id"] == "<m1@x>"
    assert m["sha256"] == "deadbeef"
    assert m["sender"] == "Acme <b@acme>"
    assert m["subject"] == "Rechnung"
    # The watcher must NEVER send tier/owner — those are server-authoritative.
    assert "tier" not in m and "owner" not in m


@pytest.mark.parametrize("code", [401, 403])
async def test_401_403_is_fatal(monkeypatch, code):
    _patch_transport(monkeypatch, lambda req: httpx.Response(code, text="no"))
    out = await _push(_pusher())
    assert out.fatal is True and out.move is AttachmentMove.LEAVE


async def test_503_is_leave(monkeypatch):
    _patch_transport(monkeypatch, lambda req: httpx.Response(503, json={"detail": {"reason": "worker_unavailable"}}))
    out = await _push(_pusher())
    assert out.move is AttachmentMove.LEAVE and out.fatal is False


async def test_network_error_is_leave(monkeypatch):
    def handler(request):
        raise httpx.ConnectError("down")

    _patch_transport(monkeypatch, handler)
    out = await _push(_pusher())
    assert out.move is AttachmentMove.LEAVE and "transport_error" in (out.detail or "")


async def test_200_unparseable_body_is_leave(monkeypatch):
    _patch_transport(monkeypatch, lambda req: httpx.Response(200, text="not json"))
    out = await _push(_pusher())
    assert out.move is AttachmentMove.LEAVE
