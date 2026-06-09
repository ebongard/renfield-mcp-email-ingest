import httpx
import pytest

import renfield_mcp_email_ingest.notify as notify_mod
from renfield_mcp_email_ingest.notify import LogNotifier, WebhookNotifier, make_notifier


def test_make_notifier_selects_impl():
    assert isinstance(make_notifier(None), LogNotifier)
    assert isinstance(make_notifier("http://hook"), WebhookNotifier)


def _patch_transport(monkeypatch, handler):
    real = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs.pop("timeout", None)
        return real(transport=httpx.MockTransport(handler), timeout=5)

    monkeypatch.setattr(notify_mod.httpx, "AsyncClient", _factory)


async def test_webhook_posts_event(monkeypatch):
    seen = {}

    def handler(request):
        seen["body"] = request.content.decode()
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={})

    _patch_transport(monkeypatch, handler)
    n = WebhookNotifier("http://hook", token="tk")
    await n.failure("mbox", "uid7", "ingest_failed")
    assert "renfield-mcp-email-ingest" in seen["body"]
    assert "ingest_failed" in seen["body"]
    assert seen["auth"] == "Bearer tk"


async def test_webhook_failure_never_raises(monkeypatch):
    def handler(request):
        raise httpx.ConnectError("down")

    _patch_transport(monkeypatch, handler)
    n = WebhookNotifier("http://hook")
    # must not raise — operator notify must never break the ingest path
    await n.fatal("mbox", "http_403")
    await n.disconnect("mbox", "timeout")
