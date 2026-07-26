import asyncio

import pytest

from renfield_mcp_email_ingest.config import Config, Mailbox
from renfield_mcp_email_ingest.contract import AttachmentMove, MessageDisposition
from renfield_mcp_email_ingest.engine import MessageEngine, aggregate
from renfield_mcp_email_ingest.pusher import PushOutcome

from .fakes import FakePusher, FakeProvider
from .msgbuild import build_message

PDF = ("rechnung.pdf", b"%PDF-1.4 hello", "application", "pdf")


def _cfg(max_mb=50):
    return Config(
        renfield_url="http://x", ingest_token="t",
        allowed_extensions=("pdf", "txt"), max_file_size_mb=max_mb,
    )


def _mbox():
    return Mailbox(
        id="mbox", host="h", username_env="U", password_env="P",
        processed_folder="Verarbeitet", failed_folder="Fehler",
    )


def _engine(provider, pusher, *, on_failed=None, on_fatal=None, max_retries=3, retry_base=0.001):
    return MessageEngine(
        config=_cfg(), mailbox=_mbox(), provider=provider, pusher=pusher,
        on_failed=on_failed, on_fatal=on_fatal,
        max_retries=max_retries, retry_base_seconds=retry_base,
    )


def _ok(status="ingested"):
    return PushOutcome(move=AttachmentMove.PROCESSED, status=status, document_id=1)


# ---- aggregate() unit table -------------------------------------------------

def test_aggregate_no_attachments_is_skip():
    assert aggregate([], had_attachments=False, all_rejected=False) is MessageDisposition.SKIP


def test_aggregate_all_rejected_is_failed():
    assert aggregate([], had_attachments=True, all_rejected=True) is MessageDisposition.FAILED


def test_aggregate_processed_wins_over_rejected_noise():
    out = [_ok()]
    assert aggregate(out, had_attachments=True, all_rejected=False) is MessageDisposition.PROCESSED


def test_aggregate_retry_beats_processed():
    out = [_ok(), PushOutcome(move=AttachmentMove.LEAVE)]
    assert aggregate(out, had_attachments=True, all_rejected=False) is MessageDisposition.LEAVE


def test_aggregate_failed_when_all_failed():
    out = [PushOutcome(move=AttachmentMove.FAILED, status="failed")]
    assert aggregate(out, had_attachments=True, all_rejected=False) is MessageDisposition.FAILED


def test_aggregate_processed_beats_failed():
    # one doc ingested, another rejected by backend → email still counts as done
    out = [_ok(), PushOutcome(move=AttachmentMove.FAILED, status="failed")]
    assert aggregate(out, had_attachments=True, all_rejected=False) is MessageDisposition.PROCESSED


# ---- engine flow ------------------------------------------------------------

async def test_single_attachment_ingested_moves_to_processed():
    raw = build_message(attachments=[PDF])
    prov = FakeProvider({"1": raw})
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng._dispatch("1")
    assert prov.moved == [("1", "Verarbeitet")]
    assert len(push.calls) == 1
    assert push.calls[0]["filename"] == "rechnung.pdf"
    assert push.calls[0]["mailbox_id"] == "mbox"
    assert len(push.calls[0]["sha256"]) == 64


async def test_failed_push_moves_to_failed_and_notifies():
    raw = build_message(attachments=[PDF])
    prov = FakeProvider({"1": raw})
    failures = []

    async def on_failed(mbox, ref, reason):
        failures.append((mbox, ref, reason))

    eng = _engine(prov, FakePusher(PushOutcome(move=AttachmentMove.FAILED, status="failed")), on_failed=on_failed)
    await eng._dispatch("1")
    assert prov.moved == [("1", "Fehler")]
    assert failures and failures[0][2] == "ingest_failed"


async def test_no_attachments_is_skipped_seen_not_moved():
    raw = build_message(attachments=[])
    prov = FakeProvider({"1": raw})
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng._dispatch("1")
    assert prov.moved == []  # never moved — not our concern
    assert prov.seen == ["1"]  # flagged \Seen so reconciliation skips it
    assert push.calls == []


async def test_mixed_ingested_plus_junk_attachment():
    # A real PDF + a junk .exe (gate-rejected). The PDF ingests; the .exe is not
    # pushed and does NOT fail the email → moved to processed, 1 push only.
    raw = build_message(attachments=[PDF, ("malware.exe", b"MZxx", "application", "octet-stream")])
    prov = FakeProvider({"1": raw})
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng._dispatch("1")
    assert prov.moved == [("1", "Verarbeitet")]
    assert len(push.calls) == 1 and push.calls[0]["filename"] == "rechnung.pdf"


async def test_all_attachments_rejected_moves_to_failed():
    raw = build_message(attachments=[("a.exe", b"MZ", "application", "octet-stream")])
    prov = FakeProvider({"1": raw})
    failures = []

    async def on_failed(mbox, ref, reason):
        failures.append(reason)

    eng = _engine(prov, FakePusher(_ok()), on_failed=on_failed)
    await eng._dispatch("1")
    assert prov.moved == [("1", "Fehler")]
    assert failures == ["all_attachments_rejected"]


async def test_multiple_attachments_all_ingested():
    raw = build_message(attachments=[
        ("a.pdf", b"%PDF a", "application", "pdf"),
        ("b.txt", b"hello txt", "text", "plain"),
    ])
    prov = FakeProvider({"1": raw})
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng._dispatch("1")
    assert prov.moved == [("1", "Verarbeitet")]
    assert len(push.calls) == 2


async def test_retry_then_exhausts_and_notifies():
    raw = build_message(attachments=[PDF])
    prov = FakeProvider({"1": raw})
    push = FakePusher(PushOutcome(move=AttachmentMove.LEAVE))  # always transient
    failures = []

    async def on_failed(mbox, ref, reason):
        failures.append(reason)

    eng = _engine(prov, push, on_failed=on_failed, max_retries=3, retry_base=0.001)
    await eng._dispatch("1")
    await asyncio.sleep(0.05)
    assert len(push.calls) == 3  # attempt 0 + 2 retries
    assert prov.moved == []  # never moved — left UNSEEN in inbox
    assert prov.seen == []  # not marked seen either — must re-surface
    assert failures and "retry_exhausted" in failures[0]


async def test_retry_exhausted_uid_is_parked_not_relooped():
    # After exhausting retries, a re-emit of the SAME uid (what IDLE does on every
    # wake) must be ignored — no fresh retry ladder, no repeat notification.
    raw = build_message(attachments=[PDF])
    prov = FakeProvider({"1": raw})
    push = FakePusher(PushOutcome(move=AttachmentMove.LEAVE))  # always transient
    failures = []

    async def on_failed(mbox, ref, reason):
        failures.append(reason)

    eng = _engine(prov, push, on_failed=on_failed, max_retries=3, retry_base=0.001)
    await eng._dispatch("1")
    await asyncio.sleep(0.05)
    calls_after_exhaust = len(push.calls)
    await eng._dispatch("1")  # IDLE re-emits the still-UNSEEN uid
    await asyncio.sleep(0.02)
    assert len(push.calls) == calls_after_exhaust  # parked — no new attempts
    assert len(failures) == 1  # notified exactly once


async def test_mark_seen_failure_parks_message():
    # A plain email (SKIP). If mark_seen fails, the message must be parked, not
    # re-dispatched forever, and the failure must not cascade into a retry push.
    class _SeenFails(FakeProvider):
        async def mark_seen(self, uid):
            raise RuntimeError("STORE failed")

    raw = build_message(attachments=[])
    prov = _SeenFails({"1": raw})
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng._dispatch("1")
    await asyncio.sleep(0.02)
    assert push.calls == [] and prov.moved == []
    await eng._dispatch("1")  # re-emit — parked, ignored
    assert push.calls == []


async def test_fatal_stops_mailbox_and_notifies():
    raw = build_message(attachments=[PDF])
    prov = FakeProvider({"1": raw})
    push = FakePusher(PushOutcome(move=AttachmentMove.LEAVE, fatal=True, http_status=403))
    fatals = []

    async def on_fatal(mbox, reason):
        fatals.append((mbox, reason))

    eng = _engine(prov, push, on_fatal=on_fatal)
    await eng._dispatch("1")
    assert fatals == [("mbox", "http_403")]
    assert prov.moved == [] and prov.seen == []  # fatal → message untouched


async def test_fatal_on_second_attachment_stops_before_pushing_third():
    raw = build_message(attachments=[
        ("a.pdf", b"%PDF a", "application", "pdf"),
        ("b.pdf", b"%PDF b", "application", "pdf"),
        ("c.pdf", b"%PDF c", "application", "pdf"),
    ])
    prov = FakeProvider({"1": raw})
    push = FakePusher(by_name={
        "a.pdf": _ok(),
        "b.pdf": PushOutcome(move=AttachmentMove.LEAVE, fatal=True, http_status=401),
        "c.pdf": _ok(),
    })
    fatals = []

    async def on_fatal(mbox, reason):
        fatals.append(reason)

    eng = _engine(prov, push, on_fatal=on_fatal)
    await eng._dispatch("1")
    assert [c["filename"] for c in push.calls] == ["a.pdf", "b.pdf"]  # c never pushed
    assert fatals == ["http_401"] and prov.moved == []


async def test_dispatch_dedup_same_uid_processed_once():
    raw = build_message(attachments=[PDF])
    prov = FakeProvider({"1": raw})
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng._dispatch("1")
    await eng._dispatch("1")  # second event for the same uid — already moved
    assert len(push.calls) == 1 and prov.moved == [("1", "Verarbeitet")]


async def test_fetch_returns_none_releases_inflight():
    prov = FakeProvider({})  # uid not present
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng._dispatch("99")
    assert push.calls == [] and prov.moved == []


@pytest.mark.parametrize("bad", ["", "   "])
async def test_empty_uid_ignored(bad):
    prov = FakeProvider({"1": build_message(attachments=[PDF])})
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng._dispatch(bad)
    assert push.calls == [] and prov.moved == []


async def test_reconcile_dispatches_unseen():
    raw = build_message(attachments=[PDF])
    prov = FakeProvider({"1": raw}, unseen=["1"], events=[])
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng.run()  # start → reconcile → watch (no events) → return
    assert prov.started is True
    assert prov.moved == [("1", "Verarbeitet")]


async def test_run_processes_watch_events():
    prov = FakeProvider(
        {"1": build_message(attachments=[("a.pdf", b"%PDF a", "application", "pdf")]),
         "2": build_message(attachments=[("b.pdf", b"%PDF b", "application", "pdf")])},
        events=["1", "2"],
    )
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    await eng.run()
    assert sorted(prov.moved) == [("1", "Verarbeitet"), ("2", "Verarbeitet")]

# ---- backend-recovery recover() ---------------------------------------------

@pytest.mark.asyncio
async def test_recover_unparks_exhausted_and_redispatches():
    """A message parked in _exhausted during a backend outage is un-parked and
    re-pushed by recover() (still UNSEEN → re-listed), no restart needed."""
    raw = build_message(attachments=[PDF])
    prov = FakeProvider({"1": raw}, unseen=["1"])
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    eng._exhausted.add("1")  # exhausted its retries during the outage
    await eng.recover()
    assert "1" not in eng._exhausted           # un-parked
    assert len(push.calls) == 1                 # re-dispatched + pushed
    assert ("1", "Verarbeitet") in prov.moved   # moved out of the inbox on success


@pytest.mark.asyncio
async def test_recover_noop_when_stopped():
    """recover() must not act on a stopped engine (avoid racing teardown)."""
    prov = FakeProvider({"1": build_message(attachments=[PDF])}, unseen=["1"])
    push = FakePusher(_ok())
    eng = _engine(prov, push)
    eng._exhausted.add("1")
    eng._stopped.set()
    await eng.recover()
    assert eng._exhausted == {"1"}   # untouched
    assert push.calls == []          # nothing dispatched
