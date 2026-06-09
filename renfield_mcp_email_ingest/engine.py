"""Per-mailbox ingest engine: consumes the provider's event-driven ``watch()``
and drives each new message through fetch → parse → per-attachment gate → push →
**aggregate** → move/flag the message.

Detection is event-driven (NEVER a poll loop). Two non-poll complements (mirror
the folder-ingest engine):
  1. **One-shot startup reconciliation** — IMAP IDLE only reports messages that
     arrive AFTER the watch starts, so UNSEEN messages already in the inbox at
     boot would be invisible. We enumerate UNSEEN ONCE at startup and dispatch
     them, then rely purely on IDLE events. A single catch-up, not a periodic scan.
  2. **Bounded per-message retry** — a transient outcome (worker down / in-flight)
     leaves the message UNSEEN in the inbox and re-attempts THAT message on an
     exponential backoff. After the cap we give up and leave it (the next
     restart's reconciliation re-tries).

The unit is an EMAIL, but one email fans out into N attachment pushes. We push
every accepted attachment, then AGGREGATE the per-attachment outcomes into a
single message disposition (precedence: fatal > retry/leave > processed > failed
> all-rejected). Re-pushing a whole message is idempotent: the backend dedups on
(content-hash, kb), so an already-ingested attachment returns ``duplicate``.

Per-attachment size + extension gating happens HERE, before any push. A
gate-rejected attachment is NOISE (an inline logo, a signature image) — skipped,
never pushed, and it does NOT fail the email; only a backend push FAILED, or an
email whose attachments are ALL gate-rejected, dispositions the email to failed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from .attachments import parse_message
from .config import Config, Mailbox
from .contract import AttachmentMove, MessageDisposition
from .gate import classify
from .providers.base import MailboxProvider
from .pusher import PushOutcome, RenfieldPusher

logger = logging.getLogger("renfield-mcp-email-ingest.engine")

# Operator-notification callback: (mailbox_id, ref, reason) -> awaitable.
FailureHook = Callable[[str, str, str], Awaitable[None]]
# Fatal (token/config) callback: (mailbox_id, reason) -> awaitable.
FatalHook = Callable[[str, str], Awaitable[None]]


def aggregate(outcomes: list[PushOutcome], *, had_attachments: bool, all_rejected: bool) -> MessageDisposition:
    """Fold per-attachment push outcomes into one message disposition.

    ``outcomes`` are the results of the attachments we actually PUSHED (accepted
    by the gate). ``had_attachments`` is whether the email had any attachment at
    all (accepted or rejected); ``all_rejected`` is whether it had attachments but
    every one was gate-rejected (so nothing was pushed)."""
    if any(o.fatal for o in outcomes):
        return MessageDisposition.LEAVE  # fatal handled separately; never move
    if any(o.move is AttachmentMove.LEAVE for o in outcomes):
        return MessageDisposition.LEAVE  # transient — re-push the whole message
    if any(o.move is AttachmentMove.PROCESSED for o in outcomes):
        return MessageDisposition.PROCESSED  # ≥1 real doc ingested → success
    if any(o.move is AttachmentMove.FAILED for o in outcomes):
        return MessageDisposition.FAILED  # real docs all terminally rejected
    if all_rejected:
        return MessageDisposition.FAILED  # had attachments, none ingestable
    # Nothing was pushed and nothing was gate-rejected → a plain email with no
    # files. Not our concern: mark it seen + leave it (``had_attachments`` is
    # False here by construction, since any accepted attachment yields a pushed
    # outcome and any rejected one sets ``all_rejected``).
    return MessageDisposition.SKIP


class MessageEngine:
    def __init__(
        self,
        *,
        config: Config,
        mailbox: Mailbox,
        provider: MailboxProvider,
        pusher: RenfieldPusher,
        on_failed: FailureHook | None = None,
        on_fatal: FatalHook | None = None,
        max_retries: int = 5,
        retry_base_seconds: float = 2.0,
    ):
        self._config = config
        self._mailbox = mailbox
        self._provider = provider
        self._pusher = pusher
        self._on_failed = on_failed
        self._on_fatal = on_fatal
        self._max_retries = max_retries
        self._retry_base = retry_base_seconds
        self._inflight: set[str] = set()
        # UIDs that exhausted their retry budget this session. They stay UNSEEN
        # in the inbox (a transient outage shouldn't mis-file them to failed/),
        # but IMAP IDLE re-emits every UNSEEN uid on each wake — without this
        # guard a permanently-failing message would restart its whole retry
        # ladder (and re-notify) on every wake. Parking matches folder-ingest's
        # "retry on the next restart's reconciliation" semantics (a fresh process
        # starts with an empty set), without the per-wake re-loop / notify spam.
        self._exhausted: set[str] = set()
        self._retry_tasks: set[asyncio.Task] = set()
        self._stopped = asyncio.Event()

    @property
    def _name(self) -> str:
        return self._mailbox.id

    async def run(self) -> None:
        await self._provider.start()
        logger.info("mailbox %s: started; reconciling unseen messages", self._name)
        await self._reconcile_unseen()
        async for event in self._provider.watch():
            if self._stopped.is_set():
                break
            await self._dispatch(event.uid)

    async def stop(self) -> None:
        self._stopped.set()
        for t in list(self._retry_tasks):
            t.cancel()
        await self._provider.stop()

    async def _reconcile_unseen(self) -> None:
        """One-shot startup catch-up (NOT a poll). See module docstring."""
        try:
            uids = await self._provider.list_unseen()
        except Exception as exc:  # noqa: BLE001 - reconciliation is best-effort
            logger.warning("mailbox %s: startup reconciliation failed: %s", self._name, exc)
            return
        for uid in uids:
            await self._dispatch(uid)

    async def _dispatch(self, uid: str) -> None:
        uid = (uid or "").strip()
        if not uid:
            return
        # De-dup: ignore a message already being processed, awaiting retry, or
        # parked after exhausting its retries (IDLE re-emits every UNSEEN uid on
        # each wake; these sets make that O(1) and prevent a re-loop).
        if uid in self._inflight or uid in self._exhausted:
            return
        self._inflight.add(uid)
        await self._process(uid, attempt=0)

    async def _process(self, uid: str, attempt: int) -> None:
        try:
            raw = await self._provider.fetch_raw(uid)
            if raw is None:
                self._inflight.discard(uid)  # already moved / vanished
                return
            parsed = parse_message(raw)

            accepted = []
            rejected = []
            for att in parsed.attachments:
                ok, reason = classify(self._config, att.filename, len(att.content))
                (accepted if ok else rejected).append((att, reason))

            if rejected:
                logger.info(
                    "mailbox %s: msg %s — %d attachment(s) skipped by gate: %s",
                    self._name, uid, len(rejected),
                    ", ".join(f"{a.filename}({r})" for a, r in rejected),
                )

            outcomes: list[PushOutcome] = []
            for att, _ in accepted:
                outcome = await self._pusher.push(
                    file_bytes=att.content,
                    filename=att.filename,
                    mailbox_id=self._mailbox.id,
                    message_id=parsed.message_id,
                    sha256=att.sha256,
                    mime=att.mime,
                    sender=parsed.sender,
                    subject=parsed.subject,
                )
                outcomes.append(outcome)
                if outcome.fatal:
                    break  # stop the mailbox; don't push the rest

            had_attachments = bool(parsed.attachments)
            all_rejected = had_attachments and not accepted
            await self._apply_outcomes(
                uid, attempt, outcomes,
                had_attachments=had_attachments, all_rejected=all_rejected,
            )
        except Exception as exc:  # noqa: BLE001 - never drop a message on a bug
            logger.exception("mailbox %s: error processing %s: %s", self._name, uid, exc)
            await self._schedule_retry(uid, attempt, "processing_error")

    async def _apply_outcomes(
        self,
        uid: str,
        attempt: int,
        outcomes: list[PushOutcome],
        *,
        had_attachments: bool,
        all_rejected: bool,
    ) -> None:
        fatal = next((o for o in outcomes if o.fatal), None)
        if fatal is not None:
            self._inflight.discard(uid)
            self._stopped.set()  # token/config error affects the whole mailbox
            logger.error("mailbox %s: fatal push outcome — stopping mailbox", self._name)
            if self._on_fatal is not None:
                await self._on_fatal(self._name, f"http_{fatal.http_status}")
            return

        disp = aggregate(
            outcomes, had_attachments=had_attachments, all_rejected=all_rejected
        )
        if disp is MessageDisposition.PROCESSED:
            await self._finish_move(uid, self._mailbox.processed_folder, "processed")
        elif disp is MessageDisposition.FAILED:
            reason = "all_attachments_rejected" if all_rejected else "ingest_failed"
            await self._finish_move(uid, self._mailbox.failed_folder, reason)
            if self._on_failed is not None:
                await self._on_failed(self._name, uid, reason)
        elif disp is MessageDisposition.SKIP:
            # Plain email, no ingestable attachments: flag \Seen so reconciliation
            # skips it, but never move the user's mail. Best-effort — a STORE
            # failure must not cascade into a retry of a doc-less email (it would
            # just re-SKIP); park it so it isn't re-dispatched every IDLE wake.
            try:
                await self._provider.mark_seen(uid)
            except Exception as exc:  # noqa: BLE001
                logger.warning("mailbox %s: mark_seen %s failed: %s", self._name, uid, exc)
                self._exhausted.add(uid)
            finally:
                self._inflight.discard(uid)
        else:  # LEAVE → bounded retry (leave UNSEEN for re-push)
            await self._schedule_retry(uid, attempt, "retry")

    async def _finish_move(self, uid: str, folder: str, reason: str) -> None:
        try:
            await self._provider.move_message(uid, folder)
            logger.info("mailbox %s: msg %s → %s (%s)", self._name, uid, folder, reason)
        finally:
            self._inflight.discard(uid)

    async def _schedule_retry(self, uid: str, attempt: int, reason: str) -> None:
        if attempt + 1 >= self._max_retries:
            # Give up: leave the message UNSEEN in the inbox but PARK it for this
            # session (release inflight + add to _exhausted) so IDLE-wake re-emits
            # don't restart the ladder. The next pod restart re-tries it (fresh
            # _exhausted). Notify ONCE.
            self._inflight.discard(uid)
            self._exhausted.add(uid)
            logger.warning(
                "mailbox %s: msg %s exhausted %d retries (%s); leaving in inbox",
                self._name, uid, self._max_retries, reason,
            )
            if self._on_failed is not None:
                await self._on_failed(self._name, uid, f"retry_exhausted: {reason}")
            return

        delay = self._retry_base * (2 ** attempt)

        async def _later() -> None:
            try:
                await asyncio.sleep(delay)
                if self._stopped.is_set():
                    self._inflight.discard(uid)
                    return
                await self._process(uid, attempt + 1)  # stays inflight across the wait
            finally:
                self._retry_tasks.discard(asyncio.current_task())

        task = asyncio.create_task(_later())
        self._retry_tasks.add(task)