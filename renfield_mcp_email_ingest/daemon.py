"""MailboxDaemonManager — owns the live providers + per-mailbox watcher daemons,
and **reloads mailboxes at runtime** when the mounted ``mailboxes.yaml`` changes.
This is the reason the dedicated MCP exists: a new mailbox is added by editing the
(ConfigMap-mounted) YAML — no redeploy, no pod recreate.

Providers are built eagerly in ``__init__`` (so the interactive tools work before
the daemon starts); ``start()`` launches one watcher task per mailbox + the
event-driven YAML watch. A mailboxes.yaml change diffs the new set against the
running one and starts/stops/rebuilds only what changed. Malformed YAML is logged
and ignored (the running mailboxes keep going).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .config import Config, Mailbox, load_mailboxes
from .engine import MessageEngine
from .notify import LogNotifier, Notifier
from .providers.base import MailboxProvider
from .pusher import RenfieldPusher
from .registry import make_provider

logger = logging.getLogger("renfield-mcp-email-ingest.daemon")

# Coalesce the multiple writes an editor / ConfigMap swap produces before
# reloading. A debounce timer, not a poll.
_RELOAD_DEBOUNCE_S = 1.0


class MailboxDaemonManager:
    def __init__(
        self,
        config: Config,
        *,
        provider_factory=make_provider,
        notifier: Notifier | None = None,
    ):
        self._config = config
        self._make_provider = provider_factory
        self._notifier = notifier or LogNotifier()
        self._pusher = RenfieldPusher(
            config.renfield_url, config.ingest_token, config.push_timeout_seconds
        )
        self._mailboxes: dict[str, Mailbox] = {}
        self._providers: dict[str, MailboxProvider] = {}
        self._engines: dict[str, MessageEngine] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._yaml_observer: Observer | None = None
        self._reload_handle: asyncio.TimerHandle | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # Backend-recovery detector. Assume healthy at boot (startup reconcile just
        # ran), so only a genuine down→up transition re-reconciles.
        self._healthy = True
        self._health_task: asyncio.Task | None = None
        for mailbox in config.mailboxes:
            self._build(mailbox)

    # -- lifecycle --

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        for name in list(self._engines):
            self._launch(name)
        if self._config.mailboxes_path:
            self._watch_yaml(self._config.mailboxes_path)
        if self._config.health_poll_seconds > 0:
            self._health_task = asyncio.create_task(self._health_poll_loop())
        logger.info("daemon started with mailboxes: %s", self.names())

    async def stop(self) -> None:
        if self._health_task is not None:
            self._health_task.cancel()
            await asyncio.gather(self._health_task, return_exceptions=True)
            self._health_task = None
        if self._yaml_observer is not None:
            self._yaml_observer.stop()
            await asyncio.to_thread(self._yaml_observer.join, 5)
            self._yaml_observer = None
        if self._reload_handle is not None:
            self._reload_handle.cancel()
        for name in list(self._mailboxes):
            await self._stop_one(name)

    async def reload(self) -> None:
        """Re-read the mailboxes YAML and apply the diff. Bad YAML → keep current."""
        if not self._config.mailboxes_path:
            return
        try:
            new = load_mailboxes(self._config.mailboxes_path)
        except Exception as exc:  # noqa: BLE001 - never crash on a bad edit
            logger.error("mailboxes reload failed (keeping current): %s", exc)
            return
        await self._apply(new)
        logger.info("mailboxes reloaded: %s", self.names())

    # -- backend-recovery detector (re-reconcile on down→up) --

    async def _health_poll_loop(self) -> None:
        """Poll the backend health endpoint; on a down→up transition re-reconcile
        every mailbox (un-park exhausted mail + re-scan UNSEEN). This un-sticks mail
        left parked after retry-exhaustion during a backend outage WITHOUT a manual
        restart — the specific gap this closes (the filesystem MCP already does it).
        A backend-liveness probe (like a readiness check), NOT an IMAP poll:
        detection stays IDLE-driven; this only reacts to the backend coming back."""
        interval = self._config.health_poll_seconds
        while True:
            try:
                await asyncio.sleep(interval)
                healthy = await self._pusher.health()
                if healthy and not self._healthy:
                    logger.info(
                        "backend recovered (health OK) — re-reconciling %d mailbox(es)",
                        len(self._engines),
                    )
                    for engine in list(self._engines.values()):
                        try:
                            await engine.recover()
                        except Exception as exc:  # noqa: BLE001
                            logger.warning("recovery reconcile failed: %s", exc)
                elif not healthy and self._healthy:
                    logger.warning(
                        "backend health check failing — will re-reconcile on recovery"
                    )
                self._healthy = healthy
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 - never let the loop die
                logger.warning("health poll loop error: %s", exc)

    # -- build / launch / stop --

    def _build(self, mailbox: Mailbox) -> None:
        provider = self._make_provider(mailbox, self._config.idle_renew_seconds)
        provider.set_disconnect_hook(self._notifier.disconnect)
        engine = MessageEngine(
            config=self._config, mailbox=mailbox, provider=provider, pusher=self._pusher,
            on_failed=self._notifier.failure, on_fatal=self._notifier.fatal,
        )
        self._mailboxes[mailbox.id] = mailbox
        self._providers[mailbox.id] = provider
        self._engines[mailbox.id] = engine

    def _launch(self, name: str) -> None:
        self._tasks[name] = asyncio.create_task(self._engines[name].run())

    async def _stop_one(self, name: str) -> None:
        task = self._tasks.pop(name, None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        engine = self._engines.pop(name, None)
        if engine is not None:
            try:
                await engine.stop()
            except Exception as exc:  # noqa: BLE001
                logger.warning("error stopping engine %s: %s", name, exc)
        self._providers.pop(name, None)
        self._mailboxes.pop(name, None)

    async def _apply(self, new_mailboxes: list[Mailbox]) -> None:
        new_by_id = {m.id: m for m in new_mailboxes}
        for name in set(self._mailboxes) - set(new_by_id):
            logger.info("mailbox %s removed — stopping", name)
            await self._stop_one(name)
        for name, mailbox in new_by_id.items():
            current = self._mailboxes.get(name)
            if current is None:
                logger.info("mailbox %s added — starting", name)
                self._build(mailbox)
                self._launch(name)
            elif current != mailbox:
                logger.info("mailbox %s changed — restarting", name)
                await self._stop_one(name)
                self._build(mailbox)
                self._launch(name)

    # -- yaml watch (event-driven reload trigger) --

    def _watch_yaml(self, mailboxes_path: str) -> None:
        yaml_path = Path(mailboxes_path).resolve()
        handler = _YamlChangeHandler(yaml_path, self._schedule_reload_threadsafe)
        self._yaml_observer = Observer()
        self._yaml_observer.schedule(handler, str(yaml_path.parent), recursive=False)
        self._yaml_observer.start()

    def _schedule_reload_threadsafe(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._debounce_reload)

    def _debounce_reload(self) -> None:
        if self._reload_handle is not None:
            self._reload_handle.cancel()
        assert self._loop is not None
        self._reload_handle = self._loop.call_later(
            _RELOAD_DEBOUNCE_S, lambda: asyncio.create_task(self.reload())
        )

    # -- accessors for the tools --

    def names(self) -> list[str]:
        return list(self._providers)

    def get(self, name: str) -> MailboxProvider | None:
        return self._providers.get(name)


class _YamlChangeHandler(FileSystemEventHandler):
    """Fires ``on_change`` when the watched mailboxes.yaml is touched. Watches the
    parent dir because a ConfigMap update swaps the file via a ``..data`` symlink
    (the file's own inode changes)."""

    def __init__(self, yaml_path: Path, on_change):
        self._name = yaml_path.name
        self._on_change = on_change

    def _relevant(self, path: str | None) -> bool:
        if not path:
            return False
        name = Path(path).name
        return name == self._name or name == "..data"

    def on_modified(self, event):
        if self._relevant(getattr(event, "src_path", None)):
            self._on_change()

    def on_created(self, event):
        if self._relevant(getattr(event, "src_path", None)):
            self._on_change()

    def on_moved(self, event):
        if self._relevant(getattr(event, "dest_path", None)) or self._relevant(
            getattr(event, "src_path", None)
        ):
            self._on_change()