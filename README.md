# renfield-mcp-email-ingest

Event-driven **email-attachment auto-ingest** for Renfield — the email analog of
[`renfield-mcp-filesystem`](../renfield-mcp-filesystem). A dedicated watcher
holds the IMAP credentials the backend must not, watches one or more mailboxes
via **IMAP IDLE** (event-driven, never polling), extracts the attachments from
each new message, and **pushes** them into Renfield over REST. The backend reuses
its existing folder-ingest pipeline (dedup, KB filing, Paperless leg) and owns
the *sphere* routing (which mailbox files at which owner/tier/KB).

```
 new mail ──IDLE──▶ extract attachments ──▶ POST /api/email-ingest/document (Bearer)
   (IMAP)            (gate: ext + size)        │
                                               ├─ 200 {status} ──▶ move the EMAIL by the
                                               │                   aggregated per-attachment
                                               │                   status (Verarbeitet / Fehler / leave)
                                               ├─ 401/403 ──▶ fatal: stop the mailbox, notify
                                               └─ 503/err ──▶ leave UNSEEN, bounded retry
```

## Why a dedicated service (not a backend-bundled MCP)

- **Credential isolation.** IMAP usernames/passwords live in this pod's Secret,
  never in the backend image. The backend only ever sees pushed bytes + a
  routing `mailbox_id`.
- **Event-driven lifecycle.** IMAP IDLE needs a long-lived async loop per
  mailbox — that is a daemon, not a per-request stdio MCP subprocess.
- **Server-authoritative sphere.** This watcher sends ONLY a `mailbox_id`. The
  backend resolves `mailbox_id → owner/tier/knowledge-base`. A leaked push token
  therefore cannot escalate a mailbox's filing tier.

## The 4-state push contract

Identical to folder-ingest (the backend reuses the same `ingest_document` +
`IngestStatus`). The backend returns, per attachment, one of
`ingested | duplicate | retry | failed` (all HTTP 200), plus `401/403` (fatal
token error) and `503` (disabled / worker down → retry). See `contract.py`
(`EMAIL_INGEST_CONTRACT_VERSION` is mirrored on both sides — bump together).

Because one email fans out into N attachment pushes, the watcher **aggregates**
the per-attachment results into a single decision for the *email*
(`engine.py::aggregate`):

| email outcome | when | action |
|---|---|---|
| **processed** | ≥1 attachment ingested/duplicate (no transient, no fatal) | move → `processed_folder` |
| **failed** | real docs all terminally rejected, OR every attachment gate-rejected | move → `failed_folder` + notify |
| **leave** | any attachment transient (retry / 503 / network) | leave UNSEEN, bounded backoff retry |
| **skip** | the email has no ingestable attachments | mark `\Seen`, leave in place |
| **(fatal)** | any 401/403 | stop the mailbox, notify — never move |

Gate-rejected attachments (wrong extension, oversize, empty) are treated as
noise (logos, signatures) and **never fail an email on their own** — only a
backend push `failed`, or an email whose attachments are *all* rejected, sends
the email to `failed_folder`. Re-pushing a whole email is idempotent: the backend
dedups on `(content-hash, kb)`, so an already-ingested attachment returns
`duplicate`.

## Configuration

Global settings come from env; the watched **mailboxes** come from a mounted
`mailboxes.yaml` (a ConfigMap), reloaded at runtime when it changes.

| env | default | meaning |
|---|---|---|
| `RENFIELD_URL` | — (required) | Renfield backend base URL |
| `RENFIELD_INGEST_TOKEN` | — (required) | the email-ingest Bearer token (`POST /api/email-ingest/token`) |
| `EMAIL_MAILBOXES_YAML` | — | path to the mounted `mailboxes.yaml` |
| `EMAIL_ALLOWED_EXTENSIONS` | `pdf,docx,…` | comma list of allowed attachment extensions |
| `EMAIL_MAX_FILE_SIZE_MB` | `50` | per-attachment size ceiling |
| `EMAIL_PUSH_TIMEOUT_SECONDS` | `120` | push HTTP timeout |
| `EMAIL_IDLE_RENEW_SECONDS` | `1500` | re-issue IDLE within this (RFC 2177 < ~30 min) |
| `EMAIL_NOTIFY_WEBHOOK_URL` | — | optional: POST failure/disconnect notifications |
| `EMAIL_MCP_HOST` / `EMAIL_MCP_PORT` | `0.0.0.0` / `8080` | streamable-http MCP bind |

`mailboxes.yaml` (see `config/mailboxes.example.yaml`) holds, per mailbox, ONLY
the IMAP connection + a routing `id` + the move-folder names. **No owner/tier/KB**
— those are server-authoritative. IMAP credentials are referenced by **env-var
name** (kept in a Secret), never inlined.

## Run

```bash
pip install .
RENFIELD_URL=http://localhost:8000 \
RENFIELD_INGEST_TOKEN=... \
EMAIL_MAILBOXES_YAML=./config/mailboxes.yaml \
BUCHHALTUNG_IMAP_USER=... BUCHHALTUNG_IMAP_PASS=... \
renfield-mcp-email-ingest
```

Validate config + credentials + the gate WITHOUT side effects (no push/move):

```bash
renfield-mcp-email-ingest-scan            # all mailboxes
renfield-mcp-email-ingest-scan buchhaltung
```

## MCP tools (read-only ops visibility)

- `list_mailboxes` — configured mailboxes + connection state.
- `list_unseen(mailbox)` — UIDs of unprocessed messages in a mailbox's inbox.

The auto-push watcher is the primary job and needs no MCP call to function.

## Deploy (k8s)

`k8s/` has the Deployment (single replica, `Recreate` — two replicas would
double-IDLE + double-push), ConfigMap (`mailboxes.yaml`), Service, and a Secret
template. Image: `registry.treehouse.x-idra.de/renfield/email-ingest-mcp`.

## Tests

```bash
pip install -e ".[dev]"
python -m pytest -o asyncio_mode=auto -q              # unit (no IMAP server needed)
# live IMAP loop (fetch/move/IDLE), opt-in — run on a host with reachable IMAP:
EMAIL_LIVE_IMAP_HOST=... EMAIL_LIVE_IMAP_USER=... EMAIL_LIVE_IMAP_PASS=... \
  python -m pytest tests/test_live_imap.py -o asyncio_mode=auto -v
```

The content/aggregation/transport logic is fully unit-tested with a fake
provider + a mock HTTP transport; the live IMAP IDLE/fetch/move loop is validated
by the opt-in live test (mirrors the filesystem repo's live inotify test).
