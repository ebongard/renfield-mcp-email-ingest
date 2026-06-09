# renfield-mcp-email-ingest — email-ingest MCP server.
# Pure-Python deps (aioimaplib + watchdog's inotify on Linux), so no apt build
# layer is needed.
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY renfield_mcp_email_ingest ./renfield_mcp_email_ingest
RUN pip install --no-cache-dir .

# Streamable-http MCP server (read-only ops tools); the IMAP-IDLE watcher daemons
# launch alongside it at process startup.
ENV EMAIL_MCP_HOST=0.0.0.0 \
    EMAIL_MCP_PORT=8080
EXPOSE 8080

# Required at runtime: RENFIELD_URL, RENFIELD_INGEST_TOKEN, EMAIL_MAILBOXES_YAML
# (+ the IMAP credential env vars referenced by mailboxes.yaml). See README.
ENTRYPOINT ["renfield-mcp-email-ingest"]
