import pytest

from renfield_mcp_email_ingest.contract import (
    EMAIL_INGEST_CONTRACT_VERSION,
    AttachmentMove,
    attachment_move_for,
)


@pytest.mark.parametrize("status,expected", [
    ("ingested", AttachmentMove.PROCESSED),
    ("duplicate", AttachmentMove.PROCESSED),
    ("failed", AttachmentMove.FAILED),
    ("retry", AttachmentMove.LEAVE),
])
def test_known_status_maps(status, expected):
    assert attachment_move_for(status) is expected


@pytest.mark.parametrize("bad", ["", "weird", "INGESTED", "none", "200"])
def test_unknown_status_is_leave(bad):
    # Contract skew must never drop an attachment.
    assert attachment_move_for(bad) is AttachmentMove.LEAVE


def test_contract_version_pinned():
    # Lock test: mirror of the backend's EMAIL_INGEST_CONTRACT_VERSION.
    assert EMAIL_INGEST_CONTRACT_VERSION == "1"
