"""Unit tests for the pure IMAP response parsers (the live IDLE/fetch/move loop
is validated by the .159 E2E)."""

from renfield_mcp_email_ingest.providers.imap import (
    extract_rfc822,
    parse_uid_search,
    reconnect_delay,
)


def test_parse_uid_search_basic():
    lines = [b"1 2 3 5 8", b"Search completed (0.001 + 0.000 secs)."]
    assert parse_uid_search(lines) == ["1", "2", "3", "5", "8"]


def test_parse_uid_search_empty():
    assert parse_uid_search([b"Search completed."]) == []
    assert parse_uid_search([]) == []
    assert parse_uid_search(None) == []


def test_parse_uid_search_dedup_and_order():
    lines = [b"4 4 2", b"2 7"]
    assert parse_uid_search(lines) == ["4", "2", "7"]


def test_parse_uid_search_ignores_non_digit_tokens():
    lines = [b"* SEARCH 10 11", b"OK done"]
    # only pure-digit tokens kept
    assert parse_uid_search(lines) == ["10", "11"]


def test_extract_rfc822_prefers_bytearray_literal():
    msg = bytearray(b"From: a@b\r\nSubject: x\r\n\r\nbody")
    lines = [b"1 FETCH (UID 5 RFC822 {28}", msg, b")", b"Fetch completed."]
    assert extract_rfc822(lines) == bytes(msg)


def test_extract_rfc822_picks_longest_bytearray():
    small = bytearray(b"hi")
    big = bytearray(b"From: a\r\n\r\n" + b"x" * 100)
    assert extract_rfc822([small, big]) == bytes(big)


def test_extract_rfc822_none_when_no_message():
    assert extract_rfc822([b"1 FETCH (UID 5)", b"Fetch completed."]) is None
    assert extract_rfc822([]) is None


def test_reconnect_delay_backs_off_and_caps():
    assert reconnect_delay(0) == 2.0
    assert reconnect_delay(1) == 4.0
    assert reconnect_delay(2) == 8.0
    assert reconnect_delay(100) == 60.0  # capped