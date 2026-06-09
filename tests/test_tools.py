from renfield_mcp_email_ingest import tools as t


class _FakeReg:
    def __init__(self, providers):
        self._providers = providers

    def names(self):
        return list(self._providers)

    def get(self, name):
        return self._providers.get(name)


class _P:
    def __init__(self, connected=True, last_error=None, unseen=()):
        self._c = connected
        self._e = last_error
        self._unseen = list(unseen)

    @property
    def connected(self):
        return self._c

    @property
    def last_error(self):
        return self._e

    async def list_unseen(self):
        return self._unseen


async def test_list_mailboxes():
    reg = _FakeReg({"buchhaltung": _P(connected=True), "down": _P(connected=False, last_error="timeout")})
    out = await t.list_mailboxes(reg)
    by_id = {m["id"]: m for m in out["mailboxes"]}
    assert by_id["buchhaltung"]["connected"] is True
    assert by_id["down"]["connected"] is False and by_id["down"]["last_error"] == "timeout"


async def test_list_unseen():
    reg = _FakeReg({"buchhaltung": _P(unseen=["3", "4"])})
    out = await t.list_unseen(reg, "buchhaltung")
    assert out["unseen_uids"] == ["3", "4"] and out["count"] == 2


async def test_list_unseen_unknown_mailbox():
    reg = _FakeReg({})
    out = await t.list_unseen(reg, "nope")
    assert "error" in out
