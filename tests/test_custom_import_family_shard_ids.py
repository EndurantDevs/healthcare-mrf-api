# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native IDs are observed, never reconstructed from an assumed reservation."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_ids as allocation
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit


def test_actual_id_order_gaps_and_cross_kind_identity_are_preserved():
    kinds = ("root_revision", "child_revision", "root_revision")
    assert allocation._checked_ids(kinds, [(1, 11), (2, 11), (3, 97)]) == (11, 11, 97)
    for rows in (
        [(1, 11), (2, 12)],
        [(2, 11), (1, 12), (3, 13)],
        [(1, True), (2, 12), (3, 13)],
        [(1, 11), (2, 12), (3, 11)],
        [(1, 0), (2, 12), (3, 13)],
    ):
        with pytest.raises(FamilyBlockUnavailable):
            allocation._checked_ids(kinds, rows)


@pytest.mark.parametrize("build_id", [None, 91])
def test_authority_commit_and_actual_returned_ids(monkeypatch, build_id):
    events = []

    class Session:
        async def execute(self, statement, parameters):
            events.append((str(statement), parameters))
            return type("Result", (), {"all": lambda self: [(1, 41), (2, 901)]})()

    @asynccontextmanager
    async def page(factory, request, supplied_build_id):
        assert supplied_build_id == build_id
        events.append("authority")
        yield Session(), None
        events.append("fresh-authority-and-commit")

    async def owner(session):
        events.append("owner")

    async def sequences(session, kinds):
        assert kinds == ("root_revision", "rejection")
        return {"root_revision": 123, "rejection": 456}

    async def prepare(session):
        events.append("statement-budget")

    monkeypatch.setattr(allocation, "_page_session", page)
    monkeypatch.setattr(allocation, "_owner", owner)
    monkeypatch.setattr(allocation, "_sequence_oids", sequences)
    monkeypatch.setattr(allocation, "_prepare_statement", prepare)
    request = SimpleNamespace(continuation=None if build_id is None else SimpleNamespace(build_id=build_id))
    actual = asyncio.run(allocation.allocate_record_ids(None, request, ("root_revision", "rejection"), maximum_ids=2))
    assert actual == (41, 901)
    assert events[:2] == ["authority", "owner"]
    assert events[-1] == "fresh-authority-and-commit"
    query, parameters = events[-2]
    assert "nextval" in query and "MATERIALIZED" in query
    assert parameters == {"kinds": ["root_revision", "rejection"]}


def test_invalid_or_excessive_batch_never_opens_authority(monkeypatch):
    def forbidden(*args):
        raise AssertionError("invalid allocation opened a database session")

    monkeypatch.setattr(allocation, "_page_session", forbidden)
    assert asyncio.run(allocation.allocate_record_ids(None, None, (), maximum_ids=1)) == ()
    with pytest.raises(ValueError):
        asyncio.run(allocation.allocate_record_ids(None, None, ("unregistered",), maximum_ids=1))
    with pytest.raises(FamilyBlockResourceLimit):
        asyncio.run(allocation.allocate_record_ids(None, None, ("root_revision",) * 2, maximum_ids=1))
