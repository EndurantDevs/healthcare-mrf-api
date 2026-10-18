# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protocol checks for the native fixture's original Source actor and heap proof."""

from types import SimpleNamespace

import pytest

from tests import test_network_cms_registry_source_pair_postgres as fixture


class ActorSession:
    """Supply observed actor/catalog values without database or SQL interpretation."""

    def __init__(self, actor, rows):
        self.actor = actor
        self.rows = rows
        self.statements = []

    async def execute(self, statement, parameters=None):
        self.statements.append((str(statement), parameters))
        if len(self.statements) == 1:
            return SimpleNamespace(one=lambda: self.actor)
        assert len(self.statements) == 2
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: self.rows))


def _profile_proof(actor):
    """Keep the login distinct from its selected protected native owner."""
    component = SimpleNamespace(publisher="source_publisher", owner="source_owner")
    database = SimpleNamespace(_source_component=component)
    names = ("provider_directory_address_overlay", "provider_directory_profile", "provider_directory_profile_evidence")
    rows = [
        {"relname": name, "oid": oid, "kind": "r", "owner_name": component.owner, "can_read": True}
        for oid, name in enumerate(names, 101)
    ]
    expected_heaps = tuple((row["relname"], row["oid"], row["owner_name"]) for row in rows)
    return database, ActorSession(actor, rows), expected_heaps


@pytest.mark.asyncio
async def test_original_publisher_can_select_source_owner():
    """The genuine LOGIN/SET LOCAL ROLE pair reaches the exact OID/SELECT checks."""
    database, session, expected_heaps = _profile_proof(("source_publisher", "source_owner"))
    await fixture._assert_pair_profile_reads(database, session, expected_heaps)
    assert len(session.statements) == 2
    assert all(sql.startswith("SELECT ") for sql, _parameters in session.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "actor",
    [
        ("source_owner", "source_owner"),
        ("source_publisher", "source_publisher"),
        ("other_publisher", "source_owner"),
        ("source_publisher", "other_owner"),
        ("source_owner", "source_publisher"),
    ],
)
async def test_spoofed_or_equal_source_actors_refuse(actor):
    """Native role equality cannot substitute for the authenticated original LOGIN."""
    database, session, expected_heaps = _profile_proof(actor)
    with pytest.raises(AssertionError):
        await fixture._assert_pair_profile_reads(database, session, expected_heaps)
    assert len(session.statements) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["oid", "owner", "select", "missing"])
async def test_actor_proof_keeps_exact_heap_guards(damage):
    """Correct actors still refuse object substitution or missing native read access."""
    database, session, expected_heaps = _profile_proof(("source_publisher", "source_owner"))
    if damage == "oid":
        session.rows[0]["oid"] += 10
    elif damage == "owner":
        session.rows[0]["owner_name"] = "other_owner"
    elif damage == "select":
        session.rows[0]["can_read"] = False
    else:
        session.rows.pop()
    with pytest.raises(AssertionError):
        await fixture._assert_pair_profile_reads(database, session, expected_heaps)
    assert len(session.statements) == 2
