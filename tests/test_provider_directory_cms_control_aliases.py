# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scoped control tokens cover historical aliases without widening target visibility."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from api import provider_directory_cms_control_scope as controls
from api import provider_directory_cms_entities as entities
from api.provider_directory_entities_contract import DirectoryRead, DirectoryReadError

OLD_ID = UUID(int=99)
FIRST_ID = UUID(int=10)
SECOND_ID = UUID(int=11)
KEY = b"synthetic-test-key-for-directory-cursors"


def generation_for(kind="organizations", shape="entity"):
    return {
        "network_scope": SimpleNamespace(source_schema='"retained"'),
        "generation_id": "gen_retained",
        "dataset_id": "retained-dataset",
        "release_id": "retained-release",
        "scope_kind": kind,
        "scope_shape": shape,
        "retained_pair": object(),
        "control_schema": '"control"',
    }


@pytest.fixture
def scoped_predicates(monkeypatch):
    def selected(generation, kind, resource_id):
        return f" AND {resource_id}=ANY(CAST(:selected_{kind} AS text[]))"

    monkeypatch.setattr(controls, "cms_network_predicate", selected)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["organizations", "sites"])
async def test_historical_aliases_are_selected_by_visible_canonical_target(scoped_predicates, kind):
    generation = generation_for(kind)
    session = SimpleNamespace(scalar=AsyncMock(return_value="bounded-digest"))
    result = await controls.scoped_cms_control_generation(session, '"control"', KEY, generation)
    statement, parameters_by_name = session.scalar.call_args.args
    sql = str(statement.compile(dialect=postgresql.dialect()))
    aliases = sql.split("aliases AS MATERIALIZED (", 1)[1].split("), redirects AS MATERIALIZED", 1)[0]
    assert 'FROM "control".provider_directory_entity_redirect p' in aliases
    assert "p.source_id='cms-npd'" in aliases
    assert "p.old_entity_id IN (SELECT entity_id FROM selected)" in aliases
    assert "target.entity_id=p.canonical_entity_id AND target.resource_type=p.resource_type" in aliases
    assert 'JOIN "retained".provider_directory_entity_release_evidence e' in aliases
    assert "e.source_id='cms-npd' AND e.resource_type=target.resource_type" in aliases
    assert "e.resource_id=target.resource_id AND e.release_id=%(release_id)s" in aliases
    assert "target.acquired_resource_sha256 IS NULL OR e.payload_sha256=target.acquired_resource_sha256" in aliases
    assert "selected_organizations" in sql
    assert ("selected_sites" in sql) is (kind == "sites")
    assert "SELECT 'redirect' AS kind,to_jsonb(p) AS value FROM aliases p" in sql
    assert parameters_by_name is generation
    assert result["generation_id"] != generation["generation_id"]
    assert session.scalar.await_count == 1


@pytest.mark.asyncio
async def test_alias_review_history_and_closures_reach_the_same_bounded_digest(scoped_predicates):
    session = SimpleNamespace(scalar=AsyncMock(return_value="bounded-digest"))
    await controls.scoped_cms_control_generation(session, '"control"', KEY, generation_for())
    sql = str(session.scalar.call_args.args[0])
    decisions = sql.split("redirects AS MATERIALIZED (", 1)[1].split("), payer_bindings", 1)[0]
    assert "SELECT 1 FROM aliases p WHERE p.old_entity_id::text=to_jsonb(d)->>'old_entity_id'" in decisions
    assert "p.resource_type=to_jsonb(d)->>'resource_type'" in decisions
    assert "'redirect-decision',value FROM redirects" in sql
    assert "'prior_decision_id' IN (SELECT value->>'decision_id' FROM redirects)" in sql
    assert sql.index("'redirect-closure'") < sql.index("LIMIT 5001")
    assert "rows<=5000 AND bytes<=1048576" in sql
    assert "ORDER BY kind,value::text" in sql
    assert '"control".provider_directory_mrf_payer_binding' in sql


@pytest.mark.asyncio
async def test_alias_overflow_still_fails_closed(scoped_predicates):
    session = SimpleNamespace(scalar=AsyncMock(return_value=None))
    with pytest.raises(DirectoryReadError) as refused:
        await controls.scoped_cms_control_generation(session, '"control"', KEY, generation_for())
    assert refused.value.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["organizations", "sites"])
async def test_old_generation_is_rejected_before_disclosing_retargeted_alias(monkeypatch, scoped_predicates, kind):
    generation = generation_for(kind)
    query = DirectoryRead(
        kind,
        "cms-npd",
        "entity",
        str(OLD_ID),
        None,
        1,
        network_ids=(42,),
        network_generation=7,
        access_scope_sha256="e" * 64,
    )

    async def pinned(session, schema, key, query, current_generation, response_headers):
        return query, generation

    monkeypatch.setattr(entities, "_scoped_generation", pinned)
    visible = AsyncMock(return_value=[{"entity_id": FIRST_ID}])
    monkeypatch.setattr(entities, "cms_entity_rows", visible)
    first_session = SimpleNamespace(scalar=AsyncMock(side_effect=["original-alias-digest", FIRST_ID]))
    first = await entities._read_snapshot(first_session, '"control"', KEY, query)
    assert first["redirect"]["canonical_id"] == str(FIRST_ID)
    assert first["redirect"]["requested_id"] == str(OLD_ID)
    retargeted = SimpleNamespace(scalar=AsyncMock(side_effect=["replacement-alias-digest", SECOND_ID]))
    with pytest.raises(DirectoryReadError) as stale:
        await entities._read_snapshot(
            retargeted, '"control"', KEY, replace(query, generation_id=first["generation_id"])
        )
    assert stale.value.status == 409
    assert retargeted.scalar.await_count == 1
    assert visible.await_count == 1


@pytest.mark.asyncio
async def test_unselected_canonical_target_remains_unavailable(monkeypatch, scoped_predicates):
    generation = generation_for()
    query = DirectoryRead(
        "organizations",
        "cms-npd",
        "entity",
        str(OLD_ID),
        None,
        1,
        network_ids=(42,),
        network_generation=7,
        access_scope_sha256="e" * 64,
    )

    async def pinned(session, schema, key, query, current_generation, response_headers):
        return query, generation

    monkeypatch.setattr(entities, "_scoped_generation", pinned)
    monkeypatch.setattr(entities, "cms_entity_rows", AsyncMock(return_value=[]))
    session = SimpleNamespace(scalar=AsyncMock(side_effect=["bounded-digest", SECOND_ID]))
    with pytest.raises(DirectoryReadError) as hidden:
        await entities._read_snapshot(session, '"control"', KEY, query)
    assert hidden.value.status == 503
