# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""All-state named NUCC claims protect retained tables before legacy rotation."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nucc_retention_read_view as view


def test_claim_projection_uses_all_recorded_name_sources_without_state_filters():
    assert view.CLAIMS_SQL.count("UNION ALL") == 3
    for name in ("relation", "preparation_relation", "reference_preparation_relation", "legacy_archive"):
        assert "FROM hp_snapshot_retention." + name in view.CLAIMS_SQL
    assert "preparation.stage_schema" in view.CLAIMS_SQL and "relation.table_name" in view.CLAIMS_SQL
    assert "{inventory,relations}" in view.CLAIMS_SQL and "'[null]'::jsonb" in view.CLAIMS_SQL
    assert "WHERE" not in view.CLAIMS_SQL and "state" not in view.CLAIMS_SQL
    assert "relation_oid" not in view.CLAIMS_SQL and "owner" not in view.CLAIMS_SQL
    assert "security_invoker=false" in view.CREATE_VIEW_SQL


@pytest.mark.asyncio
@pytest.mark.parametrize("claim", [False, True, None, asyncio.CancelledError()])
async def test_fresh_named_claim_read_refuses_claimed_or_uncertain_tables(claim):
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=["read committed", True, claim]),
    )
    if claim is False:
        await view.require_unretained_nucc_relations(
            session, schema_name="mrf", relation_names=("nucc_taxonomy_old", "nucc_taxonomy")
        )
    else:
        with pytest.raises(asyncio.CancelledError if isinstance(claim, BaseException) else view.NuccRetentionReadError):
            await view.require_unretained_nucc_relations(
                session, schema_name="mrf", relation_names=("nucc_taxonomy_old", "nucc_taxonomy")
            )
    statement, parameters = session.scalar.await_args.args
    assert view.VIEW in str(statement)
    assert "schema_name IS NULL OR relation_name IS NULL" in str(statement)
    assert parameters == {"schema": "mrf", "relations": ["nucc_taxonomy", "nucc_taxonomy_old"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("installed", [False, None])
async def test_only_absent_retention_catalog_allows_untracked_rotation(installed):
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=["read committed", installed]),
    )
    if installed is False:
        await view.require_unretained_nucc_relations(session, schema_name="mrf", relation_names=("nucc_taxonomy",))
    else:
        with pytest.raises(view.NuccRetentionReadError, match="uncertain"):
            await view.require_unretained_nucc_relations(session, schema_name="mrf", relation_names=("nucc_taxonomy",))
    assert session.scalar.await_count == 2


@pytest.mark.asyncio
async def test_rotation_requires_fresh_read_committed_claims():
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value="repeatable read"))
    with pytest.raises(view.NuccRetentionReadError, match="fresh claim read"):
        await view.require_unretained_nucc_relations(session, schema_name="mrf", relation_names=("nucc_taxonomy",))
    session.scalar.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("names", [[], ["one", "two", "three"], [True], [0], [None], [{}], ["unsafe.name"]])
async def test_invalid_named_scope_refuses_before_database_reads(names):
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock())
    with pytest.raises(view.NuccRetentionReadError, match="relation scope"):
        await view.require_unretained_nucc_relations(session, schema_name="mrf", relation_names=names)
    session.scalar.assert_not_awaited()
