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
@pytest.mark.parametrize("claim_source", ("retention", "current"))
async def test_fresh_named_claim_read_refuses_claimed_or_uncertain_tables(claim, claim_source):
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=["read committed", True, claim_source, claim]),
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
    assert view.claim_relations_source(claim_source) in str(statement)
    assert "schema_name IS NULL OR relation_name IS NULL" in str(statement)
    assert parameters == {"schema": "mrf", "relations": ["nucc_taxonomy", "nucc_taxonomy_old"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("claim", [False, True])
async def test_named_preparation_catalog_alone_reads_its_own_claims_without_the_view(claim):
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=["read committed", True, "named", claim]),
    )
    rotation = view.require_unretained_nucc_relations(session, schema_name="mrf", relation_names=("nucc_taxonomy",))
    if claim:
        with pytest.raises(view.NuccRetentionReadError, match="retained claim exists"):
            await rotation
    else:
        await rotation
    statement = str(session.scalar.await_args.args[0])
    assert view.VIEW not in statement and view.NAMED_CLAIMS_SQL in statement
    assert "schema_name IS NULL OR relation_name IS NULL" in statement
    assert view.CLAIMS_SQL.startswith(view.NAMED_CLAIMS_SQL + "\nUNION ALL\n")


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_source", ["incomplete", None, "other"])
async def test_incomplete_retention_installation_refuses_before_reading_claims(claim_source):
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=["read committed", True, claim_source]),
    )
    with pytest.raises(view.NuccRetentionReadError, match="catalogue is incomplete"):
        await view.require_unretained_nucc_relations(session, schema_name="mrf", relation_names=("nucc_taxonomy",))
    assert session.scalar.await_count == 3
    source_sql = str(session.scalar.await_args.args[0])
    for retention_only in ("publication", "legacy_archive", "reference_preparation", "reference_preparation_relation"):
        assert f"to_regclass('hp_snapshot_retention.{retention_only}') IS NULL" in source_sql


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


@pytest.mark.asyncio
async def test_plain_rotation_uses_read_locks_before_fresh_retained_claim_check(monkeypatch):
    from process import reference_family_archive as archive

    monkeypatch.setattr(archive, "is_nucc_native_handoff_required", AsyncMock(return_value=False))
    claims = AsyncMock()
    monkeypatch.setattr(view, "require_unretained_nucc_relations", claims)
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=True), execute=AsyncMock())
    await view.require_nucc_unretained_rotation(session, schema_name="mrf")
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        'LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
        'LOCK TABLE ONLY "mrf"."nucc_taxonomy_old" IN ACCESS SHARE MODE NOWAIT',
    ]
    claims.assert_awaited_once_with(session, schema_name="mrf", relation_names=["nucc_taxonomy", "nucc_taxonomy_old"])


def test_current_catalog_requires_every_claim_and_pin_source_without_legacy_view():
    assert "legacy_archive_relation" in view.CURRENT_CLAIMS_SQL
    assert view.CURRENT_CLAIMS_SQL.startswith(view.CLAIMS_SQL)
    assert "WHERE" not in view.CURRENT_CLAIMS_SQL and "state" not in view.CURRENT_CLAIMS_SQL
    current_clause = view._CLAIM_SOURCE_SQL.split("THEN 'current'")[0]
    for table in view._CURRENT_CATALOG_TABLES:
        assert f"to_regclass('hp_snapshot_retention.{table}') IS NOT NULL" in current_clause
    assert view.claim_relations_source("current") == "(" + view.CURRENT_CLAIMS_SQL + ")"
    assert view.claim_relations_source("retention") == view.VIEW


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_source,claimed", [("current", False), ("current", True), ("incomplete", None)])
async def test_address_snapshot_uses_same_current_claims_and_conservative_partial_refusal(claim_source, claimed):
    from process.entity_address_snapshot_adoption import _is_registered_snapshot

    answers = [False, claim_source] + ([claimed] if claimed is not None else [])
    database = SimpleNamespace(scalar=AsyncMock(side_effect=answers))
    assert await _is_registered_snapshot(database, "retained_snapshot", ("nucc_taxonomy",)) is (claimed is not False)
    assert database.scalar.await_count == len(answers)
    if claimed is not None:
        assert view.CURRENT_CLAIMS_SQL in database.scalar.await_args.args[0]
        assert database.scalar.await_args.kwargs == {"schema": "retained_snapshot", "names": ["nucc_taxonomy"]}
