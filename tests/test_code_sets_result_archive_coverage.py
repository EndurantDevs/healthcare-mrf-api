# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic guard coverage for the scoped code-set archive."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from db.models import CodeCatalog
from process import code_sets_result_archive as archive
from process import reference_family_archive


class _Result:
    def __init__(self, *, mapping=None, rows=()):
        self.mapping = mapping
        self.rows = rows

    def mappings(self):
        return self

    def one_or_none(self):
        return self.mapping

    def all(self):
        return self.rows

    def scalars(self):
        return self

    def one(self):
        return self.mapping


def _session(*, scalar_values=None, execute_values=None):
    return SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock() if scalar_values is None else AsyncMock(side_effect=scalar_values),
        execute=AsyncMock() if execute_values is None else AsyncMock(side_effect=execute_values),
    )


def _source():
    return archive.CodeSetsSourceGeneration(
        uuid4(),
        1,
        datetime(2026, 9, 23, 12, tzinfo=timezone.utc),
        len(archive.SOURCES),
        "a" * 64,
        "b" * 64,
    )


def _generation(*, local_generation=1, source=None, catalog_oid=99):
    if source is None:
        return archive.CodeSetsGeneration(uuid4(), local_generation, None, None, None, None, None, None)
    return archive.CodeSetsGeneration(
        uuid4(),
        local_generation,
        source.origin_lineage_id,
        source.origin_generation,
        source.published_at,
        catalog_oid,
        source.row_count,
        source.row_sha256,
    )


def _stage(source=None):
    source = source or _source()
    dataset_id = uuid4()
    return archive.CodeSetsStage(
        dataset_id,
        archive.stage_schema(dataset_id),
        11,
        12,
        source,
        source.row_count,
        source.row_sha256,
    )


def test_validators_reject_invalid_archive_values():
    source_generation = _source()
    manifest = source_generation.as_dict()

    for invalid_manifest in ({}, {**manifest, "origin_lineage_id": "not-a-uuid"}, {**manifest, "origin_generation": 0}):
        with pytest.raises(archive.CodeSetsArchiveError, match="manifest is invalid"):
            archive.validate_manifest(invalid_manifest)

    with pytest.raises(archive.CodeSetsArchiveError, match="stage identity"):
        archive.stage_schema("not-a-uuid")
    with pytest.raises(archive.CodeSetsArchiveError, match="schema is invalid"):
        archive._schema("not-a-schema")
    with pytest.raises(archive.CodeSetsArchiveError, match="caller transaction"):
        archive._transaction(SimpleNamespace(in_transaction=lambda: False))
    with pytest.raises(archive.CodeSetsArchiveError, match="unavailable"):
        archive._generation(None)
    with pytest.raises(archive.CodeSetsArchiveError, match="invalid"):
        archive._generation({})

    invalid_generation_by_field = {
        "local_lineage_id": str(uuid4()),
        "local_generation": -1,
        "origin_lineage_id": None,
        "origin_generation": None,
        "published_at": None,
        "code_catalog_oid": None,
        "row_count": None,
        "row_sha256": None,
    }
    with pytest.raises(archive.CodeSetsArchiveError, match="invalid"):
        archive._generation(invalid_generation_by_field)
    with pytest.raises(archive.CodeSetsArchiveError, match="invalid"):
        archive._generation(
            {
                **invalid_generation_by_field,
                "local_generation": 1,
                "origin_lineage_id": str(uuid4()),
                "origin_generation": 0,
                "published_at": source_generation.published_at,
                "code_catalog_oid": 1,
                "row_count": len(archive.SOURCES),
                "row_sha256": "a" * 64,
            }
        )


@pytest.mark.asyncio
async def test_scope_receipt_rejects_missing_foreign_and_incomplete_catalogs():
    with pytest.raises(archive.CodeSetsArchiveError, match="relation is unavailable"):
        await archive.scope_receipt(_session(scalar_values=[0]), "stage")
    with pytest.raises(archive.CodeSetsArchiveError, match="foreign rows"):
        await archive.scope_receipt(_session(scalar_values=[1, True]), "stage")
    with pytest.raises(archive.CodeSetsArchiveError, match="incomplete"):
        await archive.scope_receipt(
            _session(
                scalar_values=[1, False],
                execute_values=[_Result(rows=[(archive.SOURCES[0][0], 1)])],
            ),
            "stage",
        )


@pytest.mark.asyncio
async def test_publish_and_slice_reject_exhausted_or_missing_catalogs(monkeypatch):
    monkeypatch.setattr(
        archive,
        "read_generation",
        AsyncMock(return_value=_generation(local_generation=archive._MAX_GENERATION)),
    )
    with pytest.raises(archive.CodeSetsArchiveError, match="exhausted"):
        await archive.publish_local_generation(_session(), "destination")
    with pytest.raises(archive.CodeSetsArchiveError, match="relation is unavailable"):
        await archive._slice_identity(_session(scalar_values=[0]), "destination", complete=False)


@pytest.mark.asyncio
async def test_clone_and_ownership_checks_reject_changed_relations(monkeypatch):
    with pytest.raises(archive.CodeSetsArchiveError, match="clone is unavailable"):
        await archive._clone_slice(_session(scalar_values=[None, 2]), "source", "target")
    with pytest.raises(archive.CodeSetsArchiveError, match="ownership changed"):
        await archive._verify_clone(_session(scalar_values=[2, 3]), "target", 1, 3)
    with pytest.raises(archive.CodeSetsArchiveError, match="predecessor ownership changed"):
        await archive._verify_clone(_session(scalar_values=[1, 3, 5]), "target", 1, 3, 4)

    monkeypatch.setattr(
        reference_family_archive,
        "_namespace_relations",
        AsyncMock(return_value=[{"relkind": "v", "oid": 3, "index_table_oid": None}]),
    )
    with pytest.raises(archive.CodeSetsArchiveError, match="unowned relation"):
        await archive._verify_clone(_session(scalar_values=[1, 3]), "target", 1, 3)


@pytest.mark.asyncio
async def test_prepare_source_rejects_drift_clone_difference_and_incomplete_authority(monkeypatch):
    source = _source()
    generation = _generation(source=source, catalog_oid=9)
    session = _session()
    monkeypatch.setattr(archive, "read_generation", AsyncMock(return_value=generation))
    monkeypatch.setattr(archive, "scope_receipt", AsyncMock(return_value=(source.row_count, source.row_sha256, 8)))
    with pytest.raises(archive.CodeSetsArchiveError, match="generation drifted"):
        await archive.prepare_source(session, "source", uuid4())

    monkeypatch.setattr(
        archive,
        "scope_receipt",
        AsyncMock(side_effect=[(source.row_count, source.row_sha256, 9), (2, source.row_sha256, 13)]),
    )
    monkeypatch.setattr(archive, "_clone_slice", AsyncMock(return_value=(12, 13)))
    with pytest.raises(archive.CodeSetsArchiveError, match="clone differs"):
        await archive.prepare_source(session, "source", uuid4())


@pytest.mark.asyncio
async def test_restore_stage_and_verification_reject_invalid_physical_state(monkeypatch):
    manifest = _source().as_dict()
    with pytest.raises(archive.CodeSetsArchiveError, match="restore stage is unavailable"):
        await archive.precreate_restore(
            _session(scalar_values=[None, 2, 3]), destination="destination", dataset_id=uuid4(), manifest=manifest
        )

    session = _session(scalar_values=[1, 2, 3])
    monkeypatch.setattr(archive, "_verify_clone", AsyncMock())
    monkeypatch.setattr(archive, "_column_signature", AsyncMock(return_value=("column",)))
    with pytest.raises(archive.CodeSetsArchiveError, match="restore schema differs"):
        await archive.precreate_restore(session, destination="destination", dataset_id=uuid4(), manifest=manifest)

    with pytest.raises(archive.CodeSetsArchiveError, match="stage identity"):
        await archive.verify_stage(_session(), object())

    stage = _stage()
    monkeypatch.setattr(archive, "scope_receipt", AsyncMock(return_value=(2, stage.row_sha256, stage.catalog_oid)))
    with pytest.raises(archive.CodeSetsArchiveError, match="stage content changed"):
        await archive.verify_stage(_session(), stage)

    monkeypatch.setattr(
        archive,
        "scope_receipt",
        AsyncMock(return_value=(stage.row_count, stage.row_sha256, stage.catalog_oid)),
    )
    with pytest.raises(archive.CodeSetsArchiveError, match="stage schema changed"):
        await archive.verify_stage(_session(), stage)


@pytest.mark.asyncio
async def test_column_signature_rejects_catalog_shape_changes():
    with pytest.raises(archive.CodeSetsArchiveError, match="columns differ"):
        await archive._column_signature(_session(execute_values=[_Result(rows=[])]), 1)

    columns = [(column.name,) for column in CodeCatalog.__table__.columns]
    with pytest.raises(archive.CodeSetsArchiveError, match="key differs"):
        await archive._column_signature(
            _session(execute_values=[_Result(rows=columns), _Result(rows=["code"])]),
            1,
        )


@pytest.mark.asyncio
async def test_prepare_predecessor_rejects_changed_rows_and_stage_shape(monkeypatch):
    stage = _stage()
    expected = _generation(source=stage.source_generation)
    session = _session()
    read_generation = AsyncMock(return_value=_generation(source=stage.source_generation))
    monkeypatch.setattr(archive, "read_generation", read_generation)
    with pytest.raises(archive.CodeSetsArchiveError, match="destination generation changed"):
        await archive.prepare_predecessor(session, destination="destination", stage=stage, expected=expected)

    untracked = _generation()
    read_generation.return_value = untracked
    monkeypatch.setattr(archive, "_slice_identity", AsyncMock(return_value=(1, "a" * 64, 99)))
    with pytest.raises(archive.CodeSetsArchiveError, match="untracked"):
        await archive.prepare_predecessor(session, destination="destination", stage=stage, expected=untracked)

    read_generation.return_value = expected
    slice_identity = AsyncMock(return_value=(stage.row_count, "c" * 64, 99))
    monkeypatch.setattr(archive, "_slice_identity", slice_identity)
    with pytest.raises(archive.CodeSetsArchiveError, match="source rows changed"):
        await archive.prepare_predecessor(session, destination="destination", stage=stage, expected=expected)

    slice_identity.return_value = (stage.row_count, stage.row_sha256, 99)
    monkeypatch.setattr(archive, "verify_stage", AsyncMock())
    column_signature = AsyncMock(side_effect=[("stage",), ("destination",)])
    monkeypatch.setattr(archive, "_column_signature", column_signature)
    with pytest.raises(archive.CodeSetsArchiveError, match="table shape differs"):
        await archive.prepare_predecessor(session, destination="destination", stage=stage, expected=expected)


@pytest.mark.asyncio
async def test_prepare_predecessor_rejects_nonempty_or_changed_clone(monkeypatch):
    stage = _stage()
    expected = _generation(source=stage.source_generation)
    monkeypatch.setattr(archive, "read_generation", AsyncMock(return_value=expected))
    slice_identity = AsyncMock(return_value=(stage.row_count, stage.row_sha256, 99))
    monkeypatch.setattr(archive, "_slice_identity", slice_identity)
    monkeypatch.setattr(archive, "verify_stage", AsyncMock())
    monkeypatch.setattr(archive, "_column_signature", AsyncMock(return_value=("same",)))
    monkeypatch.setattr(archive, "_verify_clone", AsyncMock())
    with pytest.raises(archive.CodeSetsArchiveError, match="predecessor is not empty"):
        await archive.prepare_predecessor(
            _session(scalar_values=[True]),
            destination="destination",
            stage=stage,
            expected=expected,
            precreated_predecessor_oid=13,
        )

    slice_identity.side_effect = [
        (stage.row_count, stage.row_sha256, 99),
        (2, "c" * 64, 13),
    ]
    monkeypatch.setattr(archive, "_clone_slice", AsyncMock(return_value=(stage.schema_oid, 13)))
    with pytest.raises(archive.CodeSetsArchiveError, match="predecessor clone differs"):
        await archive.prepare_predecessor(_session(), destination="destination", stage=stage, expected=expected)


@pytest.mark.asyncio
async def test_validate_prepared_stage_rejects_untracked_changed_and_mismatched_predecessors(monkeypatch):
    stage = _stage()
    session = _session()
    untracked = _generation()
    read_generation = AsyncMock(return_value=untracked)
    monkeypatch.setattr(archive, "read_generation", read_generation)
    slice_identity = AsyncMock(return_value=(1, "a" * 64, 99))
    monkeypatch.setattr(archive, "_slice_identity", slice_identity)
    with pytest.raises(archive.CodeSetsArchiveError, match="untracked"):
        await archive.validate_prepared_stage(session, destination="destination", stage=stage, predecessor_oid=13)

    expected = _generation(source=stage.source_generation)
    read_generation.return_value = expected
    slice_identity.return_value = (stage.row_count, "c" * 64, 99)
    with pytest.raises(archive.CodeSetsArchiveError, match="source rows changed"):
        await archive.validate_prepared_stage(session, destination="destination", stage=stage, predecessor_oid=13)

    slice_identity.return_value = (stage.row_count, stage.row_sha256, 99)
    monkeypatch.setattr(archive, "verify_stage", AsyncMock())
    column_signature = AsyncMock(side_effect=[("predecessor",), ("destination",)])
    monkeypatch.setattr(archive, "_column_signature", column_signature)
    with pytest.raises(archive.CodeSetsArchiveError, match="predecessor table shape differs"):
        await archive.validate_prepared_stage(session, destination="destination", stage=stage, predecessor_oid=13)

    column_signature.side_effect = [("same",), ("same",)]
    slice_identity.side_effect = [
        (stage.row_count, stage.row_sha256, 99),
        (2, "c" * 64, 13),
    ]
    with pytest.raises(archive.CodeSetsArchiveError, match="predecessor differs"):
        await archive.validate_prepared_stage(session, destination="destination", stage=stage, predecessor_oid=13)


@pytest.mark.asyncio
async def test_activate_stage_rejects_changed_generation_and_untracked_rows(monkeypatch):
    stage = _stage()
    expected = _generation(source=stage.source_generation)
    prepared = archive.CodeSetsPreparedStage(stage, expected, 13, stage.row_count, stage.row_sha256)
    read_generation = AsyncMock(return_value=_generation(source=stage.source_generation))
    monkeypatch.setattr(archive, "read_generation", read_generation)
    with pytest.raises(archive.CodeSetsArchiveError, match="destination generation changed"):
        await archive.activate_stage(_session(), destination="destination", prepared=prepared)

    untracked = _generation()
    read_generation.return_value = untracked
    monkeypatch.setattr(archive, "_slice_identity", AsyncMock(return_value=(1, "a" * 64, 99)))
    with pytest.raises(archive.CodeSetsArchiveError, match="untracked"):
        await archive.activate_stage(
            _session(),
            destination="destination",
            prepared=archive.CodeSetsPreparedStage(stage, untracked, 13, stage.row_count, stage.row_sha256),
        )


@pytest.mark.asyncio
async def test_activate_stage_rejects_changed_predecessor_content_and_authority(monkeypatch):
    stage = _stage()
    expected = _generation(source=stage.source_generation)
    prepared = archive.CodeSetsPreparedStage(stage, expected, 13, stage.row_count, stage.row_sha256)
    monkeypatch.setattr(archive, "read_generation", AsyncMock(return_value=expected))
    slice_identity = AsyncMock()
    monkeypatch.setattr(archive, "_slice_identity", slice_identity)
    monkeypatch.setattr(archive, "verify_stage", AsyncMock())
    column_signature = AsyncMock()
    monkeypatch.setattr(archive, "_column_signature", column_signature)
    monkeypatch.setattr(archive, "_has_collision", AsyncMock(return_value=False))
    monkeypatch.setattr(archive, "_replace_slice", AsyncMock())
    scope_receipt = AsyncMock()
    monkeypatch.setattr(archive, "scope_receipt", scope_receipt)
    set_generation = AsyncMock()
    monkeypatch.setattr(archive, "_set_generation", set_generation)

    slice_identity.side_effect = [(stage.row_count, stage.row_sha256, 99)]
    column_signature.side_effect = [("stage",), ("destination",)]
    with pytest.raises(archive.CodeSetsArchiveError, match="destination table shape differs"):
        await archive.activate_stage(_session(), destination="destination", prepared=prepared)

    slice_identity.side_effect = [
        (stage.row_count, stage.row_sha256, 99),
        (2, "c" * 64, 13),
    ]
    column_signature.side_effect = [("same",), ("same",)]
    with pytest.raises(archive.CodeSetsArchiveError, match="predecessor changed"):
        await archive.activate_stage(_session(), destination="destination", prepared=prepared)

    slice_identity.side_effect = [
        (stage.row_count, stage.row_sha256, 99),
        (stage.row_count, stage.row_sha256, 13),
    ]
    column_signature.side_effect = [("same",), ("same",)]
    scope_receipt.return_value = (2, "c" * 64, 99)
    with pytest.raises(archive.CodeSetsArchiveError, match="activation result differs"):
        await archive.activate_stage(_session(), destination="destination", prepared=prepared)

    scope_receipt.return_value = (stage.row_count, stage.row_sha256, 99)
    slice_identity.side_effect = [
        (stage.row_count, stage.row_sha256, 99),
        (stage.row_count, stage.row_sha256, 13),
    ]
    column_signature.side_effect = [("same",), ("same",)]
    set_generation.return_value = expected
    with pytest.raises(archive.CodeSetsArchiveError, match="activation authority differs"):
        await archive.activate_stage(_session(), destination="destination", prepared=prepared)


@pytest.mark.asyncio
async def test_rollback_rejects_changed_state(monkeypatch):
    stage = _stage()
    source_generation = stage.source_generation
    previous = _generation(local_generation=4, source=source_generation)
    current = _generation(local_generation=5, source=source_generation)
    activation = archive.CodeSetsActivation(
        stage.dataset_id,
        previous,
        current,
        archive.predecessor_schema(stage.dataset_id),
        stage.schema_oid,
        stage.catalog_oid,
        13,
        source_generation.row_count,
        source_generation.row_sha256,
    )
    with pytest.raises(archive.CodeSetsArchiveError, match="rollback authority is invalid"):
        await archive.rollback_activation(_session(), destination="destination", activation=object())

    monkeypatch.setattr(archive, "read_generation", AsyncMock(return_value=current))
    monkeypatch.setattr(
        archive,
        "scope_receipt",
        AsyncMock(return_value=(source_generation.row_count, source_generation.row_sha256, current.code_catalog_oid)),
    )
    monkeypatch.setattr(archive, "_verify_clone", AsyncMock())
    column_signature = AsyncMock(side_effect=[("predecessor",), ("destination",)])
    monkeypatch.setattr(archive, "_column_signature", column_signature)
    with pytest.raises(archive.CodeSetsArchiveError, match="rollback table shape differs"):
        await archive.rollback_activation(_session(), destination="destination", activation=activation)

    column_signature.side_effect = [("same",), ("same",)]
    slice_identity = AsyncMock(return_value=(2, "c" * 64, 13))
    monkeypatch.setattr(archive, "_slice_identity", slice_identity)
    with pytest.raises(archive.CodeSetsArchiveError, match="rollback predecessor differs"):
        await archive.rollback_activation(_session(), destination="destination", activation=activation)

    slice_identity.side_effect = [
        (source_generation.row_count, source_generation.row_sha256, 13),
        (2, "c" * 64, current.code_catalog_oid),
    ]
    column_signature.side_effect = [("same",), ("same",)]
    monkeypatch.setattr(archive, "_has_collision", AsyncMock(return_value=False))
    monkeypatch.setattr(archive, "_replace_slice", AsyncMock())
    with pytest.raises(archive.CodeSetsArchiveError, match="rollback result differs"):
        await archive.rollback_activation(_session(), destination="destination", activation=activation)

    slice_identity.side_effect = [
        (source_generation.row_count, source_generation.row_sha256, 13),
        (source_generation.row_count, source_generation.row_sha256, current.code_catalog_oid),
    ]
    column_signature.side_effect = [("same",), ("same",)]
    monkeypatch.setattr(archive, "_set_generation", AsyncMock(return_value=current))
    with pytest.raises(archive.CodeSetsArchiveError, match="rollback authority differs"):
        await archive.rollback_activation(_session(), destination="destination", activation=activation)


@pytest.mark.asyncio
async def test_cleanup_drops_owned_stage(monkeypatch):
    stage = _stage()
    verify_clone = AsyncMock()
    monkeypatch.setattr(archive, "_verify_clone", verify_clone)
    cleanup_session = _session()
    await archive.cleanup_stage(cleanup_session, stage, predecessor_oid=13)
    verify_clone.assert_awaited_with(cleanup_session, stage.schema_name, stage.schema_oid, stage.catalog_oid, 13)
    statements = [call.args[0].text for call in cleanup_session.execute.await_args_list]
    assert statements == [
        f'DROP TABLE "{stage.schema_name}".{archive.PREDECESSOR_TABLE}',
        f'DROP TABLE "{stage.schema_name}".{CodeCatalog.__tablename__}',
        f'DROP SCHEMA "{stage.schema_name}"',
    ]
    assert all("CASCADE" not in statement for statement in statements)
    cleanup_without_predecessor = _session()
    await archive.cleanup_stage(cleanup_without_predecessor, stage)
    verify_clone.assert_awaited_with(
        cleanup_without_predecessor, stage.schema_name, stage.schema_oid, stage.catalog_oid, None
    )
    assert [call.args[0].text for call in cleanup_without_predecessor.execute.await_args_list] == statements[1:]
