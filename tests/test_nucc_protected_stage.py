"""Named candidate orchestration; actual native grants require PostgreSQL proof."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import nucc_protected_stage as stage

DATASET = UUID("11111111-1111-4111-8111-111111111111")


def ownership():
    return stage.native.ReferenceFamilyStageOwnership(
        "nucc", DATASET, stage.native.reference_family_stage_schema(DATASET), None, (("nucc_taxonomy", None),)
    )


class Transaction:
    def __init__(self):
        self.statements = []
        self.events = []
        self.scalar = AsyncMock(return_value=False)

    def in_transaction(self):
        return True

    @asynccontextmanager
    async def begin_nested(self):
        self.events.append("savepoint")
        try:
            yield self
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("release")

    async def connection(self):
        self.events.append("materialize")
        return self

    async def execute(self, statement):
        self.statements.append(str(statement))


@pytest.mark.asyncio
@pytest.mark.parametrize("source", (False, True))
async def test_precreate_freezes_empty_model_and_grants_candidate_insert(monkeypatch, source):
    candidate, session = ownership(), Transaction()
    create, seal = AsyncMock(), AsyncMock()
    monkeypatch.setattr(stage.native, "_create_model_family", create)
    monkeypatch.setattr(stage.native, "capture_model_family_stage_ownership", AsyncMock(return_value=candidate))
    monkeypatch.setattr(stage.native, "seal_model_family_storage", seal)
    actor = AsyncMock(return_value={"role": "source_builder"})
    monkeypatch.setattr(stage, "_source_actor", actor)
    source_session = object() if source else None
    assert await stage.precreate_nucc_stage(session, dataset_id=DATASET, source_session=source_session) is candidate
    create.assert_awaited_once_with(
        session,
        stage.native.reference_family_spec("nucc"),
        candidate.schema_name,
        create_indexes=False,
        ordinary_heaps=True,
    )
    seal.assert_awaited_once_with(session, candidate, require_empty=True, freeze_schema=False)
    assert session.events == ["savepoint", "materialize", "release"]
    assert session.statements == (
        [
            f'GRANT USAGE ON SCHEMA "{candidate.schema_name}" TO "source_builder"',
            f'GRANT INSERT ON "{candidate.schema_name}"."nucc_taxonomy" TO "source_builder"',
        ]
        if source
        else []
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", (None, "11111111-1111-4111-8111-111111111111", True))
async def test_precreate_refuses_non_native_dataset(monkeypatch, dataset):
    create = AsyncMock()
    monkeypatch.setattr(stage, "_precreate_nucc_stage", create)
    with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="dataset differs"):
        await stage.precreate_nucc_stage(Transaction(), dataset_id=dataset)
    create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("records", ([], None, "rows", [()] * 5001))
async def test_copy_refuses_outside_bounded_batch_before_native_io(monkeypatch, records):
    copy = AsyncMock()
    monkeypatch.setattr(stage.native, "native_copy_record_batch", copy)
    with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="batch exceeds bound"):
        await stage.copy_nucc_stage(Transaction(), ownership(), records)
    copy.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", (False, True))
async def test_copy_uses_fixed_model_column_order_and_preserves_native_permission_denial(monkeypatch, denied):
    candidate, session = ownership(), Transaction()
    verify = AsyncMock()
    copy = AsyncMock(side_effect=PermissionError("native INSERT denied") if denied else None, return_value=1)
    monkeypatch.setattr(stage.native, "verify_reference_family_stage_ownership", verify)
    monkeypatch.setattr(stage.native, "native_copy_record_batch", copy)
    if denied:
        with pytest.raises(PermissionError, match="native INSERT denied"):
            await stage.copy_nucc_stage(session, candidate, [(1,)])
    else:
        assert await stage.copy_nucc_stage(session, candidate, [(1,)]) == 1
    privilege_query, parameters = session.scalar.await_args.args
    assert "has_table_privilege" in str(privilege_query)
    assert parameters == {"relation": f'"{candidate.schema_name}"."nucc_taxonomy"'}
    verify.assert_awaited_once_with(session, candidate)
    model = stage.native.reference_family_spec("nucc").model_types[0]
    copy.assert_awaited_once_with(
        session,
        model,
        schema_name=candidate.schema_name,
        table_name="nucc_taxonomy",
        columns=tuple(model.__table__.columns.keys()),
        records=[(1,)],
    )
    assert session.statements[0] == f'GRANT INSERT ON "{candidate.schema_name}"."nucc_taxonomy" TO CURRENT_USER'
    if denied:
        assert session.events[-1] == "rollback"
    else:
        assert session.statements[-1] == f'REVOKE INSERT ON "{candidate.schema_name}"."nucc_taxonomy" FROM CURRENT_USER'
        assert session.events[-1] == "release"


@pytest.mark.asyncio
@pytest.mark.parametrize("source", (False, True))
async def test_candidate_close_revokes_source_insert_before_configured_freeze(monkeypatch, source):
    session, candidate = Transaction(), ownership()
    seal = AsyncMock()
    monkeypatch.setattr(stage.native, "seal_model_family_storage", seal)
    actor = AsyncMock(return_value={"role": "source_builder"})
    monkeypatch.setattr(stage, "_source_actor", actor)
    source_session = object() if source else None
    await stage._close_candidate_grants(session, candidate, source_session=source_session)
    assert session.statements == [
        f'LOCK TABLE ONLY "{candidate.schema_name}"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
        *([f'REVOKE INSERT ON "{candidate.schema_name}"."nucc_taxonomy" FROM "source_builder"'] if source else []),
    ]
    seal.assert_awaited_once_with(session, candidate, freeze_schema=False)
    assert actor.await_count == int(source)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "freeze", "indexes", "validation", "content", "layout"))
async def test_finish_closes_writes_before_indexes_and_full_semantic_validation(monkeypatch, failure):
    candidate, session, events = ownership(), Transaction(), []

    async def close(*_args, **_options):
        events.append("close-writes")
        if failure == "freeze":
            raise PermissionError("freeze denied")

    async def indexes(*_args):
        events.append("indexes")
        if failure == "indexes":
            raise RuntimeError("index failure")

    async def validate(*_args, **_options):
        events.append("set-validation")
        if failure == "validation":
            raise RuntimeError("invalid indexed set")
        return {"row_count": 4}

    async def freeze(*_args):
        events.append("full-schema-freeze")

    monkeypatch.setattr(stage.native, "seal_model_family_storage", freeze)
    child_by_field = {"schema": candidate.schema_name, "name": "nucc_taxonomy"}
    incoming = AsyncMock(
        side_effect=[child_by_field, {**child_by_field, "schema": "other"}] if failure == "layout" else None,
        return_value=child_by_field,
    )
    monkeypatch.setattr(stage, "_close_candidate_grants", close)
    monkeypatch.setattr(stage.native, "verify_reference_family_stage_ownership", AsyncMock())
    monkeypatch.setattr(stage.native, "complete_reference_family_restore", indexes)
    monkeypatch.setattr(stage.native, "validate_nucc_reference_set", validate)
    monkeypatch.setattr(
        stage.native, "_projected_row_identity", AsyncMock(return_value=(5 if failure == "content" else 4, "a" * 64))
    )
    monkeypatch.setattr(stage.native, "_family_schema_identity", AsyncMock(return_value="b" * 64))
    monkeypatch.setattr(stage.native, "_relation_oid", AsyncMock(return_value=1))
    monkeypatch.setattr(stage.partition, "_capture_incoming", incoming)
    if failure:
        with pytest.raises(
            (RuntimeError, PermissionError),
            match="freeze denied|index failure|invalid indexed set|census changed|layout changed",
        ):
            await stage.finish_nucc_stage(session, candidate)
        assert "rollback" in session.events
    else:
        prepared = await stage.finish_nucc_stage(session, candidate)
        assert prepared["validation"] == {"row_count": 4} and prepared["semantic_sha256"] == "a" * 64
        assert prepared["schema_sha256"] == "b" * 64
        assert all(
            call.args == (session, (candidate.schema_name, "nucc_taxonomy")) for call in incoming.await_args_list
        )
    assert events[:1] == ["close-writes"]
    if failure not in {"freeze", "indexes"}:
        assert events == ["close-writes", "indexes", "full-schema-freeze", "set-validation"]
