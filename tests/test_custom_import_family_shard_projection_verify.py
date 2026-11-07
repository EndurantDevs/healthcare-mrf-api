# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Projection proof logic; native SQL and custody require separate qualification."""

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import family_shard_projection_verify as verification
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_validate import (
    CandidateScope,
    ClosedCandidateIdentity,
    FamilyBlockValidationError,
)
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from process.custom_import.materialization import GenerationIdentity
from process.custom_import.query_projection_evidence import ProjectionDigest
from process.custom_import.query_projection_sources import field_slots
from process.custom_import.query_projection_values import configured_fields
from tests.test_custom_import_query_projection_sources import _definition

_LIMITS = SourceFactLimits(16 * 1024**2, 1024**2, 128 * 1024)


def _scope():
    layout = FamilyBlockLayout(_definition(), {"rates": 7})
    tables = layout.tables(17)
    binding = FamilyBlockBinding(17, layout, tables, {role: 1000 + slot for slot, role in enumerate(tables)}, 9)
    scope = CandidateScope(17, 4, 1, 2, 3, 5, 6, 7, b"t" * 32)
    identity = ClosedCandidateIdentity(
        scope, datetime(2030, 1, 1, tzinfo=timezone.utc), layout.digest, 9, tuple(sorted(binding.oids_by_role.items()))
    )
    return binding, GenerationIdentity(4, 1, 2, 3), identity


@pytest.mark.parametrize(
    "fault", ("generation", "dataset", "definition", "schema", "snapshot", "owner", "oids", "layout")
)
def test_complete_closed_identity_is_required(fault):
    binding, generation, identity = _scope()
    verification._scope(binding, generation, identity)
    if fault in {"generation", "dataset", "definition", "schema"}:
        name = {
            "generation": "generation_id",
            "dataset": "dataset_id",
            "definition": "definition_revision_id",
            "schema": "schema_revision_id",
        }[fault]
        generation = replace(generation, **{name: 99})
    else:
        changes_by_fault = {
            "snapshot": {"family_id": 99},
            "owner": {"owner_oid": 99},
            "oids": {"oids_by_role": {}},
            "layout": {"layout": FamilyBlockLayout(_definition(reserved=True), {"rates": 7})},
        }
        binding = replace(binding, **changes_by_fault[fault])
    with pytest.raises((FamilyBlockValidationError, ValueError)):
        verification._scope(binding, generation, identity)


def test_duck_scope_cannot_omit_the_producer_parameters():
    binding, generation, identity = _scope()
    scope = SimpleNamespace(**vars(identity.scope), parameters=lambda: {"base_generation_id": None})
    with pytest.raises(FamilyBlockValidationError, match="closed scope"):
        verification._scope(binding, generation, replace(identity, scope=scope))


@pytest.mark.parametrize("role", ("query_root", "query_child"))
def test_semantic_order_uses_canonical_values_and_full_entity_scope(role):
    binding, generation, _identity = _scope()
    compiled = verification._semantic_statement(binding, generation, role).compile(dialect=dialect())
    sql = str(compiled)
    assert 'canonical_value COLLATE "C"' in sql
    assert "adapter_id" in sql and "dataset_id" in sql
    assert "npi" in compiled.params.values() and 1 in compiled.params.values()
    if role == "query_child":
        assert "key_variant" in sql and "key_rank" in sql
        assert 'canonical_child_key COLLATE "C"' in sql
    order = sql.split("ORDER BY", 1)[1]
    assert "entity_binding_id" not in order and "key_rank" not in order and "child_key_sha256" not in order


def _rows(definition, child):
    fields = tuple(field for field in configured_fields(definition) if (field.collection is not None) == child)
    slots = field_slots(definition)
    return [
        {
            "canonical_entity": entity,
            "selected_value": selected,
            **({"canonical_child_key": key} if child else {}),
            **{
                f"field_{slots[field.field_id]}": None
                if index == 0
                else (Decimal("1.000000000002") if field.value_type == "decimal" else -2)
                for field in fields
            },
        }
        for index, (entity, selected, key) in enumerate((("entity_a", -1, '["a"]'), ("entity_b", 2, '["z"]')))
    ]


class _Cursor:
    def __init__(self, rows, failure=None, close_failure=None):
        self.rows, self.failure, self.close_failure = rows, failure, close_failure
        self.closed = False

    def mappings(self):
        return self

    async def partitions(self, count):
        for offset in range(0, len(self.rows), count):
            yield self.rows[offset : offset + count]
        if self.failure is not None:
            raise self.failure

    async def close(self):
        self.closed = True
        if self.close_failure is not None:
            raise self.close_failure


class _Transport:
    """Explicit SQL transport fake; no native authority or constraint assertion."""

    def __init__(self, rows):
        self.rows = rows
        self.census = SimpleNamespace(row_count=len(rows), row_bytes=2048 if rows else 0)
        self.cursor = _Cursor(rows)
        self.fetch = None

    async def execute(self, statement):
        sql = str(statement.compile(dialect=dialect()))
        assert "count(" in sql and "ORDER BY" not in sql
        if "max(" not in sql:
            return SimpleNamespace(scalar_one=lambda: len(self.rows))
        return SimpleNamespace(one=lambda: self.census)

    async def stream(self, statement):
        self.fetch = statement.get_execution_options()["yield_per"]
        assert 1 <= self.fetch <= 2
        return self.cursor


async def _digest(tmp_path, role, transport, ready=None):
    binding, generation, _identity = _scope()
    work = _Scratch(PlanWork(tmp_path / "proof", _LIMITS, lambda _path: None))
    digest = ProjectionDigest(binding.layout.definition)

    async def prepared():
        if ready is not None:
            await ready()

    try:
        await verification._digest_scope(
            transport,
            binding,
            generation,
            role,
            digest,
            work,
            SourceAllocationLimits(_LIMITS, 2, 8192),
            prepared,
        )
        return digest
    finally:
        work.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("child", (False, True))
async def test_native_cursor_hashes_exact_original_digest_inputs(tmp_path, child):
    binding, _generation, _identity = _scope()
    definition, role = binding.layout.definition, "query_child" if child else "query_root"
    rows = _rows(definition, child)
    transport = _Transport(rows)
    observed = await _digest(tmp_path, role, transport)
    expected = ProjectionDigest(definition)
    fields = tuple(field for field in configured_fields(definition) if (field.collection is not None) == child)
    slots = field_slots(definition)
    for row in rows:
        values_by_field = {field.field_id: row[f"field_{slots[field.field_id]}"] for field in fields}
        if child:
            expected.add_child(
                row["canonical_entity"], row["selected_value"], row["canonical_child_key"], values_by_field
            )
        else:
            expected.add_root(row["canonical_entity"], row["selected_value"], values_by_field)
    assert observed.finish() == expected.finish() and observed.checkpoint() == expected.checkpoint()
    assert transport.cursor.closed and not (tmp_path / "proof").exists()


def _damage_stream(transport, fault):
    if fault in {"missing", "extra", "order"}:
        rows_by_fault = {
            "missing": transport.rows[:1],
            "extra": transport.rows + [dict(transport.rows[-1], canonical_entity="entity_c")],
            "order": list(reversed(transport.rows)),
        }
        transport.cursor.rows = rows_by_fault[fault]
        return
    if fault == "width":
        transport.census.row_bytes = 8193
    elif fault == "late":
        transport.cursor.failure = RuntimeError("synthetic cursor EOF failure")
    elif fault == "close":
        transport.cursor.close_failure = RuntimeError("synthetic cursor close failure")
    else:
        transport.census.row_count += 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("missing", "extra", "width", "late", "close", "order", "census"))
async def test_incomplete_or_unbounded_semantic_stream_cannot_complete(tmp_path, fault):
    binding, _generation, _identity = _scope()
    transport = _Transport(_rows(binding.layout.definition, False))
    _damage_stream(transport, fault)
    with pytest.raises((FamilyBlockValidationError, FamilyBlockResourceLimit, RuntimeError)):
        await _digest(tmp_path, "query_root", transport)
    if fault not in {"width", "census"}:
        assert transport.cursor.closed
    assert not (tmp_path / "proof").exists()


@pytest.mark.asyncio
async def test_read_transaction_cannot_change_during_preparation(tmp_path, monkeypatch):
    state = SimpleNamespace(token=object())

    async def identity(_session):
        return state.token, None, None, None

    async def prepare(_session):
        state.token = object()

    monkeypatch.setattr(verification, "_read_transaction_identity", identity)
    work = _Scratch(PlanWork(tmp_path / "proof", _LIMITS, lambda _path: None))
    try:
        with pytest.raises(FamilyBlockValidationError, match="changed its transaction"):
            await verification._prepare(None, await identity(None), work, prepare)
    finally:
        work.close()


@pytest.mark.asyncio
async def test_last_index_statement_requires_a_postcheck(monkeypatch):
    binding, _generation, identity = _scope()
    snapshot_by_name = {**identity.scope.parameters(), "frozen_at": None}
    descriptor_by_name = dict(
        family_id=binding.family_id,
        phase="closed",
        layout_contract="custom-import/family-block-layout/v1",
        layout_sha256=binding.layout.digest,
        definition_sha256=bytes.fromhex(binding.layout.definition.digest),
        collection_slots=dict(binding.layout.collection_slots),
        closed_at=identity.closed_at,
    )
    documents = iter((snapshot_by_name, descriptor_by_name))
    state = SimpleNamespace(changed=False)

    async def execute(_statement):
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: next(documents)))

    async def resolved(*_arguments, **_keywords):
        return binding

    async def ready():
        if state.changed:
            raise RuntimeError("synthetic final statement changed transaction or deadline")

    async def indexes(session, _binding, *, prepare):
        await prepare(session)
        state.changed = True

    monkeypatch.setattr(verification, "resolve_family_block_binding", resolved)
    monkeypatch.setattr(verification, "validate_family_block_indexes", indexes)
    with pytest.raises(RuntimeError, match="final statement"):
        await verification._closed_identity(SimpleNamespace(execute=execute), binding, identity, ready)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("closed", "verified", "logged"))
@pytest.mark.parametrize("fault", (None, "phase", "closure", "frozen", "owner", "oids", "capture"))
async def test_later_immutable_projection_recheck_preserves_every_identity_guard(monkeypatch, phase, fault):
    binding, _generation, identity = _scope()
    snapshot_by_name = {**identity.scope.parameters(), "frozen_at": None}
    descriptor_by_name = dict(
        family_id=binding.family_id,
        phase=phase,
        layout_contract="custom-import/family-block-layout/v1",
        layout_sha256=binding.layout.digest,
        definition_sha256=bytes.fromhex(binding.layout.definition.digest),
        collection_slots=dict(binding.layout.collection_slots),
        closed_at=identity.closed_at,
    )
    resolved_binding = binding
    if fault == "phase":
        descriptor_by_name["phase"] = "loading"
    if fault == "closure":
        descriptor_by_name["closed_at"] = identity.closed_at.replace(year=2031)
    if fault == "frozen":
        snapshot_by_name["frozen_at"] = identity.closed_at
    if fault == "owner":
        resolved_binding = replace(binding, owner_oid=99)
    if fault == "oids":
        resolved_binding = replace(binding, oids_by_role={})
    if fault == "capture":
        snapshot_by_name["capture_bundle_id"] = 99
    documents = iter((snapshot_by_name, descriptor_by_name))
    observations = []

    async def execute(_statement):
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: next(documents)))

    async def resolved(*_arguments, **_keywords):
        return resolved_binding

    async def ready():
        observations.append("ready")

    async def indexes(session, current_binding, *, prepare):
        assert current_binding is binding
        observations.append("indexes")
        await prepare(session)

    monkeypatch.setattr(verification, "resolve_family_block_binding", resolved)
    monkeypatch.setattr(verification, "validate_family_block_indexes", indexes)
    operation = verification._closed_identity(SimpleNamespace(execute=execute), binding, identity, ready)
    if fault is None:
        assert await operation is binding
        assert observations[-3:] == ["indexes", "ready", "ready"]
    else:
        with pytest.raises(FamilyBlockValidationError):
            await operation
        assert "indexes" not in observations
