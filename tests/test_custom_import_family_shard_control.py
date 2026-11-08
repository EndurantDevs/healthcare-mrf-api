# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The owner shard path retains sealed source, pointer and retry boundaries."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import family_shard_control as control
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.runner_types import CandidateRegistry, CandidateRunnerError
from tests.test_custom_import_build_source import _request


def _capture_case():
    request = _request()
    registry = CandidateRegistry({"details": 7}, {"root_source": 1, "detail_source": 2}, 1)
    identity_by_field = dict(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        capture_state="sealed",
        payload_contract="custom-import/parquet-parts/v2",
    )
    capture = SimpleNamespace(**identity_by_field, stream_count=2)
    streams = [SimpleNamespace(**identity_by_field, stream_slot=slot) for slot in (1, 2)]
    return request, registry, capture, streams


def test_all_sealed_streams_and_exact_registry_are_required():
    control._check_capture(*_capture_case(), 20)


@pytest.mark.parametrize(
    ("object_index", "field", "replacement"),
    [
        (0, "capture_state", "pending"),
        (0, "payload_contract", None),
        (0, "dataset_id", 999),
        (0, "stream_count", 1),
        (0, "stream_count", 10),
        (1, "capture_state", "pending"),
        (1, "payload_contract", None),
        (1, "schema_revision_id", 999),
        (1, "stream_slot", 2),
    ],
)
def test_capture_rejects_partial_legacy_wrong_scope_and_duplicate_streams(object_index, field, replacement):
    request, registry, capture, streams = _capture_case()
    setattr(capture if object_index == 0 else streams[0], field, replacement)
    with pytest.raises(FamilyBlockUnavailable, match="capture"):
        control._check_capture(request, registry, capture, streams, 20)


def test_missing_or_extra_capture_streams_and_projected_budget_refuse():
    request, registry, capture, streams = _capture_case()
    for partial in (streams[:1], [*streams, streams[0]]):
        with pytest.raises(FamilyBlockUnavailable, match="census"):
            control._check_capture(request, registry, capture, partial, 20)
    with pytest.raises(FamilyBlockUnavailable, match="shape"):
        control._check_capture(request, registry, capture, streams, 21)


async def test_fresh_build_cannot_use_changed_pointer_or_unsealed_base(monkeypatch):
    request = _request()
    pointer = None
    is_sealed = False

    async def current(*_args):
        return pointer

    async def execute(*_args):
        return SimpleNamespace(scalar_one=lambda: is_sealed)

    monkeypatch.setattr(control, "load_current_pointer", current)
    monkeypatch.setattr(control, "_execute", execute)
    await control._fresh_base(None, request)
    pointer = SimpleNamespace(generation_id=17, version=2)
    with pytest.raises(FamilyBlockUnavailable, match="pointer"):
        await control._fresh_base(None, request)
    retained = replace(request, expected_base_generation_id=17, expected_pointer_version=2)
    with pytest.raises(FamilyBlockUnavailable, match="sealed"):
        await control._fresh_base(None, retained)
    is_sealed = True
    await control._fresh_base(None, retained)


async def test_one_set_census_refuses_any_earlier_copy_or_generation(monkeypatch):
    tables_by_role = {
        role: Table(role, MetaData(), Column("id", Integer), schema="synthetic")
        for role in ("family", "fragment", "root_record", "pack", "shard_receipt")
    }
    occupied = [False] * 6
    statements = []

    async def execute(_session, statement):
        statements.append(str(statement.compile(dialect=dialect())))
        return SimpleNamespace(one=lambda: tuple(occupied))

    monkeypatch.setattr(control, "_execute", execute)
    binding = SimpleNamespace(tables_by_role=tables_by_role)
    await control._empty_candidate(None, _request(), binding)
    assert len(statements) == 1 and all(f"synthetic.{name}" in statements[0] for name in tables_by_role)
    for index in range(len(occupied)):
        occupied[index] = True
        with pytest.raises(FamilyBlockUnavailable, match="earlier candidate"):
            await control._empty_candidate(None, _request(), binding)
        occupied[index] = False


def test_control_keeps_original_identity_and_has_no_completed_phase():
    request = _request()
    execution = SimpleNamespace(capture_bundle_id=19, request_identity_sha256=b"r" * 32)
    build = control._new_build(request, execution)
    control._assert_build_identity(build, request, execution)
    assert build.phase is None and build.generation_id is None
    assert build.source_frozen_at is build.graph_frozen_at is build.output_frozen_at is build.verified_at is None
    with pytest.raises(CandidateRunnerError, match="identity or bounds"):
        control._assert_build_identity(build, replace(request, page_row_limit=request.page_row_limit - 1), execution)
