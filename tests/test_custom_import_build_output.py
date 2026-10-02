# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete winner groups and frozen proof identity precede ordinary finality."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildVerification,
    CustomImportGeneration,
    CustomImportRootScalar,
)
from process.custom_import import build_output as output
from process.custom_import import materialization as material
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRegistry, CandidateRunnerError
from tests.test_custom_import_build_graph import _request
from tests.test_custom_import_materialization import _child_candidate


def _generation(request):
    return CustomImportGeneration(
        generation_id=10,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=20,
        producing_fence=request.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
    )


def _candidate(family_id=30, child_id=40, digest=b"f" * 32):
    candidate = _child_candidate(
        family_revision_id=family_id, child_revision_id=child_id, semantic_suffix="synthetic", service_code="A100"
    )
    return replace(candidate, family_sha256=digest)


def _group(request, candidate):
    registry = CandidateRegistry({"rates": 7}, {"providers": 1, "rates": 2}, 1)
    contract = material._winner_candidate_contract(
        request.definition, material._profile_scopes(request.definition, registry.child_collection_slots)
    )
    normalized = material._normalize_winner_candidate(candidate, contract)
    _, digest = material._context_key(request.definition.selection_profiles[0], normalized, contract.fields_by_id)
    return registry, (1, candidate.entity_binding_id, digest)


@pytest.mark.asyncio
async def test_membership_retry_pages_over_completed_keys(monkeypatch):
    memberships = [
        [
            SimpleNamespace(root_record_id=1, family_revision_id=11, attached=True),
            SimpleNamespace(root_record_id=2, family_revision_id=12, attached=False),
        ],
        [SimpleNamespace(root_record_id=3, family_revision_id=13, attached=False)],
        [],
    ]
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[SimpleNamespace(all=lambda page=page: page) for page in memberships]),
        add_all=Mock(),
    )

    @asynccontextmanager
    async def _page(*_args):
        yield session, SimpleNamespace(generation_id=10)

    monkeypatch.setattr(output, "_page_session", _page)
    for name in ("_prepare_statement", "_flush_page", "_heartbeat"):
        monkeypatch.setattr(output, name, AsyncMock())
    await output._attach_families(None, _request(page_row_limit=2), 7)
    statements = [call.args[0] for call in session.execute.await_args_list]
    assert [statement.compile().params["root_record_id_1"] for statement in statements] == [0, 2, 3]
    assert all("EXISTS" not in str(criterion) for statement in statements for criterion in statement._where_criteria)
    assert [model.root_record_id for call in session.add_all.call_args_list for model in call.args[0]] == [2, 3]


def test_reducer_exhausts_one_large_group_including_losers(monkeypatch):
    request = _request()
    first = _candidate()
    registry, group = _group(request, first)
    exhausted_flags = []

    def _candidates(*_args):
        for index in range(5000):
            yield _candidate(30 + index, 40 + index, (5000 - index).to_bytes(32, "big"))
        exhausted_flags.append(True)

    monkeypatch.setattr(output, "_context_candidates", _candidates)
    monkeypatch.setattr(output, "_one_row", lambda *_args: (SimpleNamespace(candidate_context_id=99),))
    winner, context_id = output._reduce_group(None, request, registry, 7, _generation(request), group)
    assert exhausted_flags == [True]
    assert winner.family_revision_id == 5029 and context_id == 99


@pytest.mark.parametrize("has_better", [False, True])
def test_surviving_tie_rules_are_the_existing_comparator(monkeypatch, has_better):
    request = _request()
    first = _candidate()
    conflict = replace(first, family_revision_id=31, context_child_revision_id=41)
    better = _candidate(32, 42, b"\x00" * 32)
    registry, group = _group(request, first)
    candidates = (first, conflict, better) if has_better else (first, conflict)
    monkeypatch.setattr(output, "_context_candidates", lambda *_args: iter(candidates))
    monkeypatch.setattr(output, "_one_row", lambda *_args: (SimpleNamespace(candidate_context_id=99),))
    if not has_better:
        with pytest.raises(material.WinnerMaterializationError, match="conflicting physical identity"):
            output._reduce_group(None, request, registry, 7, _generation(request), group)
        return
    winner, _ = output._reduce_group(None, request, registry, 7, _generation(request), group)
    assert winner.family_revision_id == 32


def test_late_group_failure_cannot_produce_a_winner(monkeypatch):
    request = _request()
    first = _candidate()
    registry, group = _group(request, first)

    def _candidates(*_args):
        yield first
        raise CandidateRunnerError("late candidate validation")

    monkeypatch.setattr(output, "_context_candidates", _candidates)
    lookup = Mock()
    monkeypatch.setattr(output, "_one_row", lookup)
    with pytest.raises(CandidateRunnerError, match="late candidate"):
        output._reduce_group(None, request, registry, 7, _generation(request), group)
    lookup.assert_not_called()


def _proof_tuple():
    request = _request()
    generation = _generation(request)
    stamp = dt.datetime(2029, 1, 1, tzinfo=dt.UTC)
    build = CustomImportBuildAttempt(
        build_id=7,
        generation_id=generation.generation_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=20,
        producing_fence=request.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
        phase="verified",
        source_frozen_at=stamp,
        graph_frozen_at=stamp,
        output_frozen_at=stamp,
        verified_at=stamp,
    )
    proof = CustomImportBuildVerification(
        build_id=7,
        generation_id=generation.generation_id,
        verification_state="complete",
        scan_stage="complete",
        source_frozen_at=stamp,
        graph_frozen_at=stamp,
        output_frozen_at=stamp,
        verified_at=stamp,
    )
    return build, generation, proof


@pytest.mark.parametrize(
    "model_index,field,bad_value",
    [
        (0, "phase", "verifying"),
        (0, "generation_id", 11),
        (1, "dataset_id", 2),
        (1, "definition_revision_id", 3),
        (1, "schema_revision_id", 4),
        (1, "execution_id", 5),
        (1, "capture_bundle_id", 21),
        (1, "producing_fence", 2),
        (1, "producing_token_sha256", b"z" * 32),
        (2, "build_id", 8),
        (2, "generation_id", 11),
        (2, "verification_state", "scanning"),
        (2, "scan_stage", "families"),
        (2, "source_frozen_at", None),
        (2, "graph_frozen_at", None),
        (2, "output_frozen_at", None),
        (2, "verified_at", None),
    ],
)
def test_structural_proof_binds_exact_frozen_identity(model_index, field, bad_value):
    models = _proof_tuple()
    output._proof_matches(*models)
    setattr(models[model_index], field, bad_value)
    with pytest.raises(CandidateRunnerError, match="exact complete frozen"):
        output._proof_matches(*models)


@pytest.mark.parametrize(
    "pointer_generation,pointer_version,digest,is_unchanged",
    [
        (9, 2, b"d" * 32, True),
        (10, 2, b"d" * 32, False),
        (9, 3, b"d" * 32, False),
        (9, 2, b"e" * 32, False),
        (None, None, b"d" * 32, False),
    ],
)
@pytest.mark.asyncio
async def test_no_change_requires_original_pointer_and_digest(
    monkeypatch, pointer_generation, pointer_version, digest, is_unchanged
):
    request = _request(expected_base_generation_id=9, expected_pointer_version=2)
    build = SimpleNamespace(base_generation_id=9, base_pointer_version=2)
    base = SimpleNamespace(generation_id=9)
    seal = SimpleNamespace(effective_output_sha256=digest)
    pointer = (
        None
        if pointer_generation is None
        else SimpleNamespace(generation_id=pointer_generation, pointer_version=pointer_version)
    )
    monkeypatch.setattr(output, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(output.publication, "_locked_generation", AsyncMock(return_value=base))
    monkeypatch.setattr(output.publication, "_validated_generation_seal", AsyncMock(return_value=seal))
    monkeypatch.setattr(output.publication, "_locked_pointer", AsyncMock(return_value=pointer))
    compare = Mock(wraps=output.publication._require_expected_pointer)
    monkeypatch.setattr(output.publication, "_require_expected_pointer", compare)
    actual = await output._unchanged_base(None, request, build, SimpleNamespace(effective_output_sha256=b"d" * 32))
    assert (actual is not None) is is_unchanged
    assert compare.call_count == int(is_unchanged)


def test_scalar_validation_rejects_missing_extra_or_changed_rows(monkeypatch):
    expected = CustomImportRootScalar(root_revision_id=1, field_slot=2, value_state="value", string_value="one")
    changed = CustomImportRootScalar(root_revision_id=1, field_slot=2, value_state="value", string_value="two")
    for actual in ((), ((expected,), (expected,)), ((changed,),)):
        monkeypatch.setattr(output, "_read_rows", lambda *_args, **_kwargs: iter(actual))
        with pytest.raises(CandidateRunnerError, match="typed scalar"):
            output._verify_projections(None, _request(), 7, [expected], None, CustomImportRootScalar, ())


def test_scalar_validation_compares_typed_decimal_values_not_storage_scale(monkeypatch):
    expected = CustomImportRootScalar(
        root_revision_id=1, field_slot=2, field_type="decimal", value_state="value", decimal_value=Decimal("12")
    )
    actual = CustomImportRootScalar(
        root_revision_id=1,
        field_slot=2,
        field_type="decimal",
        value_state="value",
        decimal_value=Decimal("12.000000000000"),
    )
    monkeypatch.setattr(output, "_read_rows", lambda *_args, **_kwargs: iter(((actual,),)))
    output._verify_projections(None, _request(), 7, [expected], None, CustomImportRootScalar, ())
