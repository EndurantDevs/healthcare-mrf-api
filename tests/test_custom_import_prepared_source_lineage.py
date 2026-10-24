# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact verifier layout and sealed prepared identity; native guards are separate."""

import hashlib
from copy import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process.custom_import import prepared_source_lineage as lineage
from process.custom_import.prepared_source_metadata import prepared_source_summary, summarize_prepared_stream
from process.custom_import.prepared_source_seal import _part_metadata
from process.custom_import.prepared_source_store import prepared_source_identity
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_prepared_source_context import _retained_rows
from tests.test_custom_import_prepared_source_seal import _closed_header, _context, _part_rows
from tests.test_custom_import_verification_root_ranges import _migration


@pytest.mark.parametrize("sequence", (None, 0, 25, 27, 58, 60, 62))
def test_unrelated_checkpoint_reads_only_its_sequence(sequence, monkeypatch):
    request = object()
    execute = Mock(return_value=SimpleNamespace(scalar_one_or_none=lambda: sequence))
    session = SimpleNamespace(in_transaction=lambda: True, execute=execute)
    prepare = Mock()
    monkeypatch.setattr(lineage, "_prepare_read", prepare)

    def forbidden(*_args):
        raise AssertionError("unrelated checkpoint must not load binding or verification authority")

    for name in ("_binding_query", "_names", "_owner", "_header_summary", "_finality"):
        monkeypatch.setattr(lineage, name, forbidden)
    assert lineage.verify_prepared_source_lineage(session, request, 7, deadline=20) is None
    prepare.assert_called_once_with(session, request, 20)
    execute.assert_called_once()
    statement, parameters = execute.call_args.args
    assert tuple(column.name for column in statement.selected_columns) == ("page_sequence",)
    assert statement.compile().params == {"build_id_1": 7} and parameters == {}
    assert "FOR UPDATE" not in str(statement)


@pytest.mark.parametrize("locked_sequence", (None, 27, 59))
def test_moved_checkpoint_defers_without_running_or_advancing_a_different_check(locked_sequence, monkeypatch):
    request = SimpleNamespace(definition=object())
    session = SimpleNamespace(in_transaction=lambda: True)
    execution = SimpleNamespace(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    loaded = SimpleNamespace(**vars(execution), definition=request.definition)
    binding_records = (
        object(),
        execution,
        SimpleNamespace(binding_contract=lineage.OBJECT_SOURCE_BINDING_CONTRACT),
        object(),
        object(),
    )
    proof = None if locked_sequence is None else {"page_sequence": locked_sequence}
    read_proof = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: proof))
    execution_results = (
        SimpleNamespace(scalar_one_or_none=lambda: 26),
        SimpleNamespace(one_or_none=lambda: binding_records),
        None,
        None,
        SimpleNamespace(one=lambda: binding_records),
        read_proof,
    )
    execute = Mock(side_effect=execution_results)
    owner = Mock()
    monkeypatch.setattr(lineage, "_execute", execute)
    monkeypatch.setattr(lineage, "_names", lambda _session: ("synthetic_control", '"synthetic_control"'))
    monkeypatch.setattr(lineage, "_owner", owner)
    monkeypatch.setattr(lineage, "_verify_request", Mock())
    monkeypatch.setattr(lineage, "_loaded_object_source_binding", lambda *_args: loaded)

    def forbidden(*_args):
        raise AssertionError("moved checkpoint must not run a structural check or update its receipt")

    for name in ("_proof_valid", "_finality", "_header_summary"):
        monkeypatch.setattr(lineage, name, forbidden)
    assert lineage.verify_prepared_source_lineage(session, request, 7, deadline=20) is None
    owner.assert_called_once()
    assert execute.call_count == len(execution_results)
    statements = tuple(str(call.args[3]) for call in execute.call_args_list)
    assert "lock_custom_import_build" in statements[3] and "FOR UPDATE" in statements[-1]
    assert not any(statement.startswith("UPDATE ") for statement in statements)


@pytest.mark.parametrize(
    "schema", ("mrf", "synthetic_control", 'synthetic "schema', "synthetic'schema", "synthetic\\schema")
)
def test_layout_digest_and_checkpoint_order_match_the_current_renderer(schema):
    migration = _migration()
    control = '"' + schema.replace('"', '""') + '"'
    parameters = lineage._identity_parameters(schema, control)
    body = " " + migration._body(schema) + " "
    normalized = (
        body.replace(parameters["literal"], "E'__CONTROL__'")
        .replace(parameters["escaped_identifier"], '"__CONTROL__"')
        .replace(parameters["identifier"], '"__CONTROL__"')
    )
    assert hashlib.sha256(normalized.encode()).digest() == lineage._VERIFIER_SHA256
    labels = tuple(label for group in migration._arms()._LABELS for label in group)
    stages = migration._arms()._STAGES
    assert tuple(labels[sequence] for sequence in lineage._CHECKPOINTS) == (
        "source_occurrence_position",
        "capture_stream",
    )
    for sequence, (stage, next_stage, _query) in lineage._CHECKPOINTS.items():
        assert (stage, next_stage) == (stages[sequence], stages[sequence + 1])


def _sealed_material():
    retained = _retained_rows()
    context = _context(retained)
    rows = _part_rows(context, 2)
    header = _closed_header(context, rows)
    summaries = tuple(
        summarize_prepared_stream(
            (_part_metadata(row, context) for row in rows if row[3] == slot), stream_slot=slot, policy=context.policy
        )
        for slot in (1, 2)
    )
    summary = prepared_source_summary(
        prepared_source_identity(context), summaries, stream_count=2, policy=context.policy
    )
    header.state, header.sealed_at = "sealed", context.build_deadline_at
    header.canonical_summary, header.prepared_sha256 = summary.canonical, summary.digest
    build = SimpleNamespace(capture_bundle_id=context.capture_bundle_id, build_deadline_at=context.build_deadline_at)
    return header, build, retained, summaries


def test_header_uses_retained_identity_and_original_deadline_without_source_permit():
    header, build, retained, expected = _sealed_material()
    assert lineage._header_summary(header, build, retained.execution, retained.bundle, retained.loaded) == expected
    header.producer_fence += 1
    header.producer_token_sha256 = b"x" * 32
    assert lineage._header_summary(header, build, retained.execution, retained.bundle, retained.loaded) == expected


@pytest.mark.parametrize(
    "field,value",
    (
        ("state", "closed"),
        ("sealed_at", None),
        ("execution_id", 99),
        ("capture_bundle_id", 99),
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("source_binding_revision_id", 99),
        ("source_binding_sha256", b"x" * 32),
        ("request_identity_sha256", b"x" * 32),
        ("raw_manifest_sha256", b"x" * 32),
        ("processing_policy_sha256", b"x" * 32),
        ("canonical_processing_policy", "{}"),
        ("canonical_summary", "{}"),
        ("prepared_sha256", b"x" * 32),
        ("record_count", 99),
        ("preparation_deadline_at", None),
        ("version", 0),
    ),
)
def test_header_refuses_changed_pins_or_incomplete_preparation(field, value):
    header, build, retained, _expected = _sealed_material()
    setattr(header, field, value)
    with pytest.raises(CandidateRunnerError, match="lineage_invalid"):
        lineage._header_summary(header, build, retained.execution, retained.bundle, retained.loaded)


def _proof(sequence):
    frozen = object()
    proof = dict.fromkeys(lineage._CURSORS)
    proof.update(dict.fromkeys(lineage._COUNTS, 0))
    proof.update(
        page_sequence=sequence,
        scan_stage=lineage._CHECKPOINTS[sequence][0],
        verification_contract="custom-import/build-structure/v1",
        _manual_acceptance=None,
        verification_state="scanning",
        verified_at=None,
        generation_id=7,
        source_frozen_at=frozen,
        graph_frozen_at=frozen,
        output_frozen_at=frozen,
    )
    return proof, SimpleNamespace(
        phase="verifying", generation_id=7, source_frozen_at=frozen, graph_frozen_at=frozen, output_frozen_at=frozen
    )


@pytest.mark.parametrize("sequence", (26, 59))
def test_only_the_exact_unaccounted_ordinary_checkpoint_can_advance(sequence):
    proof, build = _proof(sequence)
    assert lineage._proof_valid(proof, build) == lineage._CHECKPOINTS[sequence]
    for field, value in (
        ("verification_contract", "custom-import/manual-acceptance/v1"),
        ("_manual_acceptance", {}),
        ("verification_state", "complete"),
        ("scan_stage", "complete"),
        ("verified_at", object()),
        ("generation_id", 8),
        ("source_frozen_at", object()),
        ("graph_frozen_at", None),
        *((name, 1) for name in (*lineage._CURSORS, *lineage._COUNTS)),
    ):
        changed = copy(proof)
        changed[field] = value
        with pytest.raises(CandidateRunnerError, match="progress_mismatch"):
            lineage._proof_valid(changed, build)
