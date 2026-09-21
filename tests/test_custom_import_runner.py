# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit coverage for the transaction-owning custom-import candidate runner."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import process.custom_import.runner as runner
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import LeaseGrant
from process.custom_import.family import assemble_root_families
from process.custom_import.publication import (
    GenerationSealReceipt,
    PublicationConflict,
    PublicationKind,
    PublicationReceipt,
)
from process.custom_import.runner import CandidateRunnerError, CandidateRunRequest, CandidateRunResult, run_candidate
from process.custom_import.runner_codec import decode_payload_scalar, value_document

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json(_FIXTURE.read_text())


def _snapshot_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    document["refresh_mode"] = "snapshot"
    return CustomImportDefinition.from_json(json.dumps(document))


def _decimal_root_key_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    root = document["schema"]["root"]
    root["logical_key"] = ["npi", "rank"]
    root["fields"].append({"id": "rank", "slot": 6, "type": "decimal", "nullable": False})
    rates = document["schema"]["children"][0]
    rates["parent_key"].append({"child": "rate_rank", "root": "rank"})
    rates["fields"].append({"id": "rate_rank", "slot": 7, "type": "decimal", "nullable": False})
    return CustomImportDefinition.from_json(json.dumps(document))


def _decimal_child_key_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    rates = document["schema"]["children"][0]
    rates["child_key"] = ["amount"]
    rates["fields"][2]["nullable"] = False
    return CustomImportDefinition.from_json(json.dumps(document))


def _timestamp_child_key_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    rates = document["schema"]["children"][0]
    rates["child_key"] = ["captured_at"]
    rates["fields"].append({"id": "captured_at", "slot": 6, "type": "timestamp", "nullable": False})
    return CustomImportDefinition.from_json(json.dumps(document))


def _request(
    definition: CustomImportDefinition,
    *,
    roots: list[dict[str, object]] | None = None,
    children: list[dict[str, object]] | None = None,
) -> CandidateRunRequest:
    default_roots = [
        {
            "npi": "1234567893",
            "display_name": "Synthetic Clinic",
        }
    ]
    default_child_rows = [
        {
            "rate_npi": "1234567893",
            "service_code": "SYNTHETIC",
            "amount": Decimal("12.50"),
        }
    ]
    return CandidateRunRequest(
        dataset_id=11,
        definition_revision_id=12,
        schema_revision_id=13,
        execution_id=14,
        lease_token="synthetic-runner-token",
        definition=definition,
        roots=default_roots if roots is None else roots,
        children_by_collection={"rates": default_child_rows if children is None else children},
    )


def _grant() -> LeaseGrant:
    from datetime import UTC, datetime, timedelta

    return LeaseGrant(
        execution_id=14,
        fence=1,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
        state="running",
    )


def _publication(kind: PublicationKind = "activated") -> PublicationReceipt:
    return PublicationReceipt(
        publication_event_id=1,
        dataset_id=11,
        definition_revision_id=12,
        schema_revision_id=13,
        execution_id=14,
        event_kind=kind,
        from_generation_id=None,
        to_generation_id=31,
        expected_pointer_version=0,
        committed_pointer_version=1,
        event_sha256="0" * 64,
    )


def _seal() -> GenerationSealReceipt:
    return GenerationSealReceipt(
        generation_id=31,
        dataset_id=11,
        execution_id=14,
        materialization_sha256="0" * 64,
        effective_output_sha256="1" * 64,
        root_count=1,
        family_count=1,
        generation_family_count=1,
        family_child_count=1,
        winner_count=1,
        profile_count=1,
        root_scalar_count=1,
        child_scalar_count=1,
    )


@pytest.mark.asyncio
async def test_runner_routes_accepted_families_to_seal_and_activation(monkeypatch):
    request = _request(_definition())
    admission_by_name: dict[str, object] = {}

    async def claim(*_args):
        return _grant()

    async def materialize(_factory, _request, _grant_value, admitted):
        admission_by_name["families"] = admitted.families
        return runner._MaterializedCandidate(31, None, len(admitted.families), len(admitted.rejections))

    async def heartbeat(*_args):
        return _grant()

    async def seal(*_args):
        return _seal()

    async def activate(*_args):
        return _publication()

    monkeypatch.setattr(runner, "_claim", claim)
    monkeypatch.setattr(runner, "_materialize_candidate", materialize)
    monkeypatch.setattr(runner, "_heartbeat", heartbeat)
    monkeypatch.setattr(runner, "_seal", seal)
    monkeypatch.setattr(runner, "_activate", activate)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == "activated"
    assert run_result.generation_id == 31
    assert len(admission_by_name["families"]) == 1


@pytest.mark.asyncio
async def test_runner_retains_valid_root_when_another_root_has_invalid_child(monkeypatch):
    definition = _definition()
    request = _request(
        definition,
        roots=[
            {"npi": "1234567893", "display_name": "Synthetic First"},
            {"npi": "1003000126", "display_name": "Synthetic Second"},
        ],
        children=[
            {"rate_npi": "1234567893", "service_code": "FIRST", "amount": Decimal("10.00")},
            {"rate_npi": "1003000126", "service_code": "SECOND", "amount": True},
        ],
    )
    admission_by_name: dict[str, object] = {}

    async def claim(*_args):
        return _grant()

    async def materialize(_factory, _request, _grant_value, admitted):
        admission_by_name["accepted"] = len(admitted.families)
        admission_by_name["rejections"] = {rejection.code for rejection in admitted.rejections}
        return runner._MaterializedCandidate(31, None, len(admitted.families), len(admitted.rejections))

    async def heartbeat(*_args):
        return _grant()

    async def seal(*_args):
        return _seal()

    async def activate(*_args):
        return _publication()

    monkeypatch.setattr(runner, "_claim", claim)
    monkeypatch.setattr(runner, "_materialize_candidate", materialize)
    monkeypatch.setattr(runner, "_heartbeat", heartbeat)
    monkeypatch.setattr(runner, "_seal", seal)
    monkeypatch.setattr(runner, "_activate", activate)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == "activated"
    assert admission_by_name == {"accepted": 1, "rejections": {"field_type_invalid"}}


@pytest.mark.asyncio
async def test_runner_rejects_duplicate_canonical_root_keys(monkeypatch):
    definition = _decimal_root_key_definition()
    roots = [
        {"npi": "1234567893", "rank": 1, "display_name": "Synthetic First"},
        {"npi": "1234567893", "rank": "1", "display_name": "Synthetic Second"},
    ]
    child_records = [
        {"rate_npi": "1234567893", "rate_rank": 1, "service_code": "FIRST", "amount": Decimal("10.00")},
        {"rate_npi": "1234567893", "rate_rank": "1", "service_code": "SECOND", "amount": Decimal("20.00")},
    ]
    request = _request(definition, roots=roots, children=child_records)
    initial_admission = assemble_root_families(definition, roots, request.children_by_collection)
    assert len(initial_admission.families) == 2
    assert not initial_admission.rejections

    async def not_claimed(*_args):
        return None

    monkeypatch.setattr(runner, "_claim", not_claimed)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == "not_claimed"
    assert run_result.accepted_family_count == 0
    assert run_result.rejection_count == 2


@pytest.mark.asyncio
async def test_runner_rejects_accepted_root_matching_a_rejected_canonical_key(monkeypatch):
    definition = _decimal_root_key_definition()
    roots = [
        {"npi": "1234567893", "rank": 1, "display_name": "Synthetic Replacement"},
        {"npi": "1234567893", "rank": "1.0", "display_name": True},
    ]
    child_records = [{"rate_npi": "1234567893", "rate_rank": 1, "service_code": "REPLACEMENT", "amount": Decimal("10")}]
    request = _request(definition, roots=roots, children=child_records)
    initial_admission = assemble_root_families(definition, roots, request.children_by_collection)
    assert len(initial_admission.families) == 1
    assert {rejection.code for rejection in initial_admission.rejections} == {"field_type_invalid"}

    async def not_claimed(*_args):
        return None

    monkeypatch.setattr(runner, "_claim", not_claimed)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == "not_claimed"
    assert run_result.accepted_family_count == 0
    assert run_result.rejection_count == 2


def test_family_admission_rejects_canonical_child_key_duplicates():
    definition = _decimal_child_key_definition()
    admission_result = assemble_root_families(
        definition,
        [
            {"npi": "1234567893", "display_name": "Duplicate Child"},
            {"npi": "1003000126", "display_name": "Valid Child"},
        ],
        {
            "rates": [
                {"rate_npi": "1234567893", "service_code": "FIRST", "amount": 1},
                {"rate_npi": "1234567893", "service_code": "SECOND", "amount": "1.0"},
                {"rate_npi": "1003000126", "service_code": "VALID", "amount": Decimal("2")},
            ]
        },
    )

    assert [family.root["npi"] for family in admission_result.families] == ["1003000126"]
    assert {rejection.code for rejection in admission_result.rejections} == {"duplicate_child_key"}


def test_family_admission_rejects_same_instant_timestamp_child_keys():
    definition = _timestamp_child_key_definition()
    local = datetime(2026, 11, 1, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=0)
    admission_result = assemble_root_families(
        definition,
        [
            {"npi": "1234567893", "display_name": "Duplicate Timestamp"},
            {"npi": "1003000126", "display_name": "Valid Timestamp"},
        ],
        {
            "rates": [
                {
                    "rate_npi": "1234567893",
                    "service_code": "FIRST",
                    "amount": Decimal("12.50"),
                    "captured_at": local,
                },
                {
                    "rate_npi": "1234567893",
                    "service_code": "SECOND",
                    "amount": Decimal("12.50"),
                    "captured_at": local.astimezone(UTC),
                },
                {
                    "rate_npi": "1003000126",
                    "service_code": "VALID",
                    "amount": Decimal("12.50"),
                    "captured_at": datetime(2026, 11, 1, 7, 30, tzinfo=UTC),
                },
            ]
        },
    )

    assert [family.root["npi"] for family in admission_result.families] == ["1003000126"]
    assert {rejection.code for rejection in admission_result.rejections} == {"duplicate_child_key"}


def test_family_admission_distinguishes_timestamp_folds():
    definition = _timestamp_child_key_definition()
    new_york = ZoneInfo("America/New_York")
    result = assemble_root_families(
        definition,
        [{"npi": "1234567893", "display_name": "Distinct Timestamps"}],
        {
            "rates": [
                {
                    "rate_npi": "1234567893",
                    "service_code": "FIRST",
                    "amount": Decimal("12.50"),
                    "captured_at": datetime(2026, 11, 1, 1, 30, tzinfo=new_york, fold=0),
                },
                {
                    "rate_npi": "1234567893",
                    "service_code": "SECOND",
                    "amount": Decimal("12.50"),
                    "captured_at": datetime(2026, 11, 1, 1, 30, tzinfo=new_york, fold=1),
                },
            ]
        },
    )

    assert len(result.families) == 1
    assert len(result.families[0].children["rates"]) == 2
    assert result.rejections == ()


def test_timestamp_normalization_overflows_are_structured():
    definition = _timestamp_child_key_definition()
    timestamp_field = definition.child_fields[-1]
    boundary_timestamp = datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1)))

    with pytest.raises(CandidateRunnerError, match="accepted timestamp value is malformed"):
        value_document(timestamp_field, boundary_timestamp)
    with pytest.raises(CandidateRunnerError, match="retained timestamp value is malformed"):
        decode_payload_scalar(timestamp_field, boundary_timestamp.isoformat(), "retained")

    admission_result = assemble_root_families(
        definition,
        [{"npi": "1234567893", "display_name": "Boundary Timestamp"}],
        {
            "rates": [
                {
                    "rate_npi": "1234567893",
                    "service_code": "BOUNDARY",
                    "amount": Decimal("12.50"),
                    "captured_at": boundary_timestamp,
                }
            ]
        },
    )

    assert not admission_result.families
    assert {rejection.code for rejection in admission_result.rejections} == {"field_type_invalid"}


@pytest.mark.asyncio
async def test_runner_rejects_incomplete_snapshot_before_materialization(monkeypatch):
    request = _request(_snapshot_definition())

    async def claim(*_args):
        return _grant()

    async def finish_rejected(_factory, request_value, _grant_value, admitted):
        return CandidateRunResult(
            status="candidate_rejected",
            execution_id=request_value.execution_id,
            accepted_family_count=len(admitted.families),
            rejection_count=len(admitted.rejections),
        )

    async def unexpected_materialization(*_args):
        raise AssertionError("an incomplete snapshot must not materialize")

    monkeypatch.setattr(runner, "_claim", claim)
    monkeypatch.setattr(runner, "_finish_rejected_candidate", finish_rejected)
    monkeypatch.setattr(runner, "_materialize_candidate", unexpected_materialization)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == "candidate_rejected"


@pytest.mark.asyncio
async def test_runner_uses_atomic_no_change_before_a_separate_seal(monkeypatch):
    request = _request(_definition())
    no_change = _publication("no_change")

    async def claim(*_args):
        return _grant()

    async def materialize(*_args):
        return runner._MaterializedCandidate(
            generation_id=31,
            pointer=runner._Pointer(30, 12, 13, 7),
            accepted_family_count=1,
            rejection_count=0,
        )

    async def heartbeat(*_args):
        return _grant()

    async def record_no_change(*_args):
        return no_change

    async def unexpected_seal(*_args):
        raise AssertionError("no-change candidate must not take the ordinary seal path")

    monkeypatch.setattr(runner, "_claim", claim)
    monkeypatch.setattr(runner, "_materialize_candidate", materialize)
    monkeypatch.setattr(runner, "_heartbeat", heartbeat)
    monkeypatch.setattr(runner, "_record_no_change_or_none", record_no_change)
    monkeypatch.setattr(runner, "_seal", unexpected_seal)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == "no_change"
    assert run_result.publication is no_change


@pytest.mark.asyncio
async def test_runner_seals_after_a_no_change_pointer_race(monkeypatch):
    request = _request(_definition())

    async def claim(*_args):
        return _grant()

    async def materialize(*_args):
        return runner._MaterializedCandidate(
            generation_id=31,
            pointer=runner._Pointer(30, 12, 13, 7),
            accepted_family_count=1,
            rejection_count=0,
        )

    async def heartbeat(*_args):
        return _grant()

    async def pointer_race(*_args):
        raise PublicationConflict("current generation compare-and-swap failed")

    async def no_terminal_outcome(*_args):
        return None

    async def seal(*_args):
        return _seal()

    async def not_activated(*_args):
        return None

    monkeypatch.setattr(runner, "_claim", claim)
    monkeypatch.setattr(runner, "_materialize_candidate", materialize)
    monkeypatch.setattr(runner, "_heartbeat", heartbeat)
    monkeypatch.setattr(runner, "_record_no_change_or_none", pointer_race)
    monkeypatch.setattr(runner, "_finality_conflict_outcome", no_terminal_outcome)
    monkeypatch.setattr(runner, "_seal", seal)
    monkeypatch.setattr(runner, "_activate", not_activated)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == "sealed_unpublished"
    assert run_result.seal == _seal()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_status"),
    ((runner._CancellationRequested("synthetic"), "canceled"), (runner._LeaseLost("synthetic"), "lease_lost")),
)
async def test_runner_keeps_cancellation_and_lost_lease_outcomes_terminal(monkeypatch, failure, expected_status):
    request = _request(_definition())

    async def claim(*_args):
        return _grant()

    async def materialize(*_args):
        raise failure

    async def finish_canceled(_factory, request_value, _grant_value, admitted, materialized=None):
        return CandidateRunResult(
            status="canceled",
            execution_id=request_value.execution_id,
            generation_id=None if materialized is None else materialized.generation_id,
            accepted_family_count=len(admitted.families),
            rejection_count=len(admitted.rejections),
        )

    monkeypatch.setattr(runner, "_claim", claim)
    monkeypatch.setattr(runner, "_materialize_candidate", materialize)
    monkeypatch.setattr(runner, "_finish_canceled_candidate", finish_canceled)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == expected_status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("heartbeat_result", "expected_status"),
    ((None, "lease_lost"), (replace(_grant(), state="canceling"), "canceled")),
)
async def test_runner_stops_finality_when_authority_changes_after_graph(
    monkeypatch,
    heartbeat_result,
    expected_status,
):
    request = _request(_definition())

    async def claim(*_args):
        return _grant()

    async def materialize(*_args):
        return runner._MaterializedCandidate(31, None, 1, 0)

    async def heartbeat(*_args):
        return heartbeat_result

    async def finish_canceled(_factory, request_value, _grant_value, admitted, materialized=None):
        return CandidateRunResult(
            status="canceled",
            execution_id=request_value.execution_id,
            generation_id=None if materialized is None else materialized.generation_id,
            accepted_family_count=len(admitted.families),
            rejection_count=len(admitted.rejections),
        )

    async def unexpected_finality(*_args):
        raise AssertionError("finality must not run after the post-graph authority check")

    monkeypatch.setattr(runner, "_claim", claim)
    monkeypatch.setattr(runner, "_materialize_candidate", materialize)
    monkeypatch.setattr(runner, "_heartbeat", heartbeat)
    monkeypatch.setattr(runner, "_finish_canceled_candidate", finish_canceled)
    monkeypatch.setattr(runner, "_record_no_change_or_none", unexpected_finality)
    monkeypatch.setattr(runner, "_seal", unexpected_finality)

    run_result = await run_candidate(lambda: None, request)

    assert run_result.status == expected_status
