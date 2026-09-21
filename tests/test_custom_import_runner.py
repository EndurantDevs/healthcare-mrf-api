# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit coverage for the transaction-owning custom-import candidate runner."""

from __future__ import annotations

import json
import hashlib
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

import process.custom_import.family as family
import process.custom_import.runner as runner
import process.custom_import.runner_codec as runner_codec
import process.custom_import.runner_registry as runner_registry
from process.custom_import.definition import ChildCollection, CustomImportDefinition, Field, KeyPart
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


def _codec_field(field_id: str, value_type: str, *, nullable: bool = False) -> Field:
    return Field(
        field_id=field_id,
        field_slot=99,
        value_type=value_type,
        nullable=nullable,
        projection_slot=None,
        collection=None,
    )


def test_runner_codec_rejects_malformed_key_documents():
    definition = _definition()
    retained = SimpleNamespace(children=(SimpleNamespace(collection="rates", values_by_field={"amount": 1}),))
    assert runner_codec.family_children(retained) == (("rates", {"amount": 1}),)
    assert runner_codec.root_key_evidence_from_tuple(definition, ["1234567893"]) is None
    assert runner_codec.root_key_evidence_from_tuple(definition, ("1234567893",)) is not None

    with pytest.raises(CandidateRunnerError, match="not UTF-8"):
        runner_codec._digest_text_fragment(hashlib.sha256(), "\ud800")
    with pytest.raises(CandidateRunnerError, match="root key is incomplete"):
        runner_codec.root_key_document(definition, {})
    with pytest.raises(CandidateRunnerError, match="root key is malformed"):
        runner_codec.root_key_document_from_tuple(definition, ())
    with pytest.raises(CandidateRunnerError, match="family key is incomplete"):
        runner_codec.key_document(("missing",), {}, {})


def test_runner_codec_rejects_invalid_value_documents():
    for field, input_value, error_message in (
        (_codec_field("amount", "decimal"), "not-a-decimal", "decimal value is not canonical"),
        (_codec_field("effective_date", "date"), "2026-01-01", "date value is malformed"),
        (_codec_field("captured_at", "timestamp"), "2026-01-01", "timestamp value is malformed"),
        (_codec_field("rank", "integer"), True, "integer value is malformed"),
        (_codec_field("active", "boolean"), 1, "boolean value is malformed"),
        (_codec_field("display_name", "string"), 1, "string value is malformed"),
    ):
        with pytest.raises(CandidateRunnerError, match=error_message):
            runner_codec.value_document(field, input_value)

    with pytest.raises(CandidateRunnerError, match="candidate canonical value is malformed"):
        runner_codec.canonical({"value": object()})


def test_runner_codec_rejects_invalid_payload_documents():
    required_field = _codec_field("required", "string")
    nullable_field = _codec_field("optional", "string", nullable=True)

    with pytest.raises(CandidateRunnerError, match="fields do not match"):
        runner_codec.payload_values(
            (required_field,), '{"contract":"custom-import-record/v1","fields":[]}', label="payload"
        )
    with pytest.raises(CandidateRunnerError, match="not canonical text"):
        runner_codec.parse_canonical_payload(1, "payload")
    with pytest.raises(CandidateRunnerError, match="payload is malformed"):
        runner_codec.parse_canonical_payload("not-json", "payload")
    with pytest.raises(CandidateRunnerError, match="unknown contract"):
        runner_codec.parse_canonical_payload('{"contract":"other"}', "payload")
    with pytest.raises(CandidateRunnerError, match="field identity"):
        runner_codec.payload_field_value(required_field, {"field": "other", "value": {}}, "payload")
    with pytest.raises(CandidateRunnerError, match="field value does not match"):
        runner_codec.payload_field_value(required_field, {"field": "required", "value": 1}, "payload")
    with pytest.raises(CandidateRunnerError, match="omits a required"):
        runner_codec.payload_field_value(
            required_field, {"field": "required", "value": {"state": "missing"}}, "payload"
        )
    assert (
        runner_codec.payload_field_value(
            nullable_field, {"field": "optional", "value": {"state": "missing"}}, "payload"
        )
        is runner_codec._MISSING
    )
    with pytest.raises(CandidateRunnerError, match="nulls a required"):
        runner_codec.payload_field_value(
            required_field, {"field": "required", "value": {"state": "null", "type": "string"}}, "payload"
        )
    assert (
        runner_codec.payload_field_value(
            nullable_field, {"field": "optional", "value": {"state": "null", "type": "string"}}, "payload"
        )
        is None
    )
    with pytest.raises(CandidateRunnerError, match="field value is malformed"):
        runner_codec.payload_field_value(
            required_field, {"field": "required", "value": {"state": "value", "type": "string"}}, "payload"
        )


def test_runner_codec_rejects_invalid_decoded_scalars():
    for field, input_value, error_message in (
        (_codec_field("amount", "decimal"), "not-a-decimal", "decimal value is malformed"),
        (_codec_field("effective_date", "date"), 1, "date value is malformed"),
        (_codec_field("effective_date", "date"), "not-a-date", "date value is malformed"),
        (_codec_field("captured_at", "timestamp"), 1, "timestamp value is malformed"),
        (_codec_field("captured_at", "timestamp"), "not-a-timestamp", "timestamp value is malformed"),
        (_codec_field("captured_at", "timestamp"), "2026-01-01T00:00:00", "timestamp value is malformed"),
        (_codec_field("display_name", "string"), 1, "string value is malformed"),
        (_codec_field("rank", "integer"), True, "integer value is malformed"),
        (_codec_field("active", "boolean"), 1, "boolean value is malformed"),
    ):
        with pytest.raises(CandidateRunnerError, match=error_message):
            runner_codec.decode_payload_scalar(field, input_value, "payload")


def test_runner_rejects_invalid_host_boundaries_before_lifecycle_work():
    request = _request(_definition())

    with pytest.raises(CandidateRunnerError, match="requires a session factory"):
        runner.validate_candidate_request(None, request)
    with pytest.raises(CandidateRunnerError, match="request is malformed"):
        runner.validate_candidate_request(lambda: None, object())
    with pytest.raises(CandidateRunnerError, match="definition is malformed"):
        runner.validate_candidate_request(lambda: None, replace(request, definition=object()))
    with pytest.raises(CandidateRunnerError, match="complete_scope must be boolean"):
        runner.validate_candidate_request(lambda: None, replace(request, complete_scope=1))
    with pytest.raises(CandidateRunnerError, match="bounded root and child"):
        runner.validate_candidate_request(lambda: None, replace(request, roots={}))
    with pytest.raises(CandidateRunnerError, match="lease token is malformed"):
        runner.validate_candidate_request(lambda: None, replace(request, lease_token=object()))
    with pytest.raises(CandidateRunnerError, match="dataset_id must be a positive integer"):
        runner.validate_candidate_identifiers(replace(request, dataset_id=0))
    with pytest.raises(CandidateRunnerError, match="definition is not canonical"):
        runner.validate_definition_canonical(replace(request.definition, canonical="not-json"))
    with pytest.raises(CandidateRunnerError, match="does not match its canonical"):
        runner.validate_definition_canonical(replace(request.definition, refresh_mode="snapshot"))


class _LifecycleSession:
    def begin(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _ObservedSession(_LifecycleSession):
    def __init__(self, execution, lease, now):
        self.execution = execution
        self.lease = lease
        self.now = now
        self.no_autoflush = nullcontext()

    async def get(self, model, _execution_id):
        return self.execution if model is runner.CustomImportExecution else self.lease

    async def scalar(self, _statement):
        return self.now


@pytest.mark.asyncio
async def test_runner_observes_exact_execution_and_lease_state():
    request = _request(_definition())
    grant = _grant()
    now = datetime.now(UTC)

    def execution(state="running", **changes):
        execution_fields_by_name = {
            "dataset_id": request.dataset_id,
            "definition_revision_id": request.definition_revision_id,
            "schema_revision_id": request.schema_revision_id,
            "capture_bundle_id": 1,
            "state": state,
        }
        execution_fields_by_name.update(changes)
        return SimpleNamespace(**execution_fields_by_name)

    def session_factory(current_execution, lease):
        return lambda: _ObservedSession(current_execution, lease, now)

    valid_lease = SimpleNamespace(
        fence=grant.fence,
        token_sha256=runner.lease_token_sha256(request.lease_token),
        expires_at=now + timedelta(minutes=1),
    )
    assert (
        await runner.observed_finality_state(session_factory(execution(dataset_id=99), valid_lease), request, grant)
        is None
    )
    assert (
        await runner.observed_finality_state(session_factory(execution("canceling"), valid_lease), request, grant)
        == "canceling"
    )
    assert (
        await runner.observed_finality_state(session_factory(execution("canceled"), valid_lease), request, grant)
        == "canceled"
    )
    assert await runner.observed_finality_state(session_factory(execution(), None), request, grant) == "lease_lost"
    assert await runner.observed_finality_state(session_factory(execution(), valid_lease), request, grant) is None


@pytest.mark.asyncio
async def test_runner_classifies_terminal_state_edges(monkeypatch):
    request = _request(_definition())
    admitted = assemble_root_families(request.definition, request.roots, request.children_by_collection)
    materialized = runner._MaterializedCandidate(31, None, 1, 0)
    transition = SimpleNamespace(changed=False, state="canceled")
    original_finish_canceled = runner._finish_canceled_candidate

    async def finish(*_args, **_kwargs):
        return transition

    async def canceled(*_args, **_kwargs):
        return CandidateRunResult("canceled", request.execution_id)

    monkeypatch.setattr(runner, "finish_execution", finish)
    monkeypatch.setattr(runner, "_finish_canceled_candidate", canceled)
    assert (
        await runner._finish_rejected_candidate(_LifecycleSession, request, _grant(), admitted)
    ).status == "canceled"

    transition.state = "canceling"
    assert (
        await runner._finish_rejected_candidate(_LifecycleSession, request, _grant(), admitted)
    ).status == "canceled"

    transition.state = "unknown"
    assert (
        await runner._finish_rejected_candidate(_LifecycleSession, request, _grant(), admitted)
    ).status == "lease_lost"

    monkeypatch.setattr(runner, "_finish_canceled_candidate", original_finish_canceled)
    transition.changed = True
    transition.state = "running"
    assert (
        await runner._finish_canceled_candidate(_LifecycleSession, request, _grant(), admitted)
    ).status == "canceled"
    assert (
        await runner._finish_canceled_candidate(_LifecycleSession, request, _grant(), admitted, materialized)
    ).generation_id == 31
    transition.changed = False
    assert (
        await runner._finish_canceled_candidate(_LifecycleSession, request, _grant(), admitted, materialized)
    ).status == "lease_lost"

    transition.changed = True
    assert (
        await runner._finish_rejected_candidate(_LifecycleSession, request, _grant(), admitted)
    ).status == "candidate_rejected"


@pytest.mark.asyncio
async def test_runner_classifies_finality_conflicts_without_fallbacks(monkeypatch):
    request = _request(_definition())
    admitted = assemble_root_families(request.definition, request.roots, request.children_by_collection)
    materialized = runner._MaterializedCandidate(31, runner._Pointer(30, 12, 13, 7), 1, 0)
    observed_state_by_name = {"state": "canceling"}

    async def observed_state(*_args):
        return observed_state_by_name["state"]

    async def canceled(*_args, **_kwargs):
        return CandidateRunResult("canceled", request.execution_id, generation_id=31)

    monkeypatch.setattr(runner, "observed_finality_state", observed_state)
    monkeypatch.setattr(runner, "_finish_canceled_candidate", canceled)
    assert (
        await runner._finality_conflict_outcome(_LifecycleSession, request, _grant(), admitted, materialized)
    ).status == "canceled"
    observed_state_by_name["state"] = "canceled"
    assert (
        await runner._finality_conflict_outcome(_LifecycleSession, request, _grant(), admitted, materialized)
    ).status == "canceled"
    observed_state_by_name["state"] = "lease_lost"
    assert (
        await runner._finality_conflict_outcome(_LifecycleSession, request, _grant(), admitted, materialized)
    ).status == "lease_lost"
    observed_state_by_name["state"] = None
    assert await runner._finality_conflict_outcome(_LifecycleSession, request, _grant(), admitted, materialized) is None

    async def no_change_conflict(*_args):
        raise PublicationConflict("finality race")

    async def terminal_conflict(*_args):
        return CandidateRunResult("canceled", request.execution_id, generation_id=31)

    monkeypatch.setattr(runner, "_record_no_change_or_none", no_change_conflict)
    monkeypatch.setattr(runner, "_finality_conflict_outcome", terminal_conflict)
    assert (
        await runner.no_change_result_or_none(_LifecycleSession, request, _grant(), admitted, materialized)
    ).status == "canceled"

    async def seal_conflict(*_args):
        raise PublicationConflict("finality race")

    monkeypatch.setattr(runner, "_seal", seal_conflict)
    assert (
        await runner.seal_and_activate_candidate(_LifecycleSession, request, _grant(), admitted, materialized)
    ).status == "canceled"

    async def no_terminal_conflict(*_args):
        return None

    monkeypatch.setattr(runner, "_finality_conflict_outcome", no_terminal_conflict)
    with pytest.raises(PublicationConflict, match="finality race"):
        await runner.seal_and_activate_candidate(_LifecycleSession, request, _grant(), admitted, materialized)


@pytest.mark.asyncio
async def test_runner_keeps_only_the_declared_no_change_and_activation_conflicts(monkeypatch):
    request = _request(_definition())
    materialized = runner._MaterializedCandidate(31, runner._Pointer(30, 12, 13, 7), 1, 0)

    async def mismatch(*_args, **_kwargs):
        raise PublicationConflict(runner._NO_CHANGE_DIFFERENCE)

    monkeypatch.setattr(runner, "record_no_change", mismatch)
    assert await runner._record_no_change_or_none(_LifecycleSession, request, _grant(), materialized) is None

    async def unexpected_conflict(*_args, **_kwargs):
        raise PublicationConflict("unexpected")

    monkeypatch.setattr(runner, "record_no_change", unexpected_conflict)
    with pytest.raises(PublicationConflict, match="unexpected"):
        await runner._record_no_change_or_none(_LifecycleSession, request, _grant(), materialized)
    monkeypatch.setattr(runner, "activate_generation", unexpected_conflict)
    assert await runner._activate(_LifecycleSession, request, materialized) is None


def test_family_validation_rejects_noncanonical_child_and_scalar_values():
    decimal_field = _codec_field("value", "decimal")
    timestamp_field = _codec_field("value", "timestamp")
    collection = ChildCollection(name="rates", parent_key=(), child_key=("value",))

    assert family._canonical_child_key(collection, ("not-a-decimal",), {"value": decimal_field}) is None
    assert family._canonical_child_key(collection, (datetime(2026, 1, 1),), {"value": timestamp_field}) is None
    assert not family._is_value_type_valid("not-a-decimal", "decimal")
    projected_string = Field("value", 99, "string", False, 1, None)
    assert not family._is_scalar_storage_valid(projected_string, "\ud800")
    projected_decimal = Field("value", 99, "decimal", False, 1, None)
    assert not family._is_scalar_storage_valid(projected_decimal, object())
    assert not family.is_decimal_scalar_storage_valid("not-a-decimal")
    assert family._normalize_decimal_fraction(Decimal(0)) == Decimal(0)


def test_family_validation_rejects_missing_child_identity_after_field_validation(monkeypatch):
    collection = ChildCollection(
        name="rates",
        parent_key=(KeyPart(child_field="parent", root_field="id"),),
        child_key=("value",),
    )
    rejection_codes = defaultdict(set)
    family._admit_child_records(
        SimpleNamespace(child_collections=(collection,)),
        {"rates": ({"parent": "root", "value": None},)},
        {("root",): {}},
        {"rates": {"value": _codec_field("value", "string", nullable=True)}},
        rejection_codes,
    )
    assert rejection_codes[("root",)] == {"child_key_missing"}

    class BrokenDecimal:
        def __new__(cls, _value):
            raise family.InvalidOperation

    monkeypatch.setattr(family, "Decimal", BrokenDecimal)
    assert family.normalize_source_decimal("1") is None
    assert family._is_scalar_storage_valid(_codec_field("value", "date"), datetime(2026, 1, 1))


class _RegistryResult:
    def __init__(self, row=None):
        self._row = row

    def scalar_one_or_none(self):
        return self._row


class _RegistrySession:
    def __init__(self):
        self.row = None
        self.rows = ()
        self.clock = None
        self.info = {}
        self.no_autoflush = nullcontext()
        self.scalars = AsyncMock(side_effect=lambda _statement: SimpleNamespace(all=lambda: list(self.rows)))

    async def execute(self, _statement):
        return _RegistryResult(self.row)

    async def scalar(self, _statement):
        return self.clock


@pytest.mark.asyncio
async def test_runner_registry_rejects_missing_or_expired_identity_state():
    request = _request(_definition())
    session = _RegistrySession()

    with pytest.raises(CandidateRunnerError, match="dataset does not exist"):
        await runner_registry.lock_dataset(session, request.dataset_id)
    with pytest.raises(CandidateRunnerError, match="execution does not exist"):
        await runner_registry.lock_execution(session, request)
    session.row = SimpleNamespace(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        capture_bundle_id=None,
    )
    with pytest.raises(CandidateRunnerError, match="identity does not match"):
        await runner_registry.lock_execution(session, request)

    with pytest.raises(CandidateRunnerError, match="authority is not bound"):
        await runner_registry.prepare_materialization_statement(session)
    session.info[runner_registry._MATERIALIZATION_WINDOW_KEY] = runner_registry._MaterializationLeaseWindow(
        expires_at=datetime.now(UTC),
        monotonic_deadline=0,
    )
    with pytest.raises(runner_registry.LeaseAuthorityLost, match="window expired"):
        await runner_registry.prepare_materialization_statement(session)
    session.clock = datetime(2026, 1, 1)
    with pytest.raises(CandidateRunnerError, match="aware timestamp"):
        await runner_registry.database_now(session)


@pytest.mark.asyncio
async def test_runner_registry_rejects_collection_and_field_catalog_drift():
    request = _request(_definition())
    session = _RegistrySession()

    session.rows = (
        SimpleNamespace(
            collection_name="rates", collection_slot=1, canonical_key_shape="wrong", key_shape_sha256=b"wrong"
        ),
    )
    with pytest.raises(CandidateRunnerError, match="child collection keys"):
        await runner_registry.load_collection_slots(session, request)
    session.rows = ()
    with pytest.raises(CandidateRunnerError, match="field rows"):
        await runner_registry.validate_field_rows(session, request, {"rates": 1})

    field_rows = [
        SimpleNamespace(
            field_slot=field.field_slot,
            field_name=field.field_id,
            collection_slot=0 if field.collection is None else 1,
            field_type=field.value_type,
            is_nullable=field.nullable,
            projection_slot=field.projection_slot or 0,
        )
        for field in request.definition.fields
    ]
    field_rows[0].field_name = "wrong"
    session.rows = field_rows
    with pytest.raises(CandidateRunnerError, match="field rows"):
        await runner_registry.validate_field_rows(session, request, {"rates": 1})

    session.rows = [
        SimpleNamespace(field_slot=field.field_slot, field_id="wrong") for field in request.definition.fields
    ]
    with pytest.raises(CandidateRunnerError, match="field slots"):
        await runner_registry.validate_field_slot_ledger(session, request)


@pytest.mark.asyncio
async def test_runner_registry_rejects_stream_and_alias_drift():
    request = _request(_definition())
    session = _RegistrySession()
    streams = [
        SimpleNamespace(
            stream_id=stream.stream_id,
            stream_slot=index,
            record_kind=stream.record_kind,
            collection_slot=None if stream.child_collection is None else 1,
            decoder=stream.format,
            compression=stream.compression,
            snapshot_token_selector=stream.snapshot_token,
            record_path=stream.record_path,
        )
        for index, stream in enumerate(request.definition.source_streams, start=1)
    ]
    streams[0].record_kind = "wrong"
    session.rows = streams
    with pytest.raises(CandidateRunnerError, match="source streams"):
        await runner_registry.load_stream_slots(session, request, {"rates": 1})
    streams[0].record_kind = "root"
    streams[1].stream_slot = streams[0].stream_slot
    with pytest.raises(CandidateRunnerError, match="slots are not unique"):
        await runner_registry.load_stream_slots(session, request, {"rates": 1})

    session.rows = ()
    with pytest.raises(CandidateRunnerError, match="field aliases"):
        await runner_registry.validate_alias_rows(session, request, {"providers": 1, "rates": 2})


@pytest.mark.asyncio
async def test_runner_registry_rejects_lost_materialization_authority():
    request = _request(_definition())
    session = _RegistrySession()
    execution = SimpleNamespace(execution_id=request.execution_id)

    with pytest.raises(runner_registry.LeaseAuthorityLost, match="lease is no longer current"):
        await runner_registry.establish_materialization_authority(
            session,
            request,
            _grant(),
            execution,
            None,
            datetime.now(UTC),
        )

    session.info[runner_registry._MATERIALIZATION_WINDOW_KEY] = object()
    with pytest.raises(CandidateRunnerError, match="already bound"):
        await runner_registry.establish_materialization_authority(
            session,
            request,
            _grant(),
            execution,
            SimpleNamespace(),
            datetime.now(UTC),
        )

    session.info.clear()
    with pytest.raises(runner_registry.LeaseAuthorityLost, match="lease is no longer current"):
        await runner_registry.establish_materialization_authority(
            session,
            request,
            _grant(),
            execution,
            SimpleNamespace(),
            datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_runner_registry_rejects_multiple_root_streams():
    request = _request(_definition())
    session = _RegistrySession()
    root_stream = request.definition.source_streams[0]
    second_root_stream = replace(root_stream, stream_id="other_root")
    root_only_request = replace(request, definition=SimpleNamespace(source_streams=(root_stream, second_root_stream)))
    session.rows = (
        SimpleNamespace(
            stream_id=root_stream.stream_id,
            stream_slot=1,
            record_kind="root",
            collection_slot=None,
            decoder=root_stream.format,
            compression=root_stream.compression,
            snapshot_token_selector=root_stream.snapshot_token,
            record_path=root_stream.record_path,
        ),
        SimpleNamespace(
            stream_id=second_root_stream.stream_id,
            stream_slot=2,
            record_kind="root",
            collection_slot=None,
            decoder=second_root_stream.format,
            compression=second_root_stream.compression,
            snapshot_token_selector=second_root_stream.snapshot_token,
            record_path=second_root_stream.record_path,
        ),
    )
    with pytest.raises(CandidateRunnerError, match="one root stream"):
        await runner_registry.load_stream_slots(session, root_only_request, {})


@pytest.mark.asyncio
async def test_runner_registry_rejects_selection_profile_drift(monkeypatch):
    request = _request(_definition())
    session = _RegistrySession()

    async def no_statement_budget(*_args):
        return None

    monkeypatch.setattr(runner_registry, "prepare_materialization_statement", no_statement_budget)
    monkeypatch.setattr(runner_registry, "selection_profile_models", lambda *_args, **_kwargs: (object(),))
    monkeypatch.setattr(runner_registry, "has_profile_mismatch", lambda *_args: True)
    session.rows = (object(),)
    with pytest.raises(CandidateRunnerError, match="selection profiles"):
        await runner_registry.ensure_selection_profiles(
            session, request, SimpleNamespace(child_collection_slots={"rates": 1})
        )
