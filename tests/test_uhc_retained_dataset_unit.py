# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import zlib

import pytest

from process import uhc_retained_dataset as retained
from process import uhc_publication_sql
from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
    SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS,
    SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_KEY,
)
from process.provider_directory_resource_hash import (
    legacy_resource_payload_sha256,
    resource_payload_sha256,
)
from process.uhc_provider_file_identity import (
    PROVIDER_MEMBERSHIP,
    UHCSourceFileDescriptor,
    logical_scope_for_file,
)
from process.uhc_provider_quarantine_contract import (
    UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
    UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
    UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS,
)
from process.uhc_retained_dataset import (
    UHC_RETAINED_CANONICAL_CONTRACT_ID,
    UHC_RETAINED_SOURCE_ID,
    UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID,
    UhcAdmittedCatalogSet,
    UhcAdmittedFile,
    UhcRetainedDatasetError,
    _plan_payload,
    _plan_key,
    _plan_key_payload,
    _provider_resource_rows,
    _summary_input_hash,
    validate_uhc_summary_input,
)
from process.uhc_semantic_build_store import (
    UHC_SEMANTIC_CONTRACT_ID,
    UHC_SEMANTIC_CONTRACT_VERSION,
)


def _provider_record(provider_type: str) -> dict[str, object]:
    is_individual = provider_type == "INDIVIDUAL"
    return {
        "type": provider_type,
        "npi": "1003821380" if is_individual else "1000000491",
        "name": (
            {"first": "Ada", "middle": None, "last": "Lovelace"}
            if is_individual
            else None
        ),
        "facility_name": None if is_individual else "Example Clinic",
        "facility_type": None if is_individual else ["Clinic"],
        "gender": "F" if is_individual else None,
        "accepting": "accepting",
        "addresses": [
            {
                "address": "1 Main St",
                "city": "Chicago",
                "state": "IL",
                "zip": "60601",
                "phone": "3125551212",
            }
        ],
        "plans": [
            {
                "plan_id_type": "HIOS-PLAN-ID",
                "plan_id": "12345IL0010001",
                "years": [2026],
                "network_tier": "PREFERRED",
            }
        ],
        "specialty": ["Family Medicine"],
        "last_updated_on": "2026-07-01",
    }


@pytest.mark.parametrize(
    ("resource_type", "payload"),
    [
        ("InsurancePlan", {"name": "Example Plan"}),
        ("Organization", {"name": "Example Organization"}),
        ("OrganizationAffiliation", {"organization_ref": "Organization/1"}),
        (
            "Practitioner",
            {
                "names": [{"family": "Example", "given": ["Person"]}],
                "family_name": "Example",
                "given_names": ["Person"],
                "full_name": "Person Example",
            },
        ),
        ("PractitionerRole", {"practitioner_ref": "Practitioner/1"}),
        ("Location", {"name": "Example Location"}),
    ],
)
def test_official_file_rows_use_explicit_neutral_hash_contract(
    resource_type,
    payload,
):
    row = retained._canonical_row(
        resource_type,
        "resource-1",
        payload,
        "rank-1",
    )
    stored_payload = json.loads(row[3])

    assert row[2] == resource_payload_sha256(stored_payload)
    assert row[2] == legacy_resource_payload_sha256(stored_payload)


def _individual_exchange_scope():
    return logical_scope_for_file(
        UHCSourceFileDescriptor(
            "ifp",
            PROVIDER_MEMBERSHIP,
            "JSON_Providers_ILIEX.json",
        )
    )


def _assert_individual_plan_payload(logical_scope) -> None:
    """Assert canonical plan details for one individual exchange scope."""
    plan_key = _plan_key(
        logical_scope,
        _provider_record("INDIVIDUAL")["plans"][0],
        2026,
    )
    plan = _plan_payload(
        json.dumps(
            _plan_key_payload(plan_key),
            separators=(",", ":"),
            sort_keys=True,
        ),
        "uhcplan-example",
        [{"marketing_name": "Example Group Plan", "years": [2026]}],
    )
    assert plan["name"] == "Example Group Plan"
    assert plan["plan_identifier"].startswith(
        "HIOS-PLAN-ID:12345IL0010001:2026:"
    )
    assert plan["plan_json"]["plan_key"]["jurisdiction"] == "IL"
    assert plan["plan_json"]["detail_available"] is True


def _provider_lineage_by_field(
    logical_scope,
    source_file_id: str,
    artifact_sha256: str,
) -> dict[str, object]:
    return {
        "catalog_set_sha256": "c" * 64,
        "source_file_id": source_file_id,
        "file_name": "JSON_Providers_ILIEX.json",
        "artifact_sha256": artifact_sha256,
        "record_ordinal": 0,
        "logical_scope_id": logical_scope.logical_scope_id,
    }


def _assert_facility_evidence(
    payload_by_type: dict[str, object],
    facility_lineage_by_field: dict[str, object],
) -> None:
    organization = payload_by_type["Organization"]
    affiliation = payload_by_type["OrganizationAffiliation"]
    assert organization["name"] == "Example Clinic"
    assert organization["npi"] == 1000000491
    assert organization["type_codes"] == ["Clinic"]
    assert organization["address_json"] == [
        {
            "city": "Chicago",
            "country": "US",
            "line": ["1 Main St"],
            "postalCode": "60601",
            "state": "IL",
        }
    ]
    assert organization["tax_id"] is None
    assert organization["tin_status"] == "unavailable_from_uhc_source"
    assert organization["source_lineage"] == facility_lineage_by_field
    assert affiliation["organization_ref"] is None
    assert affiliation[
        "participating_organization_ref"
    ].startswith("Organization/")
    assert affiliation["insurance_plan_refs"]
    assert (
        affiliation["relationship_type"]
        == "payer_reported_provider_plan_membership"
    )
    assert affiliation["ownership_status"] == "not_asserted"
    assert affiliation["source_lineage"] == facility_lineage_by_field


def test_provider_semantics_emit_profile_ready_six_family_relationships():
    """Provider semantics emit all six profile-ready resource families."""
    logical_scope = _individual_exchange_scope()
    individual_lineage_by_field = _provider_lineage_by_field(
        logical_scope,
        "a" * 64,
        "d" * 64,
    )
    facility_lineage_by_field = _provider_lineage_by_field(
        logical_scope,
        "b" * 64,
        "e" * 64,
    )
    individual_rows, individual_keys = _provider_resource_rows(
        _provider_record("INDIVIDUAL"),
        source_file_id="a" * 64,
        ordinal=0,
        logical_scope=logical_scope,
        source_lineage=individual_lineage_by_field,
    )
    facility_rows, facility_keys = _provider_resource_rows(
        _provider_record("FACILITY"),
        source_file_id="b" * 64,
        ordinal=0,
        logical_scope=logical_scope,
        source_lineage=facility_lineage_by_field,
    )
    payload_by_type = {
        resource_type: json.loads(payload_json)
        for resource_type, _resource_id, _hash, payload_json, _rank in (
            individual_rows + facility_rows
        )
    }

    assert set(payload_by_type) == {
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    }
    assert payload_by_type["Practitioner"]["full_name"] == "Ada Lovelace"
    assert payload_by_type["PractitionerRole"]["insurance_plan_refs"]
    _assert_facility_evidence(
        payload_by_type,
        facility_lineage_by_field,
    )
    assert payload_by_type["Location"]["name"] == "Example Clinic"
    assert individual_keys == facility_keys
    _assert_individual_plan_payload(logical_scope)


def test_provider_address_projection_preserves_second_line() -> None:
    payload = retained._address_payload(
        {
            "address": "1 Main St",
            "address_2": "Suite 200",
            "city": "Chicago",
            "state": "IL",
            "zip": "60601",
        }
    )

    assert payload["line"] == ["1 Main St", "Suite 200"]


def _valid_summary_input() -> dict[str, object]:
    summary_input_by_field: dict[str, object] = {
        "contract_id": UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID,
        "complete": True,
        "source_id": UHC_RETAINED_SOURCE_ID,
        "catalog_set_sha256": "a" * 64,
        "semantic_contract_id": UHC_SEMANTIC_CONTRACT_ID,
        "semantic_contract_version": UHC_SEMANTIC_CONTRACT_VERSION,
        "canonical_contract_id": UHC_RETAINED_CANONICAL_CONTRACT_ID,
        "semantic_build_ids": ["b" * 64, "c" * 64],
        "semantic_set_sha256": "d" * 64,
        "input_set_sha256": "e" * 64,
        "layout_set_sha256": "f" * 64,
        "encoder_digest": "1" * 64,
        "quarantine_proof_sha256": "2" * 64,
        "count_by_field": {
            field_name: 0
            for field_name in SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS
        },
        "count_by_category": {
            "conflict_counts": {},
            "rejected_counts": {},
            "intentional_drop_counts": {
                drop_key: (
                    1
                    if drop_key == SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_KEY
                    else 0
                )
                for drop_key in SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS
            },
            "unknown_field_counts": {},
        },
    }
    summary_input_by_field["input_sha256"] = _summary_input_hash(
        summary_input_by_field
    )
    return summary_input_by_field


def test_summary_input_requires_exact_semantic_v3_identity():
    value = _valid_summary_input()
    assert validate_uhc_summary_input(value) == value

    value["semantic_contract_id"] = "healthporta.uhc.semantic-facts.v1"
    value["input_sha256"] = _summary_input_hash(value)
    with pytest.raises(UhcRetainedDatasetError, match="contract is invalid"):
        validate_uhc_summary_input(value)


def _admitted_file(tmp_path: Path, *, source_file_id: str = "a" * 64):
    raw_path = tmp_path / f"{source_file_id[:8]}-raw.json"
    manifest_path = tmp_path / f"{source_file_id[:8]}-manifest.json"
    raw_path.write_bytes(b"[]")
    manifest_path.write_bytes(b"{}")
    raw_path.chmod(0o600)
    manifest_path.chmod(0o600)
    return UhcAdmittedFile(
        catalog_set_sha256="b" * 64,
        source_file_id=source_file_id,
        family="ifp",
        collection_kind="provider_membership",
        file_name="JSON_Providers_ILIEX.json",
        artifact_sha256=hashlib.sha256(b"[]").hexdigest(),
        artifact_byte_count=2,
        raw_contract_version=2,
        raw_range_count=4,
        record_count=1,
        range_set_sha256="c" * 64,
        manifest_sha256=hashlib.sha256(b"{}").hexdigest(),
        raw_producer_build_id="unit-fixture-producer-v1",
        raw_path=raw_path,
        manifest_path=manifest_path,
    )


def _claim(admitted, encoder_sha256, *, sealed_reuse=False):
    identity = admitted.semantic_identity(encoder_sha256)
    return retained.UhcSemanticBuildClaim(
        semantic_build_id=identity.semantic_build_id,
        lease_token=None if sealed_reuse else "lease",
        attempt_count=1,
        stage_schema="mrf_test",
        stage_relation=identity.stage_relation,
        sealed_reuse=sealed_reuse,
    )


def test_schema_identifier_hash_count_and_mapping_guards(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-schema")
    with pytest.raises(UhcRetainedDatasetError, match="schema"):
        retained._schema_name()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf_test")
    assert retained._qualified("mrf_test", "stage") == '"mrf_test"."stage"'
    with pytest.raises(UhcRetainedDatasetError, match="identifier"):
        retained._quoted("bad-name")
    with pytest.raises(UhcRetainedDatasetError, match="SHA-256"):
        retained._require_sha256("bad", "hash")
    for value in (True, "1", 0):
        with pytest.raises(UhcRetainedDatasetError, match="count is invalid"):
            retained._positive_int(value, "count")
    assert retained._positive_int(0, "count", allow_zero=True) == 0
    assert retained._mapping('{"a":1}', "mapping") == {"a": 1}
    for value in ("{", []):
        with pytest.raises(UhcRetainedDatasetError, match="mapping is invalid"):
            retained._mapping(value, "mapping")


@pytest.mark.parametrize(
    "uri",
    [
        None,
        "https://example.invalid/file",
        "file://remote/tmp/file",
        "file:///tmp/file?query=1",
        "file:///tmp/file#fragment",
        "file:///path/that/does/not/exist",
    ],
)
def test_retained_file_path_rejects_invalid_or_unavailable_uri(uri):
    with pytest.raises(UhcRetainedDatasetError):
        retained._retained_file_path(uri, "raw")


def test_retained_file_path_accepts_private_file_and_rejects_unsafe(
    tmp_path,
):
    path = tmp_path / "raw"
    path.write_bytes(b"[]")
    path.chmod(0o600)
    assert retained._retained_file_path(path.as_uri(), "raw") == path
    path.chmod(0o622)
    with pytest.raises(UhcRetainedDatasetError, match="unsafe"):
        retained._retained_file_path(path.as_uri(), "raw")


def test_semantic_binary_resolves_configured_and_sibling_binary(
    monkeypatch,
    tmp_path,
):
    binary = tmp_path / "uhc_semantic_facts"
    binary.write_text("#!/bin/sh\nexit 0\n")
    binary.chmod(0o700)
    monkeypatch.setenv("HLTHPRT_UHC_SEMANTIC_BIN", str(binary))
    assert retained.uhc_semantic_binary() == binary.resolve()
    assert retained._sha256_file(binary) == hashlib.sha256(
        binary.read_bytes()
    ).hexdigest()

    monkeypatch.delenv("HLTHPRT_UHC_SEMANTIC_BIN")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_RUST_SCANNER_BIN",
        str(tmp_path / "ptg2_scanner"),
    )
    assert retained.uhc_semantic_binary() == binary.resolve()


def test_semantic_binary_rejects_missing_or_non_executable(
    monkeypatch,
    tmp_path,
):
    monkeypatch.delenv("HLTHPRT_UHC_SEMANTIC_BIN", raising=False)
    monkeypatch.delenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", raising=False)
    with pytest.raises(UhcRetainedDatasetError, match="requires"):
        retained.uhc_semantic_binary()
    monkeypatch.setenv(
        "HLTHPRT_UHC_SEMANTIC_BIN",
        str(tmp_path / "missing"),
    )
    with pytest.raises(UhcRetainedDatasetError, match="unavailable"):
        retained.uhc_semantic_binary()
    binary = tmp_path / "binary"
    binary.write_bytes(b"not executable")
    binary.chmod(0o600)
    monkeypatch.setenv("HLTHPRT_UHC_SEMANTIC_BIN", str(binary))
    with pytest.raises(UhcRetainedDatasetError, match="not executable"):
        retained.uhc_semantic_binary()


class _Reader:
    def __init__(self, chunks):
        self.chunks = list(chunks)

    async def read(self, _size):
        return self.chunks.pop(0) if self.chunks else b""


@pytest.mark.asyncio
async def test_bounded_stderr_and_stdout_observation(monkeypatch):
    assert await retained._read_bounded_stderr(_Reader([b"a", b"b"])) == b"ab"
    monkeypatch.setattr(retained, "_MAX_STDERR_BYTES", 1)
    with pytest.raises(UhcRetainedDatasetError, match="exceeded"):
        await retained._read_bounded_stderr(_Reader([b"ab"]))

    observation = retained._CopyStreamObservation()
    assert [
        chunk
        async for chunk in retained._stdout_chunks(
            _Reader([b"a", b"b"]),
            observation,
        )
    ] == [b"a", b"b"]
    assert observation.byte_count == 2
    assert observation.sha256 == hashlib.sha256(b"ab").hexdigest()


@pytest.mark.asyncio
async def test_process_termination_covers_completed_term_and_kill(
    monkeypatch,
):
    completed = SimpleNamespace(returncode=0, wait=AsyncMock(return_value=0))
    await retained._terminate_process(completed)
    completed.wait.assert_awaited_once()

    kill_signals = []
    monkeypatch.setattr(
        retained.os,
        "killpg",
        lambda pid, signal_number: kill_signals.append(
            (pid, signal_number)
        ),
    )
    running = SimpleNamespace(
        returncode=None,
        pid=123,
        wait=AsyncMock(return_value=0),
    )
    await retained._terminate_process(running)
    assert kill_signals[-1] == (123, retained.signal.SIGTERM)

    async def timeout_once(_awaitable, *, timeout):
        del timeout
        if hasattr(_awaitable, "close"):
            _awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(retained.asyncio, "wait_for", timeout_once)
    stubborn = SimpleNamespace(
        returncode=None,
        pid=456,
        wait=AsyncMock(return_value=0),
    )
    await retained._terminate_process(stubborn)
    assert kill_signals[-1] == (456, retained.signal.SIGKILL)


def test_semantic_arguments_and_native_report_contract(tmp_path):
    admitted = _admitted_file(tmp_path)
    arguments = retained._semantic_arguments(
        Path("/opt/uhc_semantic_facts"),
        admitted,
    )
    assert arguments[0] == "/opt/uhc_semantic_facts"
    assert "--source-binding-id" in arguments
    assert retained._native_report(b'{"fact_count":1}') == {"fact_count": 1}
    for stderr in (b"\xff", b"not-json", b"[]"):
        with pytest.raises(UhcRetainedDatasetError, match="report is invalid"):
            retained._native_report(stderr)


@pytest.mark.asyncio
async def test_reused_semantic_file_covers_build_missing_and_success(
    monkeypatch,
    tmp_path,
):
    admitted = _admitted_file(tmp_path)
    encoder = "d" * 64
    identity = admitted.semantic_identity(encoder)
    assert await retained._reused_semantic_file(
        object(),
        admitted,
        identity,
        _claim(admitted, encoder),
    ) is None

    monkeypatch.setattr(
        retained,
        "load_sealed_uhc_semantic_build",
        AsyncMock(return_value=None),
    )
    with pytest.raises(UhcRetainedDatasetError, match="disappeared"):
        await retained._reused_semantic_file(
            object(),
            admitted,
            identity,
            _claim(admitted, encoder, sealed_reuse=True),
        )
    build_row_by_field = {
        "stage_schema": "mrf",
        "stage_relation": "stage",
    }
    retained.load_sealed_uhc_semantic_build.return_value = build_row_by_field
    verifier = AsyncMock()
    monkeypatch.setattr(
        retained,
        "verify_sealed_uhc_semantic_build",
        verifier,
    )
    sealed = await retained._reused_semantic_file(
        object(),
        admitted,
        identity,
        _claim(admitted, encoder, sealed_reuse=True),
    )
    assert sealed.build_row == build_row_by_field
    verifier.assert_awaited_once()


@pytest.mark.asyncio
async def test_native_semantic_seal_requires_persisted_build(
    monkeypatch,
    tmp_path,
):
    admitted = _admitted_file(tmp_path)
    encoder = "d" * 64
    identity = admitted.semantic_identity(encoder)
    claim = _claim(admitted, encoder)
    for function_name in (
        "prepare_uhc_semantic_stage_indexes",
        "verify_uhc_semantic_stage",
        "seal_uhc_semantic_build",
    ):
        monkeypatch.setattr(retained, function_name, AsyncMock(return_value={}))
    monkeypatch.setattr(
        retained,
        "load_sealed_uhc_semantic_build",
        AsyncMock(return_value=None),
    )
    with pytest.raises(UhcRetainedDatasetError, match="was not persisted"):
        await retained._seal_native_semantic_report(
            object(),
            admitted,
            identity,
            claim,
            {},
            {},
        )
    retained.load_sealed_uhc_semantic_build.return_value = {
        "stage_schema": "mrf",
        "stage_relation": "stage",
    }
    assert (
        await retained._seal_native_semantic_report(
            object(),
            admitted,
            identity,
            claim,
            {},
            {},
        )
    ).identity == identity


@pytest.mark.asyncio
async def test_failed_semantic_build_terminates_cancels_and_quarantines(
    monkeypatch,
    tmp_path,
):
    admitted = _admitted_file(tmp_path)
    claim = _claim(admitted, "d" * 64)
    terminate = AsyncMock()
    quarantine = AsyncMock()
    monkeypatch.setattr(retained, "_terminate_process", terminate)
    monkeypatch.setattr(
        retained,
        "quarantine_uhc_semantic_build",
        quarantine,
    )
    pending = asyncio.create_task(asyncio.sleep(60))
    await retained._quarantine_failed_semantic_build(
        object(),
        claim,
        SimpleNamespace(),
        pending,
    )
    assert pending.cancelled()
    terminate.assert_awaited_once()
    quarantine.assert_awaited_once()
    await retained._quarantine_failed_semantic_build(
        object(),
        claim,
        None,
        None,
    )


@pytest.mark.asyncio
async def test_start_semantic_encoder_requires_both_pipes(
    monkeypatch,
    tmp_path,
):
    admitted = _admitted_file(tmp_path)
    missing = SimpleNamespace(stdout=None, stderr=None)
    monkeypatch.setattr(
        retained.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=missing),
    )
    with pytest.raises(UhcRetainedDatasetError, match="pipes"):
        await retained._start_semantic_encoder(Path("/bin/false"), admitted)

    process = SimpleNamespace(stdout=_Reader([]), stderr=_Reader([b"{}"]))
    retained.asyncio.create_subprocess_exec.return_value = process
    observed_process, stderr_task = await retained._start_semantic_encoder(
        Path("/bin/true"),
        admitted,
    )
    assert observed_process is process
    assert await stderr_task == b"{}"


@pytest.mark.asyncio
async def test_consume_semantic_encoder_covers_failure_and_success(
    monkeypatch,
    tmp_path,
):
    admitted = _admitted_file(tmp_path)
    encoder = "d" * 64
    identity = admitted.semantic_identity(encoder)
    claim = _claim(admitted, encoder)

    async def consume_copy(_connection, _claim_value, copy_stream):
        async for _chunk in copy_stream:
            continue
        return 3

    monkeypatch.setattr(
        retained,
        "copy_uhc_semantic_stage",
        AsyncMock(side_effect=consume_copy),
    )
    failure = SimpleNamespace(
        stdout=_Reader([b"copy"]),
        wait=AsyncMock(return_value=1),
    )
    with pytest.raises(UhcRetainedDatasetError, match="encoder failed"):
        await retained._consume_semantic_encoder(
            object(),
            admitted,
            identity,
            claim,
            failure,
            asyncio.create_task(
                asyncio.sleep(0, result=b"native failure")
            ),
        )

    seal = AsyncMock(
        return_value=retained.UhcSealedSemanticFile(
            admitted,
            identity,
            {"stage_schema": "mrf", "stage_relation": "stage"},
        )
    )
    monkeypatch.setattr(retained, "_seal_native_semantic_report", seal)
    success = SimpleNamespace(
        stdout=_Reader([b"copy"]),
        wait=AsyncMock(return_value=0),
    )
    sealed_result = await retained._consume_semantic_encoder(
        object(),
        admitted,
        identity,
        claim,
        success,
        asyncio.create_task(asyncio.sleep(0, result=b"{}")),
    )
    assert sealed_result.identity == identity
    assert seal.await_args.args[-1]["copy_row_count"] == 3
    assert seal.await_args.args[-1]["output_bytes"] == 4


@pytest.mark.asyncio
async def test_build_semantic_file_quarantines_failure_and_returns_success(
    monkeypatch,
    tmp_path,
):
    admitted = _admitted_file(tmp_path)
    encoder = "d" * 64
    identity = admitted.semantic_identity(encoder)
    claim = _claim(admitted, encoder)
    monkeypatch.setattr(
        retained,
        "_start_semantic_encoder",
        AsyncMock(side_effect=RuntimeError("start failed")),
    )
    quarantine = AsyncMock()
    monkeypatch.setattr(
        retained,
        "_quarantine_failed_semantic_build",
        quarantine,
    )
    with pytest.raises(RuntimeError, match="start failed"):
        await retained._build_semantic_file(
            object(),
            admitted,
            Path("/bin/false"),
            identity,
            claim,
        )
    quarantine.assert_awaited_once()

    process = SimpleNamespace()
    stderr_task = SimpleNamespace()
    retained._start_semantic_encoder.side_effect = None
    retained._start_semantic_encoder.return_value = (process, stderr_task)
    expected = retained.UhcSealedSemanticFile(
        admitted,
        identity,
        {"stage_schema": "mrf", "stage_relation": "stage"},
    )
    monkeypatch.setattr(
        retained,
        "_consume_semantic_encoder",
        AsyncMock(return_value=expected),
    )
    assert await retained._build_semantic_file(
        object(),
        admitted,
        Path("/bin/true"),
        identity,
        claim,
    ) == expected


@pytest.mark.asyncio
async def test_run_one_semantic_build_selects_reuse_or_new_build(
    monkeypatch,
    tmp_path,
):
    admitted = _admitted_file(tmp_path)
    encoder = "d" * 64
    identity = admitted.semantic_identity(encoder)
    claim = _claim(admitted, encoder)
    expected = retained.UhcSealedSemanticFile(
        admitted,
        identity,
        {"stage_schema": "mrf", "stage_relation": "stage"},
    )
    monkeypatch.setattr(
        retained,
        "claim_uhc_semantic_build",
        AsyncMock(return_value=claim),
    )
    monkeypatch.setattr(
        retained,
        "_reused_semantic_file",
        AsyncMock(return_value=expected),
    )
    build = AsyncMock(return_value=expected)
    monkeypatch.setattr(retained, "_build_semantic_file", build)
    assert await retained._run_one_semantic_build(
        object(),
        admitted,
        Path("/bin/true"),
        encoder,
    ) == expected
    build.assert_not_awaited()
    retained._reused_semantic_file.return_value = None
    assert await retained._run_one_semantic_build(
        object(),
        admitted,
        Path("/bin/true"),
        encoder,
    ) == expected
    build.assert_awaited_once()


@pytest.mark.asyncio
async def test_semantic_set_rejects_mixed_encoder_and_accepts_exact_set(
    monkeypatch,
    tmp_path,
):
    binary = tmp_path / "binary"
    binary.write_bytes(b"binary")
    binary.chmod(0o700)
    admitted = _admitted_file(tmp_path)
    admitted_set = UhcAdmittedCatalogSet(
        catalog_set_sha256=admitted.catalog_set_sha256,
        files=(admitted,),
        provider_file_count=1,
        plan_file_count=0,
    )
    with pytest.raises(
        UhcRetainedDatasetError,
        match="connection is unavailable",
    ):
        await retained.ensure_sealed_uhc_semantic_set(
            None,
            admitted_set,
            binary=binary,
        )
    expected_encoder = hashlib.sha256(b"binary").hexdigest()
    wrong = retained.UhcSealedSemanticFile(
        admitted,
        admitted.semantic_identity("e" * 64),
        {"stage_schema": "mrf", "stage_relation": "stage"},
    )
    monkeypatch.setattr(
        retained,
        "_run_one_semantic_build",
        AsyncMock(return_value=wrong),
    )
    with pytest.raises(UhcRetainedDatasetError, match="incomplete or mixed"):
        await retained.ensure_sealed_uhc_semantic_set(
            object(),
            admitted_set,
            binary=binary,
        )
    exact = retained.UhcSealedSemanticFile(
        admitted,
        admitted.semantic_identity(expected_encoder),
        {"stage_schema": "mrf", "stage_relation": "stage"},
    )
    retained._run_one_semantic_build.return_value = exact
    assert await retained.ensure_sealed_uhc_semantic_set(
        object(),
        admitted_set,
        binary=binary,
    ) == (exact,)


def _semantic_parallelism_stubs(counts_by_state, encoder_sha256):
    @contextlib.asynccontextmanager
    async def connection_factory():
        counts_by_state["connections"] += 1
        yield object()

    async def build_one(
        _connection,
        admitted_file,
        _binary,
        actual_encoder_sha256,
    ):
        assert actual_encoder_sha256 == encoder_sha256
        counts_by_state["active"] += 1
        counts_by_state["maximum_active"] = max(
            counts_by_state["maximum_active"],
            counts_by_state["active"],
        )
        await asyncio.sleep(0)
        counts_by_state["active"] -= 1
        return retained.UhcSealedSemanticFile(
            admitted_file,
            admitted_file.semantic_identity(encoder_sha256),
            {"stage_schema": "mrf", "stage_relation": "stage"},
        )

    return connection_factory, build_one


@pytest.mark.asyncio
async def test_semantic_file_builds_use_bounded_parallel_connections(
    monkeypatch,
    tmp_path,
):
    """Semantic builds overlap on independent bounded connections."""
    binary = tmp_path / "binary"
    binary.write_bytes(b"binary")
    binary.chmod(0o700)
    admitted_files = tuple(
        _admitted_file(tmp_path, source_file_id=character * 64)
        for character in ("a", "b", "c", "d")
    )
    admitted_set = UhcAdmittedCatalogSet(
        catalog_set_sha256="b" * 64,
        files=admitted_files,
        provider_file_count=4,
        plan_file_count=0,
    )
    encoder_sha256 = hashlib.sha256(b"binary").hexdigest()
    counts_by_state = {
        "active": 0,
        "maximum_active": 0,
        "connections": 0,
    }

    connection_factory, build_one = _semantic_parallelism_stubs(
        counts_by_state,
        encoder_sha256,
    )
    monkeypatch.setattr(retained, "_run_one_semantic_build", build_one)
    sealed_files = await retained.ensure_sealed_uhc_semantic_set(
        None,
        admitted_set,
        binary=binary,
        connection_factory=connection_factory,
        file_concurrency=2,
    )

    assert [
        sealed_file.admitted.source_file_id
        for sealed_file in sealed_files
    ] == [
        admitted_file.source_file_id for admitted_file in admitted_files
    ]
    assert counts_by_state["connections"] == len(admitted_files)
    assert counts_by_state["maximum_active"] == 2


def test_semantic_file_concurrency_rejects_invalid_or_unbounded_values(
    monkeypatch,
):
    monkeypatch.setenv(
        "HLTHPRT_UHC_SEMANTIC_FILE_CONCURRENCY",
        "not-an-integer",
    )
    with pytest.raises(UhcRetainedDatasetError, match="is invalid"):
        retained.uhc_semantic_file_concurrency()

    monkeypatch.setenv(
        "HLTHPRT_UHC_SEMANTIC_FILE_CONCURRENCY",
        str(retained.MAX_SEMANTIC_FILE_CONCURRENCY + 1),
    )
    with pytest.raises(UhcRetainedDatasetError, match="must be in"):
        retained.uhc_semantic_file_concurrency()

    for requested_concurrency in (
        0,
        retained.MAX_SEMANTIC_FILE_CONCURRENCY + 1,
    ):
        with pytest.raises(UhcRetainedDatasetError, match="must be in"):
            retained._validated_semantic_file_concurrency(
                requested_concurrency
            )


@pytest.mark.asyncio
async def test_parallel_semantic_build_cancels_unfinished_siblings(
    monkeypatch,
    tmp_path,
):
    admitted_files = tuple(
        _admitted_file(tmp_path, source_file_id=character * 64)
        for character in ("a", "b")
    )
    sibling_cancelled = asyncio.Event()

    @contextlib.asynccontextmanager
    async def connection_factory():
        yield object()

    async def build_one(
        _connection,
        admitted_file,
        _binary,
        _encoder_sha256,
    ):
        if admitted_file.source_file_id == "a" * 64:
            await asyncio.sleep(0)
            raise RuntimeError("semantic build failed")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    monkeypatch.setattr(retained, "_run_one_semantic_build", build_one)
    with pytest.raises(RuntimeError, match="semantic build failed"):
        await retained._run_parallel_semantic_builds(
            admitted_files,
            Path("/bin/true"),
            "d" * 64,
            connection_factory,
            2,
        )
    assert sibling_cancelled.is_set()


def test_publication_sql_contracts_are_loaded():
    assert "__ENDPOINT_DATASET_REF__" in (
        uhc_publication_sql.PUBLISH_VALIDATED_UHC_DATASET_SQL
    )
    assert "selected_resources" in (
        uhc_publication_sql.PUBLISH_VALIDATED_UHC_DATASET_SQL
    )


def test_retained_catalog_and_resource_shape_guards(monkeypatch):
    """Catalog identity and retained bindings fail closed."""
    with pytest.raises(UhcRetainedDatasetError, match="file counts"):
        retained._catalog_file_counts(
            [
                {
                    "catalog_file_count": 2,
                    "provider_file_count": 1,
                    "plan_reference_file_count": 1,
                }
            ]
        )

    binding_by_field = {
        "source_file_id": "a" * 64,
        "artifact_sha256": "b" * 64,
        "collection_kind": "provider_membership",
        "family": "ifp",
        "availability": "published",
        "catalog_support": "cataloged",
        "binding_released_at": None,
        "artifact_status": "verified",
        "layout_status": "verified",
        "raw_reference_released_at": None,
        "manifest_reference_released_at": None,
        "raw_contract_version": 2,
        "raw_range_count": 3,
        "verified_range_count": 3,
    }
    with pytest.raises(UhcRetainedDatasetError, match="range proof"):
        retained._validated_binding_identity(binding_by_field, set())

    monkeypatch.setattr(
        retained,
        "_retained_file_path",
        Mock(side_effect=[Path("/a"), Path("/b"), Path("/c"), Path("/c")]),
    )
    with pytest.raises(UhcRetainedDatasetError, match="references disagree"):
        retained._validated_binding_paths(
            {
                "raw_storage_uri": "a",
                "raw_reference_uri": "b",
                "manifest_storage_uri": "c",
                "manifest_reference_uri": "c",
            }
        )


def test_retained_resource_shape_guards():
    """Canonical resource fields reject invalid shapes."""
    assert retained._clean_text(" \x00 ") is None
    for years in (None, [], [True], [1999]):
        with pytest.raises(UhcRetainedDatasetError, match="years"):
            retained._plan_years({"years": years})
    with pytest.raises(UhcRetainedDatasetError, match="not unique"):
        retained._plan_years({"years": [2026, 2026]})
    with pytest.raises(UhcRetainedDatasetError, match="plan key"):
        retained._plan_key(_individual_exchange_scope(), {}, 2026)
    assert retained._phone_digits(None) is None
    assert retained._phone_digits("+1 (312) 555-1212") == "3125551212"
    assert retained._telecom(
        [{"phone": None}, {"phone": "3125551212"}, {"phone": "3125551212"}]
    ) == [{"system": "phone", "value": "3125551212"}]
    assert retained._provider_name({"name": []}) == ([], None, [], None)
    with pytest.raises(UhcRetainedDatasetError, match="invalid shape"):
        retained._provider_resource_context({}, "a" * 64, 0, {})


@pytest.mark.asyncio
async def test_admitted_catalog_loader_rejects_missing_and_mixed_counts(
    monkeypatch,
):
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[]))
    with pytest.raises(UhcRetainedDatasetError, match="was not found"):
        await retained.load_complete_admitted_uhc_catalog_set(
            connection,
            "a" * 64,
        )

    connection.fetch.return_value = [object()]
    monkeypatch.setattr(
        retained,
        "_catalog_file_counts",
        Mock(return_value=(1, 1, 0)),
    )
    monkeypatch.setattr(
        retained,
        "_admitted_uhc_file",
        Mock(
            return_value=SimpleNamespace(
                source_file_id="b" * 64,
                collection_kind="plan_reference",
            )
        ),
    )
    with pytest.raises(UhcRetainedDatasetError, match="collection counts"):
        await retained.load_complete_admitted_uhc_catalog_set(
            connection,
            "a" * 64,
        )


def test_fact_framing_and_decoding_rejects_invalid_inputs(monkeypatch):
    with pytest.raises(UhcRetainedDatasetError, match="framing"):
        list(retained._framed_fact_lines(bytearray(b"\n")))
    with pytest.raises(UhcRetainedDatasetError, match="compression"):
        list(retained._decoded_fact_lines(zlib.compress(b'{"fact":1}')))
    monkeypatch.setattr(retained, "_MAX_FACT_RECORD_BYTES", 1)
    with pytest.raises(UhcRetainedDatasetError, match="memory bound"):
        list(retained._decoded_fact_lines(zlib.compress(b"{}")))
    with pytest.raises(UhcRetainedDatasetError, match="JSON is invalid"):
        retained._decoded_semantic_fact(b"{")
    with pytest.raises(UhcRetainedDatasetError, match="not an object"):
        retained._decoded_semantic_fact(b"[]")


def _sealed_fact_file(tmp_path, block):
    admitted = _admitted_file(tmp_path)
    identity = admitted.semantic_identity("d" * 64)
    return retained.UhcSealedSemanticFile(
        admitted,
        identity,
        {
            "stage_schema": "mrf_test",
            "stage_relation": "stage",
            "fact_blocks_json": [block],
        },
    )


def _fact_record_test_fixture(tmp_path):
    compressed = zlib.compress(b'{"fact":1}\n')
    block_by_field = {
        "range_ordinal": 0,
        "record_start": 0,
        "record_count": 1,
        "compressed_payload_sha256": hashlib.sha256(compressed).hexdigest(),
        "semantic_block_sha256": "e" * 64,
    }
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=compressed))
    sealed = _sealed_fact_file(tmp_path, block_by_field)
    return compressed, block_by_field, connection, sealed


@pytest.mark.asyncio
async def test_fact_record_stream_validates_every_block_boundary(tmp_path):
    """Fact streaming rejects every missing, changed, or miscounted block."""
    _compressed, block_by_field, connection, sealed = (
        _fact_record_test_fixture(tmp_path)
    )
    sealed_string = retained.UhcSealedSemanticFile(
        sealed.admitted,
        sealed.identity,
        {
            **sealed.build_row,
            "fact_blocks_json": json.dumps([block_by_field]),
        },
    )
    fact_rows = [
        fact_record
        async for fact_record in retained._fact_records(
            connection,
            sealed_string,
        )
    ]
    assert fact_rows[0][3] == {"fact": 1}

    invalid_blocks = (
        "{}",
        [None],
        [{**block_by_field, "range_ordinal": 1}],
    )
    for blocks in invalid_blocks:
        invalid = retained.UhcSealedSemanticFile(
            sealed.admitted,
            sealed.identity,
            {**sealed.build_row, "fact_blocks_json": blocks},
        )
        with pytest.raises(UhcRetainedDatasetError):
            async for _ in retained._fact_records(connection, invalid):
                continue


@pytest.mark.asyncio
async def test_fact_record_stream_rejects_missing_hash_and_count(
    tmp_path,
):
    """Fact streaming rejects missing payloads and proof drift."""
    compressed, block_by_field, connection, sealed = (
        _fact_record_test_fixture(tmp_path)
    )
    connection.fetchval.return_value = None
    with pytest.raises(UhcRetainedDatasetError, match="block is missing"):
        async for _ in retained._fact_records(connection, sealed):
            continue
    connection.fetchval.return_value = compressed
    changed_hash = _sealed_fact_file(
        tmp_path,
        {
            **block_by_field,
            "compressed_payload_sha256": "0" * 64,
        },
    )
    with pytest.raises(UhcRetainedDatasetError, match="hash changed"):
        async for _ in retained._fact_records(connection, changed_hash):
            continue
    changed_count = _sealed_fact_file(
        tmp_path,
        {**block_by_field, "record_count": 2},
    )
    with pytest.raises(UhcRetainedDatasetError, match="record count"):
        async for _ in retained._fact_records(connection, changed_count):
            continue


@pytest.mark.asyncio
async def test_copy_and_retained_only_fact_paths():
    """Empty copies and retained-only facts remain publication no-ops."""

    connection = SimpleNamespace(copy_records_to_table=AsyncMock())
    await retained._copy_batches(connection, "stage", (), [])
    connection.copy_records_to_table.assert_not_awaited()

    buffers, proof_builder, admitted, _ = _retained_only_fact_fixture()
    retained._append_canonical_fact(
        buffers,
        admitted,
        0,
        0,
        {},
        proof_builder,
        (),
    )
    proof_builder.observe_rows.assert_not_called()


def _retained_only_fact_fixture():
    buffers = retained._CanonicalLandingBuffers([], [], [])
    proof_builder = Mock()
    retained_scope = SimpleNamespace(
        pairing_status=retained.PAIRING_UNPAIRED_RETAINED_ONLY
    )
    admitted = SimpleNamespace(
        source_file_id="a" * 64,
        logical_scope=retained_scope,
        collection_kind="provider_membership",
    )
    tombstone_by_field = {
        "_healthporta_quarantine": {
            "contract_id": UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
            "reason": UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
            "source_file_id": admitted.source_file_id,
            "range_ordinal": 0,
            "occurrence_ordinal": 1,
            "record_sha256": "b" * 64,
        }
    }
    return buffers, proof_builder, admitted, tombstone_by_field


def test_retained_only_quarantine_paths() -> None:
    """Retained-only tombstones stay private and lineage-bound."""

    buffers, proof_builder, admitted, tombstone_by_field = (
        _retained_only_fact_fixture()
    )
    retained._append_canonical_fact(
        buffers,
        admitted,
        0,
        1,
        tombstone_by_field,
        proof_builder,
        (),
    )
    assert buffers == retained._CanonicalLandingBuffers([], [], [])
    proof_builder.observe_rows.assert_not_called()
    with pytest.raises(UhcRetainedDatasetError, match="lineage mismatch"):
        retained._append_canonical_fact(
            buffers,
            admitted,
            1,
            1,
            tombstone_by_field,
            proof_builder,
            (),
        )
    admitted.collection_kind = "plan_reference"
    with pytest.raises(UhcRetainedDatasetError, match="plan fact"):
        retained._append_canonical_fact(
            buffers,
            admitted,
            0,
            1,
            tombstone_by_field,
            proof_builder,
            (),
        )
    with pytest.raises(UhcRetainedDatasetError, match="collection kind"):
        retained._append_canonical_fact(
            buffers,
            admitted,
            0,
            0,
            {},
            proof_builder,
            (),
        )


def _valid_plan_key_string():
    plan_key = retained._plan_key(
        _individual_exchange_scope(),
        _provider_record("INDIVIDUAL")["plans"][0],
        2026,
    )
    return json.dumps(
        retained._plan_key_payload(plan_key),
        sort_keys=True,
    )


def test_plan_and_lineage_guards_cover_encoded_values(tmp_path):
    with pytest.raises(UhcRetainedDatasetError, match="staged plan key"):
        retained._validated_plan_key_map("{")
    with pytest.raises(UhcRetainedDatasetError, match="plan key"):
        retained._validated_plan_key_map("{}")
    invalid_key = json.loads(_valid_plan_key_string())
    invalid_key["plan_id"] = ""
    with pytest.raises(UhcRetainedDatasetError, match="plan key"):
        retained._plan_payload(
            json.dumps(invalid_key),
            "plan-a",
            [],
        )
    plan_row = retained._canonical_plan_row(
        {
            "plan_key": _valid_plan_key_string(),
            "resource_id": "plan-a",
            "details": json.dumps([]),
        }
    )
    assert plan_row[0] == "InsurancePlan"

    admitted = _admitted_file(tmp_path)
    sealed = retained.UhcSealedSemanticFile(
        admitted,
        admitted.semantic_identity("d" * 64),
        {
            "fact_blocks_json": json.dumps(
                [{"range_ordinal": 0, "semantic_block_sha256": "e" * 64}]
            )
        },
    )
    assert retained._semantic_input_lineage((sealed,))[0][
        "range_ordinal"
    ] == 0
    for blocks in (None, [None]):
        invalid = retained.UhcSealedSemanticFile(
            admitted,
            sealed.identity,
            {"fact_blocks_json": blocks},
        )
        with pytest.raises(UhcRetainedDatasetError, match="fact block"):
            retained._semantic_input_lineage((invalid,))


@pytest.mark.asyncio
async def test_plan_landing_and_stage_seal_cover_terminal_pages(
    monkeypatch,
):
    connection = SimpleNamespace(
        execute=AsyncMock(),
        fetchrow=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        retained,
        "_semantic_input_lineage",
        Mock(return_value=()),
    )
    plan_page_by_field = {
        "plan_key": _valid_plan_key_string(),
        "resource_id": "plan-a",
        "details": [],
    }
    monkeypatch.setattr(retained, "_COPY_BATCH_ROWS", 1)
    monkeypatch.setattr(
        retained,
        "_plan_resource_page",
        AsyncMock(side_effect=[[plan_page_by_field], []]),
    )
    monkeypatch.setattr(retained, "_copy_batches", AsyncMock())
    await retained._land_plan_resources(
        connection,
        ("resource", "plan", "key", "evidence", "sealed"),
        (),
        Mock(),
    )
    with pytest.raises(UhcRetainedDatasetError, match="six-resource"):
        await retained._seal_canonical_resource_stage(
            connection,
            ("resource", "plan", "key", "evidence", "sealed"),
            SimpleNamespace(resource_counts={"Practitioner": 1}),
        )
    with pytest.raises(UhcRetainedDatasetError, match="plan-key proof"):
        await retained._plan_key_counts(connection, "key")


def test_semantic_set_and_evidence_shards_reject_mixed_shapes(tmp_path):
    admitted = _admitted_file(tmp_path)
    other = _admitted_file(tmp_path, source_file_id="f" * 64)
    sealed = retained.UhcSealedSemanticFile(
        admitted,
        admitted.semantic_identity("d" * 64),
        {},
    )
    changed = retained.UhcSealedSemanticFile(
        other,
        other.semantic_identity("e" * 64),
        {},
    )
    admitted_set = retained.UhcAdmittedCatalogSet(
        catalog_set_sha256=admitted.catalog_set_sha256,
        files=(admitted, other),
        provider_file_count=2,
        plan_file_count=0,
    )
    with pytest.raises(UhcRetainedDatasetError, match="encoder set"):
        retained._semantic_set_identity(admitted_set, (sealed, changed))

    evidence_row_by_field = {
        "range_ordinal": 0,
        "evidence_count": 1,
        "layout_sha256": "1" * 64,
    }
    evidence_build_by_field = {
        "stage_schema": "mrf",
        "stage_relation": "stage",
        "evidence_ranges_json": json.dumps([evidence_row_by_field]),
        "evidence_identity_set_sha256": "2" * 64,
    }
    evidence_file = retained.UhcSealedSemanticFile(
        admitted,
        sealed.identity,
        evidence_build_by_field,
    )
    assert retained._npi_evidence_proof_shards((evidence_file,))[0][
        "row_count"
    ] == 1
    for ranges in (None, [None]):
        invalid = retained.UhcSealedSemanticFile(
            admitted,
            sealed.identity,
            {
                **evidence_build_by_field,
                "evidence_ranges_json": ranges,
            },
        )
        with pytest.raises(UhcRetainedDatasetError, match="evidence range"):
            retained._npi_evidence_proof_shards((invalid,))


def _summary_counters():
    counter_by_field = {
        field_name: 0
        for field_name in retained._SUMMARY_ADDITIVE_COUNTERS
    }
    counter_by_field.update(
        invalid_npi_individual_records=0,
        invalid_npi_facility_records=0,
        invalid_npi_address_rows=0,
        invalid_npi_provider_plan_rows=0,
        quarantine_identity_set_sha256="0" * 64,
    )
    return counter_by_field


def test_summary_accumulator_counts_retained_only_provider_evidence():
    count_by_field = dict.fromkeys(
        SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
        0,
    )
    retained_only = dict.fromkeys(
        SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS,
        0,
    )
    rejected = dict.fromkeys(
        UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS,
        0,
    )
    counters = _summary_counters()
    for counter_field in SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS.values():
        counters[counter_field] = 1
    files = (
        SimpleNamespace(
            admitted=SimpleNamespace(
                source_file_id="a" * 64,
                collection_kind="plan_reference",
                logical_scope=SimpleNamespace(pairing_status="paired"),
            ),
            build_row={"counters_json": counters},
            stage_ref="mrf.plan",
        ),
        SimpleNamespace(
            admitted=SimpleNamespace(
                source_file_id="b" * 64,
                collection_kind="provider_membership",
                logical_scope=SimpleNamespace(
                    pairing_status=retained.PAIRING_UNPAIRED_RETAINED_ONLY
                ),
            ),
            build_row={
                "counters_json": counters,
                "evidence_count": 1,
            },
            stage_ref="mrf.provider",
        ),
    )
    expected, stages, quarantine_proof = (
        retained._accumulate_sealed_summary_counts(
            files,
            count_by_field,
            retained_only,
            rejected,
        )
    )
    assert expected == 1
    assert stages == ["mrf.provider"]
    assert len(quarantine_proof) == 64
    assert retained_only[SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_KEY] == 1


def _quarantined_provider_file(
    source_file_id: str,
    invalid_count: int,
    *,
    provider_count: int | None = None,
):
    if provider_count is None:
        provider_count = invalid_count
    counters = _summary_counters()
    counters.update(
        raw_provider_records=provider_count,
        raw_individual_records=provider_count,
        raw_address_rows=provider_count,
        raw_provider_plan_rows=provider_count,
        invalid_npi_count=invalid_count,
        invalid_npi_individual_records=invalid_count,
        invalid_npi_address_rows=invalid_count,
        invalid_npi_provider_plan_rows=invalid_count,
        quarantine_identity_set_sha256=hashlib.sha256(
            source_file_id.encode()
        ).hexdigest(),
    )
    return SimpleNamespace(
        admitted=SimpleNamespace(
            source_file_id=source_file_id,
            collection_kind="provider_membership",
            logical_scope=SimpleNamespace(pairing_status="paired"),
        ),
        build_row={
            "counters_json": counters,
            "evidence_count": provider_count - invalid_count,
        },
        stage_ref=f"mrf.provider_{source_file_id[0]}",
    )


def test_summary_accumulator_applies_quarantine_rate_per_provider_file():
    count_by_field = dict.fromkeys(
        SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
        0,
    )
    retained_only = dict.fromkeys(
        SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS,
        0,
    )
    rejected = dict.fromkeys(
        UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS,
        0,
    )
    files = (
        _quarantined_provider_file("a" * 64, 1),
        _quarantined_provider_file("b" * 64, 1),
    )

    expected, stages, quarantine_proof = (
        retained._accumulate_sealed_summary_counts(
            files,
            count_by_field,
            retained_only,
            rejected,
        )
    )

    assert expected == 0
    assert stages == ["mrf.provider_a", "mrf.provider_b"]
    assert count_by_field["raw_provider_records"] == 2
    assert count_by_field["invalid_npi_count"] == 2
    assert rejected["invalid_npi_checksum"] == 2
    assert len(quarantine_proof) == 64


def test_summary_accumulator_accepts_bounded_multi_quarantine_file():
    count_by_field = dict.fromkeys(
        SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
        0,
    )
    retained_only = dict.fromkeys(
        SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS,
        0,
    )
    rejected = dict.fromkeys(
        UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS,
        0,
    )

    expected, _, _ = retained._accumulate_sealed_summary_counts(
        (
            _quarantined_provider_file(
                "a" * 64,
                2,
                provider_count=10_001,
            ),
        ),
        count_by_field,
        retained_only,
        rejected,
    )

    assert expected == 9_999
    assert count_by_field["raw_provider_records"] == 10_001
    assert count_by_field["invalid_npi_count"] == 2
    assert rejected["invalid_npi_checksum"] == 2


def test_summary_accumulator_rejects_over_rate_provider_file():
    count_by_field = dict.fromkeys(
        SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
        0,
    )
    retained_only = dict.fromkeys(
        SOURCE_SUMMARY_UHC_RETAINED_ONLY_DROP_FIELDS,
        0,
    )
    rejected = dict.fromkeys(
        UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS,
        0,
    )

    with pytest.raises(UhcRetainedDatasetError, match="file ceiling"):
        retained._accumulate_sealed_summary_counts(
            (_quarantined_provider_file("a" * 64, 2),),
            count_by_field,
            retained_only,
            rejected,
        )


@pytest.mark.asyncio
async def test_combined_summary_rejects_disagreeing_provider_census(
    monkeypatch,
):
    monkeypatch.setattr(
        retained,
        "_accumulate_sealed_summary_counts",
        Mock(return_value=(1, [], "a" * 64)),
    )
    monkeypatch.setattr(
        retained,
        "summarize_uhc_npi_evidence_stages",
        AsyncMock(
            return_value=SimpleNamespace(
                distinct_npis=0,
                duplicate_npi_groups=0,
                conflicting_npi_groups=0,
                conflict_counts={},
            )
        ),
    )
    monkeypatch.setattr(
        retained,
        "_plan_key_counts",
        AsyncMock(
            return_value={
                "membership_plan_key_count": 0,
                "detail_plan_key_count": 0,
                "matched_plan_key_count": 0,
                "missing_plan_detail_count": 0,
                "orphan_plan_detail_count": 0,
            }
        ),
    )
    with pytest.raises(UhcRetainedDatasetError, match="counters disagree"):
        await retained._combined_summary_counts(
            object(),
            SimpleNamespace(provider_file_count=1, plan_file_count=0),
            (),
            ("resource", "plan", "key", "evidence", "sealed"),
        )


@pytest.mark.asyncio
async def test_combined_summary_enforces_catalog_quarantine_ceiling(monkeypatch):
    def accumulate(_files, counts, _retained_only, rejected):
        counts.update(
            raw_provider_records=35,
            raw_individual_records=35,
            raw_address_rows=35,
            raw_provider_plan_rows=35,
            invalid_npi_count=33,
        )
        rejected.update(
            invalid_npi_checksum=33,
            invalid_npi_checksum_individual_records=33,
            invalid_npi_checksum_facility_records=0,
            invalid_npi_checksum_address_rows=33,
            invalid_npi_checksum_provider_plan_rows=33,
        )
        return 2, [], "a" * 64

    monkeypatch.setattr(
        retained,
        "_accumulate_sealed_summary_counts",
        accumulate,
    )
    monkeypatch.setattr(
        retained,
        "summarize_uhc_npi_evidence_stages",
        AsyncMock(
            return_value=SimpleNamespace(
                distinct_npis=2,
                duplicate_npi_groups=0,
                conflicting_npi_groups=0,
                conflict_counts={},
            )
        ),
    )
    monkeypatch.setattr(
        retained,
        "_plan_key_counts",
        AsyncMock(
            return_value={
                "membership_plan_key_count": 0,
                "detail_plan_key_count": 0,
                "matched_plan_key_count": 0,
                "missing_plan_detail_count": 0,
                "orphan_plan_detail_count": 0,
            }
        ),
    )

    with pytest.raises(UhcRetainedDatasetError, match="publication ceiling"):
        await retained._combined_summary_counts(
            object(),
            SimpleNamespace(provider_file_count=1, plan_file_count=1),
            (),
            ("resource", "plan", "key", "evidence", "sealed"),
        )


def _assert_summary_shape_rejections() -> None:
    summary_by_field = _valid_summary_input()
    missing_field_summary_by_name = dict(summary_by_field)
    missing_field_summary_by_name.pop("complete")
    with pytest.raises(UhcRetainedDatasetError, match="shape"):
        validate_uhc_summary_input(missing_field_summary_by_name)

    invalid_build = _valid_summary_input()
    invalid_build["semantic_build_ids"] = []
    with pytest.raises(UhcRetainedDatasetError, match="build set"):
        validate_uhc_summary_input(invalid_build)

    invalid_counts = _valid_summary_input()
    invalid_counts["count_by_field"].pop(
        next(iter(SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS))
    )
    with pytest.raises(UhcRetainedDatasetError, match="count fields"):
        validate_uhc_summary_input(invalid_counts)

    invalid_categories = _valid_summary_input()
    invalid_categories["count_by_category"] = []
    with pytest.raises(UhcRetainedDatasetError, match="invalid"):
        validate_uhc_summary_input(invalid_categories)
    invalid_category_map = _valid_summary_input()
    invalid_category_map["count_by_category"]["conflict_counts"] = []
    with pytest.raises(UhcRetainedDatasetError, match="categories"):
        validate_uhc_summary_input(invalid_category_map)

    unknown = _valid_summary_input()
    unknown["count_by_category"]["unknown_field_counts"] = {"unknown": 1}
    with pytest.raises(UhcRetainedDatasetError, match="unaccounted"):
        validate_uhc_summary_input(unknown)


def _assert_summary_dimension_rejections() -> None:
    for rejected_field in (
        "invalid_npi_checksum_address_rows",
        "invalid_npi_checksum_provider_plan_rows",
    ):
        invalid_rejection = _valid_summary_input()
        invalid_rejection["count_by_field"].update(
            raw_provider_records=1,
            raw_individual_records=1,
            raw_address_rows=1,
            raw_provider_plan_rows=1,
            invalid_npi_count=1,
        )
        invalid_rejection["count_by_category"]["rejected_counts"] = {
            "invalid_npi_checksum": 1,
            "invalid_npi_checksum_individual_records": 1,
            "invalid_npi_checksum_facility_records": 0,
            "invalid_npi_checksum_address_rows": 1,
            "invalid_npi_checksum_provider_plan_rows": 1,
        }
        invalid_rejection["count_by_category"]["rejected_counts"][
            rejected_field
        ] = 0
        invalid_rejection["input_sha256"] = _summary_input_hash(
            invalid_rejection
        )
        with pytest.raises(UhcRetainedDatasetError, match="unaccounted"):
            validate_uhc_summary_input(invalid_rejection)


def test_summary_input_accepts_complete_structural_rejection_map() -> None:
    summary_input = _valid_summary_input()
    summary_input["count_by_field"].update(
        raw_provider_records=1,
        raw_individual_records=0,
        raw_facility_records=1,
        raw_address_rows=1,
        raw_provider_plan_rows=1,
        invalid_npi_count=1,
        provider_file_count=1,
    )
    summary_input["count_by_category"]["rejected_counts"] = {
        "invalid_npi_checksum": 0,
        "invalid_npi_checksum_individual_records": 0,
        "invalid_npi_checksum_facility_records": 0,
        "invalid_npi_checksum_address_rows": 0,
        "invalid_npi_checksum_provider_plan_rows": 0,
        "invalid_npi_structure": 1,
        "invalid_npi_structure_individual_records": 0,
        "invalid_npi_structure_facility_records": 1,
        "invalid_npi_structure_address_rows": 1,
        "invalid_npi_structure_provider_plan_rows": 1,
    }
    summary_input["input_sha256"] = _summary_input_hash(summary_input)

    assert validate_uhc_summary_input(summary_input) == summary_input


def _assert_summary_identity_rejections() -> None:
    changed_hash = _valid_summary_input()
    changed_hash["input_sha256"] = "0" * 64
    with pytest.raises(UhcRetainedDatasetError, match="hash is invalid"):
        validate_uhc_summary_input(changed_hash)

    admitted_set = SimpleNamespace(files=(SimpleNamespace(source_file_id="a"),))
    with pytest.raises(UhcRetainedDatasetError, match="partial semantic set"):
        retained._assert_complete_semantic_set(admitted_set, ())


def test_summary_validation_rejects_each_outer_contract_boundary() -> None:
    """Summary validation rejects shape, dimension, and identity drift."""

    _assert_summary_shape_rejections()
    _assert_summary_dimension_rejections()
    _assert_summary_identity_rejections()


@pytest.mark.asyncio
async def test_canonical_build_failure_cleans_private_stage(monkeypatch):
    admitted_set = SimpleNamespace(files=())
    cleanup_uhc_canonical_stage = retained.cleanup_uhc_canonical_stage
    monkeypatch.setattr(
        retained,
        "_stage_names",
        Mock(
            return_value=(
                "provider_directory_uhc_resource_a",
                "provider_directory_uhc_plan_a",
                "provider_directory_uhc_key_a",
                "provider_directory_uhc_evidence_a",
                "provider_directory_uhc_sealed_a",
            )
        ),
    )
    monkeypatch.setattr(
        retained,
        "_land_uhc_canonical_content",
        AsyncMock(side_effect=RuntimeError("injected")),
    )
    cleanup = AsyncMock()
    monkeypatch.setattr(retained, "cleanup_uhc_canonical_stage", cleanup)
    with pytest.raises(RuntimeError, match="injected"):
        await retained.build_uhc_canonical_stage(
            object(),
            admitted_set,
            (),
        )
    cleanup.assert_awaited_once()

    with pytest.raises(UhcRetainedDatasetError, match="unowned"):
        await cleanup_uhc_canonical_stage(
            SimpleNamespace(execute=AsyncMock()),
            retained.UhcCanonicalStage(
                schema="mrf",
                resource_relation="unowned",
                auxiliary_relations=(),
                resource_counts={},
                content_proof={},
                summary_input={},
                semantic_build_ids=(),
                phase_metrics={},
            ),
        )
