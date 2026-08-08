"""Closed-contract unit checks for signed exact-wave admission."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from api import control_imports
from api.control_import_wave_attestation import (
    ATTESTATION_VERSION,
    AUTHORIZATION_BASIS,
    LEGACY_ATTESTATION_VERSION,
    SUPERSESSION_ATTESTATION_VERSION,
)
from api.control_import_waves import (
    ImportWaveConflict,
    sign_cohort_attestation,
    validate_import_wave_payload,
)
from db.models import PTGImportWaveClaim
from process.ptg_parts.ptg_wave_admission_fence import (
    PTGWaveOwnershipConflict,
    is_ptg_wave_owned_run,
)
from process.ptg_wave_state import canonical_json, sha256_digest


_KEY = "test-control-key"


def _unsigned(
    count: int = 2,
    *,
    schema_version: str = ATTESTATION_VERSION,
) -> dict:
    imported_digest = hashlib.sha256(
        "\0".join(
            f"coordinate-unit-{ordinal}\0v1" for ordinal in range(count)
        ).encode("utf-8")
    ).hexdigest()
    snapshot_by_field = {
        "snapshot_digest": "a" * 64,
        "membership_digest": "b" * 64,
        "inventory_digest": "c" * 64,
        "subscription_coverage_digest": "d" * 64,
        "entitlement_coverage_count": 2,
        "entitlement_coverage_digest": "8" * 64,
        "catalog_generation": "9" * 64,
    }
    if schema_version in {ATTESTATION_VERSION, SUPERSESSION_ATTESTATION_VERSION}:
        snapshot_by_field.update(
            authorization_basis=AUTHORIZATION_BASIS,
            authorization_digest="7" * 64,
        )
    unsigned = {
        "schema_version": schema_version,
        "wave_id": "wave-unit",
        "idempotency_key": "wave-unit-key",
        "snapshot": snapshot_by_field,
        "partition": {
            "complete": True,
            "physical_coordinate_count": count,
            "physical_coordinate_digest": "e" * 64,
            "imported_coordinate_count": count,
            "imported_coordinate_digest": imported_digest,
            "reused_coordinate_count": 0,
            "reused_coordinate_digest": "0" * 64,
            "partition_digest": "f" * 64,
        },
        "intents": [
            {
                "ordinal": ordinal,
                "run_id": f"run-unit-{ordinal}",
                "source_file_import_id": f"coordinate-unit-{ordinal}",
                "content_version": "v1",
                "params": {
                    "source_file_import_id": f"coordinate-unit-{ordinal}",
                    "import_id": f"coordinate-unit-{ordinal}",
                },
            }
            for ordinal in range(count)
        ],
    }
    if schema_version == SUPERSESSION_ATTESTATION_VERSION:
        unsigned["supersession"] = _supersession_proof(
            successor_wave_id=unsigned["wave_id"],
            intent_count=count,
        )
    return unsigned


def _supersession_proof(*, successor_wave_id: str, intent_count: int) -> dict:
    unsigned = {
        "schema_version": "healthporta.ptg-wave.logical-preclaim-supersession.v1",
        "recovery_basis": "logical_preclaim_failure",
        "predecessor": {
            "wave_id": "retired-wave-unit",
            "wave_digest": "1" * 64,
            "manifest_digest": "2" * 64,
            "jobs_digest": "3" * 64,
            "intent_count": intent_count,
        },
        "successor_wave_id": successor_wave_id,
        "database": {
            "pristine_run_count": intent_count,
            "claim_count": 0,
            "outcome_count": 0,
            "worker_start_event_count": 0,
        },
        "kubernetes": {
            "job_name": "hpw-ptg-wave-" + "1" * 40,
            "job_uid": "synthetic-job-uid",
            "completion_mode": "Indexed",
            "completions": 12,
            "parallelism": 12,
            "backoff_limit": 0,
            "failed": 12,
            "active": 0,
            "succeeded": 0,
            "ready": 0,
            "terminating": 0,
            "failed_condition": True,
            "complete_condition": False,
        },
        "redis": {
            "unclaimed_attestation_digest": "4" * 64,
            "ready_slot_count": 0,
            "release_present": False,
            "queued_ordinal_count": 0,
            "job_ordinal_count": 0,
            "result_ordinal_count": 0,
            "retry_ordinal_count": 0,
            "in_progress_ordinal_count": 0,
            "health_check_present": False,
        },
    }
    return {**unsigned, "proof_digest": sha256_digest(canonical_json(unsigned))}


def _payload(
    count: int = 2,
    *,
    schema_version: str = ATTESTATION_VERSION,
) -> dict:
    unsigned = _unsigned(count, schema_version=schema_version)
    return {
        "cohort_attestation": {
            **unsigned,
            "signature": sign_cohort_attestation(unsigned, key=_KEY),
        }
    }


def test_signed_full_request_identity_binds_snapshot_partition_and_all_intents():
    first = validate_import_wave_payload(_payload(), attestation_key=_KEY)
    changed = _payload()
    changed["cohort_attestation"]["intents"][1]["content_version"] = "v2"
    changed["cohort_attestation"]["partition"]["imported_coordinate_digest"] = (
        hashlib.sha256(
            "coordinate-unit-0\0v1\0coordinate-unit-1\0v2".encode("utf-8")
        ).hexdigest()
    )
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in changed["cohort_attestation"].items()
        if key != "signature"
    }
    changed["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    second = validate_import_wave_payload(changed, attestation_key=_KEY)
    assert first["request_digest"] != second["request_digest"]
    assert first["wave_digest"] != second["wave_digest"]
    assert first["partition"]["complete"] is True
    assert len(first["intents"]) == 2


def test_entitlement_coverage_snapshot_is_required_and_bound():
    first = validate_import_wave_payload(_payload(), attestation_key=_KEY)
    assert first["snapshot"]["entitlement_coverage_count"] == 2
    assert first["snapshot"]["entitlement_coverage_digest"] == "8" * 64

    changed = _payload()
    changed["cohort_attestation"]["snapshot"]["entitlement_coverage_digest"] = (
        "7" * 64
    )
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in changed["cohort_attestation"].items()
        if key != "signature"
    }
    changed["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    second = validate_import_wave_payload(changed, attestation_key=_KEY)
    assert second["request_digest"] != first["request_digest"]
    assert second["wave_digest"] != first["wave_digest"]


@pytest.mark.parametrize("count", [None, True, False, -1, 1.5])
def test_v2_entitlement_diagnostic_count_must_be_nonnegative_integer(count):
    payload = _payload()
    payload["cohort_attestation"]["snapshot"]["entitlement_coverage_count"] = count
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in payload["cohort_attestation"].items()
        if key != "signature"
    }
    payload["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    with pytest.raises(ValueError, match="invalid|non-negative"):
        validate_import_wave_payload(payload, attestation_key=_KEY)


def test_v2_zero_entitlement_diagnostic_is_authorized_by_exact_basis():
    payload = _payload()
    payload["cohort_attestation"]["snapshot"][
        "entitlement_coverage_count"
    ] = 0
    unsigned_attestation_map = {
        key: field_value
        for key, field_value in payload["cohort_attestation"].items()
        if key != "signature"
    }
    payload["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )

    result = validate_import_wave_payload(payload, attestation_key=_KEY)

    assert result["snapshot"]["entitlement_coverage_count"] == 0
    assert result["snapshot"]["authorization_basis"] == AUTHORIZATION_BASIS
    assert result["snapshot"]["authorization_digest"] == "7" * 64


def test_legacy_v1_keeps_positive_entitlement_requirement():
    payload = _payload(schema_version=LEGACY_ATTESTATION_VERSION)
    payload["cohort_attestation"]["snapshot"][
        "entitlement_coverage_count"
    ] = 0
    unsigned_attestation_map = {
        key: field_value
        for key, field_value in payload["cohort_attestation"].items()
        if key != "signature"
    }
    payload["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )

    with pytest.raises(ValueError, match="positive integer"):
        validate_import_wave_payload(payload, attestation_key=_KEY)


def test_fixed_legacy_v1_attestation_keeps_original_replay_identity():
    fixture_path = (
        Path(__file__).parent
        / "fixtures"
        / "ptg_import_wave_attestation_v1.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    result = validate_import_wave_payload(payload, attestation_key=_KEY)

    assert (
        payload["cohort_attestation"]["signature"]
        == "8b407ed3da16e5423df0eb7709a767eb5c2a62712f67714441c7ca0aede68c56"
    )
    assert result["attestation"]["schema_version"] == LEGACY_ATTESTATION_VERSION
    assert result["request_digest"] == (
        "1b939050d2ec4a79e5f6b57351fc9490a417e17c300c8c2ca00fa58e1711753d"
    )
    assert result["wave_digest"] == (
        "45608a6eb035a2e895fc2c65e29ee2ebb02730d509663190213a47054abc4f7a"
    )


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    (
        ("authorization_basis", "other", "authorization_basis"),
        ("authorization_digest", "bad", "SHA-256"),
    ),
)
def test_v2_authorization_evidence_is_exact(field_name, value, message):
    payload = _payload()
    payload["cohort_attestation"]["snapshot"][field_name] = value
    unsigned_attestation_map = {
        key: field_value
        for key, field_value in payload["cohort_attestation"].items()
        if key != "signature"
    }
    payload["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )

    with pytest.raises(ValueError, match=message):
        validate_import_wave_payload(payload, attestation_key=_KEY)


def test_entitlement_coverage_digest_is_exact_sha256_and_not_optional():
    invalid = _payload()
    invalid["cohort_attestation"]["snapshot"]["entitlement_coverage_digest"] = (
        "not-a-digest"
    )
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in invalid["cohort_attestation"].items()
        if key != "signature"
    }
    invalid["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    with pytest.raises(ValueError, match="SHA-256"):
        validate_import_wave_payload(invalid, attestation_key=_KEY)

    missing = _payload()
    missing["cohort_attestation"]["snapshot"].pop(
        "entitlement_coverage_digest"
    )
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in missing["cohort_attestation"].items()
        if key != "signature"
    }
    missing["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    with pytest.raises(ValueError, match="snapshot fields are not exact"):
        validate_import_wave_payload(missing, attestation_key=_KEY)


def test_unsigned_or_tampered_operator_payload_fails_closed(monkeypatch):
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)
    with pytest.raises(ValueError, match="key is required"):
        validate_import_wave_payload(_payload())
    tampered = _payload()
    tampered["cohort_attestation"]["partition"]["physical_coordinate_count"] = 3
    with pytest.raises(ValueError, match="signature is invalid"):
        validate_import_wave_payload(tampered, attestation_key=_KEY)


def test_complete_partition_and_unique_coordinate_identities_are_required():
    incomplete = _payload()
    incomplete["cohort_attestation"]["partition"]["complete"] = False
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in incomplete["cohort_attestation"].items()
        if key != "signature"
    }
    incomplete["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    with pytest.raises(ValueError, match="complete cohort"):
        validate_import_wave_payload(incomplete, attestation_key=_KEY)

    duplicate = _payload()
    duplicate["cohort_attestation"]["intents"][1]["source_file_import_id"] = "coordinate-unit-0"
    duplicate["cohort_attestation"]["intents"][1]["params"] = {
        "source_file_import_id": "coordinate-unit-0", "import_id": "coordinate-unit-0"
    }
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in duplicate["cohort_attestation"].items()
        if key != "signature"
    }
    duplicate["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    with pytest.raises(ValueError, match="must be unique"):
        validate_import_wave_payload(duplicate, attestation_key=_KEY)


def test_canonical_signed_replay_has_stable_request_identity():
    first = validate_import_wave_payload(_payload(), attestation_key=_KEY)
    replay = validate_import_wave_payload(copy.deepcopy(_payload()), attestation_key=_KEY)
    assert replay["request_digest"] == first["request_digest"]
    assert replay["release_queue"] == first["release_queue"]


def test_v3_supersession_proof_is_signed_and_successor_bound():
    payload = _payload(schema_version=SUPERSESSION_ATTESTATION_VERSION)
    validated = validate_import_wave_payload(payload, attestation_key=_KEY)

    assert validated["supersession"] == payload["cohort_attestation"]["supersession"]
    assert validated["supersession"]["successor_wave_id"] == "wave-unit"

    tampered = copy.deepcopy(payload)
    tampered["cohort_attestation"]["supersession"]["successor_wave_id"] = "other"
    unsigned_attestation_map = {
        key: value
        for key, value in tampered["cohort_attestation"].items()
        if key != "signature"
    }
    tampered["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    with pytest.raises(ValueError, match="another successor"):
        validate_import_wave_payload(tampered, attestation_key=_KEY)


@pytest.mark.asyncio
async def test_route_maps_unhashable_attestation_version_to_bad_request(
    monkeypatch,
):
    from api import control_wave_routes
    from sanic.exceptions import BadRequest

    class _Request:
        json = _payload()
        headers = {}
        body = b""

    _Request.json["cohort_attestation"]["schema_version"] = []
    monkeypatch.setattr(control_wave_routes, "require_control_auth", lambda _request: None)
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", _KEY)

    with pytest.raises(BadRequest, match="schema_version is unsupported"):
        await control_wave_routes.control_admit_import_wave(_Request())


def test_rejected_claim_contract_requires_a_nonnull_failure_code():
    constraint = next(
        item
        for item in PTGImportWaveClaim.__table__.constraints
        if item.name == "ptg_import_wave_claim_contract_check"
    )

    assert "failure_code IS NOT NULL" in str(constraint.sqltext)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "args"),
    [
        ("request_cancel", ("run-unit-0",)),
        ("retry_import_run", ("run-unit-0", {})),
        ("finalize_import_run", ("run-unit-0", {})),
    ],
)
async def test_wave_owned_lifecycle_operations_stop_before_side_effects(
    monkeypatch, operation, args
):
    calls: list[str] = []

    async def get_run(_run_id):
        return {
            "run_id": "run-unit-0", "importer": "ptg", "status": "queued",
            "metrics": {},
        }

    async def reject_owner(_executor, _run_id):
        calls.append("ownership")
        raise PTGWaveOwnershipConflict("wave-owned import run is controller-managed")

    async def side_effect(*_args, **_kwargs):
        calls.append("side-effect")
        raise AssertionError("direct lifecycle operation reached a side effect")

    monkeypatch.setattr(control_imports, "get_import_run", get_run)
    monkeypatch.setattr(control_imports, "require_not_wave_owned_run", reject_owner)
    monkeypatch.setattr(control_imports, "_cancel_signal_for_run", side_effect)
    monkeypatch.setattr(control_imports, "create_import_run", side_effect)
    with pytest.raises(PTGWaveOwnershipConflict):
        await getattr(control_imports, operation)(*args)
    assert calls == ["ownership"]


@pytest.mark.asyncio
async def test_wave_ownership_lookup_tolerates_pre_migration_schema():
    class _PreMigrationConnection:
        async def scalar(self, *_args, **_kwargs):
            return None

        async def all(self, *_args, **_kwargs):  # pragma: no cover - safety assertion
            raise AssertionError("intent query requires the migration")

    assert not await is_ptg_wave_owned_run(
        _PreMigrationConnection(),
        "run-unit-0",
    )


@pytest.mark.asyncio
async def test_wave_ownership_lookup_uses_durable_intent_relation():
    class _IntentConnection:
        def __init__(self):
            self.statements = []

        async def scalar(self, *_args, **_kwargs):
            return "mrf.ptg_import_wave_intent"

        async def all(self, statement, *_args, **_kwargs):
            self.statements.append(statement)
            return [("wave-unit",)]

    connection = _IntentConnection()
    assert await is_ptg_wave_owned_run(connection, "run-unit-0")
    assert "ptg_import_wave_intent" in str(connection.statements[0])


@pytest.mark.asyncio
async def test_terminal_worker_sync_skips_durable_wave_owner_without_metrics(
    monkeypatch,
):
    source_run_map = {
        "run_id": "run-unit-0",
        "importer": "ptg",
        "status": "running",
        "metrics": {},
    }
    calls: list[str] = []

    async def is_wave_owned(_executor, run_id):
        assert run_id == "run-unit-0"
        calls.append("ownership")
        return True

    async def unexpected_worker_state(*_args, **_kwargs):  # pragma: no cover - safety assertion
        raise AssertionError("wave-owned run must stay controller-managed")

    monkeypatch.setattr(control_imports, "is_ptg_wave_owned_run", is_wave_owned)
    monkeypatch.setattr(
        control_imports,
        "_active_worker_state",
        unexpected_worker_state,
    )

    assert (
        await control_imports._sync_terminal_worker_failure(source_run_map)
        == source_run_map
    )
    assert calls == ["ownership"]
