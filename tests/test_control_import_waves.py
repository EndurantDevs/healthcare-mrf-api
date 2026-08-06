"""Closed-contract unit checks for signed exact-wave admission."""

from __future__ import annotations

import copy
import hashlib

import pytest

from api import control_imports
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


_KEY = "test-control-key"


def _unsigned(count: int = 2) -> dict:
    imported_digest = hashlib.sha256(
        "\0".join(
            f"coordinate-unit-{ordinal}\0v1" for ordinal in range(count)
        ).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": "healthporta.ptg-import-wave-attestation.v1",
        "wave_id": "wave-unit",
        "idempotency_key": "wave-unit-key",
        "snapshot": {
            "snapshot_digest": "a" * 64,
            "membership_digest": "b" * 64,
            "inventory_digest": "c" * 64,
            "subscription_coverage_digest": "d" * 64,
            "entitlement_coverage_count": 2,
            "entitlement_coverage_digest": "8" * 64,
            "catalog_generation": "9" * 64,
        },
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


def _payload(count: int = 2) -> dict:
    unsigned = _unsigned(count)
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


@pytest.mark.parametrize("count", [None, True, False, 0, -1, 1.5])
def test_entitlement_coverage_count_must_be_a_positive_nonbool_integer(count):
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
    with pytest.raises(ValueError, match="positive integer"):
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
