# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical initial authority permits size drift without changing the target contract."""

import copy
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_failed_cleanup as cleanup
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as contract
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_profile_initial_test_support import signed_initial_envelope
from tests.test_provider_directory_profile_initial import _geometry, _target, fhir
from tests.test_provider_directory_profile_initial_replay import _consume_replay_authority, _selection

BUILD = "pdpb_" + "5" * 32
OWNER = "run_" + "a" * 32


def _original_authority(geometry, target_by_field):
    """Use a genuine signed initial guard and its immutable, now expired consumption."""
    execution = _selection()
    source_pairs = tuple((pair["source_id"], pair["dataset_id"]) for pair in execution.attestation.pairs)
    geometry = replace(
        geometry,
        profile_as_of=execution.attestation.desired_profile_as_of,
        selection_proof_id=execution.attestation.proof_id,
        profile_input_digest=execution.attestation.profile_input_digest,
        profile_schema_version=execution.attestation.profile_schema_version,
        profile_strategy_version=execution.attestation.profile_strategy_version,
        desired_source_vector_hash=fhir._provider_directory_profile_source_vector_hash(source_pairs),
        initial_target_state_sha256=contract.target_state_sha256(target_by_field),
        **{
            name: observed
            for name, observed in target_by_field.items()
            if "_target_" in name and name in vars(geometry)
        },
    )
    accepted = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=2)
    database = SimpleNamespace(
        **vars(geometry), temp_tablespace_oid=geometry.tablespace_oid, temp_tablespace_name=geometry.tablespace_name
    )
    envelope, receipt = signed_initial_envelope(geometry, database, target_by_field, execution, accepted)
    trust = capacity_trust_from_envelope(envelope)
    lease, consumed = _consume_replay_authority(
        envelope, trust, accepted, geometry, {"run_id": OWNER, "build_id": BUILD}, {}, receipt
    )
    params = copy.deepcopy(lease.signing_preflight_guard["healthcare_request"]["profile_execution"])
    params["provider_directory_profile_capacity_attestation"] = envelope
    checkpoint_by_field = {
        "build_id": BUILD,
        "owner_run_id": OWNER,
        "executable_plan_hash": geometry.executable_plan_hash,
        "desired_source_vector_hash": geometry.desired_source_vector_hash,
        "desired_source_context_vector_hash": geometry.desired_context_vector_hash,
        "profile_as_of": geometry.profile_as_of,
    }
    owner_by_field = {"run_id": OWNER, "params": params}
    assert lease.expires_at < datetime.now(timezone.utc)
    return geometry, checkpoint_by_field, owner_by_field, consumed, trust


def _manifest_fixture(monkeypatch, *, drift=True):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    original = _target()
    geometry, checkpoint, owner, consumed, trust = _original_authority(_geometry(), original)
    current = {**original, "profile_target_bytes": original["profile_target_bytes"] + 8192} if drift else original
    targets = contract.InitialTargets(current["evidence_target_oid"], current["profile_target_oid"], current)
    monkeypatch.setattr(initial, "capture_targets", AsyncMock(return_value=targets))
    monkeypatch.setattr(
        initial,
        "receipt_layout",
        AsyncMock(
            return_value=SimpleNamespace(
                relation_oid=geometry.initial_receipt_oid,
                exact_fingerprint=geometry.initial_receipt_storage_fingerprint,
                effective_tablespace_oids=(geometry.tablespace_oid,),
            )
        ),
    )
    rows = [SimpleNamespace(_mapping=consumed)]
    monkeypatch.setattr(fhir.db, "all", AsyncMock(return_value=rows))
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=False))
    monkeypatch.setattr(fhir.db, "status", AsyncMock())
    monkeypatch.setattr(fhir.profile_capacity_runtime, "configured_capacity_lease_trust", lambda: trust)
    return original, geometry, checkpoint, owner, consumed, rows, targets


@pytest.mark.asyncio
@pytest.mark.parametrize("locked", [False, True])
async def test_size_drift_retains_complete_signed_manifest_and_fresh_physics(monkeypatch, locked):
    original, geometry, checkpoint, owner, _consumed, _rows, targets = _manifest_fixture(monkeypatch)
    manifest, observed = await cleanup._initial_manifest(
        fhir, "mrf", geometry, locked=locked, checkpoint=checkpoint, owner=owner
    )
    assert manifest["target_state"] == original
    assert contract.target_state_sha256(manifest["target_state"]) == geometry.initial_target_state_sha256
    assert observed is targets
    assert observed.payload["profile_target_bytes"] != original["profile_target_bytes"]
    assert fhir.db.status.await_count == int(locked)
    assert "admission_purpose = 'profile'" in fhir.db.all.call_args.args[0]


@pytest.mark.asyncio
async def test_exact_target_fast_path_requires_no_historical_trust(monkeypatch):
    original, geometry, checkpoint, owner, *_rest = _manifest_fixture(monkeypatch, drift=False)
    monkeypatch.setattr(cleanup, "_original_initial_target", AsyncMock(side_effect=AssertionError("unexpected replay")))
    manifest, _observed = await cleanup._initial_manifest(
        fhir, "mrf", geometry, locked=False, checkpoint=checkpoint, owner=owner
    )
    assert manifest["target_state"] == original
    fhir.db.all.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault,error",
    [
        ("missing", "replay_consumption_missing"),
        ("ambiguous", "replay_consumption_ambiguous"),
        ("signature", "replay_capacity_invalid"),
        ("guard", "replay_capacity_invalid"),
        ("database", "replay_capacity_invalid"),
        ("geometry", "replay_capacity_invalid"),
        ("source", "replay_capacity_consumption_changed"),
        ("context", "replay_capacity_consumption_changed"),
        ("purpose", "replay_capacity_consumption_changed"),
        ("accepted_at", "replay_capacity_invalid"),
        ("owner", "initial_original_binding_changed"),
        ("build", "initial_original_binding_changed"),
        ("checkpoint_source", "initial_original_binding_changed"),
        ("proof", "initial_original_binding_changed"),
        ("owner_envelope", "initial_original_owner_changed"),
        ("owner_execution", "initial_original_authority_changed"),
        ("missing_owner_params", "initial_original_binding_changed"),
        ("trust", "replay_capacity_invalid"),
    ],
)
async def test_size_drift_refuses_unbound_or_forged_original_authority(monkeypatch, fault, error):
    _original, geometry, checkpoint, owner, consumed, consumption_rows, _targets = _manifest_fixture(monkeypatch)
    match fault:
        case "missing":
            consumption_rows.clear()
        case "ambiguous":
            consumption_rows.append(consumption_rows[0])
        case "guard" | "database":
            body = json.loads(consumed["canonical_lease_json"])
            if fault == "guard":
                body["signing_preflight_guard"]["healthcare_receipt"]["serving_generation_preflight"][
                    "profile_rows"
                ] = 1
            else:
                body["database_oid"] += 1
            consumed["canonical_lease_json"] = cleanup.canonical(body)
        case "signature":
            consumed["signature"] = "A" * 86
        case "geometry":
            geometry = replace(geometry, initial_receipt_oid=geometry.initial_receipt_oid + 1)
        case "source" | "context":
            consumed["source_vector_hash" if fault == "source" else "source_context_vector_hash"] = "ff" * 32
        case "purpose":
            consumed["admission_purpose"] = "cms_nonprofile"
        case "accepted_at":
            consumed["accepted_at"] = datetime.now(timezone.utc)
        case "owner" | "build":
            consumed["run_id" if fault == "owner" else "build_id"] = (
                "run_" if fault == "owner" else "pdpb_"
            ) + "1" * 32
        case "checkpoint_source":
            checkpoint["desired_source_vector_hash"] = "00" * 32
        case "proof":
            owner["params"]["provider_directory_profile_selection_attestation"].pop("proof_id")
        case "owner_envelope":
            owner["params"]["provider_directory_profile_capacity_attestation"] = {}
        case "owner_execution":
            owner["params"]["provider_directory_profile_generation"] += 1
        case "missing_owner_params":
            owner["params"] = {}
        case "trust":
            monkeypatch.setattr(fhir.profile_capacity_runtime, "configured_capacity_lease_trust", lambda: None)
    with pytest.raises(RuntimeError, match=error + "$"):
        await cleanup._initial_manifest(fhir, "mrf", geometry, locked=True, checkpoint=checkpoint, owner=owner)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("evidence_target_oid", 20003),
        ("profile_target_oid", 20004),
        ("evidence_target_storage_fingerprint", "00" * 32),
        ("profile_target_storage_fingerprint", "00" * 32),
        ("evidence_rows", 1),
        ("profile_rows", 1),
        ("serving_singleton_absent", False),
        ("initial_commit_receipt_absent", False),
        ("historical_publication", {"changed": True}),
        ("resolution", "legacy_as_of_unknown"),
    ],
)
async def test_size_exception_never_accepts_other_target_drift(monkeypatch, field, value):
    _original, geometry, checkpoint, owner, _consumed, _rows, targets = _manifest_fixture(monkeypatch)
    targets.payload[field] = value
    with pytest.raises(RuntimeError, match="initial_target_changed$"):
        await cleanup._initial_manifest(fhir, "mrf", geometry, locked=False, checkpoint=checkpoint, owner=owner)
