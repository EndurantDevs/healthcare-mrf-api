# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent purpose, signature, freshness and shared-volume accounting guards."""

import base64
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from process import provider_directory_profile_failed_cleanup as cleanup

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def sign(body, key):
    return {
        "authorization": body,
        "signature": base64.urlsafe_b64encode(
            key.sign((cleanup.signature_domain(body) + "\n" + cleanup.canonical(body)).encode())
        )
        .decode()
        .rstrip("="),
    }


def authorization_fixture(now=NOW):
    """Return one logical signed permit and independent source/key trust."""
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
    trust_key = SimpleNamespace(
        key_id="key-a",
        public_key=key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw),
        attestor_release_digest="b" * 64,
        status="active",
        retired_at=None,
        verify_until=None,
    )
    trust = SimpleNamespace(
        attestor_id="authority-a", environment_id="environment-a", active_key_id="key-a", keys=(trust_key,)
    )
    storage_classes = [
        {
            "storage_class": role,
            "reserved_bytes": reserved,
            "available_bytes": 10**10,
            "available_after_all_reservations_bytes": 10**10 - reserved,
        }
        for role, reserved in (("data", 10**6), ("temp", 0), ("wal", 10**8))
    ]
    return sign(_authorization_body(now, trust, storage_classes), key), trust, key


def test_independent_signature_and_retained_claim_replay():
    envelope, trust, _key = authorization_fixture()
    assert cleanup.validate_authorization(envelope, trust=trust, now=NOW) == envelope["authorization"]
    with pytest.raises(RuntimeError, match="authorization_expired"):
        cleanup.validate_authorization(envelope, trust=trust, now=NOW + timedelta(hours=1))
    assert cleanup.validate_authorization(envelope, trust=trust, now=NOW + timedelta(hours=1), claimed_at=NOW)
    with pytest.raises(RuntimeError, match="independent_trust_missing"):
        cleanup.validate_authorization(envelope, trust=None, now=NOW)


@pytest.mark.parametrize("version", [1, 2, 3])
def test_historical_authentication_preserves_raw_history_and_full_ceilings(version):
    """Original signed history accounts ceilings without granting fresh disposal authority."""
    envelope, trust, key = authorization_fixture()
    body = envelope["authorization"]
    body["contract_id"] = f"healthporta.provider-directory.failed-profile-cleanup-authorization.v{version}"
    body["volumes"] = body.pop("storage_classes")
    if version != 1:
        body.update(variant="legacy_full_swap" if version == 2 else "initial_full_swap", physical={})
        body["publication" if version == 2 else "initial"] = {"retained": "opaque signed history"}
    signed = sign(body, key)
    original_bytes = cleanup.canonical(signed)
    original_digest = cleanup.digest(signed)
    trust.keys[0].status = "retired"
    trust.keys[0].retired_at = NOW + timedelta(minutes=1)
    trust.keys[0].verify_until = NOW + timedelta(minutes=20)
    authenticated = cleanup.authenticate_historical_authorization(
        signed, trust=trust, now=NOW + timedelta(days=1), claimed_at=NOW
    )
    assert authenticated == body
    assert authenticated["limits"] == body["limits"]
    assert cleanup.canonical(signed) == original_bytes
    assert cleanup.digest(signed) == original_digest
    with pytest.raises(RuntimeError, match="legacy_semantic_evidence_required$"):
        cleanup.validate_authorization(signed, trust=trust, now=NOW, claimed_at=NOW)
    tampered = deepcopy(signed)
    tampered["authorization"]["limits"]["wal_bytes"] += 1
    with pytest.raises(RuntimeError, match="signature_invalid$"):
        cleanup.authenticate_historical_authorization(
            tampered, trust=trust, now=NOW + timedelta(days=1), claimed_at=NOW
        )


@pytest.mark.parametrize(
    "change", ["purpose", "extra", "stage", "oid", "volume", "freshness", "signature", "deadline", "limit", "trust"]
)
def test_closed_authority_rejects_changed_or_unfunded_operation(change):
    envelope, trust, key = authorization_fixture()
    body = deepcopy(envelope["authorization"])
    if change == "purpose":
        body["purpose"] = "profile"
    if change == "extra":
        body["sql"] = "DROP TABLE arbitrary"
    if change == "stage":
        body["stages"][0]["table_name"] = "arbitrary"
    if change == "oid":
        body["stages"][0]["name"] = body["stages"][1]["name"]
    if change == "volume":
        body["storage_classes"][0]["available_after_all_reservations_bytes"] = 10**10
    if change == "freshness":
        body["observations"]["host_observed_at"] = (NOW - timedelta(seconds=6)).isoformat()
    if change == "deadline":
        body["max_operation_deadline"] = (NOW + timedelta(seconds=121)).isoformat()
    if change == "limit":
        body["limits"]["statement_count"] = 6
    if change == "trust":
        trust.keys[0].status = "revoked"
    envelope = sign(body, key)
    if change == "signature":
        envelope["signature"] = "A" * 86
    with pytest.raises(RuntimeError):
        cleanup.validate_authorization(envelope, trust=trust, now=NOW)


@pytest.mark.parametrize("legacy", [False, True])
def test_stored_cleanup_observations_do_not_reage_initial_admission(legacy):
    factory = legacy_authorization_fixture if legacy else authorization_fixture
    envelope, trust, _key = factory()
    claimed = NOW + timedelta(seconds=30)
    with pytest.raises(RuntimeError, match="observation_stale"):
        cleanup.validate_authorization(envelope, trust=trust, now=claimed)
    assert (
        cleanup.validate_authorization(envelope, trust=trust, now=NOW + timedelta(minutes=3), claimed_at=claimed)
        == envelope["authorization"]
    )


def test_complete_checkpoint_preimage_restores_original_timestamp():
    envelope, _trust, _key = authorization_fixture()
    body = envelope["authorization"]
    original_by_field = {
        "build_id": body["checkpoint"]["build_id"],
        "last_error": "complete original error\nwith detail",
        "updated_at": NOW,
        "capacity_geometry_hash": "a" * 64,
    }
    body["checkpoint"]["preimage_sha256"] = cleanup.digest(
        cleanup.json_values(original_by_field), cleanup.CONTRACT + ".checkpoint"
    )
    import hashlib

    body["checkpoint"]["original_error_sha256"] = hashlib.sha256(original_by_field["last_error"].encode()).hexdigest()
    envelope = sign(body, _key)
    claim_by_field = {
        "operation_id": body["operation_id"],
        "reservation_id": body["reservation_id"],
        "nonce": body["nonce"],
        "build_id": body["checkpoint"]["build_id"],
        "owner_run_id": body["checkpoint"]["owner_run_id"],
        "checkpoint_preimage_sha256": body["checkpoint"]["preimage_sha256"],
        "authorization_sha256": cleanup.digest(envelope),
        "authorization_json": cleanup.canonical(body),
        "signature": envelope["signature"],
        "claimed_at": NOW,
        "expires_at": cleanup.timestamp(body["expires_at"]),
        "max_operation_deadline": cleanup.timestamp(body["max_operation_deadline"]),
    }
    receipt_by_field = {
        "contract_id": cleanup.RECEIPT_CONTRACT,
        "operation_id": body["operation_id"],
        "authorization_sha256": cleanup.digest(envelope),
        "checkpoint_preimage_sha256": body["checkpoint"]["preimage_sha256"],
        "original_updated_at": NOW.isoformat(),
        "original_error_sha256": body["checkpoint"]["original_error_sha256"],
        "disposed_stages": body["stages"],
        "completed_at": (NOW + timedelta(seconds=1)).isoformat(),
        "wal_start_lsn": "0/100",
        "wal_precommit_lsn": "0/200",
        "wal_precommit_bytes": 256,
    }
    completed_by_field = dict(
        original_by_field,
        last_error=original_by_field["last_error"] + cleanup.completion_suffix(receipt_by_field),
        updated_at=NOW + timedelta(seconds=1),
    )
    assert cleanup.validate_completion_preimage(completed_by_field, claim_by_field) == receipt_by_field
    with pytest.raises(RuntimeError, match="preimage_changed"):
        cleanup.validate_completion_preimage(dict(completed_by_field, capacity_geometry_hash="b" * 64), claim_by_field)


@pytest.mark.parametrize("receipt", [None, [], "invalid", 1, True])
@pytest.mark.asyncio
async def test_completion_marker_refuses_non_object_before_claim_lookup(receipt):
    checkpoint_by_field = {"last_error": cleanup.MARKER + cleanup.canonical(receipt) + "]"}
    database = SimpleNamespace(first=AsyncMock())
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_completion_invalid"):
        cleanup.validate_completion_preimage(checkpoint_by_field, {})
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_completion_invalid"):
        await cleanup.validate_disposed_checkpoint(SimpleNamespace(db=database), "synthetic", checkpoint_by_field)
    database.first.assert_not_awaited()


def test_canonical_cross_repository_golden():
    envelope, _trust, _key = authorization_fixture()
    assert cleanup.digest(envelope) == "67075e66d177fb7a75bd2857477c655643c70cc79fb2aa6e0f903968b847ebfd"


@pytest.mark.parametrize("mode", ["execute", "reconcile"])
def test_cli_dispatch_validates_legacy_authority_in_its_own_namespace(monkeypatch, capsys, mode):
    """Load the actual CLI and validate legacy input before substituting database execution."""
    import asyncio
    import runpy
    import sys

    envelope, trust, _key = legacy_authorization_fixture()

    def run_operator(coroutine):
        try:
            namespace = coroutine.cr_frame.f_globals
            arguments = coroutine.cr_frame.f_locals["arguments"]
            body = namespace["validate_authorization"](envelope, trust=trust, now=NOW)
            return {"mode": arguments.mode, "variant": body["variant"]}
        finally:
            coroutine.close()

    monkeypatch.setattr(asyncio, "run", run_operator)
    monkeypatch.setattr(sys, "argv", [cleanup.__file__, mode, "--private-input-file", "synthetic-authority.json"])
    runpy.run_path(cleanup.__file__, run_name="__main__")
    assert cleanup.json.loads(capsys.readouterr().out) == {"mode": mode, "variant": "legacy_full_swap"}


def legacy_authorization_fixture(now=NOW):
    """Bind the two-stage full-swap variant to named history and numeric costs."""
    envelope, trust, key = authorization_fixture(now)
    body = envelope["authorization"]
    body.update(contract_id=cleanup.LEGACY_CONTRACT, variant="legacy_full_swap")
    body["checkpoint"].update(
        lineage_kind="absent",
        owner_params_sha256="f" * 64,
        selection_proof_id=None,
        authority_revision=None,
        control_generation=None,
        capacity_geometry_hash=None,
        executable_plan_hash=None,
    )
    body["stages"] = body["stages"][:2]
    body["limits"]["statement_count"] = 4
    body["capacity"] = {
        "observed_at": now.isoformat(),
        "postgres": _legacy_postgres_fields(trust),
        "checkpoint_layout": {
            "schema": "mrf",
            "name": "provider_directory_profile_build_checkpoint",
            "main_index_pages": [1, 1, 1],
            "toast_index_pages": [1],
            "toastable_column_count": 1,
            "old_toast_chunks": 35,
            "row_payload_upper_bytes": 32000,
        },
        "claim_layout": {
            "schema": "mrf",
            "name": "provider_directory_profile_failed_cleanup_claim",
            "main_index_pages": [1, 1, 1],
            "toast_index_pages": [1],
            "toastable_column_count": 1,
        },
        "stage_dependencies": [
            {"role": role, "object_count": 5, "relation_count": 3, "catalog_tuple_count": 25}
            for role in ("evidence", "profile")
        ],
        "commit_wal_upper_bytes": 10000000,
    }
    _bind_legacy_catalogs(body)
    body["publication"] = _legacy_publication_fixture()
    return sign(body, key), trust, key


def _legacy_publication_fixture():
    """Return the exact named publication history used by current full-swap fixtures."""
    return {
        "serving_state": "empty",
        "relations": {
            role: {"schema": "mrf", "name": name}
            for role, name in (
                ("serving_generation", "provider_directory_profile_serving_generation"),
                ("delta_receipt", "provider_directory_profile_delta_receipt"),
                ("cms_receipt", "provider_directory_cms_serving_receipt"),
            )
        },
        "targets": [{"role": role, "schema": "mrf", "name": "published_" + role} for role in ("evidence", "profile")],
        "published_run_id": "run-published",
        "published_run_preimage_sha256": "d" * 64,
        "published_result_sha256": "e" * 64,
        "published_generation_id": "pdprofile-published",
        "published_proof_id": "f" * 64,
        "published_control_generation": 6,
    }


def test_closed_legacy_has_separate_domain_and_preserves_explicit_absence():
    envelope, trust, _key = legacy_authorization_fixture()
    body = cleanup.validate_authorization(envelope, trust=trust, now=NOW)
    assert body["checkpoint"]["capacity_geometry_hash"] is None
    assert body["checkpoint"]["selection_proof_id"] is None
    assert cleanup.stage_roles(body) == ("evidence", "profile")
    assert cleanup.signature_domain(body) != cleanup.SIGNATURE_DOMAIN
    assert cleanup.digest(envelope) == "55e4e245b5d8545fd6a24e3770f5fef3e0b0f79bb80fc1db2d8f20027991e5ac"


@pytest.mark.parametrize(
    "change",
    [
        "old_v1",
        "extra",
        "partial",
        "affected",
        "target_alias",
        "publication_missing",
        "capacity_stale",
        "unknown_variant",
        "old_toast",
        "row_payload",
        "catalog_missing",
        "catalog_unknown",
        "catalog_total",
        "catalog_large",
        "catalog_allowance",
    ],
)
def test_legacy_closed_authority_refuses_unknown_identity_or_physical_inputs(change):
    envelope, trust, key = legacy_authorization_fixture()
    body = envelope["authorization"]
    if change == "old_v1":
        body["contract_id"] = cleanup.CONTRACT
    if change == "extra":
        body["publication"]["skip_receipts"] = True
    if change == "partial":
        body["checkpoint"]["control_generation"] = 7
    if change == "affected":
        body["stages"].append({"role": "affected_npi", "schema": "mrf", "name": "affected_failed"})
    if change == "target_alias":
        body["stages"][0].update({name: body["publication"]["targets"][0][name] for name in ("schema", "name")})
    if change == "publication_missing":
        body["publication"]["published_run_id"] = None
    if change == "capacity_stale":
        body["capacity"]["observed_at"] = (NOW - timedelta(seconds=6)).isoformat()
    if change == "unknown_variant":
        body["variant"] = "full_swap"
    if change == "old_toast":
        body["capacity"]["checkpoint_layout"]["old_toast_chunks"] = -1
    if change == "row_payload":
        body["capacity"]["checkpoint_layout"]["row_payload_upper_bytes"] = 65537
    dependency = body["capacity"]["stage_dependencies"][0]
    if change == "catalog_missing":
        dependency["catalog_deletions"].pop()
    if change == "catalog_unknown":
        dependency["catalog_deletions"][0]["catalog"] = "pg_unknown"
    if change == "catalog_total":
        dependency["catalog_tuple_count"] += 1
    if change == "catalog_large":
        dependency["catalog_deletions"][0]["wal_upper_bytes"] = 32 * 1024 * 1024 + 1
    if change == "catalog_allowance":
        dependency["drop_wal_upper_bytes"] -= 1
    with pytest.raises(RuntimeError):
        cleanup.validate_authorization(sign(body, key), trust=trust, now=NOW)


def _authorization_body(now, trust, storage_classes):
    """Build the closed named-stage cleanup authorization body."""
    return {
        "contract_id": cleanup.CONTRACT,
        "purpose": "failed_profile_cleanup",
        "authorization_id": "auth-a",
        "operation_id": "operation-a",
        "reservation_id": "reservation-a",
        "nonce": "nonce-a",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=15)).isoformat(),
        "max_operation_deadline": (now + timedelta(seconds=120)).isoformat(),
        "attestor_id": trust.attestor_id,
        "key_id": trust.active_key_id,
        "environment_id": trust.environment_id,
        "attestor_release_digest": "b" * 64,
        "executor_identity": {"runtime_sha256": "c" * 64, "release_digest": "d" * 64},
        "database": {"schema": "mrf"},
        "checkpoint": {
            "build_id": "pdpb_" + "e" * 32,
            "owner_run_id": "run-a",
            "preimage_sha256": "f" * 64,
            "original_error_sha256": "a" * 64,
            "resume_lineage_hash": "b" * 64,
            "capacity_geometry_hash": "c" * 64,
            "executable_plan_hash": "d" * 64,
            "selection_proof_id": "proof-a",
            "authority_revision": 1,
            "control_generation": 2,
        },
        "stages": [
            {"role": role, "schema": "mrf", "name": role + "_failed"}
            for role in ("evidence", "profile", "affected_npi")
        ],
        "observations": {
            **{kind + "_observed_at": now.isoformat() for kind in ("host", "control", "healthcare")},
            **{kind + "_sha256": "f" * 64 for kind in ("host", "control", "healthcare")},
            "accounted_reservation_ids": ["reservation-a"],
        },
        "storage_classes": storage_classes,
        "limits": {
            "data_bytes": 10**6,
            "temp_bytes": 0,
            "wal_bytes": 10**8,
            "statement_count": 5,
            "lock_timeout_ms": 1000,
            "statement_timeout_ms": 30000,
            "max_operation_seconds": 120,
        },
    }


def _legacy_postgres_fields(trust):
    """Return the fixed PostgreSQL settings used by the legacy authority fixture."""
    return {
        "postgres_server_version_num": 180000,
        "postgres_block_size_bytes": 8192,
        "postgres_wal_block_size_bytes": 8192,
        "postgres_wal_segment_size_bytes": 16777216,
        "postgres_full_page_writes": True,
        "postgres_wal_compression": "off",
        "postgres_wal_level": "replica",
        "postgres_wal_log_hints": False,
        "postgres_data_checksums": True,
        "postgres_default_toast_compression": "pglz",
        "postgres_checkpoint_timeout_seconds": 300,
        "postgres_max_wal_size_bytes": 1073741824,
        "postgres_toast_max_chunk_size_bytes": 1996,
        "postgres_maxalign_bytes": 8,
        "postgres_btree_version": 4,
    }


def _bind_legacy_catalogs(body):
    """Bind the original per-catalog deletion counts and WAL ceilings."""
    catalogs = (
        "pg_class",
        "pg_attribute",
        "pg_index",
        "pg_type",
        "pg_constraint",
        "pg_attrdef",
        "pg_depend",
        "pg_shdepend",
        "pg_description",
        "pg_seclabel",
        "pg_init_privs",
        "pg_statistic",
    )
    for dependency in body["capacity"]["stage_dependencies"]:
        dependency["catalog_deletions"] = [
            {
                "catalog": name,
                "schema": "pg_catalog",
                "name": name,
                "deleted_rows": 2,
                "deleted_toast_chunks": 3 if name == "pg_description" else 0,
                "wal_upper_bytes": 100000,
            }
            for position, name in enumerate(catalogs)
        ]
        dependency["catalog_tuple_count"] = 24
        dependency["drop_wal_upper_bytes"] = 32 * 1024 * 1024 + 1200000


@pytest.mark.parametrize(
    "legacy,kind",
    [(legacy, kind) for legacy in (False, True) for kind in ("host", "control", "healthcare")] + [(True, "capacity")],
)
@pytest.mark.parametrize("is_stored", [False, True])
def test_observations_cannot_postdate_signed_issuance(legacy, kind, is_stored):
    """Reject future signed observations before admission and during stored reconciliation."""
    factory = legacy_authorization_fixture if legacy else authorization_fixture
    envelope, trust, key = factory()
    body = envelope["authorization"]
    target = body["capacity"] if kind == "capacity" else body["observations"]
    field = "observed_at" if kind == "capacity" else kind + "_observed_at"
    target[field] = (NOW + timedelta(seconds=1)).isoformat()
    envelope = sign(body, key)
    admission_at = NOW + timedelta(seconds=2)
    reason = "capacity_observation_stale" if kind == "capacity" else "observation_stale"
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        cleanup.validate_authorization(
            envelope,
            trust=trust,
            now=NOW + timedelta(minutes=3) if is_stored else admission_at,
            claimed_at=admission_at if is_stored else None,
        )


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize(
    "observed_offset,admission_offset,accepted",
    [(-5, 0, True), (-5.000001, 0, False), (-3, 2, True), (-3.000001, 2, False), (0, 5, True), (0, 5.000001, False)],
)
def test_observation_windows_keep_inclusive_microsecond_bounds(legacy, observed_offset, admission_offset, accepted):
    """Keep both windows inclusive without re-aging durable claim observations."""
    factory = legacy_authorization_fixture if legacy else authorization_fixture
    envelope, trust, key = factory()
    body = envelope["authorization"]
    observed_at = (NOW + timedelta(seconds=observed_offset)).isoformat()
    for kind in ("host", "control", "healthcare"):
        body["observations"][kind + "_observed_at"] = observed_at
    if legacy:
        body["capacity"]["observed_at"] = observed_at
    envelope = sign(body, key)
    admission_at = NOW + timedelta(seconds=admission_offset)
    if accepted:
        assert cleanup.validate_authorization(envelope, trust=trust, now=admission_at) == body
    else:
        with pytest.raises(RuntimeError, match="observation_stale"):
            cleanup.validate_authorization(envelope, trust=trust, now=admission_at)
    if -5 <= observed_offset <= 0:
        assert (
            cleanup.validate_authorization(
                envelope, trust=trust, now=NOW + timedelta(minutes=3), claimed_at=NOW + timedelta(seconds=30)
            )
            == body
        )
    else:
        with pytest.raises(RuntimeError, match="observation_stale"):
            cleanup.validate_authorization(envelope, trust=trust, now=NOW + timedelta(minutes=3), claimed_at=NOW)


@pytest.mark.parametrize("variant", ["source_delta", "full_swap", "", None])
@pytest.mark.parametrize("is_stored", [False, True])
def test_legacy_contract_refuses_signed_source_delta_coordinates(variant, is_stored):
    """A v2 signature cannot turn v1 coordinates into a mismatched completion receipt."""
    envelope, trust, key = authorization_fixture()
    legacy, _trust, _key = legacy_authorization_fixture()
    body = envelope["authorization"]
    body.update(
        contract_id=cleanup.LEGACY_CONTRACT,
        variant=variant,
        capacity=legacy["authorization"]["capacity"],
        publication=legacy["authorization"]["publication"],
    )
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_variant_invalid"):
        cleanup.validate_authorization(sign(body, key), trust=trust, now=NOW, claimed_at=NOW if is_stored else None)


def initial_authorization_fixture(now=NOW, *, with_geometry=False):
    """Bind current initial geometry to independently funded named cleanup."""
    from process import provider_directory_profile_capacity as capacity
    from process import provider_directory_profile_initial_contract as initial
    from tests.test_provider_directory_profile_capacity import _geometry_payload

    envelope, trust, key = authorization_fixture(now)
    capacity_snapshot = legacy_authorization_fixture(now)[0]["authorization"]["capacity"]
    target_by_field = _initial_target()
    geometry = _geometry_payload(
        contract_id=initial.GEOMETRY_CONTRACT,
        materialization_mode="full_swap",
        physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID,
        current_source_vector_hash=None,
        current_context_vector_hash=None,
        initial_target_state_sha256=initial.target_state_sha256(target_by_field),
    )
    geometry.update(capacity_snapshot["postgres"])
    for role in ("evidence", "profile"):
        geometry["relations"][role + "_target"].update(target_by_field["targets"][role])
    geometry["relations"]["initial_receipt"] = {
        "schema": "mrf",
        "name": initial.RECEIPT_TABLE,
        "main_index_pages": [1],
        "toast_index_pages": [1],
        "toastable_column_count": 1,
    }
    body = _initial_authorization_body(envelope["authorization"], geometry, target_by_field, capacity_snapshot)
    body["checkpoint"]["capacity_geometry_hash"] = capacity.capacity_geometry_hash(
        capacity.validated_capacity_geometry(geometry)
    )
    cleanup._validate_initial_request(dict(body, capacity_geometry=geometry))
    signed = sign(body, key)
    return (signed, trust, key, geometry) if with_geometry else (signed, trust, key)


def _initial_target():
    from process import provider_directory_profile_initial_contract as initial

    return {
        "contract_id": initial.TARGET_CONTRACT,
        "resolution": "empty",
        "serving_singleton_absent": True,
        "initial_commit_receipt_absent": True,
        "targets": {
            "evidence": {"schema": "mrf", "name": "provider_directory_profile_evidence"},
            "profile": {"schema": "mrf", "name": "provider_directory_profile"},
        },
        "evidence_target_bytes": 0,
        "profile_target_bytes": 0,
        "evidence_rows": 0,
        "profile_rows": 0,
        "historical_publication": None,
    }


def _initial_authorization_body(body, geometry, target, capacity_snapshot):
    """Keep source/plan/target names bound to the original logical geometry."""
    from process import provider_directory_profile_initial_contract as initial

    body.update(
        contract_id=cleanup.INITIAL_CONTRACT,
        variant="initial_full_swap",
        capacity=capacity_snapshot,
        initial={
            "target_state": target,
            "initial_target_state_sha256": geometry["initial_target_state_sha256"],
            "receipt": {"schema": "mrf", "name": initial.RECEIPT_TABLE},
        },
    )
    body["stages"] = body["stages"][:2]
    body["limits"]["statement_count"] = 4
    body["checkpoint"].update(
        selection_proof_id=geometry["selection_proof_id"], executable_plan_hash=geometry["executable_plan_hash"]
    )
    capacity_snapshot["checkpoint_layout"].update(
        {name: geometry["relations"]["build_checkpoint"][name] for name in ("schema", "name")}
    )
    return body


def test_initial_cleanup_has_closed_verified_geometry_and_two_stage_authority():
    envelope, trust, _key = initial_authorization_fixture()
    body = cleanup.validate_authorization(envelope, trust=trust, now=NOW)
    assert body["contract_id"] == cleanup.INITIAL_CONTRACT
    assert cleanup.stage_roles(body) == ("evidence", "profile")
    assert body["initial"]["target_state"]["resolution"] == "empty"
    assert body["checkpoint"]["selection_proof_id"]
    assert body["checkpoint"]["capacity_geometry_hash"]
    assert cleanup.signature_domain(body) not in {cleanup.SIGNATURE_DOMAIN, cleanup.LEGACY_CONTRACT + ".signature"}


@pytest.mark.parametrize("change", ["variant", "old_version", "target", "alias", "stale", "future", "third_stage"])
def test_initial_cleanup_refuses_mixed_or_changed_original_authority(change):
    envelope, trust, key = initial_authorization_fixture()
    body = envelope["authorization"]
    match change:
        case "variant":
            body["variant"] = "legacy_full_swap"
        case "old_version":
            body["contract_id"] = cleanup.LEGACY_CONTRACT
        case "target":
            body["initial"]["target_state"]["targets"]["profile"]["name"] = "other_profile"
        case "alias":
            body["stages"][0].update(body["initial"]["target_state"]["targets"]["evidence"])
        case "stale":
            body["capacity"]["observed_at"] = (NOW - timedelta(seconds=6)).isoformat()
        case "future":
            body["capacity"]["observed_at"] = (NOW + timedelta(seconds=1)).isoformat()
        case "third_stage":
            body["stages"].append({"role": "affected_npi", "schema": "mrf", "name": "affected_failed"})
    with pytest.raises((RuntimeError, ValueError)):
        cleanup.validate_authorization(sign(body, key), trust=trust, now=NOW)


def historical_trust_document(trust):
    """Serialize the original public-only trust set for independently pinned retention."""
    return {
        "contract_id": "provider-directory-database-capacity-trust-v3",
        "signature_algorithm": "Ed25519",
        **{
            name: getattr(trust, name)
            for name in (
                "attestor_id",
                "environment_id",
                "active_key_id",
            )
        },
        "keys": [
            {
                "key_id": key.key_id,
                "public_key_hex": key.public_key.hex(),
                "attestor_release_digest": key.attestor_release_digest,
                "status": key.status,
                "retired_at": key.retired_at.isoformat().replace("+00:00", "Z") if key.retired_at else None,
                "verify_until": key.verify_until.isoformat().replace("+00:00", "Z") if key.verify_until else None,
            }
            for key in trust.keys
        ],
    }


@pytest.fixture
def cleanup_history_directory():
    """Keep valid protected files outside the repository and system temporary roots."""
    with TemporaryDirectory(prefix="profile-cleanup-history-", dir=Path.home()) as directory:
        yield Path(directory)


def install_historical_trust(monkeypatch, directory, documents):
    """Use the real protected loader and a separately configured content pin."""
    path = directory / "cleanup-history.json"
    raw = cleanup.canonical(
        {"contract_id": "healthporta.provider-directory.failed-profile-cleanup-trust-history.v1", "epochs": documents}
    ).encode()
    path.write_bytes(raw)
    path.chmod(0o600)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_HISTORY_FILE", str(path))
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_HISTORY_SHA256",
        cleanup.hashlib.sha256(raw).hexdigest(),
    )
    return path


def _completed_history_fixture():
    envelope, trust, key = authorization_fixture()
    body = envelope["authorization"]
    checkpoint_by_field = {
        "build_id": body["checkpoint"]["build_id"],
        "last_error": "original failure",
        "updated_at": NOW,
    }
    body["checkpoint"]["preimage_sha256"] = cleanup.digest(
        cleanup.json_values(checkpoint_by_field), cleanup.CONTRACT + ".checkpoint"
    )
    body["checkpoint"]["original_error_sha256"] = cleanup.hashlib.sha256(
        checkpoint_by_field["last_error"].encode()
    ).hexdigest()
    envelope = sign(body, key)
    claim_by_field = {
        "operation_id": body["operation_id"],
        "reservation_id": body["reservation_id"],
        "nonce": body["nonce"],
        "build_id": body["checkpoint"]["build_id"],
        "owner_run_id": body["checkpoint"]["owner_run_id"],
        "checkpoint_preimage_sha256": body["checkpoint"]["preimage_sha256"],
        "authorization_sha256": cleanup.digest(envelope),
        "authorization_json": cleanup.canonical(body),
        "signature": envelope["signature"],
        "claimed_at": NOW,
        "expires_at": cleanup.timestamp(body["expires_at"]),
        "max_operation_deadline": cleanup.timestamp(body["max_operation_deadline"]),
    }
    receipt_by_field = {
        "contract_id": cleanup.RECEIPT_CONTRACT,
        "operation_id": body["operation_id"],
        "authorization_sha256": cleanup.digest(envelope),
        "checkpoint_preimage_sha256": body["checkpoint"]["preimage_sha256"],
        "original_updated_at": NOW.isoformat(),
        "original_error_sha256": body["checkpoint"]["original_error_sha256"],
        "disposed_stages": body["stages"],
        "completed_at": (NOW + timedelta(seconds=1)).isoformat(),
        "wal_start_lsn": "0/100",
        "wal_precommit_lsn": "0/200",
        "wal_precommit_bytes": 256,
    }
    checkpoint_by_field.update(
        last_error=checkpoint_by_field["last_error"] + cleanup.completion_suffix(receipt_by_field),
        updated_at=NOW + timedelta(seconds=1),
    )
    return envelope, trust, checkpoint_by_field, claim_by_field, receipt_by_field


def _completed_history_database(body, *, stage_present=False, database_changed=False):
    return SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(return_value=stage_present)),
        _unscoped_qt=lambda schema, name: f'"{schema}"."{name}"',
        _validated_profile_checkpoint_stage_names=lambda _checkpoint: (
            "evidence_stage",
            "profile_stage",
            "affected_stage",
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["removed", "retired", "volumes"])
async def test_completed_history_uses_pinned_original_trust_without_live_authority(
    monkeypatch, cleanup_history_directory, change
):
    envelope, trust, checkpoint, claim, receipt = _completed_history_fixture()
    original = historical_trust_document(trust)
    install_historical_trust(monkeypatch, cleanup_history_directory, [original])
    match change:
        case "removed":
            trust.keys = ()
        case "retired":
            trust.keys[0].status = "retired"
            trust.keys[0].retired_at = NOW + timedelta(minutes=1)
            trust.keys[0].verify_until = NOW + timedelta(minutes=16)
        case "volumes":
            trust.environment_id = "other-environment"
    monkeypatch.setattr(cleanup, "_now_utc", lambda: NOW + timedelta(hours=1))
    with pytest.raises(RuntimeError):
        cleanup.validate_authorization(envelope, trust=trust, now=cleanup._now_utc(), claimed_at=claim["claimed_at"])
    observed = await cleanup._completed_claim_receipt(
        _completed_history_database(envelope["authorization"]), "mrf", checkpoint, claim, trust
    )
    assert observed == receipt
    with pytest.raises(RuntimeError):
        cleanup.validate_authorization(envelope, trust=trust, now=NOW)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,reason",
    [
        ("incomplete", "completion_missing"),
        ("preimage", "completion_preimage_changed"),
        ("claim", "claim_corrupt"),
        ("stage", "disposed_stage_present"),
        ("database", "database_changed"),
        ("pin", "history_trust_digest_changed"),
        ("signature", "history_trust_epoch_invalid"),
        ("epoch", "history_trust_epoch_invalid"),
        ("duplicate", "history_trust_epochs_invalid"),
        ("absent", "history_trust_missing"),
    ],
)
async def test_historical_archive_never_replaces_exact_completion_or_authentic_epoch(
    monkeypatch, cleanup_history_directory, change, reason
):
    envelope, trust, checkpoint, claim, _receipt = _completed_history_fixture()
    original = historical_trust_document(trust)
    documents = [original]
    if change == "epoch":
        original["keys"][0]["public_key_hex"] = "0" * 64
    if change == "duplicate":
        documents.append(deepcopy(original))
    install_historical_trust(monkeypatch, cleanup_history_directory, documents)
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_TRUST_FILE", raising=False)
    match change:
        case "incomplete":
            checkpoint["last_error"] = "original failure"
        case "preimage":
            checkpoint["unexpected"] = True
        case "claim":
            claim["nonce"] = "different"
        case "pin":
            monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_HISTORY_SHA256", "0" * 64)
        case "signature":
            # Even a self-consistent row and receipt cannot manufacture an authentic signature.
            envelope["signature"] = "A" * 86
            claim.update(signature=envelope["signature"], authorization_sha256=cleanup.digest(envelope))
            _receipt["authorization_sha256"] = cleanup.digest(envelope)
            checkpoint["last_error"] = "original failure" + cleanup.completion_suffix(_receipt)
        case "absent":
            monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_HISTORY_FILE")
    database = _completed_history_database(
        envelope["authorization"], stage_present=change == "stage", database_changed=change == "database"
    )
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        await cleanup._completed_claim_receipt(
            database, "other" if change == "database" else "mrf", checkpoint, claim, None
        )


@pytest.mark.parametrize("change", ["geometry_hash", "geometry", "target", "receipt", "proof", "physical"])
def test_initial_request_revalidates_full_original_geometry_before_signing(change):
    envelope, _trust, _key, geometry = initial_authorization_fixture(with_geometry=True)
    body = envelope["authorization"]
    match change:
        case "geometry_hash":
            body["checkpoint"]["capacity_geometry_hash"] = "0" * 64
        case "geometry":
            geometry["relations"]["initial_receipt"]["name"] = "other_receipt"
        case "target":
            body["initial"]["initial_target_state_sha256"] = "0" * 64
        case "receipt":
            body["initial"]["receipt"]["name"] = "other_receipt"
        case "proof":
            body["checkpoint"]["selection_proof_id"] = "different-proof"
        case "physical":
            body["capacity"]["postgres"]["postgres_wal_level"] = "minimal"
    with pytest.raises((RuntimeError, ValueError)):
        cleanup._validate_initial_request(dict(body, capacity_geometry=geometry))


@pytest.mark.parametrize(
    "change", ["missing_pin", "too_many", "private_key", "contract", "fields", "permissions", "oversize", "symlink"]
)
def test_history_archive_requires_bounded_public_only_protected_file(monkeypatch, cleanup_history_directory, change):
    """Archive configuration adds no permissive file, field or key defaults."""
    original = historical_trust_document(authorization_fixture()[1])
    path = install_historical_trust(monkeypatch, cleanup_history_directory, [original])
    assert len(cleanup._configured_cleanup_history()) == 1
    document = cleanup.json.loads(path.read_bytes())
    match change:
        case "missing_pin":
            monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_HISTORY_SHA256")
        case "too_many":
            document["epochs"] *= 17
        case "private_key":
            document["epochs"][0]["private_key"] = "refused"
        case "contract":
            document["contract_id"] = "unknown"
        case "fields":
            document["unknown"] = True
        case "permissions":
            path.chmod(0o644)
        case "oversize":
            path.write_bytes(path.read_bytes() + b" " * (256 * 1024))
        case "symlink":
            alias = cleanup_history_directory / "history-link.json"
            alias.symlink_to(path)
            monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_HISTORY_FILE", str(alias))
    if change in {"too_many", "private_key", "contract", "fields"}:
        path.write_text(cleanup.canonical(document))
        monkeypatch.setenv(
            "HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_HISTORY_SHA256",
            cleanup.hashlib.sha256(path.read_bytes()).hexdigest(),
        )
    with pytest.raises((RuntimeError, ValueError)):
        cleanup._configured_cleanup_history()


@pytest.mark.parametrize("directory_root", ["/tmp", "/var/tmp"])
def test_history_archive_rejects_system_temporary_roots(monkeypatch, directory_root):
    """A valid signature and file mode never make an OS temporary path acceptable."""
    with TemporaryDirectory(prefix="profile-cleanup-history-", dir=directory_root) as directory:
        install_historical_trust(monkeypatch, Path(directory), [historical_trust_document(authorization_fixture()[1])])
        with pytest.raises(RuntimeError, match="failed_profile_cleanup_private_path_invalid"):
            cleanup._configured_cleanup_history()


def install_current_trust(monkeypatch, directory, document):
    """Configure a real independently pinned current trust file for loader regressions."""
    path = directory / "cleanup-current.json"
    raw = cleanup.canonical(document).encode()
    path.write_bytes(raw)
    path.chmod(0o600)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_TRUST_FILE", str(path))
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_TRUST_SHA256",
        cleanup.hashlib.sha256(raw).hexdigest(),
    )
    return path


def overlapping_historical_trust(trust, claimed_at):
    """Two original public epochs authenticate the same claim across genuine key rotation."""
    original = historical_trust_document(trust)
    rotated = deepcopy(original)
    new_key = Ed25519PrivateKey.from_private_bytes(bytes(reversed(range(32))))
    rotated["keys"].append(
        dict(
            original["keys"][0],
            key_id="key-b",
            public_key_hex=new_key.public_key()
            .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
            .hex(),
        )
    )
    retired_at = (claimed_at + timedelta(minutes=1)).replace(microsecond=0)
    rotated["keys"][0].update(
        status="retired",
        retired_at=retired_at.isoformat().replace("+00:00", "Z"),
        verify_until=(retired_at + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
    )
    rotated["active_key_id"] = "key-b"
    return [original, rotated]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,error_type,reason",
    [
        ("configuration", RuntimeError, "independent_trust_missing"),
        ("pin", RuntimeError, "independent_trust_digest_changed"),
        ("path", RuntimeError, "private_path_invalid"),
        ("mode", RuntimeError, "private_file_invalid"),
        ("duplicate", RuntimeError, "json_duplicate_field"),
        ("missing", FileNotFoundError, None),
        ("oserror", OSError, None),
        ("json", ValueError, None),
        ("typed", ValueError, "trust_fields_invalid"),
    ],
)
async def test_completed_history_survives_current_trust_load_failure(
    monkeypatch, cleanup_history_directory, change, error_type, reason
):
    envelope, trust, checkpoint, claim, receipt = _completed_history_fixture()
    original = historical_trust_document(trust)
    install_historical_trust(monkeypatch, cleanup_history_directory, [original])
    path = install_current_trust(monkeypatch, cleanup_history_directory, original)
    match change:
        case "configuration":
            monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_TRUST_FILE")
        case "pin":
            monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_TRUST_SHA256", "0" * 64)
        case "path" | "oserror":
            missing_path = "relative.json" if change == "path" else str(path.parent / ("x" * 256))
            monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_TRUST_FILE", missing_path)
        case "mode":
            path.chmod(0o644)
        case "duplicate":
            path.write_text('{"contract_id":1,"contract_id":2}')
        case "missing":
            path.unlink()
        case "json":
            path.write_text("{")
        case "typed":
            install_current_trust(monkeypatch, cleanup_history_directory, {})
    with pytest.raises(error_type, match=reason):
        cleanup.configured_cleanup_trust()
    monkeypatch.setattr(cleanup, "_now_utc", lambda: NOW + timedelta(hours=1))
    assert (
        await cleanup._completed_claim_receipt(
            _completed_history_database(envelope["authorization"]), "mrf", checkpoint, claim, None
        )
        == receipt
    )


@pytest.mark.asyncio
async def test_completed_history_accepts_overlapping_authentic_epochs(monkeypatch, cleanup_history_directory):
    envelope, trust, checkpoint, claim, receipt = _completed_history_fixture()
    documents = overlapping_historical_trust(trust, claim["claimed_at"])
    install_historical_trust(monkeypatch, cleanup_history_directory, documents)
    epochs = cleanup._configured_cleanup_history()
    assert len(epochs) == 2
    for epoch in epochs:
        assert (
            cleanup.validate_authorization(
                envelope, trust=epoch, now=claim["claimed_at"], claimed_at=claim["claimed_at"]
            )
            == envelope["authorization"]
        )
    monkeypatch.setattr(cleanup, "_now_utc", lambda: NOW + timedelta(hours=1))
    with pytest.raises(RuntimeError, match="trust_key_retired_invalid"):
        cleanup.validate_authorization(
            envelope, trust=epochs[-1], now=cleanup._now_utc(), claimed_at=claim["claimed_at"]
        )
    assert (
        await cleanup._completed_claim_receipt(
            _completed_history_database(envelope["authorization"]), "mrf", checkpoint, claim, epochs[-1]
        )
        == receipt
    )


@pytest.mark.parametrize("change,reason", [("claim", "authorization_expired"), ("observation", "observation_stale")])
def test_completed_history_preserves_authorization_refusal_reason(monkeypatch, change, reason):
    envelope, _trust, _checkpoint, claim, _receipt = _completed_history_fixture()
    if change == "claim":
        claim["claimed_at"] -= timedelta(seconds=1)
    else:
        envelope["authorization"]["observations"]["host_observed_at"] = (NOW + timedelta(seconds=1)).isoformat()
        envelope = sign(envelope["authorization"], authorization_fixture()[2])
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_TRUST_FILE", raising=False)
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_HISTORY_FILE", raising=False)
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        cleanup._validate_completed_authorization(envelope, claim, None)


def test_protected_history_allows_read_access_time_update(monkeypatch, cleanup_history_directory):
    """The actual read may change atime without changing any protected file identity."""
    original = historical_trust_document(authorization_fixture()[1])
    path = install_historical_trust(monkeypatch, cleanup_history_directory, [original])
    os.utime(path, ns=(0, path.stat().st_mtime_ns))
    before = path.stat()
    assert len(cleanup._configured_cleanup_history()) == 1
    after = path.stat()
    assert cleanup._file_identity(before) == cleanup._file_identity(after)
    assert cleanup.json.loads(path.read_bytes())["epochs"] == [original]


@pytest.mark.parametrize("change", ["content", "mode", "inode"])
def test_protected_history_refuses_mutation_during_read(monkeypatch, cleanup_history_directory, change):
    """Only reader access time is excluded; real protected-file changes still refuse."""
    path = install_historical_trust(
        monkeypatch, cleanup_history_directory, [historical_trust_document(authorization_fixture()[1])]
    )
    read = os.read

    def changed_read(descriptor, size):
        raw = read(descriptor, size)
        match change:
            case "content":
                path.write_bytes(raw + b" ")
            case "mode":
                path.chmod(0o644)
            case "inode":
                replacement = path.with_name("replacement.json")
                replacement.write_bytes(raw)
                replacement.chmod(0o600)
                replacement.replace(path)
        return raw

    monkeypatch.setattr(os, "read", changed_read)
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_private_file_changed"):
        cleanup._configured_cleanup_history()
