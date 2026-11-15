# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed, domain-separated authority for one failed Profile stage disposal."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

CONTRACT = "healthporta.provider-directory.failed-profile-cleanup-authorization.v4"
LEGACY_CONTRACT = "healthporta.provider-directory.failed-profile-cleanup-authorization.v5"
LEGACY_RECEIPT_CONTRACT = "healthporta.provider-directory.failed-profile-cleanup-receipt.v5"
INITIAL_CONTRACT = "healthporta.provider-directory.failed-profile-cleanup-authorization.v6"
INITIAL_RECEIPT_CONTRACT = "healthporta.provider-directory.failed-profile-cleanup-receipt.v6"
SIGNATURE_DOMAIN = CONTRACT + ".signature"
DIGEST_DOMAIN = CONTRACT + ".digest"
RECEIPT_CONTRACT = "healthporta.provider-directory.failed-profile-cleanup-receipt.v4"
MARKER = "\n[provider_directory_failed_profile_cleanup_v1 "
MAX_BYTES = 64 * 1024
BODY_FIELDS = frozenset(
    "contract_id purpose authorization_id operation_id reservation_id nonce issued_at expires_at max_operation_deadline attestor_id key_id environment_id attestor_release_digest executor_identity database checkpoint stages observations storage_classes limits".split()
)
DATABASE_FIELDS = frozenset({"schema"})
CHECKPOINT_FIELDS = frozenset(
    "build_id owner_run_id preimage_sha256 original_error_sha256 resume_lineage_hash capacity_geometry_hash executable_plan_hash selection_proof_id authority_revision control_generation".split()
)
OBSERVATION_FIELDS = frozenset(
    "host_observed_at host_sha256 control_observed_at control_sha256 healthcare_observed_at healthcare_sha256 accounted_reservation_ids".split()
)
LIMIT_FIELDS = frozenset(
    "data_bytes temp_bytes wal_bytes statement_count statement_timeout_ms lock_timeout_ms max_operation_seconds".split()
)
RECEIPT_FIELDS = frozenset(
    "contract_id operation_id authorization_sha256 checkpoint_preimage_sha256 original_updated_at original_error_sha256 disposed_stages completed_at wal_start_lsn wal_precommit_lsn wal_precommit_bytes".split()
)


LEGACY_BODY_FIELDS = BODY_FIELDS | {"variant", "capacity", "publication"}
INITIAL_BODY_FIELDS = BODY_FIELDS | {"variant", "capacity", "initial"}
LEGACY_CHECKPOINT_FIELDS = CHECKPOINT_FIELDS | {"lineage_kind", "owner_params_sha256"}
POSTGRES_FIELDS = frozenset(
    "postgres_server_version_num postgres_block_size_bytes postgres_wal_block_size_bytes postgres_wal_segment_size_bytes postgres_full_page_writes postgres_wal_compression postgres_wal_level postgres_wal_log_hints postgres_data_checksums postgres_default_toast_compression postgres_checkpoint_timeout_seconds postgres_max_wal_size_bytes postgres_toast_max_chunk_size_bytes postgres_maxalign_bytes postgres_btree_version".split()
)
PUBLICATION_FIELDS = frozenset(
    "serving_state relations targets published_run_id published_run_preimage_sha256 published_result_sha256 published_generation_id published_proof_id published_control_generation".split()
)


HISTORICAL_CONTRACTS = frozenset(
    f"healthporta.provider-directory.failed-profile-cleanup-authorization.v{version}" for version in (1, 2, 3)
)
_HISTORICAL_BASE_FIELDS = (BODY_FIELDS - {"storage_classes"}) | {"volumes"}
_HISTORICAL_FIELDS = {
    "healthporta.provider-directory.failed-profile-cleanup-authorization.v1": _HISTORICAL_BASE_FIELDS,
    "healthporta.provider-directory.failed-profile-cleanup-authorization.v2": _HISTORICAL_BASE_FIELDS
    | {"variant", "physical", "publication"},
    "healthporta.provider-directory.failed-profile-cleanup-authorization.v3": _HISTORICAL_BASE_FIELDS
    | {"variant", "physical", "initial"},
}


def signature_domain(body):
    """Keep each version in its own Ed25519 signature domain."""
    return body["contract_id"] + ".signature"


def stage_roles(body):
    """Return the closed ordered roles for the authenticated variant."""
    return (
        ("evidence", "profile")
        if body.get("variant") in {"legacy_full_swap", "initial_full_swap"}
        else ("evidence", "profile", "affected_npi")
    )


def _now_utc():
    return datetime.now(timezone.utc)


def fail(reason):
    """Emit a bounded diagnostic without signed or database contents."""
    raise RuntimeError("failed_profile_cleanup_" + reason)


def canonical(value):
    """Use identical UTF-8 canonical bytes across issuing and executing repositories."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value, domain=DIGEST_DOMAIN):
    """Bind canonical values to this operation's independent hash domain."""
    if domain == DIGEST_DOMAIN and isinstance(value, dict) and isinstance(value.get("authorization"), dict):
        domain = value["authorization"]["contract_id"] + ".digest"
    return hashlib.sha256((domain + "\n" + canonical(value)).encode()).hexdigest()


def exact(value, fields):
    """Reject unknown fields rather than granting accidental future authority."""
    if not isinstance(value, dict) or set(value) != fields:
        fail("fields_invalid")
    return value


def timestamp(value):
    """Keep aware database timestamp precision in stored receipts and claims."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except AttributeError, TypeError, ValueError:
        fail("timestamp_invalid")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        fail("timestamp_invalid")
    return parsed.astimezone(timezone.utc)


def text(value, maximum=64):
    """Require a nonempty bounded opaque identity."""
    if (
        not isinstance(value, str)
        or not 0 < len(value) <= maximum
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value) is None
    ):
        fail("identity_invalid")
    return value


def hash64(value):
    """Require a canonical SHA-256 identity."""
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        fail("digest_invalid")
    return value


def integer(value, minimum=0, maximum=(1 << 63) - 1):
    """Reject booleans, negative ceilings and overflowing counters."""
    if type(value) is not int or not minimum <= value <= maximum:
        fail("integer_invalid")
    return value


def _validate_coordinates(body):
    database = exact(body["database"], DATABASE_FIELDS)
    text(database["schema"], 63)
    if body.get("variant") == "legacy_full_swap":
        _validate_legacy_coordinates(body)
        return
    checkpoint = exact(body["checkpoint"], CHECKPOINT_FIELDS)
    if (
        not isinstance(checkpoint["build_id"], str)
        or re.fullmatch(r"pdpb_[0-9a-f]{32}", checkpoint["build_id"]) is None
    ):
        fail("build_invalid")
    for field in ("owner_run_id", "selection_proof_id"):
        text(checkpoint[field])
    for field in (
        "preimage_sha256",
        "original_error_sha256",
        "resume_lineage_hash",
        "capacity_geometry_hash",
        "executable_plan_hash",
    ):
        hash64(checkpoint[field])
    integer(checkpoint["authority_revision"], 1)
    integer(checkpoint["control_generation"], 1)
    exact(body["executor_identity"], {"runtime_sha256", "release_digest"})
    for field_value in body["executor_identity"].values():
        hash64(field_value)


def _validate_legacy_coordinates(body):
    if body["variant"] != "legacy_full_swap":
        fail("variant_invalid")
    checkpoint = exact(body["checkpoint"], LEGACY_CHECKPOINT_FIELDS)
    if re.fullmatch(r"pdpb_[0-9a-f]{32}", checkpoint["build_id"]) is None:
        fail("build_invalid")
    text(checkpoint["owner_run_id"])
    for field in ("preimage_sha256", "original_error_sha256", "owner_params_sha256"):
        hash64(checkpoint[field])
    for field in ("resume_lineage_hash", "capacity_geometry_hash", "executable_plan_hash"):
        if checkpoint[field] is not None:
            hash64(checkpoint[field])
    lineage_values = tuple(
        checkpoint[field] for field in ("selection_proof_id", "authority_revision", "control_generation")
    )
    if checkpoint["lineage_kind"] == "absent":
        if lineage_values != (None, None, None):
            fail("legacy_lineage_partial")
    elif checkpoint["lineage_kind"] == "complete" and all(value is not None for value in lineage_values):
        text(lineage_values[0])
        integer(lineage_values[1], 1)
        integer(lineage_values[2], 1)
    else:
        fail("legacy_lineage_partial")
    exact(body["executor_identity"], {"runtime_sha256", "release_digest"})
    for value in body["executor_identity"].values():
        hash64(value)


def _validate_legacy_manifests(body):
    """Validate named publication data and numeric disposal limits."""
    _validate_capacity_dependencies(body)
    publication = exact(body["publication"], PUBLICATION_FIELDS)
    if publication["serving_state"] not in {"empty", "singleton"}:
        fail("publication_invalid")
    relations = exact(publication["relations"], {"serving_generation", "delta_receipt", "cms_receipt"})
    for coordinate in relations.values():
        _named_coordinate(coordinate)
    for field in ("published_run_id", "published_generation_id", "published_proof_id"):
        text(publication[field], 128)
    for field in ("published_run_preimage_sha256", "published_result_sha256"):
        hash64(publication[field])
    integer(publication["published_control_generation"], 1)
    _validate_named_stages(publication["targets"], ("evidence", "profile"))
    if {_coordinate_key(stage) for stage in body["stages"]} & {
        _coordinate_key(target) for target in publication["targets"]
    }:
        fail("stage_is_serving")


def _validate_capacity_dependencies(body):
    """Check bounded native catalog deletion counters without dependency identity."""
    capacity = _validated_cleanup_capacity(body)
    dependencies = capacity["stage_dependencies"]
    if not isinstance(dependencies, list) or len(dependencies) != 2:
        fail("dependencies_invalid")
    for entry, role in zip(dependencies, ("evidence", "profile"), strict=True):
        exact(
            entry,
            {
                "role",
                "object_count",
                "relation_count",
                "catalog_tuple_count",
                "catalog_deletions",
                "drop_wal_upper_bytes",
            },
        )
        if entry["role"] != role:
            fail("dependencies_invalid")
        integer(entry["object_count"], 1, 128)
        integer(entry["relation_count"], 1, 32)
        integer(entry["catalog_tuple_count"], 1, 8192)
    _validate_catalog_deletions(dependencies)


def _validate_catalog_deletions(dependencies):
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
    for dependency in dependencies:
        deletions = dependency["catalog_deletions"]
        if not isinstance(deletions, list) or len(deletions) != len(catalogs):
            fail("catalog_deletions_invalid")
        for deletion, catalog in zip(deletions, catalogs, strict=True):
            exact(
                deletion,
                {"catalog", "schema", "name", "deleted_rows", "deleted_toast_chunks", "wal_upper_bytes"},
            )
            if deletion["catalog"] != catalog:
                fail("catalog_deletions_invalid")
            if deletion["schema"] != "pg_catalog" or deletion["name"] != catalog:
                fail("catalog_deletions_invalid")
            for field in ("deleted_rows", "deleted_toast_chunks", "wal_upper_bytes"):
                integer(deletion[field])
        variable_wal = sum(deletion["wal_upper_bytes"] for deletion in deletions)
        if (
            variable_wal > 32 * 1024 * 1024
            or dependency["catalog_tuple_count"] != sum(deletion["deleted_rows"] for deletion in deletions)
            or dependency["drop_wal_upper_bytes"] != 32 * 1024 * 1024 + variable_wal
        ):
            fail("drop_catalog_wal_exceeded")


def _validate_stages(body):
    _validate_named_stages(body["stages"], stage_roles(body))


def _named_coordinate(coordinate):
    exact(coordinate, {"schema", "name"})
    text(coordinate["schema"], 63)
    text(coordinate["name"], 63)
    return coordinate


def _coordinate_key(coordinate):
    return coordinate["schema"], coordinate["name"]


def _validate_named_stages(stages, roles):
    if not isinstance(stages, list) or len(stages) != len(roles):
        fail("stages_invalid")
    for stage, role in zip(stages, roles, strict=True):
        exact(stage, {"role", "schema", "name"})
        if stage["role"] != role:
            fail("stages_invalid")
        _named_coordinate({name: stage[name] for name in ("schema", "name")})
    if len({_coordinate_key(stage) for stage in stages}) != len(roles):
        fail("stage_name_reused")


def _validate_limits(body):
    limits = exact(body["limits"], LIMIT_FIELDS)
    for field in LIMIT_FIELDS:
        integer(limits[field], 0 if field in {"temp_bytes", "data_bytes"} else 1)
    if limits["statement_count"] != len(stage_roles(body)) + 2 or not 1 <= limits["max_operation_seconds"] <= 120:
        fail("operation_limit_invalid")
    if not 1 <= limits["lock_timeout_ms"] <= limits["statement_timeout_ms"] <= 30000:
        fail("timeout_invalid")
    classes = body["storage_classes"]
    if not isinstance(classes, list) or len(classes) != 3:
        fail("storage_classes_invalid")
    outstanding = 0
    for entry, role in zip(classes, ("data", "temp", "wal"), strict=True):
        exact(entry, {"storage_class", "reserved_bytes", "available_bytes", "available_after_all_reservations_bytes"})
        if entry["storage_class"] != role or entry["reserved_bytes"] != limits[role + "_bytes"]:
            fail("storage_limit_invalid")
        for field in ("reserved_bytes", "available_bytes", "available_after_all_reservations_bytes"):
            integer(entry[field])
        deficit = entry["available_bytes"] - entry["reserved_bytes"] - entry["available_after_all_reservations_bytes"]
        if deficit < 0:
            fail("reservation_accounting_invalid")
        outstanding += deficit
    if (
        min(entry["available_bytes"] for entry in classes)
        < sum(entry["reserved_bytes"] for entry in classes) + outstanding
    ):
        fail("reservation_accounting_invalid")


def _validate_observations(body, *, at, fresh):
    if (
        body.get("variant") in {"legacy_full_swap", "initial_full_swap"}
        and fresh
        and not at - timedelta(seconds=5) <= timestamp(body["capacity"]["observed_at"]) <= at
    ):
        fail("capacity_observation_stale")
    observations = exact(body["observations"], OBSERVATION_FIELDS)
    for kind in ("host", "control", "healthcare"):
        hash64(observations[kind + "_sha256"])
        observed = timestamp(observations[kind + "_observed_at"])
        if fresh and not at - timedelta(seconds=5) <= observed <= at:
            fail("observation_stale")
    ids = observations["accounted_reservation_ids"]
    if not isinstance(ids, list) or ids != sorted(set(ids)) or body["reservation_id"] not in ids:
        fail("accounting_identities_invalid")
    for identity in ids:
        text(identity, maximum=128)


def _authorization_body(candidate):
    """Select an exact current version; old signatures confer no new disposal authority."""
    contract = candidate.get("contract_id") if isinstance(candidate, dict) else None
    if not isinstance(contract, str):
        fail("purpose_invalid")
    if contract in HISTORICAL_CONTRACTS:
        fail("legacy_semantic_evidence_required")
    fields_by_contract = {
        CONTRACT: BODY_FIELDS,
        LEGACY_CONTRACT: LEGACY_BODY_FIELDS,
        INITIAL_CONTRACT: INITIAL_BODY_FIELDS,
    }
    body = exact(candidate, fields_by_contract.get(contract, BODY_FIELDS))
    if contract not in fields_by_contract or body["purpose"] != "failed_profile_cleanup":
        fail("purpose_invalid")
    variant = {LEGACY_CONTRACT: "legacy_full_swap", INITIAL_CONTRACT: "initial_full_swap"}.get(contract)
    if body.get("variant") != variant:
        fail("variant_invalid")
    return body


def _validated_initial_target(raw):
    from process.provider_directory_profile_initial_contract import target_state_sha256, validated_target_state

    target = validated_target_state(raw)
    return target, target_state_sha256(target)


def _validate_initial_request(body):
    """Verify full original geometry before emitting the compact signed authority."""
    from process import provider_directory_profile_capacity as capacity
    from process import provider_directory_profile_initial_contract as initial

    _validate_initial_manifest(body)
    geometry = capacity.validated_capacity_geometry(body["capacity_geometry"])
    if not isinstance(geometry, initial.InitialCapacityGeometry):
        fail("initial_geometry_required")
    target = body["initial"]["target_state"]
    _validate_initial_bindings(
        body,
        capacity.capacity_geometry_payload(geometry),
        target,
        capacity.capacity_geometry_hash(geometry),
        initial.target_state_sha256(target),
    )


def _validate_initial_manifest(body):
    """Bind the logical initial target state and independent disposal capacity."""
    manifest = exact(body["initial"], {"target_state", "initial_target_state_sha256", "receipt"})
    target, target_hash = _validated_initial_target(manifest["target_state"])
    if manifest["initial_target_state_sha256"] != target_hash:
        fail("initial_target_changed")
    _named_coordinate(manifest["receipt"])
    _validate_capacity_dependencies(body)
    forbidden_names = {_coordinate_key(coordinate) for coordinate in target["targets"].values()}
    forbidden_names.add(_coordinate_key(manifest["receipt"]))
    forbidden_names.update(_coordinate_key(body["capacity"][name]) for name in ("checkpoint_layout", "claim_layout"))
    if len(forbidden_names) != 5 or any(_coordinate_key(stage) in forbidden_names for stage in body["stages"]):
        fail("stage_is_serving")


def _validate_initial_bindings(body, geometry, target_state, geometry_hash, target_hash):
    """Bind original semantic geometry and target names to separately funded cleanup."""
    checkpoint = body["checkpoint"]
    if (
        body["initial"]["initial_target_state_sha256"] != geometry["initial_target_state_sha256"]
        or geometry_hash != checkpoint["capacity_geometry_hash"]
        or target_hash != geometry["initial_target_state_sha256"]
        or geometry["selection_proof_id"] != checkpoint["selection_proof_id"]
        or geometry["executable_plan_hash"] != checkpoint["executable_plan_hash"]
    ):
        fail("initial_geometry_changed")
    for role in ("evidence", "profile"):
        if {name: geometry["relations"][role + "_target"][name] for name in ("schema", "name")} != target_state[
            "targets"
        ][role]:
            fail("initial_target_changed")
    history = target_state["historical_publication"]
    if history is not None and (
        history["run_id"] == checkpoint["owner_run_id"]
        or history["result"]["proof_id"] == checkpoint["selection_proof_id"]
        or history["result"]["generation"] == checkpoint["control_generation"]
    ):
        fail("build_is_serving")
    if any(geometry[name] != observed_setting for name, observed_setting in body["capacity"]["postgres"].items()):
        fail("capacity_runtime_changed")
    expected = geometry["relations"]["build_checkpoint"]
    if any(expected[name] != body["capacity"]["checkpoint_layout"][name] for name in ("schema", "name")):
        fail("initial_checkpoint_layout_changed")
    if {name: geometry["relations"]["initial_receipt"][name] for name in ("schema", "name")} != body["initial"][
        "receipt"
    ]:
        fail("initial_receipt_layout_changed")


def validate_authorization(envelope, *, trust, now, claimed_at=None):
    """Verify independent cleanup trust; stored claims allow bounded retired-key replay."""
    exact(envelope, {"authorization", "signature"})
    body = _authorization_body(envelope["authorization"])
    if len(canonical(envelope).encode()) > MAX_BYTES:
        fail("authorization_too_large")
    for field in (
        "authorization_id",
        "operation_id",
        "reservation_id",
        "nonce",
        "attestor_id",
        "key_id",
        "environment_id",
    ):
        text(body[field])
    hash64(body["attestor_release_digest"])
    _validate_coordinates(body)
    if body["contract_id"] == LEGACY_CONTRACT:
        _validate_legacy_manifests(body)
    elif body["contract_id"] == INITIAL_CONTRACT:
        _validate_initial_manifest(body)
    _validate_stages(body)
    _validate_limits(body)
    issued = timestamp(body["issued_at"])
    expires = timestamp(body["expires_at"])
    deadline = timestamp(body["max_operation_deadline"])
    at = claimed_at or now
    if not issued <= at < min(expires, deadline) or expires - issued > timedelta(minutes=15):
        fail("authorization_expired")
    if deadline - issued > timedelta(seconds=body["limits"]["max_operation_seconds"]):
        fail("deadline_invalid")
    _validate_observations(body, at=issued, fresh=True)
    if claimed_at is None:
        _validate_observations(body, at=at, fresh=True)
    _validate_trust(body, trust=trust, now=now, claimed_at=claimed_at)
    return _verify_signature(envelope, body, trust)


def _verify_signature(envelope, body, trust):
    key = next(entry for entry in trust.keys if entry.key_id == body["key_id"])
    from cryptography.exceptions import InvalidSignature

    try:
        signature = envelope["signature"]
        if not isinstance(signature, str) or re.fullmatch(r"[A-Za-z0-9_-]{86}", signature) is None:
            fail("signature_invalid")
        decoded = base64.urlsafe_b64decode(signature + "==")
        if base64.urlsafe_b64encode(decoded).decode().rstrip("=") != signature:
            fail("signature_invalid")
        Ed25519PublicKey.from_public_bytes(key.public_key).verify(
            decoded, (signature_domain(body) + "\n" + canonical(body)).encode()
        )
    except (ValueError, TypeError, InvalidSignature) as error:
        raise RuntimeError("failed_profile_cleanup_signature_invalid") from error
    return body


def authenticate_historical_authorization(envelope, *, trust, now, claimed_at):
    """Authenticate original1/2/3 bytes and full ceilings, without semantic disposal authority."""
    exact(envelope, {"authorization", "signature"})
    candidate = envelope["authorization"]
    contract = candidate.get("contract_id") if isinstance(candidate, dict) else None
    if not isinstance(contract, str) or contract not in _HISTORICAL_FIELDS:
        fail("purpose_invalid")
    body = exact(candidate, _HISTORICAL_FIELDS[contract])
    if body["purpose"] != "failed_profile_cleanup" or len(canonical(envelope).encode()) > MAX_BYTES:
        fail("purpose_invalid")
    for field in (
        "authorization_id",
        "operation_id",
        "reservation_id",
        "nonce",
        "attestor_id",
        "key_id",
        "environment_id",
    ):
        text(body[field])
    hash64(body["attestor_release_digest"])
    limits = exact(body["limits"], LIMIT_FIELDS)
    for name, ceiling in limits.items():
        integer(ceiling, 0 if name in {"data_bytes", "temp_bytes"} else 1)
    issued = timestamp(body["issued_at"])
    expires = timestamp(body["expires_at"])
    deadline = timestamp(body["max_operation_deadline"])
    if (
        not isinstance(claimed_at, datetime)
        or claimed_at.tzinfo is None
        or not issued <= claimed_at < min(expires, deadline)
    ):
        fail("authorization_expired")
    if expires - issued > timedelta(minutes=15) or deadline - issued > timedelta(
        seconds=limits["max_operation_seconds"]
    ):
        fail("deadline_invalid")
    # Retained semantic/physical documents remain opaque original signed history.
    # Current key retirement is evaluated at the claim's original acceptance time.
    _validate_trust(body, trust=trust, now=claimed_at, claimed_at=claimed_at)
    return _verify_signature(envelope, body, trust)


def _validate_trust(body, *, trust, now, claimed_at):
    if trust is None:
        fail("independent_trust_missing")
    for name in ("attestor_id", "environment_id"):
        if body[name] != getattr(trust, name):
            fail("trust_binding_invalid")
    keys = [item for item in trust.keys if item.key_id == body["key_id"]]
    if len(keys) != 1 or keys[0].attestor_release_digest != body["attestor_release_digest"]:
        fail("trust_key_invalid")
    key = keys[0]
    if key.status == "active" and key.key_id == trust.active_key_id:
        return
    if key.status != "retired" or claimed_at is None or key.retired_at is None or key.verify_until is None:
        fail("trust_key_invalid")
    if (
        now >= key.verify_until
        or timestamp(body["issued_at"]) > key.retired_at
        or timestamp(body["expires_at"]) > key.verify_until
    ):
        fail("trust_key_retired_invalid")


def json_values(value):
    """Encode the entire checkpoint preimage without discarding timestamp precision."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_values(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_values(item) for item in value]
    return value


def claim_ref(fhir, schema):
    """Resolve the single installed spent-authority ledger."""
    return fhir._unscoped_qt(schema, "provider_directory_profile_failed_cleanup_claim")


def _refuse_inherited_authority(fhir):
    from process.provider_directory_cms_preparation import _ACTIVE

    if fhir._provider_directory_profile_capacity_admission() is not None or _ACTIVE.get() is not None:
        fail("active_capacity_context")
    if fhir.db._transaction_binding() is not None:
        fail("fresh_transaction_required")


async def _database_binding(fhir, schema):
    return {"schema": schema}


async def _capacity_identity(fhir, schema, serving):
    """Read logical target names and numeric PostgreSQL capacity assumptions."""
    return await fhir._provider_directory_profile_capacity_database_identity(schema, serving)


async def _checkpoint_and_owner(fhir, schema, build_id, owner_run_id, *, locked, variant=None):
    suffix = " FOR UPDATE NOWAIT" if locked else ""
    checkpoint_row = await fhir.db.first(
        f"SELECT * FROM {fhir._provider_directory_profile_checkpoint_ref(schema)} WHERE build_id=:build" + suffix,
        build=build_id,
    )
    if checkpoint_row is None:
        fail("checkpoint_missing")
    checkpoint_by_field = dict(checkpoint_row._mapping)
    owner_row = await fhir.db.first(
        f"SELECT * FROM {fhir._unscoped_qt(schema, 'import_run')} WHERE run_id=:owner" + suffix, owner=owner_run_id
    )
    if owner_row is None:
        fail("owner_missing")
    owner_by_field = dict(owner_row._mapping)
    if (
        checkpoint_by_field["owner_run_id"] != owner_run_id
        or checkpoint_by_field["state"] != "failed"
        or owner_by_field["importer"] != "provider-directory-fhir"
        or owner_by_field["status"] not in {"failed", "canceled", "cancelled", "dead_letter"}
        or owner_by_field["finished_at"] is None
    ):
        fail("owner_not_failed_terminal")
    observed_variant = _cleanup_variant(checkpoint_by_field)
    if variant is not None and observed_variant != variant:
        fail("variant_changed")
    if observed_variant in {"legacy_full_swap", "initial_full_swap"} and (
        checkpoint_by_field["affected_npi_stage"] is not None
    ):
        fail("legacy_affected_stage_present")
    return checkpoint_by_field, owner_by_field


def _checkpoint_coordinates(checkpoint, owner):
    from process import provider_directory_profile_capacity as capacity

    if not isinstance(checkpoint["last_error"], str):
        fail("complete_failure_error_missing")
    if _cleanup_variant(checkpoint) == "legacy_full_swap":
        return _legacy_checkpoint_coordinates(checkpoint, owner), None
    geometry = capacity.validated_capacity_geometry(checkpoint["capacity_geometry_json"])
    if geometry.physical_projection_contract_id != capacity.BOUNDED_ADMISSION_CONTRACT_ID:
        fail("historical_geometry_unsupported")
    if capacity.capacity_geometry_hash(geometry) != checkpoint["capacity_geometry_hash"]:
        fail("geometry_changed")
    params = owner["params"]
    if isinstance(params, str):
        params = json.loads(params)
    selection = params.get("provider_directory_profile_selection_attestation", {})
    result_by_field = {
        field: checkpoint[field]
        for field in (
            "build_id",
            "owner_run_id",
            "resume_lineage_hash",
            "capacity_geometry_hash",
            "executable_plan_hash",
        )
    }
    result_by_field.update(
        preimage_sha256=digest(
            json_values(checkpoint),
            (INITIAL_CONTRACT if _cleanup_variant(checkpoint) == "initial_full_swap" else CONTRACT) + ".checkpoint",
        ),
        original_error_sha256=hashlib.sha256((checkpoint["last_error"] or "").encode()).hexdigest(),
        selection_proof_id=selection.get("proof_id"),
        authority_revision=selection.get("authority_revision"),
        control_generation=params.get("provider_directory_profile_generation"),
    )
    return result_by_field, geometry


async def _stage_manifest(fhir, schema, checkpoint, *, locked):
    names = tuple(name for name in fhir._validated_profile_checkpoint_stage_names(checkpoint) if name is not None)
    manifest_values = []
    roles = stage_roles({"variant": _cleanup_variant(checkpoint)})
    for name, role in zip(names, roles, strict=True):
        oid = await fhir._provider_directory_relation_oid(schema, name)
        if oid is None:
            fail("stage_missing")
        if locked:
            await fhir.db.status(f"LOCK TABLE {fhir._unscoped_qt(schema, name)} IN ACCESS EXCLUSIVE MODE NOWAIT")
        await fhir._provider_directory_profile_capacity_relation_layout(oid, expected_persistence="p")
        manifest_values.append({"role": role, "schema": schema, "name": name})
    return manifest_values


def _cleanup_variant(checkpoint):
    if checkpoint["materialization_mode"] == "source_delta":
        return "source_delta"
    if (
        checkpoint["materialization_mode"] == "full_swap"
        and checkpoint["capacity_geometry_status"] == "legacy_unavailable"
    ):
        return "legacy_full_swap"
    if checkpoint["materialization_mode"] == "full_swap" and checkpoint["capacity_geometry_status"] == "verified":
        from process import provider_directory_profile_capacity as capacity
        from process.provider_directory_profile_initial_contract import InitialCapacityGeometry

        geometry = capacity.validated_capacity_geometry(checkpoint["capacity_geometry_json"])
        if isinstance(geometry, InitialCapacityGeometry):
            return "initial_full_swap"
    fail("historical_geometry_unsupported")


def _legacy_checkpoint_coordinates(checkpoint, owner):
    params = owner["params"]
    if isinstance(params, str):
        params = json.loads(params)
    if not isinstance(params, dict) or not isinstance(checkpoint["last_error"], str):
        fail("legacy_owner_preimage_invalid")
    selection = params.get("provider_directory_profile_selection_attestation") or {}
    if not isinstance(selection, dict):
        fail("legacy_lineage_partial")
    lineage = (
        selection.get("proof_id"),
        selection.get("authority_revision"),
        params.get("provider_directory_profile_generation"),
    )
    if all(lineage_value is None for lineage_value in lineage):
        kind = "absent"
    elif all(lineage_value is not None for lineage_value in lineage):
        kind = "complete"
    else:
        fail("legacy_lineage_partial")
    return {
        **{
            name: checkpoint[name]
            for name in (
                "build_id",
                "owner_run_id",
                "resume_lineage_hash",
                "capacity_geometry_hash",
                "executable_plan_hash",
            )
        },
        "preimage_sha256": digest(json_values(checkpoint), LEGACY_CONTRACT + ".checkpoint"),
        "original_error_sha256": hashlib.sha256(checkpoint["last_error"].encode()).hexdigest(),
        "owner_params_sha256": digest(params, LEGACY_CONTRACT + ".owner-params"),
        "lineage_kind": kind,
        "selection_proof_id": lineage[0],
        "authority_revision": lineage[1],
        "control_generation": lineage[2],
    }


async def _publication_manifest(fhir, schema, checkpoint, coordinates, published_run_id, *, locked):
    """Fence named published targets and the latest genuine result without adopting it."""
    from types import SimpleNamespace

    relations_by_role = {
        "serving_generation": {"schema": schema, "name": "provider_directory_profile_serving_generation"},
        "delta_receipt": {"schema": schema, "name": "provider_directory_profile_delta_receipt"},
        "cms_receipt": {"schema": schema, "name": "provider_directory_cms_serving_receipt"},
    }
    for coordinate in relations_by_role.values():
        if not await fhir.db.scalar(
            "SELECT to_regclass(:name) IS NOT NULL", name=fhir._unscoped_qt(coordinate["schema"], coordinate["name"])
        ):
            fail("installed_publication_metadata_missing")
    if locked:
        await fhir.db.status(
            "LOCK TABLE "
            + ",".join(fhir._unscoped_qt(entry["schema"], entry["name"]) for entry in relations_by_role.values())
            + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
        )
    serving_rows = await fhir.db.all(f"SELECT * FROM {fhir._provider_directory_profile_serving_generation_ref(schema)}")
    if len(serving_rows) > 1:
        fail("publication_ambiguous")
    targets_by_role = dict(serving_rows[0]._mapping).get("targets_json") if serving_rows else None
    if isinstance(targets_by_role, str):
        targets_by_role = json.loads(targets_by_role)
    if targets_by_role is None:
        targets_by_role = {
            "evidence": {"schema": schema, "name": "provider_directory_profile_evidence"},
            "profile": {"schema": schema, "name": "provider_directory_profile"},
        }
    target_entries = []
    for role in ("evidence", "profile"):
        coordinate = _named_coordinate(targets_by_role[role])
        if not await fhir.db.scalar(
            "SELECT to_regclass(:name) IS NOT NULL", name=fhir._unscoped_qt(coordinate["schema"], coordinate["name"])
        ):
            fail("published_target_missing")
        if locked:
            await fhir.db.status(
                f"LOCK TABLE {fhir._unscoped_qt(coordinate['schema'], coordinate['name'])} IN ACCESS SHARE MODE NOWAIT"
            )
        target_entries.append({"role": role, **coordinate})
    publication = await _published_history_manifest(fhir, schema, coordinates, published_run_id, locked=locked)
    profile = targets_by_role["profile"]
    if not await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {fhir._unscoped_qt(profile['schema'], profile['name'])} WHERE generation_id=:generation)",
        generation=publication["published_generation_id"],
    ):
        fail("published_target_generation_missing")
    publication.update(
        serving_state="singleton" if serving_rows else "empty", relations=relations_by_role, targets=target_entries
    )
    await _exclude_legacy_history(fhir, schema, coordinates, serving_rows, publication)
    return publication, SimpleNamespace(targets=targets_by_role)


async def _published_history_manifest(fhir, schema, coordinates, published_run_id, *, locked):
    if not published_run_id or published_run_id == coordinates["owner_run_id"]:
        fail("published_history_required")
    suffix = " FOR UPDATE NOWAIT" if locked else ""
    published_run = await fhir.db.first(
        f"SELECT * FROM {fhir._unscoped_qt(schema, 'import_run')} WHERE run_id=:run" + suffix, run=published_run_id
    )
    if published_run is None:
        fail("published_history_missing")
    owner_by_field = dict(published_run._mapping)
    metrics = owner_by_field["metrics"]
    if isinstance(metrics, str):
        metrics = json.loads(metrics)
    result_by_field = metrics.get("profile_selection_result") if isinstance(metrics, dict) else None
    if (
        owner_by_field["importer"] != "provider-directory-fhir"
        or owner_by_field["status"] != "succeeded"
        or owner_by_field["finished_at"] is None
        or not isinstance(result_by_field, dict)
        or result_by_field.get("status") != "published"
        or result_by_field.get("operation") != "publish"
        or result_by_field.get("profile_generation_id") == coordinates["build_id"]
    ):
        fail("published_history_invalid")
    proof, generation, revision = (
        result_by_field.get("proof_id"),
        result_by_field.get("generation"),
        result_by_field.get("authority_revision"),
    )
    text(proof, 128)
    integer(generation, 1)
    integer(revision, 1)
    text(result_by_field.get("profile_generation_id"), 128)
    latest = await fhir.db.scalar(
        f"SELECT run_id FROM {fhir._unscoped_qt(schema, 'import_run')} "
        "WHERE importer='provider-directory-fhir' AND status='succeeded' "
        "AND metrics::jsonb->'profile_selection_result' IS NOT NULL "
        "ORDER BY finished_at DESC NULLS LAST,run_id DESC LIMIT 1"
    )
    if latest != published_run_id:
        fail("published_history_not_latest")
    # Preserve raw historical metrics, including unknown as-of; cleanup never adopts them.
    return {
        "published_run_id": published_run_id,
        "published_run_preimage_sha256": digest(json_values(owner_by_field), LEGACY_CONTRACT + ".published-run"),
        "published_result_sha256": digest(result_by_field, LEGACY_CONTRACT + ".published-result"),
        "published_generation_id": result_by_field["profile_generation_id"],
        "published_proof_id": proof,
        "published_control_generation": generation,
    }


async def _exclude_legacy_history(fhir, schema, coordinates, serving_rows, publication):
    if coordinates["selection_proof_id"] in {publication["published_proof_id"]} or coordinates[
        "control_generation"
    ] in {publication["published_control_generation"]}:
        fail("build_is_serving")
    for receipt_row in serving_rows:
        serving_by_field = dict(receipt_row._mapping)
        if (
            serving_by_field.get("generation_id") == coordinates["build_id"]
            or (
                coordinates["selection_proof_id"] is not None
                and serving_by_field.get("selection_proof_id") == coordinates["selection_proof_id"]
            )
            or (
                coordinates["control_generation"] is not None
                and serving_by_field.get("control_generation") == coordinates["control_generation"]
            )
        ):
            fail("build_is_serving")
        if (
            serving_by_field["generation_id"] != publication["published_generation_id"]
            or serving_by_field["selection_proof_id"] != publication["published_proof_id"]
            or serving_by_field["control_generation"] != publication["published_control_generation"]
        ):
            fail("published_history_mismatch")
    delta = fhir._unscoped_qt(schema, "provider_directory_profile_delta_receipt")
    cms = fhir._unscoped_qt(schema, "provider_directory_cms_serving_receipt")
    if await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {delta} WHERE build_id=:build OR selection_proof_id=:proof OR control_generation=:generation)",
        build=coordinates["build_id"],
        proof=coordinates["selection_proof_id"],
        generation=coordinates["control_generation"],
    ):
        fail("historical_delta_publication")
    if await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {cms} WHERE payload->'profile'->>'generation_id'=:build OR payload->'profile'->>'selection_proof_id'=:proof OR payload->'profile'->>'control_generation'=:generation)",
        build=coordinates["build_id"],
        proof=coordinates["selection_proof_id"],
        generation=str(coordinates["control_generation"]) if coordinates["control_generation"] is not None else None,
    ):
        fail("historical_cms_publication")


async def _metadata_layouts(fhir, schema, *, locked):
    if locked:
        await fhir.db.status(f"LOCK TABLE {claim_ref(fhir, schema)} IN SHARE UPDATE EXCLUSIVE MODE NOWAIT")
    layouts_by_role = {}
    for role, name in (
        ("checkpoint", "provider_directory_profile_build_checkpoint"),
        ("claim", "provider_directory_profile_failed_cleanup_claim"),
    ):
        oid = await fhir._provider_directory_relation_oid(schema, name)
        if oid is None:
            fail("installed_schema_missing")
        layouts_by_role[role] = await fhir._provider_directory_profile_capacity_relation_layout(
            oid,
            expected_persistence="p",
            expected_user_trigger_count=2 if role == "claim" else 0,
            expected_immutable_trigger_error="failed_profile_cleanup_claim_immutable" if role == "claim" else None,
        )
    return layouts_by_role


# These are the catalog mutations in PG18's closed heap/index/type/constraint/
# attrdef DROP path and deleteOneObject's shared dependencies/comments/labels/ACLs.
# Extended statistics, rules, inheritance, foreign tables, publications, defaults
# owning sequences and extension objects are outside the permitted closure.
_DROP_CATALOG_SCOPES = (
    ("pg_class", "oid=ANY(CAST(:relations AS oid[]))"),
    ("pg_attribute", "attrelid=ANY(CAST(:relations AS oid[]))"),
    ("pg_index", "indexrelid=ANY(CAST(:relations AS oid[]))"),
    ("pg_type", "oid=ANY(CAST(:objects AS oid[]))"),
    ("pg_constraint", "oid=ANY(CAST(:objects AS oid[]))"),
    ("pg_attrdef", "oid=ANY(CAST(:objects AS oid[]))"),
    ("pg_depend", "objid=ANY(CAST(:objects AS oid[])) OR refobjid=ANY(CAST(:objects AS oid[]))"),
    (
        "pg_shdepend",
        "objid=ANY(CAST(:objects AS oid[])) AND dbid=(SELECT oid FROM pg_database WHERE datname=current_database())",
    ),
    ("pg_description", "objoid=ANY(CAST(:objects AS oid[]))"),
    ("pg_seclabel", "objoid=ANY(CAST(:objects AS oid[]))"),
    ("pg_init_privs", "objoid=ANY(CAST(:objects AS oid[]))"),
    ("pg_statistic", "starelid=ANY(CAST(:relations AS oid[]))"),
)


async def _lock_drop_catalogs(fhir):
    """Freeze auxiliary payload/TOAST before spending authority; no privilege elevation."""
    await fhir.db.status(
        "LOCK TABLE "
        + ",".join("pg_catalog." + name for name, _predicate in _DROP_CATALOG_SCOPES)
        + " IN SHARE MODE NOWAIT"
    )


async def _catalog_deletion_manifest(fhir, relation_oids, object_oids):
    from types import SimpleNamespace

    from process.provider_directory_profile_capacity_target import _target_wal_bytes
    from process.provider_directory_profile_capacity_types import ProviderDirectoryProfileTargetDeltaInput

    manifests = []
    params_by_field = {"relations": relation_oids, "objects": object_oids}
    compression = await fhir.db.scalar("SHOW default_toast_compression")
    for name, predicate in _DROP_CATALOG_SCOPES:
        oid = await fhir.db.scalar("SELECT CAST(CAST(:name AS regclass) AS oid)::bigint", name="pg_catalog." + name)
        layout = await fhir._provider_directory_profile_capacity_relation_layout(oid, expected_persistence="p")
        source_sql = "SELECT * FROM pg_catalog." + name + " WHERE " + predicate
        deleted_rows = await fhir.db.scalar("SELECT count(*) FROM (" + source_sql + ") AS scoped", **params_by_field)
        toast_chunks = await fhir._provider_directory_profile_toast_chunk_count(
            source_sql=source_sql,
            relation_oid=oid,
            toast_oid=layout.toast_oid,
            toastable_columns=layout.toastable_columns,
            expected_compression=compression,
            params=params_by_field,
        )
        mutation = ProviderDirectoryProfileTargetDeltaInput(
            relation_name="evidence_target",
            inserted_rows=0,
            inserted_toast_chunks=0,
            deleted_rows=deleted_rows,
            deleted_toast_chunks=toast_chunks,
            deleted_logical_bytes=0,
            main_index_pages=layout.main_index_pages,
            toast_index_pages=layout.toast_index_pages,
        )
        wal_bytes = _target_wal_bytes(SimpleNamespace(postgres_block_size_bytes=8192), mutation)
        manifests.append(
            {
                "catalog": name,
                "schema": "pg_catalog",
                "name": name,
                "deleted_rows": deleted_rows,
                "deleted_toast_chunks": toast_chunks,
                "wal_upper_bytes": wal_bytes,
            }
        )
    return manifests


async def _require_supported_drop_catalogs(fhir, relation_oids):
    # System catalog updates outside this path cannot be silently charged as deletes.
    if await fhir.db.scalar(
        """SELECT
        EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=ANY(CAST(:oids AS oid[])) OR inhparent=ANY(CAST(:oids AS oid[]))) OR
        EXISTS(SELECT 1 FROM pg_publication_rel WHERE prrelid=ANY(CAST(:oids AS oid[]))) OR
        EXISTS(SELECT 1 FROM pg_subscription_rel WHERE srrelid=ANY(CAST(:oids AS oid[]))) OR
        EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=ANY(CAST(:oids AS oid[]))) OR
        EXISTS(SELECT 1 FROM pg_statistic_ext WHERE stxrelid=ANY(CAST(:oids AS oid[]))) OR
        EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=ANY(CAST(:oids AS oid[]))) OR
        EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE a.attrelid=ANY(CAST(:oids AS oid[])) AND a.attnum>0 AND NOT a.attisdropped
              AND pg_get_serial_sequence(format('%I.%I',n.nspname,c.relname),a.attname) IS NOT NULL) OR
        EXISTS(SELECT 1 FROM pg_class WHERE oid=ANY(CAST(:oids AS oid[])) AND (relkind NOT IN ('r','i','t') OR relispartition))""",
        oids=relation_oids,
    ):
        fail("drop_catalog_shape_unsupported")


async def _stage_dependency_manifest(fhir, schema, stage):
    """Measure bounded native DROP catalog costs; RESTRICT enforces external dependencies."""
    oid = await fhir._provider_directory_relation_oid(stage["schema"], stage["name"])
    if oid is None:
        fail("stage_missing")
    layout = await fhir._provider_directory_profile_capacity_relation_layout(oid, expected_persistence="p")
    relation_oids = [oid, *layout.main_index_oids, *layout.toast_index_oids]
    if layout.toast_oid is not None:
        relation_oids.append(layout.toast_oid)
    if not 1 <= len(relation_oids) <= 32:
        fail("drop_dependency_bound_unsupported")
    await _require_supported_drop_catalogs(fhir, relation_oids)
    if await fhir.db.scalar("SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtenabled<>'D')"):
        fail("drop_event_trigger_unsupported")
    object_rows = await fhir.db.all(
        """SELECT oid::bigint FROM pg_type WHERE typrelid=ANY(CAST(:oids AS oid[]))
        UNION SELECT typarray::bigint FROM pg_type WHERE typrelid=ANY(CAST(:oids AS oid[])) AND typarray<>0
        UNION SELECT oid::bigint FROM pg_constraint WHERE conrelid=ANY(CAST(:oids AS oid[]))
        UNION SELECT oid::bigint FROM pg_attrdef WHERE adrelid=ANY(CAST(:oids AS oid[]))""",
        oids=relation_oids,
    )
    object_oids = [*relation_oids, *(int(catalog_row[0]) for catalog_row in object_rows)]
    if not 1 <= len(object_oids) <= 128:
        fail("drop_dependency_bound_unsupported")
    catalog_deletions = await _catalog_deletion_manifest(fhir, relation_oids, object_oids)
    catalog_rows = sum(entry["deleted_rows"] for entry in catalog_deletions)
    if catalog_rows > 8192:
        fail("drop_catalog_bound_unsupported")
    variable_wal = sum(entry["wal_upper_bytes"] for entry in catalog_deletions)
    if variable_wal > 32 * 1024 * 1024:
        fail("drop_catalog_wal_exceeded")
    return {
        "role": stage["role"],
        "object_count": len(object_oids),
        "relation_count": len(relation_oids),
        "catalog_tuple_count": catalog_rows,
        "catalog_deletions": catalog_deletions,
        "drop_wal_upper_bytes": 32 * 1024 * 1024 + variable_wal,
    }


def _commit_wal_bound(dependencies, runtime):
    # PG18 sinval.h uses int8: nonnegative IDs have 128 possible cache values.
    # It defines six negative invalidation families; charge eight plus both
    # old/new tuple versions. SharedInvalidationMessage/relfile/statistic entries
    # are at most 64 bytes; xact.h/xact.c serialize these arrays at COMMIT.
    # Ordinary user metadata DML emits no catalog invalidations. No savepoints,
    # event triggers, external dependencies or unsupported object kinds are allowed.
    invalidations = 2 * (128 + 8) * sum(entry["catalog_tuple_count"] for entry in dependencies)
    relfiles_and_stats = (2 + 8) * sum(entry["relation_count"] for entry in dependencies)
    commit_bound = 3 * (64 * (invalidations + relfiles_and_stats) + 2 * runtime.postgres_wal_block_size_bytes)
    return commit_bound


async def _complete_checkpoint_payload_bound(fhir, schema, checkpoint):
    """Bound the full retained row using PostgreSQL's complete JSON representation."""
    json_bytes = await fhir.db.scalar(
        f"SELECT octet_length(to_jsonb(c)::text)::bigint FROM "
        f"{fhir._provider_directory_profile_checkpoint_ref(schema)} AS c WHERE build_id=:build",
        build=checkpoint["build_id"],
    )
    # Four bytes per JSON byte cover JSONB entries/headers; the existing row
    # allowance also covers fixed tuple fields, alignment and the bounded receipt.
    if json_bytes is None or 4 * json_bytes + 8192 > MAX_BYTES:
        fail("complete_checkpoint_payload_unsupported")
    return 4 * json_bytes + 8192


async def _cleanup_capacity_manifest(fhir, schema, checkpoint, serving, stages, *, locked):
    from types import SimpleNamespace

    from process.provider_directory_profile_capacity_geometry import (
        _assert_postgres_storage_limits,
        _assert_postgres_wal_limits,
    )

    observed = await _capacity_identity(fhir, schema, serving)
    postgres_by_field = {name: getattr(observed, name) for name in POSTGRES_FIELDS if hasattr(observed, name)}
    postgres_by_field.update(
        postgres_toast_max_chunk_size_bytes=1996, postgres_maxalign_bytes=8, postgres_btree_version=4
    )
    runtime = SimpleNamespace(**postgres_by_field, statement_timeout_ms=30000, lock_timeout_ms=5000)
    _assert_postgres_storage_limits(runtime)
    _assert_postgres_wal_limits(runtime)
    layouts = await _metadata_layouts(fhir, schema, locked=locked)
    layout = layouts["checkpoint"]
    old_toast = await fhir._provider_directory_profile_toast_chunk_count(
        source_sql=f"SELECT * FROM {fhir._provider_directory_profile_checkpoint_ref(schema)} WHERE build_id=:build",
        relation_oid=layout.relation_oid,
        toast_oid=layout.toast_oid,
        toastable_columns=layout.toastable_columns,
        expected_compression=runtime.postgres_default_toast_compression,
        params={"build": checkpoint["build_id"]},
    )
    dependencies = [await _stage_dependency_manifest(fhir, schema, stage) for stage in stages]
    manifest_by_field = {
        "observed_at": _now_utc().isoformat(),
        "postgres": postgres_by_field,
        "checkpoint_layout": {
            **_layout_coordinates(layout),
            "old_toast_chunks": old_toast,
            "row_payload_upper_bytes": await _complete_checkpoint_payload_bound(fhir, schema, checkpoint),
        },
        "claim_layout": _layout_coordinates(layouts["claim"]),
        "stage_dependencies": dependencies,
        "commit_wal_upper_bytes": _commit_wal_bound(dependencies, runtime),
    }
    return manifest_by_field, runtime, layouts


def _layout_coordinates(layout):
    return {
        "schema": layout.schema_name,
        "name": layout.relation_name,
        "main_index_pages": list(layout.main_index_pages),
        "toast_index_pages": list(layout.toast_index_pages),
        "toastable_column_count": len(layout.toastable_columns),
    }


async def project_cleanup_budget(fhir, schema, checkpoint, geometry, *, capacity_snapshot=None, layouts=None):
    """Project actual checkpoint/claim heaps, indexes, TOAST, locks, DROP and both commits."""
    from process.provider_directory_profile_capacity_physical import _metadata_mutation_projection
    from process.provider_directory_profile_capacity_types import (
        CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
        CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE,
    )

    if capacity_snapshot is None:
        await _complete_checkpoint_payload_bound(fhir, schema, checkpoint)
        stages = await _stage_manifest(fhir, schema, checkpoint, locked=False)
        dependencies = [await _stage_dependency_manifest(fhir, schema, stage) for stage in stages]
        commit_bound = _commit_wal_bound(dependencies, geometry)
    else:
        commit_bound = capacity_snapshot["commit_wal_upper_bytes"]
    if layouts is None:
        layouts = await _metadata_layouts(fhir, schema, locked=False)
    projections = []
    for role, name, operation in (
        ("checkpoint", "provider_directory_profile_build_checkpoint", "update"),
        ("claim", "provider_directory_profile_failed_cleanup_claim", "insert"),
    ):
        layout = layouts[role]
        old_toast = 0
        if operation == "update":
            old_toast = await fhir._provider_directory_profile_toast_chunk_count(
                source_sql=f"SELECT * FROM {fhir._provider_directory_profile_checkpoint_ref(schema)} WHERE build_id=:build",
                relation_oid=layout.relation_oid,
                toast_oid=layout.toast_oid,
                toastable_columns=layout.toastable_columns,
                expected_compression=geometry.postgres_default_toast_compression,
                params={"build": checkpoint["build_id"]},
            )
        mutation = fhir._provider_directory_profile_control_metadata_input(
            layout, relation_name=name, operation=operation, observed_deleted_toast_chunks=old_toast
        )
        projections.append(_metadata_mutation_projection(geometry, mutation))
    # Failure reserve covers abort/catalog work; rollback never refunds emitted WAL.
    stage_count = 2 if capacity_snapshot is not None else 3
    if capacity_snapshot is not None:
        dependencies = capacity_snapshot["stage_dependencies"]
    wal = sum(projection[1] for projection in projections) + sum(
        entry["drop_wal_upper_bytes"] for entry in dependencies
    )
    wal += (4 if capacity_snapshot is not None else 3) * CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE + commit_bound
    wal += CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT
    return {
        "data_bytes": sum(projection[0] for projection in projections),
        "temp_bytes": 0,
        "wal_bytes": wal,
        "statement_count": stage_count + 2,
        "statement_timeout_ms": min(30000, geometry.statement_timeout_ms),
        "lock_timeout_ms": min(5000, geometry.lock_timeout_ms),
        "max_operation_seconds": 120,
    }


def _initial_cleanup_binding(checkpoint, owner, geometry):
    """Bind historical verification to the actual failed checkpoint and terminal owner."""
    params = owner["params"]
    if isinstance(params, str):
        params = json.loads(params)
    if not isinstance(params, dict) or not isinstance(
        params.get("provider_directory_profile_selection_attestation"), dict
    ):
        fail("initial_original_binding_changed")
    expected_by_field = {
        "executable_plan_hash": geometry.executable_plan_hash,
        "desired_source_vector_hash": geometry.desired_source_vector_hash,
        "desired_source_context_vector_hash": geometry.desired_context_vector_hash,
        "profile_as_of": geometry.profile_as_of,
    }
    if (
        owner["run_id"] != checkpoint["owner_run_id"]
        or any(checkpoint[name] != value for name, value in expected_by_field.items())
        or params["provider_directory_profile_selection_attestation"].get("proof_id") != geometry.selection_proof_id
    ):
        fail("initial_original_binding_changed")
    return params, {
        "build_id": checkpoint["build_id"],
        "executable_plan_hash": checkpoint["executable_plan_hash"],
        "selection_proof_id": geometry.selection_proof_id,
        "to_source_vector_hash": checkpoint["desired_source_vector_hash"],
        "to_source_context_vector_hash": checkpoint["desired_source_context_vector_hash"],
        "profile_as_of": checkpoint["profile_as_of"],
    }


async def _original_initial_target(fhir, schema, checkpoint, owner, geometry):
    """Authenticate expired admission only as provenance, never as cleanup funding."""
    from process import provider_directory_profile_capacity as capacity
    from process import provider_directory_profile_initial_contract as contract

    params, binding = _initial_cleanup_binding(checkpoint, owner, geometry)
    consumed = await fhir._replay_bound_consumption(
        fhir._unscoped_qt(schema, fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__),
        owner["run_id"],
        checkpoint["build_id"],
    )
    if consumed["run_id"] != owner["run_id"] or consumed["build_id"] != checkpoint["build_id"]:
        fail("initial_original_binding_changed")
    lease = fhir._verified_provider_directory_profile_replay_lease(consumed, binding, geometry)
    envelope_by_field = {"lease": json.loads(consumed["canonical_lease_json"]), "signature": consumed["signature"]}
    if params.get("provider_directory_profile_capacity_attestation") != envelope_by_field:
        fail("initial_original_owner_changed")
    guard = lease.signing_preflight_guard
    request, receipt = guard["healthcare_request"], guard["healthcare_receipt"]
    execution = request["profile_execution"]
    if (
        request["contract_id"] != contract.REQUEST_CONTRACT
        or request.get("profile_materialization") != contract.MATERIALIZATION
        or receipt["contract_id"] != contract.RECEIPT_CONTRACT
        or receipt.get("profile_materialization") != contract.MATERIALIZATION
        or receipt["capacity_geometry"] != capacity.capacity_geometry_payload(geometry)
        or any(
            params.get(name) != expected_value
            for name, expected_value in execution.items()
            if name != "provider_directory_profile_capacity_attestation"
        )
    ):
        fail("initial_original_authority_changed")
    target_by_field = contract.validated_target_state(receipt["serving_generation_preflight"])
    if contract.target_state_sha256(target_by_field) != geometry.initial_target_state_sha256:
        fail("initial_target_changed")
    return target_by_field


async def _initial_manifest(fhir, schema, geometry, *, locked, checkpoint, owner):
    """Pin genuine incumbent targets and absent publication without inventing a predecessor."""
    from process import provider_directory_profile_initial as initial
    from process import provider_directory_profile_initial_contract as contract

    if not isinstance(geometry, contract.InitialCapacityGeometry):
        fail("initial_geometry_required")
    if await fhir._provider_directory_relation_oid(schema, contract.RECEIPT_TABLE) is None:
        fail("initial_receipt_layout_changed")
    if locked:
        names = (
            contract.RECEIPT_TABLE,
            "provider_directory_profile_delta_receipt",
            "provider_directory_cms_serving_receipt",
            "provider_directory_profile_evidence",
            "provider_directory_profile",
        )
        await fhir.db.status(
            "LOCK TABLE "
            + ",".join(fhir._unscoped_qt(schema, name) for name in names)
            + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
        )
    delta = fhir._unscoped_qt(schema, "provider_directory_profile_delta_receipt")
    if await fhir.db.scalar(f"SELECT EXISTS (SELECT 1 FROM {delta})"):
        fail("initial_delta_history")
    initial_targets = await initial.capture_targets(fhir, schema)
    try:
        target_by_field = contract.validated_target_state(initial_targets.payload)
    except ValueError:
        fail("initial_target_changed")
    if contract.target_state_sha256(target_by_field) != geometry.initial_target_state_sha256:
        original = await _original_initial_target(fhir, schema, checkpoint, owner, geometry)
        if any(
            target_by_field[name] != expected_value
            for name, expected_value in original.items()
            if name not in {"evidence_target_bytes", "profile_target_bytes"}
        ):
            fail("initial_target_changed")
        target_by_field = original
    receipt_coordinate_by_field = {"schema": schema, "name": contract.RECEIPT_TABLE}
    expected = geometry.relations.get("initial_receipt")
    if expected is None or any(expected[name] != receipt_coordinate_by_field[name] for name in ("schema", "name")):
        fail("initial_receipt_layout_changed")
    return {
        "target_state": target_by_field,
        "initial_target_state_sha256": geometry.initial_target_state_sha256,
        "receipt": receipt_coordinate_by_field,
    }, initial_targets


async def _inspect_initial_cleanup(fhir, schema, checkpoint, coordinates, geometry, stages, binding, owner):
    """Project separately funded cleanup for exactly two verified initial stages."""
    from process import provider_directory_profile_capacity as capacity

    initial, targets = await _initial_manifest(fhir, schema, geometry, locked=False, checkpoint=checkpoint, owner=owner)
    capacity_snapshot, runtime, layouts = await _cleanup_capacity_manifest(
        fhir, schema, checkpoint, targets, stages, locked=False
    )
    report_by_field = {
        "variant": "initial_full_swap",
        "checkpoint": coordinates,
        "stages": stages,
        "database": binding,
        "initial": initial,
        "capacity": capacity_snapshot,
        "capacity_geometry": capacity.capacity_geometry_payload(geometry),
        "limits": await project_cleanup_budget(
            fhir, schema, checkpoint, runtime, capacity_snapshot=capacity_snapshot, layouts=layouts
        ),
    }
    _validate_initial_request(report_by_field)
    return report_by_field


async def inspect_failed_profile_cleanup(fhir, *, build_id, owner_run_id, published_run_id=None):
    """Observe one complete failed preimage; this read-only report grants no disposal authority."""
    from sqlalchemy import text as sql_text

    _refuse_inherited_authority(fhir)
    schema = fhir._schema()
    async with fhir.db.transaction() as session:
        await session.execute(sql_text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(sql_text("SET LOCAL statement_timeout='30s'"))
        checkpoint, owner = await _checkpoint_and_owner(fhir, schema, build_id, owner_run_id, locked=False)
        coordinates, geometry = _checkpoint_coordinates(checkpoint, owner)
        manifest = await _stage_manifest(fhir, schema, checkpoint, locked=False)
        binding = await _database_binding(fhir, schema)
        if _cleanup_variant(checkpoint) == "legacy_full_swap":
            publication, serving = await _publication_manifest(
                fhir, schema, checkpoint, coordinates, published_run_id, locked=False
            )
            if {_coordinate_key(stage) for stage in manifest} & {
                _coordinate_key(target_entry) for target_entry in publication["targets"]
            }:
                fail("stage_is_serving")
            capacity_snapshot, geometry, layouts = await _cleanup_capacity_manifest(
                fhir, schema, checkpoint, serving, manifest, locked=False
            )
            return {
                "variant": "legacy_full_swap",
                "checkpoint": coordinates,
                "stages": manifest,
                "database": binding,
                "capacity": capacity_snapshot,
                "publication": publication,
                "limits": await project_cleanup_budget(
                    fhir, schema, checkpoint, geometry, capacity_snapshot=capacity_snapshot, layouts=layouts
                ),
            }
        if published_run_id is not None:
            fail("legacy_arguments_invalid")
        if _cleanup_variant(checkpoint) == "initial_full_swap":
            return await _inspect_initial_cleanup(
                fhir, schema, checkpoint, coordinates, geometry, manifest, binding, owner
            )
        return {
            "checkpoint": coordinates,
            "stages": manifest,
            "database": binding,
            "limits": await project_cleanup_budget(fhir, schema, checkpoint, geometry),
        }


async def _exclude_publication(fhir, schema, checkpoint, coordinates):
    delta = fhir._unscoped_qt(schema, "provider_directory_profile_delta_receipt")
    cms = fhir._unscoped_qt(schema, "provider_directory_cms_serving_receipt")
    await fhir.db.status(f"LOCK TABLE {delta},{cms} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    serving_row = await fhir.db.first(
        f"SELECT * FROM {fhir._provider_directory_profile_serving_generation_ref(schema)} WHERE singleton_key='global' FOR UPDATE NOWAIT"
    )
    if serving_row is None:
        fail("existing_serving_prerequisite_missing")
    serving_by_field = dict(serving_row._mapping)
    if any(serving_by_field.get(field) == coordinates[field] for field in ("selection_proof_id", "control_generation")):
        fail("build_is_serving")
    serving = fhir._profile_serving_state_from_row(serving_row)
    live_names = {_coordinate_key(entry) for entry in serving.targets.values()}
    for coordinate in serving.targets.values():
        await fhir.db.status(
            f"LOCK TABLE {fhir._unscoped_qt(coordinate['schema'], coordinate['name'])} IN ACCESS SHARE MODE NOWAIT"
        )
    if any(
        (schema, name) in live_names
        for name in fhir._validated_profile_checkpoint_stage_names(checkpoint)
        if name is not None
    ):
        fail("stage_is_serving")
    if await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {delta} WHERE build_id=:build OR selection_proof_id=:proof OR control_generation=:generation)",
        build=coordinates["build_id"],
        proof=coordinates["selection_proof_id"],
        generation=coordinates["control_generation"],
    ):
        fail("historical_delta_publication")
    if await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {cms} WHERE payload->'profile'->>'selection_proof_id'=:proof OR payload->'profile'->>'control_generation'=:generation)",
        proof=coordinates["selection_proof_id"],
        generation=str(coordinates["control_generation"]),
    ):
        fail("historical_cms_publication")
    return serving


async def _exclude_competitors(fhir, schema, owner_run_id):
    run = fhir._unscoped_qt(schema, "import_run")
    if await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {run} WHERE importer='provider-directory-fhir' "
        "AND status=ANY(CAST(:statuses AS text[])) AND run_id<>:owner)",
        statuses=list(fhir._PROFILE_ACTIVE_RUN_STATUSES),
        owner=owner_run_id,
    ):
        fail("active_competing_owner")
    checkpoint = fhir._provider_directory_profile_checkpoint_ref(schema)
    if await fhir.db.scalar(f"SELECT EXISTS(SELECT 1 FROM {checkpoint} WHERE state<>'failed')"):
        fail("active_competing_checkpoint")
    preflight = fhir._profile_capacity_preflight_receipt_ref(schema)
    if await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {preflight} WHERE consumed_at IS NULL AND expires_at>clock_timestamp())"
    ):
        fail("unresolved_preflight")


async def _read_claim(fhir, schema, operation_id):
    from sqlalchemy import text as sql_text

    async with fhir.db.session() as session:
        row = (
            (
                await session.execute(
                    sql_text(f"SELECT * FROM {claim_ref(fhir, schema)} WHERE operation_id=:operation"),
                    {"operation": operation_id},
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None


def _claim_envelope(claim):
    envelope_by_field = {"authorization": json.loads(claim["authorization_json"]), "signature": claim["signature"]}
    if (
        canonical(envelope_by_field["authorization"]) != claim["authorization_json"]
        or digest(envelope_by_field) != claim["authorization_sha256"]
    ):
        fail("claim_corrupt")
    body = envelope_by_field["authorization"]
    expected_by_field = {
        "operation_id": body["operation_id"],
        "reservation_id": body["reservation_id"],
        "nonce": body["nonce"],
        "build_id": body["checkpoint"]["build_id"],
        "owner_run_id": body["checkpoint"]["owner_run_id"],
        "checkpoint_preimage_sha256": body["checkpoint"]["preimage_sha256"],
    }
    if any(claim[name] != value for name, value in expected_by_field.items()):
        fail("claim_corrupt")
    if claim["expires_at"] != timestamp(body["expires_at"]) or claim["max_operation_deadline"] != timestamp(
        body["max_operation_deadline"]
    ):
        fail("claim_corrupt")
    return envelope_by_field


async def _commit_spent_claim(fhir, schema, envelope):
    """A second independent transaction spends authority before any stage DROP."""
    from sqlalchemy import text as sql_text

    body = envelope["authorization"]
    params_by_field = {
        "operation": body["operation_id"],
        "digest": digest(envelope),
        "reservation": body["reservation_id"],
        "nonce": body["nonce"],
        "build": body["checkpoint"]["build_id"],
        "owner": body["checkpoint"]["owner_run_id"],
        "preimage": body["checkpoint"]["preimage_sha256"],
        "expires": timestamp(body["expires_at"]),
        "deadline": timestamp(body["max_operation_deadline"]),
        "body": canonical(body),
        "signature": envelope["signature"],
    }
    is_inserted = False
    try:
        async with fhir.db.session() as session, session.begin():
            await session.execute(sql_text("SET LOCAL lock_timeout='1s'"))
            await session.execute(sql_text("SET LOCAL statement_timeout='5s'"))
            mutation_result = await session.execute(
                sql_text(
                    f"INSERT INTO {claim_ref(fhir, schema)} "
                    "(operation_id,authorization_sha256,reservation_id,nonce,build_id,owner_run_id,checkpoint_preimage_sha256,"
                    "expires_at,max_operation_deadline,authorization_json,signature) VALUES "
                    "(:operation,:digest,:reservation,:nonce,:build,:owner,:preimage,:expires,:deadline,:body,:signature) "
                    "ON CONFLICT DO NOTHING"
                ),
                params_by_field,
            )
            if mutation_result.rowcount != 1:
                fail("authorization_already_spent")
            is_inserted = True
    except Exception:
        if not is_inserted:
            raise
        # A lost claim COMMIT acknowledgment may be settled only by exact fresh readback.
        claim = await _read_claim(fhir, schema, body["operation_id"])
        if claim is None or _claim_envelope(claim) != envelope:
            raise
    claim = await _read_claim(fhir, schema, body["operation_id"])
    if claim is None or _claim_envelope(claim) != envelope:
        fail("claim_commit_unresolved")
    return claim


def _receipt_contract(body):
    """Select the completion contract from the closed signed authorization version."""
    return {
        CONTRACT: RECEIPT_CONTRACT,
        LEGACY_CONTRACT: LEGACY_RECEIPT_CONTRACT,
        INITIAL_CONTRACT: INITIAL_RECEIPT_CONTRACT,
    }.get(body["contract_id"])


def completion_suffix(receipt):
    """Append canonical authenticated completion without replacing the failure."""
    exact(
        receipt,
        RECEIPT_FIELDS
        | ({"variant"} if receipt.get("contract_id") in {LEGACY_RECEIPT_CONTRACT, INITIAL_RECEIPT_CONTRACT} else set()),
    )
    return MARKER + canonical(receipt) + "]"


def _completion_marker(error):
    """Decode an object receipt before any claim lookup or preimage validation."""
    position = error.rfind(MARKER)
    if position < 0 or not error.endswith("]"):
        fail("completion_missing")
    try:
        receipt = json.loads(error[position + len(MARKER) : -1])
    except ValueError, TypeError:
        fail("completion_invalid")
    if not isinstance(receipt, dict):
        fail("completion_invalid")
    return position, receipt


def validate_completion_preimage(checkpoint, claim):
    """Restore only original error/timestamp; bind every other retained checkpoint coordinate."""
    error = checkpoint["last_error"] or ""
    position, receipt = _completion_marker(error)
    exact(
        receipt,
        RECEIPT_FIELDS
        | ({"variant"} if receipt.get("contract_id") in {LEGACY_RECEIPT_CONTRACT, INITIAL_RECEIPT_CONTRACT} else set()),
    )
    if error[position:] != completion_suffix(receipt) or receipt["contract_id"] not in {
        RECEIPT_CONTRACT,
        LEGACY_RECEIPT_CONTRACT,
        INITIAL_RECEIPT_CONTRACT,
    }:
        fail("completion_invalid")
    body = _claim_envelope(claim)["authorization"]
    expected_contract = _receipt_contract(body)
    if receipt["contract_id"] != expected_contract or receipt.get("variant") != body.get("variant"):
        fail("completion_variant_changed")
    original_error = error[:position]
    if (
        receipt["operation_id"] != claim["operation_id"]
        or receipt["authorization_sha256"] != claim["authorization_sha256"]
        or receipt["checkpoint_preimage_sha256"] != claim["checkpoint_preimage_sha256"]
        or receipt["original_error_sha256"] != body["checkpoint"]["original_error_sha256"]
        or hashlib.sha256(original_error.encode()).hexdigest() != receipt["original_error_sha256"]
        or receipt["disposed_stages"] != body["stages"]
    ):
        fail("completion_binding_invalid")
    preimage_by_field = dict(
        checkpoint, last_error=original_error, updated_at=datetime.fromisoformat(receipt["original_updated_at"])
    )
    if (
        digest(json_values(preimage_by_field), body["contract_id"] + ".checkpoint")
        != claim["checkpoint_preimage_sha256"]
    ):
        fail("completion_preimage_changed")
    expected_updated = timestamp(receipt["completed_at"])
    if checkpoint["updated_at"].tzinfo is None:
        expected_updated = expected_updated.replace(tzinfo=None)
    if checkpoint["updated_at"] != expected_updated or timestamp(receipt["completed_at"]) < claim["claimed_at"]:
        fail("completion_timestamp_invalid")
    integer(receipt["wal_precommit_bytes"])
    if receipt["wal_precommit_bytes"] > body["limits"]["wal_bytes"]:
        fail("completion_wal_invalid")
    return receipt


async def _reconcile_under_guards(fhir, envelope, trust):
    from sqlalchemy import text as sql_text

    schema = fhir._schema()
    claim = await _read_claim(fhir, schema, envelope["authorization"]["operation_id"])
    if claim is None:
        fail("claim_missing")
    if _claim_envelope(claim) != envelope:
        fail("claim_identity_conflict")
    async with fhir.db.transaction() as session:
        await session.execute(sql_text("SET TRANSACTION READ ONLY"))
        await session.execute(sql_text("SET LOCAL statement_timeout='5s'"))
        checkpoint, _owner = await _checkpoint_and_owner(
            fhir,
            schema,
            claim["build_id"],
            claim["owner_run_id"],
            locked=False,
            variant=envelope["authorization"].get("variant", "source_delta"),
        )
        return await _completed_claim_receipt(fhir, schema, checkpoint, claim, trust)


async def reconcile_failed_profile_cleanup(fhir, envelope, *, cleanup_trust):
    """A spent permit is read-only: exact success replays; every ambiguity refuses."""
    _refuse_inherited_authority(fhir)
    async with fhir._provider_directory_artifact_scope_guard(fhir._schema()):
        async with fhir._provider_directory_profile_build_guards(fhir._schema()):
            return await _reconcile_under_guards(fhir, envelope, cleanup_trust)


async def _accept_fenced_request(fhir, schema, body):
    checkpoint, owner = await _checkpoint_and_owner(
        fhir,
        schema,
        body["checkpoint"]["build_id"],
        body["checkpoint"]["owner_run_id"],
        locked=True,
        variant=body.get("variant", "source_delta"),
    )
    coordinates, geometry = _checkpoint_coordinates(checkpoint, owner)
    if coordinates != body["checkpoint"]:
        fail("checkpoint_preimage_changed")
    if body["database"] != await _database_binding(fhir, schema):
        fail("database_changed")
    if body.get("variant") == "legacy_full_swap":
        return await _accept_legacy_fenced_request(fhir, schema, body, checkpoint, owner, coordinates)
    if body.get("variant") == "initial_full_swap":
        from process import provider_directory_profile_capacity as capacity

        _validate_initial_request(dict(body, capacity_geometry=capacity.capacity_geometry_payload(geometry)))
        initial, initial_targets = await _initial_manifest(
            fhir, schema, geometry, locked=True, checkpoint=checkpoint, owner=owner
        )
        if initial != body["initial"]:
            fail("initial_preimage_changed")
        return await _accept_capacity_fenced_stages(fhir, schema, body, checkpoint, owner, initial_targets)
    serving = await _exclude_publication(fhir, schema, checkpoint, coordinates)
    observed = await _capacity_identity(fhir, schema, serving)
    if any(
        getattr(geometry, field) != observed_setting
        for field, observed_setting in fhir._geometry_postgres_values(observed).items()
    ):
        fail("capacity_runtime_changed")
    await _exclude_competitors(fhir, schema, owner["run_id"])
    if await _stage_manifest(fhir, schema, checkpoint, locked=True) != body["stages"]:
        fail("stage_manifest_changed")
    layouts = await _metadata_layouts(fhir, schema, locked=True)
    projection = await project_cleanup_budget(fhir, schema, checkpoint, geometry, layouts=layouts)
    await _assert_cleanup_budget(fhir, schema, body, projection)
    if len((checkpoint["last_error"] or "").encode()) + 8192 > MAX_BYTES:
        fail("complete_error_exceeds_budget")
    return checkpoint


async def _assert_cleanup_budget(fhir, schema, body, projection):
    if any(body["limits"][field] < projection[field] for field in ("data_bytes", "temp_bytes", "wal_bytes")):
        fail("budget_exceeded")
    reservation_rows = await fhir.db.all(
        f"SELECT reservation_id FROM {fhir._unscoped_qt(schema, 'provider_directory_profile_capacity_lease_consumption')} WHERE expires_at>clock_timestamp() UNION SELECT reservation_id FROM {claim_ref(fhir, schema)} WHERE expires_at>clock_timestamp()"
    )
    if not {row[0] for row in reservation_rows}.issubset(set(body["observations"]["accounted_reservation_ids"])):
        fail("original_reservation_unaccounted")


async def _accept_legacy_fenced_request(fhir, schema, body, checkpoint, owner, coordinates):
    publication, serving = await _publication_manifest(
        fhir, schema, checkpoint, coordinates, body["publication"]["published_run_id"], locked=True
    )
    if publication != body["publication"]:
        fail("publication_preimage_changed")
    if {_coordinate_key(stage) for stage in body["stages"]} & {
        _coordinate_key(target) for target in publication["targets"]
    }:
        fail("stage_is_serving")
    return await _accept_capacity_fenced_stages(fhir, schema, body, checkpoint, owner, serving)


async def _accept_capacity_fenced_stages(fhir, schema, body, checkpoint, owner, serving):
    """Recheck named stages, numeric catalog costs and independently reserved ceilings."""
    await _exclude_competitors(fhir, schema, owner["run_id"])
    stages = await _stage_manifest(fhir, schema, checkpoint, locked=True)
    if stages != body["stages"]:
        fail("stage_manifest_changed")
    capacity_snapshot, runtime, layouts = await _cleanup_capacity_manifest(
        fhir, schema, checkpoint, serving, stages, locked=True
    )
    if {key: value for key, value in capacity_snapshot.items() if key != "observed_at"} != {
        key: value for key, value in body["capacity"].items() if key != "observed_at"
    }:
        fail("capacity_preimage_changed")
    projection = await project_cleanup_budget(
        fhir, schema, checkpoint, runtime, capacity_snapshot=capacity_snapshot, layouts=layouts
    )
    await _assert_cleanup_budget(fhir, schema, body, projection)
    return checkpoint


async def _receipt_tail_bound(fhir, schema, body, checkpoint):
    from types import SimpleNamespace

    from process import provider_directory_profile_capacity as capacity
    from process.provider_directory_profile_capacity_physical import _metadata_mutation_projection

    has_capacity = body.get("variant") in {"legacy_full_swap", "initial_full_swap"}
    runtime = (
        SimpleNamespace(**body["capacity"]["postgres"])
        if has_capacity
        else capacity.validated_capacity_geometry(checkpoint["capacity_geometry_json"])
    )
    layout = (await _metadata_layouts(fhir, schema, locked=False))["checkpoint"]
    old_toast = await fhir._provider_directory_profile_toast_chunk_count(
        source_sql=f"SELECT * FROM {fhir._provider_directory_profile_checkpoint_ref(schema)} WHERE build_id=:build",
        relation_oid=layout.relation_oid,
        toast_oid=layout.toast_oid,
        toastable_columns=layout.toastable_columns,
        expected_compression=runtime.postgres_default_toast_compression,
        params={"build": checkpoint["build_id"]},
    )
    mutation = fhir._provider_directory_profile_control_metadata_input(
        layout, relation_name="build_checkpoint", operation="update", observed_deleted_toast_chunks=old_toast
    )
    stages = await _stage_manifest(fhir, schema, checkpoint, locked=False)
    dependencies = [await _stage_dependency_manifest(fhir, schema, stage) for stage in stages]
    return _metadata_mutation_projection(runtime, mutation)[1] + _commit_wal_bound(dependencies, runtime), has_capacity


async def _dispose_claimed_stages(fhir, schema, envelope, checkpoint, start_lsn):
    body = envelope["authorization"]
    # READ COMMITTED is required to observe the separately committed immutable claim.
    database_record = await fhir.db.first(
        f"SELECT * FROM {claim_ref(fhir, schema)} WHERE operation_id=:operation", operation=body["operation_id"]
    )
    if database_record is None or _claim_envelope(dict(database_record._mapping)) != envelope:
        fail("claim_not_visible")
    tail_wal, has_capacity = await _receipt_tail_bound(fhir, schema, body, checkpoint)
    for name in (name for name in fhir._validated_profile_checkpoint_stage_names(checkpoint) if name is not None):
        await fhir.db.status(f"DROP TABLE {fhir._unscoped_qt(schema, name)} RESTRICT")
    observation = await fhir.db.first(
        "SELECT clock_timestamp() AS completed_at, pg_current_wal_insert_lsn()::text AS lsn, "
        "pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint AS bytes",
        start=start_lsn,
    )
    values_by_field = dict(observation._mapping)
    if values_by_field["bytes"] + tail_wal > body["limits"]["wal_bytes"]:
        fail("observed_wal_exceeded")
    receipt_by_field = {
        "contract_id": _receipt_contract(body),
        "operation_id": body["operation_id"],
        "authorization_sha256": digest(envelope),
        "checkpoint_preimage_sha256": body["checkpoint"]["preimage_sha256"],
        "original_updated_at": checkpoint["updated_at"].isoformat(),
        "original_error_sha256": body["checkpoint"]["original_error_sha256"],
        "disposed_stages": body["stages"],
        "completed_at": values_by_field["completed_at"].isoformat(),
        "wal_start_lsn": start_lsn,
        "wal_precommit_lsn": values_by_field["lsn"],
        "wal_precommit_bytes": values_by_field["bytes"],
    }
    if has_capacity:
        receipt_by_field["variant"] = body["variant"]
    complete_error = (checkpoint["last_error"] or "") + completion_suffix(receipt_by_field)
    if len(complete_error.encode()) > MAX_BYTES or (
        has_capacity
        and 4 * len(canonical(json_values(dict(checkpoint, last_error=complete_error))).encode()) > MAX_BYTES
    ):
        fail("complete_error_exceeds_budget")
    mutation_result = await fhir.db.status(
        f"UPDATE {fhir._provider_directory_profile_checkpoint_ref(schema)} SET last_error=:error, "
        "updated_at=CAST(:completed AS timestamptz) AT TIME ZONE 'UTC' WHERE build_id=:build AND owner_run_id=:owner AND state='failed'",
        error=complete_error,
        completed=values_by_field["completed_at"],
        build=checkpoint["build_id"],
        owner=checkpoint["owner_run_id"],
    )
    if mutation_result != 1:
        fail("checkpoint_update_changed")
    return receipt_by_field


async def _disposal_transaction(fhir, envelope, trust):
    import asyncio

    from sqlalchemy import text as sql_text

    from process.provider_directory_profile_temp_limit import apply_temp_file_limit

    body, schema = envelope["authorization"], fhir._schema()
    remaining = (timestamp(body["max_operation_deadline"]) - _now_utc()).total_seconds()
    if remaining <= 0:
        fail("deadline_reached")
    async with asyncio.timeout(remaining), fhir.db.transaction() as session:
        await session.execute(sql_text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        for setting, timeout_ms in (
            ("lock_timeout", body["limits"]["lock_timeout_ms"]),
            ("statement_timeout", body["limits"]["statement_timeout_ms"]),
            ("transaction_timeout", max(1, int(remaining * 1000))),
        ):
            await session.execute(sql_text(f"SET LOCAL {setting}='{timeout_ms}ms'"))
        await apply_temp_file_limit(fhir.db, 0)
        await session.execute(sql_text("SET LOCAL timezone='UTC'"))
        start_lsn = await fhir.db.scalar("SELECT pg_current_wal_insert_lsn()::text")
        await fhir._lock_profile_capacity_preflight_state(schema)
        await _lock_drop_catalogs(fhir)
        checkpoint = await _accept_fenced_request(fhir, schema, body)
        validate_authorization(envelope, trust=trust, now=_now_utc())
        if 4 * len(canonical(body).encode()) + 1024 > MAX_BYTES:
            fail("complete_claim_payload_unsupported")
        await _commit_spent_claim(fhir, schema, envelope)
        return await _dispose_claimed_stages(fhir, schema, envelope, checkpoint, start_lsn)


async def execute_failed_profile_cleanup(fhir, envelope, *, cleanup_trust, executor_identity):
    """Spend one fresh cleanup permit, dispose exact failed stages, then settle ambiguity."""
    _refuse_inherited_authority(fhir)
    if envelope.get("authorization", {}).get("executor_identity") != executor_identity:
        fail("executor_identity_changed")
    schema = fhir._schema()
    prior = await _read_claim(fhir, schema, envelope["authorization"]["operation_id"])
    if prior is None:
        if cleanup_trust is None:
            cleanup_trust = configured_cleanup_trust()
        validate_authorization(envelope, trust=cleanup_trust, now=_now_utc())
    async with fhir._provider_directory_artifact_scope_guard(schema):
        async with fhir._provider_directory_profile_build_guards(schema):
            if await _read_claim(fhir, schema, envelope["authorization"]["operation_id"]) is not None:
                return await _reconcile_under_guards(fhir, envelope, cleanup_trust)
            validate_authorization(envelope, trust=cleanup_trust, now=_now_utc())
            return await _dispose_and_reconcile_under_guards(fhir, envelope, cleanup_trust)


async def _dispose_and_reconcile_under_guards(fhir, envelope, cleanup_trust):
    import asyncio

    try:
        await _disposal_transaction(fhir, envelope, cleanup_trust)
    except BaseException as original_error:
        # The transaction context has settled rollback/commit before this fresh read.
        reconciliation = asyncio.create_task(_reconcile_under_guards(fhir, envelope, cleanup_trust))
        try:
            return await asyncio.shield(reconciliation)
        except BaseException:
            if not reconciliation.done():
                try:
                    await asyncio.shield(reconciliation)
                except BaseException:
                    raise original_error
            raise original_error
    return await _reconcile_under_guards(fhir, envelope, cleanup_trust)


def _file_identity(file_stat):
    """Ignore read-induced access time; retain identity, permissions and content-change metadata."""
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_mode,
        file_stat.st_uid,
        file_stat.st_gid,
        file_stat.st_nlink,
        file_stat.st_size,
        file_stat.st_mtime_ns,
        file_stat.st_ctime_ns,
    )


def _protected_json_file(path):
    """Read a bounded, stable, current-user-owned private file outside Git and temporary roots."""
    import os
    import stat
    from pathlib import Path

    path = Path(path)
    if not path.is_absolute() or any((parent / ".git").exists() for parent in path.parents):
        fail("private_path_invalid")
    resolved = path.resolve(strict=True)
    if any(resolved.is_relative_to(root) for root in (Path("/tmp").resolve(), Path("/var/tmp").resolve())):
        fail("private_path_invalid")
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_uid != os.geteuid()
        or before.st_nlink != 1
        or not 0 < before.st_size <= 256 * 1024
    ):
        fail("private_file_invalid")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        raw = os.read(descriptor, 256 * 1024 + 1)
        identity = _file_identity(before)
        if (
            identity != _file_identity(os.fstat(descriptor))
            or identity != _file_identity(path.lstat())
            or len(raw) != before.st_size
        ):
            fail("private_file_changed")
    finally:
        os.close(descriptor)
    return raw, json.loads(raw, object_pairs_hook=_unique_object, parse_constant=lambda _value: fail("json_invalid"))


def _unique_object(pairs):
    result_by_field = {}
    for key, value in pairs:
        if key in result_by_field:
            fail("json_duplicate_field")
        result_by_field[key] = value
    return result_by_field


def configured_cleanup_trust():
    """Cleanup has its own default-off file and deployment-authorized digest."""
    import os

    from process.provider_directory_profile_capacity_trust_config import validated_capacity_lease_trust

    path = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_TRUST_FILE")
    expected = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_TRUST_SHA256")
    if not path or not expected:
        fail("independent_trust_missing")
    raw, document = _protected_json_file(path)
    if hashlib.sha256(raw).hexdigest() != hash64(expected):
        fail("independent_trust_digest_changed")
    return validated_capacity_lease_trust(document)


def _configured_cleanup_history():
    """Load independently pinned original public trust documents for completed history only."""
    import os

    from process.provider_directory_profile_capacity_trust_config import validated_capacity_lease_trust

    path = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_HISTORY_FILE")
    expected = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_HISTORY_SHA256")
    if not path or not expected:
        fail("history_trust_missing")
    raw, document = _protected_json_file(path)
    if hashlib.sha256(raw).hexdigest() != hash64(expected):
        fail("history_trust_digest_changed")
    exact(document, {"contract_id", "epochs"})
    if document["contract_id"] != "healthporta.provider-directory.failed-profile-cleanup-trust-history.v1":
        fail("history_trust_contract_invalid")
    epochs = document["epochs"]
    if not isinstance(epochs, list) or not 1 <= len(epochs) <= 16:
        fail("history_trust_epochs_invalid")
    if len({canonical(epoch) for epoch in epochs}) != len(epochs):
        fail("history_trust_epochs_invalid")
    return tuple(_historical_cleanup_trust(epoch, validated_capacity_lease_trust) for epoch in epochs)


def _historical_cleanup_trust(document, validator):
    """Read original pinned key epochs without importing their retired physical bindings."""
    fields = {"contract_id", "signature_algorithm", "environment_id", "attestor_id", "active_key_id", "keys"}
    if isinstance(document, dict) and document.get("contract_id") == "provider-directory-database-capacity-trust-v2":
        exact(
            document, fields | {"database_system_identifier", "database_oid", "database_name", "tablespaces", "volumes"}
        )
        authority_by_field = {name: document[name] for name in fields}
        authority_by_field["contract_id"] = "provider-directory-database-capacity-trust-v3"
        return validator(authority_by_field)
    return validator(document)


def _validate_completed_authorization(envelope, claim, trust):
    """Verify original signed authority only after complete durable disposal evidence."""
    current = trust
    if current is None:
        try:
            current = configured_cleanup_trust()
        except RuntimeError, OSError, ValueError:
            current = None
    try:
        validate_authorization(envelope, trust=current, now=_now_utc(), claimed_at=claim["claimed_at"])
        return
    except RuntimeError as error:
        if str(error) not in {
            "failed_profile_cleanup_" + reason
            for reason in (
                "independent_trust_missing",
                "trust_binding_invalid",
                "trust_key_invalid",
                "trust_key_retired_invalid",
                "signature_invalid",
            )
        }:
            raise
    for original_trust in _configured_cleanup_history():
        try:
            validate_authorization(
                envelope, trust=original_trust, now=claim["claimed_at"], claimed_at=claim["claimed_at"]
            )
        except RuntimeError:
            continue
        return
    fail("history_trust_epoch_invalid")


async def _completed_claim_receipt(fhir, schema, checkpoint, claim, trust):
    """Historical verification cannot precede exact completion, database and absent-stage proof."""
    envelope = _claim_envelope(claim)
    if envelope["authorization"].get("contract_id") in HISTORICAL_CONTRACTS:
        authenticate_historical_authorization(envelope, trust=trust, now=_now_utc(), claimed_at=claim["claimed_at"])
        fail("legacy_semantic_evidence_required")
    receipt = validate_completion_preimage(checkpoint, claim)
    binding = await _database_binding(fhir, schema)
    if any(envelope["authorization"]["database"][field] != value for field, value in binding.items()):
        fail("database_changed")
    for name in (name for name in fhir._validated_profile_checkpoint_stage_names(checkpoint) if name is not None):
        if await fhir.db.scalar("SELECT to_regclass(:name) IS NOT NULL", name=fhir._unscoped_qt(schema, name)):
            fail("disposed_stage_present")
    _validate_completed_authorization(envelope, claim, trust)
    return receipt


async def validate_disposed_checkpoint(fhir, schema, checkpoint):
    """A stale capacity checkpoint requires authentic durable receipt, never just a marker."""
    _position, receipt = _completion_marker(checkpoint.get("last_error") or "")
    row = await fhir.db.first(
        f"SELECT * FROM {claim_ref(fhir, schema)} WHERE operation_id=:operation", operation=receipt.get("operation_id")
    )
    if row is None:
        fail("claim_missing")
    claim_by_field = dict(row._mapping)
    await _completed_claim_receipt(fhir, schema, checkpoint, claim_by_field, None)


def _executor_source_manifest(root):
    """Bind the fixed entrypoint/import closure to content bytes, independent of file identity."""
    pending_paths = [
        "process/provider_directory_profile_failed_cleanup.py",
        "process/provider_directory_fhir.py",
        "db/connection.py",
        "requirements.txt",
    ]
    manifest_by_path = {}
    while pending_paths:
        name = pending_paths.pop()
        if name in manifest_by_path:
            continue
        source_bytes = _executor_source_bytes(root / name)
        if source_bytes != _executor_source_bytes(root / name):
            fail("executor_source_changed")
        manifest_by_path[name] = hashlib.sha256(source_bytes).hexdigest()
        if name.endswith(".py"):
            pending_paths.extend(_executor_local_import_paths(root, name, source_bytes))
    return dict(sorted(manifest_by_path.items()))


def _executor_source_bytes(path):
    import os

    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        content = os.read(descriptor, 8 * 1024 * 1024 + 1)
        if len(content) > 8 * 1024 * 1024:
            fail("executor_source_unsupported")
        if os.read(descriptor, 1):
            fail("executor_source_changed")
        return content
    finally:
        os.close(descriptor)


def _executor_local_import_paths(root, name, source_bytes):
    import ast
    from pathlib import PurePosixPath

    package_parts = list(PurePosixPath(name).parent.parts)
    candidates = set()
    for node in ast.walk(ast.parse(source_bytes)):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            prefix = package_parts[: len(package_parts) - node.level + 1] if node.level else []
            base = ".".join((*prefix, *((node.module or "").split(".") if node.module else ())))
            modules = [base, *(base + "." + alias.name for alias in node.names if alias.name != "*")]
        else:
            continue
        for module in modules:
            parts = module.split(".")
            if not parts or parts[0] not in {"process", "db", "api", "service", "support"}:
                continue
            for relative in ("/".join(parts) + ".py", "/".join(parts) + "/__init__.py"):
                if (root / relative).is_file():
                    candidates.add(relative)
            for position in range(1, len(parts)):
                initializer = "/".join(parts[:position]) + "/__init__.py"
                if (root / initializer).is_file():
                    candidates.add(initializer)
    return sorted(candidates)


async def observed_executor_identity(fhir):
    """Measure the installed source and real image-baked runtime before accepting authority."""
    from pathlib import Path

    from process.provider_directory_profile_runtime_observation import observe_profile_runtime

    root = Path(__file__).resolve().parents[1]
    manifest_by_field = _executor_source_manifest(root)
    return {
        "runtime_sha256": digest(await observe_profile_runtime(fhir.db), CONTRACT + ".runtime"),
        "release_digest": digest(manifest_by_field, CONTRACT + ".executor-release"),
    }


async def _operator(arguments):
    import importlib

    fhir = importlib.import_module("process.provider_directory_fhir")
    await fhir.db.connect()
    try:
        identity = await observed_executor_identity(fhir)
        if arguments.mode == "inspect":
            if not arguments.build_id or not arguments.owner_run_id or arguments.private_input_file:
                fail("arguments_invalid")
            report = await inspect_failed_profile_cleanup(
                fhir,
                build_id=arguments.build_id,
                owner_run_id=arguments.owner_run_id,
                published_run_id=arguments.published_run_id,
            )
            report["executor_identity"] = identity
            return report
        if (
            not arguments.private_input_file
            or arguments.build_id
            or arguments.owner_run_id
            or arguments.published_run_id
        ):
            fail("arguments_invalid")
        _raw, envelope = _protected_json_file(arguments.private_input_file)
        if arguments.mode == "reconcile":
            return await reconcile_failed_profile_cleanup(fhir, envelope, cleanup_trust=None)
        return await execute_failed_profile_cleanup(fhir, envelope, cleanup_trust=None, executor_identity=identity)
    finally:
        await fhir.db.disconnect()


def main():
    """Only explicit protected inspect, execute and reconcile actions are supported."""
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("inspect", "execute", "reconcile"))
    parser.add_argument("--build-id")
    parser.add_argument("--owner-run-id")
    parser.add_argument("--published-run-id")
    parser.add_argument("--private-input-file")
    print(canonical(asyncio.run(_operator(parser.parse_args()))))


def _validated_cleanup_capacity(body):
    """Validate numeric runtime/layout bounds and named metadata coordinates."""
    capacity = exact(
        body["capacity"],
        {
            "observed_at",
            "postgres",
            "checkpoint_layout",
            "claim_layout",
            "stage_dependencies",
            "commit_wal_upper_bytes",
        },
    )
    timestamp(capacity["observed_at"])
    postgres = exact(capacity["postgres"], POSTGRES_FIELDS)
    for field, observed_setting in postgres.items():
        if field in {"postgres_full_page_writes", "postgres_wal_log_hints", "postgres_data_checksums"}:
            if type(observed_setting) is not bool:
                fail("capacity_invalid")
        elif field in {"postgres_wal_level", "postgres_wal_compression", "postgres_default_toast_compression"}:
            text(observed_setting)
        else:
            integer(observed_setting, 1)
    for role in ("checkpoint_layout", "claim_layout"):
        fields = {"schema", "name", "main_index_pages", "toast_index_pages", "toastable_column_count"}
        if role == "checkpoint_layout":
            fields |= {"old_toast_chunks", "row_payload_upper_bytes"}
        layout = exact(capacity[role], fields)
        _named_coordinate({key: layout[key] for key in ("schema", "name")})
        for name in ("main_index_pages", "toast_index_pages"):
            pages = layout[name]
            if not isinstance(pages, list) or len(pages) > 64 or (name == "main_index_pages" and not pages):
                fail("capacity_layout_invalid")
            for page in pages:
                integer(page, 1)
        integer(layout["toastable_column_count"], 0, 128)
    integer(capacity["checkpoint_layout"]["old_toast_chunks"])
    integer(capacity["checkpoint_layout"]["row_payload_upper_bytes"], 1, MAX_BYTES)
    integer(capacity["commit_wal_upper_bytes"], 1)
    return capacity


if __name__ == "__main__":
    main()
