# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read an exact committed composite result without granting fresh build authority."""

import re
from types import SimpleNamespace

from process import provider_directory_cms_serving_receipt as common
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_contract as contract


def _stale(fhir, reason):
    return fhir.ProviderDirectoryArtifactBuildStale("cms_serving_replay_" + reason)


async def _registered_execution(fhir, execution):
    """Require the original immutable proof and authority observation, not the latest one."""
    attestation = execution.attestation
    if contract.validated_profile_selection_attestation(attestation.payload) != attestation:
        raise _stale(fhir, "attestation_changed")
    identity = contract._identity_without_authority(attestation.payload)
    digest = selection._input_identity_digest(identity)
    if await selection._registered_proof(digest) != {
        "proof_id": attestation.proof_id,
        "identity_json": identity,
    }:
        raise _stale(fhir, "proof_not_registered")
    row = await fhir.db.first(
        f"SELECT input_identity_digest,payload_json FROM "
        f"{selection._table_ref(selection.ProviderDirectoryProfileSelectionObservation)} "
        "WHERE authority_revision=:revision;",
        revision=attestation.authority_revision,
    )
    observed = fhir._pagination_checkpoint_row_mapping(row) if row is not None else {}
    if observed != {"input_identity_digest": digest, "payload_json": attestation.payload}:
        raise _stale(fhir, "observation_not_registered")


async def _historical_common(fhir, schema, generation_id):
    """Use the generation index to retain the original result through address-only successors."""
    row = await fhir.db.first(
        f"SELECT receipt_id,payload FROM {fhir._unscoped_qt(schema, common._TABLE)} "
        "WHERE profile_generation_id=:generation_id "
        "ORDER BY created_at,receipt_id LIMIT 1;",
        generation_id=generation_id,
    )
    if row is None:
        return None
    result_by_field = dict(fhir._pagination_checkpoint_row_mapping(row))
    common.validate_receipt_payload(result_by_field["payload"])
    return result_by_field


def _assert_common_execution(fhir, execution, delta, receipt):
    """Match historical native fields and exact source pins to the authenticated delta."""
    attestation, payload_by_field = execution.attestation, receipt["payload"]
    expected_selection_by_field = {
        "proof_id": attestation.proof_id,
        "fingerprint": attestation.selection_fingerprint,
        "catalog_digest": attestation.catalog_digest,
    }
    pins = [{key: pair[key] for key in common._PIN_FIELDS} for pair in attestation.pairs]
    source_pairs = tuple((pair["source_id"], pair["dataset_id"]) for pair in pins)
    expected_profile_by_field = {
        key: delta[key]
        for key in (
            "operation",
            "control_generation",
            "generation_id",
            "selection_proof_id",
            "authority_revision",
            "profile_as_of",
            "executable_plan_hash",
            "evidence_target_oid",
            "profile_target_oid",
            "evidence_rows",
            "profile_rows",
        )
    }
    expected_profile_by_field.update(
        status="purged" if attestation.operation == "purge" else "published",
        profile_schema_version=attestation.profile_schema_version,
        profile_strategy_version=attestation.profile_strategy_version,
        source_vector_hash=delta["to_source_vector_hash"],
        source_context_vector_hash=delta["to_source_context_vector_hash"],
    )
    if (
        payload_by_field["profile"] != expected_profile_by_field
        or payload_by_field["selection"] != expected_selection_by_field
        or (
            payload_by_field["desired_datasets"] != pins
            or fhir._provider_directory_profile_source_vector_hash(source_pairs) != delta["to_source_vector_hash"]
            or (
                attestation.desired_profile_as_of is not None
                and attestation.desired_profile_as_of != delta["profile_as_of"]
            )
        )
    ):
        raise _stale(fhir, "common_result_changed")
    desired = attestation.desired_cms_dataset
    if desired is not None:
        incumbent = attestation.expected_cms_incumbent
        if {key: payload_by_field["cms"][key] for key in common._PIN_FIELDS} != {
            key: desired[key] for key in common._PIN_FIELDS
        } or (
            payload_by_field["expected_incumbent"]
            != ({key: incumbent[key] for key in common._PIN_FIELDS} if incumbent else None)
        ):
            raise _stale(fhir, "cms_identity_changed")
    if payload_by_field["cms"]["proof_version"] != 2:
        raise _stale(fhir, "coverage_version_changed")
    return source_pairs


async def _assert_database(fhir, lease):
    """Check cluster/database authority without requiring superseded relation OIDs to survive."""
    row = await fhir.db.first(
        "SELECT c.system_identifier::text AS database_system_identifier, "
        "d.oid::bigint AS database_oid,d.datname::text AS database_name "
        "FROM pg_database d CROSS JOIN pg_control_system() c WHERE d.datname=current_database();"
    )
    expected_by_field = {
        key: getattr(lease, key) for key in ("database_system_identifier", "database_oid", "database_name")
    }
    if row is None or dict(fhir._pagination_checkpoint_row_mapping(row)) != expected_by_field:
        raise _stale(fhir, "database_changed")


async def _replay(fhir, execution, run_id, metrics):
    """Reuse immutable Profile ownership, signature, geometry, and timeline validation."""
    schema = fhir._schema()
    await fhir._replay_control_run(schema, run_id)
    consumption_ref = fhir._unscoped_qt(schema, fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__)
    current = await fhir._replay_current_consumption(consumption_ref, run_id)
    delta = await fhir._replay_exact_receipt(schema, execution, current)
    if delta is None:
        return None
    receipt = await _historical_common(fhir, schema, delta["generation_id"])
    if receipt is None:
        if execution.attestation.operation == "purge" and not execution.attestation.desired_cms_dataset:
            if await _historical_common(fhir, schema, delta["from_generation_id"]) is None:
                return None
        raise _stale(fhir, "common_receipt_missing")
    await _registered_execution(fhir, execution)
    owner = fhir._provider_directory_profile_replay_receipt_owner_run_id(delta)
    if owner != run_id and current is not None:
        raise _stale(fhir, "current_consumption_conflict")
    consumption = await fhir._replay_bound_consumption(consumption_ref, owner, delta["build_id"])
    await fhir._assert_replay_owner(schema, owner, run_id)
    geometry, lease = fhir._replay_capacity_artifacts(consumption, delta, execution)
    fhir._assert_replay_timeline(consumption, delta, lease)
    await _assert_database(fhir, lease)
    source_pairs = _assert_common_execution(fhir, execution, delta, receipt)
    serving = SimpleNamespace(
        generation_id=delta["generation_id"], profile_as_of=delta["profile_as_of"], source_vector=source_pairs
    )
    result_by_field = dict(metrics)
    result_by_field["profile"] = fhir._provider_directory_profile_replay_metrics(
        delta, serving, geometry, lease, owner, run_id
    )
    payload_by_field = receipt["payload"]
    result_by_field["cms_serving"] = {
        "receipt_id": receipt["receipt_id"],
        "dataset_id": payload_by_field["cms"]["dataset_id"],
        "profile_generation_id": delta["generation_id"],
        "address_generation": payload_by_field["address"]["local_generation"],
        "doctors_generation": payload_by_field["doctors"]["local_generation"],
        "recovered_commit": True,
    }
    result_by_field["artifact_dataset_ids"] = sorted(
        pair["dataset_id"] for pair in payload_by_field["desired_datasets"]
    )
    fhir._attach_profile_selection_result(execution, result_by_field)
    return result_by_field


async def replay_committed_cms_profile(fhir, *, run_id, control_run_id, execution, metrics):
    """Return only historical completion; a miss still requires ordinary current selection admission."""
    attestation = execution.attestation
    if not (
        attestation.desired_cms_dataset
        or attestation.operation == "purge"
        or any(pair["source_id"] == "cms-npd" for pair in attestation.pairs)
    ):
        return None
    if not run_id or run_id != control_run_id or re.fullmatch(r"run_[0-9a-f]{32}", run_id) is None:
        return None
    async with fhir.db.transaction():
        await fhir.db.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;")
        await fhir.db.status("SET LOCAL statement_timeout='5s';")
        return await _replay(fhir, execution, run_id, metrics)
