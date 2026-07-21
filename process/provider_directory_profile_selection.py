# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable authority and runtime checks for global Profile selection."""

from __future__ import annotations

import json
from typing import Any, Mapping

from db.models import (
    ProviderDirectoryProfileSelectionAuthority,
    ProviderDirectoryProfileSelectionObservation,
    ProviderDirectoryProfileSelectionProof,
    db,
)
from process.provider_directory_profile_selection_contract import (
    PROFILE_EXECUTION_CONTRACT_ID,
    PROFILE_SELECTION_ATTESTATION_CONTRACT_ID,
    PROFILE_SELECTION_LINEAGE_AUTHORITY,
    PROFILE_SELECTION_REQUEST_CONTRACT_ID,
    PROFILE_SELECTION_RESULT_CONTRACT_ID,
    PROFILE_SELECTION_RESULT_METRIC,
    ProviderDirectoryProfileExecution,
    ProviderDirectoryProfileSelectionAttestation,
    ProviderDirectoryProfileSelectionDrift,
    ProviderDirectoryProfileSelectionError,
    ProviderDirectoryProfileSelectionStale,
    _ATTESTATION_FIELDS,
    _GLOBAL_PROFILE_PARAMS,
    _clean_text,
    _identity_without_authority,
    _positive_integer,
    _proof_id,
    _required_hash,
    _required_text,
    configured_node_id,
    profile_selection_result,
    stable_hash,
    validated_profile_execution,
    validated_profile_selection_attestation,
)
from process.provider_directory_profile_selection_snapshot import (
    _ComputedProfileSelection,
    _compute_current_selection,
    _computed_selection_from_rows,
    _row_mapping,
    _table_ref,
)


_AUTHORITY_KEY = "global"
_AUTHORITY_LOCK_KEY = "provider-directory-profile-selection-authority:v1"
_REQUEST_FIELDS = {
    "contract_id",
    "node_id",
    "catalog_digest",
    "selection_fingerprint",
    "datasets",
}
_REQUEST_DATASET_FIELDS = {"source_id", "dataset_id"}


def _validated_request(request_value: Any) -> dict[str, Any]:
    if not isinstance(request_value, Mapping) or set(request_value) != _REQUEST_FIELDS:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection request fields are invalid"
        )
    request_map = dict(request_value)
    if request_map.get("contract_id") != PROFILE_SELECTION_REQUEST_CONTRACT_ID:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection request contract is invalid"
        )
    datasets = _validated_dataset_collection(request_map.get("datasets"))
    return {
        "contract_id": PROFILE_SELECTION_REQUEST_CONTRACT_ID,
        "node_id": _required_text(request_map, "node_id", limit=64),
        "catalog_digest": _required_hash(request_map, "catalog_digest"),
        "selection_fingerprint": _required_hash(
            request_map,
            "selection_fingerprint",
        ),
        "datasets": datasets,
    }


def _validated_dataset_collection(raw_datasets: Any) -> list[dict[str, str]]:
    if not isinstance(raw_datasets, list):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection request datasets are invalid"
        )
    datasets = [
        _validated_dataset_projection(raw_dataset)
        for raw_dataset in raw_datasets
    ]
    sorted_datasets = sorted(
        datasets,
        key=lambda dataset_map: (
            dataset_map["source_id"],
            dataset_map["dataset_id"],
        ),
    )
    dataset_keys = [
        (dataset_map["source_id"], dataset_map["dataset_id"])
        for dataset_map in datasets
    ]
    if datasets != sorted_datasets or len(dataset_keys) != len(set(dataset_keys)):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection request datasets are not unique and sorted"
        )
    return datasets


def _validated_dataset_projection(raw_dataset: Any) -> dict[str, str]:
    if (
        not isinstance(raw_dataset, Mapping)
        or set(raw_dataset) != _REQUEST_DATASET_FIELDS
    ):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection request dataset fields are invalid"
        )
    dataset_map = dict(raw_dataset)
    return {
        "source_id": _required_text(dataset_map, "source_id", limit=96),
        "dataset_id": _required_text(dataset_map, "dataset_id", limit=128),
    }


def _input_identity_digest(identity_map: Mapping[str, Any]) -> str:
    return stable_hash(
        identity_map,
        domain="provider_directory_profile_selection_authority_input.v1",
    )


async def _registered_proof(
    input_identity_digest: str,
) -> dict[str, Any] | None:
    database_row = await db.first(
        f"SELECT proof_id, identity_json "
        f"FROM {_table_ref(ProviderDirectoryProfileSelectionProof)} "
        "WHERE input_identity_digest = :input_identity_digest;",
        input_identity_digest=input_identity_digest,
    )
    if database_row is None:
        return None
    registered_map = _row_mapping(database_row)
    identity_json = registered_map.get("identity_json")
    if not isinstance(identity_json, Mapping):
        raise RuntimeError("provider_directory_profile_selection_registry_corrupt")
    return {
        "proof_id": registered_map.get("proof_id"),
        "identity_json": dict(identity_json),
    }


async def _latest_registered_observation() -> dict[str, Any] | None:
    database_row = await db.first(
        f"SELECT input_identity_digest, payload_json "
        f"FROM {_table_ref(ProviderDirectoryProfileSelectionObservation)} "
        "ORDER BY authority_revision DESC LIMIT 1;"
    )
    if database_row is None:
        return None
    observation_map = _row_mapping(database_row)
    payload_json = observation_map.get("payload_json")
    if not isinstance(payload_json, Mapping):
        raise RuntimeError("provider_directory_profile_selection_registry_corrupt")
    return {
        "input_identity_digest": observation_map.get(
            "input_identity_digest"
        ),
        "payload_json": dict(payload_json),
    }


async def _next_authority_revision() -> int:
    await db.status(
        f"""
        INSERT INTO {_table_ref(ProviderDirectoryProfileSelectionAuthority)} (
            authority_key, last_revision, created_at, updated_at
        ) VALUES (:authority_key, 0, now(), now())
        ON CONFLICT (authority_key) DO NOTHING;
        """,
        authority_key=_AUTHORITY_KEY,
    )
    revision_row = await db.first(
        f"""
        UPDATE {_table_ref(ProviderDirectoryProfileSelectionAuthority)}
           SET last_revision = last_revision + 1,
               updated_at = now()
         WHERE authority_key = :authority_key
        RETURNING last_revision;
        """,
        authority_key=_AUTHORITY_KEY,
    )
    revision = int(_row_mapping(revision_row).get("last_revision") or 0)
    if revision < 1:
        raise RuntimeError("provider_directory_profile_selection_revision_invalid")
    return revision


def _attestation_payload(
    identity_map: Mapping[str, Any],
    authority_revision: int,
) -> dict[str, Any]:
    identity_with_revision_map = {
        **identity_map,
        "authority_revision": authority_revision,
    }
    unordered_attestation_map = {
        **identity_with_revision_map,
        "proof_id": _proof_id(identity_map),
    }
    field_order = (
        "contract_id",
        "proof_id",
        "node_id",
        "catalog_digest",
        "selection_fingerprint",
        "authority_revision",
        "profile_schema_version",
        "profile_strategy_version",
        "source_context_digest",
        "profile_input_digest",
        "operation",
        "pairs",
    )
    return {name: unordered_attestation_map[name] for name in field_order}


async def _ensure_selection_proof(
    identity_digest: str,
    identity_map: Mapping[str, Any],
) -> None:
    proof_id = _proof_id(identity_map)
    registered_proof = await _registered_proof(identity_digest)
    if registered_proof is not None:
        if registered_proof != {
            "proof_id": proof_id,
            "identity_json": dict(identity_map),
        }:
            raise RuntimeError("provider_directory_profile_selection_registry_corrupt")
        return
    await db.status(
        f"""
        INSERT INTO {_table_ref(ProviderDirectoryProfileSelectionProof)} (
            input_identity_digest, proof_id, identity_json, created_at
        ) VALUES (
            :input_identity_digest, :proof_id,
            CAST(:identity_json AS jsonb), now()
        );
        """,
        input_identity_digest=identity_digest,
        proof_id=proof_id,
        identity_json=json.dumps(
            identity_map,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


async def _store_observation(
    identity_digest: str,
    attestation_map: Mapping[str, Any],
) -> None:
    await db.status(
        f"""
        INSERT INTO {_table_ref(ProviderDirectoryProfileSelectionObservation)} (
            authority_revision, input_identity_digest, payload_json, created_at
        ) VALUES (
            :authority_revision, :input_identity_digest,
            CAST(:payload_json AS jsonb), now()
        );
        """,
        input_identity_digest=identity_digest,
        authority_revision=attestation_map["authority_revision"],
        payload_json=json.dumps(
            attestation_map,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


async def _register_selection_proof(
    computed_selection: _ComputedProfileSelection,
) -> ProviderDirectoryProfileSelectionAttestation:
    """Replay or allocate one serialized durable authority revision."""

    identity_map = computed_selection.identity_payload
    identity_digest = _input_identity_digest(identity_map)
    await db.scalar(
        "SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0));",
        lock_key=_AUTHORITY_LOCK_KEY,
    )
    latest_observation = await _latest_registered_observation()
    if latest_observation is not None:
        latest_attestation = validated_profile_selection_attestation(
            latest_observation["payload_json"]
        )
        if latest_observation["input_identity_digest"] == identity_digest:
            if _identity_without_authority(latest_attestation.payload) != identity_map:
                raise RuntimeError(
                    "provider_directory_profile_selection_registry_corrupt"
                )
            return latest_attestation
    await _ensure_selection_proof(identity_digest, identity_map)
    authority_revision = await _next_authority_revision()
    attestation_map = _attestation_payload(identity_map, authority_revision)
    if attestation_map["proof_id"] != _proof_id(identity_map):
        raise RuntimeError("provider_directory_profile_selection_registry_corrupt")
    await _store_observation(identity_digest, attestation_map)
    return validated_profile_selection_attestation(attestation_map)


def _expected_request(
    computed_selection: _ComputedProfileSelection,
    node_id: str,
) -> dict[str, Any]:
    identity_map = computed_selection.identity_payload
    return {
        "contract_id": PROFILE_SELECTION_REQUEST_CONTRACT_ID,
        "node_id": node_id,
        "catalog_digest": identity_map["catalog_digest"],
        "selection_fingerprint": identity_map["selection_fingerprint"],
        "datasets": list(computed_selection.request_projection),
    }


async def attest_profile_selection(
    request_payload: Any,
    catalog_map: Mapping[str, Any],
) -> dict[str, Any]:
    """Independently attest the caller's exact current global selection."""

    request_map = _validated_request(request_payload)
    node_id = configured_node_id()
    async with db.transaction():
        await db.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
        computed_selection = await _compute_current_selection(
            catalog_map,
            node_id=node_id,
            lock_selection=True,
        )
        if request_map != _expected_request(computed_selection, node_id):
            raise ProviderDirectoryProfileSelectionDrift(
                "provider_directory_profile_selection_drift"
            )
    async with db.transaction():
        attestation = await _register_selection_proof(computed_selection)
    try:
        await assert_registered_profile_selection_current(
            attestation,
            catalog_map,
        )
    except ProviderDirectoryProfileSelectionStale as exc:
        raise ProviderDirectoryProfileSelectionDrift(
            "provider_directory_profile_selection_drift"
        ) from exc
    return dict(attestation.payload)


async def _assert_registered_current_in_transaction(
    attestation: ProviderDirectoryProfileSelectionAttestation,
    catalog_map: Mapping[str, Any],
) -> None:
    computed_selection = await _compute_current_selection(
        catalog_map,
        node_id=configured_node_id(),
        lock_selection=True,
    )
    identity_map = computed_selection.identity_payload
    if _identity_without_authority(attestation.payload) != identity_map:
        raise ProviderDirectoryProfileSelectionStale(
            "provider_directory_profile_selection_changed"
        )
    identity_digest = _input_identity_digest(identity_map)
    registered_proof = await _registered_proof(identity_digest)
    if registered_proof != {
        "proof_id": attestation.proof_id,
        "identity_json": identity_map,
    }:
        raise ProviderDirectoryProfileSelectionStale(
            "provider_directory_profile_selection_proof_not_registered"
        )
    latest_observation = await _latest_registered_observation()
    if (
        latest_observation is None
        or latest_observation["input_identity_digest"] != identity_digest
        or latest_observation["payload_json"] != attestation.payload
    ):
        raise ProviderDirectoryProfileSelectionStale(
            "provider_directory_profile_selection_proof_not_registered"
        )


async def assert_registered_profile_selection_current(
    attestation: ProviderDirectoryProfileSelectionAttestation,
    catalog_map: Mapping[str, Any],
) -> None:
    """Recompute a proof before staging in its own short locked snapshot."""

    async with db.transaction():
        await db.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
        await _assert_registered_current_in_transaction(attestation, catalog_map)


async def assert_profile_selection_current_in_transaction(
    attestation: ProviderDirectoryProfileSelectionAttestation,
    catalog_map: Mapping[str, Any],
) -> None:
    """Recompute a proof under the caller's atomic cutover transaction."""

    await _assert_registered_current_in_transaction(attestation, catalog_map)
