# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain practitioner roles that differ only per network as one role."""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Mapping

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_resource_hash import (
    RESOURCE_TRANSPORT_PAYLOAD_FIELDS,
    _stable_json,
    is_semantic_resource_hash_contract,
    resource_payload_sha256_for_contract,
)
from process.provider_directory_resource_hash import (
    merge_semantic_resource_payloads as _merge_equal_content_payloads,
)

_SHA256_HEX_RE = re.compile(r"[0-9a-f]{64}")
# Each staged observation commits to the source roles it stands for, so completion can still refuse one source
# role that arrived with two contents. Completion does not retain these.
NATIVE_ROLES = "native_roles"
_ROLE_IDENTITY_FIELDS = frozenset({"resource_id", "fhir_meta", NATIVE_ROLES, *RESOURCE_TRANSPORT_PAYLOAD_FIELDS})
_ROLE_NETWORK_FIELDS = ("network_refs", "identifiers", "insurance_plan_refs")
_ROLE_ACCEPTANCE_FIELDS = ("accepting_patients", "new_patient_acceptance")
MERGED_ROLE_SOURCES_ENV = "HLTHPRT_PROVIDER_DIRECTORY_MERGED_ROLE_SOURCES"
# A repeat is a stored observation of a role the loader already holds: only its new per-network entries.
_REPEAT = "merged_role_repeat"
_REPEAT_FIELDS = (*_ROLE_NETWORK_FIELDS, *_ROLE_ACCEPTANCE_FIELDS, NATIVE_ROLES)
_ORDER_INDEX = '(resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal)'
# One index probe per role: its first staged observation, which is always a full one. The stored text is
# returned as it is because extracting fields in the database costs several times the probe itself.
_FIRST_STAGED_ROLE_SQL = (
    "SELECT role_id AS resource_id,first.payload_json FROM unnest($1::text[]) role_id CROSS JOIN LATERAL ("
    "SELECT payload_json FROM {relation} "
    'WHERE resource_type COLLATE "C"=\'PractitionerRole\' AND resource_id COLLATE "C"=role_id '
    "AND batch_ordinal>greatest($2::bigint,coalesce(($3::jsonb->>'PractitionerRole')::bigint,0)) "
    "ORDER BY batch_ordinal,row_ordinal LIMIT 1) first"
)
_ordered_input_by_name: dict[str, bool] = {}


def _opted_in_source_ids() -> set[str]:
    return {name.strip() for name in os.getenv(MERGED_ROLE_SOURCES_ENV, "").split(",") if name.strip()}


def _entry_sha256(name: str, entry: Any) -> str:
    return hashlib.sha256(_stable_json([name, entry]).encode()).hexdigest()


class _HashedRole(dict):
    """A finished merged role that keeps its commitments, so the repeat check and the proof shard reuse them."""

    __slots__ = ("components",)


def _role_network_fields(payload_by_field: Mapping[str, Any]) -> list[str]:
    """Name the list fields that vary per network within one merged role."""

    names = list(_ROLE_NETWORK_FIELDS)
    for name in _ROLE_ACCEPTANCE_FIELDS:
        entries = payload_by_field.get(name)
        # Acceptance without its own network would lose which network it describes.
        if (
            isinstance(entries, list)
            and entries
            and all(isinstance(entry, Mapping) and entry.get("from_network_ref") for entry in entries)
        ):
            names.append(name)
    return names


def _role_union(*lists: Any) -> list[Any]:
    entry_by_identity = {_stable_json(entry): entry for entries in lists if entries for entry in entries}
    return [entry_by_identity[identity] for identity in sorted(entry_by_identity)]


def merged_role_id(payload_by_field: Mapping[str, Any]) -> str:
    """Identify the roles that differ only in their per-network fields."""

    network_fields = _role_network_fields(payload_by_field)
    meta_by_name = payload_by_field.get("fhir_meta")
    if isinstance(meta_by_name, Mapping):
        meta_by_name = {name: value for name, value in meta_by_name.items() if name not in {"versionId", "lastUpdated"}}
    varying_fields = {*_ROLE_IDENTITY_FIELDS, *network_fields}
    body_by_field = {name: value for name, value in payload_by_field.items() if name not in varying_fields}
    # A role that names no network stays apart from the roles that do.
    identity_parts = [body_by_field, meta_by_name, network_fields, bool(payload_by_field.get("network_refs"))]
    return hashlib.sha256(_stable_json(identity_parts).encode()).hexdigest()


def merged_role_payload(payload_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Re-key one role to its merge identity with canonical per-network lists."""

    merged_by_field = dict(payload_by_field)
    for name in _role_network_fields(payload_by_field):
        if isinstance(merged_by_field.get(name), list):
            merged_by_field[name] = _role_union(merged_by_field[name])
    merged_by_field["resource_id"] = merged_role_id(payload_by_field)
    return merged_by_field


def composed_merged_role_sha256(base_hash: str, entry_hashes: list[str]) -> str:
    """Compose a lossless cross-batch merged-role content commitment."""

    return hashlib.sha256(
        _stable_json(["merged-practitioner-role", base_hash, sorted(entry_hashes)]).encode()
    ).hexdigest()


def _is_merged_role(payload_by_field: Mapping[str, Any]) -> bool:
    """Whether a payload is a merged role or a repeat of one: its identity is a content hash it matches."""

    role_id = payload_by_field.get("resource_id")
    if type(role_id) is not str or _SHA256_HEX_RE.fullmatch(role_id) is None:
        return False
    if payload_by_field.get(_REPEAT) is True:
        return all(name in _REPEAT_FIELDS for name in payload_by_field if name not in {"resource_id", _REPEAT})
    return role_id == merged_role_id(payload_by_field)


def merged_role_hash_components(payload_by_field: Mapping[str, Any]) -> tuple[str, list[str], str] | None:
    """Return the base, per-network entry and payload commitments of a merged role, or None for any other payload."""

    if type(payload_by_field) is _HashedRole:
        return payload_by_field.components
    if not _is_merged_role(payload_by_field):
        return None
    if payload_by_field.get(_REPEAT) is True:
        names = [name for name in payload_by_field if name not in {"resource_id", _REPEAT}]
    else:
        names = _role_network_fields(payload_by_field)
    role_id = payload_by_field["resource_id"]
    entry_hashes = sorted(
        {
            _entry_sha256(name, entry)
            for name in names
            if name != NATIVE_ROLES
            for entry in payload_by_field.get(name) or ()
        }
    )
    return role_id, entry_hashes, composed_merged_role_sha256(role_id, entry_hashes)


def merged_role_rows(
    retained_rows: list[dict[str, Any]],
    resource_rows: list[dict[str, Any]],
    resource_hash_contract: str,
) -> list[dict[str, Any]]:
    """Retain an opted-in source's roles that differ only per network as one row each.

    The retained rows already passed the conflict rule under their native identities.
    """

    opted_in_source_ids = _opted_in_source_ids()
    # shortcut: read per batch and not persisted on the dataset; changing it mid-acquisition mixes merged and
    # unmerged roles in one edition, so persist the choice on the dataset before toggling a source that is loading.
    if (
        not retained_rows
        or retained_rows[0]["resource_type"] != "PractitionerRole"
        or not is_semantic_resource_hash_contract(resource_hash_contract)
        or any(resource_row.get("source_id") not in opted_in_source_ids for resource_row in resource_rows)
    ):
        return retained_rows
    payload_by_role_id: dict[str, dict[str, Any]] = {}
    for retained_row in retained_rows:
        payload_by_field = merged_role_payload(retained_row["payload_json"])
        # 24 hex digits for the source role's identity, then 16 for its content.
        native_id_sha256 = hashlib.sha256(retained_row["resource_id"].encode()).hexdigest()
        payload_by_field[NATIVE_ROLES] = [native_id_sha256[:24] + retained_row["payload_hash"][:16]]
        previous = payload_by_role_id.get(payload_by_field["resource_id"])
        payload_by_role_id[payload_by_field["resource_id"]] = (
            payload_by_field if previous is None else merge_semantic_resource_payloads(previous, payload_by_field)
        )
    merged_rows = []
    for role_id, payload_by_field in sorted(payload_by_role_id.items()):
        hashed_role = _HashedRole(payload_by_field)
        hashed_role.components = merged_role_hash_components(payload_by_field)
        merged_rows.append(
            {
                **retained_rows[0],
                "resource_id": role_id,
                "payload_hash": resource_payload_sha256_for_contract(
                    hashed_role, resource_hash_contract, resource_type="PractitionerRole"
                ),
                "payload_json": hashed_role,
            }
        )
    return merged_rows


def _merge_role_payloads(first: Mapping[str, Any], second: Mapping[str, Any]) -> dict[str, Any] | None:
    """Union the per-network fields of two observations of one merged role."""

    role_id = second.get("resource_id")
    # Only an identity that is its own content hash is a merged role; any other keeps the conflict rule.
    if role_id != first.get("resource_id") or not (_is_merged_role(first) and _is_merged_role(second)):
        return None
    observed_roles = [observation for observation in (first, second) if observation.get(_REPEAT) is not True]
    if observed_roles:
        # Both bodies are equal, so the whole of one observation supplies the provenance.
        merged_by_field = dict(
            max(
                observed_roles,
                key=lambda observation: _stable_json([observation.get(name) for name in sorted(_ROLE_IDENTITY_FIELDS)]),
            )
        )
        network_fields = [*_role_network_fields(observed_roles[0]), NATIVE_ROLES]
    else:
        merged_by_field = {"resource_id": role_id, _REPEAT: True}
        network_fields = list(_REPEAT_FIELDS)
    for observation in (first, second):
        if observation.get(_REPEAT) is True and any(
            name not in network_fields for name in observation if name not in {"resource_id", _REPEAT}
        ):
            return None
    for name in network_fields:
        if isinstance(first.get(name), list) or isinstance(second.get(name), list):
            merged_by_field[name] = _role_union(first.get(name), second.get(name))
    return merged_by_field


def merge_semantic_resource_payloads(
    first_payload: Mapping[str, Any],
    second_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Union merged roles; every other repeated identity keeps the equal-content rule."""

    if _SHA256_HEX_RE.fullmatch(str(second_payload.get("resource_id"))):
        merged_role = _merge_role_payloads(first_payload, second_payload)
        if merged_role is not None:
            return merged_role
    return _merge_equal_content_payloads(first_payload, second_payload)


def is_role_repeat_source(source_ids: Any) -> bool:
    """Whether every source of a candidate retains merged roles, so its input is ordered for lookups."""

    return bool(source_ids) and set(source_ids).issubset(_opted_in_source_ids())


def role_order_index_sql(schema: str, input_name: str) -> str:
    """Order one resource input so staged roles can be found by identity; completion reuses the same index."""

    relation = storage_relation(schema, input_name)
    return f"CREATE INDEX IF NOT EXISTS {storage_identifier(input_name + '_order')} ON {relation} {_ORDER_INDEX}"


async def _first_staged_role_hashes(
    connection: Any,
    schema: str,
    input_name: str,
    binding_by_field: Mapping[str, Any],
    role_ids: list[str],
) -> dict[str, set[str]]:
    """Per-network entries of the first observation the ordered input holds for each role; none when unordered."""

    if not role_ids:  # no statement for a batch without merged roles
        return {}
    if input_name not in _ordered_input_by_name:
        _ordered_input_by_name[input_name] = await connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", storage_relation(schema, input_name + "_order")
        )
    if not _ordered_input_by_name[input_name]:
        return {}
    resets = binding_by_field["resets"]
    staged_rows = await connection.fetch(
        _FIRST_STAGED_ROLE_SQL.format(relation=storage_relation(schema, input_name)),
        role_ids,
        resets.get("*", 0),
        _stable_json(resets),
    )
    hashes_by_role_id = {}
    for staged_row in staged_rows:
        fields_by_name = staged_row["payload_json"]
        if not isinstance(fields_by_name, Mapping):  # depends on whether the connection decodes json
            fields_by_name = json.loads(fields_by_name)
        hashes_by_role_id[staged_row["resource_id"]] = {
            _entry_sha256(name, entry) for name in _REPEAT_FIELDS for entry in fields_by_name.get(name) or ()
        }
    return hashes_by_role_id


def _role_repeat_row(resource_row: dict[str, Any], first_hashes: set[str]) -> dict[str, Any]:
    """Reduce one observation to the per-network entries its first staged observation does not hold."""

    payload_by_field = resource_row["payload_json"]
    repeat_by_field: dict[str, Any] = {"resource_id": resource_row["resource_id"], _REPEAT: True}
    for name in (*_role_network_fields(payload_by_field), NATIVE_ROLES):
        new_entries = [
            entry for entry in payload_by_field.get(name) or () if _entry_sha256(name, entry) not in first_hashes
        ]
        if new_entries:
            repeat_by_field[name] = new_entries
    return {
        **resource_row,
        "payload_hash": merged_role_hash_components(repeat_by_field)[2],
        "payload_json": repeat_by_field,
    }


async def staged_role_rows(
    connection: Any,
    schema: str,
    input_name: str,
    binding_by_field: Mapping[str, Any],
    resource_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Stage a role the ordered input already holds as a repeat carrying only its new per-network entries."""

    role_ids = sorted(
        {
            resource_row["resource_id"]
            for resource_row in resource_rows
            if resource_row["resource_type"] == "PractitionerRole"
            and isinstance(resource_row["payload_json"], Mapping)
            and resource_row["payload_json"].get(_REPEAT) is not True
            and merged_role_hash_components(resource_row["payload_json"]) is not None
        }
    )
    hashes_by_role_id = await _first_staged_role_hashes(connection, schema, input_name, binding_by_field, role_ids)
    return [
        _role_repeat_row(resource_row, hashes_by_role_id[resource_row["resource_id"]])
        if resource_row["resource_type"] == "PractitionerRole" and resource_row["resource_id"] in hashes_by_role_id
        else resource_row
        for resource_row in resource_rows
    ]


async def require_observed_merged_roles(connection: Any, relation: str) -> None:
    """Refuse a reduced role that is still only a repeat: its full observation was never stored."""

    if await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {relation} WHERE resource_type='PractitionerRole' "
        f"AND payload_json::jsonb ? '{_REPEAT}')"
    ):
        raise ValueError("provider_directory_merged_role_observation_missing")


async def require_one_content_per_native_role(
    connection: Any, relation: str, active_where: str, active_arguments: tuple
) -> None:
    """Keep the conflict rule for one source role that arrived with two contents, in any batches."""

    if await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {relation} CROSS JOIN LATERAL "
        f"jsonb_array_elements_text(payload_json::jsonb->'{NATIVE_ROLES}') native_role {active_where}"
        "AND resource_type='PractitionerRole' "
        "GROUP BY left(native_role,24) HAVING min(native_role)<>max(native_role))",
        *active_arguments,
    ):
        raise ValueError("provider_directory_resource_payload_conflict")
