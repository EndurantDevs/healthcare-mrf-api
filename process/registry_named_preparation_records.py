# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed portable records for named snapshot preparation."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any
from uuid import UUID

MAX_BUNDLE_SECTIONS = 64
SCHEMA = "hp_snapshot_retention"
PREPARATION = SCHEMA + ".preparation"
PREPARATION_TABLES = (PREPARATION, PREPARATION + "_relation", PREPARATION + "_claim")


class SnapshotContractError(ValueError):
    """A bounded public error code without package or credential contents."""


def identifier(value: Any, *, maximum: int = 128) -> str:
    """Accept bounded machine identifiers, never URLs or file paths."""

    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]*", value):
        raise ValueError("snapshot identifier must be a machine identifier")
    if not 1 <= len(value) <= maximum:
        raise ValueError("snapshot identifier length is invalid")
    return value


def json_object(value: Any, *, maximum: int = 262144) -> dict[str, Any]:
    """Copy a finite JSON object, rejecting oversized or non-JSON data."""

    if not isinstance(value, dict):
        raise ValueError("snapshot record must be a JSON object")
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError("snapshot record must contain finite JSON data") from error
    if len(encoded.encode("utf-8")) > maximum:
        raise ValueError("snapshot record exceeds its size limit")
    return json.loads(encoded)


def digest(value: dict[str, Any]) -> str:
    """Hash normalized JSON for immutable identity comparison."""

    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def require(value, reason):
    """Reject a failed retention capability or identity condition."""
    if not value:
        raise SnapshotContractError("snapshot_retention_" + reason)


def local_uuid(value):
    """Normalize an explicit local UUID; never look up current or origin identities."""
    require(isinstance(value, (str, UUID)), "identifier_invalid")
    try:
        return UUID(str(value))
    except ValueError as exc:
        raise SnapshotContractError("snapshot_retention_identifier_invalid") from exc


def sha256(value):
    """Require an exact lower-case SHA-256 attestation."""
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "digest_invalid")
    return value


def relation_inventory(expected_inventory):
    """Resolve snapshots by qualified names, including retained older documents."""
    require(
        isinstance(expected_inventory, dict)
        and "relations" in expected_inventory
        and set(expected_inventory) <= {"relations", "database_oid"},
        "inventory_invalid",
    )
    relations = expected_inventory["relations"]
    require(isinstance(relations, list) and 1 <= len(relations) <= MAX_BUNDLE_SECTIONS, "inventory_invalid")
    named_relations = []
    for relation in relations:
        require(
            isinstance(relation, dict)
            and set(relation)
            <= {
                "schema_name",
                "relation_name",
                "relation_oid",
                "schema_oid",
                "schema_owner_oid",
                "owner_oid",
                "relfilenode",
            },
            "inventory_invalid",
        )
        for field in ("schema_name", "relation_name"):
            require(isinstance(relation.get(field), str), "inventory_invalid")
            require(bool(re.fullmatch(r"[a-z][a-z0-9_]{0,62}", relation[field])), "inventory_invalid")
        require(not relation["schema_name"].startswith("pg_"), "inventory_invalid")
        require(relation["schema_name"] != SCHEMA, "inventory_invalid")
        named_relations.append({field: relation[field] for field in ("schema_name", "relation_name")})
    require(
        len({(relation_row["schema_name"], relation_row["relation_name"]) for relation_row in relations})
        == len(relations),
        "inventory_invalid",
    )
    return {
        "relations": sorted(named_relations, key=lambda relation: (relation["schema_name"], relation["relation_name"]))
    }


def name(value):
    """Bound local identifiers to simple lowercase PostgreSQL names."""
    if not isinstance(value, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value) is None:
        raise SnapshotContractError("snapshot_alias_name_invalid")
    return value


def qualified(schema, relation):
    """Quote validated identifiers without admitting SQL fragments."""
    return '"' + name(schema) + '"."' + name(relation) + '"'


@dataclass(frozen=True)
class ProtectedPreparation:
    owner_role: str
    publisher_role: str
    runtime_roles: tuple[str, ...]


@dataclass(frozen=True)
class NativePreparationAdmission:
    preparation_id: str
    node_id: str
    importer_id: str
    dataset_key: str
    run_id: str
    generation_id: str
    publication_fence: str
    idempotency_key: str
    request_sha256: str
    expected_generation_id: str | None = None
    expected_fence_token: str | None = None
    predecessor: object | None = None
    predecessor_pin_id: str | None = None
    predecessor_pin_fence: str | None = None


@dataclass(frozen=True)
class NativePreparationIntake:
    attempt_id: str
    inventory: dict
    receipt: dict


@dataclass(frozen=True)
class PreparationClaim:
    preparation_id: str
    lease_token: str
    attempt: int


def intake_values(request):
    """Keep the original builder inventory distinct from later protected ownership."""
    require(type(request) is NativePreparationIntake, "preparation_intake_invalid")
    receipt = json_object(request.receipt)
    require(bool(receipt), "handoff_missing")
    return {
        "attempt_id": identifier(request.attempt_id, maximum=128),
        "inventory": relation_inventory(request.inventory),
        "receipt": receipt,
    }


def claim_values(claim):
    """A claim names one persisted incarnation, never an expiry-based authority guess."""
    require(type(claim) is PreparationClaim, "claim_invalid")
    require(type(claim.attempt) is int and claim.attempt > 0, "claim_invalid")
    return local_uuid(claim.preparation_id), local_uuid(claim.lease_token), claim.attempt


def admission_values(spec, request, authority, *, fhir_source=None, named_kind=None):
    """Detach a closed named admission before its first database operation."""
    require(type(request) is NativePreparationAdmission, "preparation_admission_invalid")
    admission_by_field = asdict(request)
    for field in ("preparation_id", "generation_id", "publication_fence"):
        admission_by_field[field] = str(local_uuid(admission_by_field[field]))
    for field in ("node_id", "importer_id", "dataset_key", "run_id", "idempotency_key"):
        admission_by_field[field] = identifier(admission_by_field[field], maximum=128)
    admission_by_field["request_sha256"] = sha256(admission_by_field["request_sha256"])
    optional = ("expected_generation_id", "expected_fence_token", "predecessor_pin_id", "predecessor_pin_fence")
    require(
        named_kind in {"catalog", "network", "office"} and admission_by_field["importer_id"] == named_kind,
        "named_kind_invalid",
    )
    require(
        spec is None
        and fhir_source is None
        and request.predecessor is None
        and all(admission_by_field[field] is None for field in optional),
        "predecessor_invalid",
    )
    require(admission_by_field["preparation_id"] == admission_by_field["generation_id"], "named_candidate_changed")
    admission_by_field["kind"] = named_kind
    admission_by_field.update(authority)
    return json_object(admission_by_field), None
