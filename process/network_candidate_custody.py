# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded logical candidate metadata and declared control relation reads."""

import hashlib
import json
import re
from datetime import datetime, timezone
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_membership_copy import MembershipCopyTarget

MAX_RECEIPT_BYTES = 2 * 1024 * 1024


SOURCE_EVENT_ASSERTION = "source_event_assertion"


_SOURCE_CHECKPOINTS = {
    "cms": "cms_source_notification",
    "ptg": "ptg_source_notification",
    "aca": "aca_source_notification",
}


_STABLE = (
    "dataset_id",
    "schema_id",
    "producer_id",
    "candidate_id",
    "schema_name",
    "schema_revision",
    "source_generations",
    "approved_custom_revision",
    "expected_head",
    "expected_rows",
    "accepted_rows",
    "index_ready",
    "source_recipes_json",
    "validation_json",
    "created_at",
)


class NetworkCandidateCustodyError(ValueError):
    """Candidate metadata or declared data access is unavailable."""


_API_CONTROL_TABLES = frozenset(
    ("network_membership_candidate", "network_serving_manifest", "registry_approved_record")
)


def _require(condition):
    if not condition:
        raise NetworkCandidateCustodyError("network_candidate_custody_unavailable")


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _digest(value):
    return hashlib.sha256(_encoded(value)).hexdigest()


def _source_event_assertion(report):
    """Validate the closed logical source-event assertion and its checkpoint fields."""
    assertion = report.get(SOURCE_EVENT_ASSERTION)
    _require(type(assertion) is dict and set(assertion) == {"revision", "kind", "checkpoint"})
    _require(type(assertion["revision"]) is int and assertion["revision"] == 1)
    kind = assertion["kind"]
    _require(type(kind) is str and kind in _SOURCE_CHECKPOINTS)
    checkpoint = assertion["checkpoint"]
    _require(type(checkpoint) is dict)
    fields = {"event_id", "attempt_id"}
    if kind == "cms":
        fields |= {"notification_sha256", "pair_sha256", "coordinates", "replaced_coordinates", "recipe_sha256"}
        if checkpoint.get("replaced_coordinates") is None:
            fields |= {"mode", "base_manifest"}
            _require(checkpoint.get("mode") == "initial" and checkpoint.get("base_manifest") is None)
    else:
        fields |= {"header", "reader_identity_sha256"}
    _require(set(checkpoint) == fields)
    try:
        for field in ("event_id", "attempt_id"):
            field_value = checkpoint[field]
            _require(type(field_value) is str and str(UUID(field_value)) == field_value and UUID(field_value).int != 0)
        for field in fields:
            if field.endswith("sha256"):
                _require(type(checkpoint[field]) is str and re.fullmatch("[0-9a-f]{64}", checkpoint[field]) is not None)
        if kind == "cms":
            from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates

            for field in ("coordinates", "replaced_coordinates"):
                field_value = checkpoint[field]
                if field == "replaced_coordinates" and field_value is None:
                    continue
                _require(type(field_value) is dict)
                coordinates = RegistryNetworkSourceCoordinates(**field_value)
                _require(coordinates.source_system == "fhir" and coordinates.source_id == "cms-npd")
        elif kind == "aca":
            from process.registry_aca_source_notifications import validate_registry_aca_source_event

            validate_registry_aca_source_event(checkpoint["header"])
        else:
            from process.registry_ptg_source_notifications import validate_registry_ptg_source_event

            validate_registry_ptg_source_event(checkpoint["header"])
    except ValueError, TypeError, KeyError, AttributeError:
        raise NetworkCandidateCustodyError("network_candidate_custody_unavailable") from None
    key = _SOURCE_CHECKPOINTS[kind]
    _require(not any(other in report for other in _SOURCE_CHECKPOINTS.values() if other != key))
    return key, checkpoint


def _evidence(candidate, *, revision=1):
    _require(type(revision) is int and revision in (1, 2, 3, 4, 5))
    evidence_by_name = {}
    _require(set(candidate) == set(_STABLE) | {"state"})
    for field in _STABLE:
        field_value = candidate[field]
        if field in ("source_generations", "source_recipes_json", "validation_json") and type(field_value) is str:
            field_value = json.loads(field_value)
        if field == "validation_json" and revision in (2, 3, 4, 5):
            _require(type(field_value) is dict)
        if field == "validation_json" and (
            revision == 2 or (revision in (3, 4, 5) and SOURCE_EVENT_ASSERTION in field_value)
        ):
            key, checkpoint = _source_event_assertion(field_value)
            _require(candidate["state"] != "published" or key in field_value)
            report_by_name = dict(field_value)
            if key in report_by_name:
                _require(_encoded(report_by_name.pop(key)) == _encoded(checkpoint))
            field_value = report_by_name
        if isinstance(field_value, datetime):
            field_value = field_value.astimezone(timezone.utc).isoformat()
        evidence_by_name[field] = str(field_value) if isinstance(field_value, UUID) else field_value
    _require(len(_encoded(evidence_by_name)) <= MAX_RECEIPT_BYTES)
    return evidence_by_name


async def _candidate(connection, target, schema, *, lock=False):
    _require(type(target) is MembershipCopyTarget)
    row = await connection.fetchrow(
        f"SELECT * FROM {_identifier(schema)}.network_membership_candidate WHERE candidate_id=$1"
        + (" FOR UPDATE" if lock else ""),
        UUID(target.candidate_id),
    )
    _require(row is not None and all(str(row[key]) == getattr(target, key) for key in _STABLE[:5]))
    return dict(row)


async def read_network_control_relation_pins(connection, relation_names, *, control_schema):
    """Read configured control projections through ordinary pinned SQL access."""
    from process.network_compact_layout import CompactAddressRelation
    from process.registry_management_permissions import _TABLES

    _require(connection.is_in_transaction() and type(relation_names) is tuple)
    _require(0 < len(relation_names) <= len(_API_CONTROL_TABLES))
    _require(all(type(name) is str and name in _API_CONTROL_TABLES for name in relation_names))
    _require(len(set(relation_names)) == len(relation_names))
    modes = await connection.fetchrow(
        "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"
    )
    _require(tuple(modes) in (("repeatable read", "on"), ("serializable", "on")))
    namespace = _identifier(control_schema)
    for name in relation_names:
        columns = ",".join(_identifier(column) for column in _TABLES[name])
        await connection.execute(f"SELECT {columns} FROM {namespace}.{_identifier(name)} LIMIT 0")
    return tuple(CompactAddressRelation(control_schema, name) for name in relation_names)
