# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable receipt, schema contract and validators for precomputed network statistics.

Persisted `network_stats.postgres.v1` receipts carrying the nine original B-tree indexes
remain valid for unfiltered reads and `/stats` endpoints. Newly prepared candidates emit
eleven indexes (adding GIN array indexes on `states` and `core_states`), and operating-state
list/search filters require `has_network_stats_state_gin` before querying those GIN indexes,
returning 503 until a GIN-indexed generation is published.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict

from api.provider_specialty_static import PRIMARY_CARE_TAXONOMY_CODES
from process.network_compact_layout import (
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
    compact_identifier,
)
from process.network_index_catalog import index_catalog_statement
from process.network_membership_copy import MembershipCopyTarget

CONTRACT = "network_stats.postgres.v1"
DIM_TABLE = "network_stats_specialty_dim"
SUMMARY_TABLE = "network_stats_summary"
STATE_TABLE = "network_stats_state"
SPECIALTY_TABLE = "network_stats_specialty"
STATE_SPECIALTY_TABLE = "network_stats_state_specialty"

STATS_TABLE_ORDER = (
    DIM_TABLE,
    SUMMARY_TABLE,
    STATE_TABLE,
    SPECIALTY_TABLE,
    STATE_SPECIALTY_TABLE,
)
STATS_TABLES = frozenset(STATS_TABLE_ORDER)

ROLLUP_SPECS = (
    (20001, "primary_care", "Primary Care"),
    (20002, "behavioral_health", "Behavioral Health"),
    (20003, "dental", "Dental"),
    (20004, "vision", "Vision"),
    (20005, "pharmacy", "Pharmacy"),
    (20006, "hospital", "Hospital"),
    (20007, "urgent_care", "Urgent Care"),
    (20008, "clinical_lab", "Clinical Laboratory"),
)
PRIMARY_CARE_CODES = PRIMARY_CARE_TAXONOMY_CODES

STATS_INDEX_PATHS = (
    (DIM_TABLE, ["specialty_id"], True, True),
    (DIM_TABLE, ["specialty_kind", "specialty_key"], False, True),
    (SUMMARY_TABLE, ["network_id"], True, True),
    (STATE_TABLE, ["network_id", "state_code"], True, True),
    (STATE_TABLE, ["state_code", "network_id"], False, False),
    (SPECIALTY_TABLE, ["network_id", "specialty_id"], True, True),
    (SPECIALTY_TABLE, ["specialty_id", "network_id"], False, False),
    (STATE_SPECIALTY_TABLE, ["network_id", "state_code", "specialty_id"], True, True),
    (STATE_SPECIALTY_TABLE, ["state_code", "specialty_id", "network_id"], False, False),
)
STATS_GIN_INDEX_PATHS = (
    (SUMMARY_TABLE, ["states"]),
    (SUMMARY_TABLE, ["core_states"]),
)

_SCOPE_FIELDS = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
_RECEIPT_FIELDS = {"contract", "scope", "tables", "counts", "input_layout", "indexes", "sha256"}
_COUNT_FIELDS = (
    "specialty_dim_rows",
    "summary_rows",
    "state_rows",
    "specialty_rows",
    "state_specialty_rows",
)
_SUMMARY_INT_FIELDS = (
    "total_npi_count",
    "office_npi_count",
    "provider_only_npi_count",
    "individual_npi_count",
    "organization_npi_count",
    "unique_address_count",
    "unique_location_count",
    "state_count",
    "core_state_count",
    "county_count",
    "zip5_count",
)
_SUMMARY_FIELDS = frozenset((*_SUMMARY_INT_FIELDS, "states", "core_states"))
_STATE_CODE_RE = re.compile(r"[A-Z0-9]{2,8}\Z")


class NetworkStatsError(ValueError):
    """Precomputed network statistics receipt, index profile or data parity differs."""


def require_stats_condition(is_valid: bool, detail: str = "Network stats contract differs") -> None:
    """Raise NetworkStatsError when a contract invariant fails."""
    if not is_valid:
        raise NetworkStatsError(detail)


def stats_digest(document_by_field: dict) -> str:
    """Compute canonical SHA-256 over a JSON-serializable descriptor."""
    encoded = json.dumps(document_by_field, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _decode_json(encoded_or_obj):
    return json.loads(encoded_or_obj) if isinstance(encoded_or_obj, str) else encoded_or_obj


def network_stats_relations(schema_name: str) -> tuple[CompactAddressRelation, ...]:
    """List the five immutable serving relations owned by a candidate schema."""
    return tuple(CompactAddressRelation(schema_name, table_name) for table_name in STATS_TABLE_ORDER)


def _expected_btree_index_map(schema_name: str) -> dict[str, tuple[str, str]]:
    prefix = f"{schema_name}."
    return {
        f"{DIM_TABLE}_pkey": (
            DIM_TABLE,
            f"CREATE UNIQUE INDEX {DIM_TABLE}_pkey ON {prefix}{DIM_TABLE} USING btree (specialty_id)",
        ),
        f"{DIM_TABLE}_key_idx": (
            DIM_TABLE,
            f"CREATE UNIQUE INDEX {DIM_TABLE}_key_idx ON {prefix}{DIM_TABLE} USING btree (specialty_kind, specialty_key)",
        ),
        f"{SUMMARY_TABLE}_pkey": (
            SUMMARY_TABLE,
            f"CREATE UNIQUE INDEX {SUMMARY_TABLE}_pkey ON {prefix}{SUMMARY_TABLE} USING btree (network_id)",
        ),
        f"{STATE_TABLE}_pkey": (
            STATE_TABLE,
            f"CREATE UNIQUE INDEX {STATE_TABLE}_pkey ON {prefix}{STATE_TABLE} USING btree (network_id, state_code)",
        ),
        f"{STATE_TABLE}_code_idx": (
            STATE_TABLE,
            f"CREATE INDEX {STATE_TABLE}_code_idx ON {prefix}{STATE_TABLE} USING btree (state_code, network_id)",
        ),
        f"{SPECIALTY_TABLE}_pkey": (
            SPECIALTY_TABLE,
            f"CREATE UNIQUE INDEX {SPECIALTY_TABLE}_pkey ON {prefix}{SPECIALTY_TABLE} USING btree (network_id, specialty_id)",
        ),
        f"{SPECIALTY_TABLE}_id_idx": (
            SPECIALTY_TABLE,
            f"CREATE INDEX {SPECIALTY_TABLE}_id_idx ON {prefix}{SPECIALTY_TABLE} USING btree (specialty_id, network_id)",
        ),
        f"{STATE_SPECIALTY_TABLE}_pkey": (
            STATE_SPECIALTY_TABLE,
            f"CREATE UNIQUE INDEX {STATE_SPECIALTY_TABLE}_pkey ON {prefix}{STATE_SPECIALTY_TABLE} USING btree (network_id, state_code, specialty_id)",
        ),
        f"{STATE_SPECIALTY_TABLE}_lookup_idx": (
            STATE_SPECIALTY_TABLE,
            f"CREATE INDEX {STATE_SPECIALTY_TABLE}_lookup_idx ON {prefix}{STATE_SPECIALTY_TABLE} USING btree (state_code, specialty_id, network_id)",
        ),
    }


def _expected_gin_index_map(schema_name: str) -> dict[str, tuple[str, str]]:
    prefix = f"{schema_name}."
    return {
        f"{SUMMARY_TABLE}_states_gin_idx": (
            SUMMARY_TABLE,
            f"CREATE INDEX {SUMMARY_TABLE}_states_gin_idx ON {prefix}{SUMMARY_TABLE} USING gin (states)",
        ),
        f"{SUMMARY_TABLE}_core_states_gin_idx": (
            SUMMARY_TABLE,
            f"CREATE INDEX {SUMMARY_TABLE}_core_states_gin_idx ON {prefix}{SUMMARY_TABLE} USING gin (core_states)",
        ),
    }


def _expected_index_map(schema_name: str) -> dict[str, tuple[str, str]]:
    return {**_expected_btree_index_map(schema_name), **_expected_gin_index_map(schema_name)}


def _normalized_btree_def(definition: str) -> str:
    stripped = re.sub(r"::(?:character varying|text)(?:\[\])?", "", definition)
    stripped = re.sub(r'COLLATE\s+"C"', "", stripped)
    return re.sub(r'[()\s"]+', "", stripped)


def _validated_descriptor(receipt_by_field, *, require_state_gin: bool = False) -> dict:
    require_stats_condition(
        type(receipt_by_field) is dict
        and set(receipt_by_field) == _RECEIPT_FIELDS
        and receipt_by_field["contract"] == CONTRACT
    )
    unsigned_by_field = {key: entry for key, entry in receipt_by_field.items() if key != "sha256"}
    require_stats_condition(receipt_by_field["sha256"] == stats_digest(unsigned_by_field))
    scope_by_field = receipt_by_field["scope"]
    require_stats_condition(type(scope_by_field) is dict and set(scope_by_field) == set(_SCOPE_FIELDS))
    scope_pin = MembershipCopyTarget(**scope_by_field)
    require_stats_condition(receipt_by_field["tables"] == list(STATS_TABLE_ORDER))
    counts_by_name = receipt_by_field["counts"]
    require_stats_condition(
        type(counts_by_name) is dict
        and set(counts_by_name) == set(_COUNT_FIELDS)
        and all(type(counts_by_name[key]) is int and counts_by_name[key] >= 0 for key in _COUNT_FIELDS)
        and counts_by_name["specialty_dim_rows"] >= len(ROLLUP_SPECS)
    )
    layout_by_role = receipt_by_field["input_layout"]
    require_stats_condition(
        type(layout_by_role) is dict and set(layout_by_role) == {"base", "custom", "ref_map", "artifacts"}
    )
    layout = CompactNetworkLayout(
        CompactAddressRelation(**layout_by_role["ref_map"]),
        CompactAddressRelation(**layout_by_role["base"]),
        tuple(CompactEdgeArtifact(**entry) for entry in layout_by_role["artifacts"]),
        None if layout_by_role["custom"] is None else CompactAddressRelation(**layout_by_role["custom"]),
    )
    require_stats_condition(layout.ref_map.schema_name == scope_pin.schema_name)
    indexes = receipt_by_field["indexes"]
    btree_by_name = _expected_btree_index_map(scope_pin.schema_name)
    expected_by_name = _expected_index_map(scope_pin.schema_name)
    require_stats_condition(type(indexes) is list)
    normalized_indexes = [
        {field: index_entry[field] for field in ("table_name", "name", "definition")} for index_entry in indexes
    ]
    present_names = {entry["name"] for entry in normalized_indexes}
    require_stats_condition(len(normalized_indexes) == len(present_names))
    if require_state_gin:
        require_stats_condition(present_names == set(expected_by_name))
    else:
        require_stats_condition(present_names in (set(expected_by_name), set(btree_by_name)))
    for index_entry in normalized_indexes:
        expected_table, expected_def = expected_by_name[index_entry["name"]]
        require_stats_condition(
            index_entry["table_name"] == expected_table
            and type(index_entry["definition"]) is str
            and 0 < len(index_entry["definition"]) < 8192
            and _normalized_btree_def(index_entry["definition"]) == _normalized_btree_def(expected_def)
        )
    return {
        **{key: receipt_by_field[key] for key in _RECEIPT_FIELDS},
        "input_layout": json.loads(json.dumps(asdict(layout))),
        "indexes": normalized_indexes,
    }


def has_network_stats_state_gin(descriptor_by_field: dict | None) -> bool:
    """Return whether a validated network_stats receipt includes both state GIN indexes."""
    if descriptor_by_field is None:
        return False
    schema_name = descriptor_by_field["scope"]["schema_name"]
    present_names = {entry["name"] for entry in descriptor_by_field["indexes"]}
    return set(_expected_gin_index_map(schema_name)) <= present_names


def network_stats_receipt(candidate) -> dict | None:
    """Validate and return the optional network_stats serving receipt on a candidate."""
    report_by_field = _decode_json(candidate.get("validation_json")) or {}
    serving_by_field = report_by_field.get("serving_readiness") or {}
    if "network_stats" not in serving_by_field:
        return None
    descriptor_by_field = _validated_descriptor(serving_by_field["network_stats"])
    require_stats_condition(descriptor_by_field["scope"] == {field: str(candidate[field]) for field in _SCOPE_FIELDS})
    expected_artifacts = [
        {key: entry[key] for key in ("candidate_id", "schema_name")}
        for entry in serving_by_field.get("edge_artifacts", ())
    ]
    require_stats_condition(descriptor_by_field["input_layout"]["artifacts"] == expected_artifacts)
    return json.loads(json.dumps(descriptor_by_field))


def network_stats_schema(candidate) -> str | None:
    """Return the validated candidate schema owning network_stats tables when present."""
    descriptor_by_field = network_stats_receipt(candidate)
    if descriptor_by_field is None:
        return None
    compact_identifier(candidate["schema_name"])
    return candidate["schema_name"]


def require_network_stats_index_records(
    index_entries: list[dict], descriptor_by_field: dict, *, require_state_gin: bool = False
) -> None:
    """Verify native catalog index definitions against the validated network_stats receipt."""
    validated_by_field = _validated_descriptor(descriptor_by_field, require_state_gin=require_state_gin)
    actual_by_name = {entry["name"]: entry for entry in index_entries}
    for expected_entry in validated_by_field["indexes"]:
        actual_entry = actual_by_name.get(expected_entry["name"])
        require_stats_condition(
            actual_entry is not None
            and actual_entry.get("table_name") == expected_entry["table_name"]
            and actual_entry["definition"] == expected_entry["definition"]
            and all(actual_entry.get(flag) is True for flag in ("indisvalid", "indisready", "indislive"))
        )


async def fetch_network_stats_index_records(connection, schema_name: str) -> list[dict]:
    """Read index definitions across all five network_stats tables in canonical search_path."""
    compact_identifier(schema_name)
    statement = index_catalog_statement(
        """SELECT t.relname AS table_name, c.relname AS name, i.indisvalid, i.indisready, i.indislive,
        pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_namespace n
        JOIN pg_class t ON t.relnamespace = n.oid AND t.relkind = 'r'
        JOIN pg_index i ON i.indrelid = t.oid
        JOIN pg_class c ON c.oid = i.indexrelid
        WHERE n.nspname = $1::text AND t.relname = ANY($2::text[])
        ORDER BY t.relname, c.relname""",
    )
    decoded_entries = _decode_json(await connection.fetchval(statement, schema_name, list(STATS_TABLE_ORDER)))
    return sorted(decoded_entries, key=lambda entry: (entry["table_name"], entry["name"]))


async def verify_network_stats_indexes(connection, descriptor_by_field: dict) -> None:
    """Verify live index catalog state for all network_stats relations before publication."""
    validated_by_field = _validated_descriptor(descriptor_by_field)
    schema_name = validated_by_field["scope"]["schema_name"]
    index_entries = await fetch_network_stats_index_records(connection, schema_name)
    require_network_stats_index_records(index_entries, validated_by_field)


def validate_network_stats_summary_document(summary_by_field) -> dict:
    """Validate a closed per-network summary stats object emitted by serving or catalog reads."""
    if type(summary_by_field) is not dict or set(summary_by_field) != _SUMMARY_FIELDS:
        raise ValueError("network_stats_summary_invalid")
    for field_name in _SUMMARY_INT_FIELDS:
        field_val = summary_by_field[field_name]
        if type(field_val) is not int or not 0 <= field_val <= 2147483647:
            raise ValueError("network_stats_summary_invalid")
    for list_field, count_field in (("states", "state_count"), ("core_states", "core_state_count")):
        codes = summary_by_field[list_field]
        if (
            type(codes) is not list
            or len(codes) != summary_by_field[count_field]
            or len(codes) > 128
            or len(set(codes)) != len(codes)
            or any(type(code) is not str or _STATE_CODE_RE.fullmatch(code) is None for code in codes)
        ):
            raise ValueError("network_stats_summary_invalid")
    if (
        summary_by_field["office_npi_count"] + summary_by_field["provider_only_npi_count"]
        != summary_by_field["total_npi_count"]
        or summary_by_field["individual_npi_count"] + summary_by_field["organization_npi_count"]
        > summary_by_field["total_npi_count"]
        or summary_by_field["unique_address_count"] > summary_by_field["unique_location_count"]
        or summary_by_field["core_state_count"] > summary_by_field["state_count"]
        or not set(summary_by_field["core_states"]) <= set(summary_by_field["states"])
    ):
        raise ValueError("network_stats_summary_invalid")
    return summary_by_field
