# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict shared-block PTG V3 snapshot metadata loading."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from sqlalchemy import text

from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from api.ptg2_serving_utils import ein_plan_id_variants
from process.ptg_parts.domain import PTG2_CANDIDATE_ACTIVATION_CONTRACT
from process.ptg_parts.ptg2_candidate_attestation import (
    PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_COLD_LOOKUP_CONTRACT,
    PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
    PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    PTG2_V3_SHARED_GENERATION,
)
from process.ptg_parts.ptg2_shared_reuse import PTG2_V3_SOURCE_SET_CONTRACT

from api.ptg2_types import PTG2ServingTables

PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_V3_ARCH_VERSION = "postgres_binary_v3"
PTG2_V3_STORAGE_TYPE = "ptg2_shared_blocks_v3"
PTG2_V3_SERVING_LAYOUT = "lean_provider_key_v1"
PTG2_V3_SHARED_BLOCK_LAYOUT = "dense_shared_blocks_v3"
PTG2_V3_AUDIT_CONTRACT = "persisted_served_occurrence_sample_v2"
PTG2_V3_AUDIT_METHOD = "publish_time_stratified_v1"
PTG2_V3_AUDIT_MAX_SAMPLE_ROWS = 2560
PTG2_DATABASE_EVIDENCE_CONTRACT = "postgresql_session_v1"
_COVERAGE_SCOPE_ID_RE = re.compile(r"^[0-9a-f]{64}$")


def _safe_table_name(value: Any, *, default_schema: str = PTG2_SCHEMA) -> str | None:
    """Validate a schema-qualified relation name without probing PostgreSQL."""

    if not value:
        return None
    parts = str(value).strip().split(".", 1)
    schema_name, table_name = (default_schema, parts[0]) if len(parts) == 1 else parts
    identifier = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
    if not identifier.fullmatch(schema_name) or not identifier.fullmatch(table_name):
        return None
    return f"{schema_name}.{table_name}"


def _optional_integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _strict_coverage_scope_id(serving_index: dict[str, Any]) -> str:
    value = serving_index.get("coverage_scope_id")
    if not isinstance(value, str) or not _COVERAGE_SCOPE_ID_RE.fullmatch(value):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing a canonical 64-lowercase-hex "
            "coverage_scope_id; reimport the snapshot"
        )
    return value


def _serving_index_atom_key_bits(serving_index: dict[str, Any]) -> int | None:
    direct_bits = _optional_integer(serving_index.get("atom_key_bits"))
    if direct_bits is not None:
        return direct_bits
    serving_binary = serving_index.get("serving_binary")
    if not isinstance(serving_binary, dict):
        return None
    price_atoms = serving_binary.get("price_atoms_v3")
    if not isinstance(price_atoms, dict):
        return None
    return _optional_integer(price_atoms.get("atom_key_bits"))


def _serving_binary_section_integer(
    serving_index: dict[str, Any],
    section_name: str,
    field_name: str,
) -> int | None:
    serving_binary = serving_index.get("serving_binary")
    if not isinstance(serving_binary, dict):
        return None
    section = serving_binary.get(section_name)
    if not isinstance(section, dict):
        return None
    return _optional_integer(section.get(field_name))


def _strict_v3_audit_sample(
    serving_index: dict[str, Any],
    *,
    source_count: int,
) -> dict[str, Any]:
    audit_sample = serving_index.get("audit_sample")
    if not isinstance(audit_sample, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing its persisted audit sample; "
            "reimport the snapshot"
        )
    expected_values_by_field = {
        "contract": PTG2_V3_AUDIT_CONTRACT,
        "method": PTG2_V3_AUDIT_METHOD,
        "serving_multiplicity_semantics": PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    }
    for field_name, expected_value in expected_values_by_field.items():
        if str(audit_sample.get(field_name) or "").strip().lower() != expected_value:
            raise PTG2ManifestArtifactError(
                f"PTG2 postgres_binary_v3 audit sample has invalid {field_name}; "
                "reimport the snapshot"
            )
    sample_count = _optional_integer(audit_sample.get("sample_count"))
    maximum_rows = _optional_integer(audit_sample.get("maximum_rows"))
    if (
        sample_count is None
        or sample_count < 0
        or maximum_rows != PTG2_V3_AUDIT_MAX_SAMPLE_ROWS
        or sample_count > maximum_rows
        or _optional_integer(audit_sample.get("format_version")) != 2
        or _optional_integer(audit_sample.get("source_count")) != source_count
        or str(audit_sample.get("occurrence_identity") or "").strip().lower()
        != "sha256_candidate_ordinal_source_key_v2"
        or audit_sample.get("complete_population") is not False
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 audit sample bounds are invalid; reimport the snapshot"
        )
    sample_digest = str(audit_sample.get("sample_digest") or "").strip().lower()
    if not _COVERAGE_SCOPE_ID_RE.fullmatch(sample_digest):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 audit sample digest is invalid; reimport the snapshot"
        )
    return dict(audit_sample)


def _strict_v3_source_set(
    serving_index: dict[str, Any],
    *,
    source_count: int,
) -> dict[str, Any] | None:
    """Validate a logical snapshot source-set seal when one is published."""

    source_set = serving_index.get("source_set")
    if source_set is None:
        return None
    if not isinstance(source_set, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot source_set is malformed; reimport the snapshot"
        )
    digest = str(source_set.get("raw_container_sha256_digest") or "").strip()
    if (
        set(source_set)
        != {"contract", "source_count", "raw_container_sha256_digest"}
        or str(source_set.get("contract") or "").strip().lower()
        != PTG2_V3_SOURCE_SET_CONTRACT
        or _optional_integer(source_set.get("source_count")) != source_count
        or not _COVERAGE_SCOPE_ID_RE.fullmatch(digest)
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot source_set is invalid; reimport the snapshot"
        )
    return {
        "contract": PTG2_V3_SOURCE_SET_CONTRACT,
        "source_count": source_count,
        "raw_container_sha256_digest": digest,
    }


def _database_execution_evidence(row_fields: Any) -> dict[str, Any]:
    """Return non-sensitive evidence read from the active PostgreSQL session."""

    server_version_num = _optional_integer(
        row_fields.get("postgres_server_version_num")
    )
    database_evidence_by_field = {
        "contract": PTG2_DATABASE_EVIDENCE_CONTRACT,
        "server_version_num": server_version_num,
        "database_selected": row_fields.get("database_selected"),
        "backend_session_active": row_fields.get("backend_session_active"),
        "transaction_snapshot_observed": row_fields.get(
            "transaction_snapshot_observed"
        ),
    }
    if (
        server_version_num is None
        or server_version_num < 10000
        or database_evidence_by_field["database_selected"] is not True
        or database_evidence_by_field["backend_session_active"] is not True
        or database_evidence_by_field["transaction_snapshot_observed"] is not True
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 snapshot query did not return valid PostgreSQL execution evidence"
        )
    return database_evidence_by_field


def _strict_v3_manifest_fields(
    serving_index: dict[str, Any],
) -> tuple[int | None, str | None, str | None, dict[str, Any]]:
    """Validate the only serving contract accepted by the API."""

    arch_version = str(serving_index.get("arch_version") or "").strip().lower()
    storage_generation = (
        str(serving_index.get("storage_generation") or "").strip().lower()
    )
    cold_lookup_contract = (
        str(serving_index.get("cold_lookup_contract") or "").strip().lower()
    )
    price_membership_semantics = (
        str(serving_index.get("price_membership_semantics") or "").strip().lower()
    )
    serving_multiplicity_semantics = (
        str(serving_index.get("serving_multiplicity_semantics") or "").strip().lower()
    )
    shared_snapshot_key = _optional_integer(serving_index.get("shared_snapshot_key"))
    source_count = _optional_integer(serving_index.get("source_count"))

    if arch_version != PTG2_V3_ARCH_VERSION:
        raise PTG2ManifestArtifactError(
            "only postgres_binary_v3 snapshots are supported; reimport the snapshot"
        )
    if storage_generation != PTG2_V3_SHARED_GENERATION:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing storage_generation=shared_blocks_v3; "
            "reimport the snapshot"
        )
    if cold_lookup_contract != PTG2_V3_COLD_LOOKUP_CONTRACT:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing cold_lookup_contract=ptg_v3_cold_v2; "
            "reimport the snapshot"
        )
    if price_membership_semantics != PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing "
            "price_membership_semantics=multiset_v1; reimport the snapshot"
        )
    if serving_multiplicity_semantics != PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing "
            "serving_multiplicity_semantics=source_multiset_v1; reimport the snapshot"
        )
    if shared_snapshot_key is None or shared_snapshot_key <= 0:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing a positive shared_snapshot_key; "
            "reimport the snapshot"
        )
    if source_count is None or source_count <= 0 or source_count > 2**31:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing a valid source_count; "
            "reimport the snapshot"
        )
    _strict_coverage_scope_id(serving_index)

    required_marker_values_by_field = {
        "storage": "manifest_snapshot",
        "type": PTG2_V3_STORAGE_TYPE,
        "provider_scope_strategy": "postgres_shared_graph",
        "id_storage": "binary128",
        "serving_table_layout": PTG2_V3_SERVING_LAYOUT,
        "shared_block_layout": PTG2_V3_SHARED_BLOCK_LAYOUT,
    }
    for field_name, expected_value in required_marker_values_by_field.items():
        actual_value = str(serving_index.get(field_name) or "").strip().lower()
        if actual_value != expected_value:
            raise PTG2ManifestArtifactError(
                f"PTG2 postgres_binary_v3 snapshot is missing {field_name}={expected_value}; "
                "reimport the snapshot"
            )
    if serving_index.get("snapshot_scoped") is not True:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot must be snapshot_scoped; reimport the snapshot"
        )

    materialized_tables = serving_index.get("materialized_tables")
    legacy_table_fields = (
        "table",
        "serving_binary_table",
        "price_code_set_table",
        "price_atom_table",
        "price_atom_dictionary_table",
        "price_set_entry_table",
        "procedure_table",
        "code_count_table",
        "provider_set_table",
        "provider_set_component_table",
        "provider_set_entry_table",
        "provider_entry_component_table",
        "provider_group_member_table",
        "provider_npi_scope_table",
        "provider_group_location_table",
        "provider_group_rate_scope_table",
        "provider_set_dictionary_table",
    )
    if any(serving_index.get(field_name) for field_name in legacy_table_fields) or (
        isinstance(materialized_tables, dict) and bool(materialized_tables)
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 manifests must not declare legacy materialized tables; "
            "reimport the snapshot"
        )
    if (
        serving_index.get("artifacts")
        or serving_index.get("artifact_uri")
        or serving_index.get("storage_uri")
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 manifests must not declare filesystem or sidecar artifacts; "
            "reimport the snapshot"
        )

    serving_binary = serving_index.get("serving_binary")
    required_sections = (
        "price_dictionary",
        "price_set_atom_memberships_v3",
        "price_atoms_v3",
    )
    if not isinstance(serving_binary, dict) or any(
        not isinstance(serving_binary.get(section_name), dict)
        for section_name in required_sections
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing strict serving_binary metadata; "
            "reimport the snapshot"
        )
    if str(serving_binary.get("format") or "").strip().lower() != PTG2_V3_ARCH_VERSION:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot has an invalid serving_binary format; "
            "reimport the snapshot"
        )

    price_dictionary = serving_binary["price_dictionary"]
    membership = serving_binary["price_set_atom_memberships_v3"]
    price_atoms = serving_binary["price_atoms_v3"]

    def required_integer(section: dict[str, Any], field_name: str) -> int:
        """Read one required non-null integer from a manifest section."""

        value = _optional_integer(section.get(field_name))
        if value is None:
            raise PTG2ManifestArtifactError(
                f"PTG2 postgres_binary_v3 snapshot is missing {field_name}; reimport the snapshot"
            )
        return value

    price_set_count = required_integer(price_dictionary, "price_set_count")
    block_bytes = required_integer(price_dictionary, "block_bytes")
    membership_span = required_integer(membership, "block_span")
    atom_span = required_integer(price_atoms, "block_span")
    atom_key_bits = _serving_index_atom_key_bits(serving_index)
    if price_set_count < 0 or block_bytes < 16 or block_bytes % 16 != 0:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 price dictionary metadata is invalid; reimport the snapshot"
        )
    if membership_span <= 0 or atom_span <= 0 or atom_key_bits not in {24, 32}:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 sparse price metadata is invalid; reimport the snapshot"
        )
    if str(price_dictionary.get("artifact_kind") or "") != "by_code_price_dictionary":
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 price dictionary kind is invalid; reimport the snapshot"
        )
    return (
        shared_snapshot_key,
        storage_generation,
        cold_lookup_contract,
        _strict_v3_audit_sample(serving_index, source_count=source_count),
    )


async def snapshot_serving_tables(
    session: Any,
    snapshot_id: str,
    *,
    candidate_audit_access: PTG2CandidateAuditAccess | None = None,
) -> PTG2ServingTables:
    """Load one published snapshot through its sealed shared-layout binding."""

    query_params_by_name: dict[str, Any] = {
        "snapshot_id": str(snapshot_id),
        "storage_generation": PTG2_V3_SHARED_GENERATION,
    }
    if candidate_audit_access is not None:
        if candidate_audit_access.snapshot_id != str(snapshot_id):
            raise PTG2ManifestArtifactError("PTG2 snapshot is unavailable")
        query_params_by_name.update(
            candidate_activation_contract=PTG2_CANDIDATE_ACTIVATION_CONTRACT,
            candidate_source_key=candidate_audit_access.source_key,
            candidate_plan_ids=ein_plan_id_variants(candidate_audit_access.plan_id),
            candidate_plan_market_type=candidate_audit_access.plan_market_type,
        )
        query_sql = f"""
            SELECT snapshot.manifest->'serving_index' AS candidate_serving_index,
                   layout.layout_manifest->'serving_index'->'audit_sample'
                       AS layout_audit_sample,
                   layout.layout_manifest->'serving_index'->>'coverage_scope_id'
                       AS layout_coverage_scope_id,
                   layout.layout_manifest->'serving_index'->>'code_count'
                       AS layout_code_count,
                   snapshot_scope.plan_id AS snapshot_plan_id,
                   snapshot_scope.plan_market_type AS snapshot_plan_market_type,
                   encode(snapshot_scope.coverage_scope_id, 'hex')
                       AS snapshot_coverage_scope_id,
                   current_setting('server_version_num')::integer
                       AS postgres_server_version_num,
                   current_database() IS NOT NULL AS database_selected,
                   pg_backend_pid() > 0 AS backend_session_active,
                   txid_current_snapshot() IS NOT NULL
                       AS transaction_snapshot_observed
              FROM {PTG2_SCHEMA}.ptg2_snapshot snapshot
              JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_binding binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_layout layout
                ON layout.snapshot_key = binding.snapshot_key
              JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_scope snapshot_scope
                ON snapshot_scope.snapshot_id = snapshot.snapshot_id
             WHERE snapshot.snapshot_id = :snapshot_id
               AND snapshot.status = 'validated'
               AND snapshot.manifest->'activation'->>'contract'
                   = :candidate_activation_contract
               AND snapshot.manifest->'activation'->>'state' = 'validated'
               AND lower(btrim(COALESCE(
                   snapshot.manifest->'activation'->>'source_key', ''
               ))) = :candidate_source_key
               AND lower(btrim(COALESCE(
                   snapshot.manifest->'serving_index'->>'source_key', ''
               ))) = :candidate_source_key
               AND layout.state = 'sealed'
               AND layout.generation = :storage_generation
               AND EXISTS (
                   SELECT 1
                     FROM {PTG2_SCHEMA}.ptg2_v3_snapshot_plan_scope
                          AS candidate_plan_scope
                    WHERE candidate_plan_scope.snapshot_id = snapshot.snapshot_id
                      AND candidate_plan_scope.plan_id
                          = ANY(CAST(:candidate_plan_ids AS text[]))
                      AND candidate_plan_scope.plan_market_type
                          = :candidate_plan_market_type
               )
               AND binding.snapshot_key = CASE
                   WHEN snapshot.manifest->'serving_index'->>'shared_snapshot_key'
                        ~ '^[1-9][0-9]*$'
                   THEN (
                       snapshot.manifest->'serving_index'->>'shared_snapshot_key'
                   )::bigint
                   ELSE NULL
               END
             LIMIT 1
        """
    else:
        query_params_by_name["attestation_contracts"] = list(
            PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
        )
        query_sql = f"""
            SELECT layout.layout_manifest->'serving_index'
                       AS layout_serving_index,
                   binding.snapshot_key AS bound_snapshot_key,
                   snapshot_scope.plan_id AS snapshot_plan_id,
                   snapshot_scope.plan_market_type AS snapshot_plan_market_type,
                   encode(snapshot_scope.coverage_scope_id, 'hex')
                       AS snapshot_coverage_scope_id,
                   attestation.source_key AS attested_source_key,
                   encode(attestation.coverage_scope_id, 'hex')
                       AS attested_coverage_scope_id,
                   encode(attestation.source_set_digest, 'hex')
                       AS attested_source_set_digest,
                   encode(attestation.audit_sample_digest, 'hex')
                       AS attested_audit_sample_digest,
                   source_summary.source_row_count,
                   source_summary.distinct_source_key_count,
                   source_summary.minimum_source_key,
                   source_summary.maximum_source_key,
                   current_setting('server_version_num')::integer
                       AS postgres_server_version_num,
                   current_database() IS NOT NULL AS database_selected,
                   pg_backend_pid() > 0 AS backend_session_active,
                   txid_current_snapshot() IS NOT NULL
                       AS transaction_snapshot_observed
              FROM {PTG2_SCHEMA}.ptg2_snapshot snapshot
              JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_binding binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_layout layout
                ON layout.snapshot_key = binding.snapshot_key
              JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_scope snapshot_scope
                ON snapshot_scope.snapshot_id = snapshot.snapshot_id
              JOIN {PTG2_SCHEMA}.ptg2_v3_candidate_audit_attestation attestation
                ON attestation.snapshot_id = snapshot.snapshot_id
               AND attestation.snapshot_key = binding.snapshot_key
               AND attestation.coverage_scope_id
                   = snapshot_scope.coverage_scope_id
              CROSS JOIN LATERAL (
                  SELECT COUNT(*)::bigint AS source_row_count,
                         COUNT(DISTINCT source.source_key)::bigint
                             AS distinct_source_key_count,
                         MIN(source.source_key) AS minimum_source_key,
                         MAX(source.source_key) AS maximum_source_key
                    FROM {PTG2_SCHEMA}.ptg2_v3_snapshot_source source
                   WHERE source.snapshot_id = snapshot.snapshot_id
              ) source_summary
             WHERE snapshot.snapshot_id = :snapshot_id
               AND snapshot.status = 'published'
               AND layout.state = 'sealed'
               AND layout.generation = :storage_generation
               AND attestation.contract = ANY(
                   CAST(:attestation_contracts AS text[])
               )
               AND attestation.activated_at IS NOT NULL
               AND attestation.plan_id = snapshot_scope.plan_id
               AND attestation.plan_market_type
                   = snapshot_scope.plan_market_type
             LIMIT 1
        """
    snapshot_query = await session.execute(
        text(query_sql),
        query_params_by_name,
    )
    snapshot_record = snapshot_query.one_or_none()
    if snapshot_record is None:
        raise PTG2ManifestArtifactError(
            "PTG2 snapshot is not published and bound to a sealed shared V3 layout"
        )
    row_fields = (
        snapshot_record
        if isinstance(snapshot_record, dict)
        else snapshot_record._mapping
    )
    if candidate_audit_access is not None:
        serving_index = row_fields.get("candidate_serving_index")
        if isinstance(serving_index, str):
            try:
                serving_index = json.loads(serving_index)
            except json.JSONDecodeError as exc:
                raise PTG2ManifestArtifactError(
                    "PTG2 serving_index manifest is malformed"
                ) from exc
    else:
        serving_index = row_fields.get("layout_serving_index")
    if isinstance(serving_index, str):
        try:
            serving_index = json.loads(serving_index)
        except json.JSONDecodeError as exc:
            raise PTG2ManifestArtifactError(
                "PTG2 serving_index manifest is malformed"
            ) from exc
    if not isinstance(serving_index, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 snapshot is missing a strict serving_index; reimport the snapshot"
        )

    (
        shared_snapshot_key,
        storage_generation,
        cold_lookup_contract,
        audit_sample,
    ) = _strict_v3_manifest_fields(serving_index)
    coverage_scope_id = _strict_coverage_scope_id(serving_index)
    code_count = _optional_integer(serving_index.get("code_count"))
    source_count = _optional_integer(serving_index.get("source_count"))
    if candidate_audit_access is not None:
        layout_audit_sample = row_fields.get("layout_audit_sample")
        if isinstance(layout_audit_sample, str):
            try:
                layout_audit_sample = json.loads(layout_audit_sample)
            except json.JSONDecodeError as exc:
                raise PTG2ManifestArtifactError(
                    "PTG2 sealed layout audit sample is malformed"
                ) from exc
        if (
            not isinstance(layout_audit_sample, dict)
            or layout_audit_sample != audit_sample
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 sealed layout audit sample does not match its snapshot manifest"
            )
        if str(row_fields.get("layout_coverage_scope_id") or "") != coverage_scope_id:
            raise PTG2ManifestArtifactError(
                "PTG2 sealed layout coverage scope does not match its snapshot manifest"
            )
        if str(row_fields.get("snapshot_coverage_scope_id") or "") != coverage_scope_id:
            raise PTG2ManifestArtifactError(
                "PTG2 snapshot coverage scope binding does not match its manifest"
            )
        layout_code_count = _optional_integer(row_fields.get("layout_code_count"))
        if code_count is None or code_count < 0 or layout_code_count != code_count:
            raise PTG2ManifestArtifactError(
                "PTG2 sealed layout code count does not match its snapshot manifest"
            )
        source_set_by_field = _strict_v3_source_set(
            serving_index,
            source_count=int(source_count or 0),
        )
        source_key = (
            str(serving_index.get("source_key") or "").strip().lower() or None
        )
        if source_key != candidate_audit_access.source_key:
            raise PTG2ManifestArtifactError(
                "PTG2 candidate source does not match its snapshot manifest"
            )
    else:
        bound_snapshot_key = _optional_integer(row_fields.get("bound_snapshot_key"))
        if bound_snapshot_key != shared_snapshot_key:
            raise PTG2ManifestArtifactError(
                "PTG2 sealed layout binding does not match its metadata"
            )
        if (
            str(row_fields.get("snapshot_coverage_scope_id") or "")
            != coverage_scope_id
            or str(row_fields.get("attested_coverage_scope_id") or "")
            != coverage_scope_id
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 published scope does not match its sealed layout"
            )
        if str(row_fields.get("attested_audit_sample_digest") or "") != str(
            audit_sample.get("sample_digest") or ""
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 published audit attestation does not match its sealed sample"
            )
        source_row_count = _optional_integer(row_fields.get("source_row_count"))
        distinct_source_key_count = _optional_integer(
            row_fields.get("distinct_source_key_count")
        )
        minimum_source_key = _optional_integer(
            row_fields.get("minimum_source_key")
        )
        maximum_source_key = _optional_integer(
            row_fields.get("maximum_source_key")
        )
        if (
            source_count is None
            or source_count <= 0
            or source_row_count != source_count
            or distinct_source_key_count != source_count
            or minimum_source_key != 0
            or maximum_source_key != source_count - 1
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 published source dictionary is not complete and dense"
            )
        source_set_digest = str(
            row_fields.get("attested_source_set_digest") or ""
        )
        if not _COVERAGE_SCOPE_ID_RE.fullmatch(source_set_digest):
            raise PTG2ManifestArtifactError(
                "PTG2 published source-set attestation is invalid"
            )
        source_set_by_field = {
            "contract": PTG2_V3_SOURCE_SET_CONTRACT,
            "source_count": source_count,
            "raw_container_sha256_digest": source_set_digest,
        }
        source_key = (
            str(row_fields.get("attested_source_key") or "").strip() or None
        )
        if source_key is None:
            raise PTG2ManifestArtifactError(
                "PTG2 published source key is missing from its attestation"
            )
        if code_count is None or code_count < 0:
            raise PTG2ManifestArtifactError(
                "PTG2 sealed layout code count is invalid"
            )
    serving_rate_count = _optional_integer(serving_index.get("serving_rates"))
    if code_count == 0 and serving_rate_count is not None and serving_rate_count > 0:
        raise PTG2ManifestArtifactError(
            "PTG2 shared layout is missing code metadata for a non-empty snapshot"
        )
    network_names = serving_index.get("network_names")
    return PTG2ServingTables(
        snapshot_id=str(snapshot_id),
        arch_version=PTG2_V3_ARCH_VERSION,
        storage="manifest_snapshot",
        shared_snapshot_key=shared_snapshot_key,
        storage_generation=storage_generation,
        cold_lookup_contract=cold_lookup_contract,
        serving_table_layout=PTG2_V3_SERVING_LAYOUT,
        shared_block_layout=PTG2_V3_SHARED_BLOCK_LAYOUT,
        source_count=source_count,
        code_count=code_count,
        coverage_scope_id=coverage_scope_id,
        plan_id=str(row_fields.get("snapshot_plan_id") or "").strip() or None,
        plan_market_type=(
            str(row_fields.get("snapshot_plan_market_type") or "").strip() or None
        ),
        source_key=source_key,
        audit_sample=audit_sample,
        source_set=source_set_by_field,
        database_evidence=_database_execution_evidence(row_fields),
        source_trace_set_hash=str(
            serving_index.get("source_trace_set_hash") or ""
        ).strip()
        or None,
        network_names=(
            [str(network_name) for network_name in network_names]
            if isinstance(network_names, list)
            else None
        ),
        price_atom_constant_values=(
            dict(serving_index.get("price_atom_constant_values") or {})
            if isinstance(serving_index.get("price_atom_constant_values"), dict)
            else None
        ),
        price_dictionary_item_count=_serving_binary_section_integer(
            serving_index,
            "price_dictionary",
            "price_set_count",
        ),
        price_dictionary_block_bytes=_serving_binary_section_integer(
            serving_index,
            "price_dictionary",
            "block_bytes",
        ),
        provider_shard_span=_serving_binary_section_integer(
            serving_index,
            "assigned_encoder",
            "provider_shard_span",
        ),
        atom_key_bits=_serving_index_atom_key_bits(serving_index),
        price_key_block_span=_serving_binary_section_integer(
            serving_index,
            "price_set_atom_memberships_v3",
            "block_span",
        ),
        atom_key_block_span=_serving_binary_section_integer(
            serving_index,
            "price_atoms_v3",
            "block_span",
        ),
    )
