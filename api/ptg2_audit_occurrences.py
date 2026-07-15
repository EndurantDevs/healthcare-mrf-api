# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict, cache-free HTTP payloads for persisted PTG V3 audit occurrences."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

import orjson
from sanic.exceptions import InvalidUsage
from sqlalchemy import text

from api.code_systems import canonical_catalog_code, normalize_code_system
from api.ptg2_candidate_audit import candidate_audit_access_from_args
from api.ptg2_db_sidecars import lookup_shared_price_atoms_from_db
from api.ptg2_response import _canonical_price_row
from api.ptg2_serving import (
    _version_three_dictionary_values,
    _version_three_price_payload,
)
from api.ptg2_serving_utils import ein_plan_id_variants
from api.ptg2_snapshot import current_snapshot_id
from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    fetch_snapshot_source_provenance,
    fetch_snapshot_source_set_metadata,
)
from api.ptg2_tables import (
    PTG2_SCHEMA,
    PTG2_V3_AUDIT_CONTRACT,
    PTG2_V3_AUDIT_METHOD,
    snapshot_serving_tables,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_shared_audit import persisted_audit_sample_digest
from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_SHARED_GENERATION


AUDIT_QUERY_MODE = "exact_source"
AUDIT_PRICING_SCOPE = "plan_scoped_ptg"
AUDIT_ORDER_BY = "occurrence_id"
AUDIT_ORDER = "asc"
AUDIT_MAX_LIMIT = 100
AUDIT_MAX_CANONICAL_NUMBER_CHARS = 131_072


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _required_argument(args: Mapping[str, Any], name: str) -> str:
    value = str(args.get(name) or "").strip()
    if not value:
        raise InvalidUsage(f"Parameter '{name}' is required")
    return value


def _required_integer(
    args: Mapping[str, Any],
    name: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    raw_value = args.get(name)
    if raw_value in (None, "", "null"):
        raise InvalidUsage(f"Parameter '{name}' is required")
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError) as exc:
        raise InvalidUsage(f"Parameter '{name}' must be an integer") from exc
    if value < minimum:
        raise InvalidUsage(f"Parameter '{name}' must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise InvalidUsage(f"Parameter '{name}' must be <= {maximum}")
    return value


def _require_exact_argument(args: Mapping[str, Any], name: str, expected: str) -> str:
    value = _required_argument(args, name).lower()
    if value != expected:
        raise InvalidUsage(f"Parameter '{name}' must be '{expected}'")
    return value


def _numeric_json_fragment(value: Any) -> orjson.Fragment:
    """Preserve the stored decimal exactly while emitting a JSON number."""

    text_value = ("" if value is None else str(value)).strip()
    try:
        decimal_value = Decimal(text_value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit occurrence has an invalid negotiated rate"
        ) from exc
    if not decimal_value.is_finite():
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit occurrence has a non-finite negotiated rate"
        )
    expanded = format(decimal_value, "f")
    if decimal_value.is_zero():
        expanded = "0"
    elif "." in expanded:
        expanded = expanded.rstrip("0").rstrip(".")
    if expanded in {"", "-0"}:
        expanded = "0"
    if len(expanded) > AUDIT_MAX_CANONICAL_NUMBER_CHARS:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit occurrence negotiated rate is too large"
        )
    return orjson.Fragment(expanded.encode("ascii"))


def _canonical_identity(row: Mapping[str, Any]) -> tuple[str, str, str | None]:
    code_system = normalize_code_system(row.get("reported_code_system"))
    code = canonical_catalog_code(code_system, row.get("reported_code"))
    if not code_system or not code:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit occurrence has an invalid code identity"
        )
    arrangement_value = row.get("negotiation_arrangement")
    arrangement = (
        str(arrangement_value).strip().upper()
        if arrangement_value not in (None, "")
        else None
    )
    return code_system, code, arrangement


def _optional_exact_text(row: Mapping[str, Any], field_name: str) -> str | None:
    value = row.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise PTG2ManifestArtifactError(
            f"PTG2 v3 audit occurrence has invalid {field_name} metadata"
        )
    return value


def _exact_network_names(row: Mapping[str, Any]) -> list[str]:
    values = row.get("network_names")
    if not isinstance(values, (list, tuple)) or any(
        not isinstance(value, str) for value in values
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit occurrence has invalid provider-set network metadata"
        )
    return list(values)


def _exact_scalar_or_text_array(value: Any, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, (list, tuple)) or any(
        not isinstance(item, str) for item in value
    ):
        raise PTG2ManifestArtifactError(
            f"PTG2 v3 audit occurrence has invalid {field_name} metadata"
        )
    return list(value)


def _audit_tuple(
    occurrence: Mapping[str, Any],
    *,
    price_atom: Any,
    dictionary_values: Mapping[tuple[str, int], Any],
    constant_values: Mapping[str, Any],
) -> dict[str, Any]:
    code_system, code, arrangement = _canonical_identity(occurrence)
    raw_price_payload = _version_three_price_payload(
        price_atom,
        dictionary_values,
        constant_values,
    )
    service_codes = _exact_scalar_or_text_array(
        raw_price_payload.get("service_code"),
        field_name="service_code",
    )
    modifiers = _exact_scalar_or_text_array(
        raw_price_payload.get("billing_code_modifier"),
        field_name="billing_code_modifier",
    )
    price_payload = _canonical_price_row(raw_price_payload)
    price_payload["service_code"] = sorted(
        {
            canonical_code
            for service_code_value in service_codes
            if (canonical_code := canonical_catalog_code("POS", service_code_value))
        }
    )
    price_payload["billing_code_modifier"] = sorted(
        {
            modifier
            for modifier_value in modifiers
            if (modifier := modifier_value.strip().upper())
        }
    )
    price_payload["negotiated_rate"] = _numeric_json_fragment(
        price_atom.negotiated_rate
    )
    return {
        "code_system": code_system,
        "code": code,
        "npi": int(occurrence["npi"]),
        "negotiation_arrangement": arrangement,
        "billing_code_type_version": _optional_exact_text(
            occurrence, "billing_code_type_version"
        ),
        "name": _optional_exact_text(occurrence, "source_name"),
        "description": _optional_exact_text(occurrence, "source_description"),
        "network_names": _exact_network_names(occurrence),
        "negotiated_type": price_payload.get("negotiated_type"),
        "negotiated_rate": price_payload["negotiated_rate"],
        "expiration_date": price_payload.get("expiration_date"),
        "service_code": list(price_payload.get("service_code") or []),
        "billing_class": price_payload.get("billing_class"),
        "setting": price_payload.get("setting"),
        "billing_code_modifier": list(price_payload.get("billing_code_modifier") or []),
        "additional_information": price_payload.get("additional_information"),
    }


def _audit_source_payload(
    *,
    source_artifact_key: int,
    logical_source_key: str | None,
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    provenance_source_key = provenance.get("source_key")
    if (
        isinstance(provenance_source_key, bool)
        or provenance_source_key is None
        or int(provenance_source_key) != int(source_artifact_key)
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit occurrence source provenance is inconsistent"
        )
    payload = {
        key: value
        for key, value in provenance.items()
        if key != "source_key"
    }
    payload["source_artifact_key"] = int(source_artifact_key)
    normalized_logical_key = str(logical_source_key or "").strip()
    if normalized_logical_key:
        payload["source_key"] = normalized_logical_key
    return payload


def _audit_digest_coordinates(row: Mapping[str, Any]) -> dict[str, int]:
    """Expose the persisted fields covered by the sealed sample digest."""

    return {
        "code_key": int(row["code_key"]),
        "provider_set_key": int(row["provider_set_key"]),
        "price_key": int(row["price_key"]),
        "source_artifact_key": int(row["source_key"]),
        "npi": int(row["npi"]),
        "atom_ordinal": int(row["atom_ordinal"]),
        "atom_key": int(row["atom_key"]),
    }


def _audit_page_sql(*, filter_market_type: bool) -> str:
    """Build the audit-page query, optionally filtering scope by market type."""

    market_filter = (
        "AND logical_scope.plan_market_type = :plan_market_type"
        if filter_market_type
        else ""
    )
    return f"""
        WITH matching_scope AS MATERIALIZED (
            SELECT logical_scope.coverage_scope_id
              FROM {PTG2_SCHEMA}.ptg2_v3_snapshot_scope logical_scope
             WHERE logical_scope.snapshot_id = :snapshot_id
               AND logical_scope.plan_id = ANY(CAST(:plan_ids AS text[]))
               {market_filter}
        ),
        scope_summary AS (
            SELECT (SELECT COUNT(*) FROM matching_scope) AS scope_count,
                   (SELECT coverage_scope_id FROM matching_scope LIMIT 1)
                       AS coverage_scope_id
        ),
        sample_total AS (
            SELECT COUNT(*) AS total
              FROM {PTG2_SCHEMA}.ptg2_v3_audit_occurrence audit
             WHERE audit.snapshot_key = :shared_snapshot_key
        ),
        sample_page AS MATERIALIZED (
            SELECT audit.occurrence_id,
                   audit.code_key,
                   audit.provider_set_key,
                   audit.price_key,
                   audit.source_key,
                   audit.npi,
                   audit.atom_ordinal,
                   audit.atom_key
              FROM {PTG2_SCHEMA}.ptg2_v3_audit_occurrence audit
             WHERE audit.snapshot_key = :shared_snapshot_key
             ORDER BY audit.occurrence_id ASC
             LIMIT :limit OFFSET :offset
        )
        SELECT scope_summary.scope_count,
               sample_total.total,
               sample_page.occurrence_id,
               sample_page.code_key,
               sample_page.provider_set_key,
               sample_page.price_key,
               sample_page.source_key,
               sample_page.npi,
               sample_page.atom_ordinal,
               sample_page.atom_key,
               code.reported_code_system,
               code.reported_code,
               code.negotiation_arrangement,
               code.billing_code_type_version,
               code.source_name,
               code.source_description,
               provider_set.network_names,
               CASE
                   WHEN code.coverage_scope_id = scope_summary.coverage_scope_id
                   THEN TRUE ELSE FALSE
               END AS code_scope_matches,
               provider_set.provider_set_key IS NOT NULL
                   AS provider_set_scope_matches
          FROM scope_summary
         CROSS JOIN sample_total
          LEFT JOIN sample_page ON TRUE
          LEFT JOIN {PTG2_SCHEMA}.ptg2_v3_code code
            ON code.snapshot_key = :shared_snapshot_key
           AND code.code_key = sample_page.code_key
          LEFT JOIN {PTG2_SCHEMA}.ptg2_v3_provider_set provider_set
            ON provider_set.snapshot_key = :shared_snapshot_key
           AND provider_set.provider_set_key = sample_page.provider_set_key
         ORDER BY sample_page.occurrence_id ASC
    """


async def audit_occurrences_payload(
    session: Any,
    args: Mapping[str, Any],
) -> dict[str, Any]:
    """Return one strict page from a persisted shared-layout audit sample."""

    plan_id = _required_argument(args, "plan_id")
    requested_snapshot_id = _required_argument(args, "snapshot_id")
    mode = _require_exact_argument(args, "mode", AUDIT_QUERY_MODE)
    order_by = _require_exact_argument(args, "order_by", AUDIT_ORDER_BY)
    order = _require_exact_argument(args, "order", AUDIT_ORDER)
    limit = _required_integer(args, "limit", minimum=1, maximum=AUDIT_MAX_LIMIT)
    offset = _required_integer(args, "offset", minimum=0)
    plan_market_type = str(args.get("plan_market_type") or "").strip().lower()
    requested_source_key = str(args.get("source_key") or "").strip()
    candidate_audit_access = candidate_audit_access_from_args(args)

    resolved_snapshot_id = await current_snapshot_id(
        session,
        requested_snapshot_id=requested_snapshot_id,
        requested_source_key=requested_source_key or None,
        requested_plan_id=plan_id,
        requested_plan_market_type=plan_market_type or None,
        candidate_audit_access=candidate_audit_access,
    )
    if resolved_snapshot_id != requested_snapshot_id:
        raise InvalidUsage(
            "Parameter 'snapshot_id' must identify a published sealed PTG V3 snapshot"
        )
    serving_tables = await snapshot_serving_tables(
        session,
        resolved_snapshot_id,
        candidate_audit_access=candidate_audit_access,
    )
    logical_source_key = str(serving_tables.source_key or "").strip()
    if requested_source_key and requested_source_key != logical_source_key:
        raise InvalidUsage(
            "Parameter 'source_key' does not identify the requested logical snapshot"
        )
    shared_snapshot_key = serving_tables.shared_snapshot_key
    if not serving_tables.uses_shared_blocks or shared_snapshot_key is None:
        raise PTG2ManifestArtifactError(
            "PTG2 audit occurrences require a sealed shared-block V3 snapshot"
        )
    sealed_source_set = serving_tables.source_set
    if not isinstance(sealed_source_set, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 audit occurrences require a sealed complete source set"
        )
    database_evidence = serving_tables.database_evidence
    if not isinstance(database_evidence, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 audit occurrences require PostgreSQL execution evidence"
        )
    try:
        observed_source_set = await fetch_snapshot_source_set_metadata(
            session,
            schema_name=PTG2_SCHEMA,
            logical_snapshot_id=resolved_snapshot_id,
            expected_source_count=int(serving_tables.source_count or 0),
        )
    except PTG2SharedBlockError as exc:
        raise PTG2ManifestArtifactError(str(exc)) from exc
    if observed_source_set != sealed_source_set:
        raise PTG2ManifestArtifactError(
            "PTG2 snapshot source rows disagree with the sealed source set"
        )
    audit_sample = serving_tables.audit_sample
    if not isinstance(audit_sample, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 audit occurrences require a sealed persisted audit sample"
        )

    query_result = await session.execute(
        text(_audit_page_sql(filter_market_type=bool(plan_market_type))),
        {
            "snapshot_id": resolved_snapshot_id,
            "plan_ids": ein_plan_id_variants(plan_id),
            "plan_market_type": plan_market_type,
            "shared_snapshot_key": int(shared_snapshot_key),
            "limit": limit,
            "offset": offset,
        },
    )
    result_rows = [
        _row_mapping(result_row_by_field)
        for result_row_by_field in query_result
    ]
    if not result_rows:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit sample query returned no contract row"
        )
    first_row = result_rows[0]
    if int(first_row.get("scope_count") or 0) != 1:
        raise InvalidUsage(
            "Parameters 'plan_id' and 'snapshot_id' do not identify one plan scope"
        )
    total = int(first_row.get("total") or 0)
    if total != int(audit_sample["sample_count"]):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 persisted audit rows disagree with the sealed sample manifest"
        )
    digest_result = await session.execute(
        text(
            f"""
            SELECT occurrence_id, code_key, provider_set_key, price_key,
                   source_key, npi, atom_ordinal, atom_key
              FROM {PTG2_SCHEMA}.ptg2_v3_audit_occurrence
             WHERE snapshot_key = :shared_snapshot_key
             ORDER BY occurrence_id
            """
        ),
        {"shared_snapshot_key": int(shared_snapshot_key)},
    )
    digest_rows = [
        _row_mapping(digest_row_by_field)
        for digest_row_by_field in digest_result
    ]
    if len(digest_rows) != total:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 persisted audit rows disagree with the sealed sample count"
        )
    observed_digest = persisted_audit_sample_digest(digest_rows)
    if observed_digest != str(audit_sample["sample_digest"]):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 persisted audit rows disagree with the sealed sample digest"
        )
    digest_rows_by_occurrence_id = {
        bytes(digest_row_by_field["occurrence_id"]): digest_row_by_field
        for digest_row_by_field in digest_rows
    }
    if len(digest_rows_by_occurrence_id) != total:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 persisted audit rows contain duplicate occurrence ids"
        )
    page_rows = [
        result_row_by_field
        for result_row_by_field in result_rows
        if result_row_by_field.get("occurrence_id") is not None
    ]
    for page_row_by_field in page_rows:
        occurrence_id = bytes(page_row_by_field.get("occurrence_id") or b"")
        sealed_row = digest_rows_by_occurrence_id.get(occurrence_id)
        if (
            sealed_row is None
            or _audit_digest_coordinates(page_row_by_field)
            != _audit_digest_coordinates(sealed_row)
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 audit page rows disagree with the validated sample digest"
            )
        if not page_row_by_field.get("code_scope_matches"):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 audit occurrence references missing or out-of-scope code metadata"
            )
        if not page_row_by_field.get("provider_set_scope_matches"):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 audit occurrence references missing provider-set metadata"
            )
        if len(occurrence_id) != 32:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 audit occurrence id must contain exactly 32 bytes"
            )
        source_key = page_row_by_field.get("source_key")
        if (
            isinstance(source_key, bool)
            or source_key is None
            or int(source_key) < 0
            or int(source_key) >= int(serving_tables.source_count or 0)
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 audit occurrence has an invalid source key"
            )
        npi = int(page_row_by_field.get("npi") or 0)
        if not 1_000_000_000 <= npi <= 9_999_999_999:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 audit occurrence has an invalid NPI"
            )

    atom_keys = {
        int(page_row_by_field["atom_key"])
        for page_row_by_field in page_rows
    }
    price_atoms_by_key = await lookup_shared_price_atoms_from_db(
        session,
        int(shared_snapshot_key),
        atom_keys=atom_keys,
        atom_key_bits=serving_tables.atom_key_bits,
        block_span=serving_tables.atom_key_block_span,
        schema_name=PTG2_SCHEMA,
    )
    missing_atom_keys = atom_keys.difference(price_atoms_by_key)
    if missing_atom_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit occurrence references a missing price atom"
        )
    dictionary_values = await _version_three_dictionary_values(
        session,
        serving_tables,
        price_atoms_by_key,
    )
    constant_values = (
        serving_tables.price_atom_constant_values
        if isinstance(serving_tables.price_atom_constant_values, dict)
        else {}
    )
    selected_source_keys = {
        int(page_row_by_field["source_key"])
        for page_row_by_field in page_rows
    }
    try:
        source_provenance_by_key = (
            await fetch_snapshot_source_provenance(
                session,
                schema_name=PTG2_SCHEMA,
                logical_snapshot_id=resolved_snapshot_id,
                source_keys=selected_source_keys,
                expected_source_count=int(serving_tables.source_count or 0),
            )
            if selected_source_keys
            else {}
        )
    except PTG2SharedBlockError as exc:
        raise PTG2ManifestArtifactError(str(exc)) from exc
    if set(source_provenance_by_key) != selected_source_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 audit occurrence source mapping is missing"
        )
    occurrence_items = [
        {
            "occurrence_id": bytes(page_row_by_field["occurrence_id"]).hex(),
            "digest_coordinates": _audit_digest_coordinates(page_row_by_field),
            **_audit_source_payload(
                source_artifact_key=int(page_row_by_field["source_key"]),
                logical_source_key=serving_tables.source_key,
                provenance=source_provenance_by_key[
                    int(page_row_by_field["source_key"])
                ],
            ),
            "tuple": _audit_tuple(
                page_row_by_field,
                price_atom=price_atoms_by_key[
                    int(page_row_by_field["atom_key"])
                ],
                dictionary_values=dictionary_values,
                constant_values=constant_values,
            ),
        }
        for page_row_by_field in page_rows
    ]
    query_by_name = {
        "plan_id": plan_id,
        "snapshot_id": requested_snapshot_id,
        "mode": mode,
        "order_by": order_by,
        "order": order,
        "limit": limit,
        "offset": offset,
    }
    if plan_market_type:
        query_by_name["plan_market_type"] = plan_market_type
    if requested_source_key:
        query_by_name["source_key"] = requested_source_key
    provenance_by_field = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "database_backend": "postgresql",
        "plan_id": plan_id,
        "snapshot_id": resolved_snapshot_id,
        "mode": mode,
        "pricing_scope": AUDIT_PRICING_SCOPE,
        "database_evidence": dict(database_evidence),
    }
    if requested_source_key:
        provenance_by_field["source_key"] = logical_source_key
    return {
        "result_state": "matched" if total else "no_matching_rates",
        "pricing_scope": AUDIT_PRICING_SCOPE,
        "resolved_snapshot_id": resolved_snapshot_id,
        "items": occurrence_items,
        "pagination": {
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(occurrence_items) < total,
        },
        "query": query_by_name,
        "provenance": provenance_by_field,
        "source_set": dict(observed_source_set),
        "audit_sample": {
            "contract": PTG2_V3_AUDIT_CONTRACT,
            "format_version": 2,
            "method": PTG2_V3_AUDIT_METHOD,
            "sample_count": total,
            "maximum_rows": int(audit_sample["maximum_rows"]),
            "sample_digest": str(audit_sample["sample_digest"]),
            "source_count": int(audit_sample["source_count"]),
            "occurrence_identity": str(audit_sample["occurrence_identity"]),
            "complete_population": False,
            "serving_multiplicity_semantics": str(
                audit_sample["serving_multiplicity_semantics"]
            ),
        },
    }
