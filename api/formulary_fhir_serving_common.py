# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared validation, SQL naming, and response helpers for FHIR formularies."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from sanic.exceptions import InvalidUsage


FHIR_FORMULARY_ID_RE = re.compile(r"^fhir_[a-z2-7]{26}$")
FHIR_SOURCE_TYPE = "fhir"
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def is_fhir_formulary_id(formulary_id: str) -> bool:
    """Return whether a route value is a strict public FHIR formulary ID."""

    return bool(FHIR_FORMULARY_ID_RE.fullmatch(str(formulary_id or "")))


def source_selection(args) -> str:
    """Resolve and validate legacy, FHIR, and explicit union source filters."""

    requested_type = str(args.get("source_type") or "").strip().lower()
    source_id = str(args.get("source_id") or "").strip()
    source_plan_identifier = str(
        args.get("source_plan_identifier") or ""
    ).strip()
    if requested_type:
        if requested_type not in {"legacy", "fhir", "all"}:
            raise InvalidUsage("source_type must be legacy, fhir, or all")
        selection = requested_type
    else:
        selection = "fhir" if source_id or source_plan_identifier else "legacy"
    if selection == "legacy" and (source_id or source_plan_identifier):
        raise InvalidUsage(
            "FHIR source filters cannot be combined with source_type=legacy"
        )
    has_annual_filter = any(
        args.get(filter_name) not in (None, "")
        for filter_name in ("year", "state", "issuer_id")
    )
    if selection == "fhir" and has_annual_filter:
        raise InvalidUsage(
            "annual legacy filters cannot be combined with a FHIR-only query"
        )
    return selection


def quoted_identifier(identifier: str) -> str:
    """Quote one PostgreSQL identifier without admitting SQL syntax."""

    return '"' + identifier.replace('"', '""') + '"'


def table_name(name: str) -> str:
    """Return one configured schema-qualified table name."""

    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    return f"{quoted_identifier(schema)}.{quoted_identifier(name)}"


def row_mapping(database_row) -> dict[str, Any]:
    """Normalize a SQLAlchemy or mapping row into a dictionary."""

    return dict(getattr(database_row, "_mapping", database_row))


def iso_value(timestamp):
    """Serialize timestamp-like values without changing scalar values."""

    if timestamp is not None and hasattr(timestamp, "isoformat"):
        return timestamp.isoformat()
    return timestamp


def json_value(json_data, default):
    """Decode JSON text while retaining already-decoded database values."""

    if json_data is None:
        return default
    if isinstance(json_data, str):
        try:
            return json.loads(json_data)
        except json.JSONDecodeError:
            return default
    return json_data


def source_conditions(
    args,
    query_params_by_name: dict[str, Any],
    *,
    alias_name: str = "a",
) -> list[str]:
    """Append validated FHIR source filters and their bind parameters."""

    conditions: list[str] = []
    source_id = str(args.get("source_id") or "").strip()
    if source_id:
        query_params_by_name["source_id"] = source_id
        conditions.append("cp.source_id = :source_id")
    source_plan_identifier = str(
        args.get("source_plan_identifier") or ""
    ).strip()
    if source_plan_identifier:
        query_params_by_name["source_plan_identifier"] = (
            source_plan_identifier
        )
        conditions.append(
            f"{alias_name}.source_plan_identifier = :source_plan_identifier"
        )
    return conditions


def current_join() -> str:
    """Return the published-generation join shared by serving queries."""

    return (
        f"FROM {table_name('fhir_formulary_current')} cur "
        f"JOIN {table_name('fhir_formulary_dataset')} d "
        "ON d.dataset_id = cur.dataset_id AND d.status = 'published' "
        f"JOIN {table_name('fhir_formulary_dataset_coverage_plan')} dcp "
        "ON dcp.dataset_id = d.dataset_id "
        f"JOIN {table_name('fhir_formulary_coverage_plan')} cp "
        "ON cp.public_id = dcp.public_id "
        f"JOIN {table_name('fhir_formulary_coverage_plan_version')} cpv "
        "ON cpv.coverage_version_id = dcp.coverage_version_id "
        f"JOIN {table_name('fhir_formulary_drug_plan_alias')} a "
        "ON a.public_id = cp.public_id "
        f"JOIN {table_name('fhir_formulary_dataset_alias')} da "
        "ON da.dataset_id = d.dataset_id AND da.alias_id = a.alias_id "
        f"JOIN {table_name('fhir_formulary_drug_plan_alias_version')} av "
        "ON av.alias_version_id = da.alias_version_id "
    )


def optional_bool(raw_value, parameter_name: str) -> bool | None:
    """Parse an optional boolean-like request argument."""

    if raw_value in (None, "", "null"):
        return None
    normalized_value = str(raw_value).strip().lower()
    if normalized_value in {"true", "1", "yes"}:
        return True
    if normalized_value in {"false", "0", "no"}:
        return False
    raise InvalidUsage(f"Parameter '{parameter_name}' must be boolean-like")


def source_plan_identifier(args) -> str | None:
    """Return one normalized optional DrugPlan alias filter."""

    return str(args.get("source_plan_identifier") or "").strip() or None


def upstream_payload(plan_by_field: dict[str, Any]) -> dict[str, Any]:
    """Shape upstream CoveragePlan provenance for a public response."""

    return {
        "resource_type": "List",
        "id": plan_by_field["upstream_list_id"],
        "version_id": plan_by_field.get("upstream_version_id"),
        "status": plan_by_field.get("status")
        or plan_by_field.get("upstream_status"),
        "last_updated": iso_value(plan_by_field.get("upstream_last_updated")),
    }


def dataset_payload(dataset_by_field: dict[str, Any]) -> dict[str, Any]:
    """Shape immutable published-dataset provenance for a response."""

    return {
        "dataset_id": dataset_by_field["dataset_id"],
        "cutoff": iso_value(dataset_by_field.get("cutoff_at")),
        "published_at": iso_value(dataset_by_field.get("published_at")),
        "coverage_hash": dataset_by_field.get("coverage_hash"),
        "membership_hash": dataset_by_field.get("membership_hash"),
    }
