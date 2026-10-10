# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe exact transformed raw inputs; this is not a preventive capacity proof."""

from __future__ import annotations

import hashlib
import importlib
import json
from contextlib import asynccontextmanager

from sqlalchemy import text

from process import entity_address_candidate_preparation as candidate
from process import provider_directory_cms_native_inputs as native_inputs
from process.provider_directory_cms_preparation import desired_fence_hash

_TEXT_FIELDS = (
    "entity_type",
    "entity_id",
    "inference_method",
    "entity_name",
    "entity_subtype",
    "type",
    "base_address_version",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
    "country_code",
    "telephone_number",
    "fax_number",
    "phone_number",
    "phone_extension",
    "fax_number_digits",
    "fax_extension",
    "formatted_address",
    "formatted_address_source",
    "place_id",
    "address_source",
    "source_record_id",
)
_INTEGER_ARRAYS = ("taxonomy_array", "plans_network_array", "procedures_array", "medications_array")
_TEXT_ARRAYS = ("aca_plan_array", "aca_network_array", "ptg_plan_array", "ptg_source_array", "group_plan_array")
_UNRESOLVED_TERMS = (
    "raw_heap_toast_and_defaults",
    "raw_enrichment_tuple_versions",
    "evidence_groups_and_payloads",
    "support_rows_and_candidate_fanout",
    "gin_gist_and_other_index_pages",
    "simultaneous_relation_live_set",
    "logging_rewrites_and_wal",
    "query_sort_hash_and_materialization_temp",
)


def _native():
    return importlib.import_module("process.entity_address_unified")


def normalized_raw_select_sql(source_select: str) -> str:
    """Reuse the exact INSERT input expressions without creating or modifying a stage."""
    native = _native()
    return (
        "".join(
            (
                native._insert_raw_base_rows_cte_sql(source_select),
                native._insert_raw_sanitized_cte_sql(),
                native._insert_raw_normalized_cte_sql(),
                native._insert_raw_select_sql(),
            )
        )
        .strip()
        .removesuffix(";")
    )


def _scalar_statistics_sql(expression: str) -> str:
    """Keep empty-input totals zero and PostgreSQL's arbitrary-precision SUM result."""
    return (
        "jsonb_build_object('total', COALESCE(SUM(" + expression + "), 0), "
        "'maximum', COALESCE(MAX(" + expression + "), 0))"
    )


def _array_statistics_sql(name: str) -> str:
    """Count dimensions, null slots and actual element bytes, excluding storage overhead."""
    payload = (
        f"(SELECT COALESCE(SUM(octet_length(element)), 0) FROM unnest({name}) element)"
        if name in _TEXT_ARRAYS
        else f"(SELECT count(element) * 4 FROM unnest({name}) element)"
    )
    return (
        "jsonb_build_object('elements', "
        + _scalar_statistics_sql(f"cardinality({name})")
        + ", 'dimensions', "
        + _scalar_statistics_sql(f"COALESCE(array_ndims({name}), 0)")
        + ", 'element_octets', "
        + _scalar_statistics_sql(payload)
        + ")"
    )


def raw_observation_sql(raw_select: str) -> str:
    """Return one aggregate row without loading source records or arrays into Python."""
    text_fields = ", ".join(f"'{name}', " + _scalar_statistics_sql(f"octet_length({name})") for name in _TEXT_FIELDS)
    array_fields = ", ".join(f"'{name}', " + _array_statistics_sql(name) for name in (*_INTEGER_ARRAYS, *_TEXT_ARRAYS))
    return (
        "SELECT count(*) AS row_count, jsonb_build_object(" + text_fields + ") AS text_octets, "
        "jsonb_build_object(" + array_fields + ") AS arrays FROM (" + raw_select + ") raw_rows"
    )


def _require_overlay(address, fence, overlay):
    """Accept only the internal engine projection bound to this exact desired build."""
    try:
        from process.provider_directory_cms_overlay_projection import DesiredOverlayProjection
    except ImportError as error:
        raise RuntimeError("cms_native_projection_desired_overlay_unavailable") from error
    if (
        not isinstance(overlay, DesiredOverlayProjection)
        or overlay.native_address_input_hash != address.input_hash
        or overlay.desired_fence_hash != desired_fence_hash(fence)
        or not isinstance(overlay.statement_sql, str)
        or not overlay.statement_sql.strip()
    ):
        raise RuntimeError("cms_native_projection_desired_overlay_changed")


async def _availability(session, schema, expected, address):
    """Derive optional source branches and columns from the captured external relation set."""
    physical = expected["native_input_fence"]["relations"]
    available_by_name = {
        name: physical[f'"{namespace}"."{name}"']["relation_oid"] is not None
        for namespace, name in native_inputs._relations(schema)
        if namespace == schema
    }
    for name, column in (
        ("npi_address", "address_key"),
        ("doctor_clinician_address", "address_key"),
        ("provider_enrollment_ffs_address", "address_key"),
        ("facility_anchor", "address_key"),
        ("mrf_address", "address_key"),
        ("facility_anchor", "medicare_ccn"),
    ):
        relation = address.source_relation_overrides.get(name, name)
        available_by_name[f"{name}.{column}"] = bool(
            await session.scalar(
                text(
                    "SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(:relation) "
                    "AND attname=:column AND attnum>0 AND NOT attisdropped)"
                ),
                {"relation": f'"{schema}"."{relation}"', "column": column},
            )
        )
    available_by_name.update({name: True for name in _native().PROVIDER_DIRECTORY_DATASET_FENCE_TABLES})
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    available_by_name.update(
        {
            name: expected["native_input_fence"]["cms_office_read_relations"][name]["relation_oid"] is not None
            for name in CMS_OFFICE_READ_TABLES
        }
    )
    return available_by_name


def _source_queries(schema, available, address, fence, overlay, *, has_address_canon):
    """Use the same full-builder source routing, with mandatory virtual desired overlay."""
    from process.provider_directory_cms_address import _dataset_pins

    inputs = candidate.ProviderDirectoryAddressSourceQueryInput(
        _dataset_pins(fence),
        tuple(
            sorted(
                (name, stage)
                for name, stage in address.source_relation_overrides.items()
                if name in candidate._RELATION_OVERRIDES
            )
        ),
        json.loads(address.input_json)["profile_as_of"],
        overlay,
    )
    with candidate.source_query_scope(inputs):
        native = _native()
        base = native._source_selects(schema, available, is_address_canon_available=has_address_canon)
        return native._current_provider_directory_source_selects(schema, available, base, has_compatibility_data=True)


async def _assert_snapshot(address, fence, expected):
    """Check physical/native authority, desired dataset proof and independently sealed Doctors."""
    from process import entity_address_prepared_doctors as doctors
    from process.provider_directory_cms_overlay_projection import assert_desired_overlay_inputs

    schema = address.fhir._schema()
    await assert_desired_overlay_inputs(address.fhir, address.execution, fence, address)
    await doctors.capture_dependencies(
        address.fhir.db,
        schema,
        tuple(address.source_relation_overrides.items()),
        doctors=address.doctors,
        dependency_bindings=expected["desired_geo_bindings"],
    )


def _sql_settings(expected: dict) -> list[tuple[str, str]]:
    """Use the factory's bound limits even before a preparation lease exists."""
    temp_limit = expected.get("temp_file_limit_bytes_per_backend")
    if (
        type(temp_limit) is not int
        or temp_limit <= 0
        or temp_limit % 1024
        or any(
            type(expected.get(name)) is not int or expected[name] != 0
            for name in ("max_parallel_workers_per_gather", "max_parallel_maintenance_workers")
        )
    ):
        raise RuntimeError("cms_native_projection_execution_bounds_invalid")
    settings_by_name = dict(_native()._entity_address_sql_settings())
    settings_by_name.update(
        temp_file_limit=f"{temp_limit // 1024}kB",
        max_parallel_workers_per_gather="0",
        max_parallel_maintenance_workers="0",
    )
    return list(settings_by_name.items())


@asynccontextmanager
async def _observation_snapshot(database, expected):
    """Bound and verify the actual read-only backend before any input scans."""
    native, settings = _native(), _sql_settings(expected)
    async with database.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        async with native.entity_address_tuned_transaction(database, settings, native._sql_literal, native.logger):
            bounded = await session.scalar(
                text(
                    "SELECT pg_size_bytes(current_setting('temp_file_limit')) = :temp_bytes "
                    "AND current_setting('max_parallel_workers_per_gather')::int = 0 "
                    "AND current_setting('max_parallel_maintenance_workers')::int = 0"
                ),
                {"temp_bytes": expected["temp_file_limit_bytes_per_backend"]},
            )
            if bounded is not True:
                raise RuntimeError("cms_native_projection_executing_session_settings_changed")
            yield session


async def observe_native_raw_projection(address, fence, overlay) -> dict:
    """Read exact desired transformed inputs before scratch; never authorize a reservation."""
    _require_overlay(address, fence, overlay)
    database = address.fhir.db
    if database is not _native().db or database._transaction_binding() is not None:
        raise RuntimeError("cms_native_projection_requires_unbound_shared_database")
    expected = json.loads(address.input_json)
    async with _observation_snapshot(database, expected) as session:
        await _assert_snapshot(address, fence, expected)
        available = await _availability(session, address.fhir._schema(), expected, address)
        selects = _source_queries(
            address.fhir._schema(),
            available,
            address,
            fence,
            overlay,
            has_address_canon=await _native()._is_address_canon_available(address.fhir._schema()),
        )
        observations = await _observe_sources(session, selects)
    async with _observation_snapshot(database, expected):
        await _assert_snapshot(address, fence, expected)
    return _result(address, overlay, observations)


async def _observe_sources(session, selects: list[str]) -> list[dict]:
    """Execute sequentially in the caller's read-only snapshot; cancellation propagates."""
    observations = []
    for source_select in selects:
        raw_select = normalized_raw_select_sql(source_select)
        row = (await session.execute(text(raw_observation_sql(raw_select)))).mappings().one()
        observations.append({"query_sha256": hashlib.sha256(raw_select.encode()).hexdigest(), **dict(row)})
    return observations


def _result(address, overlay, observations: list[dict]) -> dict:
    """Expose measurements and missing terms without presenting bytes as an admission bound."""
    result_by_field = {
        "contract_id": "provider-directory-cms-native-raw-observation.v1",
        "native_address_input_hash": address.input_hash,
        "desired_fence_hash": overlay.desired_fence_hash,
        "row_count": sum(row["row_count"] for row in observations),
        "sources": observations,
        "capacity_complete": False,
        "unresolved_terms": list(_UNRESOLVED_TERMS),
    }
    result_by_field["observation_sha256"] = hashlib.sha256(
        json.dumps(result_by_field, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return result_by_field
