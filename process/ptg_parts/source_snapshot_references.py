"""Reference-group loading and shaping for exact PTG snapshot controls."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from db.plan_registry_schema import plan_registry_schema
from process.ptg_parts.db_tables import _quote_ident

ReferenceRowLoader = Callable[..., Awaitable[list[Any]]]


async def source_binding_schema(executor: Any, projection_schema: str) -> str | None:
    """Omit absent Source storage only when neither serving header claims it."""
    control_schema = plan_registry_schema()
    header = f"{_quote_ident(projection_schema)}.plan_release_serving_revision"
    rows = await executor.all(
        "SELECT to_regclass(:source_header) IS NOT NULL AS source_header_exists, "
        "to_regclass(:bindings) IS NOT NULL AS bindings_exist, "
        "to_regclass(:header) IS NOT NULL AS header_exists",
        source_header=f"{_quote_ident(control_schema)}.hp_plan_serving_revision"
        if control_schema is not None
        else None,
        bindings=f"{_quote_ident(control_schema)}.hp_plan_release_binding" if control_schema is not None else None,
        header=header,
    )
    catalog_by_field = dict(rows[0] if isinstance(rows[0], dict) else rows[0]._mapping)
    if catalog_by_field["bindings_exist"]:
        return control_schema
    if catalog_by_field["source_header_exists"]:
        raise RuntimeError("Source release binding storage is missing")
    if catalog_by_field["header_exists"]:
        claims = await executor.all(
            f"SELECT EXISTS(SELECT FROM {header} WHERE CAST(source_manifest AS jsonb) ? 'binding_storage') AS claimed"
        )
        claim_by_field = dict(claims[0] if isinstance(claims[0], dict) else claims[0]._mapping)
        if claim_by_field["claimed"]:
            raise RuntimeError("Claimed Source release binding storage is missing")
    return None


async def load_snapshot_reference_rows(
    schema: str,
    snapshot_id: str,
    loader: ReferenceRowLoader,
    executor: Any,
) -> dict[str, list[Any]]:
    """Load every pointer and release-binding reference group."""

    reference_specs = (
        ("global", "ptg2_current_snapshot", "slot", "snapshot_id"),
        ("source", "ptg2_current_source_snapshot", "source_key", "snapshot_id"),
        ("plan", "ptg2_current_plan_source", "plan_source_key", "snapshot_id"),
        ("previous_global", "ptg2_current_snapshot", "slot", "previous_snapshot_id"),
        ("previous_source", "ptg2_current_source_snapshot", "source_key", "previous_snapshot_id"),
        ("previous_plan", "ptg2_current_plan_source", "plan_source_key", "previous_snapshot_id"),
        ("pin", "ptg2_snapshot_pin", "owner_type, owner_id", "snapshot_id"),
        (
            "release_binding",
            "plan_release_snapshot_binding",
            "serving_revision_id, role, binding_ordinal",
            "snapshot_id",
        ),
    )
    reference_rows_by_kind: dict[str, list[Any]] = {}
    for kind, table, selected_fields, reference_field in reference_specs:
        reference_rows_by_kind[kind] = await loader(
            schema,
            snapshot_id,
            table=table,
            selected_fields=selected_fields,
            reference_field=reference_field,
        )
    reference_rows_by_kind["pin"].extend(
        await loader(
            schema,
            snapshot_id,
            table="plan_release_snapshot_pin",
            selected_fields="'plan_release_serving_revision'::text AS owner_type, serving_revision_id AS owner_id",
            order_fields="serving_revision_id",
        )
    )
    control_schema = await source_binding_schema(executor, schema)
    if control_schema is not None:
        reference_rows_by_kind["release_binding"].extend(
            await loader(
                control_schema,
                snapshot_id,
                table="hp_plan_release_binding",
                selected_fields="serving_revision_id, role, ordinal AS binding_ordinal",
                order_fields="serving_revision_id, role, ordinal",
            )
        )
    return reference_rows_by_kind


def reference_string_values(
    reference_rows: list[Any],
    field: str,
    row_mapping: Callable[[Any], dict[str, Any]],
) -> list[str]:
    """Return one stable string value from every loaded reference row."""

    return [str(row_mapping(reference_row).get(field)) for reference_row in reference_rows]
