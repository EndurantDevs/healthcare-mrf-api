# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared model index completion and indexed, set-based relationship checks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import ARRAY, Column, MetaData, Table, text
from sqlalchemy.dialects import postgresql

if TYPE_CHECKING:
    from process.reference_family_archive import ReferenceFamilySpec


def _model_additional_indexes(spec, model_type):
    """Select the producer's existing initial and additional index declarations."""
    indexes = tuple(getattr(model_type, "__my_initial_indexes__", ()) or ()) + tuple(
        getattr(model_type, "__my_additional_indexes__", ()) or ()
    )
    if spec.importer_id in {"mrf", "mrf-address"}:
        # Ordinary publication retains initial and additional indexes only for these stages.
        indexes = (
            indexes
            if model_type.__tablename__
            in {"plan_benefits_marketplace", "mrf_address", "mrf_address_evidence", "plan_search_summary"}
            else ()
        )
    return indexes


async def _create_model_indexes(
    session: Any, spec: ReferenceFamilySpec, schema_name: str, *, create_constraints: bool = False, physical_names=None
) -> None:
    """Complete model keys and indexes before checking the closed relationship sets."""
    from process import reference_family_archive as native

    metadata = MetaData(schema=schema_name)
    if create_constraints and spec.importer_id == "mrf" and native.STAGE_TABLE not in spec.table_names:
        await native._create_table_constraints(
            session, Table(native.STAGE_TABLE, metadata, Column("address_key", postgresql.UUID, primary_key=True))
        )
    for model_type in spec.model_types:
        native._clone_model_table(model_type.__table__, metadata, schema=schema_name)
    for model_type in spec.model_types:
        table = metadata.tables[f"{schema_name}.{model_type.__tablename__}"]
        if physical_names is not None:
            table.name = physical_names[model_type.__tablename__]
        if create_constraints:
            if spec.importer_id == "provider-quality" and table.primary_key.columns:
                table.primary_key.name = native._index_name_for_table(table.name, f"{schema_name}_{table.name}_pkey")
            await native._create_table_constraints(session, table)
        for statement in native._declared_index_statements(model_type, table, schema_name, ()):
            await native._execute_model_statement(session, statement)
        if model_type.__tablename__ == native.STAGE_TABLE:
            await native.canonical_spatial_index(
                session,
                schema_name,
                native.STAGE_TABLE,
                lambda schema, name: f"{native._quoted(schema)}.{native._quoted(name)}",
            )
        if spec.importer_id == "provider-quality":
            primary_elements = tuple(getattr(model_type, "__my_index_elements__", ()) or ())
            if primary_elements:
                primary_name = native._index_name_for_table(
                    model_type.__tablename__,
                    f"{schema_name}_{model_type.__tablename__}_idx_primary",
                )
                await session.execute(
                    text(
                        f"CREATE UNIQUE INDEX {native._quoted(primary_name)} ON {native._quoted(schema_name)}."
                        f"{native._quoted(model_type.__tablename__)} ({', '.join(primary_elements)})"
                    )
                )
        indexes = _model_additional_indexes(spec, model_type)
        for statement in native._declared_index_statements(
            model_type,
            table,
            schema_name,
            indexes,
            native_indexes=False,
            index_schema_name=table.name if physical_names is not None else None,
        ):
            await native._execute_model_statement(session, statement)
    if create_constraints:
        for table in metadata.tables.values():
            await native._create_table_constraints(session, table, backing_indexes=False)
        await native._validate_model_foreign_keys(session, metadata)
        await native._validate_declared_model_relationships(session, spec, schema_name, physical_names=physical_names)


async def _validate_declared_model_relationships(session, spec, schema_name, *, physical_names=None):
    """Check closed typed scalar/array edges after every family index is complete."""
    from process import reference_family_archive as native

    for child_model, child_name, parent_model, parent_name, is_array, allows_null in spec.relationships:
        if (
            child_model not in spec.model_types
            or parent_model not in spec.model_types
            or child_name not in child_model.__table__.columns
            or parent_name not in parent_model.__table__.columns
            or type(is_array) is not bool
            or type(allows_null) is not bool
        ):
            raise native.ReferenceFamilyArchiveError("model relationship declaration differs")
        child = child_model.__table__.columns[child_name]
        parent = parent_model.__table__.columns[parent_name]
        if (
            isinstance(child.type, ARRAY) != is_array
            or (allows_null and not child.nullable)
            or not (
                (parent.primary_key and len(parent_model.__table__.primary_key.columns) == 1)
                or (
                    physical_names is not None
                    and any(
                        index.get("index_elements") == [parent_name]
                        and index.get("using", "btree") in {"btree", "gin"}
                        and not index.get("where")
                        for index in getattr(parent_model, "__my_additional_indexes__", ())
                    )
                )
            )
        ):
            raise native.ReferenceFamilyArchiveError("model relationship column shape differs")
        names = physical_names or {model.__tablename__: model.__tablename__ for model in spec.model_types}
        child_table = f"{native._quoted(schema_name)}.{native._quoted(names[child_model.__tablename__])}"
        parent_table = f"{native._quoted(schema_name)}.{native._quoted(names[parent_model.__tablename__])}"
        child_column = f"c.{native._quoted(child_name)}"
        if is_array:
            missing = (
                f"EXISTS(SELECT 1 FROM unnest({child_column}) AS member(value) "
                f"WHERE NOT EXISTS(SELECT 1 FROM {parent_table} p WHERE p.{native._quoted(parent_name)}=member.value))"
            )
        else:
            missing = f"NOT EXISTS(SELECT 1 FROM {parent_table} p WHERE p.{native._quoted(parent_name)}={child_column})"
        invalid = (
            f"{child_column} IS NOT NULL AND ({missing})" if allows_null else f"{child_column} IS NULL OR ({missing})"
        )
        statement = text(f"SELECT EXISTS(SELECT 1 FROM {child_table} c WHERE {invalid})")
        violates = (
            await session.fetchval(str(statement))
            if callable(getattr(session, "is_in_transaction", None))
            else await session.scalar(statement)
        )
        if violates is not False:
            raise native.ReferenceFamilyArchiveError(
                "staged declared model relationship differs: "
                f"{child_model.__tablename__}.{child_name} -> {parent_model.__tablename__}.{parent_name}"
            )
