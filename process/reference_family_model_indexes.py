# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared model index completion and indexed, set-based relationship checks."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    ARRAY,
    Column,
    ForeignKeyConstraint,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import AddConstraint, CreateIndex, CreateTable

from process.mrf_address_publication import canonical_spatial_index

if TYPE_CHECKING:
    from process.reference_family_archive import ReferenceFamilySpec


def _additional_index_sql(
    schema_name: str,
    model_type: type,
    index_spec: Mapping[str, Any],
    *,
    table_name=None,
    index_schema_name=None,
    resolved_opclasses=None,
) -> str:
    """Compile reviewed local index options with unchanged native naming and SQL."""
    from process import reference_family_archive as native

    allowed_keys = {"index_elements", "name", "using", "unique", "include", "where", "staging_name"}
    if set(index_spec) - allowed_keys:
        raise native.ReferenceFamilyArchiveError("reference family model index is unsupported")
    staging_name = index_spec.get("staging_name")
    if staging_name is not None and (
        not isinstance(staging_name, str) or native._IDENTIFIER.fullmatch(staging_name) is None
    ):
        raise native.ReferenceFamilyArchiveError("reference family model staging index name is invalid")
    elements = index_spec.get("index_elements")
    if (
        not isinstance(elements, (tuple, list))
        or not elements
        or not all(native._is_reviewed_index_element(index_element) for index_element in elements)
    ):
        raise native.ReferenceFamilyArchiveError("reference family model index is invalid")
    suffix = index_spec.get("name", "_".join(elements))
    if not isinstance(suffix, str) or native._IDENTIFIER.fullmatch(suffix) is None:
        raise native.ReferenceFamilyArchiveError("reference family model index is invalid")
    index_name = native._index_name_for_table(
        model_type.__tablename__,
        f"{index_schema_name or schema_name}_{model_type.__tablename__}_idx_{suffix}",
    )
    method = index_spec.get("using")
    if method is not None and method not in {"btree", "gin", "gist", "hash", "brin", "spgist"}:
        raise native.ReferenceFamilyArchiveError("reference family model index method is invalid")
    is_unique = index_spec.get("unique", False)
    if type(is_unique) is not bool:
        raise native.ReferenceFamilyArchiveError("reference family model index uniqueness is invalid")
    included_columns = index_spec.get("include", ())
    if not isinstance(included_columns, (tuple, list)) or not all(
        isinstance(column_name, str) and native._IDENTIFIER.fullmatch(column_name) is not None
        for column_name in included_columns
    ):
        raise native.ReferenceFamilyArchiveError("reference family model index include is invalid")
    using = f" USING {method}" if method else ""
    unique = "UNIQUE " if is_unique else ""
    include = f" INCLUDE ({', '.join(included_columns)})" if included_columns else ""
    where = native._reviewed_index_where(index_spec.get("where"))
    compiled_elements = [
        native._witness_index_element(model_type, method or "btree", element, resolved_opclasses)
        for element in elements
    ]
    return (
        f"CREATE {unique}INDEX {native._quoted(index_name)} ON {native._quoted(schema_name)}."
        f"{native._quoted(table_name or model_type.__tablename__)}{using} ({', '.join(compiled_elements)}){include}{where}"
    )


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
            await canonical_spatial_index(
                session,
                schema_name,
                native.STAGE_TABLE,
                lambda schema, name: f"{native._quoted(schema)}.{native._quoted(name)}",
            )
        if spec.importer_id in {"provider-quality", "plan-attributes"}:
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


def _reviewed_index_where(where_clause):
    """Keep the existing predicate boundary and exact suffix for trusted local indexes."""
    from process import reference_family_archive as native

    if where_clause not in {
        None,
        "estimated_gross_margin IS NOT NULL",
        "rx_code_system = 'HP_RX_CODE'",
        "geography_scope = 'zip5'",
        "type='practice'",
        "type='practice' AND phone_number IS NOT NULL AND phone_number <> ''",
    }:
        raise native.ReferenceFamilyArchiveError("reference family model index predicate is unsupported")
    return "" if where_clause is None else f" WHERE {where_clause}"


def _declared_index_statements(
    model, table, schema_name, indexes, *, index_schema_name=None, native_indexes=True, resolved_opclasses=None
):
    """Compile the same declarations for an isolated heap or its native catalog witness."""
    from process import reference_family_archive as archive

    native = (
        tuple(CreateIndex(index) for index in sorted(table.indexes, key=lambda item: item.name))
        if native_indexes
        else ()
    )
    return native + tuple(
        text(
            archive._additional_index_sql(
                schema_name,
                model,
                index,
                table_name=table.name,
                index_schema_name=index_schema_name,
                resolved_opclasses=resolved_opclasses,
            )
        )
        for index in indexes
    )


def _model_index_catalog_plan(model, schema_name, *, resolved_opclasses=None):
    """Compile only trusted local indexes on an empty transaction-local native witness."""
    from process import reference_family_archive as native
    from process.entity_address_native_publication import _model_catalog_table

    table = _model_catalog_table(model, allow_model_relationships=True)
    table.schema = "pg_temp"
    for constraint in tuple(table.constraints):
        if not isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)):
            table.constraints.remove(constraint)
    for column in table.columns:
        column.identity = None
        column.server_default = None
        column.default = None
    if any(not all(isinstance(column, Column) for column in index.expressions) for index in table.indexes):
        raise native.ReferenceFamilyArchiveError("model witness index expression is unsupported")
    if resolved_opclasses is not None:
        for index in table.indexes:
            options = index.dialect_options["postgresql"]
            method = options.get("using") or "btree"
            options["ops"] = {
                column.name: ".".join(
                    native._quoted(part)
                    for part in resolved_opclasses[(method, (options.get("ops") or {}).get(column.name), column.name)]
                )
                for column in index.expressions
            }
    indexes = tuple(getattr(model, "__my_initial_indexes__", ()) or ()) + tuple(
        getattr(model, "__my_additional_indexes__", ()) or ()
    )
    statements = native._declared_index_statements(
        model, table, "pg_temp", indexes, index_schema_name=schema_name, resolved_opclasses=resolved_opclasses
    )
    return table, (CreateTable(table), *statements)


def _model_index_input(model, method, element):
    """Read one already reviewed declaration's native input, not a SQL statement."""
    parts = element.split()
    opclass = parts[-1] if parts[-1].endswith("_ops") else None
    column = parts[0] if parts[0] in model.__table__.columns else None
    return method, opclass, column


def _witness_index_element(model, method, element, resolved_opclasses):
    """Bind only a source-verified native class while keeping function lookup in pg_catalog."""
    from process import reference_family_archive as native

    if resolved_opclasses is None:
        return element
    key = native._model_index_input(model, method, element)
    expression = element.rsplit(" ", 1)[0] if key[1] is not None else element
    ordering = ""
    if expression.endswith((" DESC", " ASC")):
        expression, ordering = expression.rsplit(" ", 1)
        ordering = " " + ordering
    opclass = ".".join(native._quoted(part) for part in resolved_opclasses[key])
    return f"{expression} {opclass}{ordering}"


def _model_index_catalog_inputs(model):
    """Select compiler-owned operator-class inputs, never portable index metadata."""
    from process import reference_family_archive as native

    inputs = []
    for constraint in model.__table__.constraints:
        if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)):
            inputs.extend(("btree", None, column.name) for column in constraint.columns)
    for index in model.__table__.indexes:
        options = index.dialect_options["postgresql"]
        for column in index.expressions:
            if not isinstance(column, Column):
                raise native.ReferenceFamilyArchiveError("model witness index expression is unsupported")
            inputs.append((options.get("using") or "btree", (options.get("ops") or {}).get(column.name), column.name))
    indexes = tuple(getattr(model, "__my_initial_indexes__", ()) or ()) + tuple(
        getattr(model, "__my_additional_indexes__", ()) or ()
    )
    for index in indexes:
        native._additional_index_sql("pg_temp", model, index)
        for element in index["index_elements"]:
            inputs.append(native._model_index_input(model, index.get("using") or "btree", element))
    return inputs


def _is_reviewed_index_element(index_element: object) -> bool:
    from process import reference_family_archive as native

    if not isinstance(index_element, str):
        return False

    tokens = index_element.split()
    if 0 < len(tokens) <= 2 and all(native._IDENTIFIER.fullmatch(token) is not None for token in tokens):
        return True
    return index_element in {
        "lower(synonym)",
        "lower(provider_type)",
        "lower(provider_name)",
        "lower(service_description)",
        "lower(reported_code)",
        "lower(display_name)",
        "lower(rx_name)",
        "lower(generic_name)",
        "lower(brand_name)",
        "lower(COALESCE(rx_name, '')) gin_trgm_ops",
        "lower(COALESCE(generic_name, '')) gin_trgm_ops",
        "lower(COALESCE(brand_name, '')) gin_trgm_ops",
        "lower(COALESCE(rx_code, '')) gin_trgm_ops",
        "lower(short_description)",
        "upper(from_system)",
        "upper(from_code)",
        "upper(to_system)",
        "upper(to_code)",
        "LEFT(postal_code, 5)",
        "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
    }


async def _create_table_constraints(session: Any, table: Table, *, backing_indexes: bool = True) -> None:
    """Finish model keys or checks; relationships use indexed set checks instead."""
    from process import reference_family_archive as native

    if not backing_indexes:
        for column in table.columns:
            for constraint in tuple(column.constraints):
                column.constraints.remove(constraint)
                table.append_constraint(constraint)
    constraints = (
        constraint
        for constraint in table.constraints
        if not isinstance(constraint, ForeignKeyConstraint)
        and isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)) == backing_indexes
        and (not isinstance(constraint, PrimaryKeyConstraint) or constraint.columns)
    )
    for constraint in sorted(
        constraints, key=lambda item: (type(item).__name__, str(item.name or ""), tuple(item.columns.keys()))
    ):
        await native._execute_model_statement(session, AddConstraint(constraint))
