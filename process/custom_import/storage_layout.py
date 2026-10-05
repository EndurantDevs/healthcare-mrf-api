# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed physical snapshot layout; execution authority belongs to the registry.

This module only prepares model-defined tables and DDL. It never executes DDL,
accepts a caller's relation names, or changes an incumbent relation. A protected
registry must create the complete family atomically and retain its relation OIDs
before writers or reads can use it.
"""

from __future__ import annotations

from functools import lru_cache
from types import MappingProxyType

from sqlalchemy import ForeignKeyConstraint, Index, MetaData, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import aliased
from sqlalchemy.schema import AddConstraint, CreateIndex

from db.models import custom_import as models

SNAPSHOT_MODELS = (
    models.CustomImportRootRecord,
    models.CustomImportEntityBinding,
    models.CustomImportPack,
    models.CustomImportRejection,
    models.CustomImportRootRevision,
    models.CustomImportChildRevision,
    models.CustomImportFamilyRevision,
    models.CustomImportFamilyChild,
    models.CustomImportGenerationFamily,
    models.CustomImportRootScalar,
    models.CustomImportChildScalar,
    models.CustomImportWinner,
    models.CustomImportBuildOccurrence,
    models.CustomImportBuildFamily,
    models.CustomImportBuildCandidateContext,
)
_DIALECT = postgresql.dialect()
_PHASE_INDEX_NAMES = {
    "admission": frozenset(
        {
            "custom_import_build_occurrence_page_idx",
            "custom_import_build_raw_parent_idx",
            "custom_import_build_source_child_idx",
            "custom_import_build_final_child_idx",
            "custom_import_build_typed_root_idx",
        }
    ),
    "graph": frozenset(
        {
            "custom_import_build_graph_child_idx",
            "custom_import_build_occurrence_pack_idx",
            "custom_import_build_family_hash_idx",
            "custom_import_build_family_pending_idx",
        }
    ),
    "output": frozenset({"custom_import_build_context_order_idx"}),
}
_SCALAR_INDEX_COLUMNS = (
    ("text", "string_value"),
    ("int", "integer_value"),
    ("number", "decimal_value"),
    ("date", "date_value"),
    ("time", "timestamp_value"),
)


def snapshot_schema(family_id: int) -> str:
    """Derive an internal namespace solely from a native registry identity."""

    if type(family_id) is not int or not 0 < family_id < 2**63:
        raise ValueError("snapshot family identity must be a positive bigint")
    return f"ci_snapshot_{family_id}"


@lru_cache(maxsize=32)
def snapshot_tables(family_id: int):
    """Clone the fixed row shapes without mutating canonical model metadata."""

    metadata = MetaData(schema=snapshot_schema(family_id))
    tables_by_name = {}
    for model in SNAPSHOT_MODELS:
        snapshot_table = model.__table__.to_metadata(metadata, schema=metadata.schema)
        for constraint in tuple(snapshot_table.constraints):
            if isinstance(constraint, ForeignKeyConstraint):
                snapshot_table.constraints.remove(constraint)
        for column in snapshot_table.c:
            column.foreign_keys.clear()
        snapshot_table.foreign_keys.clear()
        tables_by_name[model.__tablename__] = snapshot_table
    return MappingProxyType(tables_by_name)


def snapshot_load_statements(family_id: int) -> tuple[str, ...]:
    """Prepare one atomic family creation, with no foreign keys or row triggers.

    LIKE preserves actual native defaults, including canonical sequences. New
    snapshots therefore do not allocate a conflicting local revision-ID space.
    PK/UNIQUE and unique partial indexes are the only load-time indexes.
    Omit UNIQUE supersets of the PK: without FKs they add no invariant.
    """

    schema_name = snapshot_schema(family_id)
    statements = [f'CREATE SCHEMA "{schema_name}"']
    tables_by_name = snapshot_tables(family_id)
    for model in SNAPSHOT_MODELS:
        canonical_table = model.__table__
        snapshot_table = tables_by_name[model.__tablename__]
        destination = _DIALECT.identifier_preparer.format_table(snapshot_table)
        canonical_name = _DIALECT.identifier_preparer.format_table(canonical_table)
        statements.append(
            f"CREATE TABLE {destination} (LIKE {canonical_name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"
        )
        primary_key_columns = frozenset(snapshot_table.primary_key.columns.keys())
        constraints = sorted(
            (
                constraint
                for constraint in snapshot_table.constraints
                if isinstance(constraint, PrimaryKeyConstraint)
                or (
                    isinstance(constraint, UniqueConstraint)
                    and not primary_key_columns.issubset(constraint.columns.keys())
                )
            ),
            key=lambda constraint: (
                not isinstance(constraint, PrimaryKeyConstraint),
                constraint.name or "",
                tuple(constraint.columns.keys()),
            ),
        )
        statements.extend(str(AddConstraint(constraint).compile(dialect=_DIALECT)) for constraint in constraints)
        statements.extend(
            _index_sql(index) for index in sorted(snapshot_table.indexes, key=lambda index: index.name) if index.unique
        )
    return tuple(statements)


@lru_cache(maxsize=32)
def snapshot_models(family_id: int):
    """Compile fixed hot aliases after a caller verifies its registry binding.

    Only metadata is cached, never authority or current-generation resolution.
    Canonical control tables and global natural-key interners stay unchanged.
    """

    tables_by_name = snapshot_tables(family_id)
    return MappingProxyType(
        {model: aliased(model, tables_by_name[model.__tablename__], adapt_on_names=True) for model in SNAPSHOT_MODELS}
    )


def snapshot_phase_index_statements(family_id: int, phase: str) -> tuple[str, ...]:
    """Prepare only fixed indexes needed by the next bounded processing phase."""

    if phase not in _PHASE_INDEX_NAMES:
        raise ValueError("snapshot index phase is unsupported")
    indexes_by_name = {
        index.name: index for snapshot_table in snapshot_tables(family_id).values() for index in snapshot_table.indexes
    }
    return tuple(_index_sql(indexes_by_name[name]) for name in sorted(_PHASE_INDEX_NAMES[phase]))


def snapshot_serving_index_statements(family_id: int) -> tuple[str, ...]:
    """Prepare typed query indexes on the frozen replacement, never live heaps."""

    statements = []
    tables_by_name = snapshot_tables(family_id)
    for scope in ("root", "child"):
        scalar_table = tables_by_name[f"custom_import_{scope}_scalar"]
        key_names = ["schema_revision_id"] + (["collection_slot"] if scope == "child" else []) + ["field_slot"]
        for suffix, value_column in _SCALAR_INDEX_COLUMNS:
            index = Index(
                f"custom_import_{scope}_scalar_{suffix}_idx",
                *(scalar_table.c[name] for name in (*key_names, value_column)),
                postgresql_where=scalar_table.c.value_state == "value",
            )
            statements.append(_index_sql(index))
            scalar_table.indexes.remove(index)
    return tuple(statements)


def _index_sql(index: Index) -> str:
    """Compile native model index expressions, including their partial predicates."""

    return str(CreateIndex(index).compile(dialect=_DIALECT))
