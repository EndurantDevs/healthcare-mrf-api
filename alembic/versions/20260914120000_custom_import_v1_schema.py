# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Create migration-owned generic custom-import v1 storage.

Revision ID: 20260914120000_custom_import_v1_schema
Revises: 20260907220000_hospital_price_missing_plan

The revision is strictly schema-only.  It creates no dataset, definition,
capture, execution, generation, or pointer rows.
"""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from alembic import op
from db.models import custom_import as models


revision = "20260914120000_custom_import_v1_schema"
down_revision = "20260907220000_hospital_price_missing_plan"
branch_labels = None
depends_on = None


_TABLE_MODELS = (
    models.CustomImportDataset,
    models.CustomImportSchemaRevision,
    models.CustomImportFieldSlot,
    models.CustomImportChildCollection,
    models.CustomImportField,
    models.CustomImportDefinitionRevision,
    models.CustomImportSourceStream,
    models.CustomImportFieldAlias,
    models.CustomImportSelectionProfile,
    models.CustomImportExecution,
    models.CustomImportLease,
    models.CustomImportCaptureBundle,
    models.CustomImportCapture,
    models.CustomImportPack,
    models.CustomImportRejection,
    models.CustomImportRootRecord,
    models.CustomImportRootRevision,
    models.CustomImportChildRevision,
    models.CustomImportFamilyRevision,
    models.CustomImportFamilyChild,
    models.CustomImportGeneration,
    models.CustomImportGenerationFamily,
    models.CustomImportRootScalar,
    models.CustomImportChildScalar,
    models.CustomImportEntityBinding,
    models.CustomImportWinner,
    models.CustomImportCurrentGeneration,
    models.CustomImportPublicationEvent,
)
_TABLE_NAMES = tuple(model.__tablename__ for model in _TABLE_MODELS)
_IMMUTABLE_TABLES = tuple(
    table_name
    for table_name in _TABLE_NAMES
    if table_name
    not in {
        "custom_import_execution",
        "custom_import_lease",
        "custom_import_current_generation",
    }
)
_INDEX_SPECS = (
    ("custom_import_capture", "custom_import_capture_content_idx", ("content_sha256",), None),
    ("custom_import_pack", "custom_import_pack_execution_stream_idx", ("execution_id", "stream_slot", "pack_ordinal"), None),
    ("custom_import_rejection", "custom_import_rejection_execution_idx", ("execution_id", "code"), None),
    ("custom_import_generation_family", "custom_import_generation_family_lookup_idx", ("generation_id", "root_record_id"), None),
    ("custom_import_root_scalar", "custom_import_root_scalar_text_idx", ("schema_revision_id", "field_slot", "string_value"), "value_state = 'value'"),
    ("custom_import_root_scalar", "custom_import_root_scalar_int_idx", ("schema_revision_id", "field_slot", "integer_value"), "value_state = 'value'"),
    ("custom_import_root_scalar", "custom_import_root_scalar_number_idx", ("schema_revision_id", "field_slot", "decimal_value"), "value_state = 'value'"),
    ("custom_import_root_scalar", "custom_import_root_scalar_date_idx", ("schema_revision_id", "field_slot", "date_value"), "value_state = 'value'"),
    ("custom_import_root_scalar", "custom_import_root_scalar_time_idx", ("schema_revision_id", "field_slot", "timestamp_value"), "value_state = 'value'"),
    ("custom_import_child_scalar", "custom_import_child_scalar_text_idx", ("schema_revision_id", "collection_slot", "field_slot", "string_value"), "value_state = 'value'"),
    ("custom_import_child_scalar", "custom_import_child_scalar_int_idx", ("schema_revision_id", "collection_slot", "field_slot", "integer_value"), "value_state = 'value'"),
    ("custom_import_child_scalar", "custom_import_child_scalar_number_idx", ("schema_revision_id", "collection_slot", "field_slot", "decimal_value"), "value_state = 'value'"),
    ("custom_import_child_scalar", "custom_import_child_scalar_date_idx", ("schema_revision_id", "collection_slot", "field_slot", "date_value"), "value_state = 'value'"),
    ("custom_import_child_scalar", "custom_import_child_scalar_time_idx", ("schema_revision_id", "collection_slot", "field_slot", "timestamp_value"), "value_state = 'value'"),
    ("custom_import_entity_binding", "custom_import_entity_binding_lookup_idx", ("dataset_id", "adapter_id", "canonical_value"), None),
    ("custom_import_winner", "custom_import_winner_lookup_idx", ("generation_id", "profile_slot", "entity_binding_id"), None),
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _copy_tables(schema: str) -> sa.MetaData:
    metadata = sa.MetaData()
    for model in _TABLE_MODELS:
        model.__table__.to_metadata(metadata, schema=schema)
    return metadata


def _immutable_function_sql(schema: str) -> str:
    qualified = f"{_quote(schema)}.guard_custom_import_immutable_row"
    return f"""
    CREATE FUNCTION {qualified}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        RAISE EXCEPTION 'custom_import_immutable_row'
            USING ERRCODE = 'P0001';
    END;
    $function$;
    """


def _revoke_immutable_function_sql(schema: str) -> str:
    """Return the function permission statement as a separate operation."""

    qualified = f"{_quote(schema)}.guard_custom_import_immutable_row"
    return f"REVOKE ALL ON FUNCTION {qualified}() FROM PUBLIC"


def _immutable_trigger_sql(schema: str, table_name: str) -> str:
    qualified_table = f"{_quote(schema)}.{_quote(table_name)}"
    qualified_function = f"{_quote(schema)}.guard_custom_import_immutable_row()"
    trigger = _quote(f"{table_name}_immutable_row_guard")
    return f"""
    CREATE TRIGGER {trigger}
    BEFORE UPDATE OR DELETE ON {qualified_table}
    FOR EACH ROW EXECUTE FUNCTION {qualified_function};
    """


def upgrade() -> None:
    """Install the v1 schema, indexes, and immutable-content guards."""

    schema = _schema()
    metadata = _copy_tables(schema)
    for table in metadata.sorted_tables:
        op.execute(str(CreateTable(table).compile(dialect=postgresql.dialect())))
    for table_name, index_name, columns, predicate in _INDEX_SPECS:
        table = metadata.tables[f"{schema}.{table_name}"]
        index = sa.Index(
            index_name,
            *(table.c[column] for column in columns),
            postgresql_where=sa.text(predicate) if predicate else None,
        )
        op.execute(str(CreateIndex(index).compile(dialect=postgresql.dialect())))
    # asyncpg prepares each Alembic operation independently and rejects a
    # string containing more than one command.  Keep creation and revocation
    # as distinct operations so the migration works with the runtime driver.
    op.execute(_immutable_function_sql(schema))
    op.execute(_revoke_immutable_function_sql(schema))
    for table_name in _IMMUTABLE_TABLES:
        op.execute(_immutable_trigger_sql(schema, table_name))


def downgrade() -> None:
    """Do not remove retained immutable import evidence during a downgrade."""

    return None
