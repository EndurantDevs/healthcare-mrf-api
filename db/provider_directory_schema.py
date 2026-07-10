# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Idempotent Provider Directory schema invariants for sync-managed databases."""

from __future__ import annotations

from typing import Any

from sqlalchemy import inspect, text


CHECKPOINT_TABLE = "provider_directory_pagination_checkpoint"
DATASET_TABLE = "provider_directory_endpoint_dataset"
CHECKPOINT_PRIMARY_KEY = "provider_directory_pagination_checkpoint_pkey"
LEGACY_PRIMARY_KEY_COLUMNS = (
    "canonical_api_base",
    "resource_type",
    "source_scope_hash",
)
ROOT_PRIMARY_KEY_COLUMNS = LEGACY_PRIMARY_KEY_COLUMNS + (
    "acquisition_root_run_id",
)
CHECKPOINT_REQUIRED_COLUMNS = set(ROOT_PRIMARY_KEY_COLUMNS) | {
    "dataset_id",
    "retry_of_run_id",
    "updated_at",
}
DATASET_REQUIRED_COLUMNS = {
    "dataset_id",
    "import_run_id",
    "acquisition_root_run_id",
}


def _qualified_table(sync_connection: Any, schema: str, table_name: str) -> str:
    preparer = sync_connection.dialect.identifier_preparer
    return f"{preparer.quote_schema(schema)}.{preparer.quote(table_name)}"


def _quoted_identifier(sync_connection: Any, identifier: str) -> str:
    return sync_connection.dialect.identifier_preparer.quote(identifier)


def _checkpoint_root_backfill_sql(checkpoint_ref: str, dataset_ref: str) -> tuple[str, ...]:
    return (
        f"""
        UPDATE {dataset_ref} AS dataset
           SET acquisition_root_run_id = checkpoint_root.acquisition_root_run_id
          FROM (
                SELECT dataset_id, min(acquisition_root_run_id) AS acquisition_root_run_id
                  FROM {checkpoint_ref}
                 WHERE acquisition_root_run_id IS NOT NULL
                 GROUP BY dataset_id
               ) AS checkpoint_root
         WHERE dataset.dataset_id = checkpoint_root.dataset_id
           AND dataset.acquisition_root_run_id IS NULL;
        """,
        f"""
        UPDATE {dataset_ref} AS dataset
           SET acquisition_root_run_id = dataset.import_run_id
         WHERE dataset.acquisition_root_run_id IS NULL
           AND dataset.import_run_id IS NOT NULL
           AND NOT EXISTS (
                SELECT 1
                  FROM {checkpoint_ref} AS checkpoint
                 WHERE checkpoint.dataset_id = dataset.dataset_id
                   AND checkpoint.acquisition_root_run_id IS NULL
                   AND checkpoint.retry_of_run_id IS NOT NULL
           );
        """,
        f"""
        UPDATE {checkpoint_ref} AS checkpoint
           SET acquisition_root_run_id = dataset.acquisition_root_run_id
          FROM {dataset_ref} AS dataset
         WHERE checkpoint.dataset_id = dataset.dataset_id
           AND checkpoint.acquisition_root_run_id IS NULL;
        """,
    )


def _checkpoint_root_mismatch_count(
    sync_connection: Any,
    checkpoint_ref: str,
    dataset_ref: str,
) -> int:
    mismatch_result = sync_connection.execute(
        text(
            f"""
            SELECT count(*)
              FROM {checkpoint_ref} AS checkpoint
              LEFT JOIN {dataset_ref} AS dataset
                ON dataset.dataset_id = checkpoint.dataset_id
             WHERE checkpoint.acquisition_root_run_id IS NULL
                OR checkpoint.acquisition_root_run_id IS DISTINCT FROM
                   dataset.acquisition_root_run_id;
            """
        )
    )
    return int(mismatch_result.scalar_one())


def _rekey_checkpoint_table(
    sync_connection: Any,
    checkpoint_ref: str,
    dataset_ref: str,
) -> None:
    for statement in _checkpoint_root_backfill_sql(checkpoint_ref, dataset_ref):
        sync_connection.execute(text(statement))
    if _checkpoint_root_mismatch_count(
        sync_connection,
        checkpoint_ref,
        dataset_ref,
    ):
        raise RuntimeError(
            "provider_directory_pagination_checkpoint_root_backfill_failed"
        )
    primary_key_name = _quoted_identifier(
        sync_connection,
        CHECKPOINT_PRIMARY_KEY,
    )
    primary_key_columns = ", ".join(
        _quoted_identifier(sync_connection, column_name)
        for column_name in ROOT_PRIMARY_KEY_COLUMNS
    )
    sync_connection.execute(
        text(
            f"""
            ALTER TABLE {checkpoint_ref}
                ALTER COLUMN acquisition_root_run_id SET NOT NULL,
                DROP CONSTRAINT {primary_key_name},
                ADD CONSTRAINT {primary_key_name}
                    PRIMARY KEY ({primary_key_columns});
            """
        )
    )


def _required_columns_missing(
    inspector: Any,
    table_name: str,
    schema: str,
    required_columns: set[str],
) -> set[str]:
    existing_columns = {
        column_record["name"]
        for column_record in inspector.get_columns(table_name, schema=schema)
    }
    return required_columns - existing_columns


def _validate_schema_objects(inspector: Any, schema: str) -> None:
    if not inspector.has_table(DATASET_TABLE, schema=schema):
        raise RuntimeError("provider_directory_endpoint_dataset_missing")
    missing_checkpoint_columns = _required_columns_missing(
        inspector,
        CHECKPOINT_TABLE,
        schema,
        CHECKPOINT_REQUIRED_COLUMNS,
    )
    missing_dataset_columns = _required_columns_missing(
        inspector,
        DATASET_TABLE,
        schema,
        DATASET_REQUIRED_COLUMNS,
    )
    if missing_checkpoint_columns or missing_dataset_columns:
        missing_columns = sorted(
            missing_checkpoint_columns | missing_dataset_columns
        )
        raise RuntimeError(
            "provider_directory_pagination_root_columns_missing:"
            + ",".join(missing_columns)
        )


def _lock_schema_objects(
    sync_connection: Any,
    checkpoint_ref: str,
    dataset_ref: str,
) -> None:
    sync_connection.execute(text("SET LOCAL lock_timeout = '5s';"))
    sync_connection.execute(
        text(f"LOCK TABLE {dataset_ref} IN SHARE ROW EXCLUSIVE MODE;")
    )
    sync_connection.execute(
        text(f"LOCK TABLE {checkpoint_ref} IN ACCESS EXCLUSIVE MODE;")
    )


def ensure_provider_directory_pagination_root_identity(
    sync_connection: Any,
    schema: str,
    sync_summary_by_kind: dict[str, list[str]],
) -> None:
    """Reconcile the one deployed primary-key change that generic sync cannot."""
    inspector = inspect(sync_connection)
    if not inspector.has_table(CHECKPOINT_TABLE, schema=schema):
        return
    _validate_schema_objects(inspector, schema)
    checkpoint_ref = _qualified_table(sync_connection, schema, CHECKPOINT_TABLE)
    dataset_ref = _qualified_table(sync_connection, schema, DATASET_TABLE)
    _lock_schema_objects(sync_connection, checkpoint_ref, dataset_ref)
    locked_inspector = inspect(sync_connection)
    primary_key_record = locked_inspector.get_pk_constraint(
        CHECKPOINT_TABLE,
        schema=schema,
    )
    primary_key_columns = tuple(primary_key_record.get("constrained_columns") or ())
    if primary_key_columns == LEGACY_PRIMARY_KEY_COLUMNS:
        _rekey_checkpoint_table(sync_connection, checkpoint_ref, dataset_ref)
        sync_summary_by_kind["constraints"].append(
            f"{schema}.{CHECKPOINT_PRIMARY_KEY}"
        )
    elif primary_key_columns == ROOT_PRIMARY_KEY_COLUMNS:
        if _checkpoint_root_mismatch_count(
            sync_connection,
            checkpoint_ref,
            dataset_ref,
        ):
            raise RuntimeError(
                "provider_directory_pagination_checkpoint_root_mismatch"
            )
    else:
        raise RuntimeError(
            "provider_directory_pagination_checkpoint_primary_key_unknown:"
            + ",".join(primary_key_columns)
        )
