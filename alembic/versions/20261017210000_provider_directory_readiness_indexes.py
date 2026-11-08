# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Index current dataset relation readiness without rewriting sealed metadata."""

from __future__ import annotations

import os

from sqlalchemy import text

from alembic import op
from db.migration_index_adoption import IndexDefinition, _expected_index_shape, _shape_from_catalog
from db.migration_index_catalog import _index_catalog_record

revision = "20261017210000_provider_directory_readiness_indexes"
down_revision = "20261015020000_custom_import_query_projections"
branch_labels = None
depends_on = None

TABLE = "provider_directory_endpoint_dataset"
# Freeze the migration's version and names independently of future read protocols.
INDEXES = (
    ("dataset_network_plan", "provider_directory_dataset_network_plan_ready_v1_idx"),
    ("dataset_affiliation_organization", "provider_directory_dataset_affiliation_ready_v1_idx"),
)


def _ready_predicate(metadata_key: str) -> str:
    # Freeze v1 independently of future runtime read protocols.
    metadata = f"publication_metadata_json::jsonb -> '{metadata_key}'"
    return f"""
        is_current IS TRUE
        AND status = 'published'
        AND published_at IS NOT NULL
        AND superseded_at IS NULL
        AND COALESCE(acquisition_root_run_id, import_run_id) IS NOT NULL
        AND COALESCE({metadata} ->> 'complete', 'false') = 'true'
        AND COALESCE({metadata} ->> 'version', '') = '1'
        AND COALESCE({metadata} ->> 'dataset_id', '') = dataset_id"""


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _create_sql(schema: str, key: str, name: str) -> str:
    return (
        f"CREATE INDEX CONCURRENTLY {_q(name)} ON {_q(schema)}.{_q(TABLE)} (dataset_id) WHERE {_ready_predicate(key)}"
    )


def _drop_sql(schema: str, name: str) -> str:
    return f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.{_q(name)}"


def _validated_index_state(schema: str, key: str, name: str):
    record = _index_catalog_record(op, name, TABLE, schema)
    if record is None:
        if op.get_bind().scalar(text("SELECT to_regclass(:name)"), {"name": f"{_q(schema)}.{_q(name)}"}):
            raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{name}")
        return None
    expected = _expected_index_shape(
        op,
        TABLE,
        schema,
        IndexDefinition(("dataset_id",), predicate=_ready_predicate(key)),
        {},
    )
    if _shape_from_catalog(record) != expected:
        raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{name}")
    return all(record[field] for field in ("indisvalid", "indisready", "indislive"))


def _index_state(schema: str, key: str, name: str):
    bind = op.get_bind()
    limits = bind.execute(text("SELECT current_setting('lock_timeout'), current_setting('statement_timeout')")).one()
    with bind.begin_nested():
        _limit_timeouts(local=True)
        state = _validated_index_state(schema, key, name)
        _restore_timeouts(limits, local=True)
    return state


def _limit_timeouts(*, local: bool) -> None:
    op.get_bind().execute(
        text(
            "SELECT set_config(name, LEAST(NULLIF(setting::bigint, 0), "
            "CASE name WHEN 'lock_timeout' THEN 2000 ELSE 120000 END)::text, :local) "
            "FROM pg_settings WHERE name IN ('lock_timeout', 'statement_timeout')"
        ),
        {"local": local},
    )


def _restore_timeouts(limits, *, local: bool) -> None:
    op.get_bind().execute(
        text("SELECT set_config('lock_timeout', :lock, :local), set_config('statement_timeout', :statement, :local)"),
        {"lock": limits[0], "statement": limits[1], "local": local},
    )


def _run_concurrently(statement: str) -> None:
    bind = op.get_bind()
    limits = bind.execute(text("SELECT current_setting('lock_timeout'), current_setting('statement_timeout')")).one()
    with op.get_context().autocommit_block():
        try:
            _limit_timeouts(local=False)
            bind.exec_driver_sql(statement)
        finally:
            _restore_timeouts(limits, local=False)


def upgrade() -> None:
    """Build or recover only exact readiness indexes with bounded waits."""
    schema = _schema()
    if op.get_context().as_sql:
        with op.get_context().autocommit_block():
            for key, name in INDEXES:
                op.execute(_create_sql(schema, key, name))
        return
    for key, name in INDEXES:
        state = _index_state(schema, key, name)
        if state is True:
            continue
        if state is False:
            _run_concurrently(_drop_sql(schema, name))
        _run_concurrently(_create_sql(schema, key, name))
        if _index_state(schema, key, name) is not True:
            raise RuntimeError(f"required_index_missing:{schema}.{name}")


def downgrade() -> None:
    """Remove matching readiness indexes without changing sealed dataset rows."""
    schema = _schema()
    if op.get_context().as_sql:
        with op.get_context().autocommit_block():
            for _key, name in reversed(INDEXES):
                op.execute(_drop_sql(schema, name))
        return
    for key, name in reversed(INDEXES):
        if _index_state(schema, key, name) is not None:
            _run_concurrently(_drop_sql(schema, name))
