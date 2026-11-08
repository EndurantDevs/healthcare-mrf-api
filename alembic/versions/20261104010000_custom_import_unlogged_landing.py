# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Use crash-replayable landing scratch for future snapshot families."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261104010000_custom_import_unlogged_landing"
down_revision = "20261102040000_custom_import_post_seal_audit"
branch_labels = None
depends_on = None


def _continuation():
    path = Path(__file__).with_name("20261019010000_custom_import_build_continuation.py")
    spec = importlib.util.spec_from_file_location("unlogged_landing_previous", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema():
    return _continuation()._schema()


def _installer(continuation, child, bulk, schema):
    """Reproduce the current installer, including all retained-writer guards."""
    storage = bulk._storage()
    source = child._installer(bulk, schema, corrected=True)
    for name in continuation._LEAF_NAMES:
        source = child._replace_once(
            source,
            storage._literal(continuation._leaf_statement(name, schema, corrected=False)),
            storage._literal(continuation._leaf_statement(name, schema, corrected=True)),
        )
    return source


def _definitions(schema):
    continuation = _continuation()
    bulk = continuation._bulk()
    child = continuation._previous("20261009000000_custom_import_child_presence_decode")
    storage = bulk._storage()
    installer = _installer(continuation, child, bulk, schema)
    unlogged_installer = child._replace_once(
        installer,
        "CREATE TABLE __CANDIDATE__.source_bulk_landing",
        "CREATE UNLOGGED TABLE __CANDIDATE__.source_bulk_landing",
    )
    unlogged_installer = child._replace_once(
        unlogged_installer,
        "c.relname='source_bulk_landing' AND c.relkind='r' AND c.relpersistence='p'",
        "c.relname='source_bulk_landing' AND c.relkind='r' AND c.relpersistence='u'",
    )
    yield (
        "install_custom_import_snapshot_writers(bigint)",
        "bigint",
        installer,
        unlogged_installer,
        "$bulk_snapshot$",
        False,
    )
    relations = storage._sql(schema, storage._body(schema, storage._RELATIONS_BODY))
    unlogged_relations = child._replace_once(
        relations,
        "c.relkind='r' AND c.relpersistence='p' AND n.nspname=namespace\n          AND c.relname='source_bulk_landing'",
        "c.relkind='r' AND c.relpersistence IN ('p','u') AND n.nspname=namespace\n"
        "          AND c.relname='source_bulk_landing'",
    )
    header = (
        f"CREATE FUNCTION {storage._quote(schema)}.resolve_custom_import_snapshot_relations(p_family_id bigint) "
        "RETURNS TABLE(relation_slot smallint,table_oid bigint) "
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $snapshot$ "
    )
    yield (
        "resolve_custom_import_snapshot_relations(bigint)",
        "TABLE(relation_slot smallint,table_oid bigint)",
        header + relations + " $snapshot$",
        header + unlogged_relations + " $snapshot$",
        "$snapshot$",
        True,
    )
    batch = bulk._control_sql(storage, schema, bulk._resource("source_batch_snapshot.sql")[0])
    unlogged_batch = child._replace_once(
        batch,
        "c.relkind='r' AND c.relpersistence='p' AND c.relowner::bigint=f.landing_table_owner",
        "c.relkind='r' AND c.relpersistence IN ('p','u') AND c.relowner::bigint=f.landing_table_owner",
    )
    yield ("resolve_custom_import_source_batch_snapshot(uuid)", "bigint", batch, unlogged_batch, "$fn$", True)


def upgrade():
    """Refresh existing guards only; never alter a retained family's storage."""
    continuation = _continuation()
    bulk = continuation._bulk()
    child = continuation._previous("20261009000000_custom_import_child_presence_decode")
    child.op = op
    schema = _schema()
    quoted = bulk._storage()._quote(schema)
    # Registration takes this lock too, so no installer can cross the refresh.
    op.execute(f"LOCK TABLE {quoted}.custom_import_snapshot_family IN SHARE ROW EXCLUSIVE MODE")
    for signature, result, previous, corrected, delimiter, preserve_grants in _definitions(schema):
        child._refresh(
            op.get_bind(),
            f"{quoted}.{signature}",
            f"{quoted}.custom_import_generation",
            result,
            previous,
            corrected,
            delimiter,
            preserve_execute_grants=preserve_grants,
        )


def downgrade():
    """Keep existing unlogged candidates readable; rollback uses a forward migration."""
    raise RuntimeError("custom_import_unlogged_landing_requires_forward_migration")
