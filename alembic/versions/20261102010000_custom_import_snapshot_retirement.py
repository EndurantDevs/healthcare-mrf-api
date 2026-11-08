# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Remember retired snapshot identities without adding a database callable or grant."""

import importlib.util
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20261102010000_custom_import_snapshot_retirement"
down_revision = "20261029010000_registry_source_inventory"
branch_labels = None
depends_on = None

_TABLE_DDL = (
    "\n"
    "CREATE TABLE __SCHEMA__.custom_import_snapshot_retirement (\n"
    "\tfamily_id BIGINT NOT NULL, \n"
    "\tengine_id VARCHAR(63) NOT NULL, \n"
    "\tdataset_id BIGINT NOT NULL, \n"
    "\tdefinition_revision_id BIGINT NOT NULL, \n"
    "\tschema_revision_id BIGINT NOT NULL, \n"
    "\texecution_id BIGINT NOT NULL, \n"
    "\tcapture_bundle_id BIGINT NOT NULL, \n"
    "\tproducing_fence BIGINT NOT NULL, \n"
    "\tproducing_token_sha256 BYTEA NOT NULL, \n"
    "\tgeneration_id BIGINT, \n"
    "\tretained_bytes BIGINT NOT NULL, \n"
    "\treason VARCHAR(24) NOT NULL, \n"
    "\tretired_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL, \n"
    "\tPRIMARY KEY (family_id), \n"
    "\tCONSTRAINT custom_import_retirement_attempt_key UNIQUE (execution_id, producing_fence), \n"
    "\tCONSTRAINT custom_import_retirement_identity_check CHECK (family_id > 0 AND dataset_id > 0 "
    "AND definition_revision_id > 0 AND schema_revision_id > 0 AND execution_id > 0 AND "
    "capture_bundle_id > 0 AND producing_fence > 0 AND octet_length(producing_token_sha256) = 32 "
    "AND (generation_id IS NULL OR generation_id > 0) AND retained_bytes >= 0 AND engine_id ~ "
    "'^[a-z][a-z0-9_]{0,62}$'), \n"
    "\tCONSTRAINT custom_import_retirement_generation_key UNIQUE (generation_id), \n"
    "\tCONSTRAINT custom_import_retirement_reason_check CHECK (reason IN ('replaced_fence', "
    "'abandoned', 'no_change', 'superseded'))\n"
    ")\n"
    "\n"
)


def _previous(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("retirement_" + name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema():
    return _previous("20261005030000_custom_import_snapshot_storage")._schema()


def _definitions(schema):
    continuation = _previous("20261019010000_custom_import_build_continuation")
    storage = continuation._bulk()._storage()
    read_guard = """
    IF EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_snapshot_retirement
               WHERE generation_id=p_generation_id
                  OR (execution_id=g.execution_id AND producing_fence=g.producing_fence)) THEN
        RAISE EXCEPTION 'custom_import_snapshot_retired' USING ERRCODE='55000';
    END IF;
"""
    attempt_guard = """
    IF EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_snapshot_retirement
               WHERE execution_id=p_execution_id AND producing_fence=p_fence) THEN
        RAISE EXCEPTION 'custom_import_snapshot_retired' USING ERRCODE='55000';
    END IF;
"""
    for name, arguments, body, marker, guard in (
        (
            "resolve_custom_import_generation_snapshot",
            "bigint,bigint,bigint,bigint",
            storage._READ_BINDING_BODY,
            "    SELECT * INTO g FROM __SCHEMA__.custom_import_generation WHERE generation_id=p_generation_id;\n",
            read_guard,
        ),
        (
            "lock_custom_import_snapshot_attempt",
            "bigint,bigint,bytea",
            continuation._snapshot_body(storage),
            "    SELECT * INTO l FROM __SCHEMA__.custom_import_lease WHERE execution_id=p_execution_id FOR UPDATE;\n",
            attempt_guard,
        ),
    ):
        if body.count(marker) != 1:
            raise RuntimeError("custom_import_retirement_source_mismatch")
        old = " " + storage._sql(schema, storage._body(schema, body)) + " "
        changed = body.replace(marker, marker + guard, 1)
        if name == "resolve_custom_import_generation_snapshot":
            fallback = "IF f.family_id IS NULL THEN RETURN NULL; END IF;"
            if changed.count(fallback) != 1:
                raise RuntimeError("custom_import_retirement_source_mismatch")
            changed = changed.replace(
                fallback,
                read_guard + "\n" + fallback,
                1,
            )
        new = " " + storage._sql(schema, storage._body(schema, changed)) + " "
        yield name, arguments, old, new


def upgrade():
    """Preserve native identities and privileges while denying retired reads/writes."""
    schema = _schema()
    continuation = _previous("20261019010000_custom_import_build_continuation")
    storage = continuation._bulk()._storage()
    quoted = storage._quote(schema)
    ddl_schema = postgresql.dialect().identifier_preparer.quote_schema(schema)
    op.execute(_TABLE_DDL.replace("__SCHEMA__", ddl_schema))
    identity = f"{quoted}.custom_import_snapshot_retirement"
    owners = (
        op.get_bind()
        .execute(
            text("""
        SELECT pg_catalog.pg_get_userbyid(p.proowner) FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname=:schema AND p.proname IN
            ('resolve_custom_import_generation_snapshot','lock_custom_import_snapshot_attempt')
    """),
            {"schema": schema},
        )
        .scalars()
        .all()
    )
    if len(owners) != 2 or len(set(owners)) != 1:
        raise RuntimeError("custom_import_retirement_owner_mismatch")
    op.execute(f"ALTER TABLE {identity} OWNER TO {storage._quote(owners[0])}")
    op.execute(f"REVOKE ALL ON TABLE {identity} FROM PUBLIC")
    storage.op = op
    storage._revoke_defaults("TABLE", identity)
    continuation.op = op
    continuation._schema = lambda: schema
    for name, arguments, old, new in _definitions(schema):
        continuation._refresh(f"{quoted}.{name}({arguments})", old, new)


def downgrade():
    """Never re-enable legacy fallback for a generation whose data was retired."""
    schema = _schema()
    continuation = _previous("20261019010000_custom_import_build_continuation")
    quoted = continuation._bulk()._storage()._quote(schema)
    bind = op.get_bind()
    op.execute(f"LOCK TABLE {quoted}.custom_import_snapshot_retirement IN ACCESS EXCLUSIVE MODE")
    if bind.scalar(text(f"SELECT EXISTS (SELECT 1 FROM {quoted}.custom_import_snapshot_retirement)")):
        raise RuntimeError("custom_import_retirement_downgrade_blocked")
    continuation.op = op
    continuation._schema = lambda: schema
    for name, arguments, old, new in reversed(tuple(_definitions(schema))):
        continuation._refresh(f"{quoted}.{name}({arguments})", new, old)
    op.execute(f"DROP TABLE {quoted}.custom_import_snapshot_retirement")
