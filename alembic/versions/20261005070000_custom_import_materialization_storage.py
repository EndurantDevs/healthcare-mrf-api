# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Route standalone materialization through exact protected revision homes."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from alembic import op

revision = "20261005070000_custom_import_materialization_storage"
down_revision = "20261005060000_custom_import_snapshot_finality"
branch_labels = None
depends_on = None

_RESOURCE_DIRECTORY = Path(__file__).resolve().parents[1] / "sql/custom_import_materialization_storage"
_BOUNDARY = "\n-- statement boundary --\n"
_RESOURCES = ("homes.sql", "control.sql", "installation.sql", "leaf.sql")
_FUNCTION = re.compile(r"CREATE FUNCTION __CONTROL__\.([a-z_]+)\((.*?)\)\s*RETURNS", re.DOTALL)
_LEAF_SPECS = (
    (
        "persist_custom_import_scalar_models",
        "bigint,bigint,__CONTROL__.custom_import_root_scalar[],__CONTROL__.custom_import_child_scalar[],"
        "bigint[],bytea,timestamptz",
        "integer",
    ),
    (
        "persist_custom_import_winner_models",
        "bigint,bigint,bigint,bigint,__CONTROL__.custom_import_winner[],bigint[],bytea,timestamptz",
        "integer",
    ),
    ("check_custom_import_materialization_completion", "bigint[],bigint[],bigint,smallint[],bigint[],bytea[]", "void"),
)


def _bulk():
    """Reuse fixed schema quoting and default-ACL closure from the prior revision."""
    path = Path(__file__).with_name("20261005040000_custom_import_bulk_snapshot_writers.py")
    spec = importlib.util.spec_from_file_location("materialization_bulk_snapshot", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _schema() -> str:
    return _bulk()._schema()


def _resource(name: str) -> tuple[str, ...]:
    """Read only the four fixed packaged SQL resources."""
    if name not in _RESOURCES:
        raise ValueError("unknown materialization storage resource")
    return tuple(part.strip() for part in (_RESOURCE_DIRECTORY / name).read_text().split(_BOUNDARY))


def _render(bulk, storage, schema: str, statement: str) -> str:
    """Resolve migration-owned control identifiers without a search-path rewrite."""
    statement = statement.replace("__CONTROL_LITERAL__", storage._literal(schema))
    statement = statement.replace("__RELATION_NAMES__", bulk._array(storage, storage._RELATION_NAMES))
    return bulk._control_sql(storage, schema, statement)


def _installation(bulk, storage, schema: str, statement: str) -> str:
    """Embed only the three fixed leaf bodies and signatures into their installer."""
    signatures = (_render(bulk, storage, schema, f"{name}({arguments})") for name, arguments, _result in _LEAF_SPECS)
    statement = statement.replace("__SIGNATURES__", bulk._array(storage, signatures))
    statement = statement.replace("__RESULTS__", bulk._array(storage, (result for _, _, result in _LEAF_SPECS)))
    # Quote complete DDL only after resolving its control identifiers. The two
    # remaining placeholders are replaced from a locked positive family ID.
    statement = _render(bulk, storage, schema, statement)
    if "__LEAF_DDL__" in statement:
        leaves = (_render(bulk, storage, schema, leaf) for leaf in _resource("leaf.sql"))
        statement = statement.replace("__LEAF_DDL__", bulk._array(storage, leaves))
    return statement


def _close_acl(storage, kind: str, identity: str) -> None:
    """New storage helpers and control rows have no non-owner direct privilege."""
    op.execute(f"REVOKE ALL ON {kind} {identity} FROM PUBLIC")
    storage._revoke_defaults(kind, identity)


def _function_identities(bulk, storage, schema: str, statement: str):
    """Extract fixed resource declarations for native ACL and downgrade targets."""
    for name, arguments in _FUNCTION.findall(statement):
        types = bulk._argument_types(arguments) if arguments.strip() else ""
        yield _render(bulk, storage, schema, f"__CONTROL__.{name}({types})")


def upgrade() -> None:
    """Install definitions only; do not create snapshots or retire old guards."""
    bulk = _bulk()
    storage = bulk._storage()
    schema = _schema()
    for name in ("homes.sql", "control.sql", "installation.sql"):
        for statement in _resource(name):
            op.execute(_installation(bulk, storage, schema, statement))
            for identity in _function_identities(bulk, storage, schema, statement):
                _close_acl(storage, "FUNCTION", identity)
    # Canonical fallback is restricted to actual unregistered producers by the
    # same public dispatcher. It is not permission to fall back from a bad home.
    for statement in _resource("leaf.sql"):
        canonical = statement.replace("__CANDIDATE__", "__CONTROL__").replace("__FAMILY_ID__", "NULL::bigint")
        op.execute(_render(bulk, storage, schema, canonical))
        for identity in _function_identities(bulk, storage, schema, canonical):
            _close_acl(storage, "FUNCTION", identity)
    for table in ("custom_import_revision_home", "custom_import_materialization_page"):
        _close_acl(storage, "TABLE", f"{storage._quote(schema)}.{table}")
    _close_acl(storage, "SEQUENCE", f"{storage._quote(schema)}.custom_import_materialization_page_page_id_seq")


def downgrade() -> None:
    """Refuse removal while any batch receipt or installed leaf function survives."""
    bulk = _bulk()
    storage = bulk._storage()
    schema = _schema()
    op.execute(
        _render(
            bulk,
            storage,
            schema,
            """
        DO $materialization$ BEGIN
            LOCK TABLE __CONTROL__.custom_import_revision_home,
                __CONTROL__.custom_import_materialization_page IN ACCESS EXCLUSIVE MODE;
            IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_revision_home)
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_materialization_page)
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
                    JOIN pg_namespace n ON n.nspname='ci_snapshot_'||f.family_id::text
                    JOIN pg_proc p ON p.pronamespace=n.oid WHERE p.proname IN (
                        'persist_custom_import_scalar_models','persist_custom_import_winner_models',
                        'check_custom_import_materialization_completion')) THEN
                RAISE EXCEPTION 'custom_import_materialization_storage_downgrade_blocked'; END IF;
        END $materialization$
    """,
        )
    )
    op.execute(f"DROP TABLE {storage._quote(schema)}.custom_import_materialization_page")
    for name in reversed(_RESOURCES):
        for statement in reversed(_resource(name)):
            canonical = statement.replace("__CANDIDATE__", "__CONTROL__")
            for identity in _function_identities(bulk, storage, schema, canonical):
                op.execute(f"DROP FUNCTION {identity}")
    op.execute(f"DROP TABLE {storage._quote(schema)}.custom_import_revision_home")
