# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate retained processing without changing immutable producer rows."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import sqlalchemy as sa

from alembic import op

revision = "20261019010000_custom_import_build_continuation"
down_revision = "20261017010000_custom_import_raw_objects"
branch_labels = None
depends_on = None

_RESOURCES = Path(__file__).resolve().parents[1] / "sql/custom_import_build_continuation"
_BOUNDARY = "\n-- statement boundary --\n"
_LEASE_CHECK = (
    "(continuation_id IS NULL AND continuation_started_at IS NULL) OR "
    "(continuation_id > 0 AND fence > 0 AND token_sha256 IS NOT NULL AND expires_at IS NOT NULL "
    "AND isfinite(expires_at) AND (continuation_started_at IS NULL OR isfinite(continuation_started_at)))"
)
_LEAF_NAMES = (
    "copy_custom_import_build_retained_roots_page",
    "copy_custom_import_build_retained_families_page",
    "winner_batch_finalize",
)


def _previous(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("continuation_" + name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bulk():
    return _previous("20261005040000_custom_import_bulk_snapshot_writers")


def _schema():
    return _bulk()._schema()


def _render(schema, source):
    bulk = _bulk()
    return bulk._control_sql(bulk._storage(), schema, source.replace("__SCHEMA__", "__CONTROL__"))


def _replace(source, old, new, *, count=1):
    if source.count(old) != count:
        raise RuntimeError("custom_import_continuation_source_mismatch")
    return source.replace(old, new)


def _authority():
    return "EXISTS (" + (_RESOURCES / "authority.sql").read_text().split("EXISTS (", 1)[1].strip()


def _effective_deadline():
    return (
        "CASE WHEN l.continuation_id IS NULL THEN b.build_deadline_at WHEN "
        + _authority()
        + " THEN l.expires_at ELSE '-infinity'::timestamptz END"
    )


def _build_body(previous):
    source = previous._LOCK_CUSTOM_IMPORT_BUILD_BODY
    source = _replace(source, "timeout_ms numeric;", "timeout_ms numeric; processing_deadline timestamptz;")
    source = _replace(
        source,
        "        now_at := clock_timestamp();",
        """
        now_at := clock_timestamp();
        processing_deadline:=b.build_deadline_at;
        IF l.continuation_id IS NOT NULL THEN
            IF NOT __AUTHORITY__ THEN RAISE EXCEPTION 'custom_import_build_continuation_lost'; END IF;
            processing_deadline:=l.expires_at;
        END IF;""".replace("__AUTHORITY__", _authority()),
    )
    source = _replace(
        source,
        "l.token_sha256 IS DISTINCT FROM b.producing_token_sha256",
        "(l.continuation_id IS NULL AND l.token_sha256 IS DISTINCT FROM b.producing_token_sha256)",
    )
    source = _replace(source, "b.build_deadline_at <= now_at", "processing_deadline <= now_at")
    return _replace(source, "least(l.expires_at,b.build_deadline_at)", "least(l.expires_at,processing_deadline)")


def _snapshot_body(storage):
    source = storage._AUTHORITY_BODY
    source = _replace(
        source,
        "    now_at := clock_timestamp();",
        """
    IF l.continuation_id IS NOT NULL THEN
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt
            WHERE execution_id=p_execution_id AND producing_fence=p_fence;
        IF b.build_id IS NULL OR b.producing_token_sha256 IS DISTINCT FROM p_token_sha256 THEN
            RAISE EXCEPTION 'custom_import_snapshot_attempt_lost'; END IF;
        PERFORM __SCHEMA__.lock_custom_import_build(b.build_id);
    END IF;
    now_at := clock_timestamp();""",
    )
    return _replace(
        source,
        "l.token_sha256 IS DISTINCT FROM p_token_sha256",
        "(l.continuation_id IS NULL AND l.token_sha256 IS DISTINCT FROM p_token_sha256)",
    )


def _materialization(source):
    if "custom_import_materialization_budget(" in source.split("RETURNS", 1)[0]:
        source = _replace(
            source,
            "JOIN __CONTROL__.custom_import_lease l ON l.execution_id=a.execution_id",
            "JOIN __CONTROL__.custom_import_lease l ON l.execution_id=a.execution_id\n    JOIN __CONTROL__.custom_import_execution e ON e.execution_id=a.execution_id",
        )
        return _replace(
            source, "b.build_deadline_at,p_expected_deadline", _effective_deadline() + ",p_expected_deadline"
        )
    source = _replace(
        source,
        "e.state,l.fence,l.token_sha256)",
        "e.state,l.fence,CASE WHEN l.continuation_id IS NULL THEN l.token_sha256 WHEN "
        + _authority()
        + " THEN b.producing_token_sha256 ELSE NULL END)",
    )
    return _replace(
        source, "b.build_deadline_at,p_expected_deadline", _effective_deadline() + ",p_expected_deadline", count=2
    )


def _materialization_expected(source):
    return _replace(
        source,
        "    now_at:=clock_timestamp();",
        """
    IF l.continuation_id IS NOT NULL THEN
        PERFORM __CONTROL__.lock_custom_import_snapshot_attempt(e.execution_id,p_expected[6],p_token);
        l.token_sha256:=p_token;
    END IF;
    now_at:=clock_timestamp();""",
    )


def _generation_materialization(source):
    return _replace(
        source,
        "    IF NOT FOUND OR l.fence IS DISTINCT FROM g.producing_fence",
        """
    IF l.continuation_id IS NOT NULL THEN
        PERFORM __CONTROL__.lock_custom_import_snapshot_attempt(g.execution_id,g.producing_fence,g.producing_token_sha256);
        l.token_sha256:=g.producing_token_sha256;
    END IF;
    IF l.execution_id IS NULL OR l.fence IS DISTINCT FROM g.producing_fence""",
    )


def _trigger_overlay(source, *, generation):
    prefix = "locked_" if generation else "lease_"
    field = "producing" if generation else "sealing"
    marker = f"        IF {prefix}fence IS NULL OR {prefix}expires_at IS NULL"
    overlay = f"""
        IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_lease
            WHERE execution_id=NEW.execution_id AND continuation_id IS NOT NULL) THEN
            PERFORM __CONTROL__.lock_custom_import_snapshot_attempt(
                NEW.execution_id,NEW.{field}_fence,NEW.{field}_token_sha256);
            {prefix}token_sha256:=NEW.{field}_token_sha256;
        END IF;
"""
    return _replace(source, marker, overlay + marker)


def _function_body(statement, delimiter):
    pieces = statement.split(delimiter)
    if len(pieces) != 3:
        raise RuntimeError("custom_import_continuation_source_mismatch")
    return pieces[1]


def _refresh(identity, old_body, new_body, *, preserve_execute_grants=True):
    """Change only a reviewed body, preserving OID, signature, owner and ACLs."""
    bind = op.get_bind()
    installed_function = bind.execute(
        sa.text("""
        SELECT p.prosrc,to_jsonb(p)-'prosrc' metadata,pg_get_functiondef(p.oid) definition
        FROM pg_proc p JOIN pg_language language ON language.oid=p.prolang
        JOIN pg_class owner_table ON owner_table.oid=to_regclass(:owner_table)
        WHERE p.oid=to_regprocedure(:identity) AND p.prokind='f' AND p.prosecdef
          AND language.lanname='plpgsql' AND p.proowner=owner_table.relowner
          AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
          AND NOT p.proisstrict AND NOT p.proleakproof AND p.prosqlbody IS NULL
          AND p.provolatile='v' AND p.proparallel='u' AND p.pronargdefaults=0 AND p.prosupport=0
          AND p.procost=100 AND p.prorows=CASE WHEN p.proretset THEN 1000 ELSE 0 END
          AND NOT EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
              WHERE acl.grantee=0 OR (NOT :preserve_execute_grants AND acl.grantee<>p.proowner))
    """),
        {
            "identity": identity,
            "owner_table": f"{_bulk()._storage()._quote(_schema())}.custom_import_generation",
            "preserve_execute_grants": preserve_execute_grants,
        },
    ).first()
    if installed_function is None or installed_function.prosrc != old_body:
        raise RuntimeError("custom_import_continuation_function_identity_mismatch")
    op.execute(sa.text(_replace(installed_function.definition, old_body, new_body)))
    after = bind.execute(
        sa.text("SELECT prosrc,to_jsonb(p)-'prosrc' metadata FROM pg_proc p WHERE oid=to_regprocedure(:identity)"),
        {"identity": identity},
    ).first()
    if after is None or after.prosrc != new_body or after.metadata != installed_function.metadata:
        raise RuntimeError("custom_import_continuation_function_refresh_mismatch")


def _control_definitions(schema):
    """Fixed earlier definitions remain the drift-detection authority."""
    bounded = _previous("20261002010000_custom_import_bounded_build")
    storage = _bulk()._storage()
    for name, args, old, new in (
        ("lock_custom_import_build", "bigint", bounded._LOCK_CUSTOM_IMPORT_BUILD_BODY, _build_body(bounded)),
        (
            "lock_custom_import_snapshot_attempt",
            "bigint,bigint,bytea",
            storage._AUTHORITY_BODY,
            _snapshot_body(storage),
        ),
        (
            "begin_custom_import_build",
            "bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz",
            bounded._BEGIN_CUSTOM_IMPORT_BUILD_BODY,
            _replace(
                bounded._BEGIN_CUSTOM_IMPORT_BUILD_BODY,
                "IF e.state IS DISTINCT FROM 'running' OR p_fence IS NULL",
                "IF l.continuation_id IS NOT NULL OR e.state IS DISTINCT FROM 'running' OR p_fence IS NULL",
            ),
        ),
    ):
        yield name, args, " " + _render(schema, old) + " ", " " + _render(schema, new) + " "
    yield from _materialization_definitions(schema)
    yield from _trigger_definitions(schema)


def _materialization_definitions(schema):
    materialization = _previous("20261005070000_custom_import_materialization_storage")
    legacy = _previous("20261005050000_custom_import_legacy_snapshot_writers")
    for statement, transform, delimiter in (
        *(
            (sql, _materialization, "$fn$")
            for sql in materialization._resource("control.sql")
            if any(
                f"CREATE FUNCTION __CONTROL__.{name}(" in sql
                for name in ("custom_import_materialization_budget", "lock_custom_import_materialization_set")
            )
        ),
        *(
            (
                sql,
                _materialization_expected
                if "check_custom_import_materialization_expected(" in sql.split("RETURNS", 1)[0]
                else _generation_materialization,
                "$function$" if "AS $function$" in sql else "$fn$",
            )
            for sql in legacy._resource("legacy_authority.sql")
            if any(
                f"CREATE FUNCTION __CONTROL__.{name}(" in sql
                for name in (
                    "check_custom_import_materialization_expected",
                    "check_custom_import_generation_materialization_authority",
                )
            )
        ),
    ):
        match = re.search(r"CREATE FUNCTION __CONTROL__\.([a-z_]+)\((.*?)\)\s*RETURNS", statement, re.DOTALL)
        assert match
        yield (
            match[1],
            _bulk()._argument_types(match[2]),
            _render(schema, _function_body(statement, delimiter)),
            _render(schema, _function_body(transform(statement), delimiter)),
        )


def _trigger_definitions(schema):
    finality = _previous("20260917130000_custom_import_generation_finality")
    for name, statement, generation in (
        ("guard_custom_import_generation_insert", finality._generation_insert_function_sql(schema), True),
        ("guard_custom_import_no_change_seal_insert", finality._no_change_seal_function_sql(schema), False),
    ):
        old = _function_body(statement, "$function$")
        yield name, "", old, _render(schema, _trigger_overlay(old, generation=generation))


def _leaf_statement(name, schema, *, corrected):
    bulk = _bulk()
    source = next(
        sql
        for sql in bulk._resource("snapshot_writers.sql")
        if sql.startswith(f"CREATE FUNCTION __CANDIDATE__.{name}(")
    )
    if corrected:
        start = source.index("    SELECT e.state,l.fence,l.token_sha256,l.expires_at INTO producer")
        end = source.index(" END IF;", start) + len(" END IF;")
        source = source[:start] + "    PERFORM __CONTROL__.lock_custom_import_build(b.build_id);" + source[end:]
    return _render(schema, source)


def _refresh_writers(schema):
    """Replace existing registered functions and future installation atomically."""
    child = _previous("20261009000000_custom_import_child_presence_decode")
    bulk = _bulk()
    storage = bulk._storage()
    old = child._installer(bulk, schema, corrected=True)
    new = old
    for name in _LEAF_NAMES:
        new = _replace(
            new,
            storage._literal(_leaf_statement(name, schema, corrected=False)),
            storage._literal(_leaf_statement(name, schema, corrected=True)),
        )
    _refresh(
        f"{storage._quote(schema)}.install_custom_import_snapshot_writers(bigint)",
        _function_body(old, "$bulk_snapshot$"),
        _function_body(new, "$bulk_snapshot$"),
        preserve_execute_grants=False,
    )
    for namespace in child._registered_namespaces(op.get_bind(), schema, bulk):
        family_id = int(namespace.removeprefix("ci_snapshot_"))
        base = op.get_bind().scalar(
            sa.text(f"""
            SELECT base_snapshot.family_id FROM {storage._quote(schema)}.custom_import_snapshot_family candidate
            LEFT JOIN {storage._quote(schema)}.custom_import_build_attempt build
                ON build.execution_id=candidate.execution_id AND build.producing_fence=candidate.producing_fence
            LEFT JOIN {storage._quote(schema)}.custom_import_generation generation ON generation.generation_id=candidate.generation_id
            LEFT JOIN {storage._quote(schema)}.custom_import_snapshot_family base_snapshot
                ON base_snapshot.generation_id=coalesce(build.base_generation_id,generation.base_generation_id)
            WHERE candidate.family_id=:family_id
        """),
            {"family_id": family_id},
        )
        base_namespace = schema if base is None else f"ci_snapshot_{base}"
        quote_ident = lambda value: op.get_bind().scalar(sa.text("SELECT quote_ident(:value)"), {"value": value})
        for name in _LEAF_NAMES:
            _, args, _result = next(writer for writer in bulk._WRITERS if writer[0] == name)
            bodies = []
            for corrected in (False, True):
                statement = _leaf_statement(name, schema, corrected=corrected)
                statement = (
                    statement.replace("__CANDIDATE__", quote_ident(namespace))
                    .replace("__BASE__", quote_ident(base_namespace))
                    .replace("__FAMILY_ID__", f"{family_id}::bigint")
                )
                bodies.append(_function_body(statement, "$fn$"))
            _refresh(
                f"{storage._quote(namespace)}.{name}({bulk._argument_types(args)})",
                *bodies,
                preserve_execute_grants=False,
            )


def _schema_changes(schema, *, ledger=True):
    """Add only low-volume control metadata; no existing row is rebound."""
    op.add_column("custom_import_lease", sa.Column("continuation_id", sa.BigInteger()), schema=schema)
    op.add_column(
        "custom_import_lease", sa.Column("continuation_started_at", sa.TIMESTAMP(timezone=True)), schema=schema
    )
    op.create_check_constraint(
        "custom_import_lease_continuation_shape", "custom_import_lease", _LEASE_CHECK, schema=schema
    )
    if not ledger:
        return
    for statement in (_RESOURCES / "schema.sql").read_text().split(_BOUNDARY):
        op.execute(sa.text(_render(schema, statement)))
    op.create_foreign_key(
        "custom_import_lease_continuation_fkey",
        "custom_import_lease",
        "custom_import_build_continuation",
        ["continuation_id"],
        ["continuation_id"],
        source_schema=schema,
        referent_schema=schema,
        ondelete="RESTRICT",
    )
    storage = _bulk()._storage()
    storage.op = op
    for kind, name in (
        ("TABLE", "custom_import_build_continuation"),
        ("SEQUENCE", "custom_import_build_continuation_continuation_id_seq"),
    ):
        identity = f"{storage._quote(schema)}.{name}"
        op.execute(sa.text(f"REVOKE ALL ON {kind} {identity} FROM PUBLIC"))
        storage._revoke_defaults(kind, identity)


def upgrade():
    """Install a dormant opt-in contract; never authorize or start a worker."""
    schema = _schema()
    bounded = _previous("20261017010000_custom_import_raw_objects")
    bounded.op = op
    bounded._bound_lock_wait()
    quoted = _bulk()._storage()._quote(schema)
    op.execute(sa.text(f"LOCK TABLE {quoted}.custom_import_snapshot_family IN SHARE ROW EXCLUSIVE MODE"))
    _schema_changes(schema)
    _authority_changes(schema)


def _authority_changes(schema):
    """Refresh current and registered gates without changing their callers."""
    quoted = _bulk()._storage()._quote(schema)
    for name, args, old, new in _control_definitions(schema):
        _refresh(f"{quoted}.{name}({args})", old, new)
    _refresh_writers(schema)


def downgrade():
    """Authorization history must not disappear in a rollback."""
    raise RuntimeError("custom_import_build_continuation_requires_forward_migration")
