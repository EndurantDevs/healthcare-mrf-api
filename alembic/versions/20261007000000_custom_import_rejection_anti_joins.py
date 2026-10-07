# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Use set-based lineage checks without changing historical renderers."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261007000000_custom_import_rejection_anti_joins"
down_revision = "20261005080000_custom_import_writer_cutover"
branch_labels = None
depends_on = None

_OLD_PROBE = """            WHERE o.rejection_id=r.rejection_id OR o.resolved_rejection_id=r.rejection_id
        )"""
_NEW_PROBE = """            WHERE o.rejection_id=r.rejection_id
        ) AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
            JOIN expected_build b ON b.build_id=o.build_id
            WHERE o.resolved_rejection_id=r.rejection_id
        )"""


def _finality():
    path = Path(__file__).with_name("20261005060000_custom_import_snapshot_finality.py")
    spec = importlib.util.spec_from_file_location("rejection_anti_join_finality", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _source_query() -> str:
    """Preserve the historical query for earlier exact-body migration checks."""
    query = _finality()._resource("source_lineage")
    query = _replace_once(query, _OLD_PROBE, _NEW_PROBE)
    query = _replace_once(query, "ON event.build_id=o.build_id AND event.origin='source'", "ON event.origin='source'")
    query = _replace_once(
        query,
        "WHERE r.rejection_id=o.resolved_rejection_id",
        "WHERE r.rejection_id=o.resolved_rejection_id AND event.build_id=o.build_id",
    )
    start = query.index("        SELECT 'occurrence_owner'::text")
    end = query.index("        UNION ALL\n        SELECT 'source_occurrence_position'")
    return query[:start] + _owner_query(query[start:end]) + query[end:]


def _replace_once(query: str, previous: str, corrected: str) -> str:
    if query.count(previous) != 1:
        raise RuntimeError("custom_import_rejection_probe_source_mismatch")
    return query.replace(previous, corrected)


def _owner_query(previous: str) -> str:
    """Union the two owner failure sets without repeated correlated row probes."""
    query = _replace_once(
        previous,
        "SELECT 'occurrence_owner'::text AS failure_code,o.occurrence_id::bigint AS row_identity",
        "SELECT o.occurrence_id",
    )
    query = _replace_once(query, "bs.build_id=b.build_id AND bs.stream_slot=o.stream_slot", "bs.build_id=b.build_id")
    query = _replace_once(
        query,
        "JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=o.pack_id",
        "CROSS JOIN __CANDIDATE__.custom_import_pack p",
    )
    query = _replace_once(
        query,
        "WHERE b.build_id=o.build_id AND ROW",
        "WHERE b.build_id=o.build_id AND bs.stream_slot=o.stream_slot AND p.pack_id=o.pack_id AND ROW",
    )
    query = _replace_once(
        query,
        "          OR (o.root_record_id IS NOT NULL AND NOT EXISTS (",
        "        UNION\n        SELECT o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o\n"
        "        WHERE o.root_record_id IS NOT NULL AND NOT EXISTS (",
    )
    query = _replace_once(query, "          ))\n", "          )\n")
    return (
        "        SELECT 'occurrence_owner'::text AS failure_code,invalid_owner.occurrence_id::bigint AS row_identity\n"
        "        FROM (\n" + query + "        ) invalid_owner\n"
    )


def _output_query() -> str:
    """Expose outer-row predicates to anti-join planning without changing scope."""
    query = _finality()._resource("output_relationships")
    for previous, corrected in (
        ("f.dataset_id=r.dataset_id AND f.field_slot=s.field_slot", "f.dataset_id=r.dataset_id"),
        (
            "WHERE r.root_revision_id=s.root_revision_id",
            "WHERE r.root_revision_id=s.root_revision_id AND f.field_slot=s.field_slot",
        ),
        ("f.dataset_id=c.dataset_id AND f.field_slot=s.field_slot", "f.dataset_id=c.dataset_id"),
        (
            "WHERE c.child_revision_id=s.child_revision_id",
            "WHERE c.child_revision_id=s.child_revision_id AND f.field_slot=s.field_slot",
        ),
        (" AND p.profile_slot=w.profile_slot", ""),
        (
            "AND m.family_revision_id=w.family_revision_id\n",
            "AND m.family_revision_id=w.family_revision_id AND p.profile_slot=w.profile_slot\n",
        ),
        (
            "JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=c.family_revision_id",
            "CROSS JOIN __CANDIDATE__.custom_import_family_revision f",
        ),
        (" AND p.profile_slot=c.profile_slot", ""),
        (
            "WHERE b.build_id=c.build_id AND ROW",
            "WHERE b.build_id=c.build_id AND f.family_revision_id=c.family_revision_id "
            "AND p.profile_slot=c.profile_slot AND ROW",
        ),
    ):
        query = _replace_once(query, previous, corrected)
    query = _replace_once(query, "SELECT violation.* FROM (", "WITH violations AS MATERIALIZED (")
    query = _replace_once(query, ") violation LIMIT 1;", ") SELECT * FROM violations LIMIT 1;")
    return query


def _schema() -> str:
    return _finality()._schema()


def _body(finality, schema: str, *, corrected: bool) -> str:
    """Render the unchanged validator with equivalent set-based lineage checks."""
    bulk = finality._bulk()
    storage = bulk._storage()
    queries = []
    for name in finality._QUERY_NAMES:
        query = _source_query() if corrected and name == "source_lineage" else finality._resource(name)
        if corrected and name == "output_relationships":
            query = _output_query()
        queries.append(storage._literal(bulk._control_sql(storage, schema, query)))
    body = finality._VALIDATE_BODY.replace("__QUERIES__", "ARRAY[" + ",".join(queries) + "]::text[]")
    body = body.replace("__CONTROL_LITERAL__", storage._literal(schema))
    body = body.replace("__COUNTS__", storage._literal(bulk._control_sql(storage, schema, finality._COUNTS_QUERY)))
    return bulk._control_sql(storage, schema, body)


def _installed(bind, schema: str):
    """Require the existing owner-only validator, never create a missing function."""
    quoted_schema = _finality()._bulk()._storage()._quote(schema)
    return bind.execute(
        text("""
            SELECT p.oid,p.proowner,p.proacl,p.prosrc FROM pg_proc p
            JOIN pg_language language ON language.oid=p.prolang
            JOIN pg_class owner_table ON owner_table.oid=to_regclass(:owner_table)
            WHERE p.oid=to_regprocedure(:identity) AND p.prokind='f' AND p.prosecdef
              AND p.prorettype='jsonb'::regtype AND language.lanname='plpgsql'
              AND NOT p.proretset AND NOT p.proisstrict AND NOT p.proleakproof
              AND p.provolatile='v' AND p.proparallel='u' AND p.pronargdefaults=0
              AND p.procost=100 AND p.prorows=0
              AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
              AND p.proowner=owner_table.relowner AND NOT EXISTS (
                SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) privilege
                WHERE privilege.grantee<>p.proowner)
        """),
        {
            "identity": f"{quoted_schema}.verify_custom_import_snapshot_structure(bigint)",
            "owner_table": f"{quoted_schema}.custom_import_generation",
        },
    ).first()


def upgrade() -> None:
    """Refresh one existing function; preserve callers, authority and candidate data."""
    finality = _finality()
    schema = _schema()
    previous = _body(finality, schema, corrected=False)
    corrected = _body(finality, schema, corrected=True)
    bind = op.get_bind()
    installed = _installed(bind, schema)
    if installed is None or installed.prosrc not in {" " + previous + " ", " " + corrected + " "}:
        raise RuntimeError("custom_import_rejection_validator_identity_mismatch")
    finality.op = op
    finality._function(
        finality._bulk(),
        schema,
        "verify_custom_import_snapshot_structure",
        "p_generation_id bigint",
        "jsonb",
        corrected,
        existing=True,
    )
    refreshed = _installed(bind, schema)
    if refreshed is None or tuple(installed[:3]) != tuple(refreshed[:3]) or refreshed.prosrc != " " + corrected + " ":
        raise RuntimeError("custom_import_rejection_validator_refresh_mismatch")


def downgrade() -> None:
    """Do not restore the repeated full-snapshot rejection search."""
    raise RuntimeError("custom_import_rejection_anti_joins_requires_forward_migration")
