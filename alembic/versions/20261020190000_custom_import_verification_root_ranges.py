# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Checkpoint the two frozen source-edge checks by complete root ranges."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261020190000_custom_import_verification_root_ranges"
down_revision = "20261020180000_custom_import_verification_arms"
branch_labels = None
depends_on = None

_ROOT_BATCH_SIZE = 16_384
_ROOTS = """SELECT array_agg(root_record_id ORDER BY root_record_id) FROM (
    SELECT DISTINCT root_record_id FROM (
        (SELECT root_record_id FROM __CANDIDATE__.custom_import_build_family
         WHERE build_id=$1 AND ($2 IS NULL OR root_record_id>$2)
         ORDER BY root_record_id LIMIT $3)
        UNION ALL
        (SELECT DISTINCT root_record_id FROM __CANDIDATE__.custom_import_build_occurrence
         WHERE build_id=$1 AND origin='source' AND child_revision_id IS NOT NULL
           AND root_record_id IS NOT NULL AND ($2 IS NULL OR root_record_id>$2)
         ORDER BY root_record_id LIMIT $3)
    ) candidates ORDER BY root_record_id LIMIT $3
) roots"""
_RANGE = """WITH range_occurrences AS MATERIALIZED (
            SELECT * FROM __CANDIDATE__.custom_import_build_occurrence
            WHERE build_id=$11 AND origin='source' AND child_revision_id IS NOT NULL
              AND root_record_id=ANY($10::bigint[])
        ), expected_build AS ("""
_CHECKPOINT = """    IF stage IN (41,42) AND cardinality(root_ids)>0 THEN
        UPDATE __CONTROL__.custom_import_build_verification
            SET after_root_record_id=root_ids[cardinality(root_ids)] WHERE build_id=b.build_id;
        RETURN QUERY SELECT 'scanning'::text,v.scan_stage::text,v.page_sequence,cardinality(root_ids); RETURN;
    END IF;
"""


def _arms():
    path = Path(__file__).with_name(down_revision + ".py")
    spec = importlib.util.spec_from_file_location("root_range_arms", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema():
    return _arms()._schema()


def _range_query(arms, query):
    """Keep every violation predicate; restrict complete-key groups to this page."""
    replace = arms._staged()._previous("20261019010000_custom_import_build_continuation")._replace
    query = query.replace("__CANDIDATE__.custom_import_build_occurrence", "range_occurrences")
    query = replace(query, "WITH expected_build AS (", _RANGE)
    if "SELECT 'source_edge_not_accepted'" in query:
        query = replace(
            query,
            "WHERE plan.selection_kind='source' AND NOT EXISTS (",
            "WHERE plan.selection_kind='source' AND plan.build_id=$11\n"
            "          AND plan.root_record_id=ANY($10::bigint[]) AND NOT EXISTS (",
        )
    return query


def _body(schema, *, root_batch_size=_ROOT_BATCH_SIZE):
    """Use a fixed metadata range, independent of source payload transport pages."""
    assert type(root_batch_size) is int and 0 < root_batch_size <= _ROOT_BATCH_SIZE
    arms = _arms()
    staged = arms._staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    body = arms._body(schema)
    for index in (40, 41):
        query = arms._materialized_query(arms._queries(staged)[index], replace)
        query = replace(query, arms._ACCEPTED_CHILDREN, arms._GROUPED_CHILDREN)
        body = replace(
            body,
            storage._literal(bulk._control_sql(storage, schema, query)),
            storage._literal(bulk._control_sql(storage, schema, _range_query(arms, query))),
        )
    cursors = ",".join("v." + name for name in staged._CURSORS)
    other_cursors = ",".join("v." + name for name in staged._CURSORS if name != "after_root_record_id")
    body = replace(body, "failure record; stage integer;", "failure record; root_ids bigint[]; stage integer;")
    body = replace(
        body,
        f"num_nonnulls({cursors})<>0",
        f"num_nonnulls({other_cursors})<>0\n       OR (v.after_root_record_id IS NOT NULL AND stage NOT IN (41,42))",
    )
    roots = storage._literal(bulk._control_sql(storage, schema, _ROOTS))
    body = replace(
        body,
        "    IF stage<cardinality(stages) THEN\n        query:=",
        "    IF stage IN (41,42) THEN\n"
        f"        query:=replace({roots},'__CANDIDATE__',quote_ident(namespace));\n"
        f"        EXECUTE query INTO root_ids USING b.build_id,v.after_root_record_id,{root_batch_size};\n"
        "    END IF;\n    IF stage<cardinality(stages) THEN\n        query:=",
    )
    body = replace(
        body,
        "g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,g.base_generation_id;",
        "g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,g.base_generation_id,root_ids,b.build_id;",
    )
    body = replace(
        body,
        "    IF stage<cardinality(stages) THEN\n        UPDATE",
        bulk._control_sql(storage, schema, _CHECKPOINT) + "    IF stage<cardinality(stages) THEN\n        UPDATE",
    )
    return replace(
        body,
        "SET scan_stage=stages[stage+1],page_sequence=v.page_sequence+1 WHERE build_id=b.build_id;",
        "SET scan_stage=stages[stage+1],page_sequence=v.page_sequence+1,after_root_record_id=NULL WHERE build_id=b.build_id;",
    )


def _guard(schema, *, ranged):
    staged = _arms()._staged()
    bounded = staged._previous("20261002010000_custom_import_bounded_build")
    membership = staged._previous("20261002040000_custom_import_child_memberships")
    body = membership._replace(
        bounded._GUARD_CUSTOM_IMPORT_BUILD_OWNED_BODY,
        "'plan_complete_at',",
        "'plan_complete_at','" + "','".join(membership._CURSORS) + "',",
    ).replace("__SCHEMA_NAME__", schema.replace("'", "''"))
    if ranged:
        replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
        body = replace(
            body,
            "OR NEW.page_sequence<>OLD.page_sequence+1 THEN",
            """OR NOT (NEW.page_sequence=OLD.page_sequence+1 OR (
                OLD.verification_state='scanning' AND OLD.scan_stage='profiles' AND OLD.page_sequence IN (40,41)
                AND NEW.after_root_record_id IS NOT NULL
                AND (OLD.after_root_record_id IS NULL OR NEW.after_root_record_id>OLD.after_root_record_id)
                AND to_jsonb(NEW)-'after_root_record_id'=to_jsonb(OLD)-'after_root_record_id')) THEN""",
        )
    return " " + bounded._sql(schema, body) + " "


def _adopt_verifier(schema):
    """Preserve the exact ranged body only through its existing metadata boundary."""
    staged = _arms()._staged()
    continuation = staged._previous("20261019010000_custom_import_build_continuation")
    continuation.op = op
    continuation._schema = lambda: schema
    quoted = staged._finality()._bulk()._storage()._quote(schema)
    body = " " + _body(schema) + " "
    continuation._refresh(f"{quoted}.verify_custom_import_build_structure(bigint)", body, body)


def upgrade():
    """Atomically refresh both existing bodies without changing proof interpretation."""
    arms = _arms()
    schema = _schema()
    staged = arms._staged()
    storage = staged._finality()._bulk()._storage()
    quoted = storage._quote(schema)
    bind = op.get_bind()
    identities = (
        (
            f"{quoted}.verify_custom_import_build_structure(bigint)",
            " " + arms._body(schema) + " ",
            " " + _body(schema) + " ",
        ),
        (f"{quoted}.guard_custom_import_build_owned()", _guard(schema, ranged=False), _guard(schema, ranged=True)),
    )
    manual = staged._previous("20261020200000_custom_import_manual_acceptance")
    manual.op = op
    if manual._installed_manual(schema):
        op.execute(text(f"LOCK TABLE {quoted}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE"))
        _adopt_verifier(schema)
        return
    installed_bodies = []
    for identity, previous, updated in identities:
        current = bind.execute(
            text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"), {"identity": identity}
        ).scalar_one_or_none()
        if current not in {previous, updated}:
            raise RuntimeError("custom_import_root_range_identity_mismatch")
        installed_bodies.append(current)
    op.execute(text(f"LOCK TABLE {quoted}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE"))
    if (
        installed_bodies[0] == identities[0][1]
        and bind.execute(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {quoted}.custom_import_build_verification "
                "WHERE verification_state<>'complete' AND after_root_record_id IS NOT NULL)"
            )
        ).scalar_one()
    ):
        raise RuntimeError("custom_import_root_range_incompatible_progress")
    continuation = staged._previous("20261019010000_custom_import_build_continuation")
    continuation.op = op
    continuation._schema = lambda: schema
    continuation._refresh(identities[0][0], installed_bodies[0], identities[0][2])
    _refresh_guard(identities[1][0], installed_bodies[1], identities[1][2], quoted)


def _installed_guard(identity, quoted):
    """Resolve only the existing invoker trigger with its original owner boundary."""
    bind = op.get_bind()
    return bind.execute(
        text("""SELECT p.prosrc,to_jsonb(p)-'prosrc' AS metadata,pg_get_functiondef(p.oid) AS definition
        FROM pg_proc p JOIN pg_language language ON language.oid=p.prolang
        JOIN pg_class owner_table ON owner_table.oid=to_regclass(:owner_table)
        WHERE p.oid=to_regprocedure(:identity) AND p.prokind='f' AND NOT p.prosecdef
          AND p.prorettype='trigger'::regtype AND language.lanname='plpgsql'
          AND p.proowner=owner_table.relowner AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
          AND NOT p.proretset AND NOT p.proisstrict AND NOT p.proleakproof AND p.prosqlbody IS NULL
          AND p.provolatile='v' AND p.proparallel='u' AND p.pronargdefaults=0 AND p.prosupport=0
          AND p.procost=100 AND p.prorows=0
          AND NOT EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl WHERE acl.grantee=0)"""),
        {"identity": identity, "owner_table": f"{quoted}.custom_import_generation"},
    ).first()


def _refresh_guard(identity, previous, updated, quoted):
    """The existing trigger remains invoker-only with identical native metadata."""
    bind = op.get_bind()
    installed = _installed_guard(identity, quoted)
    if installed is None or installed.prosrc != previous or installed.definition.count(previous) != 1:
        raise RuntimeError("custom_import_root_range_guard_identity_mismatch")
    op.execute(text(installed.definition.replace(previous, updated, 1)))
    refreshed = bind.execute(
        text("SELECT prosrc,to_jsonb(p)-'prosrc' AS metadata FROM pg_proc p WHERE oid=to_regprocedure(:identity)"),
        {"identity": identity},
    ).one()
    if refreshed.prosrc != updated or refreshed.metadata != installed.metadata:
        raise RuntimeError("custom_import_root_range_guard_refresh_mismatch")


def downgrade():
    """A partial root range requires its forward-only cursor interpretation."""
    raise RuntimeError("custom_import_root_range_requires_forward_migration")
