# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Find content checks and PTG snapshot tables by name instead of stored object ids.

A database restored from another server keeps names but not object ids, so the
stored ids no longer find the history table that carries the content checks, and
PTG snapshot cleanup refused every snapshot. Installed bodies keep their owner,
privileges and settings; only the listed fragments change.
"""

from sqlalchemy import text

from alembic import op

revision = "20261126120000_stored_id_lookups_by_name"
down_revision = "20261104020000_custom_import_family_blocks"
branch_labels = None
depends_on = None

_HISTORY_BY_NAME = "to_regclass(format('%I.%I',{schema},\n               'pd_dataset_history_' || ({plan}#>>ARRAY['tables',parent_name,'original_oid'])))"
_DATASET_EDITS = {
    "guard_pd_dataset_candidate_parent": (
        ("conrelid=(plan#>>ARRAY['tables',parent_name,'original_oid'])::oid", "plan"),
    ),
    "validate_pd_dataset_bulk_relationships": (
        ("conrelid=(migration_plan#>>ARRAY['tables',parent_name,'original_oid'])::oid", "migration_plan"),
    ),
}
_CANDIDATE_IDENTITY = """        IF to_regclass(format('{q}.%I',candidate.table_name)) IS DISTINCT FROM candidate.table_oid THEN
            RAISE EXCEPTION 'ptg_snapshot_candidate_identity' USING ERRCODE='55000';
        END IF;
"""
_PTG_EDITS = {
    "ptg_snapshot_relation": (_CANDIDATE_IDENTITY,),
    "finish_ptg_snapshot_candidate": (
        """    IF to_regclass(format('{q}.%I',p_candidate))<>c.table_oid THEN
        RAISE EXCEPTION 'ptg_snapshot_candidate_identity' USING ERRCODE='55000';
    END IF;
""",
    ),
    "attach_ptg_snapshot_candidates": (_CANDIDATE_IDENTITY,),
    "cleanup_ptg_snapshot_candidates": (
        """        IF to_regclass(format('{q}.%I',history.table_name||'_history'))::oid IS DISTINCT FROM (history.plan->>'oid')::oid THEN
            RAISE EXCEPTION 'ptg_snapshot_candidate_identity' USING ERRCODE='55000';
        END IF;
""",
        _CANDIDATE_IDENTITY,
    ),
    "delete_ptg_snapshot_history": (
        """        IF to_regclass(format('{q}.%I',history_name)) IS DISTINCT FROM history.table_oid
           OR to_regclass(format('{q}.%I',history.table_name)) IS DISTINCT FROM history.parent_oid THEN
            RAISE EXCEPTION 'ptg_snapshot_gc_identity' USING ERRCODE='55000';
        END IF;
""",
    ),
}
_PTG_REPLACEMENTS = {
    "check_ptg_snapshot_write": (
        (
            "    SELECT * INTO candidate FROM {s}.ptg2_snapshot_candidate WHERE table_oid=p_relation;\n",
            "    SELECT * INTO candidate FROM {s}.ptg2_snapshot_candidate\n"
            "      WHERE table_name=(SELECT relname FROM pg_class WHERE oid=p_relation);\n",
        ),
    ),
}
_STALE_MARKERS = (
    "'original_oid'])::oid",
    "ptg_snapshot_candidate_identity",
    "ptg_snapshot_gc_identity",
    "WHERE table_oid=p_relation",
)


def _quoted(schema):
    return '"' + schema.replace('"', '""') + '"'


def _literal(schema):
    return "'" + schema.replace("'", "''") + "'"


def _edits(name, schema):
    if name in _DATASET_EDITS:
        return tuple(
            (fragment, "conrelid=" + _HISTORY_BY_NAME.format(schema=_literal(schema), plan=plan))
            for fragment, plan in _DATASET_EDITS[name]
        )
    if name in _PTG_REPLACEMENTS:
        return tuple(
            (old.format(s=_quoted(schema)), new.format(s=_quoted(schema))) for old, new in _PTG_REPLACEMENTS[name]
        )
    quoted = _quoted(schema).replace("'", "''")
    return tuple((block.format(q=quoted), "") for block in _PTG_EDITS[name])


def upgrade():
    """Rewrite each installed copy that still uses a stored id; fresh installs already match the source."""
    connection = op.get_bind()
    rows = connection.execute(
        text(
            "SELECT p.oid, n.nspname, p.proname, pg_get_functiondef(p.oid) AS definition "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE p.proname = ANY(:names) ORDER BY n.nspname, p.proname"
        ),
        {"names": [*_DATASET_EDITS, *_PTG_EDITS, *_PTG_REPLACEMENTS]},
    ).mappings()
    for row in rows:
        definition = row["definition"]
        for fragment, replacement in _edits(row["proname"], row["nspname"]):
            definition = definition.replace(fragment, replacement)
        if any(marker in definition for marker in _STALE_MARKERS):
            raise RuntimeError(f"stored_id_lookup_unrecognized:{row['nspname']}.{row['proname']}")
        if definition != row["definition"]:
            connection.exec_driver_sql(definition)


def downgrade():
    """Name lookups find the same relations on the original database, so nothing is restored."""
