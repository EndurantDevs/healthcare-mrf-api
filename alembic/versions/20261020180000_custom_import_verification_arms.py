# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Checkpoint each unchanged frozen relationship check independently."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261020180000_custom_import_verification_arms"
down_revision = "20261020150000_custom_import_staged_verification"
branch_labels = None
depends_on = None

_LABELS = (
    tuple(
        """metadata_scope generation_scope generation_target_required base_generation_scope build_scope
        root_dictionary_owner root_dictionary_canonical_identity root_dictionary_identity entity_dictionary_owner
        entity_dictionary_canonical_identity root_dictionary_rebinding entity_dictionary_identity
        entity_dictionary_rebinding pack_reference pack_revision_count pack_source_ordinal_duplicate rejection_reference
        root_revision_reference child_revision_reference family_reference family_child_reference family_child_count
        family_child_duplicate_key generation_membership_reference family_missing_generation_membership""".split()
    ),
    tuple(
        """occurrence_owner source_occurrence_position occurrence_root_revision occurrence_child_revision
        occurrence_initial_rejection occurrence_resolved_rejection root_revision_missing_occurrence
        rejection_occurrence_identity child_revision_missing_occurrence rejection_missing_occurrence family_plan_reference
        source_family_plan family_missing_plan expected_family_plan_missing family_plan_not_selected
        source_edge_not_accepted accepted_child_missing_edge""".split()
    ),
    tuple(
        """retained_plan_base_reference retained_base_root_pack retained_base_edge_reference retained_base_child_pack
        retained_family_content retained_occurrence_plan retained_root_copy retained_child_copy
        retained_family_missing_root_copy retained_edge_missing_copy retained_copy_missing_edge
        retained_base_child_missing_copy""".split()
    ),
    tuple(
        """root_scalar_reference child_scalar_reference winner_reference context_reference winner_context
        capture_stream""".split()
    ),
)
_STAGES = (
    ("capture",) * len(_LABELS[0])
    + ("profiles",) * len(_LABELS[1])
    + ("families",) * len(_LABELS[2])
    + ("root_scalars",) * len(_LABELS[3])
    + ("child_scalars", "winners")
)
_ACCEPTED_CHILDREN = """        ), accepted_children AS (
            SELECT o.* FROM __CANDIDATE__.custom_import_build_occurrence o
            JOIN streams s ON s.stream_slot=o.stream_slot AND s.record_kind='child' AND s.collection_slot=o.collection_slot
            WHERE o.origin='source' AND o.child_revision_id IS NOT NULL AND o.resolved_rejection_id IS NULL
              AND (s.duplicate_policy<>'collapse_identical' OR NOT EXISTS (
                SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence later
                WHERE later.build_id=o.build_id AND later.origin='source' AND later.stream_slot=o.stream_slot
                  AND later.root_record_id=o.root_record_id AND later.collection_slot=o.collection_slot
                  AND later.raw_parent_key_sha256=o.raw_parent_key_sha256 AND later.child_key_sha256=o.child_key_sha256
                  AND later.child_revision_id IS NOT NULL AND later.source_ordinal>o.source_ordinal
              ))
        ), plan_roots AS ("""
_GROUPED_CHILDREN = """        ), later_child_maxima AS MATERIALIZED (
            SELECT build_id,stream_slot,root_record_id,collection_slot,raw_parent_key_sha256,child_key_sha256,
              max(source_ordinal) AS last_source_ordinal,true AS has_later
            FROM __CANDIDATE__.custom_import_build_occurrence
            WHERE origin='source' AND child_revision_id IS NOT NULL
            GROUP BY build_id,stream_slot,root_record_id,collection_slot,raw_parent_key_sha256,child_key_sha256
            HAVING count(*)>1
        ), accepted_children AS (
            SELECT o.* FROM __CANDIDATE__.custom_import_build_occurrence o
            JOIN streams s ON s.stream_slot=o.stream_slot AND s.record_kind='child' AND s.collection_slot=o.collection_slot
            LEFT JOIN later_child_maxima later ON later.build_id=o.build_id AND later.stream_slot=o.stream_slot
              AND later.root_record_id=o.root_record_id AND later.collection_slot=o.collection_slot
              AND later.raw_parent_key_sha256=o.raw_parent_key_sha256 AND later.child_key_sha256=o.child_key_sha256
              AND later.last_source_ordinal>o.source_ordinal
            WHERE o.origin='source' AND o.child_revision_id IS NOT NULL AND o.resolved_rejection_id IS NULL
              AND (s.duplicate_policy<>'collapse_identical' OR later.has_later IS NULL)
        ), plan_roots AS ("""


def _staged():
    path = Path(__file__).with_name(down_revision + ".py")
    spec = importlib.util.spec_from_file_location("arm_verification_staged", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema():
    return _staged()._schema()


def _split(query, labels):
    """Recognize only the fixed failure labels, never nested set operations."""
    matches = tuple(re.finditer(r"(?m)^( +)SELECT '([a-z_]+)'", query))
    if tuple(match[2] for match in matches) != labels or len({match[1] for match in matches}) != 1:
        raise RuntimeError("custom_import_arm_query_source_mismatch")
    indent = matches[0][1]
    is_materialized = "WITH violations AS MATERIALIZED (" in query
    opening = "WITH violations AS MATERIALIZED (" if is_materialized else "SELECT violation.* FROM ("
    closing = ") SELECT * FROM violations LIMIT 1;" if is_materialized else ") violation LIMIT 1;"
    if query.count(opening) != 1 or query.count(closing) != 1:
        raise RuntimeError("custom_import_arm_query_source_mismatch")
    end = query.index(closing)
    if query[end + len(closing) :].strip():
        raise RuntimeError("custom_import_arm_query_source_mismatch")
    prefix, suffix = query[: matches[0].start()], query[end:]
    pattern = rf"\n{indent}UNION ALL\n(?={indent}SELECT '(?:{'|'.join(labels[1:])})')"
    arms = re.split(pattern, query[matches[0].start() : end])
    if len(arms) != len(labels) or any(
        not arm.startswith(f"{indent}SELECT '{label}'") for arm, label in zip(arms, labels, strict=True)
    ):
        raise RuntimeError("custom_import_arm_query_source_mismatch")
    if is_materialized:
        prefix = prefix.replace(opening, "WITH violations(failure_code,row_identity) AS MATERIALIZED (")
    else:
        suffix = suffix.replace(closing, ") violation(failure_code,row_identity) LIMIT 1;")
    return tuple(prefix + arm + suffix for arm in arms)


def _queries(staged):
    groups = staged._queries(staged._finality())
    checks = tuple(check for query, labels in zip(groups[:4], _LABELS, strict=True) for check in _split(query, labels))
    return (*checks, groups[4])


def _checkpoint_body(schema):
    staged = _staged()
    finality = staged._finality()
    bulk, storage = finality._bulk(), finality._bulk()._storage()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    literal = storage._literal

    def array(values):
        """Render fixed SQL text values with the historical literal encoder."""
        return "ARRAY[" + ",".join(literal(value) for value in values) + "]::text[]"

    old_queries = (bulk._control_sql(storage, schema, query) for query in staged._queries(finality))
    queries = (bulk._control_sql(storage, schema, query) for query in _queries(staged))
    body = replace(staged._body(schema), array(old_queries), array(queries))
    body = replace(body, array(staged._STAGES).removesuffix("::text[]"), array(_STAGES).removesuffix("::text[]"))
    body = replace(
        body,
        "stage:=array_position(stages,v.scan_stage);",
        """stage:=NULL;
    IF v.page_sequence>=0 AND v.page_sequence<cardinality(stages) THEN
        stage:=v.page_sequence::integer+1;
    END IF;""",
    )
    body = replace(
        body, "v.page_sequence IS DISTINCT FROM (stage-1)::bigint", "v.scan_stage IS DISTINCT FROM stages[stage]"
    )
    return replace(body, "IF stage<6 THEN", "IF stage<cardinality(stages) THEN", count=2)


def _materialized_query(query, replace):
    """Keep the outer LIMIT outside the complete unchanged violation query."""
    query = replace(query, "SELECT violation.* FROM (", "WITH violations(failure_code,row_identity) AS MATERIALIZED (")
    return replace(query, ") violation(failure_code,row_identity) LIMIT 1;", ") SELECT * FROM violations LIMIT 1;")


def _plan_body(schema):
    """Fence the plan check from the outer LIMIT without changing predicates."""
    staged = _staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    index = len(_LABELS[0]) + _LABELS[1].index("expected_family_plan_missing")
    query = _queries(staged)[index]
    materialized = _materialized_query(query, replace)
    return replace(
        _checkpoint_body(schema),
        storage._literal(bulk._control_sql(storage, schema, query)),
        storage._literal(bulk._control_sql(storage, schema, materialized)),
    )


def _resource_body(schema):
    """Fence all source, retained and output checks from startup-plan bias."""
    staged = _staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    body = _checkpoint_body(schema)
    for query in _queries(staged)[len(_LABELS[0]) : -1]:
        if "WITH violations(failure_code,row_identity) AS MATERIALIZED (" in query:
            continue
        body = replace(
            body,
            storage._literal(bulk._control_sql(storage, schema, query)),
            storage._literal(bulk._control_sql(storage, schema, _materialized_query(query, replace))),
        )
    return body


def _body(schema):
    """Group the unchanged later-occurrence comparison for the two edge checks."""
    staged = _staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    body = _resource_body(schema)
    for label in ("source_edge_not_accepted", "accepted_child_missing_edge"):
        index = len(_LABELS[0]) + _LABELS[1].index(label)
        query = _materialized_query(_queries(staged)[index], replace)
        grouped = replace(query, _ACCEPTED_CHILDREN, _GROUPED_CHILDREN)
        body = replace(
            body,
            storage._literal(bulk._control_sql(storage, schema, query)),
            storage._literal(bulk._control_sql(storage, schema, grouped)),
        )
    return body


def upgrade():
    """Preserve complete proofs and compatible per-check partial progress."""
    staged = _staged()
    storage = staged._finality()._bulk()._storage()
    schema = _schema()
    quoted = storage._quote(schema)
    identity = f"{quoted}.verify_custom_import_build_structure(bigint)"
    previous = " " + staged._body(schema) + " "
    checkpoint, plan = " " + _checkpoint_body(schema) + " ", " " + _plan_body(schema) + " "
    resource = " " + _resource_body(schema) + " "
    updated = " " + _body(schema) + " "
    bind = op.get_bind()
    installed = bind.execute(
        text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"), {"identity": identity}
    ).scalar_one_or_none()
    ranges = staged._previous("20261020190000_custom_import_verification_root_ranges")
    if installed == " " + ranges._body(schema) + " ":
        ranges.op = op
        guard = ranges._installed_guard(f"{quoted}.guard_custom_import_build_owned()", quoted)
        if guard is None or guard.prosrc != ranges._guard(schema, ranged=True):
            manual = staged._previous("20261020200000_custom_import_manual_acceptance")
            manual.op = op
            if guard is None or guard.prosrc != manual._guard(schema) or not manual._installed_manual(schema):
                raise RuntimeError("custom_import_arm_verification_root_guard_mismatch")
        op.execute(text(f"LOCK TABLE {quoted}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE"))
        ranges._adopt_verifier(schema)
        return
    if installed not in {previous, checkpoint, plan, resource, updated}:
        raise RuntimeError("custom_import_arm_verification_identity_mismatch")
    op.execute(text(f"LOCK TABLE {quoted}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE"))
    if installed == previous:
        cursors = ",".join(staged._CURSORS)
        zero_counts = " AND ".join(f"{name}=0" for name in staged._COUNTS)
        incompatible = bind.execute(
            text(f"""
            SELECT EXISTS(SELECT 1 FROM {quoted}.custom_import_build_verification
            WHERE verification_state<>'complete' AND NOT (
                verification_state='scanning' AND scan_stage='capture' AND page_sequence=0
                AND num_nonnulls({cursors})=0 AND {zero_counts}))
        """)
        ).scalar_one()
        if incompatible:
            raise RuntimeError("custom_import_arm_verification_incompatible_progress")
    continuation = staged._previous("20261019010000_custom_import_build_continuation")
    continuation.op = op
    continuation._schema = lambda: schema
    continuation._refresh(identity, installed, updated)


def downgrade():
    """An arm checkpoint cannot be reinterpreted as a completed resource."""
    raise RuntimeError("custom_import_arm_verification_requires_forward_migration")
