# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Checkpoint fixed frozen-snapshot checks without changing their validation."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261020150000_custom_import_staged_verification"
down_revision = "20261019010000_custom_import_build_continuation"
branch_labels = None
depends_on = None

# Existing cursor labels now identify the next whole-set check, not row pages.
_STAGES = ("capture", "profiles", "families", "root_scalars", "child_scalars", "winners")
_CURSORS = (
    "after_capture_stream_slot",
    "after_profile_slot",
    "after_root_record_id",
    "current_family_revision_id",
    "current_family_expected_child_count",
    "current_family_seen_child_count",
    "after_child_collection_slot",
    "after_child_revision_id",
    "after_root_scalar_revision_id",
    "after_root_scalar_field_slot",
    "after_child_scalar_revision_id",
    "after_child_scalar_field_slot",
    "after_winner_profile_slot",
    "after_winner_entity_binding_id",
    "after_winner_context_key_sha256",
)
_COUNTS = (
    "root_count",
    "family_count",
    "generation_family_count",
    "family_child_count",
    "winner_count",
    "profile_count",
    "root_scalar_count",
    "child_scalar_count",
)
_STEP = r"""
    stage:=array_position(stages,v.scan_stage);
    IF v.verification_state IS DISTINCT FROM 'scanning' OR stage IS NULL
       OR v.page_sequence IS DISTINCT FROM (stage-1)::bigint
       OR num_nonnulls(__CURSORS__)<>0 OR __NONZERO_COUNTS__ THEN
        RAISE EXCEPTION 'custom_import_build_verification_progress_mismatch';
    END IF;
__BINDING__
    IF stage<6 THEN
        query:=(__QUERIES__)[stage];
        IF NOT (position('__ORIGIN_QUERY__' IN query)>0 AND b.build_id IS NOT NULL) THEN
            query:=replace(replace(query,'__CANDIDATE__',quote_ident(namespace)),'__BASE__',quote_ident(base_namespace));
            query:=replace(query,'__ORIGIN_QUERY__','');
            EXECUTE query INTO failure USING g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
                g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,g.base_generation_id;
            IF failure.failure_code IS NOT NULL THEN
                RAISE EXCEPTION 'custom_import_candidate_relationship_invalid'
                    USING ERRCODE='23514',DETAIL=format('%s:%s',failure.failure_code,failure.row_identity);
            END IF;
        END IF;
    ELSE
        query:=replace(__COUNTS_QUERY__,'__CANDIDATE__',quote_ident(namespace));
        EXECUTE query INTO counts USING g.generation_id,g.definition_revision_id;
    END IF;
    PERFORM __CONTROL__.lock_custom_import_snapshot_finality(g.generation_id,g.dataset_id,
        g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256);
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF stage<6 THEN
        UPDATE __CONTROL__.custom_import_build_verification
            SET scan_stage=stages[stage+1],page_sequence=v.page_sequence+1 WHERE build_id=b.build_id;
        RETURN QUERY SELECT 'scanning'::text,stages[stage+1],v.page_sequence+1,0; RETURN;
    END IF;
"""


def _previous(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("staged_verification_" + name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _finality():
    return _previous("20261005060000_custom_import_snapshot_finality")


def _schema():
    return _finality()._schema()


def _queries(finality):
    corrected = _previous("20261007000000_custom_import_rejection_anti_joins")
    return tuple(
        corrected._source_query()
        if name == "source_lineage"
        else corrected._output_query()
        if name == "output_relationships"
        else finality._resource(name)
        for name in finality._QUERY_NAMES
    )


def _body(schema):
    finality = _finality()
    bulk = finality._bulk()
    storage = bulk._storage()
    literal = storage._literal
    binding = finality._VALIDATE_BODY.split("BEGIN\n", 1)[1].split("    FOREACH query IN ARRAY", 1)[0]
    binding = binding.replace("p_generation_id", "b.generation_id")
    step = _STEP.replace("__BINDING__", binding)
    step = step.replace("__CURSORS__", ",".join(f"v.{name}" for name in _CURSORS))
    step = step.replace("__NONZERO_COUNTS__", " OR ".join(f"v.{name}<>0" for name in _COUNTS))
    step = step.replace(
        "__QUERIES__",
        "ARRAY["
        + ",".join(literal(bulk._control_sql(storage, schema, query)) for query in _queries(finality))
        + "]::text[]",
    )
    step = step.replace("__COUNTS_QUERY__", literal(bulk._control_sql(storage, schema, finality._COUNTS_QUERY)))
    step = step.replace("__CONTROL_LITERAL__", literal(schema))
    declaration = (
        "counts jsonb; verified timestamptz;\n"
        "    g __CONTROL__.custom_import_generation; f __CONTROL__.custom_import_snapshot_family;\n"
        "    snapshot_id bigint; base_family_id bigint; namespace text; base_namespace text; query text;\n"
        "    failure record; stage integer; stages text[]:=ARRAY["
        + ",".join(literal(stage) for stage in _STAGES)
        + "];"
    )
    body = finality._BUILD_BODY.replace("counts jsonb; verified timestamptz;", declaration)
    body = body.replace("    counts:=__CONTROL__.verify_custom_import_snapshot_structure(b.generation_id);", step)
    return bulk._control_sql(storage, schema, body)


def upgrade():
    """Replace one reviewed body; never reinterpret historical partial proofs."""
    finality = _finality()
    bulk = finality._bulk()
    storage = bulk._storage()
    schema = _schema()
    quoted = storage._quote(schema)
    identity = f"{quoted}.verify_custom_import_build_structure(bigint)"
    previous = " " + bulk._control_sql(storage, schema, finality._BUILD_BODY) + " "
    updated = " " + _body(schema) + " "
    bind = op.get_bind()
    installed = bind.execute(
        text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"), {"identity": identity}
    ).scalar_one_or_none()
    if installed not in {previous, updated}:
        raise RuntimeError("custom_import_staged_verification_identity_mismatch")
    # Serialize the old/new progress interpretation with existing proof writers.
    op.execute(text(f"LOCK TABLE {quoted}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE"))
    if (
        installed == previous
        and bind.execute(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {quoted}.custom_import_build_verification WHERE verification_state<>'complete')"
            )
        ).scalar_one()
    ):
        raise RuntimeError("custom_import_staged_verification_incompatible_progress")
    continuation = _previous("20261019010000_custom_import_build_continuation")
    continuation.op = op
    continuation._schema = lambda: schema
    continuation._refresh(identity, installed, updated)


def downgrade():
    """Partial set proofs cannot be reinterpreted as historical row cursors."""
    raise RuntimeError("custom_import_staged_verification_requires_forward_migration")
