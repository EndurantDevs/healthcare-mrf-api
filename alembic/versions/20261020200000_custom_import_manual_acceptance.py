# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Record privileged manual acceptance without fabricating structural accounting."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "20261020200000_custom_import_manual_acceptance"
down_revision = "20261020190000_custom_import_verification_root_ranges"
branch_labels = None
depends_on = None

_NORMAL = "custom-import/build-structure/v1"
_MANUAL = "custom-import/manual-acceptance/v1"
# Exact catalog-deparsed predicates distinguish a temporary installation from partial DDL.
_CONSTRAINT_DIGESTS = {
    "custom_import_build_verification_shape": "21dbd56e31cfe4fc1a7c9f5221689bd522047ad07b4607bf2ec0452192da73b1",
    "custom_import_build_verification_acceptance": "75b95e87739c6f76d306a9835eacf535be4100475b516290f7d88167df3c2c1a",
}
_ACCEPTANCE_CHECK = (
    f"(verification_contract = '{_NORMAL}' AND manual_acceptance IS NULL) OR "
    f"(verification_contract = '{_MANUAL}' AND manual_acceptance IS NOT NULL "
    "AND jsonb_typeof(manual_acceptance) = 'object' "
    f"AND manual_acceptance->>'contract' = '{_MANUAL}' AND "
    "((verification_state = 'scanning' AND scan_stage = 'winners' AND page_sequence = 61) OR "
    "(verification_state = 'complete' AND scan_stage = 'complete' AND page_sequence = 62)))"
)
_TRANSITION = """
            IF NEW.verification_contract IS DISTINCT FROM OLD.verification_contract THEN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=session_user AND rolsuper)
                    OR OLD.verification_contract<>'custom-import/build-structure/v1'
                    OR OLD.manual_acceptance IS NOT NULL OR OLD.verification_state<>'scanning'
                    OR OLD.scan_stage<>'profiles' OR OLD.page_sequence NOT IN (40,41)
                    OR NEW.verification_contract<>'custom-import/manual-acceptance/v1'
                    OR NEW.verification_state<>'scanning' OR NEW.scan_stage<>'winners' OR NEW.page_sequence<>61
                    OR NEW.verified_at IS NOT NULL OR b.phase<>'verifying'
                    OR num_nonnulls(__NEW_CURSORS__)<>0 OR __NONZERO_COUNTS__
                    OR to_jsonb(NEW)-ARRAY[__CHANGED_FIELDS__] IS DISTINCT FROM
                        to_jsonb(OLD)-ARRAY[__CHANGED_FIELDS__]
                    OR NEW.manual_acceptance IS NULL OR jsonb_typeof(NEW.manual_acceptance)<>'object'
                    OR NEW.manual_acceptance-ARRAY['request_ref','reason']<>'{}'::jsonb
                    OR jsonb_typeof(NEW.manual_acceptance->'request_ref') IS DISTINCT FROM 'string'
                    OR jsonb_typeof(NEW.manual_acceptance->'reason') IS DISTINCT FROM 'string'
                    OR length(NEW.manual_acceptance->>'request_ref') NOT BETWEEN 1 AND 128
                    OR length(NEW.manual_acceptance->>'reason') NOT BETWEEN 1 AND 2048 THEN
                    RAISE EXCEPTION 'custom_import_manual_acceptance_invalid'; END IF;
                b:=__CONTROL__.lock_custom_import_build(b.build_id);
                IF ROW(OLD.generation_id,OLD.source_frozen_at,OLD.graph_frozen_at,OLD.output_frozen_at)
                    IS DISTINCT FROM ROW(b.generation_id,b.source_frozen_at,b.graph_frozen_at,b.output_frozen_at)
                    OR b.source_frozen_at IS NULL OR b.graph_frozen_at IS NULL OR b.output_frozen_at IS NULL
                    OR EXISTS (SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE generation_id=b.generation_id)
                    THEN RAISE EXCEPTION 'custom_import_manual_acceptance_invalid'; END IF;
                PERFORM __CONTROL__.lock_custom_import_snapshot_finality(b.generation_id,b.dataset_id,
                    b.definition_revision_id,b.schema_revision_id,b.execution_id,b.capture_bundle_id,
                    b.producing_fence,b.producing_token_sha256);
                NEW.manual_acceptance:=jsonb_build_object(
                    'contract','custom-import/manual-acceptance/v1',
                    'request_ref',NEW.manual_acceptance->>'request_ref','reason',NEW.manual_acceptance->>'reason',
                    'accepted_by',session_user,'accepted_at',clock_timestamp(),
                    'previous_proof',to_jsonb(OLD),'frozen_build',to_jsonb(b),
                    'waived_ordinals',to_jsonb(ARRAY(SELECT generate_series(OLD.page_sequence::integer+1,61))));
                RETURN NEW;
            END IF;
            IF NEW.manual_acceptance IS DISTINCT FROM OLD.manual_acceptance THEN
                RAISE EXCEPTION 'custom_import_manual_acceptance_immutable'; END IF;
"""


def _ranges():
    path = Path(__file__).with_name(down_revision + ".py")
    spec = importlib.util.spec_from_file_location("manual_acceptance_ranges", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema():
    return _ranges()._schema()


def _guard(schema):
    """Keep ordinary transitions; admit one explicitly attributed operator decision."""
    ranges = _ranges()
    staged = ranges._arms()._staged()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    body = ranges._guard(schema, ranged=True)
    changed = ("verification_contract", "manual_acceptance", "scan_stage", "page_sequence", *staged._CURSORS)
    transition = _TRANSITION.replace("__NEW_CURSORS__", ",".join("NEW." + name for name in staged._CURSORS))
    transition = transition.replace("__NONZERO_COUNTS__", " OR ".join("NEW." + name + "<>0" for name in staged._COUNTS))
    transition = transition.replace("__CHANGED_FIELDS__", ",".join("'" + name + "'" for name in changed))
    bulk = staged._finality()._bulk()
    transition = bulk._control_sql(bulk._storage(), schema, transition)
    body = replace(
        body,
        "        IF TG_OP='INSERT' THEN RETURN NEW; END IF;",
        """        IF TG_OP='INSERT' THEN
            IF TG_TABLE_NAME='custom_import_build_verification' THEN
                IF NEW.verification_contract<>'custom-import/build-structure/v1' OR NEW.manual_acceptance IS NOT NULL
                    THEN RAISE EXCEPTION 'custom_import_manual_acceptance_invalid'; END IF;
            END IF;
            RETURN NEW;
        END IF;""",
    )
    start = "        WHEN 'custom_import_build_verification' THEN\n"
    body = replace(
        body,
        start,
        start
        + f"            SELECT * INTO b FROM {bulk._storage()._quote(schema)}.custom_import_build_attempt WHERE build_id=OLD.build_id;\n"
        + transition,
    )
    return body


def _shape(previous):
    """Preserve every existing shape predicate while admitting the explicit method."""
    old = f"verification_contract)::text = '{_NORMAL}'::text"
    if previous.count(old) != 1:
        raise RuntimeError("custom_import_manual_acceptance_shape_identity_mismatch")
    return previous.replace(old, f"verification_contract)::text = ANY (ARRAY['{_NORMAL}'::text,'{_MANUAL}'::text])")


def _installed_manual(schema):
    """Recognize only the complete temporary DDL and matching protected trigger."""
    ranges = _ranges()
    ranges.op = op
    quoted = ranges._arms()._staged()._finality()._bulk()._storage()._quote(schema)
    installed = ranges._installed_guard(f"{quoted}.guard_custom_import_build_owned()", quoted)
    if installed is None or installed.prosrc != _guard(schema):
        return False
    bind = op.get_bind()
    relation = f"{quoted}.custom_import_build_verification"
    column_matches = bind.execute(
        text("""SELECT EXISTS(SELECT 1 FROM pg_attribute
        WHERE attrelid=to_regclass(:relation) AND attname='manual_acceptance' AND attnum>0
          AND NOT attisdropped AND atttypid='jsonb'::regtype AND atttypmod=-1
          AND NOT attnotnull AND NOT atthasdef AND attidentity='' AND attgenerated='' AND attcollation=0)"""),
        {"relation": relation},
    ).scalar_one()
    constraints = bind.execute(
        text("""SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint
        WHERE conrelid=to_regclass(:relation) AND conname IN
          ('custom_import_build_verification_shape','custom_import_build_verification_acceptance')
          AND contype='c' AND convalidated AND conislocal AND coninhcount=0 AND NOT connoinherit"""),
        {"relation": relation},
    ).all()
    digests_by_name = {
        constraint_row.conname: hashlib.sha256(constraint_row.definition.encode()).hexdigest()
        for constraint_row in constraints
    }
    return column_matches and digests_by_name == _CONSTRAINT_DIGESTS


def upgrade():
    """Add immutable provenance and refresh only the recognized existing trigger."""
    ranges = _ranges()
    ranges.op = op
    schema = _schema()
    quoted = ranges._arms()._staged()._finality()._bulk()._storage()._quote(schema)
    if _installed_manual(schema):
        op.execute(text(f"LOCK TABLE {quoted}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE"))
        ranges._adopt_verifier(schema)
        return
    identity = f"{quoted}.guard_custom_import_build_owned()"
    installed = ranges._installed_guard(identity, quoted)
    if installed is None or installed.prosrc != ranges._guard(schema, ranged=True):
        raise RuntimeError("custom_import_manual_acceptance_guard_identity_mismatch")
    op.execute(text(f"LOCK TABLE {quoted}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE"))
    shape = (
        op.get_bind()
        .execute(
            text("""SELECT pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conrelid=to_regclass(:relation) AND conname='custom_import_build_verification_shape'"""),
            {"relation": f"{quoted}.custom_import_build_verification"},
        )
        .scalar_one()
    )
    shape = _shape(shape)
    op.add_column(
        "custom_import_build_verification", sa.Column("manual_acceptance", JSONB(none_as_null=True)), schema=schema
    )
    op.drop_constraint("custom_import_build_verification_shape", "custom_import_build_verification", schema=schema)
    op.execute(
        text(
            f"ALTER TABLE {quoted}.custom_import_build_verification ADD CONSTRAINT custom_import_build_verification_shape {shape}"
        )
    )
    op.create_check_constraint(
        "custom_import_build_verification_acceptance",
        "custom_import_build_verification",
        _ACCEPTANCE_CHECK,
        schema=schema,
    )
    ranges._refresh_guard(identity, installed.prosrc, _guard(schema), quoted)


def downgrade():
    """Manual provenance cannot be relabeled as an automated structural result."""
    raise RuntimeError("custom_import_manual_acceptance_requires_forward_migration")
