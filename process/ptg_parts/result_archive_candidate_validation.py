# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finalize one adopted PTG candidate for a fresh destination audit.

Archive preparation produces destination-owned logical evidence and a sealed
destination layout.  This module joins those receipts back to their local rows,
installs only the sealed layout's serving manifest, and applies the normal
candidate-audit target validator.  It does not copy or create an attestation,
change a serving pointer, enqueue work, or activate the candidate. The separate
LOCAL publisher rechecks protected custody and a fresh held audit before using
the existing pointer engine in its caller-owned installation transaction.
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_candidate_audit import (
    CANDIDATE_AUDIT_MODE_AUDIT_ONLY,
    CandidateAuditTarget,
    validate_candidate_audit_target_state,
)
from process.ptg_candidate_audit import (
    IMPORTER_NAME as CANDIDATE_AUDIT_IMPORTER,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.frozen_rate_binding import _canonical_source_key
from process.ptg_parts.frozen_rate_files import FrozenRateFileValidationError
from process.ptg_parts.ptg2_candidate_attestation import CANDIDATE_SOURCE_RECORDS_SQL
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_source_lifecycle_lock
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg2_shared_source_set import shared_source_set_metadata
from process.ptg_parts.result_archive_adoption import (
    RESULT_ARCHIVE_ADOPTION_CONTRACT,
    PreparedResultArchiveLayout,
)
from process.ptg_parts.result_archive_candidate_initialization import (
    ResultArchiveCandidateInitializationError,
    _required_source_key,
)
from process.ptg_parts.result_archive_candidate_preparation import (
    RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT,
    PreparedResultArchiveCandidate,
)
from process.ptg_parts.source_pointers import (
    _stage_snapshot_in_pointer_transaction,
    candidate_snapshot_attributes,
)


async def require_local_publication_controls(session):
    """Attest the existing low-volume controls; this does not authorize a candidate."""
    from db import models
    from process.ptg_parts import ptg2_physical_binding as native
    from process.reference_family_archive import _require_transaction

    _require_transaction(session)
    model_types = (
        models.PTG2ImportRun,
        models.PTG2Snapshot,
        models.PTG2V3SnapshotLayout,
        models.PTG2V3SnapshotBinding,
        models.PTG2V3SnapshotScope,
        models.PTG2V3SnapshotPlanScope,
        models.PTG2V3CandidateAuditAttestation,
        models.PTG2SnapshotPin,
        models.PTG2CurrentSourceSnapshot,
        models.PTG2CurrentPlanSource,
        models.PTG2V4AttemptFence,
        models.PTG2V4AttemptStage,
    )
    schema_name = resolve_ptg2_schema()
    await session.execute(
        text(
            "LOCK TABLE "
            + ",".join(f"{_quote_ident(schema_name)}.{_quote_ident(model.__tablename__)}" for model in model_types)
            + " IN ACCESS SHARE MODE NOWAIT"
        )
    )
    try:
        connection = await session.connection()
        await connection.run_sync(
            lambda driver: _require_local_publication_model_catalog(driver, schema_name, model_types)
        )
        await _require_local_publication_attempt_guards(session, schema_name, model_types)
    except (RuntimeError, ValueError) as error:
        raise native.PTG2PhysicalBindingError("PTG local publication control catalog differs") from error


def _require_local_publication_model_catalog(connection, schema_name, model_types):
    """Reuse native model keys and PostgreSQL canonical CHECKs without canonical DDL."""
    from uuid import uuid4

    import sqlalchemy as sa

    from db import migration_adoption as adoption
    from db.migration_expression_adoption import _normalized_expression
    from db.migration_ptg2_v4_attempt_audit import validate_attempt_audit_trigger

    inspector = sa.inspect(connection)
    for model in model_types:
        table = model.__table__
        columns_by_name = {column["name"]: column for column in inspector.get_columns(table.name, schema=schema_name)}
        if set(columns_by_name) != set(table.c.keys()) or any(
            not adoption._is_type_compatible(columns_by_name[column.name]["type"], column.type)
            or columns_by_name[column.name]["nullable"] != column.nullable
            for column in table.c
        ):
            raise ValueError("PTG publication control columns differ")
        adoption._validate_primary_key(inspector, schema_name, table.name, tuple(table.constraints), None)
        adoption._validate_unique_constraints(inspector, schema_name, table.name, tuple(table.constraints))
        adoption._validate_foreign_keys(inspector, schema_name, table.name, tuple(table.constraints))
        temporary_name = "ptg_control_check_" + uuid4().hex
        temporary = _quote_ident(temporary_name)
        connection.execute(
            text(
                f"CREATE TEMPORARY TABLE {temporary} (LIKE {_quote_ident(schema_name)}.{_quote_ident(table.name)}) ON COMMIT DROP"
            )
        )
        try:
            for constraint in table.constraints:
                if isinstance(constraint, sa.CheckConstraint):
                    connection.execute(
                        text(
                            f"ALTER TABLE {temporary} ADD CONSTRAINT {_quote_ident(constraint.name)} CHECK ({constraint.sqltext})"
                        )
                    )
            _require_local_publication_checks(
                connection, schema_name, table.name, temporary_name, _normalized_expression
            )
            _require_local_publication_indexes(connection, schema_name, table, temporary_name)
        finally:
            connection.execute(text(f"DROP TABLE {temporary}"))
    validate_attempt_audit_trigger(connection, schema_name, is_legacy=False)


def _require_local_publication_indexes(connection, schema_name, table, temporary_name):
    """Keep the sealed semantic tuple and every native key valid and ready."""
    predicate = None
    if table.name == "ptg2_v3_snapshot_layout":
        sealed_index = next(
            index for index in table.indexes if index.name == "ptg2_v3_snapshot_layout_sealed_mapping_idx"
        )
        temporary_index = _quote_ident(temporary_name + "_sealed")
        columns_sql = ",".join(_quote_ident(column.name) for column in sealed_index.columns)
        connection.execute(
            text(
                f"CREATE UNIQUE INDEX {temporary_index} ON {_quote_ident(temporary_name)} ({columns_sql}) WHERE {sealed_index.dialect_options['postgresql']['where']}"
            )
        )
        predicate = connection.execute(
            text("SELECT pg_get_expr(indpred,indrelid) FROM pg_index WHERE indexrelid=to_regclass(:index)"),
            {"index": temporary_name + "_sealed"},
        ).scalar_one()
    proof = connection.execute(
        text(
            "SELECT c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
            "AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) "
            "AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid) "
            "AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=c.oid AND NOT convalidated) "
            "AND NOT EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND (NOT i.indisvalid OR NOT i.indisready "
            "OR NOT i.indislive OR i.indexprs IS NOT NULL OR (i.indpred IS NOT NULL AND (c.relname<>'ptg2_v3_snapshot_layout' "
            "OR pg_get_expr(i.indpred,i.indrelid) IS DISTINCT FROM :predicate)))) "
            "AND NOT EXISTS(SELECT 1 FROM pg_index i,unnest(i.indclass) cls JOIN pg_opclass op ON op.oid=cls "
            "WHERE i.indrelid=c.oid AND op.opcnamespace<>'pg_catalog'::regnamespace) "
            "AND (:predicate IS NULL OR EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisunique "
            "AND pg_get_expr(i.indpred,i.indrelid)=:predicate AND ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY key(attnum,ordinal) "
            "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=key.attnum WHERE key.ordinal<=i.indnkeyatts ORDER BY key.ordinal) "
            "= ARRAY['generation','mapping_digest','support_digest']::text[])) "
            "FROM pg_class c WHERE c.oid=to_regclass(:relation)"
        ),
        {"relation": f"{_quote_ident(schema_name)}.{_quote_ident(table.name)}", "predicate": predicate},
    ).scalar_one_or_none()
    if proof is not True:
        raise ValueError("PTG publication native keys or semantic tuple differ")


def _require_local_publication_checks(connection, schema_name, table_name, temporary_name, normalize):
    """Compare native parsed model CHECKs, rejecting unvalidated or executable drift."""
    rows = (
        connection.execute(
            text(
                "SELECT c.conrelid=to_regclass(:temporary) AS expected,c.conname,pg_get_constraintdef(c.oid,true) AS definition,"
                "c.convalidated AND NOT EXISTS(SELECT 1 FROM pg_depend d LEFT JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND p.oid=d.refobjid "
                "LEFT JOIN pg_operator o ON d.refclassid='pg_operator'::regclass AND o.oid=d.refobjid WHERE d.classid='pg_constraint'::regclass "
                "AND d.objid=c.oid AND (p.pronamespace<>'pg_catalog'::regnamespace OR o.oprnamespace<>'pg_catalog'::regnamespace)) AS safe "
                "FROM pg_constraint c WHERE c.contype='c' AND c.conrelid IN (to_regclass(:temporary),to_regclass(:actual))"
            ),
            {"temporary": temporary_name, "actual": f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"},
        )
        .mappings()
        .all()
    )
    expected_by_name = {row["conname"]: normalize(row["definition"]) for row in rows if row["expected"]}
    actual_by_name = {row["conname"]: normalize(row["definition"]) for row in rows if not row["expected"]}
    if any(not row["safe"] for row in rows) or actual_by_name != expected_by_name:
        raise ValueError("PTG publication control checks differ")


async def _require_local_publication_attempt_guards(session, schema_name, model_types):
    """Require the existing lifecycle and coordinate guards, never just their names."""
    from db.migration_expression_adoption import _normalized_expression
    from db.migration_ptg2_legacy_v3_guard_sql import common_attempt_guard_sql
    from db.migration_ptg2_v4_attempt_fence import _LIFECYCLE_FUNCTION_BODY

    common_sql = common_attempt_guard_sql(
        guard=f'{_quote_ident(schema_name)}."guard_ptg2_v4_attempt"',
        legacy_audit=f'{_quote_ident(schema_name)}."ptg2_legacy_v3_metadata_reconcile_audit"',
        snapshot=f'{_quote_ident(schema_name)}."ptg2_snapshot"',
        internal_run=f'{_quote_ident(schema_name)}."ptg2_import_run"',
        fence=f'{_quote_ident(schema_name)}."ptg2_v4_attempt_fence"',
    )
    functions_by_name = {
        "lock_ptg2_v4_attempt_lifecycle": _LIFECYCLE_FUNCTION_BODY,
        "guard_ptg2_v4_attempt": common_sql.split("AS $$", 1)[1].rsplit("$$", 1)[0],
    }
    functions = (
        (
            await session.execute(
                text(
                    "SELECT p.proname,p.prosrc,p.pronargs,p.proargtypes::text AS argument_types,p.prorettype::regtype::text AS result_type,l.lanname,"
                    "NOT p.prosecdef AND NOT p.proretset AND NOT p.proisstrict AND NOT p.proleakproof AND p.prokind='f' AND p.provolatile='v' "
                    "AND p.proparallel='u' AND p.proconfig IS NULL AND ((p.proname='guard_ptg2_v4_attempt' AND p.pronargdefaults=1 "
                    "AND pg_get_expr(p.proargdefaults,0)='false') OR (p.proname='lock_ptg2_v4_attempt_lifecycle' AND p.pronargdefaults=0)) "
                    "AS safe FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                    "JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname=:schema AND p.proname=ANY(:names)"
                ),
                {"schema": schema_name, "names": list(functions_by_name)},
            )
        )
        .mappings()
        .all()
    )
    if len(functions) != 2 or any(
        not function["safe"]
        or function["lanname"] != "plpgsql"
        or function["pronargs"] != (3 if function["proname"] == "guard_ptg2_v4_attempt" else 0)
        or function["argument_types"] != ("25 25 16" if function["proname"] == "guard_ptg2_v4_attempt" else "")
        or function["result_type"] != ("void" if function["proname"] == "guard_ptg2_v4_attempt" else "trigger")
        or _normalized_expression(function["prosrc"]) != _normalized_expression(functions_by_name[function["proname"]])
        for function in functions
    ):
        raise ValueError("PTG publication coordinate guard differs")
    for model in model_types:
        if model.__tablename__ not in {"ptg2_v3_snapshot_layout", "ptg2_v4_attempt_fence", "ptg2_v4_attempt_stage"}:
            await _require_local_publication_transition_guards(session, schema_name, model.__table__)


async def _require_local_publication_transition_guards(session, schema_name, table):
    """Authenticate each native transition trigger and its exact fixed coordinate projection."""
    snapshot_columns = (
        ("snapshot_id", "previous_snapshot_id")
        if table.name in {"ptg2_current_source_snapshot", "ptg2_current_plan_source"}
        else ("snapshot_id",)
        if "snapshot_id" in table.c
        else ()
    )
    run_columns = ("import_run_id",) if table.name in {"ptg2_snapshot", "ptg2_import_run"} else ()
    expected_body = _local_publication_transition_body(schema_name, snapshot_columns, run_columns)
    guards = (
        (
            await session.execute(
                text(
                    "SELECT t.tgname,t.tgtype,t.tgenabled::text AS tgenabled,t.tgoldtable,t.tgnewtable,p.proname,p.prosrc,n.nspname,"
                    "NOT t.tgisinternal AND NOT t.tgdeferrable AND t.tgnargs=0 AND t.tgqual IS NULL AND t.tgattr::text='' "
                    "AND NOT p.prosecdef AND p.pronargs=0 AND p.prorettype='trigger'::regtype AND p.proconfig IS NULL "
                    "AND p.prokind='f' AND p.provolatile='v' AND p.proparallel='u' AND l.lanname='plpgsql' AS safe "
                    "FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang "
                    "WHERE t.tgrelid=to_regclass(:relation) AND t.tgname=ANY(:names)"
                ),
                {
                    "relation": f"{_quote_ident(schema_name)}.{_quote_ident(table.name)}",
                    "names": [
                        table.name + "_attempt_lifecycle_lock",
                        *[
                            table.name + "_attempt_" + operation + "_guard"
                            for operation in ("insert", "update", "delete")
                        ],
                    ],
                },
            )
        )
        .mappings()
        .all()
    )
    expected_by_name = {
        table.name + "_attempt_lifecycle_lock": (30, None, None, "lock_ptg2_v4_attempt_lifecycle", None)
    }
    expected_by_name.update(
        {
            table.name + "_attempt_" + operation + "_guard": (
                mask,
                old,
                new,
                "guard_" + table.name + "_attempt",
                expected_body,
            )
            for operation, mask, old, new in (
                ("insert", 4, None, "attempt_new_rows"),
                ("update", 16, "attempt_old_rows", "attempt_new_rows"),
                ("delete", 8, "attempt_old_rows", None),
            )
        }
    )
    if len(guards) != 4 or any(
        not _local_publication_guard_matches(guard, expected_by_name, schema_name) for guard in guards
    ):
        raise ValueError("PTG publication transition guard differs")


def _local_publication_guard_matches(guard, expected_by_name, schema_name):
    """Check native trigger events, transition tables and function identity together."""
    from db.migration_expression_adoption import _normalized_expression

    expected = expected_by_name.get(guard["tgname"])
    return (
        expected is not None
        and guard["safe"]
        and guard["tgenabled"] in {"O", "A"}
        and guard["nspname"] == schema_name
        and (guard["tgtype"], guard["tgoldtable"], guard["tgnewtable"], guard["proname"]) == expected[:4]
        and (expected[4] is None or _normalized_expression(guard["prosrc"]) == _normalized_expression(expected[4]))
    )


def _local_publication_transition_body(schema_name, snapshot_columns, run_columns):
    """Render the historical coordinate projection without accepting arbitrary SQL."""
    pairs = [(snapshot_columns[0], run_columns[0])] if snapshot_columns and run_columns else []
    pairs.extend((column, None) for column in snapshot_columns[len(pairs) :])
    pairs.extend((None, column) for column in run_columns[1 if pairs and pairs[0][1] else 0 :])

    def projection(alias):
        """Use only reviewed canonical snapshot/run columns in each transition table."""
        return " UNION ".join(
            "SELECT "
            + (f"{_quote_ident(snapshot)}::text" if snapshot else "NULL::text")
            + " AS snapshot_id, "
            + (f"{_quote_ident(run)}::text" if run else "NULL::text")
            + " AS internal_run_id FROM "
            + alias
            for snapshot, run in pairs
        )

    return f"""DECLARE coordinate record; BEGIN
    IF TG_OP IN ('INSERT', 'UPDATE') THEN FOR coordinate IN SELECT DISTINCT snapshot_id, internal_run_id
    FROM ({projection("attempt_new_rows")}) AS coordinates WHERE snapshot_id IS NOT NULL OR internal_run_id IS NOT NULL LOOP
    PERFORM {_quote_ident(schema_name)}.\"guard_ptg2_v4_attempt\"(coordinate.snapshot_id, coordinate.internal_run_id); END LOOP; END IF;
    IF TG_OP IN ('DELETE', 'UPDATE') THEN FOR coordinate IN SELECT DISTINCT snapshot_id, internal_run_id
    FROM ({projection("attempt_old_rows")}) AS coordinates WHERE snapshot_id IS NOT NULL OR internal_run_id IS NOT NULL LOOP
    PERFORM {_quote_ident(schema_name)}.\"guard_ptg2_v4_attempt\"(coordinate.snapshot_id, coordinate.internal_run_id); END LOOP; END IF; RETURN NULL; END;"""


async def publish_local_data_candidate_in_transaction(
    session, *, operation, expected_attestation_digest, rollback_owner_id
):
    """Publish authenticated prepared metadata through the existing held-audit pointer engine.

    The trusted receiver first locks/rechecks the actual current operation and
    mints its installed-binding witness. The caller owns commit, installation,
    retained registration and current-generation publication in this transaction.
    """
    from uuid import UUID

    from process.ptg_parts import ptg2_physical_binding as native
    from process.ptg_parts import source_pointers as pointers
    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

    if type(expected_attestation_digest) is not bytes or len(expected_attestation_digest) != 32:
        raise native.PTG2PhysicalBindingError("PTG local publication requires its exact held attestation")
    await native.require_local_binding_publisher(session)
    snapshot_id = "snapshot-archive-" + str(UUID(str(operation["operation_id"])))
    _preparation, evidence, physical_binding = await native._prepared_local_header(session, snapshot_id)
    ownership, _descriptor = native._prepared_local_binding(evidence, snapshot_id, physical_binding.owner_oid)
    await native.require_frozen_local_preparation(session, operation=operation, ownership=ownership)
    ready = evidence["activation_evidence"]
    schema_name = resolve_ptg2_schema()
    await pointers._acquire_source_pointer_gc_lock(session, source_key=ready["source_key"])
    candidate = await native._require_local_control(session, schema_name, evidence, physical_binding)
    activation_context = await _local_activation_context(session, schema_name, snapshot_id, candidate, ready)
    await pointers.pin_reviewed_activation_predecessor(
        session,
        schema_name=schema_name,
        activation_by_field=activation_context.activation_by_field,
        activated_at=activation_context.activated_at,
        rollback_owner_id=rollback_owner_id,
        is_reviewed_audit_only=True,
    )
    await pointers._complete_candidate_activation(
        session,
        schema_name=schema_name,
        source_key=ready["source_key"],
        snapshot_id=snapshot_id,
        activation_context=activation_context,
        expected_audit_only_attestation_digest=expected_attestation_digest,
        rollback_owner_id=rollback_owner_id,
    )
    return await native._local_publication_receipt(session, evidence, physical_binding)


async def _local_activation_context(session, schema_name, snapshot_id, candidate, ready):
    """Resolve the conditional current vector and ordered plan entries before pinning."""
    from process.ptg_parts import source_pointers as pointers

    activation_identity = pointers._validated_activation_identity(
        candidate,
        source_key=ready["source_key"],
        expected_current_snapshot_id=ready["expected_current_snapshot_id"],
    )
    activated_at = await pointers._database_utc_timestamp(session)
    plan_entries = await pointers._candidate_plan_pointer_entries(
        session,
        schema_name=schema_name,
        source_key=ready["source_key"],
        snapshot_id=snapshot_id,
        previous_snapshot_id=activation_identity["previous_snapshot_id"],
        import_month=candidate["import_month"],
        activated_at=activated_at,
    )
    return pointers._CandidateActivationContext(
        candidate,
        activation_identity,
        pointers._validated_allowed_activation_identity(candidate, source_key=ready["source_key"]),
        activated_at,
        candidate["import_month"],
        plan_entries,
    )


async def local_data_publication_receipt(session, evidence, physical_binding):
    """Record and verify the actual published postimage, not proposed activation JSON."""
    from process.ptg_parts import ptg2_physical_binding as native
    from process.ptg_parts import result_archive_candidate_initialization as initialization
    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

    schema_name = resolve_ptg2_schema()
    candidate = await native._local_candidate_control(session, schema_name, physical_binding)
    if candidate is None:
        raise native.PTG2PhysicalBindingError("PTG local published control is unavailable")
    plan_scopes = await initialization._staged_plan_scopes(
        session, staging_schema=schema_name, source_snapshot_id=physical_binding.snapshot_id
    )
    publication_by_field = {
        "contract": native.PHYSICAL_BINDING_CONTRACT,
        "destination_snapshot_id": physical_binding.snapshot_id,
        "destination_layout_key": physical_binding.destination_layout_key,
        "payload_snapshot_id": physical_binding.payload_snapshot_id,
        "payload_snapshot_key": physical_binding.payload_snapshot_key,
        "dataset_id": str(physical_binding.dataset_id),
        "schema_oid": physical_binding.schema_oid,
        "owner_oid": physical_binding.owner_oid,
        "relation_oids": [list(pair) for pair in physical_binding.relation_oids],
        "sequence_oids": [list(entry) for entry in physical_binding.sequence_oids],
        "destination_activation": {
            "source_key": evidence["activation_evidence"]["source_key"],
            "snapshot_id": physical_binding.snapshot_id,
            "previous_snapshot_id": candidate["previous_snapshot_id"],
            "activated_at": candidate["published_at"].isoformat(),
            "audit_report_digest": bytes(candidate["audit_report_digest"]).hex(),
        },
        "published_control_sha256": initialization._local_control_sha256(
            candidate["manifest"], candidate["options"], plan_scopes
        ),
    }
    plans = [{"plan_id": plan_id, "plan_market_type": market} for plan_id, market in plan_scopes]
    native._require_local_published_postimage(candidate, plans, evidence, publication_by_field, physical_binding)
    return publication_by_field


RESULT_ARCHIVE_CANDIDATE_VALIDATION_CONTRACT = "ptg_result_archive_candidate_validation_v1"
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_ALLOWED_AMOUNT_TABLES = (
    "ptg2_allowed_amount_plan",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_provider_payment",
)
_LOCKED_CANDIDATE_SQL = """
    SELECT snapshot.snapshot_id, snapshot.import_run_id,
           snapshot.import_month, snapshot.status,
           snapshot.created_at, snapshot.validated_at,
           snapshot.published_at, snapshot.previous_snapshot_id,
           snapshot.manifest, internal_run.status AS run_status,
           internal_run.report AS run_report,
           internal_run.options -> 'invalid_price_exclusion_policy'
               AS invalid_price_exclusion_policy,
           binding.snapshot_key, scope.plan_id,
           scope.plan_market_type, scope.coverage_scope_id,
           layout.state AS layout_state,
           layout.generation AS layout_generation,
           layout.mapping_digest AS layout_mapping_digest,
           layout.layout_manifest,
           v4_root.state AS v4_root_state,
           v4_root.map_digest AS v4_root_map_digest,
           current_pointer.snapshot_id AS current_snapshot_id,
           frozen.binding_sha256 AS frozen_binding_sha256,
           frozen.binding_payload AS frozen_binding_payload,
           EXISTS (
               SELECT 1 FROM {schema}.ptg2_v3_candidate_audit_attestation attestation
                WHERE attestation.snapshot_id = snapshot.snapshot_id
           ) AS has_attestation,
           timezone('UTC', statement_timestamp()) AS staged_at
      FROM {schema}.ptg2_snapshot snapshot
      JOIN {schema}.ptg2_import_run internal_run
        ON internal_run.import_run_id = snapshot.import_run_id
      JOIN {schema}.ptg2_v3_snapshot_binding binding
        ON binding.snapshot_id = snapshot.snapshot_id
      JOIN {schema}.ptg2_v3_snapshot_scope scope
        ON scope.snapshot_id = snapshot.snapshot_id
      JOIN {schema}.ptg2_v3_snapshot_layout layout
        ON layout.snapshot_key = binding.snapshot_key
      JOIN {schema}.ptg2_v4_snapshot_map_root v4_root
        ON v4_root.snapshot_key = layout.snapshot_key
      LEFT JOIN {schema}.ptg2_frozen_source_file_binding frozen
        ON frozen.internal_run_id = snapshot.import_run_id
      LEFT JOIN {schema}.ptg2_current_source_snapshot current_pointer
        ON current_pointer.source_key = :source_key
     WHERE snapshot.snapshot_id = :snapshot_id
     FOR UPDATE OF snapshot, internal_run, binding, scope
"""


class ResultArchiveCandidateValidationError(RuntimeError):
    """The adopted destination state cannot enter the normal audit workflow."""


@dataclass(frozen=True)
class ValidatedResultArchiveCandidate:
    """Committed-state work that the coordinator must enqueue after this transaction."""

    contract: str
    destination_snapshot_id: str
    destination_import_run_id: str
    destination_snapshot_key: int
    source_key: str
    expected_current_snapshot_id: str | None
    next_importer: str
    next_parameters: Mapping[str, Any]
    status: str = "audit_required"
    requires_fresh_destination_attestation: bool = True


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return dict(getattr(value, "_mapping", value) or {})


def _require_transaction(session: Any) -> None:
    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction) or not in_transaction():
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation requires an already-open caller transaction"
        )


def _validated_schema(schema_name: str) -> str:
    normalized = str(schema_name or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError("schema_name must be a simple PostgreSQL identifier")
    if normalized != resolve_ptg2_schema():
        raise ValueError("schema_name must match the configured PTG schema")
    return normalized


def _validate_preparation_receipts(
    prepared_candidate: PreparedResultArchiveCandidate,
    prepared_layout: PreparedResultArchiveLayout,
) -> str:
    if (
        not isinstance(prepared_candidate, PreparedResultArchiveCandidate)
        or prepared_candidate.contract != RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT
        or prepared_candidate.requires_fresh_destination_attestation is not True
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation logical preparation receipt is invalid"
        )
    if (
        not isinstance(prepared_layout, PreparedResultArchiveLayout)
        or prepared_layout.contract != RESULT_ARCHIVE_ADOPTION_CONTRACT
        or prepared_layout.requires_fresh_destination_attestation is not True
        or len(bytes(prepared_layout.mapping_digest or b"")) != 32
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation layout preparation receipt is invalid"
        )
    snapshot_id = str(prepared_candidate.destination_snapshot_id or "").strip()
    if not snapshot_id or snapshot_id != prepared_layout.destination_snapshot_id:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation preparation receipts identify different snapshots"
        )
    return snapshot_id


def _validated_source_key(value: Any, *, exact_published: bool = False) -> str:
    """Keep published source identity exact; frozen bindings retain legacy normalization."""

    try:
        return _required_source_key(value) if exact_published else _canonical_source_key(value)
    except (FrozenRateFileValidationError, ResultArchiveCandidateInitializationError) as exc:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation destination source scope is unavailable"
        ) from exc


async def _candidate_source_key(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    exact_published: bool = False,
) -> str:
    source_key_query = await session.execute(
        text(
            f"SELECT manifest->'activation'->>'source_key' "
            f"FROM {_quote_ident(schema_name)}.ptg2_snapshot "
            "WHERE snapshot_id = :snapshot_id"
        ),
        {"snapshot_id": snapshot_id},
    )
    source_key_rows = source_key_query.all()
    raw_source_key = source_key_rows[0][0] if len(source_key_rows) == 1 else None
    return _validated_source_key(raw_source_key, exact_published=exact_published)


async def _locked_candidate_row(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    source_key: str,
) -> dict[str, Any]:
    candidate_query = await session.execute(
        text(_LOCKED_CANDIDATE_SQL.format(schema=_quote_ident(schema_name))),
        {"snapshot_id": snapshot_id, "source_key": source_key},
    )
    candidate_rows = [_mapping(candidate_record) for candidate_record in candidate_query]
    if len(candidate_rows) != 1:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation local candidate is missing or ambiguous"
        )
    return candidate_rows[0]


async def _source_records(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
) -> list[dict[str, Any]]:
    source_query = await session.execute(
        text(CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(schema_name))),
        {"snapshot_id": snapshot_id},
    )
    return [_mapping(source_record) for source_record in source_query]


async def _allowed_amount_counts(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
) -> dict[str, int]:
    schema = _quote_ident(schema_name)
    counts_by_table: dict[str, int] = {}
    for table_name in _ALLOWED_AMOUNT_TABLES:
        count_query = await session.execute(
            text(f"SELECT COUNT(*) FROM {schema}.{_quote_ident(table_name)} WHERE snapshot_id = :snapshot_id"),
            {"snapshot_id": snapshot_id},
        )
        counts_by_table[table_name] = int(count_query.scalar_one())
    return counts_by_table


def _validated_layout_serving_index(
    candidate_row: Mapping[str, Any],
    prepared_layout: PreparedResultArchiveLayout,
) -> dict[str, Any]:
    if (
        int(candidate_row.get("snapshot_key") or 0) != prepared_layout.destination_snapshot_key
        or candidate_row.get("layout_state") != "sealed"
        or candidate_row.get("layout_generation") != "shared_blocks_v4"
        or bytes(candidate_row.get("layout_mapping_digest") or b"") != bytes(prepared_layout.mapping_digest)
        or candidate_row.get("v4_root_state") != "complete"
        or bytes(candidate_row.get("v4_root_map_digest") or b"") != bytes(prepared_layout.mapping_digest)
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation layout differs from its preparation receipt"
        )
    layout_manifest = _mapping(candidate_row.get("layout_manifest"))
    serving_index = _mapping(layout_manifest.get("serving_index"))
    if serving_index.get("shared_snapshot_key") != prepared_layout.destination_snapshot_key:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation layout has no destination-local serving key"
        )
    return serving_index


async def _validate_published_layout_authority(
    session: Any,
    *,
    schema_name: str,
    receipt: Mapping[str, Any],
    prepared_layout: PreparedResultArchiveLayout,
) -> None:
    """Join the adopted map and finalizer to the sealed published-result receipt."""

    identity = receipt["identity"]
    mapping_digest = bytes(prepared_layout.mapping_digest).hex()
    if (
        prepared_layout.source_snapshot_key != identity["snapshot_key"]
        or mapping_digest != identity["layout_mapping_digest"]
        or mapping_digest != identity["map_digest"]
    ):
        raise ResultArchiveCandidateValidationError("published result layout differs from source authority")
    finalizer_query = await session.execute(
        text(
            f"SELECT state, map_digest FROM {_quote_ident(schema_name)}.ptg2_v4_finalizer_map_root "
            "WHERE snapshot_key = :snapshot_key FOR KEY SHARE"
        ),
        {"snapshot_key": prepared_layout.destination_snapshot_key},
    )
    finalizer_rows = finalizer_query.all()
    if (
        len(finalizer_rows) != 1
        or finalizer_rows[0][0] != "complete"
        or bytes(finalizer_rows[0][1] or b"").hex() != identity["finalizer_map_digest"]
    ):
        raise ResultArchiveCandidateValidationError("published result finalizer differs from source authority")


async def _validate_logical_preparation(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    candidate_row: Mapping[str, Any],
    prepared_candidate: PreparedResultArchiveCandidate,
) -> None:
    """Recheck the local candidate's logical evidence before audit staging."""

    expected_counts_by_table = {
        str(table_name): int(row_count)
        for table_name, row_count in prepared_candidate.allowed_amount_row_counts.items()
    }
    if prepared_candidate.authority_contract is not None:
        from process.ptg_parts.result_archive_candidate_initialization import ResultArchiveCandidateInitializationError
        from process.ptg_parts.result_archive_published_authority import PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT
        from process.ptg_parts.result_archive_receive_binding import validate_local_published_result

        if (
            prepared_candidate.authority_contract != PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT
            or prepared_candidate.frozen_binding_sha256 is not None
        ):
            raise ResultArchiveCandidateValidationError("published result preparation authority is invalid")
        receipt = _mapping(candidate_row.get("manifest")).get("result_archive_source")
        try:
            authority_digest = await validate_local_published_result(
                session,
                schema_name=schema_name,
                snapshot_id=snapshot_id,
                receipt=receipt,
            )
        except (ValueError, RuntimeError, ResultArchiveCandidateInitializationError) as exc:
            raise ResultArchiveCandidateValidationError("published result local authority differs") from exc
        is_authority_matching = (
            authority_digest == prepared_candidate.authority_sha256
            and receipt["identity"]["snapshot_id"] == prepared_candidate.source_snapshot_id
            and candidate_row.get("frozen_binding_sha256") is None
        )
    else:
        is_authority_matching = (
            isinstance(prepared_candidate.frozen_binding_sha256, str)
            and str(candidate_row.get("frozen_binding_sha256") or "") == prepared_candidate.frozen_binding_sha256
            and prepared_candidate.authority_sha256 is None
        )
    if (
        expected_counts_by_table
        != await _allowed_amount_counts(
            session,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
        )
        or not is_authority_matching
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation logical evidence differs from its preparation receipt"
        )


def _candidate_attributes(
    candidate_row: Mapping[str, Any],
    *,
    source_key: str,
    serving_index: Mapping[str, Any],
    exact_published: bool = False,
) -> dict[str, Any]:
    status = str(candidate_row.get("status") or "").strip().lower()
    run_status = str(candidate_row.get("run_status") or "").strip().lower()
    manifest = _mapping(candidate_row.get("manifest"))
    activation = _mapping(manifest.get("activation"))
    if status == "building" and run_status == "running":
        if activation.get("state") != "building" or "serving_index" in manifest:
            raise ResultArchiveCandidateValidationError("archive candidate validation building state is not pristine")
        previous_snapshot_id = candidate_row.get("current_snapshot_id")
        validated_at = candidate_row.get("staged_at")
    elif status == "validated" and run_status == "validated":
        if manifest.get("serving_index") != serving_index:
            raise ResultArchiveCandidateValidationError("archive candidate validation replay serving manifest changed")
        previous_snapshot_id = candidate_row.get("previous_snapshot_id")
        validated_at = candidate_row.get("validated_at")
    else:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation destination state is not building or replayable"
        )
    if _validated_source_key(activation.get("source_key"), exact_published=exact_published) != source_key:
        raise ResultArchiveCandidateValidationError("archive candidate validation destination source scope changed")
    manifest["serving_index"] = copy.deepcopy(dict(serving_index))
    return candidate_snapshot_attributes(
        {
            "snapshot_id": candidate_row["snapshot_id"],
            "import_run_id": candidate_row["import_run_id"],
            "import_month": candidate_row["import_month"],
            "created_at": candidate_row["created_at"],
            "validated_at": validated_at,
            "published_at": None,
            "previous_snapshot_id": previous_snapshot_id,
            "manifest": manifest,
        },
        source_key=source_key,
        previous_snapshot_id=previous_snapshot_id,
    )


def _attach_destination_source_identity(
    serving_index: Mapping[str, Any],
    *,
    source_key: str,
    source_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Bind locked logical scope and copied source rows to destination serving."""

    sealed_source_key = serving_index.get("source_key")
    if "source_key" in serving_index and (
        not isinstance(sealed_source_key, str) or sealed_source_key.strip().lower() != source_key
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation sealed source key differs from local scope"
        )

    try:
        source_set = shared_source_set_metadata(
            source_record.get("raw_container_sha256") for source_record in source_records
        )
    except ValueError as error:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation destination source set is invalid"
        ) from error
    sealed_source_set = serving_index.get("source_set")
    if sealed_source_set is not None and (
        not isinstance(sealed_source_set, Mapping) or dict(sealed_source_set) != source_set
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation sealed source set differs from local evidence"
        )
    return {
        **serving_index,
        "source_key": source_key,
        "source_set": source_set,
    }


async def _complete_local_run(
    session: Any,
    *,
    schema_name: str,
    candidate_attributes: Mapping[str, Any],
) -> None:
    schema = _quote_ident(schema_name)
    manifest_json = json.dumps(candidate_attributes["manifest"], default=str)
    completion_query = await session.execute(
        text(
            f"""
            WITH completed AS (
                UPDATE {schema}.ptg2_import_run
                   SET status = 'validated',
                       finished_at = statement_timestamp(),
                       heartbeat_at = statement_timestamp(),
                       report = CAST(:manifest_json AS jsonb),
                       error = NULL
                 WHERE import_run_id = :import_run_id
                   AND status = 'running'
                RETURNING import_run_id
            )
            SELECT import_run_id FROM completed
            UNION ALL
            SELECT import_run_id
              FROM {schema}.ptg2_import_run
             WHERE import_run_id = :import_run_id
               AND status = 'validated'
               AND report::jsonb = CAST(:manifest_json AS jsonb)
               AND NOT EXISTS (SELECT 1 FROM completed)
            LIMIT 1
            """
        ),
        {
            "import_run_id": candidate_attributes["import_run_id"],
            "manifest_json": manifest_json,
        },
    )
    if completion_query.first() is None:
        raise ResultArchiveCandidateValidationError("archive candidate validation local run could not become terminal")


def _audit_validation_row(
    candidate_row: Mapping[str, Any],
    candidate_attributes: Mapping[str, Any],
) -> dict[str, Any]:
    audit_state_by_name = dict(candidate_row)
    audit_state_by_name.update(candidate_attributes)
    audit_state_by_name.update(
        {
            "audit_report_digest": None,
            "audit_report": None,
            "audit_activation_intent": None,
            "audit_activated_at": None,
        }
    )
    return audit_state_by_name


async def _validated_audit_target(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    source_key: str,
    candidate_state: Mapping[str, Any],
    prepared_candidate: PreparedResultArchiveCandidate,
    prepared_layout: PreparedResultArchiveLayout,
) -> tuple[CandidateAuditTarget, dict[str, Any]]:
    """Validate local evidence with the normal audit target contract."""

    if bool(candidate_state.get("has_attestation")):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation requires a fresh destination attestation"
        )
    await _validate_logical_preparation(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        candidate_row=candidate_state,
        prepared_candidate=prepared_candidate,
    )
    if prepared_candidate.authority_contract is not None:
        await _validate_published_layout_authority(
            session,
            schema_name=schema_name,
            receipt=_mapping(candidate_state.get("manifest"))["result_archive_source"],
            prepared_layout=prepared_layout,
        )
    candidate_sources = await _source_records(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
    )
    serving_index = _attach_destination_source_identity(
        _validated_layout_serving_index(
            candidate_state,
            prepared_layout,
        ),
        source_key=source_key,
        source_records=candidate_sources,
    )
    candidate_attributes = _candidate_attributes(
        candidate_state,
        source_key=source_key,
        serving_index=serving_index,
        exact_published=prepared_candidate.authority_contract is not None,
    )
    try:
        audit_target = validate_candidate_audit_target_state(
            _audit_validation_row(candidate_state, candidate_attributes),
            candidate_run_id=str(candidate_attributes["import_run_id"]),
            source_records=candidate_sources,
        )
    except (RuntimeError, ValueError) as error:
        raise ResultArchiveCandidateValidationError(
            f"archive candidate validation normal audit target rejected: {error}"
        ) from error
    return audit_target, candidate_attributes


def _audit_handoff(
    audit_target: CandidateAuditTarget,
) -> ValidatedResultArchiveCandidate:
    """Return only the resumable post-commit audit-only work request."""

    return ValidatedResultArchiveCandidate(
        contract=RESULT_ARCHIVE_CANDIDATE_VALIDATION_CONTRACT,
        destination_snapshot_id=audit_target.snapshot_id,
        destination_import_run_id=audit_target.candidate_run_id,
        destination_snapshot_key=audit_target.snapshot_key,
        source_key=audit_target.source_key,
        expected_current_snapshot_id=audit_target.expected_current_snapshot_id,
        next_importer=CANDIDATE_AUDIT_IMPORTER,
        next_parameters={
            "candidate_run_id": audit_target.candidate_run_id,
            "snapshot_id": audit_target.snapshot_id,
            "candidate_audit_mode": CANDIDATE_AUDIT_MODE_AUDIT_ONLY,
        },
    )


async def validate_result_archive_candidate_for_audit(
    session: Any,
    *,
    schema_name: str,
    prepared_candidate: PreparedResultArchiveCandidate,
    prepared_layout: PreparedResultArchiveLayout,
) -> ValidatedResultArchiveCandidate:
    """Stage one local candidate and return its post-commit audit-only request."""

    _require_transaction(session)
    destination_schema = _validated_schema(schema_name)
    snapshot_id = _validate_preparation_receipts(prepared_candidate, prepared_layout)
    source_key = await _candidate_source_key(
        session,
        schema_name=destination_schema,
        snapshot_id=snapshot_id,
        exact_published=prepared_candidate.authority_contract is not None,
    )
    await acquire_ptg2_source_lifecycle_lock(session, source_key=source_key)
    candidate_state = await _locked_candidate_row(
        session,
        schema_name=destination_schema,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )
    audit_target, candidate_attributes = await _validated_audit_target(
        session,
        schema_name=destination_schema,
        snapshot_id=snapshot_id,
        source_key=source_key,
        candidate_state=candidate_state,
        prepared_candidate=prepared_candidate,
        prepared_layout=prepared_layout,
    )
    await _stage_snapshot_in_pointer_transaction(
        session,
        schema_name=destination_schema,
        snapshot_attributes=candidate_attributes,
    )
    await _complete_local_run(
        session,
        schema_name=destination_schema,
        candidate_attributes=candidate_attributes,
    )
    return _audit_handoff(audit_target)


async def stage_local_data_candidate_for_audit(session, *, ownership, metadata, initialized, audit, owner_oid):
    """Stage actual destination controls only after the isolated native set audit.

    The caller authenticates frozen custody and source grant, then persists this postimage in the same transaction.
    This function neither publishes nor creates an attestation.
    """
    from dataclasses import asdict

    from process.ptg_parts import ptg2_physical_binding as native
    from process.ptg_parts import result_archive_candidate_initialization as initialization
    from process.ptg_parts import result_archive_candidate_preparation as preparation

    _require_transaction(session)
    schema_name = resolve_ptg2_schema()
    scope_by_field = native.validate_local_serving_scope(metadata["closure_metadata"]["serving_scope"])
    await acquire_ptg2_source_lifecycle_lock(session, source_key=scope_by_field["source_key"])
    await preparation._local_audit_control(session, initialized, audit["control_sha256"], scope_by_field)
    descriptor_by_field = {
        "ownership": asdict(ownership),
        "initialization": asdict(initialized),
        "data": {
            "payload_snapshot_id": metadata["source_snapshot_id"],
            "payload_snapshot_key": metadata["source_snapshot_key"],
        },
    }
    # The ledger owns custody; this descriptor supplies exact paths to the authenticated creation transaction.
    descriptor_by_field["ownership"]["dataset_id"] = str(ownership.dataset_id)
    if descriptor_by_field["ownership"].pop("auxiliary_oid") is not None:
        raise ResultArchiveCandidateValidationError("local data auxiliary custody differs")
    _ownership, physical_binding = native._prepared_local_binding(
        descriptor_by_field, initialized.destination_snapshot_id, owner_oid
    )
    candidate = await native._local_candidate_control(session, schema_name, physical_binding)
    if candidate is None or candidate["snapshot_key"] != initialized.destination_layout_key:
        raise ResultArchiveCandidateValidationError("local data metadata layout binding differs")
    source_records = await initialization._rows(
        session,
        CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(ownership.schema_name)),
        {"snapshot_id": physical_binding.payload_snapshot_id},
    )
    serving_index = _attach_destination_source_identity(
        candidate["layout_manifest"]["serving_index"],
        source_key=scope_by_field["source_key"],
        source_records=source_records,
    )
    attributes = _candidate_attributes(
        candidate, source_key=scope_by_field["source_key"], serving_index=serving_index, exact_published=True
    )
    audit_target, identity = _local_audit(candidate, attributes, source_records, physical_binding, initialized, audit)
    await _stage_snapshot_in_pointer_transaction(session, schema_name=schema_name, snapshot_attributes=attributes)
    await _complete_local_run(session, schema_name=schema_name, candidate_attributes=attributes)
    return {
        **asdict(_audit_handoff(audit_target)),
        "control_sha256": initialization._local_control_sha256(
            attributes["manifest"],
            candidate["options"],
            tuple(tuple(plan) for plan in scope_by_field["plan_scopes"]),
        ),
        "identity": identity,
    }


def _local_audit(candidate, candidate_attributes, source_records, physical_binding, initialized, audit):
    """Compare native audit identity before selecting the normal held-audit handoff."""
    from process.ptg_parts.ptg2_candidate_attestation import _candidate_identity

    audit_row = _audit_validation_row(candidate, candidate_attributes)
    audit_row["raw_container_sha256_values"] = [
        source_record["raw_container_sha256"] for source_record in source_records
    ]
    audit_row["frozen_source_records"] = source_records
    identity = _candidate_identity(audit_row, physical_binding=physical_binding)
    portable_identity_by_field = {
        key: identity_value.hex() if isinstance(identity_value, bytes) else identity_value
        for key, identity_value in identity.items()
    }
    if portable_identity_by_field != {**audit["identity"], "snapshot_key": initialized.destination_layout_key}:
        raise ResultArchiveCandidateValidationError("local data native audit identity differs")
    audit_target = validate_candidate_audit_target_state(
        audit_row,
        candidate_run_id=initialized.destination_import_run_id,
        source_records=source_records,
        physical_binding=physical_binding,
    )
    return audit_target, portable_identity_by_field


async def local_data_physical_read_state(session, snapshot_id, *, is_prepared):
    """Resolve one qualified read view and keep exact payload locks in this reader transaction."""
    from process.ptg_parts import ptg2_physical_binding as native

    native._qualified_local_read_view_sha(is_prepared)
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise native.PTG2PhysicalBindingError("PTG local read requires a caller transaction")
    is_publisher, owner_oid, authority_by_field = await _local_read_authority(
        session, snapshot_id, is_prepared=is_prepared
    )
    schema_name = resolve_ptg2_schema()
    ownership, physical_binding = _local_read_authority_binding(
        authority_by_field, snapshot_id, owner_oid, is_prepared=is_prepared
    )
    if is_publisher and is_prepared:
        header, _evidence, header_binding = await native._prepared_local_header(
            session, snapshot_id, owner_oid=owner_oid
        )
        if header["validation_sha256"] != authority_by_field["validation_sha256"] or header_binding != physical_binding:
            raise native.PTG2PhysicalBindingError("PTG local publisher read preimage differs")
    for name, _oid in physical_binding.relation_oids:
        await session.execute(text(f"LOCK TABLE ONLY {physical_binding.relation(name)} IN ACCESS SHARE MODE NOWAIT"))
    await native.verify_local_data_family(session, ownership)
    await native._require_closed_local_custody(session, ownership, owner_oid)
    evidence = authority_by_field["native_validation"]
    if await native.local_data_catalog_digest(session, ownership) != evidence["catalog_sha256"]:
        raise native.PTG2PhysicalBindingError("PTG local read catalog changed")
    candidate = await native._local_candidate_control(
        session, schema_name, physical_binding, lock_controls=is_publisher is True
    )
    if candidate is None:
        raise native.PTG2PhysicalBindingError("PTG local read control is unavailable")
    plan_result = await session.execute(
        text(
            f"SELECT plan_id,plan_market_type FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_plan_scope "
            "WHERE snapshot_id=:snapshot_id ORDER BY plan_id,plan_market_type"
        ),
        {"snapshot_id": snapshot_id},
    )
    plans = plan_result.mappings().all()
    if is_prepared:
        candidate = native._require_local_control_postimage(
            candidate, tuple((plan["plan_id"], plan["plan_market_type"]) for plan in plans), evidence, physical_binding
        )
    else:
        native._require_local_published_postimage(
            candidate, plans, evidence, authority_by_field["native_publication"], physical_binding
        )
    info = getattr(session, "info", None)
    if isinstance(info, dict):
        info.setdefault("ptg2_local_read_bindings", {})[physical_binding.schema_name] = physical_binding
        info.setdefault("ptg2_local_read_catalog_sha256", {})[physical_binding.schema_name] = evidence["catalog_sha256"]
    return authority_by_field, evidence, physical_binding, dict(candidate)


async def _local_read_authority(session, snapshot_id, *, is_prepared):
    """Qualify the actual role and fixed read view before reading its bounded authority."""
    from process.ptg_parts import ptg2_physical_binding as native

    catalog_owner_oid = await native.local_preparation_catalog_owner(session)
    is_publisher = await session.scalar(
        text("SELECT pg_has_role(current_user,CAST(:owner_oid AS oid),'USAGE')"), {"owner_oid": catalog_owner_oid}
    )
    if is_publisher is True:
        owner_oid = await native.require_local_physical_publisher_view(session, is_prepared=is_prepared)
    else:
        owner_oid = await native.require_local_physical_read_view(session, is_prepared=is_prepared)
    schema_name = resolve_ptg2_schema()
    view_name = "ptg2_prepared_physical_binding" if is_prepared else "ptg2_installed_physical_binding"
    authority_result = await session.execute(
        text(
            f"SELECT * FROM {_quote_ident(schema_name)}.{_quote_ident(view_name)} "
            "WHERE destination_snapshot_id=:snapshot_id LIMIT 2"
        ),
        {"snapshot_id": snapshot_id},
    )
    authority_rows = authority_result.mappings().all()
    if len(authority_rows) != 1:
        raise native.PTG2PhysicalBindingError("PTG local read authority is unavailable")
    return is_publisher, owner_oid, dict(authority_rows[0])


async def local_data_serving_row(session, snapshot_id, row_by_field, *, is_prepared):
    """Supply actual local payload evidence to the existing strict API manifest validators."""
    from process.ptg_parts import ptg2_physical_binding as native

    _authority, _evidence, physical_binding, candidate = await local_data_physical_read_state(
        session, snapshot_id, is_prepared=is_prepared
    )
    resolved_row_by_field = _local_serving_row_fields(row_by_field, candidate, physical_binding)
    if is_prepared:
        witness_result = await session.execute(
            text(
                "SELECT contract,selection_method,encode(source_set_digest,'hex') AS source_set_digest,"
                "encode(sample_digest,'hex') AS sample_digest,queryable_occurrence_population_count AS occurrence_population_count,"
                "provider_population_count,occurrence_witness_count AS occurrence_count,provider_witness_count AS provider_count,"
                "encode(payload_sha256,'hex') AS payload_sha256 "
                f"FROM {physical_binding.relation('ptg2_v3_source_audit_witness')} WHERE snapshot_key=:snapshot_key"
            ),
            {"snapshot_key": physical_binding.payload_snapshot_key},
        )
        witness = witness_result.mappings().one_or_none()
        if witness is not None:
            resolved_row_by_field.update(
                {"persisted_witness_" + key: field_value for key, field_value in witness.items()}
            )
    else:
        resolved_row_by_field.update(await _local_serving_source_identity(session, physical_binding))
    return resolved_row_by_field, physical_binding


async def _local_serving_source_identity(session, physical_binding):
    """Read the complete bounded source dictionary from the already pinned payload."""
    from process.ptg_parts import ptg2_physical_binding as native

    source_result = await session.execute(
        text(
            f"SELECT {','.join(native._SOURCE_FIELDS)} FROM {physical_binding.relation('ptg2_v3_snapshot_source')} "
            "WHERE snapshot_id=:snapshot_id ORDER BY source_key LIMIT 257"
        ),
        {"snapshot_id": physical_binding.payload_snapshot_id},
    )
    source_rows = source_result.mappings().all()
    if not 0 < len(source_rows) <= 256:
        raise native.PTG2PhysicalBindingError("PTG local serving source dictionary differs")
    source_keys = [source_by_field["source_key"] for source_by_field in source_rows]
    return dict(
        source_row_count=len(source_rows),
        distinct_source_key_count=len(set(source_keys)),
        minimum_source_key=min(source_keys),
        maximum_source_key=max(source_keys),
        source_identity_rows=[dict(source_by_field) for source_by_field in source_rows],
    )


def _local_read_authority_binding(authority_by_field, snapshot_id, owner_oid, *, is_prepared):
    """Decode only after the fixed native view body and its owner have been authenticated."""
    from process.ptg_parts import ptg2_physical_binding as native

    try:
        expected_contract = (
            "ptg.prepared-physical-binding-read.v1" if is_prepared else "ptg.installed-physical-binding-read.v1"
        )
        evidence = authority_by_field["native_validation"]
        audit = evidence["native_audit"]
        model_sha256 = native.local_data_model_digest()
        if (
            set(authority_by_field) != set(native._local_read_view_columns(is_prepared))
            or authority_by_field["contract"] != expected_contract
            or authority_by_field["owner_oid"] != owner_oid
            or authority_by_field["destination_snapshot_id"] != snapshot_id
            or evidence["contract"] != "ptg_result.postgres.v2"
            or evidence["initialization"]["destination_snapshot_id"] != snapshot_id
            or audit["contract"] != "ptg-local-data.native-set-audit.v1"
            or not isinstance(audit["identity"], dict)
            or audit["model_sha256"] != model_sha256
            or evidence["data"]["model_sha256"] != model_sha256
            or audit["catalog_sha256"] != evidence["catalog_sha256"]
            or evidence["activation_evidence"]["control_sha256"] != evidence["control_sha256"]
            or any(
                not isinstance(authority_by_field[field], str)
                or re.fullmatch(r"[0-9a-f]{64}", authority_by_field[field]) is None
                for field in ("manifest_sha256", "validation_sha256", "inventory_sha256")
            )
            or (
                authority_by_field["artifact_sha256"] is not None
                and (
                    not isinstance(authority_by_field["artifact_sha256"], str)
                    or re.fullmatch(r"[0-9a-f]{64}", authority_by_field["artifact_sha256"]) is None
                )
            )
        ):
            raise ValueError
        ownership, physical_binding = native._prepared_local_binding(evidence, snapshot_id, owner_oid)
        native._require_local_inventory(
            ownership,
            authority_by_field["relation_inventory"],
            authority_by_field["sequence_inventory"],
            authority_by_field["inventory_sha256"],
        )
        if (
            not is_prepared
            and native._local_publication_binding(evidence, authority_by_field["native_publication"])[1]
            != physical_binding
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise native.PTG2PhysicalBindingError("PTG local read authority differs") from error
    return ownership, physical_binding


def validate_local_serving_scope(scope_by_field):
    """Decode bounded coordinate evidence, never local admission or client authorization."""
    import re

    from process.ptg_parts.ptg2_physical_binding import SERVING_SCOPE_CONTRACT, PTG2PhysicalBindingError

    fields = {
        "contract",
        "snapshot_id",
        "source_key",
        "coverage_scope_id",
        "primary_plan",
        "plan_scopes",
        "source_assignments",
    }
    if (
        type(scope_by_field) is not dict
        or set(scope_by_field) != fields
        or scope_by_field["contract"] != SERVING_SCOPE_CONTRACT
    ):
        raise PTG2PhysicalBindingError("PTG local serving scope is invalid")
    if (
        type(scope_by_field["snapshot_id"]) is not str
        or not 0 < len(scope_by_field["snapshot_id"]) <= 96
        or type(scope_by_field["source_key"]) is not str
        or not re.fullmatch(r"[a-z0-9][a-z0-9_]{0,47}", scope_by_field["source_key"])
        or type(scope_by_field["coverage_scope_id"]) is not str
        or not re.fullmatch(r"[0-9a-f]{64}", scope_by_field["coverage_scope_id"])
        or type(scope_by_field["plan_scopes"]) is not list
        or not 0 < len(scope_by_field["plan_scopes"]) <= 256
        or type(scope_by_field["source_assignments"]) is not list
        or not 0 < len(scope_by_field["source_assignments"]) <= 256
    ):
        raise PTG2PhysicalBindingError("PTG local serving scope is invalid")
    _validate_local_scope_plans(scope_by_field)
    _validate_local_scope_assignments(scope_by_field)
    return scope_by_field


def _validate_local_scope_plans(scope_by_field):
    """Preserve complete sorted plan coordinates and the declared primary plan."""
    from process.ptg_parts.ptg2_physical_binding import PTG2PhysicalBindingError

    for plan in scope_by_field["plan_scopes"]:
        if (
            type(plan) is not list
            or len(plan) != 2
            or type(plan[0]) is not str
            or not 0 < len(plan[0]) <= 64
            or plan[0] != plan[0].strip()
            or type(plan[1]) is not str
            or len(plan[1]) > 32
            or plan[1] != plan[1].strip().lower()
        ):
            raise PTG2PhysicalBindingError("PTG local plan scope is invalid")
    if scope_by_field["primary_plan"] not in scope_by_field["plan_scopes"] or scope_by_field["plan_scopes"] != [
        list(plan) for plan in sorted({tuple(plan) for plan in scope_by_field["plan_scopes"]})
    ]:
        raise PTG2PhysicalBindingError("PTG local plan scope is incomplete")


def _validate_local_scope_assignments(scope_by_field):
    """Validate the same bounded native source assignments and canonical scope encoding."""
    from api.ptg2_tables import PTG2ManifestArtifactError, _validated_published_source_set
    from process.ptg_parts.canonical import canonical_json_dumps
    from process.ptg_parts.ptg2_physical_binding import _SOURCE_FIELDS, PTG2PhysicalBindingError

    for assignment in scope_by_field["source_assignments"]:
        if (
            type(assignment) is not dict
            or set(assignment) != set(_SOURCE_FIELDS)
            or type(assignment["source_key"]) is not int
            or not 0 <= assignment["source_key"] < 2**31
            or type(assignment["source_type"]) is not str
            or len(assignment["source_type"]) > 32
        ):
            raise PTG2PhysicalBindingError("PTG local source assignment is invalid")
    try:
        _validated_published_source_set(
            scope_by_field["source_assignments"], expected_source_count=len(scope_by_field["source_assignments"])
        )
    except PTG2ManifestArtifactError as error:
        raise PTG2PhysicalBindingError("PTG local source assignment semantics differ") from error
    keys = [assignment["source_key"] for assignment in scope_by_field["source_assignments"]]
    if keys != sorted(keys) or len(canonical_json_dumps(scope_by_field).encode()) > 32768:
        raise PTG2PhysicalBindingError("PTG local source scope is incomplete or oversized")


__all__ = [
    "RESULT_ARCHIVE_CANDIDATE_VALIDATION_CONTRACT",
    "ResultArchiveCandidateValidationError",
    "ValidatedResultArchiveCandidate",
    "validate_result_archive_candidate_for_audit",
    "stage_local_data_candidate_for_audit",
]


def _local_serving_row_fields(row_by_field, candidate, physical_binding):
    """Rewrite only from already authenticated native payload and destination controls."""
    serving_index = candidate["manifest"]["serving_index"]
    layout_serving_index = candidate["layout_manifest"]["serving_index"]
    resolved_row_by_field = {
        **row_by_field,
        **{
            key: candidate[key]
            for key in (
                "attested_source_key",
                "attested_coverage_scope_id",
                "attested_source_set_digest",
                "attested_audit_sample_digest",
            )
        },
    }
    resolved_row_by_field.update(
        candidate_serving_index=serving_index,
        layout_serving_index=layout_serving_index,
        snapshot_source_set=serving_index.get("source_set"),
        bound_snapshot_key=physical_binding.destination_layout_key,
        layout_audit_sample=layout_serving_index.get("audit_sample"),
        layout_source_witness=layout_serving_index.get("source_witness"),
        layout_coverage_scope_id=layout_serving_index.get("coverage_scope_id"),
        layout_code_count=layout_serving_index.get("code_count"),
        snapshot_plan_id=candidate["plan_id"],
        snapshot_plan_market_type=candidate["plan_market_type"],
        snapshot_coverage_scope_id=bytes(candidate["coverage_scope_id"]).hex(),
    )
    return resolved_row_by_field
