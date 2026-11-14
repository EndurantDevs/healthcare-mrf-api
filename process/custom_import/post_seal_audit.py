# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected, resumable structural verification of an already sealed build.

This does not issue a SOURCE fixed-point certificate. Each call owns one bounded
SQL arm or complete-key page; the caller commits it or rolls back every error.
Original build proofs, manual provenance, seals and serving rows stay unchanged.
"""

import asyncio
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from sqlalchemy import select, text

from db.models.custom_import import CustomImportGeneration, CustomImportGenerationSeal
from db.models.custom_import_post_seal_audit import CustomImportPostSealAudit as Audit
from db.models.custom_import_post_seal_audit import (
    CustomImportPostSealAuditCheckpoint as Checkpoint,
)
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_scratch, publication, retirement
from process.custom_import.definition import canonical_json
from process.custom_import.storage_layout import SNAPSHOT_MODELS, snapshot_schema

CONTRACT = "custom-import/post-seal-structure/v1"
SAMPLED_CONTRACT = "custom-import/post-seal-structure-sampled/v1"
ROOT_PAGE_ROWS = 16_384
MAX_STEP_SECONDS = 120
_ZERO = b"\0" * 32
_CONTROL_FIELDS = {
    "custom_import_schema_revision": (
        "schema_revision_id dataset_id revision_number canonical_schema schema_sha256 created_at "
    ).split(),
    "custom_import_definition_revision": (
        "definition_revision_id dataset_id schema_revision_id revision_number contract_version refresh_mode "
        "canonical_definition definition_sha256 created_at "
    ).split(),
    "custom_import_execution": (
        "execution_id dataset_id definition_revision_id schema_revision_id idempotency_key mechanism state "
        "capture_bundle_id request_identity_sha256 source_binding_revision_id terminal_reason started_at "
        "finished_at created_at updated_at "
    ).split(),
    "custom_import_capture_bundle": (
        "capture_bundle_id dataset_id definition_revision_id schema_revision_id snapshot_token "
        "snapshot_token_sha256 canonical_manifest manifest_sha256 stream_count sealed_at payload_contract "
        "capture_state producing_execution_id producing_fence producing_token_sha256 request_identity_sha256 "
        "source_binding_revision_id source_binding_sha256 source_request_sha256 statement_sha256 "
        "canonical_policy policy_sha256 acquisition_started_at acquisition_deadline_at committed_part_count "
        "committed_byte_count committed_decoded_byte_count committed_arrow_byte_count committed_record_count "
        "committed_manifest_byte_count "
    ).split(),
    "custom_import_capture": (
        "capture_bundle_id dataset_id definition_revision_id schema_revision_id stream_slot content_sha256 "
        "byte_count canonical_manifest manifest_sha256 payload_contract payload_part_count payload_set_sha256 "
        "sealed_at capture_state eof_at manifest_set_sha256 committed_part_count committed_byte_count "
        "committed_decoded_byte_count committed_arrow_byte_count committed_record_count "
        "committed_manifest_byte_count "
    ).split(),
    "custom_import_generation": (
        "generation_id dataset_id definition_revision_id schema_revision_id execution_id capture_bundle_id "
        "base_generation_id base_dataset_id source_bundle_sha256 candidate_sha256 root_count family_count "
        "producing_fence producing_token_sha256 created_at "
    ).split(),
    "custom_import_generation_seal": (
        "generation_id dataset_id definition_revision_id schema_revision_id execution_id capture_bundle_id "
        "seal_contract materialization_contract verification_evidence query_projection_contract "
        "query_projection_capability_sha256 query_projection_sha256 query_projection_root_count "
        "query_projection_child_count sealing_fence sealing_token_sha256 root_count family_count "
        "generation_family_count family_child_count winner_count profile_count root_scalar_count "
        "child_scalar_count materialization_sha256 effective_output_sha256 sealed_at "
    ).split(),
    "custom_import_build_attempt": (
        "build_id build_contract dataset_id definition_revision_id schema_revision_id execution_id "
        "capture_bundle_id producing_fence producing_token_sha256 request_identity_sha256 base_generation_id "
        "base_pointer_version refresh_mode complete_scope phase generation_id page_row_limit page_byte_limit "
        "statement_timeout_ms build_deadline_at admission_after_occurrence_id plan_stage plan_page_sequence "
        "plan_after_base_root_record_id plan_after_source_root_record_id plan_complete_at "
        "output_after_profile_slot output_after_entity_binding_id output_after_context_key_sha256 "
        "source_occurrence_count candidate_error_count selected_family_count completed_family_count "
        "candidate_context_count generation_family_count winner_count next_rejection_ordinal source_frozen_at "
        "graph_frozen_at output_frozen_at verified_at created_at "
    ).split(),
    "custom_import_build_verification": (
        "build_id generation_id verification_contract manual_acceptance verification_state scan_stage "
        "page_sequence after_capture_stream_slot after_profile_slot after_root_record_id "
        "current_family_revision_id current_family_expected_child_count current_family_seen_child_count "
        "after_child_collection_slot after_child_revision_id after_root_scalar_revision_id "
        "after_root_scalar_field_slot after_child_scalar_revision_id after_child_scalar_field_slot "
        "after_winner_profile_slot after_winner_entity_binding_id after_winner_context_key_sha256 root_count "
        "family_count generation_family_count family_child_count winner_count profile_count root_scalar_count "
        "child_scalar_count source_frozen_at graph_frozen_at output_frozen_at verified_at "
    ).split(),
    "custom_import_query_projection": (
        "family_id generation_id dataset_id definition_revision_id schema_revision_id execution_id "
        "producing_fence producing_token_sha256 projection_contract definition_sha256 capability_sha256 phase "
        "after_entity_binding_id after_selected_value after_child_key_sha256 index_count root_count "
        "child_count verified_root_count verified_child_count digested_root_count digested_child_count "
        "digest_chain_sha256 digest_after_entity digest_after_selected_value digest_after_child_key "
        "projection_sha256 complete_at created_at "
    ).split(),
}
_CHECKPOINT_FIELDS = (
    "checkpoint",
    "arm_ordinal",
    "next_ordinal",
    "root_after_id",
    "root_eof",
    "outcome",
    "page_root_count",
    "range_sha256",
    "state",
    "failure_code",
    "counts",
)


class PostSealAuditRefused(ValueError):
    """No complete current structural authorization can be established."""


@dataclass(frozen=True)
class PostSealAuditProgress:
    audit_id: uuid.UUID
    checkpoint: int
    next_ordinal: int
    root_after_id: int | None
    state: str
    failure_code: str | None
    replayed: bool = False
    sampling: dict | None = None


@dataclass
class _AuditContext:
    family: object
    build: object
    generation: object
    proof: object
    seal: object
    quoted: str
    base_family: object
    identity: dict
    tables: tuple


def _plain(document):
    return json.loads(json.dumps(document, default=publication._json_value, allow_nan=False))


def _digest(document):
    return hashlib.sha256(CONTRACT.encode() + b"\0" + canonical_json(_plain(document)).encode()).digest()


def _root_range_digest(roots):
    """Hash a bounded ordered integer page with the existing canonical bytes."""
    if (
        type(roots) is not list
        or len(roots) > ROOT_PAGE_ROWS
        or any(type(root) is not int or root <= 0 for root in roots)
        or roots != sorted(set(roots))
    ):
        raise PostSealAuditRefused("post-seal root range did not advance completely")
    encoded = json.dumps(roots, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(CONTRACT.encode() + b"\0" + encoded.encode()).digest()


@lru_cache(maxsize=1)
def _sql_resource():
    raw = Path(__file__).with_name("post_seal_audit_sql.json").read_bytes()
    document = json.loads(raw)
    if (
        document.get("contract") != "custom-import/post-seal-structure-sql/v1"
        or document.get("counts_checkpoint") != 62
    ):
        raise PostSealAuditRefused("post-seal SQL contract is unsupported")
    if [arm["ordinal"] for arm in document["arms"]] != list(range(1, 62)):
        raise PostSealAuditRefused("post-seal SQL arm coverage is incomplete")
    for arm in document["arms"]:
        if hashlib.sha256(arm["sql"].encode()).hexdigest() != arm["sha256"]:
            raise PostSealAuditRefused("post-seal SQL arm digest differs")
        if arm["root_range"] != (arm["ordinal"] in (41, 42)) or arm["legacy_only"] != (arm["ordinal"] == 61):
            raise PostSealAuditRefused("post-seal SQL applicability differs")
    return document, hashlib.sha256(raw).digest()


def _logical_row_document(table, alias):
    fields = _CONTROL_FIELDS[table]
    return "jsonb_build_object(" + ",".join(f"'{name}',{alias}.{name}" for name in fields) + ")"


async def _row_digests(session, quoted, generation, build_id=None):
    keys = (
        ("custom_import_generation", "generation_id", generation.generation_id),
        ("custom_import_generation_seal", "generation_id", generation.generation_id),
        ("custom_import_execution", "execution_id", generation.execution_id),
        ("custom_import_capture_bundle", "capture_bundle_id", generation.capture_bundle_id),
        ("custom_import_definition_revision", "definition_revision_id", generation.definition_revision_id),
        ("custom_import_schema_revision", "schema_revision_id", generation.schema_revision_id),
    )
    if build_id is not None:
        keys += (
            ("custom_import_build_attempt", "build_id", build_id),
            ("custom_import_build_verification", "build_id", build_id),
        )
    projection = await session.scalar(
        select(CustomImportQueryProjection).where(CustomImportQueryProjection.generation_id == generation.generation_id)
    )
    if projection is not None:
        keys += (("custom_import_query_projection", "family_id", projection.family_id),)
    branches, parameters = [], {}
    for index, (table, column, identifier) in enumerate(keys):
        branches.append(
            f"SELECT '{table}' name,encode(sha256(convert_to({_logical_row_document(table, 'r')}::text,'UTF8')),'hex') digest FROM {quoted}.{table} r WHERE {column}=:id_{index}"
        )
        parameters[f"id_{index}"] = identifier
    control_digests = (await session.execute(text(" UNION ALL ".join(branches)), parameters)).all()
    if len(control_digests) != len(keys):
        raise PostSealAuditRefused("post-seal immutable control identity is incomplete")
    digests_by_table = {digest_record.name: digest_record.digest for digest_record in control_digests}
    captures = (
        await session.execute(
            text(f"""SELECT stream_slot,
        encode(sha256(convert_to({_logical_row_document("custom_import_capture", "c")}::text,'UTF8')),'hex') digest
        FROM {quoted}.custom_import_capture c WHERE capture_bundle_id=:bundle ORDER BY stream_slot"""),
            {"bundle": generation.capture_bundle_id},
        )
    ).all()
    digests_by_table["capture_streams"] = _digest(
        {str(digest_record.stream_slot): digest_record.digest for digest_record in captures}
    ).hex()
    return digests_by_table


async def _base_context(session, family, generation):
    if generation.base_generation_id is None:
        return None, None
    base = await session.get(CustomImportGeneration, generation.base_generation_id)
    if base is None or base.dataset_id != family.dataset_id:
        raise PostSealAuditRefused("post-seal retained base identity differs")
    await publication._validated_generation_seal(session, base)
    base_family = await session.scalar(
        select(CustomImportSnapshotFamily).where(CustomImportSnapshotFamily.generation_id == base.generation_id)
    )
    if base_family is None or base_family.frozen_at is None:
        raise PostSealAuditRefused("post-seal audit requires a registered frozen retained base")
    return base_family, base


async def _leaf_tables(session, family):
    if family is None:
        return ()
    names = [model.__tablename__ for model in SNAPSHOT_MODELS]
    names.extend(await retirement._optional_relation_names(session, family.family_id))
    return tuple(f'"{snapshot_schema(family.family_id)}"."{name}"' for name in names)


async def _projection_tables(session, family):
    if family is None:
        return ()
    manifest = await session.scalar(
        select(CustomImportQueryProjection).where(CustomImportQueryProjection.generation_id == family.generation_id)
    )
    if manifest is None:
        return ()
    if manifest.family_id != family.family_id:
        raise PostSealAuditRefused("post-seal projection family identity differs")
    return tuple(
        f'"{snapshot_schema(family.family_id)}"."custom_import_derived_{scope}"' for scope in ("root", "child")
    )


async def _context(session, family_id, engine_id):
    snapshot_schema(family_id)
    retirement.lifecycle_lock_key(engine_id, 1)
    try:
        _schema, quoted = await retirement._owner_schema(session)
        family, tombstone = await retirement._locked_family(session, family_id, engine_id)
    except retirement.SnapshotRetirementRefused as error:
        raise PostSealAuditRefused("post-seal audit requires protected owner authority") from error
    if family is None or tombstone is not None:
        raise PostSealAuditRefused("post-seal audit storage is retired")
    build, generation, proof, seal = await build_scratch._eligible(session, family, require_automated=False)
    base_family, base = await _base_context(session, family, generation)
    candidate_tables = await _leaf_tables(session, family)
    base_tables = await _leaf_tables(session, base_family)
    tables = candidate_tables + base_tables
    tables += await _projection_tables(session, family) + await _projection_tables(session, base_family)
    await session.execute(text("LOCK TABLE " + ",".join(tables) + " IN SHARE MODE NOWAIT"))
    source_digest = await publication._validated_source_bundle_digest(session, generation)
    identity_by_name = {
        "candidate": _plain(build_scratch._affinity(family, build, generation, proof, seal)),
        "candidate_rows": await _row_digests(session, quoted, generation, build.build_id),
        "candidate_relations": candidate_tables,
        "source_digest": source_digest.hex(),
        "counts": {name: getattr(proof, name) for name in build_scratch._COUNT_NAMES},
        "base": None
        if base is None
        else {
            "family_id": base_family.family_id,
            "source_digest": (await publication._validated_source_bundle_digest(session, base)).hex(),
            "rows": await _row_digests(session, quoted, base),
            "relations": base_tables,
        },
    }
    return _AuditContext(family, build, generation, proof, seal, quoted, base_family, identity_by_name, tables)


async def _validate_indexes(session, context):
    indexes = (
        (
            await session.execute(
                text("""
        SELECT n.nspname namespace,c.relname name,heap.relname table_name,
          c.relpersistence='p' persistent,am.amname,
          i.indisvalid AND i.indisready AND i.indislive live,i.indisunique unique_index,
          ARRAY(SELECT pg_get_indexdef(c.oid,k,false) FROM generate_series(1,i.indnatts) k) keys,
          regexp_replace(coalesce(pg_get_expr(i.indpred,i.indrelid),''),'[()[:space:]]|::text','','g') predicate,
          ARRAY(SELECT unnest(i.indoption::smallint[])) options
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_class heap ON heap.oid=i.indrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am am ON am.oid=c.relam
        WHERE i.indrelid=ANY(CAST(:relations AS regclass[])) ORDER BY n.nspname,c.relname
    """),
                {"relations": list(context.tables)},
            )
        )
        .mappings()
        .all()
    )
    by_name = {(index_record["namespace"], index_record["name"]): index_record for index_record in indexes}
    for spec in _sql_resource()[0]["indexes"]:
        actual = by_name.get((snapshot_schema(context.family.family_id), spec["name"]))
        expected_keys = [re.sub(r" DESC$", "", key) for key in spec["keys"]]
        expected_options = [3 if key.endswith(" DESC") else 0 for key in spec["keys"]]
        if (
            actual is None
            or actual["table_name"] != spec["table"]
            or actual["keys"] != expected_keys
            or actual["options"] != expected_options
        ):
            raise PostSealAuditRefused("post-seal phase or serving index differs")
        if (
            actual["predicate"] != spec["predicate"]
            or actual["amname"] != "btree"
            or actual["unique_index"]
            or not all(actual[k] for k in ("live", "persistent"))
        ):
            raise PostSealAuditRefused("post-seal phase or serving index is unavailable")


def _sampling_plan(identity):
    # The frozen candidate fixes the draw; restarting cannot choose easier checks.
    seed = hashlib.sha256(
        SAMPLED_CONTRACT.encode() + b"\0" + canonical_json(_plain(identity["candidate"])).encode()
    ).digest()
    population = (*range(1, 61), 62)  # Arm 61 is inapplicable to these sealed builds.
    ranked = sorted(population, key=lambda ordinal: hashlib.sha256(seed + ordinal.to_bytes(2, "big")).digest())
    return {
        "contract": SAMPLED_CONTRACT,
        "percent": 20,
        "population_count": len(population),
        "seed": seed.hex(),
        "selected_ordinals": sorted(ranked[: (len(population) + 4) // 5]),
    }


def _selected_ordinals(binding):
    if "sampling" not in binding:
        return None
    plan = binding["sampling"]
    try:
        is_valid = type(plan) is dict and canonical_json(plan) == canonical_json(_sampling_plan(binding["identity"]))
    except ValueError:
        is_valid = False
    if not is_valid:
        raise PostSealAuditRefused("post-seal audit sample plan differs")
    return frozenset(plan["selected_ordinals"])


async def _check_known_failures(session, family_id, binding):
    selected = _selected_ordinals(binding)
    if selected is None:
        return
    omitted_failure = await session.scalar(
        select(Checkpoint.arm_ordinal)
        .join(Audit, Audit.audit_id == Checkpoint.audit_id)
        .where(
            Audit.family_id == family_id,
            Audit.binding["identity"]["candidate"] == binding["identity"]["candidate"],
            Checkpoint.state == "failed",
            Checkpoint.arm_ordinal.not_in(selected),
        )
        .limit(1)
    )
    if omitted_failure is not None:
        raise PostSealAuditRefused("post-seal sampled audit cannot omit a known failed check")


async def _binding(session, context, *, sample_percent=None):
    await _validate_indexes(session, context)
    binding = _plain(
        {
            "contract": CONTRACT,
            "identity": context.identity,
            "root_page_rows": ROOT_PAGE_ROWS,
        }
    )
    if sample_percent == 20:
        binding["sampling"] = _sampling_plan(context.identity)
    return binding


def _checkpoint_digest(audit, values_by_name, previous):
    return _digest(
        {
            "audit_id": str(audit.audit_id),
            "sql_sha256": bytes(audit.sql_sha256).hex(),
            "binding_sha256": bytes(audit.binding_sha256).hex(),
            "previous_sha256": previous.hex(),
            "checkpoint": values_by_name,
        }
    )


async def _append(session, audit, previous, **values_by_name):
    before = _ZERO if previous is None else bytes(previous.checkpoint_sha256)
    checkpoint = Checkpoint(
        audit_id=audit.audit_id,
        **values_by_name,
        previous_sha256=before,
        checkpoint_sha256=_checkpoint_digest(audit, values_by_name, before),
    )
    session.add(checkpoint)
    await session.flush()
    return checkpoint


def _progress(checkpoint, *, replayed=False, sampling=None):
    return PostSealAuditProgress(
        checkpoint.audit_id,
        checkpoint.checkpoint,
        checkpoint.next_ordinal,
        checkpoint.root_after_id,
        checkpoint.state,
        checkpoint.failure_code,
        replayed,
        sampling,
    )


async def _latest(session, audit_id):
    return await session.scalar(
        select(Checkpoint)
        .where(Checkpoint.audit_id == audit_id)
        .order_by(Checkpoint.checkpoint.desc())
        .limit(1)
        .execution_options(populate_existing=True)
    )


async def _current_binding(session, audit, context):
    if bytes(audit.sql_sha256) != _sql_resource()[1]:
        raise PostSealAuditRefused("post-seal audit SQL changed; start a new attempt")
    selected = _selected_ordinals(audit.binding)
    binding = await _binding(session, context, sample_percent=20 if selected is not None else None)
    if _digest(binding) != bytes(audit.binding_sha256) or binding != audit.binding:
        raise PostSealAuditRefused("post-seal audit binding changed; start a new attempt")


async def _begin(session, family_id, engine_id, restart, sample_percent):
    context = await _context(session, family_id, engine_id)
    previous = await session.scalar(
        select(Audit)
        .where(Audit.family_id == family_id)
        .order_by(Audit.started_at.desc(), Audit.audit_id.desc())
        .limit(1)
    )
    if previous is not None and not restart:
        if previous.engine_id != engine_id:
            raise PostSealAuditRefused("post-seal audit engine identity differs")
        previous_percent = 20 if _selected_ordinals(previous.binding) is not None else 100
        if sample_percent is not None and sample_percent != previous_percent:
            raise PostSealAuditRefused("post-seal audit sample changed; start a new attempt")
        await _current_binding(session, previous, context)
        await _check_known_failures(session, family_id, previous.binding)
        checkpoint = await _latest(session, previous.audit_id)
        if checkpoint is None or checkpoint.state == "failed":
            raise PostSealAuditRefused("post-seal audit failed; start a new attempt explicitly")
        return _progress(checkpoint, replayed=True, sampling=previous.binding.get("sampling"))
    binding = await _binding(session, context, sample_percent=sample_percent)
    await _check_known_failures(session, family_id, binding)
    audit = Audit(
        family_id=family_id,
        engine_id=engine_id,
        build_id=context.build.build_id,
        generation_id=context.generation.generation_id,
        dataset_id=context.family.dataset_id,
        sql_sha256=_sql_resource()[1],
        binding_sha256=_digest(binding),
        binding=binding,
    )
    session.add(audit)
    await session.flush()
    initial = await _append(
        session,
        audit,
        None,
        checkpoint=0,
        arm_ordinal=0,
        next_ordinal=1,
        root_after_id=None,
        root_eof=True,
        outcome="started",
        page_root_count=0,
        range_sha256=None,
        state="scanning",
        failure_code=None,
        counts=None,
    )
    return _progress(initial, sampling=audit.binding.get("sampling"))


async def begin_post_seal_audit(session, family_id, *, engine_id, restart=False, sample_percent=None):
    """Resume its recorded scope; new attempts default to full unless 20% is explicit."""
    if type(restart) is not bool:
        raise PostSealAuditRefused("post-seal restart flag is invalid")
    if sample_percent is not None and (type(sample_percent) is not int or sample_percent not in (20, 100)):
        raise PostSealAuditRefused("post-seal audit sample percent is invalid")
    async with asyncio.timeout(MAX_STEP_SECONDS):
        return await _begin(session, family_id, engine_id, restart, sample_percent)


def _compiled(sql, context):
    replacements_by_token = {
        "__CONTROL__": context.quoted,
        "__LEAF__": '"' + snapshot_schema(context.family.family_id) + '"',
        "__BASE_LEAF__": context.quoted
        if context.base_family is None
        else '"' + snapshot_schema(context.base_family.family_id) + '"',
    }
    for token, identifier in replacements_by_token.items():
        sql = sql.replace(token, identifier)
    if re.search(r"__[A-Z_]+__", sql):
        raise PostSealAuditRefused("post-seal SQL relation binding is incomplete")
    return text(
        re.sub(
            r"\$(\d+)",
            lambda match: (
                "CAST(:p_"
                + match[1]
                + " AS "
                + ("bytea" if match[1] == "8" else "bigint[]" if match[1] == "10" else "bigint")
                + ")"
            ),
            sql,
        )
    )


def _parameters(context):
    names = (
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "capture_bundle_id",
        "producing_fence",
        "producing_token_sha256",
        "base_generation_id",
    )
    return {f"p_{index}": getattr(context.generation, name) for index, name in enumerate(names, 1)} | {
        "p_11": context.build.build_id
    }


async def _complete_counts(session, context, values_by_name, resource):
    """Finish only when the actual final population equals the frozen audit identity."""
    counts = await session.scalar(
        _compiled(resource["counts_sql"], context),
        {"p_1": context.generation.generation_id, "p_2": context.generation.definition_revision_id},
    )
    if counts != context.identity["counts"]:
        return values_by_name | {
            "next_ordinal": values_by_name["arm_ordinal"],
            "state": "failed",
            "outcome": "failed",
            "failure_code": "count_mismatch",
        }
    return values_by_name | {"state": "complete", "outcome": "counts", "counts": counts}


async def _membership_violation(session, context, arm, parameters):
    # A failed statement rolls back SET LOCAL with the savepoint. On success,
    # restore the caller's exact effective setting before later binding SQL.
    async with session.begin_nested():
        previous_nestloop = await session.scalar(text("SELECT current_setting('enable_nestloop')"))
        await session.execute(text("SELECT set_config('enable_nestloop','off',true)"))
        violation = (await session.execute(_compiled(arm["sql"], context), parameters)).first()
        await session.execute(
            text("SELECT set_config('enable_nestloop',:previous_nestloop,true)"),
            {"previous_nestloop": previous_nestloop},
        )
    return violation


def _next_checkpoint(previous):
    ordinal = previous.next_ordinal
    return {
        "checkpoint": previous.checkpoint + 1,
        "arm_ordinal": ordinal,
        "next_ordinal": ordinal + 1,
        "root_after_id": None,
        "root_eof": True,
        "outcome": "passed",
        "page_root_count": 0,
        "range_sha256": None,
        "state": "scanning",
        "failure_code": None,
        "counts": None,
    }


async def _run_arm(session, context, previous):
    """Advance exactly one bounded invariant or root page from its retained checkpoint."""
    ordinal = previous.next_ordinal
    values_by_name = _next_checkpoint(previous)
    resource = _sql_resource()[0]
    if ordinal == 62:
        return await _complete_counts(session, context, values_by_name, resource)
    arm = resource["arms"][ordinal - 1]
    if arm["legacy_only"]:
        return values_by_name | {"outcome": "not_applicable"}
    parameters = _parameters(context)
    if arm["root_range"]:
        roots = await session.scalar(
            _compiled(resource["root_page_sql"], context),
            {"p_1": context.build.build_id, "p_2": previous.root_after_id, "p_3": ROOT_PAGE_ROWS},
        )
        if not roots:
            return values_by_name | {"outcome": "range_eof", "range_sha256": _digest([])}
        range_digest = _root_range_digest(roots)
        if previous.root_after_id is not None and roots[0] <= previous.root_after_id:
            raise PostSealAuditRefused("post-seal root range did not advance completely")
        parameters["p_10"] = roots
        values_by_name.update(
            next_ordinal=ordinal,
            root_after_id=roots[-1],
            root_eof=False,
            outcome="range_page",
            page_root_count=len(roots),
            range_sha256=range_digest,
        )
    if ordinal == 21:
        violation = await _membership_violation(session, context, arm, parameters)
    else:
        violation = (await session.execute(_compiled(arm["sql"], context), parameters)).first()
    if violation is not None:
        return values_by_name | {
            "state": "failed",
            "outcome": "failed",
            "next_ordinal": ordinal,
            "failure_code": "invariant_violation",
        }
    return values_by_name


async def _advance(session, audit_id, engine_id, expected_checkpoint):
    audit = await session.get(Audit, audit_id)
    if audit is None or audit.engine_id != engine_id:
        raise PostSealAuditRefused("post-seal audit identity differs")
    context = await _context(session, audit.family_id, engine_id)
    await session.refresh(audit, with_for_update=True)
    await _current_binding(session, audit, context)
    previous = await _latest(session, audit_id)
    if previous is None or expected_checkpoint > previous.checkpoint or previous.state == "failed":
        raise PostSealAuditRefused("post-seal audit checkpoint is unavailable")
    if expected_checkpoint < previous.checkpoint:
        replay = await session.get(Checkpoint, (audit_id, expected_checkpoint + 1))
        if replay is None:
            raise PostSealAuditRefused("post-seal audit replay checkpoint is missing")
        return _progress(replay, replayed=True, sampling=audit.binding.get("sampling"))
    if previous.state == "complete":
        return _progress(previous, replayed=True, sampling=audit.binding.get("sampling"))
    selected = _selected_ordinals(audit.binding)
    if selected is not None and previous.next_ordinal not in selected:
        values_by_name = _next_checkpoint(previous) | {"outcome": "not_applicable"}
        if previous.next_ordinal == 62:
            values_by_name.update(state="complete", counts={})
    else:
        values_by_name = await _run_arm(session, context, previous)
    await _current_binding(session, audit, await _context(session, audit.family_id, engine_id))
    if values_by_name["state"] == "complete":
        await _validate_chain(session, audit, require_complete=False)
    return _progress(await _append(session, audit, previous, **values_by_name), sampling=audit.binding.get("sampling"))


async def advance_post_seal_audit(session, audit_id, *, engine_id, expected_checkpoint):
    """Commit one complete invariant/page; timeout/interruption leaves its cursor unchanged."""
    if type(expected_checkpoint) is not int or expected_checkpoint < 0:
        raise PostSealAuditRefused("post-seal expected checkpoint is invalid")
    try:
        audit_id = uuid.UUID(str(audit_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise PostSealAuditRefused("post-seal audit identifier is invalid") from error
    async with asyncio.timeout(MAX_STEP_SECONDS):
        return await _advance(session, audit_id, engine_id, expected_checkpoint)


def _validate_transition(previous, checkpoint, page_rows, selected=None):
    expected = 0 if previous is None else previous.next_ordinal
    if checkpoint.arm_ordinal != expected or checkpoint.state == "failed":
        raise PostSealAuditRefused("post-seal audit did not pass every applicable invariant")
    if selected is not None and expected > 0 and expected not in selected:
        if (
            checkpoint.outcome != "not_applicable"
            or checkpoint.next_ordinal != expected + 1
            or checkpoint.root_after_id is not None
            or not checkpoint.root_eof
            or checkpoint.page_root_count != 0
            or checkpoint.range_sha256 is not None
            or checkpoint.state != ("complete" if expected == 62 else "scanning")
            or checkpoint.counts != ({} if expected == 62 else None)
        ):
            raise PostSealAuditRefused("post-seal audit sampled omission differs")
        return
    if expected in (41, 42):
        if checkpoint.outcome not in {"range_page", "range_eof"}:
            raise PostSealAuditRefused("post-seal range was not checked to EOF")
        if checkpoint.outcome == "range_eof":
            if checkpoint.page_root_count != 0 or not checkpoint.root_eof or checkpoint.range_sha256 != _digest([]):
                raise PostSealAuditRefused("post-seal range EOF is missing")
            return
        if checkpoint.root_eof or not 0 < checkpoint.page_root_count <= page_rows or checkpoint.range_sha256 is None:
            raise PostSealAuditRefused("post-seal range coverage is incomplete")
        if previous.root_after_id is not None and checkpoint.root_after_id <= previous.root_after_id:
            raise PostSealAuditRefused("post-seal range cursor regressed")
    elif expected == 61:
        if checkpoint.outcome != "not_applicable":
            raise PostSealAuditRefused("post-seal invariant was incorrectly skipped")
    elif checkpoint.outcome != ("started" if expected == 0 else "counts" if expected == 62 else "passed"):
        raise PostSealAuditRefused("post-seal invariant outcome differs")


async def _validate_chain(session, audit, *, require_complete=True):
    selected = _selected_ordinals(audit.binding)
    page_rows = audit.binding.get("root_page_rows")
    if type(page_rows) is not int or not 0 < page_rows <= 16_384:
        raise PostSealAuditRefused("post-seal audit page bound is invalid")
    previous, after, expected = None, -1, 0
    while True:
        checkpoints = (
            await session.scalars(
                select(Checkpoint)
                .where(Checkpoint.audit_id == audit.audit_id, Checkpoint.checkpoint > after)
                .order_by(Checkpoint.checkpoint)
                .limit(256)
            )
        ).all()
        if not checkpoints:
            break
        for checkpoint in checkpoints:
            if checkpoint.checkpoint != expected:
                raise PostSealAuditRefused("post-seal audit checkpoint coverage has a gap")
            prior_hash = _ZERO if previous is None else bytes(previous.checkpoint_sha256)
            fields_by_name = {name: getattr(checkpoint, name) for name in _CHECKPOINT_FIELDS}
            if bytes(checkpoint.previous_sha256) != prior_hash or bytes(
                checkpoint.checkpoint_sha256
            ) != _checkpoint_digest(audit, fields_by_name, prior_hash):
                raise PostSealAuditRefused("post-seal audit checkpoint hash differs")
            _validate_transition(previous, checkpoint, page_rows, selected)
            previous, expected = checkpoint, expected + 1
        after = checkpoints[-1].checkpoint
    if previous is None or (require_complete and (previous.state != "complete" or previous.next_ordinal != 63)):
        raise PostSealAuditRefused("post-seal audit is incomplete")
    if previous.state == "complete":
        expected_counts = {} if selected is not None and 62 not in selected else audit.binding["identity"]["counts"]
        if previous.counts != expected_counts:
            raise PostSealAuditRefused("post-seal audit counts differ")
        if any(type(count) is not int or count < 0 for count in previous.counts.values()):
            raise PostSealAuditRefused("post-seal audit count types differ")
    return previous


async def completed_audit_receipt(session, audit_id, *, engine_id, family_id, expected_candidate=None):
    """Validate the immutable historical ledger without requiring a retained base to remain live."""
    await retirement._owner_schema(session)
    audit = await session.get(Audit, uuid.UUID(str(audit_id)))
    if (
        audit is None
        or audit.engine_id != engine_id
        or audit.family_id != family_id
        or _digest(audit.binding) != bytes(audit.binding_sha256)
    ):
        raise PostSealAuditRefused("post-seal completed audit identity differs")
    if expected_candidate is not None and audit.binding["identity"]["candidate"] != _plain(expected_candidate):
        raise PostSealAuditRefused("post-seal completed candidate identity differs")
    checkpoint = await _validate_chain(session, audit)
    receipt_by_name = {
        "contract": CONTRACT,
        "audit_id": str(audit.audit_id),
        "binding_sha256": bytes(audit.binding_sha256).hex(),
        "sql_sha256": bytes(audit.sql_sha256).hex(),
        "checkpoint_sha256": bytes(checkpoint.checkpoint_sha256).hex(),
    }
    if "sampling" in audit.binding:
        receipt_by_name.update(contract=SAMPLED_CONTRACT, sampling=audit.binding["sampling"])
    return receipt_by_name


async def verified_audit_for_trim(session, context, *, engine_id):
    """Require completion of the recorded full or explicitly sampled audit scope."""
    audit = await session.scalar(
        select(Audit)
        .where(Audit.family_id == context.family.family_id, Audit.engine_id == engine_id)
        .order_by(Audit.started_at.desc(), Audit.audit_id.desc())
        .limit(1)
    )
    if audit is None:
        raise PostSealAuditRefused("post-seal completed automated audit is required")
    await _current_binding(session, audit, context)
    await _check_known_failures(session, context.family.family_id, audit.binding)
    return await completed_audit_receipt(
        session, audit.audit_id, engine_id=engine_id, family_id=context.family.family_id
    )


async def audit_context_for_trim(session, family_id, *, engine_id):
    """Share the exact sealed binding and write-closure checks with the cleanup operator."""
    return await _context(session, family_id, engine_id)
