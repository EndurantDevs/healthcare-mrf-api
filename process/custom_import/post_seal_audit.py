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
ROOT_PAGE_ROWS = 16_384
MAX_STEP_SECONDS = 120
_ZERO = b"\0" * 32
_FUNCTIONS = (
    "resolve_custom_import_snapshot_relations(bigint)",
    "resolve_custom_import_generation_snapshot(bigint,bigint,bigint,bigint)",
    "custom_import_snapshot_columns_sha256(bigint)",
    "verify_custom_import_build_structure(bigint)",
    "lock_custom_import_writable_snapshot(bigint,bigint,bytea)",
)
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
            f"SELECT '{table}' name,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') digest FROM {quoted}.{table} r WHERE {column}=:id_{index}"
        )
        parameters[f"id_{index}"] = identifier
    control_digests = (await session.execute(text(" UNION ALL ".join(branches)), parameters)).all()
    if len(control_digests) != len(keys):
        raise PostSealAuditRefused("post-seal immutable control identity is incomplete")
    digests_by_table = {digest_record.name: digest_record.digest for digest_record in control_digests}
    captures = (
        await session.execute(
            text(f"""SELECT stream_slot,
        encode(sha256(convert_to(to_jsonb(c)::text,'UTF8')),'hex') digest
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


def _leaf_tables(family):
    if family is None:
        return ()
    names = [model.__tablename__ for model in SNAPSHOT_MODELS]
    if family.landing_table_oid is not None:
        names.append("source_bulk_landing")
    if family.origin_table_oid is not None:
        names.append("legacy_copy_origin")
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
    tables = _leaf_tables(family) + _leaf_tables(base_family)
    tables += await _projection_tables(session, family) + await _projection_tables(session, base_family)
    await session.execute(text("LOCK TABLE " + ",".join(tables) + " IN SHARE MODE NOWAIT"))
    candidate_manifest = await build_scratch._manifest(session, quoted, family)
    base_manifest = None if base is None else await build_scratch._manifest(session, quoted, base_family)
    source_digest = await publication._validated_source_bundle_digest(session, generation)
    identity_by_name = {
        "candidate": _plain(build_scratch._affinity(family, build, generation, proof, seal)),
        "candidate_rows": await _row_digests(session, quoted, generation, build.build_id),
        "candidate_relations": candidate_manifest,
        "source_digest": source_digest.hex(),
        "counts": {name: getattr(proof, name) for name in build_scratch._COUNT_NAMES},
        "base": None
        if base is None
        else {
            "family_id": base_family.family_id,
            "source_digest": (await publication._validated_source_bundle_digest(session, base)).hex(),
            "rows": await _row_digests(session, quoted, base),
            "relations": base_manifest,
        },
    }
    return _AuditContext(family, build, generation, proof, seal, quoted, base_family, identity_by_name, tables)


async def _heap_fingerprints(session, tables):
    await session.execute(text("SELECT pg_catalog.pg_stat_clear_snapshot()"))
    rows = (
        (
            await session.execute(
                text("""
        SELECT c.oid::bigint oid,c.relowner::bigint owner,c.reltype::bigint row_type,
          c.relfilenode::bigint heap_node,c.reltoastrelid::bigint toast_oid,
          coalesce(t.relfilenode,0)::bigint toast_node,pg_relation_size(c.oid) heap_bytes,
          CASE WHEN t.oid IS NULL THEN 0 ELSE pg_relation_size(t.oid) END toast_bytes,
          pg_stat_get_tuples_inserted(c.oid) inserted,pg_stat_get_tuples_updated(c.oid) updated,
          pg_stat_get_tuples_deleted(c.oid) deleted,
          EXISTS(SELECT 1 FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
            WHERE a.grantee<>c.relowner AND a.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE','TRIGGER')) writable,
          EXISTS(SELECT 1 FROM pg_attribute a,LATERAL aclexplode(a.attacl) acl
            WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND acl.grantee<>c.relowner
              AND acl.privilege_type IN ('INSERT','UPDATE')) column_writable
        FROM pg_class c LEFT JOIN pg_class t ON t.oid=c.reltoastrelid
        WHERE c.oid=ANY(CAST(:relations AS regclass[])) ORDER BY c.oid
    """),
                {"relations": list(tables)},
            )
        )
        .mappings()
        .all()
    )
    if len(rows) != len(tables) or any(row["writable"] or row["column_writable"] for row in rows):
        raise PostSealAuditRefused("post-seal audit leaves have an open direct writer")
    return {str(row["oid"]): dict(row) for row in rows}


async def _index_fingerprints(session, context):
    indexes = (
        (
            await session.execute(
                text("""
        SELECT n.nspname namespace,c.relname name,c.oid::bigint oid,heap.relname table_name,
          c.relowner=heap.relowner owner_matches,c.relpersistence='p' persistent,am.amname,
          i.indisvalid AND i.indisready AND i.indislive live,i.indisunique unique_index,
          ARRAY(SELECT pg_get_indexdef(c.oid,k,false) FROM generate_series(1,i.indnatts) k) keys,
          regexp_replace(coalesce(pg_get_expr(i.indpred,i.indrelid),''),'[()[:space:]]|::text','','g') predicate,
          ARRAY(SELECT unnest(i.indoption::smallint[])) options
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_class heap ON heap.oid=i.indrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am am ON am.oid=c.relam
        WHERE i.indrelid=ANY(CAST(:relations AS regclass[])) ORDER BY c.oid
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
            or not all(actual[k] for k in ("live", "owner_matches", "persistent"))
        ):
            raise PostSealAuditRefused("post-seal phase or serving index is unavailable")
    return {str(index_record["oid"]): dict(index_record) for index_record in indexes}


async def _function_fingerprints(session, quoted):
    identities = [quoted + "." + signature for signature in _FUNCTIONS]
    rows = (
        await session.execute(
            text("""SELECT oid::bigint oid,
        encode(sha256(convert_to(to_jsonb(p)::text,'UTF8')),'hex') digest
        FROM pg_proc p WHERE oid=ANY(ARRAY(SELECT to_regprocedure(signature)
          FROM unnest(CAST(:identities AS text[])) signature)) ORDER BY oid"""),
            {"identities": identities},
        )
    ).all()
    if len(rows) != len(identities):
        raise PostSealAuditRefused("post-seal authority function identity is incomplete")
    return {str(row.oid): row.digest for row in rows}


async def _binding(session, context):
    return _plain(
        {
            "contract": CONTRACT,
            "identity": context.identity,
            "root_page_rows": ROOT_PAGE_ROWS,
            "heaps": await _heap_fingerprints(session, context.tables),
            "indexes": await _index_fingerprints(session, context),
            "functions": await _function_fingerprints(session, context.quoted),
        }
    )


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


def _progress(checkpoint, *, replayed=False):
    return PostSealAuditProgress(
        checkpoint.audit_id,
        checkpoint.checkpoint,
        checkpoint.next_ordinal,
        checkpoint.root_after_id,
        checkpoint.state,
        checkpoint.failure_code,
        replayed,
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
    binding = await _binding(session, context)
    if _digest(binding) != bytes(audit.binding_sha256) or binding != audit.binding:
        raise PostSealAuditRefused("post-seal audit binding changed; start a new attempt")


async def _begin(session, family_id, engine_id, restart):
    context = await _context(session, family_id, engine_id)
    binding = await _binding(session, context)
    previous = await session.scalar(
        select(Audit)
        .where(Audit.family_id == family_id)
        .order_by(Audit.started_at.desc(), Audit.audit_id.desc())
        .limit(1)
    )
    if previous is not None and not restart:
        if previous.engine_id != engine_id:
            raise PostSealAuditRefused("post-seal audit engine identity differs")
        await _current_binding(session, previous, context)
        checkpoint = await _latest(session, previous.audit_id)
        if checkpoint is None or checkpoint.state == "failed":
            raise PostSealAuditRefused("post-seal audit failed; start a new attempt explicitly")
        return _progress(checkpoint, replayed=True)
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
    return _progress(initial)


async def begin_post_seal_audit(session, family_id, *, engine_id, restart=False):
    """Bind an exact frozen attempt; restart never overwrites an earlier prefix."""
    if type(restart) is not bool:
        raise PostSealAuditRefused("post-seal restart flag is invalid")
    async with asyncio.timeout(MAX_STEP_SECONDS):
        return await _begin(session, family_id, engine_id, restart)


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


async def _run_arm(session, context, previous):
    """Advance exactly one bounded invariant or root page from its retained checkpoint."""
    ordinal = previous.next_ordinal
    values_by_name = {
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
        return _progress(replay, replayed=True)
    if previous.state == "complete":
        return _progress(previous, replayed=True)
    values_by_name = await _run_arm(session, context, previous)
    await _current_binding(session, audit, await _context(session, audit.family_id, engine_id))
    if values_by_name["state"] == "complete":
        await _validate_chain(session, audit, require_complete=False)
    return _progress(await _append(session, audit, previous, **values_by_name))


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


def _validate_transition(previous, checkpoint, page_rows):
    expected = 0 if previous is None else previous.next_ordinal
    if checkpoint.arm_ordinal != expected or checkpoint.state == "failed":
        raise PostSealAuditRefused("post-seal audit did not pass every applicable invariant")
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
            _validate_transition(previous, checkpoint, page_rows)
            previous, expected = checkpoint, expected + 1
        after = checkpoints[-1].checkpoint
    if previous is None or (require_complete and (previous.state != "complete" or previous.next_ordinal != 63)):
        raise PostSealAuditRefused("post-seal audit is incomplete")
    if previous.state == "complete" and previous.counts != audit.binding["identity"]["counts"]:
        raise PostSealAuditRefused("post-seal audit counts differ")
    if previous.state == "complete" and any(type(count) is not int or count < 0 for count in previous.counts.values()):
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
    return {
        "contract": CONTRACT,
        "audit_id": str(audit.audit_id),
        "binding_sha256": bytes(audit.binding_sha256).hex(),
        "sql_sha256": bytes(audit.sql_sha256).hex(),
        "checkpoint_sha256": bytes(checkpoint.checkpoint_sha256).hex(),
    }


async def verified_audit_for_trim(session, context, *, engine_id):
    """Authorize first cleanup only from a current complete all-arm audit."""
    audit = await session.scalar(
        select(Audit)
        .where(Audit.family_id == context.family.family_id, Audit.engine_id == engine_id)
        .order_by(Audit.started_at.desc(), Audit.audit_id.desc())
        .limit(1)
    )
    if audit is None:
        raise PostSealAuditRefused("post-seal full automated audit is required")
    await _current_binding(session, audit, context)
    return await completed_audit_receipt(
        session, audit.audit_id, engine_id=engine_id, family_id=context.family.family_id
    )


async def audit_context_for_trim(session, family_id, *, engine_id):
    """Share the exact sealed binding and write-closure checks with the cleanup operator."""
    return await _context(session, family_id, engine_id)
