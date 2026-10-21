# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded, read-only rejection pages for an exact terminal build.

This internal reader does not authorize an owner or authenticate a cursor. Its
caller must do both before exposing a page; an ordinal is only a locator.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace

from sqlalchemy import BigInteger, cast, column, func, literal_column, select, table, text, true
from sqlalchemy.dialects.postgresql import OID
from sqlalchemy.orm import load_only

from db.models.custom_import import CustomImportBuildAttempt as Build
from db.models.custom_import import CustomImportCaptureBundle as Capture
from db.models.custom_import import CustomImportDefinitionRevision as Definition
from db.models.custom_import import CustomImportExecution as Execution
from db.models.custom_import import CustomImportRejection as Rejection
from db.models.custom_import import CustomImportSchemaRevision as Schema
from db.models.custom_import import CustomImportSourceBindingRevision as Binding
from db.models.custom_import_storage import CustomImportSnapshotFamily as Snapshot
from process.custom_import.read_identity import verified_definition
from process.custom_import.runner_codec import digest_text, payload_field_value, root_key_document_from_tuple
from process.custom_import.runner_registry import load_collection_slots
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import SNAPSHOT_MODELS, snapshot_schema, snapshot_tables

MAX_EVENTS = 100
MAX_PAGE_BYTES = 65_536
_IDENTITY = ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id", "capture_bundle_id")
_CODE = re.compile(r"[a-z][a-z0-9_]{0,62}", re.ASCII)
_DIGEST = re.compile(r"[a-f0-9]{64}", re.ASCII)


class RejectionEvidenceUnavailable(ValueError):
    """The exact retained evidence cannot be read safely."""

    def __init__(self):
        super().__init__("retained rejection evidence is unavailable")


class RejectionEvidenceNotFinal(RejectionEvidenceUnavailable):
    """The retained execution has not reached a terminal state."""

    def __init__(self):
        ValueError.__init__(self, "retained rejection evidence is not final")


class RejectionEvidenceNotFound(RejectionEvidenceUnavailable):
    """The trusted execution locator does not identify a retained request."""


@dataclass(frozen=True)
class _Scope:
    dataset_id: int
    schema_revision_id: int
    definition: object = field(repr=False)
    identity: dict
    family_id: int
    producer_fence: int
    producer_digest: bytes = field(repr=False)
    fingerprint: str
    rejection_oid: int | None = None


def _encoded(document):
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _is_positive(value):
    return type(value) is int and 0 < value < 2**63


def _digest(value):
    if not isinstance(value, (bytes, memoryview)) or len(value) != 32:
        raise RejectionEvidenceUnavailable()
    return bytes(value).hex()


def _identity_columns(model, *columns):
    return load_only(*[getattr(model, name) for name in _IDENTITY], *columns)


def _metadata_columns():
    return (
        load_only(
            Definition.definition_revision_id,
            Definition.dataset_id,
            Definition.schema_revision_id,
            Definition.revision_number,
            Definition.contract_version,
            Definition.canonical_definition,
            Definition.definition_sha256,
        ),
        load_only(
            Schema.schema_revision_id,
            Schema.dataset_id,
            Schema.revision_number,
            Schema.canonical_schema,
            Schema.schema_sha256,
        ),
        load_only(
            Binding.source_binding_revision_id,
            Binding.dataset_id,
            Binding.definition_revision_id,
            Binding.schema_revision_id,
            Binding.definition_sha256,
            Binding.schema_sha256,
            Binding.binding_sha256,
        ),
        load_only(
            Capture.capture_bundle_id,
            Capture.dataset_id,
            Capture.definition_revision_id,
            Capture.schema_revision_id,
            Capture.capture_state,
            Capture.sealed_at,
            Capture.manifest_sha256,
        ),
    )


def _scope_statement(dataset_id, execution_id, build_id):
    return (
        select(Execution, Build, Snapshot, Definition, Schema, Binding, Capture)
        .select_from(Execution)
        .join(Build, Build.execution_id == Execution.execution_id)
        .join(
            Snapshot,
            (Snapshot.execution_id == Execution.execution_id) & (Snapshot.producing_fence == Build.producing_fence),
        )
        .join(Definition, Definition.definition_revision_id == Execution.definition_revision_id)
        .join(Schema, Schema.schema_revision_id == Execution.schema_revision_id)
        .outerjoin(Binding, Binding.source_binding_revision_id == Execution.source_binding_revision_id)
        .join(Capture, Capture.capture_bundle_id == Execution.capture_bundle_id)
        .where(Execution.dataset_id == dataset_id, Execution.execution_id == execution_id, Build.build_id == build_id)
        .options(
            _identity_columns(
                Execution,
                Execution.state,
                Execution.finished_at,
                Execution.request_identity_sha256,
                Execution.source_binding_revision_id,
            ),
            _identity_columns(
                Build,
                Build.build_id,
                Build.generation_id,
                Build.producing_fence,
                Build.producing_token_sha256,
                Build.request_identity_sha256,
            ),
            _identity_columns(
                Snapshot,
                Snapshot.family_id,
                Snapshot.generation_id,
                Snapshot.producing_fence,
                Snapshot.producing_token_sha256,
            ),
            *_metadata_columns(),
        )
        .limit(2)
        .execution_options(autoflush=False, populate_existing=True)
    )


def _checked_scope(scope_rows, dataset_id, execution_id, build_id):
    if len(scope_rows) != 1:
        raise RejectionEvidenceUnavailable()
    execution, build, snapshot, revision, schema, binding, capture = scope_rows[0]
    if execution.dataset_id != dataset_id or execution.execution_id != execution_id or build.build_id != build_id:
        raise RejectionEvidenceUnavailable()
    if execution.state not in {"failed", "canceled", "completed", "no_change"}:
        raise RejectionEvidenceNotFinal()
    if execution.finished_at is None or execution.finished_at.utcoffset() is None:
        raise RejectionEvidenceUnavailable()
    for name in _IDENTITY:
        expected = getattr(execution, name)
        if not _is_positive(expected) or any(getattr(pinned, name) != expected for pinned in (build, snapshot)):
            raise RejectionEvidenceUnavailable()
    if (
        not _is_positive(snapshot.family_id)
        or not _is_positive(build.producing_fence)
        or snapshot.producing_fence != build.producing_fence
        or snapshot.producing_token_sha256 != build.producing_token_sha256
        or snapshot.generation_id != build.generation_id
        or (build.generation_id is not None and not _is_positive(build.generation_id))
        or build.request_identity_sha256 != execution.request_identity_sha256
        or revision.dataset_id != dataset_id
        or schema.dataset_id != dataset_id
        or revision.schema_revision_id != execution.schema_revision_id
    ):
        raise RejectionEvidenceUnavailable()
    definition = verified_definition(revision, schema)
    identity_dict = {name: str(getattr(execution, name)) for name in _IDENTITY}
    identity_dict.update(
        build_id=str(build_id),
        generation_id=None if build.generation_id is None else str(build.generation_id),
        definition_sha256=definition.digest,
        schema_sha256=definition.schema_digest,
        state=execution.state,
        finished_at=execution.finished_at.isoformat(),
        **_retained_inputs(execution, definition, binding, capture),
    )
    private_identity_dict = dict(
        identity_dict,
        family_id=snapshot.family_id,
        producer_fence=build.producing_fence,
        producer_digest=_digest(build.producing_token_sha256),
        request_digest=(
            None if execution.request_identity_sha256 is None else _digest(execution.request_identity_sha256)
        ),
    )
    fingerprint = hashlib.sha256(b"custom-import/rejection-scope/v1\x00" + _encoded(private_identity_dict)).hexdigest()
    return _Scope(
        dataset_id,
        execution.schema_revision_id,
        definition,
        identity_dict,
        snapshot.family_id,
        build.producing_fence,
        bytes(build.producing_token_sha256),
        fingerprint,
    )


def _retained_inputs(execution, definition, binding, capture):
    if (
        capture.capture_bundle_id != execution.capture_bundle_id
        or capture.capture_state != "sealed"
        or capture.sealed_at is None
        or any(getattr(capture, name) != getattr(execution, name) for name in _IDENTITY[:3])
    ):
        raise RejectionEvidenceUnavailable()
    if execution.source_binding_revision_id is None:
        if binding is not None:
            raise RejectionEvidenceUnavailable()
        binding_id = binding_digest = None
    else:
        if (
            binding is None
            or execution.request_identity_sha256 is None
            or binding.source_binding_revision_id != execution.source_binding_revision_id
            or any(getattr(binding, name) != getattr(execution, name) for name in _IDENTITY[:3])
            or _digest(binding.definition_sha256) != definition.digest
            or _digest(binding.schema_sha256) != definition.schema_digest
        ):
            raise RejectionEvidenceUnavailable()
        binding_id, binding_digest = str(binding.source_binding_revision_id), _digest(binding.binding_sha256)
    return {
        "capture_manifest_sha256": _digest(capture.manifest_sha256),
        "source_binding_revision_id": binding_id,
        "source_binding_sha256": binding_digest,
    }


async def _lock_snapshot(session, family_id):
    connection = await session.connection()
    model_schema = Snapshot.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = schema_map.get(model_schema, model_schema)
    if not schema:
        raise RejectionEvidenceUnavailable()
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    await session.execute(
        text(f"SELECT {quoted}.lock_custom_import_materialization_storage(CAST(:family_id AS bigint), false)"),
        {"family_id": family_id},
    )
    return await _verify_relation_locks(session, quoted, family_id)


async def _verify_relation_locks(session, quoted_schema, family_id):
    # Repeatable-read catalog rows can predate a committed name swap; locks cannot.
    relations = (
        await session.execute(
            text(f"""
                SELECT r.relation_slot, r.table_oid,
                    pg_catalog.to_regclass(pg_catalog.format('%I.%I', CAST(:namespace AS text),
                        (CAST(:names AS text[]))[r.relation_slot]))::oid::bigint AS current_oid,
                    EXISTS (
                        SELECT 1 FROM pg_catalog.pg_locks held
                        WHERE held.pid=pg_catalog.pg_backend_pid() AND held.locktype='relation'
                            AND held.database=(SELECT oid FROM pg_catalog.pg_database
                                WHERE datname=pg_catalog.current_database())
                            AND held.relation=r.table_oid::oid
                            AND held.mode='AccessShareLock' AND held.granted
                    ) AS is_locked
                FROM {quoted_schema}.custom_import_snapshot_relation r
                WHERE r.family_id=:family_id ORDER BY r.relation_slot LIMIT :limit
                """),
            {
                "family_id": family_id,
                "namespace": snapshot_schema(family_id),
                "names": [model.__tablename__ for model in SNAPSHOT_MODELS],
                "limit": len(SNAPSHOT_MODELS) + 1,
            },
        )
    ).all()
    if [relation.relation_slot for relation in relations] != list(range(1, len(SNAPSHOT_MODELS) + 1)) or any(
        not _is_positive(relation.table_oid)
        or relation.current_oid != relation.table_oid
        or relation.is_locked is not True
        for relation in relations
    ):
        raise RejectionEvidenceUnavailable()
    return relations[SNAPSHOT_MODELS.index(Rejection)].table_oid


async def _resolve_scope(session, dataset_id, execution_id, build_id):
    statement = _scope_statement(dataset_id, execution_id, build_id)
    scope = _checked_scope((await session.execute(statement)).all(), dataset_id, execution_id, build_id)
    rejection_oid = await _lock_snapshot(session, scope.family_id)
    checked = _checked_scope((await session.execute(statement)).all(), dataset_id, execution_id, build_id)
    if checked.fingerprint != scope.fingerprint:
        raise RejectionEvidenceUnavailable()
    return replace(checked, rejection_oid=rejection_oid)


def _logical_key(definition, rejection):
    if rejection.canonical_root_key is None:
        return None, "not_retained"
    try:
        document = json.loads(rejection.canonical_root_key)
        fields = document["fields"]
        expected = definition.root_logical_key
        if (
            document.keys() != {"contract", "fields"}
            or document["contract"] != "custom-import-key/v1"
            or len(fields) != len(expected)
        ):
            raise ValueError
        key_parts = tuple(
            payload_field_value(definition.fields_by_id[name], encoded_field, "rejection key")
            for name, encoded_field in zip(expected, fields, strict=True)
        )
        canonical = root_key_document_from_tuple(definition, key_parts)
        if canonical != rejection.canonical_root_key or digest_text("root-key", canonical) != rejection.root_key_sha256:
            raise ValueError
        return {
            name: {
                "type": encoded_field["value"]["type"],
                "value": str(encoded_field["value"]["value"])
                if encoded_field["value"]["type"] == "integer"
                else encoded_field["value"]["value"],
            }
            for name, encoded_field in zip(expected, fields, strict=True)
        }, "retained"
    except KeyError, TypeError, ValueError, OverflowError, AttributeError, CandidateRunnerError:
        return None, "invalid"


def _event(scope, collections, rejection):
    if (
        not _is_positive(rejection.rejection_id)
        or type(rejection.code) is not str
        or _CODE.fullmatch(rejection.code) is None
    ):
        raise RejectionEvidenceUnavailable()
    if type(rejection.rejection_ordinal) is not int or not 0 <= rejection.rejection_ordinal < 2**63:
        raise RejectionEvidenceUnavailable()
    if any(
        slot is not None and (type(slot) is not int or not 0 <= slot < 32768)
        for slot in (rejection.collection_slot, rejection.field_slot)
    ):
        raise RejectionEvidenceUnavailable()
    collection = None if rejection.collection_slot in (None, 0) else collections.get(rejection.collection_slot)
    if rejection.collection_slot not in (None, 0) and collection is None:
        raise RejectionEvidenceUnavailable()
    declared = {field.field_slot: field for field in scope.definition.fields}.get(rejection.field_slot)
    if rejection.field_slot is not None and (declared is None or declared.collection != collection):
        raise RejectionEvidenceUnavailable()
    if rejection.source_ordinal is not None and (
        type(rejection.source_ordinal) is not int or not 0 <= rejection.source_ordinal < 2**63
    ):
        raise RejectionEvidenceUnavailable()
    key, key_status = _logical_key(scope.definition, rejection)
    return {
        "rejection_id": str(rejection.rejection_id),
        "ordinal": str(rejection.rejection_ordinal),
        "code": rejection.code,
        "collection": collection,
        "field_id": None if declared is None else declared.field_id,
        "source_ordinal": None if rejection.source_ordinal is None else str(rejection.source_ordinal),
        "logical_key": key,
        "logical_key_status": key_status,
        "offending_value_status": "not_retained",
    }


def _predicate(model, scope):
    return (
        model.execution_id == int(scope.identity["execution_id"]),
        model.producing_fence == scope.producer_fence,
        model.producing_token_sha256 == scope.producer_digest,
        model.dataset_id == scope.dataset_id,
        model.definition_revision_id == int(scope.identity["definition_revision_id"]),
        model.schema_revision_id == scope.schema_revision_id,
    )


def _source_relation_oid(whole_row):
    types = table("pg_type", column("oid"), column("typrelid"), schema="pg_catalog")
    return (
        select(cast(types.c.typrelid, BigInteger))
        .where(types.c.oid == cast(func.pg_catalog.pg_typeof(whole_row), OID))
        .correlate_except(types)
        .scalar_subquery()
    )


async def _page_headers(session, scope, leaf, after, limit):
    model = leaf.c
    page = (
        select(
            leaf.table_valued().label("origin"),
            model.rejection_ordinal,
            func.coalesce(func.octet_length(model.canonical_root_key), 0).label("key_bytes"),
        )
        .where(*_predicate(model, scope), model.rejection_ordinal > after)
        .order_by(model.rejection_ordinal)
        .limit(limit + 1)
        .lateral("header_page")
    )
    singleton = select(literal_column("1").label("anchor")).subquery("singleton")
    headers = (
        await session.execute(
            select(
                page.c.rejection_ordinal,
                page.c.key_bytes,
                _source_relation_oid(page.c.origin).label("source_oid"),
            )
            .select_from(singleton.outerjoin(page, true()))
            .order_by(page.c.rejection_ordinal)
        )
    ).all()
    # A schema rename can redirect names despite table locks. The actual row
    # type, including the typed NULL for an empty page, binds this statement.
    if not headers or any(header.source_oid != scope.rejection_oid for header in headers):
        raise RejectionEvidenceUnavailable()
    return [] if len(headers) == 1 and headers[0].rejection_ordinal is None else headers


async def _page_rows(session, scope, after, limit, page_bytes=MAX_PAGE_BYTES):
    leaf = snapshot_tables(scope.family_id)[Rejection.__tablename__].alias("retained_rejection")
    model = leaf.c
    headers = await _page_headers(session, scope, leaf, after, limit)
    ordinals, retained_bytes = [], 0
    for header in headers[:limit]:
        if type(header.key_bytes) is not int or header.key_bytes < 0:
            raise RejectionEvidenceUnavailable()
        if retained_bytes + header.key_bytes > page_bytes:
            if not ordinals:
                raise RejectionEvidenceUnavailable()
            break
        ordinals.append(header.rejection_ordinal)
        retained_bytes += header.key_bytes
    if not ordinals:
        return [], not headers
    rejections = (
        await session.execute(
            select(
                model.rejection_id,
                model.rejection_ordinal,
                model.code,
                model.collection_slot,
                model.field_slot,
                model.source_ordinal,
                model.canonical_root_key,
                model.root_key_sha256,
                _source_relation_oid(leaf.table_valued()).label("source_oid"),
            )
            .where(*_predicate(model, scope), model.rejection_ordinal.in_(ordinals))
            .order_by(model.rejection_ordinal)
            .limit(limit)
        )
    ).all()
    if [rejection.rejection_ordinal for rejection in rejections] != ordinals or any(
        rejection.source_oid != scope.rejection_oid for rejection in rejections
    ):
        raise RejectionEvidenceUnavailable()
    return rejections, len(ordinals) == len(headers)


def _bounded_page(scope, collections, rows, exhausted, page_bytes=MAX_PAGE_BYTES):
    page_dict = {
        "scope": scope.identity,
        "scope_sha256": scope.fingerprint,
        "events": [],
        "returned_rejection_events": 0,
        "next_after_ordinal": None,
        "complete": True,
    }
    if len(_encoded(page_dict)) > page_bytes:
        raise RejectionEvidenceUnavailable()
    for index, row in enumerate(rows):
        event = _event(scope, collections, row)
        candidate_dict = dict(
            page_dict,
            events=[*page_dict["events"], event],
            returned_rejection_events=index + 1,
            next_after_ordinal=str(row.rejection_ordinal),
            complete=False,
        )
        if len(_encoded(candidate_dict)) > page_bytes:
            if not page_dict["events"]:
                raise RejectionEvidenceUnavailable()
            return page_dict
        page_dict = candidate_dict
    if exhausted:
        page_dict.update(next_after_ordinal=None, complete=True)
    return page_dict


@asynccontextmanager
async def rejection_page_transaction(session):
    """Own the transaction covering both trusted lookup and retained page reads."""

    if session is None or session.in_transaction():
        raise RejectionEvidenceUnavailable()
    try:
        async with asyncio.timeout(5), session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            yield
    except RejectionEvidenceUnavailable:
        raise
    except Exception:
        raise RejectionEvidenceUnavailable() from None


async def retained_request_build(session, *, dataset_id, definition_revision_id, idempotency_key, build_id):
    """Resolve one retained attempt without following a mutable worker fence."""

    executions = (
        await session.execute(
            select(Execution.execution_id, Execution.dataset_id, Execution.state)
            .where(
                Execution.definition_revision_id == definition_revision_id, Execution.idempotency_key == idempotency_key
            )
            .limit(2)
            .execution_options(autoflush=False)
        )
    ).all()
    if not executions or len(executions) == 1 and executions[0].dataset_id != dataset_id:
        raise RejectionEvidenceNotFound()
    if len(executions) != 1 or not _is_positive(executions[0].execution_id):
        raise RejectionEvidenceUnavailable()
    execution = executions[0]
    if execution.state not in {"failed", "canceled", "completed", "no_change"}:
        raise RejectionEvidenceNotFinal()
    statement = select(Build.build_id).where(Build.execution_id == execution.execution_id)
    if build_id is not None:
        statement = statement.where(Build.build_id == build_id)
    builds = (await session.execute(statement.limit(2).execution_options(autoflush=False))).all()
    if len(builds) != 1 or not _is_positive(builds[0].build_id):
        raise RejectionEvidenceUnavailable()
    return execution.execution_id, builds[0].build_id


async def rejection_page_in_transaction(
    session, *, dataset_id, execution_id, build_id, after_ordinal, scope_sha256, limit, page_bytes=MAX_PAGE_BYTES
):
    """Read an already validated selector while its caller owns the read transaction."""

    scope = await _resolve_scope(session, dataset_id, execution_id, build_id)
    if scope_sha256 is not None and scope_sha256 != scope.fingerprint:
        raise RejectionEvidenceUnavailable()
    collections_by_slot = {slot: name for name, slot in (await load_collection_slots(session, scope)).items()}
    rejections, exhausted = await _page_rows(
        session, scope, -1 if after_ordinal is None else after_ordinal, limit, page_bytes
    )
    return _bounded_page(scope, collections_by_slot, rejections, exhausted, page_bytes)


async def read_rejection_page(
    session, *, dataset_id, execution_id, build_id, after_ordinal=None, scope_sha256=None, limit=MAX_EVENTS
):
    """Reacquire an exact read pin per page; never open a caller's transaction.

    Continuations require the prior scope digest. The returned ordinal is not a
    signed cursor. No owner transport or download route is enrolled here.
    """

    if (
        not all(_is_positive(identifier) for identifier in (dataset_id, execution_id, build_id))
        or type(limit) is not int
        or not 1 <= limit <= MAX_EVENTS
        or after_ordinal is not None
        and (type(after_ordinal) is not int or not 0 <= after_ordinal < 2**63)
        or scope_sha256 is not None
        and (type(scope_sha256) is not str or _DIGEST.fullmatch(scope_sha256) is None)
        or after_ordinal is not None
        and scope_sha256 is None
    ):
        raise ValueError("invalid rejection page selector")
    async with rejection_page_transaction(session):
        return await rejection_page_in_transaction(
            session,
            dataset_id=dataset_id,
            execution_id=execution_id,
            build_id=build_id,
            after_ordinal=after_ordinal,
            scope_sha256=scope_sha256,
            limit=limit,
        )
