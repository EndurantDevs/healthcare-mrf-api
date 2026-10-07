# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Ordinary admission SQL for a trusted application batch connection.

Existing staging page authority and the registered-snapshot resolver remain
mandatory; this operation grants nothing.
The connection must already be allowed to write this isolated candidate and
its protected build cursor. Restricted worker credentials are not sufficient.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

from sqlalchemy import BigInteger, Integer, String, and_, bindparam, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportBuildAttempt, CustomImportExecution, CustomImportSourceBindingRevision
from process.custom_import.admission_authorization import ADMISSION_PATH, PERMIT_CONTRACT, AdmissionPermit
from process.custom_import.build_source import (
    SourceBuildRequest,
    _call,
    _integer,
    _page_session,
    _prepare_statement,
    _resolve_build_snapshot,
)
from process.custom_import.runner_types import CandidateRunnerError, SessionFactory
from process.custom_import.storage_layout import snapshot_schema

_BUILD_FIELDS = (
    "build_id",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "execution_id",
    "capture_bundle_id",
    "producing_fence",
    "producing_token_sha256",
    "admission_after_occurrence_id",
    "page_row_limit",
    "page_byte_limit",
    "next_rejection_ordinal",
)
_PARAM_TYPES = {name: CustomImportBuildAttempt.__table__.c[name].type for name in _BUILD_FIELDS}
_PARAM_TYPES.update(
    expected_after_id=BigInteger(),
    last_id=BigInteger(),
    errors=BigInteger(),
    inserted_n=BigInteger(),
    rows_processed=Integer(),
    physical_row_cap=Integer(),
    definition_streams=JSONB(),
    memberships=JSONB(),
    decisions=JSONB(),
    root_relation=String(),
    landing_relation=String(),
    rejection_relation=String(),
    occurrence_relation=String(),
    rejection_sequence=String(),
)


class AdmissionResult(NamedTuple):
    phase: str
    after_occurrence_id: int
    rows_processed: int
    candidate_error_count: int


class AdmissionError(RuntimeError):
    """Keep admission failures out of the runner's terminal validation branch."""

    def __init__(self, code: str):
        super().__init__(code)
        self.sqlstate = "40001" if code == "custom_import_build_progress_conflict" else "P0001"


async def _has_admission_owner(session) -> bool:
    """Select direct SQL only for the existing cursor and dispatcher owner."""

    await _prepare_statement(session)
    connection = await session.connection()
    model_schema = CustomImportBuildAttempt.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    control_schema = schema_map.get(model_schema, model_schema)
    if not control_schema:
        raise CandidateRunnerError("admission requires an explicit model schema")
    control = connection.dialect.identifier_preparer.quote_schema(control_schema)
    statement = text("""
        SELECT pg_catalog.count(*)=2 AND coalesce(pg_catalog.bool_and(
            pg_catalog.pg_get_userbyid(p.proowner)=CURRENT_USER),false)
        FROM pg_catalog.pg_proc p WHERE p.oid IN (
            pg_catalog.to_regprocedure(:begin),pg_catalog.to_regprocedure(:admit))
    """).bindparams(bindparam("begin", type_=String()), bindparam("admit", type_=String()))
    return (
        await session.execute(
            statement,
            {
                "begin": f"{control}.begin_custom_import_build("
                "pg_catalog.int8,pg_catalog.int8,pg_catalog.bytea,pg_catalog.int8,pg_catalog.int8,pg_catalog.bool,"
                "pg_catalog.int4,pg_catalog.int8,pg_catalog.int4,pg_catalog.timestamptz)",
                "admit": f"{control}.admit_custom_import_build_page(pg_catalog.int8,pg_catalog.int8)",
            },
        )
    ).scalar_one() is True


@lru_cache(maxsize=1)
def _queries() -> tuple[str, ...]:
    """Load only this package's fixed ordinary statements, never installer SQL."""

    statements = Path(__file__).with_name("admission.sql").read_text().split("-- statement boundary --")
    if len(statements) != 4:
        raise CandidateRunnerError("admission SQL resource is malformed")
    return tuple(statements)


async def _statements(session, family_id):
    connection = await session.connection()
    model_schema = CustomImportBuildAttempt.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    control_schema = schema_map.get(model_schema, model_schema)
    if not control_schema:
        raise CandidateRunnerError("admission requires an explicit model schema")
    quote = connection.dialect.identifier_preparer.quote_schema
    control, candidate = quote(control_schema), quote(snapshot_schema(family_id))
    statements = []
    for query in _queries():
        statement = text(query.replace("__CONTROL__", control).replace("__CANDIDATE__", candidate))
        statements.append(
            statement.bindparams(*(bindparam(name, type_=_PARAM_TYPES[name]) for name in statement.compile().params))
        )
    relation_by_name = {
        name: f"{candidate}.{table}"
        for name, table in (
            ("root_relation", "custom_import_root_record"),
            ("landing_relation", "source_bulk_landing"),
            ("rejection_relation", "custom_import_rejection"),
            ("occurrence_relation", "custom_import_build_occurrence"),
        )
    }
    relation_by_name["rejection_sequence"] = f"{control}.custom_import_rejection_rejection_id_seq"
    return tuple(statements), relation_by_name


async def _execute(session, statement, parameter_by_name):
    await _prepare_statement(session)
    return (await session.execute(statement, parameter_by_name)).mappings().one()


async def _admit_locked(session, build, expected_after_id, *, physical_row_cap: int = 100_000):
    """Internal only: the enclosing page context owns rollback on every failure."""

    _integer(physical_row_cap, "physical_row_cap", build.page_row_limit, 100_000)
    # The page context already bounded this statement; establish trusted resolution
    # before even its next (unqualified) timeout helper, as the old leaf did.
    await session.execute(text("SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)"))
    family_id = await _resolve_build_snapshot(session, build.build_id)
    # Retain the dispatcher's immutable retained-base check as well as its writable binding.
    await _call(session, "resolve_custom_import_build_base_snapshot", (("bigint", build.build_id),))
    statements, relation_by_name = await _statements(session, family_id)
    parameter_by_name = {name: getattr(build, name) for name in _BUILD_FIELDS}
    parameter_by_name.update(relation_by_name, expected_after_id=expected_after_id, physical_row_cap=physical_row_cap)
    prerequisites = await _execute(session, statements[0], parameter_by_name)
    if prerequisites["problem"] is not None:
        raise AdmissionError(prerequisites["problem"])
    parameter_by_name.update(
        definition_streams=prerequisites["definition_streams"], memberships=prerequisites["memberships"]
    )
    await _prepare_statement(session)
    try:
        decision = (await session.execute(statements[1], parameter_by_name)).mappings().one()
    except DBAPIError as error:
        original = error.orig
        message = getattr(getattr(original, "__cause__", None), "message", None)
        if message is None:
            message = getattr(getattr(original, "diag", None), "message_primary", None)
        if getattr(original, "sqlstate", None) == "57014" and message == "canceling statement due to statement timeout":
            error._custom_import_admission_cursor = expected_after_id
        raise
    if decision["fatal_code"] is not None:
        raise AdmissionError(decision["fatal_code"])
    parameter_by_name.update(decision)
    aggregate = await _execute(session, statements[2], parameter_by_name)
    if (
        aggregate["inserted_n"] != aggregate["expected_inserted_n"]
        or aggregate["updated_n"] != aggregate["expected_updated_n"]
        or aggregate["invalid_resolution_n"] != 0
    ):
        raise AdmissionError("custom_import_build_structure_mismatch: admission aggregate differs")
    parameter_by_name["inserted_n"] = aggregate["inserted_n"]
    cursor_row = await _execute(session, statements[3], parameter_by_name)
    session.expire(build)  # Ordinary SQL changed the cursor; do not retain a stale ORM snapshot.
    return AdmissionResult(**cursor_row)


async def _verify_admission_permit(session, request, permit: AdmissionPermit) -> None:
    """Bind the fixed admission purpose to retained rows under the page's locks."""

    if type(permit) is not AdmissionPermit or (
        permit.contract != PERMIT_CONTRACT
        or permit.path != ADMISSION_PATH
        or permit.method != "POST"
        or permit.issuer != "custom-import-execution-controller"
        or permit.audience != "custom-import-engine"
        or request.dataset_id != permit.dataset_id
        or request.definition_revision_id != permit.definition_revision_id
        or request.schema_revision_id != permit.schema_revision_id
        or request.authorization_expires_at != permit.expires_at
    ):
        raise AdmissionError("custom_import_admission_authority_mismatch")
    await _prepare_statement(session)
    matched = (
        await session.execute(
            select(CustomImportExecution.execution_id)
            .join(
                CustomImportSourceBindingRevision,
                and_(
                    CustomImportSourceBindingRevision.source_binding_revision_id
                    == CustomImportExecution.source_binding_revision_id,
                    CustomImportSourceBindingRevision.dataset_id == CustomImportExecution.dataset_id,
                    CustomImportSourceBindingRevision.definition_revision_id
                    == CustomImportExecution.definition_revision_id,
                    CustomImportSourceBindingRevision.schema_revision_id == CustomImportExecution.schema_revision_id,
                ),
            )
            .where(
                CustomImportExecution.execution_id == request.execution_id,
                CustomImportExecution.dataset_id == permit.dataset_id,
                CustomImportExecution.definition_revision_id == permit.definition_revision_id,
                CustomImportExecution.schema_revision_id == permit.schema_revision_id,
                CustomImportExecution.idempotency_key == permit.idempotency_key,
                CustomImportExecution.source_binding_revision_id == permit.source_binding_revision_id,
                CustomImportSourceBindingRevision.binding_sha256 == bytes.fromhex(permit.source_binding_sha256),
            )
        )
    ).scalar_one_or_none()
    if matched != request.execution_id:
        raise AdmissionError("custom_import_admission_authority_mismatch")
    if not await _has_admission_owner(session):
        raise AdmissionError("custom_import_admission_owner_required")


async def _retry_admission_timeout(session_factory, request, build_id, error, *, admission_permit=None):
    """Retry one logical group only after the failed decision page fully closed."""

    cursor = getattr(error, "_custom_import_admission_cursor", None)
    if cursor is None or error.connection_invalidated or getattr(error, "_custom_import_retry_blocked", False):
        raise error
    async with _page_session(session_factory, request, build_id) as (session, build):
        if admission_permit is not None:
            await _verify_admission_permit(session, request, admission_permit)
        return await _admit_locked(session, build, cursor, physical_row_cap=build.page_row_limit)


async def admit_source_batch(
    session_factory: SessionFactory,
    request: SourceBuildRequest,
    build_id: int,
    expected_after_id: int,
    *,
    admission_permit: AdmissionPermit | None = None,
) -> AdmissionResult:
    """Own one physical prefix and its fresh lease check immediately before commit.

    Logical bounds remain the request's bounds. At most 100,000 occurrences are
    inspected; accepted raw bytes are capped at 256 MiB. Metadata and native
    query memory are not covered by that accepted-byte cap. Any error,
    cancellation or failed final authority check rolls back the whole prefix.
    A confirmed decision statement timeout permits one fresh-page retry of one
    logical group at the same cursor; unknown diagnostics are never retried.
    """

    _integer(build_id, "build_id", 1, (1 << 63) - 1)
    _integer(expected_after_id, "expected_after_id", 0, (1 << 63) - 1)
    try:
        async with _page_session(session_factory, request, build_id) as (session, build):
            if admission_permit is not None:
                await _verify_admission_permit(session, request, admission_permit)
            admission_result = await _admit_locked(session, build, expected_after_id)
    except DBAPIError as error:
        admission_result = await _retry_admission_timeout(
            session_factory, request, build_id, error, admission_permit=admission_permit
        )
    return admission_result
