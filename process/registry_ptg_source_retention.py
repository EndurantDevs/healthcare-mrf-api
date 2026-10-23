# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable source custody through a dedicated, prebound retention Publisher."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.registry_company_approval_fence import (
    _source_engine,
    registry_company_approval_transaction,
)
from process.registry_ptg_producer_scope import _protected_store
from process.registry_ptg_published_provisioning import (
    HEADER_TABLES,
    LOCK_COLUMN,
    PIN_INSERT_COLUMNS,
    require_published_lock_privileges,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

logger = logging.getLogger(__name__)


_ROLE_SQL = """
WITH actor AS (SELECT * FROM pg_catalog.pg_roles WHERE rolname=current_user),
source AS (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name)
SELECT current_user=session_user AND actor.rolname=:role
 AND actor.rolcanlogin AND NOT actor.rolsuper AND NOT actor.rolcreatedb
 AND NOT actor.rolcreaterole AND NOT actor.rolreplication AND NOT actor.rolbypassrls
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_auth_members membership
   WHERE membership.member=actor.oid OR membership.roleid=actor.oid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles other WHERE other.oid<>actor.oid
   AND (pg_catalog.pg_has_role(actor.oid,other.oid,'SET')
        OR pg_catalog.pg_has_role(actor.oid,other.oid,'USAGE')))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_shdepend dep
   WHERE dep.refclassid='pg_catalog.pg_authid'::regclass AND dep.refobjid=actor.oid AND dep.deptype='o')
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_namespace ns
   WHERE pg_catalog.has_schema_privilege(current_user,ns.oid,'CREATE'))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_class fact
   WHERE pg_catalog.has_table_privilege(current_user,fact.oid,'INSERT,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
    OR (fact.oid<>'pg_catalog.pg_settings'::regclass
        AND pg_catalog.has_table_privilege(current_user,fact.oid,'UPDATE'))
    OR (pg_catalog.has_table_privilege(current_user,fact.oid,'DELETE')
        AND NOT(fact.relnamespace=source.oid AND fact.relname='ptg2_snapshot_pin'))
    OR CASE WHEN fact.relkind='S'
       THEN pg_catalog.has_sequence_privilege(current_user,fact.oid,'USAGE,UPDATE') ELSE FALSE END
    OR EXISTS(SELECT FROM pg_catalog.pg_attribute field
      WHERE field.attrelid=fact.oid AND field.attnum>0 AND NOT field.attisdropped AND (
       pg_catalog.has_column_privilege(current_user,fact.oid,field.attnum,'REFERENCES')
       OR (pg_catalog.has_column_privilege(current_user,fact.oid,field.attnum,'INSERT')
           AND NOT(fact.relnamespace=source.oid AND fact.relname='ptg2_snapshot_pin'
             AND field.attname=ANY(CAST(:pin_columns AS text[]))))
       OR (fact.oid<>'pg_catalog.pg_settings'::regclass
           AND pg_catalog.has_column_privilege(current_user,fact.oid,field.attnum,'UPDATE')
           AND NOT(fact.relnamespace=source.oid AND fact.relname=ANY(CAST(:headers AS text[]))
             AND field.attname=:lock_column)))))
 AND pg_catalog.has_table_privilege(current_user,
     format('%I.ptg2_snapshot_pin',:schema_name),'DELETE')
 AND NOT EXISTS(SELECT FROM unnest(CAST(:pin_columns AS text[])) field(name)
   WHERE NOT pg_catalog.has_column_privilege(current_user,
     format('%I.ptg2_snapshot_pin',:schema_name),field.name,'INSERT'))
FROM actor CROSS JOIN source
"""


def source_retention_provisioning_statements(schema_name, publisher_role):
    """Return operator SQL; existing SELECT/USAGE and constant lock columns are prerequisites."""
    schema, role = _identifier(schema_name), _identifier(publisher_role)
    return tuple(
        f"GRANT UPDATE({LOCK_COLUMN}) ON {schema}.{_identifier(table)} TO {role}" for table in HEADER_TABLES
    ) + (f"GRANT INSERT({','.join(PIN_INSERT_COLUMNS)}), DELETE ON {schema}.ptg2_snapshot_pin TO {role}",)


def retention_sessions(service):
    """Require a separate genuine Publisher on the same configured database."""
    sessions = service.source_retention_sessions
    if sessions is None:
        raise RuntimeError("registry_ptg_source_retention_unconfigured")
    try:
        publisher = _source_engine(sessions)
    except ValueError as error:
        raise RuntimeError("registry_ptg_source_retention_unavailable") from error
    reader_sessions = service.reader_sessions
    if type(reader_sessions) is not async_sessionmaker or reader_sessions.class_ is not AsyncSession:
        raise RuntimeError("registry_ptg_source_retention_unconfigured")
    reader = reader_sessions.kw.get("bind")
    if type(reader) is not AsyncEngine or reader_sessions.kw.get("binds"):
        raise RuntimeError("registry_ptg_source_retention_unconfigured")
    forbidden_roles = {reader.url.username, service.store.owner_role, service.store.approval_role}
    if service.office_custody is not None:
        forbidden_roles |= {service.office_custody.owner_role, service.office_custody.publisher_role}
    identity = lambda engine: (engine.url.host, engine.url.port, engine.url.database, engine.url.query)
    if (
        publisher is reader
        or identity(publisher) != identity(reader)
        or publisher.url.username in forbidden_roles | {None}
    ):
        raise RuntimeError("registry_ptg_source_retention_unconfigured")
    return sessions


async def verify_source_retention_publisher(session, service):
    """Check native effective privileges, including inherited writes outside the source schema."""
    role = service.source_retention_sessions.kw["bind"].url.username
    protected = await session.scalar(
        text(_ROLE_SQL),
        {
            "role": role,
            "schema_name": service.schema_name,
            "pin_columns": PIN_INSERT_COLUMNS,
            "headers": HEADER_TABLES,
            "lock_column": LOCK_COLUMN,
        },
    )
    if protected is not True:
        raise RuntimeError("registry_ptg_source_retention_unprotected")
    await require_published_lock_privileges(session, service.schema_name, pin_writer=True, retention_publisher=True)


@asynccontextmanager
async def source_retention_transaction(service, *, release=False):
    """Own an actual independent commit; never accept a caller savepoint."""
    sessions = retention_sessions(service)
    async with registry_company_approval_transaction(
        sessions, control_schema=service.store.control_schema or registry_schema(), is_exclusive=release
    ) as session:
        await verify_source_retention_publisher(session, service)
        yield session


async def retain_published_review_source(service, envelope, deadline, state):
    """Freshly authorize and commit the exact operation pin before Approver handoff."""
    from process.ptg_parts.result_archive_allowed_authority import commit_allowed_source_authority
    from process.ptg_parts.result_archive_source_authority import commit_ptg_result_archive_source_authority
    from process.registry_ptg_published_plan_contract import INTENT_FIELDS
    from process.registry_ptg_published_plan_scope import (
        TABLE,
        _approved_association,
        _checked_replay,
        _document,
        _existing,
        _parameters,
        resolve_published_plan,
    )
    from process.registry_ptg_scope_engine import _remaining

    retention_sessions(service)
    _remaining(deadline)
    await service.authority.authorize(envelope, deadline=deadline)
    _remaining(deadline)
    async with source_retention_transaction(service) as session:
        table = await _protected_store(session, service.store, write=False, table_name=TABLE)
        full, authority, evidence = await resolve_published_plan(
            session,
            service.schema_name,
            {name: envelope["command"][name] for name in INTENT_FIELDS},
            envelope["command"]["ownership"],
            envelope["actor"],
            pin_writer=True,
            retention_publisher=True,
        )
        if full != envelope["command"]:
            raise ValueError("registry_ptg_scope_source_changed")
        document = _document(full, envelope["actor"], evidence)
        parameters = _parameters(document)
        reviews = await _existing(session, table, parameters)
        if reviews:
            _checked_replay(reviews, parameters, document)
        else:
            await _approved_association(session, table, full)
        state["authority"] = authority
        commit = (
            commit_allowed_source_authority
            if full["review_type"] == "published_allowed_amount_plan"
            else commit_ptg_result_archive_source_authority
        )
        retained = await commit(session, schema_name=service.schema_name, authority=authority)
        if retained.as_dict() != authority:
            raise ValueError("registry_ptg_scope_source_changed")
    return authority


async def retain_office_review_source(service, envelope, deadline, state):
    """Retain one independent office operation without widening the office Publisher."""
    from process.ptg_parts.result_archive_published_authority import prepare_ptg_published_result_source_authority
    from process.ptg_parts.result_archive_source_authority import commit_ptg_result_archive_source_authority
    from process.registry_ptg_published_plan_scope import read_registry_ptg_published_plan_scope
    from process.registry_ptg_scope_engine import _remaining

    retention_sessions(service)
    command = envelope["command"]
    _remaining(deadline)
    await service.authority.authorize(
        {name: envelope[name] for name in ("actor", "session_token_sha256", "command")}, deadline=deadline
    )
    _remaining(deadline)
    async with source_retention_transaction(service) as session:
        source_review_by_field = await read_registry_ptg_published_plan_scope(
            session,
            scope_id=command["scope_id"],
            client_id=command["client_id"],
            approval_sha256=command["scope_approval_sha256"],
            store=service.store,
            retention_publisher=True,
        )
        expected = command["source"]["evidence"]["source_authority"]
        if source_review_by_field["evidence"]["source_authority"] != expected:
            raise ValueError("registry_ptg_office_source_changed")
        prepared = await prepare_ptg_published_result_source_authority(
            session,
            schema_name=service.schema_name,
            operation_id="registry_ptg_office_review_" + UUID(command["capture_id"]).hex,
            snapshot_id=command["source"]["snapshot_id"],
        )
        authority = prepared.as_dict()
        if (
            authority["identity"] != expected["identity"]
            or authority["source_key"] != command["source"]["binding_source_key"]
        ):
            raise ValueError("registry_ptg_office_source_changed")
        state["authority"] = authority
        await commit_ptg_result_archive_source_authority(session, schema_name=service.schema_name, authority=authority)
    return authority


@asynccontextmanager
async def retained_review_source(service, envelope, deadline, *, office=False):
    """Reconcile interrupted preparation/approval, including an unknown pin commit."""
    state_by_field = {}
    try:
        retain = retain_office_review_source if office else retain_published_review_source
        authority = await retain(service, envelope, deadline, state_by_field)
        yield authority
    except BaseException:
        if "authority" in state_by_field:
            try:
                await drain_operation(
                    reconcile_unapproved_review_source(service, state_by_field["authority"]),
                    preserve_cancellation=False,
                )
            except BaseException:
                logger.warning("Source retention reconciliation deferred; exact operation remains held")
        raise


async def reconcile_unapproved_review_source(service, authority):
    """Keep established/uncertain approvals; release only the exact unapproved operation."""
    from process.ptg_parts.result_archive_allowed_authority import validated_allowed_authority
    from process.ptg_parts.result_archive_published_authority import (
        reconcile_ptg_published_result_source_release,
        validate_ptg_published_result_source_authority,
    )
    from process.ptg_parts.result_archive_source_authority import _release_validated_authority_pin
    from process.registry_ptg_allowed_plan_contract import AUTHORITY_CONTRACT as ALLOWED_CONTRACT

    if type(authority) is not dict or type(authority.get("operation_id")) is not str:
        raise ValueError("registry_ptg_source_retention_invalid")
    operation = authority["operation_id"]
    is_office = operation.startswith("registry_ptg_office_review_")
    prefix = "registry_ptg_office_review_" if is_office else "registry_ptg_published_review_"
    suffix = operation.removeprefix(prefix)
    if operation != prefix + suffix or len(suffix) != 32 or UUID(hex=suffix).hex != suffix or not UUID(hex=suffix).int:
        raise ValueError("registry_ptg_source_retention_invalid")
    if authority.get("contract") == ALLOWED_CONTRACT:
        if is_office:
            raise ValueError("registry_ptg_source_retention_invalid")
        allowed = validated_allowed_authority(authority)
    else:
        validate_ptg_published_result_source_authority(authority)
        allowed = None
    async with source_retention_transaction(service, release=True) as session:
        tables = (
            ("registry_ptg_office_approval", "registry_ptg_office_hold")
            if is_office
            else ("registry_ptg_published_plan_scope",)
        )
        for name in tables:
            table = await _protected_store(session, service.store, write=False, table_name=name)
            held = await session.scalar(
                text(
                    f"SELECT EXISTS(SELECT FROM {table} WHERE {'capture_id' if is_office else 'scope_id'}=CAST(:id AS uuid))"
                ),
                {"id": str(UUID(hex=suffix))},
            )
            if held is not False:
                return "retained"
        if allowed is not None:
            return await _release_validated_authority_pin(
                session, schema_name=service.schema_name, authority=allowed, acknowledge_absent=True
            )
        return await reconcile_ptg_published_result_source_release(
            session, schema_name=service.schema_name, authority=authority
        )
