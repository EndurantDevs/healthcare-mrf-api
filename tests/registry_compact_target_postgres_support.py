# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine compact publication and retained target reviews for address-boundary checks."""

import hashlib
import json
from contextlib import AsyncExitStack, asynccontextmanager
from traceback import extract_tb
from uuid import uuid4

import asyncpg
import pytest

from process.network_compact_layout import compact_identifier
from process.registry_required_target_coverage import read_registry_required_target_coverage
from tests.test_registry_approval_store_postgres import _actor, _approve, _command
from tests.test_registry_generic_fhir_reuse_postgres import (
    _add_mapping,
    _compose,
    _publish,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    bootstrap_db as bootstrap_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    generic_capture_db as generic_capture_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    generic_composition_db as generic_composition_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    serving_schema as serving_schema,
)
from tests.test_registry_network_binding_approval_postgres import _bind
from tests.test_registry_required_target_coverage_postgres import _decision, _ledger
from tests.test_registry_required_target_review_store_postgres import _SOURCE_FIELDS, _review

_READ_TABLES = (
    "network_serving_manifest",
    "network_membership_candidate",
    "network_serving_control",
    "network_candidate_custody",
    "registry_approved_record",
    "registry_source_snapshot",
    "registry_source_observation",
)


async def _assert_reader(connection, role):
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        settings = await connection.fetchrow("""SELECT current_user,session_user,
          current_setting('transaction_isolation'),current_setting('transaction_read_only'),
          r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,
          EXISTS(SELECT FROM (SELECT DISTINCT relowner AS owner_oid FROM pg_class) owners
            WHERE pg_has_role(r.oid,owners.owner_oid,'MEMBER') OR pg_has_role(r.oid,owners.owner_oid,'USAGE')
              OR pg_has_role(r.oid,owners.owner_oid,'SET')) AS relation_owner_reach,
          EXISTS(SELECT FROM (SELECT DISTINCT proowner AS owner_oid FROM pg_proc) owners
            WHERE pg_has_role(r.oid,owners.owner_oid,'MEMBER') OR pg_has_role(r.oid,owners.owner_oid,'USAGE')
              OR pg_has_role(r.oid,owners.owner_oid,'SET')) AS executable_owner_reach
          FROM pg_roles r WHERE r.rolname=current_user""")
        assert tuple(settings) == (role, role, "repeatable read", "on", *([False] * 7))


async def _close_reader(context):
    if context.coverage_reader is not None:
        await context.coverage_reader.close()
        assert context.coverage_reader.is_closed()


@asynccontextmanager
async def coverage_reader(context):
    """Reuse the declared native reader; register exact cleanup before changing its login state."""
    fixture, context.coverage_reader = context.fixture, None
    role, password = fixture.roles["reader"], uuid4().hex
    quoted_role, schema = compact_identifier(role), compact_identifier(fixture.control_schema)
    async with AsyncExitStack() as stack:
        stack.push_async_callback(fixture.connection.execute, f"ALTER ROLE {quoted_role} NOLOGIN PASSWORD NULL")
        stack.push_async_callback(_close_reader, context)
        await fixture.connection.execute(f"ALTER ROLE {quoted_role} LOGIN PASSWORD '{password}'")
        await fixture.connection.execute(f"GRANT USAGE ON SCHEMA {schema} TO {quoted_role}")
        relations = ",".join(schema + "." + compact_identifier(name) for name in _READ_TABLES)
        await fixture.connection.execute(f"GRANT SELECT ON {relations} TO {quoted_role}")
        context.coverage_reader = await asyncpg.connect(
            fixture.engine.url.set(
                drivername="postgresql",
                username=role,
                password=password,
            ).render_as_string(hide_password=False)
        )
        await _assert_reader(context.coverage_reader, role)
        yield context.coverage_reader


async def _review_source(context):
    fixture = context.fixture
    edition, ledger, required_targets = await _ledger(fixture)
    retained = json.loads(
        await fixture.connection.fetchval(
            f'SELECT to_jsonb(binding)::text FROM "{fixture.control_schema}".registry_network_binding binding '
            "WHERE source_key='network-one' AND NOT archived"
        )
    )
    binding_by_field = {name: retained[name] for name in _SOURCE_FIELDS}
    binding_by_field["network_id"] = context.network_id
    review_document_dict = {
        "ledger_snapshot_id": str(edition.snapshot_id),
        "ledger_artifact_sha256": ledger["artifact_sha256"],
        "decisions": [
            _decision(required_target["target_key"], binding_by_field) for required_target in required_targets[:2]
        ],
    }
    _, review = await _review(
        (fixture.connection, fixture.control_schema, fixture.engine), edition, review_document_dict
    )
    binding_by_field.update(
        operation="rebind",
        expected_revision=retained["revision"],
        expected_network_id=context.network_id,
        evidence_id=review["evidence_id"],
        evidence_sha256=review["artifact_sha256"],
    )
    actor = _actor()
    binding = (await _bind(fixture.connection, fixture.control_schema, binding_by_field, actor))["records"][0]
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, binding),
        actor,
    )
    context.edition, context.revision = edition, approval["approved_revision"]


@pytest.fixture
async def compact_coverage_db(generic_composition_db):
    context = generic_composition_db
    await _review_source(context)
    context.original = await _publish(context, await _compose(context, context.revision), first=True)
    revision = await _add_mapping(context, "network-unapproved")
    context.current = await _publish(context, await _compose(context, revision, context.original))
    async with coverage_reader(context):
        yield context


def _observe_compact_sql(record):
    if "actual_edges" in record.query and "native_input" in record.query:
        print(
            "COMPACT_TARGET_SQL "
            + json.dumps(
                {
                    "sha256": hashlib.sha256(record.query.encode()).hexdigest(),
                    "argument_count": len(record.args),
                    "elapsed_seconds": record.elapsed,
                    "exception": None if record.exception is None else repr(record.exception),
                }
            ),
            flush=True,
        )


def _observe_exception(error):
    chain, seen = [], set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        chain.append(
            {
                "type": type(error).__name__,
                "message": str(error),
                "frames": [frame.name for frame in extract_tb(error.__traceback__)],
            }
        )
        error = error.__cause__ or error.__context__
    print("COMPACT_TARGET_EXCEPTION " + json.dumps(chain), flush=True)


async def read_compact_coverage(context, generation_id=None):
    fixture = context.fixture
    with context.coverage_reader.query_logger(_observe_compact_sql):
        try:
            async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
                return await read_registry_required_target_coverage(
                    context.coverage_reader,
                    context.edition.snapshot_id,
                    generation_id=generation_id,
                    control_schema=fixture.control_schema,
                )
        except Exception as error:
            _observe_exception(error)
            raise
