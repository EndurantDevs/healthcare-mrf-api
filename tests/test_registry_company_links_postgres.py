# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit company relationships through real drafts, approval and HTTP."""

import asyncio
import json
from dataclasses import replace
from datetime import datetime
from uuid import UUID, uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker

from api.endpoint import registry_management as management
from process import registry_record_store
from process.company_network_link_store import read_group_company_links
from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_record_store import RegistryRecordCommand, RegistryRecordConflict, apply_registry_record_command
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _approved, _command, _create, _draft

pytestmark = pytest.mark.asyncio
_AUTH = {"Authorization": "Bearer synthetic-registry-service-token"}


async def _seed(engine, schema, actor):
    company = await _draft(engine, schema, _create("company"), actor)
    group = await _draft(engine, schema, _create("group"), actor)
    networks = [await _draft(engine, schema, _create("network"), actor) for _ in range(2)]
    return company, group, networks


def _links(company, networks=(), group=None):
    return RegistryRecordCommand(
        "company_links",
        UUID(company["record_id"]),
        "create",
        0,
        {
            "network_ids": [network["record_id"] for network in networks],
            "group_id": group["record_id"] if group else None,
        },
        "Explicit company relationships",
        uuid4().hex,
    )


async def _preview(connection, schema, command, actor):
    async with connection.transaction():
        return await preview_registry_approval(connection, command, actor, control_schema=schema)


def _assertion(company, network, **changes):
    return {
        "company_id": company["record_id"],
        "network_id": network["record_id"],
        "relationship_role": "uses",
        "benefit_domain": "medical",
        "applicability": "national",
        "states": [],
        "valid_from": "2026-01-01",
        "valid_to": "2027-01-01",
        "evidence_text": "Example explicit relationship",
        **changes,
    }


async def test_native_assertions_canonical_replay_preserve_and_explicit_clear(serving_schema, monkeypatch):
    connection, schema, engine = serving_schema
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    assertions = [_assertion(company, networks[1], benefit_domain=None), _assertion(company, networks[0])]
    creation = replace(
        _links(company, networks, group),
        fields={
            "network_ids": [network["record_id"] for network in networks],
            "group_id": group["record_id"],
            "network_assertions": assertions,
        },
    )
    first = await _draft(engine, schema, creation, actor)
    expected_assertions = list(reversed(assertions))
    assert first["record"]["network_assertions"] == expected_assertions
    with monkeypatch.context() as unavailable:
        unavailable.setattr(registry_record_store, "_fast_module", lambda: None)
        assert await _draft(engine, schema, creation, actor) == first
    preserve = replace(
        creation,
        operation="correct",
        expected_revision=1,
        fields={"network_ids": creation.fields["network_ids"], "group_id": None},
        idempotency_key=uuid4().hex,
    )
    preserved = await _draft(engine, schema, preserve, actor)
    assert preserved["record"]["network_assertions"] == expected_assertions
    with pytest.raises(ValueError, match="target_invalid"):
        await _draft(
            engine,
            schema,
            replace(
                preserve, expected_revision=2, fields={"network_ids": [], "group_id": None}, idempotency_key=uuid4().hex
            ),
            actor,
        )
    cleared = await _draft(
        engine,
        schema,
        replace(
            preserve,
            expected_revision=2,
            fields={"network_ids": [], "group_id": None, "network_assertions": []},
            idempotency_key=uuid4().hex,
        ),
        actor,
    )
    assert cleared["record"]["network_ids"] == [] and cleared["record"]["network_assertions"] == []
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


async def test_group_company_snapshot_is_exact_bounded_and_never_expands_permissions(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    first, group, networks = await _seed(engine, schema, actor)
    second = await _draft(engine, schema, _create("company"), actor)
    foreign_group = await _draft(engine, schema, _create("group"), actor)
    foreign = await _draft(engine, schema, _create("company"), actor)
    for company, assigned_group in ((first, group), (second, group), (foreign, foreign_group)):
        await _draft(engine, schema, _links(company, networks, assigned_group), actor)
    group_id = UUID(group["record_id"])
    page = await read_group_company_links(connection, group_id, limit=1, control_schema=schema)
    following = await read_group_company_links(connection, group_id, limit=1, offset=1, control_schema=schema)
    found_companies = [page["records"][0], following["records"][0]]
    assert [company_entry["company"]["company_id"] for company_entry in found_companies] == sorted(
        (first["record_id"], second["record_id"])
    )
    assert all(company_entry["links"]["group_id"] == group["record_id"] for company_entry in found_companies)
    assert {key: field_value for key, field_value in page["group"].items() if key != "created_at"} == {
        key: field_value for key, field_value in group["record"].items() if key != "created_at"
    }
    assert datetime.fromisoformat(page["group"]["created_at"]) == datetime.fromisoformat(group["record"]["created_at"])
    assert page["draft_revision"] == following["draft_revision"]
    assert page["limit"] == 1 and page["offset"] == 0 and following["offset"] == 1
    allowed = await read_group_company_links(
        connection, group_id, company_ids=[UUID(second["record_id"])], control_schema=schema
    )
    assert [company_entry["company"]["company_id"] for company_entry in allowed["records"]] == [second["record_id"]]
    empty = await read_group_company_links(connection, group_id, company_ids=[], control_schema=schema)
    assert empty["records"] == []
    assert await read_group_company_links(connection, uuid4(), control_schema=schema) is None
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


@pytest.mark.parametrize(
    "change",
    [
        {"company_id": str(uuid4())},
        {"network_id": 2147483647},
        {"valid_from": "2026-02-30"},
        {"valid_to": "2026-01-01"},
        {"applicability": "states", "states": []},
        {"benefit_domain": "other"},
    ],
)
async def test_native_assertion_invalid_scope_or_semantics_never_mutates(serving_schema, change):
    connection, schema, engine = serving_schema
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    before = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    creation = replace(
        _links(company, networks),
        fields={
            "network_ids": [network["record_id"] for network in networks],
            "group_id": None,
            "network_assertions": [_assertion(company, networks[0], **change)],
        },
    )
    with pytest.raises(ValueError):
        await _draft(engine, schema, creation, actor)
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == before
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry_links') == 0


async def test_overlapping_company_periods_require_review_and_preserve_history(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    assertion = _assertion(company, networks[0])
    creation = replace(
        _links(company, networks),
        fields={
            "network_ids": [network["record_id"] for network in networks],
            "group_id": None,
            "network_assertions": [assertion, {**assertion, "evidence_text": "Different overlapping evidence"}],
        },
    )
    with pytest.raises(ValueError, match="review_required"):
        await _draft(engine, schema, creation, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry_links') == 0
    nonoverlapping = replace(
        creation,
        fields={
            **creation.fields,
            "network_assertions": [
                assertion,
                {**assertion, "valid_from": "2027-01-01", "valid_to": None},
            ],
        },
    )
    retained = await _draft(engine, schema, nonoverlapping, actor)
    approval = await _approve(connection, schema, await _command(connection, schema, retained), actor)
    assert (
        json.loads((await _approved(connection, schema, approval["approved_revision"]))[0]["record_json"])[
            "network_assertions"
        ]
        == retained["record"]["network_assertions"]
    )


async def test_pre_migration_empty_assertions_approve_without_rewriting_history(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    company = await _draft(engine, schema, _create("company"), actor)
    retained = await _draft(engine, schema, _links(company), actor)
    await connection.execute(
        f"UPDATE \"{schema}\".registry_record_history SET record_json=record_json-'network_assertions' WHERE record_kind='company_links'"
    )
    original = await connection.fetchval(
        f"SELECT record_json FROM \"{schema}\".registry_record_history WHERE record_kind='company_links'"
    )
    approval = await _command(connection, schema, retained)
    preview = await _preview(connection, schema, approval, actor)
    assert "network_assertions" not in preview["records"][0]["after"]
    approved = await _approve(connection, schema, approval, actor)
    assert (await _approved(connection, schema, approved["approved_revision"]))[0]["record_json"] == original
    assert (
        await connection.fetchval(
            f"SELECT record_json FROM \"{schema}\".registry_record_history WHERE record_kind='company_links'"
        )
        == original
    )


async def test_durable_links_clear_replay_and_independent_company(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    creation = _links(company, reversed(networks), group)
    first = await _draft(engine, schema, creation, actor)
    assert first["record"] == {
        "company_id": company["record_id"],
        "network_ids": sorted(network["record_id"] for network in networks),
        "group_id": group["record_id"],
        "network_assertions": [],
        "archived": False,
        "revision": 1,
        "created_at": first["record"]["created_at"],
    }
    clear = replace(
        creation,
        operation="correct",
        expected_revision=1,
        fields={"network_ids": [], "group_id": None},
        idempotency_key=uuid4().hex,
    )
    cleared = await _draft(engine, schema, clear, actor)
    assert cleared["record"]["network_ids"] == [] and cleared["record"]["group_id"] is None
    assert cleared["record"]["revision"] == 2
    assert await _draft(engine, schema, creation, actor) == first
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _draft(engine, schema, replace(creation, reason="Changed retry"), actor)
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _draft(engine, schema, replace(clear, idempotency_key=uuid4().hex), actor)
    assert await connection.fetchval(f'SELECT roles FROM "{schema}".company_registry') == ["employer"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry') == 1
    assert (
        await connection.fetchval(
            f"SELECT count(*) FROM \"{schema}\".registry_record_history WHERE record_kind='company_links'"
        )
        == 2
    )
    group_only = replace(
        clear,
        expected_revision=2,
        fields={"network_ids": [], "group_id": group["record_id"]},
        idempotency_key=uuid4().hex,
    )
    assert (await _draft(engine, schema, group_only, actor))["record"]["network_ids"] == []
    await connection.execute(f'UPDATE "{schema}".company_group_registry SET archived=true')
    assert await _draft(engine, schema, creation, actor) == first


@pytest.mark.parametrize(
    "fields",
    [
        {"network_ids": [True], "group_id": None},
        {"network_ids": [0], "group_id": None},
        {"network_ids": [-1], "group_id": None},
        {"network_ids": [1.5], "group_id": None},
        {"network_ids": ["1"], "group_id": None},
        {"network_ids": [1, 1], "group_id": None},
        {"network_ids": [2147483648], "group_id": None},
        {"network_ids": list(range(1, 5002)), "group_id": None},
        {"network_ids": [], "group_id": "00000000-0000-0000-0000-000000000000"},
        {"network_ids": [], "group_id": str(uuid4()).upper()},
        {"network_ids": [], "group_id": True},
        {"network_ids": [], "group_id": "invalid"},
        {"network_ids": [], "group_id": None, "inferred": True},
    ],
)
async def test_invalid_link_fields_never_write(serving_schema, fields):
    connection, schema, engine = serving_schema
    actor = _actor()
    company = await _draft(engine, schema, _create("company"), actor)
    with pytest.raises(ValueError):
        await _draft(engine, schema, replace(_links(company), fields=fields), actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry_links') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 1


async def test_references_require_existing_active_and_allocated_heads(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    creation = _links(company, networks, group)
    invalid_commands = [
        replace(creation, record_id=uuid4()),
        replace(creation, fields={"network_ids": [], "group_id": str(uuid4())}),
        replace(creation, fields={"network_ids": [2147483647], "group_id": None}),
    ]
    await connection.execute(
        f"INSERT INTO \"{schema}\".network_registry_record(network_id,display_name) VALUES(2147483646,'Unallocated example')"
    )
    invalid_commands.append(replace(creation, fields={"network_ids": [2147483646], "group_id": None}))
    for invalid_command in invalid_commands:
        with pytest.raises(ValueError, match="target_invalid"):
            await _draft(engine, schema, invalid_command, actor)
    for table, column, record_id in (
        ("company_registry", "company_id", UUID(company["record_id"])),
        ("company_group_registry", "group_id", UUID(group["record_id"])),
        ("network_registry_record", "network_id", networks[0]["record_id"]),
    ):
        await connection.execute(f'UPDATE "{schema}"."{table}" SET archived=true WHERE {column}=$1', record_id)
        with pytest.raises(ValueError, match="target_invalid"):
            await _draft(engine, schema, creation, actor)
        await connection.execute(f'UPDATE "{schema}"."{table}" SET archived=false WHERE {column}=$1', record_id)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry_links') == 0


async def test_selective_links_approval_preserves_pending_and_prior_versions(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    creation = _links(company, networks[:1], group)
    first = await _draft(engine, schema, creation, actor)
    approval = await _command(connection, schema, first)
    preview = await _preview(connection, schema, approval, actor)
    assert preview["additions_count"] == 1 and preview["unresolved_count"] == 0
    assert preview["records"] == [
        {
            "record_kind": "company_links",
            "record_id": company["record_id"],
            "record_revision": 1,
            "before": None,
            "after": first["record"],
        }
    ]
    approved = await _approve(connection, schema, approval, actor)
    first_map = await _approved(connection, schema, approved["approved_revision"])
    assert len(first_map) == 1 and first_map[0]["record_kind"] == "company_links"
    assert json.loads(first_map[0]["record_json"]) == first["record"]
    correction = replace(
        creation,
        operation="correct",
        expected_revision=1,
        fields={"network_ids": [networks[1]["record_id"]], "group_id": None},
        idempotency_key=uuid4().hex,
    )
    pending = await _draft(engine, schema, correction, actor)
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _preview(connection, schema, await _command(connection, schema, first), actor)
    next_approval = await _approve(connection, schema, await _command(connection, schema, company), actor)
    next_map = await _approved(connection, schema, next_approval["approved_revision"])
    retained = next(
        approved_record for approved_record in next_map if approved_record["record_kind"] == "company_links"
    )
    assert json.loads(retained["record_json"])["network_ids"] == [networks[0]["record_id"]]
    assert retained["record_revision"] == 1 and pending["custom_revision"] < next_approval["approved_revision"]
    correction_approval = await _command(connection, schema, pending)
    preview = await _preview(connection, schema, correction_approval, actor)
    assert preview["corrections_count"] == 1
    assert preview["records"][0]["before"] == first["record"]
    assert preview["records"][0]["after"] == pending["record"]
    await _approve(connection, schema, correction_approval, actor)
    assert await _approved(connection, schema, approved["approved_revision"]) == first_map
    assert await _approve(connection, schema, approval, actor) == {**approved, "replayed": True}


async def test_company_links_selection_namespace_and_history_tamper(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    company = await _draft(engine, schema, _create("company"), actor)
    retained = await _draft(engine, schema, _links(company), actor)
    approval = await _command(connection, schema, retained)
    for invalid_id in (True, 42, company["record_id"].upper(), "00000000-0000-0000-0000-000000000000"):
        changed_selection = ({"record_kind": "company_links", "record_id": invalid_id, "revision": 1},)
        with pytest.raises(ValueError, match="selection_invalid"):
            await _approve(connection, schema, replace(approval, selection=changed_selection), actor)
    await connection.execute(
        f'UPDATE "{schema}".registry_record_history '
        "SET record_json=jsonb_set(record_json,'{network_ids}','[42]') WHERE record_kind='company_links'"
    )
    for operation in (_preview, _approve):
        with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
            await operation(connection, schema, approval, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approved_record') == 0


async def test_archive_restore_and_caller_rollback(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    company = await _draft(engine, schema, _create("company"), actor)
    creation = _links(company)
    first = await _draft(engine, schema, creation, actor)
    await _approve(connection, schema, await _command(connection, schema, first), actor)
    archived = await _draft(
        engine,
        schema,
        replace(creation, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex),
        actor,
    )
    approval = await _command(connection, schema, archived)
    assert (await _preview(connection, schema, approval, actor))["archives_count"] == 1
    await _approve(connection, schema, approval, actor)
    restored = await _draft(
        engine,
        schema,
        replace(creation, operation="restore", expected_revision=2, fields={}, idempotency_key=uuid4().hex),
        actor,
    )
    assert (await _preview(connection, schema, await _command(connection, schema, restored), actor))[
        "restores_count"
    ] == 1
    before = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    async with async_sessionmaker(engine)() as session:
        transaction = await session.begin()
        await apply_registry_record_command(
            session,
            replace(creation, operation="correct", expected_revision=3, idempotency_key=uuid4().hex),
            actor,
            schema=schema,
        )
        await transaction.rollback()
    assert await connection.fetchval(f'SELECT revision FROM "{schema}".company_registry_links') == 3
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == before
    assert (
        await connection.fetchval(
            f"SELECT count(*) FROM \"{schema}\".registry_record_history WHERE record_kind='company_links'"
        )
        == 3
    )


async def test_same_revision_corrections_have_one_winner(serving_schema):
    _, schema, engine = serving_schema
    actor = _actor()
    company = await _draft(engine, schema, _create("company"), actor)
    creation = _links(company)
    await _draft(engine, schema, creation, actor)
    outcomes = await asyncio.gather(
        *[
            _draft(
                engine,
                schema,
                replace(creation, operation="correct", expected_revision=1, idempotency_key=uuid4().hex),
                actor,
            )
            for _ in range(2)
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, RegistryRecordConflict) for outcome in outcomes) == 1


async def test_one_and_five_thousand_networks_use_constant_sql(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    companies = [await _draft(engine, schema, _create("company"), actor) for _ in range(2)]
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_identity(network_id,allocation_key) OVERRIDING SYSTEM VALUE SELECT n,md5(n::text)::uuid FROM generate_series(1,5000) n'
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".network_registry_record(network_id,display_name) SELECT n,'Example allocated network' FROM generate_series(1,5000) n"
    )
    statements = []

    def record_statement(connection, cursor, statement, parameters, context, executemany):
        """Observe actual driver statements without replacing native execution."""
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", record_statement)
    try:
        counts = []
        for company, size in zip(companies, (1, 5000), strict=True):
            statements.clear()
            receipt = await _draft(
                engine,
                schema,
                replace(_links(company), fields={"network_ids": list(range(1, size + 1)), "group_id": None}),
                actor,
            )
            assert len(receipt["record"]["network_ids"]) == size
            counts.append(len(statements))
            assert sum("cardinality" in statement for statement in statements) == 1
        assert counts[0] == counts[1]
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record_statement)


@pytest.fixture
async def company_links_app(serving_schema, monkeypatch):
    _, schema, engine = serving_schema
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-registry-service-token")
    for name in (
        "apply_registry_record_command",
        "get_registry_record",
        "list_registry_records",
        "apply_company_network_link_batch",
        "read_group_company_links",
    ):
        original = getattr(management, name)

        def mapped_call(original, schema_keyword):
            async def invoke(*args, **kwargs):
                return await original(*args, **kwargs, **{schema_keyword: schema})

            return invoke

        monkeypatch.setattr(
            management,
            name,
            mapped_call(
                original,
                "control_schema"
                if name in {"apply_company_network_link_batch", "read_group_company_links"}
                else "schema",
            ),
        )
    app = Sanic("company_links_http_" + uuid4().hex)
    app.blueprint(Blueprint.group([management.blueprint], version_prefix="/api/v"))
    sessions = async_sessionmaker(engine)

    @app.middleware("request")
    async def open_session(request):
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, response):
        await request.ctx.sa_session.close()

    yield app, serving_schema


async def test_control_http_company_links_create_read_and_clear(company_links_app):
    app, (connection, schema, engine) = company_links_app
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    creation = _links(company, networks, group)
    command_by_name = {
        "record_id": company["record_id"],
        "operation": "create",
        "expected_revision": 0,
        "fields": creation.fields,
        "reason": creation.reason,
        "idempotency_key": creation.idempotency_key,
        "actor": {"kind": actor.kind, "user_id": str(actor.user_id), "client_id": actor.client_id},
    }
    endpoint = "/api/v1/registry/manage/company_links"
    _, denied = await app.asgi_client.post(endpoint, json=command_by_name)
    assert denied.status == 403
    _, created = await app.asgi_client.post(endpoint, headers=_AUTH, json=command_by_name)
    assert created.status == 200 and created.json["record_id"] == company["record_id"]
    assert created.headers["Cache-Control"] == "private, no-store"
    _, readback = await app.asgi_client.get(endpoint + "/" + company["record_id"], headers=_AUTH)
    assert readback.json["record"] == created.json["record"]
    _, listed = await app.asgi_client.get(endpoint + "?record_ids=" + company["record_id"], headers=_AUTH)
    assert listed.status == 200 and listed.json["records"] == [created.json["record"]]
    cleared_command_by_name = {
        **command_by_name,
        "operation": "correct",
        "expected_revision": 1,
        "fields": {"network_ids": [], "group_id": None},
        "idempotency_key": uuid4().hex,
    }
    _, cleared = await app.asgi_client.post(endpoint, headers=_AUTH, json=cleared_command_by_name)
    assert cleared.status == 200 and cleared.json["record"]["network_ids"] == []
    for invalid_command_by_name in (
        {**command_by_name, "record_id": True},
        {**command_by_name, "fields": {"network_ids": [True], "group_id": None}},
        {**command_by_name, "fields": {"network_ids": [], "group_id": group["record_id"].upper()}},
    ):
        _, invalid = await app.asgi_client.post(endpoint, headers=_AUTH, json=invalid_command_by_name)
        assert invalid.status == 400
    assert await connection.fetchval(f'SELECT revision FROM "{schema}".company_registry_links') == 2
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


async def test_control_http_batch_exact_selection_replay_and_last_target_rollback(company_links_app):
    app, (connection, schema, engine) = company_links_app
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    second_company = await _draft(engine, schema, _create("company"), actor)
    selected_targets = [
        {
            "company_id": selected["record_id"],
            "expected_revision": 0,
            "network_ids": [networks[0]["record_id"]],
            "group_id": group["record_id"],
            "network_assertions": [_assertion(selected, networks[0], benefit_domain="dental")],
        }
        for selected in (company, second_company)
    ]
    command_by_name = {
        "targets": selected_targets,
        "reason": "Example explicitly selected companies",
        "idempotency_key": uuid4().hex,
        "selection_group_id": group["record_id"],
        "actor": {"kind": actor.kind, "user_id": str(actor.user_id), "client_id": actor.client_id},
    }
    path = "/api/v1/registry/manage/company_links/batch"
    _, denied = await app.asgi_client.post(path, json=command_by_name)
    assert denied.status == 403
    before = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    _, retained = await app.asgi_client.post(path, json=command_by_name, headers=_AUTH)
    assert retained.status == 200 and len(retained.json["records"]) == 2
    assert retained.json["custom_revision"] == before + 1
    assert retained.headers["Cache-Control"] == "private, no-store"
    assert {receipt["record_id"] for receipt in retained.json["records"]} == {
        company["record_id"],
        second_company["record_id"],
    }
    _, replay = await app.asgi_client.post(path, json=command_by_name, headers=_AUTH)
    assert replay.status == 200 and replay.json == retained.json
    stale_by_name = {
        **command_by_name,
        "idempotency_key": uuid4().hex,
        "targets": [{**selected_target, "expected_revision": 1} for selected_target in selected_targets],
    }
    stale_by_name["targets"][-1]["expected_revision"] = 0
    _, rejected = await app.asgi_client.post(path, json=stale_by_name, headers=_AUTH)
    assert rejected.status == 409
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry_links WHERE revision=1') == 2
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == before + 1
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0
    for invalid in (
        {**command_by_name, "targets": []},
        {**command_by_name, "targets": [selected_targets[0], selected_targets[0]]},
        {**command_by_name, "extra": True},
    ):
        _, rejected = await app.asgi_client.post(path, json=invalid, headers=_AUTH)
        assert rejected.status == 400


async def test_control_http_group_snapshot_narrows_companies_and_rejects_invalid_scope(company_links_app):
    app, (_, schema, engine) = company_links_app
    actor = _actor()
    company, group, networks = await _seed(engine, schema, actor)
    await _draft(engine, schema, _links(company, networks, group), actor)
    query_by_name = {"group_id": group["record_id"], "company_ids": [company["record_id"]], "limit": 50, "offset": 0}
    path = "/api/v1/registry/manage/company_links/group_snapshot"
    _, denied = await app.asgi_client.post(path, json=query_by_name)
    assert denied.status == 403
    _, accepted = await app.asgi_client.post(path, json=query_by_name, headers=_AUTH)
    assert accepted.status == 200 and accepted.json["records"][0]["company"]["company_id"] == company["record_id"]
    _, empty = await app.asgi_client.post(path, json={**query_by_name, "company_ids": []}, headers=_AUTH)
    assert empty.status == 200 and empty.json["records"] == []
    for invalid in (
        {**query_by_name, "company_ids": "all"},
        {**query_by_name, "company_ids": [company["record_id"]] * 2},
        {**query_by_name, "limit": True},
        {**query_by_name, "extra": True},
    ):
        _, rejected = await app.asgi_client.post(path, json=invalid, headers=_AUTH)
        assert rejected.status == 400
