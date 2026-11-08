# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Both native link writers and approval require one trusted current benefit set."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process.company_network_link_store import apply_company_network_link_batch
from process.registry_approval_store import approve_registry_records
from process.registry_benefit_plan_metadata import RegistryBenefitPlanSelectionInvalid
from process.registry_manual_undo import (
    MAX_UNDO_SNAPSHOT_BYTES,
    RegistryManualUndoCommand,
    prepare_registry_manual_undo,
    read_registry_manual_history,
)
from process.registry_record_store import (
    RegistryAddressUnavailable,
    RegistryRecordConflict,
    apply_registry_record_command,
)
from tests.test_company_network_link_store_postgres import _assertion, _batch, _seed, _state
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _command, _CountedConnection, _create, _draft
from tests.test_registry_benefit_plan_metadata import _client as _metadata_client
from tests.test_registry_benefit_plan_metadata import _document, _reply
from tests.test_registry_company_links_postgres import _assertion as _record_assertion
from tests.test_registry_company_links_postgres import _links
from tests.test_registry_company_links_postgres import _seed as _record_seed

pytestmark = pytest.mark.asyncio
_PLAN = "hpplan_" + "0" * 26


def _access_document(request, service_state_dict=None):
    import json

    selected = json.loads(request.content)
    state = service_state_dict or {}
    is_denied = state.get("access") == "denied"
    return {
        "client_id": state.get("response_client_id", selected["client_id"]),
        "allowed_ids": [] if is_denied else selected["healthporta_plan_ids"],
        "denied_ids": selected["healthporta_plan_ids"] if is_denied else [],
        "checked_at": "2026-10-15T07:00:00.123456Z",
    }


def _client(handler):
    def with_access(request):
        if request.url.path.endswith("/selected-access"):
            return _reply(_access_document(request))
        return handler(request)

    return _metadata_client(with_access)


def _service(service_state_dict):
    def handler(request):
        import json

        if request.url.path.endswith("/selected-access"):
            service_state_dict.setdefault("access_requests", []).append(request)
            if service_state_dict.get("access") == "unavailable":
                return _reply(status=503)
            return _reply(_access_document(request, service_state_dict))
        service_state_dict.setdefault("requests", []).append(request)
        body = _document(json.loads(request.content)["healthporta_plan_ids"])
        if service_state_dict["status"] == "unavailable":
            return _reply(status=503)
        if service_state_dict["status"] == "missing":
            body["items"], body["missing_ids"] = [], [_PLAN]
        else:
            for plan in body["items"]:
                plan["status"] = service_state_dict["status"]
        return _reply(body)

    return _metadata_client(handler)


async def _write(fixture, command, actor, **options):
    _, schema, engine = fixture
    async with async_sessionmaker(engine)() as session, session.begin():
        return await apply_registry_record_command(session, command, actor, schema=schema, **options)


async def _selected_record(fixture, actor):
    _, schema, engine = fixture
    company, _, networks = await _record_seed(engine, schema, actor)
    command = _links(company, networks)
    command = replace(
        command,
        fields=command.fields
        | {"network_assertions": [_record_assertion(company, networks[0], benefit_plan_ids=[_PLAN])]},
    )
    return company, networks, command


@pytest.mark.parametrize("status", ["active", "retired", "merged", "missing", "unavailable"])
async def test_selected_batch_validates_all_companies_and_replays_without_http(serving_schema, status):
    connection, schema, _ = serving_schema
    actor = _actor()
    companies, _, networks = await _seed(connection, schema)
    command = _batch(companies, networks)
    command = replace(
        command,
        targets=tuple(
            replace(
                batch_target,
                network_assertions=[_assertion(batch_target.company_id, networks[0], benefit_plan_ids=[_PLAN])],
            )
            for batch_target in command.targets
        ),
    )
    before = await _state(connection, schema)
    service_state_dict = {"status": status}
    async with _service(service_state_dict) as client, connection.transaction():
        if status != "active":
            failure = RegistryAddressUnavailable if status == "unavailable" else RegistryBenefitPlanSelectionInvalid
            with pytest.raises(failure):
                await apply_company_network_link_batch(
                    connection, command, actor, control_schema=schema, plan_metadata_client=client
                )
            assert await _state(connection, schema) == before
            return
        receipt = await apply_company_network_link_batch(
            connection, command, actor, control_schema=schema, plan_metadata_client=client
        )
        assert len(receipt["records"]) == 2 and len(service_state_dict["requests"]) == 1
        assert all(
            record_receipt["record"]["network_assertions"][0]["benefit_plan_ids"] == [_PLAN]
            for record_receipt in receipt["records"]
        )
        assert await apply_company_network_link_batch(connection, command, actor, control_schema=schema) == receipt
        audit = await connection.fetchval(
            f"SELECT event_json->'operation_context'->'benefit_plan_metadata' FROM \"{schema}\".registry_record_history WHERE record_kind='company_links' LIMIT 1"
        )
        assert audit is not None and "selected_ids_sha256" in audit


async def test_missing_config_refuses_plan_batch_but_preserves_prior_caller_work(serving_schema):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema)
    command = _batch(companies, networks)
    command = replace(
        command,
        targets=(
            replace(
                command.targets[0], network_assertions=[_assertion(companies[0], networks[0], benefit_plan_ids=[_PLAN])]
            ),
            command.targets[1],
        ),
    )
    before = await _state(connection, schema)
    async with connection.transaction():
        await connection.execute("CREATE TEMP TABLE caller_marker(marker integer) ON COMMIT DROP")
        await connection.execute("INSERT INTO caller_marker VALUES(7)")
        with pytest.raises(RegistryAddressUnavailable):
            await apply_company_network_link_batch(connection, command, _actor(), control_schema=schema)
        assert await connection.fetchval("SELECT marker FROM caller_marker") == 7
        assert await _state(connection, schema) == before


async def test_single_retained_fields_clear_undo_and_exact_replay(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    _, _, command = await _selected_record(serving_schema, actor)
    service_state_dict = {"status": "active"}
    async with _service(service_state_dict) as client:
        original = await _write(serving_schema, command, actor, plan_metadata_client=client)
        assert await _write(serving_schema, command, actor) == original
        preserved = replace(
            command,
            operation="correct",
            expected_revision=1,
            idempotency_key=uuid4().hex,
            fields={"network_ids": command.fields["network_ids"], "group_id": None},
        )
        with pytest.raises(RegistryAddressUnavailable):
            await _write(serving_schema, preserved, actor)
        clear = replace(preserved, fields=preserved.fields | {"network_assertions": []})
        await _write(serving_schema, clear, actor)
        undo = RegistryManualUndoCommand(
            "company_links", command.record_id, 2, 1, "Restore reviewed choices", uuid4().hex
        )
        async with async_sessionmaker(engine)() as session, session.begin():
            prepared = await prepare_registry_manual_undo(session, undo, actor, schema=schema)
        service_state_dict["status"] = "retired"
        with pytest.raises(RegistryBenefitPlanSelectionInvalid):
            await _write(
                serving_schema,
                prepared.command,
                actor,
                plan_metadata_client=client,
                undo_provenance=prepared.provenance,
            )
        service_state_dict["status"] = "active"
        restored = await _write(
            serving_schema, prepared.command, actor, plan_metadata_client=client, undo_provenance=prepared.provenance
        )
        assert restored["record"]["network_assertions"] == original["record"]["network_assertions"]
        assert await _write(serving_schema, prepared.command, actor, undo_provenance=prepared.provenance) == restored
        assert (
            await connection.fetchval(
                f'SELECT revision FROM "{schema}".company_registry_links WHERE company_id=$1', command.record_id
            )
            == 3
        )


async def test_archiving_is_http_free_and_restore_revalidates_retained_choices(serving_schema):
    actor = _actor()
    _, _, command = await _selected_record(serving_schema, actor)
    async with _service({"status": "active"}) as client:
        await _write(serving_schema, command, actor, plan_metadata_client=client)
        archive = replace(command, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex)
        assert (await _write(serving_schema, archive, actor))["record"]["archived"] is True
        restore = replace(archive, operation="restore", expected_revision=2, idempotency_key=uuid4().hex)
        with pytest.raises(RegistryAddressUnavailable):
            await _write(serving_schema, restore, actor)
        assert (await _write(serving_schema, restore, actor, plan_metadata_client=client))["record"][
            "archived"
        ] is False


async def test_approval_revalidates_selected_history_and_retains_replay(serving_schema):
    connection, schema, _ = serving_schema
    actor = _actor()
    company, networks, command = await _selected_record(serving_schema, actor)
    service_state_dict = {"status": "active"}
    async with _service(service_state_dict) as client:
        record = await _write(serving_schema, command, actor, plan_metadata_client=client)
        approval = await _command(connection, schema, company, *networks, record)
        service_state_dict["status"] = "retired"
        async with connection.transaction():
            with pytest.raises(RegistryBenefitPlanSelectionInvalid):
                await approve_registry_records(
                    connection, approval, actor, control_schema=schema, plan_metadata_client=client
                )
        assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0
        service_state_dict["status"] = "active"
        async with connection.transaction():
            receipt = await approve_registry_records(
                connection, approval, actor, control_schema=schema, plan_metadata_client=client
            )
            replay = await approve_registry_records(connection, approval, actor, control_schema=schema)
            assert replay["replayed"] is True and replay["approved_revision"] == receipt["approved_revision"]


async def test_metadata_preflight_releases_local_locks_and_keeps_revision_fence(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    _, _, command = await _selected_record(serving_schema, actor)
    async with _service({"status": "active"}) as client:
        await _write(serving_schema, command, actor, plan_metadata_client=client)
    replacement = replace(command, operation="correct", expected_revision=1, idempotency_key=uuid4().hex)
    clear = replace(replacement, fields=replacement.fields | {"network_assertions": []})
    committed_records = []

    async def concurrent(request):
        import json

        async with engine.connect() as other:
            await other.execute(text(f'SELECT id FROM "{schema}".registry_revision_control FOR UPDATE NOWAIT'))
        committed_records.append(await _write(serving_schema, clear, actor))
        return _reply(_document(json.loads(request.content)["healthporta_plan_ids"]))

    async with _client(concurrent) as client:
        with pytest.raises(RegistryRecordConflict):
            await _write(serving_schema, replacement, actor, plan_metadata_client=client)
    assert (
        await connection.fetchval(
            f'SELECT revision FROM "{schema}".company_registry_links WHERE company_id=$1', command.record_id
        )
        == committed_records[0]["revision"]
        == 2
    )


async def _repeated_benefit_record(fixture, network_count):
    _, schema, engine = fixture
    actor = _actor()
    company, _, networks = await _record_seed(engine, schema, actor)
    for _ in range(network_count - len(networks)):
        networks.append(await _draft(engine, schema, _create("network"), actor))
    plans = ["hpplan_" + f"{index:026X}" for index in range(512)]
    command = _links(company, networks)
    return actor, replace(
        command,
        reason="é" * 1000,
        fields=command.fields
        | {"network_assertions": [_record_assertion(company, network, benefit_plan_ids=plans) for network in networks]},
    )


async def test_repeated_benefit_ids_refuse_oversized_snapshot_without_mutation(serving_schema):
    connection, schema, engine = serving_schema
    actor, command = await _repeated_benefit_record(serving_schema, 64)
    before = await _state(connection, schema)
    service_state_dict = {"status": "active"}
    async with _service(service_state_dict) as client, async_sessionmaker(engine)() as session, session.begin():
        await session.execute(text("CREATE TEMP TABLE caller_marker(marker integer) ON COMMIT DROP"))
        await session.execute(text("INSERT INTO caller_marker VALUES(7)"))
        with pytest.raises(ValueError, match="registry_company_links_snapshot_limit"):
            await apply_registry_record_command(session, command, actor, schema=schema, plan_metadata_client=client)
        assert await session.scalar(text("SELECT marker FROM caller_marker")) == 7
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".company_registry_links')) == 0
        assert await _state(connection, schema) == before
        assert (
            await session.scalar(text(f'SELECT draft_revision FROM "{schema}".registry_revision_control')) == before[0]
        )
    assert await _state(connection, schema) == before
    assert len(service_state_dict["requests"]) == 1
    assert len(json.loads(service_state_dict["requests"][0].content)["healthporta_plan_ids"]) == 512


async def test_near_limit_benefit_snapshot_history_restore_and_undo_remain_readable(serving_schema):
    connection, schema, engine = serving_schema
    actor, command = await _repeated_benefit_record(serving_schema, 52)
    async with _service({"status": "active"}) as client:
        original = await _write(serving_schema, command, actor, plan_metadata_client=client)
        assert await _write(serving_schema, command, actor) == original
        archive = replace(command, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex)
        await _write(serving_schema, archive, actor)
        restore = replace(archive, operation="restore", expected_revision=2, idempotency_key=uuid4().hex)
        await _write(serving_schema, restore, actor, plan_metadata_client=client)
        clear = replace(
            command,
            operation="correct",
            expected_revision=3,
            idempotency_key=uuid4().hex,
            fields=command.fields | {"network_assertions": []},
        )
        await _write(serving_schema, clear, actor)
        undo = RegistryManualUndoCommand(
            "company_links", command.record_id, 4, 1, "Restore reviewed choices", uuid4().hex
        )
        async with async_sessionmaker(engine)() as session, session.begin():
            prepared = await prepare_registry_manual_undo(session, undo, actor, schema=schema)
        restored = await _write(
            serving_schema, prepared.command, actor, plan_metadata_client=client, undo_provenance=prepared.provenance
        )
    assert restored["record"]["network_assertions"] == original["record"]["network_assertions"]
    sizes = await connection.fetch(
        f"SELECT record_json::text AS snapshot_text, octet_length(record_json::text) AS snapshot_bytes, "
        f"octet_length(record_json::text)+octet_length(reason)+256 AS page_bytes "
        f"FROM \"{schema}\".registry_record_history WHERE record_kind='company_links' ORDER BY revision"
    )
    assert len(sizes) == 5
    assert sizes[0]["snapshot_bytes"] > MAX_UNDO_SNAPSHOT_BYTES * 0.9
    assert all(history_size["page_bytes"] <= MAX_UNDO_SNAPSHOT_BYTES for history_size in sizes)
    assert all(
        len(json.dumps(json.loads(history_size["snapshot_text"]), ensure_ascii=False, allow_nan=False).encode())
        == history_size["snapshot_bytes"]
        for history_size in sizes
    )
    async with async_sessionmaker(engine)() as session, session.begin():
        for offset in range(5):
            history = await read_registry_manual_history(
                session, "company_links", command.record_id, limit=1, offset=offset, schema=schema
            )
            assert len(history["records"]) == 1
            assert (
                len(json.dumps(history, ensure_ascii=False, separators=(",", ":")).encode()) <= MAX_UNDO_SNAPSHOT_BYTES
            )
            if offset != 1:
                assert history["records"][0]["record"]["network_assertions"] == original["record"]["network_assertions"]


async def _repeated_benefit_batch(fixture, network_count, *, middle_evidence=539):
    connection, schema, _ = fixture
    companies, _, networks = await _seed(connection, schema, networks=network_count)
    plans = ["hpplan_" + f"{index:026X}" for index in range(512)]
    command = _batch(companies, networks)
    return replace(
        command,
        reason="é" * 1000,
        targets=(
            replace(
                command.targets[0],
                network_assertions=[
                    _assertion(
                        companies[0],
                        network,
                        benefit_plan_ids=plans,
                        evidence_text="e" * (1000 if index < 10 else middle_evidence if index == 10 else 1),
                    )
                    for index, network in enumerate(networks)
                ],
            ),
            command.targets[1],
        ),
    )


@pytest.mark.parametrize(
    "reason,middle_evidence,sql_overflow", [("é" * 1000, 539, True), ("a" + "\x01" * 999, 139, False)]
)
async def test_batch_refuses_unreadable_history_even_when_receipt_fits(
    serving_schema, reason, middle_evidence, sql_overflow
):
    connection, schema, _ = serving_schema

    class AccountedConnection(_CountedConnection):
        async def fetchrow(self, *arguments):
            row = await super().fetchrow(*arguments)
            if row is not None and "receipt_bytes" in row:
                assert row["receipt_bytes"] <= MAX_UNDO_SNAPSHOT_BYTES
                assert row["history_bytes"] > MAX_UNDO_SNAPSHOT_BYTES
                snapshots = json.loads(row["receipt_json"])["records"]
                sql_bytes = (
                    max(len(json.dumps(item["record"], ensure_ascii=False).encode()) for item in snapshots)
                    + len(reason.encode())
                    + 256
                )
                assert (sql_bytes > MAX_UNDO_SNAPSHOT_BYTES) is sql_overflow
            return row

    counted = AccountedConnection(connection)
    command = replace(await _repeated_benefit_batch(serving_schema, 54, middle_evidence=middle_evidence), reason=reason)
    await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=9223372036854775805')
    before = await _state(connection, schema)
    async with _service({"status": "active"}) as client, connection.transaction():
        await connection.execute("CREATE TEMP TABLE caller_marker(marker integer) ON COMMIT DROP")
        await connection.execute("INSERT INTO caller_marker VALUES(7)")
        with pytest.raises(ValueError, match="registry_company_links_snapshot_limit"):
            await apply_company_network_link_batch(
                counted, command, _actor(), control_schema=schema, plan_metadata_client=client
            )
        assert await connection.fetchval("SELECT marker FROM caller_marker") == 7
        assert await _state(connection, schema) == before
    assert await _state(connection, schema) == before


async def test_near_limit_batch_history_replay_and_undo_remain_readable(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    command = await _repeated_benefit_batch(serving_schema, 52)
    async with _service({"status": "active"}) as client, connection.transaction():
        receipt = await apply_company_network_link_batch(
            connection, command, actor, control_schema=schema, plan_metadata_client=client
        )
        before_replay = await _state(connection, schema)
        assert await apply_company_network_link_batch(connection, command, actor, control_schema=schema) == receipt
        assert await _state(connection, schema) == before_replay
    company_id = command.targets[0].company_id
    async with async_sessionmaker(engine)() as session, session.begin():
        history = await read_registry_manual_history(session, "company_links", company_id, limit=1, schema=schema)
    assert len(history["records"]) == 1
    assert len(json.dumps(history, ensure_ascii=False).encode()) > MAX_UNDO_SNAPSHOT_BYTES * 0.9
    original = history["records"][0]["record"]
    clear = _links(
        {"record_id": str(company_id)}, [{"record_id": network} for network in command.targets[0].network_ids]
    )
    clear = replace(
        clear,
        operation="correct",
        expected_revision=1,
        fields={"network_ids": list(command.targets[0].network_ids), "group_id": None, "network_assertions": []},
    )
    await _write(serving_schema, clear, actor)
    undo = RegistryManualUndoCommand("company_links", company_id, 2, 1, "Restore reviewed choices", uuid4().hex)
    async with async_sessionmaker(engine)() as session, session.begin():
        prepared = await prepare_registry_manual_undo(session, undo, actor, schema=schema)
    async with _service({"status": "active"}) as client:
        restored = await _write(
            serving_schema, prepared.command, actor, plan_metadata_client=client, undo_provenance=prepared.provenance
        )
    assert restored["record"]["network_assertions"] == original["network_assertions"]
    assert (await _state(connection, schema))[1] == 0
