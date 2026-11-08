# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native COPY, scoped source CAS and atomic durable batch acceptance."""

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from process import network_source_binding_store as store
from process import registry_fhir_catalog_fingerprints as catalog_fingerprints
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.registry_fhir_catalog_extraction import FHIRCatalogOrganization
from process.registry_fhir_catalog_projection import (
    BINDING_COLUMNS,
    NETWORK_COLUMNS,
    SourceCatalogContext,
    format_fhir_catalog_projection,
)
from process.registry_publication_execution import _command_actor
from process.registry_record_store import RegistryAddressUnavailable, RegistryRecordConflict
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_network_source_binding_schema_postgres import network_binding_db
from tests.test_registry_approval_store_postgres import _CountedConnection
from tests.test_registry_fhir_catalog_custody_postgres import (
    freeze_metadata,
    metadata_custody,
    seed_observations,
    source_descriptor,
)
from tests.test_registry_record_store_postgres import _actor, record_db

pytestmark = pytest.mark.asyncio


async def _seed(connection, schema, count=2):
    """Create canonical native identity and active management heads set-wise."""
    networks = await connection.fetchval(
        f'WITH allocated AS (INSERT INTO "{schema}".network_registry_identity(allocation_key) '
        "SELECT md5($1::text||number::text)::uuid FROM generate_series(1,$2::integer) number RETURNING network_id) "
        "SELECT array_agg(network_id ORDER BY network_id) FROM allocated",
        uuid4().hex,
        count,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_record(network_id,display_name) '
        "SELECT network_id,'Example Network' FROM unnest($1::integer[]) network(network_id)",
        networks,
    )
    return networks


def _row(network_id, **changes):
    return {
        "binding_id": str(uuid4()),
        "source_system": "ptg",
        "source_id": "synthetic-source",
        "dataset_schema": "synthetic_dataset",
        "dataset_id": "dataset-one",
        "producer_id": "producer-one",
        "edition_id": "edition-one",
        "source_key": "opaque-network-key",
        "source_scope_json": {"cohort_id": "cohort-one", "snapshot_id": "snapshot-one", "company_key": "company-one"},
        "network_id": network_id,
        "evidence_id": "synthetic-review",
        "evidence_sha256": "a" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
        **changes,
    }


def _command(rows):
    return NetworkSourceBindingBatchCommand(
        json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode(),
        "Reviewed exact source scope",
        uuid4().hex,
    )


async def _apply(connection, schema, command, actor):
    async with connection.transaction():
        return await apply_network_source_binding_batch(connection, command, actor, control_schema=schema)


async def _state(connection, schema):
    return tuple(
        await connection.fetchrow(
            f'SELECT draft_revision,approved_revision,(SELECT count(*) FROM "{schema}".registry_network_binding),'
            f"(SELECT count(*) FROM \"{schema}\".registry_record_history WHERE record_kind='network_binding'),"
            f'(SELECT count(*) FROM "{schema}".registry_network_binding_batch) FROM "{schema}".registry_revision_control'
        )
    )


async def test_bind_read_and_exact_replay_preserve_approval(network_binding_db, monkeypatch):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    input_row = _row(networks[0])
    command = _command([input_row])
    receipt = await _apply(connection, schema, command, actor)
    assert set(receipt) == {"custom_revision", "records"}
    assert receipt == {
        "custom_revision": 1,
        "records": [
            {
                "record_kind": "network_binding",
                "record_id": input_row["binding_id"],
                "revision": 1,
                "network_id": networks[0],
                "archived": False,
            }
        ],
    }
    heads = await store.list_network_source_bindings(connection, networks[0], control_schema=schema)
    assert len(heads) == 1 and heads[0]["binding_id"] == input_row["binding_id"]
    assert heads[0]["source_scope_json"] == input_row["source_scope_json"]
    assert len(heads[0]["binding_key"]) == 64 and heads[0]["revision"] == 1
    assert await store.list_network_source_bindings(connection, networks[1], control_schema=schema) == []
    assert (
        await store.list_network_source_bindings(
            connection,
            networks[0],
            source_system="ptg",
            binding_key=heads[0]["binding_key"],
            control_schema=schema,
        )
        == heads
    )
    monkeypatch.setattr(store, "_fast_module", lambda: None)
    assert await _apply(connection, schema, command, actor) == receipt
    assert await _state(connection, schema) == (1, 0, 1, 1, 1)


async def test_rebind_close_reopen_retain_immutable_full_history(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    original = _row(networks[0])
    await _apply(connection, schema, _command([original]), actor)
    for operation, old_revision, previous, target_network_id, archived in [
        ("rebind", 1, networks[0], networks[1], False),
        ("close", 2, networks[1], networks[1], True),
        ("rebind", 3, networks[1], networks[0], False),
    ]:
        changed_by_field = {
            **original,
            "operation": operation,
            "expected_revision": old_revision,
            "expected_network_id": previous,
            "network_id": target_network_id,
            "evidence_id": f"review-{old_revision}",
        }
        receipt = await _apply(connection, schema, _command([changed_by_field]), actor)
        assert receipt["records"][0]["revision"] == old_revision + 1
        assert receipt["records"][0]["archived"] is archived
    history = await connection.fetch(
        f'SELECT revision,record_json,actor_json,custom_revision,idempotency_key,event_json FROM "{schema}".registry_record_history '
        "WHERE record_kind='network_binding' ORDER BY revision"
    )
    assert [entry["revision"] for entry in history] == [1, 2, 3, 4]
    events = [json.loads(entry["event_json"]) for entry in history]
    assert [event["operation"] for event in events] == ["bind", "rebind", "close", "rebind"]
    assert [event["before_revision"] for event in events] == [0, 1, 2, 3]
    assert all(event["operation_context"]["idempotency_key"] for event in events)
    documents = [json.loads(entry["record_json"]) for entry in history]
    assert [document["network_id"] for document in documents] == [networks[0], networks[1], networks[1], networks[0]]
    assert [document["archived"] for document in documents] == [False, False, True, False]
    assert len({document["binding_key"] for document in documents}) == 1
    assert all(document["source_scope_json"] == original["source_scope_json"] for document in documents)
    assert len({entry["idempotency_key"] for entry in history}) == 4
    assert all(json.loads(entry["actor_json"])["user_id"] == str(actor.user_id) for entry in history)
    assert await _state(connection, schema) == (4, 0, 1, 4, 4)


async def test_scope_edition_and_key_conflicts_are_explicit(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    first = _row(networks[0])
    await _apply(connection, schema, _command([first]), actor)
    second_by_field = {**first, "binding_id": str(uuid4()), "edition_id": "edition-two"}
    await _apply(connection, schema, _command([second_by_field]), actor)
    assert await _state(connection, schema) == (2, 0, 2, 2, 2)
    for changed in [
        {**first, "binding_id": str(uuid4())},
        {
            **first,
            "operation": "rebind",
            "expected_revision": 1,
            "expected_network_id": networks[0],
            "edition_id": "wrong-edition",
        },
        {
            **first,
            "operation": "rebind",
            "expected_revision": 1,
            "expected_network_id": networks[0],
            "binding_id": str(uuid4()),
        },
        {**first, "operation": "rebind", "expected_revision": 1, "expected_network_id": networks[1]},
    ]:
        with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
            await _apply(connection, schema, _command([changed]), actor)
        assert await _state(connection, schema) == (2, 0, 2, 2, 2)


async def test_stale_member_rejects_whole_selected_batch(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    first = _row(networks[0])
    await _apply(connection, schema, _command([first]), actor)
    valid_new = _row(networks[1], source_key="second-key")
    stale_by_field = {**first, "operation": "rebind", "expected_revision": 2, "expected_network_id": networks[0]}
    with pytest.raises(RegistryRecordConflict):
        await _apply(connection, schema, _command([valid_new, stale_by_field]), actor)
    assert await _state(connection, schema) == (1, 0, 1, 1, 1)
    heads = await store.list_network_source_bindings(connection, networks[1], control_schema=schema)
    assert heads == []


async def test_replay_binds_exact_bytes_reason_and_complete_actor(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    command = _command([_row(networks[0])])
    receipt = await _apply(connection, schema, command, actor)
    for changed in [
        replace(command, input_bytes=command.input_bytes + b" "),
        replace(command, reason="Different review"),
    ]:
        with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
            await _apply(connection, schema, changed, actor)
    for changed_actor in [
        replace(actor, user_id=uuid4()),
        replace(actor, client_id="another_client"),
        replace(actor, impersonator_id=uuid4()),
    ]:
        with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
            await _apply(connection, schema, command, changed_actor)
    other = replace(_command([_row(networks[1], source_key="other-key")]), idempotency_key=command.idempotency_key)
    await _apply(connection, schema, other, replace(actor, user_id=uuid4()))
    assert await _apply(connection, schema, command, actor) == receipt
    assert await _state(connection, schema) == (2, 0, 2, 2, 2)


async def test_archived_network_allows_closure_but_not_bind_or_reopen(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    original = _row(networks[0])
    await _apply(connection, schema, _command([original]), actor)
    await connection.execute(
        f'UPDATE "{schema}".network_registry_record SET archived=true WHERE network_id=$1', networks[0]
    )
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(connection, schema, _command([_row(networks[0], source_key="other")]), actor)
    close_by_field = {**original, "operation": "close", "expected_revision": 1, "expected_network_id": networks[0]}
    receipt = await _apply(connection, schema, _command([close_by_field]), actor)
    assert receipt["records"][0]["archived"] is True
    reopen_by_field = {**close_by_field, "operation": "rebind", "expected_revision": 2}
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(connection, schema, _command([reopen_by_field]), actor)
    with pytest.raises(RegistryRecordConflict):
        await _apply(connection, schema, _command([{**close_by_field, "expected_revision": 2}]), actor)
    assert await _state(connection, schema) == (2, 0, 1, 2, 2)


async def test_rebind_preserves_observational_fhir_alias(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    legacy = str(uuid4())
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_alias(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) '
        "VALUES('fhir','synthetic-source','legacy_fhir_uuid',$1,'alias-one',$2,'review-alias')",
        legacy,
        networks[0],
    )
    original = _row(
        networks[0],
        source_system="fhir",
        source_scope_json={
            "organization_id": "Organization-1",
            "legacy_uuid": legacy,
            "alias_scope": "alias-one",
        },
    )
    await _apply(connection, schema, _command([original]), actor)
    conflicting_by_field = {
        **original,
        "network_id": networks[1],
        "operation": "rebind",
        "expected_revision": 1,
        "expected_network_id": networks[0],
    }
    corrected = await _apply(connection, schema, _command([conflicting_by_field]), actor)
    assert corrected["records"][0]["network_id"] == networks[1]
    separate_by_field = {
        **original,
        "binding_id": str(uuid4()),
        "network_id": networks[1],
        "source_scope_json": {**original["source_scope_json"], "alias_scope": "alias-two"},
    }
    await _apply(connection, schema, _command([separate_by_field]), actor)
    assert await connection.fetchval(f'SELECT network_id FROM "{schema}".network_registry_alias') == networks[0]
    assert await _state(connection, schema) == (3, 0, 2, 3, 3)


class _CountedCopyConnection(_CountedConnection):
    def __init__(self, connection):
        super().__init__(connection)
        self.copies = 0

    async def copy_to_table(self, *arguments, **keywords):
        self.copies += 1
        return await self.connection.copy_to_table(*arguments, **keywords)


async def test_one_and_hundred_rows_use_fixed_statements_and_one_copy(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    counts = []
    for count in [1, 100]:
        wrapped = _CountedCopyConnection(connection)
        rows = [_row(networks[0], source_key=f"key-{count}-{index}") for index in range(count)]
        receipt = await _apply(wrapped, schema, _command(rows), actor)
        assert len(receipt["records"]) == count
        assert receipt["records"] == sorted(receipt["records"], key=lambda record: record["record_id"])
        assert all(record["revision"] == 1 for record in receipt["records"])
        assert wrapped.copies == 1
        counts.append(wrapped.statements)
    assert counts == [8, 8]
    assert await _state(connection, schema) == (2, 0, 101, 101, 2)


async def test_invalid_native_last_row_and_unavailable_leave_prior_work(network_binding_db, monkeypatch):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    invalid = _row(True, source_key="bad-key")
    async with connection.transaction():
        await connection.execute(f"UPDATE \"{schema}\".network_registry_record SET display_name='Retained prior work'")
        with pytest.raises(ValueError, match="input_invalid"):
            await apply_network_source_binding_batch(
                connection, _command([_row(networks[0]), invalid]), actor, control_schema=schema
            )
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0
        monkeypatch.setattr(store, "_fast_module", lambda: None)
        with pytest.raises(RegistryAddressUnavailable):
            await apply_network_source_binding_batch(
                connection, _command([_row(networks[0])]), actor, control_schema=schema
            )
    assert (
        await connection.fetchval(f'SELECT min(display_name) FROM "{schema}".network_registry_record')
        == "Retained prior work"
    )
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


@pytest.mark.parametrize("mode", ["driver_failure", "wrong_count", "unconsumed"])
async def test_copy_failure_rolls_back_stage_and_batch(network_binding_db, monkeypatch, mode):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    wrapped = _CountedCopyConnection(connection)

    async def faulty_copy(*arguments, **keywords):
        status = await connection.copy_to_table(*arguments, **keywords)
        if mode == "driver_failure":
            raise RuntimeError("synthetic driver failure")
        if mode == "unconsumed":
            keywords["source"].seek(0)
        return "COPY 0" if mode == "wrong_count" else status

    monkeypatch.setattr(wrapped, "copy_to_table", faulty_copy)
    async with connection.transaction():
        with pytest.raises(RegistryAddressUnavailable):
            await apply_network_source_binding_batch(
                wrapped, _command([_row(networks[0])]), actor, control_schema=schema
            )
        assert await connection.fetchval("SELECT 1") == 1
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_source_binding_%'"
            )
            == 0
        )
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_transaction_missing_references_and_read_bounds_are_strict(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    command = _command([_row(networks[0])])
    with pytest.raises(ValueError, match="requires_caller_transaction"):
        await apply_network_source_binding_batch(connection, command, actor, control_schema=schema)
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(connection, schema, _command([_row(2147483647)]), actor)
    for arguments in [
        {"network_id": True},
        {"network_id": 0},
        {"network_id": 2147483648},
        {"source_system": "label"},
        {"binding_key": "A" * 64},
        {"limit": True},
        {"limit": 101},
        {"offset": -1},
    ]:
        with pytest.raises(ValueError):
            await store.list_network_source_bindings(
                connection, **{"network_id": networks[0], **arguments}, control_schema=schema
            )
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_caller_rollback_retains_no_batch_heads_or_history(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with connection.transaction():
            await apply_network_source_binding_batch(
                connection, _command([_row(networks[0])]), actor, control_schema=schema
            )
            raise RuntimeError("caller rollback")
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_corrupt_native_receipts_fail_closed_before_copy(network_binding_db, monkeypatch):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    command = _command([_row(networks[0])])
    copy_bytes, row_count = store._encode(command.input_bytes)
    for encoded in [
        [copy_bytes, row_count],
        (copy_bytes,),
        (copy_bytes, True),
        (copy_bytes, 5001),
        ("not-bytes", row_count),
        (b"invalid", row_count),
        (copy_bytes + b"x", row_count),
        (store._COPY_HEADER + b"x" * store._MAX_COPY_BYTES + b"\xff\xff", row_count),
    ]:
        monkeypatch.setattr(
            store, "_fast_module", lambda: SimpleNamespace(encode_network_source_binding_batch=lambda _: encoded)
        )
        with pytest.raises(RegistryAddressUnavailable, match="native_unavailable"):
            await _apply(connection, schema, command, actor)
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_empty_and_malformed_control_inputs_are_rejected(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    command = _command([_row(networks[0])])
    for changed in [
        replace(command, input_bytes=b"[]"),
        replace(command, input_bytes=b""),
        replace(command, input_bytes=b"x" * (store._MAX_INPUT_BYTES + 1)),
        replace(command, input_bytes=bytearray(command.input_bytes)),
        replace(command, reason=" "),
        replace(command, idempotency_key=" padded"),
        replace(command, idempotency_key="a\nb"),
    ]:
        with pytest.raises(ValueError):
            await _apply(connection, schema, changed, actor)
    with pytest.raises(ValueError, match="uuid_invalid"):
        await _apply(connection, schema, command, replace(actor, user_id=str(actor.user_id)))
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def _source_batch_networks(engine, schema, actor, count):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from process.registry_record_store import apply_registry_record_command
    from tests.test_registry_approval_store_postgres import _create

    records = []
    async with async_sessionmaker(engine)() as session, session.begin():
        for _ in range(count):
            records.append(await apply_registry_record_command(session, _create("network"), actor, schema=schema))
    return records


async def _source_batch_catalog(fixture, networks):
    """Build genuine source metadata through its bounded projector and custody sealer."""

    snapshot = await seed_observations(fixture, len(networks))
    await fixture.connection.copy_records_to_table(
        "network_registry_identity",
        schema_name=fixture.control_schema,
        columns=("network_id",),
        records=[(network_record["record_id"],) for network_record in networks],
    )
    for offset in range(0, len(networks), 500):
        organizations = tuple(
            FHIRCatalogOrganization(
                f"organization-{ordinal:04d}",
                f"organization-{ordinal:04d}",
                (),
                False,
                "active",
                (),
                hashlib.sha256(f"organization-{ordinal:04d}".encode()).hexdigest(),
            )
            for ordinal in range(offset, min(offset + 500, len(networks)))
        )
        context = SourceCatalogContext(
            RegistryNetworkSourceCoordinates(**source_descriptor()["coordinates"]),
            snapshot["source_vector_json"][0]["source_generation"],
            snapshot["metadata_generation"],
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            "raw_fhir",
            legacy_aliases=tuple(
                (network_record.resource_id, str(uuid4()), "example-scope") for network_record in organizations
            ),
        )
        projection = format_fhir_catalog_projection(
            organizations,
            (),
            tuple(
                (network_record.resource_id, networks[offset + index]["record_id"])
                for index, network_record in enumerate(organizations)
            ),
            context,
        )
        for table, columns, projection_rows in (
            ("registry_fhir_catalog_network", NETWORK_COLUMNS, projection.network_rows),
            ("registry_fhir_catalog_binding", BINDING_COLUMNS, projection.binding_rows),
        ):
            await fixture.connection.copy_records_to_table(
                table,
                schema_name=fixture.target.schema_name,
                columns=columns,
                records=projection_rows,
            )
    return await freeze_metadata(fixture, snapshot=snapshot)


async def _source_batch_drafts(connection, schema, actor, networks, coordinates_by_field):
    references = []
    for offset in range(0, len(networks), 100):
        projection_rows = [
            _row(
                network_record["record_id"],
                **coordinates_by_field,
                source_key=f"organization-{offset + index:04d}",
                source_scope_json={"organization_id": f"organization-{offset + index:04d}", "capture_sha256": "a" * 64},
            )
            for index, network_record in enumerate(networks[offset : offset + 100])
        ]
        command_by_field = _command(projection_rows)
        receipt = await _apply(connection, schema, command_by_field, actor)
        references.append(
            {
                "idempotency_key": command_by_field.idempotency_key,
                "request_sha256": store._metadata(command_by_field, actor)[3],
                "custom_revision": receipt["custom_revision"],
            }
        )
    return references


@pytest.fixture
async def source_batch_case(serving_schema, metadata_custody, request):
    connection, schema, engine = serving_schema
    actor = _actor()
    networks = await _source_batch_networks(engine, schema, actor, getattr(request, "param", 3))
    snapshot = await _source_batch_catalog(metadata_custody, networks)
    snapshot["published_at"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await connection.execute(
        f'INSERT INTO "{schema}".registry_fhir_catalog_snapshot '
        f'SELECT * FROM jsonb_populate_record(NULL::"{schema}".registry_fhir_catalog_snapshot,$1::jsonb)',
        json.dumps(snapshot, default=str),
    )
    coordinates_by_field = {
        "source_system": "fhir",
        "source_id": "sample-fhir",
        "dataset_schema": "sample_source",
        "dataset_id": "dataset-one",
        "producer_id": "registry-source-fhir-witness-v1",
        "edition_id": str(uuid4()),
    }
    references = await _source_batch_drafts(connection, schema, actor, networks, coordinates_by_field)
    heads = await connection.fetchrow(f'SELECT * FROM "{schema}".registry_revision_control')
    command_by_field = {
        "operation": "publish_source_binding_batch",
        "batch_references": references,
        "source_coordinates": coordinates_by_field,
        "metadata_generation": snapshot["metadata_generation"],
        "selection_count": None,
        "selection_sha256": None,
        "expected_draft_revision": heads["draft_revision"],
        "expected_approved_revision": 0,
        "expected_serving_generation": 0,
        "reason": "Review complete Source coverage",
        "idempotency_key": uuid4().hex,
    }
    return SimpleNamespace(
        connection=connection,
        schema=schema,
        engine=engine,
        actor=actor,
        networks=networks,
        command=command_by_field,
        snapshot=snapshot,
    )


async def _seed_source_batch_catalog_aliases(case):
    from process.registry_fhir_catalog_identity import ALIAS_SCOPE

    await case.connection.copy_records_to_table(
        "network_registry_alias",
        schema_name=case.schema,
        columns=("source_system", "source_id", "alias_type", "alias_value", "scope_key", "network_id", "evidence_id"),
        records=[
            (
                "fhir",
                "sample-fhir",
                "fhir_organization",
                f"organization-{index:04d}",
                ALIAS_SCOPE,
                network["record_id"],
                "synthetic-review",
            )
            for index, network in enumerate(case.networks)
        ],
    )


async def test_source_batch_approval_retains_catalog_serving_maps(source_batch_case):
    """A real saved batch leaves pending catalog reads on the last-serving maps."""
    from process import registry_source_binding_publication as source_batch
    from process.registry_approval_store import approve_registry_records

    case = source_batch_case
    serving_revision = case.command["expected_approved_revision"]
    await _seed_source_batch_catalog_aliases(case)
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        serving_fingerprints = await catalog_fingerprints.approved_fhir_catalog_fingerprints(
            case.connection, (serving_revision,), control_schema=case.schema
        )
        compact, resolved = await source_batch.resolve_source_batch(
            case.connection, case.command, case.actor, control_schema=case.schema, allow_unbound=True
        )
    assert compact["selection_count"] == len(resolved.selection) == 6
    assert {
        selected_version["record_id"]
        for selected_version in resolved.selection
        if selected_version["record_kind"] == "network"
    } == {network["record_id"] for network in case.networks}
    assert sum(selected_version["record_kind"] == "network_binding" for selected_version in resolved.selection) == 3
    async with case.connection.transaction():
        queued = await source_batch.enqueue_source_batch(
            case.connection, compact, case.actor, "b" * 64, control_schema=case.schema
        )
    request_query = f'SELECT * FROM "{case.schema}".registry_publication_request WHERE request_id=$1'
    request_id = UUID(queued["request_id"])
    queued_record = await case.connection.fetchrow(request_query, request_id)
    command, actor, _, _ = _command_actor(queued_record)
    assert command == resolved and actor == case.actor
    async with case.connection.transaction():
        approved = await approve_registry_records(case.connection, command, actor, control_schema=case.schema)
    assert approved == {
        "approved_revision": case.command["expected_draft_revision"] + 1,
        "previous_approved_revision": serving_revision,
        "selected_count": 6,
        "replayed": False,
    }
    async with case.connection.transaction():
        replay = await approve_registry_records(case.connection, command, actor, control_schema=case.schema)
    assert replay == {**approved, "replayed": True}
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        selected_revision, selected_fingerprints = await catalog_fingerprints.select_fhir_catalog_mapping(
            case.connection,
            approved["approved_revision"],
            SimpleNamespace(approved_custom_revision=serving_revision),
            control_schema=case.schema,
        )
        assert (selected_revision, selected_fingerprints) == (serving_revision, serving_fingerprints[serving_revision])
        assert tuple(
            await case.connection.fetchrow(
                f'SELECT draft_revision,approved_revision FROM "{case.schema}".registry_revision_control'
            )
        ) == (approved["approved_revision"], approved["approved_revision"])
        assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_approval_history') == 1
        assert source_batch.stored_source_batch(await case.connection.fetchrow(request_query, request_id)) == (
            compact,
            resolved,
        )


@pytest.mark.parametrize("source_batch_case", [1301], indirect=True)
async def test_source_batch_saved_chunks_freeze_complete_selection_and_one_approval(source_batch_case):
    """A full Source resolves real draft history into one bounded native approval."""
    from process import registry_source_binding_publication as source_batch
    from process.registry_approval_store import approve_registry_records
    from process.registry_publication_execution import _command_actor

    case = source_batch_case
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        compact, resolved = await source_batch.resolve_source_batch(
            case.connection,
            case.command,
            case.actor,
            control_schema=case.schema,
            allow_unbound=True,
        )
    assert len(case.command["batch_references"]) == 14
    assert compact["selection_count"] == len(resolved.selection) == 2602
    assert {
        selected_version["record_id"]
        for selected_version in resolved.selection
        if selected_version["record_kind"] == "network"
    } == {selected_version["record_id"] for selected_version in case.networks}
    async with case.connection.transaction():
        queued = await source_batch.enqueue_source_batch(
            case.connection,
            compact,
            case.actor,
            "b" * 64,
            control_schema=case.schema,
        )
    request_id = UUID(queued["request_id"])
    selected_version = await case.connection.fetchrow(
        f'SELECT * FROM "{case.schema}".registry_publication_request WHERE request_id=$1',
        request_id,
    )
    command, actor, _, _ = _command_actor(selected_version)
    assert command == resolved and actor == case.actor
    async with case.connection.transaction():
        approved = await approve_registry_records(case.connection, command, actor, control_schema=case.schema)
    assert approved["selected_count"] == 2602
    assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_approval_history') == 1
    assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_approved_record') == 2602
    assert await case.connection.fetchval(f'SELECT generation_id FROM "{case.schema}".network_serving_control') is None
    async with case.connection.transaction():
        replay = await source_batch.enqueue_source_batch(
            case.connection,
            compact,
            case.actor,
            "b" * 64,
            control_schema=case.schema,
        )
    assert replay == {**queued, "replayed": True}
    await _assert_queued_source_batch_pages(case, request_id, compact, resolved)


async def _assert_queued_source_batch_pages(case, request_id, compact, resolved):
    from process import registry_source_binding_publication as source_batch
    from process.registry_publication_queue import read_registry_publication_request

    seen_versions = []
    for offset in range(0, compact["selection_count"], 100):
        page = await source_batch.queued_source_batch_page(
            case.connection,
            request_id,
            case.actor,
            offset,
            control_schema=case.schema,
        )
        seen_versions.extend(page["selection"])
    assert seen_versions == list(resolved.selection)
    status = await read_registry_publication_request(case.connection, request_id, control_schema=case.schema)
    assert status["selection_count"] == 2602 and "selection" not in status
    assert status["selection_sha256"] == compact["selection_sha256"]


async def test_source_batch_complete_coverage_and_original_history_refuse_tamper(source_batch_case):
    """Already approved targets reduce selections; stale or altered predecessors refuse."""
    from process import registry_source_binding_publication as source_batch
    from process.registry_approval_store import RegistryApprovalConflict, approve_registry_records
    from tests.test_registry_approval_store_postgres import _command as approval_command

    case = source_batch_case
    async with case.connection.transaction():
        await approve_registry_records(
            case.connection,
            await approval_command(case.connection, case.schema, case.networks[0]),
            case.actor,
            control_schema=case.schema,
        )
    approved_revision = await case.connection.fetchval(
        f'SELECT approved_revision FROM "{case.schema}".registry_revision_control'
    )
    document_by_field = dict(
        case.command, expected_draft_revision=approved_revision, expected_approved_revision=approved_revision
    )
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        compact, command = await source_batch.resolve_source_batch(
            case.connection,
            document_by_field,
            case.actor,
            control_schema=case.schema,
            allow_unbound=True,
        )
    assert compact["selection_count"] == 5
    assert not any(
        selected_version["record_kind"] == "network" and selected_version["record_id"] == case.networks[0]["record_id"]
        for selected_version in command.selection
    )
    await _assert_source_batch_reference_refusals(case, document_by_field, compact)
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        async with case.connection.transaction():
            await case.connection.execute(
                f"DELETE FROM \"{case.schema}\".registry_record_history WHERE record_kind='network_binding' "
                "AND record_key=(SELECT min(record_key) FROM "
                f"\"{case.schema}\".registry_record_history WHERE record_kind='network_binding')"
            )
            await source_batch.resolve_source_batch(
                case.connection, document_by_field, case.actor, control_schema=case.schema, allow_unbound=True
            )
    with pytest.raises(RegistryApprovalConflict):
        async with case.connection.transaction():
            await case.connection.execute(
                f'UPDATE "{case.schema}".registry_revision_control SET draft_revision=draft_revision+1'
            )
            await source_batch.enqueue_source_batch(
                case.connection, compact, case.actor, "b" * 64, control_schema=case.schema
            )
    assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_publication_request') == 0
    assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_approval_history') == 1


async def _assert_source_batch_reference_refusals(case, document_by_field, compact):
    from process import registry_source_binding_publication as source_batch

    for changed in (
        dict(
            document_by_field,
            batch_references=[dict(document_by_field["batch_references"][0], request_sha256="0" * 64)],
        ),
        dict(
            document_by_field,
            source_coordinates=dict(document_by_field["source_coordinates"], source_id="other-source"),
        ),
        dict(document_by_field, selection_count=4, selection_sha256=compact["selection_sha256"]),
    ):
        with pytest.raises(ValueError, match="registry_source_batch_invalid"):
            async with case.connection.transaction(isolation="repeatable_read", readonly=True):
                await source_batch.resolve_source_batch(
                    case.connection, changed, case.actor, control_schema=case.schema, allow_unbound=True
                )
