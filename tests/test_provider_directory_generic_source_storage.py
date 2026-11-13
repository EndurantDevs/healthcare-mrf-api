# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Batch authority, immutable native identities and no per-resource SQL."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_role_merge import MERGED_ROLE_SOURCES_ENV, role_order_index_sql


@pytest.mark.parametrize("failure", [asyncio.CancelledError, TimeoutError])
async def test_interrupted_candidate_copy_closes_transport_and_invalidates_its_owner(monkeypatch, failure):
    connection = SimpleNamespace(copy_records_to_table=AsyncMock(side_effect=failure))
    owner, events = object(), []

    async def close(actual):
        assert actual is connection
        events.append("close")

    async def invalidate(actual_owner, actual_connection):
        assert actual_owner is owner and actual_connection is connection
        events.append("invalidate")

    monkeypatch.setattr(storage, "close_interrupted_copy", close)
    monkeypatch.setattr(storage, "invalidate_copy_owner", invalidate)
    monkeypatch.setattr(storage, "source_copy_session", lambda: owner)
    with pytest.raises(failure):
        await storage._copy(connection, SimpleNamespace(schema="source"), "input_rows", ("id",), [(1,)])
    assert events == ["close", "invalidate"]
    connection.copy_records_to_table.assert_awaited_once()


@pytest.mark.parametrize("root", [storage.HEADER, "provider_directory_pagination_checkpoint"])
def test_legacy_constraint_trigger_is_omitted_only_from_template_shape(root):
    from process.provider_directory_source_storage_binding import _isolated_layout_shape

    plain_by_field = dict(relname=root, columns=[], constraints=[], indexes=[])
    guarded_by_field = dict(
        plain_by_field, constraints=[["legacy_guard", "t", "TRIGGER DEFERRABLE INITIALLY DEFERRED", True]]
    )
    assert _isolated_layout_shape(guarded_by_field, template=True) == _isolated_layout_shape(
        plain_by_field, template=False
    )
    assert _isolated_layout_shape(guarded_by_field, template=False) != _isolated_layout_shape(
        plain_by_field, template=False
    )


def _case():
    candidate = SimpleNamespace(
        dataset_id="dataset",
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        selected_resources=("Organization",),
        reused_from_checkpoint=False,
    )
    directory = SimpleNamespace(
        _schema=lambda: "source_schema",
        _endpoint_dataset_candidate_metadata=lambda _: {
            "source_ids": ["cms-npd"],
            "selected_resources": ["Organization"],
            "resource_hash_contract": "contract",
        },
    )
    identity = storage.candidate_identity(directory, candidate)
    tables_by_root = {
        key: dict(name="pdgs_" + "a" * 32 + suffix, oid=index + 10, owner=100, layout_sha256="c" * 64)
        for index, (key, suffix) in enumerate((("resource_input", "_ri"), ("witness_input", "_wi")))
    }
    binding_by_field = dict(
        contract=storage.CONTRACT,
        identity=identity,
        phase="loading",
        ordinal=0,
        resets={},
        tables=tables_by_root,
        auxiliary=None,
        relationship_sha256=None,
    )
    header_by_field = dict(
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        status="acquiring",
        is_current=False,
        publication_metadata_json=identity["metadata"],
        candidate_storage_json=binding_by_field,
    )
    return candidate, directory, header_by_field


@pytest.fixture
def harness(monkeypatch):
    candidate, directory, header = _case()
    copies, sql = [], []

    async def execute(query, *args):
        sql.append(query)
        if query.startswith("UPDATE"):
            assert storage._json(args[2]) == header["candidate_storage_json"]
            header["candidate_storage_json"] = storage._json(args[1])
            return "UPDATE 1"
        return "OK"

    async def copy(name, **kwargs):
        copies.append((name, kwargs))
        return f"COPY {len(kwargs['records'])}"

    connection = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=lambda *args: deepcopy(header)),
        fetchval=AsyncMock(side_effect=lambda *args: header["candidate_storage_json"]["ordinal"] + 1),
        execute=AsyncMock(side_effect=execute),
        copy_records_to_table=AsyncMock(side_effect=copy),
    )
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))

    @asynccontextmanager
    async def owner(database):
        before = deepcopy(header)
        try:
            yield SimpleNamespace(connection=connection, policy=policy)
        except BaseException:
            header.clear()
            header.update(before)
            raise

    async def capture(conn, configured, names, **kwargs):
        proofs = header["candidate_storage_json"]["tables"]
        return {table["name"]: deepcopy(table) for table in proofs.values()}

    monkeypatch.setattr(storage, "source_owner_transaction", owner)
    monkeypatch.setattr(storage, "_capture", AsyncMock(side_effect=capture))
    return SimpleNamespace(
        candidate=candidate,
        directory=directory,
        header=header,
        connection=connection,
        sql=sql,
        copies=copies,
        policy=policy,
    )


def _rows(count):
    return [
        dict(
            dataset_id="dataset",
            resource_type="Organization",
            resource_id=str(index),
            payload_hash="b" * 64,
            payload_json={"value": index},
            acquired_resource_sha256=None,
        )
        for index in range(count)
    ]


def _isolated_catalog_row(factory):
    return {
        field: getattr(factory.storage_binding, field)
        for field in ("database_oid", "catalog_schema_oid", "storage_schema_oid", "owner_oid", "namespace_owner_oid")
    }


def _set_isolated_candidate(harness, publisher, *, named):
    binding = harness.header["candidate_storage_json"]
    binding.update(
        contract=storage.NAMED_ISOLATED_CONTRACT if named else storage.ISOLATED_CONTRACT,
        storage=storage.storage_document(publisher),
        admission=None,
    )
    if named:
        for key, suffix in (("resource_input", "ri"), ("witness_input", "wi")):
            binding["tables"][key] = {"name": "pdgs_" + "a" * 32 + "_" + suffix}


@asynccontextmanager
async def _isolated_harness(harness, monkeypatch, *, named=False):
    from process import provider_directory_generic_source_policy as policy
    from tests.test_provider_directory_generic_source_policy import _isolated_factory, _named_factory

    factory = _named_factory() if named else _isolated_factory()
    native_by_field = {} if named else _isolated_catalog_row(factory)
    harness.policy = factory.policy
    harness.directory._schema = lambda: "catalog_schema"
    metadata = harness.header["candidate_storage_json"]["identity"]["metadata"]
    metadata["source_ids"] = [storage.VSP_SOURCE_ID if named else "example-source"]
    harness.directory._endpoint_dataset_candidate_metadata = lambda _: metadata
    connection, actor = harness.connection, {"session": "source_publisher", "current": "source_publisher"}

    async def fetchrow(query, *args):
        if query.startswith("SELECT session_user"):
            return dict(actor)
        if query.startswith("SELECT d.oid"):
            return native_by_field
        return deepcopy(harness.header)

    connection.fetchrow = AsyncMock(side_effect=fetchrow)
    original_fetchval = connection.fetchval
    connection.fetchval = AsyncMock(
        side_effect=lambda query, *args: (
            "repeatable read" if query == "SHOW transaction_isolation" else original_fetchval.side_effect(query, *args)
        )
    )
    connection.is_in_transaction = lambda: True
    monkeypatch.setattr(policy, "storage_roles", AsyncMock())

    @asynccontextmanager
    async def owner(database):
        before = deepcopy(harness.header)
        try:
            yield policy.bound_publisher()
        except BaseException:
            harness.header.clear()
            harness.header.update(before)
            raise

    monkeypatch.setattr(storage, "source_owner_transaction", owner)
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.bind_generic_source_publisher(
            connection, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
        ) as publisher:
            actor["current"] = "source_owner"
            _set_isolated_candidate(harness, publisher, named=named)
            yield publisher


@pytest.mark.asyncio
async def test_isolated_copy_uses_only_the_issued_storage_coordinates(harness, monkeypatch):
    async with _isolated_harness(harness, monkeypatch):
        assert await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(2)) == 2
    assert harness.copies[0][1]["schema_name"] == "source_schema"
    assert '"source_schema"."provider_directory_endpoint_dataset"' in harness.sql[-1]
    assert not any('"catalog_schema".' in statement for statement in harness.sql)


def _named_candidate_context(harness, monkeypatch, factory, source_id):
    from process import provider_directory_generic_source_policy as policy
    from tests.test_provider_directory_generic_source_policy import _connection

    harness.policy = factory.policy
    harness.directory._schema = lambda: factory.storage_binding.catalog_schema
    metadata = harness.header["publication_metadata_json"]
    metadata["source_ids"] = [source_id]
    harness.directory._endpoint_dataset_candidate_metadata = lambda _: metadata
    completion = _without_candidate_storage(harness, monkeypatch)
    native, savepoints = _connection()
    harness.connection.is_in_transaction = native.is_in_transaction
    harness.connection.is_closed = native.is_closed
    harness.connection.transaction = native.transaction
    fetchval = harness.connection.fetchval
    harness.connection.fetchval = AsyncMock(
        side_effect=lambda sql, *args: (
            harness.header["candidate_storage_json"]["ordinal"] + 1
            if sql.startswith("SELECT nextval(")
            else fetchval.side_effect(sql, *args)
        )
    )
    capture = AsyncMock(side_effect=AssertionError("named storage does not use physical identity"))
    monkeypatch.setattr(storage, "_capture", capture)

    session = object()
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=harness.connection))
    monkeypatch.setattr(storage, "source_owner_transaction", policy.source_owner_transaction)
    monkeypatch.setattr(completion, "source_owner_transaction", policy.source_owner_transaction)
    return database, completion, savepoints, capture


@asynccontextmanager
async def _bind_named_publisher(harness, factory, verify_roles):
    from process import provider_directory_generic_source_policy as policy

    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.bind_generic_source_publisher(
            harness.connection,
            factory.policy,
            verify_role_profile=verify_roles,
            transaction_isolation="repeatable read",
        ):
            yield


@pytest.mark.asyncio
async def test_named_candidate_refuses_unsupported_source(harness, monkeypatch):
    from tests.test_provider_directory_generic_source_policy import _named_factory

    factory = _named_factory()
    database, _, savepoints, capture = _named_candidate_context(harness, monkeypatch, factory, "other-source")
    async with _bind_named_publisher(harness, factory, AsyncMock()):
        with pytest.raises(GenericCandidateError, match="isolated_kind_unsupported"):
            await storage.prepare_generic_source_candidate(database, harness.directory, harness.candidate)
    assert harness.header["candidate_storage_json"] is None and not harness.copies
    assert savepoints == ["savepoint", "rollback"]
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_named_candidate_resumes_after_committing_a_batch(harness, monkeypatch):
    from process import provider_directory_generic_source_policy as policy
    from tests.test_provider_directory_generic_source_policy import _named_factory

    factory = _named_factory()
    database, completion, savepoints, capture = _named_candidate_context(
        harness, monkeypatch, factory, storage.VSP_SOURCE_ID
    )
    verify_roles = AsyncMock()
    async with _bind_named_publisher(harness, factory, verify_roles):
        loading = await storage.prepare_generic_source_candidate(database, harness.directory, harness.candidate)
        assert loading["contract"] == storage.NAMED_ISOLATED_CONTRACT
        assert set(loading["tables"]) == {"resource_input", "witness_input"}
        assert all(set(table) == {"name"} for table in loading["tables"].values())
        assert await storage.append_generic_source_batch(database, harness.directory, harness.candidate, _rows(2)) == 2
    assert policy._PUBLISHER.get() is None
    persisted = deepcopy(harness.header["candidate_storage_json"])
    assert persisted["ordinal"] == 1 and persisted["tables"] == loading["tables"]

    async with _bind_named_publisher(harness, _named_factory(), verify_roles):
        async with policy.source_owner_transaction(database):
            previous_statements = list(harness.sql)
            resumed = await storage.prepare_generic_source_candidate(database, harness.directory, harness.candidate)
            assert resumed == persisted and harness.sql == previous_statements
            assert (
                await storage.append_generic_source_batch(database, harness.directory, harness.candidate, _rows(1)) == 1
            )
        reduce = AsyncMock()
        monkeypatch.setattr(completion, "_complete_payloads", reduce)
        prepared = await completion.complete_generic_source_candidate(database, harness.directory, harness.candidate)
        assert prepared["phase"] == "prepared" and prepared["ordinal"] == 2
        assert set(prepared["tables"]) == {*storage.RELATIONS, "resource_input", "witness_input"}
        assert all(set(table) == {"name"} for table in prepared["tables"].values())
        reduce.assert_awaited_once()

    assert len(harness.copies) == 2
    for table, batch in harness.copies:
        assert table == loading["tables"]["resource_input"]["name"]
        assert batch["schema_name"] == factory.storage_binding.storage_schema
    assert savepoints.count("savepoint") == savepoints.count("release") == 6
    assert not any('"catalog_schema".' in statement for statement in harness.sql)
    verify_roles.assert_not_awaited()
    capture.assert_not_awaited()


def _without_candidate_storage(harness, monkeypatch, found=()):
    """Start from an unprepared candidate; `found` marks the existence checks that find a row."""
    from process import provider_directory_generic_source_completion as completion

    harness.header["candidate_storage_json"] = None
    harness.candidate.source_release = None
    harness.connection.fetch = AsyncMock(return_value=[])
    harness.connection.fetchval = AsyncMock(
        side_effect=lambda query, *args: (
            "private_ordinal_sequence" if "pg_get_serial_sequence" in query else any(mark in query for mark in found)
        )
    )
    proof_by_name = {}

    async def capture(connection, policy, names, **options):
        assert connection is harness.connection and policy is harness.policy
        for name in names:
            proof_by_name.setdefault(
                name, dict(name=name, oid=1000 + len(proof_by_name), owner=100, layout_sha256="e" * 64)
            )
        return {name: proof_by_name[name] for name in names}

    monkeypatch.setattr(storage, "_capture", capture)
    monkeypatch.setattr(completion, "source_owner_transaction", storage.source_owner_transaction)
    return completion


@pytest.mark.asyncio
@pytest.mark.parametrize("merged_roles", [False, True])
async def test_isolated_prepare_and_complete_share_the_existing_candidate_binding(harness, monkeypatch, merged_roles):
    monkeypatch.setenv(MERGED_ROLE_SOURCES_ENV, "example-source" if merged_roles else "")
    async with _isolated_harness(harness, monkeypatch) as publisher:
        completion = _without_candidate_storage(harness, monkeypatch)
        reduction = AsyncMock()
        monkeypatch.setattr(completion, "_complete_payloads", reduction)
        loading = await storage.prepare_generic_source_candidate(object(), harness.directory, harness.candidate)
        assert loading["contract"] == storage.ISOLATED_CONTRACT
        assert loading["storage"] == storage.storage_document(publisher)
        indexes = [statement for statement in harness.sql if statement.startswith("CREATE INDEX IF NOT EXISTS")]
        resource_input = loading["tables"]["resource_input"]["name"]
        if merged_roles:
            (index,) = indexes
            assert f'"source_schema"."{resource_input}"' in index
            assert 'resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal' in index
        else:
            assert not indexes
        prepared = await completion.complete_generic_source_candidate(object(), harness.directory, harness.candidate)
        reduction.assert_awaited_once()
        assert prepared["phase"] == "prepared" and prepared["storage"] == loading["storage"]
        assert prepared["identity"] == loading["identity"]
        assert set(prepared["tables"]) == {*storage.RELATIONS, "resource_input", "witness_input"}
        pre_replay_statements = list(harness.sql)
        assert (
            await completion.complete_generic_source_candidate(object(), harness.directory, harness.candidate)
            == prepared
        )
        assert harness.sql == pre_replay_statements
    assert sum(statement.startswith("CREATE TABLE") for statement in harness.sql) == 2 + len(storage.RELATIONS)
    assert all(
        '"catalog_schema".' not in statement
        and "ATTACH" not in statement
        and "RENAME" not in statement
        and "LOCK TABLE" not in statement
        for statement in harness.sql
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("merged_role_sources", "found", "reducer"),
    [
        ("example-source", (), "_reduce_payloads_python"),
        ("", ("count(DISTINCT (payload_hash",), "_reduce_payloads_python"),
        ("", (), "_reduce_payloads_sql"),
    ],
    ids=["merged roles", "differing observations", "identical observations"],
)
async def test_isolated_merged_role_source_orders_its_input_and_reduction(
    harness, monkeypatch, merged_role_sources, found, reducer
):
    monkeypatch.setenv(MERGED_ROLE_SOURCES_ENV, merged_role_sources)
    async with _isolated_harness(harness, monkeypatch):
        completion = _without_candidate_storage(harness, monkeypatch, found)
        reduction_by_name = {name: AsyncMock() for name in ("_reduce_payloads_python", "_reduce_payloads_sql")}
        for name, reduction in reduction_by_name.items():
            monkeypatch.setattr(completion, name, reduction)
        loading = await storage.prepare_generic_source_candidate(object(), harness.directory, harness.candidate)
        loading_statements = list(harness.sql)
        prepared = await completion.complete_generic_source_candidate(object(), harness.directory, harness.candidate)
    assert prepared["phase"] == "prepared"
    assert [name for name, reduction in reduction_by_name.items() if reduction.await_count] == [reducer]
    is_merged_role_source = bool(merged_role_sources)
    # Only a source that retains merged roles pays for the lookup index while it loads.
    order_index = role_order_index_sql(harness.policy.schema, loading["tables"]["resource_input"]["name"])
    assert (order_index in loading_statements) is is_merged_role_source
    checks = [call.args[0] for call in harness.connection.fetchval.await_args_list]
    assert any("native_role" in check for check in checks) is is_merged_role_source
    assert any("? 'merged_role_repeat'" in check for check in checks) is is_merged_role_source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("found", "error", "reductions"),
    [
        ("count(DISTINCT (source_id", "witness_replay_conflict", 0),
        ("native_role", "provider_directory_resource_payload_conflict", 0),
        ("? 'merged_role_repeat'", "provider_directory_merged_role_observation_missing", 1),
    ],
    ids=["witness replay", "source role with two contents", "repeat without its role"],
)
async def test_isolated_completion_refusal_keeps_the_loading_binding(harness, monkeypatch, found, error, reductions):
    monkeypatch.setenv(MERGED_ROLE_SOURCES_ENV, "example-source")
    async with _isolated_harness(harness, monkeypatch):
        completion = _without_candidate_storage(harness, monkeypatch, (found,))
        reduction = AsyncMock()
        monkeypatch.setattr(completion, "_reduce_payloads_python", reduction)
        loading = await storage.prepare_generic_source_candidate(object(), harness.directory, harness.candidate)
        with pytest.raises(ValueError, match=error):
            await completion.complete_generic_source_candidate(object(), harness.directory, harness.candidate)
        assert harness.header["candidate_storage_json"] == loading
    assert reduction.await_count == reductions


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["storage_schema_oid", "database_oid", "enrollment_sha256", "producer"])
async def test_isolated_stored_binding_substitution_never_reaches_copy(harness, monkeypatch, changed):
    async with _isolated_harness(harness, monkeypatch):
        document = harness.header["candidate_storage_json"]["storage"]
        document[changed] = ["c" * 40, "sha256:" + "d" * 64] if changed == "producer" else 999
        with pytest.raises(GenericCandidateError, match="storage_binding_changed"):
            await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
    harness.connection.copy_records_to_table.assert_not_awaited()
    assert harness.sql == []


@pytest.mark.asyncio
async def test_isolated_intake_rejects_specialized_source_before_target_access(harness, monkeypatch):
    async with _isolated_harness(harness, monkeypatch):
        harness.header["candidate_storage_json"]["identity"]["metadata"]["source_release"] = {"vector_sha256": "a" * 64}
        harness.connection.fetchrow.reset_mock()
        with pytest.raises(GenericCandidateError, match="isolated_kind_unsupported"):
            await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
        harness.connection.fetchrow.assert_not_awaited()
    harness.connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_many_singletons_are_one_copy_and_one_header_control(harness):
    h = harness
    count = await storage.append_generic_source_batch(object(), h.directory, h.candidate, _rows(4096))
    assert count == 4096
    assert h.connection.fetchrow.await_count == 3
    assert h.connection.fetchrow.await_args_list[0].args[0].endswith("FOR KEY SHARE")
    assert h.connection.fetchrow.await_args_list[1].args[0].endswith("FOR KEY SHARE")
    assert h.connection.fetchrow.await_args_list[2].args[0].endswith("FOR NO KEY UPDATE")
    assert len(h.copies) == 1
    assert len(h.copies[0][1]["records"]) == 4096
    assert len(h.sql) == 2 and h.sql[-1].startswith("UPDATE")
    assert "pg_advisory_xact_lock" in h.sql[0]
    assert not any("INSERT" in query or "FOR SHARE" in query for query in h.sql)
    assert {record[-2] for record in h.copies[0][1]["records"]} == {1}
    assert h.header["candidate_storage_json"]["ordinal"] == 1


@pytest.mark.asyncio
async def test_witness_failure_rolls_back_resources_and_header_ordinal(harness):
    harness_case = harness
    original = deepcopy(harness_case.header)
    harness_case.connection.copy_records_to_table.side_effect = ["COPY 1", RuntimeError("witness rejected")]
    witness_by_field = dict(
        dataset_id="dataset",
        source_id="cms-npd",
        release_id="a" * 64,
        resource_type="Organization",
        resource_id="0",
        raw_payload_sha256="c" * 64,
        normalized_payload_hash="b" * 64,
        raw_payload_json={"resourceType": "Organization", "id": "0"},
    )
    with pytest.raises(RuntimeError, match="witness rejected"):
        await storage.append_generic_source_batch(
            object(), harness_case.directory, harness_case.candidate, _rows(1), [witness_by_field]
        )
    assert harness_case.header == original
    assert len(harness_case.sql) == 1 and "pg_advisory_xact_lock" in harness_case.sql[0]


@pytest.mark.asyncio
async def test_interrupted_copy_is_closed_and_retires_its_owner(harness, monkeypatch):
    original = deepcopy(harness.header)
    close, invalidate = AsyncMock(), AsyncMock()
    monkeypatch.setattr(storage, "close_interrupted_copy", close)
    monkeypatch.setattr(storage, "invalidate_copy_owner", invalidate)
    monkeypatch.setattr(storage, "source_copy_session", lambda: "copy-session")
    harness.connection.copy_records_to_table.side_effect = TimeoutError("copy timed out")
    with pytest.raises(TimeoutError, match="copy timed out"):
        await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
    close.assert_awaited_once_with(harness.connection)
    invalidate.assert_awaited_once_with("copy-session", harness.connection)
    assert harness.header == original


@pytest.mark.asyncio
async def test_replaced_oid_refuses_before_copy(harness, monkeypatch):
    h = harness
    proofs_by_name = {
        table["name"]: {**table, "oid": 999} for table in h.header["candidate_storage_json"]["tables"].values()
    }
    monkeypatch.setattr(storage, "_capture", AsyncMock(return_value=proofs_by_name))
    with pytest.raises(GenericCandidateError, match="storage_binding_changed"):
        await storage.append_generic_source_batch(object(), h.directory, h.candidate, _rows(1))
    h.connection.copy_records_to_table.assert_not_awaited()
    assert len(h.sql) == 1 and "pg_advisory_xact_lock" in h.sql[0]


@pytest.mark.asyncio
async def test_reset_advances_cutoff_without_payload_update_or_delete(harness):
    h = harness
    await storage.append_generic_source_batch(object(), h.directory, h.candidate, _rows(1))
    await storage.reset_generic_source_family(object(), h.directory, h.candidate, "Organization")
    assert h.header["candidate_storage_json"]["resets"] == {"Organization": 1}
    assert len(h.copies) == 1
    assert all("pg_advisory_xact_lock" in query or query.startswith("UPDATE") for query in h.sql)


@pytest.fixture
def partition_directory(harness):
    directory = importlib.import_module("process.provider_directory_fhir")

    for name in (
        "_last_updated_partition_checkpoint_resource_type",
        "_last_updated_partition_fingerprint_resource_type",
    ):
        setattr(harness.directory, name, getattr(directory, name))
    return directory


@pytest.mark.asyncio
@pytest.mark.parametrize("pass_number", (None, 1, 2))
async def test_selected_partition_reset_preserves_exact_cutoffs_and_input_identity(
    harness, partition_directory, pass_number
):
    resource_type = (
        partition_directory._last_updated_partition_checkpoint_resource_type("Organization")
        if pass_number is None
        else partition_directory._last_updated_partition_fingerprint_resource_type("Organization", pass_number)
    )
    harness.header["candidate_storage_json"].update(
        ordinal=7,
        resets={"*": 1, "Organization": 2, "Location": 3, "LU:Organization:pass:2": 4},
    )
    previous = deepcopy(harness.header["candidate_storage_json"])

    await storage.reset_generic_source_family(object(), harness.directory, harness.candidate, resource_type)

    expected_by_field = {**previous, "resets": {**previous["resets"], resource_type: 7}}
    assert harness.header["candidate_storage_json"] == expected_by_field
    assert harness.copies == []
    assert len(harness.sql) == 2
    assert "pg_advisory_xact_lock" in harness.sql[0] and harness.sql[1].startswith("UPDATE")
    assert '"source_schema"."provider_directory_endpoint_dataset"' in harness.sql[1]
    lock = harness.connection.execute.await_args_list[0]
    lock_family = resource_type if pass_number is None else "Organization"
    assert storage._encoded(["source_schema", "dataset", lock_family]) in lock.args[1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_type",
    (
        "Location",
        "Location:LastUpdatedPartition",
        "LU:Location:pass:1",
        "LU:Location:pass:2",
        "Organization:LastUpdatedPartition:extra",
        "Organization:lastupdatedpartition",
        "LU:Organization:pass:0",
        "LU:Organization:pass:3",
        "LU:Organization:pass:01",
        "LU:Organization:pass:1:extra",
        "LU:Organization:other:1",
        "LU:Organization",
        "*",
        "",
    ),
)
async def test_reset_refuses_unselected_or_malformed_partition_family_before_database(
    harness, partition_directory, monkeypatch, resource_type
):
    owner = AsyncMock(side_effect=AssertionError("owner transaction must not open"))
    monkeypatch.setattr(storage, "source_owner_transaction", owner)
    previous = deepcopy(harness.header)

    with pytest.raises(GenericCandidateError, match="reset_scope_changed"):
        await storage.reset_generic_source_family(object(), harness.directory, harness.candidate, resource_type)

    owner.assert_not_called()
    harness.connection.fetchrow.assert_not_awaited()
    harness.connection.execute.assert_not_awaited()
    harness.connection.copy_records_to_table.assert_not_awaited()
    assert harness.header == previous and harness.sql == []


@pytest.mark.asyncio
@pytest.mark.parametrize("pass_number", (1, 2))
async def test_partition_reset_uses_the_payload_append_family_lock(harness, partition_directory, pass_number):
    marker = partition_directory._last_updated_partition_fingerprint_resource_type("Organization", pass_number)
    rows = [*_rows(1), {**_rows(1)[0], "resource_type": marker}]
    await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, rows)
    append_lock = harness.connection.execute.await_args_list[0].args
    harness.connection.execute.reset_mock()

    await storage.reset_generic_source_family(object(), harness.directory, harness.candidate, marker)

    reset_lock = harness.connection.execute.await_args_list[0].args
    assert reset_lock == append_lock
    assert harness.header["candidate_storage_json"]["resets"] == {marker: 1}
    assert harness.header["candidate_storage_json"]["ordinal"] == 1
    assert len(harness.copies) == 1 and len(harness.copies[0][1]["records"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", (False, True))
async def test_new_partition_candidate_reaches_real_reset_while_retained_candidate_is_untouched(
    harness, partition_directory, monkeypatch, retained
):
    from process import provider_directory_generic_source_policy as policy

    directory = partition_directory
    candidate = harness.candidate
    candidate.reused_from_checkpoint = retained
    candidate.identity = storage.candidate_identity(harness.directory, candidate)
    context = directory.PaginationCheckpointContext(
        canonical_api_base="https://directory.example/fhir",
        source_scope_hash="scope",
        source_ids=("source",),
        owner_run_id="root",
        acquisition_root_run_id="root",
        dataset_id=candidate.dataset_id,
    )
    candidate.checkpoint_context = context
    marker = directory._last_updated_partition_checkpoint_resource_type("Organization")
    initialization = directory.LastUpdatedPartitionInitialization("Organization", marker, "start_hash", "plan")
    previous = deepcopy(harness.header)

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield harness.connection

    for name in ("_schema", "_endpoint_dataset_candidate_metadata"):
        monkeypatch.setattr(directory, name, getattr(harness.directory, name))
    monkeypatch.setattr(directory, "source_checkpoint_transaction", transaction)
    monkeypatch.setattr(directory, "_mutable_endpoint_dataset_resource_mutation", transaction)
    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(directory, "_require_source_native_connection", AsyncMock())
    monkeypatch.setattr(directory, "_lock_endpoint_dataset_resource_family", AsyncMock())
    monkeypatch.setattr(storage, "resolve_existing_source_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(directory, "delete_dataset_proof_shards", AsyncMock())
    monkeypatch.setattr(directory, "delete_dataset_resource_proof_shards", AsyncMock())
    harness.connection.status = AsyncMock()

    await directory._reset_new_endpoint_candidate(harness.connection, candidate, (initialization,))

    if retained:
        assert harness.header == previous and harness.sql == [] and harness.copies == []
        harness.connection.status.assert_not_awaited()
    else:
        assert harness.header["candidate_storage_json"]["resets"] == {"*": 0, marker: 0}
        checkpoint = harness.connection.status.await_args.kwargs
        assert checkpoint["resource_type"] == marker and checkpoint["next_url"] == "plan"
        assert checkpoint["dataset_id"] == candidate.dataset_id
        assert checkpoint["acquisition_root_run_id"] == candidate.acquisition_root_run_id
        assert harness.header["candidate_storage_json"]["tables"] == previous["candidate_storage_json"]["tables"]
        assert harness.copies == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("phase", "ordinal", "schema"))
async def test_closed_overflow_or_other_namespace_never_reaches_copy(harness, change):
    h = harness
    if change == "phase":
        h.header["candidate_storage_json"]["phase"] = "prepared"
    elif change == "ordinal":
        h.header["candidate_storage_json"]["ordinal"] = 2**63 - 2
    else:
        h.directory._schema = lambda: "other_schema"
    with pytest.raises(GenericCandidateError):
        await storage.append_generic_source_batch(object(), h.directory, h.candidate, _rows(1))
    h.connection.copy_records_to_table.assert_not_awaited()


def _catalog_row():
    return dict(
        relname="pdgs_" + "a" * 32 + "_ri",
        oid=1,
        relowner=100,
        relkind="r",
        relpersistence="p",
        relrowsecurity=False,
        relforcerowsecurity=False,
        inert=True,
        native_checks=True,
        direct_acl_closed=True,
        readers_closed=True,
        relispartition=False,
        parents=[],
        columns=[["dataset_id", "character varying(96)", True, None]],
        constraints=[],
        indexes=[],
    )


@pytest.mark.parametrize(
    "field,value",
    (
        ("inert", False),
        ("direct_acl_closed", False),
        ("native_checks", False),
        ("constraints", [{"type": "f", "valid": True}]),
        ("parents", [2]),
        ("relispartition", True),
    ),
)
def test_native_private_heap_custody_refuses_hooks_grants_foreign_keys_or_attachment(field, value):
    with pytest.raises(GenericCandidateError, match="storage_custody_changed"):
        storage._table_proof(_catalog_row() | {field: value}, private=True)


def _sequence_row(*, owner=100, schema="source_schema"):
    return dict(
        input_name="pdgs_" + "a" * 32 + "_ri",
        attname="batch_ordinal",
        attidentity="d",
        deptype="i",
        oid=123,
        relname="pdgs_" + "a" * 32 + "_ri_batch_ordinal_seq",
        nspname=schema,
        relowner=owner,
        relkind="S",
        relpersistence="p",
        seqtypid="bigint",
        seqstart=1,
        seqincrement=1,
        seqmax=2**63 - 1,
        seqmin=1,
        seqcache=1,
        seqcycle=False,
        acl_closed=True,
    )


@pytest.mark.parametrize(
    "field,value",
    (
        ("attname", "row_ordinal"),
        ("attidentity", "a"),
        ("deptype", "a"),
        ("nspname", "other"),
        ("relowner", 101),
        ("relkind", "r"),
        ("relpersistence", "u"),
        ("seqtypid", "integer"),
        ("seqstart", 2),
        ("seqincrement", 2),
        ("seqmax", 100),
        ("seqmin", 0),
        ("seqcache", 2),
        ("seqcycle", True),
        ("acl_closed", False),
    ),
)
def test_input_sequence_requires_exact_native_custody(harness, field, value):
    with pytest.raises(GenericCandidateError, match="input_sequence_changed"):
        storage._require_input_sequence(_sequence_row() | {field: value}, harness.policy, 100)


@pytest.mark.asyncio
async def test_late_sibling_batch_preserves_higher_ordinal_and_reset(harness):
    harness.connection.fetchval.return_value = 3
    harness.connection.fetchval.side_effect = None

    async def concurrent_copy(name, **options):
        binding = harness.header["candidate_storage_json"]
        binding.update(ordinal=7, resets={"Location": 6})
        return f"COPY {len(options['records'])}"

    harness.connection.copy_records_to_table.side_effect = concurrent_copy
    await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
    assert harness.header["candidate_storage_json"]["ordinal"] == 7
    assert harness.header["candidate_storage_json"]["resets"] == {"Location": 6}


@pytest.mark.asyncio
async def test_failed_copy_can_resume_after_native_sequence_gap(harness):
    harness.connection.fetchval.side_effect = [1, 3]
    harness.connection.copy_records_to_table.side_effect = RuntimeError("COPY rejected")
    with pytest.raises(RuntimeError, match="COPY rejected"):
        await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
    assert harness.header["candidate_storage_json"]["ordinal"] == 0
    harness.connection.copy_records_to_table.side_effect = None
    harness.connection.copy_records_to_table.return_value = "COPY 1"
    await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
    assert harness.header["candidate_storage_json"]["ordinal"] == 3


@pytest.mark.asyncio
async def test_partition_and_resource_rows_share_one_family_lock(harness):
    rows = [*_rows(1), {**_rows(1)[0], "resource_type": "LU:Organization:pass:2"}]
    await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, rows)
    locks = [call for call in harness.connection.execute.await_args_list if "advisory" in call.args[0]]
    assert len(locks) == 1
    assert storage._encoded(["source_schema", "dataset", "Organization"]) in locks[0].args[1]
