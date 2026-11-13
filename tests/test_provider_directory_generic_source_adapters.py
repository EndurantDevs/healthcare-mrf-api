# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing CMS/FHIR entrypoints cannot fall back to ordinary target upserts."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from functools import partial
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_resource_batch as cms_batch
from process import provider_directory_generic_source_adapters as adapters
from process import provider_directory_generic_source_policy as policy
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_candidate import GenericCandidateError


@pytest.mark.asyncio
async def test_cms_missing_publisher_refuses_before_projection_session_or_target_io():
    fhir = SimpleNamespace(_bound_upsert_session=Mock(side_effect=AssertionError("target reached")))
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        await cms_batch.persist_prepared_cms_dataset_rows(fhir, object(), [{}], ({}, b""), object())
    fhir._bound_upsert_session.assert_not_called()


@pytest.mark.asyncio
async def test_cms_prepares_and_copies_without_locked_union_upsert_or_values(monkeypatch):
    session = object()
    retained_rows = [
        {
            "dataset_id": "dataset",
            "resource_type": "Organization",
            "resource_id": "example",
            "payload_json": {"b": 1, "a": [2]},
        }
    ]
    fhir = SimpleNamespace(
        db=object(),
        _schema=lambda: "source_schema",
        _bound_upsert_session=lambda: session,
        _endpoint_dataset_resource_rows=Mock(return_value=retained_rows),
        _assert_practitioner_semantic_projection_as_of=Mock(),
        _accumulated_endpoint_dataset_rows=AsyncMock(),
        _copy_upsert_rows=AsyncMock(),
        _upsert_rows_values=AsyncMock(),
    )
    candidate = SimpleNamespace(
        dataset_id="dataset",
        endpoint_id="endpoint",
        acquisition_root_run_id="run",
        source_ids=("cms-npd",),
        resource_hash_contract="contract",
        semantic_projection_as_of=None,
    )
    monkeypatch.setattr(policy, "bound_publisher", Mock())
    build = Mock(return_value=({"shard_id": "shard"}, b"compressed"))
    monkeypatch.setattr(cms_batch, "build_dataset_proof_shard", build)
    persist_proof = AsyncMock()
    monkeypatch.setattr(cms_batch, "persist_built_dataset_proof_shard", persist_proof)
    copy = AsyncMock(return_value=1)
    monkeypatch.setattr(storage, "append_generic_source_batch", copy)
    prepared_rows, proof = cms_batch.prepare_cms_dataset_rows(fhir, object(), [{}], candidate)
    assert prepared_rows == [{**retained_rows[0], "payload_json": '{"a":[2],"b":1}'}] and proof == build.return_value
    build.assert_called_once_with(
        retained_rows,
        dataset_id="dataset",
        endpoint_id="endpoint",
        acquisition_root_run_id="run",
        source_ids=("cms-npd",),
        resource_hash_contract="contract",
    )
    await cms_batch.persist_prepared_cms_dataset_rows(fhir, session, prepared_rows, proof, candidate)
    copy.assert_awaited_once_with(fhir.db, fhir, candidate, prepared_rows)
    persist_proof.assert_awaited_once_with(
        fhir.db, "source_schema", proof[0], proof[1], dataset_id="dataset", expected_resource_hash_contract="contract"
    )
    fhir._accumulated_endpoint_dataset_rows.assert_not_awaited()
    fhir._copy_upsert_rows.assert_not_awaited()
    fhir._upsert_rows_values.assert_not_awaited()


@pytest.mark.asyncio
async def test_copy_failure_propagates_before_proof_and_never_uses_values(monkeypatch):
    session = object()
    fhir = SimpleNamespace(db=object(), _schema=lambda: "source_schema", _bound_upsert_session=lambda: session)
    monkeypatch.setattr(policy, "bound_publisher", Mock())
    monkeypatch.setattr(storage, "append_generic_source_batch", AsyncMock(side_effect=RuntimeError("COPY refused")))
    persist_proof = AsyncMock()
    monkeypatch.setattr(cms_batch, "persist_built_dataset_proof_shard", persist_proof)
    candidate = SimpleNamespace(dataset_id="dataset", resource_hash_contract="contract")
    with pytest.raises(RuntimeError, match="COPY refused"):
        await cms_batch.persist_prepared_cms_dataset_rows(
            fhir, session, [{"dataset_id": "dataset"}], ({}, b""), candidate
        )
    persist_proof.assert_not_awaited()


def test_repeatable_read_is_required_for_validation_and_cutover(monkeypatch):
    monkeypatch.setattr(
        adapters, "source_transaction_available", lambda: SimpleNamespace(transaction_isolation="read committed")
    )
    assert adapters.require_source_transaction() is not None
    with pytest.raises(GenericCandidateError, match="repeatable_read_required"):
        adapters.require_source_transaction(repeatable_read=True)


@pytest.mark.asyncio
async def test_named_retained_executor_uses_issued_copy_session_without_driver_identity_check(monkeypatch):
    from db.connection import ConnectionProxy
    from process import provider_directory_projection_stage as projection
    from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

    directory = import_module("process.provider_directory_fhir")
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(rowcount=1)))
    native = object()
    publisher = SimpleNamespace(connection=native, storage_binding=NamedFHIRSourceStorage("catalog", "candidate"))
    monkeypatch.setattr(policy, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(policy, "source_copy_session", lambda: session)
    driver_lookup = AsyncMock(side_effect=AssertionError("connection identity lookup"))
    monkeypatch.setattr(projection, "_copy_driver", driver_lookup)
    unrelated = ConnectionProxy(None, object(), object())
    executor = await directory._require_source_native_connection(unrelated)
    assert await executor.status("UPDATE synthetic_candidate SET value=:value", value="kept") == 1
    session.execute.assert_awaited_once()
    assert executor._connection is session
    driver_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_explicit_executor_does_not_establish_physical_authority(monkeypatch):
    from db.connection import ConnectionProxy
    from process import provider_directory_projection_stage as projection

    executor = ConnectionProxy(None, object(), None)
    monkeypatch.setattr(policy, "bound_publisher", lambda: SimpleNamespace(storage_binding=None))
    lookup = AsyncMock(side_effect=AssertionError("physical identity lookup"))
    monkeypatch.setattr(projection, "_copy_driver", lookup)
    assert await policy.source_native_executor(executor) is executor
    lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_type", ["Organization", "Location", "InsurancePlan", "PractitionerRole"])
async def test_identity_batches_reuse_the_original_owner_transaction_without_new_begin(resource_type):
    from contextlib import asynccontextmanager

    from process import provider_directory_cms_npd as cms

    session = SimpleNamespace(begin=Mock(side_effect=AssertionError("original transaction was restarted")))

    @asynccontextmanager
    async def original_session():
        yield session

    fhir = SimpleNamespace(db=SimpleNamespace(session=original_session))
    entity = SimpleNamespace(bind_entity_batch=AsyncMock())
    network = SimpleNamespace(record_insurance_network_batch=AsyncMock())
    resource = SimpleNamespace(bind_resource_identity_batch=AsyncMock())
    await cms._write_identity_batch(
        fhir, entity, network, resource, resource_type, [{"id": "example"}], {"vector_sha256": "a" * 64}
    )
    session.begin.assert_not_called()
    if resource_type in {"Organization", "Location"}:
        assert entity.bind_entity_batch.await_args.args[0] is session
    else:
        assert resource.bind_resource_identity_batch.await_args.args[0] is session
    if resource_type in {"Organization", "InsurancePlan"}:
        assert network.record_insurance_network_batch.await_args.args[0] is session


def _loading_count_facts():
    directory = import_module("process.provider_directory_fhir")
    metadata = dict(
        source_ids=["source-a"],
        selected_resources=["Organization"],
        resource_hash_contract=directory.LEGACY_RESOURCE_HASH_CONTRACT,
    )
    identity_by_field = dict(
        dataset_id="dataset", endpoint_id="endpoint", acquisition_root_run_id="root", metadata=metadata
    )
    binding_by_field = dict(
        contract=storage.NAMED_ISOLATED_CONTRACT,
        identity=identity_by_field,
        phase="loading",
        ordinal=4,
        resets={"*": 1, "Organization": 2},
        tables={
            kind: {"schema": "source_schema", "name": "pdgs_" + "a" * 32 + suffix}
            for kind, suffix in (("resource_input", "_ri"), ("witness_input", "_wi"))
        },
        auxiliary=None,
        relationship_sha256=None,
        storage={"catalog_schema": "mrf", "storage_schema": "source_schema"},
        admission=None,
    )
    header_by_field = dict(
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        status="acquiring",
        is_current=False,
        publication_metadata_json=deepcopy(metadata),
        candidate_storage_json=binding_by_field,
    )
    context = directory.PaginationCheckpointContext(
        canonical_api_base="https://directory.example/fhir",
        source_scope_hash="scope",
        source_ids=("source-a",),
        owner_run_id="owner",
        acquisition_root_run_id="root",
        dataset_id="dataset",
        endpoint_id="endpoint",
    )
    return context, header_by_field


async def _loading_scalar(case, query, *arguments):
    if query.startswith("SELECT nextval("):
        return case.header["candidate_storage_json"]["ordinal"] + 1
    if query.startswith("SELECT candidate_storage_json FROM"):
        return deepcopy(case.header["candidate_storage_json"])
    if query.startswith('SELECT count(DISTINCT resource_id COLLATE "C")'):
        case.aggregates.append((query, arguments))
        dataset, resource, cutoff, ordinal = arguments
        return len(
            {
                row["resource_id"]
                for row in case.observations
                if row["dataset_id"] == dataset
                and row["resource_type"] == resource
                and cutoff < row["batch_ordinal"] <= ordinal
            }
        )
    assert query == "SHOW transaction_isolation"
    return "repeatable read"


async def _loading_execute(case, role_execute, query, *arguments):
    case.queries.append((query, arguments))
    if query.startswith("UPDATE"):
        assert storage._json(arguments[2]) == case.header["candidate_storage_json"]
        case.header["candidate_storage_json"] = storage._json(arguments[1])
        return "UPDATE 1"
    return await role_execute(query)


@asynccontextmanager
async def _loading_count_case(monkeypatch):
    """Exercise named candidate queries and COPY within the existing Source transaction fixture."""
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
    from tests.test_provider_directory_generic_source_policy import synthetic_source_database

    directory = import_module("process.provider_directory_fhir")
    context, header_by_field = _loading_count_facts()
    domain = SimpleNamespace(first=AsyncMock(return_value={"unique_candidate_rows": 0}), status=AsyncMock())
    async with synthetic_source_database(monkeypatch, domain, schema="source_schema") as database:
        policy._FACTORY.get().storage_binding = FHIRSourceStorageBinding("mrf", "source_schema")
        async with policy.source_transaction(database):
            monkeypatch.setattr(directory, "db", database)
            monkeypatch.setattr(directory, "_schema", lambda: "mrf")
            native = policy.bound_publisher().connection
            case = SimpleNamespace(
                directory=directory,
                context=context,
                header=header_by_field,
                database=database,
                domain=domain,
                native=native,
                observations=[],
                aggregates=[],
                savepoints=[],
                queries=[],
            )
            role_execute = native.execute.side_effect
            original_transaction = native.transaction

            def transaction():
                savepoint = original_transaction()
                case.savepoints.append(savepoint)
                return savepoint

            async def first(query, *arguments):
                assert "candidate_storage_json FROM" in query
                assert '"source_schema"."provider_directory_endpoint_dataset"' in query
                case.queries.append((query, arguments))
                return deepcopy(case.header)

            async def copy(_name, *, records, columns, **_options):
                case.observations.extend(dict(zip(columns, values)) for values in records)
                return f"COPY {len(records)}"

            native.transaction = transaction
            native.fetch = AsyncMock(return_value=[])
            native.fetchrow.side_effect = first
            native.fetchval.side_effect = partial(_loading_scalar, case)
            native.copy_records_to_table.side_effect = copy
            native.execute.side_effect = partial(_loading_execute, case, role_execute)
            yield case


@pytest.mark.asyncio
async def test_loading_count_tracks_unique_copy(monkeypatch):
    """Terminal COPY is visible without preparing a snapshot or reading the empty legacy root."""
    from tests.test_provider_directory_generic_source_storage import _rows

    async with _loading_count_case(monkeypatch) as case:
        case.observations.extend(
            [
                dict(dataset_id="dataset", resource_type="Organization", resource_id="0", batch_ordinal=3),
                dict(dataset_id="dataset", resource_type="Organization", resource_id="reset", batch_ordinal=2),
                dict(dataset_id="other", resource_type="Organization", resource_id="foreign", batch_ordinal=4),
                dict(dataset_id="dataset", resource_type="Location", resource_id="other-family", batch_ordinal=4),
                dict(dataset_id="dataset", resource_type="Organization", resource_id="future", batch_ordinal=6),
            ]
        )
        candidate = await storage.resolve_existing_source_candidate(case.database, case.directory, "dataset")
        resource_rows = _rows(2)
        assert (
            await storage.append_generic_source_batch(
                case.database, case.directory, candidate, [*resource_rows, resource_rows[0]]
            )
            == 3
        )
        copy_call = case.native.copy_records_to_table.await_args
        assert copy_call.args == (case.header["candidate_storage_json"]["tables"]["resource_input"]["name"],)
        assert copy_call.kwargs["schema_name"] == "source_schema"
        assert copy_call.kwargs["columns"] == (*storage.RESOURCE_COLUMNS, "batch_ordinal", "row_ordinal")
        assert [(copied_values[-2], copied_values[-1]) for copied_values in copy_call.kwargs["records"]] == [
            (5, 0),
            (5, 1),
            (5, 2),
        ]
        assert case.header["candidate_storage_json"]["ordinal"] == 5
        before = deepcopy((case.header, case.observations))
        case.native.execute.reset_mock()
        count = await case.directory._caresource_unique_candidate_count(case.context, "Organization")
        assert count == 2
        assert len(case.aggregates) == 1 and case.aggregates[0][1] == ("dataset", "Organization", 2, 5)
        assert '"source_schema"."pdgs_' in case.aggregates[0][0]
        assert (case.header, case.observations) == before
        assert case.header["candidate_storage_json"]["phase"] == "loading"
        statements = [call.args[0] for call in case.native.execute.await_args_list]
        assert len(statements) == 3
        assert sum("pg_advisory_xact_lock" in statement for statement in statements) == 1
        assert sum(statement.startswith("SELECT 1 FROM") for statement in statements) == 2
        case.domain.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "policy", "storage", "producer", "transaction"])
async def test_loading_count_rechecks_logical_source_scope_after_the_aggregate(monkeypatch, changed):
    async with _loading_count_case(monkeypatch) as case:
        scalar = case.native.fetchval.side_effect
        tokens = []

        async def rewrap(query, *arguments):
            result = await scalar(query, *arguments)
            if not query.startswith("SELECT count(DISTINCT"):
                return result
            current = policy.bound_publisher()
            publisher_by_field = dict(policy=replace(current.policy), storage_binding=replace(current.storage_binding))
            if changed == "policy":
                publisher_by_field["policy"] = replace(current.policy, owner_role="other_owner")
            elif changed == "storage":
                publisher_by_field["storage_binding"] = replace(current.storage_binding, catalog_schema="other_catalog")
            elif changed == "producer":
                publisher_by_field["producer_identity"] = ("c" * 40, "sha256:" + "d" * 64)
            elif changed == "transaction":
                case.native.is_in_transaction = lambda: False
            tokens.append(policy._PUBLISHER.set(replace(current, **publisher_by_field)))
            return result

        case.native.fetchval.side_effect = rewrap
        try:
            if changed is None:
                assert await adapters.candidate_resource_count(case.directory, case.context, "Organization") == 0
            else:
                with pytest.raises(
                    GenericCandidateError, match="owner_transaction_required|policy_changed|transaction_required"
                ):
                    await adapters.candidate_resource_count(case.directory, case.context, "Organization")
        finally:
            for token in reversed(tokens):
                policy._PUBLISHER.reset(token)
        assert len(case.aggregates) == 1
        case.native.copy_records_to_table.assert_not_awaited()
        case.domain.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_reader_pins_before_family_lock_and_bounds_committed_batches(monkeypatch):
    """Replay cannot upgrade a writer fence or read observations above its authenticated cutoff."""
    async with _loading_count_case(monkeypatch) as case:
        previous_fetch = case.native.fetch
        replay_queries = []

        async def replay_fetch(query, *arguments):
            if query.startswith("SELECT resource_id,payload_json FROM"):
                replay_queries.append((query, arguments))
                return [{"resource_id": "marker", "payload_json": {"value": 1}}]
            return await previous_fetch(query, *arguments)

        case.native.fetch = replay_fetch
        result = await adapters.source_retry_rows(case.directory, "dataset", "LU:Organization:pass:2")
        assert result == [{"resource_id": "marker", "payload_json": {"value": 1}}]
        assert len(replay_queries) == 1
        query, arguments = replay_queries[0]
        assert "batch_ordinal<=$5" in query
        assert arguments == ("LU:Organization:pass:2", 2, None, None, 4)
        assert all("FOR UPDATE" not in call.args[0] for call in case.native.fetchrow.await_args_list)
        family_lock = next(
            index for index, (statement, _) in enumerate(case.queries) if "pg_advisory_xact_lock" in statement
        )
        header_reads = [
            index for index, (statement, _) in enumerate(case.queries) if "candidate_storage_json FROM" in statement
        ]
        assert header_reads[0] < family_lock < header_reads[-1]
        case.native.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "hash", "orphan", "missing"])
async def test_partition_counts_stream_original_antijoins(monkeypatch, fault):
    """Historical counts retain latest observations and exact marker anti-join failures."""
    async with _loading_count_case(monkeypatch) as case:

        def content(name):
            payload = {"resource_id": "org", "name": name}
            return dict(
                dataset_id="dataset",
                resource_type="Organization",
                resource_id="org",
                payload_json=payload,
                payload_hash=case.directory.resource_payload_sha256_for_contract(
                    payload, case.directory.LEGACY_RESOURCE_HASH_CONTRACT
                ),
                acquired_resource_sha256=None,
            )

        latest = content("Current")
        marker_by_field = dict(
            dataset_id="dataset",
            resource_type="LU:Organization:pass:2",
            resource_id="org",
            payload_hash="c" * 64,
            payload_json={"candidate_payload_hash": "f" * 64 if fault == "hash" else latest["payload_hash"]},
            acquired_resource_sha256=None,
        )
        facts = [
            {**marker_by_field, "resource_type": "LU:Organization:pass:1"},
            *([] if fault == "missing" else [marker_by_field]),
            content("Old"),
            latest,
        ]
        if fault == "orphan":
            facts.append({**marker_by_field, "resource_id": "orphan"})
        facts.sort(key=lambda item: (item["resource_type"], item["resource_id"]))
        queries = []

        async def cursor(query, *arguments, **options):
            queries.append((query, arguments, options))
            for fact in facts:
                yield fact

        case.native.cursor = cursor
        monkeypatch.setattr(adapters, "source_family_rows", AsyncMock(side_effect=AssertionError("payload list used")))
        counts = await adapters.source_partition_counts(case.directory, "dataset", "Organization")
        assert counts == dict(
            candidate_count=1,
            pass1_count=1,
            pass2_count=0 if fault == "missing" else 2 if fault == "orphan" else 1,
            invalid_candidate_count=int(fault in {"hash", "missing"}),
            orphan_proof_count=int(fault == "orphan"),
        )
        assert len(queries) == 1
        query, arguments, options = queries[0]
        assert "dataset_id=$1" in query and "batch_ordinal<=$5" in query
        assert arguments[0] == "dataset" and arguments[2] == 2 and arguments[4] == 4
        assert options == {"prefetch": 4096}


def _completed_partition_result(directory):
    """A completed partition deliberately returns no prepublication compatibility rows."""
    return directory.ResourceFetchResult(
        model=directory.ProviderDirectoryOrganization,
        rows=[],
        rows_fetched=1,
        rows_written=1,
        pages_fetched=2,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
        fetch_mode=directory.LAST_UPDATED_PARTITION_FETCH_MODE,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["linked", "reverse", "none"])
async def test_partition_deferral_keeps_requested_reference_seeds(monkeypatch, mode):
    """Compact linked/reverse seeds survive without calling the compatibility writer."""
    directory = import_module("process.provider_directory_fhir")
    context = directory.PaginationCheckpointContext(
        canonical_api_base="https://directory.example/fhir",
        source_scope_hash="scope",
        source_ids=("source",),
        owner_run_id="run",
        acquisition_root_run_id="root",
        endpoint_id="endpoint",
        dataset_id="dataset",
    )
    from tests.test_provider_directory_fhir_reviewed_serial_stop import _runner

    source_by_field = {
        "source_id": "source",
        "canonical_api_base": "https://directory.example/fhir",
        "_pagination_checkpoint_context": context,
    }
    runner = _runner(source_by_field, ("Organization",))
    runner.options = replace(
        runner.options,
        deferred_materialization=False,
        linked_resource_limit=int(mode == "linked"),
        scan_role_reverse_lookup_planned=mode == "reverse",
    )
    observed_calls = []

    async def rows(actual, dataset, types):
        observed_calls.append((actual, dataset, types))
        yield {"payload_json": {"resource_id": "org", "endpoint_refs": ["Endpoint/target"], "name": "Unused payload"}}

    monkeypatch.setattr(adapters, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(adapters, "_source_family_rows", rows)
    monkeypatch.setattr(directory, "_practitioner_role_reverse_lookup_resources", lambda source: ("Organization",))
    monkeypatch.setattr(
        directory, "_practitioner_role_reverse_lookup_params", lambda source: {"Organization": "organization"}
    )
    fetch_result = _completed_partition_result(directory)
    assert await runner.record("Organization", source_by_field, fetch_result) is None
    assert runner.count_by_resource["Organization"] == 1
    assert runner.options.deferred_materialization is False
    if mode == "none":
        assert runner.rows_by_resource == {"Organization": []} and observed_calls == []
    else:
        assert observed_calls == [(directory, "dataset", ("Organization",))]
        assert runner.rows_by_resource == {
            "Organization": directory._compact_linked_reference_rows(
                "Organization", [{"resource_id": "org", "endpoint_refs": ["Endpoint/target"]}]
            )
        }
        assert directory._linked_resource_refs(runner.rows_by_resource) == [
            ("Endpoint", "target", "Endpoint/target", "endpoint_refs")
        ]
        assert list(directory._scan_practitioner_role_seed_rows(source_by_field, runner.rows_by_resource)) == [
            ("organization", "Organization", "org")
        ]
    runner.persist_rows.assert_not_awaited()
    runner.partial_progress.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "root",
        "endpoint",
        "producer",
        "metadata",
        "resource",
        "dataset",
        "schema",
        "storage",
        "table_schema",
        "duplicate_table",
        "name",
        "phase",
        "prepared",
    ],
)
async def test_loading_count_refuses_substitution(monkeypatch, change):
    """Altered acquisition, producer, family, or qualified storage scope cannot reach the aggregate."""
    async with _loading_count_case(monkeypatch) as case:
        context, resource = case.context, "Organization"
        if change in {"root", "endpoint", "dataset"}:
            field = {"root": "acquisition_root_run_id", "endpoint": "endpoint_id", "dataset": "dataset_id"}[change]
            context = replace(context, **{field: "other"})
        if change == "producer":
            context = replace(context, source_ids=("other",))
        if change == "metadata":
            case.header["publication_metadata_json"]["resource_hash_contract"] = "changed-contract"
        if change == "resource":
            resource = "Location"
        if change == "schema":
            monkeypatch.setattr(case.directory, "_schema", lambda: "other")
        if change == "storage":
            case.header["candidate_storage_json"]["storage"]["storage_schema"] = "other"
        if change == "table_schema":
            case.header["candidate_storage_json"]["tables"]["resource_input"]["schema"] = "other"
        if change == "duplicate_table":
            tables = case.header["candidate_storage_json"]["tables"]
            tables["witness_input"] = deepcopy(tables["resource_input"])
        if change == "name":
            case.header["candidate_storage_json"]["tables"]["resource_input"]["name"] = 'invalid";name'
        if change == "phase":
            case.header["status"] = "published"
        if change == "prepared":
            binding = case.header["candidate_storage_json"]
            binding["phase"] = "prepared"
            binding["tables"].update(
                {
                    root: dict(schema="source_schema", name="pdgs_" + "a" * 32 + "_" + storage._SUFFIXES[root])
                    for root in storage.RELATIONS
                }
            )
        with pytest.raises(ValueError if change == "name" else GenericCandidateError):
            await case.directory._caresource_unique_candidate_count(context, resource)
        assert case.aggregates == []
        case.domain.first.assert_not_awaited()
        case.native.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_count_keeps_legacy_reader(monkeypatch):
    """Unenrolled callers retain the original mapped relation and row conversion API."""
    directory = import_module("process.provider_directory_fhir")
    context, _header = _loading_count_facts()
    database = SimpleNamespace(first=AsyncMock(return_value={"unique_candidate_rows": 7}))
    monkeypatch.setattr(directory, "db", database)
    assert await directory._caresource_unique_candidate_count(context, "Organization") == 7
    database.first.assert_awaited_once()
    assert database.first.await_args.kwargs == dict(dataset_id="dataset", resource_type="Organization")
    assert (
        directory._qt(directory._schema(), "provider_directory_dataset_resource") in database.first.await_args.args[0]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("resets", [{"*": 4}, {"Organization": 4}, {"*": 1, "Organization": 4}])
async def test_loading_count_honors_resets(monkeypatch, resets):
    """Both global and resource retry cutoffs remove old observations without deleting rows."""
    async with _loading_count_case(monkeypatch) as case:
        case.header["candidate_storage_json"]["resets"] = resets
        case.observations.append(
            dict(dataset_id="dataset", resource_type="Organization", resource_id="old", batch_ordinal=4)
        )
        assert await case.directory._caresource_unique_candidate_count(case.context, "Organization") == 0
        assert len(case.observations) == 1 and case.aggregates[0][1] == ("dataset", "Organization", 4, 4)


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [None, True, -1, 2**63, "2"])
async def test_loading_count_rejects_invalid_cardinality(monkeypatch, result):
    """A malformed native aggregate fails inside its Source savepoint without a legacy fallback."""
    async with _loading_count_case(monkeypatch) as case:
        scalar = case.native.fetchval.side_effect

        async def invalid(query, *arguments):
            observed = await scalar(query, *arguments)
            return result if query.startswith("SELECT count(DISTINCT") else observed

        case.native.fetchval.side_effect = invalid
        before = deepcopy((case.header, case.observations))
        with pytest.raises(GenericCandidateError, match="resource_count_invalid"):
            await case.directory._caresource_unique_candidate_count(case.context, "Organization")
        assert (case.header, case.observations) == before
        assert sum(point.rollback.await_count for point in case.savepoints) == 1
        case.domain.first.assert_not_awaited()
        case.native.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("aggregate failed"), asyncio.CancelledError()])
async def test_loading_count_rolls_back_on_query_failure(monkeypatch, failure):
    """SQL errors and cancellation unwind the count savepoint without changing candidate data."""
    async with _loading_count_case(monkeypatch) as case:
        scalar = case.native.fetchval.side_effect

        async def interrupted(query, *arguments):
            observed = await scalar(query, *arguments)
            if query.startswith("SELECT count(DISTINCT"):
                raise failure
            return observed

        case.native.fetchval.side_effect = interrupted
        before = deepcopy((case.header, case.observations))
        with pytest.raises(type(failure)) as caught:
            await case.directory._caresource_unique_candidate_count(case.context, "Organization")
        assert caught.value is failure
        assert (case.header, case.observations) == before
        assert len(case.aggregates) == 1
        assert sum(point.rollback.await_count for point in case.savepoints) == 1
        case.domain.first.assert_not_awaited()
        case.native.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_count_missing_dataset(monkeypatch):
    directory = import_module("process.provider_directory_fhir")
    context, _header = _loading_count_facts()
    database = SimpleNamespace(first=AsyncMock())
    monkeypatch.setattr(directory, "db", database)
    with pytest.raises(RuntimeError, match="caresource_dataset_id_missing"):
        await directory._caresource_unique_candidate_count(replace(context, dataset_id=None), "Organization")
    database.first.assert_not_awaited()


@pytest.fixture
def candidate_admission_scope(monkeypatch):
    from db.plan_release_storage import storage_relation
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

    directory = SimpleNamespace(
        _schema=lambda: "catalog",
        _qt=storage_relation,
        ProviderDirectoryAPIEndpoint=SimpleNamespace(__tablename__="provider_directory_api_endpoint"),
    )
    connection = SimpleNamespace(
        scalar=AsyncMock(return_value=True), first=AsyncMock(return_value={"endpoint_id": "endpoint"})
    )
    publisher = SimpleNamespace(
        policy=SimpleNamespace(schema="candidate_a"), storage_binding=FHIRSourceStorageBinding("catalog", "candidate_a")
    )
    token = policy._PUBLISHER.set(publisher)
    try:
        yield SimpleNamespace(directory=directory, connection=connection, publisher=publisher)
    finally:
        policy._PUBLISHER.reset(token)


@pytest.mark.asyncio
async def test_isolated_candidate_admission_reads_catalog_and_scopes_candidate_lock(candidate_admission_scope):
    case = candidate_admission_scope
    await adapters.lock_endpoint_dataset_candidate_admission(case.directory, case.connection, "endpoint")
    assert case.connection.scalar.await_args.kwargs == {
        "admission_scope": "provider-directory-source-candidate:catalog:candidate_a:endpoint"
    }
    query = case.connection.first.await_args.args[0]
    assert 'FROM "catalog"."provider_directory_api_endpoint"' in query
    assert "FOR UPDATE" not in query and "FOR SHARE" not in query
    assert case.connection.first.await_args.kwargs == {"endpoint_id": "endpoint"}
    assert "pg_try_advisory_xact_lock" in case.connection.scalar.await_args.args[0]


@pytest.mark.asyncio
async def test_separate_candidate_schemas_use_independent_admission_scopes(candidate_admission_scope):
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

    case = candidate_admission_scope
    for schema in ("candidate_a", "candidate_b"):
        case.publisher.policy.schema = schema
        case.publisher.storage_binding = FHIRSourceStorageBinding("catalog", schema)
        await adapters.lock_endpoint_dataset_candidate_admission(case.directory, case.connection, "endpoint")
    scopes = [call.kwargs["admission_scope"] for call in case.connection.scalar.await_args_list]
    assert len(set(scopes)) == 2 and all(scope.endswith(":endpoint") for scope in scopes)


@pytest.mark.asyncio
async def test_legacy_candidate_admission_keeps_endpoint_serialization_without_waiting(candidate_admission_scope):
    case = candidate_admission_scope
    case.publisher.storage_binding = None
    await adapters.lock_endpoint_dataset_candidate_admission(case.directory, case.connection, "endpoint")
    assert case.connection.scalar.await_args.kwargs == {"admission_scope": "endpoint"}
    assert "pg_try_advisory_xact_lock" in case.connection.scalar.await_args.args[0]
    assert case.connection.first.await_args.args[0].endswith("FOR UPDATE NOWAIT;")


@pytest.mark.asyncio
@pytest.mark.parametrize("acquired", [False, None, "true"])
async def test_candidate_admission_busy_stops_before_catalog_lookup(candidate_admission_scope, acquired):
    case = candidate_admission_scope
    case.connection.scalar.return_value = acquired
    with pytest.raises(RuntimeError, match="candidate_admission_busy"):
        await adapters.lock_endpoint_dataset_candidate_admission(case.directory, case.connection, "endpoint")
    case.connection.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["catalog", "storage"])
async def test_candidate_admission_rejects_mismatched_binding_before_sql(candidate_admission_scope, changed):
    case = candidate_admission_scope
    if changed == "catalog":
        case.directory._schema = lambda: "other_catalog"
    else:
        case.publisher.policy.schema = "other_candidate"
    with pytest.raises(GenericCandidateError, match="schema_changed"):
        await adapters.lock_endpoint_dataset_candidate_admission(case.directory, case.connection, "endpoint")
    case.connection.scalar.assert_not_awaited()
    case.connection.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_fhir_candidate_admission_entrypoint_uses_shared_adapter(monkeypatch):
    directory = import_module("process.provider_directory_fhir")
    lock = AsyncMock()
    monkeypatch.setattr(adapters, "lock_endpoint_dataset_candidate_admission", lock)
    connection = object()
    await directory._lock_endpoint_dataset_candidate_admission(connection, "endpoint")
    lock.assert_awaited_once_with(directory, connection, "endpoint")


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint_exists", [True, False])
async def test_fhir_validation_uses_shared_candidate_lock(monkeypatch, endpoint_exists):
    directory = import_module("process.provider_directory_fhir")
    lock = AsyncMock(return_value={"endpoint_id": "endpoint"} if endpoint_exists else None)
    monkeypatch.setattr(adapters, "lock_endpoint_dataset_candidate_admission", lock)
    candidate = SimpleNamespace(endpoint_id="endpoint", dataset_id="candidate")
    connection = SimpleNamespace(first=AsyncMock(return_value={"dataset_id": "candidate"}))

    if endpoint_exists:
        assert await directory._locked_endpoint_dataset_candidate_record(connection, candidate) == {
            "dataset_id": "candidate"
        }
        assert connection.first.await_args.args[0].strip().endswith("FOR UPDATE;")
        assert connection.first.await_args.kwargs == {"dataset_id": "candidate", "endpoint_id": "endpoint"}
    else:
        with pytest.raises(RuntimeError, match="api_endpoint_missing_during_validation"):
            await directory._locked_endpoint_dataset_candidate_record(connection, candidate)
        connection.first.assert_not_awaited()
    lock.assert_awaited_once_with(directory, connection, "endpoint")


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["advisory", "catalog"])
async def test_candidate_admission_preserves_sql_failure(candidate_admission_scope, stage):
    case = candidate_admission_scope
    failure = RuntimeError("admission SQL failed")
    if stage == "advisory":
        case.connection.scalar.side_effect = failure
    else:
        case.connection.first.side_effect = failure
    with pytest.raises(RuntimeError, match="admission SQL failed") as caught:
        await adapters.lock_endpoint_dataset_candidate_admission(case.directory, case.connection, "endpoint")
    assert caught.value is failure
    if stage == "advisory":
        case.connection.first.assert_not_awaited()
