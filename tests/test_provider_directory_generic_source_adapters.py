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
async def test_retained_connection_proxy_must_match_the_original_physical_publisher(monkeypatch):
    from importlib import import_module

    from db.connection import ConnectionProxy
    from process import provider_directory_projection_stage as projection

    directory = import_module("process.provider_directory_fhir")
    session = object()
    driver = object()
    monkeypatch.setattr(policy, "bound_publisher", lambda: SimpleNamespace(connection=driver))
    copy_driver = AsyncMock(return_value=driver)
    monkeypatch.setattr(projection, "_copy_driver", copy_driver)
    proxy = ConnectionProxy(object(), session, None)
    await directory._require_source_native_connection(proxy)
    copy_driver.assert_awaited_once_with(session)
    copy_driver.return_value = object()
    with pytest.raises(RuntimeError, match="physical_transaction_changed"):
        await directory._require_source_native_connection(proxy)


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
    from tests.test_provider_directory_generic_source_storage import _case, _catalog_row, _sequence_row

    directory = import_module("process.provider_directory_fhir")
    _, _, header_by_field = _case()
    metadata = dict(
        source_ids=["source-a"],
        selected_resources=["Organization"],
        resource_hash_contract=directory.LEGACY_RESOURCE_HASH_CONTRACT,
        source_release={"vector_sha256": "a" * 64},
    )
    binding = header_by_field["candidate_storage_json"]
    binding["identity"]["metadata"] = deepcopy(metadata)
    binding.update(ordinal=4, resets={"*": 1, "Organization": 2})
    header_by_field["publication_metadata_json"] = deepcopy(metadata)
    catalog_rows = []
    for table in binding["tables"].values():
        catalog = _catalog_row() | dict(relname=table["name"], oid=table["oid"], relowner=1, nspname="mrf")
        table.update(storage._layout_proof(catalog))
        if table["name"].endswith("_ri"):
            table["layout_sha256"] = storage._input_layout_hash(
                table["layout_sha256"], _sequence_row(owner=1, schema="mrf")
            )
        catalog_rows.append(catalog)
    context = directory.PaginationCheckpointContext(
        canonical_api_base="https://directory.example/fhir",
        source_scope_hash="scope",
        source_ids=("source-a",),
        owner_run_id="owner",
        acquisition_root_run_id="root",
        dataset_id="dataset",
        endpoint_id="endpoint",
    )
    return context, header_by_field, catalog_rows


async def _loading_scalar(case, query, *arguments):
    if query.startswith("SELECT nextval("):
        return case.header["candidate_storage_json"]["ordinal"] + 1
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
    return "repeatable read" if query == "SHOW transaction_isolation" else 1


async def _loading_execute(case, role_execute, query, *arguments):
    if query.startswith("UPDATE"):
        assert storage._json(arguments[2]) == case.header["candidate_storage_json"]
        case.header["candidate_storage_json"] = storage._json(arguments[1])
        return "UPDATE 1"
    return await role_execute(query)


@asynccontextmanager
async def _loading_count_case(monkeypatch):
    """Supply unit native catalog and COPY state while real Source and custody guards execute."""
    from process.plan_release_storage_catalog import CATALOG_SQL
    from tests.test_provider_directory_generic_source_policy import synthetic_source_database

    directory = import_module("process.provider_directory_fhir")
    context, header, catalog_rows = _loading_count_facts()
    domain = SimpleNamespace(first=AsyncMock(return_value={"unique_candidate_rows": 0}), status=AsyncMock())
    async with synthetic_source_database(monkeypatch, domain) as database, policy.source_transaction(database):
        monkeypatch.setattr(directory, "db", database)
        native = policy.bound_publisher().connection
        case = SimpleNamespace(
            directory=directory,
            context=context,
            header=header,
            catalog_rows=catalog_rows,
            database=database,
            domain=domain,
            native=native,
            observations=[],
            aggregates=[],
            savepoints=[],
        )
        role_fetch, role_execute = native.fetch, native.execute.side_effect
        original_transaction = native.transaction

        def transaction():
            savepoint = original_transaction()
            case.savepoints.append(savepoint)
            return savepoint

        native.transaction = transaction

        async def fetch(query, *arguments):
            if query == storage._INPUT_SEQUENCES_SQL:
                from tests.test_provider_directory_generic_source_storage import _sequence_row

                return [_sequence_row(owner=1, schema="mrf")]
            if query == CATALOG_SQL:
                assert arguments[1:] == (1, [])
                return deepcopy(case.catalog_rows)
            return await role_fetch(query, *arguments)

        async def first(query, *arguments):
            if "candidate_storage_json FROM" in query:
                return deepcopy(case.header)
            assert query == "SELECT session_user::text AS session,current_user::text AS current"
            return native.fetchrow.return_value

        async def copy(_name, *, records, columns, **_options):
            case.observations.extend(dict(zip(columns, values)) for values in records)
            return f"COPY {len(records)}"

        native.fetch, native.fetchrow.side_effect = fetch, first
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
        rows = _rows(2)
        await storage.append_generic_source_batch(case.database, case.directory, candidate, [*rows, rows[0]])
        before = deepcopy((case.header, case.observations))
        case.native.execute.reset_mock()
        count = await case.directory._caresource_unique_candidate_count(case.context, "Organization")
        assert count == 2
        assert len(case.aggregates) == 1 and case.aggregates[0][1] == ("dataset", "Organization", 2, 5)
        assert '"mrf"."pdgs_' in case.aggregates[0][0]
        assert (case.header, case.observations) == before
        assert case.header["candidate_storage_json"]["phase"] == "loading"
        assert len(case.native.execute.await_args_list) == 1
        assert "pg_advisory_xact_lock" in case.native.execute.await_args.args[0]
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
        assert "pg_advisory_xact_lock" in case.native.execute.await_args.args[0]
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
        "name",
        "oid",
        "owner",
        "phase",
        "prepared",
        "actor",
        "session",
        "child",
    ],
)
async def test_loading_count_refuses_substitution(monkeypatch, change):
    """No inherited authority or altered native binding can reach the aggregate."""
    async with _loading_count_case(monkeypatch) as case:
        context, resource = case.context, "Organization"
        original_binding = case.database._transaction_binding
        original_actor = case.native.fetchrow.return_value
        if change in {"root", "endpoint", "dataset"}:
            field = {"root": "acquisition_root_run_id", "endpoint": "endpoint_id", "dataset": "dataset_id"}[change]
            context = replace(context, **{field: "other"})
        if change == "producer":
            context = replace(context, source_ids=("other",))
        if change == "metadata":
            case.header["publication_metadata_json"]["source_release"]["vector_sha256"] = "b" * 64
        if change == "resource":
            resource = "Location"
        if change == "schema":
            monkeypatch.setattr(case.directory, "_schema", lambda: "other")
        if change == "name":
            case.header["candidate_storage_json"]["tables"]["resource_input"]["name"] = 'invalid";name'
        if change in {"oid", "owner"}:
            case.catalog_rows[0]["oid" if change == "oid" else "relowner"] = 999
        if change == "phase":
            case.header["status"] = "published"
        if change == "prepared":
            binding = case.header["candidate_storage_json"]
            binding["phase"] = "prepared"
            binding["tables"].update(
                {
                    root: dict(
                        name="pdgs_" + "a" * 32 + "_" + storage._SUFFIXES[root],
                        oid=20 + index,
                        owner=1,
                        layout_sha256="c" * 64,
                    )
                    for index, root in enumerate(storage.RELATIONS)
                }
            )
        if change == "actor":
            case.native.fetchrow.return_value = dict(session="source_publisher", current="other")
        if change == "session":
            case.database._transaction_binding = lambda: SimpleNamespace(
                session=SimpleNamespace(copy_to_table=object())
            )
        try:
            with pytest.raises(GenericCandidateError):
                if change == "child":
                    await asyncio.create_task(case.directory._caresource_unique_candidate_count(context, resource))
                else:
                    await case.directory._caresource_unique_candidate_count(context, resource)
        finally:
            case.database._transaction_binding = original_binding
            case.native.fetchrow.return_value = original_actor
        assert case.aggregates == []
        case.domain.first.assert_not_awaited()
        case.native.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_count_keeps_legacy_reader(monkeypatch):
    """Unenrolled callers retain the original mapped relation and row conversion API."""
    directory = import_module("process.provider_directory_fhir")
    context, _header, _catalog = _loading_count_facts()
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
@pytest.mark.parametrize("change", ["session", "actor"])
async def test_loading_count_rechecks_original_owner(monkeypatch, change):
    """The original session and actor must survive the awaited aggregate before its result escapes."""
    async with _loading_count_case(monkeypatch) as case:
        scalar = case.native.fetchval.side_effect
        original_binding, original_actor = case.database._transaction_binding, case.native.fetchrow.return_value

        async def altered(query, *arguments):
            observed = await scalar(query, *arguments)
            if query.startswith("SELECT count(DISTINCT"):
                if change == "session":
                    case.database._transaction_binding = lambda: SimpleNamespace(session=object())
                if change == "actor":
                    case.native.fetchrow.return_value = dict(session="source_publisher", current="other")
            return observed

        case.native.fetchval.side_effect = altered
        try:
            with pytest.raises(GenericCandidateError, match="transaction_changed"):
                await case.directory._caresource_unique_candidate_count(case.context, "Organization")
        finally:
            case.database._transaction_binding, case.native.fetchrow.return_value = original_binding, original_actor
        assert len(case.aggregates) == 1
        assert sum(point.rollback.await_count for point in case.savepoints) == 1
        case.domain.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_count_missing_dataset(monkeypatch):
    directory = import_module("process.provider_directory_fhir")
    context, _header, _catalog = _loading_count_facts()
    database = SimpleNamespace(first=AsyncMock())
    monkeypatch.setattr(directory, "db", database)
    with pytest.raises(RuntimeError, match="caresource_dataset_id_missing"):
        await directory._caresource_unique_candidate_count(replace(context, dataset_id=None), "Organization")
    database.first.assert_not_awaited()
