# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named source routing, configured transactions and batch failure safety."""

import asyncio
import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.connection import Database
from process import provider_directory_generic_source_policy as policy
from process import provider_directory_named_edition as editions
from process import provider_directory_source_storage_binding as isolated
from process.provider_directory_generic_candidate import GenericCandidateError


@asynccontextmanager
async def _callable_binding(factory, connection):
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.bind_generic_source_publisher(
            connection, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
        ):
            yield


@pytest.mark.asyncio
async def test_callable_capability_preserves_the_original_native_oid_tuple():
    factory, (connection, _) = _identity_factory(), _connection()
    capability = (101, 2**32 - 1)
    factory.source_callable_capability = AsyncMock(return_value=capability)
    async with _callable_binding(factory, connection):
        assert await policy.source_callable_capability(connection, factory.policy) is capability
    factory.source_callable_capability.assert_awaited_once_with(connection, factory.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing_factory", "wrong_factory", "connection", "policy", "factory_policy"])
async def test_callable_capability_refuses_substituted_original_authority(changed):
    factory, (connection, _) = _identity_factory(), _connection()
    capture = factory.source_callable_capability = AsyncMock(return_value=(101,))
    original_policy = factory.policy
    async with _callable_binding(factory, connection):
        token = None
        supplied_connection, supplied_policy = connection, original_policy
        if changed in {"missing_factory", "wrong_factory"}:
            substitute = (
                None
                if changed == "missing_factory"
                else SimpleNamespace(policy=original_policy, source_callable_capability=capture)
            )
            token = policy._FACTORY.set(substitute)
        elif changed == "connection":
            supplied_connection = object()
        elif changed == "policy":
            supplied_policy = _policy()
        else:
            factory.policy = _policy()
        try:
            with pytest.raises(GenericCandidateError, match="callable_original_publisher_required"):
                await policy.source_callable_capability(supplied_connection, supplied_policy)
        finally:
            factory.policy = original_policy
            if token is not None:
                policy._FACTORY.reset(token)
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_callable_capability_cannot_adopt_a_factory_after_publisher_binding():
    factory, (connection, _) = _identity_factory(), _connection()
    capture = factory.source_callable_capability = AsyncMock(return_value=(101,))
    async with policy.bind_generic_source_publisher(
        connection, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        token = policy._FACTORY.set(factory)
        try:
            with pytest.raises(GenericCandidateError, match="callable_original_publisher_required"):
                await policy.source_callable_capability(connection, factory.policy)
        finally:
            policy._FACTORY.reset(token)
    capture.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", [None, [101], (True,), (0,), (-1,), (2**32,), ("101",), (101, 101)])
async def test_callable_capability_rejects_malformed_or_non_native_oid_results(capability):
    factory, (connection, _) = _identity_factory(), _connection()
    factory.source_callable_capability = AsyncMock(return_value=capability)
    async with _callable_binding(factory, connection):
        with pytest.raises(GenericCandidateError, match="callable_capability_changed"):
            await policy.source_callable_capability(connection, factory.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["factory", "publisher", "policy"])
async def test_callable_capability_rechecks_original_context_after_await(changed):
    factory, (connection, _) = _identity_factory(), _connection()
    original_policy = factory.policy
    tokens = []

    async def capture(connection, supplied_policy):
        if changed == "factory":
            tokens.append((policy._FACTORY, policy._FACTORY.set(_identity_factory())))
        elif changed == "publisher":
            replacement = SimpleNamespace(**vars(policy.bound_publisher()))
            tokens.append((policy._PUBLISHER, policy._PUBLISHER.set(replacement)))
        else:
            factory.policy = _policy()
        await asyncio.sleep(0)
        return (101,)

    factory.source_callable_capability = capture
    async with _callable_binding(factory, connection):
        try:
            with pytest.raises(GenericCandidateError, match="callable_publisher_changed"):
                await policy.source_callable_capability(connection, original_policy)
        finally:
            factory.policy = original_policy
            for variable, token in reversed(tokens):
                variable.reset(token)


def _policy():
    return policy.GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))


def _connection():
    transitions = []

    def transaction():
        return SimpleNamespace(
            start=AsyncMock(side_effect=lambda: transitions.append("savepoint")),
            rollback=AsyncMock(side_effect=lambda: transitions.append("rollback")),
            commit=AsyncMock(side_effect=lambda: transitions.append("release")),
        )

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        is_closed=lambda: False,
        fetchrow=AsyncMock(return_value={"session": "source_publisher", "current": "source_publisher"}),
        execute=AsyncMock(),
        fetchval=AsyncMock(return_value="repeatable read"),
        transaction=transaction,
    )
    return connection, transitions


def _identity_factory(*, schema="source_schema"):
    identity, configured = ("a" * 40, "sha256:" + "b" * 64), replace(_policy(), schema=schema)
    factory = SimpleNamespace(transaction=lambda: None, policy=configured, producer_identity=identity)

    def require_current_policy():
        if factory.policy != configured or factory.producer_identity != identity:
            raise ValueError("source_policy_changed")

    factory._require_current_policy = Mock(side_effect=require_current_policy)
    return factory


def _isolated_factory():
    factory = _identity_factory()
    factory.storage_binding = isolated.FHIRSourceStorageBinding("catalog_schema", "source_schema")
    return factory


def _named_factory():
    factory = _identity_factory()
    factory.storage_binding = isolated.NamedFHIRSourceStorage("catalog_schema", "source_schema")
    return factory


def _edition_reader(state, current_id, edition, pointer, counts_by_resource):
    async def fetchrow(sql, *args):
        if pointer in sql:
            assert args == ("catalog_schema", "endpoint")
            return None if state == "empty" else {"dataset_id": current_id, "schema_name": edition}
        assert f'FROM "{edition}"."provider_directory_endpoint_dataset"' in sql
        assert "status='published' AND published_at IS NOT NULL" in sql
        assert args == (current_id,)
        if state == "missing":
            return None
        return {
            "dataset_id": current_id,
            "endpoint_id": "foreign" if state == "foreign" else "endpoint",
            "publication_metadata_json": json.dumps({"source_summary_v1": {"resource_counts": counts_by_resource}}),
        }

    return SimpleNamespace(
        fetchval=AsyncMock(return_value=None if state == "legacy" else pointer),
        fetchrow=AsyncMock(side_effect=fetchrow),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["legacy", "empty", "self", "missing", "foreign", "published"])
async def test_named_predecessor_reads_only_the_catalog_edition(monkeypatch, state):
    from importlib import import_module

    from process import provider_directory_generic_source_adapters as adapters
    from process import provider_directory_projection_stage as projection

    directory = import_module("process.provider_directory_fhir")
    current_id = "candidate" if state == "self" else "incumbent"
    edition = editions.edition_schema_name(current_id)
    pointer = '"provider_directory_editions"."current_edition"'
    counts_by_resource = {"Practitioner": 3}
    incumbent_by_field = {"dataset_id": current_id, "status": "published", "resource_counts": counts_by_resource}
    legacy = AsyncMock(return_value=incumbent_by_field)
    driver = _edition_reader(state, current_id, edition, pointer, counts_by_resource)
    session = object()

    @asynccontextmanager
    async def transaction():
        yield session

    native = AsyncMock(return_value=driver)
    monkeypatch.setattr(directory, "_schema", lambda: "catalog_schema")
    monkeypatch.setattr(directory.db, "_transaction_binding", lambda: None)
    monkeypatch.setattr(directory.db, "transaction", transaction)
    monkeypatch.setattr(directory.db, "first", legacy)
    monkeypatch.setattr(projection, "_copy_driver", native)
    async with policy.bind_generic_source_transaction_factory(_named_factory()):
        if state in {"missing", "foreign"}:
            with pytest.raises(GenericCandidateError, match="current_edition_changed"):
                await adapters.current_endpoint_dataset_record(directory, "endpoint", exclude_dataset_id="candidate")
        else:
            assert await adapters.current_endpoint_dataset_record(
                directory, "endpoint", exclude_dataset_id="candidate"
            ) == ({} if state == "self" else incumbent_by_field)

    native.assert_awaited_once_with(session)
    driver.fetchval.assert_awaited_once_with("SELECT to_regclass($1)::text", pointer)
    assert driver.fetchrow.await_count == (0 if state == "legacy" else 1 if state in {"empty", "self"} else 2)
    if state in {"legacy", "empty"}:
        assert legacy.await_count == 1
        assert '"catalog_schema"."provider_directory_endpoint_dataset"' in legacy.await_args.args[0]
    else:
        legacy.assert_not_awaited()


@pytest.mark.asyncio
async def test_named_startup_uses_the_bound_schema_without_running_legacy_ddl(monkeypatch):
    from importlib import import_module

    from process import provider_directory_generic_source_adapters as adapters

    directory = import_module("process.provider_directory_fhir")
    factory = _named_factory()
    connection, _ = _connection()

    @asynccontextmanager
    async def source_transaction(database):
        assert database is directory.db
        yield

    monkeypatch.setattr(adapters, "source_transaction", source_transaction)
    monkeypatch.setattr(directory, "_schema", lambda: "catalog_schema")
    verify_roles = AsyncMock()
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.bind_generic_source_publisher(
            connection, factory.policy, verify_role_profile=verify_roles, transaction_isolation="repeatable read"
        ):
            assert await adapters.require_generic_source_layout(directory) is factory.storage_binding
            monkeypatch.setattr(directory, "_schema", lambda: "other_catalog")
            with pytest.raises(GenericCandidateError, match="schema_changed"):
                await adapters.require_generic_source_layout(directory)
    verify_roles.assert_not_awaited()
    connection.execute.assert_awaited_once_with(
        'SELECT dataset_id FROM "source_schema"."provider_directory_endpoint_dataset" LIMIT 0'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("isolated_storage", [False, True])
async def test_seen_cleanup_keeps_private_storage_separate(monkeypatch, isolated_storage):
    from importlib import import_module

    directory = import_module("process.provider_directory_fhir")
    factory = _isolated_factory() if isolated_storage else _identity_factory()
    execute = AsyncMock(return_value=9)
    monkeypatch.setattr(directory.db, "status", execute)
    async with policy.bind_generic_source_transaction_factory(factory):
        assert await directory._clear_resource_rows_seen(None) == 0
        assert await directory._clear_resource_rows_seen("synthetic-run") == (0 if isolated_storage else 9)
        if isolated_storage:
            factory._require_current_policy.assert_called_once_with()
            execute.assert_not_awaited()
            factory.producer_identity = ("c" * 40, factory.producer_identity[1])
            with pytest.raises(ValueError, match="source_policy_changed"):
                await directory._clear_resource_rows_seen("synthetic-run")
        else:
            assert "provider_directory_import_seen" in execute.await_args.args[0]
            assert execute.await_args.kwargs == {"run_id": "synthetic-run"}


@pytest.mark.asyncio
@pytest.mark.parametrize("isolated_storage", [False, True])
@pytest.mark.parametrize("dataset", [None, "synthetic-incumbent"])
async def test_published_predecessor_uses_the_canonical_catalog(monkeypatch, isolated_storage, dataset):
    from importlib import import_module

    from process import provider_directory_projection_stage as projection

    directory = import_module("process.provider_directory_fhir")
    factory = _isolated_factory() if isolated_storage else _identity_factory()
    query = AsyncMock(return_value={"dataset_id": dataset} if dataset else None)
    monkeypatch.setattr(directory, "_schema", lambda: "catalog_schema")
    monkeypatch.setattr(directory.db, "first", query)
    monkeypatch.setattr(directory.db, "_transaction_binding", lambda: None)

    @asynccontextmanager
    async def transaction():
        yield object()

    monkeypatch.setattr(directory.db, "transaction", transaction)
    monkeypatch.setattr(
        projection, "_copy_driver", AsyncMock(return_value=SimpleNamespace(fetchval=AsyncMock(return_value=None)))
    )
    async with policy.bind_generic_source_transaction_factory(factory):
        assert await directory._current_endpoint_dataset_id("endpoint", exclude_dataset_id="candidate") == dataset
    assert '"catalog_schema"."provider_directory_endpoint_dataset"' in query.await_args.args[0]
    assert "source_schema" not in query.await_args.args[0]
    assert "is_current = true" in query.await_args.args[0]
    assert "dataset_id <> :exclude_dataset_id" in query.await_args.args[0]
    assert query.await_args.kwargs == {"endpoint_id": "endpoint", "exclude_dataset_id": "candidate"}
    if isolated_storage:
        assert factory._require_current_policy.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["schema", "transaction", "producer"])
async def test_published_predecessor_refuses_changed_reader_scope(monkeypatch, changed):
    from importlib import import_module

    from process import provider_directory_projection_stage as projection

    directory = import_module("process.provider_directory_fhir")
    factory = _isolated_factory()
    query = AsyncMock(return_value={"dataset_id": "synthetic-incumbent"})
    monkeypatch.setattr(directory, "_schema", lambda: "wrong_schema" if changed == "schema" else "catalog_schema")
    monkeypatch.setattr(directory.db, "_transaction_binding", lambda: object() if changed == "transaction" else None)
    monkeypatch.setattr(directory.db, "first", query)

    async def changed_producer(*args, **kwargs):
        factory.producer_identity = ("c" * 40, factory.producer_identity[1])
        return {"dataset_id": "synthetic-incumbent"}

    if changed == "producer":
        query.side_effect = changed_producer

    @asynccontextmanager
    async def transaction():
        yield object()

    monkeypatch.setattr(directory.db, "transaction", transaction)
    monkeypatch.setattr(
        projection, "_copy_driver", AsyncMock(return_value=SimpleNamespace(fetchval=AsyncMock(return_value=None)))
    )
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises((RuntimeError, ValueError), match="catalog_reader_required|source_policy_changed"):
            await directory._current_endpoint_dataset_id("endpoint", exclude_dataset_id="candidate")
    assert query.await_count == (1 if changed == "producer" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "function_name,arguments",
    [
        ("_artifact_stored_content_proof_sql", ("dataset", "metadata", "proof")),
        ("_artifact_legacy_subset_proof_sql", ("dataset", "metadata", "proof")),
        ("_artifact_subset_parent_proof_sql", ("dataset", "metadata", "proof")),
        ("_artifact_legacy_content_proof_valid_sql", ("metadata", "proof", "run_id")),
        ("_artifact_semantic_legacy_proof_sql", ("proof",)),
        ("_artifact_semantic_content_proof_valid_sql", ("metadata", "proof", "run_id", "contract")),
    ],
)
async def test_metadata_validators_keep_canonical_function_qualification(monkeypatch, function_name, arguments):
    from importlib import import_module

    directory = import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory, "_schema", lambda: "catalog_schema")
    async with policy.bind_generic_source_transaction_factory(_isolated_factory()):
        fragment = getattr(directory, function_name)(*arguments)
        assert '"catalog_schema"."provider_directory_subset_' in fragment
        assert "source_schema" not in fragment
        with pytest.raises(GenericCandidateError, match="isolated_transaction_required"):
            directory._qt("catalog_schema", "provider_directory_subset_content_proof_valid")


@pytest.mark.asyncio
async def test_private_candidate_admission_needs_no_canonical_functions(monkeypatch):
    from importlib import import_module

    from process.provider_directory_generic_source_storage import HEADER

    directory = import_module("process.provider_directory_fhir")
    factory = _isolated_factory()
    connection, _, _ = _isolated_connection(factory)
    monkeypatch.setattr(directory, "_schema", lambda: "catalog_schema")
    async with _callable_binding(factory, connection):
        query = directory._private_verification_state_sql(directory._qt("catalog_schema", HEADER))
    assert f'"source_schema"."{HEADER}"' in query
    assert "provider_directory_cms_npd_stale_candidate" not in query
    for name in (
        "provider_directory_subset_content_proof_valid",
        "provider_directory_subset_canonical_sha256",
        "provider_directory_endpoint_dataset_admission_metadata_sha256",
    ):
        assert f'"catalog_schema"."{name}"' not in query
        assert f'"source_schema"."{name}"' not in query
    assert "dataset.endpoint_id = :endpoint_id" in query
    assert "dataset.dataset_id <> :dataset_id" in query
    assert "LIMIT 1 FOR UPDATE" in query
    assert "octet_length(dataset.publication_metadata_json::text)" in query


@pytest.mark.asyncio
async def test_private_admission_routes_through_original_binding(monkeypatch):
    """A competing validated private header blocks without canonical callable access."""
    from importlib import import_module

    from tests.test_provider_directory_twin_root_verification import _candidate, _private_header

    directory = import_module("process.provider_directory_fhir")
    factory = _isolated_factory()
    driver, _, _ = _isolated_connection(factory)
    candidate = _candidate(requires_verification=False)
    executor = SimpleNamespace(first=AsyncMock(return_value=_private_header(has_admission_receipt=True)))
    monkeypatch.setattr(directory, "_schema", lambda: "catalog_schema")
    async with _callable_binding(factory, driver):
        with pytest.raises(RuntimeError, match="active_conflict"):
            await directory._assert_no_conflicting_endpoint_candidate(executor, candidate)
    query = executor.first.await_args.args[0]
    assert 'FROM "source_schema"."provider_directory_endpoint_dataset"' in query
    assert "LIMIT 1 FOR UPDATE" in query
    assert "provider_directory_endpoint_dataset_admission_metadata_sha256" not in query
    assert executor.first.await_args.kwargs["dataset_id"] == candidate.dataset_id
    assert executor.first.await_args.kwargs["endpoint_id"] == candidate.endpoint_id


@pytest.mark.asyncio
async def test_cms_dispositions_remain_scoped_to_shared_headers():
    from process.provider_directory_cms_npd_recovery import candidate_available_sql
    from process.provider_directory_generic_source_storage import HEADER

    factory = _isolated_factory()
    connection, _, _ = _isolated_connection(factory)
    canonical, private = f'"catalog_schema"."{HEADER}"', f'"source_schema"."{HEADER}"'
    legacy_predicate = candidate_available_sql("dataset", "catalog_schema", dataset_relation=canonical)
    assert '"catalog_schema"."provider_directory_cms_npd_stale_candidate"' in legacy_predicate
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(GenericCandidateError, match="isolated_transaction_required"):
            candidate_available_sql("dataset", "catalog_schema", dataset_relation=private)
    async with _callable_binding(factory, connection):
        assert candidate_available_sql("dataset", "catalog_schema", dataset_relation=private) == "TRUE"
        assert candidate_available_sql("dataset", "catalog_schema", dataset_relation=canonical) == legacy_predicate
        assert (
            candidate_available_sql("dataset", "catalog_schema", dataset_relation='"other"."header"')
            == legacy_predicate
        )
        assert candidate_available_sql("dataset", "catalog_schema") == legacy_predicate
        with pytest.raises(GenericCandidateError, match="schema_changed"):
            candidate_available_sql("dataset", "wrong_schema", dataset_relation=private)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["private", "changed", "whitespace", "immutable", "legacy"])
async def test_validation_preserves_private_predecessor_and_legacy_current_fence(monkeypatch, case):
    from importlib import import_module

    directory = import_module("process.provider_directory_fhir")
    candidate = directory.EndpointDatasetCandidate(
        endpoint_id="endpoint",
        dataset_id="candidate",
        acquisition_root_run_id="run",
        source_ids=("source",),
        selected_resources=("Location",),
        expected_resources=("Location",),
        import_run_id="run",
        previous_dataset_id="incumbent",
        semantic_projection_as_of="2026-01-01",
    )
    locked_by_field = {
        "acquisition_root_run_id": "run",
        "is_current": False,
        "status": directory.ENDPOINT_DATASET_PUBLISHED if case == "immutable" else directory.ENDPOINT_DATASET_ACQUIRING,
        "previous_dataset_id": {"changed": "other", "whitespace": " incumbent "}.get(case, "incumbent"),
        "publication_metadata_json": directory._endpoint_dataset_candidate_metadata(candidate),
    }
    lock_candidate = AsyncMock(return_value=locked_by_field)
    lock_current = AsyncMock(return_value="incumbent")
    monkeypatch.setattr(directory, "_locked_endpoint_dataset_candidate_record", lock_candidate)
    monkeypatch.setattr(directory, "_locked_current_endpoint_dataset_id", lock_current)
    factory = _identity_factory() if case == "legacy" else _isolated_factory()
    connection, _ = _connection()
    if case != "legacy":
        connection, _, _ = _isolated_connection(factory)
    async with _callable_binding(factory, connection):
        if case in {"changed", "whitespace", "immutable"}:
            expected = "candidate_stale" if case == "immutable" else "current_changed"
            with pytest.raises(RuntimeError, match=expected):
                await directory._lock_endpoint_dataset_for_validation(connection, candidate)
        else:
            assert await directory._lock_endpoint_dataset_for_validation(connection, candidate) == "incumbent"
    lock_candidate.assert_awaited_once_with(connection, candidate)
    assert lock_current.await_count == (1 if case == "legacy" else 0)


def _isolated_connection(factory):
    """Keep the existing helper shape while supplying only portable names."""
    from dataclasses import asdict

    connection, transitions = _connection()
    return connection, asdict(factory.storage_binding), transitions


@pytest.mark.asyncio
async def test_named_binding_routes_portable_names_without_catalog_capture():
    from dataclasses import replace

    factory = _isolated_factory()
    connection, _, _ = _isolated_connection(factory)
    factory.storage_binding = replace(factory.storage_binding)
    connection.fetch = AsyncMock(side_effect=AssertionError("catalog capture reached"))
    async with _callable_binding(factory, connection):
        publisher = policy.bound_publisher()
        assert isolated.storage_document(publisher) == {
            "catalog_schema": "catalog_schema",
            "storage_schema": "source_schema",
        }
        await policy._bind_isolated_storage(publisher)
    connection.execute.assert_awaited_once_with(
        'SELECT dataset_id FROM "source_schema"."provider_directory_endpoint_dataset" LIMIT 0'
    )
    connection.fetchrow.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    connection.fetch.assert_not_awaited()
    assert policy._PUBLISHER.get() is None and policy._FACTORY.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError("read denied"), asyncio.CancelledError()])
async def test_named_layout_preserves_access_failure_without_capture_or_fallback(failure):
    factory = _named_factory()
    connection, _ = _connection()
    connection.execute.side_effect = failure
    factory.source_callable_capability = AsyncMock(side_effect=AssertionError("catalog capture reached"))
    async with _callable_binding(factory, connection):
        with pytest.raises(type(failure)) as raised:
            await policy._bind_isolated_storage(policy.bound_publisher())
        assert raised.value is failure
    connection.execute.assert_awaited_once()
    connection.fetchrow.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    factory.source_callable_capability.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["schema", "missing", "shape"])
async def test_named_layout_refuses_changed_declared_storage_before_sql(changed):
    factory = _named_factory()
    connection, _ = _connection()
    async with _callable_binding(factory, connection):
        publisher = SimpleNamespace(**vars(policy.bound_publisher()))
        if changed == "schema":
            publisher.policy = policy.GenericSourcePolicy(
                "other_schema", "source_owner", "source_publisher", ("reader",)
            )
        elif changed == "missing":
            publisher.storage_binding = None
        else:
            publisher.storage_binding = SimpleNamespace(**vars(factory.storage_binding))
        with pytest.raises(GenericCandidateError, match="storage_changed"):
            await policy._bind_isolated_storage(publisher)
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_batch_loading", [True, False])
async def test_named_batch_and_completion_use_declared_transactions_without_capture(monkeypatch, is_batch_loading):
    database, factory, opened = _factory_case(monkeypatch)
    factory.storage_binding = isolated.FHIRSourceStorageBinding("catalog_schema", "source_schema")
    capture = factory.source_callable_capability = AsyncMock(side_effect=AssertionError("catalog capture reached"))
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.source_transaction(database, isolation="repeatable read", is_batch_loading=is_batch_loading):
            publisher = policy.bound_publisher()
            assert policy.current_source_storage_binding() == factory.storage_binding
            assert policy.source_copy_session() is opened[-1][0]
            publisher.connection.fetchrow.assert_not_awaited()
            publisher.connection.fetchval.assert_not_awaited()
    capture.assert_not_awaited()
    assert [isolation for _, isolation in opened] == ["repeatable read"]
    assert policy._PUBLISHER.get() is policy._OWNER.get() is policy._FACTORY.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["transaction", "policy", "isolation"])
async def test_publisher_refuses_invalid_transaction_declaration_before_sql(change):
    connection, _ = _connection()
    configured, isolation = _policy(), "repeatable read"
    if change == "transaction":
        connection.is_in_transaction = lambda: False
    elif change == "policy":
        configured = SimpleNamespace(**vars(configured))
    else:
        isolation = "unsupported"
    with pytest.raises(GenericCandidateError, match="transaction_required|explicit_policy_required|isolation_required"):
        async with policy.bind_generic_source_publisher(connection, configured, transaction_isolation=isolation):
            pytest.fail("invalid transaction declaration admitted")
    connection.execute.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_named_startup_rechecks_catalog_scope_without_legacy_ddl(monkeypatch, changed):
    from importlib import import_module

    from process import provider_directory_generic_source_adapters as adapters

    directory = import_module("process.provider_directory_fhir")
    factory = _isolated_factory()
    connection, _, _ = _isolated_connection(factory)

    @asynccontextmanager
    async def original_transaction(database):
        assert database is directory.db
        yield

    monkeypatch.setattr(adapters, "source_transaction", original_transaction)
    monkeypatch.setattr(directory, "_schema", lambda: "wrong_catalog" if changed else "catalog_schema")
    legacy_probe = AsyncMock(side_effect=AssertionError("legacy schema mutation path reached"))
    monkeypatch.setattr(directory, "_is_table_present", legacy_probe)
    async with _callable_binding(factory, connection):
        if changed:
            with pytest.raises(GenericCandidateError, match="schema_changed"):
                await directory._ensure_provider_directory_tables()
        else:
            await directory._ensure_provider_directory_tables()
    legacy_probe.assert_not_awaited()
    assert connection.execute.await_count == (0 if changed else 1)
    assert all("LIMIT 0" in call.args[0] for call in connection.execute.await_args_list)


@pytest.mark.asyncio
async def test_isolated_relation_routing_preserves_catalog_and_refuses_unverified_overrides():
    from importlib import import_module

    from process.provider_directory_generic_source_storage import HEADER, RELATIONS
    from process.provider_directory_proof_store import PROVIDER_DIRECTORY_PROOF_SHARD_TABLE, _proof_relation
    from process.provider_directory_source_checkpoint_catalog import CHECKPOINT_LAYOUTS

    directory = import_module("process.provider_directory_fhir")
    factory = _isolated_factory()
    connection, _, _ = _isolated_connection(factory)
    async with _callable_binding(factory, connection):
        assert directory._qt("catalog_schema", HEADER) == f'"source_schema"."{HEADER}"'
        assert _proof_relation("catalog_schema") == f'"source_schema"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"'
        for root in CHECKPOINT_LAYOUTS:
            assert directory._qt("catalog_schema", root) == f'"source_schema"."{root}"'
            with pytest.raises(GenericCandidateError, match="isolated_header_override_changed"):
                isolated.source_relation_coordinates("catalog_schema", root, "other")
        with pytest.raises(GenericCandidateError, match="isolated_header_override_changed"):
            isolated.source_relation_coordinates("catalog_schema", PROVIDER_DIRECTORY_PROOF_SHARD_TABLE, "other")
        assert directory._qt("catalog_schema", "provider_directory_source") == (
            '"catalog_schema"."provider_directory_source"'
        )
        with pytest.raises(GenericCandidateError, match="schema_changed"):
            directory._qt("source_schema", HEADER)
        with pytest.raises(GenericCandidateError, match="isolated_relation_unsupported"):
            directory._qt("catalog_schema", "provider_directory_resource_identity")
        with pytest.raises(GenericCandidateError, match="isolated_relation_override_changed"):
            directory._qt("catalog_schema", RELATIONS[0])
        with directory._provider_directory_artifact_relation_scope({RELATIONS[0]: "other_heap"}):
            with pytest.raises(GenericCandidateError, match="isolated_relation_override_changed"):
                directory._qt("catalog_schema", RELATIONS[0])
        tables_by_root = {root: "pdgs_" + "a" * 32 + "_" + str(index) for index, root in enumerate(RELATIONS)}
        with isolated.isolated_source_relation_scope(policy.bound_publisher(), tables_by_root):
            with pytest.raises(GenericCandidateError, match="isolated_relation_override_changed"):
                directory._qt("catalog_schema", RELATIONS[0])
            with directory._provider_directory_artifact_relation_scope(tables_by_root):
                assert (
                    directory._qt("catalog_schema", RELATIONS[0]) == f'"source_schema"."{tables_by_root[RELATIONS[0]]}"'
                )
        with directory._provider_directory_artifact_relation_scope({"provider_directory_source": "other_source"}):
            with pytest.raises(GenericCandidateError, match="catalog_override_forbidden"):
                directory._qt("catalog_schema", "provider_directory_source")


@pytest.mark.asyncio
async def test_named_routing_requires_transaction_and_uses_separate_publication_route():
    factory = _isolated_factory()
    connection, _, _ = _isolated_connection(factory)
    factory.publication_session_factory = AsyncMock()
    async with policy.bind_generic_source_transaction_factory(factory):
        assert isolated.source_relation_coordinates("catalog_schema", "import_run", "import_run") == (
            "catalog_schema",
            "import_run",
        )
        with pytest.raises(GenericCandidateError, match="isolated_transaction_required"):
            isolated.source_relation_coordinates(
                "catalog_schema", "provider_directory_endpoint_dataset", "provider_directory_endpoint_dataset"
            )
        with pytest.raises(GenericCandidateError, match="isolated_activation_unavailable"):
            await policy.source_publication_sessions()
        async with policy.bind_generic_source_publisher(
            connection, factory.policy, transaction_isolation="repeatable read"
        ):
            assert policy.current_source_storage_binding() == factory.storage_binding
    factory.publication_session_factory.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("storage_schema", "catalog_schema"),
        ("storage_schema", "pg_private"),
        ("storage_schema", "public"),
        ("catalog_schema", "information_schema"),
        ("catalog_schema", "pg_catalog"),
        ("storage_schema", "bad-name"),
    ],
)
def test_named_binding_refuses_ambiguous_or_invalid_schemas(field, value):
    from dataclasses import replace

    with pytest.raises((GenericCandidateError, ValueError)):
        replace(_isolated_factory().storage_binding, **{field: value})


def test_legacy_policy_serialization_and_unbound_routing_remain_exact():
    from dataclasses import asdict

    assert asdict(_policy()) == dict(
        schema="source_schema",
        owner_role="source_owner",
        publisher_role="source_publisher",
        runtime_roles=("source_reader",),
        control_runtime_roles=(),
    )
    assert isolated.source_relation_coordinates("catalog_schema", "any_root", "legacy_stage") == (
        "catalog_schema",
        "legacy_stage",
    )


def test_source_producer_identity_requires_bound_factory():
    with pytest.raises(GenericCandidateError, match="transaction_factory_required"):
        policy.source_producer_identity()


@pytest.mark.asyncio
async def test_source_producer_identity_requires_trusted_policy_freshness():
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=lambda: None)):
        with pytest.raises(GenericCandidateError, match="transaction_factory_required"):
            policy.source_producer_identity()


@pytest.mark.asyncio
async def test_source_producer_identity_preserves_immutable_tuple_in_workers():
    factory = _identity_factory()

    async def worker():
        assert policy.source_producer_identity() is factory.producer_identity
        assert policy._PUBLISHER.get() is None and policy._OWNER.get() is None

    async with policy.bind_generic_source_transaction_factory(factory):
        await asyncio.create_task(worker())
    factory._require_current_policy.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity", [None, ["node", "producer"], ("node",), ("node", ""), ("node", 1), ("node", "producer")]
)
async def test_source_producer_identity_refuses_malformed_tuple(identity):
    factory = _identity_factory()
    factory.producer_identity = identity
    factory._require_current_policy = Mock()
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(GenericCandidateError, match="producer_identity_required"):
            policy.source_producer_identity()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["policy", "producer_identity"])
async def test_source_producer_identity_rechecks_factory_freshness(changed):
    factory = _identity_factory()
    async with policy.bind_generic_source_transaction_factory(factory):
        setattr(factory, changed, None)
        with pytest.raises(ValueError, match="source_policy_changed"):
            policy.source_producer_identity()


@pytest.mark.asyncio
async def test_source_producer_identity_rechecks_publisher_policy_and_producer():
    factory = _identity_factory()
    connection, _ = _connection()
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.bind_generic_source_publisher(
            connection, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
        ):
            assert policy.source_producer_identity() is factory.producer_identity
            factory.policy = _policy()
            assert policy.source_producer_identity() == factory.producer_identity
            token = policy._FACTORY.set(_identity_factory())
            try:
                assert policy.source_producer_identity() == factory.producer_identity
            finally:
                policy._FACTORY.reset(token)


@pytest.mark.asyncio
async def test_missing_publisher_refuses_before_transaction_access():
    transaction_binding = Mock(side_effect=AssertionError("target access reached"))
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        async with policy.source_owner_transaction(SimpleNamespace(_transaction_binding=transaction_binding)):
            pytest.fail("ordinary session became an owner")
    transaction_binding.assert_not_called()


@pytest.mark.asyncio
async def test_original_publication_requires_trusted_factory_before_io():
    with pytest.raises(GenericCandidateError, match="original_publication_required"):
        await policy.source_publication_sessions()
    with pytest.raises(GenericCandidateError, match="original_publication_required"):
        async with policy.original_source_publication(object(), object()):
            pytest.fail("unbound original publication admitted")


@pytest.mark.asyncio
async def test_original_publication_delegates_same_session_without_owning_lifecycle(monkeypatch):
    database, session, sessions = object(), SimpleNamespace(in_transaction=lambda: True), object()
    calls = []

    @asynccontextmanager
    async def original(supplied_database, supplied_session, *, isolation):
        calls.append((supplied_database, supplied_session, isolation))
        yield supplied_session

    @asynccontextmanager
    async def publication_body(supplied_database):
        assert supplied_database is database
        yield session

    monkeypatch.setattr(policy, "source_publication_body", publication_body)
    factory = _identity_factory()
    factory.publication_session_factory = AsyncMock(return_value=sessions)
    factory.bind_existing_transaction = original
    async with policy.bind_generic_source_transaction_factory(factory):
        assert await policy.source_publication_sessions() is sessions
        async with policy.original_source_publication(database, session) as bound:
            assert bound is session
    assert calls == [(database, session, "read committed")]
    factory.publication_session_factory.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_intake_only_factory_cannot_supply_final_publication():
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=lambda: None)):
        with pytest.raises(GenericCandidateError, match="publication_factory_required"):
            await policy.source_publication_sessions()


@pytest.mark.asyncio
async def test_configured_owner_role_is_restored_after_candidate_writes(monkeypatch):
    connection, transitions = _connection()
    session = object()
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    verifier = AsyncMock()
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=verifier, transaction_isolation="repeatable read"
    ):
        async with policy.source_owner_transaction(database) as publisher:
            assert publisher.connection is connection
    assert transitions == ["savepoint", "release"]
    assert [call.args[0] for call in connection.execute.await_args_list] == [
        'SET LOCAL ROLE "source_owner"',
        "RESET ROLE",
    ]
    verifier.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (RuntimeError("rejected"), asyncio.CancelledError()))
async def test_rejection_and_cancellation_roll_back_original_savepoint(monkeypatch, failure):
    connection, transitions = _connection()
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=object()))
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        with pytest.raises(type(failure)):
            async with policy.source_owner_transaction(database):
                raise failure
    assert transitions == ["savepoint", "rollback"]
    assert [call.args[0] for call in connection.execute.await_args_list] == ['SET LOCAL ROLE "source_owner"']


@pytest.mark.asyncio
async def test_nested_helpers_reuse_only_the_same_original_owner(monkeypatch):
    connection, transitions = _connection()
    session = SimpleNamespace(in_transaction=lambda: True)
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        async with policy.source_owner_transaction(database):
            connection.fetchrow.return_value = {"session": "source_publisher", "current": "source_owner"}
            async with policy.source_owner_transaction(database):
                assert policy.source_copy_session() is session
        with pytest.raises(GenericCandidateError, match="owner_transaction_required"):
            policy.source_copy_session()
    assert transitions == ["savepoint", "savepoint", "release", "release"]
    assert [call.args[0] for call in connection.execute.await_args_list] == [
        'SET LOCAL ROLE "source_owner"',
        "RESET ROLE",
    ]


@pytest.mark.asyncio
async def test_closed_copy_transport_preserves_cancellation_without_second_rollback(monkeypatch):
    connection, transitions = _connection()
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=object()))
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        with pytest.raises(asyncio.CancelledError):
            async with policy.source_owner_transaction(database):
                connection.is_closed = lambda: True
                raise asyncio.CancelledError
    assert transitions == ["savepoint"]


def _factory_case(monkeypatch):
    binding = ContextVar("synthetic_source_session", default=None)
    database = SimpleNamespace(_transaction_binding=lambda: binding.get())
    opened_sessions = []

    @asynccontextmanager
    async def transaction(requested_database, *, isolation):
        assert requested_database is database
        connection, _ = _connection()
        connection.fetchval.return_value = isolation
        session = SimpleNamespace(driver=connection, in_transaction=lambda: True)
        token = binding.set(SimpleNamespace(session=session))

        async def execute(statement):
            current = "source_owner" if statement.startswith("SET LOCAL ROLE") else "source_publisher"
            connection.fetchrow.return_value = {"session": "source_publisher", "current": current}

        connection.execute.side_effect = execute
        opened_sessions.append((session, isolation))
        try:
            async with policy.bind_generic_source_publisher(
                connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation=isolation
            ):
                async with policy.source_owner_transaction(database):
                    yield session
        finally:
            binding.reset(token)

    factory = _identity_factory()
    factory.transaction = transaction
    return database, factory, opened_sessions


@pytest.mark.asyncio
async def test_factory_opens_separate_transactions_for_concurrent_workers(monkeypatch):
    database, factory, opened = _factory_case(monkeypatch)
    ready = asyncio.Event()
    entered_sessions = []

    async def worker():
        async with policy.source_transaction(database) as session:
            entered_sessions.append(session)
            if len(entered_sessions) == 2:
                ready.set()
            await asyncio.wait_for(ready.wait(), timeout=2)
            assert policy.bound_publisher().task is asyncio.current_task()
            assert policy.source_copy_session() is session
            return session

    async with policy.bind_generic_source_transaction_factory(factory):
        assert policy.source_transaction_available() is None
        sessions = await asyncio.gather(asyncio.create_task(worker()), asyncio.create_task(worker()))
    assert sessions[0] is not sessions[1]
    assert sessions[0].driver is not sessions[1].driver
    assert [isolation for _, isolation in opened] == ["read committed", "read committed"]
    assert policy._PUBLISHER.get() is None and policy._FACTORY.get() is None


@pytest.mark.asyncio
async def test_factory_must_bind_a_genuine_publisher_before_it_can_yield(monkeypatch):
    driver = AsyncMock()

    @asynccontextmanager
    async def transaction(database, *, isolation):
        yield

    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=transaction)):
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            async with policy.source_transaction(object()):
                pytest.fail("unbound factory became a writer")
    driver.assert_not_awaited()


@pytest.mark.asyncio
async def test_nested_transaction_cannot_upgrade_isolation_after_earlier_queries(monkeypatch):
    database, factory, opened = _factory_case(monkeypatch)
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.source_transaction(database):
            with pytest.raises(GenericCandidateError, match="isolation_changed"):
                async with policy.source_transaction(database, isolation="repeatable read"):
                    pytest.fail("read committed transaction was retroactively upgraded")
    assert len(opened) == 1


@pytest.mark.asyncio
async def test_seal_transaction_requests_repeatable_read_before_factory_queries(monkeypatch):
    database, factory, opened = _factory_case(monkeypatch)
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.source_transaction(database, isolation="repeatable read"):
            assert policy.bound_publisher().transaction_isolation == "repeatable read"
    assert [isolation for _, isolation in opened] == ["repeatable read"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("body refused"), asyncio.CancelledError()])
async def test_original_publisher_body_restores_owner_without_abort_role_sql(monkeypatch, failure):
    connection, transitions = _connection()
    session = SimpleNamespace(in_transaction=lambda: True)
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))

    async def execute(statement):
        actor = "source_owner" if statement.startswith("SET LOCAL ROLE") else "source_publisher"
        connection.fetchrow.return_value = {"session": "source_publisher", "current": actor}

    connection.execute.side_effect = execute
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        async with policy.source_owner_transaction(database):
            saved_owner = policy._OWNER.get()
            try:
                await _exercise_publisher_body(connection, session, database, failure)
            except (RuntimeError, asyncio.CancelledError) as error:
                assert error is failure
            assert policy._OWNER.get() is saved_owner
    statements = [call.args[0] for call in connection.execute.await_args_list]
    if failure is None:
        assert transitions == ["savepoint", "savepoint", "savepoint", "release", "release", "release"]
        assert statements == [
            'SET LOCAL ROLE "source_owner"',
            "RESET ROLE",
            'SET LOCAL ROLE "source_owner"',
            "RESET ROLE",
            'SET LOCAL ROLE "source_owner"',
            "RESET ROLE",
        ]
    else:
        assert transitions == ["savepoint", "savepoint", "rollback", "release"]
        assert statements == ['SET LOCAL ROLE "source_owner"', "RESET ROLE", "RESET ROLE"]


async def _exercise_publisher_body(connection, session, database, failure):
    async with policy.source_publisher_body(session):
        assert policy.bound_publisher().connection is connection and policy._OWNER.get() is None
        assert (await connection.fetchrow())["current"] == "source_publisher"
        if failure is not None:
            raise failure
        async with policy.source_owner_transaction(database):
            assert policy.source_copy_session() is session


def _synthetic_catalog_roles():
    """Closed catalog answers for unit orchestration, never native role proof."""
    rows = []
    for oid, name in enumerate(("source_owner", "source_publisher", "source_reader"), 1):
        rows.append(
            dict(
                oid=oid,
                rolname=name,
                rolcanlogin=name != "source_owner",
                elevated=False,
                rolsuper=False,
                rolcreatedb=False,
                rolcreaterole=False,
                rolreplication=False,
                rolbypassrls=False,
                owner_set=name != "source_reader",
                publisher_set=name == "source_publisher",
                namespace_usage=True,
                namespace_create=name != "source_reader",
                namespace_owner_reachable=name != "source_reader",
                namespace_owner_protected=True,
            )
        )
    return rows


@asynccontextmanager
async def synthetic_source_publisher(
    monkeypatch, database, *, isolation="repeatable read", schema="source_schema", binding=None
):
    """Exercise configured publisher transactions without a catalog-capture double."""
    configured = policy.GenericSourcePolicy(schema, "source_owner", "source_publisher", ("source_reader",))
    connection, transitions = _connection()
    connection.copy_records_to_table = AsyncMock()
    connection.fetch = AsyncMock(side_effect=AssertionError("unexpected catalog capture"))
    connection.fetchval.side_effect = lambda query, *args: isolation if query == "SHOW transaction_isolation" else "23"
    connection.status = AsyncMock()

    async def execute(statement, *arguments):
        if statement.startswith("SET LOCAL ROLE") or statement == "RESET ROLE":
            assert not arguments
            current = "source_owner" if statement.startswith("SET LOCAL ROLE") else "source_publisher"
            connection.fetchrow.return_value = dict(session="source_publisher", current=current)

    async def verify(_connection, schema, **roles):
        assert _connection is connection and schema == configured.schema and roles == configured.role_arguments()

    connection.execute.side_effect = execute
    token = None
    if binding is None:
        monkeypatch.setattr(
            database, "_transaction_binding", lambda: SimpleNamespace(session=connection), raising=False
        )
    else:
        token = binding.set(SimpleNamespace(session=connection))
    try:
        async with (
            policy.bind_generic_source_publisher(
                connection, configured, verify_role_profile=verify, transaction_isolation=isolation
            ),
            policy.source_owner_transaction(database),
        ):
            yield connection, transitions
    finally:
        if token is not None:
            binding.reset(token)


class SyntheticSourceWorkflow:
    """Unit orchestration with declared source facts and transaction outcome checks."""

    def __init__(self, directory, candidate, identity, *, drained=None, before_seal=None):
        from copy import deepcopy

        from process.provider_directory_registry_network_metadata import fhir_network_binding_metadata

        self.directory, self.candidate = directory, candidate
        self.database = directory.db
        self.identity = deepcopy(identity)
        assert self.identity["source_id"] == "cms-npd"
        self.schema = directory._schema() if hasattr(directory, "_schema") else "source_schema"
        self.coordinates = fhir_network_binding_metadata(
            source_id="cms-npd",
            dataset_schema=self.schema,
            dataset_id=candidate.dataset_id,
            producer_id="cms-npd-bulk-fhir-v1",
            alias_scope="cms-npd",
        )
        _declare_unit_candidate(candidate, deepcopy(identity), deepcopy(self.coordinates))
        self.dataset_id = candidate.dataset_id
        self.drained, self.before_seal = drained, before_seal
        self.events = []
        self.active = 0
        self.snapshot = ContextVar("unit_source_snapshot", default=None)

    def _checked_session(self, directory, candidate):
        assert directory is self.directory and directory.db is self.database and candidate is self.candidate
        assert candidate.dataset_id == self.dataset_id and candidate.source_ids == ("cms-npd",)
        assert candidate.source_release == self.identity and candidate.network_bindings == self.coordinates
        publisher = policy.bound_publisher()
        assert publisher.policy.schema == self.schema and publisher.task is asyncio.current_task()
        assert publisher.transaction_isolation in {"repeatable read", "serializable"}
        session = policy.source_copy_session()
        assert directory.db._transaction_binding().session is session
        assert publisher.connection is session
        return session

    @asynccontextmanager
    async def completed(self, directory, candidate):
        session = self._checked_session(directory, candidate)
        assert self.snapshot.get() is None
        if self.drained is not None:
            self.drained()
        token = self.snapshot.set(session)
        self.events.append("snapshot.begin")
        try:
            yield self
        except BaseException:
            self.events.append("snapshot.rollback")
            raise
        else:
            self.events.append("snapshot.commit")
        finally:
            self.snapshot.reset(token)

    async def seal(self, database, directory, candidate):
        assert database is self.database
        assert self.snapshot.get() is self._checked_session(directory, candidate)
        assert "identity.sealed" not in self.events
        if self.before_seal is not None:
            self.before_seal()
        self.events.append("identity.sealed")

    @asynccontextmanager
    async def _transaction(self, monkeypatch, database, *, isolation):
        assert database is self.database and isolation in {"repeatable read", "serializable"}
        self.events.append("transaction.begin")
        self.active += 1
        try:
            with monkeypatch.context() as settings:
                async with synthetic_source_publisher(
                    settings, database, isolation=isolation, schema=self.schema
                ) as pair:
                    yield pair[0]
        except BaseException:
            self.events.append("transaction.rollback")
            raise
        else:
            self.events.append("transaction.commit")
        finally:
            self.active -= 1

    @asynccontextmanager
    async def bind(self, monkeypatch, *, completed=False):
        from process import provider_directory_generic_source_adapters as adapters
        from process import provider_directory_generic_source_auxiliary as auxiliary

        factory = _identity_factory(schema=self.schema)
        factory.transaction = lambda database, **options: self._transaction(monkeypatch, database, **options)
        with monkeypatch.context() as settings:
            settings.setattr(adapters, "completed_source_scope", self.completed)
            settings.setattr(auxiliary, "seal_identity_candidate", self.seal)
            async with policy.bind_generic_source_transaction_factory(factory):
                if completed:
                    async with (
                        policy.source_transaction(self.database, isolation="repeatable read"),
                        self.completed(self.directory, self.candidate),
                    ):
                        yield self
                else:
                    yield self
        assert self.active == 0 and self.snapshot.get() is None

    def assert_outcome(self, outcome, *, sealed=False):
        expected_events = ["transaction.begin", "snapshot.begin"]
        if sealed:
            expected_events.append("identity.sealed")
        assert self.events == [*expected_events, "snapshot." + outcome, "transaction." + outcome]
        assert self.active == 0 and self.snapshot.get() is None


def _workflow_case():
    from process import provider_directory_cms_npd as cms
    from tests.test_provider_directory_cms_npd import _receipt

    directory = SimpleNamespace(db=SimpleNamespace(), _schema=lambda: "source_schema")
    candidate = SimpleNamespace(dataset_id="unit-dataset")
    return SyntheticSourceWorkflow(directory, candidate, cms.release_identity(_receipt()))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
)
async def test_unit_workflow_refuses_changed_source_coordinates(monkeypatch, field):
    workflow = _workflow_case()
    with pytest.raises(AssertionError):
        async with workflow.bind(monkeypatch):
            async with policy.source_transaction(workflow.database, isolation="repeatable read"):
                workflow.candidate.network_bindings[field] = "changed"
                async with workflow.completed(workflow.directory, workflow.candidate):
                    pytest.fail("changed coordinates admitted")
    assert workflow.events == ["transaction.begin", "transaction.rollback"]
    assert workflow.active == 0 and workflow.snapshot.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("candidate", "source", "session", "database"))
async def test_unit_workflow_refuses_changed_source_and_transaction_inputs(monkeypatch, change):
    workflow = _workflow_case()
    with pytest.raises(AssertionError):
        async with workflow.bind(monkeypatch):
            async with policy.source_transaction(workflow.database, isolation="repeatable read"):
                candidate = _changed_workflow_identity(monkeypatch, workflow, change)
                async with workflow.completed(workflow.directory, candidate):
                    pytest.fail("changed identity admitted")
    assert workflow.events == ["transaction.begin", "transaction.rollback"]
    assert workflow.active == 0 and workflow.snapshot.get() is None


def _changed_workflow_identity(monkeypatch, workflow, change):
    candidate = workflow.candidate
    if change == "candidate":
        return SimpleNamespace(**vars(candidate))
    if change == "source":
        candidate.source_release["vector_sha256"] = "f" * 64
    elif change == "session":
        monkeypatch.setattr(workflow.database, "_transaction_binding", lambda: SimpleNamespace(session=object()))
    else:
        workflow.directory.db = object()
    return candidate


def _declare_unit_candidate(candidate, identity, coordinates):
    """Fill missing unit declarations; never normalize contradictory source facts."""
    for field, declared in (
        ("source_ids", ("cms-npd",)),
        ("source_release", identity),
        ("network_bindings", coordinates),
    ):
        if hasattr(candidate, field):
            assert getattr(candidate, field) == declared
        else:
            setattr(candidate, field, declared)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ("source_ids", "source_release", "network_bindings"))
async def test_unit_workflow_rejects_conflicting_existing_declaration(field):
    expected = _workflow_case()
    candidate = SimpleNamespace(dataset_id=expected.dataset_id, **{field: "foreign"})
    with pytest.raises(AssertionError):
        SyntheticSourceWorkflow(expected.directory, candidate, expected.identity)


@asynccontextmanager
async def synthetic_source_factory(monkeypatch, database, *, schema="source_schema"):
    """Open independent unit transactions through the configured publisher API."""
    binding = ContextVar("unit_source_factory_session", default=None)
    monkeypatch.setattr(database, "_transaction_binding", binding.get, raising=False)

    @asynccontextmanager
    async def transaction(requested_database, *, isolation):
        assert requested_database is database
        with monkeypatch.context() as settings:
            async with synthetic_source_publisher(
                settings, database, isolation=isolation, schema=schema, binding=binding
            ) as (connection, _transitions):
                yield connection

    factory = _identity_factory(schema=schema)
    factory.transaction = transaction
    async with policy.bind_generic_source_transaction_factory(factory):
        yield factory
    assert binding.get() is None


@pytest.mark.asyncio
async def test_unit_factory_keeps_concurrent_transactions_separate(monkeypatch):
    database = SimpleNamespace()
    entered_sessions = []
    ready = asyncio.Event()

    async def operation():
        async with policy.source_transaction(database) as session:
            entered_sessions.append(session)
            if len(entered_sessions) == 2:
                ready.set()
            await asyncio.wait_for(ready.wait(), timeout=2)
            assert policy.bound_publisher().task is asyncio.current_task()
            assert policy.source_copy_session() is session
            return session

    async with synthetic_source_factory(monkeypatch, database):
        first, second = await asyncio.gather(operation(), operation())
    assert first is not second
    assert database._transaction_binding() is None
    assert policy._PUBLISHER.get() is policy._FACTORY.get() is None


@asynccontextmanager
async def synthetic_checkpoint_factory(monkeypatch, database, *, schema="mrf"):
    """Supply checkpoint controls while retaining active transaction checks."""
    from process import provider_directory_source_checkpoint_control as checkpoints

    async def capture(connection, configured):
        publisher = policy.bound_publisher()
        assert publisher.task is asyncio.current_task()
        assert publisher.connection is connection and publisher.policy is configured
        assert configured.schema == schema
        assert policy.source_copy_session() is database._transaction_binding().session is connection
        return {}

    with monkeypatch.context() as settings:
        settings.setattr(checkpoints, "capture_checkpoint_controls", capture)
        async with synthetic_source_factory(settings, database, schema=schema) as factory:
            yield factory


class SyntheticSourceDatabase(Database):
    """Delegate explicit unit SQL/state while retaining the actual Database guard type."""

    def __init__(self, domain):
        object.__setattr__(self, "domain", domain)

    def __getattr__(self, name):
        return getattr(self.domain, name)

    def __setattr__(self, name, value):
        if name in {"domain", "_transaction_binding", "all", "first", "scalar", "status"}:
            object.__setattr__(self, name, value)
        else:
            setattr(self.domain, name, value)

    async def all(self, *args, **kwargs):
        return await self.domain.all(*args, **kwargs)

    async def first(self, *args, **kwargs):
        return await self.domain.first(*args, **kwargs)

    async def scalar(self, *args, **kwargs):
        return await self.domain.scalar(*args, **kwargs)

    async def status(self, *args, **kwargs):
        return await self.domain.status(*args, **kwargs)


@asynccontextmanager
async def synthetic_source_database(monkeypatch, domain, *, schema="mrf"):
    """Keep original fake SQL/state/rollback oracles behind a task-local Database interface."""
    from contextlib import nullcontext

    database = SyntheticSourceDatabase(domain)
    async with synthetic_checkpoint_factory(monkeypatch, database, schema=schema) as factory:
        original = factory.transaction

        @asynccontextmanager
        async def transaction(requested_database, *, isolation):
            assert requested_database is database
            boundary = domain.transaction() if callable(getattr(domain, "transaction", None)) else nullcontext()
            async with boundary:
                async with original(database, isolation=isolation) as session:
                    yield session

        monkeypatch.setattr(factory, "transaction", transaction)
        yield database


@pytest.mark.asyncio
async def test_unit_database_facade_keeps_domain_sql_and_current_source_session(monkeypatch):
    from process.provider_directory_source_checkpoint_control import require_checkpoint_executor

    domain = SimpleNamespace(first=AsyncMock(return_value={"dataset_id": "exact"}), calls=[])
    async with synthetic_source_database(monkeypatch, domain) as database:
        async with policy.source_transaction(database) as session:
            assert isinstance(database, Database)
            publisher = await require_checkpoint_executor(database)
            assert publisher.task is asyncio.current_task()
            assert publisher.connection is session is policy.source_copy_session()
            assert database._transaction_binding().session is session
            assert await database.first("SELECT exact", dataset_id="exact") == {"dataset_id": "exact"}
            domain.first.assert_awaited_once_with("SELECT exact", dataset_id="exact")
            database.calls.append("kept")
    assert domain.calls == ["kept"]


@pytest.mark.asyncio
@pytest.mark.parametrize("substitution", ["session", "database"])
async def test_unit_database_facade_never_waives_source_executor_guards(monkeypatch, substitution):
    from process.provider_directory_source_checkpoint_control import require_checkpoint_executor

    domain = SimpleNamespace(first=AsyncMock())
    async with synthetic_source_database(monkeypatch, domain) as database, policy.source_transaction(database):
        with monkeypatch.context() as changed:
            executor = database
            if substitution == "session":
                changed.setattr(database, "_transaction_binding", lambda: SimpleNamespace(session=object()))
            elif substitution == "database":
                executor = SyntheticSourceDatabase(SimpleNamespace())
            with pytest.raises(
                GenericCandidateError,
                match="publisher_connection_required|checkpoint_transaction_changed|transaction_required",
            ):
                await require_checkpoint_executor(executor)
        domain.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_checkpoint_guard_delegates_without_publisher_or_owner():
    database, connection, transaction = Database(), object(), object()

    @asynccontextmanager
    async def guard(supplied):
        assert supplied is database and policy._PUBLISHER.get() is policy._OWNER.get() is None
        yield connection, transaction

    factory = _identity_factory()
    factory.checkpoint_guard = guard
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.source_checkpoint_guard(database) as issued:
            assert issued == (connection, transaction)
            assert policy._PUBLISHER.get() is policy._OWNER.get() is None


@pytest.mark.asyncio
async def test_checkpoint_guard_requires_issued_factory():
    with pytest.raises(GenericCandidateError, match="checkpoint_guard_required"):
        async with policy.source_checkpoint_guard(Database()):
            pytest.fail("unissued guard admitted")
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=lambda: None)):
        with pytest.raises(GenericCandidateError, match="checkpoint_guard_factory_required"):
            async with policy.source_checkpoint_guard(Database()):
                pytest.fail("intake factory admitted")


@pytest.mark.asyncio
async def test_checkpoint_guard_refuses_proxy_before_factory():
    factory = SimpleNamespace(transaction=lambda: None, checkpoint_guard=Mock())
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(GenericCandidateError, match="checkpoint_guard_database_required"):
            async with policy.source_checkpoint_guard(SimpleNamespace(_transaction_binding=lambda: None)):
                pytest.fail("proxy admitted")
    factory.checkpoint_guard.assert_not_called()


@pytest.mark.asyncio
async def test_equal_logical_publisher_and_factory_wrappers_keep_the_active_copy_scope():
    factory = _named_factory()
    connection, _ = _connection()
    session = SimpleNamespace(in_transaction=lambda: True)
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    async with _callable_binding(factory, connection), policy.source_owner_transaction(database):
        original = policy.bound_publisher()
        wrapper = _named_factory()
        factory_token = policy._FACTORY.set(wrapper)
        publisher_token = policy._PUBLISHER.set(
            replace(
                original,
                policy=replace(original.policy),
                storage_binding=replace(original.storage_binding),
                factory=wrapper,
            )
        )
        try:
            assert policy.source_producer_identity() == factory.producer_identity
            assert policy.source_copy_session() is session
        finally:
            policy._PUBLISHER.reset(publisher_token)
            policy._FACTORY.reset(factory_token)
    assert policy._OWNER.get() is policy._PUBLISHER.get() is policy._FACTORY.get() is None


@pytest.mark.asyncio
async def test_child_with_its_own_active_source_scope_uses_equal_logical_configuration():
    from process.provider_directory_profile_source_inputs import _child_context

    factory = _named_factory()
    parent_connection, _ = _connection()
    parent_session = SimpleNamespace(in_transaction=lambda: True)
    parent_database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=parent_session))

    async def child():
        child_connection, _ = _connection()
        session = SimpleNamespace(in_transaction=lambda: True)
        database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
        token = policy._FACTORY.set(_named_factory())
        try:
            async with (
                policy.bind_generic_source_publisher(
                    child_connection, replace(factory.policy), transaction_isolation="repeatable read"
                ),
                policy.source_owner_transaction(database),
            ):
                assert policy.source_producer_identity() == factory.producer_identity
                assert policy.source_copy_session() is session
                assert policy.bound_publisher().connection is child_connection
        finally:
            policy._FACTORY.reset(token)

    async with _callable_binding(factory, parent_connection), policy.source_owner_transaction(parent_database):
        original = policy.bound_publisher()
        await asyncio.create_task(child(), context=_child_context())
        assert policy.bound_publisher() is original and policy.source_copy_session() is parent_session


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation,changed",
    [
        (operation, changed)
        for operation in ("source_copy_session", "source_producer_identity")
        for changed in ("policy", "catalog", "storage", "producer", "driver", "session")
        if (operation, changed) != ("source_producer_identity", "session")
    ],
)
async def test_source_scope_refuses_logical_changes_and_inactive_transactions(operation, changed):
    factory = _named_factory()
    connection, _ = _connection()
    session = SimpleNamespace(in_transaction=lambda: True)
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    async with _callable_binding(factory, connection), policy.source_owner_transaction(database):
        wrapper = _named_factory()
        wrapper._require_current_policy = Mock()
        if changed == "policy":
            wrapper.policy = replace(wrapper.policy, owner_role="other_owner")
        if changed in {"catalog", "storage"}:
            wrapper.storage_binding = replace(wrapper.storage_binding, **{changed + "_schema": "other_schema"})
        if changed == "producer":
            wrapper.producer_identity = ("c" * 40, "sha256:" + "d" * 64)
        if changed == "driver":
            connection.is_in_transaction = lambda: False
        if changed == "session":
            session.in_transaction = lambda: False
        token = policy._FACTORY.set(wrapper)
        try:
            with pytest.raises(GenericCandidateError, match="policy_changed|transaction_required"):
                getattr(policy, operation)()
        finally:
            policy._FACTORY.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_checkpoint_guard_compares_logical_factory_configuration(changed):
    database = Database()
    factory, wrapper = _identity_factory(), _identity_factory()
    if changed:
        wrapper = _identity_factory(schema="other_schema")

    @asynccontextmanager
    async def guard(actual):
        assert actual is database
        token = policy._FACTORY.set(wrapper)
        try:
            yield "connection", "transaction"
        finally:
            policy._FACTORY.reset(token)

    factory.checkpoint_guard = guard
    async with policy.bind_generic_source_transaction_factory(factory):
        if changed:
            with pytest.raises(GenericCandidateError, match="policy_changed"):
                async with policy.source_checkpoint_guard(database):
                    pytest.fail("changed logical scope entered")
        else:
            async with policy.source_checkpoint_guard(database) as issued:
                assert issued == ("connection", "transaction")


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_original_publication_compares_logical_scope_before_the_body(monkeypatch, changed):
    factory = _identity_factory()
    wrapper = _identity_factory(schema="other_schema" if changed else "source_schema")
    session = SimpleNamespace(in_transaction=lambda: True)
    entered_events = []

    @asynccontextmanager
    async def bind(database, actual_session, *, isolation):
        assert actual_session is session and isolation == "read committed"
        token = policy._FACTORY.set(wrapper)
        try:
            yield session
        finally:
            policy._FACTORY.reset(token)

    @asynccontextmanager
    async def body(database):
        entered_events.append(True)
        yield session

    factory.bind_existing_transaction = bind
    factory.publication_session_factory = AsyncMock()
    monkeypatch.setattr(policy, "source_publication_body", body)
    async with policy.bind_generic_source_transaction_factory(factory):
        if changed:
            with pytest.raises(GenericCandidateError, match="policy_changed"):
                async with policy.original_source_publication(object(), session):
                    pytest.fail("changed logical publication scope entered")
        else:
            async with policy.original_source_publication(object(), session) as actual:
                assert actual is session
    assert entered_events == ([] if changed else [True])
