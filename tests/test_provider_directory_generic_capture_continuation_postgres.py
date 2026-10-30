# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native post-commit capture, isolated rollback and exact published replay."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import AsyncSessionTransaction

from process import provider_directory_generic_capture_continuation as continuation
from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_generic_source_policy as source
from process import provider_directory_source_local_publication as publication
from process.registry_generic_accepted_capture import generic_fhir_capture_id
from process.registry_generic_capture_continuation import RegistryGenericCaptureTarget
from tests.generic_fhir_capture_postgres_support import legacy_capture_db as legacy_capture_db
from tests.provider_directory_source_postgres_support import NativeSourceFactory

pytestmark = pytest.mark.asyncio


def _source_case(fixture):
    reader, writer = fixture.session_factory("reader"), fixture.session_factory("publisher")
    source_sessions = fixture.session_factory("publisher")
    database = SimpleNamespace(engine=source_sessions.kw["bind"], _transaction_binding=lambda: None)
    policy = source.GenericSourcePolicy(
        fixture.schema,
        fixture.role_by_kind["source"],
        fixture.role_by_kind["publisher"],
        (fixture.role_by_kind["reader"],),
    )
    factory = NativeSourceFactory(database, policy)
    factory.enroll_ordinary_native_inputs()
    factory.generic_fhir_capture_continuation = continuation.GenericSourceCaptureContinuation(
        reader,
        writer,
        RegistryGenericCaptureTarget(
            fixture.role_by_kind["owner"], (fixture.role_by_kind["reader"],), fixture.origin.preparation_schema
        ),
    )
    directory = SimpleNamespace(
        db=database,
        _schema=lambda: fixture.schema,
        UHC_RETAINED_SOURCE_ID="special-source",
        is_uhc_flex_profile_source=lambda _: False,
        _uses_alohr_graphql_connector=lambda _: False,
        _uses_uhc_official_file_connector=lambda _: False,
    )
    candidate = SimpleNamespace(
        source_ids=(fixture.origin.source_id,),
        endpoint_id=fixture.origin.endpoint_id,
        dataset_id=fixture.origin.dataset_id,
        acquisition_root_run_id=fixture.origin.root_run_id,
        already_published=False,
    )
    return SimpleNamespace(directory=directory, factory=factory, candidate=candidate)


async def _publish(case):
    async with source.bind_generic_source_transaction_factory(case.factory):
        with artifacts.source_publication_request(
            case.directory,
            [{"source_id": case.candidate.source_ids[0]}],
            True,
        ):
            await artifacts.publish_source_candidate(case.directory, case.candidate)


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_native_commit_capture_rollback_and_published_replay(legacy_capture_db, monkeypatch, failure):
    """Committed capture survives replay and failed attempts leave original state intact."""
    fixture, observed = legacy_capture_db, []
    case = _source_case(fixture)
    await fixture.admin.execute(
        f"UPDATE \"{fixture.schema}\".provider_directory_endpoint_dataset SET is_current=false,status='validated'",
    )

    async def publish(directory, candidate, source_id):
        assert directory is case.directory and candidate is case.candidate and source_id == fixture.origin.source_id
        async with fixture.admin.transaction():
            await fixture.admin.execute(
                f"UPDATE \"{fixture.schema}\".provider_directory_endpoint_dataset SET is_current=true,status='published'",
            )
        observed.append("source-committed")

    publish = AsyncMock(side_effect=publish)
    monkeypatch.setattr(publication, "publish_validated_source_local_dataset", publish)
    original = continuation.capture_committed_generic_fhir_source

    async def capture(reader, writer, **keywords):
        assert observed[0] == "source-committed"
        assert source._PUBLISHER.get() is None and source._OWNER.get() is None
        assert case.directory.db._transaction_binding() is None
        head = keywords["expected_head"]
        assert len(head.__dataclass_fields__) == 9
        assert (
            head.dataset_id == case.candidate.dataset_id and head.root_run_id == case.candidate.acquisition_root_run_id
        )
        namespace = "registry_fhir_capture_" + generic_fhir_capture_id(head).hex
        if namespace not in fixture.captures:
            fixture.captures.append(namespace)
        retained = await original(reader, writer, **keywords)
        observed.append(retained)
        if failure is not None and len(observed) == 2:
            raise failure("after-native-capture")
        return retained

    monkeypatch.setattr(continuation, "capture_committed_generic_fhir_source", capture)
    if failure is None:
        await _publish(case)
    else:
        with pytest.raises(failure, match="after-native-capture"):
            await _publish(case)
        assert await fixture.admin.fetchval("SELECT to_regnamespace($1)", fixture.captures[-1]) is None
    assert (
        await fixture.admin.fetchval(
            f"SELECT status='published' AND is_current FROM \"{fixture.schema}\".provider_directory_endpoint_dataset",
        )
        is True
    )
    case.candidate.already_published = True
    await _publish(case)
    assert publish.await_count == 1 and observed[-1].capture_id == observed[-2].capture_id
    if failure is None:
        assert observed[-1] == observed[-2]
    assert await fixture.admin.fetchval("SELECT to_regnamespace($1)", fixture.captures[-1]) is not None
    _assert_capture_pools_released(case)


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
@pytest.mark.parametrize("stale_field", ["dataset_id", "acquisition_root_run_id"])
async def test_native_published_replay_refuses_an_advanced_source_head(legacy_capture_db, monkeypatch, stale_field):
    fixture = legacy_capture_db
    prior_captures = tuple(fixture.captures)
    case = _source_case(fixture)
    case.candidate.already_published = True
    setattr(case.candidate, stale_field, "previous-publication")
    capture, publish = AsyncMock(), AsyncMock()
    monkeypatch.setattr(continuation, "capture_committed_generic_fhir_source", capture)
    monkeypatch.setattr(publication, "publish_validated_source_local_dataset", publish)
    with pytest.raises(RuntimeError, match="capture_head_changed"):
        await _publish(case)
    capture.assert_not_awaited()
    publish.assert_not_awaited()
    assert tuple(fixture.captures) == prior_captures


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_native_outer_savepoint_return_is_not_a_committed_source_callback(legacy_capture_db, monkeypatch):
    fixture = legacy_capture_db
    prior_captures = tuple(fixture.captures)
    case = _source_case(fixture)
    capture = AsyncMock()
    monkeypatch.setattr(continuation.GenericSourceCaptureContinuation, "capture", capture)

    async def publish(*args):
        async with fixture.admin.transaction():
            await fixture.admin.execute(
                f'UPDATE "{fixture.schema}".provider_directory_endpoint_dataset SET is_current=false',
            )

    monkeypatch.setattr(publication, "publish_validated_source_local_dataset", publish)
    outer = fixture.admin.transaction()
    await outer.start()
    try:
        case.directory.db._transaction_binding = lambda: SimpleNamespace(session=fixture.admin, nested=True)
        with pytest.raises(RuntimeError, match="capture_commit_required"):
            await _publish(case)
        capture.assert_not_awaited()
        assert (
            await fixture.admin.fetchval(
                f'SELECT is_current FROM "{fixture.schema}".provider_directory_endpoint_dataset',
            )
            is False
        )
    finally:
        await outer.rollback()
    assert (
        await fixture.admin.fetchval(
            f'SELECT is_current FROM "{fixture.schema}".provider_directory_endpoint_dataset',
        )
        is True
    )
    assert tuple(fixture.captures) == prior_captures


def _inject_committed_capture_fault(monkeypatch, writer_engine, failure):
    native_exit, native_copy = AsyncSessionTransaction.__aexit__, asyncpg.Connection.copy_to_table
    pending_failures, copy_calls = [failure], []

    async def exit_after_commit(transaction, error_type, error, traceback):
        returned = await native_exit(transaction, error_type, error, traceback)
        if transaction.session.bind is writer_engine and error_type is None and pending_failures:
            raise pending_failures.pop()("capture-commit-acknowledgement-lost")
        return returned

    async def counted_copy(connection, *args, **kwargs):
        copy_calls.append((args, kwargs.get("schema_name")))
        return await native_copy(connection, *args, **kwargs)

    monkeypatch.setattr(AsyncSessionTransaction, "__aexit__", exit_after_commit)
    monkeypatch.setattr(asyncpg.Connection, "copy_to_table", counted_copy)
    return pending_failures, copy_calls


async def _native_source_header(fixture):
    return tuple(
        await fixture.admin.fetchrow(
            f"SELECT dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,published_at,"
            f"publication_metadata_sha256,content_proof_admission_sha256 "
            f'FROM "{fixture.schema}".provider_directory_endpoint_dataset',
        )
    )


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_native_capture_commit_acknowledgement_loss_replays_without_copy(legacy_capture_db, monkeypatch, failure):
    fixture = legacy_capture_db
    case = _source_case(fixture)
    case.candidate.already_published = True
    capability = case.factory.generic_fhir_capture_continuation
    namespace = "registry_fhir_capture_" + generic_fhir_capture_id(fixture.origin).hex
    fixture.captures.append(namespace)
    source_header = await _native_source_header(fixture)
    pending_failures, copy_calls = _inject_committed_capture_fault(
        monkeypatch,
        capability.writer_factory.kw["bind"],
        failure,
    )
    with pytest.raises(failure, match="capture-commit-acknowledgement-lost"):
        await _publish(case)
    assert not pending_failures and copy_calls
    committed_oid = await fixture.admin.fetchval("SELECT to_regnamespace($1)::oid::bigint", namespace)
    assert type(committed_oid) is int and committed_oid > 0
    assert await _native_source_header(fixture) == source_header
    for sessions in (capability.reader_factory, capability.writer_factory):
        assert sessions.kw["bind"].pool.checkedout() == 0
    copy_calls.clear()
    await _publish(case)
    assert await fixture.admin.fetchval("SELECT to_regnamespace($1)::oid::bigint", namespace) == committed_oid
    assert await _native_source_header(fixture) == source_header
    assert not copy_calls
    for sessions in (capability.reader_factory, capability.writer_factory):
        assert sessions.kw["bind"].pool.checkedout() == 0


def _assert_capture_pools_released(case):
    """Both original continuation pools must release every checked-out connection."""
    for sessions in (
        case.factory.generic_fhir_capture_continuation.reader_factory,
        case.factory.generic_fhir_capture_continuation.writer_factory,
    ):
        assert sessions.kw["bind"].pool.checkedout() == 0
