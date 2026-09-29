# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepared archive identity follows each native address backend and publication phase."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_candidate_preparation as preparation
from process import entity_address_prepared_doctors as dependencies
from process.provider_directory_cms_archive import PreparedArchiveDelta
from tests.test_entity_address_candidate_preparation_postgres import _inputs, native
from tests.test_entity_address_prepared_doctors_postgres import _example_bindings
from tests.test_provider_directory_cms_preparation import _admission


def _archive():
    admission = _admission()
    admission._started = True
    return PreparedArchiveDelta(
        schema="mrf",
        delta_table="prepared_archive_delta",
        delta_oid=101,
        effective_relation="prepared_archive_view",
        effective_oid=102,
        target_oid=100,
        from_revision=7,
        native_input_hash=admission.plan.native_address_input_hash,
        columns=("address_key",),
        delta_rows=1,
        delta_sha256="a" * 64,
        effective_definition="SELECT address_key FROM mrf.prepared_archive_delta",
        sealed_filenode=103,
        admission=admission,
    )


def _captured(archive):
    return dependencies.PreparedAddressDependencies("mrf", None, (), _example_bindings(), archive)


def _database(monkeypatch):
    session = object()
    database = SimpleNamespace(_transaction_binding=lambda: None)

    @asynccontextmanager
    async def transaction():
        assert database._transaction_binding() is None
        database._transaction_binding = lambda: SimpleNamespace(session=session)
        try:
            yield session
        finally:
            database._transaction_binding = lambda: None

    @asynccontextmanager
    async def bounded_capture(actual_session):
        assert actual_session is session
        yield

    database.transaction = transaction
    monkeypatch.setattr(dependencies, "_archive", lambda: SimpleNamespace(_bounded_capture=bounded_capture))
    return database, session


async def test_bare_archive_override_is_rejected_before_dependency_capture():
    with pytest.raises(RuntimeError, match="archive override requires a prepared delta"):
        await dependencies.capture_dependencies(object(), "mrf", (("address_archive_v2", "arbitrary_view"),))


def _changed_archive(change):
    """Change one fixed prepared identity field for fail-closed dependency checks."""
    archive = _archive()
    if change == "fake":
        return SimpleNamespace(**vars(archive))
    if change == "unstarted":
        archive.admission._started = False
    else:
        attributes_by_change = {
            "schema": ("schema", "other"),
            "committed": ("committed", True),
            "hash": ("native_input_hash", "f" * 64),
            "admission": ("admission", object()),
        }
        setattr(archive, *attributes_by_change[change])
    return archive


@pytest.mark.parametrize("change", ["fake", "schema", "committed", "unstarted", "hash", "admission"])
async def test_archive_requires_real_prepared_identity_and_admission(monkeypatch, change):
    archive = _changed_archive(change)
    check = AsyncMock()
    monkeypatch.setattr(dependencies, "assert_prepared_dependencies", check)
    with pytest.raises(RuntimeError, match="archive input is not prepared"):
        await dependencies.capture_dependencies(
            object(),
            "mrf",
            (("address_archive_v2", "prepared_archive_view"),),
            dependency_bindings=_example_bindings(),
            archive=archive,
        )
    check.assert_not_awaited()


@pytest.mark.parametrize("overrides", [(), (("address_archive_v2", "other_view"),)])
async def test_archive_requires_its_exact_effective_view(overrides):
    with pytest.raises(RuntimeError, match="archive differs from its override"):
        await dependencies.capture_dependencies(
            object(), "mrf", overrides, dependency_bindings=_example_bindings(), archive=_archive()
        )


async def test_capture_keeps_archive_separate_from_geo_heap_bindings(monkeypatch):
    archive = _archive()
    check = AsyncMock()
    monkeypatch.setattr(dependencies, "assert_prepared_dependencies", check)
    backend = object()
    captured = await dependencies.capture_dependencies(
        backend,
        "mrf",
        tuple(archive.relation_overrides.items()),
        dependency_bindings=_example_bindings(),
        archive=archive,
    )
    assert captured.archive is archive
    assert captured.doctors is None and captured.stage_oids == ()
    assert captured.dependency_bindings == _example_bindings()
    assert "mrf.address_archive_v2" not in captured.dependency_bindings
    check.assert_awaited_once_with(backend, captured)


async def test_archive_only_source_callback_guards_the_passed_backend(monkeypatch):
    archive = _archive()
    read_guard = AsyncMock()
    monkeypatch.setattr(archive, "lock_read_backend", read_guard)
    backend = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock())
    token = preparation._NATIVE_DEPENDENCIES.set(_captured(archive))
    try:
        assert preparation.has_prepared_doctors() is True
        await preparation.lock_prepared_doctors(backend)
    finally:
        preparation._NATIVE_DEPENDENCIES.reset(token)
    read_guard.assert_awaited_once_with(backend)
    backend.status.assert_not_awaited()
    backend.scalar.assert_not_awaited()
    assert preparation.has_prepared_doctors() is False


async def test_prepared_dependency_validation_locks_archive_on_its_actual_transaction(monkeypatch):
    archive = _archive()
    database, session = _database(monkeypatch)

    async def check_backend(actual_backend):
        assert actual_backend is database
        assert actual_backend._transaction_binding().session is session

    read_guard = AsyncMock(side_effect=check_backend)
    monkeypatch.setattr(archive, "lock_read_backend", read_guard)
    full_scan = AsyncMock(side_effect=AssertionError("archive contents must not be rescanned per backend"))
    monkeypatch.setattr(archive, "assert_ready", full_scan)
    check_bindings = AsyncMock()
    monkeypatch.setattr(dependencies, "_assert_bindings", check_bindings)
    await dependencies.assert_prepared_dependencies(database, _captured(archive))
    read_guard.assert_awaited_once_with(database)
    check_bindings.assert_awaited_once_with(session, "mrf", _example_bindings())
    full_scan.assert_not_awaited()
    assert database._transaction_binding() is None


@pytest.mark.parametrize("applied", [False, True])
async def test_applied_archive_guard_precedes_native_address_activation(monkeypatch, applied):
    archive = _archive()
    database, session = _database(monkeypatch)
    applied_guard = AsyncMock(side_effect=None if applied else RuntimeError("archive merge missing"))
    monkeypatch.setattr(archive, "assert_applied_backend", applied_guard)
    check_bindings = AsyncMock()
    monkeypatch.setattr(dependencies, "_assert_bindings", check_bindings)
    async with database.transaction():
        if applied:
            await dependencies.assert_applied_dependencies(database, _captured(archive))
            check_bindings.assert_awaited_once_with(session, "mrf", _example_bindings())
        else:
            with pytest.raises(RuntimeError, match="archive merge missing"):
                await dependencies.assert_applied_dependencies(database, _captured(archive))
            check_bindings.assert_not_awaited()
    applied_guard.assert_awaited_once_with(database)


async def test_applied_archive_requires_the_publication_owner_transaction(monkeypatch):
    archive = _archive()
    database, _session = _database(monkeypatch)
    applied_guard = AsyncMock()
    monkeypatch.setattr(archive, "assert_applied_backend", applied_guard)
    with pytest.raises(RuntimeError, match="dependency cutover requires a transaction"):
        await dependencies.assert_applied_dependencies(database, _captured(archive))
    applied_guard.assert_not_awaited()


@pytest.mark.parametrize("wrong_admission", [False, True])
async def test_native_preparation_requires_the_archive_exact_admission(monkeypatch, wrong_admission):
    archive = _archive()
    monkeypatch.setattr(native.db, "_transaction_binding", lambda: None)
    monkeypatch.setattr(preparation.admitted, "assert_full_recipe", lambda *_args: None)
    inputs = replace(_inputs(), semantic_as_of=archive.admission.plan.desired_profile_as_of)
    admission = _admission() if wrong_admission else None
    capture = AsyncMock()
    monkeypatch.setattr(dependencies, "capture_dependencies", capture)
    with pytest.raises(RuntimeError, match="archive differs from its admission"):
        await preparation.prepare_provider_directory_entity_address(
            {},
            {},
            preparation_input=inputs,
            archive=archive,
            admission=admission,
            native_input_hash=admission.plan.native_address_input_hash if admission is not None else None,
        )
    capture.assert_not_awaited()


async def test_native_preparation_threads_archive_and_restores_scope_after_interruption(monkeypatch):
    archive = _archive()
    inputs = replace(
        _inputs(),
        relation_overrides=tuple(archive.relation_overrides.items()),
        semantic_as_of=archive.admission.plan.desired_profile_as_of,
    )
    captured = _captured(archive)
    capture = AsyncMock(return_value=captured)
    monkeypatch.setattr(dependencies, "capture_dependencies", capture)
    monkeypatch.setattr(native.db, "_transaction_binding", lambda: None)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(preparation.admitted, "assert_full_recipe", lambda *_args: None)

    async def fail_build(_ctx, _task):
        assert preparation.current() is inputs
        assert preparation._NATIVE_DEPENDENCIES.get() is captured
        assert preparation.admitted._ADMISSION.get().admission is archive.admission
        raise RuntimeError("preparation interrupted")

    monkeypatch.setattr(native, "process_entity_address_unified_data", fail_build)
    with pytest.raises(RuntimeError, match="preparation interrupted"):
        await preparation.prepare_provider_directory_entity_address(
            {},
            {},
            preparation_input=inputs,
            archive=archive,
            admission=archive.admission,
            native_input_hash=archive.native_input_hash,
            dependency_bindings=_example_bindings(),
        )
    capture.assert_awaited_once_with(
        native.db,
        "mrf",
        inputs.relation_overrides,
        doctors=None,
        dependency_bindings=_example_bindings(),
        archive=archive,
    )
    assert preparation.current() is None
    assert preparation._NATIVE_DEPENDENCIES.get() is None
    assert preparation.admitted._ADMISSION.get() is None
