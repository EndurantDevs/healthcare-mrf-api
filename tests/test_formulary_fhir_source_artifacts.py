# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import datetime as dt
from pathlib import Path
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import uhc_drug_file_catalog as drug_catalog
from process.formulary_fhir import source_artifact_binding
from process.formulary_fhir import source_artifacts
from process.formulary_fhir.uhc_source_artifacts import (
    identities_from_uhc_drug_catalog,
)
from process.provider_directory_retained_artifact_base import RetainedArtifactError
from tests.uhc_provider_file_catalog_test_data import live_catalog_payloads


SOURCE_ID = "uhc-official-drugs"
VERIFIED_AT = dt.datetime(2026, 8, 10, 8, tzinfo=dt.UTC)


@asynccontextmanager
async def _transaction():
    yield


def _catalog():
    return drug_catalog.observed_drug_catalog_from_payloads(
        live_catalog_payloads(),
        source_raw_set_sha256="a" * 64,
    )


def _database():
    return SimpleNamespace(
        all=AsyncMock(),
        first=AsyncMock(),
        status=AsyncMock(return_value=1),
        transaction=_transaction,
    )


def _row(identity, *, status="pending", artifact_index=0):
    row_by_field = {
        **source_artifacts.identity_fields(identity),
        "artifact_sha256": None,
        "artifact_byte_count": None,
        "status": status,
        "verified_at": None,
    }
    if status == "verified":
        row_by_field.update(
            {
                "artifact_sha256": f"{artifact_index + 1:064x}",
                "artifact_byte_count": identity.expected_byte_count or 1,
                "verified_at": VERIFIED_AT,
            }
        )
    return row_by_field


def _set_row(identities):
    identity = identities[0]
    return {
        "source_id": identity.source_id,
        "source_file_set_sha256": identity.source_file_set_sha256,
        "raw_listing_projection_sha256": (
            identity.raw_listing_projection_sha256
        ),
        "expected_file_count": len(identities),
    }


def _observation_row(identities, source_observation_sha256):
    identity = identities[0]
    return {
        "source_id": identity.source_id,
        "source_observation_sha256": source_observation_sha256,
        "source_file_set_sha256": identity.source_file_set_sha256,
        "raw_listing_projection_sha256": (
            identity.raw_listing_projection_sha256
        ),
    }


@pytest.mark.asyncio
async def test_registers_exact_48_file_set_and_replays_without_identity_change(
    monkeypatch,
):
    catalog = _catalog()
    identities = identities_from_uhc_drug_catalog(
        SOURCE_ID,
        catalog,
    )
    database = _database()
    database.all.return_value = [_row(identity) for identity in identities]
    database.first.side_effect = [
        _set_row(identities),
        _observation_row(identities, catalog.source_raw_set_sha256),
    ]
    lock_source = AsyncMock()
    monkeypatch.setattr(source_artifacts, "lock_source", lock_source)

    registered = await source_artifacts.register_source_file_set(
        identities,
        source_observation_sha256=catalog.source_raw_set_sha256,
        database=database,
    )

    assert registered == identities
    assert len(registered) == 48
    assert database.status.await_count == 50
    lock_source.assert_awaited_once_with(database, SOURCE_ID)


@pytest.mark.asyncio
async def test_pending_files_returns_only_unverified_exact_catalog_rows(
    monkeypatch,
):
    catalog = _catalog()
    identities = identities_from_uhc_drug_catalog(
        SOURCE_ID,
        catalog,
    )
    database = _database()
    database.first.return_value = _set_row(identities)
    database.all.return_value = [
        _row(identity, status="verified", artifact_index=index)
        if index == 0
        else _row(identity)
        for index, identity in enumerate(identities)
    ]
    verify_retained = Mock()
    monkeypatch.setattr(
        source_artifacts,
        "_verify_retained_source_artifact",
        verify_retained,
    )

    pending = await source_artifacts.pending_source_files(
        identities,
        database=database,
    )

    assert pending == identities[1:]
    verify_retained.assert_called_once()


@pytest.mark.asyncio
async def test_missing_verified_blob_is_selected_for_exact_restore(monkeypatch):
    identities = identities_from_uhc_drug_catalog(SOURCE_ID, _catalog())
    database = _database()
    database.first.return_value = _set_row(identities)
    database.all.return_value = [
        _row(identity, status="verified", artifact_index=index)
        for index, identity in enumerate(identities)
    ]
    verify_retained = Mock(
        side_effect=RetainedArtifactError("retained_blob_unavailable")
    )
    monkeypatch.setattr(
        source_artifacts,
        "_verify_retained_source_artifact",
        verify_retained,
    )

    pending = await source_artifacts.pending_source_files(
        identities,
        database=database,
    )

    assert pending == identities


@pytest.mark.asyncio
async def test_corrupt_verified_blob_remains_fail_closed(monkeypatch):
    identities = identities_from_uhc_drug_catalog(SOURCE_ID, _catalog())
    database = _database()
    database.first.return_value = _set_row(identities)
    database.all.return_value = [
        _row(identity, status="verified", artifact_index=index)
        for index, identity in enumerate(identities)
    ]
    monkeypatch.setattr(
        source_artifacts,
        "_verify_retained_source_artifact",
        Mock(side_effect=RetainedArtifactError("retained_blob_digest_mismatch")),
    )

    with pytest.raises(RetainedArtifactError, match="digest_mismatch"):
        await source_artifacts.pending_source_files(
            identities,
            database=database,
        )


def _install_binding_test_doubles(monkeypatch):
    identity = identities_from_uhc_drug_catalog(
        SOURCE_ID,
        _catalog(),
    )[0]
    pending_row = _row(identity)
    verified_row = _row(identity, status="verified")
    database = _database()
    database.first.side_effect = [pending_row, pending_row, verified_row]
    install_and_verify = Mock()
    verify_retained = Mock()
    monkeypatch.setattr(
        source_artifact_binding,
        "install_and_verify_source_artifact",
        install_and_verify,
    )
    monkeypatch.setattr(
        source_artifact_binding,
        "verify_retained_source_artifact",
        verify_retained,
    )
    return identity, pending_row, verified_row, install_and_verify, verify_retained


@pytest.mark.asyncio
async def test_verified_binding_fills_pending_row_once(monkeypatch):
    """A pending ledger row installs bytes and performs one immutable fill."""

    identity, pending_row, verified_row, install_and_verify, _ = (
        _install_binding_test_doubles(monkeypatch)
    )
    database = _database()
    database.first.side_effect = [pending_row, pending_row, verified_row]

    verified = await source_artifacts.bind_verified_source_artifact(
        identity,
        source_path=Path("/retained-download"),
        artifact_sha256="1".zfill(64),
        artifact_byte_count=identity.expected_byte_count or 1,
        database=database,
    )

    assert verified.identity == identity
    assert verified.artifact_sha256 == "1".zfill(64)
    database.status.assert_awaited_once()
    install_and_verify.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_after_install_still_fills_ledger_before_propagating(
    monkeypatch,
):
    """Cancellation cannot split retained CAS publication from ledger fill."""

    identity = identities_from_uhc_drug_catalog(SOURCE_ID, _catalog())[0]
    pending_row = _row(identity)
    verified_row = _row(identity, status="verified")
    database = _database()
    database.first.side_effect = [pending_row, pending_row, verified_row]
    install_started = threading.Event()
    allow_install = threading.Event()

    def install_and_verify(*_args) -> None:
        install_started.set()
        assert allow_install.wait(timeout=5)

    monkeypatch.setattr(
        source_artifact_binding,
        "install_and_verify_source_artifact",
        install_and_verify,
    )
    bind_task = asyncio.create_task(
        source_artifacts.bind_verified_source_artifact(
            identity,
            source_path=Path("/retained-download"),
            artifact_sha256="1".zfill(64),
            artifact_byte_count=identity.expected_byte_count or 1,
            database=database,
        )
    )
    assert await asyncio.to_thread(install_started.wait, 5)
    bind_task.cancel()
    allow_install.set()
    with pytest.raises(asyncio.CancelledError):
        await bind_task

    database.status.assert_awaited_once()
    replay_database = _database()
    replay_database.first.return_value = verified_row
    verify_retained = Mock()
    monkeypatch.setattr(
        source_artifact_binding,
        "verify_retained_source_artifact",
        verify_retained,
    )
    replayed = await source_artifacts.bind_verified_source_artifact(
        identity,
        artifact_sha256="1".zfill(64),
        artifact_byte_count=identity.expected_byte_count or 1,
        database=replay_database,
    )
    assert replayed.identity == identity
    verify_retained.assert_called_once()


@pytest.mark.asyncio
async def test_verified_binding_replays_exact_retained_bytes(monkeypatch):
    """An exact verified replay rehashes retained bytes without another fill."""

    identity, _, verified_row, _, verify_retained = (
        _install_binding_test_doubles(monkeypatch)
    )
    replay_database = _database()
    replay_database.first.return_value = verified_row
    replayed = await source_artifacts.bind_verified_source_artifact(
        identity,
        artifact_sha256="1".zfill(64),
        artifact_byte_count=identity.expected_byte_count or 1,
        database=replay_database,
    )
    assert replayed.artifact_sha256 == "1".zfill(64)
    replay_database.status.assert_not_awaited()
    verify_retained.assert_called_once_with(
        replayed.artifact_sha256,
        replayed.artifact_byte_count,
    )


@pytest.mark.asyncio
async def test_verified_binding_restores_missing_exact_blob(monkeypatch):
    """A verified row may reinstall only its exact previously sealed bytes."""

    identity, _, verified_row, install_and_verify, _ = (
        _install_binding_test_doubles(monkeypatch)
    )
    restore_database = _database()
    restore_database.first.return_value = verified_row
    restored = await source_artifacts.bind_verified_source_artifact(
        identity,
        source_path=Path("/fresh-exact-download"),
        artifact_sha256="1".zfill(64),
        artifact_byte_count=identity.expected_byte_count or 1,
        database=restore_database,
    )
    assert restored.artifact_sha256 == "1".zfill(64)
    assert install_and_verify.call_args_list[-1].args == (
        Path("/fresh-exact-download"),
        restored.artifact_sha256,
        restored.artifact_byte_count,
    )


@pytest.mark.asyncio
async def test_complete_set_hash_is_order_independent_and_requires_every_file(
    monkeypatch,
):
    catalog = _catalog()
    identities = identities_from_uhc_drug_catalog(
        SOURCE_ID,
        catalog,
    )
    verified_rows = [
        _row(identity, status="verified", artifact_index=index)
        for index, identity in enumerate(identities)
    ]
    database = _database()
    database.first.return_value = _set_row(identities)
    database.all.return_value = list(reversed(verified_rows))
    verify_retained = Mock()
    monkeypatch.setattr(
        source_artifacts,
        "_verify_retained_source_artifact",
        verify_retained,
    )

    complete_set = await source_artifacts.load_complete_source_artifact_set(
        identities,
        database=database,
    )

    assert len(complete_set.artifacts) == 48
    assert complete_set.artifact_set_sha256 == source_artifacts.artifact_set_sha256(
        complete_set.artifacts
    )

    database.all.return_value = verified_rows[:-1]
    with pytest.raises(RuntimeError, match="set is inconsistent"):
        await source_artifacts.load_complete_source_artifact_set(
            identities,
            database=database,
        )


@pytest.mark.asyncio
async def test_binding_rejects_content_drift_and_catalog_byte_mismatch():
    identity = identities_from_uhc_drug_catalog(
        SOURCE_ID,
        _catalog(),
    )[0]
    database = _database()
    database.first.return_value = _row(identity, status="verified")

    with pytest.raises(RuntimeError, match="content changed"):
        await source_artifacts.bind_verified_source_artifact(
            identity,
            artifact_sha256="f" * 64,
            artifact_byte_count=identity.expected_byte_count or 1,
            database=database,
        )

    with pytest.raises(ValueError, match="byte count"):
        await source_artifacts.bind_verified_source_artifact(
            identity,
            artifact_sha256="1".zfill(64),
            artifact_byte_count=(identity.expected_byte_count or 1) + 1,
            database=database,
        )
