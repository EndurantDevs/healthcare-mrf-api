# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from process.formulary_fhir.uhc_drug_acquisition import (
    acquire_uhc_drug_artifacts,
)
import process.formulary_fhir.uhc_drug_acquisition_lease as acquisition_lease
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    claim_uhc_drug_source_acquisition,
)
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    heartbeat_uhc_drug_source_acquisition,
)
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    release_uhc_drug_source_acquisition,
)
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    require_active_uhc_drug_source_acquisition,
)
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    UHCDrugSourceAcquisitionLeaseError,
)
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID
from process.formulary_fhir.uhc_source import register_uhc_formulary_source
from process.formulary_fhir.repository_shared import table_name
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import FOUNDATION_PATH
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.uhc_drug_vertical_postgres_support import acquisition_fixture
from tests.uhc_drug_vertical_postgres_support import runtime_database
from tests.uhc_drug_acquisition_lease_pg_support import _bind_with_current_owner
from tests.uhc_drug_acquisition_lease_pg_support import _PendingBindRace
from tests.uhc_drug_acquisition_lease_pg_support import _reclaim_after_stale_bind
from tests.uhc_drug_acquisition_lease_pg_support import _start_pending_bind_race


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)

VERSIONS = Path(FOUNDATION_PATH).parent
ARTIFACT_PATH = VERSIONS / "20260810030000_fhir_formulary_source_artifact.py"
LEASE_PATH = VERSIONS / ("20260811030000_fhir_formulary_source_acquisition_lease.py")


async def _upgrade_schema(engine: Any, schema_name: str) -> Any:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f"CREATE SCHEMA {quoted(schema_name)}")
    migrations = (
        load_migration(FOUNDATION_PATH, "uhc_lease_foundation"),
        load_migration(ARTIFACT_PATH, "uhc_lease_artifact"),
        load_migration(LEASE_PATH, "uhc_lease_storage"),
    )
    for migration in migrations:
        await run_migration(engine, migration, "upgrade")
    return migrations[-1]


class _BlockingContent:
    def __init__(self, started: asyncio.Event) -> None:
        self._started = started

    async def iter_chunked(self, _chunk_size: int):
        self._started.set()
        await asyncio.Event().wait()
        yield b""


class _BlockingResponse:
    def __init__(self, source_url: str, started: asyncio.Event) -> None:
        self.url = source_url
        self.status = 200
        self.headers: dict[str, str] = {}
        self.content_length = None
        self.content = _BlockingContent(started)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _BlockingSession:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.requested_urls: list[str] = []

    def get(
        self,
        source_url: str,
        *,
        allow_redirects: bool,
        headers: dict[str, str],
    ) -> _BlockingResponse:
        assert allow_redirects is False
        assert headers == {"Accept-Encoding": "identity"}
        self.requested_urls.append(source_url)
        return _BlockingResponse(source_url, self.started)


def _session_factory(session: Any):
    @asynccontextmanager
    async def factory(_timeout: Any):
        yield session

    return factory


async def _wait_for_first_http(
    acquisition_task: asyncio.Task[Any],
    blocking_session: _BlockingSession,
) -> None:
    request_started = asyncio.create_task(blocking_session.started.wait())
    async with asyncio.timeout(10):
        completed, _pending = await asyncio.wait(
            (acquisition_task, request_started),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if acquisition_task in completed:
            await acquisition_task
        await request_started


async def _assert_complete_retry(
    raw_proof: Any,
    database: Any,
    session_factory: Any,
    session: Any,
) -> None:
    completed = await acquire_uhc_drug_artifacts(
        raw_proof,
        database=database,
        session_factory=session_factory,
    )
    assert completed.file_count == 48
    assert completed.downloaded_file_count == 48
    assert len(session.requested_urls) == 48
    assert len(set(session.requested_urls)) == 48


async def _assert_released_lease(database: Any) -> None:
    lease_row = await database.first(
        f"SELECT lease_generation, lease_token, lease_expires_at FROM "
        f"{table_name('fhir_formulary_source_acquisition_lease')} WHERE "
        "source_id = :source_id;",
        source_id=UHC_FORMULARY_SOURCE_ID,
    )
    assert lease_row.lease_generation == 1
    assert lease_row.lease_token is None
    assert lease_row.lease_expires_at is None


async def _require_oversized_windows_rejected(database: Any) -> None:
    """Prove direct claim and heartbeat writes cannot exceed the DB TTL cap."""

    malicious_token = "f" * 64
    with pytest.raises(Exception):
        async with database.transaction():
            await acquisition_lease._set_action(
                database,
                "claim",
                source_id=UHC_FORMULARY_SOURCE_ID,
                lease_generation=None,
                lease_token=malicious_token,
            )
            await database.status(
                f"UPDATE {table_name('fhir_formulary_source_acquisition_lease')} "
                "SET lease_generation = lease_generation + 1, "
                "lease_token = :lease_token, "
                "lease_expires_at = transaction_timestamp() + "
                "INTERVAL '2 hours', "
                "lease_heartbeat_at = transaction_timestamp(), "
                "claimed_at = transaction_timestamp(), "
                "updated_at = transaction_timestamp() WHERE "
                "source_id = :source_id AND lease_token IS NULL;",
                source_id=UHC_FORMULARY_SOURCE_ID,
                lease_token=malicious_token,
            )

    current = await claim_uhc_drug_source_acquisition(
        UHC_FORMULARY_SOURCE_ID,
        lease_seconds=5,
        database=database,
    )
    with pytest.raises(Exception):
        async with database.transaction():
            await acquisition_lease._set_action(
                database,
                "heartbeat",
                source_id=current.source_id,
                lease_generation=current.lease_generation,
                lease_token=current.lease_token,
            )
            await database.status(
                f"UPDATE {table_name('fhir_formulary_source_acquisition_lease')} "
                "SET lease_expires_at = transaction_timestamp() + "
                "INTERVAL '2 hours', "
                "lease_heartbeat_at = transaction_timestamp(), "
                "updated_at = transaction_timestamp() WHERE "
                "source_id = :source_id AND "
                "lease_generation = :lease_generation AND "
                "lease_token = :lease_token;",
                source_id=current.source_id,
                lease_generation=current.lease_generation,
                lease_token=current.lease_token,
            )
    await release_uhc_drug_source_acquisition(current, database=database)


@pytest.mark.asyncio
async def test_pg_claim_reclaim_stale_token_and_downgrade_fences(
    monkeypatch,
) -> None:
    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    database = runtime_database(url)
    try:
        lease_migration = await _upgrade_schema(engine, schema_name)
        await run_migration(engine, lease_migration, "downgrade")
        await run_migration(engine, lease_migration, "upgrade")
        await register_uhc_formulary_source(database=database)

        first = await claim_uhc_drug_source_acquisition(
            UHC_FORMULARY_SOURCE_ID,
            lease_seconds=1,
            database=database,
        )
        with pytest.raises(UHCDrugSourceAcquisitionLeaseError) as busy:
            await claim_uhc_drug_source_acquisition(
                UHC_FORMULARY_SOURCE_ID,
                lease_seconds=1,
                database=database,
            )
        assert busy.value.code == "busy"

        with pytest.raises(Exception, match="downgrade_blocked"):
            await run_migration(engine, lease_migration, "downgrade")

        await asyncio.sleep(1.2)
        second = await claim_uhc_drug_source_acquisition(
            UHC_FORMULARY_SOURCE_ID,
            lease_seconds=5,
            database=database,
        )
        assert second.lease_generation == first.lease_generation + 1
        assert second.lease_token != first.lease_token
        for stale_operation in (
            require_active_uhc_drug_source_acquisition,
            heartbeat_uhc_drug_source_acquisition,
            release_uhc_drug_source_acquisition,
        ):
            with pytest.raises(UHCDrugSourceAcquisitionLeaseError) as stale:
                await stale_operation(first, database=database)
            assert stale.value.code == "lease_lost"
        await require_active_uhc_drug_source_acquisition(second, database=database)
        await release_uhc_drug_source_acquisition(second, database=database)
        await _require_oversized_windows_rejected(database)
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_pg_pending_bind_rechecks_expiry_after_blocked_lease_lock(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """An expired owner cannot fill after its lease-row lock wait drains."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    database = runtime_database(url)
    blocker_database = runtime_database(url)
    allow_blocker_release = asyncio.Event()
    blocker_has_lock = asyncio.Event()
    race: _PendingBindRace | None = None
    try:
        await _upgrade_schema(engine, schema_name)
        race = await _start_pending_bind_race(
            database,
            blocker_database,
            retained_artifact_test_root,
            allow_blocker_release,
            blocker_has_lock,
        )
        second_claim = await _reclaim_after_stale_bind(
            race,
            database,
            allow_blocker_release,
        )
        await _bind_with_current_owner(race, second_claim, database)
        await race.blocker_task
    finally:
        allow_blocker_release.set()
        tasks = (
            ()
            if race is None
            else (
                race.blocker_task,
                race.second_claim_task,
            )
        )
        for unfinished_task in tasks:
            if unfinished_task is not None and not unfinished_task.done():
                unfinished_task.cancel()
                await asyncio.gather(unfinished_task, return_exceptions=True)
        await database.disconnect()
        await blocker_database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_pg_concurrent_acquisition_is_busy_and_cancel_releases_for_retry(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """A live claimant excludes HTTP, then cancellation enables exact retry."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    monkeypatch.setenv("HLTHPRT_UHC_FORMULARY_MIN_FREE_BYTES", "1")
    database = runtime_database(url)
    try:
        await _upgrade_schema(engine, schema_name)
        raw_proof, successful_factory, successful_session = acquisition_fixture(
            monkeypatch,
            retained_artifact_test_root,
        )
        blocking_session = _BlockingSession()
        first_acquisition = asyncio.create_task(
            acquire_uhc_drug_artifacts(
                raw_proof,
                database=database,
                session_factory=_session_factory(blocking_session),
            )
        )
        await _wait_for_first_http(first_acquisition, blocking_session)

        with pytest.raises(UHCDrugSourceAcquisitionLeaseError) as busy:
            await acquire_uhc_drug_artifacts(
                raw_proof,
                database=database,
                session_factory=successful_factory,
            )
        assert busy.value.code == "busy"
        assert successful_session.requested_urls == []

        first_acquisition.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_acquisition
        await _assert_released_lease(database)

        await _assert_complete_retry(
            raw_proof,
            database,
            successful_factory,
            successful_session,
        )
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()
