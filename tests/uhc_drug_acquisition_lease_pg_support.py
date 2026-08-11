# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import partial
import hashlib
from pathlib import Path
from typing import Any

import pytest

from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.source_artifact_binding import (
    bind_verified_source_artifact,
)
from process.formulary_fhir.source_artifact_contract import SourceArtifactIdentity
from process.formulary_fhir.source_artifacts import register_source_file_set
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    claim_uhc_drug_source_acquisition,
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


class _FenceObservedDatabase:
    """Expose the exact lease-row lock attempt while delegating all I/O."""

    def __init__(self, database: Any) -> None:
        self._database = database
        self.fence_lock_attempted = asyncio.Event()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._database, name)

    async def scalar(self, statement: Any, **parameters: Any) -> Any:
        if "source_acquisition_lease" in str(statement) and "FOR UPDATE" in str(
            statement
        ):
            self.fence_lock_attempted.set()
        return await self._database.scalar(statement, **parameters)


@dataclass(slots=True)
class _PendingBindRace:
    identity: SourceArtifactIdentity
    body: bytes
    staged_path: Path
    observed_database: _FenceObservedDatabase
    first_claim: Any
    blocker_task: asyncio.Task[None]
    bind_task: asyncio.Task[Any]
    second_claim_task: asyncio.Task[Any] | None = None


async def _hold_lease_row(
    database: Any,
    lock_acquired: asyncio.Event,
    allow_release: asyncio.Event,
) -> None:
    async with database.transaction():
        await database.scalar(
            "SELECT lease_generation FROM "
            f"{table_name('fhir_formulary_source_acquisition_lease')} "
            "WHERE source_id = :source_id FOR UPDATE;",
            source_id=UHC_FORMULARY_SOURCE_ID,
        )
        lock_acquired.set()
        await allow_release.wait()


def _single_pending_identity(body: bytes) -> SourceArtifactIdentity:
    return SourceArtifactIdentity(
        source_id=UHC_FORMULARY_SOURCE_ID,
        source_file_set_sha256="1" * 64,
        source_file_id="2" * 64,
        raw_listing_projection_sha256="3" * 64,
        family="cs",
        file_name="synthetic-drug.json",
        source_url="https://example.invalid/synthetic-drug.json",
        catalog_modified_at="2026-08-10T00:00:00Z",
        catalog_entry_sha256="4" * 64,
        expected_byte_count=len(body),
    )


async def _start_pending_bind_race(
    database: Any,
    blocker_database: Any,
    retained_root: Path,
    allow_blocker_release: asyncio.Event,
    blocker_has_lock: asyncio.Event,
) -> _PendingBindRace:
    await register_uhc_formulary_source(database=database)
    body = b"[{}]"
    identity = _single_pending_identity(body)
    await register_source_file_set(
        (identity,),
        source_observation_sha256="5" * 64,
        database=database,
    )
    first_claim = await claim_uhc_drug_source_acquisition(
        UHC_FORMULARY_SOURCE_ID,
        lease_seconds=1,
        database=database,
    )
    staged_path = retained_root / "pending-drug.json"
    staged_path.write_bytes(body)
    observed_database = _FenceObservedDatabase(database)
    blocker_task = asyncio.create_task(
        _hold_lease_row(
            blocker_database,
            blocker_has_lock,
            allow_blocker_release,
        )
    )
    await blocker_has_lock.wait()
    transaction_fence = partial(
        require_active_uhc_drug_source_acquisition,
        first_claim,
        database=observed_database,
    )
    bind_task = asyncio.create_task(
        bind_verified_source_artifact(
            identity,
            source_path=staged_path,
            artifact_sha256=hashlib.sha256(body).hexdigest(),
            artifact_byte_count=len(body),
            database=observed_database,
            transaction_fence=transaction_fence,
        )
    )
    await observed_database.fence_lock_attempted.wait()
    return _PendingBindRace(
        identity,
        body,
        staged_path,
        observed_database,
        first_claim,
        blocker_task,
        bind_task,
    )


async def _reclaim_after_stale_bind(
    race: _PendingBindRace,
    database: Any,
    allow_blocker_release: asyncio.Event,
) -> Any:
    await asyncio.sleep(1.2)
    race.second_claim_task = asyncio.create_task(
        claim_uhc_drug_source_acquisition(
            UHC_FORMULARY_SOURCE_ID,
            lease_seconds=5,
            database=database,
        )
    )
    allow_blocker_release.set()
    with pytest.raises(UHCDrugSourceAcquisitionLeaseError) as stale_bind:
        await race.bind_task
    assert stale_bind.value.code == "lease_lost"
    second_claim = await race.second_claim_task
    assert second_claim.lease_generation == race.first_claim.lease_generation + 1
    return second_claim


async def _bind_with_current_owner(
    race: _PendingBindRace,
    second_claim: Any,
    database: Any,
) -> None:
    artifact_row = await database.first(
        "SELECT status, artifact_sha256 FROM "
        f"{table_name('fhir_formulary_source_artifact')} WHERE "
        "source_id = :source_id AND source_file_id = :source_file_id;",
        source_id=race.identity.source_id,
        source_file_id=race.identity.source_file_id,
    )
    assert artifact_row.status == "pending"
    assert artifact_row.artifact_sha256 is None
    second_fence = partial(
        require_active_uhc_drug_source_acquisition,
        second_claim,
        database=race.observed_database,
    )
    artifact_digest = hashlib.sha256(race.body).hexdigest()
    verified_artifact = await bind_verified_source_artifact(
        race.identity,
        source_path=race.staged_path,
        artifact_sha256=artifact_digest,
        artifact_byte_count=len(race.body),
        database=race.observed_database,
        transaction_fence=second_fence,
    )
    assert verified_artifact.identity == race.identity
    assert verified_artifact.artifact_sha256 == artifact_digest
    assert verified_artifact.artifact_byte_count == len(race.body)
    await release_uhc_drug_source_acquisition(second_claim, database=database)
