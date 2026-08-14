# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared offline harness for Flex Practitioner acquisition tests."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import datetime as dt
import hashlib
import json

from process import uhc_flex_practitioner_acquisition as acquisition
from process.uhc_flex_official_cohort_contract import (
    build_uhc_flex_official_cohort,
)
from process.uhc_flex_official_cohort_store import (
    UHCFlexOfficialCohortSyncResult,
)
from process.uhc_flex_practitioner_query import (
    validate_uhc_flex_practitioner_search_bundle,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
    UHCFlexPractitionerRegistrationResult,
)
from process.uhc_flex_practitioner_store import (
    UHCFlexPractitionerAcquisitionSummary,
    UHCFlexPractitionerStoreError,
    UHCFlexPractitionerWorkClaim,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    build_uhc_flex_practitioner_twin_admission,
    build_uhc_flex_practitioner_twin_attempt,
    UHCFlexPractitionerSealedRoot,
)


OPERATION_KEY = "a" * 64
PROJECTION_DATE = "2026-08-10"
MEMBER_NPIS = (
    1000000004,
    1000000012,
    1000000020,
    1000000038,
    1000000046,
    1000000053,
    1000000061,
    1000000079,
)


def cohort_fixture(*, suffix: str = "a", npi_count: int = 2):
    return build_uhc_flex_official_cohort(
        official_endpoint_id=f"official-endpoint-{suffix}",
        official_dataset_id=f"official-dataset-{suffix}",
        official_acquisition_root_run_id=suffix * 64,
        official_dataset_hash=suffix * 64,
        official_content_proof_sha256=("f" if suffix == "a" else "e") * 64,
        practitioner_resource_count=npi_count,
        npi_count=npi_count,
    )


def registration_fixture(*, created: bool = False):
    return UHCFlexPractitionerRegistrationResult(
        source_id=acquisition.UHC_FLEX_PRACTITIONER_SOURCE_ID,
        endpoint_id=uhc_flex_practitioner_endpoint_identity().endpoint_id,
        endpoint_created=created,
        source_created=created,
    )


def query_result_fixture(npi: int):
    if npi == MEMBER_NPIS[0]:
        resources = [
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": "synthetic-practitioner-1",
                    "identifier": [
                        {
                            "system": "http://hl7.org/fhir/sid/us-npi",
                            "value": str(npi),
                        }
                    ],
                }
            }
        ]
    else:
        resources = []
    return validate_uhc_flex_practitioner_search_bundle(
        npi,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(resources),
            "entry": resources,
        },
    )


class FakeSession:
    def __init__(self, role: str, serial: int, connection_limit: int) -> None:
        self.role = role
        self.serial = serial
        self.connection_limit = connection_limit


class FakeDatabase:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    @asynccontextmanager
    async def transaction(self):
        try:
            yield self
        except BaseException:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1


class AcquisitionHarness:
    def __init__(self, *, npi_count: int = 2) -> None:
        self.npis = MEMBER_NPIS[:npi_count]
        self.database = FakeDatabase()
        self.cohorts = [cohort_fixture(npi_count=npi_count)]
        self.registrations = [registration_fixture()]
        self.events: list[str] = []
        self.identities = {}
        self.pending: dict[str, list[int]] = {}
        self.attempts: dict[tuple[str, int], int] = {}
        self.active: dict[tuple[str, int], UHCFlexPractitionerWorkClaim] = {}
        self.terminal: dict[
            str,
            dict[int, tuple[str, str | None, int, str | None]],
        ] = {}
        self.summaries = {}
        self.admissions = {}
        self.admission_error = None
        self.session_serial = 0
        self.sessions: list[FakeSession] = []
        self.fetch_calls: dict[tuple[str, int], int] = {}
        self.fetch_failures: dict[tuple[str, int, int], BaseException] = {}
        self.fetch_count = 0
        self.active_fetches = 0
        self.maximum_active_fetches = 0
        self.fetch_barrier_target = 0
        self.fetch_barrier = asyncio.Event()
        self.block_fetch = False
        self.fetch_entered = asyncio.Event()
        self.sleep_delays: list[float] = []
        self.progress = []

    def acquisition_role(self, acquisition_id: str) -> str:
        return self.identities[acquisition_id].acquisition_role

    async def register_source(self, *, database):
        self.events.append("register")
        index = min(self.events.count("register") - 1, len(self.registrations) - 1)
        return self.registrations[index]

    async def sync_cohort(self, *, database):
        self.events.append("cohort")
        index = min(self.events.count("cohort") - 1, len(self.cohorts) - 1)
        return UHCFlexOfficialCohortSyncResult(self.cohorts[index], False)

    async def initialize_root(self, identity, *, database):
        role = identity.acquisition_role
        self.events.append(f"initialize:{role}")
        if identity.acquisition_id in self.identities:
            assert self.identities[identity.acquisition_id] == identity
            return 0
        self.identities[identity.acquisition_id] = identity
        self.pending[identity.acquisition_id] = list(self.npis)
        self.terminal[identity.acquisition_id] = {}
        return 1

    async def claim_work(
        self,
        acquisition_id,
        *,
        requested_npi=None,
        excluded_npis=(),
        fresh_only=None,
        lease_seconds,
        database,
    ):
        pending = self.pending[acquisition_id]
        if requested_npi is None:
            eligible_npis = [
                npi for npi in pending if npi not in excluded_npis
            ]
            if not eligible_npis:
                return None
            fresh_npis = [
                npi
                for npi in eligible_npis
                if self.attempts.get((acquisition_id, npi), 0) == 0
            ]
            candidates = fresh_npis if fresh_only is True else fresh_npis or eligible_npis
            if not candidates:
                return None
            npi = min(candidates)
            pending.remove(npi)
        else:
            if requested_npi not in pending:
                return None
            pending.remove(requested_npi)
            npi = requested_npi
        attempt_key = (acquisition_id, npi)
        attempt = self.attempts.get(attempt_key, 0) + 1
        self.attempts[attempt_key] = attempt
        lease_token = hashlib.sha256(
            f"{acquisition_id}:{npi}:{attempt}".encode()
        ).hexdigest()
        claim = UHCFlexPractitionerWorkClaim(
            acquisition_id=acquisition_id,
            cohort_id=self.identities[acquisition_id].cohort_id,
            requested_npi=npi,
            attempt=attempt,
            lease_token=lease_token,
        )
        self.active[attempt_key] = claim
        self.events.append(f"claim:{self.acquisition_role(acquisition_id)}")
        return claim

    async def fetch(self, session, requested_npi):
        role = session.role
        call_key = (role, requested_npi)
        call_number = self.fetch_calls.get(call_key, 0) + 1
        self.fetch_calls[call_key] = call_number
        self.fetch_count += 1
        self.active_fetches += 1
        self.maximum_active_fetches = max(
            self.maximum_active_fetches,
            self.active_fetches,
        )
        self.events.append(f"fetch:{role}")
        self.fetch_entered.set()
        try:
            if self.block_fetch:
                await asyncio.Future()
            if self.fetch_barrier_target:
                if self.active_fetches >= self.fetch_barrier_target:
                    self.fetch_barrier.set()
                await self.fetch_barrier.wait()
                await asyncio.sleep(0)
            planned_error = self.fetch_failures.get(
                (role, requested_npi, call_number)
            )
            if planned_error is not None:
                raise planned_error
            return query_result_fixture(requested_npi)
        finally:
            self.active_fetches -= 1

    def active_claim_key(self, claim):
        claim_key = (claim.acquisition_id, claim.requested_npi)
        assert self.active.get(claim_key) == claim
        return claim_key

    async def complete_result(self, claim, query_result, *, database):
        claim_key = self.active_claim_key(claim)
        self.active.pop(claim_key)
        self.terminal[claim.acquisition_id][claim.requested_npi] = (
            query_result.outcome,
            query_result.result_sha256,
            query_result.resource_count,
            None,
        )
        self.events.append(
            f"complete:{self.acquisition_role(claim.acquisition_id)}"
        )

    async def complete_error(self, claim, *, error_code, database):
        claim_key = self.active_claim_key(claim)
        self.active.pop(claim_key)
        self.terminal[claim.acquisition_id][claim.requested_npi] = (
            "error",
            None,
            0,
            error_code,
        )
        self.events.append(
            f"error:{self.acquisition_role(claim.acquisition_id)}:{error_code}"
        )

    async def release_work(self, claim, *, database):
        claim_key = self.active_claim_key(claim)
        self.active.pop(claim_key)
        self.pending[claim.acquisition_id].append(claim.requested_npi)
        self.pending[claim.acquisition_id].sort()
        self.events.append(
            f"release:{self.acquisition_role(claim.acquisition_id)}"
        )

    def terminal_hash(self, acquisition_id: str) -> str:
        terminal_rows = [
            (npi, *self.terminal[acquisition_id][npi])
            for npi in sorted(self.terminal[acquisition_id])
        ]
        return hashlib.sha256(
            json.dumps(terminal_rows, separators=(",", ":")).encode()
        ).hexdigest()

    async def seal_root(self, identity, *, database):
        role = identity.acquisition_role
        self.events.append(f"seal:{role}")
        if identity.acquisition_id in self.summaries:
            return self.summaries[identity.acquisition_id]
        terminal = self.terminal[identity.acquisition_id]
        error_count = sum(
            terminal_value[0] == "error"
            for terminal_value in terminal.values()
        )
        if (
            self.pending[identity.acquisition_id]
            or any(key[0] == identity.acquisition_id for key in self.active)
            or len(terminal) != identity.expected_npi_count
            or error_count
        ):
            raise UHCFlexPractitionerStoreError("state")
        summary = UHCFlexPractitionerAcquisitionSummary(
            acquisition_id=identity.acquisition_id,
            expected_npi_count=identity.expected_npi_count,
            matched_count=sum(
                terminal_value[0] == "matched"
                for terminal_value in terminal.values()
            ),
            unmatched_count=sum(
                terminal_value[0] == "unmatched"
                for terminal_value in terminal.values()
            ),
            error_count=0,
            resource_count=sum(
                terminal_value[2]
                for terminal_value in terminal.values()
            ),
            terminal_set_sha256=self.terminal_hash(identity.acquisition_id),
            cohort_complete=True,
            endpoint_collection_complete=False,
            endpoint_complete=False,
        )
        self.summaries[identity.acquisition_id] = summary
        return summary

    def sealed_root(self, acquisition_id: str) -> UHCFlexPractitionerSealedRoot:
        identity = self.identities[acquisition_id]
        summary = self.summaries[acquisition_id]
        return UHCFlexPractitionerSealedRoot(
            acquisition_id=identity.acquisition_id,
            cohort_id=identity.cohort_id,
            acquisition_role=identity.acquisition_role,
            source_id=identity.source_id,
            connector_id=identity.connector_id,
            query_contract_id=identity.query_contract_id,
            storage_contract_id=identity.storage_contract_id,
            run_id=identity.run_id,
            dataset_intent_id=identity.dataset_intent_id,
            expected_npi_count=identity.expected_npi_count,
            resource_count=summary.resource_count,
            terminal_set_sha256=summary.terminal_set_sha256,
        )

    async def admit_twins(
        self,
        baseline_acquisition_id,
        candidate_acquisition_id,
        *,
        semantic_projection_as_of,
        operation_key,
        database,
    ):
        self.events.append("admit")
        if self.admission_error is not None:
            self.events.append("attempt_persisted")
            raise self.admission_error
        if candidate_acquisition_id in self.admissions:
            return self.admissions[candidate_acquisition_id]
        attempt = build_uhc_flex_practitioner_twin_attempt(
            self.sealed_root(baseline_acquisition_id),
            self.sealed_root(candidate_acquisition_id),
            semantic_projection_as_of=semantic_projection_as_of,
            operation_key=operation_key,
            attempted_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
        )
        admission = build_uhc_flex_practitioner_twin_admission(
            attempt,
            admitted_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
        )
        self.admissions[candidate_acquisition_id] = admission
        return admission

    @asynccontextmanager
    async def session_scope(self, connection_limit: int):
        role = ("baseline", "candidate")[self.session_serial % 2]
        session = FakeSession(role, self.session_serial, connection_limit)
        self.session_serial += 1
        self.sessions.append(session)
        self.events.append(f"session_enter:{role}")
        try:
            yield session
        finally:
            self.events.append(f"session_exit:{role}")

    async def sleep(self, delay_seconds: float):
        self.sleep_delays.append(delay_seconds)
        self.events.append("sleep")
        await asyncio.sleep(0)

    async def progress_callback(self, progress):
        self.progress.append(progress)

    def dependencies(self):
        return acquisition.UHCFlexPractitionerAcquisitionDependencies(
            register_source=self.register_source,
            sync_cohort=self.sync_cohort,
            initialize_root=self.initialize_root,
            claim_work=self.claim_work,
            fetch=self.fetch,
            complete_result=self.complete_result,
            complete_error=self.complete_error,
            release_work=self.release_work,
            seal_root=self.seal_root,
            admit_twins=self.admit_twins,
            session_scope=self.session_scope,
            sleep=self.sleep,
        )


def enabled_config(**changes):
    return acquisition.UHCFlexPractitionerAcquisitionConfig(
        enabled=True,
        **changes,
    )


async def acquire_with_harness(harness: AcquisitionHarness, **options):
    return await acquisition.acquire_uhc_flex_practitioner_twins(
        operation_key=OPERATION_KEY,
        semantic_projection_as_of=PROJECTION_DATE,
        config=options.pop("config", enabled_config(concurrency=2)),
        database=harness.database,
        dependencies=harness.dependencies(),
        progress_callback=options.pop(
            "progress_callback",
            harness.progress_callback,
        ),
        **options,
    )
