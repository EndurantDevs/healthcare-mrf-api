# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded acquisition of the exact UHC IFP and CS drug-file set."""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
from typing import Any

from db.models import db
from process.formulary_fhir.async_safety import cancellable_to_thread
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.source import require_source_unchanged
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.source_artifacts import (
    load_complete_source_artifact_set,
    load_selected_source_artifact_set,
)
from process.formulary_fhir.source_artifacts import pending_source_files
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    require_active_uhc_drug_source_acquisition,
)
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    run_with_uhc_drug_source_acquisition_lease,
)
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    UHCDrugSourceAcquisitionClaim,
)
from process.formulary_fhir.uhc_drug_transport import (
    acquire_pending_uhc_drug_artifacts,
)
from process.formulary_fhir.uhc_drug_transport import CancelCheck
from process.formulary_fhir.uhc_drug_transport import (
    default_uhc_drug_session_factory,
)
from process.formulary_fhir.uhc_drug_transport import ProgressCallback
from process.formulary_fhir.uhc_drug_transport import SessionFactory
from process.formulary_fhir.uhc_drug_transport import (
    UHCDrugArtifactAcquisitionError,
)
from process.formulary_fhir.uhc_drug_transport import (
    uhc_drug_download_concurrency,
)
from process.formulary_fhir.uhc_drug_staged_validation import (
    validate_retained_uhc_drug_artifact,
)
from process.formulary_fhir.uhc_source import register_uhc_formulary_source
from process.formulary_fhir.uhc_source_artifacts import (
    register_uhc_source_file_set,
)
from process.uhc_provider_file_catalog import load_retained_uhc_catalog_proof


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugArtifactAcquisitionResult:
    """Summarize one selected retained set without exposing source URLs."""

    source_id: str
    source_observation_sha256: str = field(repr=False)
    source_file_set_sha256: str = field(repr=False)
    artifact_set_sha256: str = field(repr=False)
    file_count: int
    downloaded_file_count: int
    reused_file_count: int
    downloaded_byte_count: int
    artifacts: VerifiedSourceArtifactSet = field(repr=False)
    expected_file_count: int = 48
    excluded_file_count: int = 0
    excluded_source_file_ids: tuple[str, ...] = field(
        default=(),
        repr=False,
    )

    def __post_init__(self) -> None:
        strict_hash(self.source_observation_sha256, "source observation hash")
        selected_source_file_ids = tuple(
            artifact.identity.source_file_id
            for artifact in self.artifacts.artifacts
        ) if type(self.artifacts) is VerifiedSourceArtifactSet else ()
        excluded_source_file_ids = self.excluded_source_file_ids
        if (
            type(self.artifacts) is not VerifiedSourceArtifactSet
            or type(excluded_source_file_ids) is not tuple
            or any(type(source_file_id) is not str for source_file_id in excluded_source_file_ids)
            or self.source_id != self.artifacts.source_id
            or self.source_file_set_sha256 != self.artifacts.source_file_set_sha256
            or self.artifact_set_sha256 != self.artifacts.artifact_set_sha256
            or self.file_count != len(self.artifacts.artifacts)
            or self.expected_file_count != 48
            or self.excluded_file_count != 48 - self.file_count
            or len(excluded_source_file_ids) != self.excluded_file_count
            or len(set(excluded_source_file_ids))
            != len(excluded_source_file_ids)
            or set(selected_source_file_ids) & set(excluded_source_file_ids)
            or len(selected_source_file_ids) + len(excluded_source_file_ids)
            != self.expected_file_count
            or self.file_count < 1
            or self.downloaded_file_count + self.reused_file_count != self.file_count
            or min(
                self.file_count,
                self.downloaded_file_count,
                self.reused_file_count,
                self.downloaded_byte_count,
                self.excluded_file_count,
            )
            < 0
        ):
            raise ValueError("UHC drug artifact acquisition result is invalid")
        for source_file_id in excluded_source_file_ids:
            strict_hash(source_file_id, "excluded source file id")
        family_count_by_name = {
            family: sum(
                artifact.identity.family == family
                for artifact in self.artifacts.artifacts
            )
            for family in ("cs", "ifp")
        }
        if any(count > 24 for count in family_count_by_name.values()):
            raise ValueError("UHC drug artifact acquisition result is invalid")

    @property
    def is_coverage_complete(self) -> bool:
        """Return whether all advertised artifacts were validated."""

        return self.excluded_file_count == 0

    def __repr__(self) -> str:
        return (
            "UHCDrugArtifactAcquisitionResult("
            f"source_id={self.source_id!r}, file_count={self.file_count}, "
            f"downloaded_file_count={self.downloaded_file_count}, "
            f"reused_file_count={self.reused_file_count})"
        )


async def _invoke_cancel(cancel_check: CancelCheck | None) -> None:
    if cancel_check is None:
        return
    cancellation_result = cancel_check()
    if inspect.isawaitable(cancellation_result):
        await cancellation_result


async def _require_postflight_binding(binding: Any, *, database: Any) -> None:
    postflight_binding = await register_uhc_formulary_source(database=database)
    if postflight_binding != binding:
        raise UHCDrugArtifactAcquisitionError(
            "UHC formulary source changed during artifact acquisition"
        )


def _acquisition_result(
    binding: Any,
    registration: Any,
    pending: tuple[Any, ...],
    downloaded_bytes: int,
    artifacts: VerifiedSourceArtifactSet,
) -> UHCDrugArtifactAcquisitionResult:
    selected_ids = {
        artifact.identity.source_file_id for artifact in artifacts.artifacts
    }
    excluded_source_file_ids = tuple(
        identity.source_file_id
        for identity in registration.identities
        if identity.source_file_id not in selected_ids
    )
    pending_ids = {identity.source_file_id for identity in pending}
    downloaded_file_count = len(selected_ids & pending_ids)
    return UHCDrugArtifactAcquisitionResult(
        source_id=binding.source_id,
        source_observation_sha256=registration.source_observation_sha256,
        source_file_set_sha256=artifacts.source_file_set_sha256,
        artifact_set_sha256=artifacts.artifact_set_sha256,
        file_count=len(artifacts.artifacts),
        downloaded_file_count=downloaded_file_count,
        reused_file_count=len(artifacts.artifacts) - downloaded_file_count,
        downloaded_byte_count=downloaded_bytes,
        artifacts=artifacts,
        expected_file_count=48,
        excluded_file_count=48 - len(selected_ids),
        excluded_source_file_ids=excluded_source_file_ids,
    )


async def acquire_uhc_drug_artifacts(
    raw_proof: Any,
    *,
    database: Any = db,
    session_factory: SessionFactory = default_uhc_drug_session_factory,
    cancel_check: CancelCheck | None = None,
    progress_callback: ProgressCallback | None = None,
) -> UHCDrugArtifactAcquisitionResult:
    """Acquire one already-retained proof under a durable source claim."""

    await _invoke_cancel(cancel_check)
    binding = await register_uhc_formulary_source(database=database)

    async def acquire_claimed(
        claim: UHCDrugSourceAcquisitionClaim,
    ) -> UHCDrugArtifactAcquisitionResult:
        """Run the exact retained proof inside the granted generation."""

        return await _acquire_registered_uhc_drug_artifacts(
            raw_proof,
            binding=binding,
            claim=claim,
            database=database,
            session_factory=session_factory,
            cancel_check=cancel_check,
            progress_callback=progress_callback,
        )

    return await run_with_uhc_drug_source_acquisition_lease(
        binding.source_id,
        acquire_claimed,
        database=database,
    )


def _selected_source_file_ids(
    identities: tuple[Any, ...],
    rejected_ids: set[str],
) -> tuple[str, ...]:
    selected_ids = tuple(
        identity.source_file_id
        for identity in identities
        if identity.source_file_id not in rejected_ids
    )
    if not selected_ids:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact set has no valid source artifacts",
            failure_evidence=("artifact_rejected",),
        )
    return selected_ids


async def _screen_reused_source_file_ids(
    artifacts: VerifiedSourceArtifactSet,
    pending: tuple[Any, ...],
    rejected_ids: set[str],
    cancel_check: CancelCheck | None,
) -> None:
    pending_ids = {identity.source_file_id for identity in pending}
    for artifact in artifacts.artifacts:
        source_file_id = artifact.identity.source_file_id
        if source_file_id in pending_ids:
            continue
        await _invoke_cancel(cancel_check)
        try:
            await cancellable_to_thread(
                validate_retained_uhc_drug_artifact,
                artifact,
            )
        except UHCDrugArtifactAcquisitionError as error:
            if error.retryable or error.failure_evidence != ("artifact_rejected",):
                raise
            rejected_ids.add(source_file_id)
    await _invoke_cancel(cancel_check)


async def _load_acquired_artifacts(
    registration: Any,
    pending: tuple[Any, ...],
    rejected_source_file_ids: tuple[str, ...],
    *,
    database: Any,
    cancel_check: CancelCheck | None,
) -> VerifiedSourceArtifactSet:
    """Load current artifacts after source-invalid retained rows are omitted."""

    rejected_ids = set(rejected_source_file_ids)
    selected_ids = _selected_source_file_ids(registration.identities, rejected_ids)
    if rejected_ids:
        artifacts = await load_selected_source_artifact_set(
            registration.identities,
            selected_source_file_ids=selected_ids,
            require_unselected_pending=False,
            database=database,
            cancel_check=cancel_check,
        )
    else:
        artifacts = await load_complete_source_artifact_set(
            registration.identities,
            database=database,
            cancel_check=cancel_check,
        )

    await _screen_reused_source_file_ids(
        artifacts,
        pending,
        rejected_ids,
        cancel_check,
    )
    final_selected_ids = _selected_source_file_ids(
        registration.identities,
        rejected_ids,
    )
    if final_selected_ids == selected_ids:
        return artifacts
    return await load_selected_source_artifact_set(
        registration.identities,
        selected_source_file_ids=final_selected_ids,
        require_unselected_pending=False,
        database=database,
        cancel_check=cancel_check,
    )


async def _acquire_registered_uhc_drug_artifacts(
    raw_proof: Any,
    *,
    binding: Any,
    claim: UHCDrugSourceAcquisitionClaim,
    database: Any,
    session_factory: SessionFactory,
    cancel_check: CancelCheck | None,
    progress_callback: ProgressCallback | None,
) -> UHCDrugArtifactAcquisitionResult:
    """Register, download, and reverify inside one exact live generation."""

    async def claim_check() -> None:
        """Lock and recheck the exact generation at an action boundary."""

        await require_active_uhc_drug_source_acquisition(claim, database=database)

    await _invoke_cancel(cancel_check)
    await claim_check()
    registration = await register_uhc_source_file_set(
        binding.source_id,
        raw_proof,
        database=database,
    )
    await require_source_unchanged(binding, database=database)
    pending = await pending_source_files(
        registration.identities,
        database=database,
        cancel_check=cancel_check,
    )
    await _invoke_cancel(cancel_check)
    await claim_check()
    downloaded_bytes, rejected_source_file_ids = await acquire_pending_uhc_drug_artifacts(
        pending,
        database=database,
        session_factory=session_factory,
        cancel_check=cancel_check,
        claim_check=claim_check,
        progress_callback=progress_callback,
    )
    await _invoke_cancel(cancel_check)
    await claim_check()
    artifacts = await _load_acquired_artifacts(
        registration,
        pending,
        rejected_source_file_ids,
        database=database,
        cancel_check=cancel_check,
    )
    await _invoke_cancel(cancel_check)
    await claim_check()
    await require_source_unchanged(binding, database=database)
    await _require_postflight_binding(binding, database=database)
    return _acquisition_result(
        binding,
        registration,
        pending,
        downloaded_bytes,
        artifacts,
    )


async def acquire_current_uhc_drug_artifacts(
    *,
    raw_set_sha256: str | None = None,
    database: Any = db,
    session_factory: SessionFactory = default_uhc_drug_session_factory,
    cancel_check: CancelCheck | None = None,
    progress_callback: ProgressCallback | None = None,
) -> UHCDrugArtifactAcquisitionResult:
    """Snapshot retained listings and acquire every file under one claim."""

    await _invoke_cancel(cancel_check)
    binding = await register_uhc_formulary_source(database=database)

    async def acquire_current_claimed(
        claim: UHCDrugSourceAcquisitionClaim,
    ) -> UHCDrugArtifactAcquisitionResult:
        """Snapshot retained listings and acquire them in one generation."""

        await require_active_uhc_drug_source_acquisition(
            claim,
            database=database,
        )
        raw_proof = await load_retained_uhc_catalog_proof(
            raw_set_sha256=raw_set_sha256,
            database=database,
        )
        await require_active_uhc_drug_source_acquisition(
            claim,
            database=database,
        )
        return await _acquire_registered_uhc_drug_artifacts(
            raw_proof,
            binding=binding,
            claim=claim,
            database=database,
            session_factory=session_factory,
            cancel_check=cancel_check,
            progress_callback=progress_callback,
        )

    return await run_with_uhc_drug_source_acquisition_lease(
        binding.source_id,
        acquire_current_claimed,
        database=database,
    )


__all__ = (
    "UHCDrugArtifactAcquisitionError",
    "UHCDrugArtifactAcquisitionResult",
    "acquire_current_uhc_drug_artifacts",
    "acquire_uhc_drug_artifacts",
    "uhc_drug_download_concurrency",
)
