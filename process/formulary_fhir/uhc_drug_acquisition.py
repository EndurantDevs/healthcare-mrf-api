# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded acquisition of the exact UHC IFP and CS drug-file set."""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
from typing import Any

from db.models import db
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.source import require_source_unchanged
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.source_artifacts import (
    load_complete_source_artifact_set,
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
from process.formulary_fhir.uhc_source import register_uhc_formulary_source
from process.formulary_fhir.uhc_source_artifacts import (
    register_uhc_source_file_set,
)
from process.uhc_provider_file_catalog import load_retained_uhc_catalog_proof


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugArtifactAcquisitionResult:
    """Summarize one complete retained set without exposing source URLs."""

    source_id: str
    source_observation_sha256: str = field(repr=False)
    source_file_set_sha256: str = field(repr=False)
    artifact_set_sha256: str = field(repr=False)
    file_count: int
    downloaded_file_count: int
    reused_file_count: int
    downloaded_byte_count: int
    artifacts: VerifiedSourceArtifactSet = field(repr=False)

    def __post_init__(self) -> None:
        strict_hash(self.source_observation_sha256, "source observation hash")
        if (
            type(self.artifacts) is not VerifiedSourceArtifactSet
            or self.source_id != self.artifacts.source_id
            or self.source_file_set_sha256 != self.artifacts.source_file_set_sha256
            or self.artifact_set_sha256 != self.artifacts.artifact_set_sha256
            or self.file_count != 48
            or self.file_count != len(self.artifacts.artifacts)
            or self.downloaded_file_count + self.reused_file_count != self.file_count
            or min(
                self.file_count,
                self.downloaded_file_count,
                self.reused_file_count,
                self.downloaded_byte_count,
            )
            < 0
        ):
            raise ValueError("UHC drug artifact acquisition result is invalid")
        family_count_by_name = {
            family: sum(
                artifact.identity.family == family
                for artifact in self.artifacts.artifacts
            )
            for family in ("cs", "ifp")
        }
        if family_count_by_name != {"cs": 24, "ifp": 24}:
            raise ValueError("UHC drug artifact acquisition result is invalid")

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
    return UHCDrugArtifactAcquisitionResult(
        source_id=binding.source_id,
        source_observation_sha256=registration.source_observation_sha256,
        source_file_set_sha256=artifacts.source_file_set_sha256,
        artifact_set_sha256=artifacts.artifact_set_sha256,
        file_count=len(artifacts.artifacts),
        downloaded_file_count=len(pending),
        reused_file_count=len(artifacts.artifacts) - len(pending),
        downloaded_byte_count=downloaded_bytes,
        artifacts=artifacts,
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

        await require_active_uhc_drug_source_acquisition(
            claim,
            database=database,
        )

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
    downloaded_bytes = await acquire_pending_uhc_drug_artifacts(
        pending,
        database=database,
        session_factory=session_factory,
        cancel_check=cancel_check,
        claim_check=claim_check,
        progress_callback=progress_callback,
    )
    await _invoke_cancel(cancel_check)
    await claim_check()
    artifacts = await load_complete_source_artifact_set(
        registration.identities,
        database=database,
        cancel_check=cancel_check,
    )
    await _invoke_cancel(cancel_check)
    await claim_check()
    await require_source_unchanged(binding, database=database)
    await _require_postflight_binding(binding, database=database)
    return _acquisition_result(
        binding, registration, pending, downloaded_bytes, artifacts
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
