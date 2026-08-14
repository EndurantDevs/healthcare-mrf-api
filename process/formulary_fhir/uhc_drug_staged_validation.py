# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Semantic validation for one staged UHC formulary artifact."""

import datetime as dt
from collections.abc import Callable
from pathlib import Path
from typing import Any

from process.formulary_fhir.source_artifact_contract import SourceArtifactIdentity
from process.formulary_fhir.source_artifact_contract import VerifiedSourceArtifact
from process.formulary_fhir.source_artifacts import open_verified_source_artifact
from process.formulary_fhir.uhc_drug_payload import UHCDrugPayloadError
from process.formulary_fhir.uhc_drug_payload import count_uhc_drug_stream_items
from process.formulary_fhir.uhc_drug_spool import UHCDrugNormalizationError
from process.formulary_fhir.uhc_drug_spool import normalized_uhc_drug_source_records
from process.formulary_fhir.uhc_drug_transport_contract import UHCDrugArtifactAcquisitionError


def _validate_uhc_drug_artifact(
    artifact: VerifiedSourceArtifact,
    open_input: Callable[[], Any],
    cancel_check: Callable[[], None] | None,
) -> int:
    """Apply the exact structural and normalization contract to one artifact."""

    try:
        with open_input() as validation_file:
            expected_count = count_uhc_drug_stream_items(
                validation_file, cancel_check=cancel_check
            )
        with open_input() as input_file:
            observed_count = sum(
                1
                for _source_record, _memberships in normalized_uhc_drug_source_records(
                    artifact, input_file, cancel_check
                )
            )
        if observed_count != expected_count:
            raise UHCDrugNormalizationError("UHC drug artifact record census changed")
    except (UHCDrugNormalizationError, UHCDrugPayloadError):
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact source data is invalid",
            failure_evidence=("artifact_rejected",),
        ) from None
    return observed_count


def validate_staged_uhc_drug_artifact(
    source_path: Path,
    identity: SourceArtifactIdentity,
    artifact_sha256: str,
    artifact_byte_count: int,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> int:
    """Require every staged record to satisfy the spool source contract."""

    artifact = VerifiedSourceArtifact(
        identity=identity,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=artifact_byte_count,
        verified_at=dt.datetime.now(dt.UTC),
    )
    return _validate_uhc_drug_artifact(
        artifact,
        lambda: source_path.open("rb"),
        cancel_check,
    )


def validate_retained_uhc_drug_artifact(
    artifact: VerifiedSourceArtifact,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> int:
    """Apply current source semantics to one previously verified retained blob."""

    return _validate_uhc_drug_artifact(
        artifact,
        lambda: open_verified_source_artifact(artifact),
        cancel_check,
    )


__all__ = (
    "validate_retained_uhc_drug_artifact",
    "validate_staged_uhc_drug_artifact",
)
