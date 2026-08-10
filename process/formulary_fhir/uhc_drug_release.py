# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Receipt-only publication boundary for an admitted UHC drug candidate."""

from __future__ import annotations

import datetime as dt
from typing import Any

from db.models import db
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.source import require_source_unchanged
from process.formulary_fhir.uhc_drug_receipt import (
    reconstruct_uhc_drug_publication_inputs,
)
from process.formulary_fhir.uhc_drug_receipt import validate_uhc_drug_receipt_id
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID


UHC_DRUG_PUBLICATION_LOCK_WAIT_SECONDS = 5.0
UHC_DRUG_PUBLICATION_LOCK_RETRY_SECONDS = 0.1


def _validated_publication_result(
    receipt_id: str,
    candidate_dataset_id: str,
    admitted_at: dt.datetime,
    publication: PublicationResult,
) -> PublicationResult:
    if not (
        type(publication) is PublicationResult
        and publication.source_id == UHC_FORMULARY_SOURCE_ID
        and publication.dataset_id == candidate_dataset_id
        and type(publication.generation) is int
        and publication.generation > 0
        and type(publication.published_at) is dt.datetime
    ):
        raise RuntimeError("UHC drug publication result is inconsistent")
    published_at = utc_timestamp(
        publication.published_at,
        "UHC drug publication timestamp",
    )
    if published_at < admitted_at:
        raise RuntimeError("UHC drug publication result is inconsistent")
    validate_uhc_drug_receipt_id(receipt_id)
    return publication


async def _publish_under_lease(
    receipt_id: str,
    database: Any,
    repository: Any | None,
) -> PublicationResult:
    publication_inputs = await reconstruct_uhc_drug_publication_inputs(
        receipt_id=receipt_id,
        database=database,
    )
    selected_repository = (
        repository
        if repository is not None
        else FHIRFormularyRepository(
            source_id=publication_inputs.binding.source_id,
            database=database,
        )
    )
    publication = await selected_repository.publish_dataset(
        dataset=publication_inputs.candidate
    )
    await require_source_unchanged(
        publication_inputs.binding,
        database=database,
    )
    admission = publication_inputs.receipt.admission
    return _validated_publication_result(
        publication_inputs.receipt.receipt_id,
        admission.candidate_dataset_id,
        admission.admitted_at,
        publication,
    )


async def publish_admitted_uhc_drug_candidate(
    *,
    receipt_id: str,
    database: Any = db,
    repository: Any | None = None,
) -> PublicationResult:
    """Reconstruct and publish only one exact durable UHC receipt."""

    normalized_receipt_id = validate_uhc_drug_receipt_id(receipt_id)
    async with manual_lock.manual_source_lease(
        database,
        UHC_FORMULARY_SOURCE_ID,
        wait_seconds=UHC_DRUG_PUBLICATION_LOCK_WAIT_SECONDS,
        retry_seconds=UHC_DRUG_PUBLICATION_LOCK_RETRY_SECONDS,
    ):
        return await _publish_under_lease(
            normalized_receipt_id,
            database,
            repository,
        )


__all__ = ("publish_admitted_uhc_drug_candidate",)
