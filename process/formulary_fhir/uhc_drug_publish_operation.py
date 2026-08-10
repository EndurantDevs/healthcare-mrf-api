# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off publication phase for one durable UHC receipt."""

from __future__ import annotations

import asyncio
from typing import Any

from db.models import db
from process.formulary_fhir.uhc_drug_operation import (
    require_uhc_publication_gate,
)
from process.formulary_fhir.uhc_drug_operation import UHCDrugOperationError
from process.formulary_fhir.uhc_drug_operation import (
    UHCDrugPublicationOperationResult,
)
from process.formulary_fhir.uhc_drug_operation import uhc_operation_error
from process.formulary_fhir.uhc_drug_operation import receipt_operation_evidence
from process.formulary_fhir.uhc_drug_receipt import (
    load_uhc_drug_admission_receipt,
)
from process.formulary_fhir.uhc_drug_receipt import validate_uhc_drug_receipt_id
from process.formulary_fhir.uhc_drug_release import (
    publish_admitted_uhc_drug_candidate,
)


async def publish_uhc_drug_receipt(
    *,
    receipt_id: str,
    database: Any = db,
) -> UHCDrugPublicationOperationResult:
    """Publish one durable receipt under the separate publication gate."""

    require_uhc_publication_gate()
    try:
        normalized_receipt_id = validate_uhc_drug_receipt_id(receipt_id)
        receipt = await load_uhc_drug_admission_receipt(
            receipt_id=normalized_receipt_id,
            database=database,
        )
        publication = await publish_admitted_uhc_drug_candidate(
            receipt_id=normalized_receipt_id,
            database=database,
        )
        try:
            receipt_evidence = receipt_operation_evidence(receipt)
        except (TypeError, ValueError):
            raise UHCDrugOperationError("evidence") from None
        if publication.dataset_id != receipt_evidence.candidate_dataset_id:
            raise UHCDrugOperationError("evidence")
        return UHCDrugPublicationOperationResult(
            evidence=receipt_evidence,
            generation=publication.generation,
            published_at=publication.published_at,
        )
    except (asyncio.CancelledError, TimeoutError):
        raise
    except UHCDrugOperationError:
        raise
    except (TypeError, ValueError):
        raise UHCDrugOperationError("invalid_request") from None
    except Exception as error:
        raise uhc_operation_error(error, "publication") from None


__all__ = ("publish_uhc_drug_receipt",)
