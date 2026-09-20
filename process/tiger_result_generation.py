# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit publisher-owned TIGER replacement, including reviewed static bootstrap."""

from dataclasses import asdict

from process.reference_family_archive import (
    ReferenceFamilyArchiveError,
    _bounded_capture,
    _require_transaction,
    activate_validated_reference_family_stage,
)
from process.reference_family_result_generation import publish_local_reference_family_generation


async def publish_tiger_generation(
    session,
    *,
    ownership,
    manifest,
    expected_incumbent,
    validation_receipt,
    cutover,
):
    """Real-swap a validated TIGER stage and establish a new local publication.

    The caller supplies trusted publisher validation and owns the transaction.
    Existing static data can be explicitly cloned, validated, and republished by
    this path; no historical generation or source lineage is inferred from it.
    Table/index/sequence DDL still requires the protected relation owner, not the
    application role that owns the generation ledger.
    """

    _require_transaction(session)
    if (
        ownership.importer_id != "tiger"
        or expected_incumbent.importer_id != "tiger"
        or expected_incumbent.schema_name != "tiger"
        or cutover.authority != "manual"
        or cutover.source_serving_generation is not None
    ):
        raise ReferenceFamilyArchiveError("TIGER local publication scope differs")
    async with _bounded_capture(session):
        receipt = await activate_validated_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=expected_incumbent,
            validation_receipt=validation_receipt,
            cutover=cutover,
        )
        authority = await publish_local_reference_family_generation(session, importer_id="tiger", schema_name="tiger")
    return {
        "contract": "tiger-local-publication.v1",
        "package_id": cutover.package_id,
        "dataset_id": str(ownership.dataset_id),
        "activation": asdict(receipt),
        "generation": authority.as_dict(),
    }
