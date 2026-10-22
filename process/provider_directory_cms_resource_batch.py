# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mandatory native COPY for CMS normalized resource batches.

Semantic projection, union, proof construction, and existing database guards
remain shared with the FHIR loader. COPY failures propagate to the enclosing
source/witness transaction; this path never falls back to VALUES inserts.
"""

from typing import Any


async def persist_cms_dataset_rows(
    fhir: Any, session: Any, model: type, resource_rows: list[dict[str, Any]], candidate: Any
) -> list[dict[str, Any]]:
    """Append bounded normalized observations to the exact isolated candidate."""
    from process.provider_directory_generic_source_policy import bound_publisher
    from process.provider_directory_generic_source_storage import append_generic_source_batch

    bound_publisher()
    if fhir._bound_upsert_session() is not session:
        raise RuntimeError("cms_npd_resource_batch_transaction_missing")
    if not resource_rows or len(resource_rows) > 4_096:
        raise RuntimeError("cms_npd_resource_batch_size_invalid")
    rows = fhir._endpoint_dataset_resource_rows(
        model, resource_rows, dataset_id=candidate.dataset_id, resource_hash_contract=candidate.resource_hash_contract
    )
    fhir._assert_practitioner_semantic_projection_as_of(
        model, resource_rows, candidate.resource_hash_contract, candidate.semantic_projection_as_of
    )
    await append_generic_source_batch(fhir.db, fhir, candidate, rows)
    await fhir.persist_dataset_proof_shard(
        fhir.db,
        fhir._schema(),
        rows,
        dataset_id=candidate.dataset_id,
        expected_resource_hash_contract=candidate.resource_hash_contract,
    )
    return rows
