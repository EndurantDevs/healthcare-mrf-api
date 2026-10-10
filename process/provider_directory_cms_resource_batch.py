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
    """Write one fenced resource family using the existing semantic contract."""
    if fhir._bound_upsert_session() is not session:
        raise RuntimeError("cms_npd_resource_batch_transaction_missing")
    if not resource_rows or len(resource_rows) > 1_000:
        raise RuntimeError("cms_npd_resource_batch_size_invalid")
    incoming_rows = fhir._endpoint_dataset_resource_rows(
        model,
        resource_rows,
        dataset_id=candidate.dataset_id,
        resource_hash_contract=candidate.resource_hash_contract,
    )
    fhir._assert_practitioner_semantic_projection_as_of(
        model,
        resource_rows,
        candidate.resource_hash_contract,
        candidate.semantic_projection_as_of,
    )
    dataset_rows = await fhir._accumulated_endpoint_dataset_rows(
        fhir.db,
        incoming_rows,
        dataset_id=candidate.dataset_id,
        resource_hash_contract=candidate.resource_hash_contract,
        semantic_projection_as_of=candidate.semantic_projection_as_of,
    )
    await fhir._lock_mutable_endpoint_dataset_resource_parents(fhir.db, [candidate.dataset_id])
    table = fhir.ProviderDirectoryDatasetResource.__table__
    await fhir._copy_upsert_rows(
        fhir.ProviderDirectoryDatasetResource,
        dataset_rows,
        [column.name for column in table.columns],
        [column.name for column in table.primary_key.columns],
        skip_unchanged=False,
        transaction_session=session,
    )
    await fhir.persist_dataset_proof_shard(
        fhir.db,
        fhir._schema(),
        incoming_rows,
        dataset_id=candidate.dataset_id,
        expected_resource_hash_contract=candidate.resource_hash_contract,
    )
    return dataset_rows
