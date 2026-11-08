# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mandatory native COPY for CMS normalized resource batches.

Semantic projection, union, proof construction, and existing database guards
remain shared with the FHIR loader. Row building and proof framing are pure so
staging worker processes can run them; the publisher process only copies. COPY
failures propagate to the enclosing source/witness transaction; this path never
falls back to VALUES inserts.
"""

from typing import Any

from process import provider_directory_generic_source_storage as storage
from process.provider_directory_proof_store import build_dataset_proof_shard, persist_built_dataset_proof_shard


def prepare_cms_dataset_rows(
    fhir: Any, model: type, resource_rows: list[dict[str, Any]], candidate: Any
) -> tuple[list[dict[str, Any]], tuple[dict[str, Any], bytes]]:
    """Build the exact retained rows, their batch proof and canonical payload text without a database."""
    if not resource_rows or len(resource_rows) > 4_096:
        raise RuntimeError("cms_npd_resource_batch_size_invalid")
    rows = fhir._endpoint_dataset_resource_rows(
        model, resource_rows, dataset_id=candidate.dataset_id, resource_hash_contract=candidate.resource_hash_contract
    )
    fhir._assert_practitioner_semantic_projection_as_of(
        model, resource_rows, candidate.resource_hash_contract, candidate.semantic_projection_as_of
    )
    proof = build_dataset_proof_shard(
        rows,
        dataset_id=candidate.dataset_id,
        endpoint_id=candidate.endpoint_id,
        acquisition_root_run_id=candidate.acquisition_root_run_id,
        source_ids=candidate.source_ids,
        resource_hash_contract=candidate.resource_hash_contract,
    )
    return [{**row, "payload_json": storage._encoded(row["payload_json"])} for row in rows], proof


async def persist_prepared_cms_dataset_rows(
    fhir: Any, session: Any, rows: list[dict[str, Any]], proof: tuple[dict[str, Any], bytes], candidate: Any
) -> None:
    """Append prepared rows and their proof to the exact isolated candidate."""
    from process.provider_directory_generic_source_policy import bound_publisher
    from process.provider_directory_generic_source_storage import append_generic_source_batch

    bound_publisher()
    if fhir._bound_upsert_session() is not session:
        raise RuntimeError("cms_npd_resource_batch_transaction_missing")
    await append_generic_source_batch(fhir.db, fhir, candidate, rows)
    descriptor_by_field, compressed = proof
    await persist_built_dataset_proof_shard(
        fhir.db,
        fhir._schema(),
        descriptor_by_field,
        compressed,
        dataset_id=candidate.dataset_id,
        expected_resource_hash_contract=candidate.resource_hash_contract,
    )
