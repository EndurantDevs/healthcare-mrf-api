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
    return prepare_dataset_rows(fhir, model, resource_rows, candidate)


def prepare_dataset_rows(
    fhir: Any, model: type, resource_rows: list[dict[str, Any]], candidate: Any
) -> tuple[list[dict[str, Any]], tuple[dict[str, Any], bytes]]:
    """Prepare one caller-bounded batch with the shared canonical and proof contracts."""
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


def _validate_prepared_identity(
    fhir, model, resource_rows, dataset_id, resource_hash_contract, semantic_projection_as_of
):
    """Bind prepared payloads to the writer's candidate and normalized resource family."""
    identity = resource_rows.identity
    if (identity["dataset_id"], identity["resource_hash_contract"], identity["semantic_projection_as_of"]) != (
        dataset_id,
        resource_hash_contract,
        semantic_projection_as_of,
    ) or fhir.RESOURCE_MODELS_BY_TYPE[resource_rows.resource_type] is not model:
        raise ValueError("bulk_preparation_identity_mismatch")


async def persist_dataset_rows(
    fhir, model, resource_rows, dataset_id, *, resource_hash_contract, semantic_projection_as_of
):
    """Append observations and their proof within the original publisher transaction."""
    from process.provider_directory_bulk_parallel import PreparedRows
    from process.provider_directory_generic_source_adapters import persist_source_rows, require_source_transaction
    from process.provider_directory_generic_source_policy import source_transaction

    async with source_transaction(fhir.db):
        require_source_transaction()
        is_prepared = isinstance(resource_rows, PreparedRows)
        if is_prepared:
            _validate_prepared_identity(
                fhir, model, resource_rows, dataset_id, resource_hash_contract, semantic_projection_as_of
            )
            prepared_rows = resource_rows.dataset_rows
        else:
            prepared_rows = fhir._endpoint_dataset_resource_rows(
                model, resource_rows, dataset_id=dataset_id, resource_hash_contract=resource_hash_contract
            )
        if not prepared_rows:
            return prepared_rows
        fhir._assert_practitioner_semantic_projection_as_of(
            model, resource_rows, resource_hash_contract, semantic_projection_as_of
        )
        await persist_source_rows(
            fhir,
            prepared_rows,
            resource_hash_contract=resource_hash_contract,
            semantic_projection_as_of=semantic_projection_as_of,
        )
        if is_prepared:
            await persist_built_dataset_proof_shard(
                fhir.db,
                fhir._schema(),
                *resource_rows.proof,
                dataset_id=dataset_id,
                expected_resource_hash_contract=resource_hash_contract,
            )
        else:
            await fhir.persist_dataset_proof_shard(
                fhir.db,
                fhir._schema(),
                prepared_rows,
                dataset_id=dataset_id,
                expected_resource_hash_contract=resource_hash_contract,
            )
        return prepared_rows
