# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic pointer publication for verified FHIR formulary generations."""

from __future__ import annotations

from typing import Any

from db.models import db
from process.formulary_fhir.repository_shared import SOURCE_ID
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name


async def _locked_publication_dataset(dataset_id: str) -> dict[str, Any]:
    dataset_row = await db.first(
        f"SELECT source_id, status, publish_requested, seed_eligible FROM "
        f"{table_name('fhir_formulary_dataset')} "
        "WHERE dataset_id = :dataset_id FOR UPDATE;",
        dataset_id=dataset_id,
    )
    return row_mapping(dataset_row)


async def _next_generation(source_id: str, *, require_empty: bool) -> int:
    current_row = await db.first(
        f"SELECT dataset_id, generation FROM "
        f"{table_name('fhir_formulary_current')} "
        "WHERE source_id = :source_id FOR UPDATE;",
        source_id=source_id,
    )
    current_by_field = row_mapping(current_row)
    if require_empty and current_by_field.get("dataset_id"):
        raise RuntimeError("FHIR formulary seed proof requires an empty pointer")
    return int(current_by_field.get("generation") or 0) + 1


async def _switch_publication_pointer(
    *,
    dataset_id: str,
    source_id: str,
    generation: int,
) -> None:
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_current')} ("
        "source_id, dataset_id, generation, published_at) VALUES ("
        ":source_id, :dataset_id, :generation, transaction_timestamp()) "
        "ON CONFLICT (source_id) DO UPDATE SET "
        "dataset_id = EXCLUDED.dataset_id, "
        "generation = EXCLUDED.generation, "
        "published_at = EXCLUDED.published_at;",
        source_id=source_id,
        dataset_id=dataset_id,
        generation=generation,
    )
    update_count = await db.status(
        f"UPDATE {table_name('fhir_formulary_dataset')} SET "
        "status = 'published', published_at = transaction_timestamp() "
        "WHERE dataset_id = :dataset_id AND status = 'verified';",
        dataset_id=dataset_id,
    )
    if update_count != 1:
        raise RuntimeError("FHIR formulary publication transition failed")


async def _publish_locked(dataset_id: str, *, seed_proof: bool) -> int:
    dataset_by_field = await _locked_publication_dataset(dataset_id)
    is_verified = dataset_by_field.get("status") == "verified"
    is_requested = bool(dataset_by_field.get("publish_requested"))
    is_seed_eligible = bool(dataset_by_field.get("seed_eligible"))
    is_publishable = (
        is_verified and is_seed_eligible and not is_requested
        if seed_proof
        else is_verified and is_requested and not is_seed_eligible
    )
    if not is_publishable or dataset_by_field.get("source_id") != SOURCE_ID:
        raise RuntimeError("FHIR formulary dataset is not publishable")
    generation = await _next_generation(
        SOURCE_ID,
        require_empty=seed_proof,
    )
    await _switch_publication_pointer(
        dataset_id=dataset_id,
        source_id=SOURCE_ID,
        generation=generation,
    )
    return generation


class FHIRFormularyPublicationMixin:
    """Publish ordinary generations or one explicitly eligible initial seed."""

    async def publish_dataset(self, dataset_id: str) -> int:
        """Atomically switch one source pointer after verification succeeds."""

        async with db.transaction():
            return await _publish_locked(dataset_id, seed_proof=False)

    async def publish_verified_seed(self, dataset_id: str) -> int:
        """Promote one verified manual seed without reacquiring FHIR pages."""

        async with db.transaction():
            return await _publish_locked(dataset_id, seed_proof=True)
