# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-first atomic publication of verified formulary datasets."""

from __future__ import annotations

import datetime as dt
from typing import Any

from process.formulary_fhir.repository_admission import (
    verify_twin_admission_for_publication,
)
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.repository_shared import lock_dataset
from process.formulary_fhir.repository_shared import lock_source
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name


async def _locked_current(database: Any, source_id: str) -> dict[str, Any]:
    current_row = await database.first(
        f"SELECT current.dataset_id, current.generation, current.published_at, "
        f"dataset.cutoff_at FROM {table_name('fhir_formulary_current')} "
        f"AS current JOIN {table_name('fhir_formulary_dataset')} AS dataset "
        "ON dataset.source_id = current.source_id "
        "AND dataset.dataset_id = current.dataset_id "
        "WHERE current.source_id = :source_id FOR UPDATE OF current;",
        source_id=source_id,
    )
    return row_mapping(current_row)


def _idempotent_result(
    source_id: str,
    dataset: DatasetRef,
    dataset_by_field: dict[str, Any],
    current_by_field: dict[str, Any],
) -> PublicationResult | None:
    if dataset_by_field.get("status") != "published":
        return None
    if current_by_field.get("dataset_id") != dataset.dataset_id:
        raise RuntimeError("FHIR formulary published dataset is not current")
    generation = current_by_field.get("generation")
    published_at = current_by_field.get("published_at")
    if type(generation) is not int or type(published_at) is not dt.datetime:
        raise RuntimeError("FHIR formulary current pointer is invalid")
    return PublicationResult(
        source_id,
        dataset.dataset_id,
        generation,
        published_at,
    )


def _validate_publication_policy(
    dataset: DatasetRef,
    dataset_by_field: dict[str, Any],
    current_by_field: dict[str, Any],
    *,
    seed_proof: bool,
) -> None:
    if dataset_by_field.get("status") != "verified":
        raise RuntimeError("FHIR formulary dataset is not verified")
    expected_intent = "seed" if seed_proof else "requested"
    if dataset.intent != expected_intent:
        raise RuntimeError("FHIR formulary dataset is not publishable")
    current_dataset_id = current_by_field.get("dataset_id")
    if seed_proof and (
        current_dataset_id is not None or dataset.previous_dataset_id is not None
    ):
        raise RuntimeError("FHIR formulary seed publication requires no pointer")
    if current_dataset_id != dataset.previous_dataset_id:
        raise RuntimeError("FHIR formulary publication predecessor is stale")
    current_cutoff = current_by_field.get("cutoff_at")
    if current_cutoff is not None and dataset.cutoff_at < current_cutoff:
        raise RuntimeError("FHIR formulary candidate cutoff is stale")


async def _insert_initial_pointer(
    database: Any,
    source_id: str,
    dataset_id: str,
) -> dict[str, Any]:
    pointer_row = await database.first(
        f"INSERT INTO {table_name('fhir_formulary_current')} ("
        "source_id, dataset_id, generation, published_at) VALUES ("
        ":source_id, :dataset_id, 1, transaction_timestamp()) "
        "RETURNING dataset_id, generation, published_at;",
        source_id=source_id,
        dataset_id=dataset_id,
    )
    return row_mapping(pointer_row)


async def _advance_pointer(
    database: Any,
    source_id: str,
    dataset_id: str,
    current_by_field: dict[str, Any],
) -> dict[str, Any]:
    current_dataset_id = current_by_field["dataset_id"]
    current_generation = current_by_field["generation"]
    pointer_row = await database.first(
        f"UPDATE {table_name('fhir_formulary_current')} SET "
        "dataset_id = :dataset_id, generation = :next_generation, "
        "published_at = transaction_timestamp() WHERE source_id = :source_id "
        "AND dataset_id = :current_dataset_id "
        "AND generation = :current_generation "
        "RETURNING dataset_id, generation, published_at;",
        source_id=source_id,
        dataset_id=dataset_id,
        next_generation=current_generation + 1,
        current_dataset_id=current_dataset_id,
        current_generation=current_generation,
    )
    pointer_by_field = row_mapping(pointer_row)
    if pointer_by_field.get("dataset_id") != dataset_id:
        raise RuntimeError("FHIR formulary pointer compare-and-switch failed")
    return pointer_by_field


async def _switch_pointer(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    current_by_field: dict[str, Any],
) -> dict[str, Any]:
    if current_by_field:
        return await _advance_pointer(
            database,
            source_id,
            dataset.dataset_id,
            current_by_field,
        )
    return await _insert_initial_pointer(database, source_id, dataset.dataset_id)


async def _mark_published(
    database: Any,
    source_id: str,
    dataset_id: str,
    published_at: dt.datetime,
) -> None:
    updated_count = await database.status(
        f"UPDATE {table_name('fhir_formulary_dataset')} SET "
        "status = 'published', published_at = :published_at "
        "WHERE source_id = :source_id AND dataset_id = :dataset_id "
        "AND status = 'verified';",
        source_id=source_id,
        dataset_id=dataset_id,
        published_at=published_at,
    )
    if updated_count != 1:
        raise RuntimeError("FHIR formulary publication transition failed")


async def _locked_publication_dataset(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    *,
    seed_proof: bool,
) -> dict[str, Any]:
    if seed_proof:
        return await lock_dataset(
            database,
            source_id,
            dataset,
            allowed_statuses={"verified", "published"},
        )
    _admission, dataset_by_field = await verify_twin_admission_for_publication(
        database,
        source_id,
        dataset,
    )
    return dataset_by_field


async def _publish(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    *,
    seed_proof: bool,
) -> PublicationResult:
    async with database.transaction():
        await lock_source(database, source_id)
        dataset_by_field = await _locked_publication_dataset(
            database,
            source_id,
            dataset,
            seed_proof=seed_proof,
        )
        current_by_field = await _locked_current(database, source_id)
        existing_result = _idempotent_result(
            source_id,
            dataset,
            dataset_by_field,
            current_by_field,
        )
        if existing_result is not None:
            return existing_result
        _validate_publication_policy(
            dataset,
            dataset_by_field,
            current_by_field,
            seed_proof=seed_proof,
        )
        pointer_by_field = await _switch_pointer(
            database,
            source_id,
            dataset,
            current_by_field,
        )
        published_at = pointer_by_field.get("published_at")
        generation = pointer_by_field.get("generation")
        if type(published_at) is not dt.datetime or type(generation) is not int:
            raise RuntimeError("FHIR formulary publication result is invalid")
        await _mark_published(
            database,
            source_id,
            dataset.dataset_id,
            published_at,
        )
        return PublicationResult(
            source_id,
            dataset.dataset_id,
            generation,
            published_at,
        )


class FHIRFormularyPublicationMixin:
    """Publish ordinary generations or one explicitly eligible initial seed."""

    _database: Any
    source_id: str

    async def publish_dataset(self, *, dataset: DatasetRef) -> PublicationResult:
        """Atomically publish one verified requested generation."""

        return await _publish(
            self._database,
            self.source_id,
            dataset,
            seed_proof=False,
        )

    async def publish_verified_seed(
        self,
        *,
        dataset: DatasetRef,
    ) -> PublicationResult:
        """Atomically publish one verified seed into an empty pointer."""

        return await _publish(
            self._database,
            self.source_id,
            dataset,
            seed_proof=True,
        )


__all__ = ("FHIRFormularyPublicationMixin",)
