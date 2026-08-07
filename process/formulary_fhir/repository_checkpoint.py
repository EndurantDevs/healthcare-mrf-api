# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fenced alias progress and exact completed-restart persistence."""

from __future__ import annotations

from typing import Any

from process.formulary_fhir.repository_shared import AliasRef
from process.formulary_fhir.repository_shared import CheckpointWrite
from process.formulary_fhir.repository_shared import CompletedAliasCheckpoint
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import lock_dataset
from process.formulary_fhir.repository_shared import persisted_membership_proof
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name


async def require_alias(
    database: Any,
    source_id: str,
    alias: AliasRef,
) -> dict[str, Any]:
    """Prove one alias is exactly owned by the configured source."""

    if alias.source_id != source_id:
        raise RuntimeError("FHIR formulary alias source is inconsistent")
    alias_row = await database.first(
        f"SELECT source_id, public_id, alias_id, source_plan_identifier FROM "
        f"{table_name('fhir_formulary_drug_plan_alias')} "
        "WHERE source_id = :source_id AND alias_id = :alias_id "
        "AND public_id = :public_id "
        "AND source_plan_identifier = :source_plan_identifier;",
        source_id=source_id,
        alias_id=alias.alias_id,
        public_id=alias.public_id,
        source_plan_identifier=alias.source_plan_identifier,
    )
    alias_by_field = row_mapping(alias_row)
    if alias_by_field.get("alias_id") != alias.alias_id:
        raise RuntimeError("FHIR formulary alias ownership is invalid")
    return alias_by_field


def _checkpoint_params(
    source_id: str,
    checkpoint: CheckpointWrite,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "alias_id": checkpoint.alias.alias_id,
        "source_plan_identifier": checkpoint.alias.source_plan_identifier,
        "run_id": checkpoint.dataset.run_id,
        "dataset_id": checkpoint.dataset.dataset_id,
        "fence_token": checkpoint.fence_token,
        "cutoff_at": checkpoint.dataset.cutoff_at,
        "acquisition_mode": checkpoint.acquisition_mode,
        "expected_count": checkpoint.expected_count,
        "processed_count": checkpoint.processed_count,
        "membership_hash": checkpoint.membership_hash,
        "completed": checkpoint.completed,
    }


async def _is_checkpoint_updated(
    database: Any,
    params_by_name: dict[str, Any],
) -> bool:
    updated_count = await database.status(
        f"UPDATE {table_name('fhir_formulary_checkpoint')} SET "
        "fence_token = :fence_token, expected_count = :expected_count, "
        "processed_count = :processed_count, membership_hash = :membership_hash, "
        "completed = :completed WHERE source_id = :source_id "
        "AND alias_id = :alias_id AND run_id = :run_id "
        "AND dataset_id = :dataset_id "
        "AND source_plan_identifier = :source_plan_identifier "
        "AND cutoff_at = :cutoff_at "
        "AND acquisition_mode = :acquisition_mode "
        "AND completed IS FALSE AND fence_token < :fence_token;",
        **params_by_name,
    )
    return updated_count == 1


async def _is_checkpoint_inserted(
    database: Any,
    params_by_name: dict[str, Any],
) -> bool:
    inserted_count = await database.status(
        f"INSERT INTO {table_name('fhir_formulary_checkpoint')} ("
        "source_id, alias_id, source_plan_identifier, run_id, dataset_id, "
        "fence_token, cutoff_at, acquisition_mode, expected_count, "
        "processed_count, membership_hash, completed) VALUES ("
        ":source_id, :alias_id, :source_plan_identifier, :run_id, :dataset_id, "
        ":fence_token, :cutoff_at, :acquisition_mode, :expected_count, "
        ":processed_count, :membership_hash, :completed) "
        "ON CONFLICT DO NOTHING;",
        **params_by_name,
    )
    return inserted_count == 1


async def _assert_checkpoint_write(
    database: Any,
    params_by_name: dict[str, Any],
) -> None:
    checkpoint_row = await database.first(
        f"SELECT source_id, alias_id, source_plan_identifier, run_id, "
        "dataset_id, fence_token, cutoff_at, acquisition_mode, "
        "expected_count, processed_count, membership_hash, completed FROM "
        f"{table_name('fhir_formulary_checkpoint')} "
        "WHERE source_id = :source_id AND alias_id = :alias_id "
        "AND run_id = :run_id;",
        source_id=params_by_name["source_id"],
        alias_id=params_by_name["alias_id"],
        run_id=params_by_name["run_id"],
    )
    checkpoint_by_field = row_mapping(checkpoint_row)
    if any(
        checkpoint_by_field.get(field_name) != expected_value
        for field_name, expected_value in params_by_name.items()
    ):
        raise RuntimeError("FHIR formulary checkpoint write is inconsistent")


async def save_checkpoint_row(
    database: Any,
    source_id: str,
    checkpoint: CheckpointWrite,
) -> None:
    """Insert or advance one exact checkpoint with a newer fence."""

    params_by_name = _checkpoint_params(source_id, checkpoint)
    has_written = await _is_checkpoint_updated(database, params_by_name)
    if not has_written:
        has_written = await _is_checkpoint_inserted(database, params_by_name)
    if not has_written:
        raise RuntimeError("FHIR formulary checkpoint fence was rejected")
    await _assert_checkpoint_write(database, params_by_name)


def _validated_completed_row(
    source_id: str,
    dataset: DatasetRef,
    alias: AliasRef,
    checkpoint_by_field: dict[str, Any],
) -> CompletedAliasCheckpoint:
    expected_count = checkpoint_by_field.get("expected_count")
    processed_count = checkpoint_by_field.get("processed_count")
    membership_count = checkpoint_by_field.get("membership_count")
    membership_hash = checkpoint_by_field.get("membership_hash")
    alias_membership_hash = checkpoint_by_field.get("alias_membership_hash")
    is_consistent = bool(
        type(expected_count) is int
        and expected_count >= 0
        and processed_count == expected_count
        and membership_count == expected_count
        and membership_hash == alias_membership_hash
        and checkpoint_by_field.get("alias_version_id")
        and checkpoint_by_field.get("acquisition_mode") in {"full", "reuse"}
    )
    if not is_consistent:
        raise RuntimeError("FHIR formulary completed checkpoint is inconsistent")
    return CompletedAliasCheckpoint(
        source_id=source_id,
        dataset_id=dataset.dataset_id,
        alias_id=alias.alias_id,
        alias_version_id=str(checkpoint_by_field["alias_version_id"]),
        expected_count=expected_count,
        membership_hash=str(membership_hash),
        acquisition_mode=checkpoint_by_field["acquisition_mode"],
    )


async def _completed_checkpoint_row(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    alias: AliasRef,
) -> Any:
    return await database.first(
        f"SELECT checkpoint.expected_count, checkpoint.processed_count, "
        "checkpoint.membership_hash, checkpoint.acquisition_mode, "
        "dataset_alias.alias_version_id, alias_version.membership_count, "
        "alias_version.membership_hash AS alias_membership_hash FROM "
        f"{table_name('fhir_formulary_checkpoint')} AS checkpoint JOIN "
        f"{table_name('fhir_formulary_dataset')} AS dataset "
        "ON dataset.source_id = checkpoint.source_id "
        "AND dataset.dataset_id = checkpoint.dataset_id "
        "AND dataset.run_id = checkpoint.run_id JOIN "
        f"{table_name('fhir_formulary_drug_plan_alias')} AS plan_alias "
        "ON plan_alias.source_id = checkpoint.source_id "
        "AND plan_alias.alias_id = checkpoint.alias_id "
        "AND plan_alias.source_plan_identifier = "
        "checkpoint.source_plan_identifier JOIN "
        f"{table_name('fhir_formulary_dataset_alias')} AS dataset_alias "
        "ON dataset_alias.source_id = checkpoint.source_id "
        "AND dataset_alias.dataset_id = checkpoint.dataset_id "
        "AND dataset_alias.alias_id = checkpoint.alias_id JOIN "
        f"{table_name('fhir_formulary_drug_plan_alias_version')} "
        "AS alias_version ON alias_version.source_id = checkpoint.source_id "
        "AND alias_version.alias_id = checkpoint.alias_id "
        "AND alias_version.alias_version_id = dataset_alias.alias_version_id "
        "WHERE checkpoint.source_id = :source_id "
        "AND checkpoint.dataset_id = :dataset_id "
        "AND checkpoint.run_id = :run_id "
        "AND checkpoint.alias_id = :alias_id "
        "AND checkpoint.source_plan_identifier = :source_plan_identifier "
        "AND plan_alias.public_id = :public_id "
        "AND checkpoint.cutoff_at = :cutoff_at "
        "AND dataset.cutoff_at = :cutoff_at "
        "AND checkpoint.completed IS TRUE;",
        source_id=source_id,
        dataset_id=dataset.dataset_id,
        run_id=dataset.run_id,
        alias_id=alias.alias_id,
        source_plan_identifier=alias.source_plan_identifier,
        public_id=alias.public_id,
        cutoff_at=dataset.cutoff_at,
    )


async def completed_checkpoint(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    alias: AliasRef,
) -> CompletedAliasCheckpoint | None:
    """Return a completed alias only after exact graph validation."""

    checkpoint_row = await _completed_checkpoint_row(
        database,
        source_id,
        dataset,
        alias,
    )
    if checkpoint_row is None:
        return None
    completed_alias = _validated_completed_row(
        source_id,
        dataset,
        alias,
        row_mapping(checkpoint_row),
    )
    persisted_count, persisted_hash, _variants = await persisted_membership_proof(
        database,
        source_id,
        completed_alias.alias_version_id,
    )
    if (persisted_count, persisted_hash) != (
        completed_alias.expected_count,
        completed_alias.membership_hash,
    ):
        raise RuntimeError("FHIR formulary completed membership is inconsistent")
    return completed_alias


class FHIRFormularyCheckpointMixin:
    """Expose progress writes and exact completed-alias restart checks."""

    _database: Any
    source_id: str

    async def save_checkpoint(self, checkpoint: CheckpointWrite) -> None:
        """Persist incomplete progress evidence for a page-one retry."""

        if checkpoint.completed or checkpoint.acquisition_mode != "full":
            raise ValueError("FHIR formulary progress checkpoint is invalid")
        async with self._database.transaction():
            await lock_dataset(
                self._database,
                self.source_id,
                checkpoint.dataset,
                allowed_statuses={"building"},
            )
            await require_alias(self._database, self.source_id, checkpoint.alias)
            await save_checkpoint_row(
                self._database,
                self.source_id,
                checkpoint,
            )

    async def completed_alias_checkpoint(
        self,
        *,
        dataset: DatasetRef,
        alias: AliasRef,
    ) -> CompletedAliasCheckpoint | None:
        """Return an exact completed alias; incomplete state returns nothing."""

        if dataset.source_id != self.source_id or alias.source_id != self.source_id:
            raise RuntimeError("FHIR formulary restart source is inconsistent")
        return await completed_checkpoint(
            self._database,
            self.source_id,
            dataset,
            alias,
        )


__all__ = ("FHIRFormularyCheckpointMixin",)
