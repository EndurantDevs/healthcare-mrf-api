# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fenced, resumable alias checkpoint persistence."""

from __future__ import annotations

import datetime as dt
from typing import Any

from db.models import db
from process.formulary_fhir.repository_shared import CheckpointWrite
from process.formulary_fhir.repository_shared import CompletedAliasCheckpoint
from process.formulary_fhir.repository_shared import SOURCE_ID
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name


async def _has_updated_checkpoint(checkpoint: CheckpointWrite) -> bool:
    update_status = await db.status(
        f"UPDATE {table_name('fhir_formulary_checkpoint')} SET "
        "fence_token = :fence_token, acquisition_mode = :acquisition_mode, "
        "next_url = :next_url, expected_count = :expected_count, "
        "processed_count = :processed_count, membership_hash = :membership_hash, "
        "completed = :completed WHERE source_id = :source_id "
        "AND alias_id = :alias_id "
        "AND run_id = :run_id AND fence_token < :fence_token;",
        fence_token=checkpoint.fence_token,
        acquisition_mode=checkpoint.acquisition_mode,
        next_url=checkpoint.next_url,
        expected_count=checkpoint.expected_count,
        processed_count=checkpoint.processed_count,
        membership_hash=checkpoint.membership_hash_value,
        completed=checkpoint.is_completed,
        source_id=SOURCE_ID,
        alias_id=checkpoint.alias_id,
        run_id=checkpoint.run_id,
    )
    return bool(update_status)


async def _has_inserted_checkpoint(checkpoint: CheckpointWrite) -> bool:
    insert_status = await db.status(
        f"INSERT INTO {table_name('fhir_formulary_checkpoint')} ("
        "source_id, alias_id, source_plan_identifier, run_id, dataset_id, "
        "fence_token, cutoff_at, acquisition_mode, next_url, expected_count, "
        "processed_count, membership_hash, completed) VALUES ("
        ":source_id, :alias_id, :source_plan_identifier, :run_id, :dataset_id, "
        ":fence_token, :cutoff_at, :acquisition_mode, :next_url, "
        ":expected_count, :processed_count, :membership_hash, :completed) "
        "ON CONFLICT DO NOTHING;",
        source_id=SOURCE_ID,
        alias_id=checkpoint.alias_id,
        source_plan_identifier=checkpoint.source_plan_identifier,
        run_id=checkpoint.run_id,
        dataset_id=checkpoint.dataset_id,
        fence_token=checkpoint.fence_token,
        cutoff_at=checkpoint.cutoff_at,
        acquisition_mode=checkpoint.acquisition_mode,
        next_url=checkpoint.next_url,
        expected_count=checkpoint.expected_count,
        processed_count=checkpoint.processed_count,
        membership_hash=checkpoint.membership_hash_value,
        completed=checkpoint.is_completed,
    )
    return bool(insert_status)


def _completed_checkpoint(
    checkpoint_by_field: dict[str, Any],
) -> CompletedAliasCheckpoint:
    if checkpoint_by_field.get("expected_count") is None:
        raise RuntimeError("FHIR formulary completed checkpoint has no count")
    expected_count = int(checkpoint_by_field["expected_count"])
    is_consistent = bool(
        checkpoint_by_field.get("alias_version_id")
        and checkpoint_by_field.get("membership_hash")
        and checkpoint_by_field.get("membership_hash")
        == checkpoint_by_field.get("alias_membership_hash")
        and int(checkpoint_by_field.get("processed_count") or 0)
        == expected_count
        and int(checkpoint_by_field.get("membership_count") or 0)
        == expected_count
        and checkpoint_by_field.get("acquisition_mode")
        in {"reuse", "delta", "full"}
    )
    if not is_consistent:
        raise RuntimeError("FHIR formulary completed checkpoint is inconsistent")
    return CompletedAliasCheckpoint(
        alias_version_id=checkpoint_by_field["alias_version_id"],
        expected_count=expected_count,
        membership_hash=checkpoint_by_field["membership_hash"],
        acquisition_mode=checkpoint_by_field["acquisition_mode"],
    )


class FHIRFormularyCheckpointMixin:
    """Persist and validate fenced resumable alias checkpoints."""

    async def save_checkpoint(self, checkpoint: CheckpointWrite) -> None:
        """Advance a checkpoint only when its fencing token is newer."""

        if await _has_updated_checkpoint(checkpoint):
            return
        if not await _has_inserted_checkpoint(checkpoint):
            raise RuntimeError("FHIR formulary checkpoint fence was rejected")

    async def completed_alias_checkpoint(
        self,
        *,
        dataset_id: str,
        run_id: str,
        alias_id: str,
        source_plan_identifier: str,
        cutoff_at: dt.datetime,
    ) -> CompletedAliasCheckpoint | None:
        """Return one fully proved candidate alias when resuming the same run."""

        checkpoint_row = await db.first(
            f"SELECT c.expected_count, c.processed_count, c.membership_hash, "
            "c.acquisition_mode, da.alias_version_id, av.membership_count, "
            "av.membership_hash AS alias_membership_hash "
            f"FROM {table_name('fhir_formulary_checkpoint')} c "
            f"LEFT JOIN {table_name('fhir_formulary_dataset_alias')} da "
            "ON da.dataset_id = c.dataset_id AND da.alias_id = c.alias_id "
            f"LEFT JOIN {table_name('fhir_formulary_drug_plan_alias_version')} av "
            "ON av.alias_version_id = da.alias_version_id "
            "WHERE c.source_id = :source_id AND c.dataset_id = :dataset_id "
            "AND c.run_id = :run_id AND c.alias_id = :alias_id "
            "AND c.source_plan_identifier = :source_plan_identifier "
            "AND c.cutoff_at = :cutoff_at AND c.completed IS TRUE;",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            run_id=run_id,
            alias_id=alias_id,
            source_plan_identifier=source_plan_identifier,
            cutoff_at=cutoff_at,
        )
        if checkpoint_row is None:
            return None
        return _completed_checkpoint(row_mapping(checkpoint_row))
