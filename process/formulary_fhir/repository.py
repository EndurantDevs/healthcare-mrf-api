# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL copy-on-write repository and atomic formulary publisher."""

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass, field
from typing import Any

from db.models import db
from process.formulary_fhir.repository_checkpoint import (
    FHIRFormularyCheckpointMixin,
)
from process.formulary_fhir.repository_shared import AliasVersionWrite
from process.formulary_fhir.repository_shared import CheckpointWrite
from process.formulary_fhir.repository_shared import CompletedAliasCheckpoint
from process.formulary_fhir.repository_shared import CurrentSnapshot
from process.formulary_fhir.repository_shared import PriorAliasState
from process.formulary_fhir.repository_shared import SOURCE_ID
from process.formulary_fhir.repository_shared import json_object
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_write import FHIRFormularyWriteMixin


async def _current_dataset_id() -> str | None:
    current_row = await db.first(
        f"SELECT dataset_id FROM {table_name('fhir_formulary_current')} "
        "WHERE source_id = :source_id;",
        source_id=SOURCE_ID,
    )
    dataset_id = row_mapping(current_row).get("dataset_id")
    return str(dataset_id) if dataset_id else None


async def _insert_candidate_dataset(
    *,
    dataset_id: str,
    run_id: str,
    previous_dataset_id: str | None,
    cutoff_at: dt.datetime,
    publish_requested: bool,
) -> None:
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_dataset')} ("
        "dataset_id, source_id, run_id, previous_dataset_id, cutoff_at, "
        "status, publish_requested) VALUES ("
        ":dataset_id, :source_id, :run_id, :previous_dataset_id, :cutoff_at, "
        "'building', :publish_requested) ON CONFLICT (run_id) DO NOTHING;",
        dataset_id=dataset_id,
        source_id=SOURCE_ID,
        run_id=run_id,
        previous_dataset_id=previous_dataset_id,
        cutoff_at=cutoff_at,
        publish_requested=publish_requested,
    )


async def _resumed_dataset_by_field(run_id: str) -> dict[str, Any]:
    dataset_row = await db.first(
        f"SELECT dataset_id, cutoff_at, publish_requested, status "
        f"FROM {table_name('fhir_formulary_dataset')} WHERE run_id = :run_id;",
        run_id=run_id,
    )
    return row_mapping(dataset_row)


def _validate_resumed_dataset(
    dataset_by_field: dict[str, Any],
    *,
    dataset_id: str,
    cutoff_at: dt.datetime,
    publish_requested: bool,
) -> None:
    if dataset_by_field.get("dataset_id") != dataset_id:
        raise RuntimeError("FHIR formulary run id collision")
    has_changed_parameters = bool(
        dataset_by_field.get("cutoff_at") != cutoff_at
        or bool(dataset_by_field.get("publish_requested")) != bool(publish_requested)
    )
    if has_changed_parameters:
        raise RuntimeError("FHIR formulary run resume parameters changed")
    if dataset_by_field.get("status") not in {"building", "verified"}:
        raise RuntimeError("FHIR formulary dataset is not resumable")


async def _current_pointer_by_field() -> dict[str, Any]:
    pointer_row = await db.first(
        f"SELECT c.dataset_id, d.cutoff_at "
        f"FROM {table_name('fhir_formulary_current')} c "
        f"JOIN {table_name('fhir_formulary_dataset')} d "
        "ON d.dataset_id = c.dataset_id "
        "WHERE c.source_id = :source_id;",
        source_id=SOURCE_ID,
    )
    return row_mapping(pointer_row)


async def _snapshot_alias_rows(dataset_id: str) -> list[Any]:
    return await db.all(
        f"SELECT a.public_id, a.source_plan_identifier, a.alias_id, "
        "av.alias_version_id, av.expected_count, av.membership_hash, "
        "av.cutoff_at "
        f"FROM {table_name('fhir_formulary_dataset_alias')} da "
        f"JOIN {table_name('fhir_formulary_drug_plan_alias')} a "
        "ON a.alias_id = da.alias_id "
        f"JOIN {table_name('fhir_formulary_drug_plan_alias_version')} av "
        "ON av.alias_version_id = da.alias_version_id "
        "WHERE da.dataset_id = :dataset_id "
        "ORDER BY a.public_id, a.source_plan_identifier;",
        dataset_id=dataset_id,
    )


def _prior_aliases_by_key(
    alias_rows: list[Any],
) -> dict[tuple[str, str], PriorAliasState]:
    aliases_by_key: dict[tuple[str, str], PriorAliasState] = {}
    for alias_row in alias_rows:
        alias_by_field = row_mapping(alias_row)
        alias_key = (
            alias_by_field["public_id"],
            alias_by_field["source_plan_identifier"],
        )
        aliases_by_key[alias_key] = PriorAliasState(
            alias_id=alias_by_field["alias_id"],
            alias_version_id=alias_by_field["alias_version_id"],
            expected_count=int(alias_by_field["expected_count"]),
            cutoff_at=alias_by_field["cutoff_at"],
            variants_by_medication_id={},
            membership_hash_value=alias_by_field["membership_hash"],
        )
    return aliases_by_key


async def _loaded_prior_alias(prior: PriorAliasState) -> PriorAliasState:
    membership_rows = await db.all(
        f"SELECT upstream_medication_id, variant_hash FROM "
        f"{table_name('fhir_formulary_alias_membership')} "
        "WHERE alias_version_id = :alias_version_id "
        "ORDER BY upstream_medication_id;",
        alias_version_id=prior.alias_version_id,
    )
    variants_by_id = {
        row_mapping(row)["upstream_medication_id"]: row_mapping(row)["variant_hash"]
        for row in membership_rows
    }
    if len(variants_by_id) != prior.expected_count:
        raise RuntimeError("FHIR formulary prior membership count is incomplete")
    return PriorAliasState(
        alias_id=prior.alias_id,
        alias_version_id=prior.alias_version_id,
        expected_count=prior.expected_count,
        cutoff_at=prior.cutoff_at,
        variants_by_medication_id=variants_by_id,
        membership_hash_value=prior.membership_hash_value,
    )


async def _coverage_verification_rows(dataset_id: str) -> list[Any]:
    return await db.all(
        f"SELECT cp.public_id, cp.canonical_identity, cpv.content_hash, "
        "cpv.metadata_json, a.alias_id, da.alias_version_id AS "
        "dataset_alias_version_id, a.source_plan_identifier, "
        "av.expected_count, av.membership_count, av.membership_hash FROM "
        f"{table_name('fhir_formulary_dataset_coverage_plan')} dcp "
        f"JOIN {table_name('fhir_formulary_coverage_plan')} cp "
        "ON cp.public_id = dcp.public_id "
        f"JOIN {table_name('fhir_formulary_coverage_plan_version')} cpv "
        "ON cpv.coverage_version_id = dcp.coverage_version_id "
        f"LEFT JOIN {table_name('fhir_formulary_drug_plan_alias')} a "
        "ON a.public_id = cp.public_id "
        f"LEFT JOIN {table_name('fhir_formulary_dataset_alias')} da "
        "ON da.dataset_id = dcp.dataset_id AND da.alias_id = a.alias_id "
        f"LEFT JOIN {table_name('fhir_formulary_drug_plan_alias_version')} av "
        "ON av.alias_version_id = da.alias_version_id "
        "WHERE dcp.dataset_id = :dataset_id ORDER BY cp.public_id, "
        "a.source_plan_identifier;",
        dataset_id=dataset_id,
    )


@dataclass
class _VerificationState:
    coverage_versions_by_public_id: dict[str, str] = field(default_factory=dict)
    expected_aliases_by_plan: dict[str, set[str]] = field(default_factory=dict)
    observed_aliases_by_plan: dict[str, set[str]] = field(default_factory=dict)
    alias_hashes: list[str] = field(default_factory=list)
    alias_ids: set[str] = field(default_factory=set)
    medication_count: int = 0


def _expected_aliases(coverage_by_field: dict[str, Any]) -> set[str]:
    metadata_by_field = json_object(coverage_by_field.get("metadata_json"))
    raw_aliases = metadata_by_field.get("source_plan_identifiers")
    if not isinstance(raw_aliases, list) or not raw_aliases:
        raise RuntimeError("FHIR formulary List-to-alias coverage is incomplete")
    return {str(alias) for alias in raw_aliases if str(alias)}


def _accumulate_alias_membership(
    verification: _VerificationState,
    coverage_by_field: dict[str, Any],
) -> None:
    public_id = coverage_by_field["public_id"]
    if not coverage_by_field.get("dataset_alias_version_id"):
        return
    if (
        not coverage_by_field.get("alias_id")
        or coverage_by_field.get("membership_hash") is None
    ):
        raise RuntimeError("FHIR formulary List-to-alias coverage is incomplete")
    if int(coverage_by_field["expected_count"]) != int(
        coverage_by_field["membership_count"]
    ):
        raise RuntimeError("FHIR formulary alias count proof is incomplete")
    verification.alias_ids.add(coverage_by_field["alias_id"])
    verification.observed_aliases_by_plan[public_id].add(
        coverage_by_field["source_plan_identifier"]
    )
    verification.medication_count += int(coverage_by_field["membership_count"])
    verification.alias_hashes.append(
        f"{coverage_by_field['alias_id']}:" f"{coverage_by_field['membership_hash']}"
    )


def _accumulate_verification(coverage_rows: list[Any]) -> _VerificationState:
    verification = _VerificationState()
    for coverage_row in coverage_rows:
        coverage_by_field = row_mapping(coverage_row)
        public_id = coverage_by_field["public_id"]
        verification.coverage_versions_by_public_id[public_id] = (
            f"{coverage_by_field['canonical_identity']}:"
            f"{coverage_by_field['content_hash']}"
        )
        verification.expected_aliases_by_plan[public_id] = _expected_aliases(
            coverage_by_field
        )
        verification.observed_aliases_by_plan.setdefault(public_id, set())
        _accumulate_alias_membership(verification, coverage_by_field)
    has_complete_coverage = all(
        verification.observed_aliases_by_plan.get(public_id, set()) == expected_aliases
        for public_id, expected_aliases in (
            verification.expected_aliases_by_plan.items()
        )
    )
    if not has_complete_coverage:
        raise RuntimeError("FHIR formulary List-to-alias coverage is incomplete")
    return verification


def _verification_proof(verification: _VerificationState) -> dict[str, Any]:
    coverage_hash = hashlib.sha256(
        "\n".join(sorted(verification.coverage_versions_by_public_id.values())).encode(
            "utf-8"
        )
    ).hexdigest()
    aggregate_membership_hash = hashlib.sha256(
        "\n".join(sorted(verification.alias_hashes)).encode("utf-8")
    ).hexdigest()
    return {
        "list_count": len(verification.coverage_versions_by_public_id),
        "alias_count": len(verification.alias_ids),
        "medication_membership_count": verification.medication_count,
        "coverage_hash": coverage_hash,
        "membership_hash": aggregate_membership_hash,
    }


async def _mark_verified(dataset_id: str, proof_by_field: dict[str, Any]) -> None:
    update_status = await db.status(
        f"UPDATE {table_name('fhir_formulary_dataset')} SET status = 'verified', "
        "list_count = :list_count, alias_count = :alias_count, "
        "medication_count = :medication_count, coverage_hash = :coverage_hash, "
        "membership_hash = :membership_hash, summary_json = "
        "CAST(:summary_json AS jsonb), verified_at = transaction_timestamp() "
        "WHERE dataset_id = :dataset_id AND status IN ('building', 'verified');",
        list_count=proof_by_field["list_count"],
        alias_count=proof_by_field["alias_count"],
        medication_count=proof_by_field["medication_membership_count"],
        coverage_hash=proof_by_field["coverage_hash"],
        membership_hash=proof_by_field["membership_hash"],
        summary_json=json_text(
            {
                "list_count": proof_by_field["list_count"],
                "alias_count": proof_by_field["alias_count"],
                "medication_membership_count": proof_by_field[
                    "medication_membership_count"
                ],
            }
        ),
        dataset_id=dataset_id,
    )
    if not update_status:
        raise RuntimeError("FHIR formulary dataset verification transition failed")


class FHIRFormularyRepository(
    FHIRFormularyWriteMixin,
    FHIRFormularyCheckpointMixin,
):
    """Coordinate copy-on-write persistence and atomic dataset publication."""

    async def begin_dataset(
        self,
        *,
        run_id: str,
        cutoff_at: dt.datetime,
        publish_requested: bool,
    ) -> str:
        """Create or validate a resumable candidate dataset for one run."""

        dataset_id = stable_id("ffd_", SOURCE_ID, run_id)
        await _insert_candidate_dataset(
            dataset_id=dataset_id,
            run_id=run_id,
            previous_dataset_id=await _current_dataset_id(),
            cutoff_at=cutoff_at,
            publish_requested=publish_requested,
        )
        dataset_by_field = await _resumed_dataset_by_field(run_id)
        _validate_resumed_dataset(
            dataset_by_field,
            dataset_id=dataset_id,
            cutoff_at=cutoff_at,
            publish_requested=publish_requested,
        )
        await db.status(
            f"UPDATE {table_name('fhir_formulary_dataset')} SET "
            "error_json = NULL WHERE dataset_id = :dataset_id "
            "AND status IN ('building', 'verified');",
            dataset_id=dataset_id,
        )
        return dataset_id

    async def current_snapshot(self) -> CurrentSnapshot:
        """Load published alias metadata without materializing all memberships."""

        pointer_by_field = await _current_pointer_by_field()
        dataset_id = pointer_by_field.get("dataset_id")
        if not dataset_id:
            return CurrentSnapshot(None, None, {})
        alias_rows = await _snapshot_alias_rows(str(dataset_id))
        return CurrentSnapshot(
            str(dataset_id),
            pointer_by_field.get("cutoff_at"),
            _prior_aliases_by_key(alias_rows),
        )

    async def load_prior_alias_state(
        self,
        prior: PriorAliasState,
    ) -> PriorAliasState:
        """Load one prior alias membership within the bounded worker wave."""

        if prior.variants_by_medication_id:
            return prior
        return await _loaded_prior_alias(prior)

    async def verify_dataset(self, dataset_id: str) -> dict[str, Any]:
        """Prove exact List coverage, alias counts, and deterministic hashes."""

        coverage_rows = await _coverage_verification_rows(dataset_id)
        if not coverage_rows:
            raise RuntimeError("FHIR formulary dataset has no CoveragePlan Lists")
        proof_by_field = _verification_proof(_accumulate_verification(coverage_rows))
        await _mark_verified(dataset_id, proof_by_field)
        return proof_by_field

    async def publish_dataset(self, dataset_id: str) -> int:
        """Atomically switch one source pointer after all verification succeeds."""

        async with db.transaction():
            dataset_row = await db.first(
                f"SELECT source_id, status, publish_requested FROM "
                f"{table_name('fhir_formulary_dataset')} "
                "WHERE dataset_id = :dataset_id FOR UPDATE;",
                dataset_id=dataset_id,
            )
            dataset_by_field = row_mapping(dataset_row)
            is_publishable = bool(
                dataset_by_field.get("status") == "verified"
                and dataset_by_field.get("publish_requested")
            )
            if not is_publishable:
                raise RuntimeError("FHIR formulary dataset is not publishable")
            current_row = await db.first(
                f"SELECT generation FROM {table_name('fhir_formulary_current')} "
                "WHERE source_id = :source_id FOR UPDATE;",
                source_id=dataset_by_field["source_id"],
            )
            generation = int(row_mapping(current_row).get("generation") or 0) + 1
            await db.status(
                f"INSERT INTO {table_name('fhir_formulary_current')} ("
                "source_id, dataset_id, generation, published_at) VALUES ("
                ":source_id, :dataset_id, :generation, transaction_timestamp()) "
                "ON CONFLICT (source_id) DO UPDATE SET "
                "dataset_id = EXCLUDED.dataset_id, "
                "generation = EXCLUDED.generation, "
                "published_at = EXCLUDED.published_at;",
                source_id=dataset_by_field["source_id"],
                dataset_id=dataset_id,
                generation=generation,
            )
            await db.status(
                f"UPDATE {table_name('fhir_formulary_dataset')} SET "
                "status = 'published', published_at = transaction_timestamp() "
                "WHERE dataset_id = :dataset_id AND status = 'verified';",
                dataset_id=dataset_id,
            )
        return generation

    async def fail_dataset(self, dataset_id: str, exc: BaseException) -> None:
        """Mark an unrecoverable candidate failed without moving the pointer."""

        await db.status(
            f"UPDATE {table_name('fhir_formulary_dataset')} SET "
            "status = 'failed', failed_at = transaction_timestamp(), "
            "error_json = CAST(:error_json AS jsonb) "
            "WHERE dataset_id = :dataset_id "
            "AND status IN ('building', 'verified');",
            error_json=json_text(
                {"type": type(exc).__name__, "message": str(exc)[:2000]}
            ),
            dataset_id=dataset_id,
        )

    async def interrupt_dataset(
        self,
        dataset_id: str,
        exc: BaseException,
    ) -> None:
        """Retain a fenced candidate for a same-run transient retry."""

        await db.status(
            f"UPDATE {table_name('fhir_formulary_dataset')} SET error_json = "
            "CAST(:error_json AS jsonb) WHERE dataset_id = :dataset_id "
            "AND status IN ('building', 'verified');",
            error_json=json_text(
                {
                    "type": type(exc).__name__,
                    "message": str(exc)[:2000],
                    "resumable": True,
                }
            ),
            dataset_id=dataset_id,
        )


__all__ = [
    "AliasVersionWrite",
    "CheckpointWrite",
    "CompletedAliasCheckpoint",
    "CurrentSnapshot",
    "FHIRFormularyRepository",
    "PriorAliasState",
    "SOURCE_ID",
    "medication_variant_hash",
    "membership_hash",
]
