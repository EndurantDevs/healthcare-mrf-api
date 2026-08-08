# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed library-only source registration and verification candidate."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from db.models import db
from process.formulary_fhir.client import FHIRFormularyClient
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import _binding_from_row
from process.formulary_fhir.source import load_enabled_source
from process.formulary_fhir.synchronizer import ClientFactory
from process.formulary_fhir.synchronizer import SynchronizationResult
from process.formulary_fhir.synchronizer import _run_verified_sync
from process.formulary_fhir.types import AlternativeCorrection
from process.formulary_fhir.types import FHIRSourceConfigurationError
from process.formulary_fhir.types import FormularySourceConfig
from process.formulary_fhir.types import SOURCE_RUNTIME_FIELDS
from process.formulary_fhir.types import enabled_source_config


DEFAULT_REVIEWED_SOURCE_MANIFEST = Path(__file__).with_name(
    "reviewed_source_manifest.json"
)
CANDIDATE_TIMEOUT_SECONDS = 604_800
LOCK_WAIT_SECONDS = 5.0
LOCK_RETRY_SECONDS = 0.1
MANIFEST_FIELDS = frozenset(
    {"schema_version", "importer", "reviewed_at", "source"}
)
SOURCE_FIELDS = frozenset(
    {
        "source_id",
        "canonical_base",
        "display_name",
        "enabled",
        "runtime_config_json",
        "metadata_json",
    }
)
METADATA_FIELDS = frozenset(
    {
        "access_requirement",
        "alternative_reference_correction",
        "fhir_release",
        "launch_mode",
        "publication_intent",
        "resource_types",
        "verification_state",
    }
)
ERROR_MESSAGES = {
    "busy": "FHIR formulary reviewed source is busy",
    "catalog": "FHIR formulary reviewed source catalog is inconsistent",
    "cleanup": "FHIR formulary reviewed source cleanup failed",
    "invalid_request": "FHIR formulary reviewed candidate request is invalid",
    "lock_unavailable": "FHIR formulary reviewed source lock is unavailable",
    "manifest": "FHIR formulary reviewed source manifest is invalid",
    "source": "FHIR formulary reviewed source registration failed",
}


class ReviewedSourceError(RuntimeError):
    """Expose one bounded reviewed-source failure without source details."""

    def __init__(self, code: str) -> None:
        self.code = code if code in ERROR_MESSAGES else "source"
        super().__init__(ERROR_MESSAGES[self.code])


@dataclass(frozen=True, slots=True, repr=False)
class ReviewedSourceManifest:
    """Retain one immutable checked-in source contract with redacted repr."""

    source_id: str
    canonical_base: str = field(repr=False)
    display_name: str = field(repr=False)
    reviewed_at: dt.date
    config: FormularySourceConfig = field(repr=False)
    alternative_correction: AlternativeCorrection = field(repr=False)

    def __repr__(self) -> str:
        return (
            "ReviewedSourceManifest("
            f"source_id={self.source_id!r}, reviewed_at={self.reviewed_at!r})"
        )


def _reviewed_date(raw_reviewed_at: object) -> dt.date:
    if type(raw_reviewed_at) is not str:
        raise ValueError("review date type mismatch")
    reviewed_at = dt.date.fromisoformat(raw_reviewed_at)
    if reviewed_at.isoformat() != raw_reviewed_at:
        raise ValueError("review date format mismatch")
    return reviewed_at


def _alternative_correction(
    metadata_by_field: dict[str, Any],
) -> AlternativeCorrection:
    correction_by_field = metadata_by_field.get(
        "alternative_reference_correction"
    )
    if type(correction_by_field) is not dict or set(correction_by_field) != {
        "prefix",
        "rule_version",
    }:
        raise ValueError("correction shape mismatch")
    return AlternativeCorrection(
        prefix=correction_by_field.get("prefix"),
        rule_version=correction_by_field.get("rule_version"),
    )


def _validated_metadata(
    metadata_by_field: object,
) -> AlternativeCorrection:
    if type(metadata_by_field) is not dict or set(metadata_by_field) != (
        METADATA_FIELDS
    ):
        raise ValueError("metadata fields mismatch")
    expected_metadata_by_field = {
        "access_requirement": "none",
        "fhir_release": "R4",
        "launch_mode": "manual-library",
        "publication_intent": "none",
        "resource_types": ["List", "MedicationKnowledge"],
        "verification_state": "pending-first-exhaustive-verification",
    }
    if any(
        metadata_by_field.get(field_name) != expected_field_value
        for field_name, expected_field_value in expected_metadata_by_field.items()
    ):
        raise ValueError("metadata contract mismatch")
    return _alternative_correction(metadata_by_field)


def _validated_manifest_document(
    manifest_by_field: object,
) -> ReviewedSourceManifest:
    try:
        if type(manifest_by_field) is not dict or set(manifest_by_field) != (
            MANIFEST_FIELDS
        ):
            raise ValueError("manifest fields mismatch")
        if (
            type(manifest_by_field.get("schema_version")) is not int
            or manifest_by_field.get("schema_version") != 1
            or manifest_by_field.get("importer") != "formulary-fhir"
        ):
            raise ValueError("manifest identity mismatch")
        source_by_field = manifest_by_field.get("source")
        if type(source_by_field) is not dict or set(source_by_field) != SOURCE_FIELDS:
            raise ValueError("source fields mismatch")
        source_id = strict_text(source_by_field.get("source_id"), "source id", 64)
        display_name = strict_text(
            source_by_field.get("display_name"),
            "source display name",
            256,
        )
        config = enabled_source_config(
            canonical_base=source_by_field.get("canonical_base"),
            enabled=source_by_field.get("enabled"),
            runtime_config_json=source_by_field.get("runtime_config_json"),
        )
        return ReviewedSourceManifest(
            source_id=source_id,
            canonical_base=config.canonical_base,
            display_name=display_name,
            reviewed_at=_reviewed_date(manifest_by_field.get("reviewed_at")),
            config=config,
            alternative_correction=_validated_metadata(
                source_by_field.get("metadata_json")
            ),
        )
    except (KeyError, TypeError, ValueError, FHIRSourceConfigurationError):
        raise ReviewedSourceError("manifest") from None


def _read_manifest_document() -> dict[str, Any]:
    try:
        manifest_by_field = json.loads(
            DEFAULT_REVIEWED_SOURCE_MANIFEST.read_text(encoding="utf-8")
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ReviewedSourceError("manifest") from None
    if type(manifest_by_field) is not dict:
        raise ReviewedSourceError("manifest")
    return manifest_by_field


def reviewed_source_manifest() -> ReviewedSourceManifest:
    """Read and strictly validate the sole checked-in reviewed source."""

    return _validated_manifest_document(_read_manifest_document())


def _runtime_config(manifest: ReviewedSourceManifest) -> dict[str, int]:
    return {
        field_name: getattr(manifest.config, field_name)
        for field_name in SOURCE_RUNTIME_FIELDS
    }


def _metadata(manifest: ReviewedSourceManifest) -> dict[str, Any]:
    correction = manifest.alternative_correction
    return {
        "access_requirement": "none",
        "alternative_reference_correction": {
            "prefix": correction.prefix,
            "rule_version": correction.rule_version,
        },
        "fhir_release": "R4",
        "launch_mode": "manual-library",
        "publication_intent": "none",
        "resource_types": ["List", "MedicationKnowledge"],
        "verification_state": "pending-first-exhaustive-verification",
    }


def _source_values(manifest: ReviewedSourceManifest) -> dict[str, Any]:
    return {
        "source_id": manifest.source_id,
        "canonical_base": manifest.canonical_base,
        "display_name": manifest.display_name,
        "enabled": True,
        "runtime_config_json": _runtime_config(manifest),
        "metadata_json": _metadata(manifest),
    }


def _is_exact_source(
    source_by_field: dict[str, Any],
    manifest: ReviewedSourceManifest,
) -> bool:
    observed_by_field = {
        field_name: source_by_field.get(field_name) for field_name in SOURCE_FIELDS
    }
    return json_text(observed_by_field) == json_text(_source_values(manifest))


async def _matching_source_rows(
    database: Any,
    manifest: ReviewedSourceManifest,
) -> tuple[dict[str, Any], ...]:
    source_rows = await database.all(
        f"SELECT source_id, canonical_base, display_name, enabled, "
        f"runtime_config_json, metadata_json FROM "
        f"{table_name('fhir_formulary_source')} WHERE "
        "source_id = :source_id OR canonical_base = :canonical_base "
        "ORDER BY source_id FOR UPDATE;",
        source_id=manifest.source_id,
        canonical_base=manifest.canonical_base,
    )
    return tuple(row_mapping(source_row) for source_row in source_rows)


async def _insert_source(
    database: Any,
    manifest: ReviewedSourceManifest,
) -> None:
    source_by_field = _source_values(manifest)
    inserted_count = await database.status(
        f"INSERT INTO {table_name('fhir_formulary_source')} ("
        "source_id, canonical_base, display_name, enabled, "
        "runtime_config_json, metadata_json) VALUES ("
        ":source_id, :canonical_base, :display_name, true, "
        "CAST(:runtime_config_json AS jsonb), CAST(:metadata_json AS jsonb));",
        source_id=source_by_field["source_id"],
        canonical_base=source_by_field["canonical_base"],
        display_name=source_by_field["display_name"],
        runtime_config_json=json_text(source_by_field["runtime_config_json"]),
        metadata_json=json_text(source_by_field["metadata_json"]),
    )
    if inserted_count != 1:
        raise ReviewedSourceError("source")


async def _register_manifest(
    database: Any,
    manifest: ReviewedSourceManifest,
) -> EnabledSourceBinding:
    source_table = table_name("fhir_formulary_source")
    async with database.transaction():
        await database.status(
            f"LOCK TABLE {source_table} IN SHARE ROW EXCLUSIVE MODE;"
        )
        source_rows = await _matching_source_rows(database, manifest)
        if not source_rows:
            await _insert_source(database, manifest)
            source_rows = await _matching_source_rows(database, manifest)
        if len(source_rows) != 1 or not _is_exact_source(
            source_rows[0],
            manifest,
        ):
            raise ReviewedSourceError("catalog")
    binding = await load_enabled_source(manifest.source_id, database=database)
    expected_binding = _binding_from_row(
        manifest.source_id,
        _source_values(manifest),
    )
    if binding.configuration_hash != expected_binding.configuration_hash:
        raise ReviewedSourceError("source")
    return binding


async def register_reviewed_source(
    *,
    database: Any = db,
) -> EnabledSourceBinding:
    """Idempotently insert, but never rewrite, the reviewed source row."""

    return await _register_manifest(database, reviewed_source_manifest())


def _candidate_request(
    run_id: object,
    cutoff: object,
) -> tuple[str, dt.datetime]:
    try:
        normalized_run_id = strict_text(run_id, "run id", 64)
        cutoff_at = utc_timestamp(cutoff, "candidate cutoff")
        if cutoff_at > dt.datetime.now(dt.UTC):
            raise ValueError("future cutoff")
        return normalized_run_id, cutoff_at
    except (TypeError, ValueError):
        raise ReviewedSourceError("invalid_request") from None


async def _current_pointer(database: Any, source_id: str) -> str | None:
    pointer_by_field = row_mapping(
        await database.first(
            f"SELECT dataset_id FROM {table_name('fhir_formulary_current')} "
            "WHERE source_id = :source_id;",
            source_id=source_id,
        )
    )
    if not pointer_by_field:
        return None
    try:
        return strict_text(pointer_by_field.get("dataset_id"), "dataset id", 64)
    except ValueError:
        raise ReviewedSourceError("catalog") from None


async def _require_nonpublishing_candidate(
    database: Any,
    manifest: ReviewedSourceManifest,
    synchronization_result: SynchronizationResult,
    previous_pointer: str | None,
) -> None:
    source_table = table_name("fhir_formulary_source")
    async with database.transaction():
        await database.status(
            f"LOCK TABLE {source_table} IN SHARE ROW EXCLUSIVE MODE;"
        )
        source_rows = await _matching_source_rows(database, manifest)
        if len(source_rows) != 1 or not _is_exact_source(
            source_rows[0],
            manifest,
        ):
            raise ReviewedSourceError("catalog")
        dataset_by_field = row_mapping(
            await database.first(
                f"SELECT status, publish_requested, seed_eligible FROM "
                f"{table_name('fhir_formulary_dataset')} WHERE "
                "source_id = :source_id AND dataset_id = :dataset_id;",
                source_id=manifest.source_id,
                dataset_id=synchronization_result.dataset_id,
            )
        )
        expected_dataset_by_field = {
            "status": "verified",
            "publish_requested": False,
            "seed_eligible": False,
        }
        current_pointer = await _current_pointer(database, manifest.source_id)
        if (
            dataset_by_field != expected_dataset_by_field
            or current_pointer != previous_pointer
        ):
            raise ReviewedSourceError("source")


async def _verify_registered_candidate(
    database: Any,
    manifest: ReviewedSourceManifest,
    client_factory: ClientFactory,
    run_id: str,
    cutoff_at: dt.datetime,
) -> SynchronizationResult:
    binding = await _register_manifest(database, manifest)
    repository = FHIRFormularyRepository(
        source_id=manifest.source_id,
        database=database,
    )
    async with client_factory(binding.config) as client:
        return await _run_verified_sync(
            binding=binding,
            client=client,
            repository=repository,
            database=database,
            run_id=run_id,
            cutoff_at=cutoff_at,
            intent="none",
        )


async def verify_reviewed_source_candidate(
    *,
    run_id: str,
    cutoff: dt.datetime,
    database: Any = db,
    client_factory: ClientFactory = FHIRFormularyClient,
) -> SynchronizationResult:
    """Build one locked, exact, nonpublishing candidate from reviewed config."""

    manifest = reviewed_source_manifest()
    normalized_run_id, cutoff_at = _candidate_request(run_id, cutoff)
    try:
        async with manual_lock.manual_source_lease(
            database,
            manifest.source_id,
            wait_seconds=LOCK_WAIT_SECONDS,
            retry_seconds=LOCK_RETRY_SECONDS,
        ):
            previous_pointer = await _current_pointer(
                database,
                manifest.source_id,
            )
            async with asyncio.timeout(CANDIDATE_TIMEOUT_SECONDS):
                synchronization_result = await _verify_registered_candidate(
                    database,
                    manifest,
                    client_factory,
                    normalized_run_id,
                    cutoff_at,
                )
                await _require_nonpublishing_candidate(
                    database,
                    manifest,
                    synchronization_result,
                    previous_pointer,
                )
                return synchronization_result
    except manual_lock.ManualSourceLockError as error:
        raise ReviewedSourceError(error.code) from None


__all__ = (
    "DEFAULT_REVIEWED_SOURCE_MANIFEST",
    "ReviewedSourceError",
    "ReviewedSourceManifest",
    "register_reviewed_source",
    "reviewed_source_manifest",
    "verify_reviewed_source_candidate",
)
