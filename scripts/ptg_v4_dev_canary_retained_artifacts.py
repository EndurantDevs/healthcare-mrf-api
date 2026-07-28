"""Retained compressed-input storage evidence for frozen PTG V4 canaries."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlsplit

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.domain import PTG2_ARTIFACT_RAW
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
    FrozenRateFileMismatchError,
    normalize_frozen_rate_file_set,
    normalize_frozen_verification_mode,
)
from scripts.ptg_v4_dev_canary_db_reference import _quote_identifier
from scripts.ptg_v4_dev_canary_support import CanaryConfigurationError


RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT = (
    "ptg_v4_retained_raw_artifact_storage_v1"
)
PTG2_ARTIFACT_DIR_ENV = "HLTHPRT_PTG2_ARTIFACT_DIR"
_DATABASE_VERSION_ROWS_SQL = """
    WITH requested(source_file_version_id, ordinal) AS (
        SELECT source_file_version_id, ordinal
          FROM unnest($1::text[]) WITH ORDINALITY
               AS requested(source_file_version_id, ordinal)
    )
    SELECT requested.ordinal,
           version.source_file_version_id,
           version.source_identity_hash,
           identity.source_type,
           identity.canonical_url,
           version.raw_storage_uri,
           version.raw_sha256,
           version.logical_sha256,
           version.content_length,
           version.etag,
           version.last_modified,
           version.verification_mode,
           version.payload::text AS version_payload_json,
           (
               SELECT COUNT(*)::bigint
                 FROM {schema}.ptg2_artifact_manifest AS artifact
                WHERE artifact.artifact_kind = $2
                  AND artifact.storage_uri = version.raw_storage_uri
                  AND artifact.sha256 = version.raw_sha256
                  AND artifact.byte_count = COALESCE(
                      (version.payload ->> 'raw_byte_count')::bigint,
                      version.content_length
                  )
           ) AS artifact_manifest_count,
           (
               SELECT COUNT(*)::bigint
                 FROM {schema}.ptg2_source_file_version AS reference
                WHERE reference.raw_storage_uri = version.raw_storage_uri
                  AND reference.raw_sha256 = version.raw_sha256
           ) AS source_version_reference_count
      FROM requested
      LEFT JOIN {schema}.ptg2_source_file_version AS version
        ON version.source_file_version_id = requested.source_file_version_id
      LEFT JOIN {schema}.ptg2_source_identity AS identity
        ON identity.source_identity_hash = version.source_identity_hash
     ORDER BY requested.ordinal
"""


async def collect_retained_raw_artifact_storage(
    connection: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    snapshot_manifest: Mapping[str, Any],
    artifact_root: str | Path | None = None,
) -> dict[str, Any]:
    """Measure every frozen input file referenced by one sealed snapshot."""

    descriptors, frozen_set_digest = _frozen_descriptors(snapshot_manifest)
    source_versions = _source_versions(snapshot_manifest, descriptors)
    version_ids = [
        str(version["engine_source_file_version_id"])
        for version in source_versions
    ]
    database_rows = await _database_version_rows(
        connection,
        schema_name=schema_name,
        version_ids=version_ids,
    )
    artifact_volume_root = _artifact_root(artifact_root)
    artifact_records = [
        _verified_artifact_record(
            descriptor,
            source_version,
            database_row,
            artifact_root=artifact_volume_root,
        )
        for descriptor, source_version, database_row in zip(
            descriptors,
            source_versions,
            database_rows,
            strict=True,
        )
    ]
    evidence_by_field = {
        "contract": RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT,
        "snapshot_id": str(snapshot_id),
        "frozen_rate_file_set_sha256": frozen_set_digest,
        "source_file_version_count": len(source_versions),
        "distinct_artifact_count": len(artifact_records),
        "referenced_raw_bytes": sum(
            int(artifact_record["raw_byte_count"])
            for artifact_record in artifact_records
        ),
        "referenced_physical_bytes": sum(
            int(artifact_record["physical_allocated_bytes"])
            for artifact_record in artifact_records
        ),
        "all_files_verified": True,
        "attribution": "full_referenced_physical_bytes_conservative",
        "artifacts": artifact_records,
    }
    return {
        **evidence_by_field,
        "evidence_sha256": hashlib.sha256(
            canonical_json_dumps(evidence_by_field).encode("utf-8")
        ).hexdigest(),
    }


def _frozen_descriptors(
    snapshot_manifest: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    if (
        snapshot_manifest.get("frozen_rate_file_set_contract")
        != FROZEN_RATE_FILE_SET_CONTRACT
    ):
        raise CanaryConfigurationError(
            "published canary snapshot lacks a frozen rate-file set"
        )
    try:
        descriptors, set_digest = normalize_frozen_rate_file_set(
            snapshot_manifest.get("frozen_rate_files"),
            snapshot_manifest.get("frozen_rate_file_set_sha256"),
        )
    except (FrozenRateFileMismatchError, ValueError) as exc:
        raise CanaryConfigurationError(
            "published frozen rate-file set is invalid"
        ) from exc
    declared_count = snapshot_manifest.get("frozen_rate_file_count")
    if type(declared_count) is not int or declared_count != len(descriptors):
        raise CanaryConfigurationError(
            "published frozen rate-file count is invalid"
        )
    return descriptors, set_digest


def _source_versions(
    snapshot_manifest: Mapping[str, Any],
    descriptors: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    raw_versions = snapshot_manifest.get("source_file_versions")
    if not isinstance(raw_versions, list) or len(raw_versions) != len(
        descriptors
    ):
        raise CanaryConfigurationError(
            "published frozen source-version cardinality is invalid"
        )
    version_by_id: dict[str, dict[str, Any]] = {}
    for raw_version in raw_versions:
        if not isinstance(raw_version, Mapping):
            raise CanaryConfigurationError(
                "published frozen source-version evidence is invalid"
            )
        version_by_field = dict(raw_version)
        version_id = str(
            version_by_field.get("engine_source_file_version_id") or ""
        )
        if not version_id or version_id in version_by_id:
            raise CanaryConfigurationError(
                "published frozen source-version evidence is ambiguous"
            )
        version_by_id[version_id] = version_by_field
    ordered_source_versions = []
    for descriptor in descriptors:
        version_id = str(descriptor["engine_source_file_version_id"])
        source_version_by_field = version_by_id.get(version_id)
        if source_version_by_field is None:
            raise CanaryConfigurationError(
                "published frozen source-version evidence changed"
            )
        ordered_source_versions.append(source_version_by_field)
    return ordered_source_versions


async def _database_version_rows(
    connection: Any,
    *,
    schema_name: str,
    version_ids: Sequence[str],
) -> list[dict[str, Any]]:
    """Load exact source-version and artifact-manifest corroboration rows."""

    schema = _quote_identifier(schema_name)
    database_records = await connection.fetch(
        _DATABASE_VERSION_ROWS_SQL.format(schema=schema),
        list(version_ids),
        PTG2_ARTIFACT_RAW,
    )
    if len(database_records) != len(version_ids):
        raise CanaryConfigurationError(
            "retained raw-artifact database evidence is incomplete"
        )
    return [dict(database_record) for database_record in database_records]


def _artifact_root(artifact_root: str | Path | None) -> Path:
    configured_value = (
        artifact_root
        if artifact_root is not None
        else os.getenv(PTG2_ARTIFACT_DIR_ENV)
    )
    if configured_value is None or not str(configured_value).strip():
        raise CanaryConfigurationError(
            "retained artifact measurement requires the configured artifact volume"
        )
    configured_root = Path(configured_value)
    try:
        return configured_root.resolve(strict=True)
    except OSError as exc:
        raise CanaryConfigurationError(
            "configured retained artifact volume is unavailable"
        ) from exc


def _verified_artifact_record(
    descriptor: Mapping[str, Any],
    source_version: Mapping[str, Any],
    database_row: Mapping[str, Any],
    *,
    artifact_root: Path,
) -> dict[str, Any]:
    """Return one descriptor-exact retained-file storage record."""

    version_payload = _version_payload(database_row)
    version_id, raw_sha256, raw_byte_count, logical_hash_deferred = (
        _validated_artifact_identity(
            descriptor,
            database_row,
            version_payload,
        )
    )
    _assert_source_version_evidence(
        descriptor,
        source_version,
        database_row,
        logical_hash_deferred=logical_hash_deferred,
    )
    retained_path = _retained_path(
        database_row.get("raw_storage_uri"),
        artifact_root=artifact_root,
        raw_sha256=raw_sha256,
    )
    physical_bytes = _retained_physical_bytes(
        retained_path,
        raw_byte_count=raw_byte_count,
    )
    return {
        "ordinal": int(descriptor["ordinal"]),
        "source_file_version_id": version_id,
        "raw_sha256": raw_sha256,
        "raw_byte_count": int(raw_byte_count),
        "physical_allocated_bytes": physical_bytes,
        "source_version_reference_count": int(
            database_row["source_version_reference_count"]
        ),
        "artifact_manifest_count": 1,
    }


def _validated_artifact_identity(
    descriptor: Mapping[str, Any],
    database_row: Mapping[str, Any],
    version_payload: Mapping[str, Any],
) -> tuple[str, str, int, bool]:
    """Validate the database identity behind one retained artifact."""

    version_id = str(descriptor["engine_source_file_version_id"])
    raw_sha256 = str(descriptor["raw_sha256"])
    raw_byte_count = version_payload.get("raw_byte_count")
    logical_hash_deferred = version_payload.get("logical_hash_deferred")
    if (
        database_row.get("source_file_version_id") != version_id
        or database_row.get("source_identity_hash")
        != descriptor["engine_source_identity_hash"]
        or database_row.get("source_type") != descriptor["source_type"]
        or database_row.get("canonical_url") != descriptor["canonical_url"]
        or database_row.get("raw_sha256") != raw_sha256
        or database_row.get("content_length") != descriptor["content_length"]
        or raw_byte_count != descriptor["content_length"]
        or type(logical_hash_deferred) is not bool
        or database_row.get("etag") != descriptor["etag"]
        or database_row.get("last_modified") != descriptor["last_modified"]
        or int(database_row.get("artifact_manifest_count") or 0) != 1
        or int(database_row.get("source_version_reference_count") or 0) < 1
    ):
        raise CanaryConfigurationError(
            "retained raw-artifact database evidence changed"
        )
    assert isinstance(raw_byte_count, int)
    assert isinstance(logical_hash_deferred, bool)
    return version_id, raw_sha256, raw_byte_count, logical_hash_deferred


def _retained_physical_bytes(
    retained_path: Path,
    *,
    raw_byte_count: int,
) -> int:
    """Return allocated bytes after validating the exact retained file."""

    retained_stat = retained_path.stat()
    physical_bytes = int(getattr(retained_stat, "st_blocks", 0)) * 512
    if (
        not stat.S_ISREG(retained_stat.st_mode)
        or retained_stat.st_size != raw_byte_count
        or physical_bytes <= 0
    ):
        raise CanaryConfigurationError(
            "retained raw-artifact physical file changed"
        )
    return physical_bytes


def _version_payload(database_row: Mapping[str, Any]) -> dict[str, Any]:
    raw_payload = database_row.get("version_payload_json")
    try:
        payload = (
            json.loads(raw_payload)
            if isinstance(raw_payload, str)
            else dict(raw_payload)
        )
    except (TypeError, ValueError):
        payload = {}
    if not isinstance(payload, dict):
        raise CanaryConfigurationError(
            "retained raw-artifact version payload is invalid"
        )
    return payload


def _assert_source_version_evidence(
    descriptor: Mapping[str, Any],
    source_version: Mapping[str, Any],
    database_row: Mapping[str, Any],
    *,
    logical_hash_deferred: bool,
) -> None:
    expected_logical_sha256 = (
        descriptor["raw_sha256"]
        if logical_hash_deferred
        else descriptor["logical_sha256"]
    )
    if (
        logical_hash_deferred != descriptor["logical_hash_deferred"]
        or source_version.get("logical_hash_deferred")
        is not logical_hash_deferred
        or source_version.get("raw_sha256") != descriptor["raw_sha256"]
        or source_version.get("raw_byte_count") != descriptor["content_length"]
        or source_version.get("logical_sha256") != expected_logical_sha256
        or database_row.get("logical_sha256") != expected_logical_sha256
        or normalize_frozen_verification_mode(
            source_version.get("verification_mode")
        )
        != normalize_frozen_verification_mode(
            database_row.get("verification_mode")
        )
    ):
        raise CanaryConfigurationError(
            "retained raw-artifact source-version evidence changed"
        )


def _retained_path(
    raw_storage_uri: Any,
    *,
    artifact_root: Path,
    raw_sha256: str,
) -> Path:
    if not isinstance(raw_storage_uri, str):
        raise CanaryConfigurationError(
            "retained raw-artifact storage URI is invalid"
        )
    parsed = urlsplit(raw_storage_uri)
    if (
        parsed.scheme != "file"
        or parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        raise CanaryConfigurationError(
            "retained raw-artifact storage is not on the measured artifact volume"
        )
    try:
        retained_path = Path(unquote(parsed.path)).resolve(strict=True)
        retained_path.relative_to(artifact_root)
    except (OSError, ValueError) as exc:
        raise CanaryConfigurationError(
            "retained raw-artifact path is outside the measured artifact volume"
        ) from exc
    if not retained_path.name.startswith(raw_sha256):
        raise CanaryConfigurationError(
            "retained raw-artifact path is not content addressed"
        )
    return retained_path


__all__ = [
    "PTG2_ARTIFACT_DIR_ENV",
    "RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT",
    "collect_retained_raw_artifact_storage",
]
