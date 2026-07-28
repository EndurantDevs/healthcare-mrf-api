# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate-time corroboration for protected multipart PTG evidence."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_CONTRACT,
    FROZEN_RATE_FILE_BINDING_OPTION,
    FrozenRateFileBindingMismatchError,
    frozen_internal_run_id,
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_PROOF_CONTRACT,
    FROZEN_RATE_FILE_SET_CONTRACT,
    FrozenRateFileMismatchError,
    frozen_rate_file_proof_sha256,
    normalize_frozen_rate_file_set,
)


_CANDIDATE_MARKER_FIELDS = (
    "frozen_rate_file_set_contract",
    "frozen_rate_files",
    "frozen_rate_file_set_sha256",
    "frozen_rate_file_count",
)


def _mapping(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _strict_count(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FrozenRateFileMismatchError(
            "candidate frozen rate file count is invalid"
        )
    return value


def _descriptor_from_proof(proof_row: Mapping[str, Any]) -> dict[str, Any]:
    if proof_row.get("contract") != FROZEN_RATE_FILE_PROOF_CONTRACT:
        raise FrozenRateFileMismatchError(
            "candidate frozen proof contract is invalid"
        )
    if proof_row.get("raw_byte_count") != proof_row.get("content_length"):
        raise FrozenRateFileMismatchError(
            "candidate frozen proof byte count changed"
        )
    return {
        field_name: field_value
        for field_name, field_value in proof_row.items()
        if field_name
        not in {
            "contract",
            "raw_byte_count",
            "verification_mode",
        }
    }


def _canonical_binding(
    manifest: Mapping[str, Any],
    database_binding: Mapping[str, Any] | None,
    *,
    candidate_run_id: str,
) -> dict[str, Any]:
    manifest_binding = _mapping(
        manifest.get(FROZEN_RATE_FILE_BINDING_OPTION)
    )
    database_binding_map = _mapping(database_binding)
    if (
        manifest_binding is None
        or database_binding_map is None
        or manifest_binding != database_binding_map
    ):
        raise FrozenRateFileBindingMismatchError(
            "candidate frozen source-file binding changed"
        )
    source_file_import_id = str(
        manifest_binding.get("source_file_import_id") or ""
    ).strip()
    if (
        manifest_binding.get("contract")
        != FROZEN_RATE_FILE_BINDING_CONTRACT
        or not source_file_import_id
        or candidate_run_id
        != frozen_internal_run_id(source_file_import_id)
        or manifest.get("source_file_import_id")
        != source_file_import_id
    ):
        raise FrozenRateFileBindingMismatchError(
            "candidate frozen source-file binding changed"
        )
    return manifest_binding


def _version_by_id(
    source_file_versions: Any,
    *,
    expected_count: int,
) -> dict[str, dict[str, Any]]:
    if (
        not isinstance(source_file_versions, list)
        or len(source_file_versions) != expected_count
    ):
        raise FrozenRateFileMismatchError(
            "candidate frozen source-version cardinality changed"
        )
    version_by_id: dict[str, dict[str, Any]] = {}
    raw_hashes: set[str] = set()
    for raw_version in source_file_versions:
        version = _mapping(raw_version)
        if version is None:
            raise FrozenRateFileMismatchError(
                "candidate frozen source-version evidence is invalid"
            )
        version_id = str(
            version.get("engine_source_file_version_id")
            or version.get("source_file_version_id")
            or ""
        )
        raw_sha256 = str(version.get("raw_sha256") or "")
        if (
            not version_id
            or version_id in version_by_id
            or not raw_sha256
            or raw_sha256 in raw_hashes
        ):
            raise FrozenRateFileMismatchError(
                "candidate frozen source-version evidence is ambiguous"
            )
        version_by_id[version_id] = version
        raw_hashes.add(raw_sha256)
    return version_by_id


def _validate_database_sources(
    database_sources: Sequence[Mapping[str, Any]],
    descriptors: Sequence[Mapping[str, Any]],
) -> None:
    if len(database_sources) != len(descriptors):
        raise FrozenRateFileMismatchError(
            "candidate frozen database source cardinality changed"
        )
    observed_source_keys: set[int] = set()
    observed_version_ids: set[str] = set()
    observed_raw_hashes: set[str] = set()
    observed_identities: set[tuple[str, str]] = set()
    for database_source in database_sources:
        source_key = database_source.get("source_key")
        version_count = database_source.get("source_file_version_count")
        version_id = str(
            database_source.get("source_file_version_id") or ""
        )
        raw_sha256 = str(
            database_source.get("raw_container_sha256") or ""
        )
        version_raw_sha256 = str(
            database_source.get("version_raw_sha256") or ""
        )
        if (
            type(source_key) is not int
            or type(version_count) is not int
            or version_count != 1
            or not version_id
            or not raw_sha256
            or raw_sha256 != version_raw_sha256
            or source_key in observed_source_keys
            or version_id in observed_version_ids
            or raw_sha256 in observed_raw_hashes
        ):
            raise FrozenRateFileMismatchError(
                "candidate frozen database source evidence changed"
            )
        observed_source_keys.add(source_key)
        observed_version_ids.add(version_id)
        observed_raw_hashes.add(raw_sha256)
        observed_identities.add((version_id, raw_sha256))
    expected_source_keys = set(range(len(descriptors)))
    expected_identities = {
        (
            str(descriptor["engine_source_file_version_id"]),
            str(descriptor["raw_sha256"]),
        )
        for descriptor in descriptors
    }
    if (
        observed_source_keys != expected_source_keys
        or observed_identities != expected_identities
    ):
        raise FrozenRateFileMismatchError(
            "candidate frozen database source evidence changed"
        )


def _validated_candidate_marker_tuple(
    manifest: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str, int]:
    supplied_marker_fields = tuple(
        field_name
        for field_name in _CANDIDATE_MARKER_FIELDS
        if field_name in manifest
    )
    if len(supplied_marker_fields) != len(_CANDIDATE_MARKER_FIELDS):
        raise FrozenRateFileMismatchError(
            "candidate protected frozen marker tuple is incomplete"
        )
    if (
        manifest.get("frozen_rate_file_set_contract")
        != FROZEN_RATE_FILE_SET_CONTRACT
    ):
        raise FrozenRateFileMismatchError(
            "candidate frozen set contract changed"
        )
    descriptors, set_digest = normalize_frozen_rate_file_set(
        manifest.get("frozen_rate_files"),
        manifest.get("frozen_rate_file_set_sha256"),
    )
    file_count = _strict_count(manifest.get("frozen_rate_file_count"))
    if file_count != len(descriptors):
        raise FrozenRateFileMismatchError(
            "candidate frozen rate file count changed"
        )
    return descriptors, set_digest, file_count


def _validated_candidate_proof(
    manifest: Mapping[str, Any],
    descriptors: Sequence[Mapping[str, Any]],
    file_count: int,
) -> str:
    proof_rows = manifest.get("frozen_rate_file_proof")
    if not isinstance(proof_rows, list) or len(proof_rows) != file_count:
        raise FrozenRateFileMismatchError(
            "candidate frozen proof cardinality changed"
        )
    proof_descriptors = [
        _descriptor_from_proof(proof_row)
        if isinstance(proof_row, Mapping)
        else {}
        for proof_row in proof_rows
    ]
    if proof_descriptors != descriptors:
        raise FrozenRateFileMismatchError(
            "candidate frozen proof descriptors changed"
        )
    proof_digest = frozen_rate_file_proof_sha256(proof_rows)
    if manifest.get("frozen_rate_file_proof_sha256") != proof_digest:
        raise FrozenRateFileMismatchError(
            "candidate frozen proof digest changed"
        )
    return proof_digest


def _validate_candidate_source_versions(
    manifest: Mapping[str, Any],
    descriptors: Sequence[Mapping[str, Any]],
    file_count: int,
) -> None:
    source_versions_by_id = _version_by_id(
        manifest.get("source_file_versions"),
        expected_count=file_count,
    )
    exact_fields = (
        "canonical_url",
        "raw_sha256",
        "logical_sha256",
        "logical_hash_deferred",
        "content_length",
        "etag",
        "last_modified",
        "engine_source_identity_hash",
        "engine_source_file_version_id",
    )
    for descriptor in descriptors:
        version = source_versions_by_id.get(
            str(descriptor["engine_source_file_version_id"])
        )
        if version is None or any(
            version.get(field_name) != descriptor.get(field_name)
            for field_name in exact_fields
        ):
            raise FrozenRateFileMismatchError(
                "candidate frozen source-version evidence changed"
            )


def _validated_candidate_binding(
    manifest: Mapping[str, Any],
    database_binding: Mapping[str, Any] | None,
    candidate_run_id: str,
    expected_set: tuple[str, int],
) -> dict[str, Any]:
    frozen_binding_by_name = _canonical_binding(
        manifest,
        database_binding,
        candidate_run_id=candidate_run_id,
    )
    set_digest, file_count = expected_set
    if (
        frozen_binding_by_name.get("frozen_rate_file_set_contract")
        != FROZEN_RATE_FILE_SET_CONTRACT
        or frozen_binding_by_name.get("frozen_rate_file_set_sha256")
        != set_digest
        or frozen_binding_by_name.get("frozen_rate_file_count")
        != file_count
    ):
        raise FrozenRateFileBindingMismatchError(
            "candidate frozen source-file binding changed"
        )
    return frozen_binding_by_name


def _candidate_equivalent_identity(
    frozen_binding_by_name: Mapping[str, Any],
    descriptors: Sequence[Mapping[str, Any]],
    *,
    set_digest: str,
    file_count: int,
    proof_digest: str,
) -> str:
    return canonical_json_dumps(
        {
            "contract": "ptg_frozen_candidate_identity_v1",
            "binding": dict(frozen_binding_by_name),
            "frozen_rate_file_set_sha256": set_digest,
            "frozen_rate_file_count": file_count,
            "frozen_rate_file_proof_sha256": proof_digest,
            "source_file_version_ids": [
                descriptor["engine_source_file_version_id"]
                for descriptor in descriptors
            ],
            "raw_sha256": [
                descriptor["raw_sha256"] for descriptor in descriptors
            ],
        }
    )


def validate_frozen_candidate_evidence(
    manifest: Mapping[str, Any],
    *,
    candidate_run_id: str,
    database_binding: Mapping[str, Any] | None,
    database_sources: Sequence[Mapping[str, Any]] | None,
) -> str | None:
    """Return a canonical equivalent-identity token or legacy absence."""

    supplied_marker_fields = tuple(
        field_name
        for field_name in _CANDIDATE_MARKER_FIELDS
        if field_name in manifest
    )
    has_any_frozen_binding = (
        FROZEN_RATE_FILE_BINDING_OPTION in manifest
        or database_binding is not None
    )
    if not supplied_marker_fields:
        if has_any_frozen_binding:
            raise FrozenRateFileBindingMismatchError(
                "candidate with frozen binding cannot be treated as legacy"
            )
        return None
    descriptors, set_digest, file_count = (
        _validated_candidate_marker_tuple(manifest)
    )
    proof_digest = _validated_candidate_proof(
        manifest,
        descriptors,
        file_count,
    )
    _validate_candidate_source_versions(manifest, descriptors, file_count)
    if database_sources is None:
        raise FrozenRateFileMismatchError(
            "candidate frozen database source evidence is unavailable"
        )
    _validate_database_sources(database_sources, descriptors)
    frozen_binding_by_name = _validated_candidate_binding(
        manifest,
        database_binding,
        candidate_run_id,
        (set_digest, file_count),
    )
    return _candidate_equivalent_identity(
        frozen_binding_by_name,
        descriptors,
        set_digest=set_digest,
        file_count=file_count,
        proof_digest=proof_digest,
    )


__all__ = ["validate_frozen_candidate_evidence"]
