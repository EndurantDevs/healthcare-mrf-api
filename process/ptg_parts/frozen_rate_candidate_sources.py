# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Database-source corroboration for frozen multipart candidates."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileMismatchError,
    frozen_observed_logical_sha256,
    normalize_frozen_verification_mode,
)


def validate_frozen_candidate_database_sources(
    database_sources: Sequence[Mapping[str, Any]],
    descriptors: Sequence[Mapping[str, Any]],
    proof_by_version_id: Mapping[str, Mapping[str, Any]],
) -> None:
    """Require a dense, unique, descriptor-exact database source set."""

    if len(database_sources) != len(descriptors):
        raise FrozenRateFileMismatchError(
            "candidate frozen database source cardinality changed"
        )
    observed_entries = [
        _database_source_entry(database_source)
        for database_source in database_sources
    ]
    observed_source_keys = {entry[0] for entry in observed_entries}
    observed_version_ids = {entry[1] for entry in observed_entries}
    observed_raw_hashes = {entry[2] for entry in observed_entries}
    observed_identities = {entry[3] for entry in observed_entries}
    expected_identities = {
        canonical_json_dumps(
            _expected_source_version_identity(
                descriptor,
                proof_by_version_id,
            )
        )
        for descriptor in descriptors
    }
    if (
        observed_source_keys != set(range(len(descriptors)))
        or len(observed_version_ids) != len(descriptors)
        or len(observed_raw_hashes) != len(descriptors)
        or observed_identities != expected_identities
    ):
        raise FrozenRateFileMismatchError(
            "candidate frozen database source evidence changed"
        )


def _database_source_entry(
    database_source: Mapping[str, Any],
) -> tuple[int, str, str, str]:
    """Validate one database row and return its comparison coordinates."""

    source_key = database_source.get("source_key")
    version_count = database_source.get("source_file_version_count")
    version_id = str(database_source.get("source_file_version_id") or "")
    raw_sha256 = str(database_source.get("raw_container_sha256") or "")
    if (
        type(source_key) is not int
        or type(version_count) is not int
        or version_count != 1
        or not version_id
        or not raw_sha256
        or raw_sha256
        != str(database_source.get("version_raw_sha256") or "")
    ):
        raise FrozenRateFileMismatchError(
            "candidate frozen database source evidence changed"
        )
    version_payload = _mapping(database_source.get("version_payload"))
    version_identity = _database_source_version_identity(
        database_source,
        version_payload,
    )
    return (
        source_key,
        version_id,
        raw_sha256,
        canonical_json_dumps(version_identity),
    )


def _database_source_version_identity(
    database_source: Mapping[str, Any],
    version_payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Normalize every live source-version field bound by frozen evidence."""

    if version_payload is None:
        raise FrozenRateFileMismatchError(
            "candidate frozen database source evidence changed"
        )
    content_length = database_source.get("version_content_length")
    raw_byte_count = version_payload.get("raw_byte_count")
    logical_hash_deferred = version_payload.get("logical_hash_deferred")
    if (
        type(content_length) is not int
        or type(raw_byte_count) is not int
        or type(logical_hash_deferred) is not bool
    ):
        raise FrozenRateFileMismatchError(
            "candidate frozen database source evidence changed"
        )
    return {
        "source_file_version_id": str(
            database_source.get("source_file_version_id") or ""
        ),
        "source_identity_hash": str(
            database_source.get("version_source_identity_hash") or ""
        ),
        "source_type": str(
            database_source.get("version_source_type") or ""
        ),
        "canonical_url": str(
            database_source.get("version_canonical_url") or ""
        ),
        "raw_sha256": str(
            database_source.get("version_raw_sha256") or ""
        ),
        "logical_sha256": str(
            database_source.get("version_logical_sha256") or ""
        ),
        "logical_hash_deferred": logical_hash_deferred,
        "content_length": content_length,
        "raw_byte_count": raw_byte_count,
        "etag": database_source.get("version_etag"),
        "last_modified": database_source.get("version_last_modified"),
        "verification_mode": normalize_frozen_verification_mode(
            database_source.get("version_verification_mode")
        ),
    }


def _expected_source_version_identity(
    descriptor: Mapping[str, Any],
    proof_by_version_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Build the exact database identity required by one descriptor."""

    version_id = str(descriptor["engine_source_file_version_id"])
    proof = proof_by_version_id.get(version_id)
    if proof is None:
        raise FrozenRateFileMismatchError(
            "candidate frozen proof source versions are ambiguous"
        )
    return {
        "source_file_version_id": version_id,
        "source_identity_hash": descriptor["engine_source_identity_hash"],
        "source_type": descriptor["source_type"],
        "canonical_url": descriptor["canonical_url"],
        "raw_sha256": descriptor["raw_sha256"],
        "logical_sha256": frozen_observed_logical_sha256(descriptor),
        "logical_hash_deferred": descriptor["logical_hash_deferred"],
        "content_length": descriptor["content_length"],
        "raw_byte_count": descriptor["content_length"],
        "etag": descriptor["etag"],
        "last_modified": descriptor["last_modified"],
        "verification_mode": normalize_frozen_verification_mode(
            proof.get("verification_mode")
        ),
    }


def _mapping(candidate_value: Any) -> dict[str, Any] | None:
    """Return a copied mapping or absence for malformed payloads."""

    return (
        dict(candidate_value)
        if isinstance(candidate_value, Mapping)
        else None
    )


__all__ = ["validate_frozen_candidate_database_sources"]
