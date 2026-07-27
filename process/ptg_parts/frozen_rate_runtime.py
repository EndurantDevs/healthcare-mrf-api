# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Runtime dispatch and acquisition checks for frozen PTG rate-file sets."""

from __future__ import annotations

import hashlib
import hmac
from pathlib import Path
from typing import Any, Mapping, Sequence

from process.ptg_parts.canonical import canonical_json_dumps, canonicalize_url
from process.ptg_parts.domain import PTG2LogicalArtifact, PTG2RawArtifact
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_PROOF_CONTRACT,
    FrozenRateFileMismatchError,
    FrozenRateFileValidationError,
)


def assert_frozen_input_compatibility(
    frozen_rate_files: Any,
    *,
    in_network_url: str | None = None,
    allowed_url: str | None = None,
    toc_urls: Sequence[str] | None = None,
    toc_list: str | None = None,
    file_url_contains: Sequence[str] | None = None,
    max_files: int | None = None,
) -> None:
    """Keep immutable direct dispatch separate from scalar/discovery selection."""

    if frozen_rate_files is None:
        return
    file_count = (
        len(frozen_rate_files)
        if isinstance(frozen_rate_files, list)
        else 0
    )
    if any(
        (
            in_network_url,
            allowed_url,
            toc_urls,
            toc_list,
            file_url_contains,
        )
    ) or (max_files is not None and max_files != file_count):
        raise FrozenRateFileValidationError(
            "frozen_rate_files is mutually exclusive with scalar, TOC, URL-filter, "
            "and conflicting max_files inputs"
        )


def build_frozen_rate_jobs(
    normalized_files: Sequence[Mapping[str, Any]],
    *,
    plan_info: Sequence[Mapping[str, Any]],
    source_network_names: Sequence[str],
) -> list[dict[str, Any]]:
    """Create exactly one direct job per canonical ordinal."""

    file_count = len(normalized_files)
    return [
        {
            "type": descriptor["source_type"],
            "url": descriptor["canonical_url"],
            "plan_info": [dict(plan) for plan in plan_info],
            "source_network_names": list(source_network_names),
            "_ptg_progress_index": index,
            "_ptg_progress_total": file_count,
            "_frozen_rate_file": dict(descriptor),
        }
        for index, descriptor in enumerate(normalized_files)
    ]


def bind_frozen_rate_set_to_scope(
    scope_digest: str | None,
    set_digest: str,
    file_count: int,
) -> str | None:
    """Bind a controlled rebuild scope to this exact multipart file set."""

    if scope_digest is None:
        return None
    scope_by_field = {
        "contract": "ptg_frozen_rate_file_rebuild_scope_v1",
        "file_count": file_count,
        "frozen_rate_file_set_sha256": set_digest,
        "requested_scope_sha256": scope_digest,
    }
    return hashlib.sha256(
        canonical_json_dumps(scope_by_field).encode("utf-8")
    ).hexdigest()


def validate_frozen_head(
    descriptor: Mapping[str, Any],
    raw_artifact: PTG2RawArtifact,
) -> None:
    """Verify final HEAD/body validators carried by one acquired raw artifact."""

    head = raw_artifact.head
    if head is None:
        raise FrozenRateFileMismatchError(
            "frozen rate file HEAD metadata is unavailable"
        )
    expected_etag = descriptor.get("etag")
    if expected_etag is not None and head.etag != expected_etag:
        raise FrozenRateFileMismatchError(
            "frozen rate file ETag changed before acquisition"
        )
    expected_last_modified = descriptor.get("last_modified")
    if (
        expected_last_modified is not None
        and head.last_modified != expected_last_modified
    ):
        raise FrozenRateFileMismatchError(
            "frozen rate file Last-Modified changed before acquisition"
        )
    if head.content_length != descriptor.get("content_length"):
        raise FrozenRateFileMismatchError(
            "frozen rate file HEAD content length changed before acquisition"
        )
    if (
        not head.supports_head
        or head.status is None
        or not 200 <= head.status < 300
    ):
        raise FrozenRateFileMismatchError(
            "frozen rate file HEAD validator is unavailable"
        )
    if head.url and canonicalize_url(head.url) != descriptor.get(
        "canonical_url"
    ):
        raise FrozenRateFileMismatchError(
            "frozen rate file HEAD resolved to a different canonical URL"
        )


def _cleanup_fresh_artifacts(
    raw_artifact: PTG2RawArtifact,
    logical_artifact: PTG2LogicalArtifact,
) -> None:
    if not logical_artifact.reused:
        Path(logical_artifact.logical_path).unlink(missing_ok=True)
    if not raw_artifact.reused:
        Path(raw_artifact.raw_path).unlink(missing_ok=True)


def validate_frozen_artifacts(
    descriptor: Mapping[str, Any],
    raw_artifact: PTG2RawArtifact,
    logical_artifact: PTG2LogicalArtifact,
) -> None:
    """Fail closed when retained or downloaded bytes differ from frozen proof."""

    try:
        validate_frozen_head(descriptor, raw_artifact)
        if raw_artifact.canonical_url != descriptor.get("canonical_url"):
            raise FrozenRateFileMismatchError(
                "frozen rate file canonical URL changed during acquisition"
            )
        if raw_artifact.byte_count != descriptor.get("content_length"):
            raise FrozenRateFileMismatchError(
                "frozen rate file body content length does not match"
            )
        if not hmac.compare_digest(
            raw_artifact.raw_sha256,
            str(descriptor.get("raw_sha256") or ""),
        ):
            raise FrozenRateFileMismatchError(
                "frozen rate file raw SHA-256 does not match"
            )
        if not descriptor.get("logical_hash_deferred"):
            if logical_artifact.logical_hash_deferred or not hmac.compare_digest(
                logical_artifact.logical_sha256,
                str(descriptor.get("logical_sha256") or ""),
            ):
                raise FrozenRateFileMismatchError(
                    "frozen rate file logical SHA-256 does not match"
                )
    except FrozenRateFileMismatchError:
        _cleanup_fresh_artifacts(raw_artifact, logical_artifact)
        raise


def _processed_result_by_url(
    file_results: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    result_by_url: dict[str, Mapping[str, Any]] = {}
    for file_result_by_field in file_results:
        source_url = str(file_result_by_field.get("url") or "")
        if (
            not file_result_by_field.get("success")
            or not source_url
            or source_url in result_by_url
        ):
            raise FrozenRateFileMismatchError(
                "frozen processed source-version cardinality does not match"
            )
        result_by_url[source_url] = file_result_by_field
    return result_by_url


def validate_frozen_processed_results(
    normalized_files: Sequence[Mapping[str, Any]],
    file_results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Bind generated engine source versions to every frozen part before publish."""

    if len(file_results) != len(normalized_files):
        raise FrozenRateFileMismatchError(
            "frozen processed source-version cardinality does not match"
        )
    result_by_url = _processed_result_by_url(file_results)
    return [
        _frozen_result_proof(
            descriptor,
            result_by_url.get(str(descriptor["canonical_url"])),
        )
        for descriptor in normalized_files
    ]


def _frozen_result_proof(
    descriptor: Mapping[str, Any],
    file_result_by_field: Mapping[str, Any] | None,
) -> dict[str, Any]:
    summary_by_field = (
        file_result_by_field.get("summary")
        if isinstance(file_result_by_field, Mapping)
        and isinstance(file_result_by_field.get("summary"), Mapping)
        else {}
    )
    if (
        not file_result_by_field
        or file_result_by_field.get("source_type")
        != descriptor["source_type"]
    ):
        raise FrozenRateFileMismatchError(
            "frozen processed source type or URL does not match"
        )
    exact_fields = (
        "canonical_url",
        "raw_sha256",
        "logical_sha256",
        "logical_hash_deferred",
        "content_length",
        "engine_source_identity_hash",
        "engine_source_file_version_id",
        "etag",
        "last_modified",
    )
    for field_name in exact_fields:
        expected_value = descriptor.get(field_name)
        actual_value = summary_by_field.get(field_name)
        if field_name == "logical_hash_deferred":
            actual_value = bool(actual_value)
        if actual_value != expected_value:
            raise FrozenRateFileMismatchError(
                f"frozen processed {field_name} does not match"
            )
    if summary_by_field.get("raw_byte_count") != descriptor["content_length"]:
        raise FrozenRateFileMismatchError(
            "frozen processed raw_byte_count does not match"
        )
    return {
        "contract": FROZEN_RATE_FILE_PROOF_CONTRACT,
        **dict(descriptor),
        "raw_byte_count": summary_by_field["raw_byte_count"],
        "verification_mode": summary_by_field.get("verification_mode"),
    }


__all__ = [
    "assert_frozen_input_compatibility",
    "bind_frozen_rate_set_to_scope",
    "build_frozen_rate_jobs",
    "validate_frozen_artifacts",
    "validate_frozen_head",
    "validate_frozen_processed_results",
]
