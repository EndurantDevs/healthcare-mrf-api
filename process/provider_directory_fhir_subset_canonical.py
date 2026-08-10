# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical root-neutral completion proofs for reviewed FHIR subsets."""

from __future__ import annotations

import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Mapping

from process.provider_directory_fhir_census_contract import (
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
)
from process.provider_directory_fhir_subset_identity import (
    reviewed_subset_max_advertised_count_decrease,
)


SERVER_ISSUED_SUBSET_COMPLETION_PROOF_VERSION = (
    "provider-directory-fhir-server-issued-subset-completion-v1"
)
SERVER_ISSUED_SUBSET_REQUIRED_VERSION = 3
SERVER_ISSUED_SUBSET_TERMINAL_REASON = "source_no_next"
ALLOWED_SUBSET_RESOURCE_TYPES = frozenset(
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES
)
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
_TOP_LEVEL_FIELDS = frozenset(
    {
        "proof_version",
        "contract_version",
        "semantics",
        "strategy_version",
        "traversal_version",
        "canonicalization_version",
        "completion_scopes",
        "campaign_id",
        "cutoff",
        "page_count",
        "resources",
        "dataset",
    }
)
_RESOURCE_FIELDS = frozenset(
    {
        "advertised_pre",
        "advertised_post",
        "returned_unique",
        "deficit",
        "geometry_version",
        "page_count",
        "pages",
        "processed_rows",
        "page_entry_counts",
        "continuation_shape_sha256",
        "continuation_shape_chain_sha256",
        "logical_terminal_offset",
        "logical_window_end_offset",
        "terminal_entries",
        "sparse_pages",
        "empty_pages",
        "geometry_sha256",
        "terminal_reason",
        "content_sha256",
        "acquired_content_sha256",
    }
)
_DATASET_FIELDS = frozenset(
    {
        "hash",
        "count",
        "resource_hashes",
        "resource_counts",
        "acquired_resource_hashes",
    }
)
_FORBIDDEN_KEY_PARTS = (
    "url",
    "token",
    "cursor",
    "source_id",
    "endpoint_id",
    "dataset_id",
    "root_id",
    "run_id",
)


def canonical_sha256(value: Any) -> str:
    """Hash one JSON value with the proof's fixed canonical encoding."""

    canonical_json = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _canonical_payload_number(value: int | float) -> str:
    try:
        numeric_value = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(
            "provider_directory_subset_payload_number_invalid"
        ) from exc
    if not numeric_value.is_finite():
        raise ValueError("provider_directory_subset_payload_number_invalid")
    if numeric_value.is_zero():
        return "0"
    return format(numeric_value.normalize(), "f")


def canonical_payload_json(value: Any) -> str:
    """Encode one projected payload identically in Python and PostgreSQL."""

    if value is None:
        return "null"
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) in {int, float}:
        return _canonical_payload_number(value)
    if type(value) is str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if type(value) is list:
        return "[" + ",".join(canonical_payload_json(item) for item in value) + "]"
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ValueError("provider_directory_subset_payload_key_invalid")
        return "{" + ",".join(
            canonical_payload_json(key) + ":" + canonical_payload_json(value[key])
            for key in sorted(value)
        ) + "}"
    raise ValueError("provider_directory_subset_payload_type_invalid")


def canonical_payload_sha256(value: Any) -> str:
    """Hash one transport-neutral payload with the v2 canonical encoding."""

    return hashlib.sha256(canonical_payload_json(value).encode("utf-8")).hexdigest()


def _is_sha256(value: Any) -> bool:
    return type(value) is str and _HEX_SHA256.fullmatch(value) is not None


def _is_nonnegative_int(value: Any) -> bool:
    return type(value) is int and value >= 0


def _is_canonical_utc_instant(value: Any) -> bool:
    if type(value) is not str:
        return False
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return bool(
        parsed.tzinfo is not None
        and parsed.utcoffset() is not None
        and parsed.astimezone(datetime.UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        == value
    )


def _assert_root_neutral(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, nested_value in value.items():
            if type(key) is not str or any(
                private_part in key.lower()
                for private_part in _FORBIDDEN_KEY_PARTS
            ):
                raise ValueError(
                    "provider_directory_subset_completion_proof_private"
                )
            _assert_root_neutral(nested_value)
        return
    if isinstance(value, list):
        for nested_value in value:
            _assert_root_neutral(nested_value)
        return
    if value is not None and type(value) not in {str, int, bool}:
        raise ValueError("provider_directory_subset_completion_proof_invalid")


def _has_valid_resource_counts(
    resource_proof: Mapping[str, Any],
    max_advertised_count_decrease: int,
) -> bool:
    """Return whether resource counts satisfy the selected exact profile."""

    advertised_pre = resource_proof.get("advertised_pre")
    advertised_post = resource_proof.get("advertised_post")
    returned_unique = resource_proof.get("returned_unique")
    deficit = resource_proof.get("deficit")
    return bool(
        _is_nonnegative_int(advertised_pre)
        and _is_nonnegative_int(advertised_post)
        and _is_nonnegative_int(returned_unique)
        and _is_nonnegative_int(deficit)
        and 0
        <= advertised_pre - advertised_post
        <= max_advertised_count_decrease
        and returned_unique <= advertised_post
        and deficit == advertised_pre - returned_unique
    )


def _validate_resource_scalar_fields(
    resource_proof: Mapping[str, Any],
    page_count: int,
    max_advertised_count_decrease: int,
) -> None:
    """Validate scalar proof fields under the selected exact profile."""

    numeric_fields = (
        "advertised_pre",
        "advertised_post",
        "returned_unique",
        "deficit",
        "geometry_version",
        "page_count",
        "pages",
        "processed_rows",
        "logical_terminal_offset",
        "logical_window_end_offset",
        "terminal_entries",
        "sparse_pages",
        "empty_pages",
    )
    shape_hashes = resource_proof.get("continuation_shape_sha256")
    if (
        set(resource_proof) != _RESOURCE_FIELDS
        or any(
            not _is_nonnegative_int(resource_proof.get(field_name))
            for field_name in numeric_fields
        )
        or not _has_valid_resource_counts(
            resource_proof,
            max_advertised_count_decrease,
        )
        or resource_proof["pages"] <= 0
        or resource_proof["logical_terminal_offset"]
        != (resource_proof["pages"] - 1) * page_count
        or resource_proof["logical_window_end_offset"]
        != resource_proof["pages"] * page_count
        or resource_proof["terminal_entries"] > page_count
        or resource_proof["returned_unique"]
        > resource_proof["logical_window_end_offset"]
        or resource_proof["geometry_version"] != 2
        or resource_proof["page_count"] != page_count
        or resource_proof["processed_rows"] != resource_proof["returned_unique"]
        or resource_proof["sparse_pages"] > resource_proof["pages"]
        or resource_proof["empty_pages"] > resource_proof["sparse_pages"]
        or resource_proof["terminal_reason"]
        != SERVER_ISSUED_SUBSET_TERMINAL_REASON
        or not _is_sha256(resource_proof["geometry_sha256"])
        or not _is_sha256(resource_proof["content_sha256"])
        or not _is_sha256(resource_proof["acquired_content_sha256"])
        or type(shape_hashes) is not list
        or len(shape_hashes) != resource_proof["pages"] - 1
        or any(not _is_sha256(shape_digest) for shape_digest in shape_hashes)
        or canonical_sha256(shape_hashes)
        != resource_proof.get("continuation_shape_chain_sha256")
    ):
        raise ValueError("provider_directory_subset_completion_resource_invalid")


def _resource_geometry_by_field(
    resource_proof: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "empty_pages": resource_proof["empty_pages"],
        "logical_window_end_offset": resource_proof["logical_window_end_offset"],
        "page_count": resource_proof["page_count"],
        "pages_processed": resource_proof["pages"],
        "processed_rows": resource_proof["processed_rows"],
        "page_entry_counts": resource_proof["page_entry_counts"],
        "sparse_pages": resource_proof["sparse_pages"],
        "terminal_page_entries": resource_proof["terminal_entries"],
        "terminal_page_start_offset": resource_proof["logical_terminal_offset"],
        "version": resource_proof["geometry_version"],
    }


def _validate_resource_page_geometry(
    resource_proof: Mapping[str, Any],
    page_count: int,
) -> None:
    geometry_by_field = _resource_geometry_by_field(resource_proof)
    full_pages = resource_proof["pages"] - resource_proof["sparse_pages"]
    nonempty_sparse_pages = (
        resource_proof["sparse_pages"] - resource_proof["empty_pages"]
    )
    minimum_rows = full_pages * page_count + nonempty_sparse_pages
    maximum_rows = (
        full_pages * page_count
        + nonempty_sparse_pages * max(page_count - 1, 0)
    )
    page_entry_counts = resource_proof.get("page_entry_counts")
    if (
        type(page_entry_counts) is not list
        or len(page_entry_counts) != resource_proof["pages"]
        or any(
            type(entry_count) is not int or not 0 <= entry_count <= page_count
            for entry_count in page_entry_counts
        )
        or sum(page_entry_counts) != resource_proof["processed_rows"]
        or page_entry_counts[-1] != resource_proof["terminal_entries"]
        or sum(entry_count < page_count for entry_count in page_entry_counts)
        != resource_proof["sparse_pages"]
        or page_entry_counts.count(0) != resource_proof["empty_pages"]
        or not minimum_rows <= resource_proof["processed_rows"] <= maximum_rows
        or canonical_sha256(geometry_by_field)
        != resource_proof["geometry_sha256"]
    ):
        raise ValueError("provider_directory_subset_completion_geometry_invalid")


def _validate_resource(
    resource_proof: Mapping[str, Any],
    *,
    page_count: int,
    max_advertised_count_decrease: int,
) -> int:
    _validate_resource_scalar_fields(
        resource_proof,
        page_count,
        max_advertised_count_decrease,
    )
    _validate_resource_page_geometry(resource_proof, page_count)
    return resource_proof["returned_unique"]


def _validated_completion_envelope(
    completion_proof: Any,
    completion_sha256: Any,
) -> tuple[
    dict[str, Any],
    Mapping[str, Any],
    Mapping[str, Any],
    int,
]:
    if not isinstance(completion_proof, Mapping) or not _is_sha256(
        completion_sha256
    ):
        raise ValueError("provider_directory_subset_completion_proof_invalid")
    completion_proof_by_field = dict(completion_proof)
    _assert_root_neutral(completion_proof_by_field)
    fixed_value_by_field = {
        "proof_version": SERVER_ISSUED_SUBSET_COMPLETION_PROOF_VERSION,
        "contract_version": SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
        "semantics": SERVER_ISSUED_SUBSET_SEMANTICS,
        "traversal_version": SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
        "canonicalization_version": SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    }
    completion_scopes = completion_proof_by_field.get("completion_scopes")
    max_advertised_count_decrease = (
        reviewed_subset_max_advertised_count_decrease(
            completion_proof_by_field.get("strategy_version"),
            tuple(completion_scopes)
            if type(completion_scopes) is list
            else None,
        )
    )
    resource_proof_by_type = completion_proof_by_field.get("resources")
    dataset_by_field = completion_proof_by_field.get("dataset")
    if (
        set(completion_proof_by_field) != _TOP_LEVEL_FIELDS
        or any(
            completion_proof_by_field.get(key) != expected_value
            for key, expected_value in fixed_value_by_field.items()
        )
        or max_advertised_count_decrease is None
        or type(completion_proof_by_field.get("campaign_id")) is not str
        or not completion_proof_by_field["campaign_id"]
        or not _is_canonical_utc_instant(completion_proof_by_field.get("cutoff"))
        or type(completion_proof_by_field.get("page_count")) is not int
        or not 1 <= completion_proof_by_field["page_count"] <= 1000
        or not isinstance(resource_proof_by_type, Mapping)
        or set(resource_proof_by_type) != ALLOWED_SUBSET_RESOURCE_TYPES
        or not isinstance(dataset_by_field, Mapping)
        or set(dataset_by_field) != _DATASET_FIELDS
    ):
        raise ValueError("provider_directory_subset_completion_proof_invalid")
    return (
        completion_proof_by_field,
        resource_proof_by_type,
        dataset_by_field,
        max_advertised_count_decrease,
    )


def _validate_completion_dataset(
    completion_proof_by_field: Mapping[str, Any],
    resource_proof_by_type: Mapping[str, Any],
    dataset_by_field: Mapping[str, Any],
    returned_count_by_type: Mapping[str, int],
    completion_sha256: str,
) -> None:
    resource_hash_by_type = dataset_by_field.get("resource_hashes")
    resource_count_by_type = dataset_by_field.get("resource_counts")
    acquired_resource_hash_by_type = dataset_by_field.get(
        "acquired_resource_hashes"
    )
    if (
        len(returned_count_by_type) != len(resource_proof_by_type)
        or not _is_sha256(dataset_by_field.get("hash"))
        or not _is_nonnegative_int(dataset_by_field.get("count"))
        or not isinstance(resource_hash_by_type, Mapping)
        or not isinstance(resource_count_by_type, Mapping)
        or not isinstance(acquired_resource_hash_by_type, Mapping)
        or set(resource_hash_by_type) != ALLOWED_SUBSET_RESOURCE_TYPES
        or set(resource_count_by_type) != ALLOWED_SUBSET_RESOURCE_TYPES
        or set(acquired_resource_hash_by_type) != ALLOWED_SUBSET_RESOURCE_TYPES
        or any(
            not _is_sha256(resource_digest)
            for resource_digest in resource_hash_by_type.values()
        )
        or any(
            not _is_sha256(resource_digest)
            for resource_digest in acquired_resource_hash_by_type.values()
        )
        or dict(resource_count_by_type) != returned_count_by_type
        or dataset_by_field["count"] != sum(returned_count_by_type.values())
        or any(
            resource_proof_by_type[resource_type]["content_sha256"]
            != resource_hash_by_type[resource_type]
            for resource_type in ALLOWED_SUBSET_RESOURCE_TYPES
        )
        or any(
            resource_proof_by_type[resource_type]["acquired_content_sha256"]
            != acquired_resource_hash_by_type[resource_type]
            for resource_type in ALLOWED_SUBSET_RESOURCE_TYPES
        )
        or canonical_sha256(completion_proof_by_field) != completion_sha256
    ):
        raise ValueError("provider_directory_subset_completion_dataset_invalid")


def validate_subset_completion_proof_pair(
    completion_proof: Any,
    completion_sha256: Any,
) -> tuple[dict[str, Any], str]:
    """Validate every fixed field, aggregate, and canonical digest exactly."""

    (
        completion_proof_by_field,
        resource_proof_by_type,
        dataset_by_field,
        max_advertised_count_decrease,
    ) = _validated_completion_envelope(completion_proof, completion_sha256)
    returned_count_by_type = {
        resource_type: _validate_resource(
            resource_proof,
            page_count=completion_proof_by_field["page_count"],
            max_advertised_count_decrease=max_advertised_count_decrease,
        )
        for resource_type, resource_proof in resource_proof_by_type.items()
        if isinstance(resource_proof, Mapping)
    }
    _validate_completion_dataset(
        completion_proof_by_field,
        resource_proof_by_type,
        dataset_by_field,
        returned_count_by_type,
        completion_sha256,
    )
    return completion_proof_by_field, completion_sha256
