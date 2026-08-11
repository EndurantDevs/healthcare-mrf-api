# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared exact-variant current-pointer operations for dataset publication."""

from __future__ import annotations

from typing import Any

from process.provider_directory_dataset_scoped_publication_contract import (
    ExactCurrentDataset,
    ExactDatasetPair,
    EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    LEGACY_DATASET_PATTERN,
    LEGACY_PRACTITIONER_VARIANT,
    ProviderDirectoryDatasetScopedPublicationError,
    ROOTED_COMBINED_VARIANT,
    ROOTED_DATASET_PATTERN,
    exact_current_matches_root,
    exact_dataset_variant,
    exact_uhc_dataset_pair,
)
from process.provider_directory_dataset_scoped_publication_support import (
    database_row_fields as _row_fields,
    exact_pair_registry_by_coordinate as _locked_registry_by_coordinate,
    lock_exact_pair_registry as _lock_pair_registry,
    qualified_relation as _qualified,
    schema_name as _schema_name,
    validate_exact_pair_registry as _validate_pair_registry,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
)


LEGACY_DATASET_TABLE = "provider_directory_uhc_flex_practitioner_dataset"
LEGACY_VALID_FUNCTION = "provider_directory_uhc_flex_practitioner_dataset_valid"
LEGACY_READY_FUNCTION = "provider_directory_uhc_flex_practitioner_dataset_ready"
ROOTED_DATASET_TABLE = "provider_directory_rooted_graph_dataset"
ROOTED_INTRINSIC_VALID_FUNCTION = (
    "provider_directory_rooted_graph_dataset_intrinsic_valid"
)
ROOTED_READY_FUNCTION = "provider_directory_rooted_graph_dataset_ready"
ENDPOINT_DATASET_TABLE = "provider_directory_endpoint_dataset"


def _variant_table_and_ready(variant: str) -> tuple[str, str]:
    table_name, _identity_function, ready_function = _variant_predicates(variant)
    return table_name, ready_function


def _variant_predicates(variant: str) -> tuple[str, str, str]:
    if variant == LEGACY_PRACTITIONER_VARIANT:
        return (
            LEGACY_DATASET_TABLE,
            LEGACY_VALID_FUNCTION,
            LEGACY_READY_FUNCTION,
        )
    if variant == ROOTED_COMBINED_VARIANT:
        return (
            ROOTED_DATASET_TABLE,
            ROOTED_INTRINSIC_VALID_FUNCTION,
            ROOTED_READY_FUNCTION,
        )
    raise ProviderDirectoryDatasetScopedPublicationError("foreign_current")


async def _locked_legacy_header(
    database: Any,
    pair: ExactDatasetPair,
) -> dict[str, Any]:
    return _row_fields(
        await database.first(
            f"""
            SELECT header.dataset_id, header.endpoint_id, header.source_id,
                   header.publication_contract_id
                       AS root_publication_contract_id,
                   :legacy_source_id AS root_source_id,
                   :legacy_endpoint_id AS root_endpoint_id,
                   :rooted_source_id AS acquisition_source_id,
                   :rooted_endpoint_id AS acquisition_endpoint_id,
                   :legacy_source_id AS practitioner_origin_source_id,
                   :legacy_endpoint_id AS practitioner_origin_endpoint_id,
                   header.source_authority_id,
                   graph_endpoint.endpoint_signature_hash
                       AS endpoint_signature_sha256,
                   header.status, header.is_current, header.dataset_hash,
                   header.resource_count,
                   header.resource_count AS practitioner_resource_count,
                   header.terminal_set_sha256 AS root_content_proof_sha256,
                   header.cohort_id AS root_cohort_id,
                   header.semantic_projection_as_of,
                   header.operation_key, header.acquisition_root_run_id
              FROM {_qualified(LEGACY_DATASET_TABLE)} AS header
              JOIN {_qualified('provider_directory_api_endpoint')} AS endpoint
                ON endpoint.endpoint_id = header.endpoint_id
              JOIN {_qualified('provider_directory_api_endpoint')} AS graph_endpoint
                ON graph_endpoint.endpoint_id = :rooted_endpoint_id
             WHERE header.source_id = :legacy_source_id
               AND header.endpoint_id = :legacy_endpoint_id
               AND header.is_current IS TRUE
             FOR UPDATE OF header;
            """,
            legacy_source_id=pair.legacy_source_id,
            legacy_endpoint_id=pair.legacy_endpoint_id,
            rooted_source_id=pair.rooted_source_id,
            rooted_endpoint_id=pair.rooted_endpoint_id,
        )
    )


async def _locked_rooted_header(
    database: Any,
    pair: ExactDatasetPair,
) -> dict[str, Any]:
    return _row_fields(
        await database.first(
            f"""
            SELECT header.dataset_id, header.endpoint_id, header.source_id,
                   header.publication_contract_id
                       AS root_publication_contract_id,
                   header.source_id AS root_source_id,
                   header.endpoint_id AS root_endpoint_id,
                   header.acquisition_source_id,
                   header.acquisition_endpoint_id,
                   header.practitioner_origin_source_id,
                   header.practitioner_origin_endpoint_id,
                   header.source_authority_id,
                   header.endpoint_signature_sha256,
                   header.status, header.is_current, header.dataset_hash,
                   header.resource_count, header.practitioner_resource_count,
                   header.root_content_proof_sha256,
                   header.root_cohort_id,
                   header.semantic_projection_as_of,
                   header.operation_key, header.acquisition_root_run_id
              FROM {_qualified(ROOTED_DATASET_TABLE)} AS header
             WHERE header.practitioner_origin_source_id = :legacy_source_id
               AND header.practitioner_origin_endpoint_id = :legacy_endpoint_id
               AND header.source_id = :rooted_source_id
               AND header.endpoint_id = :rooted_endpoint_id
               AND header.acquisition_source_id = :rooted_source_id
               AND header.acquisition_endpoint_id = :rooted_endpoint_id
               AND header.is_current IS TRUE
             FOR UPDATE OF header;
            """,
            legacy_source_id=pair.legacy_source_id,
            legacy_endpoint_id=pair.legacy_endpoint_id,
            rooted_source_id=pair.rooted_source_id,
            rooted_endpoint_id=pair.rooted_endpoint_id,
        )
    )


def _projection_text(value: object) -> str:
    if hasattr(value, "isoformat"):
        value = value.isoformat()
    if type(value) is not str:
        raise ProviderDirectoryDatasetScopedPublicationError("state")
    return value


async def _locked_parent_by_id(
    database: Any,
    pair: ExactDatasetPair,
) -> dict[object, dict[str, Any]]:
    parent_rows = await database.all(
        f"""
            SELECT dataset_id, endpoint_id, dataset_hash, resource_count,
                   status, is_current
              FROM {_qualified(ENDPOINT_DATASET_TABLE)}
             WHERE endpoint_id IN (:legacy_endpoint_id, :rooted_endpoint_id)
               AND is_current IS TRUE
             ORDER BY endpoint_id, dataset_id
             FOR UPDATE;
        """,
        legacy_endpoint_id=pair.legacy_endpoint_id,
        rooted_endpoint_id=pair.rooted_endpoint_id,
    )
    return {
        parent_fields.get("dataset_id"): parent_fields
        for parent_row in parent_rows
        if (parent_fields := _row_fields(parent_row))
    }


async def _locked_current_header(
    database: Any,
    pair: ExactDatasetPair,
) -> tuple[str, dict[str, Any]] | None:
    legacy_header = await _locked_legacy_header(database, pair)
    rooted_header = await _locked_rooted_header(database, pair)
    current_headers = tuple(
        (variant, header)
        for variant, header in (
            (LEGACY_PRACTITIONER_VARIANT, legacy_header),
            (ROOTED_COMBINED_VARIANT, rooted_header),
        )
        if header
    )
    if len(current_headers) > 1:
        raise ProviderDirectoryDatasetScopedPublicationError("both_current")
    return current_headers[0] if current_headers else None


def _expected_variant_coordinates(
    pair: ExactDatasetPair,
    variant: str,
) -> tuple[str, str]:
    if variant == LEGACY_PRACTITIONER_VARIANT:
        return pair.legacy_source_id, pair.legacy_endpoint_id
    return pair.rooted_source_id, pair.rooted_endpoint_id


async def _validate_locked_header(
    database: Any,
    pair: ExactDatasetPair,
    variant: str,
    header: dict[str, Any],
    parent: dict[str, Any],
    *,
    require_ready: bool = True,
) -> None:
    """Validate one locked known-variant header against its generic parent."""

    if type(require_ready) is not bool:
        raise ProviderDirectoryDatasetScopedPublicationError("state")
    expected_source_id, expected_endpoint_id = _expected_variant_coordinates(
        pair,
        variant,
    )
    _table_name, identity_function, ready_function = _variant_predicates(variant)
    is_identity_valid = await database.scalar(
        f"SELECT {_qualified(identity_function)}(:dataset_id);",
        dataset_id=header.get("dataset_id"),
    )
    is_ready = (
        await database.scalar(
            f"SELECT {_qualified(ready_function)}(:dataset_id);",
            dataset_id=header.get("dataset_id"),
        )
        if require_ready
        else None
    )
    has_matching_shared_values = (
        parent.get("endpoint_id") == header.get("endpoint_id")
        and parent.get("dataset_hash") == header.get("dataset_hash")
        and parent.get("resource_count") == header.get("resource_count")
    )
    if (
        exact_dataset_variant(header.get("dataset_id")) != variant
        or parent.get("status") != "published"
        or parent.get("is_current") is not True
        or header.get("root_source_id") != expected_source_id
        or header.get("root_endpoint_id") != expected_endpoint_id
        or header.get("acquisition_source_id") != pair.rooted_source_id
        or header.get("acquisition_endpoint_id") != pair.rooted_endpoint_id
        or header.get("root_publication_contract_id")
        != PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT[variant]
        or header.get("practitioner_origin_source_id") != pair.legacy_source_id
        or header.get("practitioner_origin_endpoint_id") != pair.legacy_endpoint_id
        or header.get("source_id") != expected_source_id
        or header.get("endpoint_id") != expected_endpoint_id
        or header.get("status") != "published"
        or header.get("is_current") is not True
        or not has_matching_shared_values
        or is_identity_valid is not True
        or (require_ready and is_ready is not True)
    ):
        raise ProviderDirectoryDatasetScopedPublicationError("foreign_current")


def _exact_current_from_header(
    variant: str,
    header: dict[str, Any],
    parent: dict[str, Any],
) -> ExactCurrentDataset:
    try:
        return ExactCurrentDataset(
            dataset_id=header.get("dataset_id"),
            endpoint_id=header.get("endpoint_id"),
            source_id=header.get("source_id"),
            root_source_id=header.get("root_source_id"),
            root_endpoint_id=header.get("root_endpoint_id"),
            acquisition_source_id=header.get("acquisition_source_id"),
            acquisition_endpoint_id=header.get("acquisition_endpoint_id"),
            practitioner_origin_source_id=header.get("practitioner_origin_source_id"),
            practitioner_origin_endpoint_id=header.get(
                "practitioner_origin_endpoint_id"
            ),
            source_authority_id=header.get("source_authority_id"),
            endpoint_signature_sha256=header.get("endpoint_signature_sha256"),
            dataset_hash=parent.get("dataset_hash"),
            resource_count=parent.get("resource_count"),
            practitioner_resource_count=header.get("practitioner_resource_count"),
            root_content_proof_sha256=header.get("root_content_proof_sha256"),
            root_cohort_id=header.get("root_cohort_id"),
            semantic_projection_as_of=_projection_text(
                header.get("semantic_projection_as_of")
            ),
            operation_key=header.get("operation_key"),
            acquisition_root_run_id=header.get("acquisition_root_run_id"),
            variant=variant,
            root_publication_contract_id=header.get("root_publication_contract_id"),
        )
    except ValueError:
        raise ProviderDirectoryDatasetScopedPublicationError("state") from None


async def lock_exact_current_dataset(
    database: Any,
    *,
    pair: ExactDatasetPair,
    require_ready: bool = True,
) -> ExactCurrentDataset | None:
    """Lock one exact current, optionally admitting a stale known identity."""

    if (
        type(pair) is not ExactDatasetPair
        or pair != exact_uhc_dataset_pair()
        or type(require_ready) is not bool
    ):
        raise ProviderDirectoryDatasetScopedPublicationError("state")
    await _lock_pair_registry(database, pair)
    parent_by_id = await _locked_parent_by_id(database, pair)
    current_header = await _locked_current_header(database, pair)
    if current_header is None:
        if parent_by_id:
            raise ProviderDirectoryDatasetScopedPublicationError("foreign_current")
        return None
    variant, header = current_header
    dataset_id = header.get("dataset_id")
    parent = parent_by_id.get(dataset_id)
    if len(parent_by_id) != 1 or parent is None:
        raise ProviderDirectoryDatasetScopedPublicationError("foreign_current")
    await _validate_locked_header(
        database,
        pair,
        variant,
        header,
        parent,
        require_ready=require_ready,
    )
    return _exact_current_from_header(variant, header, parent)


async def supersede_exact_current_dataset(
    database: Any,
    current: ExactCurrentDataset | None,
) -> None:
    """Supersede the exact dedicated header and generic pointer together."""

    if current is None:
        return
    if type(current) is not ExactCurrentDataset:
        raise ProviderDirectoryDatasetScopedPublicationError("state")
    locked = await lock_exact_current_dataset(
        database,
        pair=exact_uhc_dataset_pair(),
        require_ready=False,
    )
    if locked is None or locked != current:
        raise ProviderDirectoryDatasetScopedPublicationError("foreign_current")
    table_name, _ready_function = _variant_table_and_ready(locked.variant)
    dedicated_updated = await database.status(
        f"""
        UPDATE {_qualified(table_name)}
           SET status = 'superseded', is_current = false,
               superseded_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND endpoint_id = :endpoint_id
           AND source_id = :source_id AND status = 'published'
           AND is_current IS TRUE AND dataset_hash = :dataset_hash
           AND resource_count = :resource_count
           AND publication_contract_id = :root_publication_contract_id;
        """,
        dataset_id=locked.dataset_id,
        endpoint_id=locked.endpoint_id,
        source_id=locked.source_id,
        dataset_hash=locked.dataset_hash,
        resource_count=locked.resource_count,
        root_publication_contract_id=locked.root_publication_contract_id,
    )
    parent_updated = await database.status(
        f"""
        UPDATE {_qualified(ENDPOINT_DATASET_TABLE)}
           SET status = 'superseded', is_current = false,
               superseded_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND endpoint_id = :endpoint_id
           AND status = 'published' AND is_current IS TRUE
           AND dataset_hash = :dataset_hash AND resource_count = :resource_count;
        """,
        dataset_id=locked.dataset_id,
        endpoint_id=locked.endpoint_id,
        dataset_hash=locked.dataset_hash,
        resource_count=locked.resource_count,
    )
    if dedicated_updated != 1 or parent_updated != 1:
        raise ProviderDirectoryDatasetScopedPublicationError("state")


__all__ = (
    "exact_dataset_variant",
    "exact_current_matches_root",
    "exact_uhc_dataset_pair",
    "lock_exact_current_dataset",
    "supersede_exact_current_dataset",
    "ExactCurrentDataset",
    "ExactDatasetPair",
    "EXACT_DATASET_PUBLICATION_LOCK_IDENTITY",
    "LEGACY_DATASET_PATTERN",
    "LEGACY_PRACTITIONER_VARIANT",
    "ProviderDirectoryDatasetScopedPublicationError",
    "ROOTED_COMBINED_VARIANT",
    "ROOTED_DATASET_PATTERN",
)
