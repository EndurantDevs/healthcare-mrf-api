# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Latest and current outcome enrichment for Provider Directory sources."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from api import provider_directory_source_outcomes as outcomes
from api.provider_directory_reviewed_publication import (
    reviewed_publication_context,
)
from api.provider_directory_source_catalog_projection import (
    canonical_identity_text,
    catalog_source_id_groups,
    current_outcome_summary,
)
from api.provider_directory_rooted_fhir_publication import (
    is_rooted_fhir_catalog_entry,
    rooted_fhir_publication_summary,
    ROOTED_FHIR_PUBLICATION_FIELD,
    ROOTED_FHIR_SOURCE_ID_GROUP,
)
from api.provider_directory_source_dataset_selection import (
    _current_dataset_identities_by_id,
    _dataset_identity,
    _source_local_current_published_dataset_statement,
)
from api.provider_directory_sources import RUNNABLE_CLASSIFICATIONS
from db.models import db
from process.provider_directory_rooted_graph_publication_facade import (
    load_provider_directory_rooted_graph_dataset_readiness,
)
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_VALIDATED_PUBLICATION_ROLE,
    ProviderDirectoryDatasetIdentity,
)
from process.provider_directory_fhir_root_policy import (
    LEGACY_VERIFIED_STATUS,
    ReviewedRootPolicy,
)
from process.provider_directory_validated_publication_catalog import (
    validated_publication_candidate_payload,
)


def _profile_current_dataset_from_row(
    dataset_record: Mapping[str, Any],
    expected_source_id_groups: set[tuple[str, ...]],
) -> outcomes._CurrentPublishedDataset | None:
    """Validate the identity required by authoritative Profile selection."""

    dataset = outcomes._current_dataset_from_row(
        dataset_record,
        expected_source_id_groups,
    )
    dataset_hash = canonical_identity_text(
        dataset_record.get("dataset_hash"),
        limit=64,
    )
    root_run_id = canonical_identity_text(
        dataset_record.get("acquisition_root_run_id"),
        limit=64,
    )
    if (
        dataset is None
        or dataset.status != "published"
        or dataset.is_current is not True
        or canonical_identity_text(dataset_record.get("endpoint_id"), limit=128)
        != dataset.endpoint_id
        or canonical_identity_text(dataset_record.get("dataset_id"), limit=128)
        != dataset.dataset_id
        or root_run_id != dataset.acquisition_root_run_id
        or root_run_id is None
        or dataset_hash != dataset.dataset_hash
        or dataset_hash is None
        or len(dataset_hash) != 64
        or any(character not in "0123456789abcdef" for character in dataset_hash)
    ):
        return None
    return dataset


async def _profile_current_dataset_by_source_ids(
    source_id_groups: set[tuple[str, ...]],
) -> dict[tuple[str, ...], outcomes._CurrentPublishedDataset]:
    """Read exact current published identities using Profile state semantics."""

    if not source_id_groups:
        return {}
    query_result = await db.execute(
        _source_local_current_published_dataset_statement(source_id_groups)
    )
    selected_by_source_ids: dict[
        tuple[str, ...],
        outcomes._CurrentPublishedDataset,
    ] = {}
    for dataset_record in query_result.mappings().all():
        candidate = _profile_current_dataset_from_row(
            dataset_record,
            source_id_groups,
        )
        if candidate is None:
            continue
        selected = selected_by_source_ids.get(candidate.source_ids)
        if selected is None or outcomes._dataset_order_key(
            candidate
        ) > outcomes._dataset_order_key(selected):
            selected_by_source_ids[candidate.source_ids] = candidate
    return selected_by_source_ids


async def _canonical_validated_datasets_by_source_id(
    source_ids: list[str],
) -> dict[str, Any]:
    """Resolve proof-bearing candidates in the artifact publisher's query."""

    if not source_ids:
        return {}

    from process.provider_directory_fhir import (
        _qt,
        _resolve_provider_directory_artifact_datasets,
        _schema,
    )

    dataset_by_source_id: dict[str, Any] = {}
    for source_id in source_ids:
        try:
            fence = await _resolve_provider_directory_artifact_datasets(
                [source_id],
                should_select_validated_candidates=True,
            )
        except RuntimeError:
            continue
        for dataset in fence.datasets:
            if (
                dataset.source_id != source_id
                or not dataset.promote_on_cutover
            ):
                continue
            is_reviewed_activation = bool(
                dataset.reviewed_root_policy
                in {ReviewedRootPolicy(1), ReviewedRootPolicy(2)}
                or (
                    dataset.reviewed_root_policy is None
                    and dataset.verification_source_status
                    == LEGACY_VERIFIED_STATUS
                    and dataset.completion_proof_required_version == 3
                )
            )
            if is_reviewed_activation:
                try:
                    activation_valid = await db.scalar(
                        "SELECT "
                        + _qt(
                            _schema(),
                            "provider_directory_reviewed_subset_activation_valid",
                        )
                        + "(:source_id);",
                        source_id=source_id,
                    )
                except Exception:
                    continue
                if activation_valid is not True:
                    continue
            dataset_by_source_id[dataset.source_id] = dataset
    return dataset_by_source_id


def _automatic_publication_source_ids(
    catalog_items: list[dict[str, Any]],
    dataset_by_source_ids: Mapping[
        tuple[str, ...], outcomes._CurrentPublishedDataset
    ],
) -> list[str]:
    automatic_source_ids: set[str] = set()
    for catalog_entry in catalog_items:
        source_ids = outcomes._normalized_text_tuple(
            catalog_entry.get("source_ids")
        )
        dataset = dataset_by_source_ids.get(source_ids or ())
        is_runnable_acquisition = bool(
            catalog_entry.get("runnable") is True
            and catalog_entry.get("classification") in RUNNABLE_CLASSIFICATIONS
        )
        is_reviewed_manual = bool(
            catalog_entry.get("runnable") is False
            and catalog_entry.get("classification") == "manual_acquisition"
        )
        if (
            source_ids is not None
            and len(source_ids) == 1
            and (
                is_reviewed_manual
                or (
                    is_runnable_acquisition
                    and dataset is not None
                    and dataset.status == "validated"
                    and dataset.is_current is False
                    and (
                        (
                            dataset.publication_metadata.get(
                                "requires_twin_root_verification"
                            )
                            is True
                            and dataset.publication_metadata.get(
                                "verification_role"
                            )
                            == AUTOMATIC_VALIDATED_PUBLICATION_ROLE
                        )
                        or (
                            dataset.publication_metadata.get(
                                "requires_twin_root_verification"
                            )
                            is not True
                            and dataset.publication_metadata.get(
                                "verification_role"
                            )
                            is None
                        )
                    )
                )
            )
        ):
            automatic_source_ids.add(source_ids[0])
    return sorted(automatic_source_ids)


async def _legacy_current_identities_by_source_ids(
    dataset_by_source_ids: Mapping[
        tuple[str, ...], outcomes._CurrentPublishedDataset
    ],
    current_dataset_by_source_ids: Mapping[
        tuple[str, ...], outcomes._CurrentPublishedDataset
    ],
    canonical_dataset_by_source_id: Mapping[str, Any],
) -> dict[tuple[str, ...], ProviderDirectoryDatasetIdentity]:
    """Bind sealed candidates to scalar identities of legacy incumbents."""

    candidates_by_source_ids = {
        source_ids: dataset
        for source_ids, dataset in dataset_by_source_ids.items()
        if source_ids not in current_dataset_by_source_ids
        and len(source_ids) == 1
        and source_ids[0] in canonical_dataset_by_source_id
        and dataset.previous_dataset_id is not None
    }
    identities_by_id = await _current_dataset_identities_by_id(
        {
            dataset.previous_dataset_id
            for dataset in candidates_by_source_ids.values()
        }
    )
    return {
        source_ids: identity
        for source_ids, dataset in candidates_by_source_ids.items()
        if (identity := identities_by_id.get(dataset.previous_dataset_id))
        is not None
        and identity.endpoint_id == dataset.endpoint_id
    }


def _catalog_validated_publication_candidate(
    catalog_entry: Mapping[str, Any],
    source_ids: tuple[str, ...] | None,
    candidate_dataset: outcomes._CurrentPublishedDataset | None,
    incumbent_identity: ProviderDirectoryDatasetIdentity | None,
    canonical_dataset_by_source_id: Mapping[str, Any],
) -> dict[str, Any] | None:
    if (
        source_ids is None
        or len(source_ids) != 1
        or candidate_dataset is None
        or candidate_dataset.status != "validated"
        or candidate_dataset.is_current is not False
    ):
        return None
    canonical_dataset = canonical_dataset_by_source_id.get(source_ids[0])
    if canonical_dataset is None:
        return None
    is_runnable_acquisition = bool(
        catalog_entry.get("runnable") is True
        and catalog_entry.get("classification") in RUNNABLE_CLASSIFICATIONS
        and canonical_dataset.reviewed_root_policy is None
    )
    is_manual_legacy_reviewed = bool(
        catalog_entry.get("runnable") is False
        and catalog_entry.get("classification") == "manual_acquisition"
        and canonical_dataset.reviewed_root_policy is None
        and canonical_dataset.verification_source_status
        == LEGACY_VERIFIED_STATUS
        and canonical_dataset.completion_proof_required_version == 3
    )
    is_reviewed_manual = bool(
        catalog_entry.get("runnable") is False
        and catalog_entry.get("classification") == "manual_acquisition"
        and (
            canonical_dataset.reviewed_root_policy
            in {ReviewedRootPolicy(1), ReviewedRootPolicy(2)}
            or is_manual_legacy_reviewed
        )
    )
    if not (is_runnable_acquisition or is_reviewed_manual):
        return None
    return validated_publication_candidate_payload(
        source_ids[0],
        candidate_dataset,
        incumbent_identity,
        canonical_dataset,
        manual_legacy_reviewed=is_manual_legacy_reviewed,
    )


async def _rooted_summary_for_catalog(
    current_dataset_by_source_ids: dict[
        tuple[str, ...],
        outcomes._CurrentPublishedDataset,
    ],
) -> dict[str, Any]:
    rooted_current_dataset = current_dataset_by_source_ids.get(
        ROOTED_FHIR_SOURCE_ID_GROUP
    )
    rooted_readiness = (
        await load_provider_directory_rooted_graph_dataset_readiness(
            rooted_current_dataset.dataset_id
        )
        if rooted_current_dataset is not None
        else None
    )
    return rooted_fhir_publication_summary(
        rooted_current_dataset,
        rooted_readiness,
    )


def _publication_candidate_context(
    catalog_entry,
    source_ids,
    dataset,
    current_dataset,
    legacy_current_identity,
    canonical_dataset,
    canonical_dataset_by_source_id,
):
    candidate_dataset, candidate_incumbent, reviewed_dataset = (
        reviewed_publication_context(
            catalog_entry,
            source_ids,
            canonical_dataset,
            dataset,
            _dataset_identity(current_dataset),
            legacy_current_identity,
        )
        if canonical_dataset is not None
        else (
            dataset,
            _dataset_identity(current_dataset) or legacy_current_identity,
            None,
        )
    )
    return (
        _catalog_validated_publication_candidate(
            catalog_entry,
            source_ids,
            candidate_dataset,
            candidate_incumbent,
            canonical_dataset_by_source_id,
        ),
        reviewed_dataset,
    )


def _enriched_catalog_entry(
    catalog_entry: Mapping[str, Any],
    dataset_by_source_ids: Mapping[
        tuple[str, ...], outcomes._CurrentPublishedDataset
    ],
    current_dataset_by_source_ids: Mapping[
        tuple[str, ...], outcomes._CurrentPublishedDataset
    ],
    legacy_current_identity_by_source_ids: Mapping[
        tuple[str, ...], ProviderDirectoryDatasetIdentity
    ],
    canonical_dataset_by_source_id: Mapping[str, Any],
    rooted_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach every proven outcome to one catalog entry."""

    enriched_entry_map = dict(catalog_entry)
    source_ids = outcomes._normalized_text_tuple(catalog_entry.get("source_ids"))
    dataset = dataset_by_source_ids.get(source_ids or ())
    canonical_dataset = (
        canonical_dataset_by_source_id.get(source_ids[0])
        if source_ids is not None and len(source_ids) == 1
        else None
    )
    if dataset is not None:
        enriched_entry_map["outcome_summary"] = outcomes._outcome_summary(dataset)
    current_dataset = current_dataset_by_source_ids.get(source_ids or ())
    legacy_current_identity = legacy_current_identity_by_source_ids.get(source_ids or ())
    if current_dataset is not None:
        enriched_entry_map["current_outcome_summary"] = (
            current_outcome_summary(current_dataset)
        )
    candidate_payload_map, reviewed_candidate_dataset = _publication_candidate_context(
        catalog_entry,
        source_ids,
        dataset,
        current_dataset,
        legacy_current_identity,
        canonical_dataset,
        canonical_dataset_by_source_id,
    )
    if candidate_payload_map is not None:
        if reviewed_candidate_dataset is not None:
            enriched_entry_map["outcome_summary"] = outcomes._outcome_summary(
                reviewed_candidate_dataset
            )
        if current_dataset is None and legacy_current_identity is not None:
            enriched_entry_map["current_outcome_summary"] = {
                **legacy_current_identity.to_payload(),
                "status": "published",
                "is_current": True,
            }
        enriched_entry_map["validated_publication_candidate"] = (
            candidate_payload_map
        )
    if rooted_summary is not None and is_rooted_fhir_catalog_entry(catalog_entry):
        enriched_entry_map[ROOTED_FHIR_PUBLICATION_FIELD] = rooted_summary
    return enriched_entry_map


async def enrich_provider_directory_source_catalog(
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach latest sealed and current published source outcome summaries."""

    raw_items = catalog.get("items")
    if not isinstance(raw_items, list):
        return dict(catalog)
    catalog_items = [
        dict(catalog_entry)
        for catalog_entry in raw_items
        if isinstance(catalog_entry, Mapping)
    ]
    source_id_groups = catalog_source_id_groups(catalog_items)
    has_rooted_fhir_entry = any(
        is_rooted_fhir_catalog_entry(catalog_entry)
        for catalog_entry in catalog_items
    )
    if has_rooted_fhir_entry:
        source_id_groups.add(ROOTED_FHIR_SOURCE_ID_GROUP)
    dataset_by_source_ids = await outcomes._current_published_dataset_by_source_ids(
        source_id_groups
    )
    current_dataset_by_source_ids = await _profile_current_dataset_by_source_ids(
        source_id_groups
    )
    automatic_source_ids = _automatic_publication_source_ids(
        catalog_items,
        dataset_by_source_ids,
    )
    canonical_dataset_by_source_id = (
        await _canonical_validated_datasets_by_source_id(
            automatic_source_ids
        )
    )
    legacy_current_identity_by_source_ids = (
        await _legacy_current_identities_by_source_ids(
            dataset_by_source_ids,
            current_dataset_by_source_ids,
            canonical_dataset_by_source_id,
        )
    )
    rooted_summary = (
        await _rooted_summary_for_catalog(current_dataset_by_source_ids)
        if has_rooted_fhir_entry
        else None
    )

    enriched_items = [
        _enriched_catalog_entry(
            catalog_entry,
            dataset_by_source_ids,
            current_dataset_by_source_ids,
            legacy_current_identity_by_source_ids,
            canonical_dataset_by_source_id,
            rooted_summary,
        )
        for catalog_entry in catalog_items
    ]
    return {**dict(catalog), "items": enriched_items}
