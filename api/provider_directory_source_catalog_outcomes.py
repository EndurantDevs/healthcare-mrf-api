# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Latest and current outcome enrichment for Provider Directory sources."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from api import provider_directory_source_outcomes as outcomes
from api.provider_directory_rooted_fhir_publication import (
    is_rooted_fhir_catalog_entry,
    rooted_fhir_publication_summary,
    ROOTED_FHIR_PUBLICATION_FIELD,
    ROOTED_FHIR_SOURCE_ID_GROUP,
)
from api.provider_directory_source_dataset_selection import (
    _source_local_current_published_dataset_statement,
)
from db.models import db
from process.provider_directory_rooted_graph_publication_facade import (
    load_provider_directory_rooted_graph_dataset_readiness,
)
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
    AUTOMATIC_VALIDATED_PUBLICATION_ROLE,
    ValidatedPublicationCandidate,
    canonical_utc_timestamp,
    validated_publication_source_status,
)


def _canonical_identity_text(value: Any, *, limit: int) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text != value or len(text) > limit:
        return None
    return text


def _profile_current_dataset_from_row(
    dataset_record: Mapping[str, Any],
    expected_source_id_groups: set[tuple[str, ...]],
) -> outcomes._CurrentPublishedDataset | None:
    """Validate the identity required by authoritative Profile selection."""

    dataset = outcomes._current_dataset_from_row(
        dataset_record,
        expected_source_id_groups,
    )
    dataset_hash = _canonical_identity_text(
        dataset_record.get("dataset_hash"),
        limit=64,
    )
    root_run_id = _canonical_identity_text(
        dataset_record.get("acquisition_root_run_id"),
        limit=64,
    )
    if (
        dataset is None
        or dataset.status != "published"
        or dataset.is_current is not True
        or _canonical_identity_text(dataset_record.get("endpoint_id"), limit=128)
        != dataset.endpoint_id
        or _canonical_identity_text(dataset_record.get("dataset_id"), limit=128)
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


def _catalog_source_id_groups(
    catalog_items: list[dict[str, Any]],
) -> set[tuple[str, ...]]:
    return {
        source_ids
        for catalog_entry in catalog_items
        if (
            source_ids := outcomes._normalized_text_tuple(
                catalog_entry.get("source_ids")
            )
        )
        is not None
    }


def _current_outcome_summary(
    dataset: outcomes._CurrentPublishedDataset,
) -> dict[str, Any]:
    """Expose the published incumbent plus its authoritative lineage identity."""

    return {
        **outcomes._outcome_summary(dataset),
        "endpoint_id": dataset.endpoint_id,
        "acquisition_root_run_id": dataset.acquisition_root_run_id,
        "dataset_hash": dataset.dataset_hash,
    }


async def _canonical_validated_datasets_by_source_id(
    source_ids: list[str],
) -> dict[str, Any]:
    """Resolve proof-bearing candidates in the artifact publisher's query."""

    if not source_ids:
        return {}

    from process.provider_directory_fhir import (
        _resolve_provider_directory_artifact_datasets,
    )

    try:
        fence = await _resolve_provider_directory_artifact_datasets(
            source_ids,
            should_select_validated_candidates=True,
        )
    except RuntimeError:
        return {}
    return {
        dataset.source_id: dataset
        for dataset in fence.datasets
        if dataset.promote_on_cutover
    }


def _validated_publication_contract_from_outcomes(
    source_id: str,
    candidate_dataset: outcomes._CurrentPublishedDataset,
    incumbent_dataset: outcomes._CurrentPublishedDataset | None,
    canonical_dataset: Any,
) -> ValidatedPublicationCandidate | None:
    """Parse the observed candidate and expected current through the schema."""

    candidate_payload_map = {
        "source_id": source_id,
        "endpoint_id": candidate_dataset.endpoint_id,
        "dataset_id": candidate_dataset.dataset_id,
        "dataset_hash": candidate_dataset.dataset_hash,
        "acquisition_root_run_id": candidate_dataset.acquisition_root_run_id,
        "validated_at": canonical_utc_timestamp(candidate_dataset.validated_at),
        "automatic_publication_policy": (
            AUTOMATIC_VALIDATED_PUBLICATION_POLICY
        ),
        "completion_proof_required_version": (
            canonical_dataset.completion_proof_required_version
        ),
        "completion_proof_sha256": canonical_dataset.completion_proof_sha256,
        "verification_campaign_id": canonical_dataset.verification_campaign_id,
        "verification_source_scope_sha256": (
            canonical_dataset.verification_source_scope_hash
        ),
        "expected_current": (
            {
                "endpoint_id": incumbent_dataset.endpoint_id,
                "dataset_id": incumbent_dataset.dataset_id,
                "dataset_hash": incumbent_dataset.dataset_hash,
                "acquisition_root_run_id": (
                    incumbent_dataset.acquisition_root_run_id
                ),
            }
            if incumbent_dataset is not None
            else None
        ),
    }
    try:
        return ValidatedPublicationCandidate.from_payload(candidate_payload_map)
    except ValueError:
        return None


def _validated_publication_candidate_payload(
    source_id: str,
    candidate_dataset: outcomes._CurrentPublishedDataset,
    incumbent_dataset: outcomes._CurrentPublishedDataset | None,
    canonical_dataset: Any,
) -> dict[str, Any] | None:
    """Return a closed candidate identity only when every fence agrees."""

    publication_candidate = _validated_publication_contract_from_outcomes(
        source_id,
        candidate_dataset,
        incumbent_dataset,
        canonical_dataset,
    )
    if publication_candidate is None:
        return None
    expected_current = publication_candidate.expected_current
    expected_current_dataset_id = (
        expected_current.dataset_id if expected_current else None
    )
    if (
        candidate_dataset.source_ids != (source_id,)
        or candidate_dataset.status != "validated"
        or candidate_dataset.is_current is not False
        or candidate_dataset.previous_dataset_id != expected_current_dataset_id
        or canonical_dataset.source_id != source_id
        or canonical_dataset.endpoint_id != publication_candidate.endpoint_id
        or canonical_dataset.dataset_id != publication_candidate.dataset_id
        or canonical_dataset.dataset_hash != publication_candidate.dataset_hash
        or canonical_dataset.evidence_run_id
        != publication_candidate.acquisition_root_run_id
        or canonical_utc_timestamp(canonical_dataset.validated_at)
        != publication_candidate.validated_at
        or canonical_dataset.status != "validated"
        or canonical_dataset.is_current is not False
        or canonical_dataset.expected_incumbent_dataset_id
        != expected_current_dataset_id
        or canonical_dataset.completion_proof_required_version
        != publication_candidate.completion_proof_required_version
        or canonical_dataset.completion_proof_sha256
        != publication_candidate.completion_proof_sha256
        or canonical_dataset.verification_source_status
        != validated_publication_source_status(publication_candidate)
        or canonical_dataset.verification_campaign_id
        != publication_candidate.verification_campaign_id
        or canonical_dataset.verification_source_scope_hash
        != publication_candidate.verification_source_scope_sha256
        or canonical_dataset.verification_source_ids != (source_id,)
        or canonical_dataset.reviewed_root_policy is not None
    ):
        return None
    if (expected_current is None) != (incumbent_dataset is None):
        return None
    if incumbent_dataset is not None and (
        incumbent_dataset.status != "published"
        or incumbent_dataset.is_current is not True
    ):
        return None
    return publication_candidate.to_payload()


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
        if (
            catalog_entry.get("runnable") is True
            and catalog_entry.get("classification") == "acquisition"
            and source_ids is not None
            and len(source_ids) == 1
            and dataset is not None
            and dataset.status == "validated"
            and dataset.is_current is False
            and dataset.publication_metadata.get(
                "requires_twin_root_verification"
            )
            is True
            and dataset.publication_metadata.get("verification_role")
            == AUTOMATIC_VALIDATED_PUBLICATION_ROLE
        ):
            automatic_source_ids.add(source_ids[0])
    return sorted(automatic_source_ids)


def _catalog_validated_publication_candidate(
    catalog_entry: Mapping[str, Any],
    source_ids: tuple[str, ...] | None,
    candidate_dataset: outcomes._CurrentPublishedDataset | None,
    incumbent_dataset: outcomes._CurrentPublishedDataset | None,
    canonical_dataset_by_source_id: Mapping[str, Any],
) -> dict[str, Any] | None:
    if (
        catalog_entry.get("runnable") is not True
        or catalog_entry.get("classification") != "acquisition"
        or source_ids is None
        or len(source_ids) != 1
        or candidate_dataset is None
        or candidate_dataset.status != "validated"
        or candidate_dataset.is_current is not False
    ):
        return None
    canonical_dataset = canonical_dataset_by_source_id.get(source_ids[0])
    if canonical_dataset is None:
        return None
    return _validated_publication_candidate_payload(
        source_ids[0],
        candidate_dataset,
        incumbent_dataset,
        canonical_dataset,
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


def _enriched_catalog_entry(
    catalog_entry: Mapping[str, Any],
    dataset_by_source_ids: Mapping[
        tuple[str, ...], outcomes._CurrentPublishedDataset
    ],
    current_dataset_by_source_ids: Mapping[
        tuple[str, ...], outcomes._CurrentPublishedDataset
    ],
    canonical_dataset_by_source_id: Mapping[str, Any],
    rooted_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach every proven outcome to one catalog entry."""

    enriched_entry_map = dict(catalog_entry)
    source_ids = outcomes._normalized_text_tuple(
        catalog_entry.get("source_ids")
    )
    dataset = dataset_by_source_ids.get(source_ids or ())
    if dataset is not None:
        enriched_entry_map["outcome_summary"] = outcomes._outcome_summary(
            dataset
        )
    current_dataset = current_dataset_by_source_ids.get(source_ids or ())
    if current_dataset is not None:
        enriched_entry_map["current_outcome_summary"] = (
            _current_outcome_summary(current_dataset)
        )
    candidate_payload_map = _catalog_validated_publication_candidate(
        catalog_entry,
        source_ids,
        dataset,
        current_dataset,
        canonical_dataset_by_source_id,
    )
    if candidate_payload_map is not None:
        enriched_entry_map["validated_publication_candidate"] = (
            candidate_payload_map
        )
    if (
        rooted_summary is not None
        and is_rooted_fhir_catalog_entry(catalog_entry)
    ):
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
    source_id_groups = _catalog_source_id_groups(catalog_items)
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
    canonical_dataset_by_source_id = (
        await _canonical_validated_datasets_by_source_id(
            _automatic_publication_source_ids(
                catalog_items,
                dataset_by_source_ids,
            )
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
            canonical_dataset_by_source_id,
            rooted_summary,
        )
        for catalog_entry in catalog_items
    ]
    return {**dict(catalog), "items": enriched_items}
