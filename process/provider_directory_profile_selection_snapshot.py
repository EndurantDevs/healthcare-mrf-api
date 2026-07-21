# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Locked database snapshot for global Provider Directory Profile selection."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping

from db.models import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryEndpointDataset,
    ProviderDirectorySource,
    db,
)
from process import provider_directory_profile as profile_artifact
from process.provider_directory_profile_selection_contract import (
    PROFILE_EXECUTION_CONTRACT_ID,
    PROFILE_SELECTION_ATTESTATION_CONTRACT_ID,
    PROFILE_SELECTION_LINEAGE_AUTHORITY,
    _GLOBAL_PROFILE_PARAMS,
    _clean_text,
    _required_hash,
    _required_text,
    stable_hash,
)


@dataclass(frozen=True)
class _ComputedProfileSelection:
    """Current request projection plus authoritative proof identity fields."""

    request_projection: tuple[dict[str, str], ...]
    identity_payload: dict[str, Any]


@dataclass(frozen=True)
class _SelectionRecords:
    """Canonical pair, context, and full-input records for one snapshot."""

    pairs: list[dict[str, Any]]
    profile_inputs: list[dict[str, Any]]
    source_contexts: list[dict[str, Any]]
    request_projection: list[dict[str, str]]


def _row_mapping(database_row: Any) -> Mapping[str, Any]:
    row_mapping = getattr(database_row, "_mapping", None)
    if isinstance(row_mapping, Mapping):
        return row_mapping
    if isinstance(database_row, Mapping):
        return database_row
    return {}


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table_ref(model: Any) -> str:
    return f"{_quote_identifier(_schema())}.{_quote_identifier(model.__tablename__)}"


def _catalog_source_groups(
    catalog_map: Mapping[str, Any],
) -> tuple[tuple[str, ...], ...]:
    catalog_digest = catalog_map.get("catalog_digest")
    catalog_entries = catalog_map.get("items")
    if (
        not isinstance(catalog_digest, str)
        or len(catalog_digest) != 64
        or not isinstance(catalog_entries, list)
    ):
        raise RuntimeError("provider_directory_profile_selection_catalog_invalid")
    source_groups: list[tuple[str, ...]] = []
    seen_source_ids: set[str] = set()
    for catalog_entry in catalog_entries:
        if (
            not isinstance(catalog_entry, Mapping)
            or catalog_entry.get("runnable") is not True
            or catalog_entry.get("profile_enabled") is not True
        ):
            continue
        raw_source_ids = catalog_entry.get("source_ids")
        if not isinstance(raw_source_ids, list) or not raw_source_ids:
            raise RuntimeError("provider_directory_profile_selection_catalog_invalid")
        source_ids = tuple(sorted(
            _required_text({"source_id": source_id}, "source_id", limit=96)
            for source_id in raw_source_ids
        ))
        if len(source_ids) != len(set(source_ids)) or seen_source_ids.intersection(source_ids):
            raise RuntimeError("provider_directory_profile_selection_catalog_invalid")
        seen_source_ids.update(source_ids)
        source_groups.append(source_ids)
    return tuple(sorted(source_groups))


async def _lock_profile_selection_tables() -> None:
    """Freeze every table that can alter the global current selection."""

    await db.status(f"LOCK TABLE {_table_ref(ProviderDirectoryAPIEndpoint)} IN SHARE MODE;")
    await db.all(
        f"SELECT endpoint_id FROM {_table_ref(ProviderDirectoryAPIEndpoint)} "
        "ORDER BY endpoint_id FOR SHARE;"
    )
    await db.status(f"LOCK TABLE {_table_ref(ProviderDirectorySource)} IN SHARE MODE;")
    await db.status(
        f"LOCK TABLE {_table_ref(ProviderDirectoryEndpointDataset)} IN SHARE MODE;"
    )


async def _selection_source_rows() -> list[Mapping[str, Any]]:
    database_rows = await db.all(
        f"""
        SELECT source_id, endpoint_id, canonical_api_base, org_name, plan_name
          FROM {_table_ref(ProviderDirectorySource)}
         ORDER BY source_id;
        """
    )
    return [_row_mapping(database_row) for database_row in database_rows]


async def _selection_dataset_rows() -> list[Mapping[str, Any]]:
    database_rows = await db.all(
        f"""
        SELECT endpoint_id, dataset_id, acquisition_root_run_id, dataset_hash,
               status, is_current, resource_count, validated_at, published_at,
               superseded_at, publication_metadata_json
          FROM {_table_ref(ProviderDirectoryEndpointDataset)}
         WHERE status = 'published'
           AND is_current = true
           AND published_at IS NOT NULL
           AND superseded_at IS NULL
         ORDER BY published_at DESC, dataset_id DESC, endpoint_id DESC;
        """
    )
    return [_row_mapping(database_row) for database_row in database_rows]


def _metadata_source_ids(metadata_map: Any) -> tuple[str, ...] | None:
    if not isinstance(metadata_map, Mapping):
        return None
    raw_source_ids = metadata_map.get("source_ids")
    if not isinstance(raw_source_ids, list) or not raw_source_ids:
        return None
    source_ids = tuple(sorted(
        source_id
        for raw_source_id in raw_source_ids
        if (source_id := _clean_text(raw_source_id))
    ))
    if len(source_ids) != len(raw_source_ids) or len(source_ids) != len(set(source_ids)):
        return None
    return source_ids


def _source_selection_indexes(
    source_rows: list[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, set[str]]]:
    source_by_id: dict[str, Mapping[str, Any]] = {}
    source_ids_by_endpoint: dict[str, set[str]] = {}
    for source_row in source_rows:
        source_id = _clean_text(source_row.get("source_id"))
        endpoint_id = _clean_text(source_row.get("endpoint_id"))
        if source_id is None or source_id in source_by_id:
            raise RuntimeError("provider_directory_profile_selection_source_invalid")
        source_by_id[source_id] = source_row
        if endpoint_id is not None:
            source_ids_by_endpoint.setdefault(endpoint_id, set()).add(source_id)
    return source_by_id, source_ids_by_endpoint


def _dataset_selection_by_group(
    dataset_rows: list[Mapping[str, Any]],
    source_groups: tuple[tuple[str, ...], ...],
    source_ids_by_endpoint: Mapping[str, set[str]],
) -> dict[tuple[str, ...], Mapping[str, Any]]:
    dataset_by_group: dict[tuple[str, ...], Mapping[str, Any]] = {}
    for dataset_row in dataset_rows:
        source_group = _metadata_source_ids(
            dataset_row.get("publication_metadata_json")
        )
        endpoint_id = _clean_text(dataset_row.get("endpoint_id"))
        if (
            source_group in source_groups
            and endpoint_id is not None
            and set(source_group).issubset(
                source_ids_by_endpoint.get(endpoint_id, set())
            )
        ):
            dataset_by_group.setdefault(source_group, dataset_row)
    return dataset_by_group


def _canonical_scalar(scalar_value: Any) -> str | None:
    return str(scalar_value) if scalar_value is not None else None


def _selection_records_for_group(
    source_group: tuple[str, ...],
    dataset_row: Mapping[str, Any],
    source_by_id: Mapping[str, Mapping[str, Any]],
) -> _SelectionRecords:
    endpoint_id = _required_text(dataset_row, "endpoint_id", limit=128)
    dataset_id = _required_text(dataset_row, "dataset_id", limit=128)
    dataset_hash = _required_hash(dataset_row, "dataset_hash")
    root_run_id = _required_text(
        dataset_row,
        "acquisition_root_run_id",
        limit=64,
    )
    publication_metadata = dataset_row.get("publication_metadata_json")
    if not isinstance(publication_metadata, Mapping):
        raise RuntimeError("provider_directory_profile_selection_dataset_invalid")
    group_records = _SelectionRecords([], [], [], [])
    for source_id in source_group:
        source_row = source_by_id.get(source_id)
        if source_row is None or _clean_text(source_row.get("endpoint_id")) != endpoint_id:
            raise RuntimeError("provider_directory_profile_selection_source_invalid")
        pair_map = _selection_pair(
            source_id,
            endpoint_id,
            dataset_id,
            dataset_hash,
            root_run_id,
        )
        group_records.pairs.append(pair_map)
        group_records.request_projection.append(
            {"source_id": source_id, "dataset_id": dataset_id}
        )
        group_records.source_contexts.append(
            _source_context(source_id, endpoint_id, source_row)
        )
        group_records.profile_inputs.append(
            _profile_input(pair_map, dataset_row, publication_metadata)
        )
    return group_records


def _selection_pair(
    source_id: str,
    endpoint_id: str,
    dataset_id: str,
    dataset_hash: str,
    root_run_id: str,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "endpoint_id": endpoint_id,
        "dataset_id": dataset_id,
        "dataset_hash": dataset_hash,
        "acquisition_root_run_id": root_run_id,
        "publication_status": "published",
        "is_current": True,
        "lineage_authority": PROFILE_SELECTION_LINEAGE_AUTHORITY,
    }


def _source_context(
    source_id: str,
    endpoint_id: str,
    source_row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "endpoint_id": endpoint_id,
        "canonical_api_base": source_row.get("canonical_api_base"),
        "org_name": source_row.get("org_name"),
        "plan_name": source_row.get("plan_name"),
    }


def _profile_input(
    pair_map: Mapping[str, Any],
    dataset_row: Mapping[str, Any],
    publication_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **pair_map,
        "resource_count": int(dataset_row.get("resource_count") or 0),
        "validated_at": _canonical_scalar(dataset_row.get("validated_at")),
        "published_at": _canonical_scalar(dataset_row.get("published_at")),
        "publication_metadata_digest": stable_hash(
            publication_metadata,
            domain="provider_directory_profile_publication_metadata.v1",
        ),
    }


def _selection_records(
    source_groups: tuple[tuple[str, ...], ...],
    dataset_by_group: Mapping[tuple[str, ...], Mapping[str, Any]],
    source_by_id: Mapping[str, Mapping[str, Any]],
) -> _SelectionRecords:
    all_records = _SelectionRecords([], [], [], [])
    for source_group in source_groups:
        dataset_row = dataset_by_group.get(source_group)
        if dataset_row is None:
            continue
        group_records = _selection_records_for_group(
            source_group,
            dataset_row,
            source_by_id,
        )
        all_records.pairs.extend(group_records.pairs)
        all_records.profile_inputs.extend(group_records.profile_inputs)
        all_records.source_contexts.extend(group_records.source_contexts)
        all_records.request_projection.extend(group_records.request_projection)
    all_records.pairs.sort(key=lambda pair_map: (
        pair_map["source_id"], pair_map["dataset_id"], pair_map["endpoint_id"]
    ))
    all_records.request_projection.sort(
        key=lambda projection_map: (
            projection_map["source_id"],
            projection_map["dataset_id"],
        )
    )
    all_records.source_contexts.sort(
        key=lambda context_map: (context_map["source_id"], context_map["endpoint_id"])
    )
    all_records.profile_inputs.sort(key=lambda input_map: (
        input_map["source_id"], input_map["dataset_id"], input_map["endpoint_id"]
    ))
    return all_records


def _selection_identity(
    catalog_digest: str,
    node_id: str,
    selection_records: _SelectionRecords,
) -> dict[str, Any]:
    dataset_pairs = [
        [projection_map["source_id"], projection_map["dataset_id"]]
        for projection_map in selection_records.request_projection
    ]
    selection_fingerprint = stable_hash(
        {"node_id": node_id, "catalog_digest": catalog_digest, "datasets": dataset_pairs},
        domain="provider_directory_profile_current_selection",
    )
    source_context_digest = _source_context_digest(
        selection_records.source_contexts
    )
    profile_input_digest = _profile_input_digest(
        catalog_digest,
        source_context_digest,
        selection_records.profile_inputs,
    )
    return {
        "contract_id": PROFILE_SELECTION_ATTESTATION_CONTRACT_ID,
        "node_id": node_id,
        "catalog_digest": catalog_digest,
        "selection_fingerprint": selection_fingerprint,
        "profile_schema_version": profile_artifact.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": profile_artifact.PROFILE_BUILD_STRATEGY_VERSION,
        "source_context_digest": source_context_digest,
        "profile_input_digest": profile_input_digest,
        "operation": "publish" if selection_records.pairs else "purge",
        "pairs": selection_records.pairs,
    }


def _source_context_digest(
    source_contexts: list[dict[str, Any]],
) -> str:
    """Hash the exact source labels emitted by Profile evidence."""

    return stable_hash(
        source_contexts,
        domain="provider_directory_profile_source_context.v1",
    )


def _profile_input_digest(
    catalog_digest: str,
    source_context_digest: str,
    profile_inputs: list[dict[str, Any]],
) -> str:
    input_identity_map = {
        "catalog_digest": catalog_digest,
        "configured_profile_source_ids": list(
            profile_artifact.configured_profile_source_ids()
        ),
        "global_profile_params": _GLOBAL_PROFILE_PARAMS,
        "profile_inputs": profile_inputs,
        "profile_schema_version": profile_artifact.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": profile_artifact.PROFILE_BUILD_STRATEGY_VERSION,
        "source_context_digest": source_context_digest,
    }
    return stable_hash(
        input_identity_map,
        domain="provider_directory_profile_input.v1",
    )


def _computed_selection_from_rows(
    catalog_map: Mapping[str, Any],
    *,
    node_id: str,
    source_rows: list[Mapping[str, Any]],
    dataset_rows: list[Mapping[str, Any]],
) -> _ComputedProfileSelection:
    """Compute canonical proof identity fields from one immutable DB view."""

    source_groups = _catalog_source_groups(catalog_map)
    source_by_id, source_ids_by_endpoint = _source_selection_indexes(source_rows)
    dataset_by_group = _dataset_selection_by_group(
        dataset_rows,
        source_groups,
        source_ids_by_endpoint,
    )
    selection_records = _selection_records(
        source_groups,
        dataset_by_group,
        source_by_id,
    )
    return _ComputedProfileSelection(
        request_projection=tuple(selection_records.request_projection),
        identity_payload=_selection_identity(
            str(catalog_map["catalog_digest"]),
            node_id,
            selection_records,
        ),
    )


async def _compute_current_selection(
    catalog_map: Mapping[str, Any],
    *,
    node_id: str,
    lock_selection: bool,
) -> _ComputedProfileSelection:
    if lock_selection:
        await _lock_profile_selection_tables()
    return _computed_selection_from_rows(
        catalog_map,
        node_id=node_id,
        source_rows=await _selection_source_rows(),
        dataset_rows=await _selection_dataset_rows(),
    )
