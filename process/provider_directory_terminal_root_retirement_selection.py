# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Locked selection for one exact legacy Provider Directory root retirement."""

from __future__ import annotations

import re
from typing import Any, Mapping

from process.provider_directory_terminal_root_retirement_contract import (
    REQUIRED_CHILD_RELATIONS,
    RETIREMENT_EVIDENCE_FUNCTION,
    RETIREMENT_METADATA_KEY,
    RETIREMENT_STATUS,
    TERMINAL_FAILURE_STATUSES,
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
    TerminalRootRetirementSelection,
    canonical_json_sha256,
    json_object,
    quoted_relation,
    retirement_marker,
    retirement_resource_hash_contract,
    row_mapping,
    validated_retirement_evidence,
    validated_retirement_marker,
)

_INCOMPLETE_STATUSES = ("acquiring", "incomplete")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SYNTHETIC_EVIDENCE_RELATIONS = frozenset(
    {"provider_directory_endpoint_dataset_previous_reference"}
)


def _one_row(rows: list[Any], error_code: str = "evidence_invalid") -> dict[str, Any]:
    if len(rows) != 1:
        raise TerminalRootRetirementError(error_code)
    return row_mapping(rows[0])


async def _initial_endpoint(database: Any, endpoint_id: str) -> dict[str, Any]:
    rows = await database.all(
        f"""
        SELECT endpoint.endpoint_id, endpoint.canonical_api_base
          FROM {quoted_relation('provider_directory_api_endpoint')} AS endpoint
         WHERE endpoint.endpoint_id = :endpoint_id
         LIMIT 2;
        """,
        endpoint_id=endpoint_id,
    )
    endpoint = _one_row(rows)
    if (
        endpoint.get("endpoint_id") != endpoint_id
        or type(endpoint.get("canonical_api_base")) is not str
        or not endpoint["canonical_api_base"]
    ):
        raise TerminalRootRetirementError("evidence_invalid")
    return endpoint


async def _try_scope_lock(database: Any, lock_identity: str) -> None:
    acquired = await database.scalar(
        """
        SELECT pg_catalog.pg_try_advisory_xact_lock(
                   pg_catalog.hashtextextended(CAST(:lock_identity AS text), 0)
               );
        """,
        lock_identity=lock_identity,
    )
    if acquired is not True:
        raise TerminalRootRetirementError("busy")


async def _lock_endpoint_scope(
    database: Any,
    request: TerminalRootRetirementRequest,
    initial_endpoint: Mapping[str, Any],
) -> str:
    canonical_api_base = str(initial_endpoint["canonical_api_base"])
    await _try_scope_lock(
        database,
        f"provider-directory-pagination:{canonical_api_base}",
    )
    await _try_scope_lock(database, request.endpoint_id)
    rows = await database.all(
        f"""
        SELECT endpoint.endpoint_id, endpoint.canonical_api_base
          FROM {quoted_relation('provider_directory_api_endpoint')} AS endpoint
         WHERE endpoint.endpoint_id = :endpoint_id
         FOR UPDATE OF endpoint;
        """,
        endpoint_id=request.endpoint_id,
    )
    endpoint = _one_row(rows)
    if endpoint != dict(initial_endpoint):
        raise TerminalRootRetirementError("evidence_invalid")
    return canonical_api_base


async def _share_evidence_relations(database: Any) -> None:
    child_relations = REQUIRED_CHILD_RELATIONS - _SYNTHETIC_EVIDENCE_RELATIONS
    physical_relations = child_relations | {"provider_directory_endpoint_dataset"}
    relation_names = ("import_run", *sorted(physical_relations))
    rendered_relations = ", ".join(quoted_relation(name) for name in relation_names)
    await database.status(f"LOCK TABLE {rendered_relations} IN SHARE MODE;")


async def _locked_target(
    database: Any,
    request: TerminalRootRetirementRequest,
) -> dict[str, Any]:
    rows = await database.all(
        f"""
        SELECT dataset.*
          FROM {quoted_relation('provider_directory_endpoint_dataset')} AS dataset
         WHERE dataset.dataset_id = :dataset_id
           AND dataset.endpoint_id = :endpoint_id
           AND dataset.import_run_id = :owner_run_id
           AND dataset.acquisition_root_run_id = :root_run_id
           AND dataset.previous_dataset_id = :predecessor_id
         FOR UPDATE OF dataset;
        """,
        dataset_id=request.dataset_id,
        endpoint_id=request.endpoint_id,
        owner_run_id=request.owner_run_id,
        root_run_id=request.acquisition_root_run_id,
        predecessor_id=request.expected_current_dataset_id,
    )
    return _one_row(rows)


def _target_metadata(
    target: Mapping[str, Any],
    request: TerminalRootRetirementRequest,
) -> dict[str, Any]:
    metadata = json_object(target.get("publication_metadata_json"))
    retirement_resource_hash_contract(metadata)
    if metadata.get("source_ids") != [request.source_id]:
        raise TerminalRootRetirementError("evidence_invalid")
    return metadata


def _validate_new_target(
    target: Mapping[str, Any],
    request: TerminalRootRetirementRequest,
    metadata: Mapping[str, Any],
) -> None:
    if (
        target.get("status") != "acquiring"
        or target.get("is_current") is not False
        or target.get("dataset_hash") is not None
        or target.get("validated_at") is not None
        or target.get("published_at") is not None
        or target.get("superseded_at") is not None
        or target.get("completion_proof_required_version") is not None
        or target.get("completion_proof_json") is not None
        or target.get("completion_proof_sha256") is not None
        or type(target.get("resource_count")) is not int
        or int(target["resource_count"]) < 0
        or RETIREMENT_METADATA_KEY in metadata
        or target.get("dataset_id") != request.dataset_id
    ):
        raise TerminalRootRetirementError("evidence_invalid")


async def _locked_source(
    database: Any,
    request: TerminalRootRetirementRequest,
    canonical_api_base: str,
) -> None:
    rows = await database.all(
        f"""
        SELECT source.source_id, source.endpoint_id, source.canonical_api_base
          FROM {quoted_relation('provider_directory_source')} AS source
         WHERE source.source_id = :source_id
         FOR UPDATE OF source;
        """,
        source_id=request.source_id,
    )
    source = _one_row(rows)
    if source != {
        "source_id": request.source_id,
        "endpoint_id": request.endpoint_id,
        "canonical_api_base": canonical_api_base,
    }:
        raise TerminalRootRetirementError("evidence_invalid")


async def _locked_predecessor(
    database: Any,
    request: TerminalRootRetirementRequest,
) -> None:
    rows = await database.all(
        f"""
        SELECT dataset.*
          FROM {quoted_relation('provider_directory_endpoint_dataset')} AS dataset
         WHERE dataset.dataset_id = :predecessor_id
           AND dataset.endpoint_id = :endpoint_id
         FOR UPDATE OF dataset;
        """,
        predecessor_id=request.expected_current_dataset_id,
        endpoint_id=request.endpoint_id,
    )
    predecessor = _one_row(rows)
    dataset_hash = predecessor.get("dataset_hash")
    if (
        predecessor.get("status") != "published"
        or predecessor.get("is_current") is not True
        or type(dataset_hash) is not str
        or _SHA256.fullmatch(dataset_hash) is None
        or predecessor.get("validated_at") is None
        or predecessor.get("published_at") is None
        or predecessor.get("superseded_at") is not None
    ):
        raise TerminalRootRetirementError("evidence_invalid")


async def _require_no_competing_candidate(
    database: Any,
    request: TerminalRootRetirementRequest,
) -> None:
    rows = await database.all(
        f"""
        SELECT dataset.dataset_id
          FROM {quoted_relation('provider_directory_endpoint_dataset')} AS dataset
         WHERE dataset.endpoint_id = :endpoint_id
           AND dataset.dataset_id <> :dataset_id
           AND dataset.status = ANY(CAST(:statuses AS varchar[]))
         ORDER BY dataset.dataset_id
         FOR UPDATE OF dataset;
        """,
        endpoint_id=request.endpoint_id,
        dataset_id=request.dataset_id,
        statuses=list(_INCOMPLETE_STATUSES),
    )
    if rows:
        raise TerminalRootRetirementError("evidence_invalid")


async def _locked_lineage(
    database: Any,
    request: TerminalRootRetirementRequest,
) -> tuple[dict[str, Any], ...]:
    run_rows = await database.all(
        f"""
        WITH RECURSIVE lineage AS (
            SELECT run.run_id, run.retry_of_run_id, 0 AS depth,
                   ARRAY[run.run_id]::varchar[] AS path
              FROM {quoted_relation('import_run')} AS run
             WHERE run.run_id = :root_run_id
            UNION ALL
            SELECT child.run_id, child.retry_of_run_id, parent.depth + 1,
                   parent.path || child.run_id
              FROM {quoted_relation('import_run')} AS child
              JOIN lineage AS parent
                ON child.retry_of_run_id = parent.run_id
             WHERE NOT child.run_id = ANY(parent.path)
        )
        SELECT run.*,
               lineage.depth,
               run.finished_at IS NOT NULL
               AND run.finished_at <= pg_catalog.transaction_timestamp()
                   - pg_catalog.make_interval(secs => :minimum_age)
                   AS terminal_age_satisfied
          FROM lineage
          JOIN {quoted_relation('import_run')} AS run
            ON run.run_id = lineage.run_id
         ORDER BY lineage.depth, run.run_id;
        """,
        root_run_id=request.acquisition_root_run_id,
        minimum_age=request.minimum_terminal_age_seconds,
    )
    lineage_records = tuple(
        row_mapping(run_record) for run_record in run_rows
    )
    _validate_lineage(lineage_records, request)
    return lineage_records


def _validate_lineage(
    lineage: tuple[Mapping[str, Any], ...],
    request: TerminalRootRetirementRequest,
) -> None:
    if not lineage or lineage[0].get("run_id") != request.acquisition_root_run_id:
        raise TerminalRootRetirementError("evidence_invalid")
    previous_run_id: str | None = None
    for depth, run in enumerate(lineage):
        if (
            run.get("depth") != depth
            or run.get("retry_of_run_id") != previous_run_id
            or run.get("importer") != "provider-directory-fhir"
            or run.get("status") not in TERMINAL_FAILURE_STATUSES
            or run.get("finished_at") is None
            or run.get("terminal_age_satisfied") is not True
        ):
            raise TerminalRootRetirementError("evidence_invalid")
        previous_run_id = str(run.get("run_id") or "")
    if previous_run_id != request.owner_run_id:
        raise TerminalRootRetirementError("evidence_invalid")


async def _locked_evidence(database: Any, dataset_id: str) -> dict[str, Any]:
    evidence = await database.scalar(
        f"SELECT {quoted_relation(RETIREMENT_EVIDENCE_FUNCTION)}(:dataset_id);",
        dataset_id=dataset_id,
    )
    return validated_retirement_evidence(evidence)


def _validate_evidence_counts(
    evidence: Mapping[str, Any],
    target: Mapping[str, Any],
    lineage_count: int,
) -> None:
    relation_evidence = evidence["child_relations"]
    if (
        evidence["terminal_run_count"] != lineage_count
        or evidence["parent_resource_count"] != target["resource_count"]
        or evidence["actual_resource_count"]
        != sum(evidence["resource_counts"].values())
        or relation_evidence["provider_directory_dataset_resource"]["row_count"]
        != evidence["actual_resource_count"]
        or relation_evidence["provider_directory_dataset_proof_shard"]["row_count"]
        != evidence["proof_shard_count"]
        or relation_evidence[
            "provider_directory_endpoint_dataset_previous_reference"
        ]["row_count"]
        != 0
    ):
        raise TerminalRootRetirementError("evidence_invalid")


async def _new_selection(
    database: Any,
    request: TerminalRootRetirementRequest,
    canonical_api_base: str,
    target_by_field: Mapping[str, Any],
    metadata: dict[str, Any],
) -> TerminalRootRetirementSelection:
    _validate_new_target(target_by_field, request, metadata)
    await _locked_source(database, request, canonical_api_base)
    await _locked_predecessor(database, request)
    await _require_no_competing_candidate(database, request)
    lineage = await _locked_lineage(database, request)
    evidence = await _locked_evidence(database, request.dataset_id)
    _validate_evidence_counts(evidence, target_by_field, len(lineage))
    evidence_sha256 = canonical_json_sha256(evidence)
    if (
        request.expected_evidence_sha256 is not None
        and request.expected_evidence_sha256 != evidence_sha256
    ):
        raise TerminalRootRetirementError("evidence_changed")
    retired_at = await database.scalar(
        """
        SELECT pg_catalog.to_char(
                   pg_catalog.transaction_timestamp() AT TIME ZONE 'UTC',
                   'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'
               );
        """
    )
    marker = retirement_marker(
        evidence,
        minimum_terminal_age_seconds=request.minimum_terminal_age_seconds,
        retired_at=retired_at,
    )
    return TerminalRootRetirementSelection(
        request=request,
        canonical_api_base=canonical_api_base,
        prior_status="acquiring",
        observed_metadata=metadata,
        marker_by_field=marker,
    )


async def _replay_selection(
    database: Any,
    request: TerminalRootRetirementRequest,
    canonical_api_base: str,
    target: Mapping[str, Any],
    metadata: dict[str, Any],
) -> TerminalRootRetirementSelection:
    marker = validated_retirement_marker(metadata.get(RETIREMENT_METADATA_KEY))
    evidence_sha256 = canonical_json_sha256(marker["evidence"])
    if (
        target.get("is_current") is not False
        or request.expected_evidence_sha256 not in (None, evidence_sha256)
    ):
        raise TerminalRootRetirementError("evidence_changed")
    return TerminalRootRetirementSelection(
        request=request,
        canonical_api_base=canonical_api_base,
        prior_status=RETIREMENT_STATUS,
        observed_metadata=metadata,
        marker_by_field=marker,
    )


async def selected_terminal_root_retirement(
    database: Any,
    request: TerminalRootRetirementRequest,
) -> TerminalRootRetirementSelection:
    """Lock and validate the exact first-time or replay retirement state."""

    if type(request) is not TerminalRootRetirementRequest:
        raise TerminalRootRetirementError("request_invalid")
    initial_endpoint = await _initial_endpoint(database, request.endpoint_id)
    canonical_api_base = await _lock_endpoint_scope(
        database, request, initial_endpoint
    )
    await _share_evidence_relations(database)
    target = await _locked_target(database, request)
    metadata = _target_metadata(target, request)
    if target.get("status") == RETIREMENT_STATUS:
        return await _replay_selection(
            database, request, canonical_api_base, target, metadata
        )
    return await _new_selection(
        database, request, canonical_api_base, target, metadata
    )


__all__ = (
    "selected_terminal_root_retirement",
)
