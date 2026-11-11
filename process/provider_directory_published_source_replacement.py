# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin an explicit published replacement without moving its serving alias."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace

from process import provider_directory_generic_source_policy as source

_ACTIVE = ContextVar("provider_directory_published_source_replacement", default=None)
_PREFIX = "provider_directory_dataset_followup_"
_FIELDS = ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "parent_run_id")
_HEADER_FIELDS = (
    "dataset_id",
    "endpoint_id",
    "acquisition_root_run_id",
    "import_run_id",
    "dataset_hash",
    "resource_count",
    "published_at",
    "publication_metadata_sha256",
    "content_proof_admission_sha256",
)


@dataclass(frozen=True)
class _Selection:
    directory: object
    schema: str
    task: object
    fence: object
    incumbent: tuple
    publication: tuple


def mark_for_preparation(directory, fence):
    """Use the existing fence fields with an explicit internal scope-required type."""
    return directory._PublishedSourceReplacementFence(
        fence.datasets,
        fence.should_select_validated_candidates,
        fence.promotion_aliases,
        fence.validated_publication_candidate,
    )


def selected(directory, fence):
    """Recognize only this task's exact private preparation, without issuing authority."""
    scope = _ACTIVE.get()
    if scope is None:
        return None
    _require(
        directory,
        scope.directory is directory
        and scope.schema == directory._schema()
        and scope.task is asyncio.current_task()
        and scope.fence == fence
        and source._FACTORY.get() is None
        and not source.is_source_storage_required(),
        "scope_changed",
    )
    return scope


def _require(directory, condition, suffix):
    if not condition:
        raise directory.ProviderDirectoryArtifactBuildStale("provider_directory_published_replacement_" + suffix)


def _tables(directory):
    return tuple(
        directory._unscoped_qt(directory._schema(), name)
        for name in (
            "provider_directory_source",
            "provider_directory_endpoint_dataset",
        )
    )


async def _headers(directory, fence, executor):
    """Capture old and desired header coordinates independently of dataset lineage."""
    _, datasets = _tables(directory)
    header_rows = await executor.all(
        f"SELECT {','.join(_HEADER_FIELDS)} FROM ONLY {datasets} "
        "WHERE endpoint_id=ANY(CAST(:endpoints AS varchar[])) AND status='published' "
        "AND is_current AND published_at IS NOT NULL AND superseded_at IS NULL ORDER BY dataset_id",
        endpoints=fence.endpoint_ids,
    )
    header_rows = [directory._pagination_checkpoint_row_mapping(header_row) for header_row in header_rows]
    dataset = fence.datasets[0]
    by_endpoint = {}
    for header_row in header_rows:
        by_endpoint.setdefault(header_row["endpoint_id"], []).append(tuple(header_row[name] for name in _HEADER_FIELDS))
    old = by_endpoint.get(dataset.serving_endpoint_id or dataset.endpoint_id, [])
    new = by_endpoint.get(dataset.endpoint_id, [])
    _require(directory, len(old) == len(new) == 1, "current_headers_changed")
    for header in (old[0], new[0]):
        _require(
            directory,
            bool(header[0])
            and type(header[2]) is str
            and bool(header[2].strip())
            and type(header[5]) is int
            and header[5] >= 0,
            "header_identity_invalid",
        )
    return old[0], new[0]


async def _assert_newest(directory, fence, publication, executor):
    """Reject newer or equally dated conflicting admitted source publications."""
    sources, datasets = _tables(directory)
    metadata = directory._artifact_current_selection_metadata_sql()
    eligibility = directory._artifact_reviewed_candidate_eligibility_sql(
        datasets,
        sources,
        metadata="publication.metadata",
        content_proof_admitted=directory._artifact_content_proof_admitted_marker_sql("publication.metadata"),
        use_admission_seals=False,
    )
    dataset = fence.datasets[0]
    conflict = await executor.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {datasets} dataset "
        f"CROSS JOIN LATERAL (SELECT {metadata} AS metadata) publication "
        "WHERE dataset.status='published' AND dataset.is_current AND dataset.superseded_at IS NULL "
        "AND dataset.published_at>=:published_at AND dataset.dataset_id<>:dataset_id "
        "AND publication.metadata->'source_ids' @> jsonb_build_array(CAST(:source_id AS text)) "
        f"AND ({eligibility}))",
        published_at=publication[_HEADER_FIELDS.index("published_at")],
        dataset_id=dataset.dataset_id,
        source_id=dataset.source_id,
        published_status=directory.ENDPOINT_DATASET_PUBLISHED,
        validated_status=directory.ENDPOINT_DATASET_VALIDATED,
    )
    _require(directory, not conflict, "newer_publication")


def _descriptor_fields(directory, metrics):
    """Validate the existing descriptor coordinates without changing its V1 shape."""
    from process.provider_directory_generic_source_artifacts import _supports_overlay_source

    expected_by_field = {name: metrics.get(_PREFIX + name) for name in _FIELDS}
    _require(
        directory,
        all(
            type(field_value) is str and field_value and field_value == field_value.strip()
            for field_value in expected_by_field.values()
        )
        and len(expected_by_field["dataset_hash"]) == 64
        and set(expected_by_field["dataset_hash"]) <= set("0123456789abcdef")
        and type(metrics.get(_PREFIX + "version")) is int
        and metrics[_PREFIX + "version"] == 1
        and metrics.get(_PREFIX + "stage") == "address_overlay"
        and type(metrics.get(_PREFIX + "attempt")) is int
        and metrics[_PREFIX + "attempt"] > 0
        and _supports_overlay_source(directory, expected_by_field["source_id"], expected_by_field["dataset_id"]),
        "descriptor_invalid",
    )
    return expected_by_field


async def _resolve_publication(directory, expected_by_field):
    """Require the descriptor's exact sealed current publication."""
    fence = await directory._resolve_provider_directory_artifact_datasets(
        [expected_by_field["source_id"]],
        allow_published_replacement=True,
    )
    _require(
        directory,
        len(fence.datasets) == 1
        and fence.source_ids == [expected_by_field["source_id"]]
        and len(fence.locked_alias_datasets) == 1
        and not fence.promotion_datasets,
        "selection_changed",
    )
    dataset = fence.datasets[0]
    actual_by_field = dict(
        source_id=dataset.source_id,
        endpoint_id=dataset.endpoint_id,
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        parent_run_id=dataset.evidence_run_id,
    )
    _require(
        directory,
        actual_by_field == expected_by_field
        and dataset.status == "published"
        and dataset.is_current
        and dataset.generic_admission_sealed,
        "descriptor_stale",
    )
    return fence


async def _bind_source_contract(directory, fence):
    """Freeze existing configured-source fields independently of the old header."""
    dataset = fence.datasets[0]
    source_ref, _ = _tables(directory)
    source_row = await directory.db.first(
        f"SELECT to_jsonb(source) AS record FROM {source_ref} source WHERE source_id=:source_id",
        source_id=dataset.source_id,
    )
    _require(directory, source_row is not None, "source_changed")
    source_record = directory._json_object(directory._pagination_checkpoint_row_mapping(source_row)["record"])
    _require(
        directory,
        directory._source_metadata(source_record).get(directory.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY)
        == dataset.endpoint_id
        and source_record["endpoint_id"] == (dataset.serving_endpoint_id or dataset.endpoint_id),
        "configured_endpoint_changed",
    )
    contract = directory._artifact_source_verification_contract(
        source_record,
        verification_campaign_id=dataset.verification_campaign_id,
        verification_source_scope_hash=dataset.verification_source_scope_hash,
        source_ids=dataset.verification_source_ids,
        completion_proof_cutoff=dataset.completion_proof_cutoff,
    )
    dataset = replace(
        dataset,
        source_verification_contract=contract,
        source_verification_contract_hash=directory._identity_hash(contract),
    )
    fence = mark_for_preparation(directory, replace(fence, datasets=(dataset,)))
    return fence


@asynccontextmanager
async def preparation_scope(directory, metrics):
    """Select only an exact V1 publication; callers retain their existing stage owners."""
    _require(
        directory,
        _ACTIVE.get() is None and source._FACTORY.get() is None and not source.is_source_storage_required(),
        "ordinary_preparation_required",
    )
    fence = await _bind_source_contract(
        directory, await _resolve_publication(directory, _descriptor_fields(directory, metrics))
    )
    dataset = fence.datasets[0]
    incumbent, publication = await _headers(directory, fence, directory.db)
    _require(
        directory,
        incumbent[_HEADER_FIELDS.index("published_at")] <= publication[_HEADER_FIELDS.index("published_at")],
        "newer_incumbent",
    )
    await _assert_newest(directory, fence, publication, directory.db)
    _require(
        directory,
        publication[:3] == (dataset.dataset_id, dataset.endpoint_id, dataset.evidence_run_id)
        and publication[4] == dataset.dataset_hash,
        "descriptor_stale",
    )
    scope = _Selection(directory, directory._schema(), asyncio.current_task(), fence, incumbent, publication)
    token = _ACTIVE.set(scope)
    try:
        yield fence
        selected(directory, fence)
    finally:
        _ACTIVE.reset(token)


async def verify_locked(directory, fence, executor):
    """Recheck freshness in the original final transaction while publication is fenced."""
    scope = selected(directory, fence)
    if scope is None:
        _require(directory, not isinstance(fence, directory._PublishedSourceReplacementFence), "scope_required")
        return
    _require(
        directory,
        executor is directory.db and directory.db._transaction_binding() is not None,
        "original_transaction_required",
    )
    _require(
        directory, await executor.scalar("SHOW transaction_isolation") == "read committed", "fresh_snapshot_required"
    )
    sources, _ = _tables(directory)
    # The existing publishers hold SHARE on this same row; serialize all endpoints before reselecting.
    await executor.all(
        f"SELECT source_id FROM {sources} WHERE source_id=:source_id FOR UPDATE NOWAIT",
        source_id=fence.datasets[0].source_id,
    )
    aliases = await directory._lock_artifact_fence_aliases(fence, executor)
    directory._assert_locked_artifact_fence_aliases(fence, aliases)
    _require(
        directory,
        await _headers(directory, fence, executor) == (scope.incumbent, scope.publication),
        "incumbent_changed",
    )
    await _assert_newest(directory, fence, scope.publication, executor)


async def is_address_selection_current(directory, schema, source_id, dataset_id, root_run_id):
    """Admit retained old headers only inside the exact explicit replacement scope."""
    scope = _ACTIVE.get()
    if scope is None:
        return False
    selected(directory, scope.fence)
    dataset = scope.fence.datasets[0]
    _require(
        directory,
        schema == directory._schema()
        and (source_id, dataset_id, root_run_id) == (dataset.source_id, dataset.dataset_id, dataset.evidence_run_id),
        "address_selection_changed",
    )
    sources, datasets = _tables(directory)
    current = await directory.db.scalar(
        f"SELECT EXISTS(SELECT FROM {sources} source JOIN {datasets} dataset ON dataset.endpoint_id=source.endpoint_id "
        "WHERE source.source_id=:source_id AND dataset.dataset_id=:dataset_id "
        "AND dataset.dataset_hash=:dataset_hash AND dataset.acquisition_root_run_id=:root_run_id "
        "AND dataset.status='published' AND dataset.is_current AND dataset.superseded_at IS NULL "
        "AND dataset.published_at IS NOT NULL)",
        source_id=source_id,
        dataset_id=dataset_id,
        dataset_hash=dataset.dataset_hash,
        root_run_id=root_run_id,
    )
    _require(directory, current, "address_alias_not_committed")
    return True


async def require_retained_output(directory, stage):
    """Refuse dropping an incumbent that the common owner has not retained."""
    scope = _ACTIVE.get()
    if scope is None:
        return
    selected(directory, scope.fence)
    old = directory._unscoped_qt(stage.schema, stage.target_relation + "_old")
    _require(
        directory,
        stage.schema == scope.schema
        and not await directory.db.scalar(
            "SELECT to_regclass(:old) IS NOT NULL",
            old=old,
        ),
        "retained_output_required",
    )


async def cutover_sources(directory, fence):
    """Keep the existing source-scoped alias CAS under the original atomic owner."""
    await verify_locked(directory, fence, directory.db)
    source_ref = directory._unscoped_qt(directory._schema(), directory.ProviderDirectorySource.__tablename__)
    for dataset in fence.locked_alias_datasets:
        serving_endpoint_id = dataset.serving_endpoint_id or dataset.endpoint_id
        if (
            not (dataset.promote_on_cutover or dataset.reconcile_source_alias_on_cutover)
            or serving_endpoint_id == dataset.endpoint_id
        ):
            continue
        configured_endpoint_cas = (
            "\n               AND metadata_json::jsonb"
            f" ->> '{directory.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY}'"
            " = :endpoint_id"
            if dataset.completion_proof_required_version == directory.SERVER_ISSUED_SUBSET_REQUIRED_VERSION
            or selected(directory, fence) is not None
            else ""
        )
        updated_count = await directory.db.status(
            f"""
            UPDATE {source_ref}
               SET endpoint_id = :endpoint_id,
                   updated_at = now()
             WHERE source_id = :source_id
               AND endpoint_id = :serving_endpoint_id{configured_endpoint_cas};
            """,
            source_id=dataset.source_id,
            endpoint_id=dataset.endpoint_id,
            serving_endpoint_id=serving_endpoint_id,
        )
        if directory._coerce_rowcount(updated_count) != 1:
            raise directory.ProviderDirectoryArtifactBuildStale("provider_directory_source_endpoint_dataset_changed")


def serving_alias_exclusion_sql(dataset_ref: str, allow_published_replacement: bool) -> str:
    """Retain valid serving aliases outside explicit replacement preparation."""
    if allow_published_replacement:
        return ""
    return f"""AND NOT EXISTS (
        SELECT 1 FROM {dataset_ref} AS serving_dataset
         WHERE serving_dataset.endpoint_id = source.serving_endpoint_id
           AND serving_dataset.is_current = true
           AND serving_dataset.status = :published_status
           AND serving_dataset.published_at IS NOT NULL
           AND serving_dataset.superseded_at IS NULL
    )"""
