# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve one proved CMS replacement alongside unchanged published sources."""

from __future__ import annotations

from sqlalchemy import text

_RESOURCE_TYPES = frozenset(
    {
        "Organization",
        "Location",
        "Endpoint",
        "HealthcareService",
        "InsurancePlan",
        "Practitioner",
        "PractitionerRole",
        "OrganizationAffiliation",
    }
)


def _cms_pair(execution):
    pairs = [pair for pair in execution.attestation.pairs if pair["source_id"] == "cms-npd"]
    if len(pairs) != 1:
        raise RuntimeError("cms_serving_selection_invalid")
    desired = execution.attestation.desired_cms_dataset
    if desired is not None and desired != pairs[0]:
        raise RuntimeError("cms_serving_selection_invalid")
    return pairs[0]


def _assert_cms_fence(execution, fence, desired):
    if len(fence.datasets) != 1:
        raise RuntimeError("cms_serving_candidate_selection_changed")
    dataset = fence.datasets[0]
    if (
        dataset.source_id != "cms-npd"
        or dataset.dataset_id != desired["dataset_id"]
        or dataset.endpoint_id != desired["endpoint_id"]
        or dataset.dataset_hash != desired["dataset_hash"]
        or dataset.evidence_run_id != desired["acquisition_root_run_id"]
        or dataset.status != desired["publication_status"]
        or dataset.is_current != desired["is_current"]
        or frozenset(dataset.artifact_resources) != _RESOURCE_TYPES
    ):
        raise RuntimeError("cms_serving_candidate_selection_changed")
    expected = execution.attestation.expected_cms_incumbent
    incumbent = dataset.expected_incumbent_dataset_id if dataset.promote_on_cutover else dataset.dataset_id
    if execution.attestation.desired_cms_dataset is not None and incumbent != (
        expected["dataset_id"] if expected else None
    ):
        raise RuntimeError("cms_serving_incumbent_changed")


async def resolve_desired_fence(fhir, execution):
    """Select an unpublished candidate only for CMS, never another source."""
    desired = _cms_pair(execution)
    sources = [pair["source_id"] for pair in execution.attestation.pairs if pair["source_id"] != "cms-npd"]
    others = (
        await fhir._resolve_provider_directory_artifact_datasets(sources, should_select_validated_candidates=False)
        if sources
        else fhir.ProviderDirectoryArtifactDatasetFence(())
    )
    cms = await fhir._resolve_provider_directory_artifact_datasets(
        ["cms-npd"], should_select_validated_candidates=desired["publication_status"] == "validated"
    )
    _assert_cms_fence(execution, cms, desired)
    if others.promotion_datasets:
        raise RuntimeError("cms_serving_other_candidate_selected")
    fence = fhir.ProviderDirectoryArtifactDatasetFence(
        datasets=tuple(sorted((*others.datasets, *cms.datasets), key=lambda dataset: dataset.source_id)),
        should_select_validated_candidates=cms.should_select_validated_candidates,
        promotion_aliases=(*others.promotion_aliases, *cms.promotion_aliases),
    )
    fhir._assert_profile_selection_matches_artifact_fence(execution, fence)
    await fhir._verify_provider_directory_artifact_dataset_fence(fence)
    return fence


async def prepare_desired_fence(fhir, execution, *, run_id, metrics, publish_targets):
    """Reuse validated typed dependencies without rebuilding them before admission."""
    if fhir.db._transaction_binding() is not None:
        return await _prepare_in_snapshot(fhir, execution, run_id, metrics, publish_targets)
    async with fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        return await _prepare_in_snapshot(fhir, execution, run_id, metrics, publish_targets)


async def _prepare_in_snapshot(fhir, execution, run_id, metrics, publish_targets):
    """Validate retained proofs and physical rows within one read-only source snapshot."""
    binding = fhir.db._transaction_binding()
    if (
        binding is None
        or await binding.session.scalar(text("SHOW transaction_read_only")) != "on"
        or (await binding.session.scalar(text("SHOW transaction_isolation")) not in {"repeatable read", "serializable"})
    ):
        raise RuntimeError("cms_serving_dependency_read_snapshot_required")
    fence = await resolve_desired_fence(fhir, execution)
    fhir._assert_provider_directory_artifact_target_dependencies(
        fence, publish_artifacts_targets=publish_targets, publish_corroboration=False
    )
    retained_metrics = await retained_relation_metrics(fhir, fence, run_id=run_id, publish_targets=publish_targets)
    refreshed = await resolve_desired_fence(fhir, execution)
    fhir._assert_artifact_fence_selection_unchanged(fence, refreshed)
    metrics.update(retained_metrics)
    return refreshed


def _relation_specs(fhir):
    """Reuse the two actual native edge mappings and their canonical proof contracts."""
    return (
        (
            "dataset_network_plan",
            fhir.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY,
            fhir.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_VERSION,
            fhir.DATASET_NETWORK_PLAN_COUNT_FIELDS,
            fhir._dataset_network_plan_proof_sql,
            fhir._network_plan_proof_select,
            fhir.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE,
            ("insurance_plan_resource_id", "network_resource_id"),
        ),
        (
            "dataset_affiliation_organization",
            fhir.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY,
            fhir.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_VERSION,
            fhir.AFFILIATION_ORGANIZATION_PROOF_COUNT_FIELDS,
            fhir._dataset_affiliation_organization_proof_sql,
            fhir._affiliation_org_proof_select,
            fhir.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_TABLE,
            ("affiliation_resource_id", "participating_organization_resource_id"),
        ),
    )


def _live_edge_proof_sql(fhir, spec):
    """Compare complete edge key sets while using the existing expected-edge SELECTs."""
    _target, _key, _version, _counts, statement, select, table, columns = spec
    original = statement(should_verify_acquisition_root=True)
    tail = select()
    if not original.endswith(tail):
        raise RuntimeError("cms_serving_dependency_query_changed")
    column_sql = ", ".join(columns)
    relation = fhir._unscoped_qt(fhir._schema(), table)
    return (
        original.removesuffix(tail)
        + f""",
        observed_edges AS MATERIALIZED (SELECT {column_sql} FROM {relation} WHERE dataset_id=:dataset_id)
        {tail.strip().removesuffix(";")},
        NOT EXISTS (
            (SELECT {column_sql} FROM expected_edges EXCEPT SELECT {column_sql} FROM observed_edges)
            UNION ALL
            (SELECT {column_sql} FROM observed_edges EXCEPT SELECT {column_sql} FROM expected_edges)
        ) AS edges_match;"""
    )


def _assert_retained_proof_shape(raw, dataset, spec):
    """Reject incomplete, wrong-purpose or coercible counts before native canonicalization."""
    target, _key, version, counts, *_rest = spec
    extra_counts = ("edge_count", "replaced_edge_count")
    if target == "dataset_network_plan":
        extra_counts += (
            "duplicate_network_reference_count",
            "plan_projection_count",
            "replaced_plan_projection_count",
            "inserted_plan_projection_count",
        )
    if not isinstance(raw, dict) or (
        raw.get("complete") is not True
        or type(raw.get("version")) is not int
        or raw["version"] != version
        or raw.get("dataset_id") != dataset.dataset_id
        or raw.get("acquisition_root_run_id") != dataset.evidence_run_id
        or any(type(raw.get(name)) is not int or raw[name] < 0 for name in (*counts, *extra_counts))
    ):
        raise RuntimeError("cms_serving_dependency_proof_invalid:" + dataset.dataset_id)


async def _validated_live_relation(fhir, dataset, raw, spec):
    """Revalidate source counts, complete live edge sets and exact compact plan payloads."""
    _assert_retained_proof_shape(raw, dataset, spec)
    query_params_map = {"dataset_id": dataset.dataset_id, "acquisition_root_run_id": dataset.evidence_run_id}
    edge_proof_row = await fhir.db.first(_live_edge_proof_sql(fhir, spec), **query_params_map)
    live_proof_map = dict(fhir._pagination_checkpoint_row_mapping(edge_proof_row))
    if live_proof_map.pop("edges_match", None) is not True:
        raise RuntimeError("cms_serving_dependency_edges_changed:" + dataset.dataset_id)
    proof_options_map = dict(
        dataset_id=dataset.dataset_id,
        build_run_id=raw.get("build_run_id"),
        replaced_edge_count=raw["replaced_edge_count"],
        inserted_edge_count=raw["edge_count"],
        expected_acquisition_root_run_id=dataset.evidence_run_id,
    )
    if spec[0] == "dataset_network_plan":
        projection_row = await fhir.db.first(
            fhir._dataset_insurance_plan_projection_proof_sql(should_verify_acquisition_root=True), **query_params_map
        )
        projection_proof_map = fhir._validated_dataset_insurance_plan_projection_proof(
            dict(fhir._pagination_checkpoint_row_mapping(projection_row)),
            dataset_id=dataset.dataset_id,
            expected_acquisition_root_run_id=dataset.evidence_run_id,
            replaced_plan_projection_count=raw["replaced_plan_projection_count"],
            inserted_plan_projection_count=raw["inserted_plan_projection_count"],
        )
        canonical = fhir._validated_dataset_network_plan_proof(
            live_proof_map, plan_projection_proof_map=projection_proof_map, **proof_options_map
        )
    else:
        canonical = fhir._validated_dataset_affiliation_organization_proof(live_proof_map, **proof_options_map)
    if canonical != raw:
        raise RuntimeError("cms_serving_dependency_proof_changed:" + dataset.dataset_id)
    return canonical


async def retained_relation_metrics(fhir, fence, *, run_id, publish_targets):
    """Read sealed relation evidence once per dataset, preserving unrelated source aliases."""
    specs = _relation_specs(fhir)
    enabled_specs = [
        spec for spec in specs if fhir.is_provider_directory_publish_target_enabled(publish_targets, spec[0])
    ]
    proofs_by_key_map = {spec[1]: [] for spec in enabled_specs}
    summaries = []
    for dataset in fhir._unique_artifact_datasets(fence):
        aliases = fhir._artifact_fence_for_dataset(fence, dataset.dataset_id)
        candidate, metadata_map = await fhir._current_artifact_dataset_source_summary_candidate(
            fhir.db, dataset, tuple(sorted(aliases.source_ids))
        )
        dataset_proofs_map = {}
        for spec in enabled_specs:
            proof = await _validated_live_relation(fhir, dataset, metadata_map.get(spec[1]), spec)
            dataset_proofs_map[spec[1]] = proof
            proofs_by_key_map[spec[1]].append(proof)
        if len(enabled_specs) == 2:
            summary = fhir._existing_artifact_dataset_source_summary(
                metadata_map, dataset, candidate, dataset_proofs_map
            )
            if summary is None:
                raise RuntimeError("cms_serving_dependency_summary_missing:" + dataset.dataset_id)
            summaries.append(summary)
    aggregate = fhir._aggregate_dataset_serving_relation_proofs(proofs_by_key_map, build_run_id=run_id)
    fhir._attach_aggregate_source_summary_proof(aggregate, summaries)
    for spec in specs:
        if spec not in enabled_specs:
            aggregate[spec[1]] = fhir._provider_directory_publish_target_skipped()
    return aggregate
