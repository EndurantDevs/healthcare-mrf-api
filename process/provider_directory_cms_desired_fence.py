# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve one proved CMS replacement alongside unchanged published sources."""

from __future__ import annotations

from typing import Any

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
    if desired["publication_status"] == "published" and desired["is_current"] is False:
        cms = await _resolve_sealed_candidate(fhir, desired)
    else:
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


async def _resolve_sealed_candidate(fhir, desired):
    """Resolve only the attested CMS seal; ordinary source selection stays current-only."""
    projection, publication = fhir._artifact_dataset_option_projection_parts(True)
    schema = fhir._schema()
    datasets = fhir._unscoped_qt(schema, fhir.ProviderDirectoryEndpointDataset.__tablename__)
    source_ref = fhir._unscoped_qt(schema, fhir.ProviderDirectorySource.__tablename__)
    metadata = "publication.full_metadata_jsonb"
    eligibility = fhir._artifact_reviewed_candidate_eligibility_sql(
        datasets,
        source_ref,
        metadata=metadata,
        content_proof_admitted=fhir._artifact_content_proof_admitted_marker_sql(metadata),
        use_admission_seals=False,
    )
    dataset_rows = await fhir.db.all(
        f"""SELECT {projection},source.source_id,source.endpoint_id AS serving_endpoint_id,
             to_jsonb(source) AS source_record_json,dataset.published_at,
             true AS promote_on_cutover,false AS reconcile_source_alias_on_cutover,
             (SELECT count(*) FROM {datasets} incumbent
              WHERE incumbent.endpoint_id=dataset.endpoint_id AND incumbent.is_current) AS current_dataset_count,
             (SELECT min(dataset_id) FROM {datasets} incumbent
              WHERE incumbent.endpoint_id=dataset.endpoint_id AND incumbent.is_current) AS current_dataset_id,
             count(*) OVER () AS validated_candidate_count
           FROM {datasets} dataset JOIN {source_ref} source ON source.endpoint_id=dataset.endpoint_id
           {publication}
           WHERE source.source_id='cms-npd' AND dataset.dataset_id=:dataset_id
             AND dataset.endpoint_id=:endpoint_id AND dataset.status='published'
             AND dataset.is_current IS FALSE AND dataset.validated_at IS NOT NULL
             AND dataset.published_at IS NOT NULL AND dataset.superseded_at IS NULL
             AND publication.full_metadata_jsonb->'source_ids'='["cms-npd"]'::jsonb
             AND {fhir.candidate_available_sql("dataset", schema, dataset_relation=datasets)}
             AND ({eligibility})""",
        dataset_id=desired["dataset_id"],
        endpoint_id=desired["endpoint_id"],
    )
    return fhir._validate_provider_directory_artifact_datasets(dataset_rows, ["cms-npd"])


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


def require_dataset_selection_state(
    fhir,
    selection_state: dict[str, Any],
    dataset_id: str,
    endpoint_id: str,
) -> None:
    """Keep ordinary lifecycle checks and admit the exact CMS sealed replacement."""
    should_reconcile_source_alias = bool(selection_state.get("should_reconcile_source_alias"))
    if selection_state["should_promote_on_cutover"]:
        if should_reconcile_source_alias:
            raise RuntimeError("provider_directory_artifact_alias_cutover_ambiguous:" + dataset_id)
        if selection_state["validated_candidate_count"] != 1:
            raise RuntimeError("provider_directory_artifact_validated_candidate_ambiguous:" + endpoint_id)
        if (
            selection_state["status"] != fhir.ENDPOINT_DATASET_VALIDATED
            and not (
                selection_state["status"] == fhir.ENDPOINT_DATASET_PUBLISHED and selection_state.get("published_at")
            )
        ) or selection_state["is_current"]:
            raise RuntimeError("provider_directory_artifact_validated_candidate_invalid:" + dataset_id)
        if (
            selection_state["current_dataset_count"] > 1
            or selection_state["previous_dataset_id"] != selection_state["current_dataset_id"]
        ):
            raise RuntimeError("provider_directory_artifact_expected_incumbent_mismatch:" + dataset_id)
        return
    if should_reconcile_source_alias and (
        selection_state["status"] != fhir.ENDPOINT_DATASET_PUBLISHED
        or not selection_state["is_current"]
        or selection_state["current_dataset_count"] != 1
        or selection_state["current_dataset_id"] != dataset_id
    ):
        raise RuntimeError("provider_directory_artifact_alias_reconciliation_invalid:" + dataset_id)
    if (
        selection_state["status"] != fhir.ENDPOINT_DATASET_PUBLISHED
        or not selection_state["is_current"]
        or selection_state["current_dataset_count"] != 1
        or selection_state["current_dataset_id"] != dataset_id
    ):
        raise RuntimeError("provider_directory_artifact_current_dataset_invalid:" + dataset_id)


def is_dataset_row_exact(
    fhir,
    dataset: Any,
    dataset_row_map: dict[str, Any],
) -> bool:
    """Recheck the historical identity fields and any captured CMS seal timestamp."""
    if dataset.published_at is not None and (
        str(dataset_row_map.get("published_at")) != dataset.published_at
        or dataset_row_map.get("superseded_at") is not None
    ):
        return False
    actual_identity = fhir._artifact_fence_dataset_row_identity(dataset_row_map)
    expected_identity_by_name = fhir._expected_artifact_fence_dataset_identity(dataset)
    for field_index, (actual_value, expected_value) in enumerate(zip(actual_identity, expected_identity_by_name)):
        if field_index in {5, 6, 7, 8} and expected_value is None:
            continue
        if actual_value != expected_value:
            return False
    return True
