# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed Profile contracts for exact-cohort Practitioner evidence."""

from __future__ import annotations

import importlib
import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_sources
from process import provider_directory_profile as profile
from process import provider_directory_profile_selection_snapshot as snapshot
from process import provider_directory_profile_uhc_flex as flex_profile
from process.provider_directory_dataset_scoped_publication import (
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_fhir_root_policy import REVIEWED_ROOT_POLICY_METADATA_KEY, ReviewedRootPolicy
from process.provider_directory_rooted_graph_publication import (
    build_provider_directory_rooted_graph_dataset_identity,
    provider_directory_rooted_graph_publication_metadata,
)
from process.provider_directory_rooted_graph_single_root_contract import derive_single_root_identity
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_twin_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from tests.provider_directory_profile_uhc_flex_test_support import (
    FLEX_DATASET_ID,
    FLEX_ENDPOINT_ID,
    GRAPH_DATASET_ID,
    GRAPH_ENDPOINT_ID,
    OFFICIAL_ENDPOINT_ID,
    OFFICIAL_SOURCE_ID,
    _artifact_dataset_row,
    _catalog,
    _dataset_rows,
    _flex_metadata,
    _readiness_record,
    _rooted_dataset_rows,
    _rooted_metadata,
    _rooted_readiness_record,
    _source_rows,
)
from tests.provider_directory_rooted_graph_publication_test_support import exact_current, resource_counts
from tests.test_provider_directory_rooted_graph_request_admission import _admit, _coverage, _partial_root

importer = importlib.import_module("process.provider_directory_fhir")


def _bounded_artifact_metadata(metadata):
    return {key: metadata[key] for key in importer._ARTIFACT_SELECTION_RECEIPT_METADATA_KEYS if key in metadata}


@pytest.fixture
def rooted_v2_metadata():
    current = exact_current(retry_exhausted_count=1)
    expected = derive_single_root_identity(current, operation_key="e" * 64).candidate
    candidate = replace(
        _partial_root(completed_count=3, error_count=0, request_failure_coverage=_coverage(rooted_failed=0)),
        acquisition_id=expected.acquisition_id,
        run_id=expected.run_id,
        dataset_intent_id=expected.dataset_intent_id,
    )
    admission = _admit(candidate)
    identity = build_provider_directory_rooted_graph_dataset_identity(admission, current)
    return provider_directory_rooted_graph_publication_metadata(
        identity, admission, previous_dataset_id=current.dataset_id, resource_counts=resource_counts()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", [None, "missing", "extra", "boolean", "inherited_count", "rooted_failure", "contract", "evidence_run_id"]
)
async def test_v2_rooted_artifact_coverage_keeps_profile_guards(monkeypatch, rooted_v2_metadata, damage):
    metadata = deepcopy(rooted_v2_metadata)
    coverage = metadata["request_failure_coverage"]
    match damage:
        case "missing":
            metadata.pop("request_failure_coverage")
        case "extra":
            coverage["unknown"] = 0
        case "boolean":
            coverage["failed_requests"] = True
        case "inherited_count":
            coverage["failed_requests"] += 1
        case "rooted_failure":
            coverage.update(rooted_failed_requests=1, failed_requests=2)
            metadata["rooted_graph_complete"] = False
        case "contract":
            metadata["publication_contract_id"] = "healthporta.provider-directory.rooted-graph-publication.v1"
    projected_by_field = _bounded_artifact_metadata(metadata)
    ready = _rooted_readiness_record(
        **{
            name: rooted_v2_metadata[name]
            for name in (
                "dataset_id",
                "endpoint_id",
                "source_id",
                "source_authority_id",
                "semantic_projection_as_of",
                "admission_id",
                "operation_key",
                "publication_kind",
                "cohort_complete",
                "retry_exhausted_count",
                "rooted_graph_complete",
                "resource_counts",
            )
        },
        resource_count=sum(rooted_v2_metadata["resource_counts"].values()),
    )
    row_by_field = {
        **_rooted_dataset_rows()[0],
        **vars(ready),
        "acquisition_root_run_id": metadata["acquisition_root_run_id"],
        "publication_metadata_json": projected_by_field,
    }
    row_by_field["evidence_run_id"] = row_by_field.pop("acquisition_root_run_id")
    row_by_field.pop("import_run_id", None)
    if damage == "evidence_run_id":
        row_by_field["evidence_run_id"] = "wrong-evidence"
    monkeypatch.setattr(flex_profile, "load_uhc_flex_profile_dataset_readiness", AsyncMock(return_value=ready))
    (annotated,) = await flex_profile.annotate_uhc_flex_profile_dataset_readiness(
        [row_by_field], database=object(), row_mapping=lambda value: value
    )
    assert annotated["dataset_scoped_ready"] is (damage is None)
    assert flex_profile.is_uhc_flex_publication_metadata_valid(
        projected_by_field,
        dataset_id=metadata["dataset_id"],
        endpoint_id=metadata["endpoint_id"],
        evidence_run_id=row_by_field["evidence_run_id"],
    ) is (damage is None)


@pytest.mark.parametrize("missing", [None, "baseline_acquisition_id", "baseline_run_id"])
def test_bounded_artifact_metadata_preserves_legacy_twin_proof(missing):
    row_by_field = _artifact_dataset_row()
    row_by_field["evidence_run_id"] = row_by_field.pop("acquisition_root_run_id")
    row_by_field.pop("import_run_id", None)
    metadata = row_by_field["publication_metadata_json"]
    projected_by_field = _bounded_artifact_metadata(metadata)
    if missing is not None:
        projected_by_field.pop(missing)
    row_by_field["publication_metadata_json"] = projected_by_field
    assert flex_profile.is_uhc_flex_dataset_readiness_matching(_readiness_record(), row_by_field) is (missing is None)


@pytest.mark.asyncio
@pytest.mark.parametrize("single_root", [False, True])
@pytest.mark.parametrize(
    "damage",
    [
        None,
        "admission_contract_id",
        "publication_acquisition_id",
        "attempt_id",
        "dataset_hash",
        "database",
        "evidence_run_id",
    ],
)
async def test_rooted_artifacts_preserve_bounded_admission_metadata(monkeypatch, single_root, damage):
    metadata = _rooted_metadata()
    if single_root:
        metadata.update(
            admission_contract_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
            attempt_id=None,
            comparison_acquisition_id=None,
            acquisition_operation_key="d" * 64,
            **{REVIEWED_ROOT_POLICY_METADATA_KEY: ReviewedRootPolicy(1).document()},
        )
    projected_by_field = _bounded_artifact_metadata(metadata)
    row_by_field = {
        **_rooted_dataset_rows()[0],
        "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        "publication_metadata_json": projected_by_field,
    }
    row_by_field["evidence_run_id"] = row_by_field.pop("acquisition_root_run_id")
    row_by_field.pop("import_run_id", None)
    if damage == "dataset_hash":
        row_by_field["dataset_hash"] = "e" * 64
    elif damage == "evidence_run_id":
        row_by_field["evidence_run_id"] = "wrong-evidence"
    elif damage not in {None, "database"}:
        projected_by_field.pop(damage, None)
    loader = AsyncMock(return_value=None if damage == "database" else _rooted_readiness_record())
    monkeypatch.setattr(flex_profile, "load_uhc_flex_profile_dataset_readiness", loader)
    (annotated,) = await flex_profile.annotate_uhc_flex_profile_dataset_readiness(
        [row_by_field], database=object(), row_mapping=lambda value: value
    )
    assert annotated["dataset_scoped_ready"] is (damage is None)
    assert flex_profile.is_uhc_flex_dataset_row_ready(annotated) is (damage is None)
    options_by_name = dict(
        endpoint_id=row_by_field["endpoint_id"],
        dataset_id=row_by_field["dataset_id"],
        evidence_run_id=row_by_field["evidence_run_id"],
        selected_resources=tuple(sorted(metadata["selected_resources"])),
        publication_metadata=projected_by_field,
        dataset_scoped_ready=annotated["dataset_scoped_ready"],
    )
    if damage is None:
        assert importer._artifact_flex_retained_resources(**options_by_name) == options_by_name["selected_resources"]
        sql = importer._artifact_bounded_publication_metadata_sql("metadata")
        assert all(
            f"metadata -> '{field}'" in sql
            for field in (
                "admission_contract_id",
                "publication_acquisition_id",
                "attempt_id",
                "comparison_acquisition_id",
                "acquisition_operation_key",
                "retry_exhausted_count",
                "request_failure_coverage",
                "baseline_acquisition_id",
                "baseline_run_id",
            )
        )
    else:
        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="dataset_scoped_proof_invalid"):
            importer._artifact_flex_retained_resources(**options_by_name)


def test_flex_profile_source_is_dataset_scoped_and_shares_authority():
    assert profile.configured_dataset_scoped_profile_source_ids() == (
        UHC_FLEX_PRACTITIONER_SOURCE_ID,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
    )
    assert profile.configured_dataset_scoped_profile_variant_groups() == (
        (
            "uhc-flex-enrichment-generation",
            (
                UHC_FLEX_PRACTITIONER_SOURCE_ID,
                PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
            ),
        ),
    )
    assert profile.configured_dataset_scoped_profile_endpoints() == tuple(
        sorted(
            (
                (UHC_FLEX_PRACTITIONER_SOURCE_ID, FLEX_ENDPOINT_ID),
                (PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID, GRAPH_ENDPOINT_ID),
            )
        )
    )
    assert (
        profile.profile_source_authority_id(
            OFFICIAL_SOURCE_ID,
            OFFICIAL_ENDPOINT_ID,
        )
        == "unitedhealthcare"
    )
    assert (
        profile.profile_source_authority_id(
            UHC_FLEX_PRACTITIONER_SOURCE_ID,
            FLEX_ENDPOINT_ID,
        )
        == "unitedhealthcare"
    )
    assert (
        profile.profile_source_authority_id(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
            GRAPH_ENDPOINT_ID,
        )
        == "unitedhealthcare"
    )


def test_profile_source_helpers_fail_closed_and_render_empty_contracts(
    monkeypatch,
):
    invalid_matrix_by_field = {
        "dataset_scoped_entry_ids": (),
        "source_ids": (),
        "verification_matrix": {},
    }
    monkeypatch.setattr(
        profile,
        "load_profile_source_spec",
        lambda _path=None: invalid_matrix_by_field,
    )
    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_source_spec_invalid",
    ):
        profile.configured_dataset_scoped_profile_source_ids()

    missing_source_by_field = {
        "dataset_scoped_entry_ids": ("dataset-entry",),
        "source_ids": (),
        "verification_matrix": {"sources": []},
    }
    monkeypatch.setattr(
        profile,
        "load_profile_source_spec",
        lambda _path=None: missing_source_by_field,
    )
    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_source_spec_invalid",
    ):
        profile.configured_dataset_scoped_profile_source_ids()

    empty_contract_by_field = {
        "authority_ids_by_source_id": {},
        "dataset_scoped_entry_ids": (),
        "source_ids": ("source-a",),
        "verification_matrix": {"sources": []},
    }
    monkeypatch.setattr(
        profile,
        "load_profile_source_spec",
        lambda _path=None: empty_contract_by_field,
    )
    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_source_authority_invalid",
    ):
        profile.profile_source_authority_id("missing-source", "endpoint-a")
    assert profile.profile_source_authority_sql("source_id", "endpoint_id") == ("endpoint_id")
    assert profile.profile_reviewed_source_authority_sql("source_id") == ("NULL::varchar")
    assert profile.dataset_scoped_profile_source_ids_sql() == ("ARRAY[]::varchar[]")


@pytest.mark.parametrize(
    "mutation",
    ("missing_authority", "different_authority", "same_endpoint", "overlap"),
)
def test_dataset_variant_spec_rejects_unreviewed_group_coordinates(
    tmp_path,
    mutation,
) -> None:
    source_spec = deepcopy(profile.load_profile_source_spec())
    if mutation == "missing_authority":
        del source_spec["authority_ids_by_source_id"][PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID]
    elif mutation == "different_authority":
        source_spec["authority_ids_by_source_id"][PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID] = "different-authority"
    elif mutation == "same_endpoint":
        source_spec["dataset_scoped_endpoint_ids_by_source_id"][PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID] = (
            FLEX_ENDPOINT_ID
        )
    else:
        source_spec["dataset_scoped_variant_groups"].append(
            {
                "group_id": "overlap",
                "entry_ids": [
                    "uhc-flex-practitioner-enrichment",
                    "uhc-flex-rooted-graph-enrichment",
                ],
            }
        )
    spec_path = tmp_path / "profile-sources.json"
    spec_path.write_text(json.dumps(source_spec), encoding="utf-8")

    with pytest.raises(RuntimeError, match="source_spec_invalid"):
        profile.load_profile_source_spec(spec_path)


@pytest.mark.parametrize("coverage", ("missing", "partial"))
def test_dataset_variant_spec_requires_exact_group_coverage(
    tmp_path,
    coverage,
) -> None:
    source_spec = deepcopy(profile.load_profile_source_spec())
    if coverage == "missing":
        source_spec["dataset_scoped_variant_groups"] = []
    else:
        source_spec["dataset_scoped_entry_ids"].append("uhc-provider-files")
        source_spec["dataset_scoped_endpoint_ids_by_source_id"][OFFICIAL_SOURCE_ID] = OFFICIAL_ENDPOINT_ID
    spec_path = tmp_path / "profile-sources.json"
    spec_path.write_text(json.dumps(source_spec), encoding="utf-8")

    with pytest.raises(RuntimeError, match="source_spec_invalid"):
        profile.load_profile_source_spec(spec_path)


def test_profile_sql_reads_flex_only_from_the_selected_dataset():
    sql = profile.profile_evidence_insert_sql(
        target_ref='"fixture"."evidence"',
        source_ref='"fixture"."source"',
        practitioner_ref='"fixture"."practitioner"',
        role_ref='"fixture"."role"',
        organization_ref='"fixture"."organization"',
        service_ref='"fixture"."service"',
        dataset_resource_ref='"fixture"."dataset_resource"',
    )
    assert "typed_practitioner_rows AS MATERIALIZED" in sql
    assert "dataset_practitioner_rows AS MATERIALIZED" in sql
    for family_cte in (
        "dataset_organization_rows AS MATERIALIZED",
        "dataset_service_resource_rows AS MATERIALIZED",
        "dataset_endpoint_rows AS MATERIALIZED",
    ):
        assert family_cte in sql
    for bucketed_family_cte in (
        "typed_role_rows AS NOT MATERIALIZED",
        "dataset_role_rows AS NOT MATERIALIZED",
        "role_resource_rows AS NOT MATERIALIZED",
        "typed_affiliation_rows AS NOT MATERIALIZED",
        "dataset_affiliation_rows AS NOT MATERIALIZED",
        "affiliation_resource_rows AS NOT MATERIALIZED",
    ):
        assert bucketed_family_cte in sql
    assert 'FROM "fixture"."dataset_resource" AS resource' in sql
    assert "dataset_source_context.dataset_id = resource.dataset_id" in sql
    for resource_type in (
        "Practitioner",
        "PractitionerRole",
        "Organization",
        "OrganizationAffiliation",
        "HealthcareService",
        "Endpoint",
    ):
        assert f"resource.resource_type = '{resource_type}'" in sql
    assert "resource.payload_json::jsonb ->> 'resource_id'" in sql
    assert UHC_FLEX_PRACTITIONER_SOURCE_ID in sql
    assert "JOIN typed_source_context" in sql
    assert "typed_source_context.source_id = role.source_id" in sql
    assert "typed_source_context.source_id = service.source_id" in sql
    assert "typed_source_context.source_id = affiliation.source_id" in sql
    assert "SELECT * FROM typed_practitioner_rows" in sql
    assert "SELECT * FROM dataset_practitioner_rows" in sql
    assert "practitioner.dataset_id = role.dataset_id" in sql
    assert "service.dataset_id = role_rows.dataset_id" in sql
    assert "endpoint.dataset_id = role_rows.dataset_id" in sql
    assert "organization.dataset_id = affiliation.dataset_id" in sql


def test_public_catalog_admits_dedicated_dataset_source_without_running_probe():
    catalog = provider_directory_sources.provider_directory_source_catalog()
    generic_probe = next(
        item for item in catalog["items"] if item["canonical_base"] == "https://flex.optum.com/fhirpublic/R4"
    )
    assert generic_probe["runnable"] is False
    assert generic_probe["profile_enabled"] is False
    assert UHC_FLEX_PRACTITIONER_SOURCE_ID not in generic_probe["source_ids"]


def test_profile_aggregate_counts_authority_without_collapsing_sources():
    sql = profile.profile_insert_sql(
        evidence_ref='"fixture"."evidence"',
        target_ref='"fixture"."profile"',
        old_evidence_ref=None,
        rebuild_all=True,
    )
    assert "count(DISTINCT evidence.source_id)::integer AS source_count" in sql
    assert "THEN 'unitedhealthcare'" in sql
    assert "count(DISTINCT evidence.authority_id)::integer" in sql
    assert "'authority_id', evidence.authority_id" in sql


def test_selection_admits_only_ready_exact_cohort_and_never_generic_probe():
    computed = snapshot._computed_selection_from_rows(
        _catalog(),
        node_id="test-node",
        source_rows=_source_rows(),
        dataset_rows=_dataset_rows(),
    )
    assert computed.request_projection == (
        {"source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID, "dataset_id": FLEX_DATASET_ID},
    )
    assert computed.identity_payload["pairs"][0]["source_id"] == (UHC_FLEX_PRACTITIONER_SOURCE_ID)
    assert computed.identity_payload["operation"] == "publish"

    not_ready = snapshot._computed_selection_from_rows(
        _catalog(),
        node_id="test-node",
        source_rows=_source_rows(),
        dataset_rows=_dataset_rows(ready=False),
    )
    assert not_ready.request_projection == ()
    assert not_ready.identity_payload["operation"] == "purge"

    rooted = snapshot._computed_selection_from_rows(
        _catalog(),
        node_id="test-node",
        source_rows=_source_rows(),
        dataset_rows=_rooted_dataset_rows(),
    )
    assert rooted.request_projection == (
        {
            "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
            "dataset_id": GRAPH_DATASET_ID,
        },
    )
    rooted_profile_input = snapshot._profile_input(
        rooted.identity_payload["pairs"][0],
        _rooted_dataset_rows()[0],
        _rooted_metadata(),
    )
    assert rooted_profile_input["dataset_scoped_variant"] == (ROOTED_COMBINED_VARIANT)


def test_selection_rejects_two_variant_currents_or_foreign_current() -> None:
    with pytest.raises(
        RuntimeError,
        match="profile_selection_dataset_variant_ambiguous",
    ):
        snapshot._computed_selection_from_rows(
            _catalog(),
            node_id="test-node",
            source_rows=_source_rows(),
            dataset_rows=[*_dataset_rows(), *_rooted_dataset_rows()],
        )

    foreign = deepcopy(_rooted_dataset_rows())
    foreign[0]["dataset_id"] = "generic-current"
    with pytest.raises(
        RuntimeError,
        match="profile_selection_dataset_variant_invalid",
    ):
        snapshot._computed_selection_from_rows(
            _catalog(),
            node_id="test-node",
            source_rows=_source_rows(),
            dataset_rows=foreign,
        )


def test_selection_requires_dormant_variant_registration_before_legacy() -> None:
    source_rows = [
        source_row
        for source_row in _source_rows()
        if source_row["source_id"] != PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
    ]
    with pytest.raises(
        RuntimeError,
        match="profile_selection_dataset_variant_registry_invalid",
    ):
        snapshot._computed_selection_from_rows(
            _catalog(),
            node_id="test-node",
            source_rows=source_rows,
            dataset_rows=_dataset_rows(),
        )


def test_selection_identity_binds_projection_admission_and_operation():
    first = snapshot._computed_selection_from_rows(
        _catalog(),
        node_id="test-node",
        source_rows=_source_rows(),
        dataset_rows=_dataset_rows(),
    )
    changed_rows = deepcopy(_dataset_rows())
    changed_rows[0]["dataset_scoped_projection_as_of"] = "2026-08-08"
    changed_rows[0]["publication_metadata_json"]["semantic_projection_as_of"] = "2026-08-08"
    second = snapshot._computed_selection_from_rows(
        _catalog(),
        node_id="test-node",
        source_rows=_source_rows(),
        dataset_rows=changed_rows,
    )
    assert first.identity_payload["profile_input_digest"] != (second.identity_payload["profile_input_digest"])


def test_dataset_scoped_readiness_rejects_any_closed_field_drift():
    for field_name, value in (
        ("dataset_scoped_admission_id", "wrong"),
        ("dataset_scoped_authority_id", "other"),
        ("dataset_scoped_operation_key", "0" * 64),
        ("dataset_scoped_projection_as_of", "2026-08-08"),
    ):
        rows = _dataset_rows()
        rows[0][field_name] = value
        computed = snapshot._computed_selection_from_rows(
            _catalog(),
            node_id="test-node",
            source_rows=_source_rows(),
            dataset_rows=rows,
        )
        assert computed.request_projection == ()


def test_artifact_readiness_matches_exact_generic_parent_and_metadata():
    dataset_row = _artifact_dataset_row()
    assert flex_profile.is_uhc_flex_dataset_readiness_matching(
        _readiness_record(),
        dataset_row,
    )
    for field_name, value in (
        ("dataset_hash", "2" * 64),
        ("resource_count", 2),
        ("admission_id", "pdufpa_" + "0" * 48),
        ("operation_key", "0" * 64),
        ("semantic_projection_as_of", "2026-08-08"),
        ("source_authority_id", "other"),
    ):
        assert not flex_profile.is_uhc_flex_dataset_readiness_matching(
            _readiness_record(**{field_name: value}),
            dataset_row,
        )


def test_dataset_scoped_marker_never_bypasses_ordinary_content_proof(
    monkeypatch,
):
    monkeypatch.setattr(
        importer,
        "_validate_finalized_content_proof",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("invalid")),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="content_proof_invalid",
    ):
        importer._validate_artifact_finalized_content_proof(
            {},
            {},
            "ordinary-run",
            "ordinary-dataset",
            source_id="ordinary-source",
            dataset_scoped_ready=True,
        )


@pytest.mark.asyncio
async def test_artifact_fence_rechecks_exact_flex_readiness(monkeypatch):
    publication = importlib.import_module("process.uhc_flex_practitioner_publication")
    dataset = SimpleNamespace(
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        dataset_id=FLEX_DATASET_ID,
        endpoint_id=FLEX_ENDPOINT_ID,
        dataset_hash="1" * 64,
        resource_count=3,
        dataset_scoped_ready=True,
        dataset_scoped_variant=LEGACY_PRACTITIONER_VARIANT,
        dataset_scoped_cohort_complete=True,
        dataset_scoped_retry_exhausted_count=0,
        semantic_projection_as_of="2026-08-09",
        source_authority_id="unitedhealthcare",
        admission_id="pdufpa_" + "e" * 48,
        operation_key="f" * 64,
    )
    fence = SimpleNamespace(datasets=(dataset,))
    loader = AsyncMock(return_value=None)
    monkeypatch.setattr(
        publication,
        "load_uhc_flex_practitioner_dataset_readiness",
        loader,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="dataset_scoped_readiness_changed",
    ):
        await importer._assert_uhc_flex_profile_fence_ready(fence, object())

    loader.return_value = _readiness_record()
    await importer._assert_uhc_flex_profile_fence_ready(fence, object())
    assert loader.await_count == 2
