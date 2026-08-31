# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed Profile contracts for exact-cohort Practitioner evidence."""

from __future__ import annotations

import importlib
import json
from copy import deepcopy
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
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
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
    _source_rows,
)


importer = importlib.import_module("process.provider_directory_fhir")


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
    assert profile.profile_source_authority_sql("source_id", "endpoint_id") == (
        "endpoint_id"
    )
    assert profile.profile_reviewed_source_authority_sql("source_id") == (
        "NULL::varchar"
    )
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
        del source_spec["authority_ids_by_source_id"][
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
        ]
    elif mutation == "different_authority":
        source_spec["authority_ids_by_source_id"][
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
        ] = "different-authority"
    elif mutation == "same_endpoint":
        source_spec["dataset_scoped_endpoint_ids_by_source_id"][
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
        ] = FLEX_ENDPOINT_ID
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
        source_spec["dataset_scoped_endpoint_ids_by_source_id"][
            OFFICIAL_SOURCE_ID
        ] = OFFICIAL_ENDPOINT_ID
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
        item
        for item in catalog["items"]
        if item["canonical_base"] == "https://flex.optum.com/fhirpublic/R4"
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
    assert computed.identity_payload["pairs"][0]["source_id"] == (
        UHC_FLEX_PRACTITIONER_SOURCE_ID
    )
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
    changed_rows[0]["publication_metadata_json"][
        "semantic_projection_as_of"
    ] = "2026-08-08"
    second = snapshot._computed_selection_from_rows(
        _catalog(),
        node_id="test-node",
        source_rows=_source_rows(),
        dataset_rows=changed_rows,
    )
    assert first.identity_payload["profile_input_digest"] != (
        second.identity_payload["profile_input_digest"]
    )


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
