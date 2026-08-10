# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed Profile contracts for exact-cohort Practitioner evidence."""

from __future__ import annotations

import importlib
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_sources
from process import provider_directory_profile as profile
from process import provider_directory_profile_selection_snapshot as snapshot
from process import provider_directory_profile_uhc_flex as flex_profile
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)


OFFICIAL_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"
FLEX_ENDPOINT_ID = "a" * 64
OFFICIAL_ENDPOINT_ID = "b" * 64
FLEX_DATASET_ID = "pdufpd_" + "c" * 48
importer = importlib.import_module("process.provider_directory_fhir")


def _flex_metadata(*, projection: str = "2026-08-09") -> dict[str, object]:
    return {
        "acquisition_root_run_id": "pdufpar_" + "d" * 48,
        "admission_id": "pdufpa_" + "e" * 48,
        "cohort_complete": True,
        "dataset_id": FLEX_DATASET_ID,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "endpoint_id": FLEX_ENDPOINT_ID,
        "expected_resources": ["Practitioner"],
        "operation_key": "f" * 64,
        "publication_contract_id": (
            "healthporta.provider-directory.uhc-flex-practitioner-"
            "dataset-publication.v1"
        ),
        "resource_hash_contract": "semantic_content_v3",
        "selected_resources": ["Practitioner"],
        "semantic_projection_as_of": projection,
        "source_authority_id": "unitedhealthcare",
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "source_ids": [UHC_FLEX_PRACTITIONER_SOURCE_ID],
    }


def _source_rows() -> list[dict[str, object]]:
    return [
        {
            "source_id": OFFICIAL_SOURCE_ID,
            "endpoint_id": OFFICIAL_ENDPOINT_ID,
            "canonical_api_base": "https://files.example.test",
            "org_name": "Official files",
            "plan_name": None,
        },
        {
            "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
            "endpoint_id": FLEX_ENDPOINT_ID,
            "canonical_api_base": "https://directory.example.test/R4",
            "org_name": "Practitioner enrichment",
            "plan_name": None,
        },
        {
            "source_id": "pdfhir_0b5cfd565c53364a73981dcb",
            "endpoint_id": "probe-endpoint",
            "canonical_api_base": "https://directory.example.test/R4",
            "org_name": "Generic probe",
            "plan_name": None,
        },
    ]


def _dataset_rows(*, ready: bool = True) -> list[dict[str, object]]:
    metadata = _flex_metadata()
    return [
        {
            "endpoint_id": FLEX_ENDPOINT_ID,
            "dataset_id": FLEX_DATASET_ID,
            "acquisition_root_run_id": metadata["acquisition_root_run_id"],
            "dataset_hash": "1" * 64,
            "status": "published",
            "is_current": True,
            "resource_count": 3,
            "validated_at": "2026-08-09T00:00:00",
            "published_at": "2026-08-10T00:00:00",
            "superseded_at": None,
            "publication_metadata_json": metadata,
            "dataset_scoped_ready": ready,
            "dataset_scoped_admission_id": metadata["admission_id"],
            "dataset_scoped_projection_as_of": "2026-08-09",
            "dataset_scoped_authority_id": "unitedhealthcare",
            "dataset_scoped_operation_key": "f" * 64,
        }
    ]


def _catalog() -> dict[str, object]:
    return {
        "catalog_digest": "2" * 64,
        "items": [
            {
                "entry_id": "uhc-provider-files",
                "runnable": True,
                "profile_enabled": True,
                "source_ids": [OFFICIAL_SOURCE_ID],
            },
            {
                "entry_id": "uhc-generic-probe",
                "runnable": False,
                "profile_enabled": False,
                "source_ids": ["pdfhir_0b5cfd565c53364a73981dcb"],
            },
        ],
    }


def test_flex_profile_source_is_dataset_scoped_and_shares_authority():
    assert profile.configured_dataset_scoped_profile_source_ids() == (
        UHC_FLEX_PRACTITIONER_SOURCE_ID,
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
    assert 'FROM "fixture"."dataset_resource" AS resource' in sql
    assert "dataset_source_context.dataset_id = resource.dataset_id" in sql
    assert "resource.resource_type = 'Practitioner'" in sql
    assert "resource.payload_json::jsonb ->> 'resource_id'" in sql
    assert UHC_FLEX_PRACTITIONER_SOURCE_ID in sql
    assert "JOIN typed_source_context" in sql
    assert "ON typed_source_context.source_id = role.source_id" in sql
    assert "ON typed_source_context.source_id = service.source_id" in sql
    assert "ON typed_source_context.source_id = affiliation.source_id" in sql
    assert "SELECT * FROM typed_practitioner_rows" in sql
    assert "SELECT * FROM dataset_practitioner_rows" in sql


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


def _readiness_record(**overrides: object) -> SimpleNamespace:
    metadata = _flex_metadata()
    readiness_by_field: dict[str, object] = {
        "dataset_id": FLEX_DATASET_ID,
        "endpoint_id": FLEX_ENDPOINT_ID,
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "source_authority_id": "unitedhealthcare",
        "dataset_hash": "1" * 64,
        "resource_count": 3,
        "semantic_projection_as_of": metadata["semantic_projection_as_of"],
        "admission_id": metadata["admission_id"],
        "operation_key": metadata["operation_key"],
        "cohort_complete": True,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
    }
    readiness_by_field.update(overrides)
    return SimpleNamespace(**readiness_by_field)


def _artifact_dataset_row() -> dict[str, object]:
    metadata = _flex_metadata()
    return {
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "dataset_id": FLEX_DATASET_ID,
        "endpoint_id": FLEX_ENDPOINT_ID,
        "acquisition_root_run_id": metadata["acquisition_root_run_id"],
        "dataset_hash": "1" * 64,
        "resource_count": 3,
        "publication_metadata_json": metadata,
    }


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


@pytest.mark.asyncio
async def test_artifact_fence_takes_publication_lock_before_row_locks(
    monkeypatch,
):
    events: list[str] = []
    fence = SimpleNamespace(
        datasets=(SimpleNamespace(source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID),)
    )

    def async_step(name: str, return_value: object = None) -> AsyncMock:
        return AsyncMock(
            side_effect=lambda *_args, **_kwargs: (
                events.append(name),
                return_value,
            )[1]
        )

    monkeypatch.setattr(
        importer,
        "lock_uhc_flex_profile_publication",
        async_step("publication"),
    )
    for function_name, event_name, return_value in (
        ("_lock_artifact_fence_endpoint_advisories", "endpoint_advisory", None),
        ("_lock_artifact_fence_endpoints", "endpoint_rows", None),
        ("_lock_artifact_fence_aliases", "source_rows", []),
        ("_artifact_fence_dataset_rows", "dataset_rows", []),
        ("_artifact_eligible_validated_ids", "eligible_rows", {}),
        ("_assert_uhc_flex_profile_fence_ready", "readiness", None),
    ):
        monkeypatch.setattr(
            importer,
            function_name,
            async_step(event_name, return_value),
        )
    monkeypatch.setattr(
        importer,
        "_assert_locked_artifact_fence_aliases",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        importer,
        "_assert_locked_artifact_fence_datasets",
        lambda *_args: None,
    )

    await importer._lock_and_verify_artifact_dataset_fence(fence, object())
    assert events[0] == "publication"
    assert events[-1] == "readiness"
