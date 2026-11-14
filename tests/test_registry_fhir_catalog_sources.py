# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named source configuration, content descriptors and bounded endpoint inventory."""

from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from process import registry_fhir_catalog_sources as sources
from process.registry_fhir_catalog_observation import FHIRCatalogObservationDigests
from process.registry_fhir_catalog_observed_source import build_fhir_catalog_observation_descriptor


def read_profile():
    return sources.FHIRCatalogSourceReadProfile("source_payload", "source_catalog")


def scoped_read_profile(**options):
    return read_profile()


def published_row(**changes):
    return {
        "endpoint_id": "endpoint-one",
        "dataset_id": "dataset-one",
        "dataset_hash": "a" * 64,
        "status": "published",
        "is_current": True,
        "published_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "resource_count": 75,
        "previous_dataset_id": None,
        "root_run_id": "run-one",
        "resource_hash_contract": None,
        "source_aliases": ["alias-a", "alias-b"],
        "alias_count": 2,
        "current_count": 1,
        "row_bytes": 4096,
        **changes,
    }


def uhc_published_row():
    return published_row()


def descriptor():
    families = FHIRCatalogObservationDigests().document()
    return build_fhir_catalog_observation_descriptor(
        read_profile(),
        published_row(),
        {
            "header": {"schema": "source_catalog", "name": "provider_directory_endpoint_dataset"},
            "resource": {"schema": "source_payload", "name": "dataset_resources"},
        },
        ("https://directory.example.test/fhir/", ["alias-a", "alias-b"]),
        families,
    )


def test_named_profile_normalizes_legacy_configuration_without_exporting_it():
    legacy_profile_by_field = {
        "schema_name": "source_payload",
        "catalog_schema_name": "source_catalog",
        "owner_role": "old_owner",
        "runtime_roles": ["old_reader"],
        "published_read_scope": {"retired": True},
    }
    assert sources.parse_fhir_catalog_source_profile(legacy_profile_by_field) == read_profile()
    assert read_profile().document() == {"schema_name": "source_payload", "catalog_schema_name": "source_catalog"}


@pytest.mark.parametrize("schema", [None, "", "unsafe-name", 'a"b', "x" * 64])
def test_profile_rejects_invalid_identifiers(schema):
    with pytest.raises(ValueError):
        sources.FHIRCatalogSourceReadProfile(schema)


def test_descriptor_roundtrips_names_and_logical_content():
    selected = descriptor()
    assert sources.validate_fhir_catalog_source_descriptor(selected) == selected
    assert sources.fhir_catalog_source_generation(selected) == sources.fhir_catalog_source_generation(
        deepcopy(selected)
    )
    assert set(selected["tables"]) == {"header", "resource"}
    assert set(selected["admission"]["publication"]) == {
        "root_run_id",
        "published_at",
        "resource_hash_contract",
        "claimed_resource_count",
    }


@pytest.mark.parametrize("mutation", ["resource_schema", "table_name", "alias", "content", "unknown"])
def test_descriptor_rejects_changed_scope_or_content(mutation):
    selected = descriptor()
    if mutation == "resource_schema":
        selected["tables"]["resource"]["schema"] = "another_source"
    elif mutation == "table_name":
        selected["tables"]["resource"]["name"] = 'bad"name'
    elif mutation == "alias":
        selected["source_aliases"] = ["alias-b"]
    elif mutation == "content":
        selected["admission"]["families"]["Organization"]["rows"] = 1
    else:
        selected["unexpected"] = 5
    with pytest.raises(ValueError):
        sources.validate_fhir_catalog_source_descriptor(selected)


@pytest.mark.asyncio
async def test_inventory_uses_bounded_keyset_pages_and_real_eof(monkeypatch):
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(side_effect=[[published_row()], []]),
    )
    disposition = sources.FHIRCatalogSourceDisposition(
        "endpoint-one", "dataset-one", ("alias-a", "alias-b"), 2, "accepted", None, descriptor(), None
    )
    monkeypatch.setattr(sources, "_observed_disposition", AsyncMock(return_value=disposition))
    pages = [
        page
        async for page in sources.iter_fhir_catalog_sources(
            connection, read_profile(), page_size=7, page_bytes=8192, endpoint_ids=("endpoint-one",)
        )
    ]
    assert pages == [(disposition,)]
    assert len(connection.fetch.await_args_list) == 2
    first_arguments, last_arguments = [call.args for call in connection.fetch.await_args_list]
    assert first_arguments[1:] == ("", 7, 1000, 8192, ("endpoint-one",))
    assert last_arguments[1] == "endpoint-one"


@pytest.mark.asyncio
async def test_inventory_refuses_missing_selected_endpoint_at_eof():
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(return_value=[]),
    )
    with pytest.raises(ValueError, match="endpoint_unavailable"):
        observed_pages = [
            page
            async for page in sources.iter_fhir_catalog_sources(connection, read_profile(), endpoint_ids=("missing",))
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("active,isolation", [(False, "repeatable read"), (True, "read committed")])
async def test_inventory_requires_a_pinned_transaction(active, isolation):
    connection = Mock(
        is_in_transaction=Mock(return_value=active), fetchval=AsyncMock(return_value=isolation), fetch=AsyncMock()
    )
    with pytest.raises(ValueError, match="transaction_required"):
        observed_pages = [page async for page in sources.iter_fhir_catalog_sources(connection, read_profile())]
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_current_recheck_preserves_logical_generation_and_refuses_changed_content(monkeypatch):
    from process import registry_fhir_catalog_observed_source as observed

    connection = MagicMock(
        is_in_transaction=Mock(return_value=True), fetchval=AsyncMock(return_value="repeatable read")
    )
    selected = descriptor()
    collect = AsyncMock(return_value=Mock(descriptor=selected))
    monkeypatch.setattr(observed, "collect_fhir_catalog_observation", collect)
    assert await sources.require_current_fhir_catalog_sources(connection, (selected,)) == (
        sources.fhir_catalog_source_generation(selected),
    )
    collect.return_value.descriptor = {**selected, "dataset_sha256": "b" * 64}
    with pytest.raises(ValueError, match="source_changed"):
        await sources.require_current_fhir_catalog_sources(connection, (selected,))


def legacy_descriptor():
    selected = descriptor()
    selected.pop("tables")
    selected.update(
        resource_table_oid=1,
        resource_leaf_oids=[2],
        header_table_oid=3,
        database_oid=4,
        native_catalog_sha256="retired",
    )
    selected["native_read_profile"].update(owner_role="old_owner", runtime_roles=["old_reader"])
    selected["admission"]["publication"].update(
        original_admission_version=None,
        original_admission_kind=None,
        original_admission_sha256=None,
        original_metadata_sha256=None,
    )
    selected.update(accepted_receipt_id="old-content", accepted_receipt_sha256="retired")
    return selected


def test_legacy_descriptor_projects_named_relations_and_logical_content():
    selected = sources.validate_fhir_catalog_source_descriptor(legacy_descriptor())
    assert selected["tables"] == {
        "header": {"schema": "source_payload", "name": "provider_directory_endpoint_dataset"},
        "resource": {"schema": "source_payload", "name": "provider_directory_dataset_resource"},
    }
    assert selected["native_read_profile"] == read_profile().document()
    assert selected["admission"] == descriptor()["admission"]
    assert sources.validate_fhir_catalog_source_descriptor(selected) == selected


def catalog_schema_descriptor():
    return {**legacy_descriptor(), "catalog_schema": "source_catalog"}


def test_catalog_schema_descriptor_projects_fixed_model_names_and_portable_content():
    selected = catalog_schema_descriptor()
    projected = sources.validate_fhir_catalog_source_descriptor(selected)
    assert projected["tables"] == {
        "header": {"schema": "source_catalog", "name": "provider_directory_endpoint_dataset"},
        "resource": {"schema": "source_payload", "name": "provider_directory_dataset_resource"},
    }
    assert projected["coordinates"] == selected["coordinates"]
    assert projected["admission"] == descriptor()["admission"]
    assert projected["native_read_profile"] == read_profile().document()
    assert sources.validate_fhir_catalog_source_descriptor(projected) == projected
    selected["database_oid"] = None
    selected["native_read_profile"]["owner_role"] = "another_retired_owner"
    assert sources.validate_fhir_catalog_source_descriptor(selected) == projected


def test_catalog_schema_projection_keeps_changed_content_in_the_logical_generation():
    selected = catalog_schema_descriptor()
    original = sources.fhir_catalog_source_generation(selected)
    selected["admission"]["families"]["Organization"]["rows"] = 1
    changed = sources.validate_fhir_catalog_source_descriptor(selected)
    assert sources.fhir_catalog_source_generation(changed) != original
    changed["admission"]["families"]["Organization"]["rows"] = 2
    with pytest.raises(ValueError, match="observation_invalid"):
        sources.validate_fhir_catalog_source_descriptor(changed)


def _mutate_catalog_descriptor(selected, damage):
    if damage == "unsafe_schema":
        selected["catalog_schema"] = 'bad"catalog'
        return
    if damage == "different_catalog":
        selected["catalog_schema"] = "different_catalog"
        return
    if damage == "admission":
        selected["admission"]["kind"] = "generic"
        return
    if damage == "count":
        selected["admission"]["families"]["Organization"]["rows"] = -1
        return
    if damage == "complete":
        selected["complete"] = False
        return
    selected["unexpected_scope"] = "another_catalog"


@pytest.mark.parametrize("damage", ["unsafe_schema", "different_catalog", "admission", "count", "complete", "extra"])
def test_catalog_schema_descriptor_refuses_invalid_logical_scope_or_observation(damage):
    selected = catalog_schema_descriptor()
    _mutate_catalog_descriptor(selected, damage)
    with pytest.raises(ValueError):
        sources.validate_fhir_catalog_source_descriptor(selected)


def test_legacy_content_admission_preserves_complete_counts_and_source_scope():
    selected = legacy_descriptor()
    selected["accepted_receipt_id"] = "content-one"
    selected["accepted_receipt_sha256"] = "b" * 64
    families = ["InsurancePlan", "Organization", "OrganizationAffiliation", "PractitionerRole"]
    selected["admission"] = {
        "version": 1,
        "kind": "generic",
        "proof_sha256": "c" * 64,
        "metadata_sha256": "b" * 64,
        "resource_count": 4,
        "resource_counts": dict.fromkeys(families, 1),
        "resource_types": families,
        "selected_resources": families,
        "completion_required_version": None,
        "completion_sha256": None,
        "proof_resource_scope": None,
    }
    projected = sources.validate_fhir_catalog_source_descriptor(selected)
    assert projected["admission"] == selected["admission"]
    assert sources.validate_fhir_catalog_source_descriptor(projected) == projected
    selected["admission"]["resource_count"] = 5
    with pytest.raises(ValueError, match="admission_invalid"):
        sources.validate_fhir_catalog_source_descriptor(selected)


@pytest.mark.parametrize("legacy", [False, True])
def test_descriptor_compatibility_refuses_unknown_fields(legacy):
    selected = legacy_descriptor() if legacy else descriptor()
    selected["unexpected_scope"] = "other"
    with pytest.raises(ValueError):
        sources.validate_fhir_catalog_source_descriptor(selected)
