# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed provider selectors and exact raw-address projection refusals."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import Issuer, PlanNPIRaw
from process import registry_network_catalog_read as catalog
from process import registry_provider_plan_read as provider_reader
from process.initial import _build_mrf_address_rows
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from process.registry_provider_plan_read import RegistryProviderPlansQuery, _project_addresses
from tests.test_registry_network_catalog_read import batch_boundary as batch_boundary


@pytest.mark.parametrize(
    "changes",
    [
        {"npi": 1234567890},
        {"npi": "１２３４５６７８９０"},
        {"npi": "123456789"},
        {"npi": "12345678901"},
        {"year": True},
        {"year": 1999},
        {"year": 2101},
        {"year": "2026"},
        {"generation_id": True},
        {"generation_id": 0},
        {"generation_id": "1"},
        {"generation_id": 9223372036854775808},
    ],
)
def test_closed_provider_plan_query(changes):
    with pytest.raises(RegistryNetworkCatalogError):
        RegistryProviderPlansQuery(**({"npi": "1000000491"} | changes))


def test_provider_query_preserves_existing_identity_and_year_boundaries():
    assert RegistryProviderPlansQuery("0000000000", 2000, 9223372036854775807).npi == "0000000000"


def _document():
    address_by_field = {
        "address": "123 Example Street",
        "city": "Sample City",
        "state": "CA",
        "zip": "90210",
        "country": "US",
        "phone": "123",
    }
    _, evidence = _build_mrf_address_rows(
        {"npi": 1000000491, "addresses": [address_by_field]},
        {42: {"checksum_network": 42, "issuer_id": 7, "year": 2026, "network_tier": "tier-42"}},
        "import-one",
        "https://source.example.test/providers",
        None,
    )
    proof = evidence[0]
    proof["address_key"] = str(proof["address_key"])
    return {
        "npi_info": {
            **{column.name: None for column in PlanNPIRaw.__table__.columns},
            "npi": 1000000491,
            "checksum_network": 42,
            "addresses": [address_by_field, address_by_field | {"address": "Other office"}],
        },
        "issuer_info": {**{column.name: None for column in Issuer.__table__.columns}, "issuer_id": 7},
        "proofs": [proof],
    }


def test_exact_address_projection_preserves_only_the_proved_original_entry():
    document = _document()
    result = _project_addresses(document)
    assert result["npi_info"]["addresses"] == document["npi_info"]["addresses"][:1]
    assert result["npi_info"]["addresses"][0]["phone"] == "123"


@pytest.mark.parametrize(
    "field,value",
    [
        ("checksum", 1),
        ("source_record_id", "foreign"),
        ("address_key", "00000000-0000-0000-0000-000000000001"),
        ("first_line", "Other office"),
        ("city_name", "OTHER"),
        ("postal_code", "00000"),
    ],
)
def test_projection_requires_all_identity_fields_even_if_other_proof_fields_match(field, value):
    document = _document()
    document["proofs"][0][field] = value
    with pytest.raises(RegistryNetworkCatalogError, match="source_unavailable"):
        _project_addresses(document)


@pytest.mark.parametrize("addresses", [None, [], [{"city": "Incomplete"}]])
def test_unproved_selected_raw_row_is_unavailable_not_certified_empty(addresses):
    document = _document()
    document["npi_info"]["addresses"] = addresses
    with pytest.raises(RegistryNetworkCatalogError, match="source_unavailable"):
        _project_addresses(document)


@pytest.mark.parametrize("timestamp", ["2026-01-02T03:04:05", "2026-01-02T03:04:05+00:00"])
def test_projection_retains_legacy_timestamp_encoding(timestamp):
    document = _document()
    document["npi_info"]["last_updated_on"] = timestamp
    assert _project_addresses(document)["npi_info"]["last_updated_on"] == "2026-01-02T03:04:05Z"


def test_projection_refuses_internal_archive_columns():
    document = _document()
    document["issuer_info"]["internal_receipt"] = "not-public"
    with pytest.raises(RegistryNetworkCatalogError):
        _project_addresses(document)


@pytest.fixture
def provider_boundary(batch_boundary, monkeypatch):
    connection, manifest, pinned_revisions = batch_boundary
    monkeypatch.setattr(connection, "fetchval", AsyncMock(return_value=True), raising=False)
    monkeypatch.setattr(provider_reader, "resolve_retained_registry_source_recipes", AsyncMock(return_value=()))
    monkeypatch.setattr(
        provider_reader, "pin_retained_approved_membership_source", AsyncMock(return_value=SimpleNamespace())
    )
    monkeypatch.setattr(
        provider_reader, "_retained_memberships", AsyncMock(return_value=("retained_memberships", "true", ()))
    )
    return connection, manifest, pinned_revisions


@pytest.mark.asyncio
@pytest.mark.parametrize("requested_generation", [None, 9])
async def test_supplied_manifest_uses_exact_generation_and_retained_source_checks(
    provider_boundary, monkeypatch, requested_generation
):
    connection, manifest, pinned_revisions = provider_boundary
    resolver = AsyncMock(return_value=replace(manifest))
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", resolver)
    response = await provider_reader.read_registry_provider_plans(
        connection,
        RegistryProviderPlansQuery("1000000491", generation_id=requested_generation),
        excluded_network_ids=(2,),
        control_schema="sample_control",
        manifest=manifest,
    )
    resolver.assert_awaited_once_with(connection, generation_id=9, control_schema="sample_control")
    assert response == {
        "generation": "9",
        "approved_custom_revision": "4",
        "npi_data": [],
        "plan_data": [],
        "issuer_data": [],
    }
    assert pinned_revisions == [4]
    provider_reader.resolve_retained_registry_source_recipes.assert_awaited_once_with(
        connection, manifest, control_schema="sample_control"
    )
    provider_reader.pin_retained_approved_membership_source.assert_awaited_once_with(
        connection, approved_revision=4, control_schema="sample_control"
    )
    provider_reader._retained_memberships.assert_awaited_once_with(connection, manifest, "sample_control")
    assert len(connection.calls) == 3  # Transaction, complete accounting report and address parity remain mandatory.


@pytest.mark.asyncio
async def test_unsupplied_manifest_preserves_current_head_selection(provider_boundary, monkeypatch):
    connection, manifest, _ = provider_boundary
    resolver = AsyncMock(return_value=manifest)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", resolver)
    await provider_reader.read_registry_provider_plans(
        connection, RegistryProviderPlansQuery("1000000491"), excluded_network_ids=(), control_schema="sample_control"
    )
    resolver.assert_awaited_once_with(connection, generation_id=None, control_schema="sample_control")


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_generation", [True, 0, -1, "9", 9223372036854775808])
async def test_supplied_manifest_generation_is_not_authority(provider_boundary, invalid_generation):
    connection, manifest, pinned_revisions = provider_boundary
    with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
        await provider_reader.read_registry_provider_plans(
            connection,
            RegistryProviderPlansQuery("1000000491"),
            excluded_network_ids=(),
            control_schema="sample_control",
            manifest=replace(manifest, generation_id=invalid_generation),
        )
    assert connection.calls == [] and pinned_revisions == []
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_supplied_manifest_requires_exact_type_and_matching_query_before_database(provider_boundary):
    connection, manifest, _ = provider_boundary
    for supplied, query, message in (
        (SimpleNamespace(**manifest.__dict__), RegistryProviderPlansQuery("1000000491"), "request_invalid"),
        (manifest, RegistryProviderPlansQuery("1000000491", generation_id=10), "source_changed"),
    ):
        with pytest.raises(RegistryNetworkCatalogError, match=message):
            await provider_reader.read_registry_provider_plans(
                connection, query, excluded_network_ids=(), control_schema="sample_control", manifest=supplied
            )
    assert connection.calls == []
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"candidate_id": "22222222-2222-4222-8222-222222222222"},
        {"schema_name": "other_candidate"},
        {"schema_revision": 2},
        {"source_generations": {"custom_membership": "a" * 64, "other": "c" * 64}},
        {"approved_custom_revision": 5},
        {"manifest_sha256": "c" * 64},
        {"address_table_oid": 124},
    ],
)
async def test_supplied_manifest_must_match_entire_revalidated_identity(provider_boundary, changes):
    connection, manifest, _ = provider_boundary
    with pytest.raises(RegistryNetworkCatalogError, match="source_changed"):
        await provider_reader.read_registry_provider_plans(
            connection,
            RegistryProviderPlansQuery("1000000491"),
            excluded_network_ids=(),
            control_schema="sample_control",
            manifest=replace(manifest, **changes),
        )
    connection.fetchval.assert_not_awaited()
    provider_reader.resolve_retained_registry_source_recipes.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transaction,isolation,read_only",
    [(False, "repeatable read", "on"), (True, "read committed", "on"), (True, "repeatable read", "off")],
)
async def test_supplied_manifest_does_not_bypass_transaction_checks(
    provider_boundary, transaction, isolation, read_only
):
    connection, manifest, pinned_revisions = provider_boundary
    connection.in_transaction, connection.isolation, connection.read_only = transaction, isolation, read_only
    with pytest.raises(RegistryNetworkCatalogError, match="transaction_required"):
        await provider_reader.read_registry_provider_plans(
            connection,
            RegistryProviderPlansQuery("1000000491"),
            excluded_network_ids=(),
            control_schema="sample_control",
            manifest=manifest,
        )
    assert pinned_revisions == []
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["membership", "arrays", "address", "source"])
async def test_supplied_manifest_does_not_bypass_live_integrity_checks(provider_boundary, corruption):
    connection, manifest, _ = provider_boundary
    if corruption == "membership":
        connection.report["membership_rows"] = 1
    elif corruption == "arrays":
        connection.parity["changed_arrays"] = 1
    elif corruption == "address":
        connection.parity["address_rows"] = 2
    else:
        manifest.source_generations["custom_membership"] = "c" * 64
    with pytest.raises(RegistryNetworkCatalogError, match="directory_unavailable|source_changed"):
        await provider_reader.read_registry_provider_plans(
            connection,
            RegistryProviderPlansQuery("1000000491"),
            excluded_network_ids=(),
            control_schema="sample_control",
            manifest=manifest,
        )
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_supplied_manifest_preserves_unknown_provider_refusal_and_source_failure(provider_boundary):
    connection, manifest, _ = provider_boundary
    connection.fetchval.return_value = False
    with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
        await provider_reader.read_registry_provider_plans(
            connection,
            RegistryProviderPlansQuery("1000000491"),
            excluded_network_ids=(),
            control_schema="sample_control",
            manifest=manifest,
        )
    provider_reader.resolve_retained_registry_source_recipes.assert_not_awaited()
    connection.fetchval.return_value = True
    provider_reader.resolve_retained_registry_source_recipes.side_effect = ValueError("retained recipe unavailable")
    with pytest.raises(RegistryNetworkCatalogError, match="source_unavailable") as caught:
        await provider_reader.read_registry_provider_plans(
            connection,
            RegistryProviderPlansQuery("1000000491"),
            excluded_network_ids=(),
            control_schema="sample_control",
            manifest=manifest,
        )
    assert str(caught.value.__cause__) == "retained recipe unavailable"
    provider_reader.pin_retained_approved_membership_source.assert_not_awaited()


@pytest.mark.parametrize(
    "network_ids", [[], (), (0,), (True,), (-1,), (2147483648,), (2, 1), (1, 1), tuple(range(1, 102))]
)
@pytest.mark.asyncio
async def test_explicit_provider_plan_network_scope_refuses_before_source_reads(provider_boundary, network_ids):
    connection, manifest, _ = provider_boundary
    with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
        await provider_reader.read_registry_provider_plans(
            connection,
            RegistryProviderPlansQuery("1000000491"),
            excluded_network_ids=(),
            control_schema="sample_control",
            manifest=manifest,
            network_ids=network_ids,
        )
    assert connection.calls == []
    provider_reader.resolve_retained_registry_source_recipes.assert_not_awaited()


def test_selected_plan_network_predicate_is_before_source_rows_and_complete_bounds():
    sql = provider_reader._READ_SQL
    predicate = "($6::integer[] IS NULL OR record_key::integer=ANY($6::integer[]))"
    assert (
        sql.index("NOT record_key::integer=ANY($2")
        < sql.index(predicate)
        < sql.index("selected AS")
        < sql.index("bounds AS")
    )
    assert provider_reader._provider_plan_network_ids(None) is None
    assert provider_reader._provider_plan_network_ids((1, 2147483647)) == (1, 2147483647)
