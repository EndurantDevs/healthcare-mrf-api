# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed catalog evidence keeps explicit references and rejects mismatched plans."""

import copy
from contextlib import asynccontextmanager
from dataclasses import asdict, replace

import pytest

from process import registry_fhir_plan_tier_catalog as catalog
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_reference_rollup import staged_plan_tier_references
from process.registry_fhir_plan_tier_catalog import _validated_pair
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests.test_registry_fhir_catalog_identity_contracts import _connection as identity_connection
from tests.test_registry_fhir_catalog_projection import source_context
from tests.test_registry_fhir_catalog_reference_rollup_postgres import (
    copy_resources,
    resource_row,
)
from tests.test_registry_fhir_catalog_reference_rollup_postgres import (
    reference_database as reference_database,
)
from tests.test_registry_fhir_catalog_sources import descriptor as source_descriptor
from tests.test_registry_fhir_plan_tier_bindings import tier_document
from tests.test_registry_fhir_plan_tier_pipeline import plan_tier_rows


def _rows():
    return [
        {field: row[field] for field in ("resource_type", "resource_id", "payload_hash", "payload_json")}
        for row in plan_tier_rows(1, "dataset")
        if row["resource_type"] in {"InsurancePlan", "PractitionerRole"}
    ]


def test_plan_declared_tiers_keep_organization_edges():
    rows = _rows()
    plan = next(row for row in rows if row["resource_type"] == "InsurancePlan")
    plan["payload_json"]["network_refs"] = ["Organization/network"]
    plan["payload_json"]["plan_json"]["network_tiers"] = ["preferred", "standard"]
    old = extract_fhir_catalog_batch([plan], profile="published_normalized")
    typed = extract_fhir_catalog_batch([plan], profile="published_normalized", plan_tiers=True)
    assert typed.edges[-1] == old.edges[0]
    declarations = [edge for edge in typed.edges if getattr(edge, "plan_tier", None) is not None]
    assert {edge.plan_tier["network_key_id"] for edge in declarations} == {
        tier_document(tier=tier)["network_key_id"] for tier in ("preferred", "standard")
    }
    assert typed.edges[0].plan_tier is None


@pytest.mark.parametrize("damage", ("absent", "different-plan", "different-scope", "forged-tier"))
def test_projection_requires_exact_observed_plan(damage):
    edges = extract_fhir_catalog_batch(_rows(), profile="published_normalized", plan_tiers=True).edges
    pair_by_kind = {"plan": asdict(edges[0]), "edge": asdict(edges[1])}
    if damage == "absent":
        pair_by_kind["plan"] = None
    elif damage == "different-plan":
        pair_by_kind["plan"]["owner_resource_id"] = "another-plan"
    elif damage == "different-scope":
        pair_by_kind["plan"]["plan_scope"] = tier_document(year=2027)["plan_scope"]
    else:
        pair_by_kind["edge"]["plan_tier"]["network_tier"] = "different"
    with pytest.raises(ValueError):
        _validated_pair(pair_by_kind)


@pytest.mark.asyncio
async def test_role_rollup_keeps_exact_tiers_and_direct_reference_counts(reference_database):
    fixture = reference_database
    role = next(row for row in _rows() if row["resource_type"] == "PractitionerRole")
    payload = role["payload_json"] | {"network_refs": ["Organization/network"]}
    await copy_resources(
        fixture, [resource_row("PractitionerRole", f"role-{index:04}", payload) for index in range(20)]
    )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        async with staged_plan_tier_references(
            fixture.connection, schema_name=fixture.schema, dataset_id="dataset", page_rows=7
        ) as stage:
            observations = [observation async for page in stage.iter_batches() for observation in page]
            assert stage.accounting.source_rows == 20 and stage.accounting.reference_rows == 40
            assert len(observations) == 2 and all(observation.reference_rows == 20 for observation in observations)
            assert {
                getattr(observation.edge, "plan_tier", None)["network_key_id"]
                for observation in observations
                if getattr(observation.edge, "plan_tier", None)
            } == {tier_document()["network_key_id"]}
            assert any(observation.edge.network_resource_id == "network" for observation in observations)


@pytest.mark.asyncio
async def test_rollup_refuses_a_forged_tier_without_hiding_it(reference_database):
    fixture = reference_database
    role = next(row for row in _rows() if row["resource_type"] == "PractitionerRole")
    payload = copy.deepcopy(role["payload_json"])
    payload["network_tier"] = "different"
    await copy_resources(fixture, [resource_row("PractitionerRole", "forged", payload)])
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(ValueError):
            async with staged_plan_tier_references(
                fixture.connection, schema_name=fixture.schema, dataset_id="dataset"
            ) as stage:
                [observation async for page in stage.iter_batches() for observation in page]


def _projection_inputs():
    edges = extract_fhir_catalog_batch(_rows(), profile="published_normalized", plan_tiers=True).edges
    descriptor = source_descriptor()
    descriptor["coordinates"]["source_id"] = UHC_PROVIDER_FILE_SOURCE_ID
    descriptor["source_aliases"] = [UHC_PROVIDER_FILE_SOURCE_ID]
    context = replace(
        source_context(),
        coordinates=RegistryNetworkSourceCoordinates(**descriptor["coordinates"]),
        profile="published_normalized",
        capture_sha256=None,
    )
    return descriptor, context, {"plan": asdict(edges[0]), "edge": asdict(edges[1])}


@pytest.mark.asyncio
async def test_oversized_native_plan_page_refuses_before_decoding_or_advancing():
    connection = identity_connection()
    connection.fetch.return_value = [{"key": "a" * 64, "document": None}]
    with pytest.raises(ValueError, match="^registry_fhir_plan_tier_bounds$"):
        [page async for page in catalog.plan_tier_pages(connection, "synthetic_catalog", "b" * 64)]
    assert connection.fetch.await_count == 1
    assert connection.fetch.await_args.args[1:] == ("b" * 64, "")


@pytest.mark.parametrize("fault", ["profile", "display-bytes"])
def test_projection_refuses_foreign_profile_or_oversized_unicode_display(fault):
    _, context, pair = _projection_inputs()
    if fault == "profile":
        context = replace(context, profile="retained_normalized")
    else:
        pair["plan"]["plan_name"] = "界" * 170
    before = copy.deepcopy(pair)
    reason = "source_invalid" if fault == "profile" else "bounds"
    with pytest.raises(ValueError, match=f"^registry_fhir_plan_tier_{reason}$"):
        catalog.format_plan_tier_projection(
            (pair,), context, {tier_document()["network_key_id"]: 42}, as_of=context.accepted_at.date()
        )
    assert pair == before


def test_individually_bounded_reviewed_networks_still_respect_total_projection_budget():
    """Real plan declarations and valid aliases can exceed the combined COPY limit."""
    from process.provider_directory_fhir import (
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        resource_payload_sha256_for_contract,
    )

    _, context, _ = _projection_inputs()
    plan = next(row for row in _rows() if row["resource_type"] == "InsurancePlan")
    plan["payload_json"]["plan_json"]["network_tiers"] = [f"tier-{index}" for index in range(100)]
    plan["payload_hash"] = resource_payload_sha256_for_contract(
        plan["payload_json"], SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT, resource_type="InsurancePlan"
    )
    edges = extract_fhir_catalog_batch((plan,), profile="published_normalized", plan_tiers=True).edges
    pairs = tuple({"plan": asdict(edges[0]), "edge": asdict(edge)} for edge in edges if edge.plan_tier is not None)
    ids_by_key = {pair["edge"]["plan_tier"]["network_key_id"]: index for index, pair in enumerate(pairs, 1)}
    aliases = [f"alias-{index:03d}-" + "x" * 502 for index in range(100)]
    networks = tuple(
        dict(network_id=index, display_name="Reviewed Network", aliases=aliases, archived=False, revision=1)
        for index in ids_by_key.values()
    )
    assert len(ids_by_key) == 100 and all(len(alias.encode()) == 512 for alias in aliases)
    with pytest.raises(ValueError, match="^registry_fhir_plan_tier_bounds$"):
        catalog.format_plan_tier_projection(pairs, context, ids_by_key, networks, as_of=context.accepted_at.date())


@asynccontextmanager
async def _identity_transaction():
    yield


@pytest.mark.asyncio
async def test_plan_tier_source_refuses_before_identity_copy():
    descriptor, context, pair = _projection_inputs()
    descriptor["profile"] = "retained_normalized"
    connection = identity_connection()
    with pytest.raises(ValueError, match="^registry_fhir_plan_tier_source_invalid$"):
        await catalog.plan_tier_projection(
            connection,
            descriptor,
            context,
            (pair,),
            revision=1,
            control_schema="synthetic_catalog",
            allocate=False,
            as_of=context.accepted_at.date(),
        )
    connection.copy_records_to_table.assert_not_awaited()
    connection.execute.assert_not_awaited()
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("network_id,conflict", [(42, True), (None, False)])
async def test_unresolved_or_conflicting_identity_cannot_read_canonical_networks(network_id, conflict):
    descriptor, context, pair = _projection_inputs()
    connection = identity_connection()
    connection.transaction = _identity_transaction
    connection.fetch.return_value = [
        dict(organization_id=tier_document()["network_key_id"], network_id=network_id, conflict=conflict)
    ]
    with pytest.raises(ValueError, match="^registry_fhir_catalog_identity_conflict$"):
        await catalog.plan_tier_projection(
            connection,
            descriptor,
            context,
            (pair,),
            revision=1,
            control_schema="synthetic_catalog",
            allocate=False,
            as_of=context.accepted_at.date(),
        )
    connection.copy_records_to_table.assert_awaited_once()
    assert connection.fetch.await_count == 1
    assert "registry_approved_record" not in connection.fetch.await_args.args[0]
