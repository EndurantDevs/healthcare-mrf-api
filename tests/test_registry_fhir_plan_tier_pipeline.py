# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published typed plan evidence retains exact provider-office and capture boundaries."""

import copy
import json
from dataclasses import asdict, replace

import pytest

from process import network_fhir_membership_source as membership
from process import network_fhir_source_witness_capture as capture_module
from process import registry_source_contribution_store as facts
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_fhir_retained_capture_contract import registry_location_id
from process.network_source_binding_store import apply_network_source_binding_batch
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from process.registry_fhir_plan_tier import (
    TRANSFORM_VERSION,
    extraction_transform,
    needs_plan_tier_refresh,
    supports_plan_tiers,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests import generic_source_successor_postgres_support as source
from tests import source_witness_legacy_postgres_support as published_fixture
from tests.source_witness_capture_postgres_support import capture_arguments, source_modules, source_witness_case
from tests.test_network_fhir_retained_capture_postgres import _binding_command
from tests.test_network_fhir_source_witness_capture_postgres import _capture
from tests.test_network_fhir_source_witness_membership_postgres import (
    _extract_facts,
    _mapped_facts,
    _preview_counts,
    _read_guards_refuse,
    _registry_read_bootstrap,
    _source_recipe,
    _temporary_facts,
)
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft
from tests.test_registry_fhir_catalog_sources import descriptor as source_descriptor
from tests.test_registry_fhir_plan_tier_bindings import tier_document
from tests.test_verified_office_source_witnesses_v2_postgres import _registry_on_source

_EDITION_ROWS = source._edition_rows


def plan_tier_rows(edition, dataset):
    tier = tier_document()
    resource_rows = _EDITION_ROWS(edition, dataset)
    typed_rows = []
    for resource_row in resource_rows:
        if resource_row["resource_type"] == "Organization" or resource_row["resource_id"].startswith(
            ("provider-only-", "role-provider-only-")
        ):
            continue
        resource_payload = copy.deepcopy(resource_row["payload_json"])
        resource_payload["source_id"] = UHC_PROVIDER_FILE_SOURCE_ID
        if resource_row["resource_type"] == "InsurancePlan":
            resource_payload.update(
                resource_id=tier["plan_resource_id"],
                network_refs=[],
                plan_json={
                    "canonical_contract_id": "healthporta.uhc.provider-directory-canonical.v2",
                    "plan_key": tier["plan_scope"],
                    "network_tiers": [],
                },
            )
        elif resource_row["resource_type"] == "PractitionerRole":
            resource_payload.update(
                network_refs=[],
                insurance_plan_refs=["InsurancePlan/" + tier["plan_resource_id"]],
                **{key: tier[key] for key in ("plan_scope", "network_tier", "network_key_id")},
            )
        typed_rows.append(
            {
                **resource_row,
                "resource_id": resource_payload["resource_id"],
                "payload_json": resource_payload,
                "payload_hash": source.directory.resource_payload_sha256_for_contract(
                    resource_payload,
                    source.directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
                    resource_type=resource_row["resource_type"],
                ),
            }
        )
    return typed_rows


def test_typed_extraction_is_explicit_and_previous_transform_is_unchanged():
    resource_rows = [
        {key: resource_row[key] for key in ("resource_type", "resource_id", "payload_hash", "payload_json")}
        for resource_row in plan_tier_rows(1, "edition")
        if resource_row["resource_type"] in {"InsurancePlan", "PractitionerRole"}
    ]
    ordinary = extract_fhir_catalog_batch(resource_rows, profile="published_normalized")
    typed = extract_fhir_catalog_batch(resource_rows, profile="published_normalized", plan_tiers=True)
    assert len(ordinary.edges) == 1 and ordinary.edges[0].resolution_reason == "reference_missing"
    assert len(typed.edges) == 2 and all("plan_scope" in asdict(edge) for edge in typed.edges)
    descriptor = source_descriptor()
    descriptor["profile"] = "published_normalized"
    descriptor["coordinates"]["source_id"] = UHC_PROVIDER_FILE_SOURCE_ID
    descriptor["source_aliases"] = [UHC_PROVIDER_FILE_SOURCE_ID]
    generation = fhir_catalog_source_generation(descriptor)
    source_entry_by_field = dict(source_generation=generation, source_id=UHC_PROVIDER_FILE_SOURCE_ID, status="current")
    prior_snapshot_by_field = dict(projector_version="fhir-catalog/v1", source_vector_json=[source_entry_by_field])
    current_snapshot_by_field = dict(
        projector_version=TRANSFORM_VERSION,
        source_vector_json=[source_entry_by_field | {"extraction_transform": TRANSFORM_VERSION}],
    )
    assert supports_plan_tiers(descriptor) and needs_plan_tier_refresh(
        prior_snapshot_by_field, current_snapshot_by_field, descriptor
    )
    assert not needs_plan_tier_refresh(current_snapshot_by_field, current_snapshot_by_field, descriptor)
    unavailable_snapshot_by_field = dict(
        projector_version=TRANSFORM_VERSION, source_vector_json=[source_entry_by_field | {"status": "last_good"}]
    )
    assert not needs_plan_tier_refresh(prior_snapshot_by_field, unavailable_snapshot_by_field, descriptor)
    assert needs_plan_tier_refresh(unavailable_snapshot_by_field, current_snapshot_by_field, descriptor)
    assert needs_plan_tier_refresh(
        {
            **unavailable_snapshot_by_field,
            "source_vector_json": json.dumps(unavailable_snapshot_by_field["source_vector_json"]),
        },
        {
            **current_snapshot_by_field,
            "source_vector_json": json.dumps(current_snapshot_by_field["source_vector_json"]),
        },
        descriptor,
    )
    with extraction_transform("fhir-catalog/v1"):
        assert not supports_plan_tiers(descriptor)
        assert extract_fhir_catalog_batch(resource_rows, profile="published_normalized") == ordinary
    assert supports_plan_tiers(descriptor)
    assert not supports_plan_tiers({**descriptor, "profile": "retained_normalized"})


def _published_resources(monkeypatch, resources, *, unresolved=0):
    """Seed exact synthetic resources before original publication guards are installed."""
    monkeypatch.setattr(published_fixture, "SOURCE", UHC_PROVIDER_FILE_SOURCE_ID)
    monkeypatch.setattr(
        published_fixture,
        "legacy_resources",
        lambda: resources,
    )
    original_origin = published_fixture.reviewed_legacy_origin

    async def reviewed_origin(*args):
        return replace(await original_origin(*args), unresolved_plan_network_count=unresolved)

    monkeypatch.setattr(published_fixture, "reviewed_legacy_origin", reviewed_origin)


async def _approve_tier(serving, recipe, tier):
    """Use native binding COPY and actual registry approval for one exact tier."""
    connection, control, engine = serving
    actor = _actor()
    network = await _draft(engine, control, _create("network"), actor)
    command = _binding_command(
        recipe.binding_coordinates,
        network["record_id"],
        "placeholder",
        recipe.source_pin.source_witness_capture.capture_sha256,
    )
    document = json.loads(command.input_bytes)[0]
    document.update(
        source_key="UHCPlanTier/" + tier["network_key_id"],
        source_scope_json=tier | {"capture_sha256": recipe.source_pin.source_witness_capture.capture_sha256},
    )
    command = replace(command, input_bytes=json.dumps([document]).encode())
    async with connection.transaction():
        records = (await apply_network_source_binding_batch(connection, command, actor, control_schema=control))[
            "records"
        ]
    approval = await _approve(connection, control, await _command(connection, control, network, *records), actor)
    async with connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            connection, approved_revision=approval["approved_revision"], control_schema=control
        )
    return approved


@pytest.mark.asyncio
async def test_observed_published_plan_tier_replays_exact_office(monkeypatch):
    """Read existing published normalized data through the genuine Source exporter."""
    _published_resources(
        monkeypatch,
        [
            (resource_row["resource_type"], resource_row["resource_id"], resource_row["payload_json"])
            for resource_row in plan_tier_rows(1, "dataset-one")
        ],
    )
    async with (
        source_witness_case(monkeypatch, legacy=True) as case,
        _registry_on_source(case.fixture, monkeypatch) as serving,
    ):
        await _registry_read_bootstrap(case, serving)
        tier = await _original_catalog_tier(case)
        capture = await _capture(case, capture_module, capture_arguments(case))
        anchor = case.anchors_by_id[capture.capture_id]
        recipe = _source_recipe(capture, anchor)
        approved = await _approve_tier(serving, recipe, tier)
        control = serving[1]
        await _temporary_facts(case.reader)
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            read, documents, _ = await _extract_facts(case, recipe, approved, control, anchor, limit=1)
            descriptor, members = await _mapped_facts(case.reader, recipe, approved, control)
            await _read_guards_refuse(case, recipe, approved, control, read)
            assert descriptor["unresolved_rows"] == 0 and descriptor["membership_rows"] == 1
            assert [(resource_row["provider_id"], resource_row["location_id"]) for resource_row in members] == [
                ("1234567893", str(registry_location_id(UHC_PROVIDER_FILE_SOURCE_ID, "office-one")))
            ]
            batch = await membership.read_fhir_membership_fact_batch(
                case.reader,
                recipe.source_pin,
                registry_schema=control,
                approved_source=approved,
                binding_coordinates=recipe.binding_coordinates,
                retained_read=read,
            )
            assert (batch.unresolved_rows, batch.membership_rows, json.loads(batch.input_bytes)) == (0, 1, members)
            counts = await _preview_counts(case, recipe, approved, control, monkeypatch)
            assert counts == {"mapped_rows": 1, "omitted_rows": 0, "unresolved_rows": 0}
            assert all(
                resource_row.get("source_key") == "UHCPlanTier/" + tier["network_key_id"]
                for resource_row in documents
                if resource_row["kind"] == "edge"
            )
            assert (
                facts.decode_source_witness_page_v2(
                    (
                        await membership.read_fhir_witness_page_v2(
                            case.reader,
                            recipe.source_pin,
                            registry_schema=control,
                            approved_source=approved,
                            binding_coordinates=recipe.binding_coordinates,
                            retained_read=read,
                        )
                    )
                )
                == documents
            )


from process import registry_fhir_catalog_sync as catalog_sync
from tests.test_registry_fhir_catalog_observation_sync_postgres import (
    _prepare,
    _publish,
    _query,
)
from tests.test_registry_fhir_catalog_observation_sync_postgres import (
    metadata_custody as metadata_custody,
)
from tests.test_registry_fhir_catalog_observation_sync_postgres import (
    observation_case as observation_case,
)
from tests.test_registry_fhir_catalog_observation_sync_postgres import (
    observed_source_db as observed_source_db,
)


async def _catalog_plan_rows(case):
    await case.source.admin.execute(f'DELETE FROM "{case.source.catalog}".provider_directory_source')
    await case.source.admin.execute(
        f"INSERT INTO \"{case.source.catalog}\".provider_directory_source VALUES ($1,'endpoint-one')",
        UHC_PROVIDER_FILE_SOURCE_ID,
    )
    await case.source.admin.execute(f'DELETE FROM "{case.source.schema}".provider_directory_dataset_resource')
    rows = plan_tier_rows(1, "dataset-one")
    await case.source.admin.copy_records_to_table(
        "provider_directory_dataset_resource",
        schema_name=case.source.schema,
        records=[
            (
                "dataset-one",
                row["resource_type"],
                row["resource_id"],
                row["payload_hash"],
                json.dumps(row["payload_json"]),
            )
            for row in rows
        ],
    )


@pytest.mark.asyncio
async def test_catalog_upgrade_and_carried_typed_relationships(observation_case, monkeypatch):
    """Preserve old sealed metadata, target one source upgrade, then carry exact typed facts."""
    case = observation_case
    await _catalog_plan_rows(case)
    with monkeypatch.context() as old, extraction_transform("fhir-catalog/v1"):
        old.setattr(catalog_sync, "PROJECTOR_VERSION", "fhir-catalog/v1")
        first, _, _ = await _prepare(case)
    # A sealed old-version job can still complete with the current runtime.
    await _publish(case, first)
    original_vector = first["source_vector_json"]
    assert original_vector[0].get("extraction_transform") is None
    endpoint = await case.source.admin.fetchrow(
        f'SELECT * FROM "{case.source.catalog}".provider_directory_api_endpoint'
    )
    await case.source.admin.execute(f'DELETE FROM "{case.source.catalog}".provider_directory_api_endpoint')
    unavailable, prior, _ = await _prepare(case)
    await _publish(case, unavailable, prior)
    assert unavailable["projector_version"] == TRANSFORM_VERSION
    assert unavailable["source_vector_json"][0]["status"] == "last_good"
    assert "extraction_transform" not in unavailable["source_vector_json"][0]
    await case.source.admin.copy_records_to_table(
        "provider_directory_api_endpoint", schema_name=case.source.catalog, records=[tuple(endpoint)]
    )
    second, previous, accounting = await _prepare(case)
    assert second["source_vector_json"][0]["source_generation"] == original_vector[0]["source_generation"]
    assert second["source_vector_json"][0]["extraction_transform"] == TRANSFORM_VERSION
    assert accounting["sources"][0]["extraction"].get("reused_observations") is not True
    assert accounting["sources"][0]["projection"]["created_networks"] == 1
    await _publish(case, second, previous)
    page = await _query(case)
    typed_networks = [
        network_row
        for network_row in page["items"]
        if any(
            binding["source_scope"].get("kind") == "uhc_plan_tier_v1"
            for binding in network_row["source_bindings"]["items"]
        )
    ]
    assert len(typed_networks) == 1
    relationships = typed_networks[0]["matching_relationships"]["items"]
    assert len(relationships) == 1 and relationships[0]["plan"]["resource_id"] == tier_document()["plan_resource_id"]
    await case.source.admin.execute(f'DELETE FROM "{case.source.catalog}".provider_directory_api_endpoint')
    third, prior, _ = await _prepare(case)
    await _publish(case, third, prior)
    carried = await _query(case)
    carried_typed = next(
        network_row for network_row in carried["items"] if network_row["network_id"] == typed_networks[0]["network_id"]
    )
    assert carried_typed["matching_relationships"]["total"] == 1
    assert carried_typed["source_bindings"]["items"][0]["status"] == "last_good"


async def _original_catalog_tier(case):
    """Use the genuine Source catalog export and its delegated bounded Core replay."""
    catalog, _ = source_modules()
    observations = []

    async def collect(rows):
        observations.extend(rows)

    async with catalog.open_catalog_source_export(case.source_runtime, source_profile=case.source_profile) as export:
        descriptors = [
            source_record.descriptor
            async for page in export["inventory"]()
            for source_record in page
            if source_record.descriptor is not None
            and source_record.descriptor["coordinates"]["source_id"] == UHC_PROVIDER_FILE_SOURCE_ID
        ]
        assert len(descriptors) == 1
        assert await export["replay"](descriptors[0], collect) == 2
    edges = [json.loads(source_record[-1]) for source_record in observations]
    assert len(edges) == 2
    plan = next(edge for edge in edges if edge["owner_resource_type"] == "InsurancePlan")
    role = next(edge for edge in edges if edge["owner_resource_type"] == "PractitionerRole")
    assert plan["plan_scope"] == role["plan_tier"]["plan_scope"]
    from datetime import datetime, timezone

    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.registry_fhir_catalog_projection import SourceCatalogContext
    from process.registry_fhir_plan_tier_catalog import format_plan_tier_projection

    context = SourceCatalogContext(
        RegistryNetworkSourceCoordinates(**descriptors[0]["coordinates"]),
        fhir_catalog_source_generation(descriptors[0]),
        "c" * 64,
        datetime(2026, 11, 3, tzinfo=timezone.utc),
        "published_normalized",
    )
    projection = format_plan_tier_projection(
        (dict(edge=role, plan=plan),),
        context,
        {role["plan_tier"]["network_key_id"]: 42},
        as_of=context.accepted_at.date(),
    )
    assert len(projection.network_rows) == len(projection.relationship_rows) == len(projection.binding_rows) == 1
    assert (
        json.loads(projection.binding_rows[0][4])["source_scope"]["network_key_id"] == tier_document()["network_key_id"]
    )
    return role["plan_tier"]


def _refusal_resources():
    resource_rows = plan_tier_rows(1, "dataset-one")
    role = next(
        resource_row["payload_json"]
        for resource_row in resource_rows
        if resource_row["resource_type"] == "PractitionerRole"
    )
    plan = next(
        resource_row["payload_json"]
        for resource_row in resource_rows
        if resource_row["resource_type"] == "InsurancePlan"
    )
    resources = [
        (resource_row["resource_type"], resource_row["resource_id"], resource_row["payload_json"])
        for resource_row in resource_rows
        if resource_row["resource_type"] != "PractitionerRole"
    ]
    missing_tier = tier_document(plan_id="plan-with-missing-network")
    missing_plan = copy.deepcopy(plan)
    missing_plan.update(resource_id=missing_tier["plan_resource_id"], network_refs=["Organization/missing"])
    missing_plan["plan_json"]["plan_key"] = missing_tier["plan_scope"]
    resources.append(("InsurancePlan", missing_plan["resource_id"], missing_plan))
    changes_by_case = {
        "inactive": {"active": False},
        "expired": {"period_end": "2000-12-31"},
        "future": {"period_start": "9999-01-01"},
        "missing-office": {"location_refs": ["Location/missing"]},
        "provider-only": {"location_refs": []},
        "malformed-ref": {"network_refs": ["not-a-reference"]},
        "plan-missing-network": {
            **{key: missing_tier[key] for key in ("plan_scope", "network_tier", "network_key_id")},
            "insurance_plan_refs": ["InsurancePlan/" + missing_tier["plan_resource_id"]],
        },
        "two-offices": {"location_refs": ["Location/office-two", "Location/office-one"]},
        "forged-tier": {"network_tier": "forged"},
        "plan-mismatch": {"insurance_plan_refs": ["InsurancePlan/" + missing_tier["plan_resource_id"]]},
        "provider-conflict": {"npi": "1000000004"},
    }
    expected_by_id = {}
    for ordinal, (case_name, changes) in enumerate(changes_by_case.items()):
        identifier = f"role-{ordinal:02}-{case_name}"
        resources.append(
            ("PractitionerRole", identifier, {**copy.deepcopy(role), **changes, "resource_id": identifier})
        )
        expected_by_id[identifier] = case_name
    return resources, expected_by_id


def _assert_witness_refusal(page, case_name):
    if case_name in {"forged-tier", "plan-mismatch"}:
        with pytest.raises(facts.RegistrySourceContributionError):
            facts.decode_source_witness_page_v2(page)
        return
    documents = facts.decode_source_witness_page_v2(page)
    headers = [entry for entry in documents if entry["kind"] == "resource"]
    edges = [entry for entry in documents if entry["kind"] == "edge"]
    assert len(headers) == 1
    if case_name in {"inactive", "expired", "future"}:
        assert headers[0]["omission_reason"] == case_name and edges == []
    if case_name == "missing-office":
        assert len(edges) == 1 and edges[0]["location_unresolved"] and edges[0]["location_id"] is None
    if case_name == "provider-only":
        assert len(edges) == 1 and edges[0]["provider_only"] and edges[0]["location_id"] is None
    if case_name in {"malformed-ref", "plan-missing-network"}:
        assert any(not edge["network_witness_complete"] for edge in edges)
    if case_name == "provider-conflict":
        assert headers[0]["malformed"] is True


async def _assert_refusal_page(reader, pin, arguments, case_name):
    page = await membership.read_fhir_witness_page_v2(reader, pin, **arguments)
    assert page.source_rows == 1
    _assert_witness_refusal(page, case_name)
    if case_name == "forged-tier":
        with pytest.raises(ValueError):
            await membership.read_fhir_membership_fact_batch(reader, pin, **arguments)
        return page.next_resource
    batch = await membership.read_fhir_membership_fact_batch(reader, pin, **arguments)
    assert batch.source_rows == 1 and batch.next_resource == page.next_resource
    if case_name == "plan-missing-network":
        assert batch.membership_rows == 0 and batch.omitted_rows == 2
        assert any(
            fact["omission_reason"] == "generic_unverified_unapproved_network"
            for fact in json.loads(batch.source_fact_bytes)
        )
        return page.next_resource
    if case_name in {"inactive", "expired", "future"}:
        assert batch.membership_rows == 0 and batch.omitted_source_rows == 1
    elif case_name == "provider-only":
        assert batch.membership_rows == 0 and batch.provider_only_rows == 1 and batch.unresolved_rows == 0
    elif case_name == "two-offices":
        expected_locations = {
            str(registry_location_id(UHC_PROVIDER_FILE_SOURCE_ID, office)) for office in ("office-one", "office-two")
        }
        assert batch.membership_rows == 2 and batch.unresolved_rows == 0
        assert {member_row["location_id"] for member_row in json.loads(batch.input_bytes)} == expected_locations
        replay = await membership.read_fhir_witness_page_v2(reader, pin, **arguments)
        assert replay == page
    else:
        assert batch.unresolved_source_rows == 1 and batch.unresolved_rows > 0, case_name
    return page.next_resource


@pytest.mark.asyncio
async def test_typed_membership_refusals_keep_source_and_office_boundaries(monkeypatch):
    """One genuine captured publication covers period, identity, reference and office failures."""
    resources, expected_by_id = _refusal_resources()
    _published_resources(monkeypatch, resources, unresolved=1)
    async with (
        source_witness_case(monkeypatch, legacy=True) as case,
        _registry_on_source(case.fixture, monkeypatch) as serving,
    ):
        await _registry_read_bootstrap(case, serving)
        capture = await _capture(case, capture_module, capture_arguments(case))
        anchor = case.anchors_by_id[capture.capture_id]
        recipe = _source_recipe(capture, anchor)
        approved = await _approve_tier(serving, recipe, tier_document())
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            await capture_module.verify_source_fhir_witness_capture(
                case.source_export,
                case.reader,
                source_profile=case.source_profile,
                reviewed_origin=case.origin,
                capture=capture,
                expected_anchor=anchor,
            )
            read = await capture_module.pin_source_fhir_witness_read(case.reader, capture)
            arguments_by_name = dict(
                registry_schema=serving[1],
                approved_source=approved,
                binding_coordinates=recipe.binding_coordinates,
                retained_read=read,
                limit=1,
            )
            await _read_guards_refuse(case, recipe, approved, serving[1], read)
            cursor = None
            for identifier, case_name in sorted(expected_by_id.items()):
                cursor = await _assert_refusal_page(
                    case.reader, recipe.source_pin, {**arguments_by_name, "after_resource": cursor}, case_name
                )
                assert cursor == ("PractitionerRole", identifier)
            for reader in (membership.read_fhir_membership_fact_batch, membership.read_fhir_witness_page_v2):
                terminal = await reader(case.reader, recipe.source_pin, **arguments_by_name, after_resource=cursor)
                assert terminal.source_rows == 0 and terminal.next_resource is None
