# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine Source captures and exact reviewed mappings through existing v2 fact SQL."""

from dataclasses import replace

import pytest

from process import network_fhir_membership_source as fhir
from process import network_fhir_source_witness_capture as source_capture
from process import registry_source_contribution_store as facts
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_retained_capture_contract import registry_location_id
from process.network_fhir_source_witness_contract import PRODUCER
from process.network_source_binding_store import apply_network_source_binding_batch
from process.registry_source_recipe_composition import (
    RegistrySourceCompositionError,
    require_registry_source_recipe_custody,
)
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.source_witness_capture_postgres_support import capture_arguments, source_witness_case
from tests.test_network_fhir_retained_capture_postgres import _binding_command
from tests.test_network_fhir_source_witness_capture_postgres import _capture
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft
from tests.test_verified_office_source_witnesses_v2_postgres import _registry_on_source

pytestmark = pytest.mark.asyncio


async def _registry_read_bootstrap(case, serving):
    """Use only the existing task-owned control-schema Reader bootstrap; Source authority stays unchanged."""
    connection, control, _ = serving
    reader = case.fixture.component.reader
    await connection.execute(f'GRANT USAGE ON SCHEMA "{control}" TO "{reader}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{control}" TO "{reader}"')


def _source_recipe(capture, anchor):
    origin = capture.origin
    pin = fhir.PinnedFHIRMembershipSource(
        origin.schema_name,
        origin.source_id,
        origin.endpoint_id,
        origin.dataset_id,
        origin.dataset_sha256,
        capture.release_id,
        origin.resource_parent_oid,
        PRODUCER,
        origin.semantic_projection_as_of,
        source_witness_capture=capture,
        source_witness_anchor=anchor,
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", origin.source_id, origin.schema_name, origin.dataset_id, PRODUCER, capture.capture_id
    )
    return RegistrySourceMembershipRecipe(pin, coordinates)


async def _approve_mapping(serving, recipe, capture, *, organization=None, capture_sha=None, coordinates=None):
    """Allocate actual canonical IDs and approve only explicit typed source bindings."""
    connection, control, engine = serving
    actor = _actor()
    network = await _draft(engine, control, _create("network"), actor)
    selected_records = [network]
    if organization is not None:
        command = _binding_command(
            recipe.binding_coordinates if coordinates is None else coordinates,
            network["record_id"],
            organization,
            capture.capture_sha256 if capture_sha is None else capture_sha,
        )
        async with connection.transaction():
            selected_records.extend(
                (await apply_network_source_binding_batch(connection, command, actor, control_schema=control))[
                    "records"
                ]
            )
    approval = await _approve(connection, control, await _command(connection, control, *selected_records), actor)
    async with connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            connection, approved_revision=approval["approved_revision"], control_schema=control
        )
    return approved, network["record_id"]


async def _temporary_facts(reader):
    await facts.create_contribution_stage(reader, "source_witness_m2_facts")
    await facts.create_witness_mapping_workspace(reader, "source_witness_m2_members")
    await reader.execute(
        "CREATE TEMP TABLE source_witness_m2_members("
        "network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)"
    )


async def _extract_facts(case, recipe, approved, control, anchor, *, limit):
    reader, pin = case.reader, recipe.source_pin
    await source_capture.verify_source_fhir_witness_capture(
        case.source_export,
        reader,
        source_profile=case.source_profile,
        reviewed_origin=case.origin,
        capture=pin.source_witness_capture,
        expected_anchor=anchor,
    )
    read = await source_capture.pin_source_fhir_witness_read(reader, pin.source_witness_capture)
    cursor, ordinal, pages, documents = None, 0, [], []
    await reader.execute("DELETE FROM pg_temp.source_witness_m2_facts")
    await reader.execute("DELETE FROM pg_temp.source_witness_m2_members")
    while True:
        page = await fhir.read_fhir_witness_page_v2(
            reader,
            pin,
            registry_schema=control,
            after_resource=cursor,
            limit=limit,
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
            retained_read=read,
        )
        documents.extend(facts.decode_source_witness_page_v2(page))
        ordinal = await facts.copy_source_witness_page_v2(
            reader, page, "source_witness_m2_facts", facts.verified_office_fact_identity(recipe)[0], ordinal
        )
        pages.append(page)
        if page.source_rows == 0:
            assert page.next_resource is None
            break
        assert page.next_resource is not None and (cursor is None or cursor < page.next_resource)
        cursor = page.next_resource
    return read, documents, pages


async def _mapped_facts(reader, recipe, approved, control):
    descriptor = await facts.map_fhir_source_witnesses_v2(
        reader, recipe, approved, 'pg_temp."source_witness_m2_facts"', "source_witness_m2_members", control
    )
    members = [
        dict(row)
        for row in await reader.fetch(
            "SELECT network_id,provider_system,provider_id,location_id::text AS location_id,evidence_id "
            "FROM pg_temp.source_witness_m2_members ORDER BY network_id,provider_id,location_id"
        )
    ]
    return descriptor, members


async def _assert_unmatched_mapping(case, serving, recipe, anchor, approved):
    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        _, documents, _ = await _extract_facts(case, recipe, approved, serving[1], anchor, limit=1)
        descriptor, members = await _mapped_facts(case.reader, recipe, approved, serving[1])
        assert members == [] and descriptor["membership_rows"] == descriptor["provider_only_rows"] == 0
        assert descriptor["omitted_rows"] >= 2
    return documents


async def _negative_mapping_matrix(case, serving, recipe, capture, anchor):
    approved, _ = await _approve_mapping(serving, recipe, capture)
    original = await _assert_unmatched_mapping(case, serving, recipe, anchor, approved)
    wrong_hash = ("a" if capture.capture_sha256[0] != "a" else "b") * 64
    cross_source = replace(recipe.binding_coordinates, source_id="another-source")
    for options in (
        {"organization": "missing-network"},
        {"organization": "network-one", "capture_sha": wrong_hash},
        {"organization": "network-one", "coordinates": cross_source},
    ):
        approved, _ = await _approve_mapping(serving, recipe, capture, **options)
        assert await _assert_unmatched_mapping(case, serving, recipe, anchor, approved) == original
    return original


def _assert_exact_mapped_office(case, descriptor, members, network_id, documents):
    assert descriptor["membership_rows"] == descriptor["provider_only_rows"] == 1
    assert descriptor["unresolved_rows"] == descriptor["omitted_rows"] == 0
    assert [(row["network_id"], row["provider_system"], row["provider_id"], row["location_id"]) for row in members] == [
        (network_id, "npi", "1234567893", str(registry_location_id(case.origin.source_id, "office-one")))
    ]
    assert all(row["location_id"] != str(registry_location_id(case.origin.source_id, "office-two")) for row in members)
    provider_only_rows = [row for row in documents if row["kind"] == "edge" and row["provider_only"]]
    assert [(row["provider_id"], row["location_id"]) for row in provider_only_rows] == [("1000000004", None)]
    assert all(row["provider_id"] != "1000000004" for row in members)


async def _read_guards_refuse(case, recipe, approved, control, read):
    arguments_by_name = {
        "registry_schema": control,
        "approved_source": approved,
        "binding_coordinates": recipe.binding_coordinates,
        "retained_read": read,
    }
    for change in (
        {"retained_read": None},
        {"binding_coordinates": replace(recipe.binding_coordinates, source_id="another-source")},
        {"limit": 0},
        {"limit": fhir.MAX_ROWS + 1},
    ):
        with pytest.raises(fhir.FHIRMembershipSourceError):
            await fhir.read_fhir_witness_page_v2(case.reader, recipe.source_pin, **{**arguments_by_name, **change})
    with pytest.raises(fhir.FHIRMembershipSourceError):
        replace(recipe.source_pin, source_id="another-source")
    with pytest.raises(RegistrySourceCompositionError, match="source_witness_custody_unavailable"):
        await require_registry_source_recipe_custody(case.reader, recipe, approved, control, retained_fhir_reads={})


async def test_source_witness_capture_maps_only_approved_exact_offices_and_provider_only(monkeypatch, record_property):
    async with source_witness_case(monkeypatch) as case:
        async with _registry_on_source(case.fixture, monkeypatch) as serving:
            await _registry_read_bootstrap(case, serving)
            capture = await _capture(case, source_capture, capture_arguments(case))
            anchor = case.anchors_by_id[capture.capture_id]
            recipe = _source_recipe(capture, anchor)
            await _temporary_facts(case.reader)
            unmapped_documents = await _negative_mapping_matrix(case, serving, recipe, capture, anchor)
            approved, network_id = await _approve_mapping(serving, recipe, capture, organization="network-one")
            async with case.reader.transaction(isolation="repeatable_read", readonly=True):
                read, documents, pages = await _extract_facts(case, recipe, approved, serving[1], anchor, limit=1)
                descriptor, members = await _mapped_facts(case.reader, recipe, approved, serving[1])
                assert documents == unmapped_documents
                assert len(pages) == 3 and [page.source_rows for page in pages] == [1, 1, 0]
                _assert_exact_mapped_office(case, descriptor, members, network_id, documents)
                await _read_guards_refuse(case, recipe, approved, serving[1], read)
                _, replay, _ = await _extract_facts(case, recipe, approved, serving[1], anchor, limit=1000)
                replay_descriptor, replay_members = await _mapped_facts(case.reader, recipe, approved, serving[1])
                assert replay == documents and replay_descriptor == descriptor and replay_members == members
            record_property("component", "genuine Source witness capture to approved v2 membership facts")
            record_property("rows", str(len(members)))
            record_property("provider_only_rows", str(descriptor["provider_only_rows"]))
            record_property(
                "authority",
                "corrected M1 constructor/fresh Source proof; genuine Reader and typed approved Organization scope",
            )
            record_property(
                "bootstrap",
                "existing isolated control-schema Reader USAGE/SELECT only; no Source/schema/profile/membership broadening",
            )
            record_property(
                "limits",
                "LOCAL mapping proof only; final custody remains fail-closed, no office projection/publication/refresh/runtime/performance claim",
            )
