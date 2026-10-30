# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact catalog SQL/guard wiring; native office and custom-row parity remain separate proof."""

import asyncio
import re
from dataclasses import replace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from api.network_compact_address import compact_address_native_sql, compact_address_read_relations
from process import registry_fhir_catalog_read as fhir
from process import registry_network_catalog_read as catalog
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_read_identity import NetworkReadIdentityError
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.test_registry_fhir_catalog_read import COMPANY, GROUP
from tests.test_registry_fhir_catalog_read import page_boundary as page_boundary
from tests.test_registry_network_catalog_read import (
    batch_boundary as batch_boundary,
)
from tests.test_registry_network_catalog_read import (
    legacy_selector,
    source_coordinates,
)

SURFACES = ("fhir-list", "fhir-detail", "list", "detail", "legacy", "legacy-batch")
_PID = 77


def _compact(manifest):
    artifacts = tuple(
        CompactEdgeArtifact(str(UUID(int=index)), "network_candidate_" + UUID(int=index).hex, 300 + index)
        for index in (2, 3)
    )
    layout = CompactNetworkLayout(
        CompactAddressRelation(manifest.schema_name, "network_slice_ref", 103),
        CompactAddressRelation("synthetic_base", "entity_address_unified", 201),
        artifacts,
        CompactAddressRelation("synthetic_custom", "entity_address_unified", 202),
    )
    pins = tuple(
        CompactAddressRelation(manifest.schema_name, name, oid)
        for name, oid in (
            ("network_membership", 101),
            ("provider_location_binding", 102),
            ("network_source_contribution", 999),
        )
    )
    object.__setattr__(manifest, "_compact_layout", layout)
    object.__setattr__(manifest, "_candidate_relation_pins", pins)
    object.__setattr__(manifest, "_custody_seal_sha256", "c" * 64)
    return manifest


def _boundary(request, surface, *, empty=False):
    if surface.startswith("fhir"):
        connection, original, snapshot = request.getfixturevalue("page_boundary")
        manifest = _compact(
            PinnedNetworkServingManifest(
                9, str(UUID(int=1)), original.schema_name, 1, {}, 4, original.manifest_sha256, 123
            )
        )
        fhir.catalog_pin.return_value = manifest
        if empty:
            connection.fetchrow.return_value = {"total": 0, "rows_json": "[]"}
    else:
        connection, manifest, _ = request.getfixturevalue("batch_boundary")
        _compact(manifest)
        if empty:
            connection.page.update(total=0, rows_json="[]")
    return connection, manifest


async def _read(surface, connection):
    options_by_name = {"excluded_network_ids": (), "control_schema": "sample_control"}
    if surface.startswith("fhir"):
        return await fhir.read_fhir_catalog(
            connection,
            fhir.FHIRCatalogQuery(generation_id=9),
            network_id=7 if surface == "fhir-detail" else None,
            **options_by_name,
        )
    if surface == "list":
        return await catalog.read_registry_network_catalog(
            connection, catalog.RegistryNetworkCatalogQuery(generation_id=9), **options_by_name
        )
    if surface == "detail":
        return await catalog.read_registry_network_catalog_detail(connection, 7, generation_id=9, **options_by_name)
    selector = (
        catalog.RegistryNetworkCatalogLegacyBatchSelector("checksum_network", ("42",))
        if surface == "legacy-batch"
        else legacy_selector()
    )
    return await catalog.resolve_registry_network_catalog_legacy(
        connection, selector, generation_id=9, **options_by_name
    )


def _guarded_page(monkeypatch, connection, manifest, *, failure=None):
    events = []
    original = connection.fetchrow

    class Receipt(dict):
        def __getitem__(self, key):
            assert failure is None and events[-1] == "verify"
            return super().__getitem__(key)

    async def fetchrow(sql, *parameters):
        entry = await original(sql, *parameters)
        if " AS rows_json" in sql:
            assert events == ["pid"]
            events.append("query")
            return Receipt(entry)
        return entry

    async def verify(actual, relations, *, expected_backend_pid):
        assert actual is connection and expected_backend_pid == _PID
        assert events == ["pid", "query"]
        assert relations == manifest.candidate_relation_pins[:2] + compact_address_read_relations(
            manifest.compact_layout
        )
        events.append("verify")
        if failure is not None:
            raise failure

    monkeypatch.setattr(
        connection, "get_server_pid", Mock(side_effect=lambda: events.append("pid") or _PID), raising=False
    )
    monkeypatch.setattr(connection, "fetchrow", AsyncMock(side_effect=fetchrow))
    guard = AsyncMock(side_effect=verify)
    monkeypatch.setattr(catalog, "verify_consumed_network_relations", guard)
    return events, guard


def _directory_shape(sql, manifest):
    sql = " ".join(sql.split())
    assert (
        "(location.provider_system,location.provider_id,location.location_id)= (membership.provider_system,membership.provider_id,membership.location_id)"
        in sql
    )
    assert (
        "(address.location_key,address.entity_type,address.entity_id)= (location.location_key,location.entity_type,location.entity_id)"
        in sql
    )
    assert "WHERE membership.network_id=page.network_id" in sql
    assert "membership.network_id=ANY" not in sql
    assert '"' + manifest.schema_name + '".entity_address_unified' not in sql


@pytest.mark.parametrize("first", [11, 13, 19])
def test_shared_relation_reuses_typed_layout_and_exact_consumed_pins(batch_boundary, first):
    _, manifest, _ = batch_boundary
    _compact(manifest)
    sql, parameters, relations = catalog.catalog_directory_relation(manifest, first_parameter=first)
    expected = compact_address_native_sql(manifest.compact_layout, first_parameter=first, visible_membership_sql="TRUE")
    assert (sql, parameters) == expected
    assert parameters == tuple(artifact.candidate_id for artifact in manifest.compact_layout.artifacts)
    assert {int(number) for number in re.findall(r"\$(\d+)", sql)} == {first, first + 1}
    assert relations == manifest.candidate_relation_pins[:2] + compact_address_read_relations(manifest.compact_layout)
    assert {pin.table_oid for pin in relations} == {101, 102, 103, 201, 202, 302, 303}
    assert "ANY(" not in sql and " OR (TRUE)" in sql
    assert sql.count("edge.location_key=office.location_key") == 2
    assert "custom.location_key=base.location_key" in sql
    assert '"' + manifest.schema_name + '".entity_address_unified' not in sql


def test_fhir_statement_appends_artifacts_after_its_eighteen_existing_binds(page_boundary, batch_boundary):
    _, _, snapshot = page_boundary
    _, manifest, _ = batch_boundary
    _compact(manifest)
    sql, parameters, relations = fhir._catalog_page_statement(snapshot, manifest)
    addresses, expected, pins = catalog.catalog_directory_relation(manifest, first_parameter=19)
    assert parameters == expected and relations == pins
    assert "JOIN " + addresses + " address" in sql
    assert "$19" in sql and "$20" in sql
    _directory_shape(sql, manifest)


@pytest.mark.parametrize("policy_enabled", [False, True])
async def test_legacy_binds_and_filter_positions_survive_compact_projection(request, monkeypatch, policy_enabled):
    connection, manifest = _boundary(request, "list", empty=True)
    _guarded_page(monkeypatch, connection, manifest)
    policy = RegistryOrganizationVisibility((COMPANY,), (GROUP,)) if policy_enabled else None
    coordinates = source_coordinates()
    await catalog.read_registry_network_catalog(
        connection,
        catalog.RegistryNetworkCatalogQuery(
            generation_id=9, source=coordinates, search="Alias", benefit_code="medical"
        ),
        excluded_network_ids=(2, 5),
        organization_policy=policy,
        control_schema="sample_control",
    )
    sql, parameters = connection.calls[-1]
    assert parameters[:10] == (
        4,
        (2, 5),
        False,
        "Alias",
        coordinates.sql_parameters,
        None,
        0,
        25,
        catalog.MAX_RESPONSE_BYTES,
        "medical",
    )
    first = 13 if policy_enabled else 11
    if policy_enabled:
        assert parameters[10:12] == ((COMPANY,), (GROUP,))
        assert "NOT company.record_key=ANY($11::text[])" in sql and "ANY($12::text[])" in sql
    assert parameters[first - 1 :] == tuple(artifact.candidate_id for artifact in manifest.compact_layout.artifacts)
    assert f"${first}" in sql and f"${first + 1}" in sql
    assert sql.index("NOT record_key::integer=ANY") < sql.index("filtered AS") < sql.index("OFFSET $7")
    _directory_shape(sql, manifest)


async def test_fhir_filters_and_exclusions_keep_their_original_positions(request, monkeypatch):
    connection, manifest = _boundary(request, "fhir-list", empty=True)
    _guarded_page(monkeypatch, connection, manifest)
    policy = RegistryOrganizationVisibility((COMPANY,), (GROUP,))
    source = source_coordinates("fhir")
    await fhir.read_fhir_catalog(
        connection,
        fhir.FHIRCatalogQuery(
            company_id=COMPANY, group_id=GROUP, plan_search="Gold", source=source, benefit_code="medical"
        ),
        excluded_network_ids=(8,),
        organization_policy=policy,
        control_schema="sample_control",
    )
    sql, *parameters = connection.fetchrow.await_args.args
    assert parameters[1] == (8,) and parameters[2:4] == [(UUID(COMPANY),), (UUID(GROUP),)]
    assert parameters[4:6] == [UUID(COMPANY), UUID(GROUP)]
    assert parameters[7] == "%gold%" and parameters[10] == "medical" and parameters[11] == source.sql_parameters
    assert tuple(parameters[18:]) == tuple(artifact.candidate_id for artifact in manifest.compact_layout.artifacts)
    assert "edge.company_id=$5" in sql and "edge.plan_search LIKE $8" in sql
    assert "NOT network.network_id=ANY($2::integer[])" in sql and "OFFSET $14 LIMIT $15" in sql
    _directory_shape(sql, manifest)


@pytest.mark.parametrize(
    "damage", ["missing_membership", "missing_binding", "duplicate", "schema", "table", "type", "list", "layout"]
)
def test_compact_relation_refuses_missing_or_untyped_native_inputs(batch_boundary, damage):
    _, manifest, _ = batch_boundary
    _compact(manifest)
    pins = manifest.candidate_relation_pins
    if damage.startswith("missing"):
        removed_name = "network_membership" if damage == "missing_membership" else "provider_location_binding"
        pins = tuple(pin for pin in pins if pin.table_name != removed_name)
    if damage == "duplicate":
        pins += (pins[0],)
    if damage in {"schema", "table"}:
        changed = replace(
            pins[0], **({"schema_name": "other_candidate"} if damage == "schema" else {"table_name": "other_table"})
        )
        pins = (changed, *pins[1:])
    if damage == "type":
        pins += (object(),)
    if damage == "list":
        pins = list(pins)
    if damage == "layout":
        object.__setattr__(manifest, "_compact_layout", object())
    object.__setattr__(manifest, "_candidate_relation_pins", pins)
    with pytest.raises(ValueError):
        catalog.catalog_directory_relation(manifest, first_parameter=11)
    with pytest.raises(ValueError):
        fhir._catalog_page_statement({"schema_name": "sample_metadata"}, manifest)


@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("empty", [False, True])
async def test_public_pages_postverify_before_exposure_even_when_empty(request, monkeypatch, surface, empty):
    connection, manifest = _boundary(request, surface, empty=empty)
    events, guard = _guarded_page(monkeypatch, connection, manifest)
    if empty and surface not in {"list", "fhir-list"}:
        with pytest.raises(catalog.RegistryNetworkCatalogError, match="detail_denied"):
            await _read(surface, connection)
    else:
        result = await _read(surface, connection)
        assert len(result["items"]) == int(not empty)
    assert events == ["pid", "query", "verify"]
    guard.assert_awaited_once()
    connection.get_server_pid.assert_called_once()


@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("cancelled", [False, True])
async def test_identity_refusal_is_wrapped_and_cancellation_is_preserved(request, monkeypatch, surface, cancelled):
    connection, manifest = _boundary(request, surface, empty=True)
    failure = asyncio.CancelledError() if cancelled else NetworkReadIdentityError("synthetic identity mismatch")
    events, guard = _guarded_page(monkeypatch, connection, manifest, failure=failure)
    expected = asyncio.CancelledError if cancelled else catalog.RegistryNetworkCatalogError
    with pytest.raises(expected) as error:
        await _read(surface, connection)
    if cancelled:
        assert error.value is failure
    else:
        assert error.value.__cause__ is failure
    assert events == ["pid", "query", "verify"]
    guard.assert_awaited_once()


def test_flat_and_unattached_statements_keep_empty_extra_binds_and_relations(batch_boundary, page_boundary):
    _, manifest, _ = batch_boundary
    _, _, snapshot = page_boundary
    assert catalog.catalog_directory_relation(manifest, first_parameter=11) == (
        '"candidate_sample".entity_address_unified',
        (),
        (),
    )
    attached, parameters, relations = fhir._catalog_page_statement(snapshot, manifest)
    assert parameters == relations == () and '"candidate_sample".entity_address_unified' in attached
    unattached, parameters, relations = fhir._catalog_page_statement(snapshot, None)
    assert parameters == relations == () and "'directory_available',false" in unattached
    assert "network_membership" not in unattached and "entity_address_unified" not in unattached


@pytest.mark.parametrize("surface", ["fhir-list", "list", "unattached"])
async def test_public_flat_and_unattached_reads_add_no_pid_or_compact_check(request, monkeypatch, surface):
    if surface in {"fhir-list", "unattached"}:
        connection, _, snapshot = request.getfixturevalue("page_boundary")
        if surface == "unattached":
            snapshot.update(network_generation=None, manifest_sha256=None)
            connection.fetchrow.return_value = {"total": 0, "rows_json": "[]"}
    else:
        connection, _, _ = request.getfixturevalue("batch_boundary")
    guard = AsyncMock(side_effect=AssertionError("flat read selected compact guard"))
    monkeypatch.setattr(catalog, "verify_consumed_network_relations", guard)
    monkeypatch.setattr(
        connection, "get_server_pid", Mock(side_effect=AssertionError("flat read selected PID")), raising=False
    )
    if surface == "unattached":
        result = await fhir.read_fhir_catalog(
            connection, fhir.FHIRCatalogQuery(), excluded_network_ids=(), control_schema="sample_control"
        )
        assert result["generation"] is None and result["items"] == ()
    else:
        result = await _read(surface, connection)
        assert result["items"][0]["network_id"] == 7
    guard.assert_not_awaited()
    connection.get_server_pid.assert_not_called()
