# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact catalog SQL keeps named layouts, bounded decoders and native access refusals."""

import asyncio
import re
from dataclasses import replace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import asyncpg
import pytest

from api.network_compact_address import compact_address_native_sql
from process import registry_fhir_catalog_read as fhir
from process import registry_network_catalog_read as catalog
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
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


def _compact(manifest):
    artifacts = tuple(
        CompactEdgeArtifact(str(UUID(int=index)), "network_candidate_" + UUID(int=index).hex) for index in (2, 3)
    )
    layout = CompactNetworkLayout(
        CompactAddressRelation(manifest.schema_name, "network_slice_ref"),
        CompactAddressRelation("synthetic_base", "entity_address_unified"),
        artifacts,
        CompactAddressRelation("synthetic_custom", "entity_address_unified"),
    )
    object.__setattr__(manifest, "_compact_layout", layout)
    object.__setattr__(manifest, "address_relation", manifest.compact_layout.base)
    return manifest


def _boundary(request, surface, *, empty=False):
    if surface.startswith("fhir"):
        connection, original, snapshot = request.getfixturevalue("page_boundary")
        manifest = _compact(
            PinnedNetworkServingManifest(
                9,
                str(UUID(int=1)),
                original.schema_name,
                1,
                {},
                4,
                original.manifest_sha256,
                CompactAddressRelation(original.schema_name, "entity_address_unified"),
            )
        )
        fhir.read_published_api_pin.return_value = (manifest, None, None)
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
            assert failure is None and events[-1] == "query"
            return super().__getitem__(key)

    async def fetchrow(sql, *parameters):
        if " AS rows_json" in sql:
            assert events == []
            assert manifest.schema_name in sql and "network_slice_ref" in sql
            events.append("query")
            if failure is not None:
                raise failure
            return Receipt(await original(sql, *parameters))
        return await original(sql, *parameters)

    query = AsyncMock(side_effect=fetchrow)
    monkeypatch.setattr(connection, "fetchrow", query)
    return events, query


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
    assert any(
        predicate in sql
        for predicate in (
            "WHERE membership.network_id=page.network_id",
            "WHERE network_id=page.network_id OFFSET 0) membership",
        )
    )
    assert "membership.network_id=ANY" not in sql
    assert '"' + manifest.schema_name + '".entity_address_unified' not in sql


@pytest.mark.parametrize("first", [11, 13, 19])
def test_shared_relation_reuses_typed_layout_and_exact_consumed_pins(batch_boundary, first):
    _, manifest, _ = batch_boundary
    _compact(manifest)
    sql, parameters = catalog.catalog_directory_relation(manifest, first_parameter=first)
    expected = compact_address_native_sql(manifest.compact_layout, first_parameter=first, visible_membership_sql="TRUE")
    assert (sql, parameters) == expected
    assert parameters == tuple(artifact.candidate_id for artifact in manifest.compact_layout.artifacts)
    assert {int(number) for number in re.findall(r"\$(\d+)", sql)} == {first, first + 1}
    assert all(
        relation.schema_name in sql for relation in (manifest.compact_layout.base, manifest.compact_layout.custom)
    )
    assert "ANY(" not in sql and " OR (TRUE)" in sql
    assert sql.count("edge.location_key=office.location_key") == 2
    assert "custom.location_key=base.location_key" in sql
    assert '"' + manifest.schema_name + '".entity_address_unified' not in sql


def test_fhir_statement_uses_slice_ref_without_extra_artifact_binds(page_boundary, batch_boundary):
    _, _, snapshot = page_boundary
    _, manifest, _ = batch_boundary
    _compact(manifest)
    sql, parameters = fhir._catalog_page_statement(snapshot, manifest)
    assert parameters == ()
    assert "$20" not in sql
    assert f'FROM ONLY "{manifest.schema_name}"."network_slice_ref" ref' in sql
    assert "ref.network_id=page.network_id AND ref.row_count>0" in sql
    assert "network_membership" not in sql


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
    if policy_enabled:
        assert parameters[10:12] == ((COMPANY,), (GROUP,))
        assert "NOT company.record_key=ANY($11::text[])" in sql and "ANY($12::text[])" in sql
    assert len(parameters) == (12 if policy_enabled else 10)
    assert sql.index("NOT record_key::integer=ANY") < sql.index("filtered AS") < sql.index("OFFSET $7")
    assert f'FROM ONLY "{manifest.schema_name}"."network_slice_ref" ref' in sql
    assert "ref.network_id=page.network_id AND ref.row_count>0" in sql
    assert "network_membership" not in sql


async def test_fhir_filters_and_exclusions_keep_their_original_positions(request, monkeypatch):
    connection, manifest = _boundary(request, "fhir-list", empty=True)
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
    assert parameters[18] == "[]"
    assert len(parameters) == 19
    assert "edge.company_id=$5" in sql and "edge.plan_search LIKE $8" in sql
    assert "NOT network.network_id=ANY($2::integer[])" in sql and "OFFSET $14 LIMIT $15" in sql
    assert f'FROM ONLY "{manifest.schema_name}"."network_slice_ref" ref' in sql
    assert "ref.network_id=page.network_id AND ref.row_count>0" in sql
    assert "network_membership" not in sql


@pytest.mark.parametrize(
    "damage", ["missing_membership", "missing_binding", "duplicate", "schema", "table", "type", "list", "layout"]
)
def test_compact_relation_refuses_missing_or_untyped_native_inputs(batch_boundary, damage):
    """Typed named layout construction rejects missing, unsafe or duplicated relations."""
    _, manifest, _ = batch_boundary
    _compact(manifest)
    layout = manifest.compact_layout
    if damage == "layout":
        object.__setattr__(manifest, "_compact_layout", object())
        with pytest.raises(ValueError):
            catalog.catalog_directory_relation(manifest, first_parameter=11)
        with pytest.raises(ValueError):
            fhir._catalog_page_statement({"schema_name": "sample_metadata"}, manifest)
        return
    with pytest.raises(ValueError):
        match damage:
            case "missing_membership":
                replace(layout, ref_map=None)
            case "missing_binding":
                replace(layout, base=None)
            case "duplicate":
                replace(layout, artifacts=(*layout.artifacts, layout.artifacts[0]))
            case "schema":
                CompactAddressRelation("invalid.schema", "network_slice_ref")
            case "table":
                replace(layout, ref_map=CompactAddressRelation(manifest.schema_name, "other_table"))
            case "type":
                replace(layout, base=object())
            case "list":
                replace(layout, artifacts=list(layout.artifacts))


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
    assert events == ["query"]
    assert sum(" AS rows_json" in call.args[0] for call in guard.await_args_list) == 1


@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("cancelled", [False, True])
async def test_identity_refusal_is_wrapped_and_cancellation_is_preserved(request, monkeypatch, surface, cancelled):
    connection, manifest = _boundary(request, surface, empty=True)
    failure = asyncio.CancelledError() if cancelled else asyncpg.InsufficientPrivilegeError("named relation denied")
    events, guard = _guarded_page(monkeypatch, connection, manifest, failure=failure)
    expected = asyncio.CancelledError if cancelled else catalog.RegistryNetworkCatalogError
    with pytest.raises(expected) as error:
        await _read(surface, connection)
    if cancelled:
        assert error.value is failure
    else:
        assert error.value.__cause__ is failure
    assert events == ["query"]
    assert sum(" AS rows_json" in call.args[0] for call in guard.await_args_list) == 1


def test_flat_and_unattached_statements_keep_empty_extra_binds_and_relations(batch_boundary, page_boundary):
    _, manifest, _ = batch_boundary
    _, _, snapshot = page_boundary
    assert catalog.catalog_directory_relation(manifest, first_parameter=11) == (
        '"candidate_sample".entity_address_unified',
        (),
    )
    attached, parameters = fhir._catalog_page_statement(snapshot, manifest)
    assert parameters == () and '"candidate_sample".entity_address_unified' in attached
    unattached, parameters = fhir._catalog_page_statement(snapshot, None)
    assert parameters == () and "'directory_available',false" in unattached
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
    connection.get_server_pid.assert_not_called()


@pytest.mark.parametrize("surface", ["fhir-list", "fhir-detail"])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_fhir_named_query_preserves_native_refusals_and_cancellation(request, monkeypatch, surface, cancelled):
    connection, _ = _boundary(request, surface, empty=True)
    failure = asyncio.CancelledError() if cancelled else asyncpg.InsufficientPrivilegeError("denied")
    connection.fetchrow.side_effect = failure
    with pytest.raises(asyncio.CancelledError if cancelled else catalog.RegistryNetworkCatalogError) as error:
        await _read(surface, connection)
    assert error.value is failure if cancelled else error.value.__cause__ is failure
