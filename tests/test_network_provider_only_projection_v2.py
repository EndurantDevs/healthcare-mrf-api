# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure v2 projection checks; mocks establish no native custody or authority."""

import asyncio
import copy
import hashlib
import json
from contextlib import suppress
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import network_fhir_membership_source as fhir
from process import network_provider_only_projection as projection
from process import registry_source_contribution_store as facts
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_retained_capture_contract import capture_digest
from process.registry_source_contribution_artifacts import SourceFactArtifactReference
from tests import test_network_provider_only_projection as legacy
from tests import test_verified_office_source_witnesses_v2 as witnesses
from tests.test_network_fhir_retained_capture import pin
from tests.test_registry_generic_accepted_capture import semantic_capture
from tests.test_registry_source_contributions import _descriptor as legacy_descriptor
from tests.test_registry_source_contributions import _recipe


def _descriptor(recipe, *, anchored=False):
    descriptor = witnesses._descriptor(recipe.source_pin)
    identity, capture = facts.verified_office_fact_identity(recipe)
    descriptor.update(
        recipe_sha256=identity,
        capture_sha256=capture,
        source_generation=facts._digest([capture, descriptor["mapping_sha256"]]),
    )
    descriptor.update(
        fact_rows=2,
        membership_rows=0,
        unresolved_rows=0,
        raw_source_rows=1,
        accepted_raw_source_rows=1,
        unresolved_raw_source_rows=0,
        provider_only_rows=1,
    )
    if anchored:
        descriptor.update(fact_candidate_id=str(uuid4()), fact_table_oid=123, fact_seal_sha256="a" * 64)
    return facts.validate_source_witness_descriptor_v2(descriptor)


def _documents(source, identity="role-one"):
    header, edge = witnesses._header(source), witnesses._edge(source)
    header["resource_id"] = edge["resource_id"] = edge["evidence_basis"][4] = identity
    edge["location_id"] = edge["evidence_basis"][-1] = None
    edge["provider_only"] = True
    return header, edge


def _page(source, identity="role-one"):
    return fhir.FHIRSourceWitnessPageV2(
        source, 1, ("PractitionerRole", identity), json.dumps(_documents(source, identity)).encode()
    )


def _select_rows(documents, *, binding=None, identity="a"):
    edge = next(row for row in documents if row["kind"] == "edge")
    binding = binding or {"source_key": edge["source_key"], "source_scope_json": edge["source_scope_json"]}
    with legacy._sqlite() as connection:
        connection.execute("CREATE TABLE facts(recipe_sha256 text,fact_json text)")
        connection.execute("CREATE TABLE selected_bindings(recipe_sha256 text,network_id integer,record_json text)")
        connection.executemany(
            "INSERT INTO facts VALUES(?,?)", [("a", json.dumps(row, sort_keys=True)) for row in documents]
        )
        connection.execute(
            "INSERT INTO selected_bindings VALUES(?,?,?)", (identity, 11, json.dumps(binding, sort_keys=True))
        )
        query = projection._SELECT_SQL_V2.format(facts="facts").replace("::text", "").replace("::jsonb", "")
        return connection.execute(query).fetchall()


@pytest.mark.parametrize("recipe", [_recipe(), legacy._capture_recipe()])
def test_native_provider_only_selector_uses_header_and_preserves_evidence_bytes(recipe):
    header, edge = _documents(recipe.source_pin)
    assert "omission_reason" not in edge
    rows = _select_rows([header, edge, copy.deepcopy(edge)])
    basis = edge["evidence_basis"].copy()
    basis[6] = 11
    assert rows == [("npi", edge["provider_id"], 11, hashlib.sha256(json.dumps(basis).encode()).hexdigest())]


@pytest.mark.parametrize(
    "damage",
    [
        "no_header",
        "header_payload",
        "header_resource",
        "header_omission",
        "malformed",
        "provider_only",
        "provider_unresolved",
        "location_unresolved",
        "network_witness_complete",
        "office",
        "scope",
        "recipe",
    ],
)
def test_v2_selector_excludes_every_nonaccepted_provider_only_edge(damage):
    header, edge = _documents(_recipe().source_pin)
    documents, options = [header, edge], {}
    match damage:
        case "no_header":
            documents = [edge]
        case "header_payload":
            header["payload_sha256"] = "f" * 64
        case "header_resource":
            header["resource_id"] = "another-resource"
        case "header_omission":
            header["omission_reason"] = "inactive"
        case "malformed":
            header["malformed"] = True
        case "provider_only" | "network_witness_complete":
            edge[damage] = False
        case "provider_unresolved" | "location_unresolved":
            edge[damage] = True
        case "office":
            edge["location_id"] = witnesses.OFFICE
        case "scope":
            options["binding"] = {"source_key": edge["source_key"], "source_scope_json": {}}
        case _:
            options["identity"] = "b"
    assert _select_rows(documents, **options) == []


def test_v2_descriptor_gate_preserves_v1_and_rejects_implicit_versions():
    recipe = _recipe()
    assert projection._native_descriptor(legacy_descriptor(recipe)) == legacy_descriptor(recipe)
    descriptor = _descriptor(recipe)
    for version in (True, "2", 2.0, None, 3):
        with pytest.raises(ValueError):
            projection._native_descriptor({**descriptor, "descriptor_version": version})
    changed_by_field = {**descriptor, "capture_sha256": legacy_descriptor(recipe)["capture_sha256"]}
    with pytest.raises(ValueError):
        projection._native_descriptor(changed_by_field)


def test_v2_comparison_copy_never_rewrites_original_anchor(monkeypatch):
    recipe = _recipe()
    descriptor = _descriptor(recipe, anchored=True)
    original = copy.deepcopy(descriptor)
    reference = SourceFactArtifactReference(**{key: descriptor[key] for key in facts.FACT_ANCHOR_FIELDS})
    gate = Mock(return_value=reference)
    monkeypatch.setattr(projection, "source_fact_artifact_reference", gate)
    selected, native = projection._source(recipe, descriptor)
    assert selected == recipe and descriptor == original
    assert all(native[key] is None for key in facts.FACT_ANCHOR_FIELDS)
    assert native["capture_sha256"] == facts.verified_office_fact_identity(recipe)[1]
    gate.assert_called_once_with(descriptor)


@pytest.mark.parametrize("recipe", [_recipe(), legacy._capture_recipe()])
def test_explicit_v2_query_keeps_all_original_binding_coordinates(recipe):
    approved = ApprovedMembershipSource(3, "d" * 64)
    ordinary = projection._source_query(recipe, "facts", "controls")
    native = projection._source_query(recipe, "facts", "controls", descriptor_version=2)
    assert ordinary.endswith(projection._SELECT_SQL.format(facts="facts"))
    assert native.endswith(projection._SELECT_SQL_V2.format(facts="facts"))
    arguments = projection._source_arguments(recipe, approved, descriptor_version=2)
    assert arguments[:7] == (3, *recipe.binding_coordinates.sql_parameters)
    assert arguments[7] == facts.verified_office_fact_identity(recipe)[0]
    assert arguments[8] == (
        recipe.source_pin.retained_capture.capture_sha256
        if recipe.source_pin.retained_capture is not None
        else recipe.source_pin.alias_scope
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("anchored", [False, True])
async def test_v2_builder_validates_mapper_and_exact_artifact_before_projection(monkeypatch, anchored):
    recipe, copy_target = _recipe(), legacy._target()
    descriptor = _descriptor(recipe, anchored=anchored)
    native = projection._native_descriptor(descriptor)
    reference = object() if anchored else None
    monkeypatch.setattr(projection, "source_fact_artifact_reference", Mock(return_value=reference))
    monkeypatch.setattr(projection, "_candidate", AsyncMock())
    monkeypatch.setattr(projection, "require_retained_approved_network_source_bindings", AsyncMock())
    artifact = AsyncMock(return_value={native["recipe_sha256"]: SimpleNamespace(relation='"original".facts')})
    monkeypatch.setattr(projection, "verify_anchored_source_fact_contents", artifact)
    mapper, append = AsyncMock(return_value=native), AsyncMock()
    monkeypatch.setattr(projection, "map_fhir_source_witnesses_v2", mapper)
    monkeypatch.setattr(projection, "map_fhir_facts", AsyncMock(side_effect=AssertionError("v1 mapper selected")))
    monkeypatch.setattr(projection, "_append", append)
    connection = SimpleNamespace(execute=AsyncMock())
    approved = ApprovedMembershipSource(3, "d" * 64)
    await projection.append_provider_only_source(
        connection, copy_target, recipe, descriptor, approved, control_schema="controls"
    )
    assert artifact.await_count == int(anchored)
    expected = '"original".facts' if anchored else f'"{copy_target.schema_name}".network_source_contribution'
    assert mapper.await_args.args[3] == append.await_args.args[2] == expected
    assert mapper.await_args.kwargs == {"insert": False}
    assert append.await_args.kwargs == {"descriptor_version": 2}
    mapper.return_value = {**native, "fact_sha256": "f" * 64}
    append.reset_mock()
    with pytest.raises(projection.ProviderOnlyProjectionError):
        await projection.append_provider_only_source(
            connection, copy_target, recipe, descriptor, approved, control_schema="controls"
        )
    append.assert_not_awaited()

    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert sum(statement.startswith("CREATE TEMP TABLE") for statement in statements) == 2
    assert sum(statement.startswith("DROP TABLE pg_temp.") for statement in statements) == 2


@pytest.mark.asyncio
async def test_v2_census_uses_physical_capture_counts():
    original = pin(semantic_capture())
    physical_capture = semantic_capture("dataset-next", "2026-02-01")
    counts = (("OrganizationAffiliation", 2), ("PractitionerRole", 1))
    physical_origin = replace(
        physical_capture.origin,
        resource_counts=counts,
        descriptor_counts=counts,
        resource_hashes=tuple((kind, "a" * 64) for kind, _amount in counts),
    )
    physical_capture = replace(physical_capture, origin=physical_origin)
    physical_capture = replace(physical_capture, capture_sha256=capture_digest(physical_capture))
    physical = replace(pin(physical_capture), as_of="2026-02-01")
    source_pin = fhir.with_fhir_successor(original, physical)
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=AssertionError("retained census reread")))
    assert await projection._source_witness_census_v2(connection, original) == 0
    assert await projection._source_witness_census_v2(connection, source_pin) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 3, None, True, -1])
async def test_v2_legacy_census_requires_exact_native_count(count):
    source_pin = _recipe().source_pin
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=count))
    operation = projection._source_witness_census_v2(connection, source_pin)
    if type(count) is int and count >= 0:
        assert await operation == count
    else:
        with pytest.raises(projection.ProviderOnlyProjectionError):
            await operation
    query, dataset, kinds = connection.fetchval.await_args.args
    assert f'FROM "{source_pin.read_schema_name}".provider_directory_dataset_resource' in query
    assert "WHERE dataset_id=$1 AND resource_type=ANY($2::text[])" in query
    assert dataset == source_pin.dataset_id and kinds == ["PractitionerRole", "OrganizationAffiliation"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", [None, "cursor", "source", "census", "missing_page", "terminal_cursor", "early_terminal", "excess_page"]
)
async def test_v2_fresh_rebuild_copies_complete_pages_and_refuses_gaps(monkeypatch, damage):
    recipe = _recipe()
    first = _page(recipe.source_pin, "role-z")
    terminal = fhir.FHIRSourceWitnessPageV2(recipe.source_pin, 0, None, b"[]")
    match damage:
        case "cursor":
            terminal = _page(recipe.source_pin, "role-a")
        case "source":
            first = replace(first, source=replace(recipe.source_pin, alias_scope="different"))
        case "census":
            first = replace(first, source_rows=2)
        case "terminal_cursor":
            terminal = replace(terminal, next_resource=("PractitionerRole", "role-z"))
        case "early_terminal":
            first = terminal
    reader = AsyncMock(side_effect=[fhir.FHIRMembershipBatchBoundsError("bounded"), first, terminal])
    custody = AsyncMock(return_value="authenticated-read")
    monkeypatch.setattr(projection, "read_fhir_source_witness_page_v2", reader)
    monkeypatch.setattr(projection, "_require_recipe_custody", custody)
    monkeypatch.setattr(projection, "_read_recipe_page", AsyncMock(side_effect=AssertionError("v1 reader selected")))
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        copy_records_to_table=AsyncMock(return_value="COPY 2"),
        fetchval=AsyncMock(return_value=2 if damage == "missing_page" else 0 if damage == "excess_page" else 1),
    )
    operation = projection._rebuild_source(
        connection,
        recipe,
        "expected_facts",
        ApprovedMembershipSource(3, "d" * 64),
        "controls",
        "office-authority",
        descriptor_version=2,
    )
    if damage is None:
        await operation
        assert [call.kwargs["limit"] for call in reader.await_args_list] == [1000, 500, 500]
        assert reader.await_args.kwargs["after_resource"] == first.next_resource
        copy_records = connection.copy_records_to_table.await_args.kwargs["records"]
        assert [copy_record[:2] for copy_record in copy_records] == [
            (facts.verified_office_fact_identity(recipe)[0], index) for index in (1, 2)
        ]
        assert [json.loads(copy_record[2]) for copy_record in copy_records] == list(
            _documents(recipe.source_pin, "role-z")
        )
    else:
        with pytest.raises(ValueError):
            await operation
        assert connection.copy_records_to_table.await_count == int(
            damage in {"cursor", "missing_page", "terminal_cursor"}
        )
    custody.assert_awaited_once()
    assert reader.await_args.kwargs["retained_read"] == "authenticated-read"


@pytest.mark.asyncio
async def test_v2_rebuild_drains_native_copy_before_propagating_cancellation(monkeypatch):
    recipe = _recipe()
    started, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def copy_rows(*_args, **_kwargs):
        started.set()
        await release.wait()
        completed.set()
        return "COPY 2"

    reader = AsyncMock(return_value=_page(recipe.source_pin))
    monkeypatch.setattr(projection, "_require_recipe_custody", AsyncMock(return_value="authenticated-read"))
    monkeypatch.setattr(projection, "read_fhir_source_witness_page_v2", reader)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, copy_records_to_table=copy_rows, fetchval=AsyncMock(return_value=1)
    )
    operation = asyncio.create_task(
        projection._rebuild_source(
            connection,
            recipe,
            "expected_facts",
            ApprovedMembershipSource(3, "d" * 64),
            "controls",
            None,
            descriptor_version=2,
        )
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and not completed.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert completed.is_set() and reader.await_count == 1
    finally:
        release.set()
        if not operation.done():
            operation.cancel()
            with suppress(asyncio.CancelledError):
                await operation


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", [False, True])
async def test_v2_independent_verification_dispatches_rebuild_and_mapper(monkeypatch, mismatch):
    recipe, copy_target = _recipe(), legacy._target()
    descriptor = _descriptor(recipe)
    monkeypatch.setattr(projection, "source_fact_artifact_reference", Mock(return_value=None))
    monkeypatch.setattr(projection, "_candidate", AsyncMock())
    rebuild, append = AsyncMock(), AsyncMock()
    monkeypatch.setattr(projection, "_rebuild_source", rebuild)
    monkeypatch.setattr(projection, "_append", append)
    mapped = {**descriptor, "fact_sha256": "f" * 64} if mismatch else descriptor
    mapper = AsyncMock(return_value=mapped)
    monkeypatch.setattr(projection, "map_fhir_source_witnesses_v2", mapper)
    monkeypatch.setattr(projection, "map_fhir_facts", AsyncMock(side_effect=AssertionError("v1 mapper selected")))
    monkeypatch.setattr(projection, "_receipt", AsyncMock(return_value={"row_count": 0, "rows_sha256": "f" * 64}))
    connection = SimpleNamespace(
        execute=AsyncMock(),
        fetchrow=AsyncMock(
            return_value={
                "differs": False,
                "actual_count": 0,
                "expected_count": 0,
            }
        ),
    )
    operation = projection.verify_provider_only_projection(
        connection,
        copy_target,
        legacy._entries((recipe, descriptor)),
        ApprovedMembershipSource(3, "d" * 64),
        control_schema="controls",
    )
    if mismatch:
        with pytest.raises(projection.ProviderOnlyProjectionError):
            await operation
        append.assert_not_awaited()
    else:
        assert (await operation)["source_count"] == 1
        assert append.await_args.kwargs == {"descriptor_version": 2}
    assert rebuild.await_args.kwargs == {"descriptor_version": 2}
    assert mapper.await_args.kwargs == {"insert": False}


@pytest.mark.asyncio
async def test_v2_rebuild_propagates_single_resource_bounds_failure(monkeypatch):
    reader = AsyncMock(side_effect=fhir.FHIRMembershipBatchBoundsError("bounded"))
    monkeypatch.setattr(projection, "read_fhir_source_witness_page_v2", reader)
    monkeypatch.setattr(projection, "_require_recipe_custody", AsyncMock(return_value="authenticated-read"))
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, copy_records_to_table=AsyncMock(), fetchval=AsyncMock(return_value=1)
    )
    with pytest.raises(fhir.FHIRMembershipBatchBoundsError):
        await projection._rebuild_source(
            connection,
            _recipe(),
            "expected_facts",
            ApprovedMembershipSource(3, "d" * 64),
            "controls",
            None,
            descriptor_version=2,
        )
    assert reader.await_args.kwargs["limit"] == 1
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("zero", [False, True])
async def test_v2_staged_identity_uses_exact_origin_without_reopening_source(monkeypatch, zero):
    recipe, target = _recipe(), legacy._target()
    descriptor = _descriptor(recipe, anchored=True)
    if zero:
        descriptor.update(dict.fromkeys(facts._WITNESS_COUNTERS_V2, 0))
        descriptor["fact_sha256"] = hashlib.sha256(b"").hexdigest()
    original = copy.deepcopy(descriptor)
    monkeypatch.setattr(projection, "source_fact_artifact_reference", Mock(return_value=object()))
    metadata = AsyncMock(return_value={descriptor["recipe_sha256"]: SimpleNamespace(relation='"original".facts')})
    monkeypatch.setattr(projection, "authenticate_anchored_source_fact_metadata", metadata)
    connection, append = legacy._staged_protocol(monkeypatch)
    receipt = await projection._verify_staged_provider_only_projection(
        connection,
        target,
        (recipe,),
        legacy._staged((descriptor,)),
        ApprovedMembershipSource(3, "d" * 64),
        control_schema="controls",
    )
    assert receipt["source_count"] == 1 and descriptor == original
    assert metadata.await_args.args[1] == [descriptor]
    assert append.await_args.args[2] == '"original".facts'
    assert append.await_args.kwargs == {"descriptor_version": 2}
