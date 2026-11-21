# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source preparation keeps factory-selected origins and rolls back incomplete batches."""

import asyncio
import os
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
from asyncpg.pgproto.pgproto import UUID as PostgresUUID

from process import registry_imported_source_preparation as preparation
from process.network_fhir_source_witness_contract import digest, source_origin
from process.network_fhir_typed_capture import PREFIX
from process.network_fhir_typed_contract import graph_document
from process.registry_fhir_catalog_sources import parse_fhir_catalog_source_profile
from process.registry_imported_catalog_bindings import PublishedFHIRCatalogBindingScope
from process.registry_source_recipe_composition import SourceWitnessRuntime
from tests.test_registry_typed_source_recipe import _recipe


async def _capture_document(case, factory, writer, **arguments):
    template, schemas, captured, events = case.template, case.schemas, case.captured, case.events
    state_by_field = case.state
    assert state_by_field["source_open"] is False
    anchor = arguments["capture_options"].expected_anchor
    schemas.append(PREFIX + UUID(anchor.capture_id).hex)
    events.append(("capture", arguments))
    if len(schemas) > 1 and state_by_field["capture_failure"] is not None:
        raise state_by_field["capture_failure"]
    async with factory(source_profile=arguments["source_profile"], origin=arguments["reviewed_origin"]) as export:
        assert export["origin"] is arguments["reviewed_origin"]
    document = deepcopy(template.capture)
    document.update(
        capture_id=anchor.capture_id,
        observed_at=anchor.observed_at,
        schema_name=schemas[-1],
        catalog_descriptor=arguments["catalog_descriptor"],
        tables={
            kind: {"schema": schemas[-1], "name": relation["name"]}
            for kind, relation in template.capture["tables"].items()
        },
    )
    document["graph"] = graph_document(
        arguments["reviewed_origin"],
        arguments["membership_as_of"],
        template.capture["graph"]["counts"],
    )
    document.pop("capture_sha256")
    document["capture_sha256"] = digest(document)
    captured.append(document)
    return document


def _preparation_contexts(state_by_field, schemas, events, origins_by_id):
    @asynccontextmanager
    async def source_export(**selection):
        assert state_by_field["source_open"] is False
        state_by_field["source_open"] = True
        events.append(("source_open", selection))
        try:
            selected = selection.get("origin") or origins_by_id[selection["source_id"]]
            yield {"origin": selected}
        finally:
            events.append(("source_close", None))
            state_by_field["source_open"] = False

    @asynccontextmanager
    async def transaction():
        previous = list(schemas)
        events.append(("savepoint", None))
        try:
            yield
        except BaseException:
            schemas[:] = previous
            events.append(("rollback", None))
            raise
        else:
            events.append(("release", None))

    return source_export, transaction


@pytest.fixture
def prepared_case(monkeypatch):
    template = _recipe().source_pin
    descriptor = deepcopy(template.catalog_descriptor)
    origin = source_origin(template.capture["graph"]["origin"])
    events, schemas, captured = [], [], []
    state_by_field = {"source_open": False, "capture_failure": None}
    descriptors = [descriptor]
    origins_by_id = {origin.source_id: origin}

    source_export, transaction = _preparation_contexts(state_by_field, schemas, events, origins_by_id)
    capture_case = SimpleNamespace(
        template=template, schemas=schemas, captured=captured, events=events, state=state_by_field
    )

    async def capture(factory, writer, **arguments):
        return await _capture_document(capture_case, factory, writer, **arguments)

    scope = PublishedFHIRCatalogBindingScope("d" * 64)
    runtime = SourceWitnessRuntime(
        source_export,
        (parse_fhir_catalog_source_profile(descriptor["native_read_profile"]),),
        "typed_owner",
        ("typed_reader",),
        catalog_bindings=scope,
    )
    inputs = AsyncMock(return_value=(scope.metadata_generation, descriptors))
    monkeypatch.setattr(preparation, "require_imported_catalog_inputs", inputs)
    monkeypatch.setattr(preparation, "capture_typed_source_graph", AsyncMock(side_effect=capture))
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        fetchval=AsyncMock(return_value=datetime(2026, 11, 1, tzinfo=timezone.utc)),
        fetchrow=AsyncMock(return_value=None),
    )
    return SimpleNamespace(
        connection=connection,
        runtime=runtime,
        origin=origin,
        origins=origins_by_id,
        descriptors=descriptors,
        events=events,
        schemas=schemas,
        captured=captured,
        state=state_by_field,
        inputs=inputs,
        request_id=UUID(int=31),
        envelope={"command": {"operation": "refresh_memberships", "expected_serving_generation": 0}},
    )


async def prepare(case):
    return await preparation.prepare_imported_source_recipes(
        case.connection,
        request_id=case.request_id,
        envelope=case.envelope,
        source_runtime=case.runtime,
        control_schema="registry",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("request_id_type", [UUID, PostgresUUID], ids=["stdlib", "asyncpg"])
async def test_genuine_selected_origin_closes_before_persistent_capture(prepared_case, request_id_type):
    case = prepared_case
    case.request_id = request_id_type(str(case.request_id))
    preparation_result = await prepare(case)
    recipes = preparation_result.recipes
    case.inputs.assert_awaited_once_with(case.connection, case.envelope["command"], "registry")
    metadata = case.events[1]
    assert metadata == (
        "source_open",
        {
            "source_profile": case.runtime.source_profiles[0],
            "source_id": case.origin.source_id,
            "endpoint_id": case.origin.endpoint_id,
            "dataset_id": case.origin.dataset_id,
        },
    )
    assert [event[0] for event in case.events] == [
        "savepoint",
        "source_open",
        "source_close",
        "capture",
        "source_open",
        "source_close",
        "release",
    ]
    capture_arguments = case.events[3][1]
    assert capture_arguments["reviewed_origin"] is case.origin
    assert capture_arguments["membership_as_of"] == case.origin.semantic_projection_as_of
    expected = preparation.imported_source_capture_id(case.descriptors[0], case.origin.semantic_projection_as_of)
    assert recipes[0].source_pin.capture["schema_name"] == PREFIX + expected.hex
    assert recipes[0].source_pin.capture["capture_sha256"] == case.captured[0]["capture_sha256"]
    assert recipes[0].source_pin.capture == case.captured[0]
    assert recipes[0].source_pin.capture is not case.captured[0]
    assert preparation_result.created_schemas == (PREFIX + expected.hex,) and preparation_result.reused_schemas == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["schema_name", "source_id", "dataset_id", "endpoint_id", "dataset_sha256", "source_uri", "source_aliases"]
)
async def test_changed_factory_origin_never_reaches_capture(prepared_case, monkeypatch, field):
    case = prepared_case
    changes_by_field = {
        "schema_name": {
            "schema_name": "different_schema",
            "resource_relation": {"schema": "different_schema", "name": case.origin.resource_relation["name"]},
        },
        "source_id": {"source_id": "different", "source_aliases": ("different",)},
        "dataset_id": {"dataset_id": "different-dataset"},
        "endpoint_id": {"endpoint_id": "different-endpoint"},
        "dataset_sha256": {"dataset_sha256": "f" * 64},
        "source_uri": {"source_uri": "https://other.example/fhir/"},
        "source_aliases": {"source_aliases": (case.origin.source_id, "z-another-alias")},
    }
    changed = replace(case.origin, **changes_by_field[field])
    case.origins[case.origin.source_id] = changed
    with pytest.raises(ValueError, match="source_changed"):
        await prepare(case)
    preparation.capture_typed_source_graph.assert_not_awaited()
    assert case.schemas == [] and case.events[-1][0] == "rollback" and not case.state["source_open"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("source refusal"), asyncio.CancelledError()])
async def test_failure_or_cancellation_rolls_back_the_entire_preparation_batch(prepared_case, failure):
    case = prepared_case
    second = deepcopy(case.descriptors[0])
    second["coordinates"]["source_id"] = "alias-c"
    second["source_aliases"] = ["alias-c", "alias-d"]
    case.descriptors.append(second)
    case.origins["alias-c"] = replace(case.origin, source_id="alias-c", source_aliases=("alias-c", "alias-d"))
    case.state["capture_failure"] = failure
    with pytest.raises(type(failure)):
        await prepare(case)
    assert preparation.capture_typed_source_graph.await_count == 2
    assert case.schemas == [] and case.events[-1][0] == "rollback" and not case.state["source_open"]


@pytest.mark.asyncio
async def test_capture_hash_validation_remains_real(prepared_case, monkeypatch):
    case = prepared_case
    monkeypatch.setattr(
        preparation, "capture_typed_source_graph", AsyncMock(return_value=case.runtime.source_profiles[0].document())
    )
    with pytest.raises(ValueError):
        await prepare(case)
    assert case.schemas == [] and case.events[-1][0] == "rollback"


@pytest.mark.asyncio
async def test_mismatched_catalog_scope_fails_before_source_selection(prepared_case):
    case = prepared_case
    case.inputs.return_value = ("e" * 64, case.descriptors)
    with pytest.raises(ValueError, match="catalog_changed"):
        await prepare(case)
    assert all(event[0] != "source_open" for event in case.events)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("source refusal"), asyncio.CancelledError()])
async def test_native_partial_capture_schemas_rollback_before_caller_continues(prepared_case, monkeypatch, failure):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    case = prepared_case
    case.request_id = uuid4()
    second = deepcopy(case.descriptors[0])
    second["coordinates"]["source_id"] = "alias-c"
    second["source_aliases"] = ["alias-c", "alias-d"]
    case.descriptors.append(second)
    case.origins["alias-c"] = replace(case.origin, source_id="alias-c", source_aliases=("alias-c", "alias-d"))
    case.state["capture_failure"] = failure
    created_schemas = []
    original = preparation.capture_typed_source_graph

    async def capture(factory, writer, **arguments):
        anchor = arguments["capture_options"].expected_anchor
        schema = PREFIX + UUID(anchor.capture_id).hex
        created_schemas.append(schema)
        await writer.execute('CREATE SCHEMA "' + schema + '"')
        return await original(factory, writer, **arguments)

    monkeypatch.setattr(preparation, "capture_typed_source_graph", capture)
    case.connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        async with case.connection.transaction():
            with pytest.raises(type(failure)):
                await prepare(case)
            assert len(created_schemas) == 2 and case.connection.is_in_transaction()
            assert not await case.connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=ANY($1::text[]))", created_schemas
            )
    finally:
        for schema in reversed(created_schemas):
            await case.connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')
        await case.connection.close()
