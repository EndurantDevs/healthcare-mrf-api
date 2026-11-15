# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named graph workspaces preserve logical source and complete stream checks."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.network_fhir_source_witness_contract import CONTRACT, source_origin
from process.network_fhir_typed_contract import TypedFHIRGraphError, graph_document, validate_graph_document
from process.network_fhir_typed_source import _consume, bind_typed_graph_stage
from tests.test_network_fhir_source_witness_contract import origin_fields

COUNTS = {"source_rows": 2, "projected_rows": 2, "pair_rows": 0, "reference_rows": 0, "provider_only_roles": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_retained_preparation_state_uses_publisher_read_scope(monkeypatch, published):
    from process import provider_directory_rooted_graph_current_preflight as preflight
    from process import provider_directory_rooted_graph_publication_candidate as candidate
    from process import provider_directory_rooted_graph_publication_store as store
    from process import provider_directory_rooted_graph_source_runtime as source_runtime

    scopes = []

    @asynccontextmanager
    async def transaction(database, **options):
        scopes.append(options)

        async def scalar(statement, *arguments):
            if "hp_snapshot_retention.preparation" in statement:
                assert options == {"readonly": True, "publication": True}
                return published
            assert options == {"readonly": True}
            return "selected-dataset"

        yield SimpleNamespace(connection=SimpleNamespace(fetchval=scalar))

    runtime = SimpleNamespace(transaction=transaction)
    handoff = SimpleNamespace(document={"preparation_id": "11111111-1111-4111-8111-111111111111"})
    prepare = AsyncMock()
    monkeypatch.setattr(source_runtime, "current_rooted_graph_runtime", lambda database: runtime)
    monkeypatch.setattr(candidate, "retained_rooted_handoff", AsyncMock(return_value=handoff))
    monkeypatch.setattr(preflight, "prepare_current_root", prepare)
    monkeypatch.setattr(store, "_table", lambda root: '"catalog"."' + root + '"')
    assert await store.prepare_admitted_rooted_graph_dataset("acquisition", database=object()) is handoff
    assert scopes == [{"readonly": True}, {"readonly": True, "publication": True}]
    assert prepare.await_count == int(not published)


class GraphConnection:
    """Execute projection SQL while refusing additional metadata queries."""

    def is_in_transaction(self):
        return True

    async def execute(self, query, *arguments):
        import json

        assert query.startswith("WITH raw_page AS MATERIALIZED")
        assert 'INSERT INTO "candidate_generation"."source_cells"' in query
        return "INSERT 0 " + str(len(json.loads(arguments[0])))


def test_graph_document_has_logical_source_and_data_accounting():
    document = graph_document(source_origin(origin_fields()), "2026-01-01", COUNTS)
    assert set(document) == {
        "contract",
        "derivation_version",
        "office_policy",
        "canonicalizer_version",
        "membership_as_of",
        "origin",
        "counts",
    }
    assert validate_graph_document(document) == document
    assert document["origin"]["resource_relation"] == {
        "schema": "source_example",
        "name": "provider_directory_dataset_resource",
    }


@pytest.mark.parametrize("changes", [{"pair_rows": True}, {"source_rows": -1}, {"extra": 1}])
def test_graph_counts_reject_invalid_or_extra_fields(changes):
    with pytest.raises(TypedFHIRGraphError):
        graph_document(source_origin(origin_fields()), "2026-01-01", COUNTS | changes)


async def test_workspace_uses_qualified_names_and_validation_progress():
    stage = await bind_typed_graph_stage(
        GraphConnection(),
        schema_name="copied_generation",
        table_names={"cells": "source_cells", "addresses": "source_addresses", "pairs": "source_pairs"},
    )
    assert stage.relation("cells") == '"copied_generation"."source_cells"'
    stage.require_ready(complete=False)
    with pytest.raises(TypedFHIRGraphError, match="source_incomplete"):
        stage.require_ready()
    stage.document = graph_document(source_origin(origin_fields()), "2026-01-01", COUNTS)
    stage.checked = True
    stage.require_ready()
    stage.active = False
    with pytest.raises(TypedFHIRGraphError, match="workspace_closed"):
        stage.require_ready(complete=False)


@pytest.mark.parametrize("omit_last", [False, True])
async def test_complete_source_stream_counts_and_closure(omit_last):
    origin = source_origin(origin_fields())
    close_events = []

    async def pages():
        try:
            rows = tuple(
                {
                    "dataset_id": origin.dataset_id,
                    "resource_type": kind,
                    "resource_id": identity,
                    "payload_hash": "a" * 64,
                    "payload_json": {"resource_id": identity},
                    "acquired_resource_sha256": None,
                }
                for kind, identity in (("Location", "office-one"), ("Organization", "network-one"))
            )
            yield rows[:-1] if omit_last else rows
        finally:
            close_events.append(True)

    @asynccontextmanager
    async def source_export(**selection):
        assert selection["dataset_id"] == origin.dataset_id
        yield {"contract": CONTRACT, "origin": origin_fields(), "resources": pages}

    stage = await bind_typed_graph_stage(
        GraphConnection(),
        schema_name="candidate_generation",
        table_names={"cells": "source_cells", "addresses": "source_addresses", "pairs": "source_pairs"},
    )
    arguments = (source_export, GraphConnection(), stage, SimpleNamespace(schema_name="source_example"), origin)
    if omit_last:
        with pytest.raises(TypedFHIRGraphError, match="source_counts"):
            await _consume(*arguments, checker=False, as_of=None)
    else:
        assert await _consume(*arguments, checker=False, as_of=None) == 2
    assert close_events == [True]


def _legacy_typed_recipe():
    from copy import deepcopy
    from dataclasses import asdict

    from process.registry_fhir_catalog_sources import fhir_catalog_source_digest
    from tests.test_registry_typed_source_recipe import _recipe

    recipe = _recipe()
    recipe_by_field = {
        "source_kind": "fhir_typed",
        "source_pin": asdict(recipe.source_pin),
        "binding_coordinates": asdict(recipe.binding_coordinates),
        "selection_policy": recipe.selection_policy,
    }
    pin = recipe_by_field["source_pin"]
    capture, catalog = pin["capture"], pin["catalog_descriptor"]
    schema = catalog["coordinates"]["dataset_schema"]
    catalog.pop("tables")
    catalog.update(
        header_table_oid=101,
        resource_table_oid=102,
        resource_leaf_oids=[103],
        database_oid=104,
        native_catalog_sha256="a" * 64,
        native_read_profile={
            "schema_name": schema,
            "owner_role": "previous_owner",
            "runtime_roles": ["previous_reader"],
        },
    )
    publication = catalog["admission"]["publication"]
    publication.update(
        original_admission_version=1,
        original_admission_kind="generic",
        original_admission_sha256="b" * 64,
        original_metadata_sha256="c" * 64,
    )
    origin = capture["graph"]["origin"]
    origin.pop("header_relation")
    origin.pop("resource_relation")
    origin.update(
        header_oid=101,
        resource_parent_oid=102,
        resource_leaf_oid=103,
        native_catalog_sha256="a" * 64,
        database_oid=104,
        database_system_identifier="123456789",
    )
    capture["graph"]["database_identity"] = {"retired": "stored-only"}
    capture.pop("tables")
    capture.update(
        descriptor_version=4,
        owner_role="previous_owner",
        runtime_roles=["previous_reader"],
        catalog={"schema_oid": 201, "relation_oids": {"source_cells": 202}},
        catalog_descriptor=deepcopy(catalog),
        capture_sha256="d" * 64,
    )
    return recipe_by_field, fhir_catalog_source_digest(catalog)


def test_legacy_typed_recipe_preserves_stored_catalog_generation_through_named_codec():
    from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
    from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes

    document, generation = _legacy_typed_recipe()
    (recipe,) = decode_registry_source_recipes([document])
    pin = recipe.source_pin
    assert pin.catalog_generation == generation
    assert pin.capture["catalog_generation"] == generation
    assert pin.catalog_generation != fhir_catalog_source_generation(pin.catalog_descriptor)
    assert pin.catalog_descriptor["tables"]["resource"] == {
        "schema": pin.catalog_descriptor["coordinates"]["dataset_schema"],
        "name": "provider_directory_dataset_resource",
    }
    (roundtrip,) = decode_registry_source_recipes(canonical_registry_source_recipes((recipe,)))
    assert roundtrip.source_pin.catalog_generation == generation
    assert roundtrip.source_pin.generation_id == "d" * 64
    assert roundtrip == recipe


@pytest.mark.parametrize("database_oid", [1001, 999999])
async def test_copied_legacy_typed_reference_keeps_catalog_generation_without_database_identity_queries(database_oid):
    import json
    from unittest.mock import AsyncMock

    from process.network_fhir_typed_capture import LEGACY_COMMENT, resolve_typed_graph_capture_reference
    from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
    from process.registry_source_recipe_store import decode_registry_source_recipes

    document, generation = _legacy_typed_recipe()
    (recipe,) = decode_registry_source_recipes([document])
    pin = recipe.source_pin

    async def metadata(query, schema):
        assert "WHERE n.nspname=$1" in query and schema == pin.capture["schema_name"]
        return LEGACY_COMMENT + json.dumps(document["source_pin"]["capture"])

    connection = SimpleNamespace(
        is_in_transaction=lambda: True, database_oid=database_oid, fetchval=AsyncMock(side_effect=metadata)
    )
    resolved = await resolve_typed_graph_capture_reference(
        connection, expected_anchor=pin.anchor, capture_sha256=pin.generation_id, catalog_generation=generation
    )
    copied = PinnedTypedFHIRMembershipSource(resolved, pin.anchor, resolved["catalog_descriptor"])
    assert copied.catalog_generation == generation
    assert copied.generation_id == pin.generation_id
    assert connection.fetchval.await_count == 1
    with pytest.raises(TypedFHIRGraphError, match="reference_catalog"):
        await resolve_typed_graph_capture_reference(
            connection, expected_anchor=pin.anchor, capture_sha256=pin.generation_id, catalog_generation="e" * 64
        )


@pytest.mark.parametrize("approved_generation_matches", [False, True])
async def test_legacy_typed_mapping_uses_original_client_catalog_scope(monkeypatch, approved_generation_matches):
    from unittest.mock import AsyncMock

    from process import network_fhir_typed_mapping as mapping
    from process.registry_source_recipe_store import decode_registry_source_recipes

    document, generation = _legacy_typed_recipe()
    (recipe,) = decode_registry_source_recipes([document])
    stage = SimpleNamespace(document=recipe.source_pin.capture["graph"], require_ready=lambda: None)
    approved = SimpleNamespace(approved_revision=7)
    approval = mapping.TypedGraphApproval(
        approved, recipe.binding_coordinates, recipe.source_pin.catalog_descriptor, recipe.source_pin.catalog_generation
    )
    actual_arguments = []

    async def read_bindings(query, *arguments):
        if "bool_and" in query:
            assert "'source_generation',$8::text" in query
            actual_arguments.append(arguments)
            expected = generation if approved_generation_matches else "e" * 64
            return arguments[-1] == expected
        assert "array_agg(record_json->>'source_key')" in query
        return ["network-one"]

    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=read_bindings))
    verify = AsyncMock()
    monkeypatch.setattr(mapping, "require_approved_network_source_bindings", verify)
    monkeypatch.setattr(mapping, "_mapping_workspace", AsyncMock(return_value=object()))
    offices = AsyncMock()
    monkeypatch.setattr(mapping, "_office_inputs", offices)
    if approved_generation_matches:
        prepared_offices = await mapping._prepare_mapping_offices(connection, stage, approval, None, "control_example")
        assert prepared_offices[2] == generation
        assert prepared_offices[3][-1] == generation
        assert offices.await_args.args[-1] == ["network-one"]
    else:
        with pytest.raises(TypedFHIRGraphError, match="approved_catalog_scope"):
            await mapping._prepare_mapping_offices(connection, stage, approval, None, "control_example")
        offices.assert_not_awaited()
    assert actual_arguments[0] == (7, *recipe.binding_coordinates.sql_parameters, generation)
    verify.assert_awaited_once_with(connection, approved, recipe.binding_coordinates, control_schema="control_example")


def test_new_typed_capture_rejects_a_declared_catalog_generation_override():
    from process.network_fhir_typed_capture import validate_typed_capture
    from tests.test_registry_typed_source_recipe import _recipe

    capture = _recipe().source_pin.capture
    assert capture["descriptor_version"] == 5
    with pytest.raises(TypedFHIRGraphError, match="capture_document"):
        validate_typed_capture({**capture, "catalog_generation": "a" * 64})


@pytest.mark.parametrize("generation", [None, True, "changed"])
def test_projected_legacy_catalog_generation_requires_an_exact_logical_key(generation):
    from process.network_fhir_typed_capture import validate_typed_capture

    document, _ = _legacy_typed_recipe()
    capture = validate_typed_capture(document["source_pin"]["capture"])
    with pytest.raises(TypedFHIRGraphError, match="capture_catalog"):
        validate_typed_capture({**capture, "catalog_generation": generation})


def test_legacy_capture_projection_preserves_source_data_scope_validation():
    from process.network_fhir_typed_capture import validate_typed_capture

    document, _ = _legacy_typed_recipe()
    capture = document["source_pin"]["capture"]
    capture["catalog_descriptor"]["dataset_sha256"] = "f" * 64
    with pytest.raises(TypedFHIRGraphError, match="catalog_origin"):
        validate_typed_capture(capture)
