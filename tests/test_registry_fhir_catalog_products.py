# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure product orchestration uses closed inputs, never derived output as evidence."""

import hashlib
import json
from dataclasses import asdict, replace
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_fhir_catalog_products as products
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_retained import _descriptor
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from tests.test_registry_fhir_catalog_retained import recipe_fixture


def _observations():
    resources = [("Organization", f"network-{index}", {"name": f"Example network {index}"}) for index in range(5)]
    resources += [
        ("Organization", "insurer", {"name": "Example insurer", "part_of_ref": "Organization/parent"}),
        ("Organization", "parent", {"name": "Example parent"}),
        (
            "InsurancePlan",
            "plan-one",
            {
                "name": "Example plan one",
                "owned_by_ref": "Organization/insurer",
                "network_refs": [f"Organization/network-{index}" for index in range(5)],
                "plan_backbones": [{"network_refs": ["Organization/unrelated"]}],
            },
        ),
        (
            "InsurancePlan",
            "plan-two",
            {
                "name": "Example plan two",
                "owned_by_ref": "Organization/insurer",
                "network_refs": ["Organization/network-1"],
            },
        ),
    ]
    return extract_fhir_catalog_batch(
        tuple(
            {
                "resource_type": kind,
                "resource_id": identity,
                "payload_hash": "a" * 64,
                "payload_json": resource_payload,
            }
            for kind, identity, resource_payload in resources
        ),
        profile="retained_normalized",
    )


def _binding_document(descriptor, index, plan):
    binding_id, network_id = str(UUID(int=index)), index + 6
    binding_by_field = {
        "binding_id": binding_id,
        "revision": 3,
        **descriptor["coordinates"],
        "source_key": "InsurancePlan/" + plan,
        "source_scope_json": {
            "insurance_plan_id": plan,
            "capture_sha256": descriptor["capture_sha256"],
            "product_network_id": network_id,
        },
        "network_id": network_id,
        "archived": False,
        "evidence_id": "reviewed-product-" + str(index),
        "evidence_sha256": "e" * 64,
    }
    network_by_field = {
        "network_id": network_id,
        "display_name": "Example product " + str(index),
        "aliases": [],
        "archived": False,
        "revision": 2,
    }
    return {"record_key": binding_id, "record_revision": 3, "binding": binding_by_field, "network": network_by_field}


class ClosedInputs:
    def __init__(self):
        self.recipe = recipe_fixture()
        self.descriptor = _descriptor(self.recipe, None, 104, None)
        self.generation = fhir_catalog_source_generation(self.descriptor)
        self.sources = [self._source(self.descriptor)]
        self.selectors = [
            _binding_document(self.descriptor, index, plan)
            for index, plan in enumerate(("plan-one", "plan-one", "plan-two"), 1)
        ]
        self.observations = _observations()
        self.calls = []
        self.invalid_closure = False

    def _source(self, descriptor):
        return {
            "generation": fhir_catalog_source_generation(descriptor),
            "source_id": descriptor["coordinates"]["source_id"],
            "profile": descriptor["profile"],
            "descriptor": descriptor,
            "accepted_at": "2026-01-01T00:00:00+00:00",
        }

    @property
    def snapshot(self):
        return {
            "metadata_generation": "c" * 64,
            "metadata_approved_revision": 6,
            "as_of": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            "source_vector_json": [
                {"source_generation": source["generation"], "source_id": source["source_id"], "status": "last_good"}
                for source in self.sources
            ],
        }

    def _summary(self, request):
        selected_edges = [
            edge
            for edge in self.observations.edges
            if edge.plan_resource_id == request["plan_id"] and edge.scope_path.startswith("/normalized_aggregate/")
        ]
        organizations_by_id = {org.resource_id: org for org in self.observations.organizations}
        witnesses = "".join(
            identity + ":" + organizations_by_id[identity].source_hash + "\n"
            for identity in sorted({edge.network_resource_id for edge in selected_edges})
        )
        return {
            "generation": request["source_generation"],
            "plan_id": request["plan_id"],
            "plan_hash": "a" * 64,
            "invalid": self.invalid_closure,
            "organization_sha256": hashlib.sha256(witnesses.encode()).hexdigest(),
        }

    def _edges(self, requests, after):
        keys = {(request["source_generation"], request["plan_id"]) for request in requests}
        documents, ordinals = [], {}
        for edge in self.observations.edges:
            ordinal = ordinals.get(edge.owner_resource_id, 0)
            ordinals[edge.owner_resource_id] = ordinal + 1
            key = self.generation, edge.owner_resource_id
            if key in keys and edge.scope_path.startswith("/normalized_aggregate/") and (*key, ordinal) > after:
                documents.append(
                    {
                        "generation": self.generation,
                        "plan_id": edge.owner_resource_id,
                        "ordinal": ordinal,
                        "edge": asdict(edge),
                    }
                )
        return sorted(
            documents, key=lambda document: (document["generation"], document["plan_id"], document["ordinal"])
        )[: products._PAGE]

    async def fetch(self, sql, *parameters):
        self.calls.append((sql, parameters))
        return [{"document": json.dumps(document)} for document in self._query_documents(sql, parameters)]

    def _query_documents(self, sql, parameters):
        if "'binding',binding.record_json" in sql:
            return [document for document in self.selectors if document["record_key"] > parameters[1]][: products._PAGE]
        if "'descriptor',descriptor_json" in sql:
            return [source for source in self.sources if source["generation"] > parameters[0]][: products._PAGE]
        if "'organization_sha256',d.organization_sha256" in sql:
            return [self._summary(request) for request in json.loads(parameters[0])]
        if "'edge',o.document_json" in sql:
            return self._edges(json.loads(parameters[0]), parameters[1:])
        if "'organization',o.document_json" in sql:
            return [
                {"generation": self.generation, "organization": asdict(org)} for org in self.observations.organizations
            ]
        raise AssertionError("unexpected product input query")


async def _generate(connection):
    return [
        (kind, rows)
        async for kind, rows in products.iter_reviewed_fhir_catalog_product_rows(
            connection,
            observation_schema="closed_example",
            snapshot=connection.snapshot,
            control_schema="registry_example",
            companies=(),
            groups=(),
            crosswalk_for=lambda _source_id: (),
        )
    ]


@pytest.mark.asyncio
async def test_product_generator_pages_shared_complete_plans_and_emits_each_review_once(monkeypatch):
    monkeypatch.setattr(products, "_PAGE", 2)
    connection = ClosedInputs()
    batches = await _generate(connection)
    binding_rows = [row for kind, rows in batches if kind == "binding" for row in rows]
    relationships = [row for kind, rows in batches if kind == "relationship" for row in rows]
    assert len(binding_rows) == 3 and len(relationships) == 22
    assert [len(rows) for kind, rows in batches if kind == "binding"] == [2, 1]
    assert len({row[2] for row in binding_rows}) == 3 and len({row[1] for row in relationships}) == 22
    assert all(len(rows) <= 2 for _kind, rows in batches)
    assert {row[2] for row in relationships} == {7, 8, 9}
    assert all(json.loads(row[-1])["status"] == "last_good" for row in binding_rows)
    assert all("unrelated" not in row[-1] and "/plan/" not in row[-1] for row in relationships)
    assert all(
        "registry_fhir_catalog_binding" not in sql and "network_registry_alias" not in sql
        for sql, _ in connection.calls
    )


@pytest.mark.parametrize("byte_delta,expected_sizes", [(0, [2]), (-1, [1, 1])])
def test_output_batches_enforce_exact_byte_boundary_without_changing_uuid_rows(monkeypatch, byte_delta, expected_sizes):
    rows = tuple(("generation", index, UUID(int=index), "source", '{"name":"Žlutý"}') for index in (1, 2))
    sizes = [
        len(products.fhir_catalog_json_bytes(tuple(str(value) if type(value) is UUID else value for value in row))) + 1
        for row in rows
    ]
    bound = 2 + sum(sizes) + byte_delta
    monkeypatch.setattr(products, "DEFAULT_BYTES", bound)
    batches_by_kind = {"binding": ([], 2)}
    batches = list(products._append_output_rows(batches_by_kind, "binding", rows))
    if batches_by_kind["binding"][0]:
        batches.append(("binding", tuple(batches_by_kind["binding"][0])))
    assert [len(batch) for _kind, batch in batches] == expected_sizes
    assert tuple(row for _kind, batch in batches for row in batch) == rows
    assert all(type(row[2]) is UUID for _kind, batch in batches for row in batch)
    assert all(2 + sum(sizes[rows.index(row)] for row in batch) <= bound for _kind, batch in batches)


def test_output_batches_refuse_a_single_row_over_the_existing_byte_bound(monkeypatch):
    monkeypatch.setattr(products, "DEFAULT_BYTES", 8)
    batches_by_kind = {"binding": ([], 2)}
    with pytest.raises(products.FHIRCatalogProductError):
        list(products._append_output_rows(batches_by_kind, "binding", (("too large",),)))
    assert batches_by_kind == {"binding": ([], 2)}


@pytest.mark.asyncio
async def test_source_selector_is_independent_of_any_output_binding():
    connection = ClosedInputs()
    selected = await products.selected_reviewed_fhir_catalog_product_sources(
        connection, observation_schema="closed_example", snapshot=connection.snapshot, control_schema="registry_example"
    )
    assert selected == {connection.generation: connection.descriptor}
    assert all("registry_fhir_catalog_binding" not in sql for sql, _ in connection.calls)
    connection.selectors = []
    connection.sources = []
    assert (
        await products.selected_reviewed_fhir_catalog_product_sources(
            connection,
            observation_schema="closed_example",
            snapshot=connection.snapshot,
            control_schema="registry_example",
        )
        == {}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["missing", "ambiguous", "generation", "target", "capture", "revision", "source-key"]
)
async def test_approved_product_missing_or_ambiguous_source_refuses_without_emission(damage):
    connection = ClosedInputs()
    match damage:
        case "missing":
            connection.sources = []
        case "ambiguous":
            connection.sources.append(
                connection._source(_descriptor(connection.recipe, None, 104, "https://example.test/fhir"))
            )
            connection.sources.sort(key=lambda source: source["generation"])
        case "generation":
            connection.sources[0]["generation"] = "f" * 64
        case "target":
            connection.selectors[0]["network"]["archived"] = True
        case "capture":
            connection.selectors[0]["binding"]["source_scope_json"]["capture_sha256"] = "f" * 64
        case "revision":
            connection.selectors[0]["record_revision"] = 4
        case _:
            connection.selectors[0]["binding"]["source_key"] = "plan-one"
    with pytest.raises(ValueError):
        await _generate(connection)


@pytest.mark.asyncio
async def test_full_plan_closure_precedes_all_product_emission():
    connection = ClosedInputs()
    connection.invalid_closure = True
    with pytest.raises(products.FHIRCatalogProductError):
        await _generate(connection)
    assert not any("'edge',o.document_json" in sql for sql, _ in connection.calls)
    sql = next(sql for sql, _ in connection.calls if "'organization_sha256',d.organization_sha256" in sql)
    assert "organization.resource_id IS NULL" in sql and "count(DISTINCT" in sql
    assert "o.ordinal::text" in sql and 'w.resource_id COLLATE "C"' in sql and "s.bytes<=33554432" in sql


@pytest.mark.asyncio
async def test_product_metadata_binds_full_org_digest_and_original_review_evidence():
    connection = ClosedInputs()
    first_bindings = [row for kind, rows in await _generate(connection) if kind == "binding" for row in rows]
    connection.selectors[0]["binding"]["evidence_sha256"] = "d" * 64
    second_bindings = [row for kind, rows in await _generate(connection) if kind == "binding" for row in rows]
    assert json.loads(first_bindings[0][-1])["metadata_sha256"] != json.loads(second_bindings[0][-1])["metadata_sha256"]
    assert json.loads(second_bindings[0][-1])["evidence_sha256"] == "d" * 64
    assert first_bindings[1:] == second_bindings[1:]


@pytest.mark.asyncio
async def test_binding_fingerprint_includes_witnesses_beyond_its_first_output_page(monkeypatch):
    monkeypatch.setattr(products, "_PAGE", 2)
    connection = ClosedInputs()
    before_bindings = [row for kind, rows in await _generate(connection) if kind == "binding" for row in rows]
    connection.observations = replace(
        connection.observations,
        organizations=tuple(
            replace(org, source_hash="b" * 64) if org.resource_id == "network-4" else org
            for org in connection.observations.organizations
        ),
    )
    after_bindings = [row for kind, rows in await _generate(connection) if kind == "binding" for row in rows]
    assert all(
        json.loads(before_bindings[index][-1])["metadata_sha256"]
        != json.loads(after_bindings[index][-1])["metadata_sha256"]
        for index in (0, 1)
    )
    assert before_bindings[2] == after_bindings[2]


@pytest.mark.asyncio
async def test_actual_source_rows_cannot_be_hidden_by_omitting_their_vector_entry():
    connection = ClosedInputs()
    snapshot_by_field = {**connection.snapshot, "source_vector_json": []}
    with pytest.raises(products.FHIRCatalogProductError):
        await products.selected_reviewed_fhir_catalog_product_sources(
            connection,
            observation_schema="closed_example",
            snapshot=snapshot_by_field,
            control_schema="registry_example",
        )


@pytest.mark.asyncio
async def test_overlarge_complete_query_page_refuses_before_json_transfer():
    connection = AsyncMock()
    connection.fetch.return_value = [{"document": None}]
    with pytest.raises(products.FHIRCatalogProductError):
        await products._documents(connection, "SELECT '{}'::text AS document")


@pytest.mark.asyncio
async def test_cleanup_deletes_only_closed_product_scope_and_linked_relationships():
    connection = AsyncMock()
    connection.is_in_transaction = lambda: True
    await products.clear_reviewed_fhir_catalog_product_rows(
        connection,
        binding_relation='"candidate_example"."registry_fhir_catalog_binding"',
        relationship_relation='pg_temp."expected_relationship"',
    )
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert len(statements) == 2 and statements[0].startswith('DELETE FROM "pg_temp"."expected_relationship"')
    assert "WHERE binding_id IN (SELECT binding_id" in statements[0]
    assert all(
        "product_network_id" in sql and "'InsurancePlan/'" in sql and "evidence_id" not in sql for sql in statements
    )
    assert all("DELETE FROM" in sql and "registry_fhir_catalog_network" not in sql for sql in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("relation", ["unqualified", "a.b.c", "a.b; DROP TABLE x", '"a".b --', None])
async def test_sink_relation_names_refuse_sql_fragments(relation):
    connection = AsyncMock()
    with pytest.raises(products.FHIRCatalogProductError):
        await products.require_reviewed_product_binding_slots(connection, relation, [UUID(int=1)])
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_slots_refuse_existing_ordinary_binding_and_duplicate_review_ids():
    connection = AsyncMock()
    connection.fetchval.return_value = True
    with pytest.raises(products.FHIRCatalogProductError):
        await products.require_reviewed_product_binding_slots(connection, "candidate_example.binding", [UUID(int=1)])
    with pytest.raises(products.FHIRCatalogProductError):
        await products.require_reviewed_product_binding_slots(
            connection, "candidate_example.binding", [UUID(int=1)] * 2
        )
    connection.fetchval.return_value = False
    await products.require_reviewed_product_binding_slots(connection, "candidate_example.binding", [UUID(int=1)])


@pytest.mark.asyncio
@pytest.mark.parametrize("vector", [None, (), [None] * 1001])
async def test_product_projection_refuses_incomplete_vector_before_source_queries(vector):
    connection = ClosedInputs()
    snapshot_by_field = {**connection.snapshot, "source_vector_json": vector}
    emitted_batches = []
    with pytest.raises(products.FHIRCatalogProductError):
        async for batch in products.iter_reviewed_fhir_catalog_product_rows(
            connection,
            observation_schema="closed_example",
            snapshot=snapshot_by_field,
            control_schema="registry_example",
            companies=(),
            groups=(),
            crosswalk_for=lambda _source_id: (),
        ):
            emitted_batches.append(batch)
    assert emitted_batches == [] and len(connection.calls) == 1
    assert "'binding',binding.record_json" in connection.calls[0][0]


@pytest.mark.asyncio
async def test_retained_source_vector_cannot_claim_current_published_identity():
    connection = ClosedInputs()
    snapshot_by_field = connection.snapshot
    snapshot_by_field["source_vector_json"][0]["status"] = "current"
    with pytest.raises(products.FHIRCatalogProductError):
        await products._sources(connection, '"closed_example"', snapshot_by_field)
    assert len(connection.calls) == 1
    assert "'descriptor',descriptor_json" in connection.calls[0][0]


@pytest.mark.asyncio
async def test_product_cleanup_requires_one_enclosing_transaction():
    connection = AsyncMock()
    connection.is_in_transaction = lambda: False
    with pytest.raises(products.FHIRCatalogProductError):
        await products.clear_reviewed_fhir_catalog_product_rows(
            connection,
            binding_relation="candidate_example.binding",
            relationship_relation="candidate_example.relationship",
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("identifiers", [None, "not-a-batch", {UUID(int=1)}, [UUID(int=1)] * 1001])
async def test_binding_slot_checks_reject_nonbounded_collection_before_query(identifiers):
    connection = AsyncMock()
    with pytest.raises(products.FHIRCatalogProductError):
        await products.require_reviewed_product_binding_slots(connection, "candidate_example.binding", identifiers)
    connection.fetchval.assert_not_awaited()
