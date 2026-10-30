# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure v2 contract checks; these do not qualify native SOURCE custody or office policy."""

import asyncio
import copy
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import network_fhir_membership_source as fhir
from process import registry_source_contribution_store as facts
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_registry_generic_fhir_facts import _legacy_pin, _recipe

OFFICE = str(UUID(int=1))
LEGACY = str(UUID(int=2))


def _header(source, *, malformed=False, omission_reason=None):
    return {
        "fact_version": 2,
        "kind": "resource",
        "source_binding": fhir.fhir_source_witness_binding_v2(source),
        "resource_type": "PractitionerRole",
        "resource_id": "role-one",
        "payload_sha256": "b" * 64,
        "malformed": malformed,
        "omission_reason": omission_reason,
    }


def _edge(source_pin):
    physical = fhir.fhir_physical_source(source_pin)
    scope_by_field = {"organization_id": "network-one"}
    if physical.retained_capture is not None:
        scope_by_field["capture_sha256"] = source_pin.retained_capture.capture_sha256
    else:
        scope_by_field.update(legacy_uuid=LEGACY, alias_scope=physical.alias_scope)
    return {
        **{
            key: field_value
            for key, field_value in _header(source_pin).items()
            if key not in {"malformed", "omission_reason"}
        },
        "kind": "edge",
        "source_key": "network-one",
        "source_scope_json": scope_by_field,
        "provider_system": "npi",
        "provider_id": "1000000491",
        "location_id": OFFICE,
        "provider_only": False,
        "provider_unresolved": False,
        "location_unresolved": False,
        "network_witness_complete": True,
        "evidence_basis": [
            physical.dataset_id,
            physical.source_id,
            physical.release_id,
            "PractitionerRole",
            "role-one",
            "b" * 64,
            None,
            "1000000491",
            OFFICE,
        ],
    }


def _page(source, documents=None):
    return fhir.FHIRSourceWitnessPageV2(
        source, 1, ("PractitionerRole", "role-one"), json.dumps(documents or [_header(source), _edge(source)]).encode()
    )


def _descriptor(source):
    recipe = _recipe(source)
    identity, capture = facts.verified_office_fact_identity(recipe)
    return {
        "descriptor_version": 2,
        "transform_version": fhir.FHIR_SOURCE_WITNESS_TRANSFORM_V2,
        "recipe_sha256": identity,
        "capture_sha256": capture,
        "mapping_sha256": "c" * 64,
        "fact_sha256": "d" * 64,
        "source_generation": facts._digest([capture, "c" * 64]),
        "fact_rows": 3,
        "membership_rows": 1,
        "omitted_rows": 0,
        "unresolved_rows": 1,
        "raw_source_rows": 1,
        "accepted_raw_source_rows": 0,
        "unresolved_raw_source_rows": 1,
        "omitted_raw_source_rows": 0,
        "provider_only_rows": 0,
        **dict.fromkeys(facts.FACT_ANCHOR_FIELDS),
    }


@pytest.mark.parametrize("source", [_legacy_pin(), pin(retained())])
def test_explicit_v2_identity_never_changes_v1_defaults_or_decoder(source):
    recipe = _recipe(source)
    identity, v2 = facts.verified_office_fact_identity(recipe)
    old_identity, old_capture = facts.recipe_contribution_identity(recipe)
    assert identity == old_identity
    assert old_capture == facts._digest([identity, facts.fhir_fact_transform(source)])
    assert old_capture != v2 == facts._digest([identity, fhir.FHIR_SOURCE_WITNESS_TRANSFORM_V2])
    descriptor = facts.validate_source_witness_descriptor_v2(_descriptor(source))
    receipt_by_field = {
        "component": facts.SUMMARY_KEY,
        "revision": 2,
        "table_name": facts.CONTRIBUTION_TABLE,
        "sources": [descriptor],
    }
    with pytest.raises(facts.RegistrySourceContributionError):
        facts.validate_registry_source_contribution_receipt(receipt_by_field)
    with pytest.raises(ValueError):
        facts._validate_fhir_fact(_edge(source), source)


@pytest.mark.parametrize("source", [_legacy_pin(), pin(retained())])
def test_new_extraction_keeps_native_witnesses_without_approved_or_alias_outcomes(source):
    sql = fhir._source_witness_sql_v2(source)
    assert "approved_network_bindings" not in sql
    assert "network_registry_alias" not in sql
    assert "resource_status" not in sql
    assert "network_unresolved" not in sql
    assert "permanent_network_omission" not in sql
    assert "'kind','resource'" in sql and "'kind','edge'" in sql
    assert "FROM providers\n        UNION ALL SELECT" in sql
    assert "FROM resolved\n" in sql
    assert "network.resource_id IS NOT NULL AND network_evidence.resource_id IS NOT NULL" in sql
    assert "source_binding" in sql and "evidence_basis" in sql
    assert "'payload'," not in sql
    if source.retained_capture:
        assert "'capture_sha256',$10::text" in sql
        assert "legacy.network_id" not in sql
    else:
        assert "legacy.network_id IS NOT NULL AND legacy.resource_type='Organization'" in sql


def test_native_binding_is_explicit_and_changes_for_effective_source_identity():
    source = pin(retained())
    binding = fhir.fhir_source_witness_binding_v2(source)
    assert set(binding) == {"logical_generation_id", "original_generation_id", "physical_generation_id"}
    assert set(binding.values()) == {source.generation_id}
    assert fhir.fhir_source_witness_binding_v2(replace(source, as_of="2026-01-02")) != binding


def test_mixed_valid_unresolved_and_missing_network_edges_remain_distinct():
    source = pin(retained())
    good = _edge(source)
    missing_provider = copy.deepcopy(good)
    missing_provider.update(provider_id=None, provider_unresolved=True)
    missing_provider["evidence_basis"][7] = None
    missing_network = copy.deepcopy(good)
    missing_network.update(source_key=None, network_witness_complete=False)
    missing_network["source_scope_json"]["organization_id"] = None
    documents = [_header(source), good, missing_provider, missing_network]
    assert facts.decode_fhir_source_witness_page_v2(_page(source, documents)) == documents
    # These flags are source-native witnesses. Selected approved bindings remain absent.
    assert all("network_id" not in document and "outcome" not in document for document in documents)
    assert "coalesce(bool_or(edge_status.unresolved),false)" in facts._WITNESS_MAP_SQL_V2
    assert "NOT $9::boolean OR approved.network_id IS NOT NULL" in facts._WITNESS_MAP_SQL_V2


def test_malformed_and_temporally_omitted_resources_have_raw_headers():
    source = pin(retained())
    for omission in ("inactive", "expired", "future"):
        documents = [_header(source, omission_reason=omission)]
        assert facts.decode_fhir_source_witness_page_v2(_page(source, documents)) == documents
    document = _header(source, malformed=True)
    document["resource_id"] = "malformed raw id"
    page = replace(
        _page(source),
        next_resource=("PractitionerRole", document["resource_id"]),
        source_fact_bytes=json.dumps([document]).encode(),
    )
    assert facts.decode_fhir_source_witness_page_v2(page) == [document]


@pytest.mark.parametrize(
    "damage",
    [
        "unknown_field",
        "missing_header",
        "duplicate_header",
        "wrong_logical",
        "wrong_original",
        "wrong_physical",
        "payload_hash",
        "omitted_edge",
        "false_provider",
        "false_provider_only",
        "missing_location",
        "scope",
        "network_key",
        "version_bool",
        "header_bool",
        "raw_count",
        "cursor",
        "wrong_provider_basis",
    ],
)
def test_v2_decoder_rejects_unbound_or_lossy_rows_before_copy(damage):
    source_pin = pin(retained())
    documents = [_header(source_pin), _edge(source_pin)]
    page = _page(source_pin)
    if damage == "unknown_field":
        documents[1]["network_id"] = 7
    if damage == "missing_header":
        documents = documents[1:]
    if damage == "duplicate_header":
        documents.append(copy.deepcopy(documents[0]))
    if damage.startswith("wrong_") and damage != "wrong_provider_basis":
        documents[1]["source_binding"][damage.removeprefix("wrong_") + "_generation_id"] = "f" * 64
    if damage == "payload_hash":
        documents[1]["payload_sha256"] = documents[1]["evidence_basis"][5] = "c" * 64
    if damage == "omitted_edge":
        documents[0]["omission_reason"] = "inactive"
    if damage == "false_provider":
        documents[1]["provider_unresolved"] = True
    if damage == "false_provider_only":
        documents[1]["provider_only"] = True
    if damage == "missing_location":
        documents[1]["location_id"] = documents[1]["evidence_basis"][-1] = None
    if damage == "scope":
        documents[1]["source_scope_json"]["capture_sha256"] = "f" * 64
    if damage == "network_key":
        documents[1]["source_key"] = None
        documents[1]["source_scope_json"]["organization_id"] = None
    if damage == "version_bool":
        documents[0]["fact_version"] = True
    if damage == "header_bool":
        documents[0]["malformed"] = 0
    if damage == "raw_count":
        page = replace(page, source_rows=2)
    if damage == "cursor":
        page = replace(page, next_resource=("PractitionerRole", "role-two"))
    if damage == "wrong_provider_basis":
        documents[1]["evidence_basis"][1] = "wrong-source"
    with pytest.raises(facts.RegistrySourceContributionError, match="witness_facts_invalid"):
        facts.decode_fhir_source_witness_page_v2(replace(page, source_fact_bytes=json.dumps(documents).encode()))


def test_new_descriptor_admits_exact_three_way_counts_and_mixed_provider_only():
    descriptor = _descriptor(pin(retained()))
    descriptor["provider_only_rows"] = 1  # resource unresolved, valid separate provider-only edge remains
    assert facts.validate_source_witness_descriptor_v2(descriptor) == descriptor


@pytest.mark.parametrize(
    "damage",
    [
        "old_transform",
        "unknown",
        "missing_unresolved",
        "raw_sum",
        "edge_sum",
        "provider_only",
        "bool",
        "anchor",
        "capture",
        "generation",
    ],
)
def test_new_descriptor_rejects_ambiguous_versions_and_broken_accounting(damage):
    descriptor = _descriptor(pin(retained()))
    if damage == "old_transform":
        descriptor["transform_version"] = facts.GENERIC_TRANSFORM_VERSION
    if damage == "unknown":
        descriptor["office_authority"] = True
    if damage == "missing_unresolved":
        descriptor.pop("unresolved_raw_source_rows")
    if damage == "raw_sum":
        descriptor["accepted_raw_source_rows"] = 1
    if damage == "edge_sum":
        descriptor["unresolved_rows"] = 3
    if damage == "provider_only":
        descriptor["provider_only_rows"] = 2
    if damage == "bool":
        descriptor["descriptor_version"] = True
    if damage == "anchor":
        descriptor["fact_table_oid"] = 42
    if damage == "capture":
        descriptor["capture_sha256"] = "f" * 64
    if damage == "generation":
        descriptor["source_generation"] = "f" * 64
    with pytest.raises(facts.RegistrySourceContributionError, match="witness_descriptor_invalid"):
        facts.validate_source_witness_descriptor_v2(descriptor)


@pytest.mark.asyncio
async def test_copy_uses_existing_heap_and_native_chunks_without_dropping_duplicates():
    source = pin(retained())
    page = _page(source, [_header(source), *[_edge(source) for _ in range(5000)]])
    chunks = []

    class Connection:
        def is_in_transaction(self):
            return True

        async def copy_records_to_table(self, table_name, **kwargs):
            assert table_name == "synthetic_stage" and kwargs["schema_name"] == "pg_temp"
            assert kwargs["columns"] == ("recipe_sha256", "ordinal", "fact_json")
            chunks.append(kwargs["records"])
            return f"COPY {len(kwargs['records'])}"

    assert await facts.copy_fhir_source_witness_page_v2(Connection(), page, "synthetic_stage", "a" * 64, 17) == 5018
    assert [len(chunk) for chunk in chunks] == [5000, 1]
    assert [row[1] for chunk in chunks for row in chunk] == list(range(18, 5019))


@pytest.mark.asyncio
async def test_copy_cancellation_and_native_accounting_failure_remain_terminal():
    class Connection:
        def is_in_transaction(self):
            return True

        async def copy_records_to_table(self, *args, **kwargs):
            if self.cancel:
                raise asyncio.CancelledError
            return "COPY 0"

    connection = Connection()
    connection.cancel = True
    with pytest.raises(asyncio.CancelledError):
        await facts.copy_fhir_source_witness_page_v2(connection, _page(pin(retained())), "stage", "a" * 64, 0)
    connection.cancel = False
    with pytest.raises(facts.RegistrySourceContributionError, match="accounting_invalid"):
        await facts.copy_fhir_source_witness_page_v2(connection, _page(pin(retained())), "stage", "a" * 64, 0)


@pytest.mark.asyncio
async def test_new_reader_rechecks_admission_before_exposing_page(monkeypatch):
    assert fhir.read_fhir_source_witness_page_v2 is fhir.read_fhir_witness_page_v2
    source_pin = pin(retained())
    calls = []

    async def admission(*args):
        calls.append("admission")
        return ()

    class Connection:
        def is_in_transaction(self):
            return True

        async def fetchrow(self, sql, *arguments):
            calls.append("payload")
            assert arguments[0] == source_pin.dataset_id
            assert arguments[8] == facts._canonical(fhir.fhir_source_witness_binding_v2(source_pin))
            assert arguments[9] == source_pin.retained_capture.capture_sha256
            return {"source_facts": "[]", "source_rows": 0, "last_type": None, "last_id": None}

    monkeypatch.setattr(fhir, "_require_source_recipe", admission)
    # This isolates the new extraction protocol, not genuine SOURCE authority.
    recipe = _recipe(source_pin)
    approved = SimpleNamespace(approved_revision=1)
    monkeypatch.setattr(fhir, "_reviewed_descriptor", lambda *args: object())
    page = await fhir.read_fhir_source_witness_page_v2(
        Connection(),
        source_pin,
        registry_schema="synthetic_registry",
        approved_source=approved,
        binding_coordinates=recipe.binding_coordinates,
    )
    assert calls == ["admission", "payload", "admission"]
    assert page.source_fact_bytes == b"[]" and page.next_resource is None
    assert facts.decode_fhir_source_witness_page_v2(page) == []


def test_legacy_sql_body_and_mapping_preserve_reviewed_bytes():
    """Retain the legacy template body outside the four explicit retained-source slots."""
    legacy_template = fhir._BATCH_SQL
    for extension in (
        "{effective_bindings_cte}",
        "        {resolved_limit}\n",
        "{encoded_distinct}",
        "{product_selection_guard}",
    ):
        assert legacy_template.count(extension) == 1
        legacy_template = legacy_template.replace(extension, "", 1)
    assert (
        hashlib.sha256(legacy_template.encode()).hexdigest()
        == "efcf6a1c08465c522e86a8a5e5ddd0fd04afc99ca71587f0da364a8a023c1471"
    )
    assert (
        hashlib.sha256(facts._FACT_MAP_SQL.encode()).hexdigest()
        == "88d920c65d4757715ff7ad1e42664199aad3ef4df9bbd75a9407044f1fdf6651"
    )


def test_v2_cursor_and_transfer_use_one_explicit_c_order():
    sql = fhir._source_witness_sql_v2(pin(retained()))
    assert '(resource_type COLLATE "C",resource_id COLLATE "C")>($2::text COLLATE "C",$3::text COLLATE "C")' in sql
    assert 'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT $4' in sql
    assert sql.count('ORDER BY resource_type COLLATE "C" DESC,resource_id COLLATE "C" DESC LIMIT 1') == 2
    source = pin(retained())
    headers = [_header(source, malformed=True) for _ in range(3)]
    for header, identity in zip(headers, ("z", "A", "é"), strict=True):
        header["resource_id"] = identity
    page = fhir.FHIRSourceWitnessPageV2(source, 3, ("PractitionerRole", "é"), json.dumps(headers).encode())
    assert facts.decode_fhir_source_witness_page_v2(page) == headers


@pytest.mark.asyncio
async def test_two_chunk_copy_requires_transaction_and_drains_current_copy_on_cancellation():
    started, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    class Connection:
        active = False

        def is_in_transaction(self):
            return self.active

        async def copy_records_to_table(self, *args, **kwargs):
            calls.append(len(kwargs["records"]))
            started.set()
            await release.wait()
            completed.set()
            return f"COPY {len(kwargs['records'])}"

    source_pin = pin(retained())
    page = _page(source_pin, [_header(source_pin), *[_edge(source_pin) for _ in range(5000)]])
    connection = Connection()
    with pytest.raises(facts.RegistrySourceContributionError, match="transaction_required"):
        await facts.copy_fhir_source_witness_page_v2(connection, page, "stage", "a" * 64, 0)
    assert not calls
    connection.active = True
    task = asyncio.create_task(facts.copy_fhir_source_witness_page_v2(connection, page, "stage", "a" * 64, 0))
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and not completed.is_set()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert completed.is_set() and calls == [5000]


def test_invalid_source_strings_cannot_expand_the_immutable_transfer():
    sql = fhir._source_witness_sql_v2(pin(retained()))
    assert "'location_reference'," not in sql
    assert "'provider_id',CASE WHEN provider_unresolved THEN NULL ELSE provider_id END" in sql
    assert "CASE WHEN provider_unresolved THEN jsonb_set(evidence_basis," in sql
    assert "THEN '{7}'::text[] ELSE '{8}'::text[] END,'null'::jsonb)" in sql
    source = pin(retained())
    edge = _edge(source)
    edge["provider_id"] = edge["evidence_basis"][7] = None
    edge["provider_unresolved"] = True
    documents = [_header(source), edge]
    assert facts.decode_fhir_source_witness_page_v2(_page(source, documents)) == documents
    edge["provider_id"] = edge["evidence_basis"][7] = "invalid"
    with pytest.raises(facts.RegistrySourceContributionError, match="witness_facts_invalid"):
        facts.decode_fhir_source_witness_page_v2(_page(source, documents))


def test_successor_facts_bind_original_scope_and_physical_evidence_separately():
    from tests.test_registry_generic_accepted_capture import semantic_capture

    original = pin(semantic_capture())
    physical = replace(pin(semantic_capture("dataset-next", "2026-02-01")), as_of="2026-02-01")
    source = fhir.with_fhir_successor(original, physical)
    binding = fhir.fhir_source_witness_binding_v2(source)
    assert binding == {
        "logical_generation_id": source.generation_id,
        "original_generation_id": original.generation_id,
        "physical_generation_id": physical.generation_id,
    }
    assert len(set(binding.values())) == 3
    documents = [_header(source), _edge(source)]
    assert facts.decode_fhir_source_witness_page_v2(_page(source, documents)) == documents
    documents[1]["source_scope_json"]["capture_sha256"] = physical.retained_capture.capture_sha256
    with pytest.raises(facts.RegistrySourceContributionError, match="witness_facts_invalid"):
        facts.decode_fhir_source_witness_page_v2(_page(source, documents))
    documents = [_header(source), _edge(source)]
    documents[1]["evidence_basis"][0] = original.dataset_id
    with pytest.raises(facts.RegistrySourceContributionError, match="witness_facts_invalid"):
        facts.decode_fhir_source_witness_page_v2(_page(source, documents))


@pytest.mark.asyncio
@pytest.mark.parametrize("source_pin", [_legacy_pin(), pin(retained())])
async def test_map_protocol_binds_exact_physical_discriminator_and_new_identity(monkeypatch, source_pin):
    recipe = _recipe(source_pin)
    descriptor = _descriptor(source_pin)
    observed_calls = []

    async def mapping(*args):
        return descriptor["mapping_sha256"]

    async def fingerprint(*args):
        return descriptor["fact_sha256"], descriptor["fact_rows"]

    class Connection:
        def is_in_transaction(self):
            return True

        async def fetchrow(self, sql, *arguments):
            observed_calls.append((sql, arguments))
            return {
                **{field: descriptor[field] for field in facts._WITNESS_COUNTERS_V2},
                "invalid": False,
                "inserted_rows": 0,
            }

    # Protocol doubles only; no claim that these establish native Source or mapping authority.
    monkeypatch.setattr(facts, "source_mapping_sha256", mapping)
    monkeypatch.setattr(facts, "contribution_fact_sha256", fingerprint)
    mapping_result = await facts.map_fhir_source_witnesses_v2(
        Connection(),
        recipe,
        SimpleNamespace(approved_revision=7),
        "pg_temp.synthetic_facts",
        "synthetic_members",
        "synthetic_registry",
        insert=False,
    )
    assert mapping_result == descriptor
    _, arguments = observed_calls[0]
    assert arguments[7] == facts.verified_office_fact_identity(recipe)[0]
    assert arguments[8] is (fhir.fhir_physical_source(source_pin).retained_capture is not None)
    assert json.loads(arguments[9]) == fhir.fhir_source_witness_binding_v2(source_pin)


@pytest.mark.parametrize("damage", ["overlapping_edges", "orphan_edges"])
def test_descriptor_requires_disjoint_edge_counts_and_a_resource_header(damage):
    descriptor = _descriptor(pin(retained()))
    if damage == "overlapping_edges":
        descriptor["omitted_rows"] = 1
    else:
        descriptor["raw_source_rows"] = descriptor["unresolved_raw_source_rows"] = 0
    with pytest.raises(facts.RegistrySourceContributionError, match="witness_descriptor_invalid"):
        facts.validate_source_witness_descriptor_v2(descriptor)


def test_explicit_v2_identity_rejects_other_source_families():
    with pytest.raises(facts.RegistrySourceContributionError, match="witness_descriptor_invalid"):
        facts.verified_office_fact_identity(SimpleNamespace(source_pin=object()))
