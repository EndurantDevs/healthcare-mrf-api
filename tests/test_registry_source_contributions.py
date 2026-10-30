# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed source fact keys and receipts reject malformed or unbound reuse."""

import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import registry_source_contribution_store as contributions
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
)


def _recipe():
    source = PinnedFHIRMembershipSource(
        "synthetic_capture",
        "synthetic-source",
        "synthetic-endpoint",
        "edition-one",
        "a" * 64,
        "release-one",
        42,
        "synthetic-scope",
        "2026-10-01",
    )
    return RegistrySourceMembershipRecipe(
        source,
        RegistryNetworkSourceCoordinates(
            "fhir", source.source_id, source.schema_name, source.dataset_id, "synthetic-producer", "edition-one"
        ),
    )


def _fact(source):
    location_id = str(uuid4())
    return {
        "resource_type": "PractitionerRole",
        "resource_id": "role-one",
        "payload_sha256": "b" * 64,
        "omission_reason": None,
        "source_key": "network-one",
        "source_scope_json": {
            "organization_id": "network-one",
            "legacy_uuid": str(uuid4()),
            "alias_scope": source.alias_scope,
        },
        "provider_system": "npi",
        "provider_id": "1000000491",
        "location_id": location_id,
        "evidence_basis": [
            source.dataset_id,
            source.source_id,
            source.release_id,
            "PractitionerRole",
            "role-one",
            "b" * 64,
            None,
            "1000000491",
            location_id,
        ],
    }


def _descriptor(recipe):
    identity, capture = contributions.recipe_contribution_identity(recipe)
    mapping = "c" * 64
    return {
        "recipe_sha256": identity,
        "capture_sha256": capture,
        "mapping_sha256": mapping,
        "fact_sha256": "e" * 64,
        "source_generation": contributions._digest([capture, mapping]),
        "transform_version": contributions.TRANSFORM_VERSION,
        "fact_rows": 1,
        "membership_rows": 1,
        "omitted_rows": 0,
        "raw_source_rows": 1,
        "accepted_raw_source_rows": 1,
        "omitted_raw_source_rows": 0,
        "provider_only_rows": 0,
    }


def test_capture_identity_binds_exact_accepted_pin_and_effective_time():
    recipe = _recipe()
    assert contributions.recipe_contribution_identity(recipe) == contributions.recipe_contribution_identity(recipe)
    for changed in (
        replace(recipe.source_pin, as_of="2026-10-02"),
        replace(recipe.source_pin, dataset_sha256="d" * 64),
    ):
        assert contributions.recipe_contribution_identity(replace(recipe, source_pin=changed)) != (
            contributions.recipe_contribution_identity(recipe)
        )


@pytest.mark.parametrize(
    "damage", ["extra", "bad_scope", "bad_provider", "zero_location", "evidence", "false_omission"]
)
def test_fact_validation_rejects_unqualified_fields(damage):
    source = _recipe().source_pin
    fact = _fact(source)
    changes_by_case = {
        "extra": ("network_id", 1),
        "bad_provider": ("provider_id", "not-an-npi"),
        "zero_location": ("location_id", "00000000-0000-0000-0000-000000000000"),
        "false_omission": ("omission_reason", "inactive"),
    }
    if damage == "bad_scope":
        fact["source_scope_json"]["organization_id"] = "other-network"
    elif damage == "evidence":
        fact["evidence_basis"][6] = 12
    else:
        field, replacement = changes_by_case[damage]
        fact[field] = replacement
    with pytest.raises(ValueError):
        contributions._validate_fhir_fact(fact, source)


def test_provider_only_fact_preserves_absent_office_and_distinct_evidence():
    source = _recipe().source_pin
    fact = _fact(source)
    fact["location_id"] = fact["evidence_basis"][-1] = None
    contributions._validate_fhir_fact(fact, source)


def test_receipt_requires_matching_recipe_inventory_and_marker():
    recipe = _recipe()
    descriptor = _descriptor(recipe)
    receipt = contributions.registry_source_contribution_receipt(SimpleNamespace(contribution_receipts=(descriptor,)))
    canonical = canonical_registry_source_recipes((recipe,))
    candidate_by_field = {
        "source_recipes_json": canonical,
        "source_generations": {
            RECIPE_DIGEST_KEY: registry_source_recipes_sha256(canonical),
            contributions.SUMMARY_KEY: contributions.registry_source_contribution_sha256(receipt),
        },
        "validation_json": {contributions.SUMMARY_KEY: receipt},
    }
    assert contributions.verify_registry_source_contributions(candidate_by_field) == receipt
    for mutation in ("wrong_mapping_digest", "wrong_recipe", "missing_marker"):
        damaged = copy.deepcopy(candidate_by_field)
        if mutation == "wrong_mapping_digest":
            damaged["validation_json"][contributions.SUMMARY_KEY]["sources"][0]["mapping_sha256"] = "d" * 64
        elif mutation == "wrong_recipe":
            damaged["source_recipes_json"] = "[]"
        else:
            damaged["source_generations"].pop(contributions.SUMMARY_KEY)
        with pytest.raises(contributions.RegistrySourceContributionError):
            contributions.verify_registry_source_contributions(damaged)


@pytest.mark.asyncio
async def test_fact_copy_rejects_unbounded_transfer_before_native_write():
    class Connection:
        async def copy_records_to_table(self, *args, **kwargs):
            pytest.fail("malformed facts reached native COPY")

    source = _recipe().source_pin
    for content in (b" " * 8388609, json.dumps([{"untrusted": True}]).encode()):
        batch = SimpleNamespace(source_fact_bytes=content, source=source)
        with pytest.raises(contributions.RegistrySourceContributionError, match="facts_invalid"):
            await contributions.copy_fhir_fact_page(Connection(), batch, "synthetic_stage", "a" * 64, 0)


@pytest.mark.asyncio
async def test_native_fact_fingerprint_is_ordered_bounded_and_rejects_gaps():
    class Connection:
        def __init__(self, ordinals):
            self.ordinals = ordinals
            self.calls = []

        async def fetch(self, query, identity, cursor):
            assert "LIMIT 1000" in query and "ordinal>$2 ORDER BY ordinal" in query
            self.calls.append(cursor)
            return [{"ordinal": ordinal, "fingerprint": "a" * 64} for ordinal in self.ordinals if ordinal > cursor]

    complete = Connection([1, 2])
    digest, rows = await contributions.contribution_fact_sha256(complete, 'pg_temp."synthetic_facts"', "b" * 64)
    assert rows == 2 and digest == contributions.hashlib.sha256(("a" * 128).encode()).hexdigest()
    assert complete.calls == [0, 2]
    with pytest.raises(contributions.RegistrySourceContributionError, match="ordinal_invalid"):
        await contributions.contribution_fact_sha256(Connection([1, 3]), 'pg_temp."synthetic_facts"', "b" * 64)


def test_receipt_rejects_deep_noncontract_documents_with_closed_error():
    nested_documents = []
    for _ in range(2000):
        nested_documents = [nested_documents]
    receipt_by_field = {
        "component": contributions.SUMMARY_KEY,
        "revision": 1,
        "table_name": contributions.CONTRIBUTION_TABLE,
        "sources": nested_documents,
    }
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_registry_source_contribution_receipt(receipt_by_field)


@pytest.mark.parametrize("revision", [1, 2])
def test_source_receipt_versions_keep_exact_closed_wire(revision):
    from process.registry_source_contribution_store import FACT_ANCHOR_FIELDS

    descriptor = _descriptor(_recipe())
    stage = SimpleNamespace(contribution_receipts=(descriptor,), contribution_revision=revision)
    receipt = contributions.registry_source_contribution_receipt(stage)
    assert receipt["revision"] == revision
    assert (FACT_ANCHOR_FIELDS <= receipt["sources"][0].keys()) is (revision == 2)
    assert not (FACT_ANCHOR_FIELDS & descriptor.keys())


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [True, 0, 3, "2"])
async def test_source_stage_storage_contract_rejects_untrusted_revision(revision):
    from unittest.mock import AsyncMock

    from process.network_approved_membership_source import ApprovedMembershipSource
    from process.registry_source_recipe_composition import RegistrySourceCompositionError, stage_registry_source_recipes

    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock())
    with pytest.raises(RegistrySourceCompositionError, match="scope_invalid"):
        await stage_registry_source_recipes(
            connection,
            (),
            ApprovedMembershipSource(0, "a" * 64),
            request_id=uuid4(),
            control_schema="synthetic_control",
            contribution_revision=revision,
        )
    connection.fetchval.assert_not_awaited()


def test_receipt_json_preserves_canonical_data_and_refuses_duplicate_recipe_identity():
    descriptor = _descriptor(_recipe())
    receipt = contributions.registry_source_contribution_receipt(SimpleNamespace(contribution_receipts=(descriptor,)))
    assert contributions.validate_registry_source_contribution_receipt(json.dumps(receipt)) == receipt
    receipt["sources"] *= 2
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_registry_source_contribution_receipt(receipt)
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_registry_source_contribution_receipt('{"revision":1,"revision":2}')


@pytest.mark.parametrize(
    "anchor",
    [
        {"fact_candidate_id": "invalid", "fact_table_oid": 123, "fact_seal_sha256": "a" * 64},
        {"fact_candidate_id": str(uuid4()), "fact_table_oid": True, "fact_seal_sha256": "a" * 64},
        {"fact_candidate_id": str(uuid4()), "fact_table_oid": 123, "fact_seal_sha256": "A" * 64},
        {"fact_candidate_id": str(uuid4()), "fact_table_oid": None, "fact_seal_sha256": "a" * 64},
    ],
)
def test_anchored_receipt_refuses_partial_or_invalid_physical_identity(anchor):
    descriptor_by_name = {**_descriptor(_recipe()), **anchor}
    receipt_by_name = {
        "component": contributions.SUMMARY_KEY,
        "revision": 2,
        "table_name": contributions.CONTRIBUTION_TABLE,
        "sources": [descriptor_by_name],
    }
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_registry_source_contribution_receipt(receipt_by_name)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["COPY 2", "COPY 1", "INSERT 0 2"])
async def test_bulk_fact_copy_keeps_exact_ordinals_and_requires_native_row_accounting(status):
    source = _recipe().source_pin
    facts = [_fact(source), _fact(source)]
    connection = SimpleNamespace(copy_records_to_table=AsyncMock(return_value=status))
    batch = SimpleNamespace(source_fact_bytes=json.dumps(facts).encode(), source=source)
    arguments = (connection, batch, "synthetic_stage", "a" * 64, 17)
    if status == "COPY 2":
        assert await contributions.copy_fhir_fact_page(*arguments) == 19
    else:
        with pytest.raises(contributions.RegistrySourceContributionError, match="accounting_invalid"):
            await contributions.copy_fhir_fact_page(*arguments)
    call = connection.copy_records_to_table.await_args
    assert call.args == ("synthetic_stage",)
    assert call.kwargs["schema_name"] == "pg_temp"
    assert call.kwargs["columns"] == ("recipe_sha256", "ordinal", "fact_json")
    assert [(identity, ordinal) for identity, ordinal, _ in call.kwargs["records"]] == [("a" * 64, 18), ("a" * 64, 19)]
    assert [json.loads(record[2]) for record in call.kwargs["records"]] == facts


@pytest.mark.asyncio
async def test_empty_fact_page_advances_nothing_and_does_not_issue_copy():
    connection = SimpleNamespace(copy_records_to_table=AsyncMock())
    batch = SimpleNamespace(source_fact_bytes=b"[]", source=_recipe().source_pin)
    assert await contributions.copy_fhir_fact_page(connection, batch, "synthetic_stage", "a" * 64, 17) == 17
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["none", "contents", "insert_count"])
async def test_reused_facts_are_checked_before_copy_and_copy_must_be_complete(monkeypatch, damage):
    fingerprint = "a" * 64
    verify = AsyncMock(return_value=("b" * 64 if damage == "contents" else fingerprint, 2))
    monkeypatch.setattr(contributions, "contribution_fact_sha256", verify)
    connection = SimpleNamespace(
        execute=AsyncMock(return_value="INSERT 0 1" if damage == "insert_count" else "INSERT 0 2")
    )
    arguments = (
        connection,
        '"retained"."source_facts"',
        {"fact_sha256": fingerprint, "fact_rows": 2},
        "staged",
        "c" * 64,
    )
    if damage == "none":
        await contributions.reuse_fhir_facts(*arguments)
    else:
        with pytest.raises(contributions.RegistrySourceContributionError, match="content_invalid|accounting_invalid"):
            await contributions.reuse_fhir_facts(*arguments)
    verify.assert_awaited_once_with(connection, '"retained"."source_facts"', "c" * 64)
    if damage == "contents":
        connection.execute.assert_not_awaited()
    else:
        assert connection.execute.await_args.args[1:] == ("c" * 64,)
        assert "WHERE recipe_sha256=$1" in connection.execute.await_args.args[0]


def test_stage_cannot_replace_a_registered_contribution_receipt():
    stage = SimpleNamespace(contribution_receipts=(_descriptor(_recipe()),))
    receipt = contributions.registry_source_contribution_receipt(stage)
    contributions._require_stage_receipt(stage, receipt, None)
    with pytest.raises(contributions.RegistrySourceContributionError, match="stage_invalid"):
        contributions._require_stage_receipt(stage, {**receipt, "sources": []}, None)


@pytest.mark.asyncio
async def test_untrusted_witness_mode_is_rejected_before_predecessor_reads():
    connection = SimpleNamespace(fetch=AsyncMock())
    with pytest.raises(contributions.RegistrySourceContributionError):
        await contributions.retained_source_contributions(connection, None, "registry", witnesses=1)
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", [("resource_type", "Observation"), ("resource_id", "role/one"), ("payload_sha256", "B" * 64)]
)
async def test_fact_copy_rejects_changed_resource_identity_before_writing(field, value):
    source = _recipe().source_pin
    fact_by_field = {**_fact(source), field: value}
    connection = SimpleNamespace(copy_records_to_table=AsyncMock())
    with pytest.raises(contributions.RegistrySourceContributionError, match="facts_invalid"):
        await contributions.copy_fhir_fact_page(
            connection,
            SimpleNamespace(source=source, source_fact_bytes=json.dumps([fact_by_field]).encode()),
            "facts",
            "a" * 64,
            0,
        )
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["object", "too_many"])
async def test_fact_page_requires_a_bounded_array_before_copy(shape):
    source = _recipe().source_pin
    fact = _fact(source)
    content = json.dumps(fact if shape == "object" else [fact] * 5001).encode()
    assert len(content) < 8388608
    connection = SimpleNamespace(copy_records_to_table=AsyncMock())
    with pytest.raises(contributions.RegistrySourceContributionError, match="facts_invalid"):
        await contributions.copy_fhir_fact_page(
            connection, SimpleNamespace(source=source, source_fact_bytes=content), "facts", "a" * 64, 0
        )
    connection.copy_records_to_table.assert_not_awaited()


def test_paired_receipt_cannot_replace_the_complete_registered_recipe_vector():
    selected = _recipe()
    receipt = contributions.registry_source_contribution_receipt(
        SimpleNamespace(contribution_receipts=(_descriptor(selected),))
    )
    other = replace(selected, source_pin=replace(selected.source_pin, as_of="2026-10-02"))
    canonical = canonical_registry_source_recipes((other,))
    candidate_by_field = {
        "source_recipes_json": canonical,
        "source_generations": {
            RECIPE_DIGEST_KEY: registry_source_recipes_sha256(canonical),
            contributions.SUMMARY_KEY: contributions.registry_source_contribution_sha256(receipt),
        },
        "validation_json": {contributions.SUMMARY_KEY: receipt},
    }
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.verify_registry_source_contributions(candidate_by_field)


def test_legacy_stage_cannot_claim_a_new_inventory_receipt():
    from process.registry_source_inventory_preparation import SourceInventoryPreparation

    selected = _recipe()
    stage = SimpleNamespace(contribution_revision=1, contribution_receipts=(_descriptor(selected),))
    root = SourceInventoryPreparation((selected,)).recipe_root
    with pytest.raises(contributions.RegistrySourceContributionError, match="stage_invalid"):
        contributions.registry_source_contribution_receipt(stage, inventory_root=root)


@pytest.mark.parametrize("kind", ["witness", "mapping"])
def test_descriptor_json_roundtrip_keeps_version_and_refuses_oversized_input(kind):
    from tests.test_registry_source_mapping_v3 import _mapping
    from tests.test_verified_office_source_witnesses_v2 import _descriptor as witness_descriptor

    descriptor = witness_descriptor(_recipe().source_pin) if kind == "witness" else _mapping(_recipe())
    validate = (
        contributions.validate_source_witness_descriptor_v2
        if kind == "witness"
        else contributions.validate_source_mapping_descriptor_v3
    )
    encoded = json.dumps(descriptor)
    assert validate(encoded) == contributions.validate_source_contribution_descriptor(encoded) == descriptor
    oversized = " " * contributions.MAX_RECEIPT_BYTES + encoded
    for decoder in (validate, contributions.validate_source_contribution_descriptor):
        with pytest.raises(contributions.RegistrySourceContributionError):
            decoder(oversized)


@pytest.mark.parametrize("document", ["null", "[]", "1"])
def test_descriptor_dispatch_never_coerces_a_nonobject_json_value(document):
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_contribution_descriptor(document)


@pytest.mark.parametrize("changed", ["capture", "mapping"])
def test_mapping_generation_requires_both_exact_native_digests(changed):
    capture, mapping = ("A" * 64, "b" * 64) if changed == "capture" else ("a" * 64, None)
    with pytest.raises(contributions.RegistrySourceContributionError, match="mapping_descriptor_invalid"):
        contributions.source_mapping_generation_v3(capture, mapping, [7])


@pytest.mark.parametrize("damage", ["source_key", "scope", "legacy_identity", "provider_system", "location", "kind"])
def test_witness_page_refuses_unbound_source_edges_and_unknown_record_kinds(damage):
    from tests.test_verified_office_source_witnesses_v2 import _edge, _header, _page

    source = _recipe().source_pin
    edge = _edge(source)
    if damage == "scope":
        edge["source_scope_json"] = []
    elif damage == "legacy_identity":
        edge["source_scope_json"]["legacy_uuid"] = "00000000-0000-0000-0000-000000000000"
    else:
        field, value = {
            "source_key": ("source_key", "network/one"),
            "provider_system": ("provider_system", "provider_directory"),
            "location": ("location_id", "00000000-0000-0000-0000-000000000000"),
            "kind": ("kind", "summary"),
        }[damage]
        edge[field] = value
    with pytest.raises(contributions.RegistrySourceContributionError, match="witness_facts_invalid"):
        contributions.decode_source_witness_page_v2(_page(source, [_header(source), edge]))


@pytest.mark.parametrize("damage", ["count_type", "count_limit", "bytes_type", "array_shape", "array_limit"])
def test_witness_page_requires_a_bounded_typed_census(damage):
    from tests.test_verified_office_source_witnesses_v2 import _header, _page

    source = _recipe().source_pin
    page = _page(source)
    changes_by_case = {
        "count_type": {"source_rows": True},
        "count_limit": {"source_rows": 5001},
        "bytes_type": {"source_fact_bytes": page.source_fact_bytes.decode()},
        "array_shape": {"source_fact_bytes": json.dumps(_header(source)).encode()},
        "array_limit": {"source_fact_bytes": json.dumps([_header(source)] * 10001).encode()},
    }
    with pytest.raises(contributions.RegistrySourceContributionError, match="witness_facts_invalid"):
        contributions.decode_source_witness_page_v2(replace(page, **changes_by_case[damage]))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity,ordinal", [("not-a-digest", 0), ("a" * 64, True), ("a" * 64, -1), ("a" * 64, 2**63 - 2)]
)
async def test_witness_copy_refuses_invalid_identity_or_ordinal_overflow_before_write(identity, ordinal):
    from tests.test_verified_office_source_witnesses_v2 import _page

    connection = SimpleNamespace(is_in_transaction=lambda: True, copy_records_to_table=AsyncMock())
    with pytest.raises(contributions.RegistrySourceContributionError, match="witness_facts_invalid"):
        await contributions.copy_source_witness_page_v2(
            connection, _page(_recipe().source_pin), "facts", identity, ordinal
        )
    connection.copy_records_to_table.assert_not_awaited()
