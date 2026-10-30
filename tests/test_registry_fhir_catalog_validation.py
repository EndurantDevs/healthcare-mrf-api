# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persisted catalog facts must retain source identity and closed-candidate pins."""

import copy
import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_validation as validation
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_sources import fhir_catalog_source_digest
from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant
from tests.test_registry_fhir_catalog_read import relationship_document
from tests.test_registry_fhir_catalog_sources import descriptor


def _source_row():
    selected = descriptor()
    return {
        "descriptor_json": selected,
        "source_id": selected["coordinates"]["source_id"],
        "source_generation": fhir_catalog_source_digest(selected),
        "profile": selected["profile"],
        "accepted_at": "2026-01-01T02:00:00+02:00",
        "created_at": "2026-01-01T01:00:00+01:00",
    }


def _observation_row(kind):
    resources = (
        ("Organization", "network", {"name": "Example Network", "aliases": ["Example PPO"]}),
        (
            "InsurancePlan",
            "plan-one",
            {"name": "Example Plan", "network_refs": ["Organization/network"]},
        ),
    )
    batch = extract_fhir_catalog_batch(
        tuple(
            {"resource_type": resource, "resource_id": key, "payload_hash": "a" * 64, "payload_json": payload}
            for resource, key, payload in resources
        ),
        profile="retained_normalized",
    )
    fact = batch.organizations[0] if kind == "organization" else batch.edges[0]
    document = json.loads(json.dumps(asdict(fact)))
    return {
        "observation_kind": kind,
        "resource_type": "Organization" if kind == "organization" else fact.owner_resource_type,
        "resource_id": fact.resource_id if kind == "organization" else fact.owner_resource_id,
        "ordinal": 0,
        "document_json": document,
    }


def test_source_rows_normalize_aware_timestamps_without_changing_descriptor_identity():
    row = _source_row()
    original = copy.deepcopy(row["descriptor_json"])
    validation._validate_document("registry_fhir_catalog_source", row)
    assert row["accepted_at"] == row["created_at"] == "2026-01-01T00:00:00+00:00"
    assert row["descriptor_json"] == original
    assert row["source_generation"] == fhir_catalog_source_digest(original)


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "another-source"),
        ("profile", "raw_fhir"),
        ("source_generation", "f" * 64),
        ("accepted_at", "2026-01-01T00:00:00"),
        ("created_at", "2026-01-01T00:00:00"),
    ],
)
def test_serialized_source_row_rejects_identity_drift_and_naive_times(field, value):
    row = _source_row()
    row[field] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_source_invalid"):
        validation._validate_document("registry_fhir_catalog_source", row)


@pytest.mark.parametrize(
    "field,value",
    [
        ("coordinates", {}),
        ("coordinates", {**descriptor()["coordinates"], "source_system": "aca"}),
        ("coordinates", {**descriptor()["coordinates"], "source_id": "registry-manual"}),
        ("profile", "unknown"),
        ("complete", 1),
        ("profile", "retained_normalized"),
        ("resource_table_oid", True),
        ("resource_table_oid", 2**32),
        ("source_uri", None),
    ],
)
def test_persisted_descriptor_cannot_claim_another_source_contract(field, value):
    selected = descriptor()
    selected[field] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_source_invalid"):
        validation._source_descriptor(selected)


@pytest.mark.parametrize("replacement", [None, [], {"coordinates": {}}])
def test_source_row_refuses_missing_or_non_document_descriptor(replacement):
    row = _source_row()
    row["descriptor_json"] = replacement
    with pytest.raises(ValueError, match="registry_fhir_catalog_source_invalid"):
        validation._validate_document("registry_fhir_catalog_source", row)


@pytest.mark.parametrize("kind", ["organization", "edge"])
def test_serialized_extracted_facts_roundtrip_through_final_candidate_validation(kind):
    row = _observation_row(kind)
    original = copy.deepcopy(row)
    validation._validate_document("registry_fhir_catalog_observation", row)
    assert row == original
    if kind == "organization":
        row["document_json"].pop("part_of_ref")
        validation._validate_document("registry_fhir_catalog_observation", row)


@pytest.mark.parametrize(
    "field,value",
    [
        ("declared_network", 1),
        ("status", "deleted"),
        ("resource_id", "network/foreign"),
        ("aliases", ["same", "same"]),
        ("identifiers", ["not-an-identifier"]),
        ("part_of_ref", "Organization/parent\n"),
        ("source_hash", "a" * 63),
        ("extra", "unreviewed"),
    ],
)
def test_final_organization_document_rejects_forged_or_ambiguous_facts(field, value):
    row = _observation_row("organization")
    row["document_json"][field] = value
    with pytest.raises(ValueError):
        validation._validate_document("registry_fhir_catalog_observation", row)


@pytest.mark.parametrize("field,value", [("resource_type", "InsurancePlan"), ("resource_id", "other"), ("ordinal", 1)])
def test_organization_fact_must_match_its_stored_resource_key(field, value):
    row = _observation_row("organization")
    row[field] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_observation_invalid"):
        validation._validate_document("registry_fhir_catalog_observation", row)


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_resource_type", "Location"),
        ("network_ref", "Organization/network\n"),
        ("network_resource_id", "network/foreign"),
        ("status", "active\n"),
        ("plan_identifiers", ["not-an-identifier"]),
        ("geography", "Location/one"),
        ("geography", ["Location/one\n"]),
    ],
)
def test_final_edge_document_rejects_unbounded_or_malformed_relationship_evidence(field, value):
    row = _observation_row("edge")
    row["document_json"][field] = value
    if field == "owner_resource_type":
        row["resource_type"] = value
    with pytest.raises(ValueError):
        validation._validate_document("registry_fhir_catalog_observation", row)


def test_edge_observation_cannot_move_to_another_resource_and_keeps_unknown_geography():
    row = _observation_row("edge")
    row["document_json"]["geography"] = [None, "Location/one"]
    validation._validate_document("registry_fhir_catalog_observation", row)
    row["resource_id"] = "another-plan"
    with pytest.raises(ValueError, match="registry_fhir_catalog_observation_invalid"):
        validation._validate_document("registry_fhir_catalog_observation", row)


@pytest.mark.parametrize(
    "changes",
    [
        {"projector_version": "other"},
        {"network_generation": True},
        {"metadata_approved_revision": True},
        {"as_of": "20260101"},
        {"mapping_sha256": "c" * 63},
    ],
)
def test_snapshot_pins_refuse_substituted_versions_numeric_bools_and_noncanonical_dates(changes):
    selected = snapshot(standing_grant())
    selected.update(changes)
    with pytest.raises(ValueError):
        validation.snapshot_inputs(selected)


def test_serialized_snapshot_pins_keep_same_semantic_generation():
    selected = snapshot(standing_grant())
    serialized_by_field = {**selected, "source_vector_json": "[]", "as_of": "2026-01-01"}
    assert validation.snapshot_inputs(serialized_by_field) == validation.snapshot_inputs(selected)
    assert validation.metadata_generation(serialized_by_field) == selected["metadata_generation"]


@pytest.mark.parametrize("change", ["manual", "status", "duplicate", "order"])
def test_source_vector_keeps_exact_order_and_excludes_manual_or_duplicate_sources(change):
    first_by_field = {"source_generation": "a" * 64, "source_id": "source-a", "status": "current"}
    second_by_field = {"source_generation": "b" * 64, "source_id": "source-b", "status": "last_good"}
    source_entries = [first_by_field, second_by_field]
    if change == "manual":
        first_by_field["source_id"] = "registry-manual"
    elif change == "status":
        first_by_field["status"] = "retired"
    elif change == "duplicate":
        second_by_field["source_generation"] = first_by_field["source_generation"]
    else:
        source_entries.reverse()
    with pytest.raises(ValueError, match="registry_fhir_catalog_inputs_invalid"):
        validation.validate_fhir_catalog_source_vector(source_entries)


def test_source_vector_enforces_serialized_byte_bound_before_iteration(monkeypatch):
    source_entries = [{"source_generation": "a" * 64, "source_id": "source-a", "status": "current"}]
    size = len(validation.canonical_json(source_entries).encode())
    monkeypatch.setattr(validation, "PAGE_BYTES", size)
    validation.validate_fhir_catalog_source_vector(source_entries)
    monkeypatch.setattr(validation, "PAGE_BYTES", size - 1)
    with pytest.raises(ValueError, match="registry_fhir_catalog_inputs_invalid"):
        validation.validate_fhir_catalog_source_vector(source_entries)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_document", [False, True])
async def test_oversized_candidate_row_refuses_without_validation_or_second_page(missing_document):
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=validation.canonical_json(validation._KEYS["registry_fhir_catalog_network"])),
        fetch=AsyncMock(
            return_value=[{"document": None if missing_document else "{}", "row_bytes": validation.PAGE_BYTES + 1}]
        ),
    )
    with pytest.raises(ValueError, match="registry_fhir_catalog_validation_bounds"):
        await validation._table_digest(connection, '"catalog_snapshot"', "registry_fhir_catalog_network")
    connection.fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("record_kind", ["company", "group"])
async def test_approved_organization_map_refuses_complete_oversized_family(record_kind):
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[{"record_kind": record_kind, "document": "{}"}] * (validation.PAGE_ROWS + 1))
    )
    with pytest.raises(ValueError, match="registry_fhir_catalog_validation_bounds"):
        await validation._organization_maps(connection, '"catalog_control"', 6)
    connection.fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_document", [False, True])
async def test_organization_proof_page_refuses_oversized_rows_before_replay(missing_document):
    connection = SimpleNamespace(
        fetch=AsyncMock(
            side_effect=[
                [],
                [{"document": None if missing_document else "{}", "row_bytes": validation.PAGE_BYTES + 1}],
            ]
        )
    )
    with pytest.raises(ValueError, match="registry_fhir_catalog_validation_bounds"):
        await validation._validate_source_organizations(
            connection, '"catalog_snapshot"', '"catalog_control"', {"metadata_approved_revision": 6}, None
        )
    assert connection.fetch.await_count == 2


def _approved_organization_proof():
    relationship = relationship_document()
    return {
        "relationship": relationship,
        "organization": {"resource_id": "insurer-one"},
        "source": {"source_uri": "https://example.test/fhir/"},
        "approved_company": {**relationship["company"], "archived": False},
        "approved_group": {**relationship["group"], "archived": False},
    }


def test_legacy_organization_proof_accepts_exact_approved_same_source_identity():
    validation._validate_approved_organization_proof(_approved_organization_proof())


@pytest.mark.parametrize(
    "path,value",
    [
        (("approved_company",), None),
        (("approved_company", "archived"), True),
        (("organization",), None),
        (("relationship", "organization_reference"), "https://foreign.test/Organization/insurer-one"),
        (("approved_group", "group_kind"), "another-kind"),
    ],
)
def test_legacy_organization_proof_requires_exact_approved_identity_and_same_source(path, value):
    proof = _approved_organization_proof()
    parent_by_field = proof
    for field in path[:-1]:
        parent_by_field = parent_by_field[field]
    parent_by_field[path[-1]] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_organization_"):
        validation._validate_approved_organization_proof(proof)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["generation", "schema"])
async def test_candidate_input_mismatch_refuses_before_any_heap_or_authority_read(change):
    selected = snapshot(standing_grant())
    selected["metadata_generation" if change == "generation" else "schema_name"] = "other"
    connection = SimpleNamespace(fetch=AsyncMock(), fetchval=AsyncMock())
    with pytest.raises(ValueError, match="registry_fhir_catalog_inputs_changed"):
        await validation.validate_fhir_catalog_candidate(
            connection, SimpleNamespace(schema_name="catalog_snapshot"), selected, control_schema="catalog_control"
        )
    connection.fetch.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_pin", ["grant", "approved_records"])
async def test_candidate_rechecks_approval_pins_before_replaying_organization_authority(monkeypatch, changed_pin):
    grant = standing_grant()
    selected = snapshot(grant)
    empty_report_by_field = {"rows": 0, "sha256": "a" * 64, "batches": 0, "bytes": 0}
    monkeypatch.setattr(validation, "_table_digest", AsyncMock(return_value=empty_report_by_field))
    fingerprints = AsyncMock(return_value={6: {"metadata": "f" * 64, "membership": "b" * 64}})
    monkeypatch.setattr(validation, "approved_fhir_catalog_fingerprints", fingerprints)
    organization_replay = AsyncMock(side_effect=AssertionError("changed approval must not reach replay"))
    monkeypatch.setattr(validation, "_validate_source_organizations", organization_replay)
    configured_grant = standing_grant(version=2) if changed_pin == "grant" else grant
    with pytest.raises(ValueError, match="registry_fhir_catalog_inputs_changed"):
        await validation.validate_fhir_catalog_candidate(
            object(),
            SimpleNamespace(schema_name="catalog_snapshot"),
            selected,
            control_schema="catalog_control",
            standing_grant=configured_grant,
        )
    organization_replay.assert_not_awaited()
    assert fingerprints.await_count == int(changed_pin == "approved_records")


@pytest.mark.parametrize("attestation", [None, {}, {"metadata_generation": "a" * 64, "unknown": True}])
def test_retained_validation_refuses_incomplete_or_unknown_attestation_fields(attestation):
    with pytest.raises(ValueError, match="registry_fhir_catalog_custody_invalid"):
        validation.verify_fhir_catalog_validation(snapshot(standing_grant()), attestation)


def test_retained_attestation_refuses_incomplete_table_accounting():
    selected = snapshot(standing_grant())
    selected["accounting_json"] = None
    tables_by_name = {table: {"rows": 0, "sha256": "a" * 64, "batches": 0, "bytes": 0} for table in validation._KEYS}
    attestation_by_field = {
        "metadata_generation": selected["metadata_generation"],
        "inputs": validation.snapshot_inputs(selected),
        "tables": tables_by_name,
        "content_sha256": validation._content_digest(tables_by_name),
    }
    validation.verify_fhir_catalog_validation(selected, attestation_by_field)
    del tables_by_name["registry_fhir_catalog_source"]["bytes"]
    with pytest.raises(ValueError, match="registry_fhir_catalog_custody_invalid"):
        validation.verify_fhir_catalog_validation(selected, attestation_by_field)
