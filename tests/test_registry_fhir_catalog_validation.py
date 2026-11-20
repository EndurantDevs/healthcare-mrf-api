# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persisted catalog facts must retain source identity and closed-candidate pins."""

import copy
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_validation as validation
from process.registry_fhir_catalog_sources import fhir_catalog_source_digest
from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant
from tests.test_registry_fhir_catalog_source_companies import source_company
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


def test_source_rows_normalize_aware_timestamps_without_changing_descriptor_identity():
    row = _source_row()
    original = copy.deepcopy(row["descriptor_json"])
    validation._source_document(row)
    assert row["accepted_at"] == row["created_at"] == "2026-01-01T00:00:00+00:00"
    assert row["descriptor_json"] == original
    assert row["source_generation"] == fhir_catalog_source_digest(original)


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "another-source"),
        ("profile", "raw_fhir"),
        ("accepted_at", "2026-01-01T00:00:00"),
        ("created_at", "2026-01-01T00:00:00"),
    ],
)
def test_serialized_source_row_rejects_identity_drift_and_naive_times(field, value):
    row = _source_row()
    row[field] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_source_invalid"):
        validation._source_document(row)


@pytest.mark.parametrize(
    "field,value",
    [
        ("coordinates", {}),
        ("coordinates", {**descriptor()["coordinates"], "source_system": "aca"}),
        ("coordinates", {**descriptor()["coordinates"], "source_id": "registry-manual"}),
        ("profile", "unknown"),
        ("complete", 1),
        ("profile", "retained_normalized"),
        ("tables", {}),
        ("tables", {"header": {}, "resource": {}}),
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
        validation._source_document(row)


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


@pytest.mark.parametrize("with_transform", [False, True])
def test_source_vector_accepts_registered_company_metadata_with_existing_transform_contract(with_transform):
    entry_by_field = {
        "source_generation": "a" * 64,
        "source_id": validation.UHC_PROVIDER_FILE_SOURCE_ID,
        "status": "current",
        "source_companies": [source_company()],
    }
    if with_transform:
        entry_by_field["extraction_transform"] = validation.TRANSFORM_VERSION
    validation.validate_fhir_catalog_source_vector([entry_by_field])
    entry_by_field["source_companies"] = []
    validation.validate_fhir_catalog_source_vector([entry_by_field])


@pytest.mark.parametrize("companies", [None, (), [source_company(company_id="unknown")]])
def test_source_vector_rejects_malformed_company_metadata(companies):
    with pytest.raises(ValueError):
        validation.validate_fhir_catalog_source_vector(
            [
                {
                    "source_generation": "a" * 64,
                    "source_id": "source-a",
                    "status": "current",
                    "source_companies": companies,
                }
            ]
        )


def test_registered_label_changes_metadata_generation_without_changing_source_generation():
    selected_descriptor = descriptor()
    original_descriptor = copy.deepcopy(selected_descriptor)
    selected = snapshot(standing_grant(), (selected_descriptor,))
    original_metadata_generation = selected["metadata_generation"]
    original_source_generation = selected["source_vector_json"][0]["source_generation"]
    assert validation.metadata_generation(selected) == original_metadata_generation
    selected["source_vector_json"][0]["source_companies"] = [source_company()]
    labeled_generation = validation.metadata_generation(selected)
    assert labeled_generation != original_metadata_generation
    selected["source_vector_json"][0]["source_companies"][0]["display_name"] = "Example Mutual"
    assert validation.metadata_generation(selected) != labeled_generation
    assert selected["source_vector_json"][0]["source_generation"] == original_source_generation
    assert selected_descriptor == original_descriptor
    assert fhir_catalog_source_digest(selected_descriptor) == original_source_generation
    del selected["source_vector_json"][0]["source_companies"]
    assert validation.metadata_generation(selected) == original_metadata_generation


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


@pytest.mark.parametrize("attestation", [None, {}, {"metadata_generation": "a" * 64, "unknown": True}])
def test_retained_validation_refuses_incomplete_or_unknown_content_fields(attestation):
    with pytest.raises(ValueError, match="registry_fhir_catalog_validation_invalid"):
        validation.verify_fhir_catalog_validation(snapshot(standing_grant()), attestation)


def test_retained_validation_refuses_incomplete_table_accounting():
    selected = snapshot(standing_grant())
    selected["accounting_json"] = None
    tables_by_name = {table: {"rows": 0, "sha256": "a" * 64, "batches": 0, "bytes": 0} for table in validation._TABLES}
    attestation_by_field = {
        "metadata_generation": selected["metadata_generation"],
        "inputs": validation.snapshot_inputs(selected),
        "tables": tables_by_name,
        "content_sha256": validation._content_digest(tables_by_name),
    }
    validation.verify_fhir_catalog_validation(selected, attestation_by_field)
    del tables_by_name["registry_fhir_catalog_source"]["bytes"]
    with pytest.raises(ValueError, match="registry_fhir_catalog_validation_invalid"):
        validation.verify_fhir_catalog_validation(selected, attestation_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_pin", ["grant", "approved_records"])
async def test_candidate_rechecks_approval_pins_before_reading_candidate_tables(monkeypatch, changed_pin):
    grant = standing_grant()
    selected = snapshot(grant)
    fingerprints = AsyncMock(return_value={6: {"metadata": "f" * 64, "membership": "b" * 64}})
    monkeypatch.setattr(validation, "approved_fhir_catalog_fingerprints", fingerprints)
    sources = AsyncMock(side_effect=AssertionError("changed approval must not read candidate tables"))
    monkeypatch.setattr(validation, "_validate_sources", sources)
    configured_grant = standing_grant(version=2) if changed_pin == "grant" else grant
    with pytest.raises(ValueError, match="registry_fhir_catalog_inputs_changed"):
        await validation.validate_fhir_catalog_candidate(
            object(),
            SimpleNamespace(schema_name="catalog_snapshot"),
            selected,
            control_schema="catalog_control",
            standing_grant=configured_grant,
        )
    sources.assert_not_awaited()
    assert fingerprints.await_count == int(changed_pin == "approved_records")


def _receipt(selected, tables_by_name):
    return {
        "metadata_generation": selected["metadata_generation"],
        "inputs": validation.snapshot_inputs(selected),
        "tables": tables_by_name,
        "content_sha256": validation._content_digest(tables_by_name),
    }


@pytest.mark.parametrize("rows,byte_count", [(0, 0), (1, 8192), (1000, 90112), (1001, 90112)])
def test_accounting_digest_keeps_the_retained_record_shape(rows, byte_count):
    report = validation._accounting("registry_fhir_catalog_network", rows, byte_count)
    assert set(report) == {"rows", "sha256", "batches", "bytes"}
    assert (report["rows"], report["bytes"], report["batches"]) == (rows, byte_count, -(-rows // 1000))
    assert report["sha256"] != validation._accounting("registry_fhir_catalog_source", rows, byte_count)["sha256"]


def test_records_with_content_or_accounting_digests_both_verify_unchanged():
    selected = snapshot(standing_grant())
    selected["accounting_json"] = None
    content_by_table = {
        table: {"rows": 3, "sha256": hashlib.sha256(table.encode()).hexdigest(), "batches": 1, "bytes": 90}
        for table in validation._TABLES
    }
    accounting_by_table = {table: validation._accounting(table, 3, 8192) for table in validation._TABLES}
    for tables_by_name in (content_by_table, accounting_by_table):
        validation.verify_fhir_catalog_validation(selected, _receipt(selected, tables_by_name))
        accounted_by_field = {**selected, "accounting_json": json.dumps(tables_by_name)}
        validation.verify_fhir_catalog_validation(accounted_by_field, _receipt(selected, tables_by_name))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prior,missing,expected",
    [
        (None, False, None),
        ({"metadata_generation": None, "schema_name": None}, False, None),
        ({"metadata_generation": "a" * 64, "schema_name": None}, False, "snapshot_unavailable"),
        ({"metadata_generation": "a" * 64, "schema_name": "fhir_catalog_" + "b" * 32}, False, None),
        (
            {"metadata_generation": "a" * 64, "schema_name": "fhir_catalog_" + "b" * 32},
            True,
            "observation_history_changed",
        ),
    ],
)
async def test_published_networks_must_survive_in_the_successor(prior, missing, expected):
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=prior), fetchval=AsyncMock(return_value=missing))
    check = validation._validate_prior_networks(connection, '"candidate"', '"control"', {})
    if expected is None:
        await check
    else:
        with pytest.raises(ValueError, match="registry_fhir_catalog_" + expected):
            await check
    assert connection.fetchval.await_count == int(bool(prior and prior["schema_name"]))


@pytest.mark.asyncio
async def test_imported_inventory_successor_skips_the_published_network_check():
    connection = SimpleNamespace(fetchrow=AsyncMock(), fetchval=AsyncMock())
    await validation._validate_prior_networks(connection, '"candidate"', '"control"', {"imported_source_inventory": {}})
    connection.fetchrow.assert_not_awaited()
