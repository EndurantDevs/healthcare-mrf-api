import json
import sqlite3
from datetime import date
from types import SimpleNamespace

import pytest

from process.delta_dental_directory_capture import _create_capture_tables
from process.delta_dental_directory_json import SOURCE_ID
from process.delta_dental_directory_normalization import normalized_delta_capture
from process.provider_directory_fhir import parse_fhir_resource

_EDITION = "a" * 64
_PPO = "Delta Dental PPO"
_PREMIER = "Delta Dental Premier"
_LINEAGE = "root-synthetic"


def _office(relation=11, office=100, unit="Unit 3", npi="1234567893", products=(_PPO,)):
    return {
        "providerServiceOfficeId": relation,
        "providerNPI": npi,
        "provider": {"firstName": "Synthetic", "lastName": "Fixture"},
        "serviceOffice": {
            "serviceOfficeNumber": office,
            "location": {
                "streetAddress": "1 Example Way",
                "streetAddressLine2": unit,
                "city": "Example",
                "stateType": "MI",
                "zipCode": "00123",
            },
        },
        "products": [{"productType": value} for value in products],
    }


def _capture(tmp_path):
    database = tmp_path / f"finder-{_EDITION}.sqlite3"
    with sqlite3.connect(database) as connection:
        _create_capture_tables(connection)
        connection.execute("CREATE TABLE finder_plan(id TEXT PRIMARY KEY, definition TEXT)")
        connection.executemany(
            "INSERT INTO finder_plan VALUES (?, ?)",
            [
                (_LINEAGE, json.dumps([_EDITION, [[_PPO, "00123", 30], [_PPO, "00124", 30], [_PPO, "00125", 30]]])),
                ("other-synthetic", json.dumps([_EDITION, [[_PPO, "00126", 30]]])),
            ],
        )
        first_records = [
            _office(products=(_PPO, _PREMIER)),
            _office(relation=12, office=200, unit="Unit 4"),
            _office(relation=13, office=300),
            _office(relation=14, office=400, npi="1234567890"),
            _office(relation=15, office=500, products=("Unknown product", f"{_PPO} ")),
        ]
        second_records = [_office(), _office(relation=13, office=300, unit="Different unit")] * 50
        connection.executemany(
            "INSERT INTO finder_page VALUES (?, ?, 30, 1, ?)",
            [
                (_PPO, zip_code, json.dumps({"providerResults": office_records}).encode())
                for zip_code, office_records in (
                    ("00123", first_records),
                    ("00124", second_records),
                    ("00126", [_office(19, 900)]),
                )
            ],
        )
        connection.execute("INSERT INTO finder_gap VALUES (?, ?, 30, ?)", (_PPO, "00125", "example_gap"))
    return database


def test_source_resources_are_deterministic_positive_observations(tmp_path):
    database = _capture(tmp_path)
    input_by_field = {"edition": _EDITION, "lineage": _LINEAGE, "reviewed_products": frozenset({_PPO, _PREMIER})}
    with normalized_delta_capture(database, **input_by_field) as capture:
        resources = list(capture.iter_resources())
        proof = capture.proof
        assert capture.resource_counts == {"Location": 2, "Organization": 2, "Practitioner": 1, "PractitionerRole": 2}
    with normalized_delta_capture(database, **input_by_field) as capture:
        assert list(capture.iter_resources()) == resources
        assert capture.proof == proof
    assert proof["directory_complete"] is False
    assert proof["planned_windows"] == 3
    assert proof["observed_windows"] == 2
    assert proof["unobserved_windows"] == proof["capped_windows"] == proof["windows_with_retained_gap"] == 1
    assert proof["unresolved_rows"] == proof["conflicting_memberships"] == 1
    assert proof["deduplicated_memberships"] == 100
    resources_by_key = {(resource["resourceType"], resource["id"]): resource for resource in resources}
    assert list(resources_by_key) == sorted(resources_by_key)
    assert resources_by_key["Location", "delta-office-100"]["address"]["line"] == ["1 Example Way", "Unit 3"]
    assert len(resources_by_key["PractitionerRole", "delta-role-11"]["extension"]) == 2
    assert len(resources_by_key["PractitionerRole", "delta-role-12"]["extension"]) == 1
    parsed_rows_by_key = {
        key: parse_fhir_resource(SOURCE_ID, resource)[1] for key, resource in resources_by_key.items()
    }
    assert parsed_rows_by_key["Location", "delta-office-100"]["second_line"] == "Unit 3"
    assert parsed_rows_by_key["Location", "delta-office-100"]["country_code"] == "US"
    assert parsed_rows_by_key["Practitioner", "delta-npi-1234567893"]["npi"] == 1234567893
    role = parsed_rows_by_key["PractitionerRole", "delta-role-11"]
    assert role["location_refs"] == ["Location/delta-office-100"]
    assert role["network_refs"] == ["Organization/delta-network-ppo", "Organization/delta-network-premier"]


@pytest.mark.parametrize("invalid_kind", ["html", "additional_page", "mixed_edition"])
def test_source_capture_rejects_unsupported_or_mixed_input(tmp_path, invalid_kind):
    database = _capture(tmp_path)
    with sqlite3.connect(database) as connection:
        if invalid_kind == "html":
            connection.execute("UPDATE finder_page SET body=? WHERE zip_code='00123'", (b"<html>",))
        elif invalid_kind == "additional_page":
            connection.execute("INSERT INTO finder_page VALUES (?, '00123', 30, 2, ?)", (_PPO, b"<html>"))
        else:
            connection.execute("UPDATE finder_plan SET definition=? WHERE id='other-synthetic'", ('["wrong", []]',))
    input_by_field = {"edition": _EDITION, "lineage": _LINEAGE, "reviewed_products": frozenset({_PPO})}
    with pytest.raises(ValueError), normalized_delta_capture(database, **input_by_field):
        pytest.fail("Invalid capture reached normalized output")


def test_cumulative_lineages_keep_positive_roles_and_deduplicate_windows(tmp_path):
    database = _capture(tmp_path)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE finder_plan SET definition=? WHERE id='other-synthetic'",
            (json.dumps([_EDITION, [[_PPO, "00123", 30], [_PPO, "00126", 30]]]),),
        )
    with normalized_delta_capture(
        database, edition=_EDITION, lineage=(_LINEAGE, "other-synthetic"), reviewed_products=frozenset({_PPO})
    ) as capture:
        assert capture.proof["planned_windows"] == 4
        assert capture.proof["observed_windows"] == 3
        assert capture.proof["publisher_rows"] == 106
        assert capture.resource_counts["PractitionerRole"] == 3
        assert "delta-role-19" in {resource["id"] for resource in capture.iter_resources()}


def test_catalog_admits_only_exact_delta_positive_scope(tmp_path):
    from process.delta_dental_directory_json import SOURCE_ENDPOINT
    from process.delta_dental_directory_producer import (
        DELTA_OBSERVED_ENDPOINT_ID,
        METADATA_KEY,
        DeltaObservedSourceScope,
        _receipt_for_capture,
    )
    from process.registry_fhir_catalog_grant import fhir_catalog_admitted_current_sources
    from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT, FHIRCatalogObservationDigests
    from process.registry_fhir_catalog_observed_source import build_fhir_catalog_observation_descriptor
    from process.registry_fhir_catalog_sources import validate_fhir_catalog_source_descriptor
    from tests.test_registry_fhir_catalog_sources import descriptor, published_row, read_profile

    database = _capture(tmp_path)
    products = frozenset({_PPO})
    scope = DeltaObservedSourceScope(
        database, _EDITION, (_LINEAGE,), products, DELTA_OBSERVED_ENDPOINT_ID, date(2026, 1, 1)
    )
    with normalized_delta_capture(
        database, edition=_EDITION, lineage=scope.lineages, reviewed_products=products
    ) as capture:
        coverage_by_field = _receipt_for_capture(capture, scope)
        family_counts = capture.resource_counts
    families_by_name = FHIRCatalogObservationDigests().document()
    for family in families_by_name:
        families_by_name[family]["rows"] = family_counts.get(family, 0)
    header_by_field = published_row(
        resource_count=sum(family_counts.values()),
        endpoint_id=DELTA_OBSERVED_ENDPOINT_ID,
        **{METADATA_KEY: json.dumps(coverage_by_field)},
    )
    profile = read_profile()
    tables_by_name = descriptor()["tables"]
    published = build_fhir_catalog_observation_descriptor(
        profile, header_by_field, tables_by_name, (SOURCE_ENDPOINT, [SOURCE_ID]), families_by_name
    )
    assert validate_fhir_catalog_source_descriptor(published) == published
    from process.registry_imported_source_inventory import preferred_fhir_editions

    assert published["complete"] is False
    assert preferred_fhir_editions([published]) == [published]
    assert published["admission"][METADATA_KEY]["capped_windows"] == 1
    grant = SimpleNamespace(source_profiles=(profile,), observation_contract=CATALOG_OBSERVATION_CONTRACT)
    vector_entries = [{"source_generation": "delta-generation", "status": "current"}]
    assert fhir_catalog_admitted_current_sources(grant, vector_entries, {"delta-generation": published}) == ()
    with pytest.raises(ValueError):
        validate_fhir_catalog_source_descriptor({**published, "complete": True})
    families_by_name["PractitionerRole"]["rows"] += 1
    with pytest.raises(ValueError):
        build_fhir_catalog_observation_descriptor(
            profile, header_by_field, tables_by_name, (SOURCE_ENDPOINT, [SOURCE_ID]), families_by_name
        )


def _retained_source_state(directory, endpoint_id, run_id, metadata_key, receipt_by_field):
    """Model the persisted genuine handoff after a successful native Source attempt."""
    return {
        "status": directory.ENDPOINT_DATASET_VALIDATED,
        "endpoint_id": endpoint_id,
        "acquisition_root_run_id": run_id,
        "resource_count": sum(receipt_by_field["resource_counts"].values()),
        "previous_dataset_id": None,
        "publication_metadata_json": {
            metadata_key: receipt_by_field,
            directory.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY: "2026-01-01",
        },
    }


def _observed_source_request(run_id):
    from process.delta_dental_directory_json import SOURCE_ENDPOINT
    from process.delta_dental_directory_producer import REVIEW_FIELD, TASK_FLAG

    return {
        TASK_FLAG: True,
        REVIEW_FIELD: "f" * 32,
        "run_id": run_id,
        "source_ids": [SOURCE_ID],
        "provider_directory_endpoint_scope": SOURCE_ENDPOINT,
    }


@pytest.mark.asyncio
async def test_observed_source_stages_normal_resources_and_returns_named_handoff(monkeypatch, tmp_path):
    from importlib import import_module
    from unittest.mock import AsyncMock

    from process import delta_dental_directory_producer as producer
    from process import provider_directory_generic_source_completion as completion

    database = _capture(tmp_path)
    directory = import_module("process.provider_directory_fhir")
    run_id = "run_" + "1" * 32
    scope = producer.DeltaObservedSourceScope(
        database, _EDITION, (_LINEAGE,), frozenset({_PPO}), producer.DELTA_OBSERVED_ENDPOINT_ID, date(2026, 1, 1)
    )
    request_by_field = _observed_source_request(run_id)
    monkeypatch.setattr(producer, "_authorized_scope", AsyncMock(return_value=scope))
    monkeypatch.setattr(producer, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(directory, "_endpoint_dataset_state", AsyncMock(return_value={}))
    candidate = directory.EndpointDatasetCandidate(
        scope.endpoint_id,
        directory._endpoint_dataset_candidate_id(scope.endpoint_id, producer.RESOURCE_TYPES, run_id),
        run_id,
        (SOURCE_ID,),
        producer.RESOURCE_TYPES,
        run_id,
        None,
        semantic_projection_as_of="2026-01-01",
    )
    monkeypatch.setattr(producer, "_prepare_candidate", AsyncMock(return_value=candidate))
    writes = []

    async def persist(model, office_rows, **options):
        assert options["dataset_id"] == candidate.dataset_id and options["track_seen"] is False
        writes.append((model, office_rows))
        return len(office_rows)

    async def handoff(_directory, selected_candidate, receipts_by_dataset):
        assert selected_candidate.dataset_id == candidate.dataset_id
        receipts_by_dataset[candidate.dataset_id] = {"document": "synthetic"}

    monkeypatch.setattr(directory, "_upsert_deferred_resource_rows", persist)
    monkeypatch.setattr(directory, "_validate_endpoint_dataset_candidate", AsyncMock())
    monkeypatch.setattr(completion, "record_candidate_handoff", handoff)
    result_by_field = await directory.process_provider_directory_fhir_data({"control_run_id": run_id}, request_by_field)
    assert result_by_field["source_candidate_handoffs"] == {candidate.dataset_id: {"document": "synthetic"}}
    assert len(writes) == len(producer.RESOURCE_TYPES)
    diagnostic_by_family = directory._validate_endpoint_dataset_candidate.await_args.args[-1]
    assert all(entry["complete"] is False for entry in diagnostic_by_family.values())
    assert result_by_field[producer.METADATA_KEY]["capped_windows"] == 1
    directory._endpoint_dataset_state.return_value = _retained_source_state(
        directory, scope.endpoint_id, run_id, producer.METADATA_KEY, result_by_field[producer.METADATA_KEY]
    )
    database.rename(database.with_suffix(".held"))
    assert (
        await directory.process_provider_directory_fhir_data({"control_run_id": run_id}, request_by_field)
        == result_by_field
    )
    assert directory._validate_endpoint_dataset_candidate.await_count == 1


def test_observed_source_enrollment_has_no_fhir_transport():
    from datetime import datetime
    from importlib import import_module

    from process.delta_dental_directory_producer import (
        DELTA_OBSERVED_ENDPOINT_ID,
        TRANSPORT,
        observed_source_row,
    )

    directory = import_module("process.provider_directory_fhir")
    observed_at = datetime(2026, 1, 1)
    source_by_field = observed_source_row(observed_at)
    endpoints = directory._attach_provider_directory_endpoint_ids([source_by_field])
    assert len(endpoints) == 1
    assert endpoints[0]["endpoint_id"] == source_by_field["endpoint_id"] == DELTA_OBSERVED_ENDPOINT_ID
    assert endpoints[0]["canonical_api_base"] == source_by_field["api_base"]
    assert endpoints[0]["credential_descriptor_json"] == {}
    assert endpoints[0]["endpoint_signature_json"]["transport"] == TRANSPORT
    metadata_by_field = source_by_field["metadata_json"]
    assert metadata_by_field["provider_directory_fhir_endpoint"] is False
    assert metadata_by_field["provider_directory_profile_eligible"] is False
    assert metadata_by_field["provider_directory_acquisition_enabled"] is False
    assert directory._provider_directory_api_endpoint_row({**source_by_field, "metadata_json": None}) is None
    assert directory._resource_acquisition_blocked_reason(source_by_field) == "fully_enumerable_resources_empty"
    assert directory._source_rows_allowed_for_probe([source_by_field]) == []


def test_window_evidence_keeps_uncertain_requests_separate(tmp_path):
    database = _capture(tmp_path)
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM finder_gap")
        connection.execute("INSERT INTO finder_attempt VALUES (?, '00125', 30, 1)", (_PPO,))
        connection.execute("DELETE FROM finder_page WHERE zip_code='00124'")
    with normalized_delta_capture(
        database, edition=_EDITION, lineage=_LINEAGE, reviewed_products=frozenset({_PPO})
    ) as capture:
        assert capture.proof["unobserved_windows"] == 2
        assert capture.proof["unobserved_with_request_intent"] == 1
        assert capture.proof["unobserved_without_retained_attempt"] == 1
        assert capture.proof["unobserved_attempt_status_unknown"] == 0
