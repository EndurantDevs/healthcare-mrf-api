# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject changed candidate evidence and retain exact preparation ownership."""

import asyncio
import datetime
import json
import os
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy import Column, MetaData, String, Table

from api import provider_directory_cms_candidate_catalog as candidate_catalog
from process import cms_doctors_preparation as preparation
from process import cms_doctors_sites as sites
from process import cms_npd_tax_candidate_followup as followup
from process import cms_npd_tax_candidate_lookup as lookup
from process import cms_npd_tax_candidate_report as report
from process import cms_npd_tax_candidate_runner as runner
from process import provider_directory_cms_capacity_contract as cms_contract
from process import provider_directory_entity_identity as entity_identity
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_selection_contract as selection
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process.control_cancel import ImportCancelledError
from process.tin_npi_connector import TinNpiConnectorError
from tests.provider_directory_cms_capacity_test_support import (
    cms_execution,
    cms_guard,
    cms_plan,
    cms_request,
    sign_guard,
)
from tests.test_cms_npd_tax_candidate_followup import _Fhir
from tests.test_cms_npd_tax_candidate_lookup import _organization, _report_for_organizations, _Result, _Session
from tests.test_cms_npd_tax_candidate_runner import _admitted_release, _run_candidate_report
from tests.test_cms_npd_tax_candidate_runner import _organization as _source_organization
from tests.test_provider_directory_capacity_reservation_snapshot import _consumption, _preflight, _project, _run
from tests.test_provider_directory_cms_candidate_catalog import (
    CATALOG,
    NOW,
    _dataset,
    _pair,
)
from tests.test_provider_directory_cms_candidate_catalog import (
    _install as _install_catalog,
)
from tests.test_provider_directory_cms_candidate_catalog import (
    _run as _catalog_run,
)
from tests.test_provider_directory_cms_candidate_catalog import (
    _Session as _CatalogSession,
)
from tests.test_provider_directory_cms_capacity_contract import _cms_task
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _signed_envelope


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", cms_execution().attestation.node_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("npis", ["1000000004", b"1000000004", (True,), (1000000004,) * 2, (1234567893, 1000000004)])
async def test_candidate_lookup_rejects_noncanonical_npis_before_reading_snapshot(npis):
    session = _Session()
    with pytest.raises(ValueError, match="^CMS tax candidate NPIs are invalid$"):
        await lookup.lookup_pinned_tax_candidates(
            session, schema_name="mrf", snapshot_key=17, manifest_sha256="b" * 64, npis=npis
        )
    assert session.queries == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", [("schema_name", "bad-schema"), ("snapshot_key", True), ("manifest_sha256", "B" * 64)]
)
async def test_candidate_lookup_rejects_invalid_pin_before_sql(field, value):
    arguments_by_name = {"schema_name": "mrf", "snapshot_key": 17, "manifest_sha256": "b" * 64, "npis": ()}
    arguments_by_name[field] = value
    session = _Session()
    with pytest.raises(ValueError, match="^CMS tax candidate snapshot pin is invalid$"):
        await lookup.lookup_pinned_tax_candidates(session, **arguments_by_name)
    assert session.queries == []


@pytest.mark.asyncio
async def test_current_candidate_pin_rejects_invalid_schema_before_sql():
    session = _Session()
    with pytest.raises(ValueError, match="^CMS tax candidate schema is invalid$"):
        await lookup.current_sealed_v4_tax_pin(session, schema_name="bad-schema")
    assert session.queries == []


@pytest.mark.asyncio
async def test_unavailable_candidate_snapshot_stops_before_graph_lookup(monkeypatch):
    session = _Session()
    original_execute = session.execute

    async def execute(statement, params=None):
        if "SELECT layout.generation" in str(statement):
            session.queries.append((str(statement), params))
            return _Result()
        return await original_execute(statement, params)

    session.execute = execute
    keys = AsyncMock()
    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", keys)
    with pytest.raises(ValueError, match="^CMS tax candidate snapshot is unavailable$"):
        await _report_for_organizations(session, (_organization(1000000004),))
    keys.assert_not_awaited()
    assert all("INSERT" not in sql and "UPDATE" not in sql for sql, _params in session.queries)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "keys_by_npi,graph_by_owner,error",
    [
        ({1000000004: 4, 1234567893: 5}, {}, "CMS tax candidate NPI dictionary is invalid"),
        ({1000000004: 4}, {}, "CMS tax candidate NPI graph is incomplete"),
        ({1000000004: 4}, {4: (1, 0)}, "CMS tax candidate NPI group evidence is invalid"),
        ({1000000004: 4}, {4: (0, 0)}, "CMS tax candidate NPI group evidence is invalid"),
        ({1000000004: 4}, {4: (-1,)}, "CMS tax candidate NPI group evidence is invalid"),
    ],
)
async def test_corrupt_candidate_graph_cannot_yield_report(monkeypatch, keys_by_npi, graph_by_owner, error):
    keys = AsyncMock(return_value=keys_by_npi)
    graph = AsyncMock(return_value=graph_by_owner)
    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", keys)
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", graph)
    session = _Session()
    with pytest.raises(ValueError, match=f"^{error}$"):
        await _report_for_organizations(session, (_organization(1000000004),))
    assert not any("SELECT groups.provider_group_key" in sql for sql, _params in session.queries)
    if "dictionary" in error:
        graph.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_dense_npi_keys_cannot_merge_two_candidate_owners(monkeypatch):
    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", AsyncMock(return_value={1000000004: 4, 1234567893: 4}))
    graph = AsyncMock()
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", graph)
    with pytest.raises(ValueError, match="^CMS tax candidate NPI dictionary is invalid$"):
        await _report_for_organizations(_Session(), (_organization(1000000004, 1234567893),))
    graph.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [{"provider_group_key": 0, "tax_identity_state": "matched_ein", "tin_key": None}],
        [{"provider_group_key": 0, "tax_identity_state": "missing", "tin_key": 5}],
        [{"provider_group_key": 0, "tax_identity_state": "unknown", "tin_key": None}],
        [{"provider_group_key": 0, "tax_identity_state": "matched_ein", "tin_key": 5}] * 2,
    ],
)
async def test_corrupt_group_tax_evidence_cannot_be_serialized(monkeypatch, rows):
    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", AsyncMock(return_value={1000000004: 4}))
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", AsyncMock(return_value={4: (0,)}))
    session = _Session()
    session.group_rows = rows
    with pytest.raises(ValueError, match="^CMS tax candidate group tax evidence is invalid$"):
        await _report_for_organizations(session, (_organization(1000000004),))
    assert all("INSERT" not in sql and "UPDATE" not in sql for sql, _params in session.queries)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["missing", "malformed", "unsupported_type"])
async def test_non_ein_group_states_remain_explicit_candidate_absence(monkeypatch, state):
    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", AsyncMock(return_value={1000000004: 4}))
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", AsyncMock(return_value={4: (0,)}))
    session = _Session()
    session.group_rows = [{"provider_group_key": 0, "tax_identity_state": state, "tin_key": None}]
    payload = json.loads(await _report_for_organizations(session, (_organization(1000000004),)))
    row = payload["organizations"][0]
    assert row["state"] == "no_match" and row["missing_real_ein"] is True
    assert row["npis"][0]["candidate_tin_keys"] == []
    assert row["npis"][0]["groups_without_ein_match"] == 1
    assert sum("SELECT layout.generation" in sql for sql, _params in session.queries) == 2


@pytest.mark.asyncio
async def test_empty_candidate_report_still_checks_snapshot_twice_without_graph(monkeypatch):
    keys = AsyncMock()
    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", keys)
    session = _Session()
    payload = json.loads(await _report_for_organizations(session, ()))
    assert payload["organizations"] == [] and payload["tax_snapshot_key"] == 17
    assert sum("SELECT layout.generation" in sql for sql, _params in session.queries) == 2
    keys.assert_not_awaited()


def _candidate_report(**changes):
    arguments_by_name = {
        "dataset_id": "dataset-a",
        "release_id": "release-a",
        "tax_snapshot_key": 17,
        "tax_manifest_sha256": "b" * 64,
        "extraction_policy_sha256": "c" * 64,
        "extraction_cutoff": "2026-09-24T00:00:00.000000Z",
        "organizations": (_organization(1000000004),),
        "candidates_by_npi": {1000000004: report.CmsNpiTaxCandidate((5,), 1, 0)},
    }
    arguments_by_name.update(changes)
    return report.build_cms_tax_candidate_report(**arguments_by_name)


@pytest.mark.parametrize(
    "candidate,error",
    [
        ({"tin_keys": [5]}, "CMS tax candidate lookup is invalid"),
        (report.CmsNpiTaxCandidate([5], 1, 0), "CMS tax candidate lookup is invalid"),
        (report.CmsNpiTaxCandidate((5,), True, 0), "CMS tax candidate lookup is invalid"),
        (report.CmsNpiTaxCandidate((5,), 0, 0), "CMS tax candidate lookup is invalid"),
        (report.CmsNpiTaxCandidate((5,), 129, 0, True), "CMS tax candidate overflow evidence is invalid"),
        (report.CmsNpiTaxCandidate((), 0, 0, True), "CMS tax candidate overflow evidence is invalid"),
        (report.CmsNpiTaxCandidate((), 129, 1, True), "CMS tax candidate overflow evidence is invalid"),
    ],
)
def test_report_refuses_inconsistent_lookup_outcomes(candidate, error):
    candidates_by_npi = {1000000004: candidate}
    original = deepcopy(candidates_by_npi)
    with pytest.raises(ValueError, match=f"^{error}$"):
        _candidate_report(candidates_by_npi=candidates_by_npi)
    assert candidates_by_npi == original


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("dataset_id", " dataset-a", "CMS tax candidate report pin is invalid"),
        ("tax_snapshot_key", True, "CMS tax candidate report pin is invalid"),
        ("candidates_by_npi", [], "CMS tax candidate report pin is invalid"),
        ("extraction_cutoff", "2026-09-24T00:00:00Z", "CMS tax candidate extraction cutoff is invalid"),
        ("extraction_cutoff", "invalid", "CMS tax candidate extraction cutoff is invalid"),
        ("extraction_cutoff", VALIDATION_TIME, "CMS tax candidate extraction cutoff is invalid"),
        (
            "organizations",
            (replace(_organization(1000000004), payload_sha256="bad"),),
            "CMS tax candidate Organization is invalid",
        ),
        (
            "organizations",
            (replace(_organization(1000000004), extraction=None),),
            "CMS tax candidate Organization is invalid",
        ),
        ("candidates_by_npi", {}, "CMS tax candidate lookup is incomplete"),
    ],
)
def test_report_requires_complete_canonical_source_and_snapshot_inputs(field, value, error):
    with pytest.raises(ValueError, match=f"^{error}$"):
        _candidate_report(**{field: value})


@asynccontextmanager
async def _unexpected_session():
    raise AssertionError("invalid source witness must not open a database session")
    yield


async def _run_without_database(directory, output, *, cutoff="2026-09-24T00:00:00.000000Z"):
    return await runner.run_admitted_cms_tax_candidate_report(
        _unexpected_session,
        release_directory=directory,
        output_path=output,
        schema_name="mrf",
        dataset_id="dataset-a",
        snapshot_key=17,
        manifest_sha256="b" * 64,
        evidence_as_of=cutoff,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,error",
    [
        ("release-link", "CMS tax candidate release is unavailable"),
        ("manifest-link", "CMS tax candidate release witness is invalid"),
        ("receipt-too-large", "CMS tax candidate release witness is invalid"),
        ("source-id", "CMS tax candidate release witness is invalid"),
        ("etag", "CMS tax candidate release vector changed"),
        ("file-digest", "CMS tax candidate release witness is invalid"),
        ("distinct-count", "CMS tax candidate release witness is invalid"),
        ("organization-link", "CMS tax candidate Organization file is unavailable"),
    ],
)
async def test_runner_rejects_changed_acquired_witness_before_lookup(tmp_path, change, error):
    directory, receipt = _admitted_release(tmp_path, [_source_organization(1)])
    if change == "release-link":
        link = tmp_path / "release-link"
        link.symlink_to(directory, target_is_directory=True)
        directory = link
    if change in {"manifest-link", "organization-link"}:
        name = "manifest.json" if change == "manifest-link" else "01-Organization.ndjson.zst"
        retained = directory / (name + ".retained")
        (directory / name).rename(retained)
        (directory / name).symlink_to(retained.name)
    if change == "receipt-too-large":
        (directory / "receipt.json").write_bytes(b" " * (64 * 1024 + 1))
    if change in {"source-id", "etag", "file-digest", "distinct-count"}:
        entry = receipt["files"]["01-Organization.ndjson"]
        if change == "source-id":
            receipt["source_id"] = "other-source"
        if change == "etag":
            entry["etag"] = "changed-etag"
        if change == "file-digest":
            entry["sha256"] = "invalid"
        if change == "distinct-count":
            entry["distinct_count"] = entry["row_count"] + 1
        (directory / "receipt.json").write_text(json.dumps(receipt))
    output = tmp_path / "report.json"
    with pytest.raises(ValueError, match=f"^{error}$"):
        await _run_without_database(directory, output)
    assert not output.exists() and not list(tmp_path.glob(".cms-tax-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing-parent", "parent-link", "cutoff"])
async def test_runner_checks_output_parent_and_cutoff_before_lookup(tmp_path, change):
    directory, _receipt = _admitted_release(tmp_path, [_source_organization(1)])
    output = tmp_path / "report.json"
    cutoff = "2026-09-24T00:00:00.000000Z"
    if change == "missing-parent":
        output = tmp_path / "missing" / "report.json"
    elif change == "parent-link":
        (tmp_path / "linked").symlink_to(directory, target_is_directory=True)
        output = tmp_path / "linked" / "report.json"
    else:
        cutoff = "2026-09-24T00:00:00Z"
    error = "evidence cutoff is invalid" if change == "cutoff" else "CMS tax candidate report directory is invalid"
    with pytest.raises(ValueError, match=f"^{error}$"):
        await _run_without_database(directory, output, cutoff=cutoff)
    assert not output.exists() and not list(tmp_path.glob(".cms-tax-*"))


@pytest.mark.asyncio
async def test_runner_counts_duplicate_rows_and_explicit_skips_without_tax_projection(tmp_path, monkeypatch):
    candidate = _source_organization(1)
    inactive_organization_map = {**_source_organization(2), "active": False}
    missing_organization_map = {**_source_organization(3), "identifier": {"system": "http://hl7.org/fhir/sid/us-npi"}}
    directory, _receipt = _admitted_release(
        tmp_path, [candidate, candidate, inactive_organization_map, missing_organization_map]
    )
    payload, calls, result = await _run_candidate_report(monkeypatch, directory, tmp_path / "report.json")
    assert payload["source_file_row_count"] == 4
    assert payload["candidate_organization_count"] == result.candidate_organization_count == 1
    assert payload["skipped_distinct_organizations"] == {"inactive": 1, "missing_identifiers": 1}
    assert [row["resource_id"] for row in payload["organizations"]] == ["synthetic-1"]
    assert calls == [(), (1000000004,)]


@pytest.mark.asyncio
async def test_runner_rechecks_acquired_row_counts_before_retaining_report(tmp_path, monkeypatch):
    directory, receipt = _admitted_release(tmp_path, [_source_organization(1)])
    receipt["files"]["01-Organization.ndjson"]["row_count"] += 1
    (directory / "receipt.json").write_text(json.dumps(receipt))
    output = tmp_path / "report.json"
    with pytest.raises(ValueError, match="^CMS tax candidate Organization witness changed$"):
        await _run_candidate_report(monkeypatch, directory, output)
    assert not output.exists() and not list(tmp_path.glob(".cms-tax-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["bytes", "permissions", "link"])
async def test_runner_preserves_conflicting_retained_report_and_cleans_workspace(tmp_path, monkeypatch, change):
    directory, _receipt = _admitted_release(tmp_path, [_source_organization(1)])
    output = tmp_path / "report.json"
    await _run_candidate_report(monkeypatch, directory, output)
    if change == "bytes":
        output.write_bytes(b"different evidence\n")
    elif change == "permissions":
        output.chmod(0o644)
    else:
        retained = tmp_path / "retained.json"
        output.rename(retained)
        output.symlink_to(retained.name)
    before = output.read_bytes()
    before_mode = output.stat().st_mode
    with pytest.raises(ValueError, match="^CMS tax candidate report path already has different evidence$"):
        await _run_candidate_report(monkeypatch, directory, output)
    assert output.read_bytes() == before and output.stat().st_mode == before_mode
    assert output.is_symlink() is (change == "link")
    assert not list(tmp_path.glob(".cms-tax-*"))


@pytest.mark.parametrize("observed_at", [VALIDATION_TIME.replace(tzinfo=None), "2026-07-30T00:00:00Z"])
def test_snapshot_requires_timezone_aware_observation_time(observed_at):
    with pytest.raises(RuntimeError, match="^provider_directory_capacity_snapshot_timestamp_invalid$"):
        _project(observed_at=observed_at)


@pytest.mark.parametrize(
    "change,error",
    [("params", "object_invalid"), ("envelope", "envelope_invalid"), ("body", "envelope_contract_unsupported")],
)
def test_snapshot_rejects_nonclosed_run_evidence_without_mutating_inputs(change, error):
    owner = _run(_signed_envelope())
    if change == "params":
        owner["params"] = ["invalid"]
    elif change == "envelope":
        owner["params"]["provider_directory_profile_capacity_attestation"]["extra"] = True
    else:
        owner["params"]["provider_directory_profile_capacity_attestation"]["lease"].pop("contract_id")
    original = deepcopy(owner)
    with pytest.raises(RuntimeError, match=f"^provider_directory_capacity_snapshot_{error}$"):
        _project(runs=[owner])
    assert owner == original


@pytest.mark.parametrize("change", ["recorded-at", "expiry", "body-time"])
def test_snapshot_rejects_consumption_times_that_disagree_with_original_lease(change):
    row = _consumption()
    if change == "recorded-at":
        row["recorded_at"] += datetime.timedelta(seconds=1)
    elif change == "expiry":
        row["accepted_at"] = row["recorded_at"] = row["expires_at"]
    else:
        row["observed_at"] += datetime.timedelta(seconds=1)
    original = deepcopy(row)
    with pytest.raises(RuntimeError, match="^provider_directory_capacity_snapshot_consumption_time_changed$"):
        _project(consumptions=[row])
    assert row == original


@pytest.mark.parametrize(
    "change,error",
    [
        ("generation", "run_execution_changed"),
        ("node", "run_node_changed"),
        ("pair", "run_profile_pair_changed"),
        ("purpose", "run_purpose_changed"),
    ],
)
def test_snapshot_rejects_run_execution_and_pair_drift(change, error):
    owner = _run(sign_guard(cms_guard(), cms=True))
    if change == "generation":
        owner["params"]["provider_directory_profile_generation"] += 1
    elif change == "node":
        owner["node_id"] = "other-node"
    elif change == "pair":
        owner["params"].pop("provider_directory_profile_capacity_attestation")
    else:
        owner["params"]["provider_directory_profile_capacity_attestation"] = owner["params"][
            selection.CMS_CAPACITY_EXECUTION_PARAM
        ]
    original = deepcopy(owner)
    with pytest.raises(RuntimeError, match=f"^provider_directory_capacity_snapshot_{error}$"):
        _project(runs=[owner])
    assert owner == original


@pytest.mark.parametrize(
    "change,error",
    [
        ("contract", "preflight_contract_unsupported"),
        ("digest", "preflight_digest_changed"),
        ("issued_at", "preflight_metadata_changed"),
        ("expires_at", "preflight_metadata_changed"),
    ],
)
def test_snapshot_rejects_durable_receipt_contract_digest_and_time_drift(change, error):
    row = _preflight(_signed_envelope())
    if change == "contract":
        receipt = json.loads(row["receipt_json"])
        receipt["contract_id"] = "unsupported-contract"
        row["receipt_json"] = json.dumps(receipt)
    elif change == "digest":
        row["receipt_sha256"] = "0" * 64
    else:
        row[change] += datetime.timedelta(seconds=1)
    original = deepcopy(row)
    with pytest.raises(RuntimeError, match=f"^provider_directory_capacity_snapshot_{error}$"):
        _project(preflights=[row])
    assert row == original


def test_snapshot_rejects_two_consumption_owners_of_same_original_lease():
    row = _consumption()
    other_owner_map = {**row, "run_id": "run_" + "c" * 32}
    with pytest.raises(RuntimeError, match="^provider_directory_capacity_snapshot_consumption_owner_conflict$"):
        _project(consumptions=[row, other_owner_map])


def test_snapshot_deduplicates_repeated_terminal_owner_evidence_without_release_claim():
    owner = _run(_signed_envelope(), status="succeeded")
    original = deepcopy(owner)
    result = _project(runs=[owner, owner])
    assert owner == original
    assert len(result["reservations"]) == 1
    assert len(result["reservations"][0]["observations"]) == 1
    assert result["release_proof_available"] is False and result["capacity_complete"] is False


@pytest.mark.parametrize(
    "change,error",
    [
        ("limits", "limits_contract_invalid"),
        ("pair-geometry", "paired_profile_purpose_invalid"),
        ("pair-signature", "pair_invalid"),
        ("request-contract", "request_contract_invalid"),
    ],
)
def test_cms_request_rejects_other_contracts_and_invalid_pair_evidence(change, error):
    request = cms_request()
    admission = request[cms_contract.CMS_ADMISSION_FIELD]
    if change == "limits":
        admission["limits"]["contract_id"] = "other-contract"
    elif change == "pair-geometry":
        admission["paired_profile_lease"]["lease"]["signing_preflight_guard"]["healthcare_receipt"][
            "capacity_geometry"
        ] = None
    elif change == "pair-signature":
        admission["paired_profile_lease"]["signature"] = "!"
    else:
        request["contract_id"] = cms_contract.CMS_PROJECTION_REQUEST_CONTRACT
    original = deepcopy(request)
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError, match=f"^provider_directory_cms_capacity_{error}$"
    ):
        cms_contract.validated_cms_preflight_request(request)
    assert request == original


def test_cms_execution_rejects_malformed_outer_signature_through_task_parser():
    task = _cms_task()
    task[selection.CMS_CAPACITY_EXECUTION_PARAM]["signature"] = "!"
    original = deepcopy(task)
    with pytest.raises(
        selection.ProviderDirectoryProfileSelectionError, match="CMS capacity attestation is invalid"
    ) as failure:
        selection.validated_profile_execution(task)
    assert str(failure.value.__cause__) == "provider_directory_cms_capacity_execution_envelope_invalid"
    assert task == original


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("contract_id", "other-contract", "geometry_contract_invalid"),
        ("admission_purpose", "profile", "geometry_contract_invalid"),
        ("publish_targets", "Organization", "geometry_scope_invalid"),
        ("reservation_bytes", {"data": 1}, "geometry_reservations_invalid"),
        ("reservation_bytes", [["data"]], "geometry_reservations_invalid"),
        ("desired_profile_as_of", "20260730", "geometry_invalid"),
    ],
)
def test_cms_geometry_refuses_noncanonical_authenticated_plan_shapes(field, value, error):
    raw = deepcopy(cms_guard()["healthcare_receipt"]["capacity_geometry"])
    raw[field] = value
    original = deepcopy(raw)
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError, match=f"^provider_directory_cms_capacity_{error}$"
    ):
        cms_contract.validated_cms_capacity_geometry(raw)
    assert raw == original


@pytest.mark.parametrize(
    "field,value",
    [
        ("database_system_identifier", "invalid"),
        ("database_oid", True),
        ("database_name", ""),
        ("tablespace_oid", 0),
        ("tablespace_name", ""),
    ],
)
def test_cms_database_binding_rejects_unusable_database_and_tablespace_identity(field, value):
    binding = deepcopy(cms_guard()["healthcare_receipt"]["database_binding"])
    binding[field] = value
    original = deepcopy(binding)
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="^provider_directory_cms_capacity_database_binding_invalid$",
    ):
        cms_contract.validated_cms_database_binding(binding)
    assert binding == original


def test_valid_cms_geometry_and_request_retain_exact_plan_and_independent_pair():
    plan, _fence, _pair = cms_plan()
    guard = cms_guard(plan)
    request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
    parsed_plan = cms_contract.validated_cms_capacity_geometry(guard["healthcare_receipt"]["capacity_geometry"])
    cms_contract.assert_cms_geometry_matches_request(parsed_plan, request)
    assert parsed_plan == plan and request.request_payload == guard["healthcare_request"]
    raw = guard["healthcare_request"][cms_contract.CMS_ADMISSION_FIELD]["paired_profile_lease"]
    parsed = request.cms_nonprofile_admission["paired_profile_lease"]
    assert parsed == raw and parsed is not raw
    raw["signature"] = "changed"
    assert parsed["signature"] != "changed"


@pytest.mark.asyncio
@pytest.mark.parametrize("owned", [False, True])
async def test_doctors_site_binding_refuses_unowned_or_other_schema_stage_before_writes(monkeypatch, owned):
    context_by_name = {"context": {"group_site_stage_owned": owned}}
    database = SimpleNamespace(transaction=Mock(side_effect=AssertionError("must not start a transaction")))
    monkeypatch.setattr(sites, "db", database)
    monkeypatch.setattr(sites, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(
        sites, "make_class", Mock(return_value=SimpleNamespace(__table__=SimpleNamespace(schema="other")))
    )
    bind = AsyncMock()
    monkeypatch.setattr(sites, "bind_cms_doctors_site_batch", bind)
    error = "schema_mismatch" if owned else "stage_not_owned"
    with pytest.raises(RuntimeError, match=f"^cms_group_site_binding_{error}$"):
        await sites.bind_cms_doctors_sites(context_by_name, "20260730", "mrf", {})
    database.transaction.assert_not_called()
    bind.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_second_batch", [False, True])
async def test_doctors_site_batches_commit_prior_work_and_roll_back_cancelled_batch(monkeypatch, cancel_second_batch):
    events = []
    bound_batches = []

    @asynccontextmanager
    async def transaction():
        yield object()

    @asynccontextmanager
    async def session():
        try:
            yield object()
        except ImportCancelledError:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    async def bind(_session, *, adrs_ids):
        bound_batches.append(tuple(adrs_ids))
        events.append(tuple(adrs_ids))

    async def cancelled(_ctx, _task):
        if cancel_second_batch and len(bound_batches) == 2:
            raise ImportCancelledError("synthetic cancellation")

    table = SimpleNamespace(schema="mrf")
    monkeypatch.setattr(sites, "db", SimpleNamespace(transaction=transaction, session=session))
    monkeypatch.setattr(sites, "make_class", lambda *_args: SimpleNamespace(__table__=table))
    monkeypatch.setattr(sites, "_lock_group_stage", AsyncMock(return_value=41))
    monkeypatch.setattr(sites, "validate_group_site_stage", AsyncMock())
    pages = AsyncMock(side_effect=[["site-a", "site-b"], ["site-c"], []])
    complete = AsyncMock()
    monkeypatch.setattr(sites, "_read_site_page", pages)
    monkeypatch.setattr(sites, "_require_complete_site_bindings", complete)
    monkeypatch.setattr(sites, "bind_cms_doctors_site_batch", bind)
    monkeypatch.setattr(sites, "raise_if_cancelled", cancelled)
    context_by_name = {"context": {"group_site_stage_owned": True, "control_run_id": "run_" + "a" * 32}}
    if cancel_second_batch:
        with pytest.raises(ImportCancelledError, match="^synthetic cancellation$"):
            await sites.bind_cms_doctors_sites(context_by_name, "20260730", "mrf", {})
        assert events == [("site-a", "site-b"), "commit", ("site-c",), "rollback"]
        complete.assert_not_awaited()
        assert pages.await_count == 2
    else:
        assert await sites.bind_cms_doctors_sites(context_by_name, "20260730", "mrf", {}) == 3
        assert events == [("site-a", "site-b"), "commit", ("site-c",), "commit"]
        complete.assert_awaited_once_with(table, 41, "20260730", "mrf", {})
    assert pages.await_args_list[0].args == (table, 41, None)
    assert pages.await_args_list[1].args == (table, 41, "site-b")
    if not cancel_second_batch:
        assert pages.await_args_list[2].args == (table, 41, "site-c")


@pytest.mark.asyncio
@pytest.mark.parametrize("active,committed", [(True, False), (False, True), (False, False)])
async def test_doctors_cleanup_respects_publication_transaction_and_committed_ownership(monkeypatch, active, committed):
    prepared = SimpleNamespace(schema="mrf", stage_oids=(("target", "stage", 41),), committed=committed)
    database = SimpleNamespace(_transaction_binding=lambda: object() if active else None)
    monkeypatch.setattr(preparation, "_native", lambda: SimpleNamespace(db=database))
    drop = AsyncMock()
    monkeypatch.setattr(preparation, "_drop_owned_stages", drop)
    if active:
        with pytest.raises(RuntimeError, match="^cms_doctors_preparation_cleanup_transaction_active$"):
            await preparation.cleanup_prepared_cms_doctors(prepared)
        drop.assert_not_awaited()
    else:
        await preparation.cleanup_prepared_cms_doctors(prepared)
        if committed:
            drop.assert_not_awaited()
        else:
            drop.assert_awaited_once_with("mrf", prepared.stage_oids)


@pytest.mark.asyncio
async def test_doctors_cleanup_finishes_owned_drop_before_propagating_cancellation(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()
    prepared = SimpleNamespace(schema="mrf", stage_oids=(("target", "stage", 41),), committed=False)
    monkeypatch.setattr(
        preparation, "_native", lambda: SimpleNamespace(db=SimpleNamespace(_transaction_binding=lambda: None))
    )

    async def drop(schema, stages):
        assert (schema, stages) == (prepared.schema, prepared.stage_oids)
        entered.set()
        await release.wait()
        finished.set()

    monkeypatch.setattr(preparation, "_drop_owned_stages", drop)
    cleanup = asyncio.create_task(preparation.cleanup_prepared_cms_doctors(prepared))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        cleanup.cancel()
        await asyncio.sleep(0)
        assert not cleanup.done() and not finished.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(cleanup, timeout=2)
    assert finished.is_set()


@pytest.mark.asyncio
async def test_doctors_preparation_refuses_nested_transaction_before_database_setup(monkeypatch):
    ensure_database = AsyncMock()
    monkeypatch.setattr(
        preparation,
        "_native",
        lambda: SimpleNamespace(
            db=SimpleNamespace(_transaction_binding=lambda: object()), ensure_database=ensure_database
        ),
    )
    with pytest.raises(RuntimeError, match="^cms_doctors_preparation_requires_own_scope$"):
        async with preparation.prepare_cms_doctors_generation({"import_date": "20260730", "context": {"run": True}}):
            pytest.fail("nested preparation must not yield")
    ensure_database.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_doctors_apply_refuses_stale_authority_or_already_consumed_preparation(monkeypatch, committed):
    authority = generation.ReferenceFamilyResultGenerationAuthority(
        "cms-doctors", "00000000-0000-0000-0000-000000000001", 1, None, None
    )
    incumbent = archive.ReferenceFamilyIncumbent("cms-doctors", "mrf", (("doctor_clinician_address", 11),))
    prepared = preparation.PreparedCMSDoctorsGeneration(
        "mrf",
        "20260730",
        (("doctor_clinician_address", "stage", 41),),
        incumbent,
        authority,
        {},
        {},
        committed=committed,
    )
    session = object()
    apply = AsyncMock()
    native = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session), status=object()),
        lock_live_serving_relations=AsyncMock(),
        _apply_cms_doctors_stage=apply,
    )
    monkeypatch.setattr(preparation, "_native", lambda: native)
    lock = AsyncMock()
    monkeypatch.setattr(archive, "_lock_family", lock)
    monkeypatch.setattr(archive, "_verify_incumbent", AsyncMock())
    read = AsyncMock(return_value=replace(authority, local_generation=2))
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", read)
    error = "cms_doctors_preparation_already_committed" if committed else "cms_doctors_incumbent_authority_changed"
    with pytest.raises(RuntimeError, match=f"^{error}$"):
        await preparation.apply_prepared_cms_doctors_generation(prepared)
    apply.assert_not_awaited()
    assert prepared.native_receipt is None and prepared.metrics == {} and prepared.context == {}
    if committed:
        lock.assert_not_awaited()
        read.assert_not_awaited()
    else:
        read.assert_awaited_once_with(session, importer_id="cms-doctors", schema_name="mrf", lock=True)


def _synthetic_npis(count):
    values = []
    for prefix in range(100_000_000, 100_000_000 + count):
        for digit in range(10):
            try:
                values.append(lookup._normalize_npi(f"{prefix}{digit}"))
                break
            except TinNpiConnectorError:
                continue
    return values


@pytest.mark.asyncio
async def test_runner_refuses_one_organization_exceeding_lookup_bound_and_removes_workspace(tmp_path, monkeypatch):
    organization = _source_organization(1, npi=False)
    organization["identifier"] += [
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": str(npi)} for npi in _synthetic_npis(257)
    ]
    directory, _receipt = _admitted_release(tmp_path, [organization])
    output = tmp_path / "report.json"
    with pytest.raises(ValueError, match="^CMS tax candidate Organization NPI set exceeds lookup bound$"):
        await _run_candidate_report(monkeypatch, directory, output)
    assert not output.exists() and not list(tmp_path.glob(".cms-tax-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,error",
    [
        ("resource", "CMS tax candidate Organization witness is invalid"),
        ("line-size", "CMS tax candidate Organization line is too large"),
        ("decoded-size", "CMS tax candidate Organization bytes changed"),
        ("duplicate", "CMS tax candidate Organization ID changed"),
    ],
)
async def test_runner_detects_source_replacement_after_initial_hash_read(tmp_path, monkeypatch, change, error):
    organization_map = {**_source_organization(1), "padding": "x" * 100}
    directory, _receipt = _admitted_release(tmp_path, [organization_map, organization_map])
    source_path = directory / "01-Organization.ndjson.zst"
    original_hash = runner._file_sha256(source_path)
    raw = json.dumps(organization_map).encode() + b"\n"
    if change == "resource":
        replacement = b'{"resourceType":"Patient","id":"synthetic-1"}\n'
    elif change == "line-size":
        replacement = b"x" * (runner.MAX_RESOURCE_LINE_BYTES + 1)
    elif change == "decoded-size":
        replacement = raw * 2 + b" \n"
    else:
        replacement = raw + json.dumps({**organization_map, "name": "Different"}).encode() + b"\n"
    file_hash = runner._file_sha256
    replaced_paths = set()

    def replace_after_read(path):
        digest = file_hash(path)
        if path == source_path and path not in replaced_paths:
            replaced_paths.add(path)
            assert digest == original_hash
            with runner.zstd.open(source_path, "wb") as output:
                output.write(replacement)
        return digest

    monkeypatch.setattr(runner, "_file_sha256", replace_after_read)
    output = tmp_path / "report.json"
    with pytest.raises(ValueError, match=f"^{error}$"):
        await _run_candidate_report(monkeypatch, directory, output)
    assert replaced_paths == {source_path} and file_hash(source_path) != original_hash
    assert not output.exists() and not list(tmp_path.glob(".cms-tax-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["seen-create", "output-open"])
async def test_runner_workspace_io_failure_closes_original_descriptor_and_removes_partial_files(
    tmp_path, monkeypatch, failure
):
    directory, _receipt = _admitted_release(tmp_path, [_source_organization(1)])
    mkstemp = runner.tempfile.mkstemp
    descriptors = []

    def create_temporary(*args, **kwargs):
        if failure == "seen-create" and descriptors:
            raise OSError("synthetic workspace failure")
        descriptor, name = mkstemp(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor, name

    monkeypatch.setattr(runner.tempfile, "mkstemp", create_temporary)
    if failure == "output-open":
        monkeypatch.setattr(runner.os, "fdopen", Mock(side_effect=OSError("synthetic workspace failure")))
    output = tmp_path / "report.json"
    with pytest.raises(OSError, match="^synthetic workspace failure$"):
        await _run_candidate_report(monkeypatch, directory, output)
    assert not output.exists() and not list(tmp_path.glob(".cms-tax-*"))
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.asyncio
async def test_runner_requires_text_cutoff_even_when_datetime_can_be_normalized(tmp_path):
    directory, _receipt = _admitted_release(tmp_path, [_source_organization(1)])
    output = tmp_path / "report.json"
    with pytest.raises(ValueError, match="^CMS tax candidate extraction cutoff is invalid$"):
        await _run_without_database(directory, output, cutoff=VALIDATION_TIME)
    assert not output.exists() and not list(tmp_path.glob(".cms-tax-*"))


async def _completed_followup(tmp_path, monkeypatch, *, npi=True):
    directory, receipt = _admitted_release(tmp_path, [_source_organization(1, npi=npi)])
    fhir = _Fhir()
    monkeypatch.setattr(followup, "current_sealed_v4_tax_pin", AsyncMock(return_value=("snapshot-a", 17, "b" * 64)))
    monkeypatch.setattr(
        runner,
        "lookup_pinned_tax_candidates",
        AsyncMock(
            side_effect=lambda _session, **kwargs: {
                value: report.CmsNpiTaxCandidate((5,), 1, 0) for value in kwargs["npis"]
            }
        ),
    )
    arguments_by_name = {
        "release_directory": directory,
        "dataset_id": "dataset-a",
        "vector_sha256": receipt["vector_sha256"],
        "generated_at": receipt["generated_at"],
    }
    result = await followup.cms_npd_tax_candidate_followup(fhir, **arguments_by_name)
    assert result["status"] == ("complete" if npi else "empty") and result["retryable"] is False
    report_path = next((directory / "tax-candidates").glob("[0-9a-f]" * 64 + ".json"))
    return fhir, arguments_by_name, result, report_path


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["receipt-shape", "extra-field", "count", "digest", "receipt-json"])
async def test_followup_rejects_altered_completion_receipt_without_rewriting_artifacts(tmp_path, monkeypatch, change):
    fhir, arguments, _result, path = await _completed_followup(tmp_path, monkeypatch)
    receipt_path = followup._receipt_path(path)
    retained_receipt_map = json.loads(receipt_path.read_bytes())
    if change == "extra-field":
        retained_receipt_map["extra"] = True
    elif change == "count":
        retained_receipt_map["candidate_organization_count"] = True
    elif change == "digest":
        retained_receipt_map["report_sha256"] = "invalid"
    encoded_receipt = b"[]" if change == "receipt-shape" else json.dumps(retained_receipt_map).encode()
    receipt_path.write_bytes(b"{" if change == "receipt-json" else encoded_receipt)
    before = (path.read_bytes(), receipt_path.read_bytes())
    run_report = AsyncMock()
    monkeypatch.setattr(followup, "run_admitted_cms_tax_candidate_report", run_report)
    assert await followup.completed_cms_tax_candidate_report(fhir, **arguments) is None
    assert (path.read_bytes(), receipt_path.read_bytes()) == before
    run_report.assert_not_awaited()
    assert not list(path.parent.glob(".cms-tax-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["vector", "date", "pin"])
async def test_completed_followup_cannot_borrow_other_release_date_or_missing_tax_pin(tmp_path, monkeypatch, change):
    fhir, arguments, _result, path = await _completed_followup(tmp_path, monkeypatch)
    if change == "vector":
        arguments["vector_sha256"] = "f" * 64
    elif change == "date":
        arguments["generated_at"] = arguments["generated_at"].replace("-", "")
    pin = AsyncMock(return_value=None)
    monkeypatch.setattr(followup, "current_sealed_v4_tax_pin", pin)
    original = path.read_bytes()
    assert await followup.completed_cms_tax_candidate_report(fhir, **arguments) is None
    assert path.read_bytes() == original
    if change != "pin":
        pin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["unsafe-directory", "directory-link", "vector", "date"])
async def test_followup_failure_preserves_acquired_source_and_never_writes_a_report(tmp_path, monkeypatch, change):
    directory, receipt = _admitted_release(tmp_path, [_source_organization(1)])
    source_path = directory / "01-Organization.ndjson.zst"
    original = source_path.read_bytes()
    arguments_by_name = {
        "release_directory": directory,
        "dataset_id": "dataset-a",
        "vector_sha256": receipt["vector_sha256"],
        "generated_at": receipt["generated_at"],
    }
    if change == "unsafe-directory":
        (directory / "tax-candidates").mkdir(mode=0o755)
        (directory / "tax-candidates").chmod(0o755)
    elif change == "directory-link":
        (directory / "tax-candidates").symlink_to(tmp_path, target_is_directory=True)
    elif change == "vector":
        arguments_by_name["vector_sha256"] = "f" * 64
    else:
        arguments_by_name["generated_at"] = arguments_by_name["generated_at"].replace("-", "")
    pin = AsyncMock(return_value=("snapshot-a", 17, "b" * 64))
    run_report = AsyncMock()
    monkeypatch.setattr(followup, "current_sealed_v4_tax_pin", pin)
    monkeypatch.setattr(followup, "run_admitted_cms_tax_candidate_report", run_report)
    assert await followup.cms_npd_tax_candidate_followup(_Fhir(), **arguments_by_name) == {
        "status": "failed",
        "retryable": True,
        "retry_via": "same_byte_import",
    }
    assert source_path.read_bytes() == original
    run_report.assert_not_awaited()
    if change in {"vector", "date"}:
        pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_followup_retains_verifiable_empty_completion_instead_of_retrying(tmp_path, monkeypatch):
    fhir, arguments, result, path = await _completed_followup(tmp_path, monkeypatch, npi=False)
    assert result["candidate_organization_count"] == 0
    assert json.loads(path.read_bytes())["organizations"] == []
    assert await followup.completed_cms_tax_candidate_report(fhir, **arguments) == result


@pytest.mark.asyncio
@pytest.mark.parametrize("after", [None, "site-a"])
async def test_doctors_site_page_keeps_exact_locked_bounded_sql_cursor(monkeypatch, after):
    table = Table("group_stage", MetaData(), Column("adrs_id", String), schema="mrf")
    events = []

    async def read(_statement):
        events.append("read")
        return SimpleNamespace(all=lambda: ["site-b", "site-c"])

    session = SimpleNamespace(scalars=AsyncMock(side_effect=read))

    @asynccontextmanager
    async def transaction():
        yield session

    lock = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("lock"))
    monkeypatch.setattr(sites, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(sites, "_lock_group_stage", lock)
    assert await sites._read_site_page(table, 41, after) == ["site-b", "site-c"]
    assert events == ["lock", "read"]
    lock.assert_awaited_once_with(session, table, 41, identifier_column="adrs_id")
    statement = session.scalars.await_args.args[0]
    sql = str(statement)
    assert "SELECT DISTINCT" in sql and "adrs_id IS NOT NULL" in sql and "adrs_id !=" in sql
    assert "ORDER BY" in sql and "LIMIT" in sql
    assert ("adrs_id >" in sql) is (after is not None)
    assert statement.compile().params["param_1"] == sites.GROUP_BINDING_BATCH_SIZE
    if after is not None:
        assert after in statement.compile().params.values()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [None, "site-unbound"])
async def test_doctors_site_completeness_rechecks_owned_source_and_exact_bindings(monkeypatch, missing):
    table = Table("group_stage", MetaData(), Column("adrs_id", String), schema="mrf")
    events = []

    async def read(_statement):
        events.append("read")
        return missing

    session = SimpleNamespace(scalar=AsyncMock(side_effect=read))

    @asynccontextmanager
    async def transaction():
        yield session

    lock = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("lock"))
    validate = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("validate"))
    monkeypatch.setattr(sites, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(sites, "_lock_group_stage", lock)
    monkeypatch.setattr(sites, "validate_group_site_stage", validate)
    if missing:
        with pytest.raises(RuntimeError, match="^cms_group_site_site_binding_incomplete$"):
            await sites._require_complete_site_bindings(table, 41, "20260730", "mrf", {})
    else:
        await sites._require_complete_site_bindings(table, 41, "20260730", "mrf", {})
    assert events == ["lock", "validate", "read"]
    lock.assert_awaited_once_with(session, table, 41, identifier_column="adrs_id")
    validate.assert_awaited_once_with("20260730", "mrf", {})
    sql = str(session.scalar.await_args.args[0])
    assert "NOT (EXISTS" in sql and 'COLLATE "C"' in sql and "LIMIT" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "adrs_ids",
    [
        "site-a",
        [],
        ["site-a"] * 101,
        ["site-a", None],
        ["site-a", ""],
        ["site-a", " site-b"],
        ["site-a", "site-b "],
        ["site-a", "x" * 257],
    ],
)
async def test_doctors_site_identity_rejects_invalid_batches_before_any_database_use(adrs_ids):
    session = SimpleNamespace(execute=AsyncMock())
    error = (
        "cms_doctors_site_batch_invalid"
        if not isinstance(adrs_ids, list) or not adrs_ids or len(adrs_ids) > 100
        else "cms_doctors_site_adrs_id_invalid"
    )
    with pytest.raises(ValueError, match=f"^{error}$"):
        await entity_identity.bind_cms_doctors_site_batch(session, adrs_ids=adrs_ids)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [(), ("site-a",), ("site-a", "site-b")])
async def test_doctors_site_identity_reuses_exact_ids_and_inserts_missing_sites(existing):
    prior_ids_by_site = {"site-a": UUID(int=1), "site-b": UUID(int=2)}
    writes = []

    async def execute(statement, params=None):
        if getattr(statement, "is_select", False):
            return SimpleNamespace(all=lambda: [(key, prior_ids_by_site[key]) for key in existing])
        if getattr(statement, "is_insert", False):
            writes.append(statement)
        return SimpleNamespace()

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    ids = await entity_identity.bind_cms_doctors_site_batch(session, adrs_ids=["site-a", "site-b", "site-a"])
    assert len(ids) == 3 and ids[0] == ids[2] and ids[0] != ids[1]
    assert all(isinstance(site_id, UUID) for site_id in ids)
    for key in existing:
        assert ids[0 if key == "site-a" else 1] == prior_ids_by_site[key]
    if len(existing) == 2:
        assert writes == []
    else:
        assert len(writes) == 2
        binding = next(
            statement for statement in writes if statement.table.name == "provider_directory_cms_doctors_site_binding"
        )
        binding_params_by_name = binding.compile().params
        inserted_ids_by_site = {
            adrs_id: binding_params_by_name["site_id" + key.removeprefix("adrs_id")]
            for key, adrs_id in binding_params_by_name.items()
            if key.startswith("adrs_id")
        }
        returned_ids_by_site = {"site-a": ids[0], "site-b": ids[1]}
        assert inserted_ids_by_site == {
            key: site_id for key, site_id in returned_ids_by_site.items() if key not in existing
        }
        assert len(set(inserted_ids_by_site.values())) == len(inserted_ids_by_site)
        identity = next(statement for statement in writes if statement.table.name == "provider_directory_site_identity")
        identity_ids = {site_id for key, site_id in identity.compile().params.items() if key.startswith("site_id")}
        assert identity_ids == set(inserted_ids_by_site.values())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["missing-owner", "empty", "endpoint", "metric-source", "metric-endpoint", "prior-incumbent"]
)
async def test_candidate_catalog_keeps_exact_owner_endpoint_and_refreshed_incumbent(monkeypatch, change):
    dataset = _dataset()
    owner = _catalog_run("owner", dataset)
    session = _CatalogSession(dataset, [] if change == "missing-owner" else [owner])
    if change == "empty":
        session.datasets = []
    if change in {"endpoint", "metric-source", "metric-endpoint", "prior-incumbent"}:
        incumbent_map = {
            **deepcopy(dataset),
            "dataset_id": "incumbent",
            "acquisition_root_run_id": "prior",
            "status": "published",
            "is_current": True,
            "published_at": NOW,
        }
        session.datasets.append(incumbent_map)
        if change == "endpoint":
            incumbent_map["endpoint_id"] = "other-endpoint"
        if change in {"metric-source", "metric-endpoint", "prior-incumbent"}:
            previous_map = {**_pair(incumbent_map), "dataset_id": "previously-recorded"}
            if change == "metric-source":
                previous_map["source_id"] = "other-source"
            if change == "metric-endpoint":
                previous_map["endpoint_id"] = "other-endpoint"
            owner["candidate"]["expected_cms_incumbent"] = previous_map
    original = deepcopy((dataset, owner, session.datasets))
    _install_catalog(monkeypatch, session)
    if change == "empty":
        acquisition = AsyncMock(wraps=candidate_catalog._acquisition_leaf)
        monkeypatch.setattr(candidate_catalog, "_acquisition_leaf", acquisition)
        assert await candidate_catalog.cms_serving_candidate(CATALOG) is None
        acquisition.assert_not_awaited()
    elif change == "prior-incumbent":
        candidate_descriptor = await candidate_catalog.cms_serving_candidate(CATALOG)
        assert candidate_descriptor["expected_cms_incumbent"] == _pair(incumbent_map)
        assert candidate_descriptor["expected_cms_incumbent"]["dataset_id"] != previous_map["dataset_id"]
        assert candidate_descriptor["acquisition_run_id"] == "owner"
    else:
        error = (
            "cms_candidate_acquisition_missing"
            if change == "missing-owner"
            else "cms_candidate_endpoint_changed"
            if change == "endpoint"
            else "cms_candidate_metric_incumbent_invalid"
        )
        with pytest.raises(ValueError, match=f"^{error}$"):
            await candidate_catalog.cms_serving_candidate(CATALOG)
    assert (dataset, owner, session.datasets) == original
    assert not any("INSERT" in statement or "UPDATE" in statement for statement in session.statements)


def test_cms_request_rejects_noncanonical_python_sequence_without_bypassing_signed_pair():
    request = cms_request()
    body = request[cms_contract.CMS_ADMISSION_FIELD]["paired_profile_lease"]["lease"]
    body["tablespaces"] = tuple(body["tablespaces"])
    original = deepcopy(request)
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="^provider_directory_cms_capacity_request_not_canonical$",
    ):
        preflight.validated_capacity_preflight_request(request)
    assert request == original


def _doctors_family():
    native = preparation._native()
    stages = tuple(
        (model.__tablename__, native.make_class(model, "20260730").__tablename__, 41 + index)
        for index, model in enumerate(preparation._models())
    )
    incumbent = archive.ReferenceFamilyIncumbent(
        "cms-doctors", "mrf", tuple((target, 11 + index) for index, (target, _stage, _oid) in enumerate(stages))
    )
    serving = generation.ReferenceFamilyServingGeneration("00000000-0000-0000-0000-000000000001", 1, VALIDATION_TIME)
    authority = generation.ReferenceFamilyResultGenerationAuthority(
        "cms-doctors", serving.origin_lineage_id, 1, serving, tuple(oid for _name, oid in incumbent.relation_oids)
    )
    prepared = preparation.PreparedCMSDoctorsGeneration("mrf", "20260730", stages, incumbent, authority, {}, {})
    prepared.sealed_filenodes = tuple((oid, 1000 + oid) for _target, _stage, oid in stages)
    return native, prepared


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["indexes", "result"])
async def test_doctors_apply_rejects_catalog_or_result_drift_without_consuming_preparation(monkeypatch, failure):
    original_native, prepared = _doctors_family()
    relation_oids_by_name = {f"mrf.{stage}": oid for _target, stage, oid in prepared.stage_oids}

    async def execute(statement, params):
        assert "SELECT oid::bigint,relpersistence" in str(statement)
        oid = relation_oids_by_name[params["relation"]]
        return SimpleNamespace(one_or_none=lambda: (oid, "p", 1000 + oid))

    async def scalar(statement, params):
        sql = str(statement)
        if "SELECT count(*) FROM pg_index" in sql:
            return max(0, len(params["names"]) - (failure == "indexes"))
        if "SELECT p.oid::bigint FROM pg_proc" in sql:
            return 71
        assert "SELECT EXISTS(SELECT 1 FROM pg_trigger" in sql
        return True

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute), scalar=AsyncMock(side_effect=scalar))
    expected_oids = tuple(oid for _target, _stage, oid in prepared.stage_oids)
    returned = replace(
        prepared.incumbent_authority,
        local_generation=2,
        serving_generation=replace(prepared.incumbent_authority.serving_generation, origin_generation=2),
        relation_oids=(*expected_oids[:-1], expected_oids[-1] + 1),
    )
    apply = AsyncMock(return_value=returned)
    native = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session), status=AsyncMock()),
        make_class=original_native.make_class,
        _stage_index_name=original_native._stage_index_name,
        DoctorClinicianAddress=original_native.DoctorClinicianAddress,
        lock_live_serving_relations=AsyncMock(),
        _apply_cms_doctors_stage=apply,
    )
    monkeypatch.setattr(preparation, "_native", lambda: native)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive, "_verify_incumbent", AsyncMock())
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=prepared.incumbent_authority),
    )
    error = "cms_doctors_prepared_indexes_missing" if failure == "indexes" else "cms_doctors_preparation_result_changed"
    with pytest.raises(RuntimeError, match=f"^{error}$"):
        await preparation.apply_prepared_cms_doctors_generation(prepared)
    assert not prepared.committed and prepared.context == {} and prepared.metrics == {}
    if failure == "result":
        apply.assert_awaited_once()
        assert prepared.native_receipt == returned
    else:
        apply.assert_not_awaited()
        assert prepared.native_receipt is None
    assert all(str(call.args[0]).lstrip().startswith("SELECT") for call in session.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["capture-authority", "active-cleanup"])
async def test_doctors_preparation_fences_authority_and_defers_active_cleanup(monkeypatch, failure):
    """Changed authority stops preparation; owned cleanup waits for the outer transaction."""
    original_native, expected = _doctors_family()
    transaction_bindings = [None]
    session = object()

    @asynccontextmanager
    async def transaction():
        yield session

    prepare_sources = AsyncMock(return_value={"rows": 1})
    native = SimpleNamespace(
        db=SimpleNamespace(
            _transaction_binding=lambda: transaction_bindings[0],
            transaction=transaction,
            scalar=AsyncMock(return_value=1),
        ),
        ensure_database=AsyncMock(),
        make_class=original_native.make_class,
        DoctorClinicianAddress=original_native.DoctorClinicianAddress,
        _prepare_cms_doctors_sources=prepare_sources,
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(preparation, "_native", lambda: native)
    monkeypatch.setattr(preparation, "_stage_inventory", AsyncMock(return_value=expected.stage_oids))
    monkeypatch.setattr(archive, "capture_reference_family_incumbent", AsyncMock(return_value=expected.incumbent))
    observed = expected.incumbent_authority
    if failure == "capture-authority":
        observed = replace(observed, relation_oids=(21, 22, 23))
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=observed)
    )
    monkeypatch.setattr(preparation, "_finalize_stages", AsyncMock())
    monkeypatch.setattr(preparation, "_seal_finalized_stages", AsyncMock())
    drop = AsyncMock()
    monkeypatch.setattr(preparation, "_drop_owned_stages", drop)
    context_by_name = {
        "import_date": "20260730",
        "context": {"run": True, "education_stage_owned": True, "group_site_stage_owned": True},
    }
    if failure == "capture-authority":
        with pytest.raises(RuntimeError, match="^cms_doctors_incumbent_authority_changed$"):
            async with preparation.prepare_cms_doctors_generation(context_by_name):
                pytest.fail("changed native authority must not yield prepared tables")
        prepare_sources.assert_not_awaited()
        drop.assert_awaited_once_with("mrf", expected.stage_oids)
        assert context_by_name["context"] == {"run": True}
    else:
        try:
            with pytest.raises(RuntimeError, match="^cms_doctors_preparation_cleanup_transaction_active$"):
                async with preparation.prepare_cms_doctors_generation(context_by_name) as prepared:
                    assert prepared.context["publication_state"] == "prepared" and not prepared.committed
                    transaction_bindings[0] = SimpleNamespace(session=session)
        finally:
            transaction_bindings[0] = None
        drop.assert_not_awaited()
        assert not prepared.committed
        await preparation.cleanup_prepared_cms_doctors(prepared)
        drop.assert_awaited_once_with("mrf", expected.stage_oids)
