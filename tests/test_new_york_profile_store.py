# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import new_york_nysed_profile_acquisition as nysed_acquisition
from process import new_york_profile_binding as binding
from process import new_york_profile_store as module
from process import provider_profile_source_store as shared
from process.new_york_profile_registry import build_acquisition_cohort
from process.new_york_profile_retained import read_held_acquisition
from tests.test_new_york_nysed_profile import (
    PUBLIC_HEADER,
    _profile_body,
)
from tests.test_new_york_nysed_profile import (
    SourceResponse as NysedResponse,
)
from tests.test_new_york_nysed_profile import (
    SourceSession as NysedSession,
)
from tests.test_new_york_profile_acquisition import SourceResponse, _acquire, _education, _search, source_session
from tests.test_new_york_profile_binding import _candidate, _save_snapshot, _snapshot

RUN_ID = "a" * 32


def _artifact(run, cohort, profiles, metrics):
    bundle_by_field = {
        **(
            {"bundle_contract": run["source_manifest"]["bundle_contract"]}
            if "bundle_contract" in run["source_manifest"]
            else {}
        ),
        "schema_version": module.SCHEMA_VERSION,
        "run_id": run["run_id"],
        "source_manifest": run["source_manifest"],
        "cohort": cohort,
        "profiles": profiles,
        "acquisition": metrics,
    }
    return {
        "artifact_id": module._hash([run["run_id"], module.SOURCE_KEY]),
        "run_id": run["run_id"],
        "source_key": module.SOURCE_KEY,
        "category": "profile",
        "file_name": "manifest.json",
        "source_url": module.SOURCE_URL,
        "content_sha256": module._hash(bundle_by_field),
        "content_bytes": len(module.encoded_json(bundle_by_field)),
        "header": None,
        "downloaded_at": None,
        "metadata_json": bundle_by_field,
    }


def source_run(cohort, snapshot_sha256, row_count):
    manifest_by_field = {
        "control_run_id": "synthetic-control-run",
        "expected_current_run_id": None,
        "max_providers": None,
        "resume_from": None,
        "categories": list(module.CATEGORIES),
        "snapshot_sha256": snapshot_sha256,
        "snapshot_row_count": row_count,
        "cohort_sha256": module._hash(cohort),
        "full_cohort_licenses": 3,
        "requested_licenses": 3,
        "source": {
            "source_key": module.SOURCE_KEY,
            "source_kind": "state_regulator",
            "jurisdiction": "NY",
            "agency": "New York State Department of Health",
            "source_url": module.SOURCE_URL,
            "coverage_scope": module.COVERAGE_SCOPE,
            "registry_generation": snapshot_sha256,
        },
    }
    run_by_field = {
        "run_id": RUN_ID,
        "source_key": module.SOURCE_KEY,
        "schema_version": module.SCHEMA_VERSION,
        "jurisdiction": "NY",
        "status": "running",
        "started_at": shared._now(),
        "source_manifest": manifest_by_field,
    }
    return run_by_field


async def _capture_nysed_support(tmp_path, monkeypatch, license_number):
    destination = tmp_path / ("nysed-" + license_number)
    is_held = license_number == "222222"
    response = (
        NysedResponse(raw=b"", status=204, headers=[])
        if is_held
        else NysedResponse(_profile_body(license_number, name="EXAMPLE ALEX"))
    )
    session = NysedSession(response)
    monkeypatch.setattr(nysed_acquisition.aiohttp, "ClientSession", lambda **_options: session)
    acquired = await nysed_acquisition.acquire_license(
        license_number, destination, run_id=RUN_ID, api_key=PUBLIC_HEADER
    )
    reader = module.nysed.read_held_acquisition if is_held else module.nysed.read_acquisition
    replayed = reader(destination, receipt_sha256=acquired["receipt_sha256"])
    assert session.closed and replayed == acquired
    identity_by_field = None
    if not is_held:
        nysed_record = replayed["source_record"]
        identity_by_field = {
            "source_record_id": nysed_record["record_id"],
            "artifact_id": nysed_record["artifact_id"],
            **{
                field: replayed["facts"][0]["source_json"][field]
                for field in (
                    "source_key",
                    "source_url",
                    "content_sha256",
                    "downloaded_at",
                )
            },
            "profession_code": nysed_record["profession_code"],
            "license_number": nysed_record["license_number"],
            "legal_name": nysed_record["raw_payload"]["name"]["value"],
        }
    return destination, {
        "capture_manifest": json.loads((destination / "manifest.json").read_bytes()),
        "file_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in destination.iterdir()},
        "receipt": json.loads((destination / "result.json").read_bytes()),
        "receipt_sha256": acquired["receipt_sha256"],
        "source_identity": identity_by_field,
    }


async def _capture_profile(tmp_path, source_session, snapshot, root, monkeypatch):
    license_number = root["license_number"]
    is_held = license_number == "333333"
    responses = (
        [SourceResponse(_search(total=0))]
        if is_held
        else [SourceResponse(_search()), SourceResponse(_education(license_number))]
    )
    session = source_session(*responses)
    session.destination = tmp_path / license_number
    await _acquire(session, license_number, run_id=RUN_ID)
    file_hashes_by_name = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in session.destination.iterdir()
    }
    capture_manifest = json.loads((session.destination / "manifest.json").read_bytes())
    options_by_field = {
        "manifest_sha256": file_hashes_by_name["manifest.json"],
        "acquisition_sha256": module._hash(file_hashes_by_name),
    }
    support = None
    if is_held:
        bound = read_held_acquisition(session.destination, **options_by_field)
    else:
        destination, support = await _capture_nysed_support(tmp_path, monkeypatch, license_number)
        bound = (
            snapshot.bind_retained_acquisition(session.destination, **options_by_field)
            if support["source_identity"] is None
            else snapshot.bind_corroborated_acquisition(
                session.destination,
                **options_by_field,
                nysed_destination=destination,
                nysed_receipt_sha256=support["receipt_sha256"],
            )
        )
    descriptor, source_record, captured_facts = module.prepare_profile(
        RUN_ID, root, bound, capture_manifest=capture_manifest, file_sha256=file_hashes_by_name
    )
    return (root, bound, capture_manifest, file_hashes_by_name), descriptor, source_record, captured_facts, support


@pytest.fixture
async def captured_case(tmp_path, source_session, monkeypatch):
    candidates = [_candidate(license_number=license_number) for license_number in ("111111", "222222", "333333")]
    candidates.append(_candidate(license_number="222222", entity_type_code=2))
    saved = _save_snapshot(tmp_path, _snapshot(candidates))
    cohort = build_acquisition_cohort(**saved)
    snapshot = binding.RegistrySnapshot(
        **{("path" if key == "snapshot_path" else key): val for key, val in saved.items()}
    )
    run_by_field = source_run(cohort, saved["snapshot_sha256"], len(candidates))
    profiles, source_records, facts, inputs, nysed_support = {}, [], [], [], {}
    for root in cohort["roots"]:
        original_input, descriptor, source_record, captured_facts, support = await _capture_profile(
            tmp_path, source_session, snapshot, root, monkeypatch
        )
        inputs.append(original_input)
        license_number = root["license_number"]
        profiles[license_number] = descriptor
        if source_record is not None:
            nysed_support[license_number] = support
            source_records.append(source_record)
            facts.extend(captured_facts)
    metrics_by_field = {
        "acquisition_complete": True,
        "transport_failures": 0,
        "responses": 3,
        "acquired_profiles": 2,
        "held_attempts": 1,
        "facts": 2,
        "cohort_sha256": run_by_field["source_manifest"]["cohort_sha256"],
        "nysed_support": nysed_support,
    }
    return SimpleNamespace(
        run=run_by_field,
        cohort=cohort,
        profiles=profiles,
        records=source_records,
        facts=facts,
        metrics=metrics_by_field,
        artifact=_artifact(run_by_field, cohort, profiles, metrics_by_field),
        inputs=inputs,
    )


@pytest.fixture
def witnessed_case(captured_case):
    """Reuse genuine retained captures without replacing their canonical row digests."""
    case = copy.deepcopy(captured_case)
    case.run["source_manifest"]["bundle_contract"] = module.WITNESS_CONTRACT
    records_by_license = {record["license_number"]: record for record in case.records}
    case.profiles = {
        license_number: module.witnessed_profile(
            descriptor,
            records_by_license.get(license_number),
            [fact for fact in case.facts if fact["source_record_id"] == descriptor["record_id"]],
        )
        for license_number, descriptor in case.profiles.items()
    }
    case.artifact = _artifact(case.run, case.cohort, case.profiles, case.metrics)
    case.compact_metrics = {
        **{key: value for key, value in case.metrics.items() if key != "nysed_support"},
        "bundle": module.bundle_reference(case.artifact),
    }
    return case


def _counts():
    return {
        "retained_source_records": 2,
        "received_profiles": 2,
        "retained_facts": 2,
        "matched_public_providers": 1,
        "portfolio_only_public_providers": 0,
        "invalid_source_records": 0,
        "invalid_facts": 0,
        "foreign_artifacts": 0,
    }


def _legacy_connection():
    raw = SimpleNamespace(driver_connection=SimpleNamespace(is_closed=lambda: False))
    return SimpleNamespace(invalidated=False, invalidate=AsyncMock(), get_raw_connection=AsyncMock(return_value=raw))


def _install_retained(monkeypatch, case, *, cancel=False):
    """Exercise transfer and transaction wiring; the native suite evaluates the generated SQL."""
    from process import reference_family_archive as native

    closed_streams, fragments_by_row = [], {}

    class Rows:
        def __init__(self, statement):
            name = statement.get_final_froms()[0].name
            self.rows = case.records if name == shared.ProviderProfileSourceRecord.__tablename__ else case.facts

        async def __aiter__(self):
            for source_record in self.rows:
                if cancel:
                    raise asyncio.CancelledError
                yield SimpleNamespace(_mapping=copy.deepcopy(source_record))

        async def close(self):
            closed_streams.append(True)

    @asynccontextmanager
    async def transaction():
        yield session

    @asynccontextmanager
    async def scratch():
        yield SimpleNamespace(rollback=AsyncMock())

    async def copy_records(_session, _model, **options):
        for record in options["records"]:
            envelope_by_field = dict(zip(options["columns"], record, strict=True))
            key = (envelope_by_field["category"], envelope_by_field["file_name"])
            fragments_by_row.setdefault(key, []).append(
                (int(envelope_by_field["header"]), envelope_by_field["source_url"])
            )

    async def evaluate(_session, _schema, _source_schema, run):
        transferred_by_kind = {"records": [], "facts": []}
        for (kind, _ordinal), fragments in fragments_by_row.items():
            if kind in transferred_by_kind:
                transferred_by_kind[kind].append(json.loads("".join(value for _part, value in sorted(fragments))))
        return _legacy_inventory_oracle(run, case.artifact["metadata_json"], transferred_by_kind, case.facts)

    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(), stream=AsyncMock(side_effect=Rows), begin_nested=scratch
    )
    session.connection = AsyncMock(return_value=_legacy_connection())
    monkeypatch.setattr(shared.db, "transaction", transaction)
    monkeypatch.setattr(native, "native_copy_record_batch", copy_records)
    monkeypatch.setattr(module, "_legacy_witness_counts", evaluate)
    monkeypatch.setattr(
        shared.SourceProfileStore, "_retained_counts_by_run", AsyncMock(return_value={RUN_ID: _counts()})
    )
    monkeypatch.setattr(type(module.store), "_read_run", AsyncMock(return_value=case.run))
    monkeypatch.setattr(type(module.store), "_lock_source", AsyncMock())
    monkeypatch.setattr(shared.db, "all", AsyncMock(return_value=[SimpleNamespace(_mapping=case.artifact)]))
    return closed_streams


def _legacy_inventory_oracle(run, bundle, transferred_by_kind, stored_facts):
    """Freeze the previous decoded-row semantics independently of the new SQL verifier."""
    records_by_id = {
        value["record_id"]: value for value in bundle["profiles"].values() if value["record_id"] is not None
    }
    facts_by_id = {key: digest for descriptor in records_by_id.values() for key, digest in descriptor["facts"].items()}
    assert len(facts_by_id) == sum(len(descriptor["facts"]) for descriptor in records_by_id.values())
    counts_by_field = {}
    for kind, identifier, expected in (
        ("records", "record_id", {key: value["record_sha256"] for key, value in records_by_id.items()}),
        ("facts", "fact_id", facts_by_id),
    ):
        seen, invalid = set(), 0
        for row in transferred_by_kind[kind]:
            key = row[identifier]
            invalid += key in seen or expected.get(key) != module._hash(row)
            if kind == "records" and key in records_by_id:
                descriptor = records_by_id[key]
                invalid += not _has_legacy_record_lineage(row, descriptor, run["source_manifest"])
                support = bundle["acquisition"]["nysed_support"][descriptor["capture_manifest"]["license_number"]]
                invalid += not _legacy_nysed_support(row, support)
            seen.add(key)
        counts_by_field["invalid_bundle_" + kind] = invalid + len(expected.keys() - seen)
    if run["status"] in shared.ACTIVE_STATUSES:
        counts_by_field["invalid_bundle_facts"] += sum(row["published_at"] is not None for row in stored_facts)
    return counts_by_field


def _has_legacy_record_lineage(record, descriptor_by_field, manifest):
    capture_by_field = {key: descriptor_by_field[key] for key in module.CAPTURE_FIELDS}
    if not isinstance(record.get("match_evidence"), dict) or not isinstance(record.get("normalized_payload"), dict):
        return False
    binding = record["match_evidence"].get("registry_binding", {})
    if not isinstance(binding, dict):
        return False
    return (
        record.get("normalized_payload", {}).get("profile_capture")
        == {**capture_by_field, "artifact_id": descriptor_by_field["capture_artifact_id"]}
        and record.get("artifact_id") == module._hash([record["run_id"], module.SOURCE_KEY])
        and record.get("license_number") == descriptor_by_field["capture_manifest"]["license_number"]
        and binding.get("snapshot_sha256") == manifest["snapshot_sha256"]
        and binding.get("manifest_sha256") == descriptor_by_field["manifest_sha256"]
        and binding.get("acquisition_sha256") == descriptor_by_field["acquisition_sha256"]
        and binding.get("status") == record.get("match_status")
        and binding.get("npi") == record.get("matched_npi")
        and binding.get("reason") == descriptor_by_field["reason"]
        and descriptor_by_field["binding_outcome"] == ("accepted" if record.get("matched_npi") is not None else "held")
    )


def _legacy_nysed_support(record, support):
    if not isinstance(record.get("match_evidence"), dict):
        return False
    binding = record["match_evidence"].get("registry_binding")
    if not isinstance(binding, dict):
        return False
    corroboration = binding.get("nysed_corroboration")
    if support["receipt"]["outcome"] in {"held", "invalid"}:
        return binding.get("method") == "exact_ny_license_name_components" and corroboration is None
    return (
        binding.get("method") == module.CORROBORATED_METHOD
        and isinstance(corroboration, dict)
        and corroboration.get("receipt_sha256") == support["receipt_sha256"]
        and all(corroboration.get(field) == support["source_identity"][field] for field in module.NYSED_IDENTITY_FIELDS)
        and all(type(corroboration.get(field)) is bool for field in ("license_matches", "header_legal_name_matches"))
        and (
            record.get("matched_npi") is None
            or corroboration["license_matches"]
            and corroboration["header_legal_name_matches"]
        )
    )


async def test_complete_mixed_inventory_retains_real_unmatched_rows_without_fake_held_profiles(
    captured_case, monkeypatch
):
    case = captured_case
    before = copy.deepcopy(case.inputs)
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    final = module.store._completion_metrics(case.run, case.metrics, counts)
    assert final["requested_licenses"] == 3 and final["received_profiles"] == 2 and final["held_attempts"] == 1
    assert [record["matched_npi"] for record in case.records] == [1000000004, None]
    assert [fact["npi"] for fact in case.facts] == [1000000004, None]
    assert case.profiles["333333"]["record_id"] is None and case.profiles["333333"]["facts"] == {}
    assert case.inputs == before
    for record, fact in zip(case.records, case.facts, strict=True):
        original = record["normalized_payload"]["profile_capture"]
        assert record["artifact_id"] == case.artifact["artifact_id"] != original["artifact_id"]
        assert fact["source_json"]["artifact_id"] == record["artifact_id"]
        assert fact["source_json"]["capture_artifact_id"] == original["artifact_id"]
    assert module.completion.store is module.store and module.completion.importer == module.IMPORTER
    supports = case.metrics["nysed_support"]
    assert set(supports) == {"111111", "222222"}
    assert supports["111111"]["receipt"]["outcome"] == "acquired"
    assert supports["222222"]["receipt"]["outcome"] == "held"
    assert supports["222222"]["source_identity"] is None
    assert PUBLIC_HEADER.encode() not in module.encoded_json(case.artifact)
    assert all(record["source_key"] == module.SOURCE_KEY for record in case.records)


async def test_completion_keeps_support_in_artifact_and_returns_only_aggregate_metrics(captured_case, monkeypatch):
    case = captured_case
    before = copy.deepcopy(case.artifact)
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    final = module.store._completion_metrics(case.run, case.metrics, counts)
    assert "nysed_support" not in final and "bundle_metrics" not in final
    assert set(case.artifact["metadata_json"]["acquisition"]["nysed_support"]) == {"111111", "222222"}
    assert case.artifact == before
    assert final["facts"] == final["retained_facts"] == 2
    modified_metrics = copy.deepcopy(case.metrics)
    modified_metrics["nysed_support"]["111111"]["receipt_sha256"] = "0" * 64
    with pytest.raises(RuntimeError, match="retained_count_mismatch"):
        module.store._completion_metrics(case.run, modified_metrics, counts)


def _refresh_artifact(case):
    case.artifact["content_sha256"] = module._hash(case.artifact["metadata_json"])
    case.artifact["content_bytes"] = len(module.encoded_json(case.artifact["metadata_json"]))


@pytest.mark.parametrize("failure", ["absent", "missing_acquired", "missing_held", "extra", "substituted"])
async def test_support_inventory_covers_acquired_profiles_exactly(captured_case, failure):
    case = captured_case
    supports = case.metrics["nysed_support"]
    if failure == "absent":
        del case.metrics["nysed_support"]
    if failure == "missing_acquired":
        del supports["111111"]
    if failure == "missing_held":
        del supports["222222"]
    if failure == "extra":
        supports["333333"] = copy.deepcopy(supports["222222"])
    if failure == "substituted":
        supports["111111"] = copy.deepcopy(supports["222222"])
    _refresh_artifact(case)
    with pytest.raises(ValueError, match="nysed_"):
        module.store._bundle(case.run, [case.artifact])


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("capture_manifest", "run_id"), "b" * 32),
        (("capture_manifest", "source_key"), module.SOURCE_KEY),
        (("capture_manifest", "profession_code"), "061"),
        (("capture_manifest", "license_number"), "999999"),
        (("file_sha256", "manifest.json"), "0" * 64),
        (("file_sha256", "request.json"), "0" * 64),
        (("file_sha256", "response.json"), "0" * 64),
        (("file_sha256", "result.json"), "0" * 64),
        (("receipt_sha256",), "0" * 64),
        (("receipt", "outcome"), "failed"),
        (("receipt", "fact_count"), 0),
        (("receipt", "fact_count"), True),
        (("receipt", "completed_at"), "2000-01-01T00:00:00+00:00"),
        (("source_identity", "source_record_id"), "0" * 64),
        (("source_identity", "artifact_id"), "0" * 64),
        (("source_identity", "source_key"), module.SOURCE_KEY),
        (("source_identity", "source_url"), module.SOURCE_URL),
        (("source_identity", "profession_code"), "061"),
        (("source_identity", "license_number"), "999999"),
        (("source_identity", "legal_name"), ""),
        (("source_identity", "downloaded_at"), "2000-01-01T00:00:00+00:00"),
        (("source_identity", "content_sha256"), "invalid"),
        (("source_identity",), None),
        (("unexpected",), "synthetic-extra"),
    ],
)
async def test_support_validates_capture_receipt_and_parser_identity(captured_case, path, replacement):
    case = captured_case
    support = case.metrics["nysed_support"]["111111"]
    parent = support if len(path) == 1 else support[path[0]]
    parent[path[-1]] = replacement
    if path[0] == "receipt":
        support["receipt_sha256"] = support["file_sha256"]["result.json"] = module._hash(support["receipt"])
    _refresh_artifact(case)
    with pytest.raises(ValueError, match="nysed_"):
        module.store._bundle(case.run, [case.artifact])


@pytest.mark.parametrize("failure", ["files_missing", "files_extra", "reason", "facts", "identity"])
async def test_empty_nysed_support_keeps_exact_four_files_without_source_identity(captured_case, failure):
    case = captured_case
    support = case.metrics["nysed_support"]["222222"]
    if failure == "files_missing":
        del support["file_sha256"]["response.json"]
    if failure == "files_extra":
        support["file_sha256"]["extra.json"] = "0" * 64
    if failure == "reason":
        support["receipt"]["reason"] = "transport_error"
    if failure == "facts":
        support["receipt"]["fact_count"] = 1
    if failure == "identity":
        support["source_identity"] = copy.deepcopy(case.metrics["nysed_support"]["111111"]["source_identity"])
    support["receipt_sha256"] = support["file_sha256"]["result.json"] = module._hash(support["receipt"])
    _refresh_artifact(case)
    with pytest.raises(ValueError, match="nysed_"):
        module.store._bundle(case.run, [case.artifact])


def _substitute_corroboration_receipt(case):
    case.records[0]["match_evidence"]["registry_binding"]["nysed_corroboration"]["receipt_sha256"] = "0" * 64
    case.profiles["111111"]["record_sha256"] = module._hash(case.records[0])
    _refresh_artifact(case)


@pytest.mark.parametrize(
    "field,replacement",
    [(field, "0" * 64) for field in ("receipt_sha256", *module.NYSED_IDENTITY_FIELDS)]
    + [("license_matches", False), ("header_legal_name_matches", False), ("license_matches", 1)],
)
async def test_sql_corroboration_must_match_support_even_with_rehashed_row(
    captured_case, monkeypatch, field, replacement
):
    case = captured_case
    case.records[0]["match_evidence"]["registry_binding"]["nysed_corroboration"][field] = replacement
    case.profiles["111111"]["record_sha256"] = module._hash(case.records[0])
    _refresh_artifact(case)
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    assert counts["invalid_bundle_records"] == 1
    with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
        module.store._completion_metrics(case.run, case.metrics, counts)


@pytest.mark.parametrize("failure", ["absent", "strict", "held_corroboration", "held_method"])
async def test_support_outcome_controls_binding_method(captured_case, monkeypatch, failure):
    case = captured_case
    record = case.records[1 if failure.startswith("held") else 0]
    evidence = record["match_evidence"]["registry_binding"]
    if failure == "absent":
        del evidence["nysed_corroboration"]
    if failure == "strict":
        evidence["method"] = "exact_ny_license_name_components"
    if failure == "held_corroboration":
        evidence["nysed_corroboration"] = copy.deepcopy(
            case.records[0]["match_evidence"]["registry_binding"]["nysed_corroboration"]
        )
    if failure == "held_method":
        evidence["method"] = binding.CORROBORATED_METHOD
    case.profiles[record["license_number"]]["record_sha256"] = module._hash(record)
    _refresh_artifact(case)
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    assert counts["invalid_bundle_records"] == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"max_providers": 1},
        {"resume_from": "b" * 32},
        {"requested_licenses": 2},
        {"categories": ["education"]},
        {"control_run_id": ""},
        {"snapshot_sha256": "invalid"},
        {"snapshot_row_count": True},
        {"snapshot_row_count": 2},
        {"source": {}},
        {"extra": True},
    ],
)
async def test_manifest_freezes_full_supported_source_scope(captured_case, changes):
    captured_case.run["source_manifest"].update(changes)
    with pytest.raises(ValueError):
        module.store._manifest(captured_case.run)


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "extra",
        "capture_pin",
        "session",
        "held_total",
        "held_record",
        "duplicate_index",
        "count",
        "bundle_hash",
    ],
)
async def test_bundle_requires_every_exact_capture_and_full_cohort(captured_case, failure):
    case = captured_case
    bundle = case.artifact["metadata_json"]
    if failure == "missing":
        del bundle["profiles"]["333333"]
    if failure == "extra":
        bundle["profiles"]["444444"] = copy.deepcopy(bundle["profiles"]["333333"])
    if failure == "capture_pin":
        bundle["profiles"]["111111"]["file_sha256"]["search.response.json"] = "0" * 64
    if failure == "session":
        bundle["profiles"]["111111"]["capture_manifest"]["session_id"] = "0" * 32
    if failure == "held_total":
        bundle["profiles"]["333333"]["reported_total"] = 1
    if failure == "held_record":
        bundle["profiles"]["333333"]["record_id"] = "0" * 64
    if failure == "duplicate_index":
        bundle["cohort"]["roots"][1]["registry_occurrence_indexes"] = [0]
        bundle["source_manifest"]["cohort_sha256"] = module._hash(bundle["cohort"])
    if failure == "count":
        bundle["cohort"]["summary"]["selected_row_count"] -= 1
        bundle["source_manifest"]["cohort_sha256"] = module._hash(bundle["cohort"])
    case.artifact["content_sha256"] = "0" * 64 if failure == "bundle_hash" else module._hash(bundle)
    case.artifact["content_bytes"] = len(module.encoded_json(bundle))
    with pytest.raises(ValueError):
        module.store._bundle(case.run, [case.artifact])


@pytest.mark.parametrize(
    "kind,field,replacement",
    [
        ("record", "raw_payload", {}),
        ("record", "normalized_payload", {}),
        ("record", "matched_npi", None),
        ("record", "license_number", "999999"),
        ("record", "match_evidence", {}),
        ("record", "artifact_id", "0" * 64),
        ("fact", "value_json", {}),
        ("fact", "source_json", {}),
        ("fact", "npi", None),
        ("fact", "source_record_id", "0" * 64),
        ("fact", "published_at", datetime(2026, 1, 1)),
    ],
)
async def test_actual_retained_content_must_match_bundle_hashes(captured_case, monkeypatch, kind, field, replacement):
    case = captured_case
    (case.records if kind == "record" else case.facts)[0][field] = replacement
    closed = _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    assert counts["invalid_bundle_records" if kind == "record" else "invalid_bundle_facts"] > 0
    with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
        module.store._completion_metrics(case.run, case.metrics, counts)
    assert len(closed) == 2


@pytest.mark.parametrize("kind", ["record", "fact", "duplicate_fact", "fake_held_record"])
async def test_missing_or_extra_sql_inventory_is_rejected(captured_case, monkeypatch, kind):
    case = captured_case
    if kind == "record":
        case.records.pop()
    if kind == "fact":
        case.facts.pop()
    if kind == "duplicate_fact":
        case.facts.append(copy.deepcopy(case.facts[0]))
    if kind == "fake_held_record":
        case.records.append({**case.records[0], "record_id": "0" * 64, "license_number": "333333"})
    _install_retained(monkeypatch, case)
    with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
        module.store._completion_metrics(case.run, case.metrics, await module.store.retained_counts(RUN_ID))


@pytest.mark.parametrize(
    "changes",
    [
        {"acquisition_complete": False},
        {"transport_failures": 1},
        {"transport_failures": False},
        {"responses": 2},
        {"responses": True},
        {"acquired_profiles": 1},
        {"held_attempts": 0},
        {"facts": 1},
        {"cohort_sha256": "0" * 64},
    ],
)
async def test_declared_metrics_cannot_replace_actual_inventory(captured_case, monkeypatch, changes):
    case = captured_case
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    metrics_by_field = {**case.metrics, **changes}
    counts["bundle_metrics"] = metrics_by_field
    with pytest.raises(RuntimeError):
        module.store._completion_metrics(case.run, metrics_by_field, counts)


async def test_prepare_rejects_run_id_rewriting_and_keeps_nysed_corroboration(captured_case):
    root, bound, manifest, files = copy.deepcopy(captured_case.inputs[0])
    with pytest.raises(ValueError, match="capture_identity_changed"):
        module.prepare_profile("b" * 32, root, bound, capture_manifest=manifest, file_sha256=files)
    _, record, facts = module.prepare_profile(RUN_ID, root, bound, capture_manifest=manifest, file_sha256=files)
    assert (
        record["match_evidence"]["registry_binding"]["nysed_corroboration"]
        == bound["source_record"]["match_evidence"]["registry_binding"]["nysed_corroboration"]
    )
    assert all(fact["source_json"]["source_key"] == module.SOURCE_KEY for fact in facts)


async def test_cancelled_inventory_scan_closes_stream_and_cannot_publish(captured_case, monkeypatch):
    closed = _install_retained(monkeypatch, captured_case, cancel=True)
    completed = AsyncMock()
    monkeypatch.setattr(type(module.store), "_complete_run", completed)
    with pytest.raises(asyncio.CancelledError):
        await module.store.retained_counts(RUN_ID)
    assert closed == [True]
    completed.assert_not_awaited()


def test_volume_refuses_empty_publication_and_uses_existing_relative_guard():
    baseline_by_field = {"acquired_profiles": 100, "retained_facts": 100, "matched_public_providers": 100}
    module.store._publication_volume(baseline_by_field, None)
    module.store._publication_volume({key: 80 for key in baseline_by_field}, baseline_by_field)
    for field in baseline_by_field:
        with pytest.raises(RuntimeError, match="empty_publication"):
            module.store._publication_volume({**baseline_by_field, field: 0}, None)
        with pytest.raises(RuntimeError, match="volume_drop:" + field):
            module.store._publication_volume({**baseline_by_field, field: 79}, baseline_by_field)


def test_witness_keeps_existing_canonical_preimages_and_compact_completion(witnessed_case):
    case = witnessed_case
    bundle = module.store._bundle(case.run, [case.artifact])
    for descriptor in bundle["profiles"].values():
        if descriptor["record_id"] is None:
            assert descriptor["record_values"] is None and descriptor["fact_values"] == []
            continue
        assert module._hash(descriptor["record_values"]) == descriptor["record_sha256"]
        assert {fact["fact_id"]: module._hash(fact) for fact in descriptor["fact_values"]} == descriptor["facts"]
        assert all("published_at" not in fact for fact in descriptor["fact_values"])
    counts_by_field = {
        **_counts(),
        **module._bundle_counts(bundle),
        "invalid_bundle_records": 0,
        "invalid_bundle_facts": 0,
        "bundle_reference": module.bundle_reference(case.artifact),
    }
    final = module.store._completion_metrics(case.run, case.compact_metrics, counts_by_field)
    assert final["retained_facts"] == 2 and final["bundle"] == case.compact_metrics["bundle"]
    assert not {"nysed_support", "bundle_metrics", "bundle_reference"} & set(final)
    with pytest.raises(ValueError, match="bundle_reference_invalid"):
        module.store._completion_metrics(case.run, case.metrics, counts_by_field)


@pytest.mark.parametrize("change", ["digest", "bytes", "reference", "run", "type", "extra"])
async def test_witness_bundle_authenticates_actual_canonical_bytes(witnessed_case, change):
    case = witnessed_case
    artifact_by_field = {
        **case.artifact,
        "stored_sha256": case.artifact["content_sha256"],
        "stored_bytes": case.artifact["content_bytes"],
    }
    reference_by_field = dict(case.compact_metrics["bundle"])
    if change == "digest":
        artifact_by_field["stored_sha256"] = "0" * 64
    elif change == "bytes":
        artifact_by_field["stored_bytes"] -= 1
    replacements_by_change = {
        "reference": {"content_sha256": "0" * 64},
        "run": {"run_id": "b" * 32},
        "type": {"content_bytes": True},
        "extra": {"path": "/not-an-authority"},
    }
    reference_by_field.update(replacements_by_change.get(change, {}))
    rows = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [artifact_by_field]))
    session = SimpleNamespace(execute=AsyncMock(return_value=rows))
    with pytest.raises(ValueError):
        await module.read_witness_bundle(session, "isolated_candidate", case.run, reference_by_field)
    query = str(session.execute.await_args.args[0])
    assert "sha256(convert_to(metadata_json::text,'UTF8'))" in query
    assert "octet_length(convert_to(metadata_json::text,'UTF8'))" in query


@pytest.mark.parametrize("has_caller_session", [False, True])
async def test_retained_aggregates_preserve_query_and_use_caller_session(monkeypatch, has_caller_session):
    rows = [SimpleNamespace(_mapping={"run_id": RUN_ID, "retained_source_records": 2, "invalid_facts": None})]
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: rows)))
    database_query = AsyncMock(return_value=rows)
    monkeypatch.setattr(shared.db, "all", database_query)
    counts = await module.store._retained_counts_by_run(
        [RUN_ID, RUN_ID], schema="isolated_candidate", session=session if has_caller_session else None
    )
    assert counts == {RUN_ID: {"retained_source_records": 2, "invalid_facts": 0}}
    parameters_by_field = {
        "run_ids": [RUN_ID],
        "source_key": module.SOURCE_KEY,
        "schema_version": module.SCHEMA_VERSION,
    }
    if has_caller_session:
        database_query.assert_not_awaited()
        session.execute.assert_awaited_once()
        statement, actual_parameters = session.execute.await_args.args
        assert actual_parameters == parameters_by_field
    else:
        session.execute.assert_not_awaited()
        database_query.assert_awaited_once()
        statement = database_query.await_args.args[0]
        assert database_query.await_args.kwargs == parameters_by_field
    assert '"isolated_candidate"."provider_profile_fact"' in str(statement)
    assert await module.store._retained_counts_by_run([], session=session) == {}
    assert session.execute.await_count == int(has_caller_session)


async def test_witness_retained_checks_never_iterate_payload_rows(witnessed_case, monkeypatch):
    case = witnessed_case
    session = SimpleNamespace(scalar=AsyncMock(return_value=0))

    @asynccontextmanager
    async def transaction():
        yield session

    def forbidden(*args, **kwargs):
        raise AssertionError("witness path reached retained-row iteration")

    monkeypatch.setattr(type(module.store), "_read_run", AsyncMock(return_value=case.run))
    monkeypatch.setattr(shared.db, "transaction", transaction)
    monkeypatch.setattr(shared.db, "bind_existing_session", forbidden)
    monkeypatch.setattr(shared.db, "select", forbidden)
    monkeypatch.setattr(type(module.store), "_inventory_counts", forbidden)
    retained_counts = AsyncMock(return_value={RUN_ID: _counts()})
    monkeypatch.setattr(shared.SourceProfileStore, "_retained_counts_by_run", retained_counts)
    monkeypatch.setattr(
        module, "read_witness_bundle", AsyncMock(return_value=(case.artifact, case.artifact["metadata_json"]))
    )
    counts = await module.store.retained_counts(RUN_ID)
    retained_counts.assert_awaited_once_with([RUN_ID], schema="mrf", session=session)
    assert module.store._completion_metrics(case.run, case.compact_metrics, counts)["retained_facts"] == 2
    queries = [str(call.args[0]) for call in session.scalar.await_args_list]
    assert len(queries) == 6
    assert sum("sha256(convert_to(w.value::text,'UTF8'))" in query for query in queries) == 2
    assert sum("json_populate_record" in query and "NOT EXISTS" in query for query in queries) == 2
    assert "nysed_corroboration" in queries[-1] and "profile_capture" in queries[-1]
    assert all("::jsonb" not in query for query in queries)


@pytest.mark.parametrize("invalid_check", range(6))
async def test_native_witness_aggregate_failure_cannot_complete(witnessed_case, invalid_check):
    case = witnessed_case
    observed_counts = [0] * 6
    observed_counts[invalid_check] = 1
    session = SimpleNamespace(scalar=AsyncMock(side_effect=observed_counts))
    if invalid_check in {0, 2}:
        with pytest.raises(ValueError, match="canonical_values_invalid"):
            await module.witness_inventory_counts(session, "isolated_candidate", case.run)
    else:
        counts_by_field = {
            **_counts(),
            **module._bundle_counts(case.artifact["metadata_json"]),
            **await module.witness_inventory_counts(session, "isolated_candidate", case.run),
            "bundle_reference": module.bundle_reference(case.artifact),
        }
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            module.store._completion_metrics(case.run, case.compact_metrics, counts_by_field)


async def test_legacy_inventory_api_does_not_accept_a_new_witness(witnessed_case):
    with pytest.raises(ValueError, match="witness_requires_native_validation"):
        await module.store._inventory_counts(witnessed_case.run, witnessed_case.artifact["metadata_json"])


async def test_legacy_copy_is_byte_and_row_bounded_without_a_total_cap(monkeypatch):
    from process import reference_family_archive as native

    codec_documents = [
        {"unicode": "Ł學😀\u0000" * 2000, "floats": [1, 1.0, -0.0, 5e-324, 1.7976931348623157e308]},
        json.loads('{"duplicate": 1, "duplicate": 2, "zero": -0.0}'),
        *[{"row": index} for index in range(10)],
    ]
    transferred_by_row, batches = {}, []

    async def source(*_args):
        for ordinal, document in enumerate(codec_documents):
            yield "records", ordinal, module.encoded_json(document)

    async def copy_records(session, model, **options):
        batch = options["records"]
        assert model is shared.ProviderProfileArtifact
        assert options["schema_name"] == "pg_temp" and options["table_name"] == "scratch" and 0 < len(batch) <= 3
        assert sum(sum(len(value.encode()) if isinstance(value, str) else 8 for value in row) for row in batch) <= 2048
        batches.append(len(batch))
        for row in batch:
            envelope_by_field = dict(zip(options["columns"], row, strict=True))
            transferred_by_row.setdefault(int(envelope_by_field["file_name"]), []).append(
                (int(envelope_by_field["header"]), envelope_by_field["source_url"])
            )

    monkeypatch.setattr(module, "_LEGACY_TRANSFER_ROWS", 3)
    monkeypatch.setattr(module, "_LEGACY_TRANSFER_BYTES", 2048)
    monkeypatch.setattr(module, "_legacy_witness_values", source)
    monkeypatch.setattr(native, "native_copy_record_batch", copy_records)
    await module._copy_legacy_witness(object(), "source", "scratch", RUN_ID, {})
    assert max(batches) == 3 and sum(batches) > len(codec_documents)
    for ordinal, document in enumerate(codec_documents):
        assert "".join(part for _, part in sorted(transferred_by_row[ordinal])).encode() == module.encoded_json(
            document
        )


async def test_legacy_typed_transfer_keeps_opaque_json_in_the_exact_digest(captured_case):
    case = captured_case
    case.facts[0]["value_json"]["codec"] = {"nul": "a\u0000b", "zero": -0.0, "float": 1.0}
    closed_streams = []

    class Rows:
        def __init__(self, statement):
            self.rows = case.records if statement.get_final_froms()[0].name.endswith("source_record") else case.facts

        async def __aiter__(self):
            for source_record in self.rows:
                yield SimpleNamespace(_mapping=source_record)

        async def close(self):
            closed_streams.append(True)

    session = SimpleNamespace(stream=AsyncMock(side_effect=Rows))
    transferred_by_key = {
        (kind, ordinal): content
        async for kind, ordinal, content in module._legacy_witness_values(session, "source", RUN_ID, {})
    }
    assert transferred_by_key["facts", 0] == module.encoded_json(
        {key: value for key, value in case.facts[0].items() if key != "published_at"}
    )
    fields_by_key = json.loads(transferred_by_key["facts_fields", 0])
    assert fields_by_key["value_json"] is fields_by_key["source_json"] is None
    assert fields_by_key["fact_id"] == case.facts[0]["fact_id"] and fields_by_key["run_id"] == RUN_ID
    assert b"\\u0000" in transferred_by_key["facts", 0] and b"\\u0000" not in transferred_by_key["facts_fields", 0]
    assert closed_streams == [True, True] and session.stream.await_count == 2


def test_legacy_metadata_encoding_preserves_string_distinctions_without_literal_nul():
    strings = ["\u0000", "\\u0000", "\\\\u0000", "\\", "Ł學😀", "a\u0000b", "a\\u0000b"]
    expected_by_key = {key: [key, {"value": key}] for key in strings}
    encoded = module._legacy_metadata_values(expected_by_key)
    assert len(encoded) == len(strings) and len(set(module._legacy_metadata_values(value) for value in strings)) == len(
        strings
    )
    assert all("\u0000" not in key and "\u0000" not in value[0] for key, value in encoded.items())
    assert json.loads(module.encoded_json(encoded)) == encoded
    assert expected_by_key == {key: [key, {"value": key}] for key in strings}


async def test_legacy_scratch_is_private_temporary_model_storage_with_deferred_keys():
    from sqlalchemy.dialects import postgresql

    from process import reference_family_archive as native

    name = "ny_legacy_witness_" + "a" * 32
    table = module._legacy_witness_table(name)
    original = shared.ProviderProfileArtifact.__table__
    assert tuple(table.c.keys()) == tuple(original.c.keys()) and original.schema == "mrf"
    native._defer_table_constraints(table)
    ddl = str(module.CreateTable(table).compile(dialect=postgresql.dialect()))
    assert "CREATE TEMPORARY TABLE pg_temp." in ddl and "ON COMMIT DROP" in ddl
    assert "PRIMARY KEY" not in ddl and "UNIQUE" not in ddl and "CREATE SCHEMA" not in ddl
    session = SimpleNamespace(execute=AsyncMock())
    await native._create_table_constraints(session, module._legacy_witness_table(name))
    keys = [str(call.args[0].compile(dialect=postgresql.dialect())) for call in session.execute.await_args_list]
    assert len(keys) == 2 and any("PRIMARY KEY (artifact_id)" in key for key in keys)
    assert any("UNIQUE (run_id, source_key)" in key for key in keys)
    assert all(name in key and "pg_temp." in key for key in keys)


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), "\ud800"])
async def test_legacy_encoding_keeps_existing_codec_refusals_and_closes_cursor(captured_case, monkeypatch, invalid):
    case = captured_case
    case.records[0]["raw_payload"]["invalid_codec"] = invalid
    closed = _install_retained(monkeypatch, case)
    with pytest.raises((ValueError, UnicodeEncodeError)):
        await module.store.retained_counts(RUN_ID)
    assert closed == [True]


@pytest.mark.parametrize("failure", [None, "copy", "indexes", "counts", "cancel"])
async def test_legacy_witness_uses_a_rolled_back_savepoint(monkeypatch, captured_case, failure):
    from process import reference_family_archive as native

    events = []
    rollback = AsyncMock(side_effect=lambda: events.append("rollback"))

    @asynccontextmanager
    async def nested():
        events.append("savepoint")
        try:
            yield SimpleNamespace(rollback=rollback)
        except BaseException:
            events.append("rollback_error")
            raise

    def operation(name):
        async def run(*args, **kwargs):
            events.append(name)
            if failure == name or failure == "cancel" and name == "copy":
                raise asyncio.CancelledError if failure == "cancel" else RuntimeError("original_" + name)
            return {"invalid_bundle_records": 0, "invalid_bundle_facts": 0}

        return run

    session = SimpleNamespace(
        in_transaction=lambda: True,
        begin_nested=nested,
        execute=operation("create"),
        connection=AsyncMock(return_value=_legacy_connection()),
    )
    monkeypatch.setattr(native, "_create_table_constraints", operation("indexes"))
    monkeypatch.setattr(module, "_copy_legacy_witness", operation("copy"))
    monkeypatch.setattr(module, "_legacy_witness_counts", operation("counts"))
    if failure is None:
        assert await module.legacy_inventory_counts(session, "source", captured_case.run, {}) == {
            "invalid_bundle_records": 0,
            "invalid_bundle_facts": 0,
        }
        assert events == ["savepoint", "create", "copy", "indexes", "indexes", "counts", "rollback"]
    else:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            await module.legacy_inventory_counts(session, "source", captured_case.run, {})
        assert events[-1] == "rollback_error" and "rollback" not in events


@pytest.mark.parametrize("interruption", [asyncio.CancelledError, TimeoutError])
@pytest.mark.parametrize("state", ["live", "terminated", "invalidated"])
async def test_legacy_copy_interruption_invalidates_only_terminated_connection(
    monkeypatch, captured_case, interruption, state
):
    events = []
    failure = interruption("original_copy_interruption")
    connection = _legacy_connection()
    driver = connection.get_raw_connection.return_value.driver_connection

    async def interrupted_copy(*args):
        driver.is_closed = lambda: state == "terminated"
        connection.invalidated = state == "invalidated"
        raise failure

    async def invalidate():
        events.append("invalidate")
        connection.invalidated = True

    @asynccontextmanager
    async def nested():
        try:
            yield SimpleNamespace(rollback=AsyncMock())
        except BaseException:
            if not connection.invalidated:
                events.append("rollback_sql")
                if driver.is_closed():
                    raise RuntimeError("connection is closed") from None
            raise

    connection.invalidate = invalidate
    session = SimpleNamespace(
        in_transaction=lambda: True,
        connection=AsyncMock(return_value=connection),
        begin_nested=nested,
        execute=AsyncMock(),
    )
    monkeypatch.setattr(module, "_copy_legacy_witness", interrupted_copy)
    with pytest.raises(interruption) as caught:
        await module.legacy_inventory_counts(session, "source", captured_case.run, {})
    assert caught.value is failure
    assert events == {"live": ["rollback_sql"], "terminated": ["invalidate"], "invalidated": []}[state]


async def test_legacy_cancellation_does_not_hide_live_savepoint_failure(monkeypatch, captured_case):
    connection = _legacy_connection()
    rollback_failure = RuntimeError("unrelated_savepoint_failure")

    @asynccontextmanager
    async def nested():
        try:
            yield SimpleNamespace(rollback=AsyncMock())
        except asyncio.CancelledError:
            raise rollback_failure

    session = SimpleNamespace(
        in_transaction=lambda: True,
        connection=AsyncMock(return_value=connection),
        begin_nested=nested,
        execute=AsyncMock(),
    )
    monkeypatch.setattr(module, "_copy_legacy_witness", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(RuntimeError) as caught:
        await module.legacy_inventory_counts(session, "source", captured_case.run, {})
    assert caught.value is rollback_failure
    connection.invalidate.assert_not_awaited()


@pytest.mark.parametrize("invalid_check", range(3))
async def test_legacy_native_aggregate_refusals_cannot_complete(captured_case, invalid_check):
    outcomes = [0, 0, 0]
    outcomes[invalid_check] = 1
    results = [SimpleNamespace(one=lambda count=count: (0, count)) for count in outcomes[:2]]
    session = SimpleNamespace(execute=AsyncMock(side_effect=results), scalar=AsyncMock(return_value=outcomes[2]))
    counts = await module._legacy_witness_counts(session, "scratch", "source", captured_case.run)
    with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
        module.store._completion_metrics(
            captured_case.run,
            captured_case.metrics,
            {
                **_counts(),
                **counts,
                **module._bundle_counts(captured_case.artifact["metadata_json"]),
            },
        )
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(queries) == 2 and all("sha256(convert_to(w.canonical,'UTF8'))" in query for query in queries)
    assert all("string_agg(source_url,'' ORDER BY header::text::bigint)" in query for query in queries)
    assert all("NOT EXISTS" in query and "::jsonb" not in query and "to_jsonb" not in query for query in queries)
    assert "nysed_corroboration" in queries[0] and "profile_capture" in queries[0]


async def test_witness_cannot_fall_back_to_direct_shared_parent_publication(witnessed_case, monkeypatch):
    from process import source_profile_result_archive as archive

    @asynccontextmanager
    async def transaction():
        yield object()

    retained = AsyncMock(side_effect=AssertionError("ordinary payload validation reached"))
    update = AsyncMock(side_effect=AssertionError("ordinary payload writer reached"))
    monkeypatch.setattr(shared.db, "transaction", transaction)
    monkeypatch.setattr(shared.db, "update", update)
    monkeypatch.setattr(archive, "require_ordinary_publication_authority", AsyncMock())
    monkeypatch.setattr(type(module.store), "_lock_source", AsyncMock())
    monkeypatch.setattr(type(module.store), "_read_run", AsyncMock(return_value=witnessed_case.run))
    monkeypatch.setattr(type(module.store), "retained_counts", retained)
    with pytest.raises(RuntimeError, match="native_publication_required"):
        await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=witnessed_case.compact_metrics)
    retained.assert_not_awaited()
    update.assert_not_called()


@pytest.mark.parametrize(
    "model",
    [
        shared.ProviderProfileImportRun,
        shared.ProviderProfileArtifact,
        shared.ProviderProfileSourceRecord,
        shared.ProviderProfileFact,
    ],
)
async def test_canonical_model_comparison_preserves_json_text_and_whole_run_scope(model):
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=True))
    assert await module.is_canonical_model_equal(
        session,
        model,
        left=("candidate", model.__tablename__),
        right=("published", model.__tablename__),
        run_ids=[RUN_ID],
    )
    query = str(session.scalar.await_args.args[0])
    assert "IS DISTINCT FROM" in query and "NOT EXISTS" in query and "::jsonb" not in query
    assert all(
        f'"{column.name}"::text' in query for column in model.__table__.columns if isinstance(column.type, module.JSON)
    )
    assert session.scalar.await_args.args[1] == {"runs": [RUN_ID]}
