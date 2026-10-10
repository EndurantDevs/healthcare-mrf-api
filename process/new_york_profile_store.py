# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Publish a complete supported NYPP cohort with exact retained capture lineage."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import re
from contextlib import aclosing
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Integer, MetaData, String, select, text
from sqlalchemy.schema import CreateTable

from process import new_york_nysed_profile as nysed
from process import new_york_nysed_profile_retries as nysed_retries
from process import provider_profile_source_store as shared
from process.massachusetts_profile_acquisition import encoded_json
from process.new_york_profile_acquisition import _held_search_reason
from process.new_york_profile_binding import ACQUISITION_FILES, CORROBORATED_METHOD, QUERY_SHA256
from process.new_york_profile_registry import REGISTRY_PRECONDITIONS
from process.new_york_profile_retained import HELD_FILES, _validated_manifest
from process.new_york_profile_rows import SCHEMA_VERSION, SOURCE_KEY
from process.provider_profile_source_completion import SourceProfileCompletion
from process.provider_profile_source_store import ProfileSourcePolicy, SourceProfileStore

IMPORTER = "new-york-nypp-profile"
SOURCE_URL = "https://www.nydoctorprofile.com/"
CATEGORIES = ("education", "training", "certifications")
COVERAGE_SCOPE = "supported_nppes_derived_ny_physician_license_roots"
WITNESS_CONTRACT = "ny-profile-model-witness.v1"
CAPTURE_FIELDS = ("capture_manifest", "file_sha256", "manifest_sha256", "acquisition_sha256")
NYSED_IDENTITY_FIELDS = (
    "source_record_id",
    "artifact_id",
    "source_key",
    "source_url",
    "content_sha256",
    "downloaded_at",
    "profession_code",
    "license_number",
    "legal_name",
)
_LEGACY_TRANSFER_ROWS = 256
_LEGACY_TRANSFER_BYTES = 8 * 1024 * 1024


def _require(condition, reason):
    if not condition:
        raise ValueError("new_york_profile_" + reason)


def _sha(digest):
    return isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest) is not None


def _hash(content):
    return hashlib.sha256(encoded_json(content)).hexdigest()


def _fact_hash(fact):
    return _hash({key: content for key, content in fact.items() if key != "published_at"})


def witnessed_profile(descriptor, record, facts):
    """Keep the already-remapped model values under their original canonical digests."""
    return {
        **descriptor,
        "record_values": record,
        "fact_values": [{key: value for key, value in fact.items() if key != "published_at"} for fact in facts],
    }


def bundle_reference(artifact):
    """Bind the complete producer envelope without passing a worker filesystem path."""
    return {
        "contract": WITNESS_CONTRACT,
        **{key: artifact[key] for key in ("artifact_id", "run_id", "content_sha256", "content_bytes")},
    }


def validate_bundle_reference(value, run_id):
    """Require one bounded reference to this run's existing canonical artifact identity."""
    _require(
        isinstance(value, dict)
        and set(value) == {"contract", "artifact_id", "run_id", "content_sha256", "content_bytes"}
        and value["contract"] == WITNESS_CONTRACT
        and value["run_id"] == run_id
        and value["artifact_id"] == _hash([run_id, SOURCE_KEY])
        and _sha(value["content_sha256"])
        and type(value["content_bytes"]) is int
        and 0 < value["content_bytes"] < 2**63,
        "bundle_reference_invalid",
    )
    return value


def _capture_descriptor(run_id, license_number, capture_manifest, file_sha256, *, is_held):
    _validated_manifest(capture_manifest, _hash(capture_manifest))
    _require(
        capture_manifest["run_id"] == run_id and capture_manifest["license_number"] == license_number,
        "capture_identity_changed",
    )
    expected_files = set(HELD_FILES) if is_held else {name for name, _ in ACQUISITION_FILES}
    _require(
        isinstance(file_sha256, dict)
        and set(file_sha256) == expected_files
        and all(_sha(digest) for digest in file_sha256.values())
        and file_sha256["manifest.json"] == _hash(capture_manifest),
        "capture_inventory_invalid",
    )
    return {
        "capture_manifest": copy.deepcopy(capture_manifest),
        "file_sha256": dict(file_sha256),
        "manifest_sha256": file_sha256["manifest.json"],
        "acquisition_sha256": _hash(file_sha256),
    }


def prepare_profile(run_id, root, bound_result, *, capture_manifest, file_sha256):
    """Remap only bundle references after pinned replay; keep original capture IDs.

    This adapter does not acquire or authenticate evidence. The runner must replay
    each held attempt or bind each acquired profile against its pinned snapshot.
    """
    store._run_id(run_id)
    license_number = root["license_number"]
    source_record = bound_result["source_record"]
    capture = _capture_descriptor(run_id, license_number, capture_manifest, file_sha256, is_held=source_record is None)
    descriptor_by_field = {
        **capture,
        "acquisition_outcome": "held" if source_record is None else "acquired",
        "binding_outcome": None,
        "reason": bound_result["reason"],
        "reported_total": None,
        "capture_artifact_id": None,
        "record_id": None,
        "record_sha256": None,
        "facts": {},
    }
    if source_record is None:
        total = bound_result.get("reported_total")
        _require(
            bound_result
            == {
                "outcome": "held",
                "reason": _held_search_reason(total),
                "reported_total": total,
                "source_record": None,
                "facts": [],
            }
            and type(total) is int
            and total >= 0,
            "held_attempt_invalid",
        )
        descriptor_by_field["reported_total"] = total
        return descriptor_by_field, None, []
    return _remap_acquired_profile(run_id, license_number, bound_result, descriptor_by_field)


def _remap_acquired_profile(run_id, license_number, bound_result, descriptor_by_field):
    source_record, facts = copy.deepcopy((bound_result["source_record"], bound_result["facts"]))
    capture_by_field = {key: descriptor_by_field[key] for key in CAPTURE_FIELDS}
    _require(
        source_record["run_id"] == run_id
        and source_record["source_key"] == SOURCE_KEY
        and source_record["license_number"] == license_number
        and source_record["record_id"] == _hash([run_id, SOURCE_KEY + ":" + license_number]),
        "record_identity_changed",
    )
    binding = source_record["match_evidence"]["registry_binding"]
    _require(
        binding["manifest_sha256"] == capture_by_field["manifest_sha256"]
        and binding["acquisition_sha256"] == capture_by_field["acquisition_sha256"]
        and binding["npi"] == source_record["matched_npi"]
        and binding["status"] == source_record["match_status"]
        and binding["reason"] == bound_result["reason"]
        and bound_result["outcome"] == ("accepted" if source_record["matched_npi"] is not None else "held"),
        "binding_capture_changed",
    )
    descriptor_by_field.update(
        binding_outcome=bound_result["outcome"],
        capture_artifact_id=source_record["artifact_id"],
        record_id=source_record["record_id"],
    )
    source_record["normalized_payload"]["profile_capture"] = {
        **capture_by_field,
        "artifact_id": source_record["artifact_id"],
    }
    source_record["artifact_id"] = _hash([run_id, SOURCE_KEY])
    for fact in facts:
        _require(
            fact["run_id"] == run_id
            and fact["source_record_id"] == source_record["record_id"]
            and fact["source_json"]["artifact_id"] == descriptor_by_field["capture_artifact_id"]
            and fact["npi"] == source_record["matched_npi"]
            and fact["published_at"] is None,
            "fact_capture_changed",
        )
        fact["source_json"]["capture_artifact_id"] = fact["source_json"]["artifact_id"]
        fact["source_json"]["artifact_id"] = source_record["artifact_id"]
        _require(fact["fact_id"] not in descriptor_by_field["facts"], "duplicate_fact_identity")
        descriptor_by_field["facts"][fact["fact_id"]] = _fact_hash(fact)
    descriptor_by_field["record_sha256"] = _hash(source_record)
    return descriptor_by_field, source_record, facts


def _cohort_roots(cohort, manifest):
    _require(
        isinstance(cohort, dict)
        and _hash(cohort) == manifest["cohort_sha256"]
        and cohort.get("schema_version") == "ny-nppes-acquisition-cohort/v1"
        and cohort.get("selection_rule") == "exact_six_digit_license_with_any_valid_individual_physician_occurrence"
        and cohort.get("snapshot_sha256") == manifest["snapshot_sha256"]
        and cohort.get("query_sha256") == QUERY_SHA256
        and cohort.get("integrity_verified") is True
        and cohort.get("state_census") is False
        and cohort.get("source_identity") == "unverified",
        "cohort_changed",
    )
    roots = cohort.get("roots")
    _require(isinstance(roots, list) and len(roots) == manifest["full_cohort_licenses"], "cohort_count_changed")
    by_license, all_indexes = {}, set()
    for root in roots:
        license_number, indexes = root.get("license_number"), root.get("registry_occurrence_indexes")
        _require(
            isinstance(license_number, str)
            and re.fullmatch(r"[0-9]{6}", license_number)
            and license_number not in by_license
            and isinstance(indexes, list)
            and bool(indexes)
            and all(type(index) is int and 0 <= index < manifest["snapshot_row_count"] for index in indexes)
            and indexes == sorted(set(indexes))
            and not all_indexes.intersection(indexes)
            and root.get("registry_only_precondition") in REGISTRY_PRECONDITIONS,
            "cohort_inventory_invalid",
        )
        all_indexes.update(indexes)
        by_license[license_number] = root
    _require(
        list(by_license) == sorted(by_license)
        and cohort.get("summary", {}).get("registry_row_count") == manifest["snapshot_row_count"]
        and cohort["summary"].get("selected_row_count") == len(all_indexes)
        and cohort["summary"].get("acquisition_root_count") == len(roots),
        "cohort_inventory_invalid",
    )
    return by_license


def _validate_descriptor(descriptor_by_field, run_id, license_number, *, witnessed=False):
    _require(
        isinstance(descriptor_by_field, dict)
        and set(descriptor_by_field)
        == {
            "capture_manifest",
            "file_sha256",
            "manifest_sha256",
            "acquisition_sha256",
            "acquisition_outcome",
            "binding_outcome",
            "reason",
            "reported_total",
            "capture_artifact_id",
            "record_id",
            "record_sha256",
            "facts",
        }
        | ({"record_values", "fact_values"} if witnessed else set()),
        "attempt_descriptor_invalid",
    )
    is_held = descriptor_by_field["acquisition_outcome"] == "held"
    if witnessed:
        _require_witness_values(descriptor_by_field, is_held)
    capture = _capture_descriptor(
        run_id,
        license_number,
        descriptor_by_field["capture_manifest"],
        descriptor_by_field["file_sha256"],
        is_held=is_held,
    )
    _require(all(descriptor_by_field[key] == content for key, content in capture.items()), "attempt_capture_changed")
    if is_held:
        _require(
            descriptor_by_field["binding_outcome"] is None
            and descriptor_by_field["reason"] == _held_search_reason(descriptor_by_field["reported_total"])
            and type(descriptor_by_field["reported_total"]) is int
            and descriptor_by_field["reported_total"] >= 0
            and descriptor_by_field["capture_artifact_id"] is None
            and descriptor_by_field["record_id"] is None
            and descriptor_by_field["record_sha256"] is None
            and descriptor_by_field["facts"] == {},
            "held_descriptor_invalid",
        )
    else:
        _require(
            descriptor_by_field["acquisition_outcome"] == "acquired"
            and descriptor_by_field["binding_outcome"] in {"accepted", "held"}
            and isinstance(descriptor_by_field["reason"], str)
            and descriptor_by_field["reason"].strip()
            and descriptor_by_field["reported_total"] is None
            and _sha(descriptor_by_field["capture_artifact_id"])
            and descriptor_by_field["record_id"] == _hash([run_id, SOURCE_KEY + ":" + license_number])
            and _sha(descriptor_by_field["record_sha256"])
            and isinstance(descriptor_by_field["facts"], dict)
            and all(_sha(fact_id) and _sha(digest) for fact_id, digest in descriptor_by_field["facts"].items()),
            "acquired_descriptor_invalid",
        )


def _require_witness_values(descriptor, is_held):
    _require(
        isinstance(descriptor["fact_values"], list)
        and (
            descriptor["record_values"] is None and not descriptor["fact_values"]
            if is_held
            else isinstance(descriptor["record_values"], dict)
        ),
        "witness_shape_invalid",
    )


def _validate_nysed_capture(support, run_id, license_number):
    _require(
        isinstance(support, dict)
        and set(support)
        == {
            "capture_manifest",
            "file_sha256",
            "receipt",
            "receipt_sha256",
            "source_identity",
        },
        "nysed_support_invalid",
    )
    manifest, files, receipt = support["capture_manifest"], support["file_sha256"], support["receipt"]
    _require(
        isinstance(manifest, dict)
        and set(manifest)
        == {
            "schema_version",
            "source_key",
            "profession_code",
            "license_number",
            "run_id",
            "started_at",
        }
        and manifest["schema_version"] == nysed.SCHEMA_VERSION
        and manifest["source_key"] == nysed.SOURCE_KEY
        and manifest["profession_code"] == nysed.PROFESSION_CODE
        and manifest["run_id"] == run_id
        and manifest["license_number"] == license_number,
        "nysed_manifest_invalid",
    )
    _require(
        isinstance(files, dict)
        and set(files)
        == {"manifest.json", "request.json", "response.json", "result.json"}
        | set(nysed_retries.timeout_files(receipt, manifest, file_sha256=files))
        and all(_sha(digest) for digest in files.values())
        and files["manifest.json"] == _hash(manifest)
        and files["request.json"] == _hash(nysed.request_descriptor(license_number))
        and isinstance(receipt, dict)
        and _hash(receipt) == support["receipt_sha256"] == files["result.json"],
        "nysed_files_changed",
    )
    _validate_nysed_receipt(support, run_id, license_number)


def _validate_nysed_receipt(support, run_id, license_number):
    manifest, files, receipt = support["capture_manifest"], support["file_sha256"], support["receipt"]
    outcome = receipt.get("outcome")
    fields = {
        "schema_version",
        "outcome",
        "completed_at",
        "manifest_sha256",
        "request_sha256",
        "response_sha256",
        "fact_count",
    }
    _require(
        outcome in {"acquired", "held", "invalid"}
        and set(receipt)
        == fields
        | ({"reason"} if outcome in {"held", "invalid"} else set())
        | ({"timeout_retries"} if "timeout_retries" in receipt else set())
        and receipt["schema_version"] == nysed.SCHEMA_VERSION
        and type(receipt["fact_count"]) is int
        and all(receipt[field + "_sha256"] == files[field + ".json"] for field in ("manifest", "request", "response")),
        "nysed_receipt_invalid",
    )
    _require(
        nysed._timestamp(manifest["started_at"]) <= nysed._timestamp(receipt["completed_at"]),
        "nysed_chronology_invalid",
    )
    if outcome == "held":
        _require(
            receipt["reason"] == "no_profile_returned"
            and receipt["fact_count"] == 0
            and support["source_identity"] is None,
            "nysed_held_invalid",
        )
    elif outcome == "invalid":
        _require(
            receipt["reason"] in nysed.INVALID_PROFILE_REASONS
            and receipt["fact_count"] == 0
            and support["source_identity"] is None,
            "nysed_invalid_invalid",
        )
    else:
        _require(receipt["fact_count"] > 0, "nysed_fact_count_invalid")
        _validate_nysed_identity(support, run_id, license_number)


def _validate_nysed_identity(support, run_id, license_number):
    identity = support["source_identity"]
    _require(
        isinstance(identity, dict)
        and set(identity) == set(NYSED_IDENTITY_FIELDS)
        and identity["source_key"] == nysed.SOURCE_KEY
        and identity["profession_code"] == nysed.PROFESSION_CODE
        and identity["license_number"] == license_number
        and identity["source_record_id"]
        == _hash([run_id, f"{nysed.SOURCE_KEY}:{nysed.PROFESSION_CODE}:{license_number}"])
        and identity["artifact_id"] == support["receipt"]["response_sha256"]
        and identity["source_url"] == nysed.request_descriptor(license_number)["source_url"]
        and _sha(identity["content_sha256"])
        and isinstance(identity["legal_name"], str)
        and identity["legal_name"].strip().casefold() not in nysed.UNREPORTED,
        "nysed_identity_invalid",
    )
    _require(
        nysed._timestamp(support["capture_manifest"]["started_at"])
        <= nysed._timestamp(identity["downloaded_at"])
        <= nysed._timestamp(support["receipt"]["completed_at"]),
        "nysed_chronology_invalid",
    )
    nysed_retries.timeout_files(
        support["receipt"], support["capture_manifest"], final_response={"downloaded_at": identity["downloaded_at"]}
    )


def _validate_nysed_inventory(acquisition, profiles, run_id):
    _require(
        isinstance(acquisition, dict) and isinstance(acquisition.get("nysed_support"), dict), "nysed_inventory_missing"
    )
    support_by_license = acquisition["nysed_support"]
    expected_licenses = {
        license_number
        for license_number, descriptor in profiles.items()
        if descriptor["acquisition_outcome"] == "acquired"
    }
    _require(set(support_by_license) == expected_licenses, "nysed_inventory_incomplete")
    for license_number, support in support_by_license.items():
        _validate_nysed_capture(support, run_id, license_number)


def _witness_rows(schema, run_id, kind):
    """Expand canonical producer values without re-encoding retained database rows."""
    from process.source_profile_result_archive import _table

    run_id = store._run_id(run_id)
    artifact = _table(schema, shared.ProviderProfileArtifact.__tablename__)
    profiles = (
        f"SELECT a.metadata_json AS bundle,p.key AS license,p.value AS descriptor FROM {artifact} a "
        "CROSS JOIN LATERAL json_each(a.metadata_json->'profiles') p "
        f"WHERE a.run_id='{run_id}' AND p.value->>'acquisition_outcome'='acquired'"
    )
    if kind == "records":
        values = (
            "SELECT descriptor->>'record_id' AS identity,descriptor->>'record_sha256' AS digest,"
            "descriptor->'record_values' AS value,bundle,descriptor,license FROM profiles"
        )
    else:
        _require(kind == "facts", "witness_kind_invalid")
        values = (
            "SELECT f.value->>'fact_id' AS identity,descriptor->'facts'->>(f.value->>'fact_id') AS digest,"
            "f.value,bundle,descriptor,license FROM profiles "
            "CROSS JOIN LATERAL json_array_elements(descriptor->'fact_values') f"
        )
    return f"WITH profiles AS MATERIALIZED ({profiles}),witness AS MATERIALIZED ({values}) "


def witness_projection(schema, run_id, model, target_schema):
    """Use the installed model composite type for the shared binary COPY projection."""
    from process.source_profile_result_archive import _table

    kind = "records" if model is shared.ProviderProfileSourceRecord else "facts"
    _require(model in (shared.ProviderProfileSourceRecord, shared.ProviderProfileFact), "witness_model_invalid")
    composite = _table(target_schema, model.__tablename__)
    query = _witness_rows(schema, run_id, kind)
    return (
        f"SELECT model_row.* FROM ({query}SELECT value FROM witness) w "
        f"CROSS JOIN LATERAL json_populate_record(NULL::{composite},w.value) model_row"
    )


def _model_witness_shape(model, value, *, fact=False):
    """Reject JSON coercion and undeclared fields before comparing native model values."""
    columns = [column for column in model.__table__.columns if not (fact and column.name == "published_at")]
    names = ",".join("'" + column.name + "'" for column in columns)
    clauses = [
        f"json_typeof({value})='object'",
        f"(SELECT count(*)={len(columns)} AND count(DISTINCT key)={len(columns)} "
        f"AND bool_and(key=ANY(ARRAY[{names}])) FROM json_object_keys({value}) key)",
    ]
    for column in columns:
        field = f"{value}->'{column.name}'"
        if isinstance(column.type, JSON):
            continue
        if isinstance(column.type, Boolean):
            condition = f"json_typeof({field})='boolean'"
        elif isinstance(column.type, Integer):
            condition = f"json_typeof({field})='number' AND ({field})::text ~ '^-?(0|[1-9][0-9]*)$'"
        else:
            _require(isinstance(column.type, String), "witness_column_type_invalid")
            condition = f"json_typeof({field})='string'"
        if column.nullable:
            condition = f"json_typeof({field})='null' OR ({condition})"
        clauses.append(f"({condition})")
    return " AND ".join(clauses)


def _model_witness_row(model, alias, *, fact=False):
    from process.reference_family_archive import _quoted

    fields = [
        f"{alias}.{_quoted(column.name)}" + ("::text" if isinstance(column.type, JSON) else "")
        for column in model.__table__.columns
        if not (fact and column.name == "published_at")
    ]
    return "ROW(" + ",".join(fields) + ")"


async def is_canonical_model_equal(session, model, *, left, right, run_ids):
    """Keep canonical JSON text significant when comparing indexed NY model snapshots."""
    from process.source_profile_result_archive import _table, native

    native._require_transaction(session)
    left_table, right_table = _table(*left), _table(*right)
    keys = tuple(model.__table__.primary_key.columns)
    _require(bool(keys), "witness_primary_key_required")
    join = " AND ".join(f'l."{column.name}"=r."{column.name}"' for column in keys)
    left_rows = f"(SELECT * FROM {left_table} WHERE run_id=ANY(CAST(:runs AS text[])))"
    right_rows = f"(SELECT * FROM {right_table} WHERE run_id=ANY(CAST(:runs AS text[])))"
    return (
        await session.scalar(
            text(
                f"SELECT NOT EXISTS(SELECT 1 FROM {left_rows} l WHERE {_model_witness_row(model, 'l')} "
                f"IS DISTINCT FROM (SELECT {_model_witness_row(model, 'r')} FROM {right_rows} r WHERE {join})) "
                f"AND NOT EXISTS(SELECT 1 FROM {right_rows} r WHERE NOT EXISTS(SELECT 1 FROM {left_rows} l WHERE {join}))"
            ),
            {"runs": list(run_ids)},
        )
        is True
    )


async def witness_inventory_counts(session, schema, run):
    """Return only aggregates; payload comparison stays in indexed native set queries."""
    from process.source_profile_result_archive import _table

    counts_by_field = {}
    for model, key, kind in (
        (shared.ProviderProfileSourceRecord, "record_id", "records"),
        (shared.ProviderProfileFact, "fact_id", "facts"),
    ):
        table = _table(schema, model.__tablename__)
        witness = _witness_rows(schema, run["run_id"], kind)
        is_fact = kind == "facts"
        shape = _model_witness_shape(model, "w.value", fact=is_fact)
        invalid = await session.scalar(
            text(
                witness + f"SELECT count(*) FROM witness w WHERE ({shape}) IS DISTINCT FROM TRUE "
                "OR encode(sha256(convert_to(w.value::text,'UTF8')),'hex') IS DISTINCT FROM w.digest"
            )
        )
        _require(invalid == 0, "witness_canonical_values_invalid")
        equal = _model_witness_row(model, "r", fact=is_fact) + " IS NOT DISTINCT FROM "
        equal += _model_witness_row(model, "expected", fact=is_fact)
        invalid = await session.scalar(
            text(
                witness + f"SELECT (SELECT count(*) FROM witness w CROSS JOIN LATERAL "
                f"json_populate_record(NULL::{table},w.value) expected LEFT JOIN {table} r ON r.{key}=w.identity "
                f"AND r.run_id=:run WHERE r.{key} IS NULL OR NOT ({equal})) + "
                f"(SELECT count(*) FROM {table} r WHERE r.run_id=:run AND NOT EXISTS "
                f"(SELECT 1 FROM witness w WHERE w.identity=r.{key}))"
            ),
            {"run": run["run_id"]},
        )
        counts_by_field["invalid_bundle_" + kind] = invalid
    if run["status"] in shared.ACTIVE_STATUSES:
        counts_by_field["invalid_bundle_facts"] += await session.scalar(
            text(
                f"SELECT count(*) FROM {_table(schema, shared.ProviderProfileFact.__tablename__)} "
                "WHERE run_id=:run AND published_at IS NOT NULL"
            ),
            {"run": run["run_id"]},
        )
    counts_by_field["invalid_bundle_records"] += await _witness_lineage_errors(session, schema, run)
    return counts_by_field


async def _witness_lineage_errors(session, schema, run):
    """Keep independent capture/support evidence authoritative even after a row is rehashed."""
    from process.source_profile_result_archive import _table

    table = _table(schema, shared.ProviderProfileSourceRecord.__tablename__)
    binding = "r.match_evidence->'registry_binding'"
    capture = "r.normalized_payload->'profile_capture'"
    support = "w.bundle->'acquisition'->'nysed_support'->w.license"
    corroboration = f"{binding}->'nysed_corroboration'"
    checks = [
        "r.artifact_id=:artifact",
        "r.license_number=w.license",
        f"({binding}->'snapshot_sha256')::text=(w.bundle->'source_manifest'->'snapshot_sha256')::text",
        f"({binding}->>'npi')::bigint IS NOT DISTINCT FROM r.matched_npi",
        f"json_typeof({binding}->'npi')=CASE WHEN r.matched_npi IS NULL THEN 'null' ELSE 'number' END",
        f"{binding}->>'status'=r.match_status",
        f"json_typeof({binding}->'status')='string'",
        f"({binding}->'reason')::text=(w.descriptor->'reason')::text",
        "(w.descriptor->>'binding_outcome'='accepted')=(r.matched_npi IS NOT NULL)",
        f"{capture}->>'artifact_id'=w.descriptor->>'capture_artifact_id'",
        f"json_typeof({capture}->'artifact_id')='string'",
        f"(SELECT count(*)=5 FROM json_object_keys({capture}))",
    ]
    checks.extend(f"({capture}->'{field}')::text=(w.descriptor->'{field}')::text" for field in CAPTURE_FIELDS)
    checks.extend(
        f"({binding}->'{field}')::text=(w.descriptor->'{field}')::text"
        for field in ("manifest_sha256", "acquisition_sha256")
    )
    support_checks = [f"({corroboration}->'receipt_sha256')::text=({support}->'receipt_sha256')::text"]
    support_checks.extend(
        f"({corroboration}->'{field}')::text=({support}->'source_identity'->'{field}')::text"
        for field in NYSED_IDENTITY_FIELDS
    )
    support_checks.extend(
        f"json_typeof({corroboration}->'{field}')='boolean'"
        for field in ("license_matches", "header_legal_name_matches")
    )
    support_checks.append(
        f"(r.matched_npi IS NULL OR ({corroboration}->>'license_matches'='true' "
        f"AND {corroboration}->>'header_legal_name_matches'='true'))"
    )
    checks.append(
        f"CASE WHEN {support}->'receipt'->>'outcome'='acquired' THEN "
        f"{binding}->>'method'=:method AND ({' AND '.join(support_checks)}) ELSE "
        f"{binding}->>'method'='exact_ny_license_name_components' AND "
        f"COALESCE(json_typeof({corroboration}),'null')='null' END"
    )
    return await session.scalar(
        text(
            _witness_rows(schema, run["run_id"], "records") + f"SELECT count(*) FROM witness w "
            f"JOIN {table} r ON r.record_id=w.identity WHERE ({' AND '.join(checks)}) IS DISTINCT FROM TRUE"
        ),
        {"artifact": _hash([run["run_id"], SOURCE_KEY]), "method": CORROBORATED_METHOD},
    )


async def read_witness_bundle(session, schema, run, reference=None):
    """Authenticate the actual JSON bytes and the producer's closed retained inventory."""
    from process.source_profile_result_archive import _table

    _require(run["source_manifest"].get("bundle_contract") == WITNESS_CONTRACT, "witness_contract_required")
    artifact_rows = (
        (
            await session.execute(
                text(
                    f"SELECT a.*,encode(sha256(convert_to(metadata_json::text,'UTF8')),'hex') AS stored_sha256,"
                    f"octet_length(convert_to(metadata_json::text,'UTF8')) AS stored_bytes FROM "
                    f"{_table(schema, shared.ProviderProfileArtifact.__tablename__)} a WHERE run_id=:run"
                ),
                {"run": run["run_id"]},
            )
        )
        .mappings()
        .all()
    )
    _require(len(artifact_rows) == 1, "bundle_artifact_count_invalid")
    artifact_by_field = dict(artifact_rows[0])
    _require(
        artifact_by_field.pop("stored_sha256") == artifact_by_field["content_sha256"]
        and artifact_by_field.pop("stored_bytes") == artifact_by_field["content_bytes"],
        "bundle_canonical_bytes_changed",
    )
    observed = bundle_reference(artifact_by_field)
    if reference is not None:
        _require(validate_bundle_reference(reference, run["run_id"]) == observed, "bundle_reference_changed")
    return artifact_by_field, store._bundle(run, [artifact_by_field])


async def native_witness_counts(session, schema, run, bundle):
    """Scope the existing producer counts to one isolated or already sealed model family."""
    counts = (await store._retained_counts_by_run([run["run_id"]], schema=schema, session=session))[run["run_id"]]
    return {
        **counts,
        **await witness_inventory_counts(session, schema, run),
        **_bundle_counts(bundle),
    }


def _bundle_counts(bundle):
    return {
        "bundle_metrics": bundle["acquisition"],
        "acquired_profiles": sum(
            profile["acquisition_outcome"] == "acquired" for profile in bundle["profiles"].values()
        ),
        "held_attempts": sum(profile["acquisition_outcome"] == "held" for profile in bundle["profiles"].values()),
        "bundle_fact_count": sum(len(profile["facts"]) for profile in bundle["profiles"].values()),
    }


def _legacy_witness_fragments(run_id, kind, ordinal, content):
    """Carry canonical bytes in a disposable Artifact TEXT field, never as source authority."""
    columns = tuple(column.name for column in shared.ProviderProfileArtifact.__table__.columns)
    offset, part = 0, 0
    while offset < len(content):
        end = min(offset + _LEGACY_TRANSFER_BYTES - 512, len(content))
        while end < len(content) and content[end] & 0xC0 == 0x80:
            end -= 1
        identity = f"{kind}:{ordinal}:{part}"
        values_by_field = {
            "artifact_id": identity,
            "run_id": run_id,
            "source_key": identity,
            "file_name": str(ordinal),
            "source_url": content[offset:end].decode("utf-8"),
            "category": kind,
            "content_sha256": "",
            "content_bytes": end - offset,
            "header": str(part),
            "downloaded_at": None,
            "metadata_json": None,
        }
        values = tuple(values_by_field[column] for column in columns)
        size = sum(len(value.encode("utf-8")) if isinstance(value, str) else 8 for value in values)
        yield values, size
        offset, part = end, part + 1


def _legacy_metadata_values(content):
    """Injectively escape string data for JSON extraction, without changing digest preimages."""
    if isinstance(content, str):
        return content.replace("\\", "\\\\").replace("\u0000", "\\u0000")
    if isinstance(content, dict):
        return {_legacy_metadata_values(key): _legacy_metadata_values(value) for key, value in content.items()}
    if isinstance(content, list):
        return [_legacy_metadata_values(value) for value in content]
    return content


async def _legacy_witness_values(session, schema, run_id, bundle):
    """Only encode actual decoded model values; SQL makes every payload validation decision."""
    yield "bundle", 0, encoded_json(_legacy_metadata_values(bundle))
    for model, kind in ((shared.ProviderProfileSourceRecord, "records"), (shared.ProviderProfileFact, "facts")):
        table = model.__table__.to_metadata(MetaData(), schema=schema)
        statement = select(table).where(table.c.run_id == run_id).execution_options(yield_per=32)
        rows = await session.stream(statement)
        try:
            ordinal = 0
            async for stored in rows:
                values_by_field = {
                    key: value for key, value in stored._mapping.items() if kind != "facts" or key != "published_at"
                }
                yield kind, ordinal, encoded_json(values_by_field)
                # Opaque JSON remains digest-bound without JSON extraction rejecting a legal escaped NUL.
                opaque_fields = ("value_json", "source_json") if kind == "facts" else ("raw_payload",)
                typed_values_by_field = {
                    key: None if key in opaque_fields else value for key, value in values_by_field.items()
                }
                yield kind + "_fields", ordinal, encoded_json(_legacy_metadata_values(typed_values_by_field))
                ordinal += 1
        finally:
            await rows.close()


async def _copy_legacy_witness(session, source_schema, scratch_table, run_id, bundle):
    from process import reference_family_archive as native

    model = shared.ProviderProfileArtifact
    options_by_field = {
        "schema_name": "pg_temp",
        "table_name": scratch_table,
        "columns": tuple(column.name for column in model.__table__.columns),
    }
    batch, batch_bytes = [], 0
    async with aclosing(_legacy_witness_values(session, source_schema, run_id, bundle)) as rows:
        async for kind, ordinal, content in rows:
            for fragment, size in _legacy_witness_fragments(run_id, kind, ordinal, content):
                if batch and (len(batch) == _LEGACY_TRANSFER_ROWS or batch_bytes + size > _LEGACY_TRANSFER_BYTES):
                    await native.native_copy_record_batch(session, model, **options_by_field, records=batch)
                    batch, batch_bytes = [], 0
                batch.append(fragment)
                batch_bytes += size
        if batch:
            await native.native_copy_record_batch(session, model, **options_by_field, records=batch)


def _legacy_witness_rows(scratch_table, kind):
    from process.source_profile_result_archive import _table

    artifact = _table("pg_temp", scratch_table)
    expected = (
        "SELECT descriptor->>'record_id' AS identity,descriptor->>'record_sha256' AS digest,"
        "descriptor,snapshot,support,license FROM profiles WHERE descriptor->>'record_id' IS NOT NULL"
        if kind == "records"
        else "SELECT f.key AS identity,f.value AS digest FROM profiles "
        "CROSS JOIN LATERAL json_each_text(descriptor->'facts') f"
    )
    key = "record_id" if kind == "records" else "fact_id"
    return (
        f"WITH transferred AS MATERIALIZED (SELECT category,file_name,"
        f"string_agg(source_url,'' ORDER BY header::text::bigint) AS canonical FROM {artifact} "
        f"WHERE category IN ('bundle','{kind}','{kind}_fields') GROUP BY category,file_name),"
        "profiles AS MATERIALIZED (SELECT a.canonical::json->'source_manifest'->'snapshot_sha256' AS snapshot,"
        "a.canonical::json->'acquisition'->'nysed_support'->p.key AS support,"
        "p.key AS license,p.value AS descriptor FROM transferred a "
        "CROSS JOIN LATERAL json_each(CASE WHEN a.category='bundle' THEN a.canonical::json->'profiles' END) p),"
        f"expected AS MATERIALIZED ({expected}),witness AS MATERIALIZED (SELECT fields.canonical::json AS value,"
        f"fields.canonical::json->>'{key}' AS identity,actual.canonical FROM transferred actual "
        f"JOIN transferred fields USING(file_name) WHERE actual.category='{kind}' AND fields.category='{kind}_fields') "
    )


def _legacy_npi_equality(binding, record):
    """Preserve Python's decoded int/float/bool equality without rounding a large integer."""
    expected, observed = f"{record}->'matched_npi'", f"{binding}->'npi'"
    number = f"({observed})::text"
    floating = f"({number})::double precision"
    return (
        f"CASE WHEN ({expected})::text='null' THEN COALESCE(json_typeof({observed}),'null')='null' "
        f"WHEN json_typeof({observed})='boolean' THEN ({expected})::text="
        f"CASE WHEN ({observed})::text='true' THEN '1' ELSE '0' END "
        f"WHEN json_typeof({observed})='number' THEN CASE WHEN {number} ~ '^-?[0-9]+$' "
        f"THEN ({number})::numeric=(({expected})::text)::numeric "
        f"WHEN {floating}>=(-9223372036854775808)::double precision "
        f"AND {floating}<9223372036854775808::double precision THEN "
        f"{floating}=(({expected})::text)::double precision AND "
        f"({floating})::bigint=(({expected})::text)::bigint ELSE FALSE END ELSE FALSE END"
    )


def _legacy_lineage_checks():
    """Both operands are Python-encoded transfer values, not arbitrary stored JSON text."""
    record, descriptor = "w.value", "e.descriptor"
    binding = f"{record}->'match_evidence'->'registry_binding'"
    capture = f"{record}->'normalized_payload'->'profile_capture'"
    checks = [
        f"json_typeof({record}->'match_evidence')='object'",
        f"json_typeof({binding})='object'",
        f"json_typeof({record}->'normalized_payload')='object'",
        f"json_typeof({capture})='object'",
        f"(SELECT count(*)=5 FROM json_object_keys(CASE WHEN json_typeof({capture})='object' "
        f"THEN {capture} ELSE '{{}}'::json END))",
        f"({capture}->'artifact_id')::text=({descriptor}->'capture_artifact_id')::text",
        f"{record}->>'artifact_id'=:artifact",
        f"({record}->'license_number')::text=({descriptor}->'capture_manifest'->'license_number')::text",
        f"({binding}->'snapshot_sha256')::text=e.snapshot::text",
        f"({binding}->'status')::text=({record}->'match_status')::text",
        f"({binding}->'reason')::text=({descriptor}->'reason')::text",
        f"({descriptor}->>'binding_outcome'='accepted')=(({record}->'matched_npi')::text<>'null')",
        _legacy_npi_equality(binding, record),
    ]
    checks.extend(f"({capture}->'{field}')::text=({descriptor}->'{field}')::text" for field in CAPTURE_FIELDS)
    checks.extend(
        f"({binding}->'{field}')::text=({descriptor}->'{field}')::text"
        for field in ("manifest_sha256", "acquisition_sha256")
    )
    return " AND ".join(f"({check})" for check in checks)


def _legacy_support_checks():
    binding = "w.value->'match_evidence'->'registry_binding'"
    corroboration = f"{binding}->'nysed_corroboration'"
    support = "e.support"
    checks = [
        f"json_typeof({corroboration})='object'",
        f"({corroboration}->'receipt_sha256')::text=({support}->'receipt_sha256')::text",
    ]
    checks.extend(
        f"({corroboration}->'{field}')::text=({support}->'source_identity'->'{field}')::text"
        for field in NYSED_IDENTITY_FIELDS
    )
    checks.extend(
        f"json_typeof({corroboration}->'{field}')='boolean'"
        for field in ("license_matches", "header_legal_name_matches")
    )
    checks.append(
        f"((w.value->'matched_npi')::text='null' OR ({corroboration}->>'license_matches'='true' "
        f"AND {corroboration}->>'header_legal_name_matches'='true'))"
    )
    return (
        f"CASE WHEN {support}->'receipt'->>'outcome'='acquired' THEN {binding}->>'method'=:method "
        f"AND ({' AND '.join(checks)}) ELSE {binding}->>'method'='exact_ny_license_name_components' "
        f"AND COALESCE(json_typeof({corroboration}),'null')='null' END"
    )


async def _legacy_witness_counts(session, schema, source_schema, run):
    from process.source_profile_result_archive import _table

    counts_by_field = {}
    parameters_by_field = {
        "run": run["run_id"],
        "artifact": _hash([run["run_id"], SOURCE_KEY]),
        "method": CORROBORATED_METHOD,
    }
    for model, kind in ((shared.ProviderProfileSourceRecord, "records"), (shared.ProviderProfileFact, "facts")):
        prefix = _legacy_witness_rows(schema, kind)
        shape = _model_witness_shape(model, "w.value", fact=kind == "facts")
        invalid = f"CASE WHEN ({shape}) IS DISTINCT FROM TRUE OR w.value->>'run_id' IS DISTINCT FROM :run "
        invalid += "OR encode(sha256(convert_to(w.canonical,'UTF8')),'hex') IS DISTINCT FROM e.digest THEN 1 ELSE 0 END"
        if kind == "records":
            for predicate in (_legacy_lineage_checks(), _legacy_support_checks()):
                invalid += (
                    f" + CASE WHEN e.identity IS NOT NULL AND ({predicate}) IS DISTINCT FROM TRUE THEN 1 ELSE 0 END"
                )
        duplicates, errors = (
            await session.execute(
                text(
                    prefix + "SELECT (SELECT count(*)-count(DISTINCT identity) FROM expected),"
                    f"COALESCE((SELECT sum({invalid}) FROM witness w LEFT JOIN expected e USING(identity)),0) + "
                    "(SELECT count(*)-count(DISTINCT identity) FROM witness) + "
                    "(SELECT count(*) FROM expected e WHERE NOT EXISTS(SELECT 1 FROM witness w WHERE w.identity=e.identity))"
                ),
                parameters_by_field,
            )
        ).one()
        _require(duplicates == 0, "duplicate_fact_identity" if kind == "facts" else "duplicate_record_identity")
        counts_by_field["invalid_bundle_" + kind] = errors
    if run["status"] in shared.ACTIVE_STATUSES:
        counts_by_field["invalid_bundle_facts"] += await session.scalar(
            text(
                f"SELECT count(*) FROM {_table(source_schema, shared.ProviderProfileFact.__tablename__)} "
                "WHERE run_id=:run AND published_at IS NOT NULL"
            ),
            {"run": run["run_id"]},
        )
    return counts_by_field


def _legacy_witness_table(name):
    from process import reference_family_archive as native

    table = native._clone_model_table(shared.ProviderProfileArtifact.__table__, MetaData(), schema="pg_temp", name=name)
    table._prefixes.append("TEMPORARY")
    table.dialect_options["postgresql"]["on_commit"] = "DROP"
    for index, constraint in enumerate(sorted(table.constraints, key=lambda item: type(item).__name__)):
        constraint.name = f"{name}_key_{index}"
    return table


async def legacy_inventory_counts(session, schema, run, bundle):
    """Validate caller-fenced legacy rows in sets; connection-private scratch requires TEMP."""
    from process import reference_family_archive as native

    _require(bundle.get("bundle_contract") is None, "witness_requires_native_validation")
    native._require_transaction(session)
    scratch_name = "ny_legacy_witness_" + uuid4().hex
    table = _legacy_witness_table(scratch_name)
    native._defer_table_constraints(table)
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    async with session.begin_nested() as scratch:
        try:
            await session.execute(CreateTable(table))
            await _copy_legacy_witness(session, schema, scratch_name, run["run_id"], bundle)
            for backing_indexes in (True, False):
                await native._create_table_constraints(
                    session, _legacy_witness_table(scratch_name), backing_indexes=backing_indexes
                )
            # shortcut: native SHA256 needs a PostgreSQL-sized preimage; larger historical rows need a separate verifier.
            counts = await _legacy_witness_counts(session, scratch_name, schema, run)
            await scratch.rollback()
        except asyncio.CancelledError, TimeoutError:
            if not connection.invalidated and driver.is_closed():
                # COPY already terminated this connection; prevent savepoint cleanup from masking its interruption.
                await connection.invalidate()
            raise
    return counts


class NewYorkProfileStore(SourceProfileStore):
    """Use shared source/control transactions with a lossless per-root inventory."""

    def _manifest(self, run):
        manifest = super()._manifest(run)
        _require(
            set(manifest) - {"bundle_contract"}
            == {
                "control_run_id",
                "expected_current_run_id",
                "max_providers",
                "resume_from",
                "categories",
                "snapshot_sha256",
                "snapshot_row_count",
                "cohort_sha256",
                "full_cohort_licenses",
                "requested_licenses",
                "source",
            },
            "manifest_invalid",
        )
        _require(
            "bundle_contract" not in manifest or manifest["bundle_contract"] == WITNESS_CONTRACT,
            "bundle_contract_invalid",
        )
        _require(manifest["max_providers"] is None and manifest["resume_from"] is None, "complete_cohort_required")
        _require(
            isinstance(manifest["control_run_id"], str) and manifest["control_run_id"].strip(), "managed_run_required"
        )
        _require(
            _sha(manifest["snapshot_sha256"])
            and type(manifest["snapshot_row_count"]) is int
            and manifest["snapshot_row_count"] >= manifest["full_cohort_licenses"],
            "snapshot_invalid",
        )
        _require(
            manifest["source"]
            == {
                "source_key": SOURCE_KEY,
                "source_kind": "state_regulator",
                "jurisdiction": "NY",
                "agency": "New York State Department of Health",
                "source_url": SOURCE_URL,
                "coverage_scope": COVERAGE_SCOPE,
                "registry_generation": manifest["snapshot_sha256"],
            },
            "manifest_source_invalid",
        )
        return manifest

    def _bundle(self, run, artifacts):
        manifest = self._manifest(run)
        _require(len(artifacts) == 1, "bundle_artifact_count_invalid")
        artifact = artifacts[0]
        bundle = artifact.get("metadata_json")
        _require(
            artifact.get("artifact_id") == _hash([run["run_id"], SOURCE_KEY])
            and artifact.get("run_id") == run["run_id"]
            and artifact.get("source_key") == SOURCE_KEY
            and artifact.get("category") == "profile"
            and artifact.get("file_name") == "manifest.json"
            and artifact.get("source_url") == SOURCE_URL
            and isinstance(bundle, dict)
            and set(bundle) - {"bundle_contract"}
            == {"schema_version", "run_id", "source_manifest", "cohort", "profiles", "acquisition"}
            and bundle.get("bundle_contract") == manifest.get("bundle_contract")
            and bundle["schema_version"] == SCHEMA_VERSION
            and bundle["run_id"] == run["run_id"]
            and bundle["source_manifest"] == manifest
            and artifact.get("content_sha256") == _hash(bundle)
            and artifact.get("content_bytes") == len(encoded_json(bundle)),
            "bundle_artifact_invalid",
        )
        roots = _cohort_roots(bundle["cohort"], manifest)
        profiles = bundle["profiles"]
        _require(isinstance(profiles, dict) and set(profiles) == set(roots), "attempt_inventory_incomplete")
        for license_number, descriptor_by_field in profiles.items():
            _validate_descriptor(
                descriptor_by_field,
                run["run_id"],
                license_number,
                witnessed=bundle.get("bundle_contract") == WITNESS_CONTRACT,
            )
        _validate_nysed_inventory(bundle["acquisition"], profiles, run["run_id"])
        return bundle

    async def _inventory_counts(self, run, bundle, *, session=None, schema=None):
        _require(bundle.get("bundle_contract") is None, "witness_requires_native_validation")
        return await legacy_inventory_counts(session, schema, run, bundle)

    async def _legacy_retained_counts(self, run_id):
        from process import reference_family_archive as native

        schema = shared.ProviderProfileArtifact.__table__.schema or "mrf"
        async with shared.db.transaction() as session:
            await self._lock_source()
            await native._lock_family(
                session,
                schema,
                tuple(
                    model.__tablename__
                    for model in (
                        shared.ProviderProfileImportRun,
                        shared.ProviderProfileArtifact,
                        shared.ProviderProfileSourceRecord,
                        shared.ProviderProfileFact,
                    )
                ),
                "SHARE",
            )
            run = await self._read_run(run_id)
            counts = (await self._retained_counts_by_run([run_id], schema=schema, session=session))[run_id]
            table = shared.ProviderProfileArtifact.__table__
            artifacts = await shared.db.all(select(table).where(table.c.run_id == run_id))
            bundle = self._bundle(run, [dict(artifact._mapping) for artifact in artifacts])
            return {
                **counts,
                **await self._inventory_counts(run, bundle, session=session, schema=schema),
                **_bundle_counts(bundle),
            }

    async def retained_counts(self, run_id):
        """Reconcile complete retained models under the bundle's original validation contract."""
        run = await self._read_run(run_id)
        if run["source_manifest"].get("bundle_contract") == WITNESS_CONTRACT:
            async with shared.db.transaction() as session:
                schema = shared.ProviderProfileArtifact.__table__.schema or "mrf"
                artifact, bundle = await read_witness_bundle(session, schema, run)
                return {
                    **await native_witness_counts(session, schema, run, bundle),
                    "bundle_reference": bundle_reference(artifact),
                }
        return await self._legacy_retained_counts(run_id)

    def _acquisition_metrics(self, run, metrics, counts):
        """A witnessed completion carries the artifact reference, never its per-license support inventory."""
        if run["source_manifest"].get("bundle_contract") == WITNESS_CONTRACT:
            reference = validate_bundle_reference(metrics.get("bundle"), run["run_id"])
            _require(reference == counts.get("bundle_reference"), "bundle_reference_changed")
            return {
                **{key: metric for key, metric in counts["bundle_metrics"].items() if key != "nysed_support"},
                "bundle": reference,
            }
        return counts["bundle_metrics"]

    def _completion_metrics(self, run, metrics, counts):
        manifest = self._manifest(run)
        if not isinstance(metrics, dict) or metrics.get("acquisition_complete") is not True:
            raise RuntimeError("new_york_profile_acquisition_incomplete")
        if type(metrics.get("transport_failures")) is not int or metrics["transport_failures"] != 0:
            raise RuntimeError("new_york_profile_transport_failures")
        if (
            _hash(metrics) != _hash(self._acquisition_metrics(run, metrics, counts))
            or type(metrics.get("responses")) is not int
            or metrics["responses"] != manifest["requested_licenses"]
            or counts["acquired_profiles"] + counts["held_attempts"] != metrics["responses"]
            or counts["retained_source_records"] != counts["acquired_profiles"]
            or counts["received_profiles"] != counts["acquired_profiles"]
            or counts["retained_facts"] != counts["bundle_fact_count"]
            or metrics.get("cohort_sha256") != manifest["cohort_sha256"]
            or type(metrics.get("invalid_nysed_supports", 0)) is not int
            or metrics.get("invalid_nysed_supports", 0)
            != sum(
                support["receipt"]["outcome"] == "invalid"
                for support in counts["bundle_metrics"]["nysed_support"].values()
            )
            or any(
                type(metrics.get(key)) is not int or metrics[key] != counts[count_key]
                for key, count_key in (
                    ("acquired_profiles", "acquired_profiles"),
                    ("held_attempts", "held_attempts"),
                    ("facts", "retained_facts"),
                )
            )
        ):
            raise RuntimeError("new_york_profile_retained_count_mismatch")
        if any(
            counts[key]
            for key in (
                "invalid_source_records",
                "invalid_facts",
                "foreign_artifacts",
                "invalid_bundle_records",
                "invalid_bundle_facts",
            )
        ):
            raise RuntimeError("new_york_profile_retained_integrity_invalid")
        return {
            **{key: metric for key, metric in metrics.items() if key != "nysed_support"},
            **{key: count for key, count in counts.items() if key not in {"bundle_metrics", "bundle_reference"}},
            "requested_licenses": manifest["requested_licenses"],
            "full_cohort_licenses": manifest["full_cohort_licenses"],
            "coverage_scope": COVERAGE_SCOPE,
        }

    def _publication_volume(self, metrics, incumbent_metrics):
        if any(metrics[key] <= 0 for key in ("acquired_profiles", "retained_facts", "matched_public_providers")):
            raise RuntimeError("new_york_profile_empty_publication")
        if incumbent_metrics is not None:
            for key in ("acquired_profiles", "retained_facts", "matched_public_providers"):
                if metrics[key] * 5 < incumbent_metrics[key] * 4:
                    raise RuntimeError("new_york_profile_publication_volume_drop:" + key)


store = NewYorkProfileStore(
    ProfileSourcePolicy(
        source_key=SOURCE_KEY,
        schema_version=SCHEMA_VERSION,
        jurisdiction="NY",
        categories=CATEGORIES,
        error_prefix="new_york_profile",
        received_profile_sql="normalized_payload->>'visibility' = 'public' AND json_typeof(raw_payload->'data') = 'object'",
        invalid_fact_sql="""(f.category = 'education' AND f.fact_type = 'education_history'
        OR f.category = 'training' AND f.fact_type = 'postgraduate_training'
        OR f.category = 'certifications' AND f.fact_type = 'board_certification') IS DISTINCT FROM TRUE
        OR f.source_json->>'run_id' IS DISTINCT FROM f.run_id
        OR f.source_json->>'artifact_id' IS DISTINCT FROM r.artifact_id
        OR f.source_json->>'agency' IS DISTINCT FROM 'New York State Department of Health'
        OR f.source_json->>'jurisdiction' IS DISTINCT FROM 'NY'
        OR r.match_evidence->'registry_binding'->>'snapshot_sha256'
            IS DISTINCT FROM source_run.source_manifest->>'snapshot_sha256'
        OR r.match_evidence->'registry_binding'->>'status' IS DISTINCT FROM r.match_status""",
    )
)
completion = SourceProfileCompletion(store, IMPORTER)
