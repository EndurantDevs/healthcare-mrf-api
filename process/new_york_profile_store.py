# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Publish a complete supported NYPP cohort with exact retained capture lineage."""

from __future__ import annotations

import copy
import hashlib
import re
from contextlib import aclosing

from sqlalchemy import select

from process import new_york_nysed_profile as nysed
from process import provider_profile_source_store as shared
from process.massachusetts_profile_acquisition import encoded_json
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


def _require(condition, reason):
    if not condition:
        raise ValueError("new_york_profile_" + reason)


def _sha(digest):
    return isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest) is not None


def _hash(content):
    return hashlib.sha256(encoded_json(content)).hexdigest()


def _fact_hash(fact):
    return _hash({key: content for key, content in fact.items() if key != "published_at"})


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
                "reason": "search_not_singleton",
                "reported_total": total,
                "source_record": None,
                "facts": [],
            }
            and type(total) is int
            and total >= 0
            and total != 1,
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


def _validate_descriptor(descriptor_by_field, run_id, license_number):
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
        },
        "attempt_descriptor_invalid",
    )
    is_held = descriptor_by_field["acquisition_outcome"] == "held"
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
            and descriptor_by_field["reason"] == "search_not_singleton"
            and type(descriptor_by_field["reported_total"]) is int
            and descriptor_by_field["reported_total"] >= 0
            and descriptor_by_field["reported_total"] != 1
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


def _has_matching_record_lineage(record, descriptor_by_field, manifest):
    capture_by_field = {key: descriptor_by_field[key] for key in CAPTURE_FIELDS}
    if not isinstance(record.get("match_evidence"), dict) or not isinstance(record.get("normalized_payload"), dict):
        return False
    binding = record["match_evidence"].get("registry_binding", {})
    if not isinstance(binding, dict):
        return False
    return (
        record.get("normalized_payload", {}).get("profile_capture")
        == {**capture_by_field, "artifact_id": descriptor_by_field["capture_artifact_id"]}
        and record.get("artifact_id") == _hash([record["run_id"], SOURCE_KEY])
        and record.get("license_number") == descriptor_by_field["capture_manifest"]["license_number"]
        and binding.get("snapshot_sha256") == manifest["snapshot_sha256"]
        and binding.get("manifest_sha256") == descriptor_by_field["manifest_sha256"]
        and binding.get("acquisition_sha256") == descriptor_by_field["acquisition_sha256"]
        and binding.get("status") == record.get("match_status")
        and binding.get("npi") == record.get("matched_npi")
        and binding.get("reason") == descriptor_by_field["reason"]
        and descriptor_by_field["binding_outcome"] == ("accepted" if record.get("matched_npi") is not None else "held")
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
        and set(files) == {"manifest.json", "request.json", "response.json", "result.json"}
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
        outcome in {"acquired", "held"}
        and set(receipt) == fields | ({"reason"} if outcome == "held" else set())
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


def _has_matching_nysed_support(record, support):
    if not isinstance(record.get("match_evidence"), dict):
        return False
    binding = record["match_evidence"].get("registry_binding")
    if not isinstance(binding, dict):
        return False
    corroboration = binding.get("nysed_corroboration")
    if support["receipt"]["outcome"] == "held":
        return binding.get("method") == "exact_ny_license_name_components" and corroboration is None
    return (
        binding.get("method") == CORROBORATED_METHOD
        and isinstance(corroboration, dict)
        and corroboration.get("receipt_sha256") == support["receipt_sha256"]
        and all(corroboration.get(field) == support["source_identity"][field] for field in NYSED_IDENTITY_FIELDS)
        and all(type(corroboration.get(field)) is bool for field in ("license_matches", "header_legal_name_matches"))
        and (
            record.get("matched_npi") is None
            or corroboration["license_matches"]
            and corroboration["header_legal_name_matches"]
        )
    )


class NewYorkProfileStore(SourceProfileStore):
    """Use shared source/control transactions with a lossless per-root inventory."""

    def _manifest(self, run):
        manifest = super()._manifest(run)
        _require(
            set(manifest)
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
            and set(bundle) == {"schema_version", "run_id", "source_manifest", "cohort", "profiles", "acquisition"}
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
            _validate_descriptor(descriptor_by_field, run["run_id"], license_number)
        _validate_nysed_inventory(bundle["acquisition"], profiles, run["run_id"])
        return bundle

    async def _inventory_counts(self, run, bundle):
        records_by_id = {
            descriptor_by_field["record_id"]: descriptor_by_field
            for descriptor_by_field in bundle["profiles"].values()
            if descriptor_by_field["record_id"] is not None
        }
        facts_by_id = {
            fact_id: digest
            for descriptor_by_field in records_by_id.values()
            for fact_id, digest in descriptor_by_field["facts"].items()
        }
        _require(
            len(facts_by_id)
            == sum(len(descriptor_by_field["facts"]) for descriptor_by_field in records_by_id.values()),
            "duplicate_fact_identity",
        )
        counts_by_field = {}
        for model, expected, identifier, hash_row, label in (
            (
                shared.ProviderProfileSourceRecord,
                {key: descriptor_by_field["record_sha256"] for key, descriptor_by_field in records_by_id.items()},
                "record_id",
                _hash,
                "records",
            ),
            (shared.ProviderProfileFact, facts_by_id, "fact_id", _fact_hash, "facts"),
        ):
            table = model.__table__
            seen, invalid = set(), 0
            async with aclosing(shared.db.select(table).where(table.c.run_id == run["run_id"]).iterate()) as retained:
                async for stored_row in retained:
                    stored_by_field = dict(stored_row._mapping)
                    key = stored_by_field[identifier]
                    invalid += key in seen or expected.get(key) != hash_row(stored_by_field)
                    if label == "records" and key in records_by_id:
                        invalid += not _has_matching_record_lineage(
                            stored_by_field, records_by_id[key], run["source_manifest"]
                        )
                        license_number = records_by_id[key]["capture_manifest"]["license_number"]
                        invalid += not _has_matching_nysed_support(
                            stored_by_field, bundle["acquisition"]["nysed_support"][license_number]
                        )
                    if (
                        label == "facts"
                        and run["status"] in shared.ACTIVE_STATUSES
                        and stored_by_field["published_at"] is not None
                    ):
                        invalid += 1
                    seen.add(key)
            counts_by_field["invalid_bundle_" + label] = invalid + len(expected.keys() - seen)
        return counts_by_field

    async def retained_counts(self, run_id):
        """Reconcile the bundle against complete streamed SQL records and facts."""
        counts = await super().retained_counts(run_id)
        run = await self._read_run(run_id)
        table = shared.ProviderProfileArtifact.__table__
        artifacts = await shared.db.all(select(table).where(table.c.run_id == run_id))
        bundle = self._bundle(run, [dict(artifact._mapping) for artifact in artifacts])
        return {
            **counts,
            **await self._inventory_counts(run, bundle),
            "bundle_metrics": bundle["acquisition"],
            "acquired_profiles": sum(
                profile["acquisition_outcome"] == "acquired" for profile in bundle["profiles"].values()
            ),
            "held_attempts": sum(profile["acquisition_outcome"] == "held" for profile in bundle["profiles"].values()),
            "bundle_fact_count": sum(len(profile["facts"]) for profile in bundle["profiles"].values()),
        }

    def _completion_metrics(self, run, metrics, counts):
        manifest = self._manifest(run)
        if not isinstance(metrics, dict) or metrics.get("acquisition_complete") is not True:
            raise RuntimeError("new_york_profile_acquisition_incomplete")
        if type(metrics.get("transport_failures")) is not int or metrics["transport_failures"] != 0:
            raise RuntimeError("new_york_profile_transport_failures")
        if (
            _hash(metrics) != _hash(counts["bundle_metrics"])
            or type(metrics.get("responses")) is not int
            or metrics["responses"] != manifest["requested_licenses"]
            or counts["acquired_profiles"] + counts["held_attempts"] != metrics["responses"]
            or counts["retained_source_records"] != counts["acquired_profiles"]
            or counts["received_profiles"] != counts["acquired_profiles"]
            or counts["retained_facts"] != counts["bundle_fact_count"]
            or metrics.get("cohort_sha256") != manifest["cohort_sha256"]
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
            **{key: count for key, count in counts.items() if key != "bundle_metrics"},
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
