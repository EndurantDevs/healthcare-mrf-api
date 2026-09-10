# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate and copy a completed BORIM acquisition without opening a transport."""

from __future__ import annotations

import hashlib

from process import massachusetts_profile_acquisition as acquisition
from process.massachusetts_profile_rows import SCHEMA_VERSION, SOURCE_KEY


def envelope_hash(response):
    """Hash the complete canonical response envelope, including observation time."""
    return hashlib.sha256(acquisition.encoded_json(response)).hexdigest()


def checked_response(path, license_number, expected_hash=None):
    """Read a valid original response and reject changes after preflight."""
    if not path.is_file():
        raise ValueError("massachusetts_profile_retained_response_missing")
    response = acquisition.read_response(path, license_number)
    if expected_hash is not None and envelope_hash(response) != expected_hash:
        raise ValueError("massachusetts_profile_retained_envelope_changed")
    return response


def _parent_manifest(directory, parent, artifact):
    path = directory / "manifest.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError("massachusetts_profile_retained_manifest_invalid")
    body = path.read_bytes()
    if len(body) != artifact["content_bytes"] or hashlib.sha256(body).hexdigest() != artifact["content_sha256"]:
        raise ValueError("massachusetts_profile_retained_manifest_changed")
    manifest = acquisition.decoded_json_object(body)
    if (
        artifact["source_key"] != SOURCE_KEY
        or artifact["run_id"] != parent["run_id"]
        or artifact["file_name"] != "manifest.json"
        or artifact["category"] != "profile"
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("run_id") != parent["run_id"]
        or manifest.get("source_manifest") != parent["source_manifest"]
    ):
        raise ValueError("massachusetts_profile_retained_manifest_identity_invalid")
    metrics = manifest.get("acquisition")
    required_fields = {
        "responses",
        "response_bytes",
        "reused_responses",
        "responses_sha256",
        "acquisition_complete",
        "transport_failures",
    }
    if (
        not isinstance(metrics, dict)
        or not required_fields <= metrics.keys()
        or any(parent["metrics"].get(key) != metric_value for key, metric_value in metrics.items())
        or metrics["acquisition_complete"] is not True
        or metrics["transport_failures"] != 0
    ):
        raise ValueError("massachusetts_profile_retained_metrics_invalid")
    if (
        type(metrics["response_bytes"]) is not int
        or metrics["response_bytes"] < 0
        or type(metrics["reused_responses"]) is not int
        or not 0 <= metrics["reused_responses"] <= metrics["responses"]
    ):
        raise ValueError("massachusetts_profile_retained_metrics_invalid")
    return metrics


async def validate_acquisition(directory, parent, artifact, progress):
    """Bind every original envelope to the completed manifest before a new claim."""
    metrics = _parent_manifest(directory, parent, artifact)
    cohort = acquisition.read_cohort(directory / "cohort.json")
    if hashlib.sha256(acquisition.encoded_json(cohort)).hexdigest() != parent["source_manifest"]["cohort_sha256"]:
        raise ValueError("massachusetts_profile_retained_cohort_changed")
    roots = cohort["roots"]
    if len(roots) != parent["source_manifest"]["full_cohort_licenses"] or len(roots) != metrics["responses"]:
        raise ValueError("massachusetts_profile_retained_count_mismatch")
    profiles = directory / "profiles"
    if profiles.is_symlink() or {entry.name for entry in profiles.iterdir()} != {
        f"{root['license_number']}.json" for root in roots
    }:
        raise ValueError("massachusetts_profile_retained_files_incomplete")
    hashes_by_license = {}
    response_hash = hashlib.sha256()
    envelope_digest = hashlib.sha256()
    total_bytes = 0
    for index, root in enumerate(roots):
        await progress(index, len(roots))
        license_number = root["license_number"]
        response = checked_response(profiles / f"{license_number}.json", license_number)
        total_bytes += len(response["body_text"].encode("utf-8"))
        if total_bytes > acquisition.MAX_ACQUISITION_BYTES:
            raise ValueError("massachusetts_profile_acquisition_too_large")
        response_hash.update(
            acquisition.encoded_json([license_number, response["content_sha256"], response["downloaded_at"]])
        )
        hashes_by_license[license_number] = envelope_hash(response)
        envelope_digest.update(acquisition.encoded_json([license_number, hashes_by_license[license_number]]))
    if (
        total_bytes != metrics["response_bytes"]
        or response_hash.hexdigest() != metrics["responses_sha256"]
        or (
            "response_envelopes_sha256" in metrics
            and envelope_digest.hexdigest() != metrics["response_envelopes_sha256"]
        )
    ):
        raise ValueError("massachusetts_profile_retained_acquisition_changed")
    await progress(len(roots), len(roots))
    return cohort, {
        "lineage": {
            "source_run_id": parent["run_id"],
            "artifact_id": artifact["artifact_id"],
            "manifest_sha256": artifact["content_sha256"],
            "response_envelopes_sha256": envelope_digest.hexdigest(),
        },
        "hashes_by_license": hashes_by_license,
    }


async def copy_profiles(roots, retained, destination, hashes_by_license, progress):
    """Copy only validated original responses; a missing response is never fetched."""
    destination.mkdir(exist_ok=False)
    total_bytes = 0
    response_hash = hashlib.sha256()
    envelope_digest = hashlib.sha256()
    for index, root in enumerate(roots):
        await progress(index, len(roots))
        license_number = root["license_number"]
        response = checked_response(
            retained / f"{license_number}.json", license_number, hashes_by_license[license_number]
        )
        total_bytes += len(response["body_text"].encode("utf-8"))
        if total_bytes > acquisition.MAX_ACQUISITION_BYTES:
            raise ValueError("massachusetts_profile_acquisition_too_large")
        acquisition.write_new_json(destination / f"{license_number}.json", response)
        response_hash.update(
            acquisition.encoded_json([license_number, response["content_sha256"], response["downloaded_at"]])
        )
        envelope_digest.update(acquisition.encoded_json([license_number, hashes_by_license[license_number]]))
    await progress(len(roots), len(roots))
    return {
        "responses": len(roots),
        "response_bytes": total_bytes,
        "reused_responses": len(roots),
        "responses_sha256": response_hash.hexdigest(),
        "response_envelopes_sha256": envelope_digest.hexdigest(),
    }
