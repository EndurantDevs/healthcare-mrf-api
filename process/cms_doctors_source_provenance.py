# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical source evidence from the verified, retained Doctors distribution."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime

_CONTRACT = "cms-doctors-prepared-source.v1"
_DOMAIN = b"cms-doctors-prepared-source-v1\0"
_ARTIFACT_FIELDS = ("source_url", "content_sha256", "content_bytes", "file_name")
_EDUCATION_FIELDS = (
    "source_key",
    "dataset_id",
    "schema_version",
    "source_url",
    "content_sha256",
    "downloaded_at",
    "generation_id",
    "source_rows",
    "education_rows",
)


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _source_payload(metrics: dict) -> dict:
    """Copy only verified source identities and completed source preparation counts."""
    try:
        artifact_by_field = {name: metrics["artifact"][name] for name in _ARTIFACT_FIELDS}
        education_by_field = {name: metrics["education"][name] for name in _EDUCATION_FIELDS}
        group_by_field = {name: metrics["group_site"][name] for name in ("generation_id", "source_rows")}
        counts_by_name = {name: metrics[name] for name in ("rows", "organization_groups", "sites")}
    except KeyError, TypeError:
        raise ValueError("cms_doctors_source_provenance_incomplete") from None
    digest = artifact_by_field["content_sha256"]
    if (
        not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        or artifact_by_field["file_name"] not in (digest + ".csv", digest + ".zip")
        or type(artifact_by_field["content_bytes"]) is not int
        or artifact_by_field["content_bytes"] <= 0
        or not isinstance(artifact_by_field["source_url"], str)
        or not artifact_by_field["source_url"].strip()
        or education_by_field["source_key"] != "cms-doctors"
        or education_by_field["schema_version"] != "cms-doctor-education/v1"
        or not isinstance(education_by_field["dataset_id"], str)
        or not education_by_field["dataset_id"].strip()
        or education_by_field["source_url"] != artifact_by_field["source_url"]
        or education_by_field["content_sha256"] != digest
    ):
        raise ValueError("cms_doctors_source_provenance_mismatch")
    try:
        datetime.fromisoformat(education_by_field["downloaded_at"])
    except TypeError, ValueError:
        raise ValueError("cms_doctors_source_download_time_invalid") from None
    generation_by_field = {
        name: education_by_field[name] for name in ("source_key", "dataset_id", "schema_version", "content_sha256")
    }
    generation_id = hashlib.sha256(json.dumps(generation_by_field, sort_keys=True).encode()).hexdigest()
    if education_by_field["generation_id"] != generation_id or group_by_field["generation_id"] != generation_id:
        raise ValueError("cms_doctors_source_generation_mismatch")
    source_counts = (
        *counts_by_name.values(),
        education_by_field["source_rows"],
        education_by_field["education_rows"],
        group_by_field["source_rows"],
    )
    if (
        any(type(count) is not int or count < 0 for count in source_counts)
        or education_by_field["source_rows"] <= 0
        or counts_by_name["rows"] <= 0
        or education_by_field["source_rows"] != group_by_field["source_rows"]
    ):
        raise ValueError("cms_doctors_source_counts_invalid")
    return {
        "contract_id": _CONTRACT,
        "artifact": artifact_by_field,
        "education": education_by_field,
        "group_site": group_by_field,
        **counts_by_name,
    }


def mint_doctors_source_provenance(metrics: dict) -> str:
    """Freeze the real source validator's result, never a predicted publication identity."""
    return _canonical(_source_payload(metrics))


def read_doctors_source_provenance(value: str) -> dict:
    """Require the exact closed canonical evidence minted during source preparation."""
    try:
        payload = json.loads(value)
    except TypeError, ValueError:
        raise ValueError("cms_doctors_source_provenance_required") from None
    if not isinstance(payload, dict) or _canonical(_source_payload(payload)) != value:
        raise ValueError("cms_doctors_source_provenance_changed")
    return payload


def doctors_source_provenance_digest(value: str) -> str:
    """Identify retained source evidence separately from local stage physical identities."""
    read_doctors_source_provenance(value)
    return hashlib.sha256(_DOMAIN + value.encode("ascii")).hexdigest()
