# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Local reviewed-source catalog for manual current-version FHIR censuses."""

from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from process.provider_directory_fhir_census_binding import (
    _normalized_base_url,
    _validate_reviewed_start_url,
)
from process.provider_directory_fhir_census_contract import (
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
)
from process.provider_directory_fhir_manual_seed import (
    MANUAL_SOURCE_PENDING_STATUS,
    MANUAL_SOURCE_VERIFICATION_CAMPAIGN_FIELD,
    manual_seed_metadata,
)
from process.provider_directory_fhir_root_policy import ReviewedRootPolicy


DEFAULT_MANUAL_SOURCE_MANIFEST = Path(__file__).resolve().parents[1] / (
    "specs/provider_directory_endpoint_acquisition_manifest.json"
)
MANUAL_ACQUISITION_CLASSIFICATION = "manual_acquisition"
MANUAL_ACQUISITION_LAUNCH_MODE = "manual"
MANUAL_CURRENT_VERSION_CENSUS_FIELD = "manual_current_version_census"
MANUAL_RESOURCE_PROFILE = "A7"
MANUAL_CURRENT_VERSION_CENSUS_RESOURCES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
)
_MANUAL_ENTRY_FIELDS = frozenset(
    {
        "entry_id",
        "display_name",
        "owner_id",
        "source_ids",
        "canonical_base",
        "classification",
        "launch_mode",
        "resource_profile",
        "resources",
        MANUAL_CURRENT_VERSION_CENSUS_FIELD,
    }
)
_MANUAL_CENSUS_FIELDS = frozenset(
    {
        "contract_version",
        "semantics",
        "strategy_version",
        "traversal_version",
        "canonicalization_version",
        "completion_scopes",
        "plan_name",
        "seed_source",
        "continuation_strategy",
        "expected_nonempty_resources",
        "page_count",
        "verification_campaign_id",
        "start_urls",
    }
)
_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")


def _manifest_error(reason: str) -> RuntimeError:
    return RuntimeError(f"provider_directory_manual_source_manifest_invalid:{reason}")


def _strict_text(value: Any, *, field_name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise _manifest_error(f"{field_name}_invalid")
    return value


def _strict_slug(value: Any, *, field_name: str) -> str:
    text = _strict_text(value, field_name=field_name)
    if _SLUG_RE.fullmatch(text) is None:
        raise _manifest_error(f"{field_name}_invalid")
    return text


def _has_persisted_cutoff(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            "cutoff" in str(key).lower()
            or "lastupdated" in str(key).lower().replace("_", "")
            or _has_persisted_cutoff(nested_value)
            for key, nested_value in value.items()
        )
    if isinstance(value, list):
        return any(_has_persisted_cutoff(item) for item in value)
    return False


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate manifest members at every JSON object depth."""

    object_by_field: dict[str, Any] = {}
    for field_name, value in pairs:
        if field_name in object_by_field:
            raise ValueError("duplicate manifest field")
        object_by_field[field_name] = value
    return object_by_field


def _load_manifest(manifest_path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise _manifest_error("document_unreadable") from exc
    if (
        type(manifest) is not dict
        or type(manifest.get("schema_version")) is not int
        or manifest.get("schema_version") != 1
        or manifest.get("importer") != "provider-directory-fhir"
        or type(manifest.get("entries")) is not list
    ):
        raise _manifest_error("document_shape")
    return manifest


def _manual_entry_for_source(
    manifest: Mapping[str, Any],
    requested_source_id: str,
) -> dict[str, Any]:
    matching_entries = []
    source_owner_count = 0
    for raw_entry in manifest["entries"]:
        if type(raw_entry) is not dict:
            raise _manifest_error("entry_shape")
        raw_source_ids = raw_entry.get("source_ids")
        if type(raw_source_ids) is not list:
            raise _manifest_error("source_ids_invalid")
        if requested_source_id in raw_source_ids:
            source_owner_count += 1
        if raw_entry.get("classification") != MANUAL_ACQUISITION_CLASSIFICATION:
            continue
        if requested_source_id in raw_source_ids:
            matching_entries.append(raw_entry)
    if len(matching_entries) != 1 or source_owner_count != 1:
        raise _manifest_error("source_resolution_ambiguous")
    return matching_entries[0]


def _validated_base(raw_value: Any) -> str:
    canonical_base = _strict_text(raw_value, field_name="canonical_base")
    try:
        parsed_base = _normalized_base_url(canonical_base)
    except ValueError as exc:
        raise _manifest_error("canonical_base_invalid") from exc
    if (
        parsed_base.query
        or not parsed_base.path
        or parsed_base.path == "/"
        or parsed_base.path.endswith("/")
        or canonical_base
        != urllib.parse.urlunsplit(
            (
                "https",
                parsed_base.netloc.lower(),
                parsed_base.path,
                "",
                "",
            )
        )
    ):
        raise _manifest_error("canonical_base_invalid")
    return canonical_base


def _strict_resources(raw_value: Any, *, field_name: str) -> tuple[str, ...]:
    if type(raw_value) is not list or any(
        type(resource_type) is not str for resource_type in raw_value
    ):
        raise _manifest_error(f"{field_name}_invalid")
    resources = tuple(raw_value)
    if resources != MANUAL_CURRENT_VERSION_CENSUS_RESOURCES:
        raise _manifest_error(f"{field_name}_invalid")
    return resources


def _validated_start_urls(
    raw_value: Any,
    *,
    canonical_base: str,
    resources: tuple[str, ...],
) -> dict[str, str]:
    if type(raw_value) is not dict or set(raw_value) != set(resources):
        raise _manifest_error("start_urls_invalid")
    parsed_base = _normalized_base_url(canonical_base)
    start_url_by_resource: dict[str, str] = {}
    for resource_type in resources:
        raw_url = raw_value[resource_type]
        try:
            reviewed_url = _validate_reviewed_start_url(
                raw_url,
                canonical_base=parsed_base,
                resource_type=resource_type,
            )
        except ValueError as exc:
            raise _manifest_error("start_urls_invalid") from exc
        if reviewed_url != f"{canonical_base}/{resource_type}":
            raise _manifest_error("start_urls_invalid")
        start_url_by_resource[resource_type] = reviewed_url
    return start_url_by_resource


def _validated_manual_config(
    raw_entry: Mapping[str, Any],
    *,
    canonical_base: str,
    resources: tuple[str, ...],
) -> dict[str, Any]:
    """Validate the reviewed v3 acquisition configuration."""

    raw_config = raw_entry.get(MANUAL_CURRENT_VERSION_CENSUS_FIELD)
    if type(raw_config) is not dict:
        raise _manifest_error("manual_contract_shape")
    if _has_persisted_cutoff(raw_config):
        raise _manifest_error("persisted_cutoff_forbidden")
    if set(raw_config) != _MANUAL_CENSUS_FIELDS:
        raise _manifest_error("manual_contract_fields")
    if (
        type(raw_config.get("contract_version")) is not int
        or raw_config["contract_version"] != 3
    ):
        raise _manifest_error("contract_version_invalid")
    plan_name = _strict_text(raw_config.get("plan_name"), field_name="plan_name")
    seed_source = _strict_text(
        raw_config.get("seed_source"),
        field_name="seed_source",
    )
    continuation_strategy, fixed_identity_by_field = (
        _validated_manual_fixed_identity(raw_config)
    )
    expected_nonempty_resources = _strict_resources(
        raw_config.get("expected_nonempty_resources"),
        field_name="expected_nonempty_resources",
    )
    page_count = raw_config.get("page_count")
    if type(page_count) is not int or not 1 <= page_count <= 1000:
        raise _manifest_error("page_count_invalid")
    verification_campaign_id = _strict_slug(
        raw_config.get("verification_campaign_id"),
        field_name="verification_campaign_id",
    )
    return {
        "plan_name": plan_name,
        "seed_source": seed_source,
        "continuation_strategy": continuation_strategy,
        "expected_nonempty_resources": expected_nonempty_resources,
        "page_count": page_count,
        "contract_version": 3,
        **fixed_identity_by_field,
        "verification_campaign_id": verification_campaign_id,
        "start_urls": _validated_start_urls(
            raw_config.get("start_urls"),
            canonical_base=canonical_base,
            resources=resources,
        ),
    }


def _validated_manual_fixed_identity(
    raw_config: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Validate fixed strategy fields and return their canonical projection."""

    continuation_strategy = _strict_text(
        raw_config.get("continuation_strategy"),
        field_name="continuation_strategy",
    )
    if continuation_strategy != SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY:
        raise _manifest_error("continuation_strategy_invalid")
    fixed_identity_by_field = {
        "semantics": SERVER_ISSUED_SUBSET_SEMANTICS,
        "strategy_version": SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        "traversal_version": SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
        "canonicalization_version": (
            SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        ),
        "completion_scopes": list(SERVER_ISSUED_SUBSET_COMPLETION_SCOPES),
    }
    if any(
        raw_config.get(field_name) != expected_identity
        for field_name, expected_identity in fixed_identity_by_field.items()
    ):
        raise _manifest_error("v3_identity_invalid")
    return continuation_strategy, fixed_identity_by_field


def _stable_seed_source_id(seed_row: Mapping[str, Any]) -> str:
    identity_parts = (
        "",
        str(seed_row["org_name"]),
        str(seed_row["plan_name"]),
        str(seed_row["api_base"]),
        str(seed_row["source"]),
    )
    digest = hashlib.sha256("|".join(identity_parts).encode("utf-8")).hexdigest()
    return f"pdfhir_{digest[:24]}"


def _validated_manual_entry(
    raw_entry: dict[str, Any],
    requested_source_id: str,
) -> dict[str, Any]:
    if _has_persisted_cutoff(raw_entry):
        raise _manifest_error("persisted_cutoff_forbidden")
    if set(raw_entry) != _MANUAL_ENTRY_FIELDS:
        raise _manifest_error("entry_fields")
    _strict_slug(raw_entry.get("entry_id"), field_name="entry_id")
    display_name = _strict_text(
        raw_entry.get("display_name"),
        field_name="display_name",
    )
    _strict_slug(raw_entry.get("owner_id"), field_name="owner_id")
    source_ids = raw_entry.get("source_ids")
    if (
        type(source_ids) is not list
        or len(source_ids) != 1
        or type(source_ids[0]) is not str
        or source_ids[0] != source_ids[0].strip()
        or source_ids[0] != requested_source_id
    ):
        raise _manifest_error("source_ids_invalid")
    if raw_entry.get("launch_mode") != MANUAL_ACQUISITION_LAUNCH_MODE:
        raise _manifest_error("launch_mode_invalid")
    if raw_entry.get("resource_profile") != MANUAL_RESOURCE_PROFILE:
        raise _manifest_error("resource_profile_invalid")
    resources = _strict_resources(
        raw_entry.get("resources"),
        field_name="resources",
    )
    canonical_base = _validated_base(raw_entry.get("canonical_base"))
    config = _validated_manual_config(
        raw_entry,
        canonical_base=canonical_base,
        resources=resources,
    )
    return {
        "entry_id": raw_entry["entry_id"],
        "display_name": display_name,
        "source_id": source_ids[0],
        "canonical_base": canonical_base,
        "resources": resources,
        **config,
    }


def _manual_seed_row(
    entry: Mapping[str, Any],
    root_policy: ReviewedRootPolicy | None = None,
) -> dict[str, Any]:
    """Build one deterministic dormant seed and guard its opaque identity."""

    resources = tuple(entry["resources"])
    page_count = int(entry["page_count"])
    canonical_base = str(entry["canonical_base"])
    seed_row_by_field = {
        "id": entry["entry_id"],
        "org_name": entry["display_name"],
        "plan_name": entry["plan_name"],
        "api_base": canonical_base,
        "auth_type": "none",
        "requires_registration": False,
        "source": entry["seed_source"],
        "source_detail": "reviewed manual server-issued traversal subset source",
        "source_url": canonical_base,
        "note": (
            "Manual-only source state is controlled by reviewed subset evidence; "
            "publication remains separately proof-gated."
        ),
        "metadata_json": manual_seed_metadata(
            entry,
            resources,
            page_count,
            canonical_base,
            root_policy,
        ),
    }
    if _stable_seed_source_id(seed_row_by_field) != entry["source_id"]:
        raise _manifest_error("source_identity_drift")
    return seed_row_by_field


def reviewed_manual_census_seed_rows(
    requested_source_id: str,
    *,
    manifest_path: Path = DEFAULT_MANUAL_SOURCE_MANIFEST,
    root_policy: ReviewedRootPolicy | None = None,
) -> list[dict[str, Any]]:
    """Return the sole local reviewed seed bound to an opaque source ID."""

    source_id = _strict_text(
        requested_source_id,
        field_name="requested_source_id",
    )
    manifest = _load_manifest(manifest_path)
    raw_entry = _manual_entry_for_source(manifest, source_id)
    entry = _validated_manual_entry(raw_entry, source_id)
    return [_manual_seed_row(entry, root_policy)]


def reviewed_manual_census_source_id(
    *,
    manifest_path: Path = DEFAULT_MANUAL_SOURCE_MANIFEST,
) -> str:
    """Resolve the sole reviewed manual source without accepting a selector."""

    manifest = _load_manifest(manifest_path)
    manual_entries = [
        raw_entry
        for raw_entry in manifest["entries"]
        if type(raw_entry) is dict
        and raw_entry.get("classification")
        == MANUAL_ACQUISITION_CLASSIFICATION
    ]
    if len(manual_entries) != 1:
        raise _manifest_error("source_resolution_ambiguous")
    source_ids = manual_entries[0].get("source_ids")
    if type(source_ids) is not list or len(source_ids) != 1:
        raise _manifest_error("source_ids_invalid")
    source_id = _strict_text(source_ids[0], field_name="source_id")
    resolved_entry = _manual_entry_for_source(manifest, source_id)
    validated_entry = _validated_manual_entry(resolved_entry, source_id)
    _manual_seed_row(validated_entry)
    return source_id
