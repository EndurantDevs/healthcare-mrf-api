# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve an exact retained registry generation by its logical publication locator."""

from process.network_address_projection import _identifier
from process.network_membership_validation import _json_object


class RegistryRetainedGenerationError(ValueError):
    """The selected published logical generation is unavailable or inconsistent."""


async def resolve_registry_retained_generation(connection, candidate, *, control_schema):
    """Read retained routing names while keeping the selected edition and rollback state."""
    from process.network_published_api_pin import published_network_layout
    from process.network_serving_read import _pinned_manifest, _read_manifest

    locator = _json_object(candidate["source_generations"]).get("retained_network_source")
    if locator is None:
        return None
    manifest_rows = await connection.fetch(
        f"SELECT c.*,m.generation_id FROM {_identifier(control_schema)}.network_serving_manifest m "
        f"JOIN {_identifier(control_schema)}.network_membership_candidate c USING(candidate_id) "
        "WHERE m.manifest_sha256=$1 LIMIT 2",
        locator,
    )
    if len(manifest_rows) != 1 or manifest_rows[0]["state"] != "published":
        raise RegistryRetainedGenerationError("registry_retained_generation_unavailable")
    retained_candidate_by_field = dict(manifest_rows[0])
    entry = await _read_manifest(
        connection,
        _identifier(control_schema),
        retained_candidate_by_field["generation_id"],
        include_ineligible=True,
    )
    if entry is None:
        raise RegistryRetainedGenerationError("registry_retained_generation_unavailable")
    published_candidate_by_field = _json_object(entry["candidate_json"])
    manifest = _pinned_manifest(entry, layout=published_network_layout(published_candidate_by_field))
    if (
        manifest.manifest_sha256 != locator
        or manifest.generation_id != retained_candidate_by_field["generation_id"]
        or manifest.candidate_id != str(retained_candidate_by_field["candidate_id"])
        or manifest.schema_name != retained_candidate_by_field["schema_name"]
        or manifest.source_generations != _json_object(retained_candidate_by_field["source_generations"])
        or any(
            str(published_candidate_by_field[field]) != str(retained_candidate_by_field[field])
            for field in ("dataset_id", "schema_id", "producer_id", "schema_revision", "approved_custom_revision")
        )
    ):
        raise RegistryRetainedGenerationError("registry_retained_generation_changed")
    return manifest, retained_candidate_by_field
