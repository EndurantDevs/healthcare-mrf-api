# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate an exact retained registry generation without reactivating it."""

from process.network_address_projection import _identifier
from process.network_membership_validation import _json_object


class RegistryRetainedGenerationError(ValueError):
    """The selected manifest or its independent physical custody is unavailable."""


async def resolve_registry_retained_generation(connection, candidate, *, control_schema):
    """The candidate's digest is a locator; verify the stored manifest and seal."""
    from process.network_membership_publication import _candidate_custody
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
    custody = await _candidate_custody(connection, retained_candidate_by_field, control_schema)
    if custody is None:
        raise RegistryRetainedGenerationError("registry_retained_generation_unsealed")
    entry = await _read_manifest(
        connection,
        _identifier(control_schema),
        retained_candidate_by_field["generation_id"],
        include_ineligible=True,
    )
    if entry is None:
        raise RegistryRetainedGenerationError("registry_retained_generation_unavailable")
    manifest = _pinned_manifest(entry, custody=custody)
    if manifest.manifest_sha256 != locator:
        raise RegistryRetainedGenerationError("registry_retained_generation_changed")
    return manifest, retained_candidate_by_field
