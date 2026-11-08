# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Declare exact registry coordinates in immutable directory publication metadata."""

from dataclasses import asdict

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates

_FIELDS = {
    "source_system",
    "source_id",
    "dataset_schema",
    "dataset_id",
    "producer_id",
    "edition_id",
    "alias_scope",
    "source_key_kind",
}


def fhir_network_binding_metadata(*, source_id, dataset_schema, dataset_id, producer_id, alias_scope):
    """The published dataset identity is this publisher's explicit edition namespace.

    Producer and alias scope are explicit import configuration. This declaration
    does not assign a canonical network or assert insurer ownership.
    """
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source_id, dataset_schema, dataset_id, producer_id, dataset_id
    )
    try:
        byte_count = len(alias_scope.encode()) if type(alias_scope) is str else 0
    except UnicodeError:
        raise ValueError("provider_directory_registry_network_metadata_invalid") from None
    if (
        type(alias_scope) is not str
        or not 1 <= byte_count <= 512
        or alias_scope.strip() != alias_scope
        or not alias_scope.isprintable()
    ):
        raise ValueError("provider_directory_registry_network_metadata_invalid")
    return {**asdict(coordinates), "alias_scope": alias_scope, "source_key_kind": "organization_resource_id"}


def configured_fhir_network_metadata(source_records, source_ids, dataset_schema, dataset_id, metadata_reader):
    """Read an explicitly configured publisher namespace before candidate sealing."""
    declarations = [metadata_reader(source).get("registry_network_source") for source in source_records]
    if not any(declaration is not None for declaration in declarations):
        return None
    if len(source_records) != 1 or len(source_ids) != 1:
        raise ValueError("provider_directory_registry_network_metadata_invalid")
    declaration = declarations[0]
    if type(declaration) is not dict or set(declaration) != {"producer_id", "alias_scope"}:
        raise ValueError("provider_directory_registry_network_metadata_invalid")
    return fhir_network_binding_metadata(
        source_id=source_ids[0],
        dataset_schema=dataset_schema,
        dataset_id=dataset_id,
        producer_id=declaration["producer_id"],
        alias_scope=declaration["alias_scope"],
    )


def candidate_registry_network_metadata(candidate, dataset_schema):
    """Bind a declared descriptor to this exact candidate before sealing it."""
    declaration = candidate.registry_network_binding_metadata
    if declaration is None:
        return {}
    if type(declaration) is not dict or set(declaration) != _FIELDS or len(candidate.source_ids) != 1:
        raise ValueError("provider_directory_registry_network_metadata_invalid")
    expected = fhir_network_binding_metadata(
        source_id=candidate.source_ids[0],
        dataset_schema=dataset_schema,
        dataset_id=candidate.dataset_id,
        producer_id=declaration["producer_id"],
        alias_scope=declaration["alias_scope"],
    )
    if declaration != expected:
        raise ValueError("provider_directory_registry_network_metadata_invalid")
    return {"network_bindings": expected}


def candidate_hash_metadata(proof_resource_scope, semantic_projection_as_of, scope_key, projection_key):
    """Keep the existing optional proof/date metadata unchanged."""
    return {
        **({scope_key: list(proof_resource_scope)} if proof_resource_scope is not None else {}),
        **({projection_key: semantic_projection_as_of} if semantic_projection_as_of is not None else {}),
    }
