# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owned Source membership lifetime bound only from closed server configuration."""

from dataclasses import dataclass, replace

from process.registry_fhir_catalog_sources import parse_fhir_catalog_source_profile
from process.registry_source_recipe_composition import SourceWitnessRuntime, validate_source_witness_configuration


@dataclass(frozen=True, repr=False)
class RegistrySourceWitnessConfig:
    source_profiles: tuple
    capture_owner_role: str
    runtime_roles: tuple[str, ...]

    def __post_init__(self):
        validate_source_witness_configuration(self.source_profiles, self.capture_owner_role, self.runtime_roles)


def parse_source_witness_config(document):
    """Accept profiles and actors, never a factory, connection or request-selected authority."""
    if (
        type(document) is not dict
        or set(document) != {"source_profiles", "capture_owner_role", "runtime_roles"}
        or type(document["source_profiles"]) is not list
        or type(document["runtime_roles"]) is not list
    ):
        raise ValueError("registry_source_witness_config_invalid")
    return RegistrySourceWitnessConfig(
        tuple(parse_fhir_catalog_source_profile(profile) for profile in document["source_profiles"]),
        document["capture_owner_role"],
        tuple(document["runtime_roles"]),
    )


async def install_source_witness_runtime(config, resources, *, membership_refresh_request_id=None):
    """Close the original membership factory after all worker callbacks and owned pools drain."""
    settings = config.source_witness
    address_sources = config.initial_address_sources
    existing = address_sources.source_witness_runtime if address_sources is not None else None
    if settings is None and existing is None:
        return config
    if membership_refresh_request_id is not None:
        raise ValueError("registry_source_witness_refresh_unsupported")
    if config.custody_url is None or address_sources is None or not address_sources.compact:
        raise ValueError("registry_source_witness_worker_config_invalid")
    if settings is None:
        existing.__post_init__()
        return config
    if type(settings) is not RegistrySourceWitnessConfig or existing is not None:
        raise ValueError("registry_source_witness_worker_config_invalid")
    settings.__post_init__()
    try:
        from directory_membership_runtime import membership_source_export_runtime
    except ImportError:
        raise ValueError("registry_source_witness_factory_unavailable") from None
    if not callable(membership_source_export_runtime):
        raise ValueError("registry_source_witness_factory_unavailable")
    source_export = await resources.enter_async_context(membership_source_export_runtime())
    runtime = SourceWitnessRuntime(
        source_export, settings.source_profiles, settings.capture_owner_role, settings.runtime_roles
    )
    return replace(
        config, source_witness=None, initial_address_sources=replace(address_sources, source_witness_runtime=runtime)
    )
